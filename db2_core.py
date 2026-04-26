"""
DB2 Core
========
This module owns DB2-specific runtime behavior for the app and the OSS
pipeline layer.

What belongs here:
- DB2 connection management
- DB2 query execution through the Python driver path
- DB2 result inspection/introspection
- DB2 -> DuckDB ingress flows and DB2-specific mapping metadata

What does not belong here:
- generic DuckDB catalog/object-detail behavior
- PostgreSQL or other connector logic
- notebook-specific orchestration beyond calling this connector core

Keep this file focused on DB2 as a source/target boundary. If new logic is not
DB2-specific, it should usually live somewhere else.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit

from pydantic import BaseModel, Field


def _bootstrap_windows_ibm_db_dlls() -> None:
    if os.name != "nt" or not hasattr(os, "add_dll_directory"):
        return

    candidate_dirs: list[Path] = []
    for entry in sys.path:
        if not entry:
            continue
        base = Path(entry)
        if base.name != "site-packages":
            continue
        candidate_dirs.append(base / "clidriver" / "bin")

    backend_root = Path(__file__).resolve().parent
    candidate_dirs.append(backend_root / ".venv" / "Lib" / "site-packages" / "clidriver" / "bin")

    seen: set[str] = set()
    for candidate in candidate_dirs:
        candidate_str = str(candidate)
        if candidate_str in seen or not candidate.is_dir():
            continue
        seen.add(candidate_str)
        os.add_dll_directory(candidate_str)


_bootstrap_windows_ibm_db_dlls()

try:
    import ibm_db
    import ibm_db_dbi
except ImportError:
    ibm_db = None
    ibm_db_dbi = None

try:
    from . import base as _base_models
    from .duckdb_driver import connect_duckdb
    from .queron.runtime_models import ColumnMappingRecord
except ImportError:
    import base as _base_models
    from duckdb_driver import connect_duckdb
    from queron.runtime_models import ColumnMappingRecord

ConnectResponse = _base_models.ConnectResponse
Db2ConnectRequest = _base_models.Db2ConnectRequest
DuckDbConnectRequest = _base_models.DuckDbConnectRequest
DuckDbIngestQueryResponse = _base_models.DuckDbIngestQueryResponse
ColumnMeta = _base_models.ColumnMeta
TestConnectionResponse = _base_models.TestConnectionResponse


class _FallbackConnectorEgressResponse(BaseModel):
    target_name: str
    row_count: int
    warnings: list[str] = Field(default_factory=list)
    column_mappings: list[ColumnMappingRecord] = Field(default_factory=list)
    created_at: float
    schema_changed: bool = False


ConnectorEgressResponse = getattr(_base_models, "ConnectorEgressResponse", _FallbackConnectorEgressResponse)
_connections: dict[str, dict[str, Any]] = {}
_DUCKDB_DECIMAL_RE = re.compile(r"^DECIMAL\((\d+),\s*(\d+)\)$", re.IGNORECASE)
_DBAPI_TYPE_OBJECT_RE = re.compile(r"^DBAPITYPEOBJECT\((.+)\)$", re.IGNORECASE)
_VALID_DB2_AUTH_MODES = {"basic", "tls"}


@dataclass
class Db2MappedColumn:
    name: str
    source_type: str
    duckdb_type: str
    nullable: bool
    warnings: list[str]
    lossy: bool = False


def _require_driver() -> None:
    if ibm_db is None or ibm_db_dbi is None:
        raise RuntimeError("IBM Db2 driver is not installed. Install the 'ibm-db' package first.")


def _sanitize_chunk_size(chunk_size: int | None) -> int:
    size = int(chunk_size or 200)
    if size < 1:
        return 200
    return min(size, 5000)


def _build_db2_interruptor(conn: Any, cur: Any | None = None) -> tuple[Callable[[], dict[str, Any]], dict[str, Any]]:
    state: dict[str, Any] = {
        "db2_statement_handle_present": False,
        "db2_connection_handle_present": False,
        "db2_free_result_attempted": False,
        "db2_raw_close_attempted": False,
        "db2_connection_close_attempted": False,
    }

    def _interrupt() -> None:
        stmt_handler = getattr(cur, "stmt_handler", None) if cur is not None else None
        conn_handler = getattr(conn, "conn_handler", None)
        state["db2_statement_handle_present"] = stmt_handler is not None
        state["db2_connection_handle_present"] = conn_handler is not None
        if stmt_handler is not None and ibm_db is not None and hasattr(ibm_db, "free_result"):
            state["db2_free_result_attempted"] = True
            try:
                ibm_db.free_result(stmt_handler)
            except Exception:
                pass
        if conn_handler is not None and ibm_db is not None and hasattr(ibm_db, "close"):
            state["db2_raw_close_attempted"] = True
            try:
                ibm_db.close(conn_handler)
            except Exception:
                pass
        state["db2_connection_close_attempted"] = True
        try:
            conn.close()
        except Exception:
            pass
        return dict(state)

    return _interrupt, state


def _annotate_db2_interrupt_exception(exc: Exception, state: dict[str, Any] | None) -> Exception:
    payload = dict(getattr(exc, "queron_details", {}) or {})
    interrupt_state = dict(state or {})
    if not any(bool(interrupt_state.get(key)) for key in interrupt_state):
        return exc
    payload.update(interrupt_state)
    setattr(exc, "queron_details", payload)
    return exc


def _build_duckdb_interruptor(conn: Any) -> Callable[[], None]:
    def _interrupt() -> None:
        interrupt = getattr(conn, "interrupt", None)
        if callable(interrupt):
            try:
                interrupt()
            except Exception:
                pass
        try:
            conn.close()
        except Exception:
            pass

    return _interrupt


def _sanitize_conn_value(value: Any) -> str:
    return str(value).strip()


def _conn_str_from_parts(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connect_timeout_seconds: int | None = None,
    extra_keywords: dict[str, Any] | None = None,
    include_credentials: bool = True,
) -> str:
    parts = [
        f"DATABASE={_sanitize_conn_value(database)}",
        f"HOSTNAME={_sanitize_conn_value(host)}",
        f"PORT={int(port)}",
        "PROTOCOL=TCPIP",
    ]
    if connect_timeout_seconds is not None:
        parts.append(f"CONNECTTIMEOUT={int(connect_timeout_seconds)}")
    if include_credentials and username:
        parts.append(f"UID={_sanitize_conn_value(username)}")
    if include_credentials and password:
        parts.append(f"PWD={password}")
    for key, value in dict(extra_keywords or {}).items():
        text = _sanitize_conn_value(value)
        if not text:
            continue
        parts.append(f"{_sanitize_conn_value(key)}={text}")
    return ";".join(parts) + ";"


def _parse_jdbc_url(raw: str) -> str | None:
    jdbc = raw.strip()
    if jdbc.lower().startswith("jdbc:"):
        jdbc = jdbc[5:]
    if not jdbc.lower().startswith("db2://"):
        return None

    parsed = urlsplit(jdbc)
    database = parsed.path.lstrip("/")
    if not parsed.hostname or not database:
        return None

    return _conn_str_from_parts(
        host=parsed.hostname,
        port=parsed.port or 50000,
        database=database,
        username="",
        password="",
    )


def _conn_str_has_credentials(conn_str: str) -> bool:
    normalized = conn_str.upper()
    return "UID=" in normalized or "PWD=" in normalized


def _conn_str_has_keyword(conn_str: str, keyword: str) -> bool:
    normalized = conn_str.upper()
    return f"{str(keyword).upper()}=" in normalized


def _infer_db2_auth_mode(cfg: Db2ConnectRequest) -> str:
    explicit = str(getattr(cfg, "auth_mode", "") or "").strip().lower()
    if explicit:
        if explicit not in _VALID_DB2_AUTH_MODES:
            raise RuntimeError(f"Unsupported DB2 auth_mode '{cfg.auth_mode}'.")
        return explicit
    if (
        str(getattr(cfg, "ssl_server_certificate", "") or "").strip()
        or str(getattr(cfg, "ssl_client_keystoredb", "") or "").strip()
        or str(getattr(cfg, "ssl_client_keystash", "") or "").strip()
        or str(getattr(cfg, "ssl_client_keystore_password", "") or "").strip()
    ):
        return "tls"
    return "basic"


def _validate_db2_auth_request(cfg: Db2ConnectRequest, auth_mode: str) -> None:
    if auth_mode not in _VALID_DB2_AUTH_MODES:
        raise RuntimeError(f"Unsupported DB2 auth_mode '{auth_mode}'.")
    ssl_server_certificate = str(getattr(cfg, "ssl_server_certificate", "") or "").strip()
    ssl_client_keystoredb = str(getattr(cfg, "ssl_client_keystoredb", "") or "").strip()
    ssl_client_keystash = str(getattr(cfg, "ssl_client_keystash", "") or "").strip()
    ssl_client_keystore_password = str(getattr(cfg, "ssl_client_keystore_password", "") or "").strip()
    if auth_mode == "tls" and not ssl_server_certificate and not ssl_client_keystoredb:
        raise RuntimeError("DB2 tls auth_mode requires ssl_server_certificate or ssl_client_keystoredb.")
        if ssl_client_keystoredb and not ssl_client_keystash and not ssl_client_keystore_password:
            raise RuntimeError("DB2 tls auth_mode with ssl_client_keystoredb requires ssl_client_keystash or ssl_client_keystore_password.")


def _build_db2_auth_keywords(cfg: Db2ConnectRequest, auth_mode: str) -> dict[str, Any]:
    if auth_mode == "basic":
        return {}
    keywords: dict[str, Any] = {"Security": "SSL"}
    ssl_server_certificate = str(getattr(cfg, "ssl_server_certificate", "") or "").strip()
    ssl_client_keystoredb = str(getattr(cfg, "ssl_client_keystoredb", "") or "").strip()
    ssl_client_keystash = str(getattr(cfg, "ssl_client_keystash", "") or "").strip()
    ssl_client_keystore_password = str(getattr(cfg, "ssl_client_keystore_password", "") or "").strip()
    if ssl_server_certificate:
        keywords["SSLServerCertificate"] = ssl_server_certificate
    if ssl_client_keystoredb:
        keywords["SSLClientKeystoredb"] = ssl_client_keystoredb
    if ssl_client_keystash:
        keywords["SSLClientKeystash"] = ssl_client_keystash
    if ssl_client_keystore_password:
        keywords["SSLClientKeyStoreDBPassword"] = ssl_client_keystore_password
    return keywords


def _resolved_db2_connect_config(cfg: Db2ConnectRequest) -> dict[str, Any]:
    auth_mode = _infer_db2_auth_mode(cfg)
    _validate_db2_auth_request(cfg, auth_mode)
    auth_keywords = _build_db2_auth_keywords(cfg, auth_mode)
    if cfg.url:
        raw = cfg.url.strip()
        conn_str = _parse_jdbc_url(raw) or raw
        if not _conn_str_has_credentials(conn_str):
            suffix = []
            if cfg.username:
                suffix.append(f"UID={_sanitize_conn_value(cfg.username)}")
            if cfg.password:
                suffix.append(f"PWD={cfg.password}")
            if suffix:
                if not conn_str.endswith(";"):
                    conn_str += ";"
                conn_str += ";".join(suffix) + ";"
        if cfg.connect_timeout_seconds is not None and "CONNECTTIMEOUT=" not in conn_str.upper():
            if not conn_str.endswith(";"):
                conn_str += ";"
            conn_str += f"CONNECTTIMEOUT={int(cfg.connect_timeout_seconds)};"
        for key, value in auth_keywords.items():
            if _conn_str_has_keyword(conn_str, key):
                continue
            if not conn_str.endswith(";"):
                conn_str += ";"
            conn_str += f"{key}={_sanitize_conn_value(value)};"
        return {"auth_mode": auth_mode, "conn_str": conn_str}

    return {
        "auth_mode": auth_mode,
        "conn_str": _conn_str_from_parts(
            host=cfg.host,
            port=cfg.port,
            database=cfg.database,
            username=cfg.username,
            password=cfg.password,
            connect_timeout_seconds=cfg.connect_timeout_seconds,
            extra_keywords=auth_keywords,
        ),
    }


def _db2_connection_string_from_request(cfg: Db2ConnectRequest) -> str:
    return str(_resolved_db2_connect_config(cfg)["conn_str"])


def _dsn_from_config(cfg: Db2ConnectRequest) -> str:
    return _db2_connection_string_from_request(cfg)


def _config_from_connection_id(connection_id: str) -> str:
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("Connection not found. Please connect first.")
    return _db2_connection_string_from_request(Db2ConnectRequest(**cfg_dict))


def _dbapi_connect(conn_str: str):
    _require_driver()
    try:
        return ibm_db_dbi.connect(conn_str, "", "")
    except AttributeError:
        raw_conn = ibm_db.connect(conn_str, "", "")
        return ibm_db_dbi.Connection(raw_conn)


def _normalize_db2_display_type(type_name: str) -> str:
    raw = (type_name or "").strip()
    if not raw:
        return "UNKNOWN"
    match = _DBAPI_TYPE_OBJECT_RE.match(raw)
    if match:
        tokens = {token.strip().upper() for token in re.findall(r"'([^']+)'", match.group(1)) if token.strip()}
        if "BIGINT" in tokens:
            return "BIGINT"
        if "INTEGER" in tokens or "INT" in tokens:
            return "INTEGER"
        if "SMALLINT" in tokens:
            return "SMALLINT"
        if "DECIMAL" in tokens or "NUMERIC" in tokens:
            return "DECIMAL"
        if "DECFLOAT" in tokens:
            return "DECFLOAT"
        if "DOUBLE" in tokens or "DOUBLE PRECISION" in tokens:
            return "DOUBLE"
        if "REAL" in tokens:
            return "REAL"
        if "TIMESTAMP" in tokens:
            return "TIMESTAMP"
        if "DATE" in tokens:
            return "DATE"
        if "TIME" in tokens:
            return "TIME"
        if "VARCHAR" in tokens or "CHARACTER VARYING" in tokens or "CHAR VARYING" in tokens or "STRING" in tokens:
            return "VARCHAR"
        if "CHARACTER" in tokens or "CHAR" in tokens:
            return "CHAR"
        if "GRAPHIC" in tokens or "VARGRAPHIC" in tokens:
            return "VARCHAR"
        if "BLOB" in tokens or "BINARY" in tokens or "VARBINARY" in tokens:
            return "BLOB"
        if "BOOLEAN" in tokens or "BOOL" in tokens:
            return "BOOLEAN"
    upper = raw.upper()
    if upper.startswith("CHARACTER VARYING"):
        return "VARCHAR" + raw[len("CHARACTER VARYING") :]
    if upper.startswith("CHARACTER"):
        return "CHAR" + raw[len("CHARACTER") :]
    if upper.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    if upper.startswith("DOUBLE"):
        return "DOUBLE"
    if upper.startswith("REAL"):
        return "REAL"
    if upper.startswith("INTEGER"):
        return "INTEGER"
    if upper.startswith("SMALLINT"):
        return "SMALLINT"
    if upper.startswith("BIGINT"):
        return "BIGINT"
    return upper


def _type_name_from_description(desc: Any) -> str:
    type_code = desc[1] if len(desc) > 1 else None
    if type_code is None:
        return "UNKNOWN"
    if isinstance(type_code, str):
        return _normalize_db2_display_type(type_code)
    name = getattr(type_code, "__name__", None)
    if name:
        return _normalize_db2_display_type(name)
    return _normalize_db2_display_type(str(type_code))


def _normalize_db2_source_type(
    raw_type: str,
    *,
    precision: int | None,
    scale: int | None,
) -> str:
    t = (raw_type or "").strip().upper()
    if t in {"BOOLEAN", "BOOL"}:
        return "bool"
    if t in {"SMALLINT"}:
        return "int16"
    if t in {"INTEGER", "INT"}:
        return "int32"
    if t in {"BIGINT"}:
        return "int64"
    if t in {"REAL"}:
        return "float32"
    if t in {"DOUBLE", "DOUBLE PRECISION", "FLOAT"}:
        return "float64"
    if t in {"DATE"}:
        return "date"
    if t in {"TIME"}:
        return "time"
    if t.startswith("TIMESTAMP"):
        return "timestamp"
    if t.startswith("DECIMAL") or t.startswith("NUMERIC"):
        if isinstance(precision, int):
            s = int(scale or 0)
            return f"decimal({precision},{s})"
        return "decimal"
    if t.startswith("DECFLOAT"):
        return "float64"
    if (
        t.startswith("CHAR")
        or t.startswith("VARCHAR")
        or t.startswith("CLOB")
        or t.startswith("DBCLOB")
        or t.startswith("GRAPHIC")
        or t.startswith("VARGRAPHIC")
        or t.startswith("XML")
    ):
        return "string"
    if t.startswith("BLOB") or t.startswith("BINARY") or t.startswith("VARBINARY"):
        return "binary"
    return "unknown"


def _canonical_to_duckdb(canonical: str) -> tuple[str, list[str], bool]:
    warnings: list[str] = []
    c = (canonical or "unknown").strip().lower()
    if c == "bool":
        return "BOOLEAN", warnings, False
    if c == "int16":
        return "SMALLINT", warnings, False
    if c == "int32":
        return "INTEGER", warnings, False
    if c == "int64":
        return "BIGINT", warnings, False
    if c == "float32":
        return "REAL", warnings, False
    if c == "float64":
        return "DOUBLE", warnings, False
    if c == "string":
        return "VARCHAR", warnings, False
    if c == "date":
        return "DATE", warnings, False
    if c == "time":
        return "TIME", warnings, False
    if c == "timestamp":
        return "TIMESTAMP", warnings, False
    if c == "binary":
        return "BLOB", warnings, False
    if c.startswith("decimal(") and c.endswith(")"):
        try:
            args = c[len("decimal(") : -1]
            p_str, s_str = [part.strip() for part in args.split(",", 1)]
            precision = int(p_str)
            scale = int(s_str)
            if precision > 38:
                warnings.append(
                    f"DECIMAL precision {precision} exceeds DuckDB max precision 38. Mapping to DOUBLE."
                )
                return "DOUBLE", warnings, True
            return f"DECIMAL({precision},{scale})", warnings, False
        except Exception:
            warnings.append("Invalid DB2 DECIMAL metadata. Mapping to VARCHAR.")
            return "VARCHAR", warnings, True
    if c == "decimal":
        warnings.append("DECIMAL without precision/scale mapped to DECIMAL(38,10).")
        return "DECIMAL(38,10)", warnings, False
    warnings.append(f"Unrecognized DB2 type '{canonical}' mapped to VARCHAR.")
    return "VARCHAR", warnings, True


def _map_description_to_columns(description: Any) -> list[Db2MappedColumn]:
    mapped: list[Db2MappedColumn] = []
    if not description:
        return mapped
    for desc in description:
        name = str(desc[0]) if len(desc) > 0 else "column"
        raw_type = _type_name_from_description(desc)
        precision = desc[4] if len(desc) > 4 and isinstance(desc[4], int) else None
        scale = desc[5] if len(desc) > 5 and isinstance(desc[5], int) else None
        null_ok = bool(desc[6]) if len(desc) > 6 and desc[6] is not None else True
        canonical = _normalize_db2_source_type(raw_type, precision=precision, scale=scale)
        duckdb_type, warnings, lossy = _canonical_to_duckdb(canonical)
        mapped.append(
            Db2MappedColumn(
                name=name,
                source_type=raw_type,
                duckdb_type=duckdb_type,
                nullable=null_ok,
                warnings=warnings,
                lossy=lossy,
            )
        )
    return mapped


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _quote_compound_identifier(identifier: str) -> str:
    parts = [part.strip() for part in str(identifier).split(".") if part.strip()]
    if not parts:
        raise RuntimeError("Target table name is required.")
    return ".".join(_quote_identifier(part) for part in parts)


def _strip_sql_terminator(sql: str) -> str:
    return str(sql or "").strip().rstrip(";")


def _split_external_relation(identifier: str) -> tuple[str | None, str]:
    parts = [part.strip().strip('"') for part in str(identifier or "").split(".") if part.strip()]
    if len(parts) == 1:
        return None, parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    if len(parts) == 3:
        return parts[1], parts[2]
    raise RuntimeError(f"Invalid DB2 relation '{identifier}'.")


def _normalize_db2_target_relation(identifier: str) -> tuple[str | None, str, str]:
    schema_name, table_name = _split_external_relation(identifier)
    normalized = f"{schema_name}.{table_name}" if schema_name else table_name
    return schema_name, table_name, normalized


def _duckdb_column_meta_from_cursor(cursor) -> list[ColumnMeta]:
    if cursor.description is None:
        return []
    metas: list[ColumnMeta] = []
    for desc in cursor.description:
        type_code = desc[1] if len(desc) > 1 else None
        type_name = str(type_code or "UNKNOWN").strip().upper()
        internal_size = desc[3] if len(desc) > 3 else None
        precision = desc[4] if len(desc) > 4 else None
        scale = desc[5] if len(desc) > 5 else None
        null_ok = desc[6] if len(desc) > 6 else None
        metas.append(
            ColumnMeta(
                name=str(desc[0]),
                data_type=type_name,
                max_length=internal_size if isinstance(internal_size, int) and internal_size >= 0 else None,
                precision=precision if isinstance(precision, int) else None,
                scale=scale if isinstance(scale, int) else None,
                nullable=True if null_ok is None else bool(null_ok),
            )
        )
    return metas


def _map_duckdb_column_to_db2(column: ColumnMeta) -> tuple[str, list[str]]:
    normalized = str(column.data_type or "UNKNOWN").strip().upper()
    warnings: list[str] = []
    if normalized in {"BOOLEAN", "BOOL"}:
        return "BOOLEAN", warnings
    if normalized in {"SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE", "DATE", "TIME", "TIMESTAMP"}:
        return normalized, warnings
    if normalized in {"TIMESTAMPTZ"}:
        warnings.append("DuckDB TIMESTAMPTZ was normalized to DB2 TIMESTAMP.")
        return "TIMESTAMP", warnings
    if normalized in {"UUID", "VARCHAR", "JSON"} or normalized.startswith("VARCHAR"):
        return "VARCHAR(32672)", warnings
    if normalized in {"BLOB", "BYTEA"}:
        return "BLOB", warnings
    decimal_match = _DUCKDB_DECIMAL_RE.match(normalized)
    if decimal_match:
        precision = int(decimal_match.group(1))
        scale = int(decimal_match.group(2))
        if precision > 31:
            warnings.append(
                f"DuckDB DECIMAL({precision},{scale}) exceeds DB2 max precision 31 and was widened to DOUBLE."
            )
            return "DOUBLE", warnings
        return f"DECIMAL({precision},{scale})", warnings
    if normalized == "DECIMAL":
        precision = int(column.precision or 31)
        scale = int(column.scale or 0)
        if precision > 31:
            warnings.append(
                f"DuckDB DECIMAL({precision},{scale}) exceeds DB2 max precision 31 and was widened to DOUBLE."
            )
            return "DOUBLE", warnings
        return f"DECIMAL({precision},{scale})", warnings
    warnings.append(f"DuckDB type '{normalized}' was widened to VARCHAR(32672) for DB2 egress.")
    return "VARCHAR(32672)", warnings


def _db2_table_exists(cur, *, schema_name: str | None, table_name: str) -> bool:
    schema = str(schema_name or "").strip()
    if schema:
        cur.execute(
            """
            SELECT 1
            FROM SYSCAT.TABLES
            WHERE UPPER(TABSCHEMA) = UPPER(?) AND UPPER(TABNAME) = UPPER(?)
            FETCH FIRST 1 ROW ONLY
            """,
            (schema, table_name),
        )
    else:
        cur.execute(
            """
            SELECT 1
            FROM SYSCAT.TABLES
            WHERE UPPER(TABNAME) = UPPER(?)
            FETCH FIRST 1 ROW ONLY
            """,
            (table_name,),
        )
    return cur.fetchone() is not None


def _format_db2_remote_type(type_name: Any, *, length: Any = None, scale: Any = None) -> str:
    text = str(type_name or "UNKNOWN").strip() or "UNKNOWN"
    upper = text.upper()
    if upper in {"DECIMAL", "NUMERIC"} and length:
        return f"{upper}({length},{int(scale or 0)})"
    if upper in {"CHARACTER", "CHAR", "VARCHAR", "GRAPHIC", "VARGRAPHIC"} and length:
        return f"{upper}({length})"
    return upper


def _inspect_db2_table_columns(cur, *, schema_name: str | None, table_name: str) -> list[ColumnMeta]:
    schema = str(schema_name or "").strip()
    if schema:
        cur.execute(
            """
            SELECT COLNAME, TYPENAME, LENGTH, SCALE, NULLS
            FROM SYSCAT.COLUMNS
            WHERE UPPER(TABSCHEMA) = UPPER(?) AND UPPER(TABNAME) = UPPER(?)
            ORDER BY COLNO
            """,
            (schema, table_name),
        )
    else:
        cur.execute(
            """
            SELECT COLNAME, TYPENAME, LENGTH, SCALE, NULLS
            FROM SYSCAT.COLUMNS
            WHERE UPPER(TABNAME) = UPPER(?)
            ORDER BY COLNO
            """,
            (table_name,),
        )
    return [
        ColumnMeta(
            name=str(row[0] or "").strip(),
            data_type=_format_db2_remote_type(row[1], length=row[2], scale=row[3]),
            nullable=str(row[4] or "").upper() != "N",
        )
        for row in list(cur.fetchall() or [])
        if str(row[0] or "").strip()
    ]


def _build_egress_inferred_column_mappings(column_meta: list[ColumnMeta]) -> list[ColumnMappingRecord]:
    mappings: list[ColumnMappingRecord] = []
    for index, column in enumerate(column_meta, start=1):
        target_type, column_warnings = _map_duckdb_column_to_db2(column)
        mappings.append(
            ColumnMappingRecord(
                ordinal_position=index,
                source_column=str(column.name),
                source_type=str(column.data_type or "UNKNOWN"),
                target_column=str(column.name),
                target_type=str(target_type or "UNKNOWN"),
                connector_type="db2",
                mapping_mode="egress_inferred",
                warnings=list(column_warnings or []),
                lossy=None,
            )
        )
    return mappings


def _build_egress_remote_schema_column_mappings(
    source_columns: list[ColumnMeta],
    remote_columns: list[ColumnMeta],
    *,
    fallback_mappings: list[ColumnMappingRecord],
) -> list[ColumnMappingRecord]:
    remote_by_name = {str(column.name).lower(): column for column in remote_columns}
    fallback_by_name = {str(mapping.source_column).lower(): mapping for mapping in fallback_mappings}
    mappings: list[ColumnMappingRecord] = []
    for index, source_column in enumerate(source_columns, start=1):
        remote_column = remote_by_name.get(str(source_column.name).lower())
        fallback = fallback_by_name.get(str(source_column.name).lower())
        remote_target_type = str(remote_column.data_type if remote_column is not None else (fallback.target_type if fallback is not None else "UNKNOWN"))
        fallback_target_type = str(fallback.target_type if fallback is not None else "")
        warnings = (
            list(fallback.warnings or [])
            if remote_column is None or remote_target_type.upper() == fallback_target_type.upper()
            else []
        )
        if remote_column is None:
            warnings.append("Remote target column was not found during DB2 schema inspection.")
        mappings.append(
            ColumnMappingRecord(
                ordinal_position=index,
                source_column=str(source_column.name),
                source_type=str(source_column.data_type or "UNKNOWN"),
                target_column=str(remote_column.name if remote_column is not None else source_column.name),
                target_type=remote_target_type,
                connector_type="db2",
                mapping_mode="egress_remote_schema",
                warnings=warnings,
                lossy=fallback.lossy if fallback is not None else None,
            )
        )
    return mappings


def _ensure_db2_schema(cur, schema_name: str | None) -> None:
    schema = str(schema_name or "").strip()
    if not schema:
        return
    try:
        cur.execute(f"CREATE SCHEMA {_quote_identifier(schema)}")
    except Exception:
        pass


def _build_db2_create_table_sql(target_table: str, columns: list[ColumnMeta]) -> tuple[str, list[str]]:
    warnings: list[str] = []
    column_defs: list[str] = []
    for column in columns:
        target_type, column_warnings = _map_duckdb_column_to_db2(column)
        warnings.extend(column_warnings)
        null_part = "" if column.nullable else " NOT NULL"
        column_defs.append(f"{_quote_identifier(column.name)} {target_type}{null_part}")
    return f"CREATE TABLE {_quote_compound_identifier(target_table)} ({', '.join(column_defs)})", warnings


def _coerce_db2_row_value(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, default=str)
    return value


def _build_create_table_sql(table_name: str, columns: list[Db2MappedColumn], *, replace: bool) -> str:
    mode = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE"
    col_defs: list[str] = []
    for col in columns:
        null_part = "" if col.nullable else " NOT NULL"
        col_defs.append(f"{_quote_identifier(col.name)} {col.duckdb_type}{null_part}")
    return f"{mode} {_quote_compound_identifier(table_name)} ({', '.join(col_defs)})"


def _build_column_mappings(columns: list[Db2MappedColumn]) -> list[ColumnMappingRecord]:
    mappings: list[ColumnMappingRecord] = []
    for index, col in enumerate(columns, start=1):
        mappings.append(
            ColumnMappingRecord(
                ordinal_position=index,
                source_column=col.name,
                source_type=col.source_type,
                target_column=col.name,
                target_type=col.duckdb_type,
                connector_type="db2",
                mapping_mode="python_mapped",
                warnings=list(col.warnings or []),
                lossy=col.lossy,
            )
        )
    return mappings


def _duckdb_connection_from_id(connection_id: str):
    try:
        from . import duckdb_core
    except ImportError:
        import duckdb_core

    cfg_dict = duckdb_core._connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("DuckDB connection not found. Please connect first.")
    database = duckdb_core._resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    return duckdb_core._duckdb_connection(database)


def test_connection(req: Db2ConnectRequest):
    conn_str = _dsn_from_config(req)
    conn = None
    cur = None
    try:
        conn = _dbapi_connect(conn_str)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
        cur.fetchone()
        return TestConnectionResponse(success=True, message="Connection successful!")
    except Exception as exc:
        return TestConnectionResponse(success=False, message=str(exc))
    finally:
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def connect(req: Db2ConnectRequest):
    probe = test_connection(req)
    if not probe.success:
        raise RuntimeError(probe.message)
    conn_id = (req.connection_id or "").strip() or str(uuid.uuid4())
    payload = req.model_dump()
    payload["connection_id"] = conn_id
    _connections[conn_id] = payload
    return ConnectResponse(connection_id=conn_id, message="Connected successfully!")


def ingest_query_to_duckdb(
    *,
    source_connection_id: str,
    duckdb_connection_id: str,
    sql: str,
    sql_params: list[Any] | None = None,
    target_table: str,
    replace: bool,
    chunk_size: int,
    pipeline_id: str,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
    on_interrupt_open: Callable[[Callable[[], None]], int] | None = None,
    on_interrupt_close: Callable[[int | None], None] | None = None,
) -> DuckDbIngestQueryResponse:
    conn_str = _config_from_connection_id(source_connection_id)
    source_conn = None
    source_cur = None
    duck_conn = None
    source_interrupt_token: int | None = None
    duck_interrupt_token: int | None = None
    source_interrupt_state: dict[str, Any] | None = None
    inserted = 0
    next_progress_threshold = 1000
    warnings: list[str] = []
    warning_set: set[str] = set()

    try:
        source_conn = _dbapi_connect(conn_str)
        source_cur = source_conn.cursor()
        if on_interrupt_open is not None:
            source_interruptor, source_interrupt_state = _build_db2_interruptor(source_conn, source_cur)
            source_interrupt_token = on_interrupt_open(source_interruptor)
        if sql_params:
            source_cur.execute(sql, tuple(sql_params))
        else:
            source_cur.execute(sql)

        if source_cur.description is None:
            raise RuntimeError("Source query did not return a result set.")

        mapped_columns = _map_description_to_columns(source_cur.description)
        if not mapped_columns:
            raise RuntimeError("No columns found in source result.")

        for col in mapped_columns:
            for warning in col.warnings:
                if warning not in warning_set:
                    warning_set.add(warning)
                    warnings.append(warning)

        duck_conn = _duckdb_connection_from_id(duckdb_connection_id)
        if on_interrupt_open is not None:
            duck_interrupt_token = on_interrupt_open(_build_duckdb_interruptor(duck_conn))
        create_sql = _build_create_table_sql(target_table, mapped_columns, replace=replace)
        duck_conn.execute(create_sql)

        target_ident = _quote_compound_identifier(target_table)
        column_names = [col.name for col in mapped_columns]
        col_sql = ", ".join(_quote_identifier(name) for name in column_names)
        placeholders = ", ".join(["?"] * len(column_names))
        insert_sql = f"INSERT INTO {target_ident} ({col_sql}) VALUES ({placeholders})"

        fetch_size = _sanitize_chunk_size(chunk_size)
        col_count = len(column_names)
        while True:
            rows = source_cur.fetchmany(fetch_size)
            if not rows:
                break
            payload: list[tuple[Any, ...]] = []
            for row in rows:
                if isinstance(row, dict):
                    payload.append(tuple(row.get(col_name) for col_name in column_names))
                else:
                    payload.append(
                        tuple(row[i] if i < len(row) else None for i in range(col_count))
                    )
            if payload:
                duck_conn.executemany(insert_sql, payload)
                inserted += len(payload)
                if on_progress is not None and inserted >= next_progress_threshold:
                    on_progress({"row_count": inserted, "chunk_size": len(payload)})
                    while inserted >= next_progress_threshold:
                        next_progress_threshold += 1000
    except Exception as exc:
        raise _annotate_db2_interrupt_exception(exc, source_interrupt_state)
    finally:
        try:
            if source_cur is not None:
                source_cur.close()
        except Exception:
            pass
        try:
            if source_conn is not None:
                if on_interrupt_close is not None:
                    on_interrupt_close(source_interrupt_token)
                source_conn.close()
        except Exception:
            pass
        try:
            if duck_conn is not None:
                if on_interrupt_close is not None:
                    on_interrupt_close(duck_interrupt_token)
                duck_conn.close()
        except Exception:
            pass

    return DuckDbIngestQueryResponse(
        pipeline_id=pipeline_id,
        target_table=target_table,
        row_count=inserted,
        source_type="db2",
        source_connection_id=source_connection_id,
        warnings=warnings,
        column_mappings=_build_column_mappings(mapped_columns),
        created_at=time.time(),
        schema_changed=True,
    )


def egress_query_from_duckdb(
    *,
    target_connection_id: str,
    duckdb_database: str,
    sql: str,
    sql_params: list[Any] | None = None,
    target_table: str,
    mode: str = "replace",
    chunk_size: int = 1000,
    artifact_table: str | None = None,
    on_interrupt_open: Callable[[Callable[[], None]], int] | None = None,
    on_interrupt_close: Callable[[int | None], None] | None = None,
) -> ConnectorEgressResponse:
    normalized_mode = str(mode or "replace").strip().lower()
    if normalized_mode not in {"replace", "append", "create", "create_append"}:
        raise RuntimeError(
            f"Unsupported DB2 egress mode '{mode}'. Use replace, append, create, or create_append."
        )

    conn_str = _config_from_connection_id(target_connection_id)
    duck_conn = None
    duck_cur = None
    target_conn = None
    target_cur = None
    duck_interrupt_token: int | None = None
    target_interrupt_token: int | None = None
    target_interrupt_state: dict[str, Any] | None = None
    warnings: list[str] = []
    column_mappings: list[ColumnMappingRecord] = []
    row_count = 0
    schema_name, table_name, normalized_target_table = _normalize_db2_target_relation(target_table)
    quoted_target_table = _quote_compound_identifier(normalized_target_table)
    try:
        if artifact_table:
            try:
                from . import duckdb_core
            except ImportError:
                import duckdb_core
            duckdb_core.materialize_egress_artifact(
                database=str(duckdb_database),
                sql=sql,
                target_table=str(artifact_table),
                replace=True,
                on_interrupt_open=on_interrupt_open,
                on_interrupt_close=on_interrupt_close,
            )
            sql = f"SELECT * FROM {duckdb_core._quote_compound_identifier(str(artifact_table))}"

        duck_conn = connect_duckdb(str(duckdb_database))
        if on_interrupt_open is not None:
            duck_interrupt_token = on_interrupt_open(_build_duckdb_interruptor(duck_conn))
        duck_cur = duck_conn.cursor()
        duck_cur.execute(_strip_sql_terminator(sql), list(sql_params or []))
        if duck_cur.description is None:
            raise RuntimeError("DuckDB egress query did not return a result set.")
        column_meta = _duckdb_column_meta_from_cursor(duck_cur)
        if not column_meta:
            raise RuntimeError("DuckDB egress query did not return any columns.")
        column_mappings = _build_egress_inferred_column_mappings(column_meta)

        target_conn = _dbapi_connect(conn_str)
        target_cur = target_conn.cursor()
        if on_interrupt_open is not None:
            target_interruptor, target_interrupt_state = _build_db2_interruptor(target_conn, target_cur)
            target_interrupt_token = on_interrupt_open(target_interruptor)

        if normalized_mode in {"replace", "create", "create_append"}:
            _ensure_db2_schema(target_cur, schema_name)

        table_exists = _db2_table_exists(target_cur, schema_name=schema_name, table_name=table_name)
        if normalized_mode == "replace":
            if table_exists:
                target_cur.execute(f"DROP TABLE {quoted_target_table}")
            create_sql, warnings = _build_db2_create_table_sql(normalized_target_table, column_meta)
            target_cur.execute(create_sql)
        elif normalized_mode == "create":
            if table_exists:
                raise RuntimeError(f"Target table '{quoted_target_table}' already exists.")
            create_sql, warnings = _build_db2_create_table_sql(normalized_target_table, column_meta)
            target_cur.execute(create_sql)
        elif normalized_mode == "create_append":
            if not table_exists:
                create_sql, warnings = _build_db2_create_table_sql(normalized_target_table, column_meta)
                target_cur.execute(create_sql)
        elif not table_exists:
            raise RuntimeError(f"Target table '{quoted_target_table}' does not exist for append mode.")

        column_names = [column.name for column in column_meta]
        col_sql = ", ".join(_quote_identifier(name) for name in column_names)
        placeholders = ", ".join(["?"] * len(column_names))
        insert_sql = f"INSERT INTO {quoted_target_table} ({col_sql}) VALUES ({placeholders})"

        fetch_size = _sanitize_chunk_size(chunk_size)
        while True:
            rows = duck_cur.fetchmany(fetch_size)
            if not rows:
                break
            payload = [tuple(_coerce_db2_row_value(value) for value in row) for row in rows]
            if payload:
                target_cur.executemany(insert_sql, payload)
                row_count += len(payload)
        try:
            target_conn.commit()
        except Exception:
            pass
        try:
            remote_columns = _inspect_db2_table_columns(target_cur, schema_name=schema_name, table_name=table_name)
            if not remote_columns:
                raise RuntimeError("No columns returned for remote target table.")
            column_mappings = _build_egress_remote_schema_column_mappings(
                column_meta,
                remote_columns,
                fallback_mappings=column_mappings,
            )
        except Exception as exc:
            warnings.append(f"DB2 remote schema inspection failed; using inferred egress column mappings. {exc}")
    except Exception as exc:
        raise _annotate_db2_interrupt_exception(exc, target_interrupt_state)
    finally:
        try:
            if duck_cur is not None:
                duck_cur.close()
        except Exception:
            pass
        try:
            if duck_conn is not None:
                if on_interrupt_close is not None:
                    on_interrupt_close(duck_interrupt_token)
                duck_conn.close()
        except Exception:
            pass
        try:
            if target_cur is not None:
                target_cur.close()
        except Exception:
            pass
        try:
            if target_conn is not None:
                if on_interrupt_close is not None:
                    on_interrupt_close(target_interrupt_token)
                target_conn.close()
        except Exception:
            pass

    return ConnectorEgressResponse(
        target_name=quoted_target_table,
        row_count=row_count,
        warnings=warnings,
        column_mappings=column_mappings,
        created_at=time.time(),
        schema_changed=normalized_mode in {"replace", "create", "create_append"},
    )


def drop_table_if_exists(*, connection_id: str, target_table: str) -> None:
    conn_str = _config_from_connection_id(connection_id)
    conn = None
    cur = None
    schema_name, table_name, normalized_target_table = _normalize_db2_target_relation(target_table)
    quoted_target_table = _quote_compound_identifier(normalized_target_table)
    try:
        conn = _dbapi_connect(conn_str)
        cur = conn.cursor()
        if _db2_table_exists(cur, schema_name=schema_name, table_name=table_name):
            cur.execute(f"DROP TABLE {quoted_target_table}")
        try:
            conn.commit()
        except Exception:
            pass
    finally:
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def health():
    return {"status": "ok"}
