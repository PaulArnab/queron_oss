"""
Oracle Core
===========
Oracle-specific connection, ingress, egress, and lookup-table cleanup helpers.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from decimal import Decimal
from typing import Any
from urllib.parse import unquote, urlsplit

try:
    import oracledb
except ImportError:
    oracledb = None

try:
    from . import base as _base_models
    from .duckdb_driver import connect_duckdb
    from .queron.runtime_models import ColumnMappingRecord
except ImportError:
    import base as _base_models
    from duckdb_driver import connect_duckdb
    from queron.runtime_models import ColumnMappingRecord

ColumnMeta = _base_models.ColumnMeta
ConnectResponse = _base_models.ConnectResponse
ConnectorEgressResponse = _base_models.ConnectorEgressResponse
DuckDbIngestQueryResponse = _base_models.DuckDbIngestQueryResponse
OracleConnectRequest = _base_models.OracleConnectRequest
QueryResponse = _base_models.QueryResponse
TestConnectionResponse = _base_models.TestConnectionResponse

_connections: dict[str, dict[str, Any]] = {}
_DEFAULT_QUERY_CHUNK_SIZE = int(getattr(_base_models, "DEFAULT_QUERY_CHUNK_SIZE", 200) or 200)
_VALID_ORACLE_AUTH_MODES = {"basic", "dsn", "tns", "wallet_mtls"}
_DECIMAL_RE = re.compile(r"^DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$")
_VARCHAR_RE = re.compile(r"^VARCHAR\s*\(\s*(\d+)\s*\)$")
_ORACLE_MAX_VARCHAR2_LENGTH = 4000
_ORACLE_MIN_MEASURED_VARCHAR2_LENGTH = 32


class _MappedOracleColumn:
    def __init__(self, *, name: str, source_type: str, target_type: str, warnings: list[str], lossy: bool = False) -> None:
        self.name = name
        self.source_type = source_type
        self.target_type = target_type
        self.warnings = warnings
        self.lossy = lossy


def _require_driver() -> None:
    if oracledb is None:
        raise RuntimeError("Oracle is not installed. Install the 'oracledb' package first.")


def _sanitize_chunk_size(chunk_size: int | None) -> int:
    size = int(chunk_size or _DEFAULT_QUERY_CHUNK_SIZE)
    if size < 1:
        return _DEFAULT_QUERY_CHUNK_SIZE
    return min(size, 5000)


def _strip_sql_terminator(sql: str) -> str:
    return str(sql or "").strip().rstrip(";")


def _quote_identifier(identifier: str) -> str:
    text = str(identifier or "").strip().strip('"')
    if not text:
        raise RuntimeError("Identifier is required.")
    return '"' + text.upper().replace('"', '""') + '"'


def _quote_compound_identifier(identifier: str) -> str:
    parts = [part.strip().strip('"') for part in str(identifier or "").split(".") if part.strip()]
    if not parts:
        raise RuntimeError("Target table name is required.")
    return ".".join(_quote_identifier(part) for part in parts)


def _quote_duckdb_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _quote_duckdb_compound_identifier(identifier: str) -> str:
    parts = [part.strip() for part in str(identifier or "").split(".") if part.strip()]
    if not parts:
        raise RuntimeError("Target table name is required.")
    return ".".join(_quote_duckdb_identifier(part) for part in parts)


def _split_target_table_name(identifier: str) -> tuple[str | None, str]:
    parts = [part.strip().strip('"') for part in str(identifier or "").split(".") if part.strip()]
    if len(parts) == 1:
        return None, parts[0].upper()
    if len(parts) == 2:
        return parts[0].upper(), parts[1].upper()
    raise RuntimeError(f"Invalid Oracle target table name '{identifier}'.")


def _normalize_target_relation(identifier: str) -> tuple[str | None, str, str]:
    schema_name, table_name = _split_target_table_name(identifier)
    normalized = f"{schema_name}.{table_name}" if schema_name else table_name
    return schema_name, table_name, normalized


def _infer_oracle_auth_mode(req: OracleConnectRequest) -> str:
    explicit = str(getattr(req, "auth_mode", "") or "").strip().lower()
    if explicit:
        if explicit not in _VALID_ORACLE_AUTH_MODES:
            raise RuntimeError(f"Unsupported Oracle auth_mode '{req.auth_mode}'.")
        return explicit
    if str(getattr(req, "wallet_location", "") or "").strip():
        return "wallet_mtls"
    if str(getattr(req, "tns_alias", "") or "").strip():
        return "tns"
    if str(getattr(req, "dsn", "") or "").strip() or str(getattr(req, "url", "") or "").strip():
        return "dsn"
    return "basic"


def _validate_oracle_auth_request(req: OracleConnectRequest, auth_mode: str) -> None:
    if auth_mode not in _VALID_ORACLE_AUTH_MODES:
        raise RuntimeError(f"Unsupported Oracle auth_mode '{auth_mode}'.")
    if auth_mode == "tns" and not str(getattr(req, "tns_alias", "") or "").strip():
        raise RuntimeError("Oracle tns auth requires tns_alias.")
    if auth_mode == "wallet_mtls" and not str(getattr(req, "wallet_location", "") or "").strip():
        raise RuntimeError("Oracle wallet_mtls auth requires wallet_location.")


def _resolved_oracle_connect_config(req: OracleConnectRequest) -> dict[str, Any]:
    _require_driver()
    auth_mode = _infer_oracle_auth_mode(req)
    _validate_oracle_auth_request(req, auth_mode)
    cfg: dict[str, Any] = {
        "connection_id": str(getattr(req, "connection_id", "") or "").strip() or uuid.uuid4().hex,
        "name": str(getattr(req, "name", "") or "").strip() or "oracle",
        "host": str(getattr(req, "host", "localhost") or "localhost").strip(),
        "port": int(getattr(req, "port", 1521) or 1521),
        "service_name": str(getattr(req, "service_name", "") or getattr(req, "database", "") or "FREEPDB1").strip(),
        "sid": str(getattr(req, "sid", "") or "").strip() or None,
        "dsn": str(getattr(req, "dsn", "") or "").strip() or None,
        "tns_alias": str(getattr(req, "tns_alias", "") or "").strip() or None,
        "username": str(getattr(req, "username", "") or "").strip() or None,
        "password": str(getattr(req, "password", "") or ""),
        "auth_mode": auth_mode,
        "config_dir": str(getattr(req, "config_dir", "") or "").strip() or None,
        "wallet_location": str(getattr(req, "wallet_location", "") or "").strip() or None,
        "wallet_password": str(getattr(req, "wallet_password", "") or "").strip() or None,
        "thick_mode": bool(getattr(req, "thick_mode", False) or False),
        "instant_client_dir": str(getattr(req, "instant_client_dir", "") or "").strip() or None,
        "connect_timeout": int(getattr(req, "connect_timeout_seconds", 0) or 0) or None,
    }
    raw_url = str(getattr(req, "url", "") or "").strip()
    if raw_url:
        parsed = urlsplit(raw_url[5:] if raw_url.lower().startswith("jdbc:") else raw_url)
        if parsed.scheme and parsed.hostname:
            cfg["host"] = parsed.hostname
            if parsed.port:
                cfg["port"] = parsed.port
            if parsed.path and parsed.path.strip("/"):
                cfg["service_name"] = unquote(parsed.path.strip("/"))
            if parsed.username:
                cfg["username"] = unquote(parsed.username)
            if parsed.password:
                cfg["password"] = unquote(parsed.password)
            cfg["dsn"] = None
        else:
            cfg["dsn"] = raw_url
    if cfg["tns_alias"]:
        cfg["dsn"] = cfg["tns_alias"]
    elif not cfg["dsn"]:
        if cfg["sid"]:
            cfg["dsn"] = oracledb.makedsn(cfg["host"], int(cfg["port"]), sid=cfg["sid"])
        else:
            cfg["dsn"] = oracledb.makedsn(cfg["host"], int(cfg["port"]), service_name=cfg["service_name"])
    return cfg


def _connect_from_request(req: OracleConnectRequest):
    _require_driver()
    cfg = _resolved_oracle_connect_config(req)
    if cfg["thick_mode"]:
        try:
            oracledb.init_oracle_client(lib_dir=cfg["instant_client_dir"], config_dir=cfg["config_dir"])
        except Exception as exc:
            if "already initialized" not in str(exc).lower():
                raise
    kwargs: dict[str, Any] = {
        "user": cfg["username"],
        "password": cfg["password"],
        "dsn": cfg["dsn"],
    }
    if cfg["config_dir"]:
        kwargs["config_dir"] = cfg["config_dir"]
    if cfg["wallet_location"]:
        kwargs["wallet_location"] = cfg["wallet_location"]
    if cfg["wallet_password"]:
        kwargs["wallet_password"] = cfg["wallet_password"]
    if cfg["connect_timeout"] is not None:
        kwargs["tcp_connect_timeout"] = int(cfg["connect_timeout"])
    conn = oracledb.connect(**kwargs)
    conn.autocommit = False
    return conn


def _oracle_type_name(type_code: Any) -> str:
    name = getattr(type_code, "name", None)
    if name:
        return str(name)
    text = str(type_code or "UNKNOWN")
    return text.replace("<DbType ", "").replace(">", "")


def _normalize_oracle_source_type(
    raw_type: Any,
    *,
    internal_size: int | None = None,
    precision: int | None = None,
    scale: int | None = None,
) -> tuple[str, list[str], bool]:
    raw = _oracle_type_name(raw_type)
    normalized = raw.upper().replace("DB_TYPE_", "")
    warnings: list[str] = []
    if normalized == "NUMBER":
        if precision is not None:
            resolved_precision = int(precision)
            resolved_scale = int(scale or 0)
            if resolved_precision > 38:
                warnings.append(f"Oracle NUMBER({resolved_precision},{resolved_scale}) exceeds DuckDB max precision 38 and was widened to DOUBLE.")
                return "DOUBLE", warnings, True
            return f"DECIMAL({resolved_precision},{resolved_scale})", warnings, False
        warnings.append("Oracle unconstrained NUMBER was widened to DuckDB DOUBLE.")
        return "DOUBLE", warnings, True
    if normalized in {"BINARY_FLOAT", "BINARY_DOUBLE", "FLOAT"}:
        return "DOUBLE", warnings, False
    if normalized in {"CHAR", "NCHAR", "VARCHAR", "VARCHAR2", "NVARCHAR", "NVARCHAR2", "CLOB", "NCLOB", "LONG"}:
        return "VARCHAR", warnings, False
    if normalized == "DATE":
        return "TIMESTAMP", warnings, False
    if normalized in {"TIMESTAMP_TZ", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_LTZ", "TIMESTAMP WITH LOCAL TIME ZONE"}:
        warnings.append(f"Oracle {raw} was normalized to DuckDB TIMESTAMP without timezone.")
        return "TIMESTAMP", warnings, True
    if normalized.startswith("TIMESTAMP"):
        return "TIMESTAMP", warnings, False
    if normalized in {"BLOB", "RAW", "LONG_RAW"}:
        return "BLOB", warnings, False
    warnings.append(f"Oracle type '{raw}' was widened to DuckDB VARCHAR.")
    return "VARCHAR", warnings, True


def _map_description_to_columns(description: Any) -> list[_MappedOracleColumn]:
    mapped: list[_MappedOracleColumn] = []
    for item in list(description or []):
        name = str(item[0] or "").strip() or "column"
        type_code = item[1] if len(item) > 1 else None
        internal_size = item[3] if len(item) > 3 and isinstance(item[3], int) else None
        precision = item[4] if len(item) > 4 and isinstance(item[4], int) else None
        scale = item[5] if len(item) > 5 and isinstance(item[5], int) else None
        target_type, warnings, lossy = _normalize_oracle_source_type(
            type_code,
            internal_size=internal_size,
            precision=precision,
            scale=scale,
        )
        mapped.append(_MappedOracleColumn(name=name, source_type=_oracle_type_name(type_code), target_type=target_type, warnings=warnings, lossy=lossy))
    return mapped


def _build_duckdb_create_table_sql(target_table: str, mapped_columns: list[_MappedOracleColumn], *, replace: bool = False) -> str:
    column_sql = ", ".join(f"{_quote_duckdb_identifier(column.name)} {column.target_type}" for column in mapped_columns)
    prefix = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE"
    return f"{prefix} {_quote_duckdb_compound_identifier(target_table)} ({column_sql})"


def _duckdb_column_meta_from_cursor(cursor: Any) -> list[ColumnMeta]:
    if cursor.description is None:
        return []
    return [ColumnMeta(name=str(desc[0]), data_type=str((desc[1] if len(desc) > 1 else "") or "UNKNOWN"), nullable=True) for desc in cursor.description]


def _is_oracle_string_like_duckdb_column(column: ColumnMeta) -> bool:
    normalized = str(column.data_type or "UNKNOWN").strip().upper()
    return normalized == "VARCHAR" or normalized.startswith("VARCHAR(")


def _oracle_varchar2_type_for_column(column: ColumnMeta) -> tuple[str, list[str]]:
    normalized = str(column.data_type or "UNKNOWN").strip().upper()
    warnings: list[str] = []
    if isinstance(column.max_length, int) and column.max_length >= 0:
        observed_length = int(column.max_length)
        if observed_length > _ORACLE_MAX_VARCHAR2_LENGTH:
            warnings.append(
                f"DuckDB VARCHAR observed length {observed_length} exceeds Oracle VARCHAR2 max "
                f"{_ORACLE_MAX_VARCHAR2_LENGTH}; using CLOB."
            )
            return "CLOB", warnings
        padded = max(observed_length * 2, _ORACLE_MIN_MEASURED_VARCHAR2_LENGTH)
        return f"VARCHAR2({min(padded, _ORACLE_MAX_VARCHAR2_LENGTH)})", warnings
    varchar_match = _VARCHAR_RE.match(normalized)
    if varchar_match:
        declared_length = int(varchar_match.group(1))
        if declared_length > _ORACLE_MAX_VARCHAR2_LENGTH:
            warnings.append(f"DuckDB VARCHAR({declared_length}) exceeds Oracle VARCHAR2 max length 4000 and was widened to CLOB.")
            return "CLOB", warnings
        return f"VARCHAR2({max(declared_length, 1)})", warnings
    warnings.append("DuckDB VARCHAR did not have measured length metadata; using Oracle VARCHAR2(4000).")
    return "VARCHAR2(4000)", warnings


def _measure_oracle_string_column_lengths(
    duck_conn: Any,
    source_sql: str,
    columns: list[ColumnMeta],
    *,
    sql_params: list[Any] | None = None,
) -> list[str]:
    string_columns = [column for column in columns if _is_oracle_string_like_duckdb_column(column)]
    if not string_columns:
        return []
    select_parts = [
        f"MAX(LENGTH(CAST({_quote_duckdb_identifier(column.name)} AS VARCHAR))) AS {_quote_duckdb_identifier(f'__queron_len_{index}')}"
        for index, column in enumerate(string_columns)
    ]
    measurement_sql = f"SELECT {', '.join(select_parts)} FROM ({_strip_sql_terminator(source_sql)}) AS __queron_egress_lengths"
    try:
        row = duck_conn.execute(measurement_sql, list(sql_params or [])).fetchone()
    except Exception as exc:
        return [f"Oracle egress VARCHAR2 length measurement failed; using VARCHAR2(4000). {exc}"]
    if row is None:
        return []
    for index, column in enumerate(string_columns):
        value = row[index] if index < len(row) else None
        column.max_length = int(value or 0) if value is not None else 0
    return []


def _map_duckdb_column_to_oracle(column: ColumnMeta) -> tuple[str, list[str]]:
    normalized = str(column.data_type or "UNKNOWN").strip().upper()
    warnings: list[str] = []
    if normalized in {"BOOLEAN", "BOOL"}:
        return "NUMBER(1)", warnings
    if normalized == "SMALLINT":
        return "NUMBER(5)", warnings
    if normalized == "INTEGER":
        return "NUMBER(10)", warnings
    if normalized == "BIGINT":
        return "NUMBER(19)", warnings
    if normalized in {"REAL", "FLOAT", "DOUBLE"}:
        return "BINARY_DOUBLE", warnings
    if normalized == "DATE":
        return "DATE", warnings
    if normalized in {"TIMESTAMP", "TIMESTAMPTZ"}:
        if normalized == "TIMESTAMPTZ":
            warnings.append("DuckDB TIMESTAMPTZ was normalized to Oracle TIMESTAMP.")
        return "TIMESTAMP", warnings
    decimal_match = _DECIMAL_RE.match(normalized)
    if decimal_match:
        precision = int(decimal_match.group(1))
        scale = int(decimal_match.group(2))
        if precision > 38:
            warnings.append(f"DuckDB DECIMAL({precision},{scale}) exceeds Oracle NUMBER max precision 38 and was widened to BINARY_DOUBLE.")
            return "BINARY_DOUBLE", warnings
        return f"NUMBER({precision},{scale})", warnings
    if normalized == "DECIMAL":
        precision = int(column.precision or 38)
        scale = int(column.scale or 10)
        if precision > 38:
            warnings.append(f"DuckDB DECIMAL({precision},{scale}) exceeds Oracle NUMBER max precision 38 and was widened to BINARY_DOUBLE.")
            return "BINARY_DOUBLE", warnings
        return f"NUMBER({precision},{scale})", warnings
    if normalized in {"BLOB", "BYTEA"}:
        return "BLOB", warnings
    if normalized == "VARCHAR" or _VARCHAR_RE.match(normalized):
        target_type, length_warnings = _oracle_varchar2_type_for_column(column)
        warnings.extend(length_warnings)
        return target_type, warnings
    if normalized == "UUID":
        return "VARCHAR2(36)", warnings
    if normalized == "JSON":
        return "CLOB", warnings
    warnings.append(f"DuckDB type '{normalized}' was widened to Oracle CLOB.")
    return "CLOB", warnings


def _build_oracle_create_table_sql(target_table: str, columns: list[ColumnMeta]) -> tuple[str, list[str]]:
    warnings: list[str] = []
    column_defs: list[str] = []
    for column in columns:
        target_type, column_warnings = _map_duckdb_column_to_oracle(column)
        warnings.extend(column_warnings)
        null_part = "" if column.nullable else " NOT NULL"
        column_defs.append(f"{_quote_identifier(column.name)} {target_type}{null_part}")
    return f"CREATE TABLE {_quote_compound_identifier(target_table)} ({', '.join(column_defs)})", warnings


def _oracle_table_exists(cur: Any, *, schema_name: str | None, table_name: str) -> bool:
    if schema_name:
        cur.execute(
            "SELECT 1 FROM ALL_TABLES WHERE OWNER = :1 AND TABLE_NAME = :2",
            [schema_name.upper(), table_name.upper()],
        )
    else:
        cur.execute("SELECT 1 FROM USER_TABLES WHERE TABLE_NAME = :1", [table_name.upper()])
    return cur.fetchone() is not None


def _inspect_oracle_table_columns(cur: Any, *, schema_name: str | None, table_name: str) -> list[ColumnMeta]:
    if schema_name:
        cur.execute(
            """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :1 AND TABLE_NAME = :2
            ORDER BY COLUMN_ID
            """,
            [schema_name.upper(), table_name.upper()],
        )
    else:
        cur.execute(
            """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE
            FROM USER_TAB_COLUMNS
            WHERE TABLE_NAME = :1
            ORDER BY COLUMN_ID
            """,
            [table_name.upper()],
        )
    columns: list[ColumnMeta] = []
    for row in list(cur.fetchall() or []):
        data_type = str(row[1] or "UNKNOWN").strip()
        precision = row[3]
        scale = row[4]
        if data_type.upper() == "NUMBER" and precision is not None:
            if scale is None:
                data_type = f"NUMBER({int(precision)})"
            else:
                data_type = f"NUMBER({int(precision)},{int(scale)})"
        elif data_type.upper() in {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"} and row[2] is not None:
            data_type = f"{data_type}({int(row[2])})"
        columns.append(
            ColumnMeta(
                name=str(row[0] or "").strip(),
                data_type=data_type,
                max_length=int(row[2]) if row[2] is not None else None,
                precision=int(precision) if precision is not None else None,
                scale=int(scale) if scale is not None else None,
                nullable=str(row[5] or "").upper() != "N",
            )
        )
    return columns


def _build_egress_inferred_column_mappings(column_meta: list[ColumnMeta]) -> list[ColumnMappingRecord]:
    mappings: list[ColumnMappingRecord] = []
    for index, column in enumerate(column_meta, start=1):
        target_type, column_warnings = _map_duckdb_column_to_oracle(column)
        mappings.append(
            ColumnMappingRecord(
                ordinal_position=index,
                source_column=str(column.name),
                source_type=str(column.data_type or "UNKNOWN"),
                target_column=str(column.name),
                target_type=str(target_type or "UNKNOWN"),
                connector_type="oracle",
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
        warnings = list(fallback.warnings or []) if remote_column is None or remote_target_type.upper() == fallback_target_type.upper() else []
        if remote_column is None:
            warnings.append("Remote target column was not found during Oracle schema inspection.")
        mappings.append(
            ColumnMappingRecord(
                ordinal_position=index,
                source_column=str(source_column.name),
                source_type=str(source_column.data_type or "UNKNOWN"),
                target_column=str(remote_column.name if remote_column is not None else source_column.name),
                target_type=remote_target_type,
                connector_type="oracle",
                mapping_mode="egress_remote_schema",
                warnings=warnings,
                lossy=fallback.lossy if fallback is not None else None,
            )
        )
    return mappings


def _coerce_value(value: Any) -> Any:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, memoryview):
        return bytes(value)
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, default=str)
    return value


def _build_column_mappings(mapped_columns: list[_MappedOracleColumn]) -> list[ColumnMappingRecord]:
    return [
        ColumnMappingRecord(
            source_column=column.name,
            source_type=column.source_type,
            target_column=column.name,
            target_type=column.target_type,
            connector_type="oracle",
            mapping_mode="ingress",
            warnings=list(column.warnings or []),
            lossy=bool(column.lossy),
            ordinal_position=index,
        )
        for index, column in enumerate(mapped_columns, start=1)
    ]


def _collect_mapping_warnings(mapped_columns: list[_MappedOracleColumn]) -> list[str]:
    warnings: list[str] = []
    seen: set[str] = set()
    for column in mapped_columns:
        for warning in column.warnings or []:
            if warning and warning not in seen:
                seen.add(warning)
                warnings.append(warning)
    return warnings


def _build_oracle_interruptor(conn: Any) -> Any:
    def _interrupt() -> None:
        try:
            conn.cancel()
        except Exception:
            pass
    return _interrupt


def _build_duckdb_interruptor(conn: Any) -> Any:
    def _interrupt() -> None:
        try:
            conn.interrupt()
        except Exception:
            pass
    return _interrupt


def test_connection(req: OracleConnectRequest) -> TestConnectionResponse:
    conn = None
    try:
        conn = _connect_from_request(req)
        cur = conn.cursor()
        cur.execute("SELECT SYS_CONTEXT('USERENV', 'SERVICE_NAME') FROM DUAL")
        service_name = cur.fetchone()[0]
        return TestConnectionResponse(success=True, message=f"Connected to Oracle service '{service_name}'.")
    except Exception as exc:
        return TestConnectionResponse(success=False, message=str(exc))
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def connect(req: OracleConnectRequest) -> ConnectResponse:
    cfg = _resolved_oracle_connect_config(req)
    conn = _connect_from_request(req)
    _connections[cfg["connection_id"]] = {"connection": conn, "config": cfg, "opened_at": time.time()}
    return ConnectResponse(connection_id=str(cfg["connection_id"]), message=f"Connected to Oracle DSN '{cfg['dsn']}'.")


def disconnect(connection_id: str) -> None:
    entry = _connections.pop(str(connection_id or "").strip(), None)
    if not entry:
        return
    conn = entry.get("connection")
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


def run_query(connection_id: str, sql: str) -> QueryResponse:
    entry = _connections.get(str(connection_id or "").strip())
    if not entry:
        raise RuntimeError(f"Oracle connection '{connection_id}' is not open.")
    conn = entry.get("connection")
    cur = conn.cursor()
    cur.execute(str(sql or ""))
    if cur.description is None:
        conn.commit()
        return QueryResponse(columns=[], column_meta=[], rows=[], row_count=max(int(cur.rowcount or 0), 0), message="Statement executed successfully.")
    columns = [str(column[0]) for column in list(cur.description)]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    return QueryResponse(columns=columns, column_meta=[ColumnMeta(name=name, data_type="") for name in columns], rows=rows, row_count=len(rows))


def ingest_query_to_duckdb(
    req: OracleConnectRequest,
    *,
    sql: str,
    sql_params: list[Any] | None = None,
    duckdb_path: str,
    target_table: str,
    chunk_size: int | None = None,
    replace: bool = True,
    on_progress: Any = None,
    on_interrupt_open: Any = None,
    on_interrupt_close: Any = None,
) -> DuckDbIngestQueryResponse:
    source_conn = None
    duck_conn = None
    source_interrupt_token = None
    duck_interrupt_token = None
    inserted = 0
    next_progress_threshold = 1000
    try:
        source_conn = _connect_from_request(req)
        if callable(on_interrupt_open):
            source_interrupt_token = on_interrupt_open(_build_oracle_interruptor(source_conn))
        source_cur = source_conn.cursor()
        source_cur.execute(_strip_sql_terminator(sql), list(sql_params or []))
        if source_cur.description is None:
            raise RuntimeError("Source query did not return a result set.")
        mapped_columns = _map_description_to_columns(source_cur.description)
        if not mapped_columns:
            raise RuntimeError("No columns found in source result.")
        duck_conn = connect_duckdb(str(duckdb_path))
        if callable(on_interrupt_open):
            duck_interrupt_token = on_interrupt_open(_build_duckdb_interruptor(duck_conn))
        duck_conn.execute(_build_duckdb_create_table_sql(target_table, mapped_columns, replace=bool(replace)))
        column_names = [column.name for column in mapped_columns]
        col_sql = ", ".join(_quote_duckdb_identifier(name) for name in column_names)
        placeholders = ", ".join(["?"] * len(column_names))
        insert_sql = f"INSERT INTO {_quote_duckdb_compound_identifier(target_table)} ({col_sql}) VALUES ({placeholders})"
        fetch_size = _sanitize_chunk_size(chunk_size)
        while True:
            rows = source_cur.fetchmany(fetch_size)
            if not rows:
                break
            payload = [tuple(_coerce_value(value) for value in row) for row in rows]
            if payload:
                duck_conn.executemany(insert_sql, payload)
                inserted += len(payload)
            if callable(on_progress) and inserted >= next_progress_threshold:
                on_progress({"row_count": inserted, "chunk_size": len(payload)})
                while inserted >= next_progress_threshold:
                    next_progress_threshold += 1000
        return DuckDbIngestQueryResponse(
            pipeline_id="oracle",
            target_table=target_table,
            row_count=inserted,
            source_type="oracle",
            source_connection_id=str(getattr(req, "connection_id", "") or ""),
            warnings=_collect_mapping_warnings(mapped_columns),
            column_mappings=_build_column_mappings(mapped_columns),
            created_at=time.time(),
            schema_changed=bool(replace),
        )
    finally:
        try:
            if source_conn is not None:
                source_conn.close()
        except Exception:
            pass
        if callable(on_interrupt_close):
            on_interrupt_close(source_interrupt_token)
        try:
            if duck_conn is not None:
                duck_conn.close()
        except Exception:
            pass
        if callable(on_interrupt_close):
            on_interrupt_close(duck_interrupt_token)


def egress_query_from_duckdb(
    *,
    target_request: OracleConnectRequest,
    duckdb_database: str,
    sql: str,
    sql_params: list[Any] | None = None,
    target_table: str,
    mode: str = "replace",
    chunk_size: int = 1000,
    artifact_table: str | None = None,
    on_interrupt_open: Any = None,
    on_interrupt_close: Any = None,
) -> ConnectorEgressResponse:
    normalized_mode = str(mode or "replace").strip().lower()
    if normalized_mode not in {"replace", "append", "create", "create_append"}:
        raise RuntimeError(f"Unsupported Oracle egress mode '{mode}'. Use replace, append, create, or create_append.")
    duck_conn = None
    duck_cur = None
    target_conn = None
    target_cur = None
    duck_interrupt_token = None
    target_interrupt_token = None
    warnings: list[str] = []
    column_mappings: list[ColumnMappingRecord] = []
    row_count = 0
    schema_name, table_name, normalized_target_table = _normalize_target_relation(target_table)
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
        if callable(on_interrupt_open):
            duck_interrupt_token = on_interrupt_open(_build_duckdb_interruptor(duck_conn))
        duck_cur = duck_conn.cursor()
        duck_cur.execute(_strip_sql_terminator(sql), list(sql_params or []))
        if duck_cur.description is None:
            raise RuntimeError("DuckDB egress query did not return a result set.")
        column_meta = _duckdb_column_meta_from_cursor(duck_cur)
        if not column_meta:
            raise RuntimeError("DuckDB egress query did not return any columns.")
        warnings.extend(
            _measure_oracle_string_column_lengths(
                duck_conn,
                sql,
                column_meta,
                sql_params=sql_params,
            )
        )
        column_mappings = _build_egress_inferred_column_mappings(column_meta)
        target_conn = _connect_from_request(target_request)
        if callable(on_interrupt_open):
            target_interrupt_token = on_interrupt_open(_build_oracle_interruptor(target_conn))
        target_cur = target_conn.cursor()
        table_exists = _oracle_table_exists(target_cur, schema_name=schema_name, table_name=table_name)
        if normalized_mode == "replace":
            if table_exists:
                target_cur.execute(f"DROP TABLE {quoted_target_table}")
            create_sql, create_warnings = _build_oracle_create_table_sql(normalized_target_table, column_meta)
            warnings.extend(create_warnings)
            target_cur.execute(create_sql)
        elif normalized_mode == "create":
            if table_exists:
                raise RuntimeError(f"Target table '{quoted_target_table}' already exists.")
            create_sql, create_warnings = _build_oracle_create_table_sql(normalized_target_table, column_meta)
            warnings.extend(create_warnings)
            target_cur.execute(create_sql)
        elif normalized_mode == "create_append":
            if not table_exists:
                create_sql, create_warnings = _build_oracle_create_table_sql(normalized_target_table, column_meta)
                warnings.extend(create_warnings)
                target_cur.execute(create_sql)
        elif not table_exists:
            raise RuntimeError(f"Target table '{quoted_target_table}' does not exist for append mode.")
        column_names = [column.name for column in column_meta]
        col_sql = ", ".join(_quote_identifier(name) for name in column_names)
        placeholders = ", ".join(f":{index}" for index in range(1, len(column_names) + 1))
        insert_sql = f"INSERT INTO {quoted_target_table} ({col_sql}) VALUES ({placeholders})"
        fetch_size = _sanitize_chunk_size(chunk_size)
        while True:
            rows = duck_cur.fetchmany(fetch_size)
            if not rows:
                break
            payload = [tuple(_coerce_value(value) for value in row) for row in rows]
            if payload:
                target_cur.executemany(insert_sql, payload)
                row_count += len(payload)
        target_conn.commit()
        try:
            remote_columns = _inspect_oracle_table_columns(target_cur, schema_name=schema_name, table_name=table_name)
            if not remote_columns:
                raise RuntimeError("No columns returned for remote target table.")
            column_mappings = _build_egress_remote_schema_column_mappings(
                column_meta,
                remote_columns,
                fallback_mappings=column_mappings,
            )
        except Exception as exc:
            warnings.append(f"Oracle remote schema inspection failed; using inferred egress column mappings. {exc}")
    finally:
        try:
            if duck_cur is not None:
                duck_cur.close()
        except Exception:
            pass
        try:
            if duck_conn is not None:
                if callable(on_interrupt_close):
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
                if callable(on_interrupt_close):
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


def drop_table_if_exists(*, target_request: OracleConnectRequest, target_table: str) -> None:
    conn = None
    cur = None
    schema_name, table_name, normalized_target_table = _normalize_target_relation(target_table)
    quoted_target_table = _quote_compound_identifier(normalized_target_table)
    try:
        conn = _connect_from_request(target_request)
        cur = conn.cursor()
        if _oracle_table_exists(cur, schema_name=schema_name, table_name=table_name):
            cur.execute(f"DROP TABLE {quoted_target_table}")
        conn.commit()
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
