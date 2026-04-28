"""
MSSQL Core
==========
This module owns MSSQL-specific runtime behavior for the app and the OSS
pipeline layer.

Phase 2 scope:
- auth/config resolution
- connection management
- test_connection
- connect
- simple query helper for smoke testing

Ingress/egress and DuckDB loading are intentionally out of scope here.
"""

from __future__ import annotations

import re
import time
import uuid
from typing import Any

try:
    import pyodbc
except ImportError:
    pyodbc = None

try:
    from . import base as _base_models
    from .duckdb_driver import connect_duckdb, load_duckdb
    from .queron.runtime_models import ColumnMappingRecord
except ImportError:
    import base as _base_models
    from duckdb_driver import connect_duckdb, load_duckdb
    from queron.runtime_models import ColumnMappingRecord

ColumnMeta = _base_models.ColumnMeta
ConnectResponse = _base_models.ConnectResponse
DuckDbIngestQueryResponse = _base_models.DuckDbIngestQueryResponse
MssqlConnectRequest = _base_models.MssqlConnectRequest
QueryResponse = _base_models.QueryResponse
TestConnectionResponse = _base_models.TestConnectionResponse
ConnectorEgressResponse = getattr(_base_models, "ConnectorEgressResponse", None)


_connections: dict[str, dict[str, Any]] = {}
_VALID_MSSQL_AUTH_MODES = {"basic", "tls", "windows_sso"}
_DEFAULT_MSSQL_DRIVER = "ODBC Driver 18 for SQL Server"
_DEFAULT_QUERY_CHUNK_SIZE = int(getattr(_base_models, "DEFAULT_QUERY_CHUNK_SIZE", 200) or 200)
_DUCKDB_VARCHAR_RE = re.compile(r"^VARCHAR\((\d+)\)$", re.IGNORECASE)
_MSSQL_MAX_INLINE_NVARCHAR_LENGTH = 4000
_MSSQL_MIN_MEASURED_NVARCHAR_LENGTH = 32

if ConnectorEgressResponse is None:
    from pydantic import BaseModel, Field
    class _FallbackConnectorEgressResponse(BaseModel):
        target_name: str
        row_count: int
        warnings: list[str] = Field(default_factory=list)
        column_mappings: list[ColumnMappingRecord] = Field(default_factory=list)
        created_at: float
        schema_changed: bool = False
    ConnectorEgressResponse = _FallbackConnectorEgressResponse


class _MappedMssqlColumn:
    def __init__(self, *, name: str, source_type: str, target_type: str, warnings: list[str], lossy: bool = False) -> None:
        self.name = name
        self.source_type = source_type
        self.target_type = target_type
        self.warnings = warnings
        self.lossy = lossy


def _require_driver() -> None:
    if pyodbc is None:
        raise RuntimeError("pyodbc is not installed. Install the 'pyodbc' package first.")


def _truthy_flag(value: Any, *, default: bool) -> str:
    if value is None:
        return "yes" if default else "no"
    return "yes" if bool(value) else "no"


def _normalize_driver_name(value: Any) -> str:
    text = str(value or "").strip()
    return text or _DEFAULT_MSSQL_DRIVER


def _infer_mssql_auth_mode(req: MssqlConnectRequest) -> str:
    explicit = str(getattr(req, "auth_mode", "") or "").strip().lower()
    if explicit:
        if explicit not in _VALID_MSSQL_AUTH_MODES:
            raise RuntimeError(f"Unsupported MSSQL auth_mode '{req.auth_mode}'.")
        return explicit
    if getattr(req, "encrypt", None) is not None or getattr(req, "trust_server_certificate", None) is not None:
        return "tls"
    return "basic"


def _validate_mssql_auth_request(req: MssqlConnectRequest, auth_mode: str) -> None:
    if auth_mode not in _VALID_MSSQL_AUTH_MODES:
        raise RuntimeError(f"Unsupported MSSQL auth_mode '{auth_mode}'.")
    if auth_mode in {"basic", "tls"}:
        username = str(getattr(req, "username", "") or "").strip()
        password = str(getattr(req, "password", "") or "")
        if not username:
            raise RuntimeError(f"MSSQL {auth_mode} auth requires username.")
        if password == "":
            raise RuntimeError(f"MSSQL {auth_mode} auth requires password.")


def _build_mssql_auth_keywords(req: MssqlConnectRequest, auth_mode: str) -> dict[str, Any]:
    keywords: dict[str, Any] = {}
    if auth_mode == "windows_sso":
        keywords["Trusted_Connection"] = "yes"
    else:
        keywords["UID"] = str(getattr(req, "username", "") or "").strip()
        keywords["PWD"] = str(getattr(req, "password", "") or "")
    if auth_mode == "tls":
        keywords["Encrypt"] = _truthy_flag(getattr(req, "encrypt", None), default=True)
        keywords["TrustServerCertificate"] = _truthy_flag(getattr(req, "trust_server_certificate", None), default=False)
    elif getattr(req, "encrypt", None) is not None:
        keywords["Encrypt"] = _truthy_flag(getattr(req, "encrypt", None), default=False)
    if getattr(req, "trust_server_certificate", None) is not None:
        keywords["TrustServerCertificate"] = _truthy_flag(getattr(req, "trust_server_certificate", None), default=False)
    return keywords


def _resolved_mssql_connect_config(req: MssqlConnectRequest) -> dict[str, Any]:
    auth_mode = _infer_mssql_auth_mode(req)
    _validate_mssql_auth_request(req, auth_mode)
    return {
        "connection_id": str(getattr(req, "connection_id", "") or "").strip() or uuid.uuid4().hex,
        "name": str(getattr(req, "name", "") or "").strip() or "mssql",
        "host": str(getattr(req, "host", "localhost") or "localhost").strip(),
        "port": int(getattr(req, "port", 1433) or 1433),
        "database": str(getattr(req, "database", "master") or "master").strip(),
        "url": str(getattr(req, "url", "") or "").strip() or None,
        "driver": _normalize_driver_name(getattr(req, "driver", None)),
        "auth_mode": auth_mode,
        "timeout_seconds": int(getattr(req, "timeout_seconds", 0) or 0) or None,
        "keywords": _build_mssql_auth_keywords(req, auth_mode),
    }


def _connection_string_from_request(req: MssqlConnectRequest) -> str:
    cfg = _resolved_mssql_connect_config(req)
    if cfg["url"]:
        return str(cfg["url"])
    parts = [
        f"DRIVER={{{cfg['driver']}}}",
        f"SERVER={cfg['host']},{cfg['port']}",
        f"DATABASE={cfg['database']}",
    ]
    if cfg["timeout_seconds"] is not None:
        parts.append(f"Connection Timeout={int(cfg['timeout_seconds'])}")
    for key, value in dict(cfg["keywords"]).items():
        text = str(value or "").strip()
        if not text:
            continue
        parts.append(f"{key}={text}")
    return ";".join(parts) + ";"


def _row_to_dict(columns: list[str], row: Any) -> dict[str, Any]:
    values = list(row)
    return {columns[index]: values[index] for index in range(len(columns))}


def _sanitize_chunk_size(chunk_size: int | None) -> int:
    size = int(chunk_size or _DEFAULT_QUERY_CHUNK_SIZE)
    if size < 1:
        return _DEFAULT_QUERY_CHUNK_SIZE
    return min(size, 5000)


def _build_mssql_interruptor(conn: Any) -> Any:
    def _interrupt() -> None:
        try:
            conn.close()
        except Exception:
            pass
    return _interrupt


def _build_duckdb_interruptor(conn: Any) -> Any:
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


def _normalize_mssql_source_type(raw_type: str) -> tuple[str, list[str], bool]:
    normalized = str(raw_type or "UNKNOWN").strip().lower()
    warnings: list[str] = []
    if "int" in normalized and "big" not in normalized and "small" not in normalized and "tiny" not in normalized:
        return "INTEGER", warnings, False
    if "bigint" in normalized:
        return "BIGINT", warnings, False
    if "smallint" in normalized:
        return "SMALLINT", warnings, False
    if "tinyint" in normalized:
        return "SMALLINT", warnings, False
    if "bit" in normalized or normalized == "<class 'bool'>":
        return "BOOLEAN", warnings, False
    if "decimal" in normalized or "numeric" in normalized:
        return "DECIMAL(38,10)", warnings, False
    if "float" in normalized or "double" in normalized:
        return "DOUBLE", warnings, False
    if "real" in normalized:
        return "REAL", warnings, False
    if "date" == normalized or normalized.endswith(".date'>"):
        return "DATE", warnings, False
    if "time" in normalized and "datetime" not in normalized:
        return "TIME", warnings, False
    if "datetime" in normalized or "timestamp" in normalized:
        return "TIMESTAMP", warnings, False
    if "binary" in normalized or "bytes" in normalized:
        return "BLOB", warnings, False
    if "str" in normalized or "char" in normalized or "text" in normalized or "nchar" in normalized or "nvarchar" in normalized or "varchar" in normalized:
        return "VARCHAR", warnings, False
    warnings.append(f"MSSQL {raw_type} was widened to DuckDB VARCHAR.")
    return "VARCHAR", warnings, True


def _map_description_to_columns(description: Any) -> list[_MappedMssqlColumn]:
    mapped: list[_MappedMssqlColumn] = []
    for item in list(description or []):
        name = str(item[0] or "").strip() or "column"
        type_code = item[1] if len(item) > 1 else None
        raw_type = str(type_code or "UNKNOWN")
        target_type, warnings, lossy = _normalize_mssql_source_type(raw_type)
        mapped.append(
            _MappedMssqlColumn(
                name=name,
                source_type=raw_type,
                target_type=target_type,
                warnings=warnings,
                lossy=lossy,
            )
        )
    return mapped


def _build_column_meta(mapped_columns: list[_MappedMssqlColumn]) -> list[ColumnMeta]:
    return [
        ColumnMeta(
            name=column.name,
            data_type=column.target_type,
            nullable=True,
        )
        for column in mapped_columns
    ]


def _build_column_mappings(mapped_columns: list[_MappedMssqlColumn]) -> list[ColumnMappingRecord]:
    mappings: list[ColumnMappingRecord] = []
    for ordinal, column in enumerate(mapped_columns, start=1):
        mappings.append(
            ColumnMappingRecord(
                source_column=column.name,
                source_type=column.source_type,
                target_column=column.name,
                target_type=column.target_type,
                connector_type="mssql",
                mapping_mode="ingress",
                warnings_json=list(column.warnings or []),
                lossy=bool(column.lossy),
                ordinal_position=ordinal,
            )
        )
    return mappings


def _collect_mapping_warnings(mapped_columns: list[_MappedMssqlColumn]) -> list[str]:
    warnings: list[str] = []
    seen: set[str] = set()
    for column in mapped_columns:
        for warning in column.warnings or []:
            if warning and warning not in seen:
                seen.add(warning)
                warnings.append(warning)
    return warnings


def _quote_identifier(identifier: str) -> str:
    return "[" + str(identifier).replace("]", "]]") + "]"


def _quote_compound_identifier(identifier: str) -> str:
    parts = [part.strip() for part in str(identifier).split(".") if part.strip()]
    if not parts:
        raise RuntimeError("Target table name is required.")
    return ".".join(_quote_identifier(part) for part in parts)


def _quote_duckdb_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _quote_duckdb_compound_identifier(identifier: str) -> str:
    parts = [part.strip() for part in str(identifier).split(".") if part.strip()]
    if not parts:
        raise RuntimeError("Target table name is required.")
    return ".".join(_quote_duckdb_identifier(part) for part in parts)


def _split_target_table_name(identifier: str) -> tuple[str, str]:
    parts = [part.strip().strip("[]\"") for part in str(identifier).split(".") if part.strip()]
    if len(parts) == 1:
        return "dbo", parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise RuntimeError(f"Invalid target table name '{identifier}'.")


def _normalize_target_relation(identifier: str) -> tuple[str, str, str]:
    schema_name, table_name = _split_target_table_name(identifier)
    return schema_name, table_name, f"{schema_name}.{table_name}"


def _build_duckdb_create_table_sql(target_table: str, mapped_columns: list[_MappedMssqlColumn], *, replace: bool = False) -> str:
    column_sql = ", ".join(f"{_quote_duckdb_identifier(column.name)} {column.target_type}" for column in mapped_columns)
    prefix = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE"
    return f"{prefix} {_quote_duckdb_compound_identifier(target_table)} ({column_sql})"


def _coerce_source_value(value: Any, column: _MappedMssqlColumn) -> Any:
    if value is None:
        return None
    if column.target_type == "VARCHAR":
        return str(value)
    return value


def _duckdb_column_meta_from_cursor(cursor: Any) -> list[ColumnMeta]:
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


def _is_mssql_string_like_duckdb_column(column: ColumnMeta) -> bool:
    normalized = str(column.data_type or "UNKNOWN").strip().upper()
    return normalized == "VARCHAR" or normalized.startswith("VARCHAR(")


def _mssql_nvarchar_type_for_column(column: ColumnMeta) -> tuple[str, list[str]]:
    normalized = str(column.data_type or "UNKNOWN").strip().upper()
    warnings: list[str] = []
    if isinstance(column.max_length, int) and column.max_length >= 0:
        observed_length = int(column.max_length)
        if observed_length > _MSSQL_MAX_INLINE_NVARCHAR_LENGTH:
            warnings.append(
                f"DuckDB VARCHAR observed length {observed_length} exceeds MSSQL NVARCHAR inline max "
                f"{_MSSQL_MAX_INLINE_NVARCHAR_LENGTH}; using NVARCHAR(MAX)."
            )
            return "NVARCHAR(MAX)", warnings
        padded = max(observed_length * 2, _MSSQL_MIN_MEASURED_NVARCHAR_LENGTH)
        return f"NVARCHAR({min(padded, _MSSQL_MAX_INLINE_NVARCHAR_LENGTH)})", warnings
    varchar_match = _DUCKDB_VARCHAR_RE.match(normalized)
    if varchar_match:
        declared_length = int(varchar_match.group(1))
        if declared_length > _MSSQL_MAX_INLINE_NVARCHAR_LENGTH:
            return "NVARCHAR(MAX)", warnings
        return f"NVARCHAR({max(declared_length, 1)})", warnings
    warnings.append("DuckDB VARCHAR did not have measured length metadata; using MSSQL NVARCHAR(MAX).")
    return "NVARCHAR(MAX)", warnings


def _measure_mssql_string_column_lengths(
    duck_conn: Any,
    source_sql: str,
    columns: list[ColumnMeta],
    *,
    sql_params: list[Any] | None = None,
) -> list[str]:
    string_columns = [column for column in columns if _is_mssql_string_like_duckdb_column(column)]
    if not string_columns:
        return []
    select_parts = [
        f"MAX(LENGTH(CAST({_quote_duckdb_identifier(column.name)} AS VARCHAR))) AS {_quote_duckdb_identifier(f'__queron_len_{index}')}"
        for index, column in enumerate(string_columns)
    ]
    measurement_sql = f"SELECT {', '.join(select_parts)} FROM ({str(source_sql or '').strip().rstrip(';')}) AS __queron_egress_lengths"
    try:
        row = duck_conn.execute(measurement_sql, list(sql_params or [])).fetchone()
    except Exception as exc:
        return [f"MSSQL egress NVARCHAR length measurement failed; using NVARCHAR(MAX). {exc}"]
    if row is None:
        return []
    for index, column in enumerate(string_columns):
        value = row[index] if index < len(row) else None
        column.max_length = int(value or 0) if value is not None else 0
    return []


def _map_duckdb_column_to_mssql(column: ColumnMeta) -> tuple[str, list[str]]:
    normalized = str(column.data_type or "UNKNOWN").strip().upper()
    warnings: list[str] = []
    if normalized in {"BOOLEAN", "BOOL"}:
        return "BIT", warnings
    if normalized == "SMALLINT":
        return "SMALLINT", warnings
    if normalized == "INTEGER":
        return "INT", warnings
    if normalized == "BIGINT":
        return "BIGINT", warnings
    if normalized == "REAL":
        return "REAL", warnings
    if normalized == "DOUBLE":
        return "FLOAT", warnings
    if normalized == "DATE":
        return "DATE", warnings
    if normalized == "TIME":
        return "TIME", warnings
    if normalized in {"TIMESTAMP", "TIMESTAMPTZ"}:
        if normalized == "TIMESTAMPTZ":
            warnings.append("DuckDB TIMESTAMPTZ was normalized to MSSQL DATETIME2.")
        return "DATETIME2", warnings
    if normalized in {"VARCHAR", "JSON"} or normalized.startswith("VARCHAR"):
        target_type, length_warnings = _mssql_nvarchar_type_for_column(column)
        warnings.extend(length_warnings)
        return target_type, warnings
    if normalized in {"UUID"}:
        return "UNIQUEIDENTIFIER", warnings
    if normalized in {"BLOB", "BYTEA"}:
        return "VARBINARY(MAX)", warnings
    if normalized.startswith("DECIMAL("):
        return normalized, warnings
    if normalized == "DECIMAL":
        precision = int(column.precision or 38)
        scale = int(column.scale or 10)
        if precision > 38:
            warnings.append(f"DuckDB DECIMAL({precision},{scale}) exceeds MSSQL max precision 38 and was widened to FLOAT.")
            return "FLOAT", warnings
        return f"DECIMAL({precision},{scale})", warnings
    warnings.append(f"DuckDB type '{normalized}' was widened to MSSQL NVARCHAR(MAX).")
    return "NVARCHAR(MAX)", warnings


def _build_mssql_create_table_sql(target_table: str, columns: list[ColumnMeta]) -> tuple[str, list[str]]:
    warnings: list[str] = []
    column_defs: list[str] = []
    for column in columns:
        target_type, column_warnings = _map_duckdb_column_to_mssql(column)
        warnings.extend(column_warnings)
        null_part = "" if column.nullable else " NOT NULL"
        column_defs.append(f"{_quote_identifier(column.name)} {target_type}{null_part}")
    return f"CREATE TABLE {_quote_compound_identifier(target_table)} ({', '.join(column_defs)})", warnings


def _mssql_table_exists(cur: Any, *, schema_name: str, table_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """,
        (schema_name, table_name),
    )
    return cur.fetchone() is not None


def _format_mssql_remote_type(data_type: Any, *, max_length: Any = None, precision: Any = None, scale: Any = None) -> str:
    text = str(data_type or "UNKNOWN").strip() or "UNKNOWN"
    upper = text.upper()
    if upper in {"DECIMAL", "NUMERIC"} and precision:
        return f"{upper}({precision},{int(scale or 0)})"
    if upper in {"CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "BINARY", "VARBINARY"} and max_length:
        length = int(max_length)
        return f"{upper}({'MAX' if length < 0 else length})"
    return upper


def _inspect_mssql_table_columns(cur: Any, *, schema_name: str, table_name: str) -> list[ColumnMeta]:
    cur.execute(
        """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        (schema_name, table_name),
    )
    return [
        ColumnMeta(
            name=str(row[0] or "").strip(),
            data_type=_format_mssql_remote_type(row[1], max_length=row[2], precision=row[3], scale=row[4]),
            nullable=str(row[5] or "").upper() != "NO",
        )
        for row in list(cur.fetchall() or [])
        if str(row[0] or "").strip()
    ]


def _build_egress_inferred_column_mappings(column_meta: list[ColumnMeta]) -> list[ColumnMappingRecord]:
    mappings: list[ColumnMappingRecord] = []
    for index, column in enumerate(column_meta, start=1):
        target_type, column_warnings = _map_duckdb_column_to_mssql(column)
        mappings.append(
            ColumnMappingRecord(
                ordinal_position=index,
                source_column=str(column.name),
                source_type=str(column.data_type or "UNKNOWN"),
                target_column=str(column.name),
                target_type=str(target_type or "UNKNOWN"),
                connector_type="mssql",
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
            warnings.append("Remote target column was not found during MSSQL schema inspection.")
        mappings.append(
            ColumnMappingRecord(
                ordinal_position=index,
                source_column=str(source_column.name),
                source_type=str(source_column.data_type or "UNKNOWN"),
                target_column=str(remote_column.name if remote_column is not None else source_column.name),
                target_type=remote_target_type,
                connector_type="mssql",
                mapping_mode="egress_remote_schema",
                warnings=warnings,
                lossy=fallback.lossy if fallback is not None else None,
            )
        )
    return mappings


def _ensure_mssql_schema(cur: Any, schema_name: str) -> None:
    schema = str(schema_name or "").strip()
    if not schema or schema.lower() == "dbo":
        return
    cur.execute(
        """
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = ?)
        EXEC('CREATE SCHEMA ' + QUOTENAME(?))
        """,
        (schema, schema),
    )


def _coerce_egress_value(value: Any) -> Any:
    return value


def test_connection(req: MssqlConnectRequest) -> TestConnectionResponse:
    _require_driver()
    conn = None
    try:
        conn = pyodbc.connect(_connection_string_from_request(req))
        cur = conn.cursor()
        cur.execute("SELECT DB_NAME()")
        db_name = cur.fetchone()[0]
        return TestConnectionResponse(success=True, message=f"Connected to MSSQL database '{db_name}'.")
    except Exception as exc:
        return TestConnectionResponse(success=False, message=str(exc))
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def connect(req: MssqlConnectRequest) -> ConnectResponse:
    _require_driver()
    cfg = _resolved_mssql_connect_config(req)
    conn = pyodbc.connect(_connection_string_from_request(req))
    _connections[cfg["connection_id"]] = {
        "connection": conn,
        "config": cfg,
        "opened_at": time.time(),
    }
    return ConnectResponse(
        connection_id=str(cfg["connection_id"]),
        message=f"Connected to MSSQL database '{cfg['database']}'.",
    )


def disconnect(connection_id: str) -> None:
    entry = _connections.pop(str(connection_id or "").strip(), None)
    if not entry:
        return
    conn = entry.get("connection")
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass


def run_query(connection_id: str, sql: str) -> QueryResponse:
    _require_driver()
    entry = _connections.get(str(connection_id or "").strip())
    if not entry:
        raise RuntimeError(f"MSSQL connection '{connection_id}' is not open.")
    conn = entry.get("connection")
    cur = conn.cursor()
    cur.execute(str(sql or ""))
    if cur.description is None:
        try:
            conn.commit()
        except Exception:
            pass
        return QueryResponse(
            columns=[],
            column_meta=[],
            rows=[],
            row_count=max(int(getattr(cur, "rowcount", -1) or -1), 0),
            message="Statement executed successfully.",
        )
    columns = [str(column[0]) for column in list(cur.description)]
    rows = [_row_to_dict(columns, row) for row in cur.fetchall()]
    column_meta = [
        ColumnMeta(
            name=name,
            data_type=str((description[1] if len(description) > 1 else "") or ""),
            source_table="",
        )
        for name, description in zip(columns, list(cur.description))
    ]
    return QueryResponse(
        columns=columns,
        column_meta=column_meta,
        rows=rows,
        row_count=len(rows),
    )


def ingest_query_to_duckdb(
    req: MssqlConnectRequest,
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
    _require_driver()
    source_conn = None
    source_interrupt_token = None
    duck_conn = None
    duck_interrupt_token = None
    inserted = 0
    next_progress_threshold = 1000
    try:
        source_conn = pyodbc.connect(_connection_string_from_request(req))
        if callable(on_interrupt_open):
            source_interrupt_token = on_interrupt_open(_build_mssql_interruptor(source_conn))
        source_cur = source_conn.cursor()
        source_cur.execute(str(sql or ""), list(sql_params or []))
        mapped_columns = _map_description_to_columns(source_cur.description)
        duck_conn = connect_duckdb(duckdb_path)
        if callable(on_interrupt_open):
            duck_interrupt_token = on_interrupt_open(_build_duckdb_interruptor(duck_conn))
        duck_conn.execute(_build_duckdb_create_table_sql(target_table, mapped_columns, replace=bool(replace)))
        chunk = _sanitize_chunk_size(chunk_size)
        while True:
            rows = source_cur.fetchmany(chunk)
            if not rows:
                break
            payload = [
                tuple(_coerce_source_value(value, mapped_columns[index]) for index, value in enumerate(list(row)))
                for row in rows
            ]
            insert_sql = (
                f"INSERT INTO {_quote_duckdb_compound_identifier(target_table)} VALUES ("
                + ", ".join(["?"] * len(mapped_columns))
                + ")"
            )
            duck_conn.executemany(insert_sql, payload)
            inserted += len(payload)
            if callable(on_progress) and inserted >= next_progress_threshold:
                on_progress({"row_count": inserted, "chunk_size": len(payload)})
                while inserted >= next_progress_threshold:
                    next_progress_threshold += 1000
        return DuckDbIngestQueryResponse(
            pipeline_id="mssql",
            target_table=target_table,
            row_count=inserted,
            source_type="mssql",
            source_connection_id=str(getattr(req, "connection_id", "") or ""),
            warnings=_collect_mapping_warnings(mapped_columns),
            column_mappings=_build_column_mappings(mapped_columns),
            created_at=time.time(),
            schema_changed=bool(replace),
        )
    finally:
        if source_conn is not None:
            try:
                source_conn.close()
            except Exception:
                pass
        if callable(on_interrupt_close):
            on_interrupt_close(source_interrupt_token)
        if duck_conn is not None:
            try:
                duck_conn.close()
            except Exception:
                pass
        if callable(on_interrupt_close):
            on_interrupt_close(duck_interrupt_token)


def egress_query_from_duckdb(
    *,
    target_request: MssqlConnectRequest,
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
    _require_driver()
    normalized_mode = str(mode or "replace").strip().lower()
    if normalized_mode not in {"replace", "append", "create", "create_append"}:
        raise RuntimeError(f"Unsupported MSSQL egress mode '{mode}'. Use replace, append, create, or create_append.")

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
        duck_cur.execute(str(sql or "").strip().rstrip(";"), list(sql_params or []))
        if duck_cur.description is None:
            raise RuntimeError("DuckDB egress query did not return a result set.")
        column_meta = _duckdb_column_meta_from_cursor(duck_cur)
        if not column_meta:
            raise RuntimeError("DuckDB egress query did not return any columns.")
        warnings.extend(
            _measure_mssql_string_column_lengths(
                duck_conn,
                sql,
                column_meta,
                sql_params=sql_params,
            )
        )
        column_mappings = _build_egress_inferred_column_mappings(column_meta)

        target_conn = pyodbc.connect(_connection_string_from_request(target_request))
        if callable(on_interrupt_open):
            target_interrupt_token = on_interrupt_open(_build_mssql_interruptor(target_conn))
        target_cur = target_conn.cursor()
        if normalized_mode in {"replace", "create", "create_append"}:
            _ensure_mssql_schema(target_cur, schema_name)
        table_exists = _mssql_table_exists(target_cur, schema_name=schema_name, table_name=table_name)
        if normalized_mode == "replace":
            if table_exists:
                target_cur.execute(f"DROP TABLE {quoted_target_table}")
            create_sql, create_warnings = _build_mssql_create_table_sql(normalized_target_table, column_meta)
            warnings.extend(create_warnings)
            target_cur.execute(create_sql)
        elif normalized_mode == "create":
            if table_exists:
                raise RuntimeError(f"Target table '{quoted_target_table}' already exists.")
            create_sql, create_warnings = _build_mssql_create_table_sql(normalized_target_table, column_meta)
            warnings.extend(create_warnings)
            target_cur.execute(create_sql)
        elif normalized_mode == "create_append":
            if not table_exists:
                create_sql, create_warnings = _build_mssql_create_table_sql(normalized_target_table, column_meta)
                warnings.extend(create_warnings)
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
            payload = [tuple(_coerce_egress_value(value) for value in row) for row in rows]
            if payload:
                target_cur.executemany(insert_sql, payload)
                row_count += len(payload)
        try:
            target_conn.commit()
        except Exception:
            pass
        try:
            remote_columns = _inspect_mssql_table_columns(target_cur, schema_name=schema_name, table_name=table_name)
            if not remote_columns:
                raise RuntimeError("No columns returned for remote target table.")
            column_mappings = _build_egress_remote_schema_column_mappings(
                column_meta,
                remote_columns,
                fallback_mappings=column_mappings,
            )
        except Exception as exc:
            warnings.append(f"MSSQL remote schema inspection failed; using inferred egress column mappings. {exc}")
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


def drop_table_if_exists(*, target_request: MssqlConnectRequest, target_table: str) -> None:
    _require_driver()
    conn = None
    cur = None
    schema_name, table_name, normalized_target_table = _normalize_target_relation(target_table)
    quoted_target_table = _quote_compound_identifier(normalized_target_table)
    try:
        conn = pyodbc.connect(_connection_string_from_request(target_request))
        cur = conn.cursor()
        if _mssql_table_exists(cur, schema_name=schema_name, table_name=table_name):
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
