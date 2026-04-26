"""
MySQL Core
==========
MySQL-specific connection, ingress, egress, and lookup-table cleanup helpers.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any
from urllib.parse import unquote, urlsplit

try:
    import pymysql
    from pymysql.constants import FIELD_TYPE
except ImportError:
    pymysql = None
    FIELD_TYPE = None

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
MysqlConnectRequest = _base_models.MysqlConnectRequest
QueryResponse = _base_models.QueryResponse
TestConnectionResponse = _base_models.TestConnectionResponse

_connections: dict[str, dict[str, Any]] = {}
_DEFAULT_QUERY_CHUNK_SIZE = int(getattr(_base_models, "DEFAULT_QUERY_CHUNK_SIZE", 200) or 200)
_VALID_MYSQL_AUTH_MODES = {"basic", "tls", "mtls", "socket"}


class _MappedMysqlColumn:
    def __init__(self, *, name: str, source_type: str, target_type: str, warnings: list[str], lossy: bool = False) -> None:
        self.name = name
        self.source_type = source_type
        self.target_type = target_type
        self.warnings = warnings
        self.lossy = lossy


def _require_driver() -> None:
    if pymysql is None:
        raise RuntimeError("PyMySQL is not installed. Install the 'pymysql' package first.")


def _sanitize_chunk_size(chunk_size: int | None) -> int:
    size = int(chunk_size or _DEFAULT_QUERY_CHUNK_SIZE)
    if size < 1:
        return _DEFAULT_QUERY_CHUNK_SIZE
    return min(size, 5000)


def _strip_sql_terminator(sql: str) -> str:
    return str(sql or "").strip().rstrip(";")


def _quote_identifier(identifier: str) -> str:
    return "`" + str(identifier).replace("`", "``") + "`"


def _quote_compound_identifier(identifier: str) -> str:
    parts = [part.strip().strip("`\"") for part in str(identifier or "").split(".") if part.strip()]
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
    parts = [part.strip().strip("`\"") for part in str(identifier or "").split(".") if part.strip()]
    if len(parts) == 1:
        return None, parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise RuntimeError(f"Invalid MySQL target table name '{identifier}'.")


def _normalize_target_relation(identifier: str) -> tuple[str | None, str, str]:
    schema_name, table_name = _split_target_table_name(identifier)
    normalized = f"{schema_name}.{table_name}" if schema_name else table_name
    return schema_name, table_name, normalized


def _infer_mysql_auth_mode(req: MysqlConnectRequest) -> str:
    explicit = str(getattr(req, "auth_mode", "") or "").strip().lower()
    if explicit:
        if explicit not in _VALID_MYSQL_AUTH_MODES:
            raise RuntimeError(f"Unsupported MySQL auth_mode '{req.auth_mode}'.")
        return explicit
    if str(getattr(req, "unix_socket", "") or "").strip():
        return "socket"
    if str(getattr(req, "ssl_cert", "") or "").strip() or str(getattr(req, "ssl_key", "") or "").strip():
        return "mtls"
    if str(getattr(req, "ssl_ca", "") or "").strip():
        return "tls"
    return "basic"


def _validate_mysql_auth_request(req: MysqlConnectRequest, auth_mode: str) -> None:
    if auth_mode not in _VALID_MYSQL_AUTH_MODES:
        raise RuntimeError(f"Unsupported MySQL auth_mode '{auth_mode}'.")
    if auth_mode == "socket" and not str(getattr(req, "unix_socket", "") or "").strip():
        raise RuntimeError("MySQL socket auth requires unix_socket.")
    if auth_mode in {"tls", "mtls"} and not str(getattr(req, "ssl_ca", "") or "").strip():
        raise RuntimeError(f"MySQL {auth_mode} auth requires ssl_ca.")
    if auth_mode == "mtls":
        if not str(getattr(req, "ssl_cert", "") or "").strip() or not str(getattr(req, "ssl_key", "") or "").strip():
            raise RuntimeError("MySQL mTLS auth requires ssl_cert and ssl_key.")


def _resolved_mysql_connect_config(req: MysqlConnectRequest) -> dict[str, Any]:
    auth_mode = _infer_mysql_auth_mode(req)
    _validate_mysql_auth_request(req, auth_mode)
    cfg: dict[str, Any] = {
        "connection_id": str(getattr(req, "connection_id", "") or "").strip() or uuid.uuid4().hex,
        "name": str(getattr(req, "name", "") or "").strip() or "mysql",
        "host": str(getattr(req, "host", "localhost") or "localhost").strip(),
        "port": int(getattr(req, "port", 3306) or 3306),
        "database": str(getattr(req, "database", "mysql") or "mysql").strip(),
        "username": str(getattr(req, "username", "") or "").strip() or None,
        "password": str(getattr(req, "password", "") or ""),
        "auth_mode": auth_mode,
        "unix_socket": str(getattr(req, "unix_socket", "") or "").strip() or None,
        "connect_timeout": int(getattr(req, "connect_timeout_seconds", 0) or 0) or None,
        "ssl": None,
    }
    raw_url = str(getattr(req, "url", "") or "").strip()
    if raw_url:
        parsed = urlsplit(raw_url[5:] if raw_url.lower().startswith("jdbc:") else raw_url)
        if parsed.hostname:
            cfg["host"] = parsed.hostname
        if parsed.port:
            cfg["port"] = parsed.port
        if parsed.path and parsed.path.strip("/"):
            cfg["database"] = unquote(parsed.path.strip("/"))
        if parsed.username:
            cfg["username"] = unquote(parsed.username)
        if parsed.password:
            cfg["password"] = unquote(parsed.password)
    if auth_mode in {"tls", "mtls"}:
        ssl_payload: dict[str, Any] = {"ca": str(getattr(req, "ssl_ca", "") or "").strip()}
        if auth_mode == "mtls":
            ssl_payload["cert"] = str(getattr(req, "ssl_cert", "") or "").strip()
            ssl_payload["key"] = str(getattr(req, "ssl_key", "") or "").strip()
        cfg["ssl"] = ssl_payload
    return cfg


def _connect_from_request(req: MysqlConnectRequest):
    _require_driver()
    cfg = _resolved_mysql_connect_config(req)
    kwargs: dict[str, Any] = {
        "host": cfg["host"],
        "port": int(cfg["port"]),
        "user": cfg["username"],
        "password": cfg["password"],
        "database": cfg["database"],
        "autocommit": False,
        "charset": "utf8mb4",
    }
    if cfg["unix_socket"]:
        kwargs["unix_socket"] = cfg["unix_socket"]
        kwargs.pop("host", None)
        kwargs.pop("port", None)
    if cfg["connect_timeout"] is not None:
        kwargs["connect_timeout"] = int(cfg["connect_timeout"])
    if cfg["ssl"]:
        kwargs["ssl"] = cfg["ssl"]
    conn = pymysql.connect(**kwargs)
    cur = None
    try:
        cur = conn.cursor()
        cur.execute("SET SESSION sql_mode = CONCAT(@@sql_mode, ',ANSI_QUOTES')")
    finally:
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass
    return conn


def _field_type_name(type_code: Any) -> str:
    if FIELD_TYPE is None:
        return str(type_code or "UNKNOWN")
    for name, value in FIELD_TYPE.__dict__.items():
        if name.startswith("_"):
            continue
        if value == type_code:
            return name
    return str(type_code or "UNKNOWN")


def _normalize_mysql_source_type(type_code: Any) -> tuple[str, list[str], bool]:
    raw = _field_type_name(type_code)
    normalized = raw.upper()
    warnings: list[str] = []
    if normalized in {"TINY", "SHORT"}:
        return "SMALLINT", warnings, False
    if normalized in {"LONG", "INT24"}:
        return "INTEGER", warnings, False
    if normalized == "LONGLONG":
        return "BIGINT", warnings, False
    if normalized == "FLOAT":
        return "REAL", warnings, False
    if normalized == "DOUBLE":
        return "DOUBLE", warnings, False
    if normalized in {"DECIMAL", "NEWDECIMAL"}:
        return "DECIMAL(38,10)", warnings, False
    if normalized in {"DATE", "NEWDATE"}:
        return "DATE", warnings, False
    if normalized == "TIME":
        return "TIME", warnings, False
    if normalized in {"DATETIME", "TIMESTAMP"}:
        return "TIMESTAMP", warnings, False
    if normalized in {"TINY_BLOB", "MEDIUM_BLOB", "LONG_BLOB", "BLOB"}:
        return "BLOB", warnings, False
    if normalized in {"BIT"}:
        return "BLOB", warnings, True
    if normalized in {"JSON"}:
        warnings.append("MySQL JSON was widened to DuckDB VARCHAR.")
        return "VARCHAR", warnings, True
    if normalized in {"VAR_STRING", "STRING", "VARCHAR", "ENUM", "SET"}:
        return "VARCHAR", warnings, False
    warnings.append(f"MySQL type '{raw}' was widened to DuckDB VARCHAR.")
    return "VARCHAR", warnings, True


def _map_description_to_columns(description: Any) -> list[_MappedMysqlColumn]:
    mapped: list[_MappedMysqlColumn] = []
    for item in list(description or []):
        name = str(item[0] or "").strip() or "column"
        type_code = item[1] if len(item) > 1 else None
        target_type, warnings, lossy = _normalize_mysql_source_type(type_code)
        mapped.append(_MappedMysqlColumn(name=name, source_type=_field_type_name(type_code), target_type=target_type, warnings=warnings, lossy=lossy))
    return mapped


def _build_duckdb_create_table_sql(target_table: str, mapped_columns: list[_MappedMysqlColumn], *, replace: bool = False) -> str:
    column_sql = ", ".join(f"{_quote_duckdb_identifier(column.name)} {column.target_type}" for column in mapped_columns)
    prefix = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE"
    return f"{prefix} {_quote_duckdb_compound_identifier(target_table)} ({column_sql})"


def _duckdb_column_meta_from_cursor(cursor: Any) -> list[ColumnMeta]:
    if cursor.description is None:
        return []
    metas: list[ColumnMeta] = []
    for desc in cursor.description:
        metas.append(ColumnMeta(name=str(desc[0]), data_type=str((desc[1] if len(desc) > 1 else "") or "UNKNOWN"), nullable=True))
    return metas


def _map_duckdb_column_to_mysql(column: ColumnMeta) -> tuple[str, list[str]]:
    normalized = str(column.data_type or "UNKNOWN").strip().upper()
    warnings: list[str] = []
    if normalized in {"BOOLEAN", "BOOL"}:
        return "BOOLEAN", warnings
    if normalized == "SMALLINT":
        return "SMALLINT", warnings
    if normalized == "INTEGER":
        return "INT", warnings
    if normalized == "BIGINT":
        return "BIGINT", warnings
    if normalized == "REAL":
        return "FLOAT", warnings
    if normalized == "DOUBLE":
        return "DOUBLE", warnings
    if normalized == "DATE":
        return "DATE", warnings
    if normalized == "TIME":
        return "TIME", warnings
    if normalized in {"TIMESTAMP", "TIMESTAMPTZ"}:
        if normalized == "TIMESTAMPTZ":
            warnings.append("DuckDB TIMESTAMPTZ was normalized to MySQL DATETIME.")
        return "DATETIME", warnings
    if normalized.startswith("DECIMAL("):
        return normalized, warnings
    if normalized == "DECIMAL":
        precision = int(column.precision or 38)
        scale = int(column.scale or 10)
        return f"DECIMAL({min(precision, 65)},{min(scale, 30)})", warnings
    if normalized in {"BLOB", "BYTEA"}:
        return "LONGBLOB", warnings
    if normalized in {"VARCHAR", "JSON", "UUID"} or normalized.startswith("VARCHAR"):
        return "LONGTEXT", warnings
    warnings.append(f"DuckDB type '{normalized}' was widened to MySQL LONGTEXT.")
    return "LONGTEXT", warnings


def _build_mysql_create_table_sql(target_table: str, columns: list[ColumnMeta]) -> tuple[str, list[str]]:
    warnings: list[str] = []
    column_defs: list[str] = []
    for column in columns:
        target_type, column_warnings = _map_duckdb_column_to_mysql(column)
        warnings.extend(column_warnings)
        null_part = "" if column.nullable else " NOT NULL"
        column_defs.append(f"{_quote_identifier(column.name)} {target_type}{null_part}")
    return f"CREATE TABLE {_quote_compound_identifier(target_table)} ({', '.join(column_defs)})", warnings


def _mysql_table_exists(cur: Any, *, schema_name: str | None, table_name: str) -> bool:
    if schema_name:
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s LIMIT 1",
            (schema_name, table_name),
        )
    else:
        cur.execute("SHOW TABLES LIKE %s", (table_name,))
    return cur.fetchone() is not None


def _ensure_mysql_schema(cur: Any, schema_name: str | None) -> None:
    schema = str(schema_name or "").strip()
    if schema:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {_quote_identifier(schema)}")


def _coerce_value(value: Any) -> Any:
    if isinstance(value, memoryview):
        return bytes(value)
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, default=str)
    return value


def _build_column_mappings(mapped_columns: list[_MappedMysqlColumn]) -> list[ColumnMappingRecord]:
    return [
        ColumnMappingRecord(
            source_column=column.name,
            source_type=column.source_type,
            target_column=column.name,
            target_type=column.target_type,
            connector_type="mysql",
            mapping_mode="ingress",
            warnings=list(column.warnings or []),
            lossy=bool(column.lossy),
            ordinal_position=index,
        )
        for index, column in enumerate(mapped_columns, start=1)
    ]


def _collect_mapping_warnings(mapped_columns: list[_MappedMysqlColumn]) -> list[str]:
    warnings: list[str] = []
    seen: set[str] = set()
    for column in mapped_columns:
        for warning in column.warnings or []:
            if warning and warning not in seen:
                seen.add(warning)
                warnings.append(warning)
    return warnings


def _build_mysql_interruptor(conn: Any) -> Any:
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


def test_connection(req: MysqlConnectRequest) -> TestConnectionResponse:
    conn = None
    try:
        conn = _connect_from_request(req)
        cur = conn.cursor()
        cur.execute("SELECT DATABASE()")
        db_name = cur.fetchone()[0]
        return TestConnectionResponse(success=True, message=f"Connected to MySQL database '{db_name}'.")
    except Exception as exc:
        return TestConnectionResponse(success=False, message=str(exc))
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def connect(req: MysqlConnectRequest) -> ConnectResponse:
    cfg = _resolved_mysql_connect_config(req)
    conn = _connect_from_request(req)
    _connections[cfg["connection_id"]] = {"connection": conn, "config": cfg, "opened_at": time.time()}
    return ConnectResponse(connection_id=str(cfg["connection_id"]), message=f"Connected to MySQL database '{cfg['database']}'.")


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
        raise RuntimeError(f"MySQL connection '{connection_id}' is not open.")
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
    req: MysqlConnectRequest,
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
            source_interrupt_token = on_interrupt_open(_build_mysql_interruptor(source_conn))
        source_cur = source_conn.cursor()
        source_cur.execute(_strip_sql_terminator(sql), tuple(sql_params or []))
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
            pipeline_id="mysql",
            target_table=target_table,
            row_count=inserted,
            source_type="mysql",
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
    target_request: MysqlConnectRequest,
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
        raise RuntimeError(f"Unsupported MySQL egress mode '{mode}'. Use replace, append, create, or create_append.")
    duck_conn = None
    duck_cur = None
    target_conn = None
    target_cur = None
    duck_interrupt_token = None
    target_interrupt_token = None
    warnings: list[str] = []
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
        target_conn = _connect_from_request(target_request)
        if callable(on_interrupt_open):
            target_interrupt_token = on_interrupt_open(_build_mysql_interruptor(target_conn))
        target_cur = target_conn.cursor()
        if normalized_mode in {"replace", "create", "create_append"}:
            _ensure_mysql_schema(target_cur, schema_name)
        table_exists = _mysql_table_exists(target_cur, schema_name=schema_name, table_name=table_name)
        if normalized_mode == "replace":
            if table_exists:
                target_cur.execute(f"DROP TABLE {quoted_target_table}")
            create_sql, warnings = _build_mysql_create_table_sql(normalized_target_table, column_meta)
            target_cur.execute(create_sql)
        elif normalized_mode == "create":
            if table_exists:
                raise RuntimeError(f"Target table '{quoted_target_table}' already exists.")
            create_sql, warnings = _build_mysql_create_table_sql(normalized_target_table, column_meta)
            target_cur.execute(create_sql)
        elif normalized_mode == "create_append":
            if not table_exists:
                create_sql, warnings = _build_mysql_create_table_sql(normalized_target_table, column_meta)
                target_cur.execute(create_sql)
        elif not table_exists:
            raise RuntimeError(f"Target table '{quoted_target_table}' does not exist for append mode.")
        column_names = [column.name for column in column_meta]
        col_sql = ", ".join(_quote_identifier(name) for name in column_names)
        placeholders = ", ".join(["%s"] * len(column_names))
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
        created_at=time.time(),
        schema_changed=normalized_mode in {"replace", "create", "create_append"},
    )


def drop_table_if_exists(*, target_request: MysqlConnectRequest, target_table: str) -> None:
    conn = None
    cur = None
    schema_name, table_name, normalized_target_table = _normalize_target_relation(target_table)
    quoted_target_table = _quote_compound_identifier(normalized_target_table)
    try:
        conn = _connect_from_request(target_request)
        cur = conn.cursor()
        if _mysql_table_exists(cur, schema_name=schema_name, table_name=table_name):
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
