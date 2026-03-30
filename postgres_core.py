"""
PostgreSQL Core
===============
This module owns PostgreSQL-specific runtime behavior for the app and the OSS
pipeline layer.

What belongs here:
- PostgreSQL connection management
- PostgreSQL result inspection/introspection
- PostgreSQL -> DuckDB ingress flows and PostgreSQL-specific mapping metadata
- DuckDB -> PostgreSQL egress flows

What does not belong here:
- generic DuckDB catalog/object-detail behavior
- DB2 or other connector logic
- notebook-specific orchestration beyond calling this connector core

Keep this file focused on PostgreSQL as a source/target boundary. If new logic
is not PostgreSQL-specific, it should usually live somewhere else.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
import uuid
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit
import time

import pyarrow as pa
import psycopg
from pydantic import BaseModel, Field

try:
    from . import base as _base_models
    from . import postgres as _postgres_queries
    from .duckdb_driver import connect_duckdb
    from .queron.runtime_models import ColumnMappingRecord
except ImportError:
    import base as _base_models
    import postgres as _postgres_queries
    from duckdb_driver import connect_duckdb
    from queron.runtime_models import ColumnMappingRecord

ColumnMeta = _base_models.ColumnMeta
ConnectResponse = _base_models.ConnectResponse
DEFAULT_QUERY_CHUNK_SIZE = _base_models.DEFAULT_QUERY_CHUNK_SIZE
DuckDbConnectRequest = _base_models.DuckDbConnectRequest
DuckDbIngestQueryResponse = _base_models.DuckDbIngestQueryResponse
PgConnectRequest = _base_models.PgConnectRequest
PgQueryChunkRequest = _base_models.PgQueryChunkRequest
PgQueryRequest = _base_models.PgQueryRequest
QueryResponse = _base_models.QueryResponse
TestConnectionResponse = _base_models.TestConnectionResponse


class _FallbackConnectorEgressResponse(BaseModel):
    target_name: str
    row_count: int
    warnings: list[str] = Field(default_factory=list)
    created_at: float
    schema_changed: bool = False


ConnectorEgressResponse = getattr(_base_models, "ConnectorEgressResponse", _FallbackConnectorEgressResponse)


_connections = _postgres_queries._connections
_query_sessions = _postgres_queries._query_sessions
_DEFAULT_QUERY_CHUNK_SIZE = DEFAULT_QUERY_CHUNK_SIZE
_DECIMAL_WITH_ARGS_RE = re.compile(r"^(?:numeric|decimal)\s*\((\d+)\s*,\s*(\d+)\s*\)$", re.IGNORECASE)
_OPAQUE_TYPE_RE = re.compile(r"type_name=([a-z0-9_ ]+)", re.IGNORECASE)
_DUCKDB_DECIMAL_RE = re.compile(r"^DECIMAL\((\d+),\s*(\d+)\)$", re.IGNORECASE)


@dataclass
class _MappedPostgresColumn:
    name: str
    source_type: str
    target_type: str
    warnings: list[str]
    lossy: bool | None = None
def _sanitize_chunk_size(chunk_size: int | None) -> int:
    size = int(chunk_size or _DEFAULT_QUERY_CHUNK_SIZE)
    if size < 1:
        return _DEFAULT_QUERY_CHUNK_SIZE
    return min(size, 5000)


def _is_schema_change_query(sql: str) -> bool:
    token = (sql or "").lstrip().split(None, 1)
    if not token:
        return False
    return token[0].lower() in {"create", "drop", "alter", "truncate", "rename"}


def _statement_message(row_count: int | None, schema_changed: bool) -> str:
    if schema_changed:
        return "Statement executed successfully. Schema updated."
    if isinstance(row_count, int) and row_count >= 0:
        return f"Statement executed successfully. {row_count} row(s) affected."
    return "Statement executed successfully."


def _build_connection_uri(raw_url: str, username: str | None, password: str | None) -> str:
    raw = (raw_url or "").strip()
    if not raw:
        raise RuntimeError("PostgreSQL connection URL is required.")
    if raw.startswith("jdbc:"):
        raw = raw[len("jdbc:"):]

    parts = urlsplit(raw)
    if not parts.scheme:
        return raw

    # Keep URL as-is if credentials are already in the URI.
    if parts.username:
        return raw

    if not username:
        return raw

    host = parts.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    port = f":{parts.port}" if parts.port else ""
    user = quote(username, safe="")
    pwd = quote(password or "", safe="")
    userinfo = f"{user}:{pwd}" if (password or "") else user
    netloc = f"{userinfo}@{host}{port}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def _config_from_request(req: PgConnectRequest) -> dict[str, str]:
    if req.url:
        uri = _build_connection_uri(req.url, req.username or None, req.password or None)
        return {"uri": uri}
    raw = f"postgresql://{req.host}:{req.port}/{req.database}"
    uri = _build_connection_uri(raw, req.username or None, req.password or None)
    return {"uri": uri}


def _config_from_connection_id(connection_id: str) -> dict[str, str]:
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("Connection not found. Please connect first.")
    return _config_from_request(PgConnectRequest(**cfg_dict))


def _request_from_connection_id(connection_id: str) -> PgConnectRequest:
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("Connection not found. Please connect first.")
    return PgConnectRequest(**cfg_dict)


def _pg_connection(uri: str):
    return psycopg.connect(uri)


def _table_from_rows(columns: list[str], rows: list[tuple[Any, ...]]) -> pa.Table:
    if not columns:
        return pa.table({})
    if not rows:
        return pa.table({name: pa.array([]) for name in columns})
    records = [{columns[i]: row[i] for i in range(min(len(columns), len(row)))} for row in rows]
    return pa.Table.from_pylist(records)


def _column_meta_from_table(table: pa.Table) -> list[ColumnMeta]:
    meta: list[ColumnMeta] = []
    for field in table.schema:
        meta.append(
            ColumnMeta(
                name=str(field.name),
                data_type=str(field.type),
                max_length=None,
                precision=None,
                scale=None,
                nullable=bool(field.nullable),
            )
        )
    return meta


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _quote_compound_identifier(identifier: str) -> str:
    parts = [part.strip() for part in str(identifier).split(".") if part.strip()]
    if not parts:
        raise RuntimeError("Target table name is required.")
    return ".".join(_quote_identifier(part.strip('"')) for part in parts)


def _strip_sql_terminator(sql: str) -> str:
    return sql.strip().rstrip(";")


def _split_target_table_name(target_table: str) -> tuple[str, str]:
    parts = [part.strip().strip('"') for part in str(target_table).split(".") if part.strip()]
    if len(parts) == 1:
        return "main", parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise RuntimeError(f"Invalid target table name '{target_table}'.")


def _split_external_relation(identifier: str) -> tuple[str | None, str | None, str]:
    parts = [part.strip().strip('"') for part in str(identifier or "").split(".") if part.strip()]
    if len(parts) == 1:
        return None, None, parts[0]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    raise RuntimeError(f"Invalid PostgreSQL relation '{identifier}'.")


def _ensure_duckdb_schema(conn, schema_name: str) -> None:
    if not schema_name or schema_name.lower() == "main":
        return
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(schema_name)}")


def _ensure_postgres_schema(cur, schema_name: str | None) -> None:
    schema = str(schema_name or "").strip()
    if not schema:
        return
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(schema)}")


def _current_postgres_database_and_schema(cur) -> tuple[str | None, str | None]:
    cur.execute("SELECT current_database(), current_schema()")
    row = cur.fetchone()
    if not row:
        return None, None
    return (str(row[0]).strip() or None, str(row[1]).strip() or None)


def _normalize_postgres_target_relation(
    identifier: str,
    *,
    current_database: str | None,
    current_schema: str | None,
) -> tuple[str | None, str, str, str]:
    catalog_name, schema_name, table_name = _split_external_relation(identifier)
    if catalog_name and current_database and str(catalog_name).lower() != str(current_database).lower():
        raise RuntimeError(
            f"PostgreSQL egress target database '{catalog_name}' does not match connection database "
            f"'{current_database}'. Cross-database egress is not supported."
        )
    effective_schema = str(schema_name or current_schema or "public").strip()
    normalized = f"{effective_schema}.{table_name}" if effective_schema else table_name
    return effective_schema or None, str(table_name), normalized, _quote_compound_identifier(normalized)


def _postgres_table_exists(cur, *, schema_name: str | None, table_name: str) -> bool:
    schema = str(schema_name or "").strip()
    if schema:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            LIMIT 1
            """,
            (schema, table_name),
        )
    else:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = %s
            LIMIT 1
            """,
            (table_name,),
        )
    return cur.fetchone() is not None


def _duckdb_table_exists(conn, target_table: str) -> bool:
    schema_name, table_name = _split_target_table_name(target_table)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            LIMIT 1
            """,
            (schema_name, table_name),
        )
        return cur.fetchone() is not None
    finally:
        cur.close()


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


def _normalize_postgres_source_type(raw_type: str) -> str:
    normalized = (raw_type or "").strip().lower()
    opaque_match = _OPAQUE_TYPE_RE.search(normalized)
    if opaque_match:
        normalized = opaque_match.group(1).strip().lower()
    return normalized


def _map_postgres_type_to_duckdb(raw_type: str) -> tuple[str, list[str], bool | None]:
    normalized = _normalize_postgres_source_type(raw_type)
    warnings: list[str] = []

    if normalized in {"bool", "boolean"}:
        return "BOOLEAN", warnings, False
    if normalized in {"smallint", "int2"}:
        return "SMALLINT", warnings, False
    if normalized in {"integer", "int", "int4"}:
        return "INTEGER", warnings, False
    if normalized in {"bigint", "int8"}:
        return "BIGINT", warnings, False
    if normalized in {"real", "float4"}:
        return "REAL", warnings, False
    if normalized in {"double precision", "float8", "double"}:
        return "DOUBLE", warnings, False
    if normalized in {"date"}:
        return "DATE", warnings, False
    if normalized in {"time without time zone", "time"}:
        return "TIME", warnings, False
    if normalized in {"time with time zone", "timetz"}:
        warnings.append("PostgreSQL TIME WITH TIME ZONE was normalized to DuckDB TIME.")
        return "TIME", warnings, True
    if normalized in {"timestamp without time zone", "timestamp"}:
        return "TIMESTAMP", warnings, False
    if normalized in {"timestamp with time zone", "timestamptz"}:
        return "TIMESTAMPTZ", warnings, False
    if normalized in {"interval"}:
        return "INTERVAL", warnings, False
    if normalized in {"uuid"}:
        return "UUID", warnings, False
    if normalized in {"bytea"}:
        return "BLOB", warnings, False

    decimal_match = _DECIMAL_WITH_ARGS_RE.match(normalized)
    if decimal_match:
        precision = int(decimal_match.group(1))
        scale = int(decimal_match.group(2))
        if precision <= 38:
            return f"DECIMAL({precision},{scale})", warnings, False
        warnings.append(
            f"PostgreSQL {raw_type} exceeds DuckDB DECIMAL precision limit 38 and was widened to VARCHAR."
        )
        return "VARCHAR", warnings, True
    if normalized in {"numeric", "decimal"}:
        warnings.append(f"PostgreSQL {raw_type} was normalized to DuckDB DECIMAL(38,10).")
        return "DECIMAL(38,10)", warnings, False

    if normalized.endswith("[]"):
        warnings.append(f"PostgreSQL {raw_type} was widened to DuckDB VARCHAR.")
        return "VARCHAR", warnings, True
    if normalized.startswith("json"):
        warnings.append(f"PostgreSQL {raw_type} was widened to DuckDB VARCHAR.")
        return "VARCHAR", warnings, True
    if normalized.startswith("character varying") or normalized.startswith("varchar"):
        return "VARCHAR", warnings, False
    if normalized.startswith("character") or normalized.startswith("char"):
        return "VARCHAR", warnings, False
    if normalized in {"text", "citext", "name", "xml"}:
        return "VARCHAR", warnings, False

    warnings.append(f"PostgreSQL {raw_type} was widened to DuckDB VARCHAR.")
    return "VARCHAR", warnings, True


def _map_postgres_columns_to_duckdb(source_columns: list[ColumnMeta]) -> list[_MappedPostgresColumn]:
    mapped: list[_MappedPostgresColumn] = []
    for column in source_columns:
        target_type, warnings, lossy = _map_postgres_type_to_duckdb(column.data_type)
        mapped.append(
            _MappedPostgresColumn(
                name=column.name,
                source_type=column.data_type,
                target_type=target_type,
                warnings=warnings,
                lossy=lossy,
            )
        )
    return mapped


def _map_duckdb_column_to_postgres(column: ColumnMeta) -> tuple[str, list[str]]:
    normalized = str(column.data_type or "UNKNOWN").strip().upper()
    warnings: list[str] = []
    if normalized in {"BOOLEAN", "SMALLINT", "INTEGER", "BIGINT", "REAL", "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "UUID", "INTERVAL"}:
        return normalized, warnings
    if normalized in {"DOUBLE"}:
        return "DOUBLE PRECISION", warnings
    if normalized in {"VARCHAR", "JSON"} or normalized.startswith("VARCHAR"):
        return "TEXT", warnings
    if normalized in {"BLOB", "BYTEA"}:
        return "BYTEA", warnings
    decimal_match = _DUCKDB_DECIMAL_RE.match(normalized)
    if decimal_match:
        precision = int(decimal_match.group(1))
        scale = int(decimal_match.group(2))
        return f"NUMERIC({precision},{scale})", warnings
    if normalized == "DECIMAL":
        precision = int(column.precision or 38)
        scale = int(column.scale or 0)
        return f"NUMERIC({precision},{scale})", warnings
    warnings.append(f"DuckDB type '{normalized}' was widened to PostgreSQL TEXT.")
    return "TEXT", warnings


def _build_duckdb_create_table_sql(
    target_table: str,
    mapped_columns: list[_MappedPostgresColumn],
    *,
    replace: bool = False,
) -> str:
    column_sql = ", ".join(f"{_quote_identifier(column.name)} {column.target_type}" for column in mapped_columns)
    prefix = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE"
    return f"{prefix} {_quote_compound_identifier(target_table)} ({column_sql})"


def _build_cast_select_sql(source_table: str, mapped_columns: list[_MappedPostgresColumn]) -> str:
    select_sql = ", ".join(
        f"CAST({_quote_identifier(column.name)} AS {column.target_type}) AS {_quote_identifier(column.name)}"
        for column in mapped_columns
    )
    return f"SELECT {select_sql} FROM {_quote_compound_identifier(source_table)}"


def _build_postgres_create_table_sql(target_table: str, columns: list[ColumnMeta]) -> tuple[str, list[str]]:
    warnings: list[str] = []
    column_defs: list[str] = []
    for column in columns:
        target_type, column_warnings = _map_duckdb_column_to_postgres(column)
        warnings.extend(column_warnings)
        null_part = "" if column.nullable else " NOT NULL"
        column_defs.append(f"{_quote_identifier(column.name)} {target_type}{null_part}")
    return f"CREATE TABLE {_quote_compound_identifier(target_table)} ({', '.join(column_defs)})", warnings


def _collect_mapping_warnings(mapped_columns: list[_MappedPostgresColumn], shared_warnings: list[str] | None = None) -> list[str]:
    warnings: list[str] = []
    seen: set[str] = set()
    for warning in shared_warnings or []:
        if warning and warning not in seen:
            seen.add(warning)
            warnings.append(warning)
    for column in mapped_columns:
        for warning in column.warnings or []:
            if warning and warning not in seen:
                seen.add(warning)
                warnings.append(warning)
    return warnings


def _inspect_postgres_result_columns(uri: str, sql: str) -> list[ColumnMeta]:
    conn = _pg_connection(uri)
    cur = None
    probe_name = f"loom_probe_{uuid.uuid4().hex[:12]}"
    probe_ident = _quote_identifier(probe_name)
    stripped_sql = _strip_sql_terminator(sql)
    rows: list[tuple[Any, ...]] = []
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE TEMP VIEW {probe_ident} AS {stripped_sql}")
        cur.execute(
            f"""
            SELECT
                a.attnum AS ordinal_position,
                a.attname AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type
            FROM pg_catalog.pg_attribute a
            WHERE a.attrelid = pg_catalog.to_regclass('pg_temp.{probe_name}')
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            """
        )
        rows = list(cur.fetchall() or [])
    finally:
        try:
            if cur is not None:
                cur.execute(f"DROP VIEW IF EXISTS {probe_ident}")
        except Exception:
            pass
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass
        conn.close()

    return [
        ColumnMeta(
            name=str(row[1]),
            data_type=str(row[2] or "UNKNOWN"),
            nullable=True,
        )
        for row in rows
    ]


def _column_meta_from_cursor(conn, cursor) -> list[ColumnMeta]:
    if cursor.description is None:
        return []
    deduped_names = [str(desc[0]) for desc in cursor.description]
    return _postgres_queries._build_basic_column_meta(cursor, deduped_names, conn)


def _coerce_postgres_source_value(value: Any, column: _MappedPostgresColumn) -> Any:
    if value is None:
        return None
    target_type = str(column.target_type or "").strip().upper()
    if isinstance(value, memoryview):
        value = bytes(value)
    if target_type in {"VARCHAR", "UUID"} and isinstance(value, uuid.UUID):
        return str(value)
    if target_type == "VARCHAR" and isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, default=str)
    if target_type == "TIME" and hasattr(value, "tzinfo") and getattr(value, "tzinfo", None) is not None:
        return value.replace(tzinfo=None)
    return value


def _coerce_postgres_egress_value(value: Any) -> Any:
    if isinstance(value, memoryview):
        return bytes(value)
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, default=str)
    return value


def _inspect_duckdb_target_columns(conn, target_table: str) -> list[ColumnMeta]:
    schema_name, table_name = _split_target_table_name(target_table)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            (schema_name, table_name),
        )
        rows = list(cur.fetchall() or [])
    finally:
        cur.close()

    return [
        ColumnMeta(
            name=str(row[0]),
            data_type=str(row[1] or "UNKNOWN"),
            nullable=str(row[2] or "").upper() != "NO",
        )
        for row in rows
    ]


def _build_column_mappings(
    *,
    source_columns: list[ColumnMeta],
    target_columns: list[ColumnMeta],
    connector_type: str,
    mapping_mode: str,
    shared_warnings: list[str] | None = None,
    mapped_columns: list[_MappedPostgresColumn] | None = None,
) -> list[ColumnMappingRecord]:
    mappings: list[ColumnMappingRecord] = []
    max_columns = max(len(source_columns), len(target_columns))
    mismatch_warning = "Source and target column metadata counts did not match exactly."
    for index in range(max_columns):
        source_col = source_columns[index] if index < len(source_columns) else None
        target_col = target_columns[index] if index < len(target_columns) else None
        warnings = list(shared_warnings or [])
        mapped_col = mapped_columns[index] if mapped_columns is not None and index < len(mapped_columns) else None
        for warning in mapped_col.warnings if mapped_col is not None else []:
            if warning not in warnings:
                warnings.append(warning)
        if source_col is None or target_col is None:
            warnings.append(mismatch_warning)
        mappings.append(
            ColumnMappingRecord(
                ordinal_position=index + 1,
                source_column=str(source_col.name if source_col else (target_col.name if target_col else f"column_{index + 1}")),
                source_type=str(source_col.data_type if source_col else "UNKNOWN"),
                target_column=str(target_col.name if target_col else (source_col.name if source_col else f"column_{index + 1}")),
                target_type=str(target_col.data_type if target_col else "UNKNOWN"),
                connector_type=connector_type,
                mapping_mode=mapping_mode,
                warnings=warnings,
                lossy=mapped_col.lossy if mapped_col is not None else None,
            )
        )
    return mappings


def _execute_query_arrow_table(uri: str, sql: str) -> tuple[pa.Table | None, int]:
    conn = _pg_connection(uri)
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(sql)
        if cur.description is None:
            return None, max(0, cur.rowcount or 0)

        columns = [str(desc[0]) for desc in cur.description]
        rows = cur.fetchall()
        table = _table_from_rows(columns, rows)
        return table, table.num_rows
    finally:
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass
        conn.close()


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


def _copy_arrow_table_to_duckdb(
    conn,
    *,
    table: pa.Table,
    target_table: str,
    replace: bool,
    chunk_size: int,
) -> int:
    try:
        from . import duckdb_core
    except ImportError:
        import duckdb_core

    target_ident = duckdb_core._quote_compound_identifier(target_table)
    temp_name = "__loom_pg_arrow_chunk"

    def _register_arrow(chunk_table: pa.Table) -> None:
        try:
            conn.unregister(temp_name)
        except Exception:
            pass
        conn.register(temp_name, chunk_table)

    if table.num_rows == 0:
        _register_arrow(table)
        if replace:
            conn.execute(f"CREATE OR REPLACE TABLE {target_ident} AS SELECT * FROM {temp_name} LIMIT 0")
        else:
            conn.execute(f"CREATE TABLE {target_ident} AS SELECT * FROM {temp_name} LIMIT 0")
        return 0

    total = 0
    batches = table.to_batches(max_chunksize=chunk_size)
    for index, batch in enumerate(batches):
        chunk_table = pa.Table.from_batches([batch])
        _register_arrow(chunk_table)
        if index == 0:
            if replace:
                conn.execute(f"CREATE OR REPLACE TABLE {target_ident} AS SELECT * FROM {temp_name}")
            else:
                conn.execute(f"CREATE TABLE {target_ident} AS SELECT * FROM {temp_name}")
        else:
            conn.execute(f"INSERT INTO {target_ident} SELECT * FROM {temp_name}")
        total += chunk_table.num_rows
    return total


def _materialize_staged_postgres_arrow_to_duckdb(
    conn,
    *,
    table: pa.Table,
    target_table: str,
    replace: bool,
    chunk_size: int,
    mapped_columns: list[_MappedPostgresColumn],
) -> int:
    if not mapped_columns:
        raise RuntimeError("PostgreSQL result did not contain any columns to materialize.")

    target_schema, _ = _split_target_table_name(target_table)
    raw_stage_table = f'_queron_meta.raw_ingest_{uuid.uuid4().hex[:16]}'
    work_table = target_table if not replace else f'_queron_meta.typed_ingest_{uuid.uuid4().hex[:16]}'
    raw_stage_ident = _quote_compound_identifier(raw_stage_table)
    work_table_ident = _quote_compound_identifier(work_table)
    target_ident = _quote_compound_identifier(target_table)

    if not replace and _duckdb_table_exists(conn, target_table):
        raise RuntimeError(f"Target table '{target_table}' already exists.")

    _ensure_duckdb_schema(conn, "_queron_meta")
    _ensure_duckdb_schema(conn, target_schema)

    raw_stage_created = False
    work_table_created = False

    try:
        _copy_arrow_table_to_duckdb(
            conn,
            table=table,
            target_table=raw_stage_table,
            replace=True,
            chunk_size=chunk_size,
        )
        raw_stage_created = True

        conn.execute(_build_duckdb_create_table_sql(work_table, mapped_columns))
        work_table_created = True

        insert_sql = f"INSERT INTO {work_table_ident} {_build_cast_select_sql(raw_stage_table, mapped_columns)}"
        conn.execute(insert_sql)

        if replace:
            conn.execute(f"CREATE OR REPLACE TABLE {target_ident} AS SELECT * FROM {work_table_ident}")

        if work_table_created and replace:
            conn.execute(f"DROP TABLE IF EXISTS {work_table_ident}")
            work_table_created = False
        if raw_stage_created:
            conn.execute(f"DROP TABLE IF EXISTS {raw_stage_ident}")
            raw_stage_created = False

        return table.num_rows
    except Exception as exc:
        if work_table_created:
            try:
                conn.execute(f"DROP TABLE IF EXISTS {work_table_ident}")
            except Exception:
                pass
        if raw_stage_created:
            raise RuntimeError(
                f"Failed to coerce PostgreSQL result into target DuckDB types. "
                f"Raw staging table preserved as {raw_stage_table}. {exc}"
            ) from exc
        raise



def test_connection(req: PgConnectRequest):
    return _postgres_queries.test_connection(req)


def connect(req: PgConnectRequest):
    return _postgres_queries.connect(req)


def run_query(req: PgQueryRequest):
    return _postgres_queries.run_query(req)


def fetch_query_chunk(session_id: str, req: PgQueryChunkRequest):
    return _postgres_queries.fetch_query_chunk(session_id, req)


def close_query_session(session_id: str):
    return _postgres_queries.close_query_session(session_id)


def ingest_query_to_duckdb(
    *,
    source_connection_id: str,
    duckdb_connection_id: str,
    sql: str,
    target_table: str,
    replace: bool,
    chunk_size: int,
    pipeline_id: str,
) -> DuckDbIngestQueryResponse:
    cfg = _config_from_connection_id(source_connection_id)
    source_conn = None
    source_cur = None
    duck_conn = None
    inserted = 0
    warnings: list[str] = []
    try:
        source_conn = _pg_connection(cfg["uri"])
        source_cur = source_conn.cursor()
        source_cur.execute(_strip_sql_terminator(sql))
        if source_cur.description is None:
            raise RuntimeError("Source query did not return a result set.")

        try:
            source_columns = _inspect_postgres_result_columns(cfg["uri"], sql)
        except Exception:
            source_columns = _column_meta_from_cursor(source_conn, source_cur)
            warnings.append("Fell back to cursor metadata while inspecting PostgreSQL result metadata.")

        mapped_columns = _map_postgres_columns_to_duckdb(source_columns)
        warnings = _collect_mapping_warnings(mapped_columns, warnings)
        if not mapped_columns:
            raise RuntimeError("No columns found in source result.")

        duck_conn = _duckdb_connection_from_id(duckdb_connection_id)
        duck_conn.execute(_build_duckdb_create_table_sql(target_table, mapped_columns, replace=replace))

        target_ident = _quote_compound_identifier(target_table)
        column_names = [column.name for column in mapped_columns]
        col_sql = ", ".join(_quote_identifier(name) for name in column_names)
        placeholders = ", ".join(["?"] * len(column_names))
        insert_sql = f"INSERT INTO {target_ident} ({col_sql}) VALUES ({placeholders})"

        fetch_size = _sanitize_chunk_size(chunk_size)
        while True:
            rows = source_cur.fetchmany(fetch_size)
            if not rows:
                break
            payload = [
                tuple(
                    _coerce_postgres_source_value(value, mapped_columns[index])
                    for index, value in enumerate(row)
                )
                for row in rows
            ]
            if payload:
                duck_conn.executemany(insert_sql, payload)
                inserted += len(payload)

        target_columns = _inspect_duckdb_target_columns(duck_conn, target_table)
    finally:
        try:
            if source_cur is not None:
                source_cur.close()
        except Exception:
            pass
        try:
            if source_conn is not None:
                source_conn.close()
        except Exception:
            pass
        try:
            if duck_conn is not None:
                duck_conn.close()
        except Exception:
            pass

    return DuckDbIngestQueryResponse(
        pipeline_id=pipeline_id,
        target_table=target_table,
        row_count=inserted,
        source_type="postgres",
        source_connection_id=source_connection_id,
        warnings=warnings,
        column_mappings=_build_column_mappings(
            source_columns=source_columns,
            target_columns=target_columns,
            connector_type="postgres",
            mapping_mode="python_mapped",
            shared_warnings=warnings,
            mapped_columns=mapped_columns,
        ),
        created_at=time.time(),
        schema_changed=True,
    )


def egress_query_from_duckdb(
    *,
    target_connection_id: str,
    duckdb_database: str,
    sql: str,
    target_table: str,
    mode: str = "replace",
    chunk_size: int = 1000,
) -> ConnectorEgressResponse:
    cfg = _config_from_connection_id(target_connection_id)
    normalized_mode = str(mode or "replace").strip().lower()
    if normalized_mode not in {"replace", "append", "create", "create_append"}:
        raise RuntimeError(
            f"Unsupported PostgreSQL egress mode '{mode}'. Use replace, append, create, or create_append."
        )

    duck_conn = None
    duck_cur = None
    target_conn = None
    target_cur = None
    warnings: list[str] = []
    row_count = 0
    try:
        duck_conn = connect_duckdb(str(duckdb_database))
        duck_cur = duck_conn.cursor()
        duck_cur.execute(_strip_sql_terminator(sql))
        if duck_cur.description is None:
            raise RuntimeError("DuckDB egress query did not return a result set.")
        column_meta = _duckdb_column_meta_from_cursor(duck_cur)
        if not column_meta:
            raise RuntimeError("DuckDB egress query did not return any columns.")

        target_conn = _pg_connection(cfg["uri"])
        target_cur = target_conn.cursor()
        current_database, current_schema = _current_postgres_database_and_schema(target_cur)
        schema_name, table_name, normalized_target_table, quoted_target_table = _normalize_postgres_target_relation(
            target_table,
            current_database=current_database,
            current_schema=current_schema,
        )
        if normalized_mode in {"replace", "create", "create_append"}:
            _ensure_postgres_schema(target_cur, schema_name)

        table_exists = _postgres_table_exists(target_cur, schema_name=schema_name, table_name=table_name)
        if normalized_mode == "replace":
            if table_exists:
                target_cur.execute(f"DROP TABLE {quoted_target_table}")
            create_sql, warnings = _build_postgres_create_table_sql(normalized_target_table, column_meta)
            target_cur.execute(create_sql)
        elif normalized_mode == "create":
            if table_exists:
                raise RuntimeError(f"Target table '{quoted_target_table}' already exists.")
            create_sql, warnings = _build_postgres_create_table_sql(normalized_target_table, column_meta)
            target_cur.execute(create_sql)
        elif normalized_mode == "create_append":
            if not table_exists:
                create_sql, warnings = _build_postgres_create_table_sql(normalized_target_table, column_meta)
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
            payload = [tuple(_coerce_postgres_egress_value(value) for value in row) for row in rows]
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
            if target_cur is not None:
                target_cur.close()
        except Exception:
            pass
        try:
            if target_conn is not None:
                target_conn.close()
        except Exception:
            pass
        try:
            if duck_conn is not None:
                duck_conn.close()
        except Exception:
            pass

    return ConnectorEgressResponse(
        target_name=quoted_target_table,
        row_count=row_count,
        warnings=warnings,
        created_at=time.time(),
        schema_changed=normalized_mode in {"replace", "create", "create_append"},
    )


def health():
    return {"status": "ok"}
