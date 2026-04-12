"""
Loom Backend - DuckDB Core
==========================
This module owns DuckDB-specific behavior for the app and the OSS pipeline layer.

What belongs here:
- DuckDB connection management
- local query execution and paging
- creating local DuckDB tables from already-fetched source rows/metadata
- DuckDB object inspection and metadata reads
- DuckDB-only export and artifact helpers

What does not belong here:
- source-system connection logic for PostgreSQL, DB2, or other connectors
- connector-specific query planning or credential handling
- long-term ownership of connector-specific type rules once they move into
  the individual connector cores

For now this file keeps the minimal DuckDB mapping helpers it needs for local
table materialization so it no longer depends on ``type_mapper.py`` at runtime.
"""

from __future__ import annotations

import re
import time
import uuid
from dataclasses import dataclass
import json
import os
from pathlib import Path
import tempfile
from typing import Any

from pydantic import BaseModel, Field

try:
    from . import base as _base_models
    from .duckdb_driver import connect_duckdb, load_duckdb
    from .queron.runtime_models import (
        ColumnMappingRecord,
        CompiledContractRecord,
        NodeRunRecord,
        NodeStateRecord,
        NodeWarningEvent,
        PipelineRunRecord,
        TableLineageRecord,
        normalize_warning_events,
    )
except ImportError:
    import base as _base_models
    from duckdb_driver import connect_duckdb, load_duckdb
    from queron.runtime_models import (
        ColumnMappingRecord,
        CompiledContractRecord,
        NodeRunRecord,
        NodeStateRecord,
        NodeWarningEvent,
        PipelineRunRecord,
        TableLineageRecord,
        normalize_warning_events,
    )

ColumnMeta = _base_models.ColumnMeta
ConnectResponse = _base_models.ConnectResponse
DEFAULT_QUERY_CHUNK_SIZE = _base_models.DEFAULT_QUERY_CHUNK_SIZE
DuckDbConnectRequest = _base_models.DuckDbConnectRequest
DuckDbIngestQueryResponse = _base_models.DuckDbIngestQueryResponse
PgQueryChunkRequest = _base_models.PgQueryChunkRequest
PgQueryRequest = _base_models.PgQueryRequest
QueryResponse = _base_models.QueryResponse
TestConnectionResponse = _base_models.TestConnectionResponse


class _FallbackDuckDbExportResponse(BaseModel):
    output_path: str
    row_count: int
    file_size_bytes: int | None = None
    export_format: str
    warnings: list[str] = Field(default_factory=list)
    created_at: float


DuckDbExportResponse = getattr(_base_models, "DuckDbExportResponse", _FallbackDuckDbExportResponse)


_connections: dict[str, dict[str, Any]] = {}
_query_sessions: dict[str, dict[str, Any]] = {}
_QUERY_SESSION_IDLE_TTL_SECONDS = 300
_DEFAULT_QUERY_CHUNK_SIZE = DEFAULT_QUERY_CHUNK_SIZE
_DECIMAL_WITH_ARGS_RE = re.compile(r"^(?:numeric|decimal)\s*\((\d+)\s*,\s*(\d+)\s*\)$", re.IGNORECASE)


@dataclass
class MappingPolicy:
    strict: bool = False
    enum_as_varchar: bool = True
    json_mode: str = "varchar"  # varchar | json
    timestamp_tz_mode: str = "preserve"  # preserve | utc_timestamp
    max_decimal_precision: int = 38
    on_overflow: str = "double"  # fail | double | varchar
    on_cast_error: str = "widen_to_varchar"  # fail | null | widen_to_varchar


@dataclass
class TypeMappingResult:
    canonical_type: str
    duckdb_type: str
    nullable: bool = True
    warnings: list[str] | None = None
    lossy: bool = False


@dataclass
class MappedColumn:
    name: str
    source_type: str
    canonical_type: str
    duckdb_type: str
    nullable: bool = True
    warnings: list[str] | None = None
    lossy: bool = False


def _normalize_source_name(source: str) -> str:
    normalized = (source or "").strip().lower()
    if normalized in {"postgresql", "pg"}:
        return "postgres"
    return normalized


def _normalize_source_type(
    source: str,
    raw_type: str,
    *,
    length: int | None = None,
    precision: int | None = None,
    scale: int | None = None,
) -> str:
    src = _normalize_source_name(source)
    normalized = (raw_type or "").strip().lower()

    if normalized in {"bool", "boolean"}:
        return "bool"
    if normalized in {"smallint", "int2"}:
        return "int16"
    if normalized in {"integer", "int", "int4"}:
        return "int32"
    if normalized in {"bigint", "int8"}:
        return "int64"
    if normalized in {"real", "float4"}:
        return "float32"
    if normalized in {"double", "double precision", "float8"}:
        return "float64"
    if normalized == "date":
        return "date"
    if normalized in {"time", "timetz"}:
        return "time"
    if normalized == "timestamp":
        return "timestamp"
    if normalized in {"timestamptz", "timestamp with time zone"}:
        return "timestamptz"
    if normalized in {"json", "jsonb"}:
        return "json"
    if normalized in {"bytea", "blob", "binary", "varbinary"}:
        return "binary"
    if normalized in {"uuid", "enum", "text"}:
        return "string"
    if normalized.endswith("[]"):
        return "array"
    if normalized.startswith(("varchar", "char", "character")):
        return "string"
    if normalized.startswith(("decimal", "numeric")):
        match = _DECIMAL_WITH_ARGS_RE.match(normalized)
        if match:
            return f"decimal({int(match.group(1))},{int(match.group(2))})"
        if isinstance(precision, int):
            resolved_scale = int(scale or 0)
            return f"decimal({precision},{resolved_scale})"
        return "decimal"

    if src == "db2" and normalized.startswith("graphic"):
        return "string"

    if length is not None and normalized == "":
        return "string"

    return "unknown"


def _canonical_to_duckdb(canonical_type: str, policy: MappingPolicy | None = None) -> TypeMappingResult:
    resolved_policy = policy or MappingPolicy()
    canonical = (canonical_type or "unknown").strip().lower()
    warnings: list[str] = []
    lossy = False

    if canonical == "bool":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="BOOLEAN")
    if canonical == "int16":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="SMALLINT")
    if canonical == "int32":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="INTEGER")
    if canonical == "int64":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="BIGINT")
    if canonical == "float32":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="REAL")
    if canonical == "float64":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="DOUBLE")
    if canonical == "string":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="VARCHAR")
    if canonical == "date":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="DATE")
    if canonical == "time":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="TIME")
    if canonical == "timestamp":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="TIMESTAMP")
    if canonical == "timestamptz":
        if resolved_policy.timestamp_tz_mode == "utc_timestamp":
            warnings.append("TIMESTAMPTZ will be normalized to UTC TIMESTAMP.")
            return TypeMappingResult(canonical_type=canonical_type, duckdb_type="TIMESTAMP", warnings=warnings)
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="TIMESTAMPTZ")
    if canonical == "json":
        if resolved_policy.json_mode == "json":
            return TypeMappingResult(canonical_type=canonical_type, duckdb_type="JSON")
        warnings.append("JSON mapped to VARCHAR by policy.")
        return TypeMappingResult(
            canonical_type=canonical_type,
            duckdb_type="VARCHAR",
            warnings=warnings,
            lossy=True,
        )
    if canonical == "binary":
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="BLOB")

    match = re.match(r"^decimal\((\d+),(\d+)\)$", canonical)
    if canonical == "decimal" or match:
        if match:
            precision = int(match.group(1))
            scale = int(match.group(2))
        else:
            precision = 38
            scale = 10
            warnings.append("DECIMAL without explicit precision/scale defaulted to DECIMAL(38,10).")

        if precision <= resolved_policy.max_decimal_precision:
            return TypeMappingResult(
                canonical_type=canonical_type,
                duckdb_type=f"DECIMAL({precision},{scale})",
                warnings=warnings or None,
            )

        lossy = True
        warnings.append(
            f"DECIMAL precision {precision} exceeds max {resolved_policy.max_decimal_precision}. "
            f"Applying overflow policy '{resolved_policy.on_overflow}'."
        )
        if resolved_policy.on_overflow == "fail":
            return TypeMappingResult(
                canonical_type=canonical_type,
                duckdb_type="INVALID",
                warnings=warnings,
                lossy=lossy,
            )
        if resolved_policy.on_overflow == "varchar":
            return TypeMappingResult(
                canonical_type=canonical_type,
                duckdb_type="VARCHAR",
                warnings=warnings,
                lossy=lossy,
            )
        return TypeMappingResult(
            canonical_type=canonical_type,
            duckdb_type="DOUBLE",
            warnings=warnings,
            lossy=lossy,
        )

    if canonical in {"array", "struct", "unknown"}:
        warnings.append(f"Canonical type '{canonical}' mapped to VARCHAR.")
        return TypeMappingResult(canonical_type=canonical_type, duckdb_type="VARCHAR", warnings=warnings, lossy=True)

    warnings.append(f"Unrecognized canonical type '{canonical}' mapped to VARCHAR.")
    return TypeMappingResult(canonical_type=canonical_type, duckdb_type="VARCHAR", warnings=warnings, lossy=True)


def map_column_to_duckdb(
    *,
    name: str,
    source: str,
    raw_type: str,
    nullable: bool = True,
    length: int | None = None,
    precision: int | None = None,
    scale: int | None = None,
    policy: MappingPolicy | None = None,
) -> MappedColumn:
    canonical = _normalize_source_type(source, raw_type, length=length, precision=precision, scale=scale)
    mapped = _canonical_to_duckdb(canonical, policy=policy)
    return MappedColumn(
        name=name,
        source_type=raw_type,
        canonical_type=canonical,
        duckdb_type=mapped.duckdb_type,
        nullable=nullable,
        warnings=mapped.warnings,
        lossy=mapped.lossy,
    )


def map_columns_to_duckdb(
    columns: list[Any],
    *,
    source: str,
    policy: MappingPolicy | None = None,
) -> list[MappedColumn]:
    mapped: list[MappedColumn] = []
    for col in columns:
        if isinstance(col, dict):
            name = str(col.get("name", "column"))
            raw_type = str(col.get("data_type") or col.get("type") or "UNKNOWN")
            nullable = bool(col.get("nullable", True))
            length = col.get("max_length", col.get("length"))
            precision = col.get("precision")
            scale = col.get("scale")
        else:
            name = str(getattr(col, "name", "column"))
            raw_type = str(getattr(col, "data_type", "UNKNOWN"))
            nullable = bool(getattr(col, "nullable", True))
            length = getattr(col, "max_length", None)
            precision = getattr(col, "precision", None)
            scale = getattr(col, "scale", None)

        mapped.append(
            map_column_to_duckdb(
                name=name,
                source=source,
                raw_type=raw_type,
                nullable=nullable,
                length=length if isinstance(length, int) else None,
                precision=precision if isinstance(precision, int) else None,
                scale=scale if isinstance(scale, int) else None,
                policy=policy,
            )
        )
    return mapped


def build_duckdb_create_table_sql(table_name: str, columns: list[MappedColumn], *, replace: bool = True) -> str:
    def _quoted_identifier(identifier: str) -> str:
        return '"' + str(identifier).replace('"', '""') + '"'

    def _quoted_table(identifier: str) -> str:
        parts = [part.strip() for part in str(identifier).split(".") if part.strip()]
        if not parts:
            raise RuntimeError("Target table name is required.")
        return ".".join(_quoted_identifier(part) for part in parts)

    mode = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE"
    column_defs: list[str] = []
    for col in columns:
        null_part = "" if col.nullable else " NOT NULL"
        column_defs.append(f"{_quoted_identifier(col.name)} {col.duckdb_type}{null_part}")
    return f"{mode} {_quoted_table(table_name)} ({', '.join(column_defs)})"


def _resolve_database_path(cfg: DuckDbConnectRequest) -> str:
    raw = (cfg.url or cfg.database or ":memory:").strip()
    if raw == "" or raw == ":memory:":
        return ":memory:"
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def _duckdb_connection(database: str):
    load_duckdb()
    return connect_duckdb(database)


def _cursor_columns(cursor) -> list[str]:
    if cursor.description is None:
        return []
    return [desc[0] for desc in cursor.description]


def _rows_to_records(col_names: list[str], rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    records: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            records.append(row)
            continue
        records.append({col_names[i]: row[i] for i in range(min(len(col_names), len(row)))})
    return records


def _normalize_duckdb_display_type(type_name: str) -> str:
    raw = (type_name or "").strip()
    if not raw:
        return raw
    upper = raw.upper()
    if upper.startswith("CHARACTER VARYING"):
        return "VARCHAR" + raw[len("CHARACTER VARYING"):]
    if upper.startswith("TIMESTAMP"):
        if "WITH TIME ZONE" in upper:
            return "TIMESTAMPTZ"
        return "TIMESTAMP"
    return upper


def _build_basic_column_meta(cursor) -> list[ColumnMeta]:
    if cursor.description is None:
        return []
    metas: list[ColumnMeta] = []
    for desc in cursor.description:
        type_code = desc[1] if len(desc) > 1 else None
        type_name = _normalize_duckdb_display_type(str(type_code or "UNKNOWN"))
        internal_size = desc[3] if len(desc) > 3 else None
        precision = desc[4] if len(desc) > 4 else None
        scale = desc[5] if len(desc) > 5 else None
        null_ok = desc[6] if len(desc) > 6 else None
        metas.append(
            ColumnMeta(
                name=desc[0],
                data_type=type_name,
                max_length=internal_size if isinstance(internal_size, int) and internal_size >= 0 else None,
                precision=precision if isinstance(precision, int) else None,
                scale=scale if isinstance(scale, int) else None,
                nullable=True if null_ok is None else bool(null_ok),
            )
        )
    return metas


def _cleanup_expired_query_sessions() -> None:
    now = time.time()
    expired_ids = [
        session_id
        for session_id, session in _query_sessions.items()
        if now - session.get("last_accessed_at", now) > _QUERY_SESSION_IDLE_TTL_SECONDS
    ]
    for session_id in expired_ids:
        _query_sessions.pop(session_id, None)


def _is_incremental_query(sql: str) -> bool:
    normalized = _strip_leading_sql_comments(sql).lower()
    return normalized.startswith("select") or normalized.startswith("with")


def _is_schema_change_query(sql: str) -> bool:
    token = _strip_leading_sql_comments(sql).split(None, 1)
    if not token:
        return False
    return token[0].lower() in {"create", "drop", "alter", "truncate", "rename"}


def _strip_leading_sql_comments(sql: str) -> str:
    text = (sql or "").lstrip()
    while text:
        if text.startswith("--"):
            newline = text.find("\n")
            if newline < 0:
                return ""
            text = text[newline + 1 :].lstrip()
            continue
        if text.startswith("/*"):
            end = text.find("*/")
            if end < 0:
                return ""
            text = text[end + 2 :].lstrip()
            continue
        break
    return text


def _statement_message(row_count: int | None, schema_changed: bool) -> str:
    if schema_changed:
        return "Statement executed successfully. Schema updated."
    if isinstance(row_count, int) and row_count >= 0:
        return f"Statement executed successfully. {row_count} row(s) affected."
    return "Statement executed successfully."


def _strip_sql_terminator(sql: str) -> str:
    return sql.strip().rstrip(";")


def _sanitize_copy_option_token(value: str, *, label: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise RuntimeError(f"{label} is required.")
    if not re.fullmatch(r"[A-Za-z0-9_+-]+", text):
        raise RuntimeError(f"{label} contains unsupported characters.")
    return text


def _resolve_export_path(path: str, *, working_dir: str | None = None) -> Path:
    raw = str(path or "").strip()
    if not raw:
        raise RuntimeError("Export path is required.")
    resolved = Path(raw).expanduser()
    if not resolved.is_absolute():
        base_dir = Path(working_dir).expanduser() if working_dir is not None else Path.cwd()
        resolved = (base_dir / resolved)
    resolved = resolved.resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


def _load_query_row_count(conn, *, sql: str) -> int:
    row = conn.execute(
        f"SELECT COUNT(*) FROM ({_strip_sql_terminator(sql)}) AS queron_export_count"
    ).fetchone()
    if not row:
        return 0
    try:
        return int(row[0] or 0)
    except Exception:
        return 0


def _build_paged_sql(sql: str) -> str:
    base_sql = _strip_sql_terminator(sql)
    return f"SELECT * FROM ({base_sql}) AS loom_paged_query LIMIT ? OFFSET ?"


def _run_statement_query(req: PgQueryRequest, database: str) -> QueryResponse:
    schema_changed = _is_schema_change_query(req.sql)
    conn = _duckdb_connection(database)
    try:
        cur = conn.cursor()
        cur.execute(req.sql)
        if cur.description is None:
            raw_row_count = cur.rowcount if isinstance(cur.rowcount, int) else None
            safe_row_count = raw_row_count if isinstance(raw_row_count, int) and raw_row_count >= 0 else 0
            return QueryResponse(
                columns=[],
                column_meta=[],
                rows=[],
                row_count=safe_row_count,
                has_more=False,
                chunk_size=req.chunk_size,
                mode="statement",
                message=_statement_message(raw_row_count, schema_changed),
                schema_changed=schema_changed,
            )
        columns = _cursor_columns(cur)
        column_meta = _build_basic_column_meta(cur)
        rows = cur.fetchmany(req.chunk_size + 1)
        has_more = len(rows) > req.chunk_size
        emitted_rows = rows[:req.chunk_size]
        serialized_rows = _rows_to_records(columns, emitted_rows)

        # DuckDB returns synthetic one-column result sets for many statements
        # (for example Count/Success). Treat those as statement acknowledgements.
        pseudo_statement = len(columns) == 1 and columns[0].strip().lower() in {"count", "success"}
        if pseudo_statement:
            raw_row_count: int | None = None
            if serialized_rows:
                first_value = serialized_rows[0].get(columns[0])
                if isinstance(first_value, bool):
                    raw_row_count = 0
                elif isinstance(first_value, (int, float)) and not isinstance(first_value, bool):
                    raw_row_count = int(first_value)
            safe_row_count = raw_row_count if isinstance(raw_row_count, int) and raw_row_count >= 0 else 0
            return QueryResponse(
                columns=[],
                column_meta=[],
                rows=[],
                row_count=safe_row_count,
                has_more=False,
                chunk_size=req.chunk_size,
                mode="statement",
                message=_statement_message(raw_row_count, schema_changed),
                schema_changed=schema_changed,
            )

        return QueryResponse(
            columns=columns,
            column_meta=column_meta,
            rows=serialized_rows,
            row_count=len(serialized_rows),
            has_more=has_more,
            chunk_size=req.chunk_size,
            mode="statement",
            message=_statement_message(None, schema_changed) if schema_changed else None,
            schema_changed=schema_changed,
        )
    finally:
        conn.close()


def _fetch_offset_chunk(session: dict[str, Any], chunk_size: int) -> QueryResponse:
    offset = session.get("offset", 0)
    conn = _duckdb_connection(session["database"])
    try:
        cur = conn.cursor()
        cur.execute(_build_paged_sql(session["sql"]), (chunk_size + 1, offset))
        rows = cur.fetchall()
    finally:
        conn.close()

    has_more = len(rows) > chunk_size
    emitted_rows = rows[:chunk_size]
    serialized_rows = _rows_to_records(session["columns"], emitted_rows)
    loaded_count = session.get("loaded_count", 0) + len(serialized_rows)
    session["offset"] = offset + len(emitted_rows)
    session["loaded_count"] = loaded_count
    session["last_accessed_at"] = time.time()

    if not has_more:
        _query_sessions.pop(session["session_id"], None)

    return QueryResponse(
        columns=session["columns"],
        column_meta=session["column_meta"],
        rows=serialized_rows,
        row_count=loaded_count,
        query_session_id=session["session_id"] if has_more else None,
        has_more=has_more,
        chunk_size=session["chunk_size"],
        mode="offset",
    )


def _drain_offset_session(session: dict[str, Any]) -> QueryResponse:
    all_rows: list[dict[str, Any]] = []
    while True:
        response = _fetch_offset_chunk(session, session["chunk_size"])
        all_rows.extend(response.rows)
        if not response.has_more:
            loaded_count = session.get("loaded_count", 0)
            return QueryResponse(
                columns=response.columns,
                column_meta=response.column_meta,
                rows=all_rows,
                row_count=loaded_count,
                query_session_id=None,
                has_more=False,
                chunk_size=session["chunk_size"],
                mode="offset",
            )


def _open_offset_query_session(req: PgQueryRequest, database: str) -> QueryResponse:
    conn = _duckdb_connection(database)
    try:
        cur = conn.cursor()
        cur.execute(_build_paged_sql(req.sql), (req.chunk_size + 1, 0))
        if cur.description is None:
            return QueryResponse(
                columns=[],
                column_meta=[],
                rows=[],
                row_count=0,
                has_more=False,
                chunk_size=req.chunk_size,
                mode="offset",
            )
        columns = _cursor_columns(cur)
        column_meta = _build_basic_column_meta(cur)
        rows = cur.fetchall()
    finally:
        conn.close()

    has_more = len(rows) > req.chunk_size
    emitted_rows = rows[:req.chunk_size]
    serialized_rows = _rows_to_records(columns, emitted_rows)

    if not has_more:
        return QueryResponse(
            columns=columns,
            column_meta=column_meta,
            rows=serialized_rows,
            row_count=len(serialized_rows),
            query_session_id=None,
            has_more=False,
            chunk_size=req.chunk_size,
            mode="offset",
        )

    session_id = str(uuid.uuid4())
    _query_sessions[session_id] = {
        "session_id": session_id,
        "connection_id": req.connection_id,
        "sql": req.sql,
        "chunk_size": req.chunk_size,
        "columns": columns,
        "column_meta": column_meta,
        "database": database,
        "offset": len(emitted_rows),
        "loaded_count": len(emitted_rows),
        "last_accessed_at": time.time(),
    }

    return QueryResponse(
        columns=columns,
        column_meta=column_meta,
        rows=serialized_rows,
        row_count=len(serialized_rows),
        query_session_id=session_id,
        has_more=True,
        chunk_size=req.chunk_size,
        mode="offset",
    )


def test_connection(req: DuckDbConnectRequest):
    database = _resolve_database_path(req)
    try:
        conn = _duckdb_connection(database)
        try:
            conn.execute("SELECT 1")
        finally:
            conn.close()
        return TestConnectionResponse(success=True, message="Connection successful!")
    except Exception as exc:
        return TestConnectionResponse(success=False, message=str(exc))


def connect(req: DuckDbConnectRequest):
    database = _resolve_database_path(req)
    try:
        conn = _duckdb_connection(database)
        try:
            conn.execute("SELECT 1")
        finally:
            conn.close()
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc

    conn_id = str(uuid.uuid4())
    _connections[conn_id] = req.model_dump()
    return ConnectResponse(connection_id=conn_id, message="Connected successfully!")


def run_query(req: PgQueryRequest):
    cfg_dict = _connections.get(req.connection_id)
    if cfg_dict is None:
        raise LookupError("Connection not found. Please connect first.")

    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    _cleanup_expired_query_sessions()
    try:
        if _is_incremental_query(req.sql):
            return _open_offset_query_session(req, database)
        return _run_statement_query(req, database)
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc


def fetch_query_chunk(session_id: str, req: PgQueryChunkRequest):
    session = _query_sessions.get(session_id)
    if session is None:
        raise LookupError("Query session not found or expired.")

    chunk_size = req.chunk_size or session.get("chunk_size") or _DEFAULT_QUERY_CHUNK_SIZE
    try:
        if req.load_all:
            return _drain_offset_session(session)
        return _fetch_offset_chunk(session, chunk_size)
    except Exception as exc:
        _query_sessions.pop(session_id, None)
        raise RuntimeError(str(exc)) from exc


def close_query_session(session_id: str):
    existed = session_id in _query_sessions
    _query_sessions.pop(session_id, None)
    return {"closed": existed}


def get_schema(connection_id: str):
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("Connection not found.")
    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))

    sql = """
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog', '_queron_meta')
        ORDER BY table_schema, table_name, ordinal_position
    """
    try:
        conn = _duckdb_connection(database)
        try:
            rows = conn.execute(sql).fetchall()
        finally:
            conn.close()
        schemas: dict[str, dict[str, list[dict[str, str]]]] = {}
        for schema_name, table_name, column_name, type_name in rows:
            schemas.setdefault(schema_name, {}).setdefault(table_name, []).append(
                {"name": column_name, "type": _normalize_duckdb_display_type(str(type_name or ""))}
            )
        return {
            "schemas": list(schemas.keys()),
            "tables": [
                {"schema": schema_name, "name": table_name, "columns": columns}
                for schema_name, tables in schemas.items()
                for table_name, columns in tables.items()
            ],
        }
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc


def get_database_tree(connection_id: str):
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("Connection not found.")
    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))

    sql = """
        SELECT
            table_schema,
            table_name,
            CASE
                WHEN table_type = 'BASE TABLE' THEN 'Tables'
                WHEN table_type = 'VIEW' THEN 'Views'
                ELSE 'Tables'
            END AS category
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog', '_queron_meta')
        ORDER BY table_schema, table_name
    """
    try:
        conn = _duckdb_connection(database)
        try:
            rows = conn.execute(sql).fetchall()
        finally:
            conn.close()
        tree: dict[str, dict[str, list[dict[str, Any]]]] = {}
        for schema_name, object_name, category in rows:
            tree.setdefault(schema_name, {}).setdefault(category, []).append(
                {"name": object_name, "kind": "t" if category == "Tables" else "v", "column_count": 0}
            )
        for schema_name, categories in tree.items():
            for category_rows in categories.values():
                category_rows.sort(key=lambda item: item["name"])
        return dict(sorted(tree.items()))
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc


def get_object_details(
    connection_id: str,
    schema: str = "main",
    name: str = "test",
    category: str = "table",
    tab: str = "overview",
):
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError(f"Connection {connection_id} not found")
    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    try:
        conn = _duckdb_connection(database)
        try:
            cur = conn.cursor()
            if tab == "overview":
                return _get_overview(cur, schema, name, category)
            if tab == "columns":
                return _get_columns(cur, schema, name)
            if tab == "constraints":
                return _get_constraints(cur, schema, name)
            if tab == "indexes":
                return _get_indexes(cur, schema, name)
            if tab == "ddl":
                return _get_ddl(cur, schema, name, category)
            if tab == "er_diagram":
                return _get_er_diagram(cur, schema, name)
            if tab == "dependencies":
                return _get_dependencies(cur, schema, name, category)
            if tab == "sample_data":
                return _get_sample_data(cur, schema, name)
            return {"error": f"Tab '{tab}' not supported for category '{category}'"}
        finally:
            conn.close()
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc


def _quote_identifier(identifier: str) -> str:
    escaped = str(identifier).replace('"', '""')
    return f'"{escaped}"'


def _quote_compound_identifier(identifier: str) -> str:
    parts = [part.strip() for part in str(identifier).split(".") if part.strip()]
    if not parts:
        raise RuntimeError("Target table name is required.")
    return ".".join(_quote_identifier(part) for part in parts)


def _split_target_table_name(identifier: str) -> tuple[str, str]:
    parts = [part.strip().strip('"') for part in str(identifier).split(".") if part.strip()]
    if len(parts) == 1:
        return "main", parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise RuntimeError(f"Invalid target table name '{identifier}'.")


def _qualified_name(schema: str, name: str) -> str:
    return f"{_quote_identifier(schema)}.{_quote_identifier(name)}"


_RUN_STATUS_VALUES = ("pending", "running", "success", "success_with_warnings", "failed", "skipped")
_NODE_STATUS_VALUES = ("ready", "running", "complete", "complete_with_warnings", "failed", "skipped", "cleared")


def _run_status_check_sql() -> str:
    values = ", ".join(f"'{item}'" for item in _RUN_STATUS_VALUES)
    return f"CHECK (status IN ({values}))"


def _node_status_check_sql() -> str:
    values = ", ".join(f"'{item}'" for item in _NODE_STATUS_VALUES)
    return f"CHECK (status IN ({values}))"


def _node_state_check_sql() -> str:
    values = ", ".join(f"'{item}'" for item in _NODE_STATUS_VALUES)
    return f"CHECK (state IN ({values}))"


def _pipeline_runs_create_sql(*, table_name: str) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            run_id VARCHAR PRIMARY KEY,
            run_label VARCHAR,
            log_path VARCHAR,
            compile_id VARCHAR,
            pipeline_id VARCHAR,
            target VARCHAR,
            artifact_path VARCHAR,
            started_at VARCHAR,
            finished_at VARCHAR,
            status VARCHAR {_run_status_check_sql()},
            error_message VARCHAR
        )
        """


def _node_runs_create_sql(*, table_name: str) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            node_run_id VARCHAR PRIMARY KEY,
            run_id VARCHAR,
            node_name VARCHAR,
            node_kind VARCHAR,
            artifact_name VARCHAR,
            started_at VARCHAR,
            finished_at VARCHAR,
            status VARCHAR {_node_status_check_sql()},
            row_count_in BIGINT,
            row_count_out BIGINT,
            artifact_size_bytes BIGINT,
            error_message VARCHAR,
            warnings_json VARCHAR,
            details_json VARCHAR,
            active_node_state_id VARCHAR
        )
        """


def _compiled_contracts_create_sql(*, table_name: str) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            compile_id VARCHAR PRIMARY KEY,
            pipeline_id VARCHAR,
            pipeline_path VARCHAR,
            project_root VARCHAR,
            artifact_path VARCHAR,
            config_path VARCHAR,
            target VARCHAR,
            compiled_at VARCHAR,
            is_active BOOLEAN,
            contract_hash VARCHAR,
            edge_hash VARCHAR,
            config_hash VARCHAR,
            project_python_hash VARCHAR,
            node_hashes_json VARCHAR,
            edges_json VARCHAR,
            tracked_files_json VARCHAR,
            external_dependencies_json VARCHAR,
            spec_json VARCHAR,
            diagnostics_json VARCHAR
        )
        """


def _node_states_create_sql(*, table_name: str) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            node_state_id VARCHAR PRIMARY KEY,
            run_id VARCHAR,
            node_run_id VARCHAR,
            node_name VARCHAR,
            state VARCHAR {_node_state_check_sql()},
            is_active BOOLEAN,
            created_at VARCHAR,
            trigger VARCHAR,
            details_json VARCHAR
        )
        """


def _table_create_sql(conn, *, schema_name: str, table_name: str) -> str | None:
    row = conn.execute(
        """
        SELECT sql
        FROM duckdb_tables()
        WHERE schema_name = ? AND table_name = ?
        LIMIT 1
        """,
        (schema_name, table_name),
    ).fetchone()
    if not row:
        return None
    text = str(row[0] or "").strip()
    return text or None


def _table_supports_success_with_warnings(conn, *, schema_name: str, table_name: str) -> bool:
    sql = _table_create_sql(conn, schema_name=schema_name, table_name=table_name)
    return "success_with_warnings" in str(sql or "").lower()


def _table_contains_columns(conn, *, schema_name: str, table_name: str, column_names: list[str]) -> bool:
    sql = str(_table_create_sql(conn, schema_name=schema_name, table_name=table_name) or "").lower()
    return all(str(column_name).lower() in sql for column_name in column_names)


def _migrate_table_if_needed(
    conn,
    *,
    schema_name: str,
    table_name: str,
    create_sql: str,
    column_names: list[str],
    source_column_names: list[str] | None = None,
    required_column_names: list[str] | None = None,
) -> None:
    if _table_supports_success_with_warnings(conn, schema_name=schema_name, table_name=table_name) and (
        not required_column_names
        or _table_contains_columns(conn, schema_name=schema_name, table_name=table_name, column_names=required_column_names)
    ):
        return
    qualified_table = f"{_quote_identifier(schema_name)}.{_quote_identifier(table_name)}"
    temp_name = f"{table_name}__v2"
    qualified_temp = f"{_quote_identifier(schema_name)}.{_quote_identifier(temp_name)}"
    insert_columns_sql = ", ".join(_quote_identifier(column) for column in column_names)
    select_columns = source_column_names or column_names
    select_columns_sql = ", ".join(_quote_identifier(column) for column in select_columns)
    conn.execute(f"DROP TABLE IF EXISTS {qualified_temp}")
    conn.execute(create_sql.replace(qualified_table, qualified_temp, 1))
    conn.execute(
        f"INSERT INTO {qualified_temp} ({insert_columns_sql}) SELECT {select_columns_sql} FROM {qualified_table}"
    )
    conn.execute(f"DROP TABLE {qualified_table}")
    conn.execute(f"ALTER TABLE {qualified_temp} RENAME TO {_quote_identifier(table_name)}")


def _ensure_queron_meta_tables(conn) -> None:
    meta_schema = _quote_identifier("_queron_meta")
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {meta_schema}")
    pipeline_runs_name = f"{meta_schema}.{_quote_identifier('pipeline_runs')}"
    compiled_contracts_name = f"{meta_schema}.{_quote_identifier('compiled_contracts')}"
    node_runs_name = f"{meta_schema}.{_quote_identifier('node_runs')}"
    node_states_name = f"{meta_schema}.{_quote_identifier('node_states')}"
    conn.execute(_pipeline_runs_create_sql(table_name=pipeline_runs_name))
    conn.execute(_compiled_contracts_create_sql(table_name=compiled_contracts_name))
    conn.execute(_node_runs_create_sql(table_name=node_runs_name))
    conn.execute(_node_states_create_sql(table_name=node_states_name))
    try:
        conn.execute(f"ALTER TABLE {pipeline_runs_name} ADD COLUMN IF NOT EXISTS run_label VARCHAR")
    except Exception:
        pass
    try:
        conn.execute(f"ALTER TABLE {pipeline_runs_name} ADD COLUMN IF NOT EXISTS log_path VARCHAR")
    except Exception:
        pass
    try:
        conn.execute(f"ALTER TABLE {pipeline_runs_name} ADD COLUMN IF NOT EXISTS compile_id VARCHAR")
    except Exception:
        pass
    try:
        conn.execute(f"ALTER TABLE {node_runs_name} ADD COLUMN IF NOT EXISTS details_json VARCHAR")
    except Exception:
        pass
    _migrate_table_if_needed(
        conn,
        schema_name="_queron_meta",
        table_name="pipeline_runs",
        create_sql=_pipeline_runs_create_sql(table_name=pipeline_runs_name),
        column_names=[
            "run_id",
            "run_label",
            "log_path",
            "compile_id",
            "pipeline_id",
            "target",
            "artifact_path",
            "started_at",
            "finished_at",
            "status",
            "error_message",
        ],
        source_column_names=[
            "run_id",
            "run_label",
            "log_path",
            "compile_id",
            "notebook_id",
            "target",
            "artifact_path",
            "started_at",
            "finished_at",
            "status",
            "error_message",
        ],
        required_column_names=["pipeline_id"],
    )
    node_runs_sql = str(_table_create_sql(conn, schema_name="_queron_meta", table_name="node_runs") or "").lower()
    if "node_run_id" not in node_runs_sql or "active_node_state_id" not in node_runs_sql:
        temp_name = f"{_quote_identifier('_queron_meta')}.{_quote_identifier('node_runs__v2')}"
        conn.execute(f"DROP TABLE IF EXISTS {temp_name}")
        conn.execute(_node_runs_create_sql(table_name=temp_name))
        conn.execute(
            f"""
            INSERT INTO {temp_name} (
                node_run_id,
                run_id,
                node_name,
                node_kind,
                artifact_name,
                started_at,
                finished_at,
                status,
                row_count_in,
                row_count_out,
                artifact_size_bytes,
                error_message,
                warnings_json,
                details_json,
                active_node_state_id
            )
            SELECT
                run_id || ':' || node_name,
                run_id,
                node_name,
                node_kind,
                artifact_name,
                started_at,
                finished_at,
                CASE LOWER(status)
                    WHEN 'pending' THEN 'ready'
                    WHEN 'success' THEN 'complete'
                    WHEN 'success_with_warnings' THEN 'complete'
                    ELSE LOWER(status)
                END,
                row_count_in,
                row_count_out,
                artifact_size_bytes,
                error_message,
                warnings_json,
                COALESCE(details_json, '{{}}'),
                NULL
            FROM {node_runs_name}
            """
        )
        conn.execute(f"DROP TABLE {node_runs_name}")
        conn.execute(f"ALTER TABLE {temp_name} RENAME TO {_quote_identifier('node_runs')}")
    node_states_sql = str(_table_create_sql(conn, schema_name="_queron_meta", table_name="node_states") or "").lower()
    if "cleared" not in node_states_sql:
        temp_name = f"{_quote_identifier('_queron_meta')}.{_quote_identifier('node_states__v2')}"
        conn.execute(f"DROP TABLE IF EXISTS {temp_name}")
        conn.execute(_node_states_create_sql(table_name=temp_name))
        conn.execute(
            f"""
            INSERT INTO {temp_name} (
                node_state_id,
                run_id,
                node_run_id,
                node_name,
                state,
                is_active,
                created_at,
                trigger,
                details_json
            )
            SELECT
                node_state_id,
                run_id,
                node_run_id,
                node_name,
                LOWER(state),
                is_active,
                created_at,
                trigger,
                details_json
            FROM {node_states_name}
            """
        )
        conn.execute(f"DROP TABLE {node_states_name}")
        conn.execute(f"ALTER TABLE {temp_name} RENAME TO {_quote_identifier('node_states')}")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {meta_schema}.{_quote_identifier('column_mapping')} (
            target_schema VARCHAR,
            target_table VARCHAR,
            ordinal_position INTEGER,
            source_column VARCHAR,
            source_type VARCHAR,
            target_column VARCHAR,
            target_type VARCHAR,
            connector_type VARCHAR,
            mapping_mode VARCHAR,
            warnings_json VARCHAR,
            lossy BOOLEAN,
            created_at DOUBLE
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {meta_schema}.{_quote_identifier('table_lineage')} (
            child_schema VARCHAR,
            child_table VARCHAR,
            parent_kind VARCHAR,
            parent_name VARCHAR,
            parent_database VARCHAR,
            parent_schema VARCHAR,
            parent_table VARCHAR,
            connector_type VARCHAR,
            via_node VARCHAR,
            created_at DOUBLE
        )
        """
    )


def _normalize_mapping_record(mapping: ColumnMappingRecord | dict[str, Any]) -> ColumnMappingRecord:
    if isinstance(mapping, ColumnMappingRecord):
        return mapping
    return ColumnMappingRecord.model_validate(mapping)


def _normalize_lineage_record(lineage: TableLineageRecord | dict[str, Any]) -> TableLineageRecord:
    if isinstance(lineage, TableLineageRecord):
        return lineage
    return TableLineageRecord.model_validate(lineage)


def _normalize_pipeline_run_record(record: PipelineRunRecord | dict[str, Any]) -> PipelineRunRecord:
    if isinstance(record, PipelineRunRecord):
        return record
    return PipelineRunRecord.model_validate(record)


def _normalize_compiled_contract_record(
    record: CompiledContractRecord | dict[str, Any]
) -> CompiledContractRecord:
    if isinstance(record, CompiledContractRecord):
        return record
    return CompiledContractRecord.model_validate(record)


def _normalize_node_run_record(record: NodeRunRecord | dict[str, Any]) -> NodeRunRecord:
    if isinstance(record, NodeRunRecord):
        return record
    return NodeRunRecord.model_validate(record)


def _normalize_node_state_record(record: NodeStateRecord | dict[str, Any]) -> NodeStateRecord:
    if isinstance(record, NodeStateRecord):
        return record
    return NodeStateRecord.model_validate(record)


def _serialize_warning_events_json(
    warnings: list[NodeWarningEvent | dict[str, Any] | str] | None,
) -> str:
    normalized = normalize_warning_events(warnings or [])
    return json.dumps([item.model_dump() for item in normalized])


def _serialize_details_json(details: dict[str, Any] | None) -> str:
    return json.dumps(details if isinstance(details, dict) else {})


def _persist_pipeline_run(conn, *, record: PipelineRunRecord | dict[str, Any]) -> None:
    item = _normalize_pipeline_run_record(record)
    _ensure_queron_meta_tables(conn)
    meta_table = f"{_quote_identifier('_queron_meta')}.{_quote_identifier('pipeline_runs')}"
    values = (
        item.run_id,
        item.run_label,
        item.log_path,
        item.compile_id,
        item.pipeline_id,
        item.target,
        item.artifact_path,
        item.started_at,
        item.finished_at,
        item.status,
        item.error_message,
    )
    try:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {meta_table} (
                run_id,
                run_label,
                log_path,
                compile_id,
                pipeline_id,
                target,
                artifact_path,
                started_at,
                finished_at,
                status,
                error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )
    except Exception:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {meta_table} (
                run_id,
                run_label,
                log_path,
                compile_id,
                notebook_id,
                target,
                artifact_path,
                started_at,
                finished_at,
                status,
                error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )


def _persist_compiled_contract(
    conn,
    *,
    record: CompiledContractRecord | dict[str, Any],
) -> CompiledContractRecord:
    item = _normalize_compiled_contract_record(record)
    _ensure_queron_meta_tables(conn)
    meta_table = f"{_quote_identifier('_queron_meta')}.{_quote_identifier('compiled_contracts')}"
    compile_id = str(item.compile_id or uuid.uuid4().hex)
    compiled_at = str(item.compiled_at or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    conn.execute(f"UPDATE {meta_table} SET is_active = FALSE")
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {meta_table} (
            compile_id,
            pipeline_id,
            pipeline_path,
            project_root,
            artifact_path,
            config_path,
            target,
            compiled_at,
            is_active,
            contract_hash,
            edge_hash,
            config_hash,
            project_python_hash,
            node_hashes_json,
            edges_json,
            tracked_files_json,
            external_dependencies_json,
            spec_json,
            diagnostics_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            compile_id,
            item.pipeline_id,
            item.pipeline_path,
            item.project_root,
            item.artifact_path,
            item.config_path,
            item.target,
            compiled_at,
            True,
            item.contract_hash,
            item.edge_hash,
            item.config_hash,
            item.project_python_hash,
            json.dumps(item.node_hashes_json),
            json.dumps(item.edges_json),
            json.dumps(item.tracked_files_json),
            json.dumps(item.external_dependencies_json),
            json.dumps(item.spec_json),
            json.dumps(item.diagnostics_json),
        ),
    )
    return item.model_copy(update={"compile_id": compile_id, "compiled_at": compiled_at, "is_active": True})


def _persist_node_runs(conn, *, records: list[NodeRunRecord | dict[str, Any]]) -> None:
    if not records:
        return
    normalized_by_key: dict[str, NodeRunRecord] = {}
    for item in records:
        normalized_item = _normalize_node_run_record(item)
        normalized_by_key[normalized_item.node_run_id] = normalized_item
    normalized = list(normalized_by_key.values())
    _ensure_queron_meta_tables(conn)
    meta_table = f"{_quote_identifier('_queron_meta')}.{_quote_identifier('node_runs')}"
    rows = [
        (
            item.node_run_id,
            item.run_id,
            item.node_name,
            item.node_kind,
            item.artifact_name,
            item.started_at,
            item.finished_at,
            item.status,
            item.row_count_in,
            item.row_count_out,
            item.artifact_size_bytes,
            item.error_message,
            _serialize_warning_events_json(item.warnings_json),
            _serialize_details_json(item.details_json),
            item.active_node_state_id,
        )
        for item in normalized
    ]
    conn.executemany(
        f"""
        INSERT OR REPLACE INTO {meta_table} (
            node_run_id,
            run_id,
            node_name,
            node_kind,
            artifact_name,
            started_at,
            finished_at,
            status,
            row_count_in,
            row_count_out,
            artifact_size_bytes,
            error_message,
            warnings_json,
            details_json,
            active_node_state_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def _persist_node_states(conn, *, records: list[NodeStateRecord | dict[str, Any]]) -> None:
    if not records:
        return
    normalized = [_normalize_node_state_record(item) for item in records]
    _ensure_queron_meta_tables(conn)
    meta_table = f"{_quote_identifier('_queron_meta')}.{_quote_identifier('node_states')}"
    for item in normalized:
        if item.is_active:
            conn.execute(
                f"""
                UPDATE {meta_table}
                SET is_active = FALSE
                WHERE run_id = ? AND node_run_id = ? AND is_active = TRUE
                """,
                (item.run_id, item.node_run_id),
            )
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {meta_table} (
                node_state_id,
                run_id,
                node_run_id,
                node_name,
                state,
                is_active,
                created_at,
                trigger,
                details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.node_state_id,
                item.run_id,
                item.node_run_id,
                item.node_name,
                item.state,
                bool(item.is_active),
                item.created_at,
                item.trigger,
                _serialize_details_json(item.details_json),
            ),
        )


def _persist_column_mappings(conn, *, target_table: str, column_mappings: list[ColumnMappingRecord | dict[str, Any]]) -> None:
    if not column_mappings:
        return

    target_schema, target_name = _split_target_table_name(target_table)
    _ensure_queron_meta_tables(conn)
    meta_table = f"{_quote_identifier('_queron_meta')}.{_quote_identifier('column_mapping')}"
    conn.execute(
        f"DELETE FROM {meta_table} WHERE target_schema = ? AND target_table = ?",
        (target_schema, target_name),
    )

    rows = []
    now = time.time()
    for mapping in column_mappings:
        item = _normalize_mapping_record(mapping)
        rows.append(
            (
                target_schema,
                target_name,
                item.ordinal_position,
                item.source_column,
                item.source_type,
                item.target_column,
                item.target_type,
                item.connector_type,
                item.mapping_mode,
                json.dumps(item.warnings or []),
                item.lossy,
                now,
            )
        )
    conn.executemany(
        f"""
        INSERT INTO {meta_table} (
            target_schema,
            target_table,
            ordinal_position,
            source_column,
            source_type,
            target_column,
            target_type,
            connector_type,
            mapping_mode,
            warnings_json,
            lossy,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def _persist_table_lineage(conn, *, target_table: str, lineage: list[TableLineageRecord | dict[str, Any]]) -> None:
    target_schema, target_name = _split_target_table_name(target_table)
    _ensure_queron_meta_tables(conn)
    meta_table = f"{_quote_identifier('_queron_meta')}.{_quote_identifier('table_lineage')}"
    conn.execute(
        f"DELETE FROM {meta_table} WHERE child_schema = ? AND child_table = ?",
        (target_schema, target_name),
    )

    if not lineage:
        return

    rows = []
    now = time.time()
    for item in lineage:
        record = _normalize_lineage_record(item)
        rows.append(
            (
                target_schema,
                target_name,
                record.parent_kind,
                record.parent_name,
                record.parent_database,
                record.parent_schema,
                record.parent_table,
                record.connector_type,
                record.via_node,
                now,
            )
        )
    conn.executemany(
        f"""
        INSERT INTO {meta_table} (
            child_schema,
            child_table,
            parent_kind,
            parent_name,
            parent_database,
            parent_schema,
            parent_table,
            connector_type,
            via_node,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def record_ingest_column_mappings(
    *,
    connection_id: str,
    target_table: str,
    column_mappings: list[ColumnMappingRecord | dict[str, Any]],
) -> None:
    if not column_mappings:
        return

    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("DuckDB connection not found. Please connect first.")

    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    conn = _duckdb_connection(database)
    try:
        _persist_column_mappings(conn, target_table=target_table, column_mappings=column_mappings)
    finally:
        conn.close()


def record_table_lineage(
    *,
    connection_id: str,
    target_table: str,
    lineage: list[TableLineageRecord | dict[str, Any]],
) -> None:
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("DuckDB connection not found. Please connect first.")

    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    conn = _duckdb_connection(database)
    try:
        _persist_table_lineage(conn, target_table=target_table, lineage=lineage)
    finally:
        conn.close()


def record_pipeline_run(
    *,
    connection_id: str,
    record: PipelineRunRecord | dict[str, Any],
) -> None:
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("DuckDB connection not found. Please connect first.")

    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    conn = _duckdb_connection(database)
    try:
        _persist_pipeline_run(conn, record=record)
    finally:
        conn.close()


def record_node_runs(
    *,
    connection_id: str,
    records: list[NodeRunRecord | dict[str, Any]],
) -> None:
    if not records:
        return

    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("DuckDB connection not found. Please connect first.")

    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    conn = _duckdb_connection(database)
    try:
        _persist_node_runs(conn, records=records)
    finally:
        conn.close()


def record_node_states(
    *,
    connection_id: str,
    records: list[NodeStateRecord | dict[str, Any]],
) -> None:
    if not records:
        return

    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("DuckDB connection not found. Please connect first.")

    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    conn = _duckdb_connection(database)
    try:
        _persist_node_states(conn, records=records)
    finally:
        conn.close()


def save_compiled_contract(
    *,
    database_path: str,
    record: CompiledContractRecord | dict[str, Any],
) -> CompiledContractRecord:
    conn = _duckdb_connection(str(Path(database_path).expanduser().resolve()))
    try:
        return _persist_compiled_contract(conn, record=record)
    finally:
        conn.close()


def load_active_compiled_contract(
    *,
    database_path: str,
) -> CompiledContractRecord | None:
    resolved_database_path = str(Path(database_path).expanduser().resolve())
    conn = _duckdb_connection(resolved_database_path)
    try:
        _ensure_queron_meta_tables(conn)
        row = conn.execute(
            f"""
            SELECT
                compile_id,
                pipeline_id,
                pipeline_path,
                project_root,
                artifact_path,
                config_path,
                target,
                compiled_at,
                is_active,
                contract_hash,
                edge_hash,
                config_hash,
                project_python_hash,
                node_hashes_json,
                edges_json,
                tracked_files_json,
                external_dependencies_json,
                spec_json,
                diagnostics_json
            FROM {_quote_identifier('_queron_meta')}.{_quote_identifier('compiled_contracts')}
            WHERE is_active = TRUE
            ORDER BY compiled_at DESC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None
    finally:
        conn.close()

    def _load_json_list(value: Any) -> list[Any]:
        try:
            parsed = json.loads(str(value or "[]"))
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []

    def _load_json_dict(value: Any) -> dict[str, Any]:
        try:
            parsed = json.loads(str(value or "{}"))
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    return CompiledContractRecord(
        compile_id=row[0],
        pipeline_id=row[1],
        pipeline_path=row[2],
        project_root=row[3],
        artifact_path=row[4],
        config_path=row[5],
        target=row[6],
        compiled_at=row[7],
        is_active=bool(row[8]),
        contract_hash=row[9],
        edge_hash=row[10],
        config_hash=row[11],
        project_python_hash=row[12],
        node_hashes_json=_load_json_list(row[13]),
        edges_json=_load_json_list(row[14]),
        tracked_files_json=_load_json_list(row[15]),
        external_dependencies_json=_load_json_list(row[16]),
        spec_json=_load_json_dict(row[17]),
        diagnostics_json=_load_json_list(row[18]),
    )


def load_compiled_contract_by_id(
    *,
    database_path: str,
    compile_id: str,
) -> CompiledContractRecord | None:
    resolved_compile_id = str(compile_id or "").strip()
    if not resolved_compile_id:
        return None

    resolved_database_path = str(Path(database_path).expanduser().resolve())
    conn = _duckdb_connection(resolved_database_path)
    try:
        _ensure_queron_meta_tables(conn)
        row = conn.execute(
            f"""
            SELECT
                compile_id,
                pipeline_id,
                pipeline_path,
                project_root,
                artifact_path,
                config_path,
                target,
                compiled_at,
                is_active,
                contract_hash,
                edge_hash,
                config_hash,
                project_python_hash,
                node_hashes_json,
                edges_json,
                tracked_files_json,
                external_dependencies_json,
                spec_json,
                diagnostics_json
            FROM {_quote_identifier('_queron_meta')}.{_quote_identifier('compiled_contracts')}
            WHERE compile_id = ?
            LIMIT 1
            """,
            (resolved_compile_id,),
        ).fetchone()
        if row is None:
            return None
    finally:
        conn.close()

    def _load_json_list(value: Any) -> list[Any]:
        try:
            parsed = json.loads(str(value or "[]"))
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []

    def _load_json_dict(value: Any) -> dict[str, Any]:
        try:
            parsed = json.loads(str(value or "{}"))
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    return CompiledContractRecord(
        compile_id=row[0],
        pipeline_id=row[1],
        pipeline_path=row[2],
        project_root=row[3],
        artifact_path=row[4],
        config_path=row[5],
        target=row[6],
        compiled_at=row[7],
        is_active=bool(row[8]),
        contract_hash=row[9],
        edge_hash=row[10],
        config_hash=row[11],
        project_python_hash=row[12],
        node_hashes_json=_load_json_list(row[13]),
        edges_json=_load_json_list(row[14]),
        tracked_files_json=_load_json_list(row[15]),
        external_dependencies_json=_load_json_list(row[16]),
        spec_json=_load_json_dict(row[17]),
        diagnostics_json=_load_json_list(row[18]),
    )


def _load_database_block_size(conn) -> int | None:
    try:
        row = conn.execute("SELECT block_size FROM pragma_database_size()").fetchone()
    except Exception:
        return None
    if not row:
        return None
    try:
        return int(row[0])
    except Exception:
        return None


def _table_exists(conn, *, schema: str, name: str) -> bool:
    try:
        row = conn.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            LIMIT 1
            """,
            (schema, name),
        ).fetchone()
    except Exception:
        return False
    return bool(row)


def _load_table_row_count(conn, *, target_table: str) -> int | None:
    schema, name = _split_target_table_name(target_table)
    if not _table_exists(conn, schema=schema, name=name):
        return None
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {_qualified_name(schema, name)}").fetchone()
    except Exception:
        return None
    if not row:
        return None
    try:
        return int(row[0])
    except Exception:
        return None


def _load_table_artifact_size_bytes(conn, *, target_table: str) -> int | None:
    schema, name = _split_target_table_name(target_table)
    if not _table_exists(conn, schema=schema, name=name):
        return None
    try:
        conn.execute("CHECKPOINT")
    except Exception:
        pass
    block_size = _load_database_block_size(conn)
    if block_size is None or block_size <= 0:
        return None
    try:
        rows = conn.execute(f"PRAGMA storage_info('{schema}.{name}')").fetchall() or []
    except Exception:
        return None

    block_ids: set[int] = set()
    for row in rows:
        try:
            block_id = row[12]
        except Exception:
            block_id = None
        if isinstance(block_id, int) and block_id >= 0:
            block_ids.add(block_id)
        try:
            additional = row[15]
        except Exception:
            additional = None
        if isinstance(additional, list):
            for extra in additional:
                if isinstance(extra, int) and extra >= 0:
                    block_ids.add(extra)

    if not block_ids:
        export_path = Path(tempfile.gettempdir()) / f"queron_table_size_{uuid.uuid4().hex}.parquet"
        try:
            conn.execute(
                f"COPY {_qualified_name(schema, name)} TO ? (FORMAT PARQUET)",
                (str(export_path),),
            )
            if export_path.exists():
                return int(os.path.getsize(export_path))
        except Exception:
            return None
        finally:
            try:
                export_path.unlink(missing_ok=True)
            except Exception:
                pass
    return len(block_ids) * block_size


def get_table_row_count(
    *,
    connection_id: str,
    target_table: str,
) -> int | None:
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("DuckDB connection not found. Please connect first.")

    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    conn = _duckdb_connection(database)
    try:
        return _load_table_row_count(conn, target_table=target_table)
    finally:
        conn.close()


def get_table_artifact_size_bytes(
    *,
    connection_id: str,
    target_table: str,
) -> int | None:
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("DuckDB connection not found. Please connect first.")

    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    conn = _duckdb_connection(database)
    try:
        return _load_table_artifact_size_bytes(conn, target_table=target_table)
    finally:
        conn.close()


def materialize_egress_artifact(
    *,
    database: str,
    sql: str,
    target_table: str,
    replace: bool = True,
) -> int:
    normalized_target = str(target_table or "").strip()
    if not normalized_target:
        raise RuntimeError("Egress artifact target table is required.")
    conn = _duckdb_connection(str(database))
    try:
        schema, _table = _split_target_table_name(normalized_target)
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(schema)}")
        target_ident = _quote_compound_identifier(normalized_target)
        if replace:
            conn.execute(f"DROP TABLE IF EXISTS {target_ident}")
        conn.execute(f"CREATE TABLE {target_ident} AS {_strip_sql_terminator(sql)}")
        row = conn.execute(f"SELECT COUNT(*) FROM {target_ident}").fetchone()
    finally:
        conn.close()
    return int(row[0]) if row and row[0] is not None else 0


def export_query_to_parquet_with_artifact(
    *,
    database: str,
    sql: str,
    output_path: str,
    target_table: str,
    overwrite: bool = False,
    compression: str | None = None,
    working_dir: str | None = None,
) -> DuckDbExportResponse:
    materialize_egress_artifact(
        database=database,
        sql=sql,
        target_table=target_table,
        replace=True,
    )
    target_ident = _quote_compound_identifier(target_table)
    return export_query_to_parquet(
        database=database,
        sql=f"SELECT * FROM {target_ident}",
        output_path=output_path,
        overwrite=overwrite,
        compression=compression,
        working_dir=working_dir,
    )


def export_query_to_csv_with_artifact(
    *,
    database: str,
    sql: str,
    output_path: str,
    target_table: str,
    overwrite: bool = False,
    header: bool = True,
    delimiter: str = ",",
    working_dir: str | None = None,
) -> DuckDbExportResponse:
    materialize_egress_artifact(
        database=database,
        sql=sql,
        target_table=target_table,
        replace=True,
    )
    target_ident = _quote_compound_identifier(target_table)
    return export_query_to_csv(
        database=database,
        sql=f"SELECT * FROM {target_ident}",
        output_path=output_path,
        overwrite=overwrite,
        header=header,
        delimiter=delimiter,
        working_dir=working_dir,
    )


def export_query_to_jsonl_with_artifact(
    *,
    database: str,
    sql: str,
    output_path: str,
    target_table: str,
    overwrite: bool = False,
    working_dir: str | None = None,
) -> DuckDbExportResponse:
    materialize_egress_artifact(
        database=database,
        sql=sql,
        target_table=target_table,
        replace=True,
    )
    target_ident = _quote_compound_identifier(target_table)
    return export_query_to_jsonl(
        database=database,
        sql=f"SELECT * FROM {target_ident}",
        output_path=output_path,
        overwrite=overwrite,
        working_dir=working_dir,
    )


def export_query_to_parquet(
    *,
    database: str,
    sql: str,
    output_path: str,
    overwrite: bool = False,
    compression: str | None = None,
    working_dir: str | None = None,
) -> DuckDbExportResponse:
    conn = _duckdb_connection(str(database))
    resolved_output_path = _resolve_export_path(output_path, working_dir=working_dir)
    try:
        if resolved_output_path.exists():
            if not overwrite:
                raise RuntimeError(f"Export path '{resolved_output_path}' already exists.")
            resolved_output_path.unlink()
        row_count = _load_query_row_count(conn, sql=sql)
        options = ["FORMAT PARQUET"]
        if compression is not None:
            options.append(f"COMPRESSION '{_sanitize_copy_option_token(compression, label='Parquet compression')}'")
        conn.execute(
            f"COPY ({_strip_sql_terminator(sql)}) TO ? ({', '.join(options)})",
            (str(resolved_output_path),),
        )
    finally:
        conn.close()
    return DuckDbExportResponse(
        output_path=str(resolved_output_path),
        row_count=row_count,
        file_size_bytes=int(os.path.getsize(resolved_output_path)) if resolved_output_path.exists() else None,
        export_format="parquet",
        created_at=time.time(),
    )


def export_query_to_csv(
    *,
    database: str,
    sql: str,
    output_path: str,
    overwrite: bool = False,
    header: bool = True,
    delimiter: str = ",",
    working_dir: str | None = None,
) -> DuckDbExportResponse:
    delim = str(delimiter or "")
    if len(delim) != 1:
        raise RuntimeError("CSV delimiter must be exactly one character.")
    conn = _duckdb_connection(str(database))
    resolved_output_path = _resolve_export_path(output_path, working_dir=working_dir)
    try:
        if resolved_output_path.exists():
            if not overwrite:
                raise RuntimeError(f"Export path '{resolved_output_path}' already exists.")
            resolved_output_path.unlink()
        row_count = _load_query_row_count(conn, sql=sql)
        escaped_delim = delim.replace("'", "''")
        conn.execute(
            (
                f"COPY ({_strip_sql_terminator(sql)}) TO ? "
                f"(FORMAT CSV, HEADER {'TRUE' if header else 'FALSE'}, DELIMITER '{escaped_delim}')"
            ),
            (str(resolved_output_path),),
        )
    finally:
        conn.close()
    return DuckDbExportResponse(
        output_path=str(resolved_output_path),
        row_count=row_count,
        file_size_bytes=int(os.path.getsize(resolved_output_path)) if resolved_output_path.exists() else None,
        export_format="csv",
        created_at=time.time(),
    )


def export_query_to_jsonl(
    *,
    database: str,
    sql: str,
    output_path: str,
    overwrite: bool = False,
    working_dir: str | None = None,
) -> DuckDbExportResponse:
    conn = _duckdb_connection(str(database))
    resolved_output_path = _resolve_export_path(output_path, working_dir=working_dir)
    try:
        if resolved_output_path.exists():
            if not overwrite:
                raise RuntimeError(f"Export path '{resolved_output_path}' already exists.")
            resolved_output_path.unlink()
        row_count = _load_query_row_count(conn, sql=sql)
        conn.execute(
            f"COPY ({_strip_sql_terminator(sql)}) TO ? (FORMAT JSON, ARRAY false)",
            (str(resolved_output_path),),
        )
    finally:
        conn.close()
    return DuckDbExportResponse(
        output_path=str(resolved_output_path),
        row_count=row_count,
        file_size_bytes=int(os.path.getsize(resolved_output_path)) if resolved_output_path.exists() else None,
        export_format="jsonl",
        created_at=time.time(),
    )


def _normalize_file_format(value: str | None, *, path: str | None = None) -> str:
    text = str(value or "").strip().lower()
    if text in {"jsonl", "ndjson"}:
        return "jsonl"
    if text in {"csv", "parquet"}:
        return text
    suffix = str(path or "").strip().lower()
    if suffix.endswith(".jsonl") or suffix.endswith(".ndjson"):
        return "jsonl"
    if suffix.endswith(".csv"):
        return "csv"
    if suffix.endswith(".parquet"):
        return "parquet"
    raise RuntimeError(f"Unsupported file format '{value or path or ''}'.")


def _sanitize_sql_string_literal(value: str, *, label: str) -> str:
    text = str(value or "")
    if "'" in text:
        text = text.replace("'", "''")
    return text


def _sanitize_duckdb_type_literal(value: str) -> str:
    text = str(value or "").strip().upper()
    if not text:
        raise RuntimeError("File ingress column types must be non-empty.")
    if not re.fullmatch(r"[A-Z0-9_(), ]+", text):
        raise RuntimeError(f"Unsupported DuckDB type literal '{value}'.")
    return text


def _csv_columns_sql_literal(columns: dict[str, str]) -> str:
    items: list[str] = []
    for raw_name, raw_type in columns.items():
        name = str(raw_name or "").strip()
        if not name:
            raise RuntimeError("CSV file ingress columns must have non-empty names.")
        escaped_name = _sanitize_sql_string_literal(name, label="CSV column name")
        escaped_type = _sanitize_sql_string_literal(_sanitize_duckdb_type_literal(str(raw_type or "")), label="CSV column type")
        items.append(f"'{escaped_name}': '{escaped_type}'")
    if not items:
        raise RuntimeError("CSV file ingress columns must not be empty.")
    return "{" + ", ".join(items) + "}"


def _normalize_sniff_string(value: Any) -> str:
    text = str(value or "")
    return "" if text == "(empty)" else text


def _inspect_csv_source(conn, *, path: str) -> dict[str, Any]:
    cur = conn.execute("SELECT * FROM sniff_csv(?)", [str(path)])
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("Unable to inspect CSV file.")
    columns = [desc[0] for desc in cur.description or []]
    payload = {columns[idx]: row[idx] for idx in range(min(len(columns), len(row)))}
    raw_columns = payload.get("Columns") or []
    if not isinstance(raw_columns, list) or len(raw_columns) == 0:
        raise RuntimeError("No columns detected in CSV file.")
    detected_columns: list[dict[str, str]] = []
    for raw_column in raw_columns:
        if not isinstance(raw_column, dict):
            continue
        name = str(raw_column.get("name") or "").strip()
        type_name = str(raw_column.get("type") or "VARCHAR").strip().upper() or "VARCHAR"
        if not name:
            continue
        detected_columns.append({"name": name, "type": type_name})
    if len(detected_columns) == 0:
        raise RuntimeError("No columns detected in CSV file.")
    return {
        "format": "csv",
        "columns": detected_columns,
        "csv_options": {
            "header": bool(payload.get("HasHeader")),
            "delimiter": _normalize_sniff_string(payload.get("Delimiter")) or ",",
            "quote": _normalize_sniff_string(payload.get("Quote")),
            "escape": _normalize_sniff_string(payload.get("Escape")),
            "skip_rows": max(0, int(payload.get("SkipRows") or 0)),
        },
    }


def _inspect_parquet_source(conn, *, path: str) -> dict[str, Any]:
    rows = conn.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
    detected_columns: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        name = str(row[0] or "").strip()
        type_name = str(row[1] or "VARCHAR").strip().upper() or "VARCHAR"
        if not name:
            continue
        detected_columns.append({"name": name, "type": type_name})
    if len(detected_columns) == 0:
        raise RuntimeError("No columns detected in Parquet file.")
    return {"format": "parquet", "columns": detected_columns}


def _inspect_jsonl_source(conn, *, path: str) -> dict[str, Any]:
    rows = conn.execute("DESCRIBE SELECT * FROM read_ndjson(?)", [str(path)]).fetchall()
    detected_columns: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        name = str(row[0] or "").strip()
        type_name = str(row[1] or "VARCHAR").strip().upper() or "VARCHAR"
        if not name:
            continue
        detected_columns.append({"name": name, "type": type_name})
    if len(detected_columns) == 0:
        raise RuntimeError("No columns detected in JSONL file.")
    return {"format": "jsonl", "columns": detected_columns}


def inspect_file_source(
    *,
    path: str,
    file_format: str | None = None,
) -> dict[str, Any]:
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists() or not resolved_path.is_file():
        raise RuntimeError(f"File '{resolved_path}' was not found.")
    normalized_format = _normalize_file_format(file_format, path=str(resolved_path))
    conn = _duckdb_connection(":memory:")
    try:
        if normalized_format == "csv":
            payload = _inspect_csv_source(conn, path=str(resolved_path))
        elif normalized_format == "parquet":
            payload = _inspect_parquet_source(conn, path=str(resolved_path))
        else:
            payload = _inspect_jsonl_source(conn, path=str(resolved_path))
        payload["path"] = str(resolved_path)
        payload["format"] = normalized_format
        return payload
    finally:
        conn.close()


def ingest_file_to_duckdb(
    *,
    database: str,
    input_path: str,
    target_table: str,
    file_format: str | None = None,
    header: bool = True,
    delimiter: str = ",",
    quote: str | None = None,
    escape: str | None = None,
    skip_rows: int = 0,
    columns: dict[str, str] | None = None,
    replace: bool = False,
) -> dict[str, Any]:
    resolved_path = Path(input_path).expanduser().resolve()
    if not resolved_path.exists() or not resolved_path.is_file():
        raise RuntimeError(f"File '{resolved_path}' was not found.")
    normalized_format = _normalize_file_format(file_format, path=str(resolved_path))
    if normalized_format == "csv":
        delim = str(delimiter or "")
        if len(delim) != 1:
            raise RuntimeError("CSV delimiter must be exactly one character.")
        if quote is not None and len(str(quote)) != 1:
            raise RuntimeError("CSV quote must be exactly one character.")
        if escape is not None and len(str(escape)) != 1:
            raise RuntimeError("CSV escape must be exactly one character.")
        if not header and not columns:
            raise RuntimeError("CSV file ingress requires columns when header=False.")
        if columns:
            columns = {str(key): str(value) for key, value in dict(columns).items()}

    conn = _duckdb_connection(str(database))
    target_ident = _quote_compound_identifier(target_table)
    try:
        if normalized_format == "csv" and header and columns:
            inspected = _inspect_csv_source(conn, path=str(resolved_path))
            actual_names = [str(item.get("name") or "") for item in inspected.get("columns", []) if isinstance(item, dict)]
            expected_names = list(columns.keys())
            if actual_names != expected_names:
                raise RuntimeError(
                    "CSV header names do not match the declared columns exactly. "
                    f"Expected {expected_names}, found {actual_names}."
                )

        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(_split_target_table_name(target_table)[0])}")
        create_mode = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE"
        if normalized_format == "csv":
            options = [
                f"header = {'true' if bool(header) else 'false'}",
                f"delim = '{_sanitize_sql_string_literal(str(delimiter or ','), label='CSV delimiter')}'",
            ]
            if quote is not None:
                options.append(f"quote = '{_sanitize_sql_string_literal(str(quote), label='CSV quote')}'")
            if escape is not None:
                options.append(f"escape = '{_sanitize_sql_string_literal(str(escape), label='CSV escape')}'")
            if int(skip_rows or 0) > 0:
                options.append(f"skip = {max(0, int(skip_rows or 0))}")
            if columns:
                options.append(f"columns = {_csv_columns_sql_literal(columns)}")
            read_sql = f"SELECT * FROM read_csv(?, {', '.join(options)})"
        elif normalized_format == "parquet":
            read_sql = "SELECT * FROM read_parquet(?)"
        else:
            read_sql = "SELECT * FROM read_ndjson(?)"

        conn.execute(
            f"{create_mode} {target_ident} AS {read_sql}",
            [str(resolved_path)],
        )
        row = conn.execute(f"SELECT COUNT(*) FROM {target_ident}").fetchone()
        described = conn.execute(f"DESCRIBE {target_ident}").fetchall()
    finally:
        conn.close()

    row_count = int(row[0]) if row and row[0] is not None else 0
    output_columns = [
        {
            "name": str(item[0] or "").strip(),
            "type": str(item[1] or "").strip().upper() or "VARCHAR",
        }
        for item in described
        if isinstance(item, (list, tuple)) and len(item) >= 2 and str(item[0] or "").strip()
    ]
    return {
        "path": str(resolved_path),
        "format": normalized_format,
        "row_count": row_count,
        "columns": output_columns,
    }


def get_latest_pipeline_run(
    *,
    connection_id: str,
    status: str | None = None,
) -> dict[str, Any] | None:
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("DuckDB connection not found. Please connect first.")

    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    conn = _duckdb_connection(database)
    try:
        _ensure_queron_meta_tables(conn)
        where_sql = ""
        params: tuple[Any, ...] = ()
        if status:
            where_sql = "WHERE status = ?"
            params = (status,)
        try:
            row = conn.execute(
                f"""
                SELECT
                    run_id,
                    run_label,
                    log_path,
                    compile_id,
                    pipeline_id,
                    target,
                    artifact_path,
                    started_at,
                    finished_at,
                    status,
                    error_message
                FROM {_quote_identifier('_queron_meta')}.{_quote_identifier('pipeline_runs')}
                {where_sql}
                ORDER BY COALESCE(finished_at, started_at) DESC
                LIMIT 1
                """,
                params,
            ).fetchone()
        except Exception:
            return None
        if row is None:
            return None
        return {
            "run_id": row[0],
            "run_label": row[1],
            "log_path": row[2],
            "compile_id": row[3],
            "pipeline_id": row[4],
            "target": row[5],
            "artifact_path": row[6],
            "started_at": row[7],
            "finished_at": row[8],
            "status": row[9],
            "error_message": row[10],
        }
    finally:
        conn.close()


def get_pipeline_run_by_label(
    *,
    database_path: str,
    run_label: str,
) -> dict[str, Any] | None:
    label = str(run_label or "").strip()
    if not label:
        return None

    resolved_database_path = str(Path(database_path).expanduser().resolve())
    conn = _duckdb_connection(resolved_database_path)
    try:
        _ensure_queron_meta_tables(conn)
        row = conn.execute(
            f"""
            SELECT
                run_id,
                run_label,
                log_path,
                compile_id,
                pipeline_id,
                target,
                artifact_path,
                started_at,
                finished_at,
                status,
                error_message
            FROM {_quote_identifier('_queron_meta')}.{_quote_identifier('pipeline_runs')}
            WHERE run_label = ?
            LIMIT 1
            """,
            (label,),
        ).fetchone()
        if row is None:
            return None
        return {
            "run_id": row[0],
            "run_label": row[1],
            "log_path": row[2],
            "compile_id": row[3],
            "pipeline_id": row[4],
            "target": row[5],
            "artifact_path": row[6],
            "started_at": row[7],
            "finished_at": row[8],
            "status": row[9],
            "error_message": row[10],
        }
    finally:
        conn.close()


def get_pipeline_run_by_id(
    *,
    database_path: str,
    run_id: str,
) -> dict[str, Any] | None:
    resolved_run_id = str(run_id or "").strip()
    if not resolved_run_id:
        return None

    resolved_database_path = str(Path(database_path).expanduser().resolve())
    conn = _duckdb_connection(resolved_database_path)
    try:
        _ensure_queron_meta_tables(conn)
        row = conn.execute(
            f"""
            SELECT
                run_id,
                run_label,
                log_path,
                compile_id,
                pipeline_id,
                target,
                artifact_path,
                started_at,
                finished_at,
                status,
                error_message
            FROM {_quote_identifier('_queron_meta')}.{_quote_identifier('pipeline_runs')}
            WHERE run_id = ?
            LIMIT 1
            """,
            (resolved_run_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "run_id": row[0],
            "run_label": row[1],
            "log_path": row[2],
            "compile_id": row[3],
            "pipeline_id": row[4],
            "target": row[5],
            "artifact_path": row[6],
            "started_at": row[7],
            "finished_at": row[8],
            "status": row[9],
            "error_message": row[10],
        }
    finally:
        conn.close()


def list_pipeline_runs(
    *,
    database_path: str,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    resolved_database_path = str(Path(database_path).expanduser().resolve())
    conn = _duckdb_connection(resolved_database_path)
    try:
        _ensure_queron_meta_tables(conn)
        limit_sql = ""
        params: tuple[Any, ...] = ()
        if limit is not None:
            limit_sql = "LIMIT ?"
            params = (int(limit),)
        rows = conn.execute(
            f"""
            SELECT
                run_id,
                run_label,
                log_path,
                compile_id,
                pipeline_id,
                target,
                artifact_path,
                started_at,
                finished_at,
                status,
                error_message
            FROM {_quote_identifier('_queron_meta')}.{_quote_identifier('pipeline_runs')}
            ORDER BY COALESCE(finished_at, started_at) DESC
            {limit_sql}
            """,
            params,
        ).fetchall()
    finally:
        conn.close()

    return [
        {
            "run_id": row[0],
            "run_label": row[1],
            "log_path": row[2],
            "compile_id": row[3],
            "pipeline_id": row[4],
            "target": row[5],
            "artifact_path": row[6],
            "started_at": row[7],
            "finished_at": row[8],
            "status": row[9],
            "error_message": row[10],
        }
        for row in rows
    ]


def get_node_runs_for_run(
    *,
    connection_id: str,
    run_id: str,
) -> list[dict[str, Any]]:
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("DuckDB connection not found. Please connect first.")

    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    conn = _duckdb_connection(database)
    try:
        try:
            rows = conn.execute(
                f"""
                SELECT
                    node_run_id,
                    run_id,
                    node_name,
                    node_kind,
                    artifact_name,
                    started_at,
                    finished_at,
                    status,
                    row_count_in,
                    row_count_out,
                    artifact_size_bytes,
                    error_message,
                    warnings_json,
                    details_json,
                    active_node_state_id
                FROM {_quote_identifier('_queron_meta')}.{_quote_identifier('node_runs')}
                WHERE run_id = ?
                ORDER BY node_name
                """,
                (run_id,),
            ).fetchall()
        except Exception:
            return []
    finally:
        conn.close()

    out: list[dict[str, Any]] = []
    for row in rows:
        try:
            warnings_json = json.loads(str(row[12] or "[]"))
            if not isinstance(warnings_json, list):
                warnings_json = []
        except Exception:
            warnings_json = []
        try:
            details_json = json.loads(str(row[13] or "{}"))
            if not isinstance(details_json, dict):
                details_json = {}
        except Exception:
            details_json = {}
        out.append(
            {
                "node_run_id": row[0],
                "run_id": row[1],
                "node_name": row[2],
                "node_kind": row[3],
                "artifact_name": row[4],
                "started_at": row[5],
                "finished_at": row[6],
                "status": row[7],
                "row_count_in": row[8],
                "row_count_out": row[9],
                "artifact_size_bytes": row[10],
                "error_message": row[11],
                "warnings_json": warnings_json,
                "details_json": details_json,
                "active_node_state_id": row[14],
            }
        )
    return out


def get_node_runs_for_run_by_database(
    *,
    database_path: str,
    run_id: str,
) -> list[dict[str, Any]]:
    resolved_run_id = str(run_id or "").strip()
    if not resolved_run_id:
        return []

    resolved_database_path = str(Path(database_path).expanduser().resolve())
    conn = _duckdb_connection(resolved_database_path)
    try:
        try:
            rows = conn.execute(
                f"""
                SELECT
                    node_run_id,
                    run_id,
                    node_name,
                    node_kind,
                    artifact_name,
                    started_at,
                    finished_at,
                    status,
                    row_count_in,
                    row_count_out,
                    artifact_size_bytes,
                    error_message,
                    warnings_json,
                    details_json,
                    active_node_state_id
                FROM {_quote_identifier('_queron_meta')}.{_quote_identifier('node_runs')}
                WHERE run_id = ?
                ORDER BY node_name
                """,
                (resolved_run_id,),
            ).fetchall()
        except Exception:
            return []
    finally:
        conn.close()

    out: list[dict[str, Any]] = []
    for row in rows:
        try:
            warnings_json = json.loads(str(row[12] or "[]"))
            if not isinstance(warnings_json, list):
                warnings_json = []
        except Exception:
            warnings_json = []
        try:
            details_json = json.loads(str(row[13] or "{}"))
            if not isinstance(details_json, dict):
                details_json = {}
        except Exception:
            details_json = {}
        out.append(
            {
                "node_run_id": row[0],
                "run_id": row[1],
                "node_name": row[2],
                "node_kind": row[3],
                "artifact_name": row[4],
                "started_at": row[5],
                "finished_at": row[6],
                "status": row[7],
                "row_count_in": row[8],
                "row_count_out": row[9],
                "artifact_size_bytes": row[10],
                "error_message": row[11],
                "warnings_json": warnings_json,
                "details_json": details_json,
                "active_node_state_id": row[14],
            }
        )
    return out


def get_active_node_states_for_run(
    *,
    connection_id: str,
    run_id: str,
) -> list[dict[str, Any]]:
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("DuckDB connection not found. Please connect first.")

    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    conn = _duckdb_connection(database)
    try:
        try:
            rows = conn.execute(
                f"""
                SELECT
                    node_state_id,
                    run_id,
                    node_run_id,
                    node_name,
                    state,
                    is_active,
                    created_at,
                    trigger,
                    details_json
                FROM {_quote_identifier('_queron_meta')}.{_quote_identifier('node_states')}
                WHERE run_id = ? AND is_active = TRUE
                ORDER BY node_name
                """,
                (run_id,),
            ).fetchall()
        except Exception:
            return []
    finally:
        conn.close()

    out: list[dict[str, Any]] = []
    for row in rows:
        try:
            details_json = json.loads(str(row[8] or "{}"))
            if not isinstance(details_json, dict):
                details_json = {}
        except Exception:
            details_json = {}
        out.append(
            {
                "node_state_id": row[0],
                "run_id": row[1],
                "node_run_id": row[2],
                "node_name": row[3],
                "state": row[4],
                "is_active": bool(row[5]),
                "created_at": row[6],
                "trigger": row[7],
                "details_json": details_json,
            }
        )
    return out


def get_active_node_states_for_run_by_database(
    *,
    database_path: str,
    run_id: str,
) -> list[dict[str, Any]]:
    resolved_run_id = str(run_id or "").strip()
    if not resolved_run_id:
        return []

    resolved_database_path = str(Path(database_path).expanduser().resolve())
    conn = _duckdb_connection(resolved_database_path)
    try:
        try:
            rows = conn.execute(
                f"""
                SELECT
                    node_state_id,
                    run_id,
                    node_run_id,
                    node_name,
                    state,
                    is_active,
                    created_at,
                    trigger,
                    details_json
                FROM {_quote_identifier('_queron_meta')}.{_quote_identifier('node_states')}
                WHERE run_id = ? AND is_active = TRUE
                ORDER BY node_name
                """,
                (resolved_run_id,),
            ).fetchall()
        except Exception:
            return []
    finally:
        conn.close()

    out: list[dict[str, Any]] = []
    for row in rows:
        try:
            details_json = json.loads(str(row[8] or "{}"))
            if not isinstance(details_json, dict):
                details_json = {}
        except Exception:
            details_json = {}
        out.append(
            {
                "node_state_id": row[0],
                "run_id": row[1],
                "node_run_id": row[2],
                "node_name": row[3],
                "state": row[4],
                "is_active": bool(row[5]),
                "created_at": row[6],
                "trigger": row[7],
                "details_json": details_json,
            }
        )
    return out


def get_node_states_for_run_by_database(
    *,
    database_path: str,
    run_id: str,
    node_name: str | None = None,
) -> list[dict[str, Any]]:
    resolved_run_id = str(run_id or "").strip()
    if not resolved_run_id:
        return []

    resolved_database_path = str(Path(database_path).expanduser().resolve())
    resolved_node_name = str(node_name or "").strip()
    state_sort_sql = """
        CASE
            WHEN COALESCE(trigger, '') LIKE 'reset_%' AND state = 'cleared' THEN 0
            WHEN COALESCE(trigger, '') LIKE 'reset_%' AND state = 'ready' THEN 1
            WHEN state = 'ready' THEN 0
            WHEN state = 'running' THEN 1
            WHEN state = 'failed' THEN 2
            WHEN state = 'skipped' THEN 3
            WHEN state = 'complete' THEN 4
            WHEN state = 'complete_with_warnings' THEN 4
            WHEN state = 'cleared' THEN 5
            ELSE 9
        END
    """
    conn = _duckdb_connection(resolved_database_path)
    try:
        try:
            if resolved_node_name:
                rows = conn.execute(
                    f"""
                    SELECT
                        node_state_id,
                        run_id,
                        node_run_id,
                        node_name,
                        state,
                        is_active,
                        created_at,
                    trigger,
                    details_json
                    FROM {_quote_identifier('_queron_meta')}.{_quote_identifier('node_states')}
                    WHERE run_id = ? AND node_name = ?
                    ORDER BY created_at, {state_sort_sql}, node_state_id
                    """,
                    (resolved_run_id, resolved_node_name),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT
                        node_state_id,
                        run_id,
                        node_run_id,
                        node_name,
                        state,
                        is_active,
                        created_at,
                    trigger,
                    details_json
                    FROM {_quote_identifier('_queron_meta')}.{_quote_identifier('node_states')}
                    WHERE run_id = ?
                    ORDER BY node_name, created_at, {state_sort_sql}, node_state_id
                    """,
                    (resolved_run_id,),
                ).fetchall()
        except Exception:
            return []
    finally:
        conn.close()

    out: list[dict[str, Any]] = []
    for row in rows:
        try:
            details_json = json.loads(str(row[8] or "{}"))
            if not isinstance(details_json, dict):
                details_json = {}
        except Exception:
            details_json = {}
        out.append(
            {
                "node_state_id": row[0],
                "run_id": row[1],
                "node_run_id": row[2],
                "node_name": row[3],
                "state": row[4],
                "is_active": bool(row[5]),
                "created_at": row[6],
                "trigger": row[7],
                "details_json": details_json,
            }
        )
    return out


def clear_pipeline_targets(
    *,
    connection_id: str,
    target_tables: list[str],
) -> None:
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("DuckDB connection not found. Please connect first.")

    normalized_targets: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for target in target_tables:
        schema, name = _split_target_table_name(target)
        key = (schema, name)
        if key in seen:
            continue
        seen.add(key)
        normalized_targets.append(key)

    if not normalized_targets:
        return

    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    conn = _duckdb_connection(database)
    try:
        _ensure_queron_meta_tables(conn)
        lineage_table = f"{_quote_identifier('_queron_meta')}.{_quote_identifier('table_lineage')}"
        mapping_table = f"{_quote_identifier('_queron_meta')}.{_quote_identifier('column_mapping')}"
        for schema, name in normalized_targets:
            conn.execute(f"DROP TABLE IF EXISTS {_qualified_name(schema, name)}")
            conn.execute(
                f"DELETE FROM {mapping_table} WHERE target_schema = ? AND target_table = ?",
                (schema, name),
            )
            conn.execute(
                (
                    f"DELETE FROM {lineage_table} "
                    "WHERE (child_schema = ? AND child_table = ?) "
                    "OR (parent_kind = 'artifact' AND parent_schema = ? AND parent_table = ?)"
                ),
                (schema, name, schema, name),
            )
    finally:
        conn.close()


def list_existing_pipeline_targets(
    *,
    connection_id: str,
    target_tables: list[str],
) -> list[str]:
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("DuckDB connection not found. Please connect first.")

    normalized_targets: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str]] = set()
    for target in target_tables:
        normalized_target = str(target or "").strip()
        if not normalized_target:
            continue
        schema, name = _split_target_table_name(normalized_target)
        key = (schema, name)
        if key in seen:
            continue
        seen.add(key)
        normalized_targets.append((normalized_target, schema, name))

    if not normalized_targets:
        return []

    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    conn = _duckdb_connection(database)
    try:
        existing_targets: list[str] = []
        for original_target, schema, name in normalized_targets:
            if _table_exists(conn, schema=schema, name=name):
                existing_targets.append(original_target)
        return existing_targets
    finally:
        conn.close()


def _archive_schema_name_for_run(run_id: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_]", "", str(run_id or "").strip())
    if not normalized:
        raise RuntimeError("run_id is required to archive pipeline targets.")
    return f"run_{normalized}"


def archive_pipeline_targets(
    *,
    connection_id: str,
    run_id: str,
    target_tables: list[str],
) -> dict[str, str]:
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("DuckDB connection not found. Please connect first.")

    normalized_targets: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for target in target_tables:
        schema, name = _split_target_table_name(target)
        key = (schema, name)
        if key in seen:
            continue
        seen.add(key)
        normalized_targets.append(key)

    if not normalized_targets:
        return {}

    archive_schema = _archive_schema_name_for_run(run_id)
    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    conn = _duckdb_connection(database)
    archived: dict[str, str] = {}
    try:
        _ensure_queron_meta_tables(conn)
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(archive_schema)}")

        existing_targets: list[tuple[str, str]] = []
        for schema, name in normalized_targets:
            if _table_exists(conn, schema=schema, name=name):
                existing_targets.append((schema, name))

        for source_schema, table_name in existing_targets:
            destination_qualified = _qualified_name(archive_schema, table_name)
            source_qualified = _qualified_name(source_schema, table_name)
            if _table_exists(conn, schema=archive_schema, name=table_name):
                raise RuntimeError(
                    f"Archived artifact table '{archive_schema}.{table_name}' already exists for run '{run_id}'."
                )
            conn.execute(f"CREATE TABLE {destination_qualified} AS SELECT * FROM {source_qualified}")
            conn.execute(f"DROP TABLE {source_qualified}")
            archived[f"{source_schema}.{table_name}"] = f"{archive_schema}.{table_name}"

        if not archived:
            return {}

        mapping_table = f"{_quote_identifier('_queron_meta')}.{_quote_identifier('column_mapping')}"
        lineage_table = f"{_quote_identifier('_queron_meta')}.{_quote_identifier('table_lineage')}"
        node_runs_table = f"{_quote_identifier('_queron_meta')}.{_quote_identifier('node_runs')}"
        for source_qualified, destination_qualified in archived.items():
            source_schema, table_name = _split_target_table_name(source_qualified)
            conn.execute(
                f"""
                UPDATE {mapping_table}
                SET target_schema = ?
                WHERE target_schema = ? AND target_table = ?
                """,
                (archive_schema, source_schema, table_name),
            )
            conn.execute(
                f"""
                UPDATE {lineage_table}
                SET child_schema = ?
                WHERE child_schema = ? AND child_table = ?
                """,
                (archive_schema, source_schema, table_name),
            )
            conn.execute(
                f"""
                UPDATE {lineage_table}
                SET parent_schema = ?
                WHERE parent_kind = 'artifact' AND parent_schema = ? AND parent_table = ?
                """,
                (archive_schema, source_schema, table_name),
            )
            conn.execute(
                f"""
                UPDATE {node_runs_table}
                SET artifact_name = ?
                WHERE run_id = ? AND artifact_name = ?
                """,
                (destination_qualified, str(run_id), source_qualified),
            )
    finally:
        conn.close()

    return archived


def get_table_lineage(
    *,
    connection_id: str,
    schema: str,
    name: str,
) -> list[dict[str, Any]]:
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("DuckDB connection not found. Please connect first.")

    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    conn = _duckdb_connection(database)
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT
                    parent_kind,
                    parent_name,
                    parent_database,
                    parent_schema,
                    parent_table,
                    connector_type,
                    via_node,
                    created_at
                FROM {_quote_identifier('_queron_meta')}.{_quote_identifier('table_lineage')}
                WHERE child_schema = ? AND child_table = ?
                ORDER BY parent_kind, parent_name
                """,
                (schema, name),
            )
            rows = cur.fetchall() or []
        except Exception:
            return []
        finally:
            cur.close()
    finally:
        conn.close()

    return [
        {
            "parent_kind": row[0],
            "parent_name": row[1],
            "parent_database": row[2],
            "parent_schema": row[3],
            "parent_table": row[4],
            "connector_type": row[5],
            "via_node": row[6],
            "created_at": row[7],
        }
        for row in rows
    ]


def _load_column_mapping_metadata(cur, schema: str, name: str) -> dict[str, dict[str, Any]]:
    try:
        cur.execute(
            f"""
            SELECT
                target_column,
                source_column,
                source_type,
                target_type,
                connector_type,
                mapping_mode,
                warnings_json,
                lossy,
                ordinal_position
            FROM {_quote_identifier('_queron_meta')}.{_quote_identifier('column_mapping')}
            WHERE target_schema = ? AND target_table = ?
            ORDER BY ordinal_position
            """,
            (schema, name),
        )
        rows = cur.fetchall()
    except Exception:
        return {}

    metadata: dict[str, dict[str, Any]] = {}
    for row in rows:
        target_column = str(row[0] or "")
        if not target_column:
            continue
        try:
            warnings = json.loads(str(row[6] or "[]"))
            if not isinstance(warnings, list):
                warnings = []
        except Exception:
            warnings = []
        metadata[target_column] = {
            "source_column": row[1],
            "source_type": row[2],
            "target_type": row[3],
            "connector_type": row[4],
            "mapping_mode": row[5],
            "warnings": warnings,
            "lossy": row[7],
        }
    return metadata


def _materialize_chunk(
    conn,
    *,
    table_name: str,
    source_type: str,
    column_meta: list[Any],
    rows: list[dict[str, Any]],
    replace: bool,
    create_if_empty: bool,
) -> tuple[int, list[str]]:
    mapped_cols = map_columns_to_duckdb(column_meta, source=source_type)
    warnings: list[str] = []
    seen_warnings: set[str] = set()
    for col in mapped_cols:
        for warning in col.warnings or []:
            if warning not in seen_warnings:
                seen_warnings.add(warning)
                warnings.append(warning)

    if replace or create_if_empty:
        create_sql = build_duckdb_create_table_sql(table_name, mapped_cols, replace=replace)
        conn.execute(create_sql)

    if not rows:
        return 0, warnings

    target_ident = _quote_compound_identifier(table_name)
    column_names = [col.name for col in mapped_cols]
    col_sql = ", ".join(_quote_identifier(col) for col in column_names)
    placeholders = ", ".join(["?"] * len(column_names))
    insert_sql = f"INSERT INTO {target_ident} ({col_sql}) VALUES ({placeholders})"
    payload = [tuple(row.get(col) for col in column_names) for row in rows]
    conn.executemany(insert_sql, payload)
    return len(rows), warnings


def ingest_query_to_local_table(
    *,
    connection_id: str,
    source_type: str,
    source_connection_id: str,
    target_table: str,
    column_meta: list[Any],
    rows: list[dict[str, Any]],
    replace: bool,
    pipeline_id: str,
) -> DuckDbIngestQueryResponse:
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("DuckDB connection not found. Please connect first.")

    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    conn = _duckdb_connection(database)
    try:
        inserted, warnings = _materialize_chunk(
            conn,
            table_name=target_table,
            source_type=source_type,
            column_meta=column_meta,
            rows=rows,
            replace=replace,
            create_if_empty=True,
        )
        return DuckDbIngestQueryResponse(
            pipeline_id=pipeline_id,
            target_table=target_table,
            row_count=inserted,
            source_type=source_type,
            source_connection_id=source_connection_id,
            warnings=warnings,
            created_at=time.time(),
            schema_changed=True,
        )
    finally:
        conn.close()


def append_query_chunk_to_local_table(
    *,
    connection_id: str,
    source_type: str,
    target_table: str,
    column_meta: list[Any],
    rows: list[dict[str, Any]],
) -> tuple[int, list[str]]:
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("DuckDB connection not found. Please connect first.")

    database = _resolve_database_path(DuckDbConnectRequest(**cfg_dict))
    conn = _duckdb_connection(database)
    try:
        return _materialize_chunk(
            conn,
            table_name=target_table,
            source_type=source_type,
            column_meta=column_meta,
            rows=rows,
            replace=False,
            create_if_empty=False,
        )
    finally:
        conn.close()


def _get_overview(cur, schema: str, name: str, category: str):
    data = {
        "schema": schema,
        "name": name,
        "category": category,
        "row_count": 0,
        "size_bytes": 0,
        "column_count": 0,
        "index_count": 0,
        "fk_count": 0,
        "primary_key": None,
        "foreign_keys": [],
    }
    constraint_rows: list[tuple[Any, ...]] = []
    try:
        if category == "table":
            cur.execute(
                """
                SELECT estimated_size, column_count, index_count
                FROM duckdb_tables()
                WHERE schema_name = ? AND table_name = ?
                LIMIT 1
                """,
                (schema, name),
            )
            row = cur.fetchone()
            if row:
                data["size_bytes"] = int(row[0] or 0)
                data["column_count"] = int(row[1] or 0)
                data["index_count"] = int(row[2] or 0)
        elif category in {"view", "materialized_view"}:
            cur.execute(
                """
                SELECT column_count
                FROM duckdb_views()
                WHERE schema_name = ? AND view_name = ?
                LIMIT 1
                """,
                (schema, name),
            )
            row = cur.fetchone()
            if row:
                data["column_count"] = int(row[0] or 0)
    except Exception:
        pass

    if not data.get("column_count"):
        try:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ?
                """,
                (schema, name),
            )
            row = cur.fetchone()
            if row:
                data["column_count"] = int(row[0] or 0)
        except Exception:
            pass

    try:
        cur.execute(
            """
            SELECT
                constraint_type,
                constraint_name,
                constraint_column_names,
                referenced_table,
                referenced_column_names
            FROM duckdb_constraints()
            WHERE schema_name = ? AND table_name = ?
            ORDER BY constraint_index
            """,
            (schema, name),
        )
        constraint_rows = cur.fetchall()
    except Exception:
        constraint_rows = []

    foreign_keys: list[dict[str, Any]] = []
    for row in constraint_rows:
        ctype = str(row[0] or "").upper()
        col_names = list(row[2] or [])
        ref_table_raw = str(row[3] or "")
        ref_cols = list(row[4] or [])
        if ctype != "FOREIGN KEY" or not col_names:
            continue
        ref_schema = schema
        ref_table = ref_table_raw
        if "." in ref_table_raw:
            parts = ref_table_raw.split(".", 1)
            ref_schema, ref_table = parts[0], parts[1]
        foreign_keys.append(
            {
                "column": col_names[0],
                "ref_schema": ref_schema,
                "ref_table": ref_table,
                "ref_column": (ref_cols[0] if ref_cols else None),
            }
        )

    data["foreign_keys"] = foreign_keys
    data["fk_count"] = len(foreign_keys)

    for row in constraint_rows:
        ctype = str(row[0] or "").upper()
        col_names = list(row[2] or [])
        if ctype == "PRIMARY KEY" and col_names:
            data["primary_key"] = col_names[0]
            break

    if not data.get("index_count"):
        try:
            data["index_count"] = len(_get_indexes(cur, schema, name))
        except Exception:
            pass

    if category in {"table", "view", "materialized_view"}:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {_qualified_name(schema, name)}")
            row = cur.fetchone()
            if row:
                data["row_count"] = int(row[0] or 0)
        except Exception:
            pass
    return data


def _get_columns(cur, schema: str, name: str):
    mapping_metadata = _load_column_mapping_metadata(cur, schema, name)
    cur.execute(
        """
        SELECT
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        (schema, name),
    )
    rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        column_name = str(row[0] or "")
        metadata = mapping_metadata.get(column_name, {})
        out.append(
            {
                "name": column_name,
                "type": _normalize_duckdb_display_type(str(row[1] or "")),
                "length": row[2],
                "max_length": row[2],
                "precision": row[3],
                "scale": row[4],
                "nullable": (str(row[5] or "").upper() == "YES"),
                "default": row[6],
                "description": "",
                "auto_increment": False,
                "is_auto": False,
                "source_column": metadata.get("source_column"),
                "source_type": metadata.get("source_type"),
                "connector_type": metadata.get("connector_type"),
                "mapping_mode": metadata.get("mapping_mode"),
                "mapping_warnings": metadata.get("warnings", []),
                "lossy": metadata.get("lossy"),
            }
        )
    return out


def _get_constraints(cur, schema: str, name: str):
    try:
        cur.execute(
            """
            SELECT
                constraint_name,
                constraint_type,
                constraint_text
            FROM duckdb_constraints()
            WHERE schema_name = ? AND table_name = ?
            ORDER BY constraint_index
            """,
            (schema, name),
        )
        rows = cur.fetchall()
    except Exception:
        return []
    return [
        {"name": row[0], "type": str(row[1] or "").upper(), "definition": row[2] or ""}
        for row in rows
    ]


def _get_indexes(cur, schema: str, name: str):
    try:
        cur.execute(
            """
            SELECT index_name, sql
            FROM duckdb_indexes()
            WHERE schema_name = ? AND table_name = ?
            ORDER BY index_name
            """,
            (schema, name),
        )
        rows = cur.fetchall()
    except Exception:
        return []
    return [{"name": row[0], "definition": row[1] or ""} for row in rows]


def _get_ddl(cur, schema: str, name: str, category: str):
    columns: list[dict[str, Any]] = []
    foreign_keys: list[dict[str, Any]] = []
    try:
        columns = _get_columns(cur, schema, name)
    except Exception:
        columns = []

    try:
        cur.execute(
            """
            SELECT
                constraint_column_names,
                referenced_table,
                referenced_column_names
            FROM duckdb_constraints()
            WHERE schema_name = ? AND table_name = ? AND upper(constraint_type) = 'FOREIGN KEY'
            """,
            (schema, name),
        )
        fk_rows = cur.fetchall()
    except Exception:
        fk_rows = []

    for row in fk_rows:
        src_cols = list(row[0] or [])
        ref_table_raw = str(row[1] or "")
        ref_cols = list(row[2] or [])
        if not src_cols or not ref_table_raw:
            continue
        ref_schema = schema
        ref_table = ref_table_raw
        if "." in ref_table_raw:
            parts = ref_table_raw.split(".", 1)
            ref_schema, ref_table = parts[0], parts[1]
        foreign_keys.append(
            {
                "column": src_cols[0],
                "ref_schema": ref_schema,
                "ref_table": ref_table,
                "ref_column": (ref_cols[0] if ref_cols else None),
            }
        )

    try:
        if category == "table":
            cur.execute(
                """
                SELECT sql
                FROM duckdb_tables()
                WHERE schema_name = ? AND table_name = ?
                LIMIT 1
                """,
                (schema, name),
            )
            row = cur.fetchone()
            ddl = row[0] if row and row[0] else ""
            indexes = _get_indexes(cur, schema, name)
            if indexes:
                ddl_parts = [ddl] if ddl else []
                for idx in indexes:
                    definition = str(idx.get("definition") or "").strip()
                    if definition:
                        ddl_parts.append(definition if definition.endswith(";") else f"{definition};")
                ddl = "\n".join(ddl_parts)
            return {"ddl": ddl, "foreign_keys": foreign_keys, "columns": columns}
        if category in {"view", "materialized_view"}:
            cur.execute(
                """
                SELECT sql
                FROM duckdb_views()
                WHERE schema_name = ? AND view_name = ?
                LIMIT 1
                """,
                (schema, name),
            )
            row = cur.fetchone()
            return {"ddl": row[0] if row and row[0] else "", "foreign_keys": [], "columns": columns}
    except Exception:
        pass
    return {
        "ddl": f"-- DuckDB {category} DDL lookup is not available for {schema}.{name}.",
        "foreign_keys": foreign_keys,
        "columns": columns,
    }


def _get_dependencies(cur, schema: str, name: str, category: str):
    references: list[str] = []
    referenced_by: list[str] = []
    try:
        if category in {"view", "materialized_view"}:
            cur.execute(
                """
                SELECT sql
                FROM duckdb_views()
                WHERE schema_name = ? AND view_name = ?
                LIMIT 1
                """,
                (schema, name),
            )
            row = cur.fetchone()
            sql = str(row[0] or "") if row else ""
            for match in re.finditer(r'FROM\s+("?[\w]+"?\.)?"?([\w]+)"?', sql, flags=re.IGNORECASE):
                ref_schema = schema
                if match.group(1):
                    ref_schema = match.group(1).replace('"', "").rstrip(".")
                ref_table = match.group(2).replace('"', "")
                references.append(f"{ref_schema}.{ref_table}")
        elif category == "table":
            cur.execute(
                """
                SELECT referenced_table
                FROM duckdb_constraints()
                WHERE schema_name = ? AND table_name = ? AND upper(constraint_type) = 'FOREIGN KEY'
                """,
                (schema, name),
            )
            for row in cur.fetchall():
                ref = str(row[0] or "").strip()
                if ref:
                    references.append(ref if "." in ref else f"{schema}.{ref}")

            cur.execute(
                """
                SELECT schema_name, table_name
                FROM duckdb_constraints()
                WHERE upper(constraint_type) = 'FOREIGN KEY' AND referenced_table IN (?, ?)
                """,
                (f"{schema}.{name}", name),
            )
            for row in cur.fetchall():
                src_schema = str(row[0] or "").strip()
                src_table = str(row[1] or "").strip()
                if src_schema and src_table:
                    referenced_by.append(f"{src_schema}.{src_table}")
    except Exception:
        pass
    return {"referenced_by": sorted(set(referenced_by)), "references": sorted(set(references))}


def _get_er_diagram(cur, schema: str, name: str):
    related_tables: set[tuple[str, str]] = {(schema, name)}
    outgoing_fks: list[dict[str, Any]] = []
    incoming_fks: list[dict[str, Any]] = []

    try:
        cur.execute(
            """
            SELECT
                constraint_column_names,
                referenced_table,
                referenced_column_names
            FROM duckdb_constraints()
            WHERE schema_name = ? AND table_name = ? AND upper(constraint_type) = 'FOREIGN KEY'
            """,
            (schema, name),
        )
        rows = cur.fetchall()
    except Exception:
        rows = []

    for row in rows:
        columns = list(row[0] or [])
        ref_table_raw = str(row[1] or "")
        ref_columns = list(row[2] or [])
        if not columns or not ref_table_raw:
            continue
        ref_schema = schema
        ref_table = ref_table_raw
        if "." in ref_table_raw:
            parts = ref_table_raw.split(".", 1)
            ref_schema, ref_table = parts[0], parts[1]
        outgoing_fks.append(
            {
                "column": columns[0],
                "ref_schema": ref_schema,
                "ref_table": ref_table,
                "ref_column": (ref_columns[0] if ref_columns else None),
            }
        )
        related_tables.add((ref_schema, ref_table))

    try:
        cur.execute(
            """
            SELECT
                schema_name,
                table_name,
                constraint_column_names,
                referenced_column_names
            FROM duckdb_constraints()
            WHERE upper(constraint_type) = 'FOREIGN KEY' AND referenced_table IN (?, ?)
            """,
            (f"{schema}.{name}", name),
        )
        rows = cur.fetchall()
    except Exception:
        rows = []

    for row in rows:
        source_schema = str(row[0] or "")
        source_table = str(row[1] or "")
        source_columns = list(row[2] or [])
        ref_columns = list(row[3] or [])
        if not source_schema or not source_table or source_schema == schema and source_table == name:
            continue
        incoming_fks.append(
            {
                "source_schema": source_schema,
                "source_table": source_table,
                "source_column": (source_columns[0] if source_columns else None),
                "ref_column": (ref_columns[0] if ref_columns else None),
            }
        )
        related_tables.add((source_schema, source_table))

    table_columns: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for t_schema, t_name in related_tables:
        cols = _get_columns(cur, t_schema, t_name)
        table_columns.setdefault(t_schema, {})[t_name] = cols

    return {
        "table_columns": table_columns,
        "outgoing_fks": outgoing_fks,
        "incoming_fks": incoming_fks,
    }


def _get_sample_data(cur, schema: str, name: str):
    sql = f"SELECT * FROM {_qualified_name(schema, name)} LIMIT 20"
    cur.execute(sql)
    columns = _cursor_columns(cur)
    rows = cur.fetchall()
    column_meta = _build_basic_column_meta(cur)
    return {
        "columns": columns,
        "rows": _rows_to_records(columns, rows),
        "column_meta": column_meta,
    }


def health():
    return {"status": "ok"}
