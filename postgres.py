"""
Loom Backend - PostgreSQL Connector
====================================
A PostgreSQL driver module that manages connections,
runs queries, and returns results via PyArrow serialisation.
"""

from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import psycopg
import pyarrow as pa
from psycopg.rows import dict_row

try:
    from .base import (
        ColumnMeta,
        ConnectResponse,
        DEFAULT_QUERY_CHUNK_SIZE,
        PgConnectRequest,
        PgQueryChunkRequest,
        PgQueryRequest,
        QueryResponse,
        TestConnectionResponse,
    )
    from .type_mapper import normalize_display_type
except ImportError:
    from base import (
        ColumnMeta,
        ConnectResponse,
        DEFAULT_QUERY_CHUNK_SIZE,
        PgConnectRequest,
        PgQueryChunkRequest,
        PgQueryRequest,
        QueryResponse,
        TestConnectionResponse,
    )
    from type_mapper import normalize_display_type

# ---------------------------------------------------------------------------
# In-memory connection store  { id -> config dict }
# ---------------------------------------------------------------------------
_connections: dict[str, dict[str, Any]] = {}
_query_sessions: dict[str, dict[str, Any]] = {}
_QUERY_SESSION_IDLE_TTL_SECONDS = 300
_DEFAULT_QUERY_CHUNK_SIZE = DEFAULT_QUERY_CHUNK_SIZE
_VALID_PG_SSLMODES = {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}

# ---------------------------------------------------------------------------
# PostgreSQL OID -> type name mapping (common types)
# ---------------------------------------------------------------------------
_PG_TYPE_NAMES: dict[int, str] = {
    16: "BOOLEAN",
    20: "BIGINT",
    21: "SMALLINT",
    23: "INTEGER",
    25: "TEXT",
    700: "REAL",
    701: "DOUBLE PRECISION",
    790: "MONEY",
    1042: "CHAR",
    1043: "VARCHAR",
    1082: "DATE",
    1083: "TIME",
    1114: "TIMESTAMP",
    1184: "TIMESTAMPTZ",
    1700: "NUMERIC",
    2950: "UUID",
    3802: "JSONB",
    114: "JSON",
    1000: "BOOLEAN[]",
    1005: "SMALLINT[]",
    1007: "INTEGER[]",
    1009: "TEXT[]",
    1016: "BIGINT[]",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_connection_uri(raw_url: str, username: str | None, password: str | None) -> str:
    raw = (raw_url or "").strip()
    if not raw:
        raise RuntimeError("PostgreSQL connection URL is required.")
    if raw.startswith("jdbc:"):
        raw = raw[len("jdbc:"):]

    parts = urlsplit(raw)
    if not parts.scheme:
        return raw
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


def _infer_postgres_auth_mode(cfg: PgConnectRequest) -> str:
    explicit = str(getattr(cfg, "auth_mode", "") or "").strip().lower()
    if explicit:
        return explicit
    if str(getattr(cfg, "sslcert", "") or "").strip() or str(getattr(cfg, "sslkey", "") or "").strip():
        return "mtls"
    if (
        str(getattr(cfg, "sslmode", "") or "").strip()
        or str(getattr(cfg, "sslrootcert", "") or "").strip()
        or str(getattr(cfg, "sslpassword", "") or "").strip()
    ):
        return "tls"
    return "basic"


def _ssl_kwargs_from_config(cfg: PgConnectRequest, auth_mode: str) -> dict[str, Any]:
    if auth_mode not in {"tls", "mtls"}:
        return {}
    kwargs: dict[str, Any] = {}
    for field_name in ("sslmode", "sslrootcert", "sslcert", "sslkey", "sslpassword"):
        value = getattr(cfg, field_name, None)
        if value is None:
            continue
        text = str(value).strip() if isinstance(value, str) else value
        if text in {"", None}:
            continue
        kwargs[field_name] = text
    return kwargs


def _validate_postgres_auth_config(cfg: PgConnectRequest, auth_mode: str) -> None:
    if auth_mode not in {"basic", "tls", "mtls", "kerberos"}:
        raise RuntimeError(f"Unsupported PostgreSQL auth_mode '{auth_mode}'.")
    sslmode = str(getattr(cfg, "sslmode", "") or "").strip().lower()
    sslrootcert = str(getattr(cfg, "sslrootcert", "") or "").strip()
    sslcert = str(getattr(cfg, "sslcert", "") or "").strip()
    sslkey = str(getattr(cfg, "sslkey", "") or "").strip()
    if sslmode and sslmode not in _VALID_PG_SSLMODES:
        valid = ", ".join(sorted(_VALID_PG_SSLMODES))
        raise RuntimeError(f"Unsupported PostgreSQL sslmode '{cfg.sslmode}'. Expected one of: {valid}.")
    if auth_mode in {"tls", "mtls"} and sslmode == "disable":
        raise RuntimeError("PostgreSQL TLS auth_mode cannot use sslmode='disable'.")
    if auth_mode in {"tls", "mtls"} and sslmode in {"verify-ca", "verify-full"} and not sslrootcert:
        raise RuntimeError(f"PostgreSQL {auth_mode} with sslmode='{sslmode}' requires sslrootcert.")
    if auth_mode == "mtls" and bool(sslcert) != bool(sslkey):
        raise RuntimeError("PostgreSQL mTLS requires both sslcert and sslkey.")


def _dsn_from_config(cfg: PgConnectRequest) -> tuple[str, dict[str, Any]]:
    """Build a libpq DSN + optional kwargs from the request payload.

    Returns (dsn, extra_kwargs) where extra_kwargs may contain
    ``user`` and ``password`` when connecting via URL.
    """
    auth_mode = _infer_postgres_auth_mode(cfg)
    _validate_postgres_auth_config(cfg, auth_mode)
    kwargs: dict[str, Any] = {}
    if cfg.connect_timeout_seconds is not None:
        kwargs["connect_timeout"] = int(cfg.connect_timeout_seconds)
    if cfg.statement_timeout_ms is not None:
        kwargs["options"] = f"-c statement_timeout={int(cfg.statement_timeout_ms)}"
    kwargs.update(_ssl_kwargs_from_config(cfg, auth_mode))
    if cfg.url:
        return _build_connection_uri(cfg.url, cfg.username or None, cfg.password or None), kwargs
    return _build_connection_uri(
        f"postgresql://{cfg.host}:{cfg.port}/{cfg.database}",
        cfg.username or None,
        cfg.password or None,
    ), kwargs


@contextmanager
def _pg_connection(dsn: str, **kwargs):
    """Yield a psycopg connection and guarantee cleanup."""
    conn = psycopg.connect(dsn, **kwargs)
    try:
        yield conn
    finally:
        conn.close()


def _deduplicate_names(raw_names: list[str], table_names: list[str] | None = None) -> list[str]:
    """Deduplicate column names using table qualification.

    Duplicate columns become table.column (e.g. orders.product_id, products.product_id).
    Falls back to column_2 suffix if table name is not available.
    """
    # First pass: find which names appear more than once
    from collections import Counter
    counts = Counter(raw_names)

    col_names: list[str] = []
    seen: dict[str, int] = {}
    for i, name in enumerate(raw_names):
        if counts[name] > 1:
            # Duplicate - qualify with table name if available
            tbl = table_names[i] if table_names and i < len(table_names) else None
            if tbl:
                qualified = f"{tbl}.{name}"
                # Handle edge case: two tables with same name (shouldn't happen)
                if qualified in seen:
                    seen[qualified] += 1
                    col_names.append(f"{qualified}_{seen[qualified]}")
                else:
                    seen[qualified] = 1
                    col_names.append(qualified)
            else:
                # No table name available - fallback to _N suffix
                if name in seen:
                    seen[name] += 1
                    col_names.append(f"{name}_{seen[name]}")
                else:
                    seen[name] = 1
                    col_names.append(name)
        else:
            col_names.append(name)
    return col_names


def _rows_to_arrow(rows: list[tuple[Any, ...]], col_names: list[str]) -> pa.Table:
    """Convert row tuples into a PyArrow table using the current column order."""
    if not col_names:
        return pa.table({})
    if not rows:
        return pa.table({name: pa.array([]) for name in col_names})
    # Transpose row-major -> column-major using index-based access
    col_data: list[list] = [[] for _ in col_names]
    for row in rows:
        for i, value in enumerate(row):
            col_data[i].append(value)

    arrays = [_arrow_array_for_column(col) for col in col_data]
    return pa.table(dict(zip(col_names, arrays)))


def _rows_to_records(col_names: list[str], rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    return _rows_to_arrow(rows, col_names).to_pylist()


def _arrow_array_for_column(values: list[Any]) -> pa.Array:
    try:
        return pa.array(values)
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError):
        normalized = [str(value) if isinstance(value, uuid.UUID) else value for value in values]
        try:
            return pa.array(normalized)
        except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError):
            return pa.array([None if value is None else str(value) for value in values], type=pa.string())


def _resolve_pg_type_names(conn, type_codes: list[int]) -> dict[int, str]:
    unresolved = sorted({type_code for type_code in type_codes if type_code not in _PG_TYPE_NAMES})
    if not unresolved:
        return {}

    try:
        placeholders = ",".join(["%s"] * len(unresolved))
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT
                    t.oid,
                    CASE
                        WHEN t.typtype = 'e' THEN 'ENUM'
                        WHEN t.typtype = 'd' AND t.typbasetype <> 0 THEN format_type(t.typbasetype, t.typtypmod)
                        ELSE format_type(t.oid, NULL)
                    END AS type_name
                FROM pg_type t
                WHERE t.oid IN ({placeholders})
                """,
                unresolved,
            )
            return {
                row["oid"]: _normalize_pg_display_type(row["type_name"])
                for row in cur.fetchall()
                if row.get("type_name")
            }
    except Exception:
        return {}


def _normalize_pg_display_type(type_name: str) -> str:
    return normalize_display_type("postgres", type_name)


def _build_basic_column_meta(cursor, deduped_names: list[str], conn=None) -> list[ColumnMeta]:
    if cursor.description is None:
        return []

    resolved_type_names = _resolve_pg_type_names(conn, [desc[1] for desc in cursor.description]) if conn else {}
    metas: list[ColumnMeta] = []
    for i, desc in enumerate(cursor.description):
        type_code = desc[1]
        internal_size = desc[3]
        precision = desc[4]
        scale = desc[5]
        null_ok = desc[6]
        max_length = internal_size if isinstance(internal_size, int) and internal_size >= 0 else None

        metas.append(
            ColumnMeta(
                name=deduped_names[i],
                data_type=_normalize_pg_display_type(
                    resolved_type_names.get(type_code, _PG_TYPE_NAMES.get(type_code, f"OID:{type_code}"))
                ),
                max_length=max_length,
                precision=precision,
                scale=scale,
                nullable=True if null_ok is None else bool(null_ok),
            )
        )

    return metas


def _close_query_session(session_id: str) -> None:
    session = _query_sessions.pop(session_id, None)
    if not session:
        return

    cursor = session.get("cursor")
    conn = session.get("conn")

    try:
        if cursor is not None:
            cursor.close()
    except Exception:
        pass

    try:
        if conn is not None and not conn.closed:
            conn.rollback()
            conn.close()
    except Exception:
        pass


def _cleanup_expired_query_sessions() -> None:
    now = time.time()
    expired_ids = [
        session_id
        for session_id, session in _query_sessions.items()
        if now - session.get("last_accessed_at", now) > _QUERY_SESSION_IDLE_TTL_SECONDS
    ]
    for session_id in expired_ids:
        _close_query_session(session_id)


def _build_query_metadata(conn, cursor, sql: str, *, enrich: bool = False) -> tuple[list[str], list[ColumnMeta]]:
    raw_names = [desc[0] for desc in cursor.description]
    oid_to_table: dict[int, str] = {}
    table_oids = set()
    for desc in cursor.description:
        oid = getattr(desc, "table_oid", None)
        if oid and oid > 0:
            table_oids.add(oid)

    if table_oids:
        try:
            ph = ",".join(["%s"] * len(table_oids))
            with conn.cursor(row_factory=dict_row) as oid_cur:
                oid_cur.execute(f"SELECT oid, relname FROM pg_class WHERE oid IN ({ph})", list(table_oids))
                for row in oid_cur.fetchall():
                    oid_to_table[row["oid"]] = row["relname"]
        except Exception:
            pass

    per_col_tables = [
        oid_to_table.get(getattr(desc, "table_oid", None), "")
        for desc in cursor.description
    ]
    deduped_names = _deduplicate_names(raw_names, per_col_tables)

    if not enrich:
        return deduped_names, _build_basic_column_meta(cursor, deduped_names, conn)

    col_meta = _extract_column_meta(cursor, conn, deduped_names)
    fk_map = _extract_foreign_keys(conn, sql)
    key_map = _extract_key_constraints(conn, sql)

    for i, cm in enumerate(col_meta):
        orig = raw_names[i] if i < len(raw_names) else cm.name
        fk = fk_map.get(orig)
        if fk:
            cm.fk_ref_schema = fk["schema"]
            cm.fk_ref_table = fk["table"]
            cm.fk_ref_column = fk["column"]
        keys = key_map.get(orig, set())
        if "pk" in keys:
            cm.is_primary_key = True
        if "uq" in keys:
            cm.is_unique = True

    return deduped_names, col_meta


def _is_incremental_query(sql: str) -> bool:
    normalized = sql.lstrip().lower()
    return normalized.startswith("select") or normalized.startswith("with")


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


def _should_enrich_query_metadata(sql: str) -> bool:
    import re

    normalized = " ".join(_metadata_sql(sql).strip().rstrip(";").split())
    if not normalized.lower().startswith("select "):
        return False
    if re.search(r"\b(with|join|union|intersect|except|returning|into)\b", normalized, re.IGNORECASE):
        return False
    if re.search(r"\bfrom\s+\(", normalized, re.IGNORECASE):
        return False
    from_match = re.search(r"\bfrom\s+([A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*)?)\b", normalized, re.IGNORECASE)
    if not from_match:
        return False
    tail = normalized[from_match.end():]
    if "," in tail:
        return False
    if re.search(r"\b(from)\b", tail, re.IGNORECASE):
        return False
    return True


def _fetch_cursor_chunk(session: dict[str, Any], chunk_size: int) -> QueryResponse:
    cursor = session["cursor"]
    pending_rows = session.pop("pending_rows", [])
    rows = list(pending_rows)
    if len(rows) < chunk_size + 1:
        rows.extend(cursor.fetchmany((chunk_size + 1) - len(rows)))
    col_names = session["columns"]
    has_more = len(rows) > chunk_size
    emitted_rows = rows[:chunk_size]
    if has_more:
        session["pending_rows"] = rows[chunk_size:]
    serialized_rows = _rows_to_records(col_names, emitted_rows)
    loaded_count = session.get("loaded_count", 0) + len(serialized_rows)
    session["loaded_count"] = loaded_count
    session["last_accessed_at"] = time.time()
    if not has_more:
        _close_query_session(session["session_id"])

    return QueryResponse(
        columns=col_names,
        column_meta=session["column_meta"],
        rows=serialized_rows,
        row_count=loaded_count,
        query_session_id=session["session_id"] if has_more else None,
        has_more=has_more,
        chunk_size=session["chunk_size"],
        mode="cursor",
    )


def _drain_cursor_session(session: dict[str, Any]) -> QueryResponse:
    cursor = session["cursor"]
    rows = list(session.pop("pending_rows", []))
    rows.extend(cursor.fetchall())
    serialized_rows = _rows_to_records(session["columns"], rows)
    loaded_count = session.get("loaded_count", 0) + len(serialized_rows)
    session["loaded_count"] = loaded_count
    _close_query_session(session["session_id"])
    return QueryResponse(
        columns=session["columns"],
        column_meta=session["column_meta"],
        rows=serialized_rows,
        row_count=loaded_count,
        query_session_id=None,
        has_more=False,
        chunk_size=session["chunk_size"],
        mode="cursor",
    )


def _open_cursor_query_session(req: PgQueryRequest, dsn: str, kwargs: dict[str, str]) -> QueryResponse:
    conn = psycopg.connect(dsn, **kwargs)
    conn.autocommit = False
    session_id = str(uuid.uuid4())
    cursor = conn.cursor(name=f"loom_{session_id.replace('-', '')}")
    cursor.execute(req.sql)
    rows = cursor.fetchmany(req.chunk_size + 1)

    if cursor.description is None:
        cursor.close()
        conn.commit()
        conn.close()
        return QueryResponse(
            columns=[],
            column_meta=[],
            rows=[],
            row_count=cursor.rowcount,
            has_more=False,
            chunk_size=req.chunk_size,
            mode="statement",
        )

    deduped_names, col_meta = _build_query_metadata(conn, cursor, req.sql, enrich=_should_enrich_query_metadata(req.sql))

    session = {
        "session_id": session_id,
        "connection_id": req.connection_id,
        "sql": req.sql,
        "chunk_size": req.chunk_size,
        "columns": deduped_names,
        "column_meta": col_meta,
        "conn": conn,
        "cursor": cursor,
        "pending_rows": rows,
        "loaded_count": 0,
        "last_accessed_at": time.time(),
    }
    _query_sessions[session_id] = session
    return _fetch_cursor_chunk(session, req.chunk_size)


def _run_statement_query(req: PgQueryRequest, dsn: str, kwargs: dict[str, str]) -> QueryResponse:
    schema_changed = _is_schema_change_query(req.sql)
    with _pg_connection(dsn, **kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(req.sql)
            if cur.description is None:
                conn.commit()
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

            deduped_names, col_meta = _build_query_metadata(conn, cur, req.sql, enrich=_should_enrich_query_metadata(req.sql))
            rows = cur.fetchmany(req.chunk_size + 1)
            has_more = len(rows) > req.chunk_size
            emitted_rows = rows[:req.chunk_size]
            serialized_rows = _rows_to_records(deduped_names, emitted_rows)
            return QueryResponse(
                columns=deduped_names,
                column_meta=col_meta,
                rows=serialized_rows,
                row_count=len(serialized_rows),
                has_more=has_more,
                chunk_size=req.chunk_size,
                mode="statement",
            )


def _fetch_offset_chunk(session: dict[str, Any], chunk_size: int) -> QueryResponse:
    offset = session.get("offset", 0)
    sql = f'SELECT * FROM ({session["sql"]}) AS loom_paged_query LIMIT %s OFFSET %s'

    with _pg_connection(session["dsn"], **session["kwargs"]) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (chunk_size + 1, offset))
            rows = cur.fetchall()

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
    sql = f'SELECT * FROM ({session["sql"]}) AS loom_paged_query OFFSET %s'
    offset = session.get("offset", 0)
    with _pg_connection(session["dsn"], **session["kwargs"]) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (offset,))
            rows = cur.fetchall()

    serialized_rows = _rows_to_records(session["columns"], rows)
    loaded_count = session.get("loaded_count", 0) + len(serialized_rows)
    _query_sessions.pop(session["session_id"], None)
    return QueryResponse(
        columns=session["columns"],
        column_meta=session["column_meta"],
        rows=serialized_rows,
        row_count=loaded_count,
        query_session_id=None,
        has_more=False,
        chunk_size=session["chunk_size"],
        mode="offset",
    )


def _open_offset_query_session(req: PgQueryRequest, dsn: str, kwargs: dict[str, str]) -> QueryResponse:
    paged_sql = f'SELECT * FROM ({req.sql}) AS loom_paged_query LIMIT %s OFFSET %s'

    with _pg_connection(dsn, **kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(paged_sql, (req.chunk_size + 1, 0))
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
            deduped_names, col_meta = _build_query_metadata(conn, cur, req.sql, enrich=_should_enrich_query_metadata(req.sql))
            rows = cur.fetchall()

    has_more = len(rows) > req.chunk_size
    emitted_rows = rows[:req.chunk_size]

    session_id = str(uuid.uuid4())
    session = {
        "session_id": session_id,
        "connection_id": req.connection_id,
        "sql": req.sql,
        "chunk_size": req.chunk_size,
        "columns": deduped_names,
        "column_meta": col_meta,
        "dsn": dsn,
        "kwargs": kwargs,
        "offset": len(emitted_rows),
        "loaded_count": len(emitted_rows),
        "last_accessed_at": time.time(),
        "mode": "offset",
    }

    serialized_rows = _rows_to_records(deduped_names, emitted_rows)
    if has_more:
        _query_sessions[session_id] = session

    return QueryResponse(
        columns=deduped_names,
        column_meta=col_meta,
        rows=serialized_rows,
        row_count=len(serialized_rows),
        query_session_id=session_id if has_more else None,
        has_more=has_more,
        chunk_size=req.chunk_size,
        mode="offset",
    )


def _extract_column_meta(cursor, conn, deduped_names: list[str]) -> list[ColumnMeta]:
    """Extract rich column metadata from cursor.description + information_schema.

    Uses table_oid when the driver provides it to determine the source table per column,
    even when columns have duplicate names across JOINed tables.
    """
    if cursor.description is None:
        return []

    # --- Step 1: Resolve table OIDs to table names ---
    oid_to_table: dict[int, str] = {}
    table_oids = set()
    for desc in cursor.description:
        oid = getattr(desc, 'table_oid', None)
        if oid and oid > 0:
            table_oids.add(oid)

    if table_oids:
        try:
            placeholders = ",".join(["%s"] * len(table_oids))
            with conn.cursor(row_factory=dict_row) as oid_cur:
                oid_cur.execute(
                    f"SELECT oid, relname FROM pg_class WHERE oid IN ({placeholders})",
                    list(table_oids),
                )
                for row in oid_cur.fetchall():
                    oid_to_table[row["oid"]] = row["relname"]
        except Exception:
            pass

    # --- Step 2: Get info_schema keyed by (table_name, column_name) ---
    info_schema: dict[tuple[str, str], dict] = {}
    try:
        col_names = [desc[0] for desc in cursor.description]
        placeholders = ",".join(["%s"] * len(col_names))
        with conn.cursor(row_factory=dict_row) as info_cur:
            info_cur.execute(f"""
                SELECT column_name, table_name, character_maximum_length, numeric_precision,
                       numeric_scale, is_nullable, column_default,
                       COALESCE(col_description(
                            (table_schema || '.' || table_name)::regclass,
                           ordinal_position
                       ), '') as description
                FROM information_schema.columns
                WHERE column_name IN ({placeholders})
                ORDER BY ordinal_position
            """, col_names)
            for row in info_cur.fetchall():
                key = (row["table_name"], row["column_name"])
                info_schema[key] = row
    except Exception:
        pass

    # --- Step 3: Build metadata for each column ---
    resolved_type_names = _resolve_pg_type_names(conn, [desc[1] for desc in cursor.description])
    metas = []
    for i, desc in enumerate(cursor.description):
        orig_name = desc[0]
        display_name = deduped_names[i]
        type_code = desc[1]
        internal_size = desc[3]
        precision = desc[4]
        scale = desc[5]
        null_ok = desc[6]

        type_name = _normalize_pg_display_type(
            resolved_type_names.get(type_code, _PG_TYPE_NAMES.get(type_code, f"OID:{type_code}"))
        )

        # Determine source table from table_oid
        table_oid = getattr(desc, 'table_oid', None)
        source_table = oid_to_table.get(table_oid, "") if table_oid else ""

        # Look up info_schema with (table, column) key for accuracy
        info = info_schema.get((source_table, orig_name), {})
        if not info:
            # Fallback: try any table with this column name
            for key, val in info_schema.items():
                if key[1] == orig_name:
                    info = val
                    if not source_table:
                        source_table = key[0]
                    break

        max_length = info.get("character_maximum_length") or internal_size
        if max_length and max_length < 0:
            max_length = None

        is_nullable = True
        if null_ok is not None:
            is_nullable = bool(null_ok)
        elif info.get("is_nullable"):
            is_nullable = info["is_nullable"] == "YES"

        auto = False
        col_default = info.get("column_default", "") or ""
        if "nextval(" in str(col_default):
            auto = True

        metas.append(ColumnMeta(
            name=display_name,  # Use deduped name
            data_type=type_name,
            source_table=source_table,
            max_length=max_length if isinstance(max_length, int) else None,
            precision=precision or info.get("numeric_precision"),
            scale=scale or info.get("numeric_scale"),
            nullable=is_nullable,
            auto_increment=auto,
            description=info.get("description", ""),
        ))

    return metas


def _normalize_sql_for_metadata(sql: str) -> str:
    """Normalize simple quoted identifiers for metadata regex parsing."""
    import re

    return re.sub(r'"([A-Za-z_][\w$]*)"', r"\1", sql)


def _strip_sql_comments_for_metadata(sql: str) -> str:
    """Best-effort SQL comment stripping for metadata heuristics only."""
    import re

    without_block = re.sub(r"/\*.*?\*/", " ", sql or "", flags=re.DOTALL)
    lines = []
    for line in without_block.splitlines():
        lines.append(re.sub(r"--.*$", "", line))
    return "\n".join(lines)


def _metadata_sql(sql: str) -> str:
    return _normalize_sql_for_metadata(_strip_sql_comments_for_metadata(sql))


def _extract_table_refs_for_metadata(sql: str) -> list[tuple[str | None, str]]:
    import re

    normalized_sql = _metadata_sql(sql)
    refs: list[tuple[str | None, str]] = []
    seen: set[tuple[str | None, str]] = set()

    for match in re.finditer(r"\b(?:FROM|JOIN)\s+([A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*)?)", normalized_sql, re.IGNORECASE):
        raw = match.group(1)
        parts = [p for p in raw.split(".") if p]
        if not parts:
            continue
        if len(parts) >= 2:
            ref = (parts[-2], parts[-1])
        else:
            ref = (None, parts[-1])
        if ref in seen:
            continue
        seen.add(ref)
        refs.append(ref)
    return refs

def _extract_foreign_keys(conn, sql: str) -> dict[str, dict]:
    """Extract FK relationships for tables referenced in the SQL."""
    fk_map: dict[str, dict] = {}  # column_name -> {schema, table, column}
    table_refs = _extract_table_refs_for_metadata(sql)
    if not table_refs:
        return fk_map

    try:
        where_clauses: list[str] = []
        params: list[str] = []
        for schema, table in table_refs:
            if schema:
                where_clauses.append("(kcu.table_schema = %s AND kcu.table_name = %s)")
                params.extend([schema, table])
            else:
                where_clauses.append("(kcu.table_name = %s)")
                params.append(table)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f"""
                SELECT
                    kcu.column_name,
                    ccu.table_schema AS ref_schema,
                    ccu.table_name   AS ref_table,
                    ccu.column_name  AS ref_column
                FROM information_schema.key_column_usage kcu
                JOIN information_schema.referential_constraints rc
                    ON kcu.constraint_name = rc.constraint_name
                    AND kcu.constraint_schema = rc.constraint_schema
                JOIN information_schema.constraint_column_usage ccu
                    ON rc.unique_constraint_name = ccu.constraint_name
                    AND rc.unique_constraint_schema = ccu.constraint_schema
                WHERE {" OR ".join(where_clauses)}
            """, params)
            for row in cur.fetchall():
                fk_map[row["column_name"]] = {
                    "schema": row["ref_schema"],
                    "table": row["ref_table"],
                    "column": row["ref_column"],
                }
    except Exception:
        pass  # Graceful degradation

    return fk_map

def _extract_key_constraints(conn, sql: str) -> dict[str, set[str]]:
    """Extract PK and UNIQUE constraints for tables referenced in the SQL.

    Returns dict: column_name -> set of constraint types ('pk', 'uq').
    """
    key_map: dict[str, set[str]] = {}  # column_name -> {'pk', 'uq'}
    table_refs = _extract_table_refs_for_metadata(sql)
    if not table_refs:
        return key_map

    try:
        where_clauses: list[str] = []
        params: list[str] = []
        for schema, table in table_refs:
            if schema:
                where_clauses.append("(tc.table_schema = %s AND tc.table_name = %s)")
                params.extend([schema, table])
            else:
                where_clauses.append("(tc.table_name = %s)")
                params.append(table)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f"""
                SELECT kcu.column_name, tc.constraint_type
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE ({" OR ".join(where_clauses)})
                  AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
            """, params)
            for row in cur.fetchall():
                col = row["column_name"]
                ctype = row["constraint_type"]
                if col not in key_map:
                    key_map[col] = set()
                if ctype == "PRIMARY KEY":
                    key_map[col].add("pk")
                elif ctype == "UNIQUE":
                    key_map[col].add("uq")
    except Exception:
        pass  # Graceful degradation

    return key_map

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

def test_connection(req: PgConnectRequest):
    """Validate that the provided credentials can reach the database."""
    dsn, kwargs = _dsn_from_config(req)
    try:
        with _pg_connection(dsn, **kwargs) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return TestConnectionResponse(success=True, message="Connection successful!")
    except Exception as exc:
        return TestConnectionResponse(success=False, message=str(exc))


def connect(req: PgConnectRequest):
    """Store a connection configuration and return a connection ID."""
    dsn, kwargs = _dsn_from_config(req)
    # Quick validation - fail fast if credentials are wrong
    try:
        with _pg_connection(dsn, **kwargs) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc

    conn_id = (req.connection_id or "").strip() or str(uuid.uuid4())
    payload = req.model_dump()
    payload["connection_id"] = conn_id
    _connections[conn_id] = payload
    return ConnectResponse(connection_id=conn_id, message="Connected successfully!")


def run_query(req: PgQueryRequest):
    """Execute a SQL statement and return the first query chunk or full statement result."""
    cfg_dict = _connections.get(req.connection_id)
    if cfg_dict is None:
        raise LookupError("Connection not found. Please connect first.")

    cfg = PgConnectRequest(**cfg_dict)
    dsn, kwargs = _dsn_from_config(cfg)

    try:
        _cleanup_expired_query_sessions()
        if _is_incremental_query(req.sql):
            try:
                return _open_cursor_query_session(req, dsn, kwargs)
            except Exception:
                return _open_offset_query_session(req, dsn, kwargs)
        return _run_statement_query(req, dsn, kwargs)
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc


def fetch_query_chunk(session_id: str, req: PgQueryChunkRequest):
    session = _query_sessions.get(session_id)
    if session is None:
        raise LookupError("Query session not found or expired.")

    chunk_size = req.chunk_size or session.get("chunk_size") or _DEFAULT_QUERY_CHUNK_SIZE

    try:
        if req.load_all:
            if session.get("mode") == "offset":
                return _drain_offset_session(session)
            return _drain_cursor_session(session)
        if session.get("mode") == "offset":
            return _fetch_offset_chunk(session, chunk_size)
        return _fetch_cursor_chunk(session, chunk_size)
    except Exception as exc:
        _close_query_session(session_id)
        raise RuntimeError(str(exc)) from exc


def close_query_session(session_id: str):
    existed = session_id in _query_sessions
    _close_query_session(session_id)
    return {"closed": existed}


def get_schema(connection_id: str):
    """Return schema/table/column metadata for autocomplete."""
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("Connection not found.")

    cfg = PgConnectRequest(**cfg_dict)
    dsn, kwargs = _dsn_from_config(cfg)

    try:
        with _pg_connection(dsn, **kwargs) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    SELECT table_schema, table_name, column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY table_schema, table_name, ordinal_position
                """)
                rows = cur.fetchall()

        # Group by schema -> table -> columns
        schemas: dict[str, dict[str, list[dict]]] = {}
        for row in rows:
            s = row["table_schema"]
            t = row["table_name"]
            if s not in schemas:
                schemas[s] = {}
            if t not in schemas[s]:
                schemas[s][t] = []
            schemas[s][t].append({
                "name": row["column_name"],
                "type": row["data_type"],
            })

        return {
            "schemas": list(schemas.keys()),
            "tables": [
                {
                    "schema": schema,
                    "name": table,
                    "columns": cols,
                }
                for schema, tables in schemas.items()
                for table, cols in tables.items()
            ],
        }
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc


def get_database_tree(connection_id: str):
    """Return a hierarchical tree of database objects (Schemas -> Categories -> Objects)."""
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError("Connection not found.")

    cfg = PgConnectRequest(**cfg_dict)
    dsn, kwargs = _dsn_from_config(cfg)

    # Note: We skip 'information_schema' and 'pg_catalog' to keep the tree clean for user data,
    # but the user can adjust this if they want to see system tables.
    tree = {}

    try:
        with _pg_connection(dsn, **kwargs) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # 1. Relations (Tables, Views, MViews, Foreign Tables, Sequences)
                cur.execute("""
                    SELECT 
                        n.nspname AS schema_name,
                        c.relname AS object_name,
                        c.relkind AS kind,
                        pg_relation_size(c.oid) AS size_bytes,
                        (SELECT count(*) FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped) AS column_count,
                        (SELECT count(*) FROM pg_constraint con WHERE con.conrelid = c.oid AND con.contype = 'f') AS fk_count,
                        (SELECT count(*) FROM pg_index idx WHERE idx.indrelid = c.oid AND NOT idx.indisprimary) AS index_count,
                        (SELECT a.attname FROM pg_index idx JOIN pg_attribute a ON a.attrelid = idx.indrelid AND a.attnum = ANY(idx.indkey) WHERE idx.indrelid = c.oid AND idx.indisprimary LIMIT 1) AS primary_key
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                      AND c.relkind IN ('r', 'v', 'm', 'f', 'S')
                """)
                relations = cur.fetchall()


                # 2. Functions & Procedures
                cur.execute("""
                    SELECT 
                        n.nspname AS schema_name,
                        p.proname AS object_name,
                        p.prokind AS kind,
                        pg_get_function_result(p.oid) AS return_type,
                        pg_get_function_identity_arguments(p.oid) AS arguments
                    FROM pg_proc p
                    JOIN pg_namespace n ON n.oid = p.pronamespace
                    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                """)
                routines = cur.fetchall()

        # Build the hierarchy
        for row in relations:
            schema = row["schema_name"]
            kind = row["kind"]
            name = row["object_name"]
            
            obj_data = {
                "name": name,
                "kind": kind,
                "size_bytes": row["size_bytes"] or 0,
                "column_count": row["column_count"] or 0,
                "fk_count": row.get("fk_count", 0),
                "index_count": row.get("index_count", 0),
                "primary_key": row.get("primary_key")
            }
            
            if schema not in tree:
                tree[schema] = {"Tables": [], "Views": [], "Materialized Views": [], "Foreign Tables": [], "Sequences": [], "Functions": []}
                
            if kind == 'r': tree[schema]["Tables"].append(obj_data)
            elif kind == 'v': tree[schema]["Views"].append(obj_data)
            elif kind == 'm': tree[schema]["Materialized Views"].append(obj_data)
            elif kind == 'f': tree[schema]["Foreign Tables"].append(obj_data)
            elif kind == 'S': tree[schema]["Sequences"].append(obj_data)

        for row in routines:
            schema = row["schema_name"]
            name = row["object_name"]
            
            obj_data = {
                "name": name,
                "kind": row["kind"],
                "return_type": row["return_type"],
                "arguments": row["arguments"]
            }
            
            if schema not in tree:
                 tree[schema] = {"Tables": [], "Views": [], "Materialized Views": [], "Foreign Tables": [], "Sequences": [], "Functions": []}
                 
            # Note: prokind 'a' is aggregate, 'w' is window, 'f' is normal function, 'p' is procedure
            tree[schema]["Functions"].append(obj_data)

        # Sort everything
        for schema in tree.keys():
            for category in tree[schema].keys():
                tree[schema][category].sort(key=lambda x: x["name"])
                
        # Remove empty categories to keep the UI clean
        cleaned_tree = {}
        for schema, categories in tree.items():
            cleaned_tree[schema] = {k: v for k, v in categories.items() if v}
            
        # Return sorted dictionary
        return dict(sorted(cleaned_tree.items()))

    except Exception as exc:
        raise RuntimeError(str(exc)) from exc

# ---------------------------------------------------------------------------
# Object Details
# ---------------------------------------------------------------------------

def get_object_details(
    connection_id: str,
    schema: str = "public",
    name: str = "test",
    category: str = "table",
    tab: str = "overview"
):
    """
    Fetch specific details for a database object depending on the requested tab.
    Categories: table, view, materialized_view, sequence, function.
    Tabs: overview, columns, constraints, indexes, dependencies, ddl, arguments, definition
    """
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError(f"Connection {connection_id} not found")

    cfg = PgConnectRequest(**cfg_dict)
    dsn, kwargs = _dsn_from_config(cfg)

    try:
        with _pg_connection(dsn, **kwargs) as conn:
            with conn.cursor() as cur:
                if tab == "overview":
                    return _get_overview(cur, schema, name, category)
                elif tab == "columns":
                    return _get_columns(cur, schema, name)
                elif tab == "constraints":
                    return _get_constraints(cur, schema, name)
                elif tab == "indexes":
                    return _get_indexes(cur, schema, name)
                elif tab == "ddl":
                    return _get_ddl(cur, schema, name, category)
                elif tab == "er_diagram":
                    return _get_er_diagram(cur, schema, name)
                elif tab == "arguments" and category == "function":
                    return _get_function_arguments(cur, schema, name)
                elif tab == "definition" and category == "function":
                    return _get_function_definition(cur, schema, name)
                elif tab == "dependencies":
                    return _get_dependencies(cur, schema, name, category)
                elif tab == "sample_data":
                    return _get_sample_data(cur, conn, schema, name)
                else:
                    return {"error": f"Tab '{tab}' not supported for category '{category}'"}
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc

def _get_overview(cur, schema: str, name: str, category: str):
    data = {"schema": schema, "name": name, "category": category}
    
    if category in ["table", "view", "materialized_view"]:
        # Get relation size
        cur.execute(f"SELECT pg_total_relation_size('\"{schema}\".\"{name}\"')")
        size = cur.fetchone()[0]
        data["size_bytes"] = size

        # Always fetch exact row count for object overview.
        cur.execute(f'SELECT count(*) FROM "{schema}"."{name}"')
        data["row_count"] = cur.fetchone()[0]

        # Fetch foreign keys
        cur.execute('''
            SELECT
                kcu.column_name,
                ccu.table_schema AS ref_schema,
                ccu.table_name   AS ref_table,
                ccu.column_name  AS ref_column
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.referential_constraints rc
                ON kcu.constraint_name = rc.constraint_name
                AND kcu.constraint_schema = rc.constraint_schema
            JOIN information_schema.constraint_column_usage ccu
                ON rc.unique_constraint_name = ccu.constraint_name
                AND rc.unique_constraint_schema = ccu.constraint_schema
            WHERE kcu.table_schema = %s AND kcu.table_name = %s
        ''', (schema, name))
        data["foreign_keys"] = []
        for r in cur.fetchall():
            data["foreign_keys"].append({
                "column": r[0],
                "ref_schema": r[1],
                "ref_table": r[2],
                "ref_column": r[3]
            })

    if category == "sequence":
        cur.execute('''
            SELECT
                data_type,
                start_value,
                increment_by,
                max_value,
                min_value,
                cache_size,
                cycle
            FROM pg_sequences
            WHERE schemaname = %s AND sequencename = %s
        ''', (schema, name))
        row = cur.fetchone()
        if row:
            data["type"] = row[0]
            data["start_value"] = row[1]
            data["increment"] = row[2]
            data["max_value"] = row[3]
            data["min_value"] = row[4]
            data["cache_size"] = row[5]
            data["cycle"] = row[6]

    return data

def _get_columns(cur, schema: str, name: str):
    # Using information_schema.columns which is safer and less prone to tuple index errors
    cur.execute('''
        SELECT 
            c.column_name,
            CASE
                WHEN t.typtype = 'e' THEN 'ENUM'
                WHEN t.typtype = 'd' AND t.typbasetype <> 0 THEN format_type(t.typbasetype, t.typtypmod)
                ELSE format_type(a.atttypid, a.atttypmod)
            END AS data_type,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_nullable,
            c.column_default,
            COALESCE(col_description((c.table_schema || '.' || c.table_name)::regclass, c.ordinal_position), '') as description
        FROM information_schema.columns c
        LEFT JOIN pg_namespace n ON n.nspname = c.table_schema
        LEFT JOIN pg_class cls ON cls.relname = c.table_name AND cls.relnamespace = n.oid
        LEFT JOIN pg_attribute a ON a.attrelid = cls.oid AND a.attname = c.column_name AND a.attnum > 0 AND NOT a.attisdropped
        LEFT JOIN pg_type t ON t.oid = a.atttypid
        WHERE c.table_schema = %s AND c.table_name = %s
        ORDER BY c.ordinal_position
    ''', (schema, name))
    
    # We also need to know which columns are primary keys
    cur.execute('''
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = %s AND tc.table_name = %s
          AND tc.constraint_type = 'PRIMARY KEY'
    ''', (schema, name))
    pk_cols = {row[0] for row in cur.fetchall()}
    
    # Re-fetch columns with dict cursor for safety
    cur.execute('''
        SELECT 
            c.column_name,
            CASE
                WHEN t.typtype = 'e' THEN 'ENUM'
                WHEN t.typtype = 'd' AND t.typbasetype <> 0 THEN format_type(t.typbasetype, t.typtypmod)
                ELSE format_type(a.atttypid, a.atttypmod)
            END AS data_type,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_nullable,
            c.column_default,
            COALESCE(col_description((c.table_schema || '.' || c.table_name)::regclass, c.ordinal_position), '') as description
        FROM information_schema.columns c
        LEFT JOIN pg_namespace n ON n.nspname = c.table_schema
        LEFT JOIN pg_class cls ON cls.relname = c.table_name AND cls.relnamespace = n.oid
        LEFT JOIN pg_attribute a ON a.attrelid = cls.oid AND a.attname = c.column_name AND a.attnum > 0 AND NOT a.attisdropped
        LEFT JOIN pg_type t ON t.oid = a.atttypid
        WHERE c.table_schema = %s AND c.table_name = %s
        ORDER BY c.ordinal_position
    ''', (schema, name))
    
    cols = []
    # Using dict cursor keys if available, else tuple indices but explicitly checked
    for row in cur.fetchall():
        col_name = row[0] if isinstance(row, tuple) else row.get("column_name")
        data_type = row[1] if isinstance(row, tuple) else row.get("data_type")
        max_len = row[2] if isinstance(row, tuple) else row.get("character_maximum_length")
        num_prec = row[3] if isinstance(row, tuple) else row.get("numeric_precision")
        num_scale = row[4] if isinstance(row, tuple) else row.get("numeric_scale")
        is_nulllable_raw = row[5] if isinstance(row, tuple) else row.get("is_nullable")
        col_def = row[6] if isinstance(row, tuple) else row.get("column_default")
        desc = row[7] if isinstance(row, tuple) else row.get("description")
        
        # Parse nullable
        is_null = True
        if isinstance(is_nulllable_raw, str):
            is_null = is_nulllable_raw.upper() == 'YES'
        elif is_nulllable_raw is not None:
            is_null = bool(is_nulllable_raw)
            
        # Parse auto-increment
        is_auto = False
        if col_def and "nextval(" in str(col_def):
            is_auto = True
            
        cols.append({
            "name": col_name,
            "type": _normalize_pg_display_type(str(data_type or "")),
            "max_length": max_len,
            "precision": num_prec,
            "scale": num_scale,
            "nullable": is_null,
            "default": str(col_def) if col_def else None,
            "description": desc if desc else None,
            "is_auto": is_auto,
            "is_primary_key": col_name in pk_cols
        })
    return cols

def _get_constraints(cur, schema: str, name: str):
    # Simplified constraints fetch
    cur.execute('''
        SELECT conname, contype, pg_get_constraintdef(c.oid)
        FROM pg_constraint c
        JOIN pg_class t ON c.conrelid = t.oid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = %s AND t.relname = %s
    ''', (schema, name))
    
    constraints = []
    for row in cur.fetchall():
        ctype = row[1]
        type_str = "Primary Key" if ctype == 'p' else "Foreign Key" if ctype == 'f' else "Unique" if ctype == 'u' else "Check" if ctype == 'c' else ctype
        constraints.append({
            "name": row[0],
            "type": type_str,
            "definition": row[2]
        })
    return constraints

def _get_indexes(cur, schema: str, name: str):
    # Filter out primary keys to match the Overview count
    cur.execute('''
        SELECT i.indexname, i.indexdef
        FROM pg_indexes i
        JOIN pg_class c ON c.relname = i.indexname
        JOIN pg_index idx ON idx.indexrelid = c.oid
        WHERE i.schemaname = %s AND i.tablename = %s
          AND NOT idx.indisprimary
    ''', (schema, name))
    return [{"name": row[0], "definition": row[1]} for row in cur.fetchall()]

def _get_ddl(cur, schema: str, name: str, category: str):
    if category in ["view", "materialized_view"]:
        cur.execute(f"SELECT pg_get_viewdef('\"{schema}\".\"{name}\"', true)")
        row = cur.fetchone()
        if not row:
            return {"ddl": ""}
        object_kind = "MATERIALIZED VIEW" if category == "materialized_view" else "OR REPLACE VIEW"
        return {"ddl": f"CREATE {object_kind} \"{schema}\".\"{name}\" AS\n{row[0]}"}
    
    if category == "table":
        # 1. Get Columns
        cols = _get_columns(cur, schema, name)
        
        # 2. Get Constraints
        constrs = _get_constraints(cur, schema, name)
        
        # 3. Get Indexes
        idxs = _get_indexes(cur, schema, name)
        
        ddl_lines = []
        ddl_lines.append(f'CREATE TABLE "{schema}"."{name}" (')
        
        # Format columns
        col_lines = []
        for c in cols:
            line = f'    "{c["name"]}" {c["type"]}'
            if not c.get("nullable", True):
                line += " NOT NULL"
            if c.get("default"):
                line += f" DEFAULT {c['default']}"
            col_lines.append(line)
            
        # Format table-level constraints (PKs, FKs, UQs)
        for con in constrs:
            col_lines.append(f'    CONSTRAINT "{con["name"]}" {con["definition"]}')
            
        ddl_lines.append(",\n".join(col_lines))
        ddl_lines.append(");\n")
        
        # Format indexes
        for idx in idxs:
            # The definition returned by pg_indexes is usually a fully formed CREATE INDEX statement
            if idx.get("definition"):
                ddl_lines.append(f"{idx['definition']};")

        # Fetch foreign keys for ER diagram
        cur.execute('''
            SELECT
                kcu.column_name,
                ccu.table_schema AS ref_schema,
                ccu.table_name   AS ref_table,
                ccu.column_name  AS ref_column
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.referential_constraints rc
                ON kcu.constraint_name = rc.constraint_name
                AND kcu.constraint_schema = rc.constraint_schema
            JOIN information_schema.constraint_column_usage ccu
                ON rc.unique_constraint_name = ccu.constraint_name
                AND rc.unique_constraint_schema = ccu.constraint_schema
            WHERE kcu.table_schema = %s AND kcu.table_name = %s
        ''', (schema, name))
        foreign_keys = []
        for r in cur.fetchall():
            foreign_keys.append({
                "column": r[0],
                "ref_schema": r[1],
                "ref_table": r[2],
                "ref_column": r[3]
            })
                
        return {
            "ddl": "\n".join(ddl_lines),
            "foreign_keys": foreign_keys,
            "columns": cols
        }
        
    if category == "sequence":
        cur.execute('''
            SELECT
                data_type,
                start_value,
                increment_by,
                min_value,
                max_value,
                cache_size,
                cycle
            FROM pg_sequences
            WHERE schemaname = %s AND sequencename = %s
        ''', (schema, name))
        row = cur.fetchone()
        if not row:
            return {"ddl": ""}
        return {
            "ddl": "\n".join([
                f'CREATE SEQUENCE "{schema}"."{name}"',
                f"    AS {row[0]}",
                f"    INCREMENT BY {row[2]}",
                f"    MINVALUE {row[3]}",
                f"    MAXVALUE {row[4]}",
                f"    START WITH {row[1]}",
                f"    CACHE {row[5]}",
                f"    {'CYCLE' if row[6] else 'NO CYCLE'};",
            ]),
            "foreign_keys": [],
            "columns": [],
        }

    if category == "function":
        cur.execute('''
            SELECT pg_get_functiondef(p.oid)
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = %s AND p.proname = %s
            LIMIT 1
        ''', (schema, name))
        row = cur.fetchone()
        return {"ddl": row[0] if row else "", "foreign_keys": [], "columns": []}

    return {"ddl": f"-- DDL generation for {category} is not supported", "foreign_keys": [], "columns": []}

def _get_er_diagram(cur, schema: str, name: str):
    # Fetch foreign keys where the current table is the child (outgoing references)
    cur.execute('''
        SELECT
            kcu.column_name,
            ccu.table_schema AS ref_schema,
            ccu.table_name   AS ref_table,
            ccu.column_name  AS ref_column
        FROM information_schema.key_column_usage kcu
        JOIN information_schema.referential_constraints rc
            ON kcu.constraint_name = rc.constraint_name
            AND kcu.constraint_schema = rc.constraint_schema
        JOIN information_schema.constraint_column_usage ccu
            ON rc.unique_constraint_name = ccu.constraint_name
            AND rc.unique_constraint_schema = ccu.constraint_schema
        WHERE kcu.table_schema = %s AND kcu.table_name = %s
    ''', (schema, name))
    
    outgoing_fks = []
    related_tables = set([(schema, name)])
    for r in cur.fetchall():
        outgoing_fks.append({
            "column": r[0],
            "ref_schema": r[1],
            "ref_table": r[2],
            "ref_column": r[3]
        })
        related_tables.add((r[1], r[2]))
        
    # Fetch foreign keys where the current table is the parent (incoming references)
    cur.execute('''
        SELECT
            kcu.table_schema AS source_schema,
            kcu.table_name   AS source_table,
            kcu.column_name  AS source_column,
            ccu.column_name  AS ref_column
        FROM information_schema.key_column_usage kcu
        JOIN information_schema.referential_constraints rc
            ON kcu.constraint_name = rc.constraint_name
            AND kcu.constraint_schema = rc.constraint_schema
        JOIN information_schema.constraint_column_usage ccu
            ON rc.unique_constraint_name = ccu.constraint_name
            AND rc.unique_constraint_schema = ccu.constraint_schema
        WHERE ccu.table_schema = %s AND ccu.table_name = %s
    ''', (schema, name))
    
    incoming_fks = []
    for r in cur.fetchall():
        incoming_fks.append({
            "source_schema": r[0],
            "source_table": r[1],
            "source_column": r[2],
            "ref_column": r[3]
        })
        related_tables.add((r[0], r[1]))

    # Now fetch columns for the main table AND all related tables
    table_columns = {}
    for t_schema, t_name in related_tables:
        cols = _get_columns(cur, t_schema, t_name)
        if t_schema not in table_columns:
            table_columns[t_schema] = {}
        table_columns[t_schema][t_name] = cols
            
    return {
        "table_columns": table_columns, 
        "outgoing_fks": outgoing_fks,
        "incoming_fks": incoming_fks
    }

def _get_function_arguments(cur, schema: str, name: str):
    # Simplified
    return []

def _get_function_definition(cur, schema: str, name: str):
    cur.execute('''
        SELECT pg_get_functiondef(p.oid)
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = %s AND p.proname = %s
        LIMIT 1
    ''', (schema, name))
    row = cur.fetchone()
    return {"definition": row[0] if row else ""}

def _get_dependencies(cur, schema: str, name: str, category: str):
    return {"referenced_by": [], "references": []}

def _get_sample_data(cur, conn, schema: str, name: str):
    cur.execute(f'SELECT * FROM "{schema}"."{name}" LIMIT 20')
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description] if cur.description else []
    col_meta = _extract_column_meta(cur, conn, cols) if cols else []

    return {"columns": cols, "column_meta": col_meta, "rows": _rows_to_records(cols, rows)}

# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

def health():
    return {"status": "ok"}




