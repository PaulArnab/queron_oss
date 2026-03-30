"""
Loom Backend - IBM Db2 LUW Connector
====================================
Db2 LUW driver module using the IBM Python driver with a DB-API style wrapper.
"""

from __future__ import annotations

import os
import re
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

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
    from .base import (
        ColumnMeta,
        ConnectResponse,
        DEFAULT_QUERY_CHUNK_SIZE,
        Db2ConnectRequest,
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
        Db2ConnectRequest,
        PgQueryChunkRequest,
        PgQueryRequest,
        QueryResponse,
        TestConnectionResponse,
    )
    from type_mapper import normalize_display_type

_connections: dict[str, dict[str, Any]] = {}
_query_sessions: dict[str, dict[str, Any]] = {}
_QUERY_SESSION_IDLE_TTL_SECONDS = 300
_DEFAULT_QUERY_CHUNK_SIZE = DEFAULT_QUERY_CHUNK_SIZE


def _require_driver() -> None:
    if ibm_db is None or ibm_db_dbi is None:
        raise RuntimeError("IBM Db2 driver is not installed. Install the 'ibm-db' package first.")


def _sanitize_conn_value(value: Any) -> str:
    return str(value).strip()


def _conn_str_from_parts(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
) -> str:
    parts = [
        f"DATABASE={_sanitize_conn_value(database)}",
        f"HOSTNAME={_sanitize_conn_value(host)}",
        f"PORT={int(port)}",
        "PROTOCOL=TCPIP",
    ]
    if username:
        parts.append(f"UID={_sanitize_conn_value(username)}")
    if password:
        parts.append(f"PWD={password}")
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


def _dsn_from_config(cfg: Db2ConnectRequest) -> str:
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
        return conn_str

    return _conn_str_from_parts(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        username=cfg.username,
        password=cfg.password,
    )


def _dbapi_connect(conn_str: str):
    _require_driver()
    try:
        return ibm_db_dbi.connect(conn_str, "", "")
    except AttributeError:
        raw_conn = ibm_db.connect(conn_str, "", "")
        return ibm_db_dbi.Connection(raw_conn)


@contextmanager
def _db2_connection(conn_str: str):
    conn = _dbapi_connect(conn_str)
    try:
        yield conn
    finally:
        conn.close()


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


def _type_name_from_description(desc) -> str:
    type_code = desc[1] if len(desc) > 1 else None
    if type_code is None:
        return "UNKNOWN"
    if isinstance(type_code, str):
        return _normalize_db2_display_type(type_code)
    name = getattr(type_code, "__name__", None)
    if name:
        return _normalize_db2_display_type(name)
    return _normalize_db2_display_type(str(type_code))


def _normalize_db2_display_type(type_name: str) -> str:
    return normalize_display_type("db2", type_name)


def _build_basic_column_meta(cursor) -> list[ColumnMeta]:
    if cursor.description is None:
        return []

    metas: list[ColumnMeta] = []
    for desc in cursor.description:
        internal_size = desc[3] if len(desc) > 3 else None
        precision = desc[4] if len(desc) > 4 else None
        scale = desc[5] if len(desc) > 5 else None
        null_ok = desc[6] if len(desc) > 6 else None
        metas.append(
            ColumnMeta(
                name=desc[0],
                data_type=_type_name_from_description(desc),
                max_length=internal_size if isinstance(internal_size, int) and internal_size >= 0 else None,
                precision=precision,
                scale=scale,
                nullable=True if null_ok is None else bool(null_ok),
            )
        )
    return metas


def _get_catalog_column_meta(cur, schema: str, name: str) -> list[ColumnMeta]:
    cur.execute(
        """
        SELECT
            RTRIM(COLNAME),
            RTRIM(TYPENAME),
            LENGTH,
            SCALE,
            CASE WHEN NULLS = 'Y' THEN 1 ELSE 0 END,
            CASE WHEN IDENTITY = 'Y' THEN 1 ELSE 0 END,
            COALESCE(REMARKS, '')
        FROM SYSCAT.COLUMNS
        WHERE TABSCHEMA = ? AND TABNAME = ?
        ORDER BY COLNO
        """,
        (schema, name),
    )
    rows = cur.fetchall()
    return [
        ColumnMeta(
            name=row[0],
            data_type=_normalize_db2_display_type(str(row[1] or "")),
            max_length=int(row[2]) if row[2] is not None else None,
            scale=int(row[3]) if row[3] is not None else None,
            nullable=bool(row[4]),
            auto_increment=bool(row[5]),
            description=row[6],
        )
        for row in rows
    ]


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


def _strip_sql_terminator(sql: str) -> str:
    return sql.strip().rstrip(";")


def _build_paged_sql(sql: str) -> str:
    base_sql = _strip_sql_terminator(sql)
    return f"SELECT * FROM ({base_sql}) AS loom_paged_query OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"


def _run_statement_query(req: PgQueryRequest, conn_str: str) -> QueryResponse:
    schema_changed = _is_schema_change_query(req.sql)
    with _db2_connection(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(req.sql)
            if cur.description is None:
                try:
                    conn.commit()
                except Exception:
                    pass
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
            column_meta = _enrich_column_meta_from_catalog(conn, req.sql, _build_basic_column_meta(cur))
            rows = cur.fetchmany(req.chunk_size + 1)
            has_more = len(rows) > req.chunk_size
            emitted_rows = rows[:req.chunk_size]
            serialized_rows = _rows_to_records(columns, emitted_rows)
            return QueryResponse(
                columns=columns,
                column_meta=column_meta,
                rows=serialized_rows,
                row_count=len(serialized_rows),
                has_more=has_more,
                chunk_size=req.chunk_size,
                mode="statement",
            )


def _fetch_offset_chunk(session: dict[str, Any], chunk_size: int) -> QueryResponse:
    offset = session.get("offset", 0)
    with _db2_connection(session["conn_str"]) as conn:
        with conn.cursor() as cur:
            cur.execute(_build_paged_sql(session["sql"]), (offset, chunk_size + 1))
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


def _open_offset_query_session(req: PgQueryRequest, conn_str: str) -> QueryResponse:
    with _db2_connection(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(_build_paged_sql(req.sql), (0, req.chunk_size + 1))
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
            column_meta = _enrich_column_meta_from_catalog(conn, req.sql, _build_basic_column_meta(cur))
            rows = cur.fetchall()

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
        "conn_str": conn_str,
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


def test_connection(req: Db2ConnectRequest):
    conn_str = _dsn_from_config(req)
    try:
        with _db2_connection(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
                cur.fetchone()
        return TestConnectionResponse(success=True, message="Connection successful!")
    except Exception as exc:
        return TestConnectionResponse(success=False, message=str(exc))


def connect(req: Db2ConnectRequest):
    conn_str = _dsn_from_config(req)
    try:
        with _db2_connection(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
                cur.fetchone()
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc

    conn_id = (req.connection_id or "").strip() or str(uuid.uuid4())
    _connections[conn_id] = req.model_dump()
    return ConnectResponse(connection_id=conn_id, message="Connected successfully!")


def run_query(req: PgQueryRequest):
    cfg_dict = _connections.get(req.connection_id)
    if cfg_dict is None:
        raise LookupError("Connection not found. Please connect first.")

    conn_str = _dsn_from_config(Db2ConnectRequest(**cfg_dict))
    _cleanup_expired_query_sessions()
    try:
        if _is_incremental_query(req.sql):
            return _open_offset_query_session(req, conn_str)
        return _run_statement_query(req, conn_str)
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

    conn_str = _dsn_from_config(Db2ConnectRequest(**cfg_dict))
    sql = """
        SELECT RTRIM(TABSCHEMA), RTRIM(TABNAME), RTRIM(COLNAME), RTRIM(TYPENAME)
        FROM SYSCAT.COLUMNS
        WHERE HIDDEN <> 'Y'
          AND TABSCHEMA NOT LIKE 'SYS%'
        ORDER BY TABSCHEMA, TABNAME, COLNO
    """
    try:
        with _db2_connection(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()

        schemas: dict[str, dict[str, list[dict[str, str]]]] = {}
        for schema_name, table_name, column_name, type_name in rows:
            schemas.setdefault(schema_name, {}).setdefault(table_name, []).append(
                {"name": column_name, "type": type_name}
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

    conn_str = _dsn_from_config(Db2ConnectRequest(**cfg_dict))
    sql = """
        SELECT RTRIM(TABSCHEMA), RTRIM(TABNAME), TYPE, COALESCE(COLCOUNT, 0)
        FROM SYSCAT.TABLES
        WHERE TABSCHEMA NOT LIKE 'SYS%'
          AND TYPE IN ('T', 'V', 'A', 'N')
        ORDER BY TABSCHEMA, TABNAME
    """
    kind_map = {
        "T": "Tables",
        "V": "Views",
        "A": "Aliases",
        "N": "Nicknames",
    }

    try:
        with _db2_connection(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()

        tree: dict[str, dict[str, list[dict[str, Any]]]] = {}
        for schema_name, object_name, object_type, column_count in rows:
            category = kind_map.get(object_type, "Tables")
            tree.setdefault(schema_name, {}).setdefault(category, []).append(
                {
                    "name": object_name,
                    "kind": object_type,
                    "column_count": int(column_count or 0),
                }
            )

        for schema_name, categories in tree.items():
            for category_rows in categories.values():
                category_rows.sort(key=lambda item: item["name"])

        return dict(sorted(tree.items()))
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc


def get_object_details(
    connection_id: str,
    schema: str = "DB2INST1",
    name: str = "TEST",
    category: str = "table",
    tab: str = "overview",
):
    cfg_dict = _connections.get(connection_id)
    if cfg_dict is None:
        raise LookupError(f"Connection {connection_id} not found")

    conn_str = _dsn_from_config(Db2ConnectRequest(**cfg_dict))
    try:
        with _db2_connection(conn_str) as conn:
            with conn.cursor() as cur:
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
                if tab == "dependencies":
                    return _get_dependencies(cur, schema, name, category)
                if tab == "sample_data":
                    return _get_sample_data(cur, schema, name)
                return {"error": f"Tab '{tab}' not supported for category '{category}'"}
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc


def _get_overview(cur, schema: str, name: str, category: str):
    data = {"schema": schema, "name": name, "category": category}
    cur.execute(
        """
        SELECT TYPE, COALESCE(COLCOUNT, 0), REMARKS
        FROM SYSCAT.TABLES
        WHERE TABSCHEMA = ? AND TABNAME = ?
        """,
        (schema, name),
    )
    row = cur.fetchone()
    if row:
        data["kind"] = row[0]
        data["column_count"] = int(row[1] or 0)
        data["description"] = row[2] or ""
    data["index_count"] = 0
    data["fk_count"] = 0
    data["primary_key"] = None
    data["foreign_keys"] = []
    data["row_count"] = 0
    data["size_bytes"] = 0
    if category in {"table", "view", "materialized_view"}:
        try:
            sql = f"SELECT COUNT(*) FROM {_quote_identifier(schema)}.{_quote_identifier(name)}"
            cur.execute(sql)
            count_row = cur.fetchone()
            if count_row:
                data["row_count"] = int(count_row[0] or 0)
        except Exception:
            pass
    if category == "table":
        try:
            constraints = _get_constraints(cur, schema, name)
            data["fk_count"] = sum(1 for con in constraints if con.get("type") == "FOREIGN KEY")
            pk_constraints = [con for con in constraints if con.get("type") == "PRIMARY KEY"]
            if pk_constraints:
                data["primary_key"] = pk_constraints[0].get("name")
        except Exception:
            pass

        try:
            indexes = _get_indexes(cur, schema, name)
            data["index_count"] = len(indexes)
        except Exception:
            pass

        try:
            cur.execute(
                """
                SELECT
                    RTRIM(k.COLNAME) AS column_name,
                    RTRIM(r.REFTABSCHEMA) AS ref_schema,
                    RTRIM(r.REFTABNAME) AS ref_table
                FROM SYSCAT.REFERENCES r
                JOIN SYSCAT.KEYCOLUSE k
                    ON k.TABSCHEMA = r.TABSCHEMA
                   AND k.TABNAME = r.TABNAME
                   AND k.CONSTNAME = r.CONSTNAME
                WHERE r.TABSCHEMA = ? AND r.TABNAME = ?
                ORDER BY r.CONSTNAME, k.COLSEQ
                """,
                (schema, name),
            )
            fk_rows = cur.fetchall()
            data["foreign_keys"] = [
                {
                    "column": row[0],
                    "ref_schema": row[1],
                    "ref_table": row[2],
                    "ref_column": "",
                }
                for row in fk_rows
            ]
        except Exception:
            data["foreign_keys"] = []
    return data


def _get_columns(cur, schema: str, name: str):
    cur.execute(
        """
        SELECT COLNAME, TYPENAME, LENGTH, SCALE, NULLS, DEFAULT, REMARKS, IDENTITY
        FROM SYSCAT.COLUMNS
        WHERE TABSCHEMA = ? AND TABNAME = ?
        ORDER BY COLNO
        """,
        (schema, name),
    )
    rows = cur.fetchall()
    return [
        {
            "name": row[0],
            "type": _normalize_db2_display_type(str(row[1] or "")),
            "length": row[2],
            "scale": row[3],
            "nullable": row[4] == "Y",
            "default": row[5],
            "description": row[6] or "",
            "auto_increment": row[7] == "Y",
        }
        for row in rows
    ]


def _get_constraints(cur, schema: str, name: str):
    cur.execute(
        """
        SELECT CONSTNAME, TYPE, ENFORCED, TRUSTED
        FROM SYSCAT.TABCONST
        WHERE TABSCHEMA = ? AND TABNAME = ?
        ORDER BY CONSTNAME
        """,
        (schema, name),
    )
    rows = cur.fetchall()
    type_map = {"P": "PRIMARY KEY", "U": "UNIQUE", "F": "FOREIGN KEY", "K": "CHECK"}
    return [
        {
            "name": row[0],
            "type": type_map.get(row[1], row[1]),
            "enforced": row[2] == "Y",
            "trusted": row[3] == "Y",
        }
        for row in rows
    ]


def _get_indexes(cur, schema: str, name: str):
    cur.execute(
        """
        SELECT INDNAME, UNIQUERULE, COLNAMES
        FROM SYSCAT.INDEXES
        WHERE TABSCHEMA = ? AND TABNAME = ?
        ORDER BY INDNAME
        """,
        (schema, name),
    )
    rows = cur.fetchall()
    return [
        {
            "name": row[0],
            "unique_rule": row[1],
            "columns": row[2],
        }
        for row in rows
    ]


def _get_ddl(cur, schema: str, name: str, category: str):
    return {
        "ddl": f"-- Db2 {category} DDL lookup is not implemented yet for {schema}.{name}.",
    }


def _get_dependencies(cur, schema: str, name: str, category: str):
    return {"referenced_by": [], "references": []}


def _quote_identifier(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        escaped = identifier.replace('"', '""')
        return f'"{escaped}"'
    return identifier


def _parse_identifier_token(token: str) -> str:
    stripped = token.strip()
    if stripped.startswith('"') and stripped.endswith('"'):
        return stripped[1:-1].replace('""', '"')
    return stripped.upper()


def _extract_single_source_table(sql: str) -> tuple[str, str] | None:
    match = re.search(
        r"""
        \bfrom\b
        \s+
        (?P<schema>"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_]*)
        \s*\.\s*
        (?P<table>"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_]*)
        """,
        sql,
        re.IGNORECASE | re.VERBOSE,
    )
    if not match:
        return None
    return _parse_identifier_token(match.group("schema")), _parse_identifier_token(match.group("table"))


def _extract_db2_key_metadata(cur, schema: str, name: str) -> tuple[set[str], set[str], dict[str, dict[str, str | None]]]:
    pk_cols: set[str] = set()
    unique_cols: set[str] = set()
    fk_map: dict[str, dict[str, str | None]] = {}

    cur.execute(
        """
        SELECT RTRIM(k.COLNAME), c.TYPE
        FROM SYSCAT.TABCONST c
        JOIN SYSCAT.KEYCOLUSE k
            ON k.TABSCHEMA = c.TABSCHEMA
           AND k.TABNAME = c.TABNAME
           AND k.CONSTNAME = c.CONSTNAME
        WHERE c.TABSCHEMA = ? AND c.TABNAME = ?
          AND c.TYPE IN ('P', 'U')
        ORDER BY c.CONSTNAME, k.COLSEQ
        """,
        (schema, name),
    )
    for col_name, const_type in cur.fetchall():
        col_key = str(col_name or "").upper()
        if not col_key:
            continue
        if const_type == "P":
            pk_cols.add(col_key)
        elif const_type == "U":
            unique_cols.add(col_key)

    cur.execute(
        """
        SELECT
            RTRIM(k.COLNAME) AS column_name,
            RTRIM(r.REFTABSCHEMA) AS ref_schema,
            RTRIM(r.REFTABNAME) AS ref_table,
            RTRIM(pk.COLNAME) AS ref_column
        FROM SYSCAT.REFERENCES r
        JOIN SYSCAT.KEYCOLUSE k
            ON k.TABSCHEMA = r.TABSCHEMA
           AND k.TABNAME = r.TABNAME
           AND k.CONSTNAME = r.CONSTNAME
        LEFT JOIN SYSCAT.KEYCOLUSE pk
            ON pk.TABSCHEMA = r.REFTABSCHEMA
           AND pk.TABNAME = r.REFTABNAME
           AND pk.CONSTNAME = r.REFKEYNAME
           AND pk.COLSEQ = k.COLSEQ
        WHERE r.TABSCHEMA = ? AND r.TABNAME = ?
        ORDER BY r.CONSTNAME, k.COLSEQ
        """,
        (schema, name),
    )
    for col_name, ref_schema, ref_table, ref_column in cur.fetchall():
        col_key = str(col_name or "").upper()
        if not col_key:
            continue
        if col_key not in fk_map:
            fk_map[col_key] = {
                "schema": ref_schema or None,
                "table": ref_table or None,
                "column": ref_column or None,
            }

    return pk_cols, unique_cols, fk_map


def _enrich_column_meta_from_catalog(conn, sql: str, column_meta: list[ColumnMeta]) -> list[ColumnMeta]:
    source = _extract_single_source_table(sql)
    if source is None or not column_meta:
        return column_meta

    schema, name = source
    try:
        with conn.cursor() as meta_cur:
            catalog_meta = _get_catalog_column_meta(meta_cur, schema, name)
            pk_cols, unique_cols, fk_map = _extract_db2_key_metadata(meta_cur, schema, name)
    except Exception:
        return column_meta

    if not catalog_meta:
        return column_meta

    catalog_by_name = {meta.name.upper(): meta for meta in catalog_meta}
    enriched: list[ColumnMeta] = []
    for meta in column_meta:
        catalog_match = catalog_by_name.get(meta.name.upper())
        if catalog_match is None:
            enriched.append(meta)
            continue
        col_key = meta.name.upper()
        fk = fk_map.get(col_key)
        enriched.append(
            ColumnMeta(
                name=meta.name,
                data_type=catalog_match.data_type or meta.data_type,
                source_table=meta.source_table,
                max_length=catalog_match.max_length if catalog_match.max_length is not None else meta.max_length,
                precision=catalog_match.precision if catalog_match.precision is not None else meta.precision,
                scale=catalog_match.scale if catalog_match.scale is not None else meta.scale,
                nullable=catalog_match.nullable,
                auto_increment=catalog_match.auto_increment,
                description=catalog_match.description or meta.description,
                is_primary_key=col_key in pk_cols,
                is_unique=col_key in unique_cols,
                fk_ref_schema=fk["schema"] if fk else None,
                fk_ref_table=fk["table"] if fk else None,
                fk_ref_column=fk["column"] if fk else None,
            )
        )
    return enriched


def _get_sample_data(cur, schema: str, name: str):
    sql = (
        f"SELECT * FROM {_quote_identifier(schema)}.{_quote_identifier(name)} "
        "FETCH FIRST 20 ROWS ONLY"
    )
    cur.execute(sql)
    columns = _cursor_columns(cur)
    rows = cur.fetchall()
    column_meta = _get_catalog_column_meta(cur, schema, name)
    if not column_meta:
        column_meta = _build_basic_column_meta(cur)
    return {
        "columns": columns,
        "rows": _rows_to_records(columns, rows),
        "column_meta": column_meta,
    }


def health():
    return {"status": "ok"}
