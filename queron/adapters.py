from __future__ import annotations

import hashlib
import json
from typing import Any


def runtime_connection_id(prefix: str, config_name: str, binding: dict[str, Any]) -> str:
    serializable = json.dumps(binding, sort_keys=True, default=str)
    digest = hashlib.sha1(serializable.encode("utf-8")).hexdigest()[:12]
    safe_name = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(config_name or prefix))
    return f"queron-{prefix}-{safe_name}-{digest}"


def connect_duckdb_runtime(*, pipeline_id: str, database: str) -> str:
    import duckdb_core
    from base import DuckDbConnectRequest

    response = duckdb_core.connect(
        DuckDbConnectRequest(
            name=f"{pipeline_id}_pipeline",
            database=database,
        )
    )
    return response.connection_id


def ensure_postgres_binding(binding: dict[str, Any], config_name: str) -> str:
    import postgres_core
    from base import PgConnectRequest

    connection_id = str(binding.get("connection_id") or "").strip() or runtime_connection_id("pg", config_name, binding)
    if connection_id in postgres_core._connections:
        return connection_id

    payload = {
        "connection_id": connection_id,
        "name": str(binding.get("name") or config_name or connection_id),
        "host": binding.get("host", "localhost"),
        "port": binding.get("port", 5432),
        "database": binding.get("database", "postgres"),
        "username": binding.get("username", "postgres"),
        "password": binding.get("password", ""),
        "url": binding.get("url"),
        "auth_mode": binding.get("auth_mode"),
        "sslmode": binding.get("sslmode"),
        "sslrootcert": binding.get("sslrootcert"),
        "sslcert": binding.get("sslcert"),
        "sslkey": binding.get("sslkey"),
        "sslpassword": binding.get("sslpassword"),
        "connect_timeout_seconds": binding.get("connect_timeout_seconds"),
        "statement_timeout_ms": binding.get("statement_timeout_ms"),
        "save_password": False,
    }
    postgres_core.connect(PgConnectRequest(**payload))
    return connection_id


def ensure_db2_binding(binding: dict[str, Any], config_name: str) -> str:
    import db2_core
    from base import Db2ConnectRequest

    connection_id = str(binding.get("connection_id") or "").strip() or runtime_connection_id("db2", config_name, binding)
    if connection_id in db2_core._connections:
        return connection_id

    payload = {
        "connection_id": connection_id,
        "name": str(binding.get("name") or config_name or connection_id),
        "host": binding.get("host", "localhost"),
        "port": binding.get("port", 50000),
        "database": binding.get("database", "SAMPLE"),
        "username": binding.get("username", "db2inst1"),
        "password": binding.get("password", ""),
        "url": binding.get("url"),
        "connect_timeout_seconds": binding.get("connect_timeout_seconds"),
        "save_password": False,
    }
    db2_core.connect(Db2ConnectRequest(**payload))
    return connection_id
