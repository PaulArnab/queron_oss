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
        "krbsrvname": binding.get("krbsrvname"),
        "gssencmode": binding.get("gssencmode"),
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
        "auth_mode": binding.get("auth_mode"),
        "ssl_server_certificate": binding.get("ssl_server_certificate"),
        "ssl_client_keystoredb": binding.get("ssl_client_keystoredb"),
        "ssl_client_keystash": binding.get("ssl_client_keystash"),
        "ssl_client_keystore_password": binding.get("ssl_client_keystore_password"),
        "ssl_client_label": binding.get("ssl_client_label"),
        "connect_timeout_seconds": binding.get("connect_timeout_seconds"),
        "save_password": False,
    }
    db2_core.connect(Db2ConnectRequest(**payload))
    return connection_id


def build_mssql_request_payload(binding: dict[str, Any], config_name: str) -> dict[str, Any]:
    return {
        "name": str(binding.get("name") or config_name or "mssql"),
        "host": binding.get("host", "localhost"),
        "port": binding.get("port", 1433),
        "database": binding.get("database", "master"),
        "username": binding.get("username"),
        "password": binding.get("password"),
        "url": binding.get("url"),
        "auth_mode": binding.get("auth_mode"),
        "driver": binding.get("driver"),
        "encrypt": binding.get("encrypt"),
        "trust_server_certificate": binding.get("trust_server_certificate"),
        "timeout_seconds": binding.get("timeout_seconds"),
        "save_password": False,
    }


def ensure_mssql_binding(binding: dict[str, Any], config_name: str) -> str:
    import mssql_core
    from base import MssqlConnectRequest

    connection_id = str(binding.get("connection_id") or "").strip() or runtime_connection_id("mssql", config_name, binding)
    if connection_id in mssql_core._connections:
        return connection_id

    payload = {
        "connection_id": connection_id,
        **build_mssql_request_payload(binding, config_name),
    }
    mssql_core.connect(MssqlConnectRequest(**payload))
    return connection_id


def build_mysql_request_payload(binding: dict[str, Any], config_name: str) -> dict[str, Any]:
    return {
        "name": str(binding.get("name") or config_name or "mysql"),
        "host": binding.get("host", "localhost"),
        "port": binding.get("port", 3306),
        "database": binding.get("database", "mysql"),
        "username": binding.get("username"),
        "password": binding.get("password"),
        "url": binding.get("url") or binding.get("uri"),
        "auth_mode": binding.get("auth_mode"),
        "ssl_ca": binding.get("ssl_ca"),
        "ssl_cert": binding.get("ssl_cert"),
        "ssl_key": binding.get("ssl_key"),
        "unix_socket": binding.get("unix_socket"),
        "connect_timeout_seconds": binding.get("connect_timeout_seconds"),
        "save_password": False,
    }


def ensure_mysql_binding(binding: dict[str, Any], config_name: str) -> str:
    import mysql_core
    from base import MysqlConnectRequest

    connection_id = str(binding.get("connection_id") or "").strip() or runtime_connection_id("mysql", config_name, binding)
    if connection_id in mysql_core._connections:
        return connection_id

    payload = {
        "connection_id": connection_id,
        **build_mysql_request_payload(binding, config_name),
    }
    mysql_core.connect(MysqlConnectRequest(**payload))
    return connection_id


def build_mariadb_request_payload(binding: dict[str, Any], config_name: str) -> dict[str, Any]:
    return {
        "name": str(binding.get("name") or config_name or "mariadb"),
        "host": binding.get("host", "localhost"),
        "port": binding.get("port", 3306),
        "database": binding.get("database", "mariadb"),
        "username": binding.get("username"),
        "password": binding.get("password"),
        "url": binding.get("url") or binding.get("uri"),
        "auth_mode": binding.get("auth_mode"),
        "ssl_ca": binding.get("ssl_ca"),
        "ssl_cert": binding.get("ssl_cert"),
        "ssl_key": binding.get("ssl_key"),
        "unix_socket": binding.get("unix_socket"),
        "connect_timeout_seconds": binding.get("connect_timeout_seconds"),
        "save_password": False,
    }


def ensure_mariadb_binding(binding: dict[str, Any], config_name: str) -> str:
    try:
        import mariadb_core
    except ImportError:
        import importlib.util
        from pathlib import Path

        module_path = Path(__file__).resolve().parents[1] / "mariadb_core.py"
        spec = importlib.util.spec_from_file_location("mariadb_core", module_path)
        if spec is None or spec.loader is None:
            raise
        mariadb_core = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mariadb_core)
    from base import MariaDbConnectRequest

    connection_id = str(binding.get("connection_id") or "").strip() or runtime_connection_id("mariadb", config_name, binding)
    if connection_id in mariadb_core._connections:
        return connection_id

    payload = {
        "connection_id": connection_id,
        **build_mariadb_request_payload(binding, config_name),
    }
    mariadb_core.connect(MariaDbConnectRequest(**payload))
    return connection_id
