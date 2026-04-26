from __future__ import annotations

import datetime
from decimal import Decimal
import io
import pathlib
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import base
from duckdb_driver import connect_duckdb
import mysql_core
import queron
import queron.cli
from queron.bindings import MysqlBinding, resolve_runtime_binding_payload
from queron.compiler import compile_pipeline_code
from queron.config import resolve_connection_binding


class _FakeMysqlCursor:
    def __init__(self, conn: "_FakeMysqlConnection") -> None:
        self.conn = conn
        self.executed: list[tuple[str, object | None]] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self._fetchone_result = None
        self.closed = False

    def execute(self, sql, params=None):
        text = str(sql)
        self.executed.append((text, params))
        upper = text.upper()
        if "INFORMATION_SCHEMA.TABLES" in upper or upper.startswith("SHOW TABLES"):
            self._fetchone_result = (1,) if self.conn.table_exists else None
        elif upper.startswith("DROP TABLE"):
            self.conn.table_exists = False
        elif upper.startswith("CREATE TABLE"):
            self.conn.table_exists = True
            self.conn.create_count += 1

    def executemany(self, sql, payload):
        rows = [tuple(row) for row in payload]
        self.executemany_calls.append((str(sql), rows))
        self.conn.inserted_rows.extend(rows)

    def fetchone(self):
        return self._fetchone_result

    def close(self):
        self.closed = True


class _FakeMysqlConnection:
    def __init__(self, *, table_exists: bool) -> None:
        self.table_exists = table_exists
        self.create_count = 0
        self.inserted_rows: list[tuple[object, ...]] = []
        self.cursor_obj = _FakeMysqlCursor(self)
        self.committed = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


class _FakeMysqlIngressCursor:
    def __init__(self, *, rows, description) -> None:
        self.description = description
        self._batches = [list(rows), []]
        self.executed: list[tuple[str, object | None]] = []
        self.closed = False

    def execute(self, sql, params=None):
        self.executed.append((str(sql), params))

    def fetchmany(self, size):
        _ = size
        return self._batches.pop(0)

    def close(self):
        self.closed = True


class _FakeMysqlIngressConnection:
    def __init__(self, *, rows, description) -> None:
        self.cursor_obj = _FakeMysqlIngressCursor(rows=rows, description=description)
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True


class _FakeMysqlConnectCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, object | None]] = []
        self.closed = False

    def execute(self, sql, params=None):
        self.executed.append((str(sql), params))

    def close(self):
        self.closed = True


class _FakeMysqlConnectConnection:
    def __init__(self) -> None:
        self.cursor_obj = _FakeMysqlConnectCursor()
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True


def _write_duckdb_source(db_path: pathlib.Path) -> None:
    conn = connect_duckdb(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE source_data AS
            SELECT 1::INTEGER AS policy_id, 'POL-001'::VARCHAR AS policy_number
            UNION ALL
            SELECT 2::INTEGER AS policy_id, 'POL-002'::VARCHAR AS policy_number
            """
        )
    finally:
        conn.close()


class VerifyMysqlAuthConfigTests(unittest.TestCase):
    def test_runtime_binding_accepts_uri_alias_and_mysql_tls_fields(self):
        payload = resolve_runtime_binding_payload(
            "MYSQL_LOCAL",
            {
                "type": "mariadb",
                "uri": "mysql://user:pass@localhost:53307/LOOMDB",
                "ssl_ca": "ca.crt",
                "ssl_cert": "client.crt",
                "ssl_key": "client.key",
                "unix_socket": "/tmp/mysql.sock",
            },
        )

        self.assertEqual(payload["type"], "mysql")
        self.assertEqual(payload["url"], "mysql://user:pass@localhost:53307/LOOMDB")
        self.assertEqual(payload["ssl_ca"], "ca.crt")
        self.assertEqual(payload["ssl_cert"], "client.crt")
        self.assertEqual(payload["ssl_key"], "client.key")
        self.assertEqual(payload["unix_socket"], "/tmp/mysql.sock")

    def test_mysql_binding_resolves_tls_mtls_and_socket_fields(self):
        binding = MysqlBinding(
            host="localhost",
            port=53307,
            database="LOOMDB",
            username="loom_user",
            password="secret",
            ssl_ca="ca.crt",
            ssl_cert="client.crt",
            ssl_key="client.key",
            unix_socket="/tmp/mysql.sock",
            connect_timeout_seconds=3,
        )

        payload = binding.resolve_config("MYSQL_LOCAL")

        self.assertEqual(payload["type"], "mysql")
        self.assertEqual(payload["ssl_ca"], "ca.crt")
        self.assertEqual(payload["ssl_cert"], "client.crt")
        self.assertEqual(payload["ssl_key"], "client.key")
        self.assertEqual(payload["unix_socket"], "/tmp/mysql.sock")
        self.assertEqual(payload["connect_timeout_seconds"], 3)

    def test_connections_yaml_mysql_password_env(self):
        with patch.dict("os.environ", {"QUERON_MYSQL_PASSWORD": "EnvMysqlPass123!"}):
            payload = resolve_connection_binding(
                "MYSQL_ENV",
                {
                    "connections": {
                        "MYSQL_ENV": {
                            "type": "mysql",
                            "host": "localhost",
                            "port": 53307,
                            "database": "LOOMDB",
                            "username": "env_user",
                            "password_env": "QUERON_MYSQL_PASSWORD",
                        }
                    }
                },
            )

        self.assertEqual(payload["type"], "mysql")
        self.assertEqual(payload["password"], "EnvMysqlPass123!")

    def test_mysql_url_and_mtls_config_resolution(self):
        cfg = mysql_core._resolved_mysql_connect_config(
            base.MysqlConnectRequest(
                url="mysql://uri_user:UriMysqlPass123!@localhost:53307/LOOMDB",
                ssl_ca="ca.crt",
                ssl_cert="client.crt",
                ssl_key="client.key",
            )
        )

        self.assertEqual(cfg["host"], "localhost")
        self.assertEqual(cfg["port"], 53307)
        self.assertEqual(cfg["database"], "LOOMDB")
        self.assertEqual(cfg["username"], "uri_user")
        self.assertEqual(cfg["password"], "UriMysqlPass123!")
        self.assertEqual(cfg["auth_mode"], "mtls")
        self.assertEqual(cfg["ssl"], {"ca": "ca.crt", "cert": "client.crt", "key": "client.key"})

    def test_mysql_connection_sets_ansi_quotes_session_mode(self):
        fake_conn = _FakeMysqlConnectConnection()

        with patch.object(mysql_core.pymysql, "connect", return_value=fake_conn) as connect_mock:
            conn = mysql_core._connect_from_request(
                base.MysqlConnectRequest(
                    host="localhost",
                    port=53307,
                    database="LOOMDB",
                    username="loom_user",
                    password="LoomMysqlPass123!",
                )
            )

        self.assertIs(conn, fake_conn)
        self.assertEqual(fake_conn.cursor_obj.executed, [("SET SESSION sql_mode = CONCAT(@@sql_mode, ',ANSI_QUOTES')", None)])
        self.assertTrue(fake_conn.cursor_obj.closed)
        connect_mock.assert_called_once()
        self.assertEqual(connect_mock.call_args.kwargs["host"], "localhost")
        self.assertEqual(connect_mock.call_args.kwargs["port"], 53307)


class VerifyMysqlIngressTests(unittest.TestCase):
    def test_ingress_maps_mysql_types_to_duckdb_types(self):
        source = _FakeMysqlIngressConnection(
            rows=[
                (
                    1,
                    Decimal("1200.50"),
                    1,
                    datetime.date(2026, 1, 1),
                    datetime.datetime(2026, 4, 1, 10, 15),
                    "POL-MYSQL-001",
                )
            ],
            description=[
                ("policy_id", mysql_core.FIELD_TYPE.LONG, None, 11, 11, 0, False),
                ("premium_amount", mysql_core.FIELD_TYPE.NEWDECIMAL, None, 14, 14, 2, False),
                ("active_flag", mysql_core.FIELD_TYPE.TINY, None, 1, 1, 0, False),
                ("effective_date", mysql_core.FIELD_TYPE.DATE, None, 10, 10, 0, False),
                ("updated_at", mysql_core.FIELD_TYPE.DATETIME, None, 19, 19, 0, False),
                ("policy_number", mysql_core.FIELD_TYPE.VAR_STRING, None, 128, 128, 0, False),
            ],
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "runtime.duckdb"
            with patch.object(mysql_core, "_connect_from_request", return_value=source):
                response = mysql_core.ingest_query_to_duckdb(
                    base.MysqlConnectRequest(database="LOOMDB"),
                    sql="SELECT * FROM policy",
                    duckdb_path=str(db_path),
                    target_table="main.mysql_policy",
                    replace=True,
                )
            conn = connect_duckdb(str(db_path))
            try:
                types = conn.execute(
                    """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'main' AND table_name = 'mysql_policy'
                    ORDER BY ordinal_position
                    """
                ).fetchall()
                rows = conn.execute("SELECT policy_id, premium_amount, active_flag, effective_date, updated_at, policy_number FROM main.mysql_policy").fetchall()
            finally:
                conn.close()

        self.assertEqual(response.row_count, 1)
        self.assertEqual(
            types,
            [
                ("policy_id", "INTEGER"),
                ("premium_amount", "DECIMAL(14,2)"),
                ("active_flag", "BOOLEAN"),
                ("effective_date", "DATE"),
                ("updated_at", "TIMESTAMP"),
                ("policy_number", "VARCHAR"),
            ],
        )
        self.assertEqual(rows, [(1, Decimal("1200.50"), True, datetime.date(2026, 1, 1), datetime.datetime(2026, 4, 1, 10, 15), "POL-MYSQL-001")])
        self.assertEqual(response.column_mappings[1].target_type, "DECIMAL(14,2)")
        self.assertEqual(response.column_mappings[2].target_type, "BOOLEAN")
        self.assertTrue(source.closed)

    def test_ingress_materializes_empty_result_set_schema(self):
        source = _FakeMysqlIngressConnection(
            rows=[],
            description=[
                ("policy_id", mysql_core.FIELD_TYPE.LONG, None, 11, 11, 0, False),
                ("policy_number", mysql_core.FIELD_TYPE.VAR_STRING, None, 128, 128, 0, False),
            ],
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "runtime.duckdb"
            with patch.object(mysql_core, "_connect_from_request", return_value=source):
                response = mysql_core.ingest_query_to_duckdb(
                    base.MysqlConnectRequest(database="LOOMDB"),
                    sql="SELECT policy_id, policy_number FROM policy WHERE 1 = 0",
                    duckdb_path=str(db_path),
                    target_table="main.empty_policy",
                    replace=True,
                )
            conn = connect_duckdb(str(db_path))
            try:
                row_count = conn.execute("SELECT COUNT(*) FROM main.empty_policy").fetchone()[0]
                columns = conn.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'main' AND table_name = 'empty_policy'
                    ORDER BY ordinal_position
                    """
                ).fetchall()
            finally:
                conn.close()

        self.assertEqual(response.row_count, 0)
        self.assertEqual(row_count, 0)
        self.assertEqual(columns, [("policy_id",), ("policy_number",)])

    def test_ingress_passes_sql_and_params_to_mysql(self):
        source = _FakeMysqlIngressConnection(
            rows=[(1,)],
            description=[("policy_id", mysql_core.FIELD_TYPE.LONG, None, 11, 11, 0, False)],
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "runtime.duckdb"
            with patch.object(mysql_core, "_connect_from_request", return_value=source):
                mysql_core.ingest_query_to_duckdb(
                    base.MysqlConnectRequest(database="LOOMDB"),
                    sql="SELECT policy_id FROM policy WHERE policy_number = %s;",
                    sql_params=["POL-MYSQL-001"],
                    duckdb_path=str(db_path),
                    target_table="main.filtered_policy",
                    replace=True,
                )

        self.assertEqual(source.cursor_obj.executed, [("SELECT policy_id FROM policy WHERE policy_number = %s", ("POL-MYSQL-001",))])


class VerifyMysqlEgressTests(unittest.TestCase):
    def _run_egress(self, *, mode: str, table_exists: bool, artifact_table: str | None = None) -> tuple[object, _FakeMysqlConnection]:
        fake_target = _FakeMysqlConnection(table_exists=table_exists)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "runtime.duckdb"
            _write_duckdb_source(db_path)
            with patch.object(mysql_core, "_connect_from_request", return_value=fake_target):
                response = mysql_core.egress_query_from_duckdb(
                    target_request=base.MysqlConnectRequest(database="LOOMDB"),
                    duckdb_database=str(db_path),
                    sql="SELECT * FROM source_data ORDER BY policy_id",
                    target_table="policy_egress_test",
                    mode=mode,
                    artifact_table=artifact_table,
                )
        return response, fake_target

    def test_replace_drops_existing_table_and_recreates(self):
        response, target = self._run_egress(mode="replace", table_exists=True)

        executed_sql = [sql for sql, _params in target.cursor_obj.executed]
        self.assertTrue(any(sql.startswith("DROP TABLE") for sql in executed_sql))
        self.assertTrue(any(sql.startswith("CREATE TABLE") for sql in executed_sql))
        self.assertEqual(response.row_count, 2)
        self.assertEqual(target.inserted_rows, [(1, "POL-001"), (2, "POL-002")])
        self.assertTrue(target.committed)

    def test_append_requires_existing_table(self):
        with self.assertRaisesRegex(RuntimeError, "does not exist for append mode"):
            self._run_egress(mode="append", table_exists=False)

    def test_create_requires_missing_table(self):
        with self.assertRaisesRegex(RuntimeError, "already exists"):
            self._run_egress(mode="create", table_exists=True)

    def test_create_append_creates_when_missing(self):
        response, target = self._run_egress(mode="create_append", table_exists=False)

        self.assertEqual(target.create_count, 1)
        self.assertEqual(response.row_count, 2)
        self.assertEqual(target.inserted_rows, [(1, "POL-001"), (2, "POL-002")])

    def test_create_append_appends_when_existing(self):
        response, target = self._run_egress(mode="create_append", table_exists=True)

        self.assertEqual(target.create_count, 0)
        self.assertEqual(response.row_count, 2)
        self.assertEqual(target.inserted_rows, [(1, "POL-001"), (2, "POL-002")])

    def test_egress_artifact_table_is_materialized_when_requested(self):
        fake_target = _FakeMysqlConnection(table_exists=False)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "runtime.duckdb"
            _write_duckdb_source(db_path)

            def _materialize_artifact(*, database, sql, target_table, replace, **_kwargs):
                conn = connect_duckdb(str(database))
                try:
                    parts = [part.strip() for part in str(target_table).split(".") if part.strip()]
                    target_ident = ".".join('"' + part.replace('"', '""') + '"' for part in parts)
                    conn.execute(f"CREATE OR REPLACE TABLE {target_ident} AS {sql}")
                finally:
                    conn.close()

            with patch.object(mysql_core, "_connect_from_request", return_value=fake_target), patch(
                "duckdb_core.materialize_egress_artifact",
                side_effect=_materialize_artifact,
            ) as materialize_mock:
                response = mysql_core.egress_query_from_duckdb(
                    target_request=base.MysqlConnectRequest(database="LOOMDB"),
                    duckdb_database=str(db_path),
                    sql="SELECT * FROM source_data ORDER BY policy_id",
                    target_table="policy_egress_test",
                    mode="replace",
                    artifact_table="main.mysql_policy_egress_artifact",
                )

        self.assertEqual(response.row_count, 2)
        materialize_mock.assert_called_once()

    def test_duckdb_types_map_to_mysql_create_table_sql(self):
        create_sql, warnings = mysql_core._build_mysql_create_table_sql(
            "LOOMDB.policy_egress",
            [
                base.ColumnMeta(name="policy_id", data_type="INTEGER", nullable=False),
                base.ColumnMeta(name="is_active", data_type="BOOLEAN", nullable=True),
                base.ColumnMeta(name="payload", data_type="JSON", nullable=True),
                base.ColumnMeta(name="raw_blob", data_type="BLOB", nullable=True),
                base.ColumnMeta(name="updated_at", data_type="TIMESTAMPTZ", nullable=True),
            ],
        )

        self.assertEqual(
            create_sql,
            "CREATE TABLE `LOOMDB`.`policy_egress` "
            "(`policy_id` INT NOT NULL, `is_active` BOOLEAN, `payload` LONGTEXT, "
            "`raw_blob` LONGBLOB, `updated_at` DATETIME)",
        )
        self.assertEqual(warnings, ["DuckDB TIMESTAMPTZ was normalized to MySQL DATETIME."])


class VerifyMysqlLookupRuntimeTests(unittest.TestCase):
    def test_lookup_cleanup_drops_non_retained_table(self):
        drops: list[str] = []
        pipeline = self._write_lookup_pipeline(retain=False)
        config = self._lookup_config()
        connections = self._connections_config()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            config_path = root / "configurations.yaml"
            connections_path = root / "connections.yaml"
            pipeline_path.write_text(pipeline, encoding="utf-8")
            config_path.write_text(config, encoding="utf-8")
            connections_path.write_text(connections, encoding="utf-8")

            with patch.object(mysql_core, "drop_table_if_exists", side_effect=lambda *, target_request, target_table: drops.append(target_table)):
                queron.compile_pipeline(pipeline_path, config_path=config_path)
                result = queron.run_pipeline(pipeline_path, config_path=config_path, connections_path=connections_path, clean_existing=True)

        self.assertEqual(result.executed_nodes, ["ingest_mysql_policy", "stage_mysql_lookup"])
        self.assertEqual(drops, ['"phase_lookup_keys"'])

    def test_lookup_cleanup_skips_retained_table(self):
        pipeline = self._write_lookup_pipeline(retain=True)
        config = self._lookup_config()
        connections = self._connections_config()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            config_path = root / "configurations.yaml"
            connections_path = root / "connections.yaml"
            pipeline_path.write_text(pipeline, encoding="utf-8")
            config_path.write_text(config, encoding="utf-8")
            connections_path.write_text(connections, encoding="utf-8")

            with patch.object(mysql_core, "drop_table_if_exists") as drop_mock:
                queron.compile_pipeline(pipeline_path, config_path=config_path)
                queron.run_pipeline(pipeline_path, config_path=config_path, connections_path=connections_path, clean_existing=True)

        drop_mock.assert_not_called()

    def test_lookup_details_are_visible_in_inspect_node_and_dag(self):
        pipeline = self._write_lookup_pipeline(retain=True)
        config = self._lookup_config()
        connections = self._connections_config()
        req = base.MysqlConnectRequest(host="localhost", port=53307, database="LOOMDB", username="loom_user", password="LoomMysqlPass123!")
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                root = pathlib.Path(tmpdir)
                pipeline_path = root / "pipeline.py"
                config_path = root / "configurations.yaml"
                connections_path = root / "connections.yaml"
                pipeline_path.write_text(pipeline, encoding="utf-8")
                config_path.write_text(config, encoding="utf-8")
                connections_path.write_text(connections, encoding="utf-8")

                queron.compile_pipeline(pipeline_path, config_path=config_path)
                result = queron.run_pipeline(pipeline_path, config_path=config_path, connections_path=connections_path, clean_existing=True)
                node = queron.inspect_node(result.artifact_path, "stage_mysql_lookup")
                dag = queron.inspect_dag(result.artifact_path)
        finally:
            mysql_core.drop_table_if_exists(target_request=req, target_table="phase_lookup_keys")

        inspect_lookup = next(item for item in node.nodes if item["name"] == "stage_mysql_lookup")
        dag_lookup = next(item for item in dag.nodes if item["name"] == "stage_mysql_lookup")
        self.assertEqual(inspect_lookup["target_relation"], '"phase_lookup_keys"')
        self.assertEqual(inspect_lookup["mode"], "replace")
        self.assertTrue(inspect_lookup["retain"])
        self.assertEqual(inspect_lookup["lookup_table"], '`phase_lookup_keys`')
        self.assertEqual(inspect_lookup["details"]["lookup_table"], "`phase_lookup_keys`")
        self.assertEqual(dag_lookup["lookup_table"], "`phase_lookup_keys`")
        self.assertTrue(dag_lookup["retain"])

    def test_compile_allows_mysql_lookup_consumer_with_same_config(self):
        compiled = compile_pipeline_code(self._lookup_consumer_pipeline(consumer_config="MYSQL_LOCAL"), yaml_text=self._lookup_config())
        errors = [item for item in compiled.diagnostics if item.get("level") == "error"]

        self.assertEqual(errors, [])

    def test_compile_rejects_lookup_consumer_with_different_config(self):
        compiled = compile_pipeline_code(self._lookup_consumer_pipeline(consumer_config="MYSQL_OTHER"), yaml_text=self._lookup_config())

        self.assertTrue(any(item.get("code") == "lookup_config_mismatch" for item in compiled.diagnostics))

    def test_full_lookup_pipeline_ingress_lookup_ingress_egress(self):
        pipeline = r'''
import queron
__queron_native__ = {"pipeline_id": "mysql_lookup_full_smoke"}
@queron.mysql.ingress(config="MYSQL_LOCAL", name="ingest_mysql_policy", out="mysql_policy", sql='SELECT policy_id, policy_number, active_flag FROM {{ queron.source("policy") }} ORDER BY policy_id')
def ingest_mysql_policy(): pass
@queron.mysql.lookup(config="MYSQL_LOCAL", name="stage_mysql_lookup", table="phase_lookup_keys", out="phase_lookup_keys", sql='SELECT policy_id FROM {{ queron.ref("mysql_policy") }} WHERE active_flag = true', mode="replace")
def stage_mysql_lookup(): pass
@queron.mysql.ingress(config="MYSQL_LOCAL", name="ingest_mysql_filtered", out="mysql_filtered", sql='SELECT p.policy_id, p.policy_number FROM {{ queron.source("policy") }} p JOIN {{ queron.lookup("phase_lookup_keys") }} k ON p.policy_id = k.policy_id ORDER BY p.policy_id')
def ingest_mysql_filtered(): pass
@queron.mysql.egress(config="MYSQL_LOCAL", name="egress_mysql_filtered", table="phase_lookup_egress", out="phase_lookup_egress_artifact", sql='SELECT * FROM {{ queron.ref("mysql_filtered") }}', mode="replace")
def egress_mysql_filtered(): pass
'''
        req = base.MysqlConnectRequest(host="localhost", port=53307, database="LOOMDB", username="loom_user", password="LoomMysqlPass123!")
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                root = pathlib.Path(tmpdir)
                pipeline_path = root / "pipeline.py"
                config_path = root / "configurations.yaml"
                connections_path = root / "connections.yaml"
                pipeline_path.write_text(pipeline, encoding="utf-8")
                config_path.write_text(self._lookup_config(egress=True), encoding="utf-8")
                connections_path.write_text(self._connections_config(), encoding="utf-8")

                queron.compile_pipeline(pipeline_path, config_path=config_path)
                result = queron.run_pipeline(pipeline_path, config_path=config_path, connections_path=connections_path, clean_existing=True)
                conn = mysql_core._connect_from_request(req)
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM phase_lookup_egress")
                row_count = int(cur.fetchone()[0])
                cur.close()
                conn.close()
        finally:
            mysql_core.drop_table_if_exists(target_request=req, target_table="phase_lookup_egress")
            mysql_core.drop_table_if_exists(target_request=req, target_table="phase_lookup_keys")

        self.assertEqual(result.executed_nodes, ["ingest_mysql_policy", "stage_mysql_lookup", "ingest_mysql_filtered", "egress_mysql_filtered"])
        self.assertEqual(row_count, 2)

    def _write_lookup_pipeline(self, *, retain: bool) -> str:
        retain_text = "True" if retain else "False"
        return f'''
import queron
__queron_native__ = {{"pipeline_id": "mysql_lookup_runtime"}}
@queron.mysql.ingress(config="MYSQL_LOCAL", name="ingest_mysql_policy", out="mysql_policy", sql='SELECT policy_id, active_flag FROM {{{{ queron.source("policy") }}}} ORDER BY policy_id')
def ingest_mysql_policy(): pass
@queron.mysql.lookup(config="MYSQL_LOCAL", name="stage_mysql_lookup", table="phase_lookup_keys", out="phase_lookup_keys", sql='SELECT policy_id FROM {{{{ queron.ref("mysql_policy") }}}} WHERE active_flag = true', mode="replace", retain={retain_text})
def stage_mysql_lookup(): pass
'''

    def _lookup_consumer_pipeline(self, *, consumer_config: str) -> str:
        return f'''
import queron
__queron_native__ = {{"pipeline_id": "mysql_lookup_compile"}}
@queron.mysql.ingress(config="MYSQL_LOCAL", name="ingest_mysql_policy", out="mysql_policy", sql='SELECT policy_id, active_flag FROM {{{{ queron.source("policy") }}}}')
def ingest_mysql_policy(): pass
@queron.mysql.lookup(config="MYSQL_LOCAL", name="stage_mysql_lookup", table="phase_lookup_keys", out="phase_lookup_keys", sql='SELECT policy_id FROM {{{{ queron.ref("mysql_policy") }}}}', mode="replace")
def stage_mysql_lookup(): pass
@queron.mysql.ingress(config="{consumer_config}", name="ingest_mysql_filtered", out="mysql_filtered", sql='SELECT p.policy_id FROM {{{{ queron.source("policy") }}}} p JOIN {{{{ queron.lookup("phase_lookup_keys") }}}} k ON p.policy_id = k.policy_id')
def ingest_mysql_filtered(): pass
'''

    def _lookup_config(self, *, egress: bool = False) -> str:
        suffix = ""
        if egress:
            suffix = """
egress:
  phase_lookup_egress:
    table: phase_lookup_egress
"""
        return f"""
sources:
  policy:
    table: policy
lookup:
  phase_lookup_keys:
    table: phase_lookup_keys
{suffix}"""

    def _connections_config(self) -> str:
        return """
connections:
  MYSQL_LOCAL:
    type: mysql
    host: localhost
    port: 53307
    database: LOOMDB
    username: loom_user
    password: LoomMysqlPass123!
    connect_timeout_seconds: 3
  MYSQL_OTHER:
    type: mysql
    host: localhost
    port: 53307
    database: LOOMDB
    username: loom_user
    password: LoomMysqlPass123!
    connect_timeout_seconds: 3
"""


class VerifyMysqlCliApiIntegrationTests(unittest.TestCase):
    def test_cli_compile_run_and_inspect_dag_include_mysql_node_kinds(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path, config_path, connections_path = self._write_project(root, pipeline_id="mysql_cli_integration")

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                compile_exit = queron.cli.main(["compile", str(pipeline_path), "--config", str(config_path)])
            self.assertEqual(compile_exit, 0, stderr.getvalue())

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                run_exit = queron.cli.main(
                    [
                        "run",
                        str(pipeline_path),
                        "--config",
                        str(config_path),
                        "--connections",
                        str(connections_path),
                        "--clean-existing",
                    ]
                )
            self.assertEqual(run_exit, 0, stderr.getvalue())
            self.assertIn("Run succeeded.", stdout.getvalue())

            artifact_path = root / ".queron" / "mysql_cli_integration" / "artifact.duckdb"
            dag = queron.inspect_dag(artifact_path)
            kinds = {node["name"]: node["kind"] for node in dag.nodes}
            self.assertEqual(kinds["ingest_mysql_policy"], "mysql.ingress")
            self.assertEqual(kinds["stage_mysql_lookup"], "mysql.lookup")
            self.assertEqual(kinds["ingest_mysql_filtered"], "mysql.ingress")
            self.assertEqual(kinds["egress_mysql_policy"], "mysql.egress")

    def test_api_run_accepts_mysql_runtime_binding(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path, config_path, _connections_path = self._write_project(root, pipeline_id="mysql_api_runtime_binding")

            compiled = queron.compile_pipeline(pipeline_path, config_path=config_path)
            self.assertEqual([d for d in compiled.diagnostics if d.get("level") == "error"], [])
            result = queron.run_pipeline(
                pipeline_path,
                config_path=config_path,
                runtime_bindings={
                    "MYSQL_LOCAL": MysqlBinding(
                        host="localhost",
                        port=53307,
                        database="LOOMDB",
                        username="loom_user",
                        password="LoomMysqlPass123!",
                        connect_timeout_seconds=3,
                    )
                },
                clean_existing=True,
            )

            self.assertEqual(result.executed_nodes, ["ingest_mysql_policy", "stage_mysql_lookup", "ingest_mysql_filtered", "egress_mysql_policy"])
            dag = queron.inspect_dag(result.artifact_path)
            lookup_node = next(node for node in dag.nodes if node["name"] == "stage_mysql_lookup")
            self.assertEqual(lookup_node["kind"], "mysql.lookup")
            self.assertEqual(lookup_node["config"], "MYSQL_LOCAL")
            self.assertEqual(lookup_node["mode"], "replace")
            self.assertFalse(bool(lookup_node["retain"]))

    def test_inspect_node_shows_mysql_details(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path, config_path, connections_path = self._write_project(root, pipeline_id="mysql_inspect_details")

            queron.compile_pipeline(pipeline_path, config_path=config_path)
            result = queron.run_pipeline(pipeline_path, config_path=config_path, connections_path=connections_path, clean_existing=True)
            inspected = queron.inspect_node(result.artifact_path, "egress_mysql_policy")
            node = next(item for item in inspected.nodes if item["name"] == "egress_mysql_policy")

        self.assertEqual(node["kind"], "mysql.egress")
        self.assertEqual(node["config"], "MYSQL_LOCAL")
        self.assertEqual(node["mode"], "replace")
        self.assertEqual(node["target_relation"], '"phase_cli_egress"')
        self.assertEqual(node["out"], "phase_cli_egress_artifact")

    def _write_project(self, root: pathlib.Path, *, pipeline_id: str) -> tuple[pathlib.Path, pathlib.Path, pathlib.Path]:
        pipeline_path = root / "pipeline.py"
        config_path = root / "configurations.yaml"
        connections_path = root / "connections.yaml"
        pipeline_path.write_text(
            f'''
import queron
__queron_native__ = {{"pipeline_id": "{pipeline_id}"}}
@queron.mysql.ingress(config="MYSQL_LOCAL", name="ingest_mysql_policy", out="mysql_policy", sql='SELECT policy_id, policy_number, active_flag FROM {{{{ queron.source("policy") }}}} ORDER BY policy_id')
def ingest_mysql_policy(): pass
@queron.mysql.lookup(config="MYSQL_LOCAL", name="stage_mysql_lookup", table="phase_cli_lookup", out="phase_cli_lookup", sql='SELECT policy_id FROM {{{{ queron.ref("mysql_policy") }}}} WHERE active_flag = true', mode="replace")
def stage_mysql_lookup(): pass
@queron.mysql.ingress(config="MYSQL_LOCAL", name="ingest_mysql_filtered", out="mysql_filtered", sql='SELECT p.policy_id, p.policy_number FROM {{{{ queron.source("policy") }}}} p JOIN {{{{ queron.lookup("phase_cli_lookup") }}}} k ON p.policy_id = k.policy_id ORDER BY p.policy_id')
def ingest_mysql_filtered(): pass
@queron.mysql.egress(config="MYSQL_LOCAL", name="egress_mysql_policy", table="phase_cli_egress", out="phase_cli_egress_artifact", sql='SELECT * FROM {{{{ queron.ref("mysql_filtered") }}}}', mode="replace")
def egress_mysql_policy(): pass
''',
            encoding="utf-8",
        )
        config_path.write_text(
            """
sources:
  policy:
    table: policy
lookup:
  phase_cli_lookup:
    table: phase_cli_lookup
egress:
  phase_cli_egress:
    table: phase_cli_egress
""",
            encoding="utf-8",
        )
        connections_path.write_text(
            """
connections:
  MYSQL_LOCAL:
    type: mysql
    host: localhost
    port: 53307
    database: LOOMDB
    username: loom_user
    password: LoomMysqlPass123!
    connect_timeout_seconds: 3
""",
            encoding="utf-8",
        )
        return pipeline_path, config_path, connections_path

    def tearDown(self) -> None:
        req = base.MysqlConnectRequest(host="localhost", port=53307, database="LOOMDB", username="loom_user", password="LoomMysqlPass123!")
        mysql_core.drop_table_if_exists(target_request=req, target_table="phase_cli_lookup")
        mysql_core.drop_table_if_exists(target_request=req, target_table="phase_cli_egress")


if __name__ == "__main__":
    unittest.main()
