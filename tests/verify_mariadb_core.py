from __future__ import annotations

import pathlib
import socket
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import patch

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import base
from queron.bindings import MariaDbBinding, MysqlBinding, resolve_runtime_binding_payload
from queron.config import resolve_connection_binding
from queron.compiler import compile_pipeline_code
import mariadb_core
import queron


def _mariadb_available() -> bool:
    try:
        with socket.create_connection(("localhost", 53306), timeout=2):
            return True
    except OSError:
        return False


class VerifyMariaDbCoreTests(unittest.TestCase):
    def test_runtime_binding_keeps_mariadb_separate_from_mysql(self):
        mariadb_payload = resolve_runtime_binding_payload(
            "MARIADB_LOCAL",
            {"type": "mariadb", "uri": "mariadb://user:pass@localhost:53306/LOOMDB"},
        )
        mysql_payload = resolve_runtime_binding_payload(
            "MYSQL_LOCAL",
            {"type": "mysql", "uri": "mysql://user:pass@localhost:53307/LOOMDB"},
        )

        self.assertEqual(mariadb_payload["type"], "mariadb")
        self.assertEqual(mysql_payload["type"], "mysql")

    def test_mariadb_binding_resolves_native_fields(self):
        binding = MariaDbBinding(
            host="localhost",
            port=53306,
            database="LOOMDB",
            username="loom_user",
            password="secret",
            ssl_ca="ca.crt",
            ssl_cert="client.crt",
            ssl_key="client.key",
            unix_socket="/tmp/mariadb.sock",
            connect_timeout_seconds=3,
        )

        payload = binding.resolve_config("MARIADB_LOCAL")

        self.assertEqual(payload["type"], "mariadb")
        self.assertEqual(payload["ssl_ca"], "ca.crt")
        self.assertEqual(payload["ssl_cert"], "client.crt")
        self.assertEqual(payload["ssl_key"], "client.key")
        self.assertEqual(payload["unix_socket"], "/tmp/mariadb.sock")
        self.assertEqual(payload["connect_timeout_seconds"], 3)

    def test_mysql_binding_still_resolves_mysql(self):
        binding = MysqlBinding(host="localhost", port=53307, database="LOOMDB")
        payload = binding.resolve_config("MYSQL_LOCAL")
        self.assertEqual(payload["type"], "mysql")

    def test_connections_yaml_mariadb_password_env(self):
        with patch.dict("os.environ", {"QUERON_MARIADB_PASSWORD": "EnvMariaPass123!"}):
            payload = resolve_connection_binding(
                "MARIADB_ENV",
                {
                    "connections": {
                        "MARIADB_ENV": {
                            "type": "mariadb",
                            "host": "localhost",
                            "port": 53306,
                            "database": "LOOMDB",
                            "username": "env_user",
                            "password_env": "QUERON_MARIADB_PASSWORD",
                        }
                    }
                },
            )

        self.assertEqual(payload["type"], "mariadb")
        self.assertEqual(payload["password"], "EnvMariaPass123!")

    def test_mariadb_url_config_resolution(self):
        cfg = mariadb_core._resolved_mariadb_connect_config(
            base.MariaDbConnectRequest(url="mariadb://uri_user:UriMariaPass123!@localhost:53306/LOOMDB")
        )

        self.assertEqual(cfg["host"], "localhost")
        self.assertEqual(cfg["port"], 53306)
        self.assertEqual(cfg["database"], "LOOMDB")
        self.assertEqual(cfg["username"], "uri_user")
        self.assertEqual(cfg["password"], "UriMariaPass123!")

    def test_duckdb_types_map_to_mariadb_create_table_sql(self):
        create_sql, warnings = mariadb_core._build_mariadb_create_table_sql(
            "policy_egress",
            [
                base.ColumnMeta(name="policy_id", data_type="INTEGER"),
                base.ColumnMeta(name="policy_number", data_type="VARCHAR"),
                base.ColumnMeta(name="premium_amount", data_type="DECIMAL(18,2)"),
            ],
        )

        self.assertIn("`policy_id` INT", create_sql)
        self.assertIn("`policy_number` LONGTEXT", create_sql)
        self.assertIn("`premium_amount` DECIMAL(18,2)", create_sql)
        self.assertEqual(warnings, [])

    def test_compile_allows_mariadb_lookup_consumer_with_same_config(self):
        code = '''
import queron
queron.pipeline(pipeline_id="mariadb_lookup_compile")
@queron.mariadb.ingress(config="MARIADB_LOCAL", name="ingest_mariadb_policy", out="mariadb_policy", sql='SELECT policy_id, active_flag FROM {{ queron.source("policy") }}')
def ingest_mariadb_policy(): pass
@queron.mariadb.lookup(config="MARIADB_LOCAL", name="stage_mariadb_lookup", table="phase_lookup_keys", out="phase_lookup_keys", sql='SELECT policy_id FROM {{ queron.ref("mariadb_policy") }}', mode="replace")
def stage_mariadb_lookup(): pass
@queron.mariadb.ingress(config="MARIADB_LOCAL", name="ingest_mariadb_filtered", out="mariadb_filtered", sql='SELECT p.policy_id FROM {{ queron.source("policy") }} p JOIN {{ queron.lookup("phase_lookup_keys") }} k ON p.policy_id = k.policy_id')
def ingest_mariadb_filtered(): pass
'''
        yaml_text = """
target: dev
sources:
  policy:
    dev:
      table: policy
lookup:
  phase_lookup_keys:
    dev:
      table: phase_lookup_keys
"""
        compiled = compile_pipeline_code(code, yaml_text=yaml_text)
        self.assertFalse([item for item in compiled.diagnostics if item.get("level") == "error"])

    @unittest.skipUnless(_mariadb_available(), "MariaDB container is not available on localhost:53306")
    def test_container_native_connection_and_lookup_roundtrip(self):
        req = base.MariaDbConnectRequest(
            host="localhost",
            port=53306,
            database="LOOMDB",
            username="loom_user",
            password="LoomMariaPass123!",
            connect_timeout_seconds=10,
        )
        mariadb_core.drop_table_if_exists(target_request=req, target_table="phase_mariadb_lookup")
        mariadb_core.drop_table_if_exists(target_request=req, target_table="phase_mariadb_egress")
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "configurations.yaml").write_text(
                textwrap.dedent(
                    """
                    target: dev
                    sources:
                      policy:
                        dev:
                          table: policy
                    lookup:
                      phase_lookup:
                        dev:
                          table: phase_mariadb_lookup
                    egress:
                      phase_egress:
                        dev:
                          table: phase_mariadb_egress
                    """
                ),
                encoding="utf-8",
            )
            pipeline = root / "pipeline.py"
            pipeline.write_text(
                textwrap.dedent(
                    '''
                    import queron
                    from queron.bindings import MariaDbBinding

                    queron.pipeline(pipeline_id="mariadb_native_smoke")

                    @queron.mariadb.ingress(config="MARIADB_LOCAL", name="ingest_mariadb_policy", out="mariadb_policy", sql='SELECT policy_id, policy_number, active_flag FROM {{ queron.source("policy") }} ORDER BY policy_id')
                    def ingest_mariadb_policy(): pass

                    @queron.mariadb.lookup(config="MARIADB_LOCAL", name="stage_mariadb_lookup", table="phase_lookup", out="phase_lookup", sql='SELECT policy_id FROM {{ queron.ref("mariadb_policy") }} WHERE active_flag = true', mode="replace")
                    def stage_mariadb_lookup(): pass

                    @queron.mariadb.ingress(config="MARIADB_LOCAL", name="ingest_mariadb_filtered", out="mariadb_filtered", sql='SELECT p.policy_id, p.policy_number FROM {{ queron.source("policy") }} p JOIN {{ queron.lookup("phase_lookup") }} k ON p.policy_id = k.policy_id ORDER BY p.policy_id')
                    def ingest_mariadb_filtered(): pass

                    @queron.mariadb.egress(config="MARIADB_LOCAL", name="egress_mariadb_filtered", table="phase_egress", out="phase_egress_artifact", sql='SELECT * FROM {{ queron.ref("mariadb_filtered") }}', mode="replace")
                    def egress_mariadb_filtered(): pass

                    RUNTIME_BINDINGS = {
                        "MARIADB_LOCAL": MariaDbBinding(
                            host="localhost",
                            port=53306,
                            database="LOOMDB",
                            username="loom_user",
                            password="LoomMariaPass123!",
                            connect_timeout_seconds=10,
                        )
                    }
                    '''
                ),
                encoding="utf-8",
            )

            queron.compile_pipeline(pipeline)
            result = queron.run_pipeline(
                pipeline,
                runtime_bindings={
                    "MARIADB_LOCAL": MariaDbBinding(
                        host="localhost",
                        port=53306,
                        database="LOOMDB",
                        username="loom_user",
                        password="LoomMariaPass123!",
                        connect_timeout_seconds=10,
                    )
                },
            )
            self.assertEqual(
                result.executed_nodes,
                ["ingest_mariadb_policy", "stage_mariadb_lookup", "ingest_mariadb_filtered", "egress_mariadb_filtered"],
            )
            inspected = queron.inspect_node(result.artifact_path, "egress_mariadb_filtered")
            node = next(item for item in inspected.nodes if item["name"] == "egress_mariadb_filtered")
            self.assertEqual(node["kind"], "mariadb.egress")
            self.assertTrue(node["column_mappings"])

        mariadb_core.drop_table_if_exists(target_request=req, target_table="phase_mariadb_lookup")
        mariadb_core.drop_table_if_exists(target_request=req, target_table="phase_mariadb_egress")


if __name__ == "__main__":
    unittest.main()
