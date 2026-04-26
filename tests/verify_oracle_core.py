from __future__ import annotations

import pathlib
import socket
import sys
import tempfile
import unittest

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import duckdb

import base
import oracle_core


def _oracle_available() -> bool:
    try:
        with socket.create_connection(("localhost", 51521), timeout=2):
            return True
    except OSError:
        return False


class VerifyOracleCoreTests(unittest.TestCase):
    def test_basic_config_resolution_uses_service_name_dsn(self):
        cfg = oracle_core._resolved_oracle_connect_config(
            base.OracleConnectRequest(
                host="localhost",
                port=51521,
                service_name="FREEPDB1",
                username="loom_user",
                password="secret",
            )
        )

        self.assertEqual(cfg["auth_mode"], "basic")
        self.assertIn("SERVICE_NAME=FREEPDB1", cfg["dsn"])

    def test_tns_config_resolution_uses_alias_and_config_dir(self):
        cfg = oracle_core._resolved_oracle_connect_config(
            base.OracleConnectRequest(
                tns_alias="QUERON_ORACLE_LOCAL",
                config_dir="docker/oracle_auth/tns",
                username="loom_user",
                password="secret",
            )
        )

        self.assertEqual(cfg["auth_mode"], "tns")
        self.assertEqual(cfg["dsn"], "QUERON_ORACLE_LOCAL")
        self.assertEqual(cfg["config_dir"], "docker/oracle_auth/tns")

    def test_duckdb_types_map_to_oracle_create_table_sql(self):
        create_sql, warnings = oracle_core._build_oracle_create_table_sql(
            "policy_egress",
            [
                base.ColumnMeta(name="policy_id", data_type="INTEGER"),
                base.ColumnMeta(name="policy_number", data_type="VARCHAR"),
                base.ColumnMeta(name="premium_amount", data_type="DECIMAL(18,2)"),
            ],
        )

        self.assertIn('"POLICY_ID" NUMBER(10)', create_sql)
        self.assertIn('"POLICY_NUMBER" CLOB', create_sql)
        self.assertIn('"PREMIUM_AMOUNT" NUMBER(18,2)', create_sql)
        self.assertEqual(warnings, [])

    def test_oracle_number_and_timestamp_tz_mapping(self):
        number_type, number_warnings, number_lossy = oracle_core._normalize_oracle_source_type(
            "DB_TYPE_NUMBER",
            precision=10,
            scale=None,
        )
        timestamp_type, timestamp_warnings, timestamp_lossy = oracle_core._normalize_oracle_source_type("DB_TYPE_TIMESTAMP_TZ")

        self.assertEqual(number_type, "DECIMAL(10,0)")
        self.assertEqual(number_warnings, [])
        self.assertFalse(number_lossy)
        self.assertEqual(timestamp_type, "TIMESTAMP")
        self.assertTrue(timestamp_warnings)
        self.assertTrue(timestamp_lossy)

    def test_duckdb_varchar_length_and_decimal_overflow_mapping(self):
        bounded_type, bounded_warnings = oracle_core._map_duckdb_column_to_oracle(
            base.ColumnMeta(name="policy_number", data_type="VARCHAR", max_length=32)
        )
        wide_type, wide_warnings = oracle_core._map_duckdb_column_to_oracle(
            base.ColumnMeta(name="note", data_type="VARCHAR(5000)")
        )
        overflow_type, overflow_warnings = oracle_core._map_duckdb_column_to_oracle(
            base.ColumnMeta(name="amount", data_type="DECIMAL(39,2)")
        )

        self.assertEqual(bounded_type, "VARCHAR2(32)")
        self.assertEqual(bounded_warnings, [])
        self.assertEqual(wide_type, "CLOB")
        self.assertTrue(wide_warnings)
        self.assertEqual(overflow_type, "BINARY_DOUBLE")
        self.assertTrue(overflow_warnings)

    @unittest.skipUnless(_oracle_available(), "Oracle container is not available on localhost:51521")
    def test_container_ingress_egress_roundtrip(self):
        req = base.OracleConnectRequest(
            host="localhost",
            port=51521,
            service_name="FREEPDB1",
            username="loom_user",
            password="LoomOraclePass123!",
            connect_timeout_seconds=10,
        )
        target_table = "loom_user.phase_oracle_egress"
        oracle_core.drop_table_if_exists(target_request=req, target_table=target_table)
        with tempfile.TemporaryDirectory() as tmp:
            db = str(pathlib.Path(tmp) / "oracle_core.duckdb")
            ingest = oracle_core.ingest_query_to_duckdb(
                req,
                sql="select policy_id, policy_number, premium_amount from loom_user.policy order by policy_id",
                duckdb_path=db,
                target_table="main.oracle_policy",
                replace=True,
            )
            self.assertEqual(ingest.row_count, 3)
            self.assertTrue(ingest.column_mappings)
            con = duckdb.connect(db)
            try:
                self.assertEqual(con.execute("select count(*) from main.oracle_policy").fetchone()[0], 3)
            finally:
                con.close()
            egress = oracle_core.egress_query_from_duckdb(
                target_request=req,
                duckdb_database=db,
                sql="select policy_id, policy_number, premium_amount from main.oracle_policy",
                target_table=target_table,
                mode="create",
            )
            self.assertEqual(egress.row_count, 3)
            self.assertEqual({item.mapping_mode for item in egress.column_mappings}, {"egress_remote_schema"})

        oracle_core.drop_table_if_exists(target_request=req, target_table=target_table)


if __name__ == "__main__":
    unittest.main()
