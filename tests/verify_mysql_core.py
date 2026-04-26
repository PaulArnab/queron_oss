from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import base
from duckdb_driver import connect_duckdb
import mysql_core


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


if __name__ == "__main__":
    unittest.main()
