from __future__ import annotations

import datetime
from decimal import Decimal
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


if __name__ == "__main__":
    unittest.main()
