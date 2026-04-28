from __future__ import annotations

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
import db2_core
import duckdb_core
from duckdb_driver import load_duckdb
import postgres_core


class _Closable:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakeDuckCursor:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, sql, params=None):
        _ = sql
        _ = params

    def fetchall(self):
        return list(self._rows)

    def close(self):
        return None


class _FakeDuckConnection(_Closable):
    def __init__(self, rows):
        super().__init__()
        self._rows = rows

    def cursor(self):
        return _FakeDuckCursor(self._rows)

    def execute(self, sql):
        _ = sql

    def executemany(self, sql, payload):
        _ = sql
        _ = payload


class _FakeDb2Cursor:
    def __init__(self) -> None:
        self.description = [
            ("payload", "JSON", None, None, None, None, True),
            ("amount", "DECIMAL", None, None, 50, 2, True),
        ]
        self._batches = [[('{"a":1}', 12.34)], []]

    def execute(self, sql):
        _ = sql

    def fetchmany(self, size):
        _ = size
        return self._batches.pop(0)

    def close(self):
        return None


class _FakeDb2Connection(_Closable):
    def __init__(self) -> None:
        super().__init__()
        self._cursor = _FakeDb2Cursor()

    def cursor(self):
        return self._cursor


class _FakeDb2TypeObjectCursor:
    def __init__(self) -> None:
        self.description = [
            ("PERSON_ID", "DBAPITypeObject({'INT', 'INTEGER', 'SMALLINT'})", None, None, None, None, False),
            ("EMAIL_TYPE", "DBAPITypeObject({'CHARACTER', 'CHARACTER VARYING', 'VARCHAR', 'CHAR', 'STRING', 'CHAR VARYING'})", None, None, None, None, False),
        ]
        self._batches = [[(101, "PRIMARY")], []]

    def execute(self, sql):
        _ = sql

    def fetchmany(self, size):
        _ = size
        return self._batches.pop(0)

    def close(self):
        return None


class _FakeDb2TypeObjectConnection(_Closable):
    def __init__(self) -> None:
        super().__init__()
        self._cursor = _FakeDb2TypeObjectCursor()

    def cursor(self):
        return self._cursor


class _FakePostgresCursor:
    def __init__(self, rows):
        self.description = [
            ("snapshot_id", 23, None, None, None, None, True),
            ("amount", 1700, None, None, None, None, True),
        ]
        self._batches = [list(rows), []]

    def execute(self, sql):
        _ = sql

    def fetchmany(self, size):
        _ = size
        return self._batches.pop(0)

    def close(self):
        return None


class _FakePostgresConnection(_Closable):
    def __init__(self, rows) -> None:
        super().__init__()
        self._cursor = _FakePostgresCursor(rows)

    def cursor(self):
        return self._cursor


class VerifyIngestColumnMappingTests(unittest.TestCase):
    def test_postgres_ingest_returns_source_to_target_column_mappings(self):
        source_conn = _FakePostgresConnection(rows=[(1, Decimal("1.25"))])
        duckdb = load_duckdb()

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "runtime.duckdb"

            def _open_conn():
                return duckdb.connect(str(db_path))

            with patch.object(postgres_core, "_config_from_connection_id", return_value={"uri": "postgresql://example"}), patch.object(
                postgres_core, "_pg_connection", return_value=source_conn
            ), patch.object(
                postgres_core,
                "_inspect_postgres_result_columns",
                return_value=[
                    base.ColumnMeta(name="snapshot_id", data_type="integer", nullable=True),
                    base.ColumnMeta(name="amount", data_type="numeric(12,2)", nullable=True),
                ],
            ), patch.object(
                postgres_core, "_duckdb_connection_from_id", side_effect=lambda _: _open_conn()
            ), patch.object(postgres_core.time, "time", return_value=123.0):
                response = postgres_core.ingest_query_to_duckdb(
                    source_connection_id="pg-1",
                    duckdb_connection_id="duck-1",
                    sql="SELECT 1",
                    target_table="main.cdh_out",
                    replace=False,
                    chunk_size=200,
                    pipeline_id="pipeline-1",
                )

            inspect_conn = _open_conn()
            try:
                rows = inspect_conn.execute(
                    """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'main' AND table_name = 'cdh_out'
                    ORDER BY ordinal_position
                    """
                ).fetchall()
                loaded_rows = inspect_conn.execute(
                    """
                    SELECT snapshot_id, amount
                    FROM main.cdh_out
                    """
                ).fetchall()
            finally:
                inspect_conn.close()

        self.assertEqual(response.row_count, 1)
        self.assertEqual(rows, [("snapshot_id", "INTEGER"), ("amount", "DECIMAL(12,2)")])
        self.assertEqual(loaded_rows, [(1, Decimal("1.25"))])
        self.assertEqual(len(response.column_mappings), 2)
        self.assertEqual(response.column_mappings[0].source_type, "integer")
        self.assertEqual(response.column_mappings[0].target_type, "INTEGER")
        self.assertEqual(response.column_mappings[0].mapping_mode, "python_mapped")
        self.assertEqual(response.column_mappings[1].source_type, "numeric(12,2)")
        self.assertEqual(response.column_mappings[1].target_type, "DECIMAL(12,2)")
        self.assertTrue(source_conn.closed)

    def test_postgres_ingest_reports_cursor_metadata_fallback_when_inspection_fails(self):
        source_conn = _FakePostgresConnection(rows=[(1, Decimal("1.25"))])
        duckdb = load_duckdb()

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "runtime.duckdb"

            def _open_conn():
                return duckdb.connect(str(db_path))

            with patch.object(postgres_core, "_config_from_connection_id", return_value={"uri": "postgresql://example"}), patch.object(
                postgres_core, "_pg_connection", return_value=source_conn
            ), patch.object(
                postgres_core,
                "_inspect_postgres_result_columns",
                side_effect=RuntimeError("inspect failed"),
            ), patch.object(
                postgres_core,
                "_column_meta_from_cursor",
                return_value=[base.ColumnMeta(name="snapshot_id", data_type="INTEGER", nullable=True), base.ColumnMeta(name="amount", data_type="NUMERIC", nullable=True)],
            ), patch.object(postgres_core, "_duckdb_connection_from_id", side_effect=lambda _: _open_conn()):
                response = postgres_core.ingest_query_to_duckdb(
                    source_connection_id="pg-1",
                    duckdb_connection_id="duck-1",
                    sql="SELECT snapshot_id, amount FROM sample",
                    target_table="main.bad_amounts",
                    replace=False,
                    chunk_size=200,
                    pipeline_id="pipeline-1",
                )

        self.assertIn(
            "Fell back to cursor metadata while inspecting PostgreSQL result metadata.",
            response.warnings,
        )

    def test_db2_ingest_returns_structured_column_mappings(self):
        source_conn = _FakeDb2Connection()
        duck_conn = _FakeDuckConnection(rows=[])
        with patch.object(db2_core, "_config_from_connection_id", return_value="DATABASE=LOOMDB;"), patch.object(
            db2_core, "_dbapi_connect", return_value=source_conn
        ), patch.object(
            db2_core, "_duckdb_connection_from_id", return_value=duck_conn
        ), patch.object(db2_core.time, "time", return_value=456.0):
            response = db2_core.ingest_query_to_duckdb(
                source_connection_id="db2-1",
                duckdb_connection_id="duck-1",
                sql="SELECT payload, amount FROM sample",
                target_table="main.db2_out",
                replace=False,
                chunk_size=200,
                pipeline_id="pipeline-1",
            )

        self.assertEqual(response.row_count, 1)
        self.assertEqual(len(response.column_mappings), 2)
        self.assertEqual(response.column_mappings[0].source_type, "JSON")
        self.assertEqual(response.column_mappings[0].target_type, "VARCHAR")
        self.assertTrue(response.column_mappings[0].lossy)
        self.assertEqual(response.column_mappings[1].source_type, "DECIMAL")
        self.assertEqual(response.column_mappings[1].target_type, "DOUBLE")
        self.assertTrue(response.column_mappings[1].lossy)
        self.assertEqual(response.column_mappings[1].mapping_mode, "python_mapped")
        self.assertTrue(source_conn.closed)
        self.assertTrue(duck_conn.closed)

    def test_db2_ingest_normalizes_dbapi_type_object_display_types(self):
        source_conn = _FakeDb2TypeObjectConnection()
        duck_conn = _FakeDuckConnection(rows=[])
        with patch.object(db2_core, "_config_from_connection_id", return_value="DATABASE=LOOMDB;"), patch.object(
            db2_core, "_dbapi_connect", return_value=source_conn
        ), patch.object(
            db2_core, "_duckdb_connection_from_id", return_value=duck_conn
        ), patch.object(db2_core.time, "time", return_value=789.0):
            response = db2_core.ingest_query_to_duckdb(
                source_connection_id="db2-1",
                duckdb_connection_id="duck-1",
                sql="SELECT PERSON_ID, EMAIL_TYPE FROM sample",
                target_table="main.db2_type_object_out",
                replace=False,
                chunk_size=200,
                pipeline_id="pipeline-1",
            )

        self.assertEqual(response.row_count, 1)
        self.assertEqual(response.column_mappings[0].source_type, "INTEGER")
        self.assertEqual(response.column_mappings[0].target_type, "INTEGER")
        self.assertEqual(response.column_mappings[1].source_type, "VARCHAR")
        self.assertEqual(response.column_mappings[1].target_type, "VARCHAR")
        self.assertTrue(source_conn.closed)
        self.assertTrue(duck_conn.closed)

    def test_duckdb_object_details_columns_include_persisted_source_type(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "runtime.duckdb"
            conn_res = duckdb_core.connect(base.DuckDbConnectRequest(database=str(db_path)))
            try:
                query_res = duckdb_core.run_query(
                    base.PgQueryRequest(
                        connection_id=conn_res.connection_id,
                        sql='CREATE TABLE "main"."cdh_out" ("snapshot_id" INTEGER, "payload" VARCHAR)',
                    )
                )
                self.assertTrue(query_res.schema_changed)

                duckdb_core.record_ingest_column_mappings(
                    connection_id=conn_res.connection_id,
                    target_table="main.cdh_out",
                    column_mappings=[
                        base.ColumnMappingRecord(
                            ordinal_position=1,
                            source_column="snapshot_id",
                            source_type="integer",
                            target_column="snapshot_id",
                            target_type="INTEGER",
                            connector_type="postgres",
                            mapping_mode="adbc_passthrough",
                        ),
                        base.ColumnMappingRecord(
                            ordinal_position=2,
                            source_column="payload",
                            source_type="jsonb",
                            target_column="payload",
                            target_type="VARCHAR",
                            connector_type="postgres",
                            mapping_mode="adbc_passthrough",
                            warnings=["JSON mapped to VARCHAR by transport."],
                        ),
                    ],
                )

                columns = duckdb_core.get_object_details(
                    conn_res.connection_id,
                    schema="main",
                    name="cdh_out",
                    category="table",
                    tab="columns",
                )
                self.assertEqual(columns[0]["source_type"], "integer")
                self.assertEqual(columns[1]["source_type"], "jsonb")
                self.assertEqual(columns[1]["mapping_mode"], "adbc_passthrough")
                self.assertEqual(columns[1]["mapping_warnings"], ["JSON mapped to VARCHAR by transport."])
            finally:
                duckdb_core._connections.pop(conn_res.connection_id, None)

    def test_column_mapping_metadata_matches_target_columns_case_insensitively(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "runtime.duckdb"
            conn_res = duckdb_core.connect(base.DuckDbConnectRequest(database=str(db_path)))
            try:
                duckdb_core.run_query(
                    base.PgQueryRequest(
                        connection_id=conn_res.connection_id,
                        sql='CREATE TABLE "main"."oracle_out" ("policy_id" INTEGER)',
                    )
                )
                duckdb_core.record_ingest_column_mappings(
                    connection_id=conn_res.connection_id,
                    target_table="main.oracle_out",
                    column_mappings=[
                        base.ColumnMappingRecord(
                            ordinal_position=1,
                            source_column="policy_id",
                            source_type="INTEGER",
                            target_column="POLICY_ID",
                            target_type="NUMBER(10,0)",
                            connector_type="oracle",
                            mapping_mode="egress_remote_schema",
                        ),
                    ],
                )

                columns = duckdb_core.get_object_details(
                    conn_res.connection_id,
                    schema="main",
                    name="oracle_out",
                    category="table",
                    tab="columns",
                )
                self.assertEqual(columns[0]["source_column"], "policy_id")
                self.assertEqual(columns[0]["source_type"], "INTEGER")
                self.assertEqual(columns[0]["target_type"], "NUMBER(10,0)")
                self.assertEqual(columns[0]["connector_type"], "oracle")
                self.assertEqual(columns[0]["mapping_mode"], "egress_remote_schema")
            finally:
                duckdb_core._connections.pop(conn_res.connection_id, None)


if __name__ == "__main__":
    unittest.main()
