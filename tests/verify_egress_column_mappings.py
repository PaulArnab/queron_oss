from __future__ import annotations

import pathlib
import sys

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from base import ColumnMeta, ConnectorEgressResponse
import db2_core
from duckdb_driver import connect_duckdb
import mariadb_core
import mssql_core
import mysql_core
import oracle_core
import postgres_core


def _assert_mapping_shape(mapping, *, connector: str, mode: str, target_type: str) -> None:
    assert mapping.source_column == "amount"
    assert mapping.source_type == "DECIMAL(38,10)"
    assert mapping.target_column == "amount"
    assert mapping.target_type == target_type
    assert mapping.connector_type == connector
    assert mapping.mapping_mode == mode


def test_connector_response_accepts_column_mappings() -> None:
    response = ConnectorEgressResponse(
        target_name="target",
        row_count=1,
        column_mappings=[
            postgres_core._build_egress_inferred_column_mappings(
                [ColumnMeta(name="amount", data_type="DECIMAL(38,10)")]
            )[0]
        ],
        created_at=1.0,
    )
    assert len(response.column_mappings) == 1


def test_postgres_egress_mapping_modes() -> None:
    source = [ColumnMeta(name="amount", data_type="DECIMAL(38,10)")]
    inferred = postgres_core._build_egress_inferred_column_mappings(source)
    remote = postgres_core._build_egress_remote_schema_column_mappings(
        source,
        [ColumnMeta(name="amount", data_type="numeric(12,2)")],
        fallback_mappings=inferred,
    )
    _assert_mapping_shape(inferred[0], connector="postgres", mode="egress_inferred", target_type="NUMERIC(38,10)")
    _assert_mapping_shape(remote[0], connector="postgres", mode="egress_remote_schema", target_type="numeric(12,2)")


def test_db2_egress_mapping_modes() -> None:
    source = [ColumnMeta(name="amount", data_type="DECIMAL(31,2)")]
    inferred = db2_core._build_egress_inferred_column_mappings(source)
    remote = db2_core._build_egress_remote_schema_column_mappings(
        source,
        [ColumnMeta(name="amount", data_type="DECIMAL(10,2)")],
        fallback_mappings=inferred,
    )
    assert inferred[0].mapping_mode == "egress_inferred"
    assert remote[0].mapping_mode == "egress_remote_schema"
    assert remote[0].target_type == "DECIMAL(10,2)"


def test_db2_varchar_egress_uses_measured_length_with_padding() -> None:
    source = [ColumnMeta(name="customer_name", data_type="VARCHAR", max_length=18)]
    inferred = db2_core._build_egress_inferred_column_mappings(source)
    assert inferred[0].target_type == "VARCHAR(36)"
    assert inferred[0].warnings == []


def test_db2_varchar_egress_falls_back_without_measured_length() -> None:
    source = [ColumnMeta(name="customer_name", data_type="VARCHAR")]
    inferred = db2_core._build_egress_inferred_column_mappings(source)
    assert inferred[0].target_type == "VARCHAR(32672)"
    assert inferred[0].warnings


def test_db2_varchar_egress_caps_oversized_measured_length() -> None:
    source = [ColumnMeta(name="notes", data_type="VARCHAR", max_length=40000)]
    inferred = db2_core._build_egress_inferred_column_mappings(source)
    assert inferred[0].target_type == "VARCHAR(32672)"
    assert inferred[0].warnings


def test_db2_measures_varchar_lengths_from_duckdb_result() -> None:
    conn = connect_duckdb()
    try:
        columns = [
            ColumnMeta(name="short_text", data_type="VARCHAR"),
            ColumnMeta(name="amount", data_type="INTEGER"),
            ColumnMeta(name="empty_text", data_type="VARCHAR"),
        ]
        warnings = db2_core._measure_db2_string_column_lengths(
            conn,
            """
            SELECT 'abcd' AS short_text, 1 AS amount, NULL AS empty_text
            UNION ALL
            SELECT 'abcdefghijklmnopqr' AS short_text, 2 AS amount, '' AS empty_text
            """,
            columns,
        )
        assert warnings == []
        assert columns[0].max_length == 18
        assert columns[1].max_length is None
        assert columns[2].max_length == 0
    finally:
        conn.close()


def test_db2_remote_schema_clears_stale_fallback_warning() -> None:
    source = [ColumnMeta(name="amount", data_type="DECIMAL(38,10)")]
    fallback = db2_core._build_egress_inferred_column_mappings(source)
    assert fallback[0].target_type == "DOUBLE"
    assert fallback[0].warnings

    remote = db2_core._build_egress_remote_schema_column_mappings(
        source,
        [ColumnMeta(name="amount", data_type="DECIMAL(18,2)")],
        fallback_mappings=fallback,
    )
    assert remote[0].target_type == "DECIMAL(18,2)"
    assert remote[0].warnings == []


def test_mssql_egress_mapping_modes() -> None:
    source = [ColumnMeta(name="amount", data_type="DECIMAL(38,10)")]
    inferred = mssql_core._build_egress_inferred_column_mappings(source)
    remote = mssql_core._build_egress_remote_schema_column_mappings(
        source,
        [ColumnMeta(name="amount", data_type="DECIMAL(12,2)")],
        fallback_mappings=inferred,
    )
    _assert_mapping_shape(inferred[0], connector="mssql", mode="egress_inferred", target_type="DECIMAL(38,10)")
    _assert_mapping_shape(remote[0], connector="mssql", mode="egress_remote_schema", target_type="DECIMAL(12,2)")


def test_mssql_varchar_egress_uses_measured_length_with_padding() -> None:
    source = [ColumnMeta(name="customer_name", data_type="VARCHAR", max_length=18)]
    inferred = mssql_core._build_egress_inferred_column_mappings(source)
    assert inferred[0].target_type == "NVARCHAR(36)"
    assert inferred[0].warnings == []


def test_mssql_varchar_egress_falls_back_without_measured_length() -> None:
    source = [ColumnMeta(name="customer_name", data_type="VARCHAR")]
    inferred = mssql_core._build_egress_inferred_column_mappings(source)
    assert inferred[0].target_type == "NVARCHAR(MAX)"
    assert inferred[0].warnings


def test_mssql_varchar_egress_uses_max_for_oversized_measured_length() -> None:
    source = [ColumnMeta(name="notes", data_type="VARCHAR", max_length=5000)]
    inferred = mssql_core._build_egress_inferred_column_mappings(source)
    assert inferred[0].target_type == "NVARCHAR(MAX)"
    assert inferred[0].warnings


def test_mssql_measures_varchar_lengths_from_duckdb_result() -> None:
    conn = connect_duckdb()
    try:
        columns = [
            ColumnMeta(name="short_text", data_type="VARCHAR"),
            ColumnMeta(name="amount", data_type="INTEGER"),
            ColumnMeta(name="empty_text", data_type="VARCHAR"),
        ]
        warnings = mssql_core._measure_mssql_string_column_lengths(
            conn,
            """
            SELECT 'abcd' AS short_text, 1 AS amount, NULL AS empty_text
            UNION ALL
            SELECT 'abcdefghijklmnopqr' AS short_text, 2 AS amount, '' AS empty_text
            """,
            columns,
        )
        assert warnings == []
        assert columns[0].max_length == 18
        assert columns[1].max_length is None
        assert columns[2].max_length == 0
    finally:
        conn.close()


def test_mysql_egress_mapping_modes() -> None:
    source = [ColumnMeta(name="amount", data_type="DECIMAL(38,10)")]
    inferred = mysql_core._build_egress_inferred_column_mappings(source)
    remote = mysql_core._build_egress_remote_schema_column_mappings(
        source,
        [ColumnMeta(name="amount", data_type="decimal(12,2)")],
        fallback_mappings=inferred,
    )
    _assert_mapping_shape(inferred[0], connector="mysql", mode="egress_inferred", target_type="DECIMAL(38,10)")
    _assert_mapping_shape(remote[0], connector="mysql", mode="egress_remote_schema", target_type="decimal(12,2)")


def test_mariadb_egress_mapping_modes() -> None:
    source = [ColumnMeta(name="amount", data_type="DECIMAL(38,10)")]
    inferred = mariadb_core._build_egress_inferred_column_mappings(source)
    remote = mariadb_core._build_egress_remote_schema_column_mappings(
        source,
        [ColumnMeta(name="amount", data_type="decimal(12,2)")],
        fallback_mappings=inferred,
    )
    _assert_mapping_shape(inferred[0], connector="mariadb", mode="egress_inferred", target_type="DECIMAL(38,10)")
    _assert_mapping_shape(remote[0], connector="mariadb", mode="egress_remote_schema", target_type="decimal(12,2)")


def test_oracle_egress_mapping_modes() -> None:
    source = [ColumnMeta(name="amount", data_type="DECIMAL(38,10)")]
    inferred = oracle_core._build_egress_inferred_column_mappings(source)
    remote = oracle_core._build_egress_remote_schema_column_mappings(
        source,
        [ColumnMeta(name="amount", data_type="NUMBER(12,2)")],
        fallback_mappings=inferred,
    )
    _assert_mapping_shape(inferred[0], connector="oracle", mode="egress_inferred", target_type="NUMBER(38,10)")
    _assert_mapping_shape(remote[0], connector="oracle", mode="egress_remote_schema", target_type="NUMBER(12,2)")


def test_oracle_remote_schema_clears_stale_fallback_warning() -> None:
    source = [ColumnMeta(name="amount", data_type="DECIMAL(39,10)")]
    fallback = oracle_core._build_egress_inferred_column_mappings(source)
    assert fallback[0].target_type == "BINARY_DOUBLE"
    assert fallback[0].warnings

    remote = oracle_core._build_egress_remote_schema_column_mappings(
        source,
        [ColumnMeta(name="amount", data_type="NUMBER(18,2)")],
        fallback_mappings=fallback,
    )
    assert remote[0].target_type == "NUMBER(18,2)"
    assert remote[0].warnings == []


if __name__ == "__main__":
    test_connector_response_accepts_column_mappings()
    test_postgres_egress_mapping_modes()
    test_db2_egress_mapping_modes()
    test_db2_varchar_egress_uses_measured_length_with_padding()
    test_db2_varchar_egress_falls_back_without_measured_length()
    test_db2_varchar_egress_caps_oversized_measured_length()
    test_db2_measures_varchar_lengths_from_duckdb_result()
    test_db2_remote_schema_clears_stale_fallback_warning()
    test_mssql_egress_mapping_modes()
    test_mssql_varchar_egress_uses_measured_length_with_padding()
    test_mssql_varchar_egress_falls_back_without_measured_length()
    test_mssql_varchar_egress_uses_max_for_oversized_measured_length()
    test_mssql_measures_varchar_lengths_from_duckdb_result()
    test_mysql_egress_mapping_modes()
    test_mariadb_egress_mapping_modes()
    test_oracle_egress_mapping_modes()
    test_oracle_remote_schema_clears_stale_fallback_warning()
