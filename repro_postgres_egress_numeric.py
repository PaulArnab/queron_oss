from __future__ import annotations

import argparse
import importlib.metadata as importlib_metadata
import pathlib
import sys
import tempfile
import textwrap
import uuid

_SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
_ORIGINAL_SYS_PATH = list(sys.path)
try:
    sys.path = [entry for entry in sys.path if pathlib.Path(entry or ".").resolve() != _SCRIPT_DIR]
    import duckdb  # type: ignore
finally:
    sys.path = _ORIGINAL_SYS_PATH

import base
import postgres_core


def _package_version(name: str) -> str:
    try:
        return importlib_metadata.version(name)
    except Exception:
        return "unknown"


def _build_connect_request(args: argparse.Namespace, connection_id: str) -> base.PgConnectRequest:
    if args.url:
        return base.PgConnectRequest(
            connection_id=connection_id,
            url=args.url,
            username=args.username,
            password=args.password,
        )
    return base.PgConnectRequest(
        connection_id=connection_id,
        host=args.host,
        port=args.port,
        database=args.database,
        username=args.username,
        password=args.password,
    )


def _create_repro_duckdb(path: pathlib.Path) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute(
            """
            CREATE OR REPLACE TABLE main.policy_servicing_enriched AS
            SELECT
                CAST('P-10001' AS VARCHAR) AS policy_number,
                CAST('C10001' AS VARCHAR) AS customer_number,
                CAST('Alice' AS VARCHAR) AS first_name,
                CAST('Stone' AS VARCHAR) AS last_name,
                CAST('AUTO' AS VARCHAR) AS product_line,
                CAST('ACTIVE' AS VARCHAR) AS policy_status,
                CAST(2450.75 AS DECIMAL(12,2)) AS written_premium,
                CAST(525.10 AS DECIMAL(38,2)) AS total_coverage_premium,
                CAST(1 AS BIGINT) AS claim_count,
                CAST(16652.00 AS DECIMAL(38,2)) AS total_paid_amount,
                CAST('HIGH_TOUCH' AS VARCHAR) AS servicing_segment
            UNION ALL
            SELECT
                CAST('P-10002' AS VARCHAR),
                CAST('C10002' AS VARCHAR),
                CAST('Bob' AS VARCHAR),
                CAST('Gray' AS VARCHAR),
                CAST('HOME' AS VARCHAR),
                CAST('ACTIVE' AS VARCHAR),
                CAST(1800.00 AS DECIMAL(12,2)),
                CAST(125.40 AS DECIMAL(38,2)),
                CAST(0 AS BIGINT),
                CAST(0.00 AS DECIMAL(38,2)),
                CAST('STANDARD' AS VARCHAR)
            """
        )
    finally:
        con.close()


def _describe_query(duckdb_path: pathlib.Path, sql: str) -> None:
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        print("DuckDB describe:")
        for row in con.execute(f"DESCRIBE {sql}").fetchall():
            print("  ", row)
        arrow_table = con.execute(sql).fetch_arrow_table()
        print("Arrow schema:")
        for field in arrow_table.schema:
            print(f"   {field.name}: {field.type}")
    finally:
        con.close()


def _run_case(
    *,
    connection_id: str,
    duckdb_path: pathlib.Path,
    sql: str,
    target_table: str,
    mode: str,
) -> None:
    print()
    print(f"Running egress to {target_table}")
    print(textwrap.indent(sql.strip(), prefix="    "))
    _describe_query(duckdb_path, sql)
    try:
        response = postgres_core.egress_query_from_duckdb(
            target_connection_id=connection_id,
            duckdb_database=str(duckdb_path),
            sql=sql,
            target_table=target_table,
            mode=mode,
        )
        print(f"Result: SUCCESS ({response.row_count} row(s))")
    except Exception as exc:
        print(f"Result: FAILED ({type(exc).__name__})")
        print(f"  {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reproduce PostgreSQL egress numeric behavior outside the notebook pipeline."
    )
    parser.add_argument("--url", help="PostgreSQL connection URL. If omitted, host/port/database are used.")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--database", default="postgres")
    parser.add_argument("--username", default="postgres")
    parser.add_argument("--password", default="")
    parser.add_argument("--schema", default="public", help="Target schema for created repro tables.")
    parser.add_argument("--table-prefix", default="queron_egress_repro")
    parser.add_argument("--mode", default="create", choices=["replace", "append", "create", "create_append"])
    parser.add_argument(
        "--case",
        default="all",
        choices=["all", "raw", "decimal18", "double"],
        help="Which repro query to run.",
    )
    args = parser.parse_args()

    print("Versions:")
    print(f"  duckdb={_package_version('duckdb')}")
    print(f"  pyarrow={_package_version('pyarrow')}")
    print(f"  adbc-driver-postgresql={_package_version('adbc-driver-postgresql')}")
    print(f"  adbc-driver-manager={_package_version('adbc-driver-manager')}")

    connection_id = f"pg-repro-{uuid.uuid4().hex[:8]}"
    req = _build_connect_request(args, connection_id)
    connect_result = postgres_core.connect(req)
    print()
    print(f"Connected with connection_id={connect_result.connection_id}")

    with tempfile.TemporaryDirectory(prefix="queron_pg_egress_repro_") as tmpdir:
        duckdb_path = pathlib.Path(tmpdir) / "runtime.duckdb"
        _create_repro_duckdb(duckdb_path)
        print(f"Synthetic DuckDB repro database: {duckdb_path}")

        cases: list[tuple[str, str]] = []
        if args.case in {"all", "raw"}:
            cases.append(
                (
                    "raw",
                    """
                    SELECT *
                    FROM main.policy_servicing_enriched
                    """,
                )
            )
        if args.case in {"all", "decimal18"}:
            cases.append(
                (
                    "decimal18",
                    """
                    SELECT
                        policy_number,
                        customer_number,
                        first_name,
                        last_name,
                        product_line,
                        policy_status,
                        written_premium,
                        total_coverage_premium,
                        claim_count,
                        CAST(total_paid_amount AS DECIMAL(18,2)) AS total_paid_amount,
                        servicing_segment
                    FROM main.policy_servicing_enriched
                    """,
                )
            )
        if args.case in {"all", "double"}:
            cases.append(
                (
                    "double",
                    """
                    SELECT
                        policy_number,
                        customer_number,
                        first_name,
                        last_name,
                        product_line,
                        policy_status,
                        written_premium,
                        total_coverage_premium,
                        claim_count,
                        CAST(total_paid_amount AS DOUBLE) AS total_paid_amount,
                        servicing_segment
                    FROM main.policy_servicing_enriched
                    """,
                )
            )

        for case_name, sql in cases:
            suffix = uuid.uuid4().hex[:8]
            target_table = f'{args.schema}.{args.table_prefix}_{case_name}_{suffix}'
            _run_case(
                connection_id=connect_result.connection_id,
                duckdb_path=duckdb_path,
                sql=textwrap.dedent(sql).strip(),
                target_table=target_table,
                mode=args.mode,
            )


if __name__ == "__main__":
    main()
