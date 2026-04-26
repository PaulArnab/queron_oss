from __future__ import annotations

import os
import pathlib

import oracledb


ROOT = pathlib.Path(__file__).resolve().parent


def _count_policy(label: str, **kwargs) -> None:
    conn = oracledb.connect(**kwargs)
    try:
        cur = conn.cursor()
        cur.execute("select count(*) from loom_user.policy")
        print(f"{label}: {cur.fetchone()[0]}")
    finally:
        conn.close()


def main() -> None:
    _count_policy(
        "basic",
        user="loom_user",
        password="LoomOraclePass123!",
        dsn="localhost:51521/FREEPDB1",
    )
    _count_policy(
        "uri-style-dsn",
        user="uri_user",
        password="UriOraclePass123!",
        dsn="localhost:51521/FREEPDB1",
    )
    _count_policy(
        "descriptor",
        user="loom_user",
        password="LoomOraclePass123!",
        dsn="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=51521))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))",
    )
    _count_policy(
        "tns-alias",
        user="loom_user",
        password="LoomOraclePass123!",
        dsn="QUERON_ORACLE_LOCAL",
        config_dir=str(ROOT / "tns"),
    )
    os.environ.setdefault("QUERON_ORACLE_PASSWORD", "EnvOraclePass123!")
    _count_policy(
        "password-env-shape",
        user="env_user",
        password=os.environ["QUERON_ORACLE_PASSWORD"],
        dsn="localhost:51521/FREEPDB1",
    )


if __name__ == "__main__":
    main()
