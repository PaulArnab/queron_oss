# Example Pipeline

This page uses one large example to show the complete shape of a Queron project: local files, Python ingress, SQL models, checks, lookups, database ingress, database egress, file egress, runtime vars, and runtime bindings.

For exact command arguments, see [CLI and Python Commands](commands-reference.md). For decorator arguments, see [Pipeline DSL Reference](pipeline-dsl.md).

## Project Files

```text
customer_360_pipeline/
  pipeline.py
  configurations.yaml
  connections.yaml
  local_files/
    vip_customers.csv
    servicing_overrides.jsonl
    segment_targets.parquet
```

## Interactive `pipeline.py`

:::pipeline-example
from pathlib import Path

import queron


queron.pipeline(pipeline_id="customer_360_demo")


@queron.csv.ingress(
    name="vip_customers",
    out="vip_customers",
    path="local_files/vip_customers.csv",
    header=True,
    delimiter=",",
    columns={
        "customer_number": "VARCHAR",
        "vip_flag": "BOOLEAN",
        "vip_reason": "VARCHAR",
    },
)
def vip_customers():
    pass


@queron.postgres.ingress(
    config="PostGres",
    name="policy_core",
    out="policy_core",
    sql=f"""
SELECT
    policy_id,
    policy_number,
    customer_number,
    product_line,
    status,
    written_premium
FROM {queron.source("pg_policy")}
WHERE effective_date <= {queron.var("as_of_date")}
  AND written_premium >= {queron.var("min_premium", default=0)}
""",
)
def policy_core():
    pass


@queron.model.sql(
    name="active_policy_keys",
    out="active_policy_keys",
    query=f"""
SELECT policy_id, policy_number, customer_number
FROM {queron.ref("policy_core")}
WHERE status = 'ACTIVE'
""",
)
def active_policy_keys():
    pass


@queron.postgres.lookup(
    config="PostGres",
    name="stage_pg_active_policy_lookup",
    table="pg_active_policy_lookup",
    sql=f"SELECT policy_id FROM {queron.ref('active_policy_keys')}",
    mode="replace",
    retain=False,
    out="stage_pg_active_policy_lookup",
)
def stage_pg_active_policy_lookup():
    pass


@queron.mysql.lookup(
    config="MySqlMain",
    name="stage_mysql_customer_lookup",
    table="mysql_customer_lookup",
    sql=f"SELECT customer_number FROM {queron.ref('active_policy_keys')}",
    mode="replace",
    retain=False,
    out="stage_mysql_customer_lookup",
)
def stage_mysql_customer_lookup():
    pass


@queron.model.sql(
    name="customer_360",
    out="customer_360",
    query=f"""
SELECT
    p.policy_id,
    p.policy_number,
    p.customer_number,
    p.product_line,
    p.written_premium,
    COALESCE(v.vip_flag, FALSE) AS vip_flag,
    v.vip_reason
FROM {queron.ref("policy_core")} p
LEFT JOIN {queron.ref("vip_customers")} v
  ON p.customer_number = v.customer_number
""",
)
def customer_360():
    pass


@queron.check.fail_if_count(
    name="require_customer_360_rows",
    query=f"SELECT COUNT(*) FROM {queron.ref('customer_360')}",
    operator="<=",
    value=0,
)
def require_customer_360_rows():
    pass


@queron.csv.egress(
    name="export_customer_360_csv",
    path="exports/customer_360.csv",
    sql=f"SELECT * FROM {queron.ref('customer_360')}",
    overwrite=True,
    header=True,
    delimiter=",",
    out="export_customer_360_csv",
)
def export_customer_360_csv():
    pass


@queron.postgres.egress(
    config="PostGres",
    name="write_pg_customer_360",
    table="pg_customer_360_load",
    sql=f"SELECT * FROM {queron.ref('customer_360')}",
    mode="replace",
    retain=False,
    out="write_pg_customer_360",
)
def write_pg_customer_360():
    pass


@queron.runtime_configs
def pipeline_configs():
    return {
        "PostGres": queron.bindings.PostgresBinding(
            host="localhost",
            port=5432,
            database="retail_db",
            username="admin",
            password="password123",
        ),
        "MySqlMain": queron.bindings.MysqlBinding(
            host="localhost",
            port=3306,
            database="billing",
            username="app",
            password="password123",
        ),
    }


if __name__ == "__main__":
    result = queron.run_pipeline(
        Path(__file__).resolve(),
        config_path="configurations.yaml",
        runtime_bindings=pipeline_configs(),
        runtime_vars={
            "as_of_date": "2026-04-01",
            "min_premium": 1000,
        },
        clean_existing=True,
        run_label="local_python_run",
    )
    print(result)
:::end-pipeline-example

## `configurations.yaml`

```yaml
target: dev

sources:
  pg_policy:
    dev:
      schema: public
      table: policy
  pg_claim:
    dev:
      schema: public
      table: claim
  db2_customer:
    dev:
      schema: DB2INST1
      table: CUSTOMER
  mssql_agent:
    dev:
      schema: dbo
      table: agent
  mysql_billing:
    dev:
      schema: billing
      table: invoice
  mariadb_support_case:
    dev:
      schema: support
      table: support_case
  oracle_risk_score:
    dev:
      schema: RISK
      table: RISK_SCORE

lookup:
  pg_active_policy_lookup:
    dev:
      schema: public
      table: active_policy_lookup
  db2_customer_lookup:
    dev:
      schema: DB2INST1
      table: CUSTOMER_LOOKUP
  mssql_agent_lookup:
    dev:
      schema: dbo
      table: agent_lookup
  mysql_billing_lookup:
    dev:
      schema: billing
      table: billing_lookup
  mariadb_case_lookup:
    dev:
      schema: support
      table: case_lookup
  oracle_risk_lookup:
    dev:
      schema: RISK
      table: RISK_LOOKUP

egress:
  pg_customer_360_load:
    dev:
      schema: public
      table: customer_360_load
  db2_customer_360_load:
    dev:
      schema: DB2INST1
      table: CUSTOMER_360_LOAD
  mssql_customer_360_load:
    dev:
      schema: dbo
      table: customer_360_load
  mysql_customer_360_load:
    dev:
      schema: analytics
      table: customer_360_load
  mariadb_customer_360_load:
    dev:
      schema: analytics
      table: customer_360_load
  oracle_customer_360_load:
    dev:
      schema: ANALYTICS
      table: CUSTOMER_360_LOAD
```

## `connections.yaml`

```yaml
connections:
  PostGres:
    type: postgres
    host: localhost
    port: 5432
    database: retail_db
    username: admin
    password_env: POSTGRES_PASSWORD

  DB2Main:
    type: db2
    host: localhost
    port: 50000
    database: LOOMDB
    username: db2inst1
    password_env: DB2_PASSWORD

  MsSqlMain:
    type: mssql
    host: localhost
    port: 1433
    database: RetailDW
    username: sa
    password_env: MSSQL_PASSWORD
    driver: ODBC Driver 18 for SQL Server
    encrypt: true
    trust_server_certificate: true

  MySqlMain:
    type: mysql
    host: localhost
    port: 3306
    database: billing
    username: app
    password_env: MYSQL_PASSWORD

  MariaDbMain:
    type: mariadb
    host: localhost
    port: 3306
    database: support
    username: app
    password_env: MARIADB_PASSWORD

  OracleMain:
    type: oracle
    host: localhost
    port: 1521
    service_name: FREEPDB1
    username: app
    password_env: ORACLE_PASSWORD
```

## Execution Walkthrough

Start from the project folder.

### 1. Compile

Use [`queron compile`](commands-reference.html#compile) to validate decorators, resolve `queron.ref`, `queron.source`, and `queron.lookup`, collect runtime vars, validate the DAG, and write the active contract to the artifact database.

```bash
queron compile pipeline.py --config configurations.yaml --target dev --json
```

Expected result:

- `pipeline.py` is parsed.
- every decorator becomes a node spec.
- `configurations.yaml` resolves source, lookup, and egress relation names.
- runtime vars such as `as_of_date`, `states`, and `min_premium` are added to the contract.
- the active contract is stored under `.queron/customer_360_demo/artifact.duckdb`.

### 2. Run

Use [`queron run`](commands-reference.html#run) to execute the DAG.

```bash
queron run pipeline.py \
  --config configurations.yaml \
  --connections connections.yaml \
  --target dev \
  --run-label nightly_customer_360 \
  --vars-json "{\"as_of_date\":\"2026-04-01\",\"states\":[\"TX\",\"IL\",\"CA\"],\"min_premium\":1000}" \
  --clean-existing \
  --stream-logs
```

Execution order is dependency-driven:

1. Local file and Python ingress nodes create local DuckDB artifacts.
2. Database ingress nodes read from source systems into DuckDB.
3. Lookup nodes publish filtered DuckDB data back into remote lookup tables.
4. Lookup consumer ingress nodes read remote records using `queron.lookup(...)`.
5. `model.sql` nodes combine all sources into `customer_360`.
6. Check nodes validate the result.
7. File egress nodes export CSV, JSONL, and Parquet.
8. Database egress nodes write the final result to every target system.

### 3. Inspect Runs

Use [`queron inspect_runs`](commands-reference.html#inspect-runs) to find the run.

```bash
queron inspect_runs --pipeline-id customer_360_demo --limit 5
```

### 4. Inspect the DAG

Use [`queron inspect_dag`](commands-reference.html#inspect-dag) to see node status, dependencies, artifact names, and current state.

```bash
queron inspect_dag --pipeline-id customer_360_demo --run-label nightly_customer_360
```

### 5. Inspect One Node

Use [`queron inspect_node`](commands-reference.html#inspect-node) to debug a node or dependency slice.

```bash
queron inspect_node customer_360 --pipeline-id customer_360_demo --run-label nightly_customer_360
queron inspect_node customer_360 --pipeline-id customer_360_demo --run-label nightly_customer_360 --upstream
queron inspect_node customer_360 --pipeline-id customer_360_demo --run-label nightly_customer_360 --downstream
```

### 6. Inspect SQL Expansion

Use [`queron inspect_node_query`](commands-reference.html#inspect-node-history-logs-and-query) to see original SQL and resolved SQL.

```bash
queron inspect_node_query customer_360 --pipeline-id customer_360_demo --run-label nightly_customer_360
```

This is where you confirm that:

- `queron.ref("policy_core")` became a local DuckDB table.
- `queron.source("pg_policy")` became the configured PostgreSQL relation.
- `queron.lookup("pg_active_policy_lookup")` became the configured remote lookup table.
- runtime vars remained parameterized.

`queron.source(...)` is optional for stable database ingress sources. If the source table does not vary by environment, the ingress SQL can use a raw external relation such as `public.policy`. Keep using `queron.ref(...)` for all Queron-produced artifacts; raw local artifact names are rejected.

### 7. Inspect Logs

Use [`queron inspect_logs`](commands-reference.html#inspect-logs) and [`queron inspect_node_logs`](commands-reference.html#inspect-node-history-logs-and-query) to debug execution.

```bash
queron inspect_logs --pipeline-id customer_360_demo --run-label nightly_customer_360 --tail 100 --verbose
queron inspect_node_logs customer_360 --pipeline-id customer_360_demo --run-label nightly_customer_360 --tail 50
```

### 8. Export an Artifact

Use [`queron export_artifact`](commands-reference.html#export) to export a materialized DuckDB artifact after a run.

```bash
queron export_artifact \
  --pipeline-id customer_360_demo \
  --run-label nightly_customer_360 \
  --node-name customer_360 \
  --format parquet \
  --output-path exports/customer_360_from_artifact.parquet \
  --overwrite
```

### 9. Open the Graph UI

Use [`queron open_graph`](commands-reference.html#graph-ui) for visual inspection.

```bash
queron open_graph pipeline.py --port 8765
```

Run again and publish live logs:

```bash
queron run pipeline.py \
  --connections connections.yaml \
  --vars-json "{\"as_of_date\":\"2026-04-01\",\"states\":[\"TX\",\"IL\",\"CA\"],\"min_premium\":1000}" \
  --graph-url http://127.0.0.1:8765 \
  --stream-logs
```


