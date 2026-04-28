# User Guide

This guide walks through the normal Queron workflow: create a project, write a pipeline, configure sources and connections, compile, run, inspect, export, and use the graph UI.

For complete Python function and CLI command shapes, see [CLI and Python Commands](commands-reference.md).

## Install

Install the package in editable mode from the repository root:

```bash
pip install -e .
```

Install runtime dependencies:

```bash
pip install -r requirements.txt
```

Some database drivers also require native client libraries, especially DB2, Oracle, and MSSQL.

## Create a Project

```bash
queron init my_pipeline --sample
cd my_pipeline
```

The scaffold includes:

- `pipeline.py`
- `configurations.yaml`
- `.gitignore`

Compile and run:

```bash
queron compile pipeline.py
queron run pipeline.py
```

## Write a Pipeline

A pipeline is a Python file that imports `queron` and declares nodes with decorators.

```python
import queron

queron.pipeline(pipeline_id="numbers_pipeline")


@queron.model.sql(
    name="seed_numbers",
    out="seed_numbers",
    query="""
SELECT 1 AS id
UNION ALL
SELECT 2 AS id
""",
)
def seed_numbers():
    pass


@queron.model.sql(
    name="scaled_numbers",
    out="scaled_numbers",
    query=f"""
SELECT id, id * 10 AS scaled
FROM {queron.ref("seed_numbers")}
""",
)
def scaled_numbers():
    pass
```

Most decorated functions use `pass`. Queron reads the decorator metadata at compile time. `python.ingress` is different: Queron calls the function at runtime and ingests its return value.

## SQL Templates

Use helpers inside SQL strings:

```python
queron.ref("local_artifact")
queron.source("configured_source")
queron.lookup("configured_lookup")
queron.var("runtime_value")
```

Use `queron.ref(...)` for another node's `out`.

Use `queron.source(...)` for a source table from `configurations.yaml`.

Use `queron.lookup(...)` for a lookup table from `configurations.yaml`.

Use `queron.var(...)` for runtime parameters. Values are passed as query parameters, not string-interpolated into SQL.

## Configure Relations

`configurations.yaml` maps logical names to physical database relations.

```yaml
target: dev

sources:
  pg_policy:
    dev:
      schema: public
      table: policy

egress:
  policy_export:
    dev:
      schema: public
      table: policy_export

lookup:
  active_policy_lookup:
    dev:
      schema: public
      table: active_policy_lookup
```

Target resolution order:

1. explicit `--target` or API `target=...`
2. `QUERON_TARGET`
3. `target:` in `configurations.yaml`

## Configure Connections

Database nodes use `config="..."`. The config name must exist in `connections.yaml` unless you pass `runtime_bindings` from Python.

```yaml
connections:
  PostGres:
    type: postgres
    host: localhost
    port: 5432
    database: retail_db
    username: admin
    password_env: POSTGRES_PASSWORD
```

Use `_env` suffixes for secrets:

```yaml
password_env: POSTGRES_PASSWORD
```

Supported connection types:

- `postgres` / `postgresql`
- `db2`
- `mssql` / `sqlserver` / `sql_server`
- `mysql`
- `mariadb`
- `oracle`

## Compile

Compile validates the pipeline and writes an active contract into the artifact database.

```bash
queron compile pipeline.py
```

With target override:

```bash
queron compile pipeline.py --config configurations.yaml --target dev
```

Compile checks:

- node names and outputs
- refs, sources, and lookups
- missing config bindings
- runtime variable placement
- file options
- egress modes
- DAG cycles

## Run

```bash
queron run pipeline.py
```

Common run options:

```bash
queron run pipeline.py --connections connections.yaml
queron run pipeline.py --target-node scaled_numbers
queron run pipeline.py --run-label nightly_2026_04_26
queron run pipeline.py --clean-existing
queron run pipeline.py --stream-logs
```

`--target-node` runs one node and its upstream dependencies.

`--clean-existing` drops existing output tables before execution.

`--set-final` finalizes the latest failed or stale running run before starting a new run.

## Runtime Vars

Declare runtime vars in SQL:

```python
@queron.model.sql(
    name="recent_orders",
    out="recent_orders",
    query=f"""
SELECT *
FROM {queron.ref("orders")}
WHERE order_date >= {queron.var("start_date")}
""",
)
def recent_orders():
    pass
```

Pass values:

```bash
queron run pipeline.py --vars-json '{"start_date": "2026-01-01"}'
```

Or:

```bash
queron run pipeline.py --vars-file vars.json
```

List vars are supported in explicit `IN` positions:

```sql
WHERE status IN ({{ queron.var("statuses") }})
```

Defaults:

```python
queron.var("country", default="US")
```

Allow a value to change during resume:

```python
queron.var("batch_id", mutable_after_start=True)
```

## Resume

Resume the latest failed run:

```bash
queron resume pipeline.py
```

Resume validates the active contract and uses the previous run context. Runtime vars are reused unless explicitly provided and allowed to change.

## Stop

Graceful stop:

```bash
queron stop pipeline.py --reason "maintenance window"
```

Force stop:

```bash
queron force-stop pipeline.py --reason "cancel requested"
```

## Reset

```bash
queron reset-node pipeline.py scaled_numbers
queron reset-upstream pipeline.py scaled_numbers
queron reset-downstream pipeline.py seed_numbers
queron reset-all pipeline.py
```

Reset commands are mainly useful after failed runs, before resume.

## Inspect

From a pipeline folder containing `pipeline.py`:

```bash
queron inspect_runs
queron inspect_logs --tail 50
queron inspect_dag
queron inspect_node scaled_numbers
queron inspect_node scaled_numbers --upstream
queron inspect_node_history scaled_numbers
queron inspect_node_logs scaled_numbers --tail 100
queron inspect_node_query scaled_numbers
```

With explicit selectors:

```bash
queron inspect_dag --pipeline-id my_pipeline --run-id <run_id>
queron inspect_logs --pipeline-id my_pipeline --run-label nightly_2026_04_26
```

## Export Artifacts

Export by node:

```bash
queron export_artifact --node-name scaled_numbers --format csv
```

Export by physical artifact table:

```bash
queron export_artifact --artifact-name main.scaled_numbers --format parquet
```

Specify output:

```bash
queron export_artifact --node-name scaled_numbers --format json --output-path exports/scaled.json --overwrite
```

## Graph UI

Compile or run first:

```bash
queron open_graph pipeline.py
```

Without opening a browser:

```bash
queron open_graph pipeline.py --no-browser --port 8765
```

Send live logs from a run to an open graph server:

```bash
queron run pipeline.py --graph-url http://127.0.0.1:8765
```
