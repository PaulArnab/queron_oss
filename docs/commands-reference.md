# CLI and Python Commands

This page lists Python functions first, then CLI commands. Both sections are ordered by the way a user usually works: initialize, author/compile, run, resume/stop/reset, inspect, export, and graph UI.

## Python Functions

### Project Setup

```python
queron.init_pipeline_project(project_path, *, sample=False, force=False)
```

| Argument | Required | Description |
|---|---:|---|
| `project_path` | yes | Directory to create or update. |
| `sample` | no | Write runnable sample pipeline. |
| `force` | no | Overwrite scaffold files. |

### SQL Template Helpers

```python
queron.ref(name)
queron.source(name)
queron.lookup(name)
```

| Function | Argument | Required | Description |
|---|---|---:|---|
| `ref` | `name` | yes | Local artifact name produced by a node's `out`. |
| `source` | `name` | yes | Logical source name from `configurations.yaml`. |
| `lookup` | `name` | yes | Logical lookup name from `configurations.yaml`. |

```python
queron.var(name, *, log_value=False, mutable_after_start=False, default=unset)
```

| Argument | Required | Description |
|---|---:|---|
| `name` | yes | Runtime variable name. |
| `log_value` | no | Allow value to be logged. Defaults to `False`. |
| `mutable_after_start` | no | Allow value to change on resume. Defaults to `False`. |
| `default` | no | Python literal default value. |

### Local SQL Model

```python
@queron.model.sql(*, name, query, out, materialized="artifact", depends_on=None)
def node_function():
    pass
```

| Argument | Required | Description |
|---|---:|---|
| `name` | yes | Unique node name. |
| `query` | yes | DuckDB SQL query. |
| `out` | yes | Logical artifact name produced by this node. |
| `materialized` | no | Materialization mode. Defaults to `artifact`. |
| `depends_on` | no | Manual dependency node name or list of names. |

### Python Ingress

```python
@queron.python.ingress(fn=None, *, name=None, out, depends_on=None)
def node_function():
    return data
```

| Argument | Required | Description |
|---|---:|---|
| `fn` | no | Callable when used as a bare decorator. |
| `name` | no | Unique node name. Defaults to function name. |
| `out` | yes | Logical artifact name. |
| `depends_on` | no | Manual dependency node name or list of names. |

The function must not require arguments.

### File Ingress

```python
@queron.csv.ingress(
    *,
    name,
    out,
    path,
    header=True,
    delimiter=",",
    quote=None,
    escape=None,
    skip_rows=0,
    columns=None,
    depends_on=None,
)
def node_function():
    pass
```

| Argument | Required | Description |
|---|---:|---|
| `name` | yes | Unique node name. |
| `out` | yes | Logical artifact name. |
| `path` | yes | CSV file path. |
| `header` | no | Whether first row is a header. Defaults to `True`. |
| `delimiter` | no | Single-character delimiter. Defaults to comma. |
| `quote` | no | Optional single-character quote override. |
| `escape` | no | Optional single-character escape override. |
| `skip_rows` | no | Rows to skip before reading. |
| `columns` | no | Explicit column type mapping. Required when `header=False`. |
| `depends_on` | no | Manual dependency node name or list of names. |

```python
@queron.jsonl.ingress(*, name, out, path, depends_on=None)
@queron.parquet.ingress(*, name, out, path, depends_on=None)
```

| Argument | Required | Description |
|---|---:|---|
| `name` | yes | Unique node name. |
| `out` | yes | Logical artifact name. |
| `path` | yes | Input file path. |
| `depends_on` | no | Manual dependency node name or list of names. |

```python
@queron.file.ingress(
    *,
    name,
    out,
    path,
    format=None,
    header=True,
    delimiter=",",
    quote=None,
    escape=None,
    skip_rows=0,
    columns=None,
    depends_on=None,
)
```

| Argument | Required | Description |
|---|---:|---|
| `name` | yes | Unique node name. |
| `out` | yes | Logical artifact name. |
| `path` | yes | Input file path. |
| `format` | no | `csv`, `jsonl`, or `parquet`. Can be inferred from extension. |
| `header` | no | CSV only. Defaults to `True`. |
| `delimiter` | no | CSV only. Defaults to comma. |
| `quote` | no | CSV only. |
| `escape` | no | CSV only. |
| `skip_rows` | no | CSV only. |
| `columns` | no | CSV only. |
| `depends_on` | no | Manual dependency node name or list of names. |

### File Egress

```python
@queron.csv.egress(
    *,
    name,
    path,
    sql,
    overwrite=False,
    header=True,
    delimiter=",",
    out,
    depends_on=None,
)
```

| Argument | Required | Description |
|---|---:|---|
| `name` | yes | Unique node name. |
| `path` | yes | Output CSV path. |
| `sql` | yes | DuckDB SQL query to export. |
| `overwrite` | no | Replace existing file. |
| `header` | no | Write header row. |
| `delimiter` | no | Single-character delimiter. |
| `out` | yes | Logical artifact name for this egress node. |
| `depends_on` | no | Manual dependency node name or list of names. |

```python
@queron.jsonl.egress(*, name, path, sql, overwrite=False, out, depends_on=None)
```

| Argument | Required | Description |
|---|---:|---|
| `name` | yes | Unique node name. |
| `path` | yes | Output JSONL path. |
| `sql` | yes | DuckDB SQL query to export. |
| `overwrite` | no | Replace existing file. |
| `out` | yes | Logical artifact name. |
| `depends_on` | no | Manual dependency node name or list of names. |

```python
@queron.parquet.egress(*, name, path, sql, overwrite=False, compression=None, out, depends_on=None)
```

| Argument | Required | Description |
|---|---:|---|
| `name` | yes | Unique node name. |
| `path` | yes | Output Parquet path. |
| `sql` | yes | DuckDB SQL query to export. |
| `overwrite` | no | Replace existing file. |
| `compression` | no | Parquet compression setting. |
| `out` | yes | Logical artifact name. |
| `depends_on` | no | Manual dependency node name or list of names. |

### Database Ingress

```python
@queron.<driver>.ingress(*, config, name, out, sql, depends_on=None)
```

Drivers:

```python
queron.postgres.ingress(...)
queron.db2.ingress(...)
queron.mssql.ingress(...)
queron.mysql.ingress(...)
queron.mariadb.ingress(...)
queron.oracle.ingress(...)
```

| Argument | Required | Description |
|---|---:|---|
| `config` | yes | Connection binding name. |
| `name` | yes | Unique node name. |
| `out` | yes | Logical artifact name. |
| `sql` | yes | Source database SQL. |
| `depends_on` | no | Manual dependency node name or list of names. |

### Database Egress

```python
@queron.<driver>.egress(*, config, name, table, sql, mode="replace", retain=False, out, depends_on=None)
```

Drivers:

```python
queron.postgres.egress(...)
queron.db2.egress(...)
queron.mssql.egress(...)
queron.mysql.egress(...)
queron.mariadb.egress(...)
queron.oracle.egress(...)
```

| Argument | Required | Description |
|---|---:|---|
| `config` | yes | Connection binding name. |
| `name` | yes | Unique node name. |
| `table` | yes | Remote table or configured egress relation name. |
| `sql` | yes | DuckDB SQL query to write. |
| `mode` | no | `replace`, `append`, `create`, or `create_append`. |
| `retain` | no | Retain remote table when supported. Not exposed by DB2 or MSSQL decorators. |
| `out` | yes | Logical artifact name. |
| `depends_on` | no | Manual dependency node name or list of names. |

### Database Lookup

```python
@queron.<driver>.lookup(*, config, name, table, sql, mode="replace", retain=False, out, depends_on=None)
```

Drivers:

```python
queron.postgres.lookup(...)
queron.db2.lookup(...)
queron.mssql.lookup(...)
queron.mysql.lookup(...)
queron.mariadb.lookup(...)
queron.oracle.lookup(...)
```

| Argument | Required | Description |
|---|---:|---|
| `config` | yes | Connection binding name. |
| `name` | yes | Unique node name. |
| `table` | yes | Remote lookup table or configured lookup relation name. |
| `sql` | yes | DuckDB SQL query used to build lookup table. |
| `mode` | no | `replace`, `append`, `create`, or `create_append`. |
| `retain` | no | Retain remote lookup table when supported. Not exposed by DB2 or MSSQL decorators. |
| `out` | yes | Logical artifact name. |
| `depends_on` | no | Manual dependency node name or list of names. |

### Checks

```python
@queron.check.fail_if_count(*, name, query, operator, value, depends_on=None)
```

| Argument | Required | Description |
|---|---:|---|
| `name` | yes | Unique node name. |
| `query` | yes | SQL returning one numeric scalar. |
| `operator` | yes | One of `==`, `=`, `!=`, `>`, `>=`, `<`, `<=`. |
| `value` | yes | Numeric comparison value. |
| `depends_on` | no | Manual dependency node name or list of names. |

```python
@queron.check.fail_if_true(*, name, query, depends_on=None)
```

| Argument | Required | Description |
|---|---:|---|
| `name` | yes | Unique node name. |
| `query` | yes | SQL returning one boolean scalar. |
| `depends_on` | no | Manual dependency node name or list of names. |

### Pipeline Declaration

```python
queron.pipeline(pipeline_id="my_pipeline")
```

Declare the pipeline ID at module top level in `pipeline.py`. This is required before compile/run.

Shorthand:

```python
queron.pipeline("my_pipeline")
```

### Runtime Config Provider

```python
@queron.runtime_configs
def pipeline_configs():
    return {
        "POSTGRES_LOCAL": queron.bindings.PostgresBinding(
            config_factory=_postgres_config,
            connect_timeout_seconds=10,
        ),
        "DB2_LOCAL": queron.bindings.Db2Binding(
            config_factory=_db2_config,
            connect_timeout_seconds=10,
        ),
    }
```

The function name is not fixed. `queron run pipeline.py` calls the decorated provider when present and uses the returned dict as `runtime_bindings`.

### Compile and Run API

```python
queron.compile_pipeline(pipeline_path, *, config_path=None, target=None)
```

| Argument | Required | Description |
|---|---:|---|
| `pipeline_path` | yes | Pipeline Python file. |
| `config_path` | no | Path to `configurations.yaml`. |
| `target` | no | Target environment override. |

```python
queron.run_pipeline(
    pipeline_path,
    *,
    config_path=None,
    connections_path=None,
    runtime_bindings=None,
    runtime_vars=None,
    target=None,
    target_node=None,
    clean_existing=False,
    set_final=False,
    run_label=None,
    on_log=None,
)
```

| Argument | Required | Description |
|---|---:|---|
| `pipeline_path` | yes | Pipeline Python file. |
| `config_path` | no | Path to `configurations.yaml`. |
| `connections_path` | no | Path to `connections.yaml`. |
| `runtime_bindings` | no | In-memory connection bindings keyed by config name. |
| `runtime_vars` | no | Runtime variable values. |
| `target` | no | Target environment override. |
| `target_node` | no | Run one node and its upstream dependencies. |
| `clean_existing` | no | Drop existing output tables before execution. |
| `set_final` | no | Finalize stale failed/running run before starting. |
| `run_label` | no | Unique run label. |
| `on_log` | no | Log callback. |

```python
queron.resume_pipeline(
    pipeline_path,
    *,
    config_path=None,
    connections_path=None,
    runtime_bindings=None,
    runtime_vars=None,
    target=None,
    on_log=None,
)
```

| Argument | Required | Description |
|---|---:|---|
| `pipeline_path` | yes | Pipeline Python file. |
| `config_path` | no | Path to `configurations.yaml`. |
| `connections_path` | no | Path to `connections.yaml`. |
| `runtime_bindings` | no | In-memory connection bindings. |
| `runtime_vars` | no | Runtime vars for resume. |
| `target` | no | Target environment override. |
| `on_log` | no | Log callback. |

### Stop API

```python
queron.stop_pipeline(pipeline_path, *, run_id=None, reason=None)
queron.force_stop_pipeline(pipeline_path, *, run_id=None, reason=None)
```

| Argument | Required | Description |
|---|---:|---|
| `pipeline_path` | yes | Pipeline Python file. |
| `run_id` | no | Running run ID. Defaults to latest running run. |
| `reason` | no | Reason stored with stop request. |

### Reset API

```python
queron.reset_node(pipeline_path, *, node_name, config_path=None, target=None, on_log=None)
queron.reset_downstream(pipeline_path, *, node_name, config_path=None, target=None, on_log=None)
queron.reset_upstream(pipeline_path, *, node_name, config_path=None, target=None, on_log=None)
queron.reset_all(pipeline_path, *, config_path=None, target=None, on_log=None)
```

| Argument | Required | Description |
|---|---:|---|
| `pipeline_path` | yes | Pipeline Python file. |
| `node_name` | yes for node/upstream/downstream | Node to reset or use as selection root. |
| `config_path` | no | Path to `configurations.yaml`. |
| `target` | no | Target environment override. |
| `on_log` | no | Log callback. |

### Inspect API

```python
queron.inspect_dag(artifact_path, *, run_id=None, run_label=None)
```

| Argument | Required | Description |
|---|---:|---|
| `artifact_path` | yes | Artifact DuckDB path. |
| `run_id` | no | Run ID to inspect. |
| `run_label` | no | Run label to inspect. |

```python
queron.inspect_node(artifact_path, node_name, *, run_id=None, run_label=None, upstream=False, downstream=False)
```

| Argument | Required | Description |
|---|---:|---|
| `artifact_path` | yes | Artifact DuckDB path. |
| `node_name` | yes | Node to inspect. |
| `run_id` | no | Run ID to inspect. |
| `run_label` | no | Run label to inspect. |
| `upstream` | no | Include upstream dependencies. |
| `downstream` | no | Include downstream dependents. |

```python
queron.inspect_node_history(artifact_path, node_name, *, run_id=None, run_label=None)
queron.inspect_node_logs(artifact_path, node_name, *, run_id=None, run_label=None, tail=None)
queron.inspect_node_query(artifact_path, node_name, *, run_id=None, run_label=None)
```

| Argument | Required | Description |
|---|---:|---|
| `artifact_path` | yes | Artifact DuckDB path. |
| `node_name` | yes | Node to inspect. |
| `run_id` | no | Run ID to inspect. |
| `run_label` | no | Run label to inspect. |
| `tail` | no | Only for logs; return last N events. |

### Export API

```python
queron.export_artifact(
    artifact_path,
    *,
    node_name=None,
    artifact_name=None,
    run_id=None,
    run_label=None,
    format="csv",
    output_path=None,
    overwrite=False,
)
```

| Argument | Required | Description |
|---|---:|---|
| `artifact_path` | yes | Artifact DuckDB path. |
| `node_name` | no | Node whose artifact should be exported. |
| `artifact_name` | no | Physical artifact table, for example `main.policy_core`. |
| `run_id` | no | Run ID to export. |
| `run_label` | no | Run label to export. |
| `format` | no | `csv`, `parquet`, or `json`. |
| `output_path` | no | Explicit output file path. |
| `overwrite` | no | Replace output file. |

Use either `node_name` or `artifact_name`.

## CLI Commands

### Project Setup

```bash
queron init <path> [--sample] [--force] [--json]
```

| Argument/Flag | Required | Description |
|---|---:|---|
| `path` | yes | Directory to create or update. |
| `--sample` | no | Create runnable sample project. |
| `--force` | no | Overwrite scaffold files. |
| `--json` | no | Emit JSON output. |

### Compile

```bash
queron compile <pipeline> [--config CONFIG_PATH] [--target TARGET] [--json]
```

| Argument/Flag | Required | Description |
|---|---:|---|
| `pipeline` | yes | Pipeline Python file. |
| `--config CONFIG_PATH` | no | Path to `configurations.yaml`. |
| `--target TARGET` | no | Target environment override. |
| `--json` | no | Emit JSON output. |

### Run

```bash
queron run <pipeline> [--config CONFIG_PATH] [--target TARGET] [--connections CONNECTIONS_PATH]
                  [--target-node TARGET_NODE] [--run-label RUN_LABEL] [--clean-existing]
                  [--set-final] [--stream-logs] [--graph-url GRAPH_URL]
                  [--vars-json VARS_JSON] [--vars-file VARS_FILE] [--json]
```

| Argument/Flag | Required | Description |
|---|---:|---|
| `pipeline` | yes | Pipeline Python file. |
| `--config CONFIG_PATH` | no | Path to `configurations.yaml`. |
| `--target TARGET` | no | Target environment override. |
| `--connections CONNECTIONS_PATH` | no | Path to `connections.yaml`. |
| `--target-node TARGET_NODE` | no | Run one node plus upstream dependencies. |
| `--run-label RUN_LABEL` | no | Unique run label. |
| `--clean-existing` | no | Drop existing output tables before run. |
| `--set-final` | no | Finalize latest failed/stale running run before starting. |
| `--stream-logs` | no | Print runtime logs while running. |
| `--graph-url GRAPH_URL` | no | Publish live logs to graph server. |
| `--vars-json VARS_JSON` | no | Runtime vars as JSON object string. |
| `--vars-file VARS_FILE` | no | Runtime vars JSON file. |
| `--json` | no | Emit JSON output. |

### Resume

```bash
queron resume <pipeline> [--config CONFIG_PATH] [--target TARGET] [--connections CONNECTIONS_PATH]
                     [--stream-logs] [--graph-url GRAPH_URL]
                     [--vars-json VARS_JSON] [--vars-file VARS_FILE] [--json]
```

| Argument/Flag | Required | Description |
|---|---:|---|
| `pipeline` | yes | Pipeline Python file. |
| `--config CONFIG_PATH` | no | Path to `configurations.yaml`. |
| `--target TARGET` | no | Target environment override. |
| `--connections CONNECTIONS_PATH` | no | Path to `connections.yaml`. |
| `--stream-logs` | no | Print runtime logs while resuming. |
| `--graph-url GRAPH_URL` | no | Publish live logs to graph server. |
| `--vars-json VARS_JSON` | no | Runtime vars as JSON object string. |
| `--vars-file VARS_FILE` | no | Runtime vars JSON file. |
| `--json` | no | Emit JSON output. |

### Stop

```bash
queron stop <pipeline> [--run-id RUN_ID] [--reason REASON] [--json]
queron force-stop <pipeline> [--run-id RUN_ID] [--reason REASON] [--json]
```

| Argument/Flag | Required | Description |
|---|---:|---|
| `pipeline` | yes | Pipeline Python file. |
| `--run-id RUN_ID` | no | Running run ID. Defaults to latest running run. |
| `--reason REASON` | no | Reason to record with stop request. |
| `--json` | no | Emit JSON output. |

### Reset

```bash
queron reset-node <pipeline> <node_name> [--config CONFIG_PATH] [--target TARGET] [--json]
queron reset-downstream <pipeline> <node_name> [--config CONFIG_PATH] [--target TARGET] [--json]
queron reset-upstream <pipeline> <node_name> [--config CONFIG_PATH] [--target TARGET] [--json]
queron reset-all <pipeline> [--config CONFIG_PATH] [--target TARGET] [--json]
```

| Argument/Flag | Required | Description |
|---|---:|---|
| `pipeline` | yes | Pipeline Python file. |
| `node_name` | yes for node/upstream/downstream | Node to reset or use as graph selection root. |
| `--config CONFIG_PATH` | no | Path to `configurations.yaml`. |
| `--target TARGET` | no | Target environment override. |
| `--json` | no | Emit JSON output. |

### Inspect Runs

```bash
queron inspect_runs [--pipeline-id PIPELINE_ID] [--limit LIMIT]
```

| Argument/Flag | Required | Description |
|---|---:|---|
| `--pipeline-id PIPELINE_ID` | no | Pipeline ID. Defaults from local `pipeline.py`. |
| `--limit LIMIT` | no | Maximum run count. |

### Inspect Logs

```bash
queron inspect_logs [--pipeline-id PIPELINE_ID] [--run-id RUN_ID] [--run-label RUN_LABEL]
                    [--tail TAIL] [--verbose] [--json]
```

| Argument/Flag | Required | Description |
|---|---:|---|
| `--pipeline-id PIPELINE_ID` | no | Pipeline ID. Defaults from local `pipeline.py`. |
| `--run-id RUN_ID` | no | Run ID to inspect. |
| `--run-label RUN_LABEL` | no | Run label to inspect. |
| `--tail TAIL` | no | Return only last N log lines. |
| `--verbose` | no | Include source, kind, artifact, and details. |
| `--json` | no | Emit JSON output. |

### Inspect DAG

```bash
queron inspect_dag [--pipeline-id PIPELINE_ID] [--run-id RUN_ID] [--run-label RUN_LABEL]
```

| Argument/Flag | Required | Description |
|---|---:|---|
| `--pipeline-id PIPELINE_ID` | no | Pipeline ID. Defaults from local `pipeline.py`. |
| `--run-id RUN_ID` | no | Run ID to inspect. |
| `--run-label RUN_LABEL` | no | Run label to inspect. |

### Inspect Node

```bash
queron inspect_node <node_name> [--pipeline-id PIPELINE_ID] [--run-id RUN_ID]
                    [--run-label RUN_LABEL] [--upstream] [--downstream]
```

| Argument/Flag | Required | Description |
|---|---:|---|
| `node_name` | yes | Node to inspect. |
| `--pipeline-id PIPELINE_ID` | no | Pipeline ID. Defaults from local `pipeline.py`. |
| `--run-id RUN_ID` | no | Run ID to inspect. |
| `--run-label RUN_LABEL` | no | Run label to inspect. |
| `--upstream` | no | Include upstream dependencies. |
| `--downstream` | no | Include downstream dependents. |

### Inspect Node History, Logs, and Query

```bash
queron inspect_node_history <node_name> [--pipeline-id PIPELINE_ID] [--run-id RUN_ID] [--run-label RUN_LABEL]
queron inspect_node_logs <node_name> [--pipeline-id PIPELINE_ID] [--run-id RUN_ID] [--run-label RUN_LABEL] [--tail TAIL]
queron inspect_node_query <node_name> [--pipeline-id PIPELINE_ID] [--run-id RUN_ID] [--run-label RUN_LABEL]
```

| Argument/Flag | Required | Description |
|---|---:|---|
| `node_name` | yes | Node to inspect. |
| `--pipeline-id PIPELINE_ID` | no | Pipeline ID. Defaults from local `pipeline.py`. |
| `--run-id RUN_ID` | no | Run ID to inspect. |
| `--run-label RUN_LABEL` | no | Run label to inspect. |
| `--tail TAIL` | no | Only for `inspect_node_logs`; return last N events. |

### Export

```bash
queron export_artifact [--pipeline-id PIPELINE_ID] [--run-id RUN_ID] [--run-label RUN_LABEL]
                       [--node-name NODE_NAME] [--artifact-name ARTIFACT_NAME]
                       [--format csv|parquet|json] [--output-path OUTPUT_PATH]
                       [--overwrite] [--json]
```

| Argument/Flag | Required | Description |
|---|---:|---|
| `--pipeline-id PIPELINE_ID` | no | Pipeline ID. Defaults from local `pipeline.py`. |
| `--run-id RUN_ID` | no | Run ID to export. |
| `--run-label RUN_LABEL` | no | Run label to export. |
| `--node-name NODE_NAME` | no | Export artifact produced by node. |
| `--artifact-name ARTIFACT_NAME` | no | Export physical artifact table, for example `main.policy_core`. |
| `--format` | no | Output format: `csv`, `parquet`, or `json`. Defaults to `csv`. |
| `--output-path OUTPUT_PATH` | no | Explicit output file path. |
| `--overwrite` | no | Replace output file if it exists. |
| `--json` | no | Emit JSON output. |

Use either `--node-name` or `--artifact-name`.

### Graph UI

```bash
queron open_graph <pipeline> [--host HOST] [--port PORT] [--no-browser] [--json]
```

| Argument/Flag | Required | Description |
|---|---:|---|
| `pipeline` | yes | Pipeline Python file. |
| `--host HOST` | no | Host interface to bind. |
| `--port PORT` | no | Port to bind. |
| `--no-browser` | no | Start server without opening browser. |
| `--json` | no | Emit server metadata as JSON. |
