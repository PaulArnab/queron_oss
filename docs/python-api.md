# Python API Reference

Import:

```python
import queron
```

The public API is exported from `queron/__init__.py` and implemented in `queron/api.py`.

## Project Setup

### `init_pipeline_project(project_path, *, sample=False, force=False)`

Creates a pipeline project scaffold.

Arguments:

| Argument | Type | Default | Description |
|---|---|---|---|
| `project_path` | `str or Path` | required | Directory to create or update. |
| `sample` | `bool` | `False` | Write runnable sample pipeline. |
| `force` | `bool` | `False` | Overwrite scaffold files. |

Returns `InitPipelineProjectResult`:

- `project_path`
- `written_files`
- `created_directories`
- `sample`

## Compile

### `compile_pipeline(pipeline_path, *, config_path=None, target=None)`

Compiles a pipeline file and persists the active contract.

Returns `CompiledPipeline`:

- `spec`
- `diagnostics`
- `module_globals`
- `contract`

### `compile_pipeline_text(code, *, yaml_text=None, target=None)`

Compiles pipeline source code from a string. Useful for tests and generated pipelines.

### `has_compile_errors(compiled)`

Returns `True` if diagnostics contain at least one error.

## Run

### `run_pipeline(...)`

```python
result = queron.run_pipeline(
    "pipeline.py",
    config_path="configurations.yaml",
    connections_path="connections.yaml",
    runtime_bindings=None,
    runtime_vars={"start_date": "2026-01-01"},
    target="dev",
    target_node=None,
    clean_existing=False,
    set_final=False,
    pipeline_id=None,
    run_label="nightly",
    on_log=None,
)
```

Arguments:

| Argument | Type | Default | Description |
|---|---|---|---|
| `pipeline_path` | `str or Path` | required | Pipeline Python file. |
| `config_path` | `str, Path, or None` | `None` | Path to `configurations.yaml`. Defaults to local file when present. |
| `connections_path` | `str, Path, or None` | `None` | Path to `connections.yaml`. Defaults to env/local file when present. |
| `runtime_bindings` | `dict[str, Any] or None` | `None` | In-memory connection bindings keyed by config name. |
| `runtime_vars` | `dict[str, Any] or None` | `None` | Runtime variable values. |
| `target` | `str or None` | `None` | Target environment override. |
| `target_node` | `str or None` | `None` | Run only this node and upstream dependencies. |
| `clean_existing` | `bool` | `False` | Drop existing output tables before execution. |
| `set_final` | `bool` | `False` | Finalize stale failed/running run before starting. |
| `pipeline_id` | `str or None` | `None` | Override pipeline ID. |
| `run_label` | `str or None` | `None` | Unique run label. |
| `on_log` | `Callable[[PipelineLogEvent], None] or None` | `None` | Log callback. |

Returns `RunPipelineResult`:

- `compiled`
- `executed_nodes`
- `artifact_path`
- `run_id`
- `run_label`
- `status`
- `log_path`

### `resume_pipeline(...)`

Resumes the latest failed run.

Arguments:

| Argument | Type | Default | Description |
|---|---|---|---|
| `pipeline_path` | `str or Path` | required | Pipeline file. |
| `config_path` | `str, Path, or None` | `None` | Configuration path. |
| `connections_path` | `str, Path, or None` | `None` | Connections path. |
| `runtime_bindings` | `dict[str, Any] or None` | `None` | In-memory bindings. |
| `runtime_vars` | `dict[str, Any] or None` | `None` | Runtime vars for resume. |
| `target` | `str or None` | `None` | Target override. |
| `on_log` | callable | `None` | Log callback. |

Returns `RunPipelineResult`.

## Stop

### `stop_pipeline(pipeline_path, *, run_id=None, reason=None)`

Requests graceful stop for a running pipeline.

Returns `StopPipelineResult`:

- `artifact_path`
- `run_id`
- `run_label`
- `stop_requested`
- `stop_mode`
- `request_path`
- `message`

### `force_stop_pipeline(pipeline_path, *, run_id=None, reason=None)`

Requests force stop.

## Reset

Each reset returns `ResetPipelineResult`:

- `compiled`
- `artifact_path`
- `reset_nodes`
- `reset_tables`

### `reset_node(pipeline_path, *, node_name, config_path=None, target=None, on_log=None)`

Drops the output table for one node and clears its state.

### `reset_downstream(pipeline_path, *, node_name, config_path=None, target=None, on_log=None)`

Drops one node and all downstream dependent outputs.

### `reset_upstream(pipeline_path, *, node_name, config_path=None, target=None, on_log=None)`

Drops one node and all upstream dependency outputs.

### `reset_all(pipeline_path, *, config_path=None, target=None, on_log=None)`

Drops all pipeline output tables.

## Inspect

### `inspect_dag(artifact_path, *, run_id=None, run_label=None)`

Returns `InspectDagResult`:

- pipeline and artifact identifiers
- compile/run identifiers
- run status/finality
- runtime var contracts
- nodes
- edges
- dependency edges

### `inspect_node(artifact_path, node_name, *, run_id=None, run_label=None, upstream=False, downstream=False)`

Returns `InspectNodeResult`.

Set `upstream=True` to inspect dependencies. Set `downstream=True` to inspect dependents.

### `inspect_node_history(artifact_path, node_name, *, run_id=None, run_label=None)`

Returns node run metadata and state timeline.

### `inspect_node_logs(artifact_path, node_name, *, run_id=None, run_label=None, tail=None)`

Returns persisted logs for one node.

### `inspect_node_query(artifact_path, node_name, *, run_id=None, run_label=None)`

Returns original SQL, resolved SQL, dependencies, and artifact metadata.

## Export

### `export_artifact(...)`

```python
result = queron.export_artifact(
    artifact_path,
    node_name="customer_summary",
    format="csv",
    output_path="exports/customer_summary.csv",
    overwrite=True,
)
```

Arguments:

| Argument | Type | Default | Description |
|---|---|---|---|
| `artifact_path` | `str or Path` | required | Artifact DuckDB path. |
| `node_name` | `str or None` | `None` | Node whose artifact should be exported. |
| `artifact_name` | `str or None` | `None` | Physical artifact table, for example `main.customer_summary`. |
| `run_id` | `str or None` | `None` | Run selector. |
| `run_label` | `str or None` | `None` | Run selector. |
| `format` | `str` | `csv` | `csv`, `parquet`, or `json`. |
| `output_path` | `str, Path, or None` | `None` | Explicit output path. |
| `overwrite` | `bool` | `False` | Replace output file. |

Returns `ExportArtifactResult`.

## Runtime Bindings

Use binding objects when you do not want to use `connections.yaml`.

```python
binding = queron.PostgresBinding(
    host="localhost",
    port=5432,
    database="retail_db",
    username="admin",
    password="password123",
)

queron.run_pipeline(
    "pipeline.py",
    runtime_bindings={"PostGres": binding},
)
```

Available binding classes:

- `PostgresBinding`
- `Db2Binding`
- `MssqlBinding`
- `MysqlBinding`
- `MariaDbBinding`
- `OracleBinding`

Each binding accepts `config_factory`. Values returned by the factory override constructor values.

## Log Callbacks

Use `on_log` to receive structured runtime logs:

```python
def print_log(event):
    print(event.timestamp, event.severity, event.code, event.message)

queron.run_pipeline("pipeline.py", on_log=print_log)
```

Graph log publisher:

```python
handler = queron.graph_log_publisher("http://127.0.0.1:8765")
queron.run_pipeline("pipeline.py", on_log=handler)
```

Fan out to multiple handlers:

```python
handler = queron.fanout_log_handlers(print_log, queron.graph_log_publisher(url))
```
