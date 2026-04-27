# CLI Reference

The package exposes:

```bash
queron
```

or:

```bash
python -m queron
```

## `queron init`

```bash
queron init [-h] [--sample] [--force] [--json] path
```

Creates a pipeline project scaffold.

| Argument/Flag | Description |
|---|---|
| `path` | Directory where scaffold should be created. |
| `--sample` | Create runnable sample pipeline. |
| `--force` | Overwrite scaffold files. |
| `--json` | Write JSON output. |

## `queron compile`

```bash
queron compile [-h] [--config CONFIG_PATH] [--target TARGET] [--json] pipeline
```

Compiles a pipeline and persists the execution contract.

| Argument/Flag | Description |
|---|---|
| `pipeline` | Pipeline Python file. |
| `--config CONFIG_PATH` | Path to `configurations.yaml`. Defaults to local file when present. |
| `--target TARGET` | Target environment override. |
| `--json` | Write JSON output. |

## `queron run`

```bash
queron run [-h] [--config CONFIG_PATH] [--target TARGET]
           [--connections CONNECTIONS_PATH] [--target-node TARGET_NODE]
           [--run-label RUN_LABEL] [--clean-existing] [--set-final]
           [--stream-logs] [--graph-url GRAPH_URL]
           [--vars-json VARS_JSON] [--vars-file VARS_FILE] [--json]
           pipeline
```

Runs a compiled pipeline.

| Argument/Flag | Description |
|---|---|
| `pipeline` | Pipeline Python file. |
| `--config CONFIG_PATH` | Path to `configurations.yaml`. |
| `--target TARGET` | Target environment override. |
| `--connections CONNECTIONS_PATH` | Path to `connections.yaml`. |
| `--target-node TARGET_NODE` | Execute selected node and dependencies. |
| `--run-label RUN_LABEL` | Unique label for this run. |
| `--clean-existing` | Drop existing output tables before execution. |
| `--set-final` | Finalize latest failed/stale running run before starting. |
| `--stream-logs` | Stream logs to console. |
| `--graph-url GRAPH_URL` | Publish live logs to graph server. |
| `--vars-json VARS_JSON` | Runtime vars as JSON object. |
| `--vars-file VARS_FILE` | Runtime vars JSON file. |
| `--json` | Write JSON output. |

## `queron resume`

```bash
queron resume [-h] [--config CONFIG_PATH] [--target TARGET]
              [--connections CONNECTIONS_PATH] [--stream-logs]
              [--graph-url GRAPH_URL] [--vars-json VARS_JSON]
              [--vars-file VARS_FILE] [--json]
              pipeline
```

Resumes the latest failed run.

## `queron stop`

```bash
queron stop [-h] [--run-id RUN_ID] [--reason REASON] [--json] pipeline
```

Requests graceful stop.

## `queron force-stop`

```bash
queron force-stop [-h] [--run-id RUN_ID] [--reason REASON] [--json] pipeline
```

Requests force stop.

## Reset Commands

```bash
queron reset-node pipeline.py node_name
queron reset-downstream pipeline.py node_name
queron reset-upstream pipeline.py node_name
queron reset-all pipeline.py
```

Each reset command accepts:

| Flag | Description |
|---|---|
| `--config CONFIG_PATH` | Path to `configurations.yaml`. |
| `--target TARGET` | Target environment override. |
| `--json` | Write JSON output. |

## `queron inspect_runs`

```bash
queron inspect_runs [-h] [--pipeline-id PIPELINE_ID] [--limit LIMIT]
```

Lists recorded runs.

## `queron inspect_logs`

```bash
queron inspect_logs [-h] [--pipeline-id PIPELINE_ID] [--run-id RUN_ID]
                    [--run-label RUN_LABEL] [--tail TAIL] [--verbose] [--json]
```

Shows persisted logs for a selected run.

## `queron inspect_dag`

```bash
queron inspect_dag [-h] [--pipeline-id PIPELINE_ID] [--run-id RUN_ID]
                   [--run-label RUN_LABEL]
```

Shows compiled DAG and current node states.

## `queron inspect_node`

```bash
queron inspect_node [-h] [--pipeline-id PIPELINE_ID] [--run-id RUN_ID]
                    [--run-label RUN_LABEL] [--upstream] [--downstream]
                    node_name
```

Shows one node or a selected upstream/downstream slice.

## `queron inspect_node_history`

```bash
queron inspect_node_history [-h] [--pipeline-id PIPELINE_ID]
                            [--run-id RUN_ID] [--run-label RUN_LABEL]
                            node_name
```

Shows state timeline for one node.

## `queron inspect_node_logs`

```bash
queron inspect_node_logs [-h] [--pipeline-id PIPELINE_ID]
                         [--run-id RUN_ID] [--run-label RUN_LABEL]
                         [--tail TAIL]
                         node_name
```

Shows node-specific log events.

## `queron inspect_node_query`

```bash
queron inspect_node_query [-h] [--pipeline-id PIPELINE_ID]
                          [--run-id RUN_ID] [--run-label RUN_LABEL]
                          node_name
```

Shows original and resolved SQL for one node.

## `queron export_artifact`

```bash
queron export_artifact [-h] [--pipeline-id PIPELINE_ID]
                       [--run-id RUN_ID] [--run-label RUN_LABEL]
                       [--node-name NODE_NAME]
                       [--artifact-name ARTIFACT_NAME]
                       [--format {csv,parquet,json}]
                       [--output-path OUTPUT_PATH] [--overwrite] [--json]
```

Exports one materialized artifact table.

At least one selector is needed:

- `--node-name`
- `--artifact-name`

## `queron open_graph`

```bash
queron open_graph [-h] [--host HOST] [--port PORT] [--no-browser] [--json] pipeline
```

Starts the local graph UI server.

| Flag | Description |
|---|---|
| `--host HOST` | Host interface. |
| `--port PORT` | Port to bind. |
| `--no-browser` | Do not open browser automatically. |
| `--json` | Print server metadata and exit. |

