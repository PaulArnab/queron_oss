from __future__ import annotations

import html
import json
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
OUT = DOCS / "html"

PAGES = [
    "README.md",
    "architecture.md",
    "internals-guide.md",
    "flowcharts.md",
    "user-guide.md",
    "commands-reference.md",
    "pipeline-dsl.md",
    "python-api.md",
    "cli-reference.md",
    "datatype-mapping.md",
    "examples.md",
]


def slug(text: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9\s-]", "", text).strip().lower()
    value = re.sub(r"\s+", "-", value)
    return value or "section"


def inline(text: str) -> str:
    escaped = html.escape(text)
    escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)

    def _link(match: re.Match[str]) -> str:
        label = match.group(1)
        href = match.group(2)
        if href == "README.md":
            href = "index.html"
        elif href.endswith(".md"):
            href = href[:-3] + ".html"
        return f'<a href="{href}">{label}</a>'

    escaped = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", _link, escaped)
    return escaped


def render_table(lines: list[str]) -> str:
    rows = []
    for line in lines:
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        rows.append(cells)
    header = rows[0]
    body = rows[2:] if len(rows) > 2 else []
    parts = ["<table>", "<thead><tr>"]
    parts.extend(f"<th>{inline(cell)}</th>" for cell in header)
    parts.append("</tr></thead>")
    if body:
        parts.append("<tbody>")
        for row in body:
            parts.append("<tr>")
            parts.extend(f"<td>{inline(cell)}</td>" for cell in row)
            parts.append("</tr>")
        parts.append("</tbody>")
    parts.append("</table>")
    return "\n".join(parts)


def render_interactive_pipeline(text: str) -> str:
    links = {
        "@queron.csv.ingress": "commands-reference.html#file-ingress",
        "@queron.jsonl.ingress": "commands-reference.html#file-ingress",
        "@queron.parquet.ingress": "commands-reference.html#file-ingress",
        "@queron.file.ingress": "commands-reference.html#file-ingress",
        "@queron.python.ingress": "commands-reference.html#python-ingress",
        "@queron.model.sql": "commands-reference.html#local-sql-model",
        "@queron.check.fail_if_count": "commands-reference.html#checks",
        "@queron.check.fail_if_true": "commands-reference.html#checks",
        "@queron.postgres.ingress": "commands-reference.html#database-ingress",
        "@queron.db2.ingress": "commands-reference.html#database-ingress",
        "@queron.mssql.ingress": "commands-reference.html#database-ingress",
        "@queron.mysql.ingress": "commands-reference.html#database-ingress",
        "@queron.mariadb.ingress": "commands-reference.html#database-ingress",
        "@queron.oracle.ingress": "commands-reference.html#database-ingress",
        "@queron.postgres.lookup": "commands-reference.html#database-lookup",
        "@queron.db2.lookup": "commands-reference.html#database-lookup",
        "@queron.mssql.lookup": "commands-reference.html#database-lookup",
        "@queron.mysql.lookup": "commands-reference.html#database-lookup",
        "@queron.mariadb.lookup": "commands-reference.html#database-lookup",
        "@queron.oracle.lookup": "commands-reference.html#database-lookup",
        "@queron.csv.egress": "commands-reference.html#file-egress",
        "@queron.jsonl.egress": "commands-reference.html#file-egress",
        "@queron.parquet.egress": "commands-reference.html#file-egress",
        "@queron.file.egress": "commands-reference.html#file-egress",
        "@queron.postgres.egress": "commands-reference.html#database-egress",
        "@queron.db2.egress": "commands-reference.html#database-egress",
        "@queron.mssql.egress": "commands-reference.html#database-egress",
        "@queron.mysql.egress": "commands-reference.html#database-egress",
        "@queron.mariadb.egress": "commands-reference.html#database-egress",
        "@queron.oracle.egress": "commands-reference.html#database-egress",
        "queron.ref": "commands-reference.html#sql-template-helpers",
        "queron.source": "commands-reference.html#sql-template-helpers",
        "queron.lookup": "commands-reference.html#sql-template-helpers",
        "queron.var": "commands-reference.html#sql-template-helpers",
        "queron.run_pipeline": "commands-reference.html#compile-and-run-api",
        "queron.PostgresBinding": "python-api.html#runtime-bindings",
        "queron.Db2Binding": "python-api.html#runtime-bindings",
        "queron.MssqlBinding": "python-api.html#runtime-bindings",
        "queron.MysqlBinding": "python-api.html#runtime-bindings",
        "queron.MariaDbBinding": "python-api.html#runtime-bindings",
        "queron.OracleBinding": "python-api.html#runtime-bindings",
    }

    def link_tokens(line: str) -> str:
        escaped = html.escape(line)
        for token in sorted(links, key=len, reverse=True):
            escaped_token = html.escape(token)
            href = links[token]
            escaped = escaped.replace(escaped_token, f'<a href="{href}">{escaped_token}</a>')
        return escaped or " "

    rows = []
    for number, line in enumerate(text.splitlines(), start=1):
        rows.append(
            '<div class="code-row">'
            f'<a class="line-no" id="pipeline-L{number}" href="#pipeline-L{number}">{number}</a>'
            f'<code>{link_tokens(line)}</code>'
            "</div>"
        )
    return (
        '<section class="interactive-code">'
        '<div class="code-toolbar">'
        '<strong>pipeline.py</strong>'
        '<span>Click linked decorators, helpers, and bindings for their reference.</span>'
        "</div>"
        '<div class="code-scroll">'
        + "\n".join(rows)
        + "</div></section>"
    )


def render_interactive_flowcharts() -> str:
    compiler_columns = [
        (
            "API entry",
            [
                "compile_pipeline(pipeline.py)",
                "Read pipeline.py text",
                "Resolve configurations.yaml text",
                "Preview compile_pipeline_code",
            ],
        ),
        (
            "Load source",
            [
                "Prepare a temporary Python module for the pipeline file",
                "Run the pipeline file so Python creates the decorated functions",
                "Each Queron decorator stores node settings on its function",
                "Walk through the loaded functions and find Queron nodes",
                "Convert each decorated function into an internal node record",
            ],
        ),
        (
            "Build spec",
            [
                "Read the configuration text into a structured config object",
                "Choose the active target from the argument or config default",
                "Group the pipeline id, target, nodes, and notebook metadata",
                "Start validating and filling in missing runtime details",
            ],
        ),
        (
            "Validate graph",
            [
                "Make sure the pipeline has a pipeline id",
                "Check duplicate node names",
                "Make sure artifact-producing nodes have unique output names",
                "Convert each output name into a safe DuckDB table name",
                "Find refs, sources, and lookups inside SQL templates",
                "Use refs and manual depends_on to build dependencies",
                "Replace template helpers with real table or relation names",
                "Check runtime variables are used only in safe SQL positions",
                "Validate node-specific settings like files, targets, and checks",
                "Reject dependency loops before anything can run",
            ],
        ),
        (
            "Artifact path",
            [
                "If validation found errors, stop and return diagnostics",
                "Use the pipeline id to choose the artifact.duckdb location",
                "Look at the latest recorded run in the artifact database",
                "Do not compile over a run that is still active",
                "Clean up stale or failed run state when it is safe",
                "Compile again now that the exact artifact path is known",
            ],
        ),
        (
            "Hash contract",
            [
                "Build a compile contract that describes this exact pipeline version",
                "Check project Python imports do not point outside the project",
                "Hash every project Python file from its file bytes",
                "Combine those file hashes into one project Python hash",
                "Hash whether config exists and the exact config text",
                "Normalize each node and hash its SQL, settings, outputs, and dependencies",
                "Sort dependency edges and hash the graph shape",
                "Collect runtime variable rules and reject conflicting usage",
                "Create one final contract hash from pipeline, nodes, edges, config, project files, and vars",
            ],
        ),
        (
            "Persist",
            [
                "Create the compiled contract record used by run and inspect",
                "Store hashes, graph edges, tracked files, variables, spec, and diagnostics",
                "Open artifact.duckdb and save the contract metadata",
                "Write the row into the Queron metadata compiled contracts table",
                "Mark older compile contracts as inactive",
                "Make this compile the active contract for future run and inspect calls",
            ],
        ),
    ]
    run_columns = [
        (
            "Run request",
            [
                "User calls run_pipeline with pipeline path and optional runtime inputs",
                "Queron emits early run and compile validation log events",
                "The pipeline file is compiled or validated before execution begins",
                "If compile validation fails, no nodes run and the result is compile_failed",
            ],
        ),
        (
            "Run safety",
            [
                "Resolve the artifact.duckdb path for this pipeline",
                "Check the latest run stored in the artifact database",
                "Refuse to start if another run is still active",
                "If allowed, finalize or archive stale failed and orphaned running runs",
                "Check that a requested run label is not already used",
            ],
        ),
        (
            "Build runtime",
            [
                "Create a PipelineRuntime for this run id",
                "Attach the compiled pipeline spec and module globals",
                "Open the artifact database path and working directory",
                "Validate runtime variables against the compiled contract",
                "Attach runtime database bindings and connections path",
                "Persist early log events into the runtime log stream",
            ],
        ),
        (
            "Prepare outputs",
            [
                "If clean_existing is enabled, preserve incomplete outputs first",
                "Drop selected existing local artifact tables before running",
                "If target_node is set, select that node and all required upstream nodes",
                "Register this runtime as active so stop and force-stop can find it",
            ],
        ),
        (
            "Choose order",
            [
                "Build a topological order from the compiled dependency graph",
                "Start with nodes that have no unmet dependencies",
                "Run check nodes before other ready nodes when possible",
                "Skip nodes outside the selected run scope",
                "A node only runs after all selected dependencies are complete",
            ],
        ),
        (
            "Start run",
            [
                "Write a pipeline run row with status running",
                "Start the stop request watcher",
                "Create one node run row for each selected node",
                "Mark selected nodes as ready in node state history",
            ],
        ),
        (
            "Execute nodes",
            [
                "Before each node, check whether a stop was requested",
                "Mark the node as running and record its start time",
                "Dispatch to the runtime handler for that node kind",
                "Ingress nodes load data into local DuckDB artifact tables",
                "Model SQL nodes read refs and write a new local artifact table",
                "Check nodes run validation queries and may fail the run",
                "Egress nodes read artifacts and write files or database targets",
                "Lookup nodes prepare database lookup tables for later SQL",
            ],
        ),
        (
            "Record outcome",
            [
                "Normalize the node execution result",
                "Record row counts, artifact name, warnings, details, and finish time",
                "If warnings exist, mark the node complete_with_warnings",
                "If a node fails, mark it failed and skip downstream work when policy says so",
                "After each node, check again for stop requests at a safe boundary",
            ],
        ),
        (
            "Finish run",
            [
                "When all selected nodes finish, mark the run success or success_with_warnings",
                "Archive completed local artifact tables for the run",
                "Clean up temporary lookup tables when retention does not keep them",
                "If execution failed, mark the run failed and preserve error details",
                "Unregister the active runtime and return executed nodes, run id, status, and log path",
            ],
        ),
    ]

    run_function_flows = [
        (
            "compile_pipeline",
            "Compile pipeline.py and write the active contract.",
            [
                (
                    "Artifact use",
                    [
                        "Reads pipeline.py and configurations.yaml",
                        "Creates or updates artifact.duckdb for the pipeline",
                        "Writes one active row into _queron_meta.compiled_contracts",
                        "Marks older compiled contracts inactive",
                    ],
                ),
                (
                    "What the user gets",
                    [
                        "A compiled pipeline object with diagnostics",
                        "The artifact database path",
                        "The compile id and hashes used by later runs",
                        "No pipeline nodes execute during compile",
                    ],
                ),
            ],
        ),
        (
            "run_pipeline",
            "Start a new run and execute nodes.",
            [
                (
                    "What happens",
                    [
                        "Validates or recompiles the pipeline before execution",
                        "Creates a new run id and a running row in artifact.duckdb",
                        "Creates node run rows for the selected nodes",
                        "Executes nodes in dependency order",
                        "Returns executed nodes, artifact path, run id, status, and log path",
                    ],
                ),
                (
                    "Artifacts and nodes",
                    [
                        "Each artifact-producing node writes to its compiled local table",
                        "A target_node run executes that node plus required upstream nodes",
                        "clean_existing drops selected existing artifact tables before execution",
                        "Successful node rows store artifact name, row counts, warnings, and details",
                        "Finished runs archive completed local artifact tables by run id",
                    ],
                ),
            ],
        ),
        (
            "resume_pipeline",
            "Continue work from the latest incomplete run.",
            [
                (
                    "What happens",
                    [
                        "Finds the latest failed or incomplete run in artifact.duckdb",
                        "Validates the current compile contract before resuming",
                        "Reuses stored runtime variables unless new values are provided",
                        "Builds a runtime for the existing run context",
                        "Runs only work that is still safe and necessary",
                    ],
                ),
                (
                    "Artifacts and nodes",
                    [
                        "Completed artifact tables are kept instead of rebuilt",
                        "Failed or skipped node states guide what needs to run again",
                        "Previously completed nodes can stay complete when their artifacts exist",
                        "New node outcomes are written back to the same artifact database",
                        "The resumed run finishes with updated run and node status",
                    ],
                ),
            ],
        ),
        (
            "stop_pipeline",
            "Ask a running pipeline to stop cleanly.",
            [
                (
                    "What happens",
                    [
                        "Finds the selected running run in artifact.duckdb",
                        "Writes a graceful stop request file for that run",
                        "The active runtime checks for the request at safe boundaries",
                        "The current node is allowed to reach a safe stopping point",
                        "Returns the request path, artifact path, run id, and stop mode",
                    ],
                ),
                (
                    "Artifacts and nodes",
                    [
                        "Completed node artifacts remain in the artifact database",
                        "Nodes not yet started can be marked skipped or left ready depending on policy",
                        "The run status is updated when the runtime handles the stop",
                        "Inspect can still read completed node outputs and logs",
                    ],
                ),
            ],
        ),
        (
            "force_stop_pipeline",
            "Try to interrupt active work faster than graceful stop.",
            [
                (
                    "What happens",
                    [
                        "Writes a force stop request for the selected running run",
                        "The runtime tries registered interrupt callbacks for supported work",
                        "Some connector or Python work may only stop at fallback boundaries",
                        "The active node is converted into a stopped or failed outcome",
                        "Returns the same stop result shape as graceful stop",
                    ],
                ),
                (
                    "Artifacts and nodes",
                    [
                        "Artifacts from fully completed nodes remain usable",
                        "The interrupted node may not produce a complete artifact",
                        "Downstream nodes are not allowed to continue from incomplete inputs",
                        "Node logs record whether interruption was attempted and where it happened",
                    ],
                ),
            ],
        ),
        (
            "reset_node",
            "Clear one node output so it can be rebuilt.",
            [
                (
                    "What happens",
                    [
                        "Validates the current compiled pipeline",
                        "Selects exactly the requested node",
                        "Drops that node's local artifact table when it has one",
                        "Clears that node's run state and marks it ready again",
                        "Returns the reset node and dropped table names",
                    ],
                ),
                (
                    "Artifacts and nodes",
                    [
                        "Only the selected node artifact is removed",
                        "Upstream artifacts are left untouched",
                        "Downstream artifacts may still exist, but can now be stale",
                        "Use this when one node needs to be rerun without clearing the graph",
                    ],
                ),
            ],
        ),
        (
            "reset_downstream",
            "Clear a node and everything that depends on it.",
            [
                (
                    "What happens",
                    [
                        "Validates the current compiled pipeline",
                        "Finds the requested node and all downstream children",
                        "Drops local artifact tables for the selected downstream set",
                        "Resets those node states back to ready",
                        "Returns all reset nodes and tables",
                    ],
                ),
                (
                    "Artifacts and nodes",
                    [
                        "The selected node output is removed",
                        "Every artifact that consumed it directly or indirectly is removed",
                        "Upstream dependency artifacts are preserved",
                        "Use this after changing or invalidating one producer node",
                    ],
                ),
            ],
        ),
        (
            "reset_upstream",
            "Clear a node and the inputs feeding it.",
            [
                (
                    "What happens",
                    [
                        "Validates the current compiled pipeline",
                        "Finds the requested node and all upstream dependencies",
                        "Drops local artifact tables for that upstream set",
                        "Marks those node states ready for rebuild",
                        "Returns all reset nodes and tables",
                    ],
                ),
                (
                    "Artifacts and nodes",
                    [
                        "Input artifacts feeding the node are removed",
                        "The selected node artifact is also removed when it exists",
                        "Unrelated downstream branches are not selected by this reset",
                        "Use this when source-side inputs need to be rebuilt",
                    ],
                ),
            ],
        ),
        (
            "reset_all",
            "Clear every compiled local output.",
            [
                (
                    "What happens",
                    [
                        "Validates the current compiled pipeline",
                        "Selects every node in the compiled graph",
                        "Drops every local artifact table owned by the pipeline",
                        "Marks all selected node states ready again",
                        "Returns the full reset node and table list",
                    ],
                ),
                (
                    "Artifacts and nodes",
                    [
                        "All local artifacts are removed from artifact.duckdb",
                        "The compile contract and metadata database remain in place",
                        "The next run rebuilds the pipeline from the beginning",
                        "Use this when the local artifact state should be fully refreshed",
                    ],
                ),
            ],
        ),
    ]
    artifact_function_flows = [
        (
            "inspect_dag",
            "Show the full compiled graph and latest run state.",
            [
                (
                    "Artifact use",
                    [
                        "Reads _queron_meta.compiled_contracts for nodes and edges",
                        "Reads _queron_meta.pipeline_runs for selected run metadata",
                        "Reads _queron_meta.node_runs and node_states for statuses",
                        "Reads lineage and column metadata when available",
                    ],
                ),
                (
                    "What the user gets",
                    [
                        "Pipeline id, compile id, run id, and run status",
                        "All nodes with kind, dependencies, dependents, and output names",
                        "Runtime variable contract for the active compile",
                        "Graph UI data for node state and dependency meaning",
                    ],
                ),
            ],
        ),
        (
            "inspect_runs",
            "List recorded runs for a pipeline artifact database.",
            [
                (
                    "Artifact use",
                    [
                        "Resolves artifact.duckdb from the local pipeline or --pipeline-id",
                        "Reads _queron_meta.pipeline_runs",
                        "Uses _queron_meta.compiled_contracts to show the active pipeline context",
                        "Does not read node artifact table data",
                    ],
                ),
                (
                    "What the user gets",
                    [
                        "Run ids and optional run labels",
                        "Run status, final flag, start time, and finish time",
                        "A quick way to find which run_id to inspect or export",
                        "CLI-only helper; there is no public Python function exported for this yet",
                    ],
                ),
            ],
        ),
        (
            "inspect_logs",
            "Show persisted pipeline logs for a selected run.",
            [
                (
                    "Artifact use",
                    [
                        "Resolves the selected run from _queron_meta.pipeline_runs",
                        "Reads that run's persisted log_path",
                        "Loads the run JSONL log file",
                        "Can tail the latest events from the log file",
                    ],
                ),
                (
                    "What the user gets",
                    [
                        "Pipeline-level log events for a run",
                        "Run id, label, final flag, and log file path",
                        "A broader log view than inspect_node_logs",
                        "CLI-only helper; there is no public Python function exported for this yet",
                    ],
                ),
            ],
        ),
        (
            "inspect_node",
            "Inspect one node, or include its upstream/downstream neighborhood.",
            [
                (
                    "Artifact use",
                    [
                        "Reads the compiled node payload from compiled_contracts",
                        "Reads the selected run row and node run row",
                        "Resolves current or archived artifact table for that node",
                        "Loads column mappings for the selected node artifact",
                    ],
                ),
                (
                    "What the user gets",
                    [
                        "Node kind, config, output, artifact name, and artifact path",
                        "Current state, run status, row counts, warnings, and details",
                        "Dependencies, manual dependencies, automatic dependencies, and dependents",
                        "Runtime vars and column mapping information",
                    ],
                ),
            ],
        ),
        (
            "inspect_node_query",
            "Show the SQL Queron stored for a node.",
            [
                (
                    "Artifact use",
                    [
                        "Reads the compiled node payload from compiled_contracts",
                        "Resolves the selected run context",
                        "Resolves the node's effective artifact path and artifact table",
                        "Does not run the SQL again",
                    ],
                ),
                (
                    "What the user gets",
                    [
                        "Original SQL text when the node has SQL",
                        "Resolved SQL after refs, sources, lookups, and vars were processed",
                        "Dependencies used by that node",
                        "Current or archived artifact location tied to the selected run",
                    ],
                ),
            ],
        ),
        (
            "inspect_node_history",
            "Show the state timeline for a node in a run.",
            [
                (
                    "Artifact use",
                    [
                        "Reads the selected run from _queron_meta.pipeline_runs",
                        "Reads the node run row from _queron_meta.node_runs",
                        "Reads all state rows for that node from _queron_meta.node_states",
                        "Resolves the node's current or archived artifact table",
                    ],
                ),
                (
                    "What the user gets",
                    [
                        "Node run status, start time, finish time, and error message",
                        "Ordered state history such as ready, running, complete, failed, or skipped",
                        "Artifact names for the current and archived run outputs",
                        "Useful data for explaining why a node is currently in a state",
                    ],
                ),
            ],
        ),
        (
            "inspect_node_logs",
            "Read persisted log events for one node.",
            [
                (
                    "Artifact use",
                    [
                        "Uses artifact.duckdb to find the selected run",
                        "Reads the run's persisted log_path from pipeline run metadata",
                        "Loads the JSONL log file for that run",
                        "Filters log events down to the requested node",
                    ],
                ),
                (
                    "What the user gets",
                    [
                        "Node-specific runtime log events",
                        "Optional tail limit for the latest events",
                        "Run id, run label, run status, and archived artifact path",
                        "Useful detail when a node failed or emitted warnings",
                    ],
                ),
            ],
        ),
        (
            "export_artifact",
            "Export a materialized artifact table to a file.",
            [
                (
                    "Artifact use",
                    [
                        "Selects an artifact by node_name or artifact_name",
                        "Resolves the current or archived artifact database for the selected run",
                        "Reads the artifact table with DuckDB",
                        "Writes CSV, Parquet, or JSON to output_path",
                    ],
                ),
                (
                    "What the user gets",
                    [
                        "Output file path and export format",
                        "Row count and file size",
                        "Pipeline id, compile id, run id, and artifact name",
                        "A portable file copy of a Queron artifact table",
                    ],
                ),
            ],
        ),
    ]
    artifact_tables = [
        {
            "name": "_queron_meta.compiled_contracts",
            "summary": "The active compiled pipeline contract.",
            "description": "Stores the full compiled view of pipeline.py. This is not the raw Python file. It is Queron's saved understanding of the pipeline after decorators were read, SQL refs were resolved, dependencies were built, runtime vars were checked, and hashes were calculated.",
            "used_for": "Run, resume, reset, inspect, and graph UI start here. This row tells Queron which compile is active, where the artifact database lives, which nodes exist, how nodes depend on each other, what files affected the hash, what runtime variables are allowed, and whether compile produced diagnostics.",
            "sample": {
                "compile_id": "cmp_20260429_001",
                "pipeline_id": "customer_360_demo",
                "pipeline_path": "C:/project/pipeline.py",
                "project_root": "C:/project",
                "artifact_path": "C:/project/.queron/customer_360_demo/artifact.duckdb",
                "config_path": "C:/project/configurations.yaml",
                "target": "dev",
                "compiled_at": "2026-04-29T13:55:42Z",
                "is_active": True,
                "contract_hash": "sha256:abc123...",
                "edge_hash": "sha256:def456...",
                "config_hash": "sha256:cfg789...",
                "project_python_hash": "sha256:789abc...",
                "node_hashes_json": [
                    {
                        "node_name": "policy_ingress",
                        "node_kind": "postgres.ingress",
                        "hash": "sha256:node_ingress...",
                    },
                    {
                        "node_name": "policy_core",
                        "node_kind": "model.sql",
                        "hash": "sha256:node_model...",
                    },
                ],
                "edges_json": [["policy_ingress", "policy_core"], ["policy_core", "customer_summary"]],
                "tracked_files_json": [
                    {"path": "pipeline.py", "kind": "pipeline", "hash": "sha256:file001..."},
                    {"path": "helpers/policy_rules.py", "kind": "python", "hash": "sha256:file002..."},
                    {"path": "configurations.yaml", "kind": "config", "hash": "sha256:file003..."},
                ],
                "external_dependencies_json": [
                    {"node_name": "policy_ingress", "connection": "warehouse_dev", "kind": "postgres"}
                ],
                "vars_json": [
                    {
                        "name": "as_of_date",
                        "kind": "scalar",
                        "required": True,
                        "default": None,
                        "log_value": True,
                        "mutable_after_start": False,
                        "used_in_nodes": ["policy_core"],
                    }
                ],
                "spec_json": {
                    "node_count": 3,
                    "nodes": ["policy_ingress", "policy_core", "customer_summary"],
                    "outputs": ["policy_raw", "policy_core", "customer_summary"],
                },
                "diagnostics_json": [],
            },
        },
        {
            "name": "_queron_meta.pipeline_runs",
            "summary": "One row per pipeline run.",
            "description": "Tracks each time a pipeline starts, finishes, fails, stops, or resumes.",
            "used_for": "Inspect commands and graph UI use it to show run status, labels, runtime vars, timing, and archived artifact location.",
            "sample": {
                "run_id": "run_001",
                "run_label": "daily_load",
                "compile_id": "cmp_20260429_001",
                "pipeline_id": "customer_360_demo",
                "artifact_path": ".queron/customer_360_demo/artifact.duckdb",
                "archived_artifact_path": ".queron/customer_360_demo/run_001/artifact.duckdb",
                "started_at": "2026-04-29T14:01:00Z",
                "finished_at": "2026-04-29T14:03:15Z",
                "status": "success",
                "runtime_vars_json": {"as_of_date": "2026-04-29"},
                "is_final": True,
            },
        },
        {
            "name": "_queron_meta.node_runs",
            "summary": "One row per node in a run.",
            "description": "Records what happened to each node during a specific run.",
            "used_for": "Inspect node history, graph status, resume, reset, and failure diagnosis read this table.",
            "sample": {
                "node_run_id": "run_001:policy_core",
                "run_id": "run_001",
                "node_name": "policy_core",
                "node_kind": "model.sql",
                "artifact_name": "main.policy_core",
                "status": "complete",
                "row_count_in": 1500,
                "row_count_out": 1488,
                "artifact_size_bytes": 262144,
                "warnings_json": [],
                "details_json": {"query_seconds": 1.42},
                "active_node_state_id": "state_002",
            },
        },
        {
            "name": "_queron_meta.node_states",
            "summary": "State timeline for nodes.",
            "description": "Keeps the state changes for each node, such as ready, running, complete, failed, skipped, or cleared.",
            "used_for": "Graph UI and inspect use it to show the current node state and how that state changed over time.",
            "sample": {
                "node_state_id": "state_002",
                "run_id": "run_001",
                "node_run_id": "run_001:policy_core",
                "node_name": "policy_core",
                "state": "complete",
                "is_active": True,
                "created_at": "2026-04-29T14:02:20Z",
                "trigger": "node_completed",
                "details_json": {"artifact_name": "main.policy_core", "node_kind": "model.sql"},
            },
        },
        {
            "name": "_queron_meta.column_mapping",
            "summary": "Column type mapping metadata.",
            "description": "Stores how source columns were converted into DuckDB artifact columns.",
            "used_for": "Inspect node column views use it to explain source type, DuckDB type, mapping warnings, and lossy conversions.",
            "sample": {
                "target_schema": "main",
                "target_table": "policy_core",
                "node_name": "policy_ingress",
                "node_kind": "postgres.ingress",
                "ordinal_position": 1,
                "source_column": "premium_amount",
                "source_type": "NUMERIC(12,2)",
                "target_column": "premium_amount",
                "target_type": "DECIMAL(12,2)",
                "connector_type": "postgres",
                "mapping_mode": "ingress",
                "warnings_json": [],
                "lossy": False,
            },
        },
        {
            "name": "_queron_meta.table_lineage",
            "summary": "Where artifact tables came from.",
            "description": "Links a child artifact table to its source, lookup, or upstream artifact parents.",
            "used_for": "Graph and inspect use it to explain dependency meaning, not just dependency order.",
            "sample": {
                "child_schema": "main",
                "child_table": "customer_summary",
                "parent_kind": "artifact",
                "parent_name": "policy_core",
                "parent_schema": "main",
                "parent_table": "policy_core",
                "connector_type": "duckdb",
                "via_node": "customer_summary",
            },
        },
        {
            "name": "main.<out>",
            "summary": "Current local artifact tables.",
            "description": "These are the actual data tables produced by artifact-producing nodes. The name comes from the node's out value after Queron sanitizes it.",
            "used_for": "Later nodes read these tables through queron.ref(...), exports read from them, and inspect can preview them.",
            "sample": {
                "table_name": "main.policy_core",
                "produced_by_node": "policy_core",
                "logical_out": "policy_core",
                "sample_row": {
                    "policy_id": "P-1001",
                    "customer_id": "C-501",
                    "premium_amount": 128.50,
                    "status": "active",
                },
            },
        },
        {
            "name": "run_<run_id>.<out>",
            "summary": "Archived artifact tables for a completed run.",
            "description": "When a run finishes, Queron can copy completed local artifacts into a run-specific archive database and schema.",
            "used_for": "Inspect can read the exact artifact data for a past run instead of only the current active tables.",
            "sample": {
                "archived_artifact_path": ".queron/customer_360_demo/run_001/artifact.duckdb",
                "archived_artifact_name": "run_run001.policy_core",
                "original_artifact_name": "main.policy_core",
                "run_id": "run_001",
                "sample_row": {
                    "policy_id": "P-1001",
                    "customer_id": "C-501",
                    "premium_amount": 128.50,
                    "status": "active",
                },
            },
        },
    ]

    def _render_flow(columns: list[tuple[str, list[str]]]) -> str:
        column_html = []
        for index, (title, steps) in enumerate(columns):
            step_html = "".join(f"<li>{html.escape(step)}</li>" for step in steps)
            arrow = '<div class="compiler-flow-arrow" aria-hidden="true">-></div>' if index else ""
            column_html.append(
                arrow
                + '<section class="compiler-flow-column">'
                + f"<h3>{html.escape(title)}</h3>"
                + f"<ol>{step_html}</ol>"
                + "</section>"
            )
        return "".join(column_html)

    function_call_labels = {
        "compile_pipeline": ("Python: queron.compile_pipeline(...)", "CLI: queron compile ..."),
        "run_pipeline": ("Python: queron.run_pipeline(...)", "CLI: queron run ..."),
        "resume_pipeline": ("Python: queron.resume_pipeline(...)", "CLI: queron resume ..."),
        "stop_pipeline": ("Python: queron.stop_pipeline(...)", "CLI: queron stop ..."),
        "force_stop_pipeline": ("Python: queron.force_stop_pipeline(...)", "CLI: queron force-stop ..."),
        "reset_node": ("Python: queron.reset_node(...)", "CLI: queron reset-node ..."),
        "reset_downstream": ("Python: queron.reset_downstream(...)", "CLI: queron reset-downstream ..."),
        "reset_upstream": ("Python: queron.reset_upstream(...)", "CLI: queron reset-upstream ..."),
        "reset_all": ("Python: queron.reset_all(...)", "CLI: queron reset-all ..."),
        "inspect_dag": ("Python: queron.inspect_dag(...)", "CLI: queron inspect_dag ..."),
        "inspect_runs": ("Python: not exported", "CLI: queron inspect_runs ..."),
        "inspect_logs": ("Python: not exported", "CLI: queron inspect_logs ..."),
        "inspect_node": ("Python: queron.inspect_node(...)", "CLI: queron inspect_node ..."),
        "inspect_node_query": ("Python: queron.inspect_node_query(...)", "CLI: queron inspect_node_query ..."),
        "inspect_node_history": ("Python: queron.inspect_node_history(...)", "CLI: queron inspect_node_history ..."),
        "inspect_node_logs": ("Python: queron.inspect_node_logs(...)", "CLI: queron inspect_node_logs ..."),
        "export_artifact": ("Python: queron.export_artifact(...)", "CLI: queron export_artifact ..."),
    }

    def _with_call_details(function_name: str, columns: list[tuple[str, list[str]]]) -> list[tuple[str, list[str]]]:
        labels = function_call_labels.get(function_name)
        if not labels:
            return columns
        return [("How to call", list(labels)), *columns]

    compiler_flow_html = _render_flow(compiler_columns)
    run_flow_html = _render_flow(run_columns)

    def _modal(*, modal_id: str, title: str, summary: str, flow_html: str) -> str:
        return f"""
  <button class="flowchart-open" type="button" data-flowchart-open="{html.escape(modal_id, quote=True)}">
    <strong>{html.escape(title)}</strong>
    <span>{html.escape(summary)}</span>
  </button>
  <div class="flowchart-modal" id="{html.escape(modal_id, quote=True)}" hidden role="dialog" aria-modal="true" aria-label="{html.escape(title, quote=True)}">
    <div class="flowchart-modal-panel">
      <div class="flowchart-modal-bar">
        <strong>{html.escape(title)}</strong>
        <button class="flowchart-close" type="button" data-flowchart-close aria-label="Close">x</button>
      </div>
      <div class="compiler-flowchart">{flow_html}</div>
    </div>
  </div>
"""

    def _artifact_modal(*, modal_id: str, table: dict[str, Any]) -> str:
        sample_json = html.escape(json.dumps(table["sample"], indent=2, ensure_ascii=True))
        title = str(table["name"])
        summary = str(table["summary"])
        detail_html = f"""
      <div class="artifact-table-detail">
        <section class="artifact-table-explanation">
          <h3>What it is</h3>
          <p>{html.escape(str(table["description"]))}</p>
          <h3>What it is used for</h3>
          <p>{html.escape(str(table["used_for"]))}</p>
        </section>
        <section>
          <h3>Sample record</h3>
          <pre><code>{sample_json}</code></pre>
        </section>
      </div>"""
        return f"""
  <button class="flowchart-open" type="button" data-flowchart-open="{html.escape(modal_id, quote=True)}">
    <strong>{html.escape(title)}</strong>
    <span>{html.escape(summary)}</span>
  </button>
  <div class="flowchart-modal" id="{html.escape(modal_id, quote=True)}" hidden role="dialog" aria-modal="true" aria-label="{html.escape(title, quote=True)}">
    <div class="flowchart-modal-panel artifact-modal-panel">
      <div class="flowchart-modal-bar">
        <strong>{html.escape(title)}</strong>
        <button class="flowchart-close" type="button" data-flowchart-close aria-label="Close">x</button>
      </div>
{detail_html}
    </div>
  </div>
"""

    modals = [
        _modal(
            modal_id="compiler-flowchart-modal",
            title="Compiler Flowchart",
            summary="Open the full compiler, validation, hashing, and contract persistence flow.",
            flow_html=compiler_flow_html,
        ),
        _modal(
            modal_id="run-flowchart-modal",
            title="Run Flowchart",
            summary="Open the runtime execution, artifact, node state, and run status flow.",
            flow_html=run_flow_html,
        ),
    ]
    run_function_modals = [
        _modal(
            modal_id=f"run-function-{index}",
            title=title,
            summary=summary,
            flow_html=_render_flow(_with_call_details(title, columns)),
        )
        for index, (title, summary, columns) in enumerate(run_function_flows, start=1)
    ]
    artifact_table_modals = [
        _artifact_modal(modal_id=f"artifact-table-{index}", table=table)
        for index, table in enumerate(artifact_tables, start=1)
    ]
    artifact_function_outputs = {
        "inspect_dag": {
            "description": "Reads the compiled contract and selected run metadata to return the whole pipeline graph with node status.",
            "used_for": "Use it when you want to understand the full DAG, current run state, runtime var contract, and graph UI data.",
            "sample": {
                "pipeline_id": "customer_360_demo",
                "compile_id": "cmp_20260429_001",
                "run_id": "run_001",
                "run_status": "success",
                "is_final": True,
                "runtime_vars_contract": [{"name": "as_of_date", "required": True}],
                "nodes": [
                    {
                        "name": "policy_core",
                        "kind": "model.sql",
                        "status": "complete",
                        "artifact_name": "main.policy_core",
                        "dependencies": ["policy_ingress"],
                    }
                ],
                "edges": [["policy_ingress", "policy_core"]],
            },
        },
        "inspect_runs": {
            "description": "Lists pipeline runs recorded in the artifact database.",
            "used_for": "Use it to find run_id, run_label, run status, and whether a run is final before inspecting or exporting.",
            "sample": {
                "artifact_path": ".queron/customer_360_demo/artifact.duckdb",
                "active_compile_id": "cmp_20260429_001",
                "runs": [
                    {
                        "run_id": "run_001",
                        "run_label": "daily_load",
                        "status": "success",
                        "is_final": True,
                        "started_at": "2026-04-29T14:01:00Z",
                        "finished_at": "2026-04-29T14:03:15Z",
                    }
                ],
            },
        },
        "inspect_logs": {
            "description": "Reads persisted pipeline-level log events for one selected run.",
            "used_for": "Use it to debug the run timeline, warnings, stop requests, and high-level runtime events.",
            "sample": {
                "run_id": "run_001",
                "run_label": "daily_load",
                "is_final": True,
                "log_path": ".queron/customer_360_demo/logs/run_001.jsonl",
                "logs": [
                    {
                        "level": "info",
                        "event": "node_completed",
                        "node_name": "policy_core",
                        "message": "Node completed.",
                    }
                ],
            },
        },
        "inspect_node": {
            "description": "Returns details for one node, or a selected upstream/downstream neighborhood around it.",
            "used_for": "Use it to see node kind, dependencies, artifact location, current state, row counts, warnings, and column mappings.",
            "sample": {
                "requested_node": "policy_core",
                "selection": "node",
                "run_id": "run_001",
                "run_status": "success",
                "nodes": [
                    {
                        "name": "policy_core",
                        "kind": "model.sql",
                        "current_state": "complete",
                        "node_run_status": "complete",
                        "logical_artifact": "policy_core",
                        "artifact_name": "main.policy_core",
                        "row_count_out": 1488,
                        "dependencies": ["policy_ingress"],
                        "dependents": ["customer_summary"],
                        "column_mappings": [],
                    }
                ],
            },
        },
        "inspect_node_query": {
            "description": "Returns the SQL Queron stored for a node.",
            "used_for": "Use it to compare original SQL with resolved SQL after refs, sources, lookups, and runtime vars are processed.",
            "sample": {
                "node_name": "policy_core",
                "node_kind": "model.sql",
                "logical_artifact": "policy_core",
                "artifact_name": "main.policy_core",
                "sql": "select * from {{ queron.ref(\"policy_ingress\") }}",
                "resolved_sql": "select * from main.policy_ingress",
                "dependencies": ["policy_ingress"],
            },
        },
        "inspect_node_history": {
            "description": "Returns the state timeline for one node in a selected run.",
            "used_for": "Use it to see how a node moved through ready, running, complete, failed, skipped, or cleared states.",
            "sample": {
                "node_name": "policy_core",
                "node_kind": "model.sql",
                "node_run_status": "complete",
                "artifact_name": "main.policy_core",
                "states": [
                    {"state": "ready", "trigger": "run_started"},
                    {"state": "running", "trigger": "node_started"},
                    {"state": "complete", "trigger": "node_completed"},
                ],
            },
        },
        "inspect_node_logs": {
            "description": "Returns persisted log events for one node in a selected run.",
            "used_for": "Use it when one node failed, warned, or behaved unexpectedly and you want only that node's log events.",
            "sample": {
                "node_name": "policy_core",
                "run_id": "run_001",
                "logs": [
                    {
                        "level": "info",
                        "event": "node_started",
                        "message": "Executing node policy_core.",
                    },
                    {
                        "level": "info",
                        "event": "node_completed",
                        "message": "Node completed.",
                    },
                ],
            },
        },
        "export_artifact": {
            "description": "Reads a materialized artifact table and writes it to a portable file.",
            "used_for": "Use it to export a node output or named artifact as CSV, Parquet, or JSON for sharing, debugging, or downstream use.",
            "sample": {
                "pipeline_id": "customer_360_demo",
                "run_id": "run_001",
                "node_name": "policy_core",
                "artifact_name": "main.policy_core",
                "effective_artifact_path": ".queron/customer_360_demo/run_001/artifact.duckdb",
                "output_path": ".queron/exports/policy_core.csv",
                "export_format": "csv",
                "row_count": 1488,
                "file_size_bytes": 87342,
            },
        },
    }

    def _artifact_function_modal(*, modal_id: str, title: str, summary: str, columns: list[tuple[str, list[str]]]) -> str:
        detail = artifact_function_outputs[title]
        sample_json = html.escape(json.dumps(detail["sample"], indent=2, ensure_ascii=True))
        call_labels = function_call_labels.get(title, ())
        call_html = "".join(f"<li>{html.escape(label)}</li>" for label in call_labels)
        return f"""
  <button class="flowchart-open" type="button" data-flowchart-open="{html.escape(modal_id, quote=True)}">
    <strong>{html.escape(title)}</strong>
    <span>{html.escape(summary)}</span>
  </button>
  <div class="flowchart-modal" id="{html.escape(modal_id, quote=True)}" hidden role="dialog" aria-modal="true" aria-label="{html.escape(title, quote=True)}">
    <div class="flowchart-modal-panel artifact-modal-panel">
      <div class="flowchart-modal-bar">
        <strong>{html.escape(title)}</strong>
        <button class="flowchart-close" type="button" data-flowchart-close aria-label="Close">x</button>
      </div>
      <div class="artifact-table-detail">
        <section class="artifact-table-explanation">
          <h3>How to call</h3>
          <ul>{call_html}</ul>
          <h3>What it is</h3>
          <p>{html.escape(str(detail["description"]))}</p>
          <h3>What it is used for</h3>
          <p>{html.escape(str(detail["used_for"]))}</p>
        </section>
        <section>
          <h3>Sample output</h3>
          <pre><code>{sample_json}</code></pre>
        </section>
      </div>
    </div>
  </div>
"""
    artifact_function_modals = [
        _artifact_function_modal(
            modal_id=f"artifact-function-{index}",
            title=title,
            summary=summary,
            columns=columns,
        )
        for index, (title, summary, columns) in enumerate(artifact_function_flows, start=1)
    ]
    return f"""
<section class="flowchart-page">
  <div class="flowchart-section-head">
    <span>Core flows</span>
    <p>Start with the two big lifecycle diagrams.</p>
  </div>
  <div class="flowchart-actions">
    {"".join(modals)}
  </div>
  <div class="flowchart-section-head flowchart-section-head-spaced">
    <span>Run Functions</span>
    <p>Each function below explains what happens to artifacts and node state.</p>
  </div>
  <div class="flowchart-actions flowchart-function-actions">
    {"".join(run_function_modals)}
  </div>
  <div class="flowchart-section-head flowchart-section-head-spaced">
    <span>Artifact Structure</span>
    <p>Click a Queron-owned table or table pattern to see what it stores.</p>
  </div>
  <div class="flowchart-actions flowchart-function-actions">
    {"".join(artifact_table_modals)}
  </div>
  <div class="flowchart-section-head flowchart-section-head-spaced">
    <span>User-Facing Artifact Functions</span>
    <p>These functions read, write, inspect, or export data from artifact.duckdb and related run files.</p>
  </div>
  <div class="flowchart-actions flowchart-function-actions">
    {"".join(artifact_function_modals)}
  </div>
  <script>
(() => {{
  function openModal(modal) {{
    modal.hidden = false;
    modal.scrollTop = 0;
    const panel = modal.querySelector(".flowchart-modal-panel");
    if (panel) panel.scrollTop = 0;
    modal.querySelectorAll("pre").forEach((block) => {{
      block.scrollTop = 0;
      block.scrollLeft = 0;
    }});
    document.body.classList.add("flowchart-modal-open");
  }}
  function closeModal(modal) {{
    modal.hidden = true;
    document.body.classList.remove("flowchart-modal-open");
  }}
  document.querySelectorAll("[data-flowchart-open]").forEach((openButton) => {{
    openButton.addEventListener("click", () => {{
      const modal = document.getElementById(openButton.dataset.flowchartOpen);
      if (modal) openModal(modal);
    }});
  }});
  document.querySelectorAll(".flowchart-modal").forEach((modal) => {{
    modal.addEventListener("click", (event) => {{
      if (event.target === modal || event.target.closest("[data-flowchart-close]")) closeModal(modal);
    }});
  }});
  document.addEventListener("keydown", (event) => {{
    if (event.key !== "Escape") return;
    document.querySelectorAll(".flowchart-modal").forEach((modal) => {{
      if (!modal.hidden) closeModal(modal);
    }});
  }});
}})();
  </script>
</section>
"""


def render_markdown(text: str) -> tuple[str, str]:
    lines = text.splitlines()
    parts: list[str] = []
    title = "Queron Documentation"
    i = 0
    in_ul = False
    in_ol = False

    def close_lists() -> None:
        nonlocal in_ul, in_ol
        if in_ul:
            parts.append("</ul>")
            in_ul = False
        if in_ol:
            parts.append("</ol>")
            in_ol = False

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if stripped.startswith("```"):
            close_lists()
            lang = stripped[3:].strip()
            code_lines: list[str] = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                code_lines.append(lines[i])
                i += 1
            code_text = html.escape("\n".join(code_lines))
            if lang == "mermaid":
                parts.append(f'<pre class="mermaid">{code_text}</pre>')
            else:
                lang_class = f" language-{html.escape(lang)}" if lang else ""
                parts.append(f'<pre><code class="{lang_class}">{code_text}</code></pre>')
            i += 1
            continue

        if stripped == ":::pipeline-example":
            close_lists()
            code_lines: list[str] = []
            i += 1
            while i < len(lines) and lines[i].strip() != ":::end-pipeline-example":
                code_lines.append(lines[i])
                i += 1
            parts.append(render_interactive_pipeline("\n".join(code_lines)))
            if i < len(lines):
                i += 1
            continue

        if stripped == ":::interactive-flowcharts":
            close_lists()
            parts.append(render_interactive_flowcharts())
            i += 1
            while i < len(lines) and lines[i].strip() != ":::end-interactive-flowcharts":
                i += 1
            if i < len(lines):
                i += 1
            continue

        if stripped == "{{PIPELINE_EXAMPLE}}":
            close_lists()
            parts.append("<p><strong>Pipeline example source is missing.</strong></p>")
            i += 1
            continue

        if stripped.startswith("|") and stripped.endswith("|"):
            close_lists()
            table_lines = [line]
            i += 1
            while i < len(lines) and lines[i].strip().startswith("|") and lines[i].strip().endswith("|"):
                table_lines.append(lines[i])
                i += 1
            parts.append(render_table(table_lines))
            continue

        heading = re.match(r"^(#{1,6})\s+(.+)$", stripped)
        if heading:
            close_lists()
            level = len(heading.group(1))
            text_value = heading.group(2).strip()
            if level == 1:
                title = text_value
            parts.append(f'<h{level} id="{slug(text_value)}">{inline(text_value)}</h{level}>')
            i += 1
            continue

        if stripped.startswith("- "):
            if in_ol:
                parts.append("</ol>")
                in_ol = False
            if not in_ul:
                parts.append("<ul>")
                in_ul = True
            parts.append(f"<li>{inline(stripped[2:].strip())}</li>")
            i += 1
            continue

        ordered = re.match(r"^\d+\.\s+(.+)$", stripped)
        if ordered:
            if in_ul:
                parts.append("</ul>")
                in_ul = False
            if not in_ol:
                parts.append("<ol>")
                in_ol = True
            parts.append(f"<li>{inline(ordered.group(1).strip())}</li>")
            i += 1
            continue

        if not stripped:
            close_lists()
            i += 1
            continue

        close_lists()
        parts.append(f"<p>{inline(stripped)}</p>")
        i += 1

    close_lists()
    return title, "\n".join(parts)


def page_template(title: str, body: str, nav: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)} - Queron Docs</title>
  <link rel="stylesheet" href="styles.css?v=3">
  <script type="module">
    import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
    mermaid.initialize({{ startOnLoad: true, securityLevel: 'loose' }});
  </script>
</head>
<body>
  <aside>
    <div class="brand">Queron Docs</div>
    {nav}
  </aside>
  <main>
    {body}
  </main>
</body>
</html>
"""


def build() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    nav_items = []
    for name in PAGES:
        target = "index.html" if name == "README.md" else Path(name).with_suffix(".html").name
        label = "Home" if name == "README.md" else Path(name).stem.replace("-", " ").title()
        nav_items.append(f'<a href="{target}">{html.escape(label)}</a>')
    nav = "<nav>" + "\n".join(nav_items) + "</nav>"

    css = """
:root { color-scheme: light; --fg: #172026; --muted: #5d6b73; --line: #d9e1e6; --bg: #f7f9fb; --accent: #0f766e; }
* { box-sizing: border-box; }
body { margin: 0; font: 15px/1.55 system-ui, -apple-system, Segoe UI, sans-serif; color: var(--fg); background: white; }
aside { position: fixed; inset: 0 auto 0 0; width: 260px; overflow: auto; padding: 24px 18px; background: var(--bg); border-right: 1px solid var(--line); }
main { max-width: 1060px; margin-left: 260px; padding: 36px 56px 80px; }
.brand { font-weight: 700; font-size: 18px; margin-bottom: 18px; }
nav a { display: block; color: var(--fg); text-decoration: none; padding: 7px 8px; border-radius: 6px; }
nav a:hover { background: #eaf1f4; color: var(--accent); }
h1, h2, h3, h4 { line-height: 1.2; margin: 28px 0 12px; }
h1 { font-size: 34px; margin-top: 0; }
h2 { border-top: 1px solid var(--line); padding-top: 24px; }
p { margin: 10px 0; }
code { background: #eef3f5; padding: 2px 5px; border-radius: 4px; font-size: 0.92em; }
pre { overflow: auto; background: #fff !important; color: var(--fg) !important; padding: 16px; border-radius: 8px; border: 1px solid var(--line); }
pre code, pre code[class*="language-"] { background: transparent !important; color: inherit !important; padding: 0; }
table { width: 100%; border-collapse: collapse; margin: 16px 0 24px; font-size: 14px; }
th, td { border: 1px solid var(--line); padding: 8px 10px; vertical-align: top; }
th { background: var(--bg); text-align: left; }
ul, ol { padding-left: 24px; }
.mermaid { background: #fff; color: var(--fg); border: 1px solid var(--line); }
.interactive-code { border: 1px solid var(--line); border-radius: 8px; overflow: hidden; margin: 18px 0 28px; background: #fff; }
.code-toolbar { display: flex; gap: 18px; align-items: center; justify-content: space-between; padding: 12px 14px; color: var(--fg); border-bottom: 1px solid var(--line); background: var(--bg); }
.code-toolbar span { color: var(--muted); font-size: 13px; }
.code-scroll { max-height: 720px; overflow: auto; padding: 10px 0; }
.code-row { display: grid; grid-template-columns: 58px 1fr; min-width: 980px; }
.code-row:hover { background: #f3f7f9; }
.code-row code { display: block; white-space: pre; background: transparent; color: var(--fg); padding: 0 16px 0 8px; border-radius: 0; font-family: ui-monospace, SFMono-Regular, Consolas, monospace; font-size: 13px; line-height: 1.55; }
.code-row code a { color: #0f766e; text-decoration: none; border-bottom: 1px solid rgba(15, 118, 110, .35); }
.code-row code a:hover { color: #0b5f58; border-bottom-color: #0b5f58; }
.line-no { color: #8a99a3; text-align: right; padding-right: 12px; text-decoration: none; font: 13px/1.55 ui-monospace, SFMono-Regular, Consolas, monospace; user-select: none; }
.line-no:hover { color: var(--fg); }
.flowchart-page { margin-top: 18px; }
.flowchart-section-head { margin: 28px 0 12px; }
.flowchart-section-head span { display: block; color: var(--fg); font-weight: 700; font-size: 18px; }
.flowchart-section-head p { margin: 6px 0 0; color: var(--muted); font-size: 14px; }
.flowchart-actions { display: flex; flex-wrap: wrap; gap: 14px; align-items: stretch; }
.flowchart-function-actions .flowchart-open { width: calc((100% - 28px) / 3); min-width: 220px; }
.flowchart-open { width: min(520px, 100%); text-align: left; border: 1px solid var(--line); border-radius: 8px; background: #fff; color: var(--fg); padding: 16px; cursor: pointer; box-shadow: 0 1px 2px rgba(20, 31, 37, .04); }
.flowchart-open:hover, .flowchart-open:focus { border-color: var(--accent); outline: none; box-shadow: 0 0 0 3px rgba(15, 118, 110, .12); }
.flowchart-open strong { display: block; margin-bottom: 6px; font-size: 16px; }
.flowchart-open span { display: block; color: var(--muted); font-size: 14px; line-height: 1.4; }
.flowchart-modal[hidden] { display: none; }
.flowchart-modal { position: fixed; inset: 0; z-index: 50; background: rgba(23, 32, 38, .55); padding: 28px; overflow: auto; }
.flowchart-modal-panel { width: min(1120px, 100%); min-height: calc(100vh - 56px); margin: 0 auto; background: #fff; border-radius: 8px; border: 1px solid var(--line); box-shadow: 0 18px 48px rgba(20, 31, 37, .22); }
.flowchart-modal-bar { display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 12px 16px; background: var(--bg); border-bottom: 1px solid var(--line); border-radius: 8px 8px 0 0; }
.flowchart-close { width: 30px; height: 30px; border: 1px solid var(--line); border-radius: 6px; background: #fff; color: var(--fg); cursor: pointer; }
.compiler-flowchart { display: flex; align-items: stretch; gap: 14px; min-height: calc(100vh - 110px); padding: 20px; overflow: auto; }
.compiler-flow-column { flex: 0 0 260px; border: 1px solid var(--line); border-radius: 8px; background: #fff; box-shadow: 0 1px 2px rgba(20, 31, 37, .04); }
.compiler-flow-column h3 { margin: 0; padding: 12px 14px; font-size: 15px; border-bottom: 1px solid var(--line); background: var(--bg); border-radius: 8px 8px 0 0; }
.compiler-flow-column ol { margin: 0; padding: 12px 14px 14px 32px; }
.compiler-flow-column li { margin: 0 0 9px; color: var(--fg); font-size: 13px; line-height: 1.35; }
.compiler-flow-arrow { flex: 0 0 auto; align-self: center; color: var(--muted); font: 700 20px/1 ui-monospace, SFMono-Regular, Consolas, monospace; }
.artifact-modal-panel { max-width: 980px; }
.artifact-table-detail { display: grid; grid-template-columns: 1fr; gap: 14px; padding: 20px; }
.artifact-table-detail section { border: 1px solid var(--line); border-radius: 8px; background: #fff; padding: 14px; }
.artifact-table-detail h3 { margin: 0 0 10px; font-size: 15px; }
.artifact-table-detail p { color: var(--muted); }
.artifact-table-detail pre { margin: 0; max-height: none; max-width: 100%; overflow: auto; }
.flowchart-modal-open { overflow: hidden; }
@media (max-width: 860px) { aside { position: static; width: auto; border-right: 0; border-bottom: 1px solid var(--line); } main { margin-left: 0; padding: 24px; } }
"""
    (OUT / "styles.css").write_text(css.strip() + "\n", encoding="utf-8")

    for name in PAGES:
        source = DOCS / name
        if not source.exists():
            continue
        title, body = render_markdown(source.read_text(encoding="utf-8"))
        target = OUT / ("index.html" if name == "README.md" else source.with_suffix(".html").name)
        target.write_text(page_template(title, body, nav), encoding="utf-8")


if __name__ == "__main__":
    build()
