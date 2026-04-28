from __future__ import annotations

import argparse
import json
import os
import sys
import webbrowser
from pathlib import Path
from typing import Any

from .api import (
    _inspect_pipeline_logs,
    _inspect_pipeline_runs,
    init_pipeline_project,
    compile_pipeline,
    export_artifact,
    inspect_node,
    inspect_node_query,
    inspect_node_history,
    inspect_node_logs,
    inspect_dag,
    has_compile_errors,
    list_existing_outputs_for_file,
    reset_all,
    reset_downstream,
    reset_upstream,
    reset_node,
    resume_pipeline,
    run_pipeline,
    stop_pipeline,
    force_stop_pipeline,
)
from .runtime_models import format_log_event
from .graph_client import fanout_log_handlers, graph_log_publisher
from . import (
    _clear_pipeline_registry,
    _clear_runtime_configs_registry,
    _get_pipeline_metadata,
    _get_runtime_configs_provider,
)


def _load_runtime_vars(
    *,
    vars_json: str | None,
    vars_file: str | None,
) -> dict[str, Any] | None:
    raw_json = str(vars_json or "").strip()
    raw_file = str(vars_file or "").strip()
    if raw_json and raw_file:
        raise RuntimeError("Use either --vars-json or --vars-file, not both.")
    if raw_json:
        try:
            parsed = json.loads(raw_json)
        except Exception as exc:
            raise RuntimeError(f"--vars-json is not valid JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise RuntimeError("--vars-json must decode to an object.")
        return {str(key): value for key, value in parsed.items()}
    if raw_file:
        resolved = Path(raw_file).expanduser().resolve()
        if not resolved.exists() or not resolved.is_file():
            raise RuntimeError(f"Runtime vars file '{resolved}' was not found.")
        try:
            parsed = json.loads(resolved.read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError(f"Runtime vars file '{resolved}' is not valid JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise RuntimeError(f"Runtime vars file '{resolved}' must contain a JSON object.")
        return {str(key): value for key, value in parsed.items()}
    return None


def _graph_log_handler(graph_url: str | None):
    resolved_url = str(graph_url or os.environ.get("QUERON_GRAPH_URL") or "").strip()
    if not resolved_url:
        return None
    return graph_log_publisher(resolved_url)


def _load_pipeline_namespace(pipeline_path: str | Path) -> dict[str, Any]:
    namespace: dict[str, Any] = {"__name__": "__queron_cli_pipeline__"}
    resolved = Path(pipeline_path).expanduser().resolve()
    code = compile(resolved.read_text(encoding="utf-8"), str(resolved), "exec")
    original_sys_path = list(sys.path)
    _clear_pipeline_registry()
    _clear_runtime_configs_registry()
    try:
        pipeline_dir = str(resolved.parent)
        if pipeline_dir not in sys.path:
            sys.path.insert(0, pipeline_dir)
        exec(code, namespace, namespace)
    finally:
        sys.path[:] = original_sys_path
    return namespace


def _pipeline_id_from_file(pipeline_path: str | Path) -> str:
    resolved = Path(pipeline_path).expanduser().resolve()
    _load_pipeline_namespace(pipeline_path)
    pipeline_id = str(_get_pipeline_metadata().get("pipeline_id") or "").strip()
    if not pipeline_id:
        raise RuntimeError(f'Pipeline "{resolved}" is missing queron.pipeline(pipeline_id="...").')
    return pipeline_id


def _runtime_bindings_from_file(pipeline_path: str | Path) -> dict[str, Any] | None:
    _load_pipeline_namespace(pipeline_path)
    provider = _get_runtime_configs_provider()
    if provider is None:
        return None
    provider_name = getattr(provider, "__name__", "<runtime_configs>")
    try:
        bindings = provider()
    except Exception as exc:
        raise RuntimeError(f"Runtime configs provider '{provider_name}' failed: {exc}") from exc
    if not isinstance(bindings, dict):
        raise RuntimeError(f"Runtime configs provider '{provider_name}' must return a dict.")
    return bindings


def _resolve_cli_cwd(cwd: str | Path | None = None) -> Path:
    return Path(cwd).expanduser().resolve() if cwd is not None else Path.cwd().resolve()


def _find_local_pipeline_file(*, cwd: str | Path | None = None, filename: str = "pipeline.py") -> Path:
    resolved_cwd = _resolve_cli_cwd(cwd)
    pipeline_path = (resolved_cwd / filename).resolve()
    if not pipeline_path.exists() or not pipeline_path.is_file():
        raise RuntimeError(
            f"No local pipeline file was found at '{pipeline_path}'. Run this command from a pipeline folder or pass explicit selectors."
        )
    return pipeline_path


def _resolve_local_pipeline_id(*, cwd: str | Path | None = None, filename: str = "pipeline.py") -> str:
    return _pipeline_id_from_file(_find_local_pipeline_file(cwd=cwd, filename=filename))


def _resolve_artifact_path_from_pipeline_id(
    pipeline_id: str,
    *,
    cwd: str | Path | None = None,
) -> Path:
    normalized_pipeline_id = str(pipeline_id or "").strip()
    if not normalized_pipeline_id:
        raise RuntimeError("pipeline_id is required.")
    resolved_cwd = _resolve_cli_cwd(cwd)
    artifact_path = (resolved_cwd / ".queron" / normalized_pipeline_id / "artifact.duckdb").resolve()
    if not artifact_path.exists() or not artifact_path.is_file():
        raise RuntimeError(
            f"Artifact database '{artifact_path}' was not found. Compile or run the local pipeline first, or check --pipeline-id."
        )
    return artifact_path


def _resolve_latest_run_id_for_pipeline(artifact_path: str | Path) -> str | None:
    _resolved_artifact_path, _active_contract, runs = _inspect_pipeline_runs(artifact_path, limit=1)
    if not runs:
        return None
    latest_run_id = str(runs[0].get("run_id") or "").strip()
    return latest_run_id or None


def _resolve_cli_pipeline_and_run(
    *,
    pipeline_id: str | None = None,
    run_id: str | None = None,
    cwd: str | Path | None = None,
    filename: str = "pipeline.py",
) -> dict[str, Any]:
    explicit_pipeline_id = str(pipeline_id or "").strip() or None
    explicit_run_id = str(run_id or "").strip() or None
    local_pipeline_path: Path | None = None
    if explicit_pipeline_id is None:
        local_pipeline_path = _find_local_pipeline_file(cwd=cwd, filename=filename)
        resolved_pipeline_id = _pipeline_id_from_file(local_pipeline_path)
        artifact_root = local_pipeline_path.parent
    else:
        if not explicit_run_id:
            raise RuntimeError("--run-id is required when --pipeline-id is passed explicitly.")
        resolved_pipeline_id = explicit_pipeline_id
        artifact_root = _resolve_cli_cwd(cwd)
    artifact_path = _resolve_artifact_path_from_pipeline_id(resolved_pipeline_id, cwd=artifact_root)
    selected_run_id = explicit_run_id or _resolve_latest_run_id_for_pipeline(artifact_path)
    return {
        "pipeline_path": str(local_pipeline_path) if local_pipeline_path is not None else None,
        "pipeline_id": resolved_pipeline_id,
        "artifact_path": str(artifact_path),
        "run_id": selected_run_id,
    }


def _resolve_cli_pipeline_artifact(
    *,
    pipeline_id: str | None = None,
    cwd: str | Path | None = None,
    filename: str = "pipeline.py",
) -> dict[str, Any]:
    explicit_pipeline_id = str(pipeline_id or "").strip() or None
    local_pipeline_path: Path | None = None
    if explicit_pipeline_id is None:
        local_pipeline_path = _find_local_pipeline_file(cwd=cwd, filename=filename)
        resolved_pipeline_id = _pipeline_id_from_file(local_pipeline_path)
        artifact_root = local_pipeline_path.parent
    else:
        resolved_pipeline_id = explicit_pipeline_id
        artifact_root = _resolve_cli_cwd(cwd)
    artifact_path = _resolve_artifact_path_from_pipeline_id(resolved_pipeline_id, cwd=artifact_root)
    return {
        "pipeline_path": str(local_pipeline_path) if local_pipeline_path is not None else None,
        "pipeline_id": resolved_pipeline_id,
        "artifact_path": str(artifact_path),
    }


def _resolve_cli_run_selector(args: argparse.Namespace) -> dict[str, Any]:
    run_label = str(getattr(args, "run_label", None) or "").strip() or None
    resolved = _resolve_cli_pipeline_and_run(
        pipeline_id=getattr(args, "pipeline_id", None),
        run_id=getattr(args, "run_id", None),
    )
    selected_run_id = getattr(args, "run_id", None) or (None if run_label else resolved["run_id"])
    return {
        **resolved,
        "selected_run_id": selected_run_id,
        "selected_run_label": run_label,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="queron", description="Compile and run Queron OSS pipelines.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Create a new Queron pipeline project scaffold.")
    init_parser.add_argument("path", help="Directory where the pipeline project scaffold should be created.")
    init_parser.add_argument("--sample", action="store_true", help="Create a runnable sample pipeline scaffold.")
    init_parser.add_argument("--force", action="store_true", help="Overwrite scaffold files when the directory exists.")
    init_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    init_parser.set_defaults(handler=_handle_init)

    compile_parser = subparsers.add_parser("compile", help="Compile a Queron pipeline and persist its execution contract.")
    _add_common_compile_flags(compile_parser)
    compile_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    compile_parser.set_defaults(handler=_handle_compile)

    run_parser = subparsers.add_parser("run", help="Validate the active compile contract and execute a Queron pipeline.")
    _add_common_compile_flags(run_parser)
    run_parser.add_argument(
        "--connections",
        dest="connections_path",
        default=None,
        help="Path to connections.yaml. Defaults to ./connections.yaml when present.",
    )
    run_parser.add_argument(
        "--target-node",
        dest="target_node",
        default=None,
        help="Execute only the selected target node and its dependencies.",
    )
    run_parser.add_argument(
        "--run-label",
        dest="run_label",
        default=None,
        help="Optional unique label for this pipeline run within the artifact database.",
    )
    run_parser.add_argument(
        "--clean-existing",
        action="store_true",
        help="Drop existing pipeline output tables before execution.",
    )
    run_parser.add_argument(
        "--set-final",
        action="store_true",
        dest="set_final",
        help="Finalize the latest failed or stale running run before starting a new run.",
    )
    run_parser.add_argument(
        "--stream-logs",
        action="store_true",
        dest="stream_logs",
        help="Stream pipeline logs to the console while the run executes.",
    )
    run_parser.add_argument(
        "--graph-url",
        dest="graph_url",
        default=None,
        help="Graph server URL that should receive live runtime logs. Defaults to QUERON_GRAPH_URL when set.",
    )
    run_parser.add_argument(
        "--vars-json",
        dest="vars_json",
        default=None,
        help="Runtime vars as a JSON object string.",
    )
    run_parser.add_argument(
        "--vars-file",
        dest="vars_file",
        default=None,
        help="Path to a JSON file containing runtime vars.",
    )
    run_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    run_parser.set_defaults(handler=_handle_run)

    resume_parser = subparsers.add_parser(
        "resume",
        help="Validate the active compile contract and resume the latest failed run.",
    )
    _add_common_compile_flags(resume_parser)
    resume_parser.add_argument(
        "--connections",
        dest="connections_path",
        default=None,
        help="Path to connections.yaml. Defaults to ./connections.yaml when present.",
    )
    resume_parser.add_argument(
        "--stream-logs",
        action="store_true",
        dest="stream_logs",
        help="Stream pipeline logs to the console while the resume executes.",
    )
    resume_parser.add_argument(
        "--graph-url",
        dest="graph_url",
        default=None,
        help="Graph server URL that should receive live runtime logs. Defaults to QUERON_GRAPH_URL when set.",
    )
    resume_parser.add_argument(
        "--vars-json",
        dest="vars_json",
        default=None,
        help="Runtime vars as a JSON object string.",
    )
    resume_parser.add_argument(
        "--vars-file",
        dest="vars_file",
        default=None,
        help="Path to a JSON file containing runtime vars.",
    )
    resume_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    resume_parser.set_defaults(handler=_handle_resume)

    stop_parser = subparsers.add_parser(
        "stop",
        help="Request stop for a running Queron pipeline.",
    )
    stop_parser.add_argument("pipeline", help="Path to the compiled OSS pipeline Python file.")
    stop_parser.add_argument(
        "--run-id",
        dest="run_id",
        default=None,
        help="Running pipeline run ID to stop. Defaults to the latest running run.",
    )
    stop_parser.add_argument(
        "--reason",
        dest="reason",
        default=None,
        help="Optional reason to record with the stop request.",
    )
    stop_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    stop_parser.set_defaults(handler=_handle_stop)

    force_stop_parser = subparsers.add_parser(
        "force-stop",
        help="Request force stop for a running Queron pipeline.",
    )
    force_stop_parser.add_argument("pipeline", help="Path to the compiled OSS pipeline Python file.")
    force_stop_parser.add_argument(
        "--run-id",
        dest="run_id",
        default=None,
        help="Running pipeline run ID to stop. Defaults to the latest running run.",
    )
    force_stop_parser.add_argument(
        "--reason",
        dest="reason",
        default=None,
        help="Optional reason to record with the force-stop request.",
    )
    force_stop_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    force_stop_parser.set_defaults(handler=_handle_force_stop)

    reset_node_parser = subparsers.add_parser("reset-node", help="Drop the output table for one pipeline node.")
    _add_common_compile_flags(reset_node_parser)
    reset_node_parser.add_argument("node_name", help="Pipeline node name to reset.")
    reset_node_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    reset_node_parser.set_defaults(handler=_handle_reset_node)

    reset_downstream_parser = subparsers.add_parser(
        "reset-downstream",
        help="Drop the output tables for a node and all downstream dependents.",
    )
    _add_common_compile_flags(reset_downstream_parser)
    reset_downstream_parser.add_argument("node_name", help="Pipeline node name to reset downstream from.")
    reset_downstream_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    reset_downstream_parser.set_defaults(handler=_handle_reset_downstream)

    reset_upstream_parser = subparsers.add_parser(
        "reset-upstream",
        help="Drop the output tables for a node and all upstream dependencies.",
    )
    _add_common_compile_flags(reset_upstream_parser)
    reset_upstream_parser.add_argument("node_name", help="Pipeline node name to reset upstream from.")
    reset_upstream_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    reset_upstream_parser.set_defaults(handler=_handle_reset_upstream)

    reset_all_parser = subparsers.add_parser("reset-all", help="Drop all pipeline output tables.")
    _add_common_compile_flags(reset_all_parser)
    reset_all_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    reset_all_parser.set_defaults(handler=_handle_reset_all)

    inspect_runs_parser = subparsers.add_parser(
        "inspect_runs",
        help="List recorded runs for one pipeline artifact database.",
    )
    _add_pipeline_selector_flags(inspect_runs_parser, include_run_id=False, include_run_label=False, action_label="inspect")
    inspect_runs_parser.add_argument(
        "--limit",
        dest="limit",
        type=int,
        default=None,
        help="Maximum number of runs to display. Defaults to all runs.",
    )
    inspect_runs_parser.set_defaults(handler=_handle_inspect_runs)

    inspect_logs_parser = subparsers.add_parser(
        "inspect_logs",
        help="Show persisted logs for one pipeline run.",
    )
    _add_pipeline_selector_flags(inspect_logs_parser, include_run_id=True, include_run_label=True, action_label="inspect")
    inspect_logs_parser.add_argument(
        "--tail",
        dest="tail",
        type=int,
        default=None,
        help="Show only the last N log lines.",
    )
    inspect_logs_parser.add_argument(
        "--verbose",
        action="store_true",
        dest="verbose",
        help="Show extra log fields and details.",
    )
    inspect_logs_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    inspect_logs_parser.set_defaults(handler=_handle_inspect_logs)

    inspect_dag_parser = subparsers.add_parser(
        "inspect_dag",
        help="Show the compiled DAG and the current node state view for one pipeline run.",
    )
    _add_pipeline_selector_flags(inspect_dag_parser, include_run_id=True, include_run_label=True, action_label="inspect")
    inspect_dag_parser.set_defaults(handler=_handle_inspect_dag)

    inspect_node_parser = subparsers.add_parser(
        "inspect_node",
        help="Show one pipeline node or its upstream/downstream slice for a selected run.",
    )
    inspect_node_parser.add_argument("node_name", help="Pipeline node name to inspect.")
    _add_pipeline_selector_flags(inspect_node_parser, include_run_id=True, include_run_label=True, action_label="inspect")
    inspect_node_parser.add_argument(
        "--upstream",
        action="store_true",
        dest="upstream",
        help="Inspect the requested node and all upstream dependencies.",
    )
    inspect_node_parser.add_argument(
        "--downstream",
        action="store_true",
        dest="downstream",
        help="Inspect the requested node and all downstream dependents.",
    )
    inspect_node_parser.set_defaults(handler=_handle_inspect_node)

    inspect_node_history_parser = subparsers.add_parser(
        "inspect_node_history",
        help="Show the state timeline for one pipeline node in a selected run.",
    )
    inspect_node_history_parser.add_argument("node_name", help="Pipeline node name to inspect.")
    _add_pipeline_selector_flags(inspect_node_history_parser, include_run_id=True, include_run_label=True, action_label="inspect")
    inspect_node_history_parser.set_defaults(handler=_handle_inspect_node_history)

    inspect_node_logs_parser = subparsers.add_parser(
        "inspect_node_logs",
        help="Show log events for one pipeline node in a selected run.",
    )
    inspect_node_logs_parser.add_argument("node_name", help="Pipeline node name to inspect.")
    _add_pipeline_selector_flags(inspect_node_logs_parser, include_run_id=True, include_run_label=True, action_label="inspect")
    inspect_node_logs_parser.add_argument(
        "--tail",
        dest="tail",
        type=int,
        help="Return only the last N log lines for this node.",
    )
    inspect_node_logs_parser.set_defaults(handler=_handle_inspect_node_logs)

    inspect_node_query_parser = subparsers.add_parser(
        "inspect_node_query",
        help="Show the original and resolved SQL for one pipeline node in a selected run.",
    )
    inspect_node_query_parser.add_argument("node_name", help="Pipeline node name to inspect.")
    _add_pipeline_selector_flags(inspect_node_query_parser, include_run_id=True, include_run_label=True, action_label="inspect")
    inspect_node_query_parser.set_defaults(handler=_handle_inspect_node_query)

    export_artifact_parser = subparsers.add_parser(
        "export_artifact",
        help="Export one materialized artifact table to a file.",
    )
    _add_pipeline_selector_flags(export_artifact_parser, include_run_id=True, include_run_label=True, action_label="export")
    export_artifact_parser.add_argument(
        "--node-name",
        dest="node_name",
        default=None,
        help="Pipeline node name whose artifact should be exported.",
    )
    export_artifact_parser.add_argument(
        "--artifact-name",
        dest="artifact_name",
        default=None,
        help="Physical artifact table name to export, such as main.policy_core.",
    )
    export_artifact_parser.add_argument(
        "--format",
        dest="format",
        default="csv",
        choices=["csv", "parquet", "json"],
        help="Export file format. Defaults to csv.",
    )
    export_artifact_parser.add_argument(
        "--output-path",
        dest="output_path",
        default=None,
        help="Optional explicit output file path. Defaults under .queron/<pipeline_id>/exports/<run_id>/",
    )
    export_artifact_parser.add_argument(
        "--overwrite",
        action="store_true",
        dest="overwrite",
        help="Overwrite the output file when it already exists.",
    )
    export_artifact_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    export_artifact_parser.set_defaults(handler=_handle_export_artifact)

    docs_parser = subparsers.add_parser(
        "docs",
        help="Open the local Queron HTML documentation.",
    )
    docs_parser.add_argument(
        "--no-browser",
        dest="no_browser",
        action="store_true",
        help="Print the docs path without opening a browser window.",
    )
    docs_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    docs_parser.set_defaults(handler=_handle_docs)

    open_graph_parser = subparsers.add_parser(
        "open_graph",
        help="Launch the local graph UI for one compiled pipeline.",
    )
    open_graph_parser.add_argument("pipeline", help="Path to the compiled OSS pipeline Python file.")
    open_graph_parser.add_argument(
        "--host",
        dest="host",
        default="127.0.0.1",
        help="Host interface to bind the graph server to.",
    )
    open_graph_parser.add_argument(
        "--port",
        dest="port",
        type=int,
        default=8890,
        help="Port to bind the graph server to.",
    )
    open_graph_parser.add_argument(
        "--no-browser",
        dest="no_browser",
        action="store_true",
        help="Start the server without opening a browser window.",
    )
    open_graph_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    open_graph_parser.set_defaults(handler=_handle_open_graph)
    return parser


def _add_common_compile_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("pipeline", help="Path to the compiled OSS pipeline Python file.")
    parser.add_argument(
        "--config",
        dest="config_path",
        default=None,
        help="Path to configurations.yaml. Defaults to ./configurations.yaml when present.",
    )
    parser.add_argument(
        "--target",
        dest="target",
        default=None,
        help="Override the target environment used for source resolution.",
    )


def _add_pipeline_selector_flags(
    parser: argparse.ArgumentParser,
    *,
    include_run_id: bool,
    include_run_label: bool,
    action_label: str,
) -> None:
    parser.add_argument(
        "--pipeline-id",
        dest="pipeline_id",
        default=None,
        help=f"Pipeline ID to {action_label}. Defaults to the local pipeline.py in the current folder.",
    )
    if include_run_id:
        parser.add_argument(
            "--run-id",
            dest="run_id",
            default=None,
            help=f"Run ID to {action_label}. Defaults to the latest run only when pipeline_id is resolved from the local pipeline.",
        )
    if include_run_label:
        parser.add_argument(
            "--run-label",
            dest="run_label",
            default=None,
            help=f"Unique run label to {action_label} within this pipeline.",
        )


def _pipeline_summary(compiled) -> dict[str, Any]:
    node_count = len(compiled.spec.nodes) if compiled.spec is not None else 0
    return {
        "ok": not has_compile_errors(compiled),
        "diagnostics": compiled.diagnostics,
        "node_count": node_count,
        "target": compiled.spec.target if compiled.spec is not None else None,
    }


def _print_diagnostics(compiled) -> None:
    if not compiled.diagnostics:
        return
    nodes_by_name = compiled.spec.node_by_name() if compiled.spec is not None else {}
    for diag in compiled.diagnostics:
        code = str(diag.get("code") or "diagnostic")
        message = str(diag.get("message") or "")
        node_name = str(diag.get("node_name") or "").strip()
        node = nodes_by_name.get(node_name)
        if node is not None and node.cell_id is not None:
            prefix_name = ("python" if str(node.kind or "").strip() == "python.ingress" else "sql") + f"_{int(node.cell_id)}"
        else:
            prefix_name = node_name
        prefix = f"[pipeline][{prefix_name}]" if prefix_name else "[pipeline]"
        detail = f"{code}: {message}" if code else message
        print(f"{prefix} {detail}", file=sys.stderr)


def _emit_json(payload: dict[str, Any], *, output_path: str | None = None) -> int:
    text = json.dumps(payload, indent=2)
    if output_path:
        Path(output_path).expanduser().resolve().write_text(text + "\n", encoding="utf-8")
    else:
        sys.stdout.write(text + "\n")
    return 0


def _confirmation_payload(
    *,
    action_label: str,
    warning_message: str,
    purge_targets: list[str],
    confirmed_command: str,
) -> dict[str, Any]:
    return {
        "ok": True,
        "completed": False,
        "requires_confirmation": True,
        "phase": "awaiting_confirmation",
        "purge_targets": list(purge_targets),
        "warning": warning_message,
        "confirmation": {
            "kind": "purge_outputs",
            "action": action_label.lower(),
            "question": warning_message,
            "confirm_label": "Yes, Purge",
            "cancel_label": "Cancel",
            "confirmed_command": confirmed_command,
        },
    }


def _can_prompt_for_confirmation() -> bool:
    try:
        return bool(sys.stdin.isatty() and sys.stdout.isatty())
    except Exception:
        return False


def _prompt_for_purge_confirmation(warning_message: str, purge_targets: list[str]) -> bool:
    print(f"[pipeline] {warning_message}", file=sys.stderr)
    for table in purge_targets:
        print(f"[pipeline]   - {table}", file=sys.stderr)
    try:
        response = input("Do you want to purge these tables and continue? [y/N]: ").strip().lower()
    except EOFError:
        return False
    return response in {"y", "yes"}


def _handle_init(args: argparse.Namespace) -> int:
    try:
        result = init_pipeline_project(
            args.path,
            sample=bool(args.sample),
            force=bool(args.force),
        )
    except Exception as exc:
        if args.json_output:
            return _emit_json({"ok": False, "error": str(exc)})
        print(f"Init failed: {exc}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "project_path": result.project_path,
        "sample": result.sample,
        "written_files": result.written_files,
        "created_directories": result.created_directories,
    }
    if args.json_output:
        return _emit_json(payload)

    print(f"Project: {result.project_path}")
    print(f"Scaffold: {'sample' if result.sample else 'starter'}")
    if result.written_files:
        print(f"Written files: {', '.join(result.written_files)}")
    if result.created_directories:
        print(f"Created directories: {', '.join(result.created_directories)}")
    print("Init succeeded.")
    return 0


def _handle_compile(args: argparse.Namespace) -> int:
    try:
        compiled = compile_pipeline(
            args.pipeline,
            config_path=args.config_path,
            target=args.target,
        )
    except Exception as exc:
        if args.json_output:
            return _emit_json({"ok": False, "error": str(exc)})
        print(f"Compile failed: {exc}", file=sys.stderr)
        return 1
    payload = _pipeline_summary(compiled)
    payload["artifact_path"] = compiled.contract.artifact_path if compiled.contract is not None else "-"
    payload["compile_id"] = compiled.contract.compile_id if compiled.contract is not None else None
    if args.json_output:
        return _emit_json(payload)

    print(f"Pipeline: {Path(args.pipeline).expanduser().resolve()}")
    print(f"Artifact DB: {payload['artifact_path']}")
    if payload["compile_id"]:
        print(f"Compile ID: {payload['compile_id']}")
    print(f"Nodes: {payload['node_count']}")
    if payload["target"]:
        print(f"Target: {payload['target']}")
    if payload["ok"]:
        print("Compile succeeded.")
        return 0

    _print_diagnostics(compiled)
    return 1


def _handle_run(args: argparse.Namespace) -> int:
    def _log(event) -> None:
        sys.stderr.write(format_log_event(event))
        sys.stderr.flush()

    try:
        runtime_bindings = _runtime_bindings_from_file(args.pipeline)
        runtime_vars = _load_runtime_vars(
            vars_json=getattr(args, "vars_json", None),
            vars_file=getattr(args, "vars_file", None),
        )
    except Exception as exc:
        if args.json_output:
            return _emit_json({"ok": False, "error": str(exc)})
        print(f"Run failed: {exc}", file=sys.stderr)
        return 1
    requires_full_purge = args.target_node is None
    if requires_full_purge and not bool(getattr(args, "clean_existing", False)):
        try:
            compiled, existing_outputs, artifact_path = list_existing_outputs_for_file(
                args.pipeline,
                config_path=args.config_path,
                connections_path=args.connections_path,
                runtime_bindings=runtime_bindings,
                runtime_vars=runtime_vars,
                target=args.target,
            )
        except Exception as exc:
            if args.json_output:
                return _emit_json({"ok": False, "error": str(exc)})
            print(f"Run failed: {exc}", file=sys.stderr)
            return 1
        if has_compile_errors(compiled):
            payload = {
                **_pipeline_summary(compiled),
                "executed_nodes": [],
                "artifact_path": artifact_path,
                "run_id": None,
            }
            if args.json_output:
                return _emit_json(payload)
            _print_diagnostics(compiled)
            return 1
        if existing_outputs:
            warning_message = (
                f"Run will purge {len(existing_outputs)} existing output table"
                f"{'' if len(existing_outputs) == 1 else 's'} before execution. Re-run with --clean-existing to continue."
            )
            if not args.json_output and _can_prompt_for_confirmation():
                if _prompt_for_purge_confirmation(warning_message, existing_outputs):
                    args.clean_existing = True
                else:
                    print("[pipeline] Run cancelled.", file=sys.stderr)
                    return 0
            payload = _confirmation_payload(
                action_label="Run",
                warning_message=warning_message,
                purge_targets=existing_outputs,
                confirmed_command=f"queron run {Path(args.pipeline).expanduser().resolve()} --clean-existing",
            )
            if args.json_output:
                return _emit_json(payload)
            if not bool(getattr(args, "clean_existing", False)):
                print(f"[pipeline] {warning_message}", file=sys.stderr)
                for table in existing_outputs:
                    print(f"[pipeline]   - {table}", file=sys.stderr)
                return 0

    try:
        on_log = fanout_log_handlers(
            _log if bool(getattr(args, "stream_logs", False)) and not args.json_output else None,
            _graph_log_handler(getattr(args, "graph_url", None)),
        )
        result = run_pipeline(
            args.pipeline,
            config_path=args.config_path,
            connections_path=args.connections_path,
            runtime_bindings=runtime_bindings,
            runtime_vars=runtime_vars,
            target=args.target,
            target_node=args.target_node,
            clean_existing=bool(args.clean_existing or requires_full_purge),
            set_final=bool(getattr(args, "set_final", False)),
            run_label=args.run_label,
            on_log=on_log,
        )
    except Exception as exc:
        if args.json_output:
            return _emit_json({"ok": False, "error": str(exc)})
        print(f"Run failed: {exc}", file=sys.stderr)
        return 1
    payload = {
        **_pipeline_summary(result.compiled),
        "executed_nodes": result.executed_nodes,
        "artifact_path": result.artifact_path,
        "run_id": result.run_id,
        "run_label": result.run_label,
        "log_path": result.log_path,
    }
    if args.json_output:
        return _emit_json(payload)

    print(f"Pipeline: {Path(args.pipeline).expanduser().resolve()}")
    print(f"Artifact DB: {result.artifact_path}")
    if result.run_id:
        print(f"Run ID: {result.run_id}")
    if result.run_label:
        print(f"Run label: {result.run_label}")
    if result.log_path:
        print(f"Log file: {result.log_path}")
    if payload["ok"]:
        if result.executed_nodes:
            print(f"Executed nodes: {', '.join(result.executed_nodes)}")
        else:
            print("No nodes were executed.")
        print("Run succeeded.")
        return 0

    _print_diagnostics(result.compiled)
    return 1
def _handle_resume(args: argparse.Namespace) -> int:
    def _log(event) -> None:
        sys.stderr.write(format_log_event(event))
        sys.stderr.flush()

    try:
        runtime_bindings = _runtime_bindings_from_file(args.pipeline)
        runtime_vars = _load_runtime_vars(
            vars_json=getattr(args, "vars_json", None),
            vars_file=getattr(args, "vars_file", None),
        )
    except Exception as exc:
        if args.json_output:
            return _emit_json({"ok": False, "error": str(exc)})
        print(f"Resume failed: {exc}", file=sys.stderr)
        return 1
    try:
        on_log = fanout_log_handlers(
            _log if bool(getattr(args, "stream_logs", False)) and not args.json_output else None,
            _graph_log_handler(getattr(args, "graph_url", None)),
        )
        result = resume_pipeline(
            args.pipeline,
            config_path=args.config_path,
            connections_path=args.connections_path,
            runtime_bindings=runtime_bindings,
            runtime_vars=runtime_vars,
            target=args.target,
            on_log=on_log,
        )
    except Exception as exc:
        if args.json_output:
            return _emit_json({"ok": False, "error": str(exc)})
        print(f"Resume failed: {exc}", file=sys.stderr)
        return 1

    payload = {
        **_pipeline_summary(result.compiled),
        "executed_nodes": result.executed_nodes,
        "artifact_path": result.artifact_path,
        "run_id": result.run_id,
        "run_label": result.run_label,
        "log_path": result.log_path,
    }
    if args.json_output:
        return _emit_json(payload)

    print(f"Pipeline: {Path(args.pipeline).expanduser().resolve()}")
    print(f"Artifact DB: {result.artifact_path}")
    if result.run_id:
        print(f"Run ID: {result.run_id}")
    if result.run_label:
        print(f"Run label: {result.run_label}")
    if result.log_path:
        print(f"Log file: {result.log_path}")
    if payload["ok"]:
        print(f"Executed nodes: {', '.join(result.executed_nodes) if result.executed_nodes else 'none'}")
        print("Resume succeeded.")
        return 0

    _print_diagnostics(result.compiled)
    return 1


def _handle_stop(args: argparse.Namespace) -> int:
    try:
        result = stop_pipeline(
            args.pipeline,
            run_id=args.run_id,
            reason=args.reason,
        )
    except Exception as exc:
        if args.json_output:
            return _emit_json({"ok": False, "error": str(exc)})
        print(f"Stop failed: {exc}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "artifact_path": result.artifact_path,
        "run_id": result.run_id,
        "run_label": result.run_label,
        "stop_requested": result.stop_requested,
        "request_path": result.request_path,
        "message": result.message,
    }
    if args.json_output:
        return _emit_json(payload)

    print(f"Pipeline: {Path(args.pipeline).expanduser().resolve()}")
    print(f"Artifact DB: {result.artifact_path}")
    if result.run_id:
        print(f"Run ID: {result.run_id}")
    if result.run_label:
        print(f"Run label: {result.run_label}")
    if result.request_path:
        print(f"Stop request: {result.request_path}")
    print(result.message)
    return 0


def _handle_force_stop(args: argparse.Namespace) -> int:
    try:
        result = force_stop_pipeline(
            args.pipeline,
            run_id=args.run_id,
            reason=args.reason,
        )
    except Exception as exc:
        if args.json_output:
            return _emit_json({"ok": False, "error": str(exc)})
        print(f"Force stop failed: {exc}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "artifact_path": result.artifact_path,
        "run_id": result.run_id,
        "run_label": result.run_label,
        "stop_requested": result.stop_requested,
        "stop_mode": result.stop_mode,
        "request_path": result.request_path,
        "message": result.message,
    }
    if args.json_output:
        return _emit_json(payload)

    print(f"Pipeline: {Path(args.pipeline).expanduser().resolve()}")
    print(f"Artifact DB: {result.artifact_path}")
    if result.run_id:
        print(f"Run ID: {result.run_id}")
    if result.run_label:
        print(f"Run label: {result.run_label}")
    if result.request_path:
        print(f"Force stop request: {result.request_path}")
    print(result.message)
    return 0


def _emit_reset_result(args: argparse.Namespace, action_label: str, result) -> int:
    payload = {
        **_pipeline_summary(result.compiled),
        "artifact_path": result.artifact_path,
        "reset_nodes": result.reset_nodes,
        "reset_tables": result.reset_tables,
    }
    if getattr(args, "json_output", False):
        return _emit_json(payload)

    print(f"Pipeline: {Path(args.pipeline).expanduser().resolve()}")
    print(f"Artifact DB: {result.artifact_path}")
    if payload["ok"]:
        print(f"Reset nodes: {', '.join(result.reset_nodes) if result.reset_nodes else 'none'}")
        print(f"Reset tables: {', '.join(result.reset_tables) if result.reset_tables else 'none'}")
    print(f"{action_label} succeeded.")
    return 0


def _handle_inspect_runs(args: argparse.Namespace) -> int:
    try:
        if args.limit is not None and int(args.limit) <= 0:
            raise RuntimeError("--limit must be a positive integer.")
        resolved = _resolve_cli_pipeline_artifact(pipeline_id=args.pipeline_id)
        artifact_path, active_contract, runs = _inspect_pipeline_runs(
            resolved["artifact_path"],
            limit=args.limit,
        )
    except Exception as exc:
        print(f"Inspect runs failed: {exc}", file=sys.stderr)
        return 1

    print(f"Pipeline: {Path(active_contract.pipeline_path).expanduser().resolve()}")
    print(f"Artifact DB: {artifact_path}")
    if not runs:
        print("No runs found.")
        return 0

    print("")
    print("Runs")
    for item in runs:
        run_label = str(item.get("run_label") or "").strip() or "-"
        started_at = str(item.get("started_at") or "").strip() or "-"
        finished_at = str(item.get("finished_at") or "").strip() or "-"
        status = str(item.get("status") or "").strip() or "-"
        run_id = str(item.get("run_id") or "").strip() or "-"
        is_final = "true" if bool(item.get("is_final")) else "false"
        print(f"- {run_id}  {run_label}  {status}  final={is_final}  {started_at} -> {finished_at}")
    return 0


def _handle_inspect_logs(args: argparse.Namespace) -> int:
    try:
        resolved = _resolve_cli_run_selector(args)
        artifact_path, active_contract, selected_run, lines = _inspect_pipeline_logs(
            resolved["artifact_path"],
            run_id=resolved["selected_run_id"],
            run_label=resolved["selected_run_label"],
            tail=args.tail,
        )
    except Exception as exc:
        if getattr(args, "json_output", False):
            return _emit_json({"ok": False, "error": str(exc)})
        print(f"Inspect logs failed: {exc}", file=sys.stderr)
        return 1

    parsed_logs: list[dict[str, Any]] = []
    for line in lines:
        text = str(line or "").strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
            if isinstance(payload, dict):
                parsed_logs.append(payload)
                continue
        except Exception:
            pass
        parsed_logs.append({"message": text})

    if getattr(args, "json_output", False):
        return _emit_json(
            {
                "ok": True,
                "pipeline_path": str(Path(active_contract.pipeline_path).expanduser().resolve()),
                "artifact_path": str(artifact_path),
                "run_id": selected_run.get("run_id"),
                "run_label": str(selected_run.get("run_label") or "").strip() or None,
                "is_final": bool(selected_run.get("is_final")),
                "log_path": str(selected_run.get("log_path") or "").strip() or None,
                "logs": parsed_logs,
            }
        )

    print(f"Pipeline: {Path(active_contract.pipeline_path).expanduser().resolve()}")
    print(f"Artifact DB: {artifact_path}")
    print(f"Run ID: {selected_run.get('run_id')}")
    run_label = str(selected_run.get("run_label") or "").strip()
    if run_label:
        print(f"Run label: {run_label}")
    print(f"Final: {'true' if bool(selected_run.get('is_final')) else 'false'}")
    log_path = str(selected_run.get("log_path") or "").strip()
    if log_path:
        print(f"Log file: {log_path}")
    print("")
    print("Logs")
    if not parsed_logs:
        print("No log entries found.")
        return 0
    for entry in parsed_logs:
        timestamp = str(entry.get("timestamp") or "-").strip() or "-"
        severity = str(entry.get("severity") or "info").strip().lower() or "info"
        code = str(entry.get("code") or "-").strip() or "-"
        node_name = str(entry.get("node_name") or "-").strip() or "-"
        message = str(entry.get("message") or "").strip()
        print(f"{timestamp}  {severity:<7}  {code:<28}  {node_name:<12}  {message}")
        if getattr(args, "verbose", False):
            source = str(entry.get("source") or "-").strip() or "-"
            node_kind = str(entry.get("node_kind") or "-").strip() or "-"
            artifact_name = str(entry.get("artifact_name") or "-").strip() or "-"
            details = entry.get("details")
            print(f"  source={source}  kind={node_kind}  artifact={artifact_name}")
            if isinstance(details, dict) and details:
                print(f"  details={json.dumps(details, ensure_ascii=True)}")
    return 0


def _handle_inspect_dag(args: argparse.Namespace) -> int:
    try:
        resolved = _resolve_cli_run_selector(args)
        result = inspect_dag(
            resolved["artifact_path"],
            run_id=resolved["selected_run_id"],
            run_label=resolved["selected_run_label"],
        )
    except Exception as exc:
        print(f"Inspect dag failed: {exc}", file=sys.stderr)
        return 1

    print(f"Pipeline: {result.pipeline_path}")
    print(f"Artifact DB: {result.artifact_path}")
    if result.compile_id:
        print(f"Compile ID: {result.compile_id}")
    if result.run_id:
        print(f"Run ID: {result.run_id}")
    if result.run_label:
        print(f"Run label: {result.run_label}")
    if result.run_status:
        print(f"Run status: {result.run_status}")
    print(f"Final: {'true' if result.is_final else 'false'}")

    print("")
    print("Nodes")
    if not result.nodes:
        print("No nodes found.")
    else:
        for item in result.nodes:
            name = str(item.get("name") or "").strip() or "-"
            kind = str(item.get("kind") or "").strip() or "-"
            state = str(item.get("current_state") or "").strip() or "-"
            artifact_name = str(item.get("artifact_name") or "").strip() or "-"
            print(f"- {name}  {kind}  {state}  {artifact_name}")

    print("")
    print("Edges")
    if not result.edges:
        print("No edges found.")
        return 0
    for source, target in result.edges:
        print(f"- {source} -> {target}")
    return 0


def _handle_inspect_node(args: argparse.Namespace) -> int:
    try:
        resolved = _resolve_cli_run_selector(args)
        result = inspect_node(
            resolved["artifact_path"],
            args.node_name,
            run_id=resolved["selected_run_id"],
            run_label=resolved["selected_run_label"],
            upstream=bool(args.upstream),
            downstream=bool(args.downstream),
        )
    except Exception as exc:
        print(f"Inspect node failed: {exc}", file=sys.stderr)
        return 1

    print(f"Pipeline: {result.pipeline_path}")
    print(f"Artifact DB: {result.artifact_path}")
    if result.compile_id:
        print(f"Compile ID: {result.compile_id}")
    if result.run_id:
        print(f"Run ID: {result.run_id}")
    if result.run_label:
        print(f"Run label: {result.run_label}")
    if result.run_status:
        print(f"Run status: {result.run_status}")
    print(f"Final: {'true' if result.is_final else 'false'}")

    print("")
    print(f"Selection: {result.selection}")
    if result.requested_node:
        print(f"Requested node: {result.requested_node}")

    print("")
    print("Nodes")
    if not result.nodes:
        print("No nodes found.")
        return 0

    single_node_view = result.selection == "node" and len(result.nodes) == 1
    for item in result.nodes:
        name = str(item.get("name") or "").strip() or "-"
        kind = str(item.get("kind") or "").strip() or "-"
        state = str(item.get("current_state") or "").strip() or "-"
        logical_artifact = str(item.get("logical_artifact") or "").strip() or "-"
        artifact_name = str(item.get("artifact_name") or "").strip() or "-"
        print(f"- {name}  {kind}  {state}  logical={logical_artifact}  physical={artifact_name}")
        if not single_node_view:
            continue
        node_run_status = str(item.get("node_run_status") or "").strip() or "-"
        started_at = str(item.get("started_at") or "").strip() or "-"
        finished_at = str(item.get("finished_at") or "").strip() or "-"
        lookup_table = str(item.get("lookup_table") or "").strip()
        dependencies = [str(dep).strip() for dep in list(item.get("dependencies") or []) if str(dep).strip()]
        dependents = [str(dep).strip() for dep in list(item.get("dependents") or []) if str(dep).strip()]
        print(f"  Node run status: {node_run_status}")
        print(f"  Started: {started_at}")
        print(f"  Finished: {finished_at}")
        if lookup_table:
            print(f"  Lookup table: {lookup_table}")
        print(f"  Dependencies: {', '.join(dependencies) if dependencies else '-'}")
        print(f"  Dependents: {', '.join(dependents) if dependents else '-'}")
    return 0


def _handle_inspect_node_history(args: argparse.Namespace) -> int:
    try:
        resolved = _resolve_cli_run_selector(args)
        result = inspect_node_history(
            resolved["artifact_path"],
            args.node_name,
            run_id=resolved["selected_run_id"],
            run_label=resolved["selected_run_label"],
        )
    except Exception as exc:
        print(f"Inspect node history failed: {exc}", file=sys.stderr)
        return 1

    print(f"Pipeline: {result.pipeline_path}")
    print(f"Artifact DB: {result.artifact_path}")
    if result.compile_id:
        print(f"Compile ID: {result.compile_id}")
    if result.run_id:
        print(f"Run ID: {result.run_id}")
    if result.run_label:
        print(f"Run label: {result.run_label}")
    if result.run_status:
        print(f"Run status: {result.run_status}")
    print(f"Final: {'true' if result.is_final else 'false'}")
    if result.node_name:
        print(f"Node: {result.node_name}")
    if result.node_kind:
        print(f"Node kind: {result.node_kind}")
    if result.node_run_status:
        print(f"Node status: {result.node_run_status}")
    if result.logical_artifact:
        print(f"Logical artifact: {result.logical_artifact}")
    if result.artifact_name:
        print(f"Physical artifact: {result.artifact_name}")
    if result.started_at:
        print(f"Started: {result.started_at}")
    if result.finished_at:
        print(f"Finished: {result.finished_at}")
    if result.error_message:
        print(f"Error: {result.error_message}")

    print("")
    print("States")
    if not result.states:
        print("No node history found.")
        return 0
    for item in result.states:
        state = str(item.get("state") or "").strip() or "-"
        created_at = str(item.get("created_at") or "").strip() or "-"
        trigger = str(item.get("trigger") or "").strip() or "-"
        active_suffix = "  active" if bool(item.get("is_active")) else ""
        print(f"- {state}  {created_at}  trigger={trigger}{active_suffix}")
    return 0


def _handle_inspect_node_query(args: argparse.Namespace) -> int:
    try:
        resolved = _resolve_cli_run_selector(args)
        result = inspect_node_query(
            resolved["artifact_path"],
            args.node_name,
            run_id=resolved["selected_run_id"],
            run_label=resolved["selected_run_label"],
        )
    except Exception as exc:
        print(f"Inspect node query failed: {exc}", file=sys.stderr)
        return 1

    print(f"Pipeline: {result.pipeline_path}")
    print(f"Artifact DB: {result.artifact_path}")
    if result.pipeline_id:
        print(f"Pipeline ID: {result.pipeline_id}")
    if result.compile_id:
        print(f"Compile ID: {result.compile_id}")
    if result.run_id:
        print(f"Run ID: {result.run_id}")
    if result.run_label:
        print(f"Run label: {result.run_label}")
    if result.run_status:
        print(f"Run status: {result.run_status}")
    print(f"Final: {'true' if result.is_final else 'false'}")
    if result.node_name:
        print(f"Node: {result.node_name}")
    if result.node_kind:
        print(f"Node kind: {result.node_kind}")
    if result.logical_artifact:
        print(f"Logical artifact: {result.logical_artifact}")
    if result.dependencies:
        print(f"Dependencies: {', '.join(result.dependencies)}")

    print("")
    print("SQL")
    print(result.sql or "-")
    print("")
    print("Resolved SQL")
    print(result.resolved_sql or "-")
    return 0


def _handle_inspect_node_logs(args: argparse.Namespace) -> int:
    try:
        resolved = _resolve_cli_run_selector(args)
        result = inspect_node_logs(
            resolved["artifact_path"],
            args.node_name,
            run_id=resolved["selected_run_id"],
            run_label=resolved["selected_run_label"],
            tail=args.tail,
        )
    except Exception as exc:
        print(f"Inspect node logs failed: {exc}", file=sys.stderr)
        return 1

    print(f"Pipeline: {result.pipeline_path}")
    print(f"Artifact DB: {result.artifact_path}")
    if result.compile_id:
        print(f"Compile ID: {result.compile_id}")
    if result.run_id:
        print(f"Run ID: {result.run_id}")
    if result.run_label:
        print(f"Run label: {result.run_label}")
    if result.run_status:
        print(f"Run status: {result.run_status}")
    print(f"Final: {'true' if result.is_final else 'false'}")
    if result.node_name:
        print(f"Node: {result.node_name}")
    if result.node_kind:
        print(f"Node kind: {result.node_kind}")

    print("")
    print("Logs")
    if not result.logs:
        print("No log entries found.")
        return 0
    for entry in result.logs:
        timestamp = str(entry.get("timestamp") or "-").strip() or "-"
        severity = str(entry.get("severity") or "info").strip().lower() or "info"
        code = str(entry.get("code") or "-").strip() or "-"
        node_name = str(entry.get("node_name") or "-").strip() or "-"
        message = str(entry.get("message") or "").strip()
        print(f"{timestamp}  {severity:<7}  {code:<28}  {node_name:<12}  {message}")
    return 0


def _handle_export_artifact(args: argparse.Namespace) -> int:
    try:
        resolved = _resolve_cli_run_selector(args)
        result = export_artifact(
            resolved["artifact_path"],
            node_name=args.node_name,
            artifact_name=args.artifact_name,
            run_id=resolved["selected_run_id"],
            run_label=resolved["selected_run_label"],
            format=args.format,
            output_path=args.output_path,
            overwrite=bool(args.overwrite),
        )
    except Exception as exc:
        if getattr(args, "json_output", False):
            return _emit_json({"ok": False, "error": str(exc)})
        print(f"Export artifact failed: {exc}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "pipeline_path": result.pipeline_path,
        "artifact_path": result.artifact_path,
        "pipeline_id": result.pipeline_id,
        "compile_id": result.compile_id,
        "run_id": result.run_id,
        "run_label": result.run_label,
        "run_status": result.run_status,
        "is_final": result.is_final,
        "node_name": result.node_name,
        "node_kind": result.node_kind,
        "logical_artifact": result.logical_artifact,
        "artifact_name": result.artifact_name,
        "effective_artifact_path": result.effective_artifact_path,
        "output_path": result.output_path,
        "export_format": result.export_format,
        "row_count": result.row_count,
        "file_size_bytes": result.file_size_bytes,
    }
    if getattr(args, "json_output", False):
        return _emit_json(payload)

    print(f"Pipeline: {result.pipeline_path}")
    print(f"Artifact DB: {result.artifact_path}")
    if result.run_id:
        print(f"Run ID: {result.run_id}")
    if result.run_label:
        print(f"Run label: {result.run_label}")
    if result.node_name:
        print(f"Node: {result.node_name}")
    if result.artifact_name:
        print(f"Artifact: {result.artifact_name}")
    print(f"Format: {result.export_format}")
    print(f"Rows: {result.row_count if result.row_count is not None else '-'}")
    if result.file_size_bytes is not None:
        print(f"File size: {result.file_size_bytes} bytes")
    print(f"Output: {result.output_path}")
    return 0


def _handle_docs(args: argparse.Namespace) -> int:
    docs_path = (Path(__file__).resolve().parent.parent / "docs" / "html" / "index.html").resolve()
    if not docs_path.exists() or not docs_path.is_file():
        message = f"Docs index was not found at '{docs_path}'."
        if args.json_output:
            return _emit_json({"ok": False, "error": message})
        print(f"Docs failed: {message}", file=sys.stderr)
        return 1

    url = docs_path.as_uri()
    payload = {
        "ok": True,
        "docs_path": str(docs_path),
        "url": url,
    }
    if args.json_output:
        return _emit_json(payload)

    print(f"Docs: {docs_path}")
    if not bool(args.no_browser):
        webbrowser.open(url)
    return 0


def _handle_open_graph(args: argparse.Namespace) -> int:
    try:
        from .graph_live import build_graph_live_server, resolve_graph_live_context

        context = resolve_graph_live_context(args.pipeline)
        server = build_graph_live_server(
            context,
            host=args.host,
            port=int(args.port),
        )
    except Exception as exc:
        if args.json_output:
            return _emit_json({"ok": False, "error": str(exc)})
        print(f"Open-graph failed: {exc}", file=sys.stderr)
        return 1

    bound_host, bound_port = server.server_address[:2]
    url = f"http://{bound_host}:{bound_port}"
    payload = {
        "ok": True,
        "pipeline_path": context.pipeline_path,
        "artifact_path": context.artifact_path,
        "pipeline_id": context.pipeline_id,
        "host": bound_host,
        "port": bound_port,
        "url": url,
    }
    if args.json_output:
        server.server_close()
        return _emit_json(payload)

    print(f"Pipeline: {context.pipeline_path}")
    print(f"Artifact DB: {context.artifact_path}")
    print(f"Pipeline ID: {context.pipeline_id}")
    print(f"Graph UI: {url}")

    if not bool(args.no_browser):
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def _handle_reset_node(args: argparse.Namespace) -> int:
    try:
        result = reset_node(
            args.pipeline,
            node_name=args.node_name,
            config_path=args.config_path,
            target=args.target,
        )
    except Exception as exc:
        if args.json_output:
            return _emit_json({"ok": False, "error": str(exc)})
        print(f"Reset-node failed: {exc}", file=sys.stderr)
        return 1
    return _emit_reset_result(args, "Reset-node", result)


def _handle_reset_downstream(args: argparse.Namespace) -> int:
    try:
        result = reset_downstream(
            args.pipeline,
            node_name=args.node_name,
            config_path=args.config_path,
            target=args.target,
        )
    except Exception as exc:
        if args.json_output:
            return _emit_json({"ok": False, "error": str(exc)})
        print(f"Reset-downstream failed: {exc}", file=sys.stderr)
        return 1
    return _emit_reset_result(args, "Reset-downstream", result)


def _handle_reset_upstream(args: argparse.Namespace) -> int:
    try:
        result = reset_upstream(
            args.pipeline,
            node_name=args.node_name,
            config_path=args.config_path,
            target=args.target,
        )
    except Exception as exc:
        if args.json_output:
            return _emit_json({"ok": False, "error": str(exc)})
        print(f"Reset-upstream failed: {exc}", file=sys.stderr)
        return 1
    return _emit_reset_result(args, "Reset-upstream", result)


def _handle_reset_all(args: argparse.Namespace) -> int:
    try:
        result = reset_all(
            args.pipeline,
            config_path=args.config_path,
            target=args.target,
        )
    except Exception as exc:
        if args.json_output:
            return _emit_json({"ok": False, "error": str(exc)})
        print(f"Reset-all failed: {exc}", file=sys.stderr)
        return 1
    return _emit_reset_result(args, "Reset-all", result)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
