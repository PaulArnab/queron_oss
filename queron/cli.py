from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .api import (
    _inspect_pipeline_logs,
    _inspect_pipeline_runs,
    init_pipeline_project,
    compile_pipeline,
    inspect_node,
    inspect_node_history,
    inspect_dag,
    has_compile_errors,
    list_existing_outputs_for_file,
    reset_all,
    reset_downstream,
    reset_upstream,
    reset_node,
    resume_pipeline,
    run_pipeline,
)
from .runtime_models import format_log_event


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
        "--yes",
        action="store_true",
        dest="confirm_purge",
        help="Confirm purging existing pipeline outputs before execution.",
    )
    run_parser.add_argument(
        "--stream-logs",
        action="store_true",
        dest="stream_logs",
        help="Stream pipeline logs to the console while the run executes.",
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
    resume_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    resume_parser.set_defaults(handler=_handle_resume)

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
    inspect_runs_parser.add_argument("artifact", help="Path to the Queron artifact database to inspect.")
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
    inspect_logs_parser.add_argument("artifact", help="Path to the Queron artifact database to inspect.")
    inspect_logs_parser.add_argument(
        "--run-id",
        dest="run_id",
        default=None,
        help="Run ID to inspect. Defaults to the latest run when omitted.",
    )
    inspect_logs_parser.add_argument(
        "--run-label",
        dest="run_label",
        default=None,
        help="Unique run label to inspect within this pipeline.",
    )
    inspect_logs_parser.add_argument(
        "--tail",
        dest="tail",
        type=int,
        default=None,
        help="Show only the last N log lines.",
    )
    inspect_logs_parser.set_defaults(handler=_handle_inspect_logs)

    inspect_dag_parser = subparsers.add_parser(
        "inspect_dag",
        help="Show the compiled DAG and the current node state view for one pipeline run.",
    )
    inspect_dag_parser.add_argument("artifact", help="Path to the Queron artifact database to inspect.")
    inspect_dag_parser.add_argument(
        "--run-id",
        dest="run_id",
        default=None,
        help="Run ID to inspect. Defaults to the latest run when omitted.",
    )
    inspect_dag_parser.add_argument(
        "--run-label",
        dest="run_label",
        default=None,
        help="Unique run label to inspect within this pipeline.",
    )
    inspect_dag_parser.set_defaults(handler=_handle_inspect_dag)

    inspect_node_parser = subparsers.add_parser(
        "inspect_node",
        help="Show one pipeline node or its upstream/downstream slice for a selected run.",
    )
    inspect_node_parser.add_argument("artifact", help="Path to the Queron artifact database to inspect.")
    inspect_node_parser.add_argument("node_name", help="Pipeline node name to inspect.")
    inspect_node_parser.add_argument(
        "--run-id",
        dest="run_id",
        default=None,
        help="Run ID to inspect. Defaults to the latest run when omitted.",
    )
    inspect_node_parser.add_argument(
        "--run-label",
        dest="run_label",
        default=None,
        help="Unique run label to inspect within this pipeline.",
    )
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
    inspect_node_history_parser.add_argument("artifact", help="Path to the Queron artifact database to inspect.")
    inspect_node_history_parser.add_argument("node_name", help="Pipeline node name to inspect.")
    inspect_node_history_parser.add_argument(
        "--run-id",
        dest="run_id",
        default=None,
        help="Run ID to inspect. Defaults to the latest run when omitted.",
    )
    inspect_node_history_parser.add_argument(
        "--run-label",
        dest="run_label",
        default=None,
        help="Unique run label to inspect within this pipeline.",
    )
    inspect_node_history_parser.set_defaults(handler=_handle_inspect_node_history)
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

    requires_full_purge = args.target_node is None
    if requires_full_purge and not bool(getattr(args, "confirm_purge", False)):
        compiled, existing_outputs, artifact_path = list_existing_outputs_for_file(
            args.pipeline,
            config_path=args.config_path,
            connections_path=args.connections_path,
            target=args.target,
        )
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
                f"{'' if len(existing_outputs) == 1 else 's'} before execution. Re-run with --yes to confirm."
            )
            if not args.json_output and _can_prompt_for_confirmation():
                if _prompt_for_purge_confirmation(warning_message, existing_outputs):
                    args.confirm_purge = True
                else:
                    print("[pipeline] Run cancelled.", file=sys.stderr)
                    return 0
            payload = _confirmation_payload(
                action_label="Run",
                warning_message=warning_message,
                purge_targets=existing_outputs,
                confirmed_command=f"queron run {Path(args.pipeline).expanduser().resolve()} --yes",
            )
            if args.json_output:
                return _emit_json(payload)
            if not bool(getattr(args, "confirm_purge", False)):
                print(f"[pipeline] {warning_message}", file=sys.stderr)
                for table in existing_outputs:
                    print(f"[pipeline]   - {table}", file=sys.stderr)
                return 0

    try:
        result = run_pipeline(
            args.pipeline,
            config_path=args.config_path,
            connections_path=args.connections_path,
            target=args.target,
            target_node=args.target_node,
            clean_existing=bool(args.clean_existing or requires_full_purge),
            run_label=args.run_label,
            on_log=_log if bool(getattr(args, "stream_logs", False)) and not args.json_output else None,
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
        result = resume_pipeline(
            args.pipeline,
            config_path=args.config_path,
            connections_path=args.connections_path,
            target=args.target,
            on_log=_log if bool(getattr(args, "stream_logs", False)) and not args.json_output else None,
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
        artifact_path, active_contract, runs = _inspect_pipeline_runs(
            args.artifact,
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
        print(f"- {run_id}  {run_label}  {status}  {started_at} -> {finished_at}")
    return 0


def _handle_inspect_logs(args: argparse.Namespace) -> int:
    try:
        artifact_path, active_contract, selected_run, lines = _inspect_pipeline_logs(
            args.artifact,
            run_id=args.run_id,
            run_label=args.run_label,
            tail=args.tail,
        )
    except Exception as exc:
        print(f"Inspect logs failed: {exc}", file=sys.stderr)
        return 1

    print(f"Pipeline: {Path(active_contract.pipeline_path).expanduser().resolve()}")
    print(f"Artifact DB: {artifact_path}")
    print(f"Run ID: {selected_run.get('run_id')}")
    run_label = str(selected_run.get("run_label") or "").strip()
    if run_label:
        print(f"Run label: {run_label}")
    log_path = str(selected_run.get("log_path") or "").strip()
    if log_path:
        print(f"Log file: {log_path}")
    print("")
    print("Logs")
    if not lines:
        print("No log entries found.")
        return 0
    for line in lines:
        print(line)
    return 0


def _handle_inspect_dag(args: argparse.Namespace) -> int:
    try:
        result = inspect_dag(
            args.artifact,
            run_id=args.run_id,
            run_label=args.run_label,
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
        result = inspect_node(
            args.artifact,
            args.node_name,
            run_id=args.run_id,
            run_label=args.run_label,
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
        dependencies = [str(dep).strip() for dep in list(item.get("dependencies") or []) if str(dep).strip()]
        dependents = [str(dep).strip() for dep in list(item.get("dependents") or []) if str(dep).strip()]
        print(f"  Node run status: {node_run_status}")
        print(f"  Started: {started_at}")
        print(f"  Finished: {finished_at}")
        print(f"  Dependencies: {', '.join(dependencies) if dependencies else '-'}")
        print(f"  Dependents: {', '.join(dependents) if dependents else '-'}")
    return 0


def _handle_inspect_node_history(args: argparse.Namespace) -> int:
    try:
        result = inspect_node_history(
            args.artifact,
            args.node_name,
            run_id=args.run_id,
            run_label=args.run_label,
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
