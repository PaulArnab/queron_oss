from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .api import (
    compile_pipeline_file,
    has_compile_errors,
    list_existing_outputs_for_file,
    reset_all_file,
    reset_downstream_file,
    reset_upstream_file,
    reset_node_file,
    resume_pipeline_file,
    run_pipeline_file,
)
from .runtime_models import format_log_event


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="queron", description="Compile and run Queron OSS pipelines.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    compile_parser = subparsers.add_parser("compile", help="Compile a Queron pipeline and validate its DAG.")
    _add_common_compile_flags(compile_parser)
    compile_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    compile_parser.set_defaults(handler=_handle_compile)

    run_parser = subparsers.add_parser("run", help="Compile and execute a Queron pipeline.")
    _add_common_compile_flags(run_parser)
    run_parser.add_argument(
        "--connections",
        dest="connections_path",
        default=None,
        help="Path to connections.yaml. Defaults to ./connections.yaml when present.",
    )
    run_parser.add_argument(
        "--artifact-path",
        dest="artifact_path",
        default=None,
        help="Path to the DuckDB artifact database. Defaults to ./.queron/<pipeline>.duckdb.",
    )
    run_parser.add_argument(
        "--target-node",
        dest="target_node",
        default=None,
        help="Execute only the selected target node and its dependencies.",
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
    run_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    run_parser.set_defaults(handler=_handle_run)

    resume_parser = subparsers.add_parser(
        "resume",
        help="Resume the pipeline from the failed node in the latest failed run.",
    )
    _add_common_compile_flags(resume_parser)
    resume_parser.add_argument(
        "--connections",
        dest="connections_path",
        default=None,
        help="Path to connections.yaml. Defaults to ./connections.yaml when present.",
    )
    resume_parser.add_argument(
        "--artifact-path",
        dest="artifact_path",
        default=None,
        help="Path to the DuckDB artifact database. Defaults to ./.queron/<pipeline>.duckdb.",
    )
    resume_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    resume_parser.set_defaults(handler=_handle_resume)

    reset_node_parser = subparsers.add_parser("reset-node", help="Drop the output table for one pipeline node.")
    _add_common_compile_flags(reset_node_parser)
    _add_common_reset_flags(reset_node_parser)
    reset_node_parser.add_argument("node_name", help="Pipeline node name to reset.")
    reset_node_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    reset_node_parser.set_defaults(handler=_handle_reset_node)

    reset_downstream_parser = subparsers.add_parser(
        "reset-downstream",
        help="Drop the output tables for a node and all downstream dependents.",
    )
    _add_common_compile_flags(reset_downstream_parser)
    _add_common_reset_flags(reset_downstream_parser)
    reset_downstream_parser.add_argument("node_name", help="Pipeline node name to reset downstream from.")
    reset_downstream_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    reset_downstream_parser.set_defaults(handler=_handle_reset_downstream)

    reset_upstream_parser = subparsers.add_parser(
        "reset-upstream",
        help="Drop the output tables for a node and all upstream dependencies.",
    )
    _add_common_compile_flags(reset_upstream_parser)
    _add_common_reset_flags(reset_upstream_parser)
    reset_upstream_parser.add_argument("node_name", help="Pipeline node name to reset upstream from.")
    reset_upstream_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    reset_upstream_parser.set_defaults(handler=_handle_reset_upstream)

    reset_all_parser = subparsers.add_parser("reset-all", help="Drop all pipeline output tables.")
    _add_common_compile_flags(reset_all_parser)
    _add_common_reset_flags(reset_all_parser)
    reset_all_parser.add_argument("--json", action="store_true", dest="json_output", help="Write JSON output.")
    reset_all_parser.set_defaults(handler=_handle_reset_all)
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
def _add_common_reset_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--artifact-path",
        dest="artifact_path",
        default=None,
        help="Path to the DuckDB artifact database. Defaults to ./.queron/<pipeline>.duckdb.",
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


def _handle_compile(args: argparse.Namespace) -> int:
    compiled = compile_pipeline_file(args.pipeline, config_path=args.config_path, target=args.target)
    payload = _pipeline_summary(compiled)
    if args.json_output:
        return _emit_json(payload)

    print(f"Pipeline: {Path(args.pipeline).expanduser().resolve()}")
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
            artifact_path=args.artifact_path,
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
        result = run_pipeline_file(
            args.pipeline,
            config_path=args.config_path,
            connections_path=args.connections_path,
            target=args.target,
            artifact_path=args.artifact_path,
            target_node=args.target_node,
            clean_existing=bool(args.clean_existing or requires_full_purge),
            on_log=None if args.json_output else _log,
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
    }
    if args.json_output:
        return _emit_json(payload)

    print(f"Pipeline: {Path(args.pipeline).expanduser().resolve()}")
    print(f"Artifact DB: {result.artifact_path}")
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
        result = resume_pipeline_file(
            args.pipeline,
            config_path=args.config_path,
            connections_path=args.connections_path,
            target=args.target,
            artifact_path=args.artifact_path,
            on_log=None if args.json_output else _log,
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
    }
    if args.json_output:
        return _emit_json(payload)

    print(f"Pipeline: {Path(args.pipeline).expanduser().resolve()}")
    print(f"Artifact DB: {result.artifact_path}")
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

    _print_diagnostics(result.compiled)
    return 1


def _handle_reset_node(args: argparse.Namespace) -> int:
    try:
        result = reset_node_file(
            args.pipeline,
            node_name=args.node_name,
            config_path=args.config_path,
            target=args.target,
            artifact_path=args.artifact_path,
        )
    except Exception as exc:
        if args.json_output:
            return _emit_json({"ok": False, "error": str(exc)})
        print(f"Reset-node failed: {exc}", file=sys.stderr)
        return 1
    return _emit_reset_result(args, "Reset-node", result)


def _handle_reset_downstream(args: argparse.Namespace) -> int:
    try:
        result = reset_downstream_file(
            args.pipeline,
            node_name=args.node_name,
            config_path=args.config_path,
            target=args.target,
            artifact_path=args.artifact_path,
        )
    except Exception as exc:
        if args.json_output:
            return _emit_json({"ok": False, "error": str(exc)})
        print(f"Reset-downstream failed: {exc}", file=sys.stderr)
        return 1
    return _emit_reset_result(args, "Reset-downstream", result)


def _handle_reset_upstream(args: argparse.Namespace) -> int:
    try:
        result = reset_upstream_file(
            args.pipeline,
            node_name=args.node_name,
            config_path=args.config_path,
            target=args.target,
            artifact_path=args.artifact_path,
        )
    except Exception as exc:
        if args.json_output:
            return _emit_json({"ok": False, "error": str(exc)})
        print(f"Reset-upstream failed: {exc}", file=sys.stderr)
        return 1
    return _emit_reset_result(args, "Reset-upstream", result)


def _handle_reset_all(args: argparse.Namespace) -> int:
    try:
        result = reset_all_file(
            args.pipeline,
            config_path=args.config_path,
            target=args.target,
            artifact_path=args.artifact_path,
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
