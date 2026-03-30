from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .compiler import CompiledPipeline, compile_pipeline_code
from .executor import _select_downstream_nodes, _select_upstream_nodes, execute_pipeline, execute_selected_nodes
from .runtime import PipelineRuntime
from .runtime_models import LogCode, PipelineLogEvent, build_log_event
from .specs import PipelineSpec

@dataclass
class RunPipelineResult:
    compiled: CompiledPipeline
    executed_nodes: list[str]
    artifact_path: str
    run_id: str | None = None


@dataclass
class ResetPipelineResult:
    compiled: CompiledPipeline
    artifact_path: str
    reset_nodes: list[str]
    reset_tables: list[str]


def _emit_log_event(
    on_log: Callable[[PipelineLogEvent], None] | None,
    *,
    code: str,
    message: str,
    severity: str = "info",
    details: dict[str, Any] | None = None,
    run_id: str | None = None,
    node_name: str | None = None,
    node_kind: str | None = None,
    artifact_name: str | None = None,
) -> PipelineLogEvent:
    event = build_log_event(
        code=code,
        message=message,
        severity=severity,  # type: ignore[arg-type]
        source="cli",
        details=details,
        run_id=run_id,
        node_name=node_name,
        node_kind=node_kind,
        artifact_name=artifact_name,
    )
    if on_log is None:
        return event
    try:
        on_log(event)
    except Exception:
        pass
    return event


def _read_text_file(path: str | Path | None, *, label: str, required: bool = False) -> str | None:
    if path is None:
        return None
    resolved = Path(path).expanduser().resolve()
    if not resolved.exists():
        if required:
            raise RuntimeError(f"{label} '{resolved}' was not found.")
        return None
    if not resolved.is_file():
        raise RuntimeError(f"{label} '{resolved}' is not a file.")
    return resolved.read_text(encoding="utf-8")


def _default_pipeline_id(pipeline_path: Path) -> str:
    return pipeline_path.stem


def _default_artifact_path(pipeline_path: Path) -> Path:
    return pipeline_path.parent / ".queron" / f"{pipeline_path.stem}.duckdb"


def load_pipeline_code_from_file(path: str | Path) -> tuple[Path, str]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.exists() or not resolved.is_file():
        raise RuntimeError(f"Pipeline file '{resolved}' was not found.")
    return resolved, resolved.read_text(encoding="utf-8")


def compile_pipeline_file(
    pipeline_path: str | Path,
    *,
    config_path: str | Path | None = None,
    target: str | None = None,
) -> CompiledPipeline:
    resolved_pipeline_path, code = load_pipeline_code_from_file(pipeline_path)
    yaml_text = _read_text_file(config_path, label="Configuration file") if config_path else None
    if yaml_text is None and config_path is None:
        yaml_text = _read_text_file(resolved_pipeline_path.parent / "configurations.yaml", label="Configuration file")
    return compile_pipeline_text(code, yaml_text=yaml_text, target=target)


def compile_pipeline_text(
    code: str,
    *,
    yaml_text: str | None = None,
    target: str | None = None,
) -> CompiledPipeline:
    return compile_pipeline_code(code, yaml_text=yaml_text, target=target)


def has_compile_errors(compiled: CompiledPipeline) -> bool:
    return any(str(diag.get("level") or "").lower() == "error" for diag in compiled.diagnostics)


def execute_compiled_pipeline(
    compiled: CompiledPipeline,
    *,
    runtime: PipelineRuntime,
    target_node: str | None = None,
    selected_node_names: set[str] | None = None,
) -> list[str]:
    if compiled.spec is None:
        raise RuntimeError("Compiled pipeline is missing a spec.")
    if target_node and selected_node_names is not None:
        raise RuntimeError("Use either target_node or selected_node_names, not both.")
    if selected_node_names is not None:
        return execute_selected_nodes(compiled.spec, runtime=runtime, selected_node_names=set(selected_node_names))
    return execute_pipeline(compiled.spec, runtime=runtime, target_node=target_node)


def resume_compiled_pipeline(
    compiled: CompiledPipeline,
    *,
    runtime: PipelineRuntime,
    on_log: Callable[[PipelineLogEvent], None] | None = None,
) -> RunPipelineResult:
    if compiled.spec is None:
        raise RuntimeError("Compiled pipeline is missing a spec.")
    active_on_log = on_log or getattr(runtime, "_on_log", None)
    latest_run, node_runs, active_states = _current_failed_run_context(runtime)
    runtime.attach_run_context(run_id=str(latest_run.get("run_id") or ""), node_runs=node_runs)
    runtime._pipeline_started_at = str(latest_run.get("started_at") or "").strip() or None
    selected_nodes = _resume_selected_nodes(compiled.spec, node_runs=node_runs, active_states=active_states)
    if not selected_nodes:
        raise RuntimeError("The current failed run does not have any nodes ready to resume.")
    _emit_log_event(
        active_on_log,
        code=LogCode.PIPELINE_TARGET_SELECTED,
        message=f"Resuming run '{runtime.run_id}' from {len(selected_nodes)} node(s).",
        details={"run_id": runtime.run_id, "selected_nodes": sorted(selected_nodes)},
    )
    runtime.clear_selected_outputs(selected_nodes)
    _emit_log_event(
        active_on_log,
        code=LogCode.PIPELINE_EXECUTION_STARTED,
        message="Executing resumed pipeline segment.",
        details={"run_id": runtime.run_id, "selected_nodes": sorted(selected_nodes)},
    )
    try:
        executed = execute_compiled_pipeline(compiled, runtime=runtime, selected_node_names=selected_nodes)
    except Exception as exc:
        _emit_log_event(
            active_on_log,
            code=LogCode.PIPELINE_EXECUTION_FAILED,
            message=f"Execution failed: {exc}",
            severity="error",
            details={"exception_type": type(exc).__name__},
            run_id=getattr(runtime, "run_id", None),
        )
        raise
    _emit_log_event(
        active_on_log,
        code=LogCode.PIPELINE_EXECUTION_FINISHED,
        message=f"Finished successfully. Executed {len(executed)} node(s).",
        details={"executed_nodes": list(executed)},
        run_id=getattr(runtime, "run_id", None),
    )
    return RunPipelineResult(
        compiled=compiled,
        executed_nodes=executed,
        artifact_path=str(getattr(runtime, "duckdb_path", "")),
        run_id=getattr(runtime, "run_id", None),
    )


def reset_compiled_node(
    compiled: CompiledPipeline,
    *,
    runtime: PipelineRuntime,
    node_name: str,
) -> ResetPipelineResult:
    if compiled.spec is None:
        raise RuntimeError("Compiled pipeline is missing a spec.")
    nodes = compiled.spec.node_by_name()
    if node_name not in nodes:
        raise RuntimeError(f"Node '{node_name}' was not found in the compiled pipeline.")
    reset_tables = runtime.clear_selected_outputs({node_name})
    if getattr(runtime, "_node_run_ids", None):
        runtime.reset_selected_node_states({node_name}, trigger="reset_node")
    return ResetPipelineResult(
        compiled=compiled,
        artifact_path=str(getattr(runtime, "duckdb_path", "")),
        reset_nodes=[node_name],
        reset_tables=reset_tables,
    )


def reset_compiled_downstream(
    compiled: CompiledPipeline,
    *,
    runtime: PipelineRuntime,
    node_name: str,
) -> ResetPipelineResult:
    if compiled.spec is None:
        raise RuntimeError("Compiled pipeline is missing a spec.")
    nodes = compiled.spec.node_by_name()
    if node_name not in nodes:
        raise RuntimeError(f"Node '{node_name}' was not found in the compiled pipeline.")
    selected_nodes = _select_downstream_nodes(compiled.spec, node_name)
    reset_tables = runtime.clear_selected_outputs(selected_nodes)
    if getattr(runtime, "_node_run_ids", None):
        runtime.reset_selected_node_states(selected_nodes, trigger="reset_downstream")
    return ResetPipelineResult(
        compiled=compiled,
        artifact_path=str(getattr(runtime, "duckdb_path", "")),
        reset_nodes=sorted(selected_nodes),
        reset_tables=reset_tables,
    )


def reset_compiled_upstream(
    compiled: CompiledPipeline,
    *,
    runtime: PipelineRuntime,
    node_name: str,
) -> ResetPipelineResult:
    if compiled.spec is None:
        raise RuntimeError("Compiled pipeline is missing a spec.")
    nodes = compiled.spec.node_by_name()
    if node_name not in nodes:
        raise RuntimeError(f"Node '{node_name}' was not found in the compiled pipeline.")
    selected_nodes = _select_upstream_nodes(compiled.spec, node_name)
    reset_tables = runtime.clear_selected_outputs(selected_nodes)
    if getattr(runtime, "_node_run_ids", None):
        runtime.reset_selected_node_states(selected_nodes, trigger="reset_upstream")
    return ResetPipelineResult(
        compiled=compiled,
        artifact_path=str(getattr(runtime, "duckdb_path", "")),
        reset_nodes=sorted(selected_nodes),
        reset_tables=reset_tables,
    )


def reset_compiled_all(
    compiled: CompiledPipeline,
    *,
    runtime: PipelineRuntime,
) -> ResetPipelineResult:
    if compiled.spec is None:
        raise RuntimeError("Compiled pipeline is missing a spec.")
    selected_nodes = {node.name for node in compiled.spec.nodes}
    reset_tables = runtime.clear_selected_outputs(selected_nodes)
    if getattr(runtime, "_node_run_ids", None):
        runtime.reset_selected_node_states(selected_nodes, trigger="reset_all")
    return ResetPipelineResult(
        compiled=compiled,
        artifact_path=str(getattr(runtime, "duckdb_path", "")),
        reset_nodes=[node.name for node in compiled.spec.nodes],
        reset_tables=reset_tables,
    )


def list_existing_compiled_outputs(
    compiled: CompiledPipeline,
    *,
    runtime: PipelineRuntime,
    selected_node_names: set[str] | None = None,
) -> list[str]:
    if compiled.spec is None:
        raise RuntimeError("Compiled pipeline is missing a spec.")
    return runtime.existing_output_tables(
        selected_node_names if selected_node_names is not None else {node.name for node in compiled.spec.nodes}
    )


def _resolve_artifact_path(pipeline_path: Path, artifact_path: str | Path | None) -> Path:
    resolved = (
        Path(artifact_path).expanduser().resolve()
        if artifact_path is not None
        else _default_artifact_path(pipeline_path).resolve()
    )
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


def _resolve_connections_path(pipeline_path: Path, connections_path: str | Path | None) -> str | None:
    if connections_path is not None:
        return str(Path(connections_path).expanduser().resolve())
    default_connections = pipeline_path.parent / "connections.yaml"
    if default_connections.exists() and default_connections.is_file():
        return str(default_connections.resolve())
    return None


def _build_runtime_for_pipeline(
    *,
    pipeline_path: Path,
    compiled: CompiledPipeline,
    artifact_path: str | Path | None,
    pipeline_id: str | None,
    runtime_bindings: dict[str, Any] | None,
    connections_path: str | Path | None,
    on_log: Callable[[PipelineLogEvent], None] | None,
) -> tuple[PipelineRuntime, Path]:
    if compiled.spec is None:
        raise RuntimeError("Compiled pipeline is missing a spec.")
    resolved_artifact_path = _resolve_artifact_path(pipeline_path, artifact_path)
    runtime = PipelineRuntime(
        pipeline_id=str(pipeline_id or _default_pipeline_id(pipeline_path)),
        duckdb_path=str(resolved_artifact_path),
        working_dir=str(pipeline_path.parent.resolve()),
        spec=compiled.spec,
        module_globals=compiled.module_globals,
        runtime_bindings=runtime_bindings,
        connections_path=_resolve_connections_path(pipeline_path, connections_path),
        on_log=on_log,
    )
    return runtime, resolved_artifact_path


def _target_tables_for_nodes(spec: PipelineSpec, node_names: set[str]) -> list[str]:
    nodes = spec.node_by_name()
    seen: set[str] = set()
    target_tables: list[str] = []
    for node_name in node_names:
        node = nodes.get(node_name)
        if node is None:
            continue
        target_table = str(node.target_table or "").strip()
        if not target_table or target_table in seen:
            continue
        seen.add(target_table)
        target_tables.append(target_table)
    return target_tables


def _current_failed_run_context(runtime: PipelineRuntime) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    import duckdb_core

    latest_run = duckdb_core.get_latest_pipeline_run(
        connection_id=runtime._ensure_duckdb_connection_id(),
        status=None,
    )
    if not latest_run:
        raise RuntimeError("No pipeline run was found to resume or reset.")
    if str(latest_run.get("status") or "").strip().lower() != "failed":
        raise RuntimeError("The latest pipeline run is not failed, so there is nothing to resume or reset.")

    run_id = str(latest_run.get("run_id") or "").strip()
    if not run_id:
        raise RuntimeError("Latest failed pipeline run is missing a run_id.")

    node_runs = duckdb_core.get_node_runs_for_run(
        connection_id=runtime._ensure_duckdb_connection_id(),
        run_id=run_id,
    )
    active_states = duckdb_core.get_active_node_states_for_run(
        connection_id=runtime._ensure_duckdb_connection_id(),
        run_id=run_id,
    )
    return latest_run, node_runs, active_states


def _resume_selected_nodes(
    spec: PipelineSpec,
    *,
    node_runs: list[dict[str, Any]],
    active_states: list[dict[str, Any]],
) -> set[str]:
    active_state_by_name = {
        str(item.get("node_name") or "").strip(): str(item.get("state") or "").strip().lower()
        for item in active_states
        if str(item.get("node_name") or "").strip()
    }
    if active_state_by_name:
        return {
            node.name
            for node in spec.nodes
            if active_state_by_name.get(node.name, "ready") in {"ready", "running", "failed"}
        }

    fallback_status_by_name = {
        str(item.get("node_name") or "").strip(): str(item.get("status") or "").strip().lower()
        for item in node_runs
        if str(item.get("node_name") or "").strip()
    }
    return {
        node.name
        for node in spec.nodes
        if fallback_status_by_name.get(node.name, "ready") in {"ready", "pending", "running", "failed", "skipped"}
    }


def run_pipeline_file(
    pipeline_path: str | Path,
    *,
    config_path: str | Path | None = None,
    connections_path: str | Path | None = None,
    runtime_bindings: dict[str, Any] | None = None,
    target: str | None = None,
    artifact_path: str | Path | None = None,
    target_node: str | None = None,
    clean_existing: bool = False,
    pipeline_id: str | None = None,
    on_log: Callable[[PipelineLogEvent], None] | None = None,
) -> RunPipelineResult:
    resolved_pipeline_path = Path(pipeline_path).expanduser().resolve()
    _emit_log_event(
        on_log,
        code=LogCode.PIPELINE_RUN_STARTED,
        message=f"Starting pipeline run for {resolved_pipeline_path.name}.",
        details={"pipeline_path": str(resolved_pipeline_path)},
    )
    _emit_log_event(
        on_log,
        code=LogCode.PIPELINE_COMPILE_STARTED,
        message="Compiling pipeline file...",
    )
    compiled = compile_pipeline_file(resolved_pipeline_path, config_path=config_path, target=target)
    if has_compile_errors(compiled) or compiled.spec is None:
        _emit_log_event(
            on_log,
            code=LogCode.PIPELINE_COMPILE_FAILED,
            message="Pipeline compile failed.",
            severity="error",
            details={"diagnostic_count": len(compiled.diagnostics)},
        )
        return RunPipelineResult(
            compiled=compiled,
            executed_nodes=[],
            artifact_path=str(
                Path(artifact_path).expanduser().resolve()
                if artifact_path is not None
                else _default_artifact_path(resolved_pipeline_path)
            ),
            run_id=None,
        )
    _emit_log_event(
        on_log,
        code=LogCode.PIPELINE_COMPILE_SUCCEEDED,
        message=f"Compile succeeded with {len(compiled.spec.nodes)} node(s).",
        details={"node_count": len(compiled.spec.nodes)},
    )
    runtime, resolved_artifact_path = _build_runtime_for_pipeline(
        pipeline_path=resolved_pipeline_path,
        compiled=compiled,
        artifact_path=artifact_path,
        pipeline_id=pipeline_id,
        runtime_bindings=runtime_bindings,
        connections_path=connections_path,
        on_log=on_log,
    )
    if clean_existing:
        _emit_log_event(
            on_log,
            code=LogCode.PIPELINE_CLEAN_STARTED,
            message="Cleaning existing pipeline outputs before execution...",
        )
        runtime.clear_pipeline_outputs()
    if target_node:
        _emit_log_event(
            on_log,
            code=LogCode.PIPELINE_TARGET_SELECTED,
            message=f"Executing target node '{target_node}' with dependencies.",
            details={"target_node": target_node},
        )
    _emit_log_event(
        on_log,
        code=LogCode.PIPELINE_EXECUTION_STARTED,
        message="Executing pipeline DAG.",
        details={"target_node": target_node, "clean_existing": bool(clean_existing)},
    )
    try:
        executed = execute_compiled_pipeline(compiled, runtime=runtime, target_node=target_node)
    except Exception as exc:
        _emit_log_event(
            on_log,
            code=LogCode.PIPELINE_EXECUTION_FAILED,
            message=f"Execution failed: {exc}",
            severity="error",
            details={"exception_type": type(exc).__name__},
            run_id=getattr(runtime, "run_id", None),
        )
        raise
    _emit_log_event(
        on_log,
        code=LogCode.PIPELINE_EXECUTION_FINISHED,
        message=f"Finished successfully. Executed {len(executed)} node(s).",
        details={"executed_nodes": list(executed)},
        run_id=runtime.run_id,
    )
    return RunPipelineResult(
        compiled=compiled,
        executed_nodes=executed,
        artifact_path=str(resolved_artifact_path),
        run_id=runtime.run_id,
    )


def resume_pipeline_file(
    pipeline_path: str | Path,
    *,
    config_path: str | Path | None = None,
    connections_path: str | Path | None = None,
    runtime_bindings: dict[str, Any] | None = None,
    target: str | None = None,
    artifact_path: str | Path | None = None,
    pipeline_id: str | None = None,
    on_log: Callable[[PipelineLogEvent], None] | None = None,
) -> RunPipelineResult:
    resolved_pipeline_path = Path(pipeline_path).expanduser().resolve()
    _emit_log_event(
        on_log,
        code=LogCode.PIPELINE_RUN_STARTED,
        message=f"Resuming pipeline run for {resolved_pipeline_path.name}.",
        details={"pipeline_path": str(resolved_pipeline_path)},
    )
    _emit_log_event(
        on_log,
        code=LogCode.PIPELINE_COMPILE_STARTED,
        message="Compiling pipeline file...",
    )
    compiled = compile_pipeline_file(resolved_pipeline_path, config_path=config_path, target=target)
    if has_compile_errors(compiled) or compiled.spec is None:
        _emit_log_event(
            on_log,
            code=LogCode.PIPELINE_COMPILE_FAILED,
            message="Pipeline compile failed.",
            severity="error",
            details={"diagnostic_count": len(compiled.diagnostics)},
        )
        return RunPipelineResult(
            compiled=compiled,
            executed_nodes=[],
            artifact_path=str(_resolve_artifact_path(resolved_pipeline_path, artifact_path)),
            run_id=None,
        )
    _emit_log_event(
        on_log,
        code=LogCode.PIPELINE_COMPILE_SUCCEEDED,
        message=f"Compile succeeded with {len(compiled.spec.nodes)} node(s).",
        details={"node_count": len(compiled.spec.nodes)},
    )

    runtime, resolved_artifact_path = _build_runtime_for_pipeline(
        pipeline_path=resolved_pipeline_path,
        compiled=compiled,
        artifact_path=artifact_path,
        pipeline_id=pipeline_id,
        runtime_bindings=runtime_bindings,
        connections_path=connections_path,
        on_log=on_log,
    )
    result = resume_compiled_pipeline(compiled, runtime=runtime, on_log=on_log)
    return RunPipelineResult(
        compiled=result.compiled,
        executed_nodes=result.executed_nodes,
        artifact_path=str(resolved_artifact_path),
        run_id=result.run_id,
    )


def reset_node_file(
    pipeline_path: str | Path,
    *,
    node_name: str,
    config_path: str | Path | None = None,
    target: str | None = None,
    artifact_path: str | Path | None = None,
) -> ResetPipelineResult:
    resolved_pipeline_path = Path(pipeline_path).expanduser().resolve()
    compiled = compile_pipeline_file(resolved_pipeline_path, config_path=config_path, target=target)
    resolved_artifact_path = _resolve_artifact_path(resolved_pipeline_path, artifact_path)
    if has_compile_errors(compiled) or compiled.spec is None:
        return ResetPipelineResult(compiled=compiled, artifact_path=str(resolved_artifact_path), reset_nodes=[], reset_tables=[])

    runtime, _ = _build_runtime_for_pipeline(
        pipeline_path=resolved_pipeline_path,
        compiled=compiled,
        artifact_path=resolved_artifact_path,
        pipeline_id=None,
        runtime_bindings=None,
        connections_path=None,
        on_log=None,
    )
    try:
        latest_run, node_runs, _active_states = _current_failed_run_context(runtime)
        runtime.attach_run_context(run_id=str(latest_run.get("run_id") or ""), node_runs=node_runs)
    except Exception:
        pass
    result = reset_compiled_node(compiled, runtime=runtime, node_name=node_name)
    return ResetPipelineResult(
        compiled=result.compiled,
        artifact_path=str(resolved_artifact_path),
        reset_nodes=result.reset_nodes,
        reset_tables=result.reset_tables,
    )


def reset_downstream_file(
    pipeline_path: str | Path,
    *,
    node_name: str,
    config_path: str | Path | None = None,
    target: str | None = None,
    artifact_path: str | Path | None = None,
) -> ResetPipelineResult:
    resolved_pipeline_path = Path(pipeline_path).expanduser().resolve()
    compiled = compile_pipeline_file(resolved_pipeline_path, config_path=config_path, target=target)
    resolved_artifact_path = _resolve_artifact_path(resolved_pipeline_path, artifact_path)
    if has_compile_errors(compiled) or compiled.spec is None:
        return ResetPipelineResult(compiled=compiled, artifact_path=str(resolved_artifact_path), reset_nodes=[], reset_tables=[])

    runtime, _ = _build_runtime_for_pipeline(
        pipeline_path=resolved_pipeline_path,
        compiled=compiled,
        artifact_path=resolved_artifact_path,
        pipeline_id=None,
        runtime_bindings=None,
        connections_path=None,
        on_log=None,
    )
    try:
        latest_run, node_runs, _active_states = _current_failed_run_context(runtime)
        runtime.attach_run_context(run_id=str(latest_run.get("run_id") or ""), node_runs=node_runs)
    except Exception:
        pass
    result = reset_compiled_downstream(compiled, runtime=runtime, node_name=node_name)
    return ResetPipelineResult(
        compiled=result.compiled,
        artifact_path=str(resolved_artifact_path),
        reset_nodes=result.reset_nodes,
        reset_tables=result.reset_tables,
    )


def reset_upstream_file(
    pipeline_path: str | Path,
    *,
    node_name: str,
    config_path: str | Path | None = None,
    target: str | None = None,
    artifact_path: str | Path | None = None,
) -> ResetPipelineResult:
    resolved_pipeline_path = Path(pipeline_path).expanduser().resolve()
    compiled = compile_pipeline_file(resolved_pipeline_path, config_path=config_path, target=target)
    resolved_artifact_path = _resolve_artifact_path(resolved_pipeline_path, artifact_path)
    if has_compile_errors(compiled) or compiled.spec is None:
        return ResetPipelineResult(compiled=compiled, artifact_path=str(resolved_artifact_path), reset_nodes=[], reset_tables=[])

    runtime, _ = _build_runtime_for_pipeline(
        pipeline_path=resolved_pipeline_path,
        compiled=compiled,
        artifact_path=resolved_artifact_path,
        pipeline_id=None,
        runtime_bindings=None,
        connections_path=None,
        on_log=None,
    )
    try:
        latest_run, node_runs, _active_states = _current_failed_run_context(runtime)
        runtime.attach_run_context(run_id=str(latest_run.get("run_id") or ""), node_runs=node_runs)
    except Exception:
        pass
    result = reset_compiled_upstream(compiled, runtime=runtime, node_name=node_name)
    return ResetPipelineResult(
        compiled=result.compiled,
        artifact_path=str(resolved_artifact_path),
        reset_nodes=result.reset_nodes,
        reset_tables=result.reset_tables,
    )


def reset_all_file(
    pipeline_path: str | Path,
    *,
    config_path: str | Path | None = None,
    target: str | None = None,
    artifact_path: str | Path | None = None,
) -> ResetPipelineResult:
    resolved_pipeline_path = Path(pipeline_path).expanduser().resolve()
    compiled = compile_pipeline_file(resolved_pipeline_path, config_path=config_path, target=target)
    resolved_artifact_path = _resolve_artifact_path(resolved_pipeline_path, artifact_path)
    if has_compile_errors(compiled) or compiled.spec is None:
        return ResetPipelineResult(compiled=compiled, artifact_path=str(resolved_artifact_path), reset_nodes=[], reset_tables=[])

    runtime, _ = _build_runtime_for_pipeline(
        pipeline_path=resolved_pipeline_path,
        compiled=compiled,
        artifact_path=resolved_artifact_path,
        pipeline_id=None,
        runtime_bindings=None,
        connections_path=None,
        on_log=None,
    )
    try:
        latest_run, node_runs, _active_states = _current_failed_run_context(runtime)
        runtime.attach_run_context(run_id=str(latest_run.get("run_id") or ""), node_runs=node_runs)
    except Exception:
        pass
    result = reset_compiled_all(compiled, runtime=runtime)
    return ResetPipelineResult(
        compiled=result.compiled,
        artifact_path=str(resolved_artifact_path),
        reset_nodes=result.reset_nodes,
        reset_tables=result.reset_tables,
    )


def list_existing_outputs_for_file(
    pipeline_path: str | Path,
    *,
    config_path: str | Path | None = None,
    connections_path: str | Path | None = None,
    runtime_bindings: dict[str, Any] | None = None,
    target: str | None = None,
    artifact_path: str | Path | None = None,
    pipeline_id: str | None = None,
) -> tuple[CompiledPipeline, list[str], str]:
    resolved_pipeline_path = Path(pipeline_path).expanduser().resolve()
    compiled = compile_pipeline_file(resolved_pipeline_path, config_path=config_path, target=target)
    resolved_artifact_path = _resolve_artifact_path(resolved_pipeline_path, artifact_path)
    if has_compile_errors(compiled) or compiled.spec is None:
        return compiled, [], str(resolved_artifact_path)

    runtime, _ = _build_runtime_for_pipeline(
        pipeline_path=resolved_pipeline_path,
        compiled=compiled,
        artifact_path=resolved_artifact_path,
        pipeline_id=pipeline_id,
        runtime_bindings=runtime_bindings,
        connections_path=connections_path,
        on_log=None,
    )
    existing_outputs = list_existing_compiled_outputs(compiled, runtime=runtime)
    return compiled, existing_outputs, str(resolved_artifact_path)
