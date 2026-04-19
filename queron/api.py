from __future__ import annotations

from collections import deque
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable
import uuid

from .compiler import CompiledPipeline, compile_pipeline_code
from .executor import _select_downstream_nodes, _select_upstream_nodes, execute_pipeline, execute_selected_nodes
from .runtime import PipelineRuntime
from .runtime_models import (
    LogCode,
    NodeRunRecord,
    NodeStateRecord,
    PipelineLogEvent,
    PipelineRunRecord,
    build_log_event,
    normalize_log_event,
    utc_now_timestamp,
)
from .specs import PipelineSpec
from .templates import build_init_config_text, build_init_gitignore_text, build_init_pipeline_text

@dataclass
class RunPipelineResult:
    compiled: CompiledPipeline
    executed_nodes: list[str]
    artifact_path: str
    run_id: str | None = None
    run_label: str | None = None
    log_path: str | None = None


@dataclass
class ResetPipelineResult:
    compiled: CompiledPipeline
    artifact_path: str
    reset_nodes: list[str]
    reset_tables: list[str]


@dataclass
class StopPipelineResult:
    artifact_path: str
    run_id: str | None = None
    run_label: str | None = None
    stop_requested: bool = False
    stop_mode: str = "graceful"
    request_path: str | None = None
    message: str = ""


@dataclass
class InitPipelineProjectResult:
    project_path: str
    written_files: list[str]
    created_directories: list[str]
    sample: bool = False


@dataclass
class InspectDagResult:
    pipeline_path: str
    artifact_path: str
    pipeline_id: str | None = None
    compile_id: str | None = None
    run_id: str | None = None
    run_label: str | None = None
    run_status: str | None = None
    is_final: bool = False
    nodes: list[dict[str, Any]] = field(default_factory=list)
    edges: list[list[str]] = field(default_factory=list)


@dataclass
class InspectNodeResult:
    pipeline_path: str
    artifact_path: str
    compile_id: str | None = None
    run_id: str | None = None
    run_label: str | None = None
    run_status: str | None = None
    is_final: bool = False
    selection: str = "node"
    requested_node: str | None = None
    nodes: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class InspectNodeHistoryResult:
    pipeline_path: str
    artifact_path: str
    compile_id: str | None = None
    run_id: str | None = None
    run_label: str | None = None
    run_status: str | None = None
    is_final: bool = False
    node_name: str | None = None
    node_kind: str | None = None
    node_run_id: str | None = None
    node_run_status: str | None = None
    logical_artifact: str | None = None
    artifact_name: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    error_message: str | None = None
    states: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class InspectNodeLogResult:
    pipeline_path: str
    artifact_path: str
    compile_id: str | None = None
    run_id: str | None = None
    run_label: str | None = None
    run_status: str | None = None
    is_final: bool = False
    node_name: str | None = None
    node_kind: str | None = None
    logs: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class InspectNodeQueryResult:
    pipeline_path: str
    artifact_path: str
    pipeline_id: str | None = None
    compile_id: str | None = None
    run_id: str | None = None
    run_label: str | None = None
    run_status: str | None = None
    is_final: bool = False
    node_name: str | None = None
    node_kind: str | None = None
    logical_artifact: str | None = None
    sql: str | None = None
    resolved_sql: str | None = None
    dependencies: list[str] = field(default_factory=list)


_ACTIVE_RUNTIMES_BY_RUN_ID: dict[str, PipelineRuntime] = {}


@dataclass
class ReconcileOrphanedRunResult:
    artifact_path: str
    reconciled_run_ids: list[str] = field(default_factory=list)
    reconciled_node_names_by_run_id: dict[str, list[str]] = field(default_factory=dict)


def _register_active_runtime(runtime: PipelineRuntime) -> None:
    run_id = str(getattr(runtime, "run_id", "") or "").strip()
    if run_id:
        _ACTIVE_RUNTIMES_BY_RUN_ID[run_id] = runtime


def _unregister_active_runtime(runtime_or_run_id: PipelineRuntime | str | None) -> None:
    if isinstance(runtime_or_run_id, PipelineRuntime):
        run_id = str(getattr(runtime_or_run_id, "run_id", "") or "").strip()
    else:
        run_id = str(runtime_or_run_id or "").strip()
    if run_id:
        _ACTIVE_RUNTIMES_BY_RUN_ID.pop(run_id, None)


def _is_run_active_in_registry(run_id: str | None) -> bool:
    normalized_run_id = str(run_id or "").strip()
    return bool(normalized_run_id) and normalized_run_id in _ACTIVE_RUNTIMES_BY_RUN_ID


def _is_run_final(run: dict[str, Any] | None) -> bool:
    if not isinstance(run, dict):
        return False
    return bool(run.get("is_final"))


def _set_run_final_if_allowed(
    *,
    artifact_path: str | Path,
    run: dict[str, Any] | None,
) -> bool:
    import duckdb_core

    if not isinstance(run, dict):
        return False
    if _is_run_final(run):
        return False
    if str(run.get("status") or "").strip().lower() != "failed":
        return False
    run_id = str(run.get("run_id") or "").strip()
    pipeline_id = str(run.get("pipeline_id") or "").strip()
    if not run_id or not pipeline_id:
        return False

    resolved_artifact_path = str(Path(artifact_path).expanduser().resolve())
    connection_id = duckdb_core.connect(duckdb_core.DuckDbConnectRequest(database=resolved_artifact_path)).connection_id
    duckdb_core.record_pipeline_run(
        connection_id=connection_id,
        record=PipelineRunRecord(
            run_id=run_id,
            run_label=str(run.get("run_label") or "").strip() or None,
            log_path=str(run.get("log_path") or "").strip() or None,
            compile_id=str(run.get("compile_id") or "").strip() or None,
            pipeline_id=pipeline_id,
            target=str(run.get("target") or "").strip() or None,
            artifact_path=str(run.get("artifact_path") or "").strip() or resolved_artifact_path,
            started_at=str(run.get("started_at") or "").strip() or None,
            finished_at=str(run.get("finished_at") or "").strip() or None,
            status="failed",
            error_message=str(run.get("error_message") or "").strip() or None,
            is_final=True,
        ),
    )
    run["is_final"] = True
    return True


def _reconcile_orphaned_running_runs(
    *,
    artifact_path: str | Path,
    run_id: str | None = None,
    error_message: str = "Pipeline process exited unexpectedly during execution.",
) -> ReconcileOrphanedRunResult:
    import duckdb_core

    resolved_artifact_path = str(Path(artifact_path).expanduser().resolve())
    result = ReconcileOrphanedRunResult(artifact_path=resolved_artifact_path)
    normalized_run_id = str(run_id or "").strip()
    running_runs = [
        item
        for item in duckdb_core.list_pipeline_runs(database_path=resolved_artifact_path)
        if str(item.get("status") or "").strip().lower() == "running"
        and (not normalized_run_id or str(item.get("run_id") or "").strip() == normalized_run_id)
    ]
    orphaned_runs = [
        item
        for item in running_runs
        if str(item.get("run_id") or "").strip() and not _is_run_active_in_registry(str(item.get("run_id") or "").strip())
    ]
    if not orphaned_runs:
        return result

    connection_id = duckdb_core.connect(duckdb_core.DuckDbConnectRequest(database=resolved_artifact_path)).connection_id
    failed_at = utc_now_timestamp()
    for run in orphaned_runs:
        run_id = str(run.get("run_id") or "").strip()
        if not run_id:
            continue
        node_runs = duckdb_core.get_node_runs_for_run_by_database(database_path=resolved_artifact_path, run_id=run_id)
        active_states = duckdb_core.get_active_node_states_for_run_by_database(database_path=resolved_artifact_path, run_id=run_id)
        active_state_by_node_name = {
            str(item.get("node_name") or "").strip(): item
            for item in active_states
            if str(item.get("node_name") or "").strip()
        }
        running_node_records: list[NodeRunRecord] = []
        failed_state_records: list[NodeStateRecord] = []
        reconciled_node_names: list[str] = []
        for item in node_runs:
            node_name = str(item.get("node_name") or "").strip()
            if not node_name or str(item.get("status") or "").strip().lower() != "running":
                continue
            reconciled_node_names.append(node_name)
            failed_details = {
                "exception_type": "OrphanedRunRecovered",
                "recovery_reason": "process_missing",
            }
            active_state_id = f"{uuid.uuid4().hex}"
            failed_state_records.append(
                NodeStateRecord(
                    node_state_id=active_state_id,
                    run_id=run_id,
                    node_run_id=str(item.get("node_run_id") or "").strip(),
                    node_name=node_name,
                    state="failed",
                    is_active=True,
                    created_at=failed_at,
                    trigger="orphaned_run_reconciled",
                    details_json=failed_details,
                )
            )
            running_node_records.append(
                NodeRunRecord(
                    node_run_id=str(item.get("node_run_id") or "").strip(),
                    run_id=run_id,
                    node_name=node_name,
                    node_kind=str(item.get("node_kind") or "").strip() or "unknown",
                    artifact_name=str(item.get("artifact_name") or "").strip() or None,
                    started_at=str(item.get("started_at") or "").strip() or None,
                    finished_at=failed_at,
                    status="failed",
                    row_count_in=item.get("row_count_in"),
                    row_count_out=item.get("row_count_out"),
                    artifact_size_bytes=item.get("artifact_size_bytes"),
                    error_message=error_message,
                    warnings_json=[],
                    details_json=failed_details,
                    active_node_state_id=active_state_id,
                )
            )
        if failed_state_records:
            duckdb_core.record_node_states(connection_id=connection_id, records=failed_state_records)
        if running_node_records:
            duckdb_core.record_node_runs(connection_id=connection_id, records=running_node_records)
        duckdb_core.record_pipeline_run(
            connection_id=connection_id,
            record=PipelineRunRecord(
                run_id=run_id,
                run_label=str(run.get("run_label") or "").strip() or None,
                log_path=str(run.get("log_path") or "").strip() or None,
                compile_id=str(run.get("compile_id") or "").strip() or None,
                pipeline_id=str(run.get("pipeline_id") or "").strip(),
                target=str(run.get("target") or "").strip() or None,
                artifact_path=str(run.get("artifact_path") or "").strip() or resolved_artifact_path,
                started_at=str(run.get("started_at") or "").strip() or None,
                finished_at=failed_at,
                status="failed",
                error_message=error_message,
                is_final=False,
            ),
        )
        result.reconciled_run_ids.append(run_id)
        if reconciled_node_names:
            result.reconciled_node_names_by_run_id[run_id] = reconciled_node_names
    return result


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


def _resolve_config_input(
    pipeline_path: Path,
    config_path: str | Path | None,
) -> tuple[Path | None, str | None]:
    if config_path is not None:
        resolved = Path(config_path).expanduser().resolve()
        return resolved, _read_text_file(resolved, label="Configuration file", required=True)
    default_config = pipeline_path.parent / "configurations.yaml"
    if default_config.exists() and default_config.is_file():
        return default_config.resolve(), _read_text_file(default_config, label="Configuration file")
    return None, None


def init_pipeline_project(
    project_path: str | Path,
    *,
    sample: bool = False,
    force: bool = False,
) -> InitPipelineProjectResult:
    resolved_project_path = Path(project_path).expanduser().resolve()
    if resolved_project_path.exists() and not resolved_project_path.is_dir():
        raise RuntimeError(f"Project path '{resolved_project_path}' is not a directory.")

    existing_entries = list(resolved_project_path.iterdir()) if resolved_project_path.exists() else []
    if existing_entries and not force:
        raise RuntimeError(
            f"Project directory '{resolved_project_path}' is not empty. Re-run with force=True to overwrite scaffold files."
        )

    resolved_project_path.mkdir(parents=True, exist_ok=True)

    created_directories: list[str] = []
    for directory in (
        resolved_project_path / "local_files",
        resolved_project_path / "exports",
    ):
        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)
            created_directories.append(str(directory))
        elif not directory.is_dir():
            raise RuntimeError(f"Required scaffold directory '{directory}' already exists and is not a directory.")

    written_files: list[str] = []
    scaffold_files = {
        resolved_project_path / "pipeline.py": build_init_pipeline_text(sample=sample),
        resolved_project_path / "configurations.yaml": build_init_config_text(),
        resolved_project_path / ".gitignore": build_init_gitignore_text(),
    }
    for file_path, contents in scaffold_files.items():
        if file_path.exists() and file_path.is_dir():
            raise RuntimeError(f"Required scaffold file '{file_path}' already exists as a directory.")
        if file_path.exists() and not force and existing_entries:
            raise RuntimeError(
                f"Scaffold file '{file_path}' already exists. Re-run with force=True to overwrite scaffold files."
            )
        file_path.write_text(contents, encoding="utf-8")
        written_files.append(str(file_path))

    return InitPipelineProjectResult(
        project_path=str(resolved_project_path),
        written_files=written_files,
        created_directories=created_directories,
        sample=bool(sample),
    )


def _fallback_artifact_path_for_diagnostics(pipeline_path: Path) -> Path:
    return pipeline_path.parent / ".queron" / f"{pipeline_path.stem}.duckdb"


def _default_artifact_path(pipeline_path: Path, *, pipeline_id: str) -> Path:
    normalized_pipeline_id = str(pipeline_id or "").strip()
    if not normalized_pipeline_id:
        raise RuntimeError("Pipeline is missing a required pipeline_id in __queron_native__.")
    return pipeline_path.parent / ".queron" / f"{normalized_pipeline_id}.duckdb"


def _normalize_run_label(run_label: str | None) -> str | None:
    text = str(run_label or "").strip()
    return text or None


def _normalize_stop_reason(reason: str | None) -> str:
    text = str(reason or "").strip()
    return text or "Pipeline stop requested by user."


def _normalize_force_stop_reason(reason: str | None) -> str:
    text = str(reason or "").strip()
    return text or "Force stopped by user."


def _selected_run_is_final(selected_run: dict[str, Any] | None) -> bool:
    return bool(selected_run.get("is_final")) if isinstance(selected_run, dict) else False


def _pipeline_failure_details(exc: Exception) -> dict[str, Any]:
    details = {"exception_type": type(exc).__name__}
    extra = getattr(exc, "queron_details", None)
    if isinstance(extra, dict):
        details.update({str(key): value for key, value in extra.items()})
    return details


def load_pipeline_code_from_file(path: str | Path) -> tuple[Path, str]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.exists() or not resolved.is_file():
        raise RuntimeError(f"Pipeline file '{resolved}' was not found.")
    return resolved, resolved.read_text(encoding="utf-8")


def _compile_pipeline_impl(
    pipeline_path: str | Path,
    *,
    config_path: str | Path | None = None,
    target: str | None = None,
    artifact_path: str | Path | None = None,
) -> CompiledPipeline:
    resolved_pipeline_path, code = load_pipeline_code_from_file(pipeline_path)
    resolved_config_path, yaml_text = _resolve_config_input(resolved_pipeline_path, config_path)
    preview_compiled = compile_pipeline_code(
        code,
        yaml_text=yaml_text,
        target=target,
        source_path=resolved_pipeline_path,
        artifact_path=None,
        config_path=resolved_config_path,
    )
    resolved_artifact_path = _resolve_artifact_path(
        resolved_pipeline_path,
        artifact_path,
        compiled=preview_compiled,
        config_path=resolved_config_path,
        target=target,
    )
    import duckdb_core

    latest_runs = duckdb_core.list_pipeline_runs(database_path=str(resolved_artifact_path), limit=1)
    latest_run = latest_runs[0] if latest_runs else None
    if latest_run is not None and not _is_run_final(latest_run):
        latest_status = str(latest_run.get("status") or "").strip().lower()
        if latest_status == "failed":
            _set_run_final_if_allowed(artifact_path=resolved_artifact_path, run=latest_run)
        elif latest_status == "running":
            latest_run_id = str(latest_run.get("run_id") or "").strip()
            if _is_run_active_in_registry(latest_run_id):
                raise RuntimeError(
                    f"Pipeline run '{latest_run_id}' is currently active and compile cannot proceed until it finishes."
                )
            _reconcile_orphaned_running_runs(artifact_path=resolved_artifact_path, run_id=latest_run_id)
            reconciled_run = duckdb_core.get_pipeline_run_by_id(
                database_path=str(resolved_artifact_path),
                run_id=latest_run_id,
            )
            _set_run_final_if_allowed(artifact_path=resolved_artifact_path, run=reconciled_run)
        else:
            raise RuntimeError(
                f"The latest pipeline run has unexpected non-final status '{latest_status or 'unknown'}'."
            )
    compiled = compile_pipeline_code(
        code,
        yaml_text=yaml_text,
        target=target,
        source_path=resolved_pipeline_path,
        artifact_path=resolved_artifact_path,
        config_path=resolved_config_path,
    )
    if has_compile_errors(compiled) or compiled.spec is None or compiled.contract is None:
        return compiled

    compiled.contract = duckdb_core.save_compiled_contract(
        database_path=str(resolved_artifact_path),
        record=compiled.contract,
    )
    return compiled


def compile_pipeline(
    pipeline_path: str | Path,
    *,
    config_path: str | Path | None = None,
    target: str | None = None,
) -> CompiledPipeline:
    return _compile_pipeline_impl(
        pipeline_path,
        config_path=config_path,
        target=target,
        artifact_path=None,
    )


def compile_pipeline_text(
    code: str,
    *,
    yaml_text: str | None = None,
    target: str | None = None,
) -> CompiledPipeline:
    return compile_pipeline_code(code, yaml_text=yaml_text, target=target)


def _compiled_with_runtime_diagnostic(
    compiled: CompiledPipeline,
    *,
    code: str,
    message: str,
) -> CompiledPipeline:
    return CompiledPipeline(
        spec=compiled.spec,
        diagnostics=[
            *compiled.diagnostics,
            {
                "level": "error",
                "code": code,
                "message": message,
            },
        ],
        module_globals=compiled.module_globals,
        contract=compiled.contract,
    )


def _validated_compiled_pipeline_for_file(
    pipeline_path: str | Path,
    *,
    config_path: str | Path | None = None,
    target: str | None = None,
    artifact_path: str | Path | None = None,
) -> tuple[CompiledPipeline, Path]:
    resolved_pipeline_path, code = load_pipeline_code_from_file(pipeline_path)
    resolved_config_path, yaml_text = _resolve_config_input(resolved_pipeline_path, config_path)
    preview_compiled = compile_pipeline_code(
        code,
        yaml_text=yaml_text,
        target=target,
        source_path=resolved_pipeline_path,
        artifact_path=None,
        config_path=resolved_config_path,
    )
    resolved_artifact_path = _resolve_artifact_path(
        resolved_pipeline_path,
        artifact_path,
        compiled=preview_compiled,
        config_path=resolved_config_path,
        target=target,
    )
    compiled = compile_pipeline_code(
        code,
        yaml_text=yaml_text,
        target=target,
        source_path=resolved_pipeline_path,
        artifact_path=resolved_artifact_path,
        config_path=resolved_config_path,
    )
    if has_compile_errors(compiled) or compiled.spec is None or compiled.contract is None:
        return compiled, resolved_artifact_path

    import duckdb_core

    stored_contract = duckdb_core.load_active_compiled_contract(database_path=str(resolved_artifact_path))
    if stored_contract is None:
        return (
            _compiled_with_runtime_diagnostic(
                compiled,
                code="compile_required",
                message="No compiled pipeline contract was found. Run queron compile first.",
            ),
            resolved_artifact_path,
        )
    if stored_contract.contract_hash != compiled.contract.contract_hash:
        return (
            _compiled_with_runtime_diagnostic(
                compiled,
                code="recompile_required",
                message="Pipeline project files, DAG, or configuration changed since the last compile. Re-run queron compile.",
            ),
            resolved_artifact_path,
        )
    compiled.contract = stored_contract
    return compiled, resolved_artifact_path


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
    latest_run, node_runs, active_states = _current_failed_run_context(
        runtime,
        expected_compile_id=getattr(runtime, "compile_id", None),
    )
    runtime.attach_run_context(
        run_id=str(latest_run.get("run_id") or ""),
        node_runs=node_runs,
        run_label=str(latest_run.get("run_label") or "").strip() or None,
    )
    runtime._pipeline_started_at = str(latest_run.get("started_at") or "").strip() or None
    selected_nodes = _resume_selected_nodes(compiled.spec, node_runs=node_runs, active_states=active_states)
    if not selected_nodes:
        raise RuntimeError("The current failed run does not have any nodes ready to resume.")
    runtime._log_event(
        code=LogCode.PIPELINE_TARGET_SELECTED,
        message=f"Resuming run '{runtime.run_id}' from {len(selected_nodes)} node(s).",
        details={"run_id": runtime.run_id, "selected_nodes": sorted(selected_nodes)},
    )
    runtime.clear_selected_outputs(selected_nodes)
    runtime._log_event(
        code=LogCode.PIPELINE_EXECUTION_STARTED,
        message="Executing resumed pipeline segment.",
        details={"run_id": runtime.run_id, "selected_nodes": sorted(selected_nodes)},
    )
    try:
        executed = execute_compiled_pipeline(compiled, runtime=runtime, selected_node_names=selected_nodes)
    except Exception as exc:
        runtime._log_event(
            code=LogCode.PIPELINE_EXECUTION_FAILED,
            message=f"Execution failed: {exc}",
            severity="error",
            details=_pipeline_failure_details(exc),
        )
        raise
    runtime._log_event(
        code=LogCode.PIPELINE_EXECUTION_FINISHED,
        message=f"Finished successfully. Executed {len(executed)} node(s).",
        details={"executed_nodes": list(executed)},
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


def _pipeline_id_for_artifact_resolution(
    pipeline_path: Path,
    *,
    compiled: CompiledPipeline | None = None,
    config_path: Path | None = None,
    target: str | None = None,
) -> str | None:
    pipeline_id = str(getattr(getattr(compiled, "spec", None), "pipeline_id", "") or "").strip() or None
    if pipeline_id is not None:
        return pipeline_id

    resolved_pipeline_path, code = load_pipeline_code_from_file(pipeline_path)
    yaml_path, yaml_text = _resolve_config_input(resolved_pipeline_path, config_path)
    preview_compiled = compile_pipeline_code(
        code,
        yaml_text=yaml_text,
        target=target,
        source_path=resolved_pipeline_path,
        artifact_path=None,
        config_path=yaml_path,
    )
    return str(getattr(getattr(preview_compiled, "spec", None), "pipeline_id", "") or "").strip() or None


def _resolve_artifact_path(
    pipeline_path: Path,
    artifact_path: str | Path | None,
    *,
    compiled: CompiledPipeline | None = None,
    config_path: Path | None = None,
    target: str | None = None,
) -> Path:
    if artifact_path is not None:
        resolved = Path(artifact_path).expanduser().resolve()
        resolved.parent.mkdir(parents=True, exist_ok=True)
        return resolved

    pipeline_id = _pipeline_id_for_artifact_resolution(
        pipeline_path,
        compiled=compiled,
        config_path=config_path,
        target=target,
    )
    resolved = (
        _default_artifact_path(pipeline_path, pipeline_id=pipeline_id).resolve()
        if pipeline_id is not None
        else _fallback_artifact_path_for_diagnostics(pipeline_path).resolve()
    )
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


def _stop_request_path(*, artifact_path: Path, pipeline_id: str, run_id: str) -> Path:
    root = artifact_path.parent / "stop_requests" / str(pipeline_id).strip()
    root.mkdir(parents=True, exist_ok=True)
    return root / f"{str(run_id).strip()}.json"


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
    run_label: str | None,
    compile_id: str | None,
    runtime_bindings: dict[str, Any] | None,
    connections_path: str | Path | None,
    on_log: Callable[[PipelineLogEvent], None] | None,
) -> tuple[PipelineRuntime, Path]:
    if compiled.spec is None:
        raise RuntimeError("Compiled pipeline is missing a spec.")
    resolved_artifact_path = _resolve_artifact_path(
        pipeline_path,
        artifact_path,
        compiled=compiled,
    )
    runtime = PipelineRuntime(
        pipeline_id=str(compiled.spec.pipeline_id),
        run_label=run_label,
        compile_id=compile_id,
        duckdb_path=str(resolved_artifact_path),
        working_dir=str(pipeline_path.parent.resolve()),
        spec=compiled.spec,
        module_globals=compiled.module_globals,
        runtime_bindings=runtime_bindings,
        connections_path=_resolve_connections_path(pipeline_path, connections_path),
        on_log=on_log,
    )
    return runtime, resolved_artifact_path


def _ensure_run_label_available(
    *,
    artifact_path: str | Path,
    run_label: str | None,
) -> str | None:
    normalized_label = _normalize_run_label(run_label)
    if normalized_label is None:
        return None

    import duckdb_core

    existing = duckdb_core.get_pipeline_run_by_label(
        database_path=str(Path(artifact_path).expanduser().resolve()),
        run_label=normalized_label,
    )
    if existing is not None:
        raise RuntimeError(
            f"Run label '{normalized_label}' already exists for this pipeline. Use a unique run label or omit it."
        )
    return normalized_label


def _resolve_inspect_artifact_path(artifact_path: str | Path) -> Path:
    resolved_artifact_path = Path(artifact_path).expanduser().resolve()
    if not resolved_artifact_path.exists() or not resolved_artifact_path.is_file():
        raise RuntimeError(
            f"Artifact database '{resolved_artifact_path}' was not found. Compile or run the pipeline first."
        )
    return resolved_artifact_path


def _inspect_pipeline_runs(
    artifact_path: str | Path,
    *,
    limit: int | None = None,
):
    resolved_artifact_path = _resolve_inspect_artifact_path(artifact_path)
    _reconcile_orphaned_running_runs(artifact_path=resolved_artifact_path)

    import duckdb_core

    contract = duckdb_core.load_active_compiled_contract(database_path=str(resolved_artifact_path))
    if contract is None:
        raise RuntimeError("No compiled pipeline contract was found. Run queron compile first.")

    runs = duckdb_core.list_pipeline_runs(
        database_path=str(resolved_artifact_path),
        limit=limit,
    )
    return resolved_artifact_path, contract, runs


def _select_pipeline_run_for_inspection(
    *,
    resolved_artifact_path: Path,
    run_id: str | None = None,
    run_label: str | None = None,
    default_to_latest: bool = True,
    available_runs: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    if run_id is not None and run_label is not None:
        raise RuntimeError("Use either run_id or run_label, not both.")

    import duckdb_core

    if run_id is not None:
        selected_run = duckdb_core.get_pipeline_run_by_id(
            database_path=str(resolved_artifact_path),
            run_id=str(run_id),
        )
        if selected_run is None:
            raise RuntimeError(f"Run '{run_id}' was not found for this pipeline.")
        return selected_run

    if run_label is not None:
        selected_run = duckdb_core.get_pipeline_run_by_label(
            database_path=str(resolved_artifact_path),
            run_label=str(run_label),
        )
        if selected_run is None:
            raise RuntimeError(f"Run label '{run_label}' was not found for this pipeline.")
        return selected_run

    if not default_to_latest:
        return None

    if available_runs is not None:
        return available_runs[0] if available_runs else None

    latest_runs = duckdb_core.list_pipeline_runs(
        database_path=str(resolved_artifact_path),
        limit=1,
    )
    if not latest_runs:
        return None
    return latest_runs[0]


def _select_pipeline_run_for_stop(
    resolved_artifact_path: Path,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    import duckdb_core

    if run_id is not None:
        selected_run = _select_pipeline_run_for_inspection(
            resolved_artifact_path=resolved_artifact_path,
            run_id=run_id,
            run_label=None,
            default_to_latest=False,
        )
        if selected_run is None:
            raise RuntimeError(f"Run '{run_id}' was not found for this pipeline.")
    else:
        runs = duckdb_core.list_pipeline_runs(database_path=str(resolved_artifact_path))
        selected_run = next(
            (item for item in runs if str(item.get("status") or "").strip().lower() == "running"),
            None,
        )
        if selected_run is None:
            raise RuntimeError("No running pipeline run was found to stop.")

    selected_status = str(selected_run.get("status") or "").strip().lower()
    if selected_status != "running":
        selected_run_id = str(selected_run.get("run_id") or "").strip() or "-"
        raise RuntimeError(f"Run '{selected_run_id}' is not running, so there is nothing to stop.")
    return selected_run


def _inspect_pipeline_logs(
    artifact_path: str | Path,
    *,
    run_id: str | None = None,
    run_label: str | None = None,
    tail: int | None = None,
) -> tuple[Path, Any, dict[str, Any], list[str]]:
    resolved_artifact_path, active_contract, _runs = _inspect_pipeline_runs(
        artifact_path,
        limit=None,
    )
    if tail is not None and int(tail) <= 0:
        raise RuntimeError("tail must be a positive integer.")

    selected_run = _select_pipeline_run_for_inspection(
        resolved_artifact_path=resolved_artifact_path,
        run_id=run_id,
        run_label=run_label,
        default_to_latest=True,
    )
    if selected_run is None:
        raise RuntimeError("No runs were found for this pipeline.")

    log_path_text = str(selected_run.get("log_path") or "").strip()
    if not log_path_text:
        raise RuntimeError("The selected run does not have a persisted log file.")
    resolved_log_path = Path(log_path_text).expanduser().resolve()
    if not resolved_log_path.exists() or not resolved_log_path.is_file():
        raise RuntimeError(f"Log file '{resolved_log_path}' was not found.")

    lines = resolved_log_path.read_text(encoding="utf-8").splitlines()
    if tail is not None:
        lines = lines[-int(tail) :]
    return resolved_artifact_path, active_contract, selected_run, lines


def _inspect_node_artifact_name(node_payload: dict[str, Any]) -> str | None:
    for key in ("target_table", "target_relation", "output_path", "out"):
        text = str(node_payload.get(key) or "").strip()
        if text:
            return text
    return None


def _load_compiled_contract_for_inspection(
    *,
    resolved_artifact_path: Path,
    selected_run: dict[str, Any] | None,
):
    import duckdb_core

    compile_id: str | None = None
    if selected_run is not None:
        compile_id = str(selected_run.get("compile_id") or "").strip() or None

    if compile_id is not None:
        contract = duckdb_core.load_compiled_contract_by_id(
            database_path=str(resolved_artifact_path),
            compile_id=compile_id,
        )
        if contract is None:
            raise RuntimeError(
                f"Compile contract '{compile_id}' for the selected run was not found in the artifact database."
            )
        return contract

    contract = duckdb_core.load_active_compiled_contract(database_path=str(resolved_artifact_path))
    if contract is None:
        raise RuntimeError("No compiled pipeline contract was found. Run queron compile first.")
    return contract


def _contract_nodes_and_edges(contract) -> tuple[list[dict[str, Any]], list[list[str]]]:
    raw_nodes = contract.spec_json.get("nodes") if isinstance(contract.spec_json, dict) else []
    if not isinstance(raw_nodes, list):
        raw_nodes = []
    raw_edges = contract.edges_json if isinstance(contract.edges_json, list) else []
    edges: list[list[str]] = []
    for raw_edge in raw_edges:
        if not isinstance(raw_edge, list) or len(raw_edge) != 2:
            continue
        source = str(raw_edge[0] or "").strip()
        target = str(raw_edge[1] or "").strip()
        if source and target:
            edges.append([source, target])
    return raw_nodes, edges


def _order_contract_nodes_breadth_first(
    raw_nodes: list[dict[str, Any]],
    edges: list[list[str]],
) -> list[dict[str, Any]]:
    nodes_by_name: dict[str, dict[str, Any]] = {}
    children_by_name: dict[str, set[str]] = {}
    in_degree: dict[str, int] = {}

    for raw_node in raw_nodes:
        if not isinstance(raw_node, dict):
            continue
        name = str(raw_node.get("name") or "").strip()
        if not name:
            continue
        nodes_by_name[name] = raw_node
        children_by_name.setdefault(name, set())
        in_degree.setdefault(name, 0)

    for source, target in edges:
        if source not in nodes_by_name or target not in nodes_by_name:
            continue
        if target in children_by_name[source]:
            continue
        children_by_name[source].add(target)
        in_degree[target] = int(in_degree.get(target, 0)) + 1

    queue = deque(sorted(name for name, degree in in_degree.items() if degree == 0))
    ordered_names: list[str] = []
    seen: set[str] = set()

    while queue:
        current = queue.popleft()
        if current in seen:
            continue
        seen.add(current)
        ordered_names.append(current)

        for child in sorted(children_by_name.get(current, set())):
            in_degree[child] = max(0, int(in_degree.get(child, 0)) - 1)
            if in_degree[child] == 0 and child not in seen:
                queue.append(child)

    for name in sorted(nodes_by_name):
        if name not in seen:
            ordered_names.append(name)

    return [nodes_by_name[name] for name in ordered_names if name in nodes_by_name]


def _select_contract_node_names(
    *,
    requested_node: str,
    nodes_by_name: dict[str, dict[str, Any]],
    edges: list[list[str]],
    upstream: bool,
    downstream: bool,
) -> tuple[str, set[str]]:
    normalized_node = str(requested_node or "").strip()
    if not normalized_node:
        raise RuntimeError("node_name is required.")
    if upstream and downstream:
        raise RuntimeError("Use either upstream or downstream, not both.")
    if normalized_node not in nodes_by_name:
        raise RuntimeError(f"Node '{normalized_node}' was not found in the compiled pipeline.")

    if not upstream and not downstream:
        return "node", {normalized_node}

    adjacency: dict[str, set[str]] = {name: set() for name in nodes_by_name}
    reverse_adjacency: dict[str, set[str]] = {name: set() for name in nodes_by_name}
    for source, target in edges:
        adjacency.setdefault(source, set()).add(target)
        reverse_adjacency.setdefault(target, set()).add(source)
        adjacency.setdefault(target, set())
        reverse_adjacency.setdefault(source, set())

    if upstream:
        selection = "upstream"
        frontier = [normalized_node]
        selected = {normalized_node}
        while frontier:
            current = frontier.pop()
            for parent in reverse_adjacency.get(current, set()):
                if parent in selected:
                    continue
                selected.add(parent)
                frontier.append(parent)
        return selection, selected

    selection = "downstream"
    frontier = [normalized_node]
    selected = {normalized_node}
    while frontier:
        current = frontier.pop()
        for child in adjacency.get(current, set()):
            if child in selected:
                continue
            selected.add(child)
            frontier.append(child)
    return selection, selected


def inspect_node(
    artifact_path: str | Path,
    node_name: str,
    *,
    run_id: str | None = None,
    run_label: str | None = None,
    upstream: bool = False,
    downstream: bool = False,
) -> InspectNodeResult:
    resolved_artifact_path, _active_contract, _runs = _inspect_pipeline_runs(
        artifact_path,
        limit=None,
    )
    selected_run = _select_pipeline_run_for_inspection(
        resolved_artifact_path=resolved_artifact_path,
        run_id=run_id,
        run_label=run_label,
        default_to_latest=True,
    )

    contract = _load_compiled_contract_for_inspection(
        resolved_artifact_path=resolved_artifact_path,
        selected_run=selected_run,
    )
    raw_nodes, edges = _contract_nodes_and_edges(contract)
    nodes_by_name: dict[str, dict[str, Any]] = {}
    for raw_node in raw_nodes:
        if not isinstance(raw_node, dict):
            continue
        name = str(raw_node.get("name") or "").strip()
        if name:
            nodes_by_name[name] = raw_node

    selection, selected_names = _select_contract_node_names(
        requested_node=node_name,
        nodes_by_name=nodes_by_name,
        edges=edges,
        upstream=bool(upstream),
        downstream=bool(downstream),
    )

    node_runs_by_name: dict[str, dict[str, Any]] = {}
    active_states_by_name: dict[str, dict[str, Any]] = {}
    selected_run_id = str(selected_run.get("run_id") or "").strip() if selected_run is not None else ""
    selected_status = str(selected_run.get("status") or "").strip().lower() if selected_run is not None else ""
    selected_status = str(selected_run.get("status") or "").strip().lower() if selected_run is not None else ""
    selected_status = str(selected_run.get("status") or "").strip().lower() if selected_run is not None else ""
    if selected_run_id:
        import duckdb_core

        for item in duckdb_core.get_node_runs_for_run_by_database(
            database_path=str(resolved_artifact_path),
            run_id=selected_run_id,
        ):
            name = str(item.get("node_name") or "").strip()
            if name:
                node_runs_by_name[name] = item
        for item in duckdb_core.get_active_node_states_for_run_by_database(
            database_path=str(resolved_artifact_path),
            run_id=selected_run_id,
        ):
            name = str(item.get("node_name") or "").strip()
            if name:
                active_states_by_name[name] = item

    dependents_by_name: dict[str, list[str]] = {name: [] for name in nodes_by_name}
    for source, target in edges:
        if source in dependents_by_name and target not in dependents_by_name[source]:
            dependents_by_name[source].append(target)

    nodes: list[dict[str, Any]] = []
    for raw_node in raw_nodes:
        if not isinstance(raw_node, dict):
            continue
        name = str(raw_node.get("name") or "").strip()
        if not name or name not in selected_names:
            continue
        node_run = node_runs_by_name.get(name, {})
        active_state = active_states_by_name.get(name, {})
        dependencies = raw_node.get("dependencies")
        if not isinstance(dependencies, list):
            dependencies = []
        nodes.append(
            {
                "name": name,
                "kind": str(raw_node.get("kind") or "").strip() or None,
                "current_state": str(active_state.get("state") or "").strip() or None,
                "node_run_status": str(node_run.get("status") or "").strip() or None,
                "logical_artifact": _inspect_node_artifact_name(raw_node),
                "artifact_name": str(node_run.get("artifact_name") or "").strip() or None,
                "row_count_in": node_run.get("row_count_in"),
                "row_count_out": node_run.get("row_count_out"),
                "dependencies": [
                    str(item).strip()
                    for item in dependencies
                    if str(item).strip()
                ],
                "dependents": sorted(
                    str(item).strip()
                    for item in dependents_by_name.get(name, [])
                    if str(item).strip()
                ),
                "started_at": str(node_run.get("started_at") or "").strip() or None,
                "finished_at": str(node_run.get("finished_at") or "").strip() or None,
            }
        )

    return InspectNodeResult(
        pipeline_path=str(Path(contract.pipeline_path).expanduser().resolve()),
        artifact_path=str(resolved_artifact_path),
        compile_id=contract.compile_id,
        run_id=selected_run_id or None,
        run_label=str(selected_run.get("run_label") or "").strip() or None if selected_run is not None else None,
        run_status=str(selected_run.get("status") or "").strip() or None if selected_run is not None else None,
        is_final=_selected_run_is_final(selected_run),
        selection=selection,
        requested_node=str(node_name).strip(),
        nodes=nodes,
    )


def inspect_node_history(
    artifact_path: str | Path,
    node_name: str,
    *,
    run_id: str | None = None,
    run_label: str | None = None,
) -> InspectNodeHistoryResult:
    resolved_artifact_path, _active_contract, _runs = _inspect_pipeline_runs(
        artifact_path,
        limit=None,
    )
    selected_run = _select_pipeline_run_for_inspection(
        resolved_artifact_path=resolved_artifact_path,
        run_id=run_id,
        run_label=run_label,
        default_to_latest=True,
    )

    contract = _load_compiled_contract_for_inspection(
        resolved_artifact_path=resolved_artifact_path,
        selected_run=selected_run,
    )
    raw_nodes, _edges = _contract_nodes_and_edges(contract)
    nodes_by_name: dict[str, dict[str, Any]] = {}
    for raw_node in raw_nodes:
        if not isinstance(raw_node, dict):
            continue
        name = str(raw_node.get("name") or "").strip()
        if name:
            nodes_by_name[name] = raw_node

    normalized_node_name = str(node_name or "").strip()
    if not normalized_node_name:
        raise RuntimeError("node_name is required.")
    if normalized_node_name not in nodes_by_name:
        raise RuntimeError(f"Node '{normalized_node_name}' was not found in the compiled pipeline.")

    node_payload = nodes_by_name[normalized_node_name]
    selected_run_id = str(selected_run.get("run_id") or "").strip() if selected_run is not None else ""
    node_run: dict[str, Any] = {}
    states: list[dict[str, Any]] = []
    if selected_run_id:
        import duckdb_core

        for item in duckdb_core.get_node_runs_for_run_by_database(
            database_path=str(resolved_artifact_path),
            run_id=selected_run_id,
        ):
            if str(item.get("node_name") or "").strip() == normalized_node_name:
                node_run = item
                break
        states = duckdb_core.get_node_states_for_run_by_database(
            database_path=str(resolved_artifact_path),
            run_id=selected_run_id,
            node_name=normalized_node_name,
        )

    return InspectNodeHistoryResult(
        pipeline_path=str(Path(contract.pipeline_path).expanduser().resolve()),
        artifact_path=str(resolved_artifact_path),
        compile_id=contract.compile_id,
        run_id=selected_run_id or None,
        run_label=str(selected_run.get("run_label") or "").strip() or None if selected_run is not None else None,
        run_status=str(selected_run.get("status") or "").strip() or None if selected_run is not None else None,
        is_final=_selected_run_is_final(selected_run),
        node_name=normalized_node_name,
        node_kind=str(node_payload.get("kind") or "").strip() or None,
        node_run_id=str(node_run.get("node_run_id") or "").strip() or None,
        node_run_status=str(node_run.get("status") or "").strip() or None,
        logical_artifact=_inspect_node_artifact_name(node_payload),
        artifact_name=str(node_run.get("artifact_name") or "").strip() or None,
        started_at=str(node_run.get("started_at") or "").strip() or None,
        finished_at=str(node_run.get("finished_at") or "").strip() or None,
        error_message=str(node_run.get("error_message") or "").strip() or None,
        states=states,
    )


def inspect_node_logs(
    artifact_path: str | Path,
    node_name: str,
    *,
    run_id: str | None = None,
    run_label: str | None = None,
    tail: int | None = None,
) -> InspectNodeLogResult:
    resolved_artifact_path, _active_contract, _runs = _inspect_pipeline_runs(
        artifact_path,
        limit=None,
    )
    if tail is not None and int(tail) <= 0:
        raise RuntimeError("tail must be a positive integer.")

    selected_run = _select_pipeline_run_for_inspection(
        resolved_artifact_path=resolved_artifact_path,
        run_id=run_id,
        run_label=run_label,
        default_to_latest=True,
    )
    if selected_run is None:
        raise RuntimeError("No runs were found for this pipeline.")

    contract = _load_compiled_contract_for_inspection(
        resolved_artifact_path=resolved_artifact_path,
        selected_run=selected_run,
    )
    raw_nodes, _edges = _contract_nodes_and_edges(contract)
    nodes_by_name: dict[str, dict[str, Any]] = {}
    for raw_node in raw_nodes:
        if not isinstance(raw_node, dict):
            continue
        name = str(raw_node.get("name") or "").strip()
        if name:
            nodes_by_name[name] = raw_node

    normalized_node_name = str(node_name or "").strip()
    if not normalized_node_name:
        raise RuntimeError("node_name is required.")
    if normalized_node_name not in nodes_by_name:
        raise RuntimeError(f"Node '{normalized_node_name}' was not found in the compiled pipeline.")

    log_path_text = str(selected_run.get("log_path") or "").strip()
    if not log_path_text:
        raise RuntimeError("The selected run does not have a persisted log file.")
    resolved_log_path = Path(log_path_text).expanduser().resolve()
    if not resolved_log_path.exists() or not resolved_log_path.is_file():
        raise RuntimeError(f"Log file '{resolved_log_path}' was not found.")

    logs: list[dict[str, Any]] = []
    lines = resolved_log_path.read_text(encoding="utf-8").splitlines()
    if tail is not None:
        lines = lines[-int(tail) :]
    for line in lines:
        text = str(line or "").strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except Exception:
            continue
        try:
            event = normalize_log_event(payload)
        except Exception:
            continue
        if str(event.node_name or "").strip() != normalized_node_name:
            continue
        logs.append(event.model_dump())

    node_payload = nodes_by_name[normalized_node_name]
    return InspectNodeLogResult(
        pipeline_path=str(Path(contract.pipeline_path).expanduser().resolve()),
        artifact_path=str(resolved_artifact_path),
        compile_id=contract.compile_id,
        run_id=str(selected_run.get("run_id") or "").strip() or None,
        run_label=str(selected_run.get("run_label") or "").strip() or None,
        run_status=str(selected_run.get("status") or "").strip() or None,
        is_final=_selected_run_is_final(selected_run),
        node_name=normalized_node_name,
        node_kind=str(node_payload.get("kind") or "").strip() or None,
        logs=logs,
    )


def inspect_node_query(
    artifact_path: str | Path,
    node_name: str,
    *,
    run_id: str | None = None,
    run_label: str | None = None,
) -> InspectNodeQueryResult:
    resolved_artifact_path, _active_contract, _runs = _inspect_pipeline_runs(
        artifact_path,
        limit=None,
    )
    selected_run = _select_pipeline_run_for_inspection(
        resolved_artifact_path=resolved_artifact_path,
        run_id=run_id,
        run_label=run_label,
        default_to_latest=True,
    )

    contract = _load_compiled_contract_for_inspection(
        resolved_artifact_path=resolved_artifact_path,
        selected_run=selected_run,
    )
    raw_nodes, _edges = _contract_nodes_and_edges(contract)
    normalized_node_name = str(node_name or "").strip()
    if not normalized_node_name:
        raise RuntimeError("node_name is required.")

    node_payload: dict[str, Any] | None = None
    for raw_node in raw_nodes:
        if not isinstance(raw_node, dict):
            continue
        name = str(raw_node.get("name") or "").strip()
        if name == normalized_node_name:
            node_payload = raw_node
            break
    if node_payload is None:
        raise RuntimeError(f"Node '{normalized_node_name}' was not found in the compiled pipeline.")

    dependencies = node_payload.get("dependencies")
    if not isinstance(dependencies, list):
        dependencies = []

    selected_run_id = str(selected_run.get("run_id") or "").strip() if selected_run is not None else ""
    return InspectNodeQueryResult(
        pipeline_path=str(Path(contract.pipeline_path).expanduser().resolve()),
        artifact_path=str(resolved_artifact_path),
        pipeline_id=contract.pipeline_id,
        compile_id=contract.compile_id,
        run_id=selected_run_id or None,
        run_label=str(selected_run.get("run_label") or "").strip() or None if selected_run is not None else None,
        run_status=str(selected_run.get("status") or "").strip() or None if selected_run is not None else None,
        is_final=_selected_run_is_final(selected_run),
        node_name=normalized_node_name,
        node_kind=str(node_payload.get("kind") or "").strip() or None,
        logical_artifact=_inspect_node_artifact_name(node_payload),
        sql=str(node_payload.get("sql") or "").strip() or None,
        resolved_sql=str(node_payload.get("resolved_sql") or "").strip() or None,
        dependencies=[
            str(item).strip()
            for item in dependencies
            if str(item).strip()
        ],
    )


def inspect_dag(
    artifact_path: str | Path,
    *,
    run_id: str | None = None,
    run_label: str | None = None,
) -> InspectDagResult:
    resolved_artifact_path, _active_contract, runs = _inspect_pipeline_runs(
        artifact_path,
        limit=None,
    )
    selected_run = _select_pipeline_run_for_inspection(
        resolved_artifact_path=resolved_artifact_path,
        run_id=run_id,
        run_label=run_label,
        default_to_latest=True,
        available_runs=runs,
    )
    contract = _load_compiled_contract_for_inspection(
        resolved_artifact_path=resolved_artifact_path,
        selected_run=selected_run,
    )

    node_runs_by_name: dict[str, dict[str, Any]] = {}
    active_states_by_name: dict[str, dict[str, Any]] = {}
    selected_run_id = str(selected_run.get("run_id") or "").strip() if selected_run is not None else ""
    selected_status = str(selected_run.get("status") or "").strip().lower() if selected_run is not None else ""
    if selected_run_id:
        import duckdb_core

        for item in duckdb_core.get_node_runs_for_run_by_database(
            database_path=str(resolved_artifact_path),
            run_id=selected_run_id,
        ):
            node_name = str(item.get("node_name") or "").strip()
            if node_name:
                node_runs_by_name[node_name] = item
        for item in duckdb_core.get_active_node_states_for_run_by_database(
            database_path=str(resolved_artifact_path),
            run_id=selected_run_id,
        ):
                node_name = str(item.get("node_name") or "").strip()
                if node_name:
                    active_states_by_name[node_name] = item

    raw_nodes, edges = _contract_nodes_and_edges(contract)
    ordered_nodes = _order_contract_nodes_breadth_first(raw_nodes, edges)
    pipeline_id = (
        str(selected_run.get("pipeline_id") or "").strip()
        if selected_run is not None
        else ""
    ) or str(getattr(contract, "pipeline_id", "") or "").strip() or None

    nodes: list[dict[str, Any]] = []
    for raw_node in ordered_nodes:
        if not isinstance(raw_node, dict):
            continue
        name = str(raw_node.get("name") or "").strip()
        if not name:
            continue
        node_run = node_runs_by_name.get(name, {})
        active_state = active_states_by_name.get(name, {})
        logical_artifact = _inspect_node_artifact_name(raw_node)
        run_artifact = str(node_run.get("artifact_name") or "").strip() or None
        use_run_artifact = bool(run_artifact) and selected_status in {"success", "success_with_warnings", "failed"}
        nodes.append(
            {
                "name": name,
                "kind": str(raw_node.get("kind") or "").strip() or None,
                "artifact_name": run_artifact if use_run_artifact else logical_artifact,
                "logical_artifact": logical_artifact,
                "current_state": str(active_state.get("state") or "").strip() or None,
                "node_run_status": str(node_run.get("status") or "").strip() or None,
                "started_at": str(node_run.get("started_at") or "").strip() or None,
                "finished_at": str(node_run.get("finished_at") or "").strip() or None,
                "row_count_in": node_run.get("row_count_in"),
                "row_count_out": node_run.get("row_count_out"),
            }
        )

    return InspectDagResult(
        pipeline_path=str(Path(contract.pipeline_path).expanduser().resolve()),
        artifact_path=str(resolved_artifact_path),
        pipeline_id=pipeline_id,
        compile_id=contract.compile_id,
        run_id=selected_run_id or None,
        run_label=str(selected_run.get("run_label") or "").strip() or None if selected_run is not None else None,
        run_status=str(selected_run.get("status") or "").strip() or None if selected_run is not None else None,
        is_final=_selected_run_is_final(selected_run),
        nodes=nodes,
        edges=edges,
    )


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


def _preserve_latest_incomplete_run_outputs_before_purge(
    compiled: CompiledPipeline,
    *,
    runtime: PipelineRuntime,
) -> dict[str, str]:
    if compiled.spec is None:
        return {}

    import duckdb_core

    latest_run = duckdb_core.get_latest_pipeline_run(
        connection_id=runtime._ensure_duckdb_connection_id(),
        status=None,
    )
    if not latest_run:
        return {}

    latest_run_id = str(latest_run.get("run_id") or "").strip()
    latest_status = str(latest_run.get("status") or "").strip().lower()
    if not latest_run_id or latest_status in {"success", "success_with_warnings"}:
        return {}

    selected_node_names = {node.name for node in compiled.spec.nodes}
    existing_outputs = runtime.existing_output_tables(selected_node_names)
    if not existing_outputs:
        return {}

    runtime._log_event(
        code=LogCode.PIPELINE_ARCHIVE_STARTED,
        message=(
            f"Archiving {len(existing_outputs)} output table(s) from prior "
            f"{latest_status} run '{latest_run_id}' before purge."
        ),
        details={
            "previous_run_id": latest_run_id,
            "previous_run_status": latest_status,
            "target_tables": list(existing_outputs),
        },
    )
    archived = duckdb_core.archive_pipeline_targets(
        connection_id=runtime._ensure_duckdb_connection_id(),
        run_id=latest_run_id,
        target_tables=list(existing_outputs),
    )
    runtime._log_event(
        code=LogCode.PIPELINE_ARCHIVE_FINISHED,
        message=(
            f"Archived {len(archived)} output table(s) from prior "
            f"{latest_status} run '{latest_run_id}'."
        ),
        details={
            "previous_run_id": latest_run_id,
            "previous_run_status": latest_status,
            "archived_tables": dict(archived),
        },
    )
    return archived


def _current_failed_run_context(
    runtime: PipelineRuntime,
    *,
    expected_compile_id: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    import duckdb_core

    latest_run = duckdb_core.get_latest_pipeline_run(
        connection_id=runtime._ensure_duckdb_connection_id(),
        status=None,
    )
    if not latest_run:
        raise RuntimeError("No pipeline run was found to resume or reset.")
    if str(latest_run.get("status") or "").strip().lower() != "failed":
        raise RuntimeError("The latest pipeline run is not failed, so there is nothing to resume or reset.")
    if bool(latest_run.get("is_final")):
        raise RuntimeError("The latest failed pipeline run is final, so it cannot be resumed or reset.")
    if expected_compile_id is not None:
        latest_compile_id = str(latest_run.get("compile_id") or "").strip() or None
        if latest_compile_id is not None and latest_compile_id != expected_compile_id:
            raise RuntimeError(
                "The latest failed pipeline run belongs to an older compile contract. Re-run compile before using resume or reset."
            )

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
            if active_state_by_name.get(node.name, "ready") in {"ready", "running", "failed", "cleared", "skipped"}
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


def _run_pipeline_impl(
    pipeline_path: str | Path,
    *,
    config_path: str | Path | None = None,
    connections_path: str | Path | None = None,
    runtime_bindings: dict[str, Any] | None = None,
    target: str | None = None,
    artifact_path: str | Path | None = None,
    target_node: str | None = None,
    clean_existing: bool = False,
    set_final: bool = False,
    pipeline_id: str | None = None,
    run_label: str | None = None,
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
        message="Validating compiled pipeline contract...",
    )
    compiled, resolved_artifact_path = _validated_compiled_pipeline_for_file(
        resolved_pipeline_path,
        config_path=config_path,
        target=target,
        artifact_path=artifact_path,
    )
    if has_compile_errors(compiled) or compiled.spec is None:
        _emit_log_event(
            on_log,
            code=LogCode.PIPELINE_COMPILE_FAILED,
            message="Pipeline compile contract validation failed.",
            severity="error",
            details={"diagnostic_count": len(compiled.diagnostics)},
        )
        return RunPipelineResult(
            compiled=compiled,
            executed_nodes=[],
            artifact_path=str(resolved_artifact_path),
            run_id=None,
            log_path=None,
        )
    _emit_log_event(
        on_log,
        code=LogCode.PIPELINE_COMPILE_SUCCEEDED,
        message=f"Compiled contract validated with {len(compiled.spec.nodes)} node(s).",
        details={"node_count": len(compiled.spec.nodes)},
    )
    import duckdb_core

    latest_runs = duckdb_core.list_pipeline_runs(database_path=str(resolved_artifact_path), limit=1)
    latest_run = latest_runs[0] if latest_runs else None
    if latest_run is not None and not _is_run_final(latest_run):
        if not bool(set_final):
            raise RuntimeError(
                "The latest pipeline run is not final. Re-run with set_final=True to finalize the prior failed or stale running run first."
            )
        latest_status = str(latest_run.get("status") or "").strip().lower()
        if latest_status == "failed":
            _set_run_final_if_allowed(artifact_path=resolved_artifact_path, run=latest_run)
        elif latest_status == "running":
            latest_run_id = str(latest_run.get("run_id") or "").strip()
            if _is_run_active_in_registry(latest_run_id):
                raise RuntimeError(f"Pipeline run '{latest_run_id}' is currently active and must finish before a new run can start.")
            _reconcile_orphaned_running_runs(artifact_path=resolved_artifact_path, run_id=latest_run_id)
            reconciled_run = duckdb_core.get_pipeline_run_by_id(
                database_path=str(resolved_artifact_path),
                run_id=latest_run_id,
            )
            _set_run_final_if_allowed(artifact_path=resolved_artifact_path, run=reconciled_run)
        else:
            raise RuntimeError(
                f"The latest pipeline run has unexpected non-final status '{latest_status or 'unknown'}'."
            )
    normalized_run_label = _ensure_run_label_available(
        artifact_path=resolved_artifact_path,
        run_label=run_label,
    )
    runtime, resolved_artifact_path = _build_runtime_for_pipeline(
        pipeline_path=resolved_pipeline_path,
        compiled=compiled,
        artifact_path=artifact_path,
        run_label=normalized_run_label,
        compile_id=compiled.contract.compile_id if compiled.contract is not None else None,
        runtime_bindings=runtime_bindings,
        connections_path=connections_path,
        on_log=on_log,
    )
    if clean_existing:
        _preserve_latest_incomplete_run_outputs_before_purge(compiled, runtime=runtime)
        runtime.clear_pipeline_outputs()
    if target_node:
        runtime._log_event(
            code=LogCode.PIPELINE_TARGET_SELECTED,
            message=f"Executing target node '{target_node}' with dependencies.",
            details={"target_node": target_node},
        )
    runtime._log_event(
        code=LogCode.PIPELINE_EXECUTION_STARTED,
        message="Executing pipeline DAG.",
        details={"target_node": target_node, "clean_existing": bool(clean_existing)},
    )
    _register_active_runtime(runtime)
    try:
        executed = execute_compiled_pipeline(compiled, runtime=runtime, target_node=target_node)
    except Exception as exc:
        runtime._log_event(
            code=LogCode.PIPELINE_EXECUTION_FAILED,
            message=f"Execution failed: {exc}",
            severity="error",
            details=_pipeline_failure_details(exc),
        )
        raise
    finally:
        _unregister_active_runtime(runtime)
    runtime._log_event(
        code=LogCode.PIPELINE_EXECUTION_FINISHED,
        message=f"Finished successfully. Executed {len(executed)} node(s).",
        details={"executed_nodes": list(executed)},
    )
    return RunPipelineResult(
        compiled=compiled,
        executed_nodes=executed,
        artifact_path=str(resolved_artifact_path),
        run_id=runtime.run_id,
        run_label=runtime.run_label,
        log_path=runtime.log_path,
    )


def run_pipeline(
    pipeline_path: str | Path,
    *,
    config_path: str | Path | None = None,
    connections_path: str | Path | None = None,
    runtime_bindings: dict[str, Any] | None = None,
    target: str | None = None,
    target_node: str | None = None,
    clean_existing: bool = False,
    set_final: bool = False,
    pipeline_id: str | None = None,
    run_label: str | None = None,
    on_log: Callable[[PipelineLogEvent], None] | None = None,
) -> RunPipelineResult:
    return _run_pipeline_impl(
        pipeline_path,
        config_path=config_path,
        connections_path=connections_path,
        runtime_bindings=runtime_bindings,
        target=target,
        artifact_path=None,
        target_node=target_node,
        clean_existing=clean_existing,
        set_final=set_final,
        pipeline_id=pipeline_id,
        run_label=run_label,
        on_log=on_log,
    )


def _resume_pipeline_impl(
    pipeline_path: str | Path,
    *,
    config_path: str | Path | None = None,
    connections_path: str | Path | None = None,
    runtime_bindings: dict[str, Any] | None = None,
    target: str | None = None,
    artifact_path: str | Path | None = None,
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
        message="Validating compiled pipeline contract...",
    )
    compiled, resolved_artifact_path = _validated_compiled_pipeline_for_file(
        resolved_pipeline_path,
        config_path=config_path,
        target=target,
        artifact_path=artifact_path,
    )
    if has_compile_errors(compiled) or compiled.spec is None:
        _emit_log_event(
            on_log,
            code=LogCode.PIPELINE_COMPILE_FAILED,
            message="Pipeline compile contract validation failed.",
            severity="error",
            details={"diagnostic_count": len(compiled.diagnostics)},
        )
        return RunPipelineResult(
            compiled=compiled,
            executed_nodes=[],
            artifact_path=str(resolved_artifact_path),
            run_id=None,
            log_path=None,
        )
    _emit_log_event(
        on_log,
        code=LogCode.PIPELINE_COMPILE_SUCCEEDED,
        message=f"Compiled contract validated with {len(compiled.spec.nodes)} node(s).",
        details={"node_count": len(compiled.spec.nodes)},
    )

    runtime, resolved_artifact_path = _build_runtime_for_pipeline(
        pipeline_path=resolved_pipeline_path,
        compiled=compiled,
        artifact_path=artifact_path,
        run_label=None,
        compile_id=compiled.contract.compile_id if compiled.contract is not None else None,
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
        run_label=runtime.run_label,
        log_path=runtime.log_path,
    )


def resume_pipeline(
    pipeline_path: str | Path,
    *,
    config_path: str | Path | None = None,
    connections_path: str | Path | None = None,
    runtime_bindings: dict[str, Any] | None = None,
    target: str | None = None,
    on_log: Callable[[PipelineLogEvent], None] | None = None,
) -> RunPipelineResult:
    return _resume_pipeline_impl(
        pipeline_path,
        config_path=config_path,
        connections_path=connections_path,
        runtime_bindings=runtime_bindings,
        target=target,
        artifact_path=None,
        on_log=on_log,
    )


def _reset_node_impl(
    pipeline_path: str | Path,
    *,
    node_name: str,
    config_path: str | Path | None = None,
    target: str | None = None,
    artifact_path: str | Path | None = None,
    on_log: Callable[[PipelineLogEvent], None] | None = None,
) -> ResetPipelineResult:
    resolved_pipeline_path = Path(pipeline_path).expanduser().resolve()
    _emit_log_event(
        on_log,
        code=LogCode.PIPELINE_RESET_STARTED,
        message=f"Resetting node '{node_name}' for {resolved_pipeline_path.name}.",
        details={"pipeline_path": str(resolved_pipeline_path), "trigger": "reset_node", "selected_nodes": [node_name]},
        node_name=node_name,
    )
    compiled, resolved_artifact_path = _validated_compiled_pipeline_for_file(
        resolved_pipeline_path,
        config_path=config_path,
        target=target,
        artifact_path=artifact_path,
    )
    if has_compile_errors(compiled) or compiled.spec is None:
        _emit_log_event(
            on_log,
            code=LogCode.PIPELINE_RESET_FAILED,
            message="Reset-node validation failed because the compiled contract is unavailable.",
            severity="error",
            details={"trigger": "reset_node", "selected_nodes": [node_name]},
            node_name=node_name,
        )
        return ResetPipelineResult(compiled=compiled, artifact_path=str(resolved_artifact_path), reset_nodes=[], reset_tables=[])

    runtime, _ = _build_runtime_for_pipeline(
        pipeline_path=resolved_pipeline_path,
        compiled=compiled,
        artifact_path=resolved_artifact_path,
        run_label=None,
        compile_id=compiled.contract.compile_id if compiled.contract is not None else None,
        runtime_bindings=None,
        connections_path=None,
        on_log=on_log,
    )
    try:
        latest_run, node_runs, _active_states = _current_failed_run_context(
            runtime,
            expected_compile_id=compiled.contract.compile_id if compiled.contract is not None else None,
        )
        runtime.attach_run_context(
            run_id=str(latest_run.get("run_id") or ""),
            node_runs=node_runs,
            run_label=str(latest_run.get("run_label") or "").strip() or None,
        )
        result = reset_compiled_node(compiled, runtime=runtime, node_name=node_name)
    except Exception as exc:
        _emit_log_event(
            on_log,
            code=LogCode.PIPELINE_RESET_FAILED,
            message=f"Reset-node failed for '{node_name}'.",
            severity="error",
            details={"trigger": "reset_node", "selected_nodes": [node_name], "error": str(exc)},
            node_name=node_name,
        )
        raise
    _emit_log_event(
        on_log,
        code=LogCode.PIPELINE_RESET_FINISHED,
        message=f"Reset-node finished for '{node_name}'.",
        details={
            "trigger": "reset_node",
            "selected_nodes": list(result.reset_nodes),
            "reset_tables": list(result.reset_tables),
            "artifact_path": str(resolved_artifact_path),
        },
        node_name=node_name,
    )
    return ResetPipelineResult(
        compiled=result.compiled,
        artifact_path=str(resolved_artifact_path),
        reset_nodes=result.reset_nodes,
        reset_tables=result.reset_tables,
    )


def _reset_downstream_impl(
    pipeline_path: str | Path,
    *,
    node_name: str,
    config_path: str | Path | None = None,
    target: str | None = None,
    artifact_path: str | Path | None = None,
    on_log: Callable[[PipelineLogEvent], None] | None = None,
) -> ResetPipelineResult:
    resolved_pipeline_path = Path(pipeline_path).expanduser().resolve()
    _emit_log_event(
        on_log,
        code=LogCode.PIPELINE_RESET_STARTED,
        message=f"Resetting downstream from node '{node_name}' for {resolved_pipeline_path.name}.",
        details={"pipeline_path": str(resolved_pipeline_path), "trigger": "reset_downstream", "selected_nodes": [node_name]},
        node_name=node_name,
    )
    compiled, resolved_artifact_path = _validated_compiled_pipeline_for_file(
        resolved_pipeline_path,
        config_path=config_path,
        target=target,
        artifact_path=artifact_path,
    )
    if has_compile_errors(compiled) or compiled.spec is None:
        _emit_log_event(
            on_log,
            code=LogCode.PIPELINE_RESET_FAILED,
            message="Reset-downstream validation failed because the compiled contract is unavailable.",
            severity="error",
            details={"trigger": "reset_downstream", "selected_nodes": [node_name]},
            node_name=node_name,
        )
        return ResetPipelineResult(compiled=compiled, artifact_path=str(resolved_artifact_path), reset_nodes=[], reset_tables=[])

    runtime, _ = _build_runtime_for_pipeline(
        pipeline_path=resolved_pipeline_path,
        compiled=compiled,
        artifact_path=resolved_artifact_path,
        run_label=None,
        compile_id=compiled.contract.compile_id if compiled.contract is not None else None,
        runtime_bindings=None,
        connections_path=None,
        on_log=on_log,
    )
    try:
        latest_run, node_runs, _active_states = _current_failed_run_context(
            runtime,
            expected_compile_id=compiled.contract.compile_id if compiled.contract is not None else None,
        )
        runtime.attach_run_context(
            run_id=str(latest_run.get("run_id") or ""),
            node_runs=node_runs,
            run_label=str(latest_run.get("run_label") or "").strip() or None,
        )
        result = reset_compiled_downstream(compiled, runtime=runtime, node_name=node_name)
    except Exception as exc:
        _emit_log_event(
            on_log,
            code=LogCode.PIPELINE_RESET_FAILED,
            message=f"Reset-downstream failed for '{node_name}'.",
            severity="error",
            details={"trigger": "reset_downstream", "selected_nodes": [node_name], "error": str(exc)},
            node_name=node_name,
        )
        raise
    _emit_log_event(
        on_log,
        code=LogCode.PIPELINE_RESET_FINISHED,
        message=f"Reset-downstream finished for '{node_name}'.",
        details={
            "trigger": "reset_downstream",
            "selected_nodes": list(result.reset_nodes),
            "reset_tables": list(result.reset_tables),
            "artifact_path": str(resolved_artifact_path),
        },
        node_name=node_name,
    )
    return ResetPipelineResult(
        compiled=result.compiled,
        artifact_path=str(resolved_artifact_path),
        reset_nodes=result.reset_nodes,
        reset_tables=result.reset_tables,
    )


def reset_downstream(
    pipeline_path: str | Path,
    *,
    node_name: str,
    config_path: str | Path | None = None,
    target: str | None = None,
    on_log: Callable[[PipelineLogEvent], None] | None = None,
) -> ResetPipelineResult:
    return _reset_downstream_impl(
        pipeline_path,
        node_name=node_name,
        config_path=config_path,
        target=target,
        artifact_path=None,
        on_log=on_log,
    )


def stop_pipeline(
    pipeline_path: str | Path,
    *,
    run_id: str | None = None,
    reason: str | None = None,
) -> StopPipelineResult:
    resolved_pipeline_path = Path(pipeline_path).expanduser().resolve()
    resolved_artifact_path = _resolve_inspect_artifact_path(
        _resolve_artifact_path(
            resolved_pipeline_path,
            artifact_path=None,
            compiled=None,
            config_path=None,
            target=None,
        )
    )
    selected_run = _select_pipeline_run_for_stop(
        resolved_artifact_path,
        run_id=run_id,
    )
    resolved_run_id = str(selected_run.get("run_id") or "").strip()
    if not resolved_run_id:
        raise RuntimeError("The selected running pipeline is missing a run_id.")
    pipeline_id = str(selected_run.get("pipeline_id") or "").strip()
    if not pipeline_id:
        raise RuntimeError("The selected running pipeline is missing a pipeline_id.")

    request_path = _stop_request_path(
        artifact_path=resolved_artifact_path,
        pipeline_id=pipeline_id,
        run_id=resolved_run_id,
    )
    request_payload = {
        "pipeline_id": pipeline_id,
        "pipeline_path": str(resolved_pipeline_path),
        "artifact_path": str(resolved_artifact_path),
        "run_id": resolved_run_id,
        "run_label": str(selected_run.get("run_label") or "").strip() or None,
        "requested_at": utc_now_timestamp(),
        "reason": _normalize_stop_reason(reason),
        "stop_mode": "graceful",
        "status": "requested",
    }
    request_path.write_text(json.dumps(request_payload, indent=2) + "\n", encoding="utf-8")
    return StopPipelineResult(
        artifact_path=str(resolved_artifact_path),
        run_id=resolved_run_id,
        run_label=request_payload["run_label"],
        stop_requested=True,
        stop_mode="graceful",
        request_path=str(request_path),
        message=f"Stop requested for run '{resolved_run_id}'.",
    )


def force_stop_pipeline(
    pipeline_path: str | Path,
    *,
    run_id: str | None = None,
    reason: str | None = None,
) -> StopPipelineResult:
    resolved_pipeline_path = Path(pipeline_path).expanduser().resolve()
    resolved_artifact_path = _resolve_inspect_artifact_path(
        _resolve_artifact_path(
            resolved_pipeline_path,
            artifact_path=None,
            compiled=None,
            config_path=None,
            target=None,
        )
    )
    selected_run = _select_pipeline_run_for_stop(
        resolved_artifact_path,
        run_id=run_id,
    )
    resolved_run_id = str(selected_run.get("run_id") or "").strip()
    if not resolved_run_id:
        raise RuntimeError("The selected running pipeline is missing a run_id.")
    pipeline_id = str(selected_run.get("pipeline_id") or "").strip()
    if not pipeline_id:
        raise RuntimeError("The selected running pipeline is missing a pipeline_id.")

    request_path = _stop_request_path(
        artifact_path=resolved_artifact_path,
        pipeline_id=pipeline_id,
        run_id=resolved_run_id,
    )
    request_payload = {
        "pipeline_id": pipeline_id,
        "pipeline_path": str(resolved_pipeline_path),
        "artifact_path": str(resolved_artifact_path),
        "run_id": resolved_run_id,
        "run_label": str(selected_run.get("run_label") or "").strip() or None,
        "requested_at": utc_now_timestamp(),
        "reason": _normalize_force_stop_reason(reason),
        "stop_mode": "force",
        "status": "requested",
    }
    request_path.write_text(json.dumps(request_payload, indent=2) + "\n", encoding="utf-8")
    return StopPipelineResult(
        artifact_path=str(resolved_artifact_path),
        run_id=resolved_run_id,
        run_label=request_payload["run_label"],
        stop_requested=True,
        stop_mode="force",
        request_path=str(request_path),
        message=f"Force stop requested for run '{resolved_run_id}'.",
    )


def reset_node(
    pipeline_path: str | Path,
    *,
    node_name: str,
    config_path: str | Path | None = None,
    target: str | None = None,
    on_log: Callable[[PipelineLogEvent], None] | None = None,
) -> ResetPipelineResult:
    return _reset_node_impl(
        pipeline_path,
        node_name=node_name,
        config_path=config_path,
        target=target,
        artifact_path=None,
        on_log=on_log,
    )


def _reset_upstream_impl(
    pipeline_path: str | Path,
    *,
    node_name: str,
    config_path: str | Path | None = None,
    target: str | None = None,
    artifact_path: str | Path | None = None,
    on_log: Callable[[PipelineLogEvent], None] | None = None,
) -> ResetPipelineResult:
    resolved_pipeline_path = Path(pipeline_path).expanduser().resolve()
    _emit_log_event(
        on_log,
        code=LogCode.PIPELINE_RESET_STARTED,
        message=f"Resetting upstream from node '{node_name}' for {resolved_pipeline_path.name}.",
        details={"pipeline_path": str(resolved_pipeline_path), "trigger": "reset_upstream", "selected_nodes": [node_name]},
        node_name=node_name,
    )
    compiled, resolved_artifact_path = _validated_compiled_pipeline_for_file(
        resolved_pipeline_path,
        config_path=config_path,
        target=target,
        artifact_path=artifact_path,
    )
    if has_compile_errors(compiled) or compiled.spec is None:
        _emit_log_event(
            on_log,
            code=LogCode.PIPELINE_RESET_FAILED,
            message="Reset-upstream validation failed because the compiled contract is unavailable.",
            severity="error",
            details={"trigger": "reset_upstream", "selected_nodes": [node_name]},
            node_name=node_name,
        )
        return ResetPipelineResult(compiled=compiled, artifact_path=str(resolved_artifact_path), reset_nodes=[], reset_tables=[])

    runtime, _ = _build_runtime_for_pipeline(
        pipeline_path=resolved_pipeline_path,
        compiled=compiled,
        artifact_path=resolved_artifact_path,
        run_label=None,
        compile_id=compiled.contract.compile_id if compiled.contract is not None else None,
        runtime_bindings=None,
        connections_path=None,
        on_log=on_log,
    )
    try:
        latest_run, node_runs, _active_states = _current_failed_run_context(
            runtime,
            expected_compile_id=compiled.contract.compile_id if compiled.contract is not None else None,
        )
        runtime.attach_run_context(
            run_id=str(latest_run.get("run_id") or ""),
            node_runs=node_runs,
            run_label=str(latest_run.get("run_label") or "").strip() or None,
        )
        result = reset_compiled_upstream(compiled, runtime=runtime, node_name=node_name)
    except Exception as exc:
        _emit_log_event(
            on_log,
            code=LogCode.PIPELINE_RESET_FAILED,
            message=f"Reset-upstream failed for '{node_name}'.",
            severity="error",
            details={"trigger": "reset_upstream", "selected_nodes": [node_name], "error": str(exc)},
            node_name=node_name,
        )
        raise
    _emit_log_event(
        on_log,
        code=LogCode.PIPELINE_RESET_FINISHED,
        message=f"Reset-upstream finished for '{node_name}'.",
        details={
            "trigger": "reset_upstream",
            "selected_nodes": list(result.reset_nodes),
            "reset_tables": list(result.reset_tables),
            "artifact_path": str(resolved_artifact_path),
        },
        node_name=node_name,
    )
    return ResetPipelineResult(
        compiled=result.compiled,
        artifact_path=str(resolved_artifact_path),
        reset_nodes=result.reset_nodes,
        reset_tables=result.reset_tables,
    )


def reset_upstream(
    pipeline_path: str | Path,
    *,
    node_name: str,
    config_path: str | Path | None = None,
    target: str | None = None,
    on_log: Callable[[PipelineLogEvent], None] | None = None,
) -> ResetPipelineResult:
    return _reset_upstream_impl(
        pipeline_path,
        node_name=node_name,
        config_path=config_path,
        target=target,
        artifact_path=None,
        on_log=on_log,
    )


def _reset_all_impl(
    pipeline_path: str | Path,
    *,
    config_path: str | Path | None = None,
    target: str | None = None,
    artifact_path: str | Path | None = None,
    on_log: Callable[[PipelineLogEvent], None] | None = None,
) -> ResetPipelineResult:
    resolved_pipeline_path = Path(pipeline_path).expanduser().resolve()
    _emit_log_event(
        on_log,
        code=LogCode.PIPELINE_RESET_STARTED,
        message=f"Resetting all nodes for {resolved_pipeline_path.name}.",
        details={"pipeline_path": str(resolved_pipeline_path), "trigger": "reset_all"},
    )
    compiled, resolved_artifact_path = _validated_compiled_pipeline_for_file(
        resolved_pipeline_path,
        config_path=config_path,
        target=target,
        artifact_path=artifact_path,
    )
    if has_compile_errors(compiled) or compiled.spec is None:
        _emit_log_event(
            on_log,
            code=LogCode.PIPELINE_RESET_FAILED,
            message="Reset-all validation failed because the compiled contract is unavailable.",
            severity="error",
            details={"trigger": "reset_all"},
        )
        return ResetPipelineResult(compiled=compiled, artifact_path=str(resolved_artifact_path), reset_nodes=[], reset_tables=[])

    runtime, _ = _build_runtime_for_pipeline(
        pipeline_path=resolved_pipeline_path,
        compiled=compiled,
        artifact_path=resolved_artifact_path,
        run_label=None,
        compile_id=compiled.contract.compile_id if compiled.contract is not None else None,
        runtime_bindings=None,
        connections_path=None,
        on_log=on_log,
    )
    try:
        latest_run, node_runs, _active_states = _current_failed_run_context(
            runtime,
            expected_compile_id=compiled.contract.compile_id if compiled.contract is not None else None,
        )
        runtime.attach_run_context(
            run_id=str(latest_run.get("run_id") or ""),
            node_runs=node_runs,
            run_label=str(latest_run.get("run_label") or "").strip() or None,
        )
        result = reset_compiled_all(compiled, runtime=runtime)
    except Exception as exc:
        _emit_log_event(
            on_log,
            code=LogCode.PIPELINE_RESET_FAILED,
            message="Reset-all failed.",
            severity="error",
            details={"trigger": "reset_all", "error": str(exc)},
        )
        raise
    _emit_log_event(
        on_log,
        code=LogCode.PIPELINE_RESET_FINISHED,
        message="Reset-all finished.",
        details={
            "trigger": "reset_all",
            "selected_nodes": list(result.reset_nodes),
            "reset_tables": list(result.reset_tables),
            "artifact_path": str(resolved_artifact_path),
        },
    )
    return ResetPipelineResult(
        compiled=result.compiled,
        artifact_path=str(resolved_artifact_path),
        reset_nodes=result.reset_nodes,
        reset_tables=result.reset_tables,
    )


def reset_all(
    pipeline_path: str | Path,
    *,
    config_path: str | Path | None = None,
    target: str | None = None,
    on_log: Callable[[PipelineLogEvent], None] | None = None,
) -> ResetPipelineResult:
    return _reset_all_impl(
        pipeline_path,
        config_path=config_path,
        target=target,
        artifact_path=None,
        on_log=on_log,
    )


def list_existing_outputs_for_file(
    pipeline_path: str | Path,
    *,
    config_path: str | Path | None = None,
    connections_path: str | Path | None = None,
    runtime_bindings: dict[str, Any] | None = None,
    target: str | None = None,
    artifact_path: str | Path | None = None,
) -> tuple[CompiledPipeline, list[str], str]:
    resolved_pipeline_path = Path(pipeline_path).expanduser().resolve()
    compiled, resolved_artifact_path = _validated_compiled_pipeline_for_file(
        resolved_pipeline_path,
        config_path=config_path,
        target=target,
        artifact_path=artifact_path,
    )
    if has_compile_errors(compiled) or compiled.spec is None:
        return compiled, [], str(resolved_artifact_path)

    runtime, _ = _build_runtime_for_pipeline(
        pipeline_path=resolved_pipeline_path,
        compiled=compiled,
        artifact_path=resolved_artifact_path,
        run_label=None,
        compile_id=compiled.contract.compile_id if compiled.contract is not None else None,
        runtime_bindings=runtime_bindings,
        connections_path=connections_path,
        on_log=None,
    )
    existing_outputs = list_existing_compiled_outputs(compiled, runtime=runtime)
    return compiled, existing_outputs, str(resolved_artifact_path)
