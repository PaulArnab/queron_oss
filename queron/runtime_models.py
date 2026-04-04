from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Literal, Mapping

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


WarningSeverity = Literal["info", "warning", "error"]
WarningSource = Literal["queron", "connector", "driver"]
RunStatus = Literal["pending", "running", "success", "success_with_warnings", "failed", "skipped"]
NodeStatus = Literal["ready", "running", "complete", "failed", "skipped", "cleared"]
OnExceptionPolicy = Literal["stop"]
OnWarningPolicy = Literal["continue"]
DownstreamHardFailurePolicy = Literal["skip"]
LogSeverity = Literal["debug", "info", "warning", "error"]
LogSource = Literal["queron", "connector", "driver", "app", "cli"]


class WarningCode:
    RUNTIME_WARNING = "runtime_warning"
    DRIVER_WARNING = "driver_warning"
    CONNECTOR_WARNING = "connector_warning"
    COLUMN_MAPPING_WARNING = "column_mapping_warning"
    TYPE_MAPPING_WARNING = "type_mapping_warning"
    TYPE_COERCION_WARNING = "type_coercion_warning"
    ARTIFACT_WARNING = "artifact_warning"
    EXPORT_WARNING = "export_warning"
    EGRESS_WARNING = "egress_warning"


class LogCode:
    PIPELINE_RUN_STARTED = "pipeline_run_started"
    PIPELINE_COMPILE_STARTED = "pipeline_compile_started"
    PIPELINE_COMPILE_SUCCEEDED = "pipeline_compile_succeeded"
    PIPELINE_COMPILE_FAILED = "pipeline_compile_failed"
    PIPELINE_PURGE_CONFIRMATION_REQUIRED = "pipeline_purge_confirmation_required"
    PIPELINE_TARGET_SELECTED = "pipeline_target_selected"
    PIPELINE_CLEAN_STARTED = "pipeline_clean_started"
    PIPELINE_CLEAN_FINISHED = "pipeline_clean_finished"
    PIPELINE_EXECUTION_STARTED = "pipeline_execution_started"
    PIPELINE_EXECUTION_FINISHED = "pipeline_execution_finished"
    PIPELINE_EXECUTION_FAILED = "pipeline_execution_failed"
    PIPELINE_OUTPUTS_CLEARED = "pipeline_outputs_cleared"
    NODE_EXECUTION_STARTED = "node_execution_started"
    NODE_EXECUTION_FAILED = "node_execution_failed"
    NODE_SKIPPED = "node_skipped"
    NODE_CHECK_PASSED = "node_check_passed"
    NODE_SOURCES_RESOLVED = "node_sources_resolved"
    NODE_REFS_RESOLVED = "node_refs_resolved"
    NODE_ROWS_WRITTEN = "node_rows_written"
    NODE_ARTIFACT_CREATED = "node_artifact_created"
    NODE_EGRESS_WRITTEN = "node_egress_written"
    NODE_EXPORT_WRITTEN = "node_export_written"
    NODE_WARNING = "node_warning"


class PipelineLogEvent(BaseModel):
    timestamp: str
    code: str = Field(min_length=1)
    severity: LogSeverity = "info"
    source: LogSource = "queron"
    message: str = Field(min_length=1)
    details: dict[str, Any] = Field(default_factory=dict)
    run_id: str | None = None
    node_id: str | None = None
    node_name: str | None = None
    node_kind: str | None = None
    artifact_name: str | None = None


class NodeWarningEvent(BaseModel):
    code: str = Field(min_length=1)
    severity: WarningSeverity = "warning"
    source: WarningSource = "queron"
    message: str = Field(min_length=1)
    raw_message: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


def _clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_required_text(value: Any, *, field_name: str) -> str:
    text = _clean_optional_text(value)
    if text is None:
        raise RuntimeError(f"{field_name} is required for a warning event.")
    return text


def quote_compound_relation(value: Any) -> str | None:
    text = _clean_optional_text(value)
    if text is None:
        return None
    if "/" in text or "\\" in text:
        return text
    parts = [part.strip().strip('"') for part in text.split(".") if part.strip()]
    if not parts:
        return None
    return ".".join('"' + part.replace('"', '""') + '"' for part in parts)


def build_warning_event(
    *,
    code: str,
    message: str,
    severity: WarningSeverity = "warning",
    source: WarningSource = "queron",
    raw_message: str | None = None,
    details: Mapping[str, Any] | None = None,
) -> NodeWarningEvent:
    return NodeWarningEvent(
        code=_clean_required_text(code, field_name="code"),
        severity=severity,
        source=source,
        message=_clean_required_text(message, field_name="message"),
        raw_message=_clean_optional_text(raw_message),
        details={str(key): value for key, value in dict(details or {}).items()},
    )


def build_log_event(
    *,
    code: str,
    message: str,
    severity: LogSeverity = "info",
    source: LogSource = "queron",
    details: Mapping[str, Any] | None = None,
    timestamp: str | None = None,
    run_id: str | None = None,
    node_id: str | None = None,
    node_name: str | None = None,
    node_kind: str | None = None,
    artifact_name: str | None = None,
) -> PipelineLogEvent:
    normalized_details = {str(key): value for key, value in dict(details or {}).items()}
    resolved_node_id = (
        _clean_optional_text(node_id)
        or _clean_optional_text(normalized_details.get("node_id"))
        or _clean_optional_text(node_name)
        or _clean_optional_text(normalized_details.get("node_name"))
    )
    return PipelineLogEvent(
        timestamp=str(timestamp or utc_now_timestamp()),
        code=_clean_required_text(code, field_name="code"),
        severity=severity,
        source=source,
        message=_clean_required_text(message, field_name="message"),
        details=normalized_details,
        run_id=_clean_optional_text(run_id),
        node_id=resolved_node_id,
        node_name=_clean_optional_text(node_name),
        node_kind=_clean_optional_text(node_kind),
        artifact_name=_clean_optional_text(artifact_name),
    )


def normalize_log_event(
    event: PipelineLogEvent | Mapping[str, Any] | str,
    *,
    default_code: str = LogCode.PIPELINE_EXECUTION_STARTED,
    default_severity: LogSeverity = "info",
    default_source: LogSource = "queron",
) -> PipelineLogEvent:
    if isinstance(event, PipelineLogEvent):
        return event
    if isinstance(event, str):
        return build_log_event(
            code=default_code,
            message=event,
            severity=default_severity,
            source=default_source,
        )
    if not isinstance(event, Mapping):
        raise RuntimeError("Log events must be strings, mappings, or PipelineLogEvent values.")
    payload = dict(event)
    payload.setdefault("code", default_code)
    payload.setdefault("severity", default_severity)
    payload.setdefault("source", default_source)
    return build_log_event(
        code=payload.get("code"),
        message=payload.get("message"),
        severity=str(payload.get("severity") or default_severity).strip().lower(),
        source=str(payload.get("source") or default_source).strip().lower(),
        details=payload.get("details"),
        timestamp=payload.get("timestamp"),
        run_id=payload.get("run_id"),
        node_id=payload.get("node_id"),
        node_name=payload.get("node_name"),
        node_kind=payload.get("node_kind"),
        artifact_name=payload.get("artifact_name"),
    )


def format_log_event(event: PipelineLogEvent | Mapping[str, Any] | str) -> str:
    item = normalize_log_event(event)
    prefix = f"[pipeline][{item.node_id}]" if str(item.node_id or "").strip() else "[pipeline]"
    text = f"{prefix} {item.message}"
    if not text.endswith("\n"):
        text += "\n"
    return text


def normalize_warning_events(
    warnings: Iterable[str | Mapping[str, Any] | NodeWarningEvent] | None,
    *,
    default_code: str = WarningCode.RUNTIME_WARNING,
    default_severity: WarningSeverity = "warning",
    default_source: WarningSource = "queron",
) -> list[NodeWarningEvent]:
    normalized: list[NodeWarningEvent] = []
    for item in warnings or []:
        if isinstance(item, NodeWarningEvent):
            normalized.append(item)
            continue
        if isinstance(item, str):
            text = _clean_optional_text(item)
            if text is None:
                continue
            normalized.append(
                build_warning_event(
                    code=default_code,
                    message=text,
                    severity=default_severity,
                    source=default_source,
                )
            )
            continue
        if not isinstance(item, Mapping):
            raise RuntimeError("Warning entries must be strings, mappings, or NodeWarningEvent values.")
        payload = dict(item)
        payload.setdefault("code", default_code)
        payload.setdefault("severity", default_severity)
        payload.setdefault("source", default_source)
        normalized.append(
            NodeWarningEvent(
                code=_clean_required_text(payload.get("code"), field_name="code"),
                severity=str(payload.get("severity") or default_severity).strip().lower(),
                source=str(payload.get("source") or default_source).strip().lower(),
                message=_clean_required_text(payload.get("message"), field_name="message"),
                raw_message=_clean_optional_text(payload.get("raw_message")),
                details={str(key): value for key, value in dict(payload.get("details") or {}).items()},
            )
        )
    return normalized


class ColumnMappingRecord(BaseModel):
    ordinal_position: int
    source_column: str
    source_type: str
    target_column: str
    target_type: str
    connector_type: str
    mapping_mode: str
    warnings: list[str] = Field(default_factory=list)
    lossy: bool | None = None

    def warning_events(self) -> list[NodeWarningEvent]:
        return normalize_warning_events(
            self.warnings,
            default_code=WarningCode.COLUMN_MAPPING_WARNING,
            default_source="connector",
        )


class TableLineageRecord(BaseModel):
    parent_kind: str
    parent_name: str
    parent_database: str | None = None
    parent_schema: str | None = None
    parent_table: str | None = None
    connector_type: str | None = None
    via_node: str | None = None


class RunPolicy(BaseModel):
    on_exception: OnExceptionPolicy = "stop"
    on_warning: OnWarningPolicy = "continue"
    persist_node_outcomes: bool = True
    downstream_on_hard_failure: DownstreamHardFailurePolicy = "skip"


class NodeExecutionResult(BaseModel):
    node_name: str = Field(min_length=1)
    node_kind: str = Field(min_length=1)
    artifact_name: str | None = None
    row_count_in: int | None = None
    row_count_out: int | None = None
    artifact_size_bytes: int | None = None
    warnings: list[NodeWarningEvent] = Field(default_factory=list)
    details: dict[str, Any] = Field(default_factory=dict)


class PipelineRunRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    run_id: str = Field(min_length=1)
    compile_id: str | None = None
    pipeline_id: str = Field(min_length=1, validation_alias=AliasChoices("pipeline_id", "notebook_id"))
    pipeline_name: str | None = Field(default=None, validation_alias=AliasChoices("pipeline_name", "notebook_name"))
    target: str | None = None
    artifact_path: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    status: RunStatus
    error_message: str | None = None

    @property
    def notebook_id(self) -> str:
        return self.pipeline_id

    @property
    def notebook_name(self) -> str | None:
        return self.pipeline_name


class NodeRunRecord(BaseModel):
    node_run_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    node_name: str = Field(min_length=1)
    node_kind: str = Field(min_length=1)
    artifact_name: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    status: NodeStatus
    row_count_in: int | None = None
    row_count_out: int | None = None
    artifact_size_bytes: int | None = None
    error_message: str | None = None
    warnings_json: list[NodeWarningEvent] = Field(default_factory=list)
    details_json: dict[str, Any] = Field(default_factory=dict)
    active_node_state_id: str | None = None


class NodeStateRecord(BaseModel):
    node_state_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    node_run_id: str = Field(min_length=1)
    node_name: str = Field(min_length=1)
    state: NodeStatus
    is_active: bool = True
    created_at: str | None = None
    trigger: str | None = None
    details_json: dict[str, Any] = Field(default_factory=dict)


class CompiledContractRecord(BaseModel):
    compile_id: str | None = None
    pipeline_id: str = Field(min_length=1)
    pipeline_name: str | None = None
    pipeline_path: str = Field(min_length=1)
    project_root: str = Field(min_length=1)
    artifact_path: str = Field(min_length=1)
    config_path: str | None = None
    target: str | None = None
    compiled_at: str | None = None
    is_active: bool = True
    contract_hash: str = Field(min_length=1)
    edge_hash: str = Field(min_length=1)
    config_hash: str = Field(min_length=1)
    project_python_hash: str = Field(min_length=1)
    node_hashes_json: list[dict[str, Any]] = Field(default_factory=list)
    edges_json: list[list[str]] = Field(default_factory=list)
    tracked_files_json: list[dict[str, Any]] = Field(default_factory=list)
    external_dependencies_json: list[dict[str, Any]] = Field(default_factory=list)
    spec_json: dict[str, Any] = Field(default_factory=dict)
    diagnostics_json: list[dict[str, Any]] = Field(default_factory=list)


def utc_now_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
