from __future__ import annotations

import json
from pathlib import Path
import threading
import time
import uuid
from typing import Any, Callable

try:
    from .duckdb_driver import connect_duckdb
except ImportError:
    from duckdb_driver import connect_duckdb

from . import adapters
from .bindings import resolve_runtime_binding_value
from .config import load_connections_config, resolve_connection_binding
from .runtime_models import (
    LogCode,
    NodeExecutionResult,
    NodeRunRecord,
    NodeStateRecord,
    NodeWarningEvent,
    PipelineLogEvent,
    PipelineRunRecord,
    RunPolicy,
    TableLineageRecord,
    WarningCode,
    build_log_event,
    build_warning_event,
    normalize_warning_events,
    utc_now_timestamp,
)
from .runtime_vars import render_runtime_sql
from .specs import NodeSpec, PipelineSpec

_FILE_INGRESS_KIND_TO_FORMAT = {
    "csv.ingress": "csv",
    "jsonl.ingress": "jsonl",
    "parquet.ingress": "parquet",
}
_ALL_FILE_INGRESS_KINDS = set(_FILE_INGRESS_KIND_TO_FORMAT) | {"file.ingress"}


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _generate_run_id() -> str:
    return uuid.uuid4().hex[:16]


def _quote_compound_identifier(identifier: str) -> str:
    parts = [part.strip() for part in str(identifier).split(".") if part.strip()]
    if not parts:
        raise RuntimeError("Target table name is required.")
    return ".".join(_quote_identifier(part) for part in parts)


def _split_target_table_name(identifier: str) -> tuple[str, str]:
    parts = [part.strip().strip('"') for part in str(identifier).split(".") if part.strip()]
    if len(parts) == 1:
        return "main", parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise RuntimeError(f"Invalid target table name '{identifier}'.")


def _parse_resolved_relation(identifier: str) -> tuple[str | None, str | None, str | None]:
    text = str(identifier or "").strip()
    if not text:
        return None, None, None
    parts = [part.strip().strip('"') for part in text.split(".") if part.strip()]
    if len(parts) == 1:
        return None, None, parts[0]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return parts[0], parts[1], parts[2]


def _normalize_count_operator(value: str | None) -> str:
    text = str(value or "").strip()
    if text == "=":
        return "=="
    return text


def _coerce_numeric_scalar(value: Any) -> float:
    if isinstance(value, bool):
        raise RuntimeError("Count checks require a numeric scalar result, but received BOOLEAN.")
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        raise RuntimeError("Count checks require a numeric scalar result, but received an empty value.")
    try:
        return float(text)
    except ValueError as exc:
        raise RuntimeError(f"Count checks require a numeric scalar result, but received '{value}'.") from exc


def _coerce_boolean_scalar(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    if text in {"true", "t", "1", "yes", "y"}:
        return True
    if text in {"false", "f", "0", "no", "n"}:
        return False
    raise RuntimeError(f"Boolean checks require a boolean scalar result, but received '{value}'.")


def _build_check_failure(message: str, *, details: dict[str, Any]) -> RuntimeError:
    exc = RuntimeError(message)
    setattr(exc, "queron_details", {str(key): value for key, value in dict(details or {}).items()})
    return exc


class GracefulStopRequested(RuntimeError):
    pass


class ForceStopRequested(RuntimeError):
    pass


def _node_log_id(node: NodeSpec | None) -> str | None:
    if node is None:
        return None
    if node.cell_id is not None:
        prefix = "python" if str(node.kind or "").strip() == "python.ingress" else "sql"
        return f"{prefix}_{int(node.cell_id)}"
    return str(node.name or "").strip() or None


class PipelineRuntime:
    def __init__(
        self,
        *,
        pipeline_id: str,
        run_label: str | None = None,
        compile_id: str | None = None,
        duckdb_path: str,
        working_dir: str | None = None,
        spec: PipelineSpec,
        module_globals: dict[str, Any] | None = None,
        runtime_bindings: dict[str, Any] | None = None,
        runtime_vars: dict[str, Any] | None = None,
        config_bindings: dict[str, dict[str, Any]] | None = None,
        connections_text: str | None = None,
        connections_path: str | None = None,
        run_policy: RunPolicy | dict[str, Any] | None = None,
        on_log: Callable[[PipelineLogEvent], None] | None = None,
    ) -> None:
        self.pipeline_id = pipeline_id
        self.run_label = str(run_label).strip() or None if run_label is not None else None
        self.compile_id = str(compile_id).strip() or None if compile_id is not None else None
        self.duckdb_path = str(Path(duckdb_path).resolve())
        self.working_dir = str(Path(working_dir or Path(self.duckdb_path).parent).resolve())
        self.spec = spec
        self.module_globals = module_globals or {}
        self.run_id = _generate_run_id()
        self.log_path = str(self._resolve_log_path())
        self.runtime_bindings = runtime_bindings or {}
        self.runtime_vars = dict(runtime_vars or {})
        self.config_bindings = config_bindings or {}
        self.connections_config = load_connections_config(connections_text, yaml_path=connections_path)
        self.run_policy = run_policy if isinstance(run_policy, RunPolicy) else RunPolicy.model_validate(run_policy or {})
        self._duckdb_connection_id: str | None = None
        self._on_log = on_log
        self._run_started = False
        self._selected_node_names: set[str] = set()
        self._node_started_at: dict[str, str] = {}
        self._node_terminal_statuses: dict[str, str] = {}
        self._node_run_ids: dict[str, str] = {}
        self._node_active_state_ids: dict[str, str] = {}
        self._pipeline_started_at: str | None = None
        self._stop_request_lock = threading.Lock()
        self._pending_stop_request: dict[str, Any] | None = None
        self._stop_request_handled = False
        self._stop_watcher_shutdown = threading.Event()
        self._stop_watcher_thread: threading.Thread | None = None
        self._active_node_name: str | None = None
        self._active_node_kind: str | None = None
        self._active_force_interruptors: dict[int, Callable[[], Any]] = {}
        self._active_force_interrupt_details: dict[str, Any] = {}
        self._active_interruptor_tokens_by_object: dict[int, int] = {}
        self._active_force_target_node_name: str | None = None
        self._active_force_interrupt_attempted = False
        self._recent_force_target_node_name: str | None = None
        self._recent_force_interrupt_attempted = False
        self._recent_force_interrupt_details: dict[str, Any] = {}
        self._node_progress_row_counts: dict[str, int] = {}
        self._lookup_tables_to_drop: list[dict[str, str]] = []

    def _active_node_force_interrupt_supported(self) -> bool:
        active_kind = str(self._active_node_kind or "").strip().lower()
        return active_kind not in {
            "",
            "python.ingress",
            "db2.ingress",
            "db2.egress",
            "db2.lookup",
            "mssql.ingress",
            "mssql.egress",
            "mssql.lookup",
        }

    def _resolve_log_path(self) -> Path:
        return Path(self.duckdb_path).resolve().parent / "logs" / f"{self.run_id}.jsonl"

    def _resolve_stop_request_path(self) -> Path:
        return Path(self.duckdb_path).resolve().parent / "stop_requests" / f"{self.run_id}.json"

    def _refresh_log_path(self) -> None:
        self.log_path = str(self._resolve_log_path())

    def _write_log_event(self, event: PipelineLogEvent) -> None:
        try:
            log_path = Path(self.log_path).expanduser().resolve()
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(event.model_dump(), ensure_ascii=True) + "\n")
        except Exception:
            pass

    def _resolve_input_path(self, input_path: str | None) -> Path:
        text = str(input_path or "").strip()
        if not text:
            raise RuntimeError("File path is required.")
        candidate = Path(text).expanduser()
        if not candidate.is_absolute():
            candidate = Path(self.working_dir) / candidate
        try:
            resolved = candidate.resolve(strict=True)
        except FileNotFoundError as exc:
            raise RuntimeError(f"File '{candidate}' was not found.") from exc
        if not resolved.is_file():
            raise RuntimeError(f"File '{resolved}' is not a file.")
        return resolved

    def _consume_stop_request_payload(self) -> dict[str, Any] | None:
        with self._stop_request_lock:
            if self._pending_stop_request is not None:
                return dict(self._pending_stop_request)
        payload = self._take_stop_request_payload_from_disk()
        if payload is not None:
            self._accept_stop_request(payload)
        with self._stop_request_lock:
            return dict(self._pending_stop_request) if self._pending_stop_request is not None else None

    def _take_stop_request_payload_from_disk(self) -> dict[str, Any] | None:
        request_path = self._resolve_stop_request_path()
        if not request_path.exists() or not request_path.is_file():
            return None
        payload: dict[str, Any] = {}
        try:
            raw = json.loads(request_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                payload = {str(key): value for key, value in raw.items()}
        except Exception:
            payload = {}
        finally:
            try:
                request_path.unlink()
            except Exception:
                pass
        return payload

    def _start_stop_watcher(self) -> None:
        if self._stop_watcher_thread is not None and self._stop_watcher_thread.is_alive():
            return
        self._stop_watcher_shutdown.clear()
        self._stop_watcher_thread = threading.Thread(
            target=self._stop_watcher_loop,
            name=f"queron-stop-watcher-{self.run_id}",
            daemon=True,
        )
        self._stop_watcher_thread.start()

    def _stop_watcher_loop(self) -> None:
        while not self._stop_watcher_shutdown.is_set():
            payload = self._take_stop_request_payload_from_disk()
            if payload is not None:
                self._accept_stop_request(payload)
            time.sleep(0.1)

    def _shutdown_stop_watcher(self) -> None:
        self._stop_watcher_shutdown.set()
        watcher = self._stop_watcher_thread
        if watcher is not None and watcher.is_alive():
            watcher.join(timeout=0.3)
        self._stop_watcher_thread = None

    def _accept_stop_request(self, payload: dict[str, Any]) -> None:
        callbacks: list[Callable[[], Any]] = []
        with self._stop_request_lock:
            if self._pending_stop_request is not None:
                return
            self._pending_stop_request = dict(payload)
            self._stop_request_handled = True
            stop_mode = str(payload.get("stop_mode") or "graceful").strip().lower()
            if stop_mode == "force" and self._active_node_force_interrupt_supported():
                self._active_force_target_node_name = self._active_node_name
                self._active_force_interrupt_attempted = True
                callbacks = list(self._active_force_interruptors.values())
        for callback in callbacks:
            try:
                result = callback()
                if isinstance(result, dict) and result:
                    with self._stop_request_lock:
                        self._active_force_interrupt_details.update(
                            {str(key): value for key, value in result.items()}
                        )
            except Exception:
                pass

    def register_active_interruptor(self, callback: Callable[[], Any]) -> int:
        token = id(callback)
        with self._stop_request_lock:
            self._active_force_interruptors[token] = callback
        return token

    def unregister_active_interruptor(self, token: int | None) -> None:
        if token is None:
            return
        with self._stop_request_lock:
            self._active_force_interruptors.pop(int(token), None)

    def _begin_active_node(self, node: NodeSpec) -> None:
        with self._stop_request_lock:
            self._recent_force_target_node_name = None
            self._recent_force_interrupt_attempted = False
            self._recent_force_interrupt_details = {}
            self._active_node_name = node.name
            self._active_node_kind = node.kind
            self._active_force_target_node_name = None
            self._active_force_interrupt_attempted = False
            self._active_force_interruptors = {}
            self._active_force_interrupt_details = {}
            self._active_interruptor_tokens_by_object = {}

    def _end_active_node(self) -> None:
        with self._stop_request_lock:
            self._recent_force_target_node_name = self._active_force_target_node_name
            self._recent_force_interrupt_attempted = self._active_force_interrupt_attempted
            self._recent_force_interrupt_details = dict(self._active_force_interrupt_details)
            self._active_node_name = None
            self._active_node_kind = None
            self._active_force_target_node_name = None
            self._active_force_interrupt_attempted = False
            self._active_force_interruptors = {}
            self._active_force_interrupt_details = {}
            self._active_interruptor_tokens_by_object = {}

    def _build_force_stop_exception(
        self,
        *,
        node: NodeSpec | None,
        boundary: str,
        interrupted_current_node: bool,
        python_fallback: bool,
        completed_before_cancellation: bool,
        interruption_error: str | None = None,
    ) -> ForceStopRequested:
        payload = self._consume_stop_request_payload() or {}
        reason = str(payload.get("reason") or "").strip() or "Force stopped by user."
        node_kind = str(node.kind or "").strip().lower() if node is not None else ""
        db2_fallback = bool(python_fallback and node_kind in {"db2.ingress", "db2.egress", "db2.lookup"})
        mssql_fallback = bool(python_fallback and node_kind in {"mssql.ingress", "mssql.egress", "mssql.lookup"})
        if interrupted_current_node and node is not None:
            message = f"{reason} Node '{node.name}' was interrupted."
        elif python_fallback and node is not None:
            if db2_fallback:
                message = f"{reason} DB2 node '{node.name}' completed before stop was applied."
            elif mssql_fallback:
                message = f"{reason} MSSQL node '{node.name}' completed before stop was applied."
            else:
                message = f"{reason} Python node '{node.name}' completed before stop was applied."
        elif completed_before_cancellation and node is not None:
            message = f"{reason} Node '{node.name}' completed before cancellation landed."
        else:
            message = reason
        exc = ForceStopRequested(message)
        with self._stop_request_lock:
            interrupt_details = dict(self._active_force_interrupt_details or self._recent_force_interrupt_details)
        setattr(
            exc,
            "queron_details",
            {
                "stop_requested": True,
                "stop_mode": "force",
                "boundary": str(boundary or "").strip() or "unknown",
                "requested_at": str(payload.get("requested_at") or "").strip() or None,
                "request_path": str(self._resolve_stop_request_path()),
                "node_name": node.name if node is not None else None,
                "node_kind": node.kind if node is not None else None,
                "interrupted_current_node": bool(interrupted_current_node),
                "python_fallback": bool(python_fallback and not db2_fallback),
                "db2_fallback": bool(db2_fallback),
                "completed_before_cancellation": bool(completed_before_cancellation),
                "interruption_error": str(interruption_error or "").strip() or None,
                **interrupt_details,
            },
        )
        return exc

    def coerce_active_exception(self, node: NodeSpec, exc: Exception) -> Exception:
        payload = self._consume_stop_request_payload()
        if payload is None:
            return exc
        stop_mode = str(payload.get("stop_mode") or "graceful").strip().lower()
        if stop_mode != "force":
            return exc
        with self._stop_request_lock:
            target_node_name = self._active_force_target_node_name
            interrupt_attempted = self._active_force_interrupt_attempted
            if not target_node_name and self._recent_force_target_node_name:
                target_node_name = self._recent_force_target_node_name
            if not interrupt_attempted and self._recent_force_interrupt_attempted:
                interrupt_attempted = self._recent_force_interrupt_attempted
        if target_node_name == node.name and interrupt_attempted:
            return self._build_force_stop_exception(
                node=node,
                boundary="during_node",
                interrupted_current_node=True,
                python_fallback=False,
                completed_before_cancellation=False,
                interruption_error=str(exc),
            )
        return exc

    def raise_if_stop_requested(self, *, node: NodeSpec | None = None, boundary: str) -> None:
        payload = self._consume_stop_request_payload()
        if payload is None:
            return
        stop_mode = str(payload.get("stop_mode") or "graceful").strip().lower()
        if stop_mode == "force":
            node_kind = str((node.kind if node is not None else None) or "").strip().lower()
            fallback_node = node_kind in {
                "python.ingress",
                "db2.ingress",
                "db2.egress",
                "db2.lookup",
                "mssql.ingress",
                "mssql.egress",
                "mssql.lookup",
            }
            with self._stop_request_lock:
                target_node_name = self._active_force_target_node_name
                interrupt_attempted = self._active_force_interrupt_attempted
            raise self._build_force_stop_exception(
                node=node,
                boundary=boundary,
                interrupted_current_node=False,
                python_fallback=fallback_node,
                completed_before_cancellation=bool(node is not None and target_node_name == node.name and interrupt_attempted),
            )
        reason = str(payload.get("reason") or "").strip() or "Pipeline stop requested by user."
        exc = GracefulStopRequested(reason)
        setattr(
            exc,
            "queron_details",
            {
                "stop_requested": True,
                "stop_mode": "graceful",
                "boundary": str(boundary or "").strip() or "unknown",
                "requested_at": str(payload.get("requested_at") or "").strip() or None,
                "request_path": str(self._resolve_stop_request_path()),
                "node_name": node.name if node is not None else None,
            },
        )
        raise exc

    def _node_artifact_name(self, node: NodeSpec) -> str | None:
        for candidate in (node.target_table, node.target_relation, node.output_path):
            text = str(candidate or "").strip()
            if text:
                return text
        return None

    def _register_lookup_table_cleanup(self, node: NodeSpec, *, connector: str) -> None:
        if bool(node.retain):
            return
        target_relation = str(node.target_relation or "").strip()
        if not target_relation:
            return
        item = {
            "connector": str(connector or "").strip().lower(),
            "config": str(node.config or "").strip(),
            "table": target_relation,
            "node_name": str(node.name or "").strip(),
        }
        if item not in self._lookup_tables_to_drop:
            self._lookup_tables_to_drop.append(item)

    def _cleanup_lookup_tables(self) -> None:
        pending = list(self._lookup_tables_to_drop)
        self._lookup_tables_to_drop = []
        for item in pending:
            connector = str(item.get("connector") or "").strip().lower()
            table = str(item.get("table") or "").strip()
            config_name = str(item.get("config") or "").strip()
            if not connector or not table or not config_name:
                continue
            try:
                if connector == "postgres":
                    import postgres_core

                    binding = self._binding_for_config(config_name)
                    connection_id = adapters.ensure_postgres_binding(binding, config_name)
                    postgres_core.drop_table_if_exists(connection_id=connection_id, target_table=table)
                elif connector == "db2":
                    import db2_core

                    binding = self._binding_for_config(config_name)
                    connection_id = adapters.ensure_db2_binding(binding, config_name)
                    db2_core.drop_table_if_exists(connection_id=connection_id, target_table=table)
                elif connector == "mssql":
                    import mssql_core

                    binding = self._binding_for_config(config_name)
                    payload = adapters.build_mssql_request_payload(binding, config_name)
                    payload["connection_id"] = adapters.ensure_mssql_binding(binding, config_name)
                    mssql_core.drop_table_if_exists(
                        target_request=mssql_core.MssqlConnectRequest(**payload),
                        target_table=table,
                    )
                self._log_event(
                    code=LogCode.PIPELINE_CLEAN_FINISHED,
                    message=f"Dropped lookup table {table}.",
                    details={"lookup_table": table, "connector": connector},
                )
            except Exception as exc:
                self._log_event(
                    code=LogCode.NODE_WARNING,
                    message=f"Could not drop lookup table {table}: {exc}",
                    severity="warning",
                    details={
                        "lookup_table": table,
                        "connector": connector,
                        "exception_type": type(exc).__name__,
                    },
                )

    def _log_event(
        self,
        *,
        code: str,
        message: str,
        severity: str = "info",
        source: str = "queron",
        details: dict[str, Any] | None = None,
        node: NodeSpec | None = None,
        artifact_name: str | None = None,
    ) -> None:
        if not str(message or "").strip():
            return
        event = build_log_event(
            code=code,
            message=str(message),
            severity=severity,  # type: ignore[arg-type]
            source=source,  # type: ignore[arg-type]
            details=details,
            run_id=self.run_id,
            node_id=_node_log_id(node),
            node_name=node.name if node is not None else None,
            node_kind=node.kind if node is not None else None,
            artifact_name=artifact_name or (self._node_artifact_name(node) if node is not None else None),
        )
        self._write_log_event(event)
        if self._on_log is None:
            return
        try:
            self._on_log(event)
        except Exception:
            pass

    def _log(self, message: str) -> None:
        self._log_event(
            code=LogCode.PIPELINE_EXECUTION_STARTED,
            message=message,
        )

    def _emit_live_event(
        self,
        *,
        code: str,
        message: str,
        severity: str = "info",
        source: str = "queron",
        details: dict[str, Any] | None = None,
        node: NodeSpec | None = None,
        artifact_name: str | None = None,
    ) -> None:
        if self._on_log is None or not str(message or "").strip():
            return
        event = build_log_event(
            code=code,
            message=str(message),
            severity=severity,  # type: ignore[arg-type]
            source=source,  # type: ignore[arg-type]
            details=details,
            run_id=self.run_id,
            node_id=_node_log_id(node),
            node_name=node.name if node is not None else None,
            node_kind=node.kind if node is not None else None,
            artifact_name=artifact_name or (self._node_artifact_name(node) if node is not None else None),
        )
        try:
            self._on_log(event)
        except Exception:
            pass

    def _update_node_extract_progress(self, node: NodeSpec, *, row_count: int, chunk_size: int | None = None) -> None:
        normalized_row_count = max(0, int(row_count))
        self._node_progress_row_counts[node.name] = normalized_row_count
        self._emit_live_event(
            code=LogCode.NODE_ROWS_EXTRACTED,
            message=f"Extracted {normalized_row_count} row(s) into {node.target_table} so far.",
            node=node,
            details={
                "row_count": normalized_row_count,
                "chunk_size": int(chunk_size) if chunk_size is not None else None,
            },
        )

    def _open_tracked_duckdb_connection(self):
        conn = connect_duckdb(self.duckdb_path)
        def _interrupt() -> None:
            interrupt = getattr(conn, "interrupt", None)
            if callable(interrupt):
                try:
                    interrupt()
                except Exception:
                    pass
            try:
                conn.close()
            except Exception:
                pass

        token = self.register_active_interruptor(_interrupt)
        self._active_interruptor_tokens_by_object[id(conn)] = token
        return conn

    def _close_tracked_duckdb_connection(self, conn: Any) -> None:
        token = self._active_interruptor_tokens_by_object.pop(id(conn), None)
        self.unregister_active_interruptor(token)
        try:
            conn.close()
        except Exception:
            pass

    def _ensure_duckdb_connection_id(self) -> str:
        if self._duckdb_connection_id:
            return self._duckdb_connection_id
        self._duckdb_connection_id = adapters.connect_duckdb_runtime(
            pipeline_id=self.pipeline_id,
            database=self.duckdb_path,
        )
        return self._duckdb_connection_id

    def _binding_for_node(self, node: NodeSpec) -> dict[str, Any]:
        config_name = str(node.config or "").strip()
        if not config_name:
            raise RuntimeError(f"No runtime binding was found for config '{node.config}' on node '{node.name}'.")
        return self._binding_for_config(config_name)

    def _binding_for_config(self, config_name: str) -> dict[str, Any]:
        config_name = str(config_name or "").strip()
        binding = self.runtime_bindings.get(config_name)
        if binding is not None:
            return resolve_runtime_binding_value(config_name, binding)
        binding = self.config_bindings.get(config_name)
        if isinstance(binding, dict):
            return binding
        if config_name:
            return resolve_connection_binding(config_name, self.connections_config)
        raise RuntimeError("No runtime binding config was provided.")

    def _resolve_postgres_source_connection_id(self, node: NodeSpec, binding: dict[str, Any]) -> str:
        return adapters.ensure_postgres_binding(binding, str(node.config or node.name))

    def _resolve_db2_source_connection_id(self, node: NodeSpec, binding: dict[str, Any]) -> str:
        return adapters.ensure_db2_binding(binding, str(node.config or node.name))

    def _resolve_mssql_source_connection_id(self, node: NodeSpec, binding: dict[str, Any]) -> str:
        return adapters.ensure_mssql_binding(binding, str(node.config or node.name))

    def _node_lookup(self) -> dict[str, NodeSpec]:
        return self.spec.node_by_name()

    def _selected_nodes(self) -> list[NodeSpec]:
        nodes = self._node_lookup()
        return [nodes[name] for name in self._selected_node_names if name in nodes]

    def _normalize_execution_result(
        self,
        node: NodeSpec,
        result: NodeExecutionResult | dict[str, Any] | None,
    ) -> NodeExecutionResult:
        if isinstance(result, NodeExecutionResult):
            payload = result.model_dump()
        else:
            payload = dict(result or {})
        payload.setdefault("node_name", node.name)
        payload.setdefault("node_kind", node.kind)
        payload.setdefault("artifact_name", self._node_artifact_name(node))
        payload["warnings"] = self._normalize_runtime_warnings(payload.get("warnings"))
        payload.setdefault("details", {})
        return NodeExecutionResult.model_validate(payload)

    def _record_pipeline_run(self, record: PipelineRunRecord) -> None:
        import duckdb_core

        duckdb_core.record_pipeline_run(
            connection_id=self._ensure_duckdb_connection_id(),
            record=record,
        )

    def _record_node_runs(self, records: list[NodeRunRecord]) -> None:
        if not records:
            return
        import duckdb_core

        duckdb_core.record_node_runs(
            connection_id=self._ensure_duckdb_connection_id(),
            records=records,
        )

    def _record_node_states(self, records: list[NodeStateRecord]) -> None:
        if not records:
            return
        import duckdb_core

        duckdb_core.record_node_states(
            connection_id=self._ensure_duckdb_connection_id(),
            records=records,
        )

    def _default_node_run_id(self, node_name: str) -> str:
        return f"{self.run_id}:{node_name}"

    def _ensure_node_run_id(self, node_name: str) -> str:
        existing = self._node_run_ids.get(node_name)
        if existing:
            return existing
        generated = self._default_node_run_id(node_name)
        self._node_run_ids[node_name] = generated
        return generated

    def attach_run_context(self, *, run_id: str, node_runs: list[dict[str, Any]], run_label: str | None = None) -> None:
        self.run_id = str(run_id)
        self.run_label = str(run_label).strip() or None if run_label is not None else None
        self._refresh_log_path()
        self._run_started = False
        self._selected_node_names = set()
        self._pending_stop_request = None
        self._stop_request_handled = False
        self._node_run_ids = {
            str(item.get("node_name") or "").strip(): (
                str(item.get("node_run_id") or "").strip() or self._default_node_run_id(str(item.get("node_name") or ""))
            )
            for item in node_runs
            if str(item.get("node_name") or "").strip()
        }
        self._node_active_state_ids = {
            str(item.get("node_name") or "").strip(): str(item.get("active_node_state_id") or "").strip()
            for item in node_runs
            if str(item.get("node_name") or "").strip() and str(item.get("active_node_state_id") or "").strip()
        }

    def _append_node_state(
        self,
        *,
        node_name: str,
        state: str,
        trigger: str,
        details: dict[str, Any] | None = None,
    ) -> str:
        node_run_id = self._ensure_node_run_id(node_name)
        node_state_id = uuid.uuid4().hex
        self._record_node_states(
            [
                NodeStateRecord(
                    node_state_id=node_state_id,
                    run_id=self.run_id,
                    node_run_id=node_run_id,
                    node_name=node_name,
                    state=state,  # type: ignore[arg-type]
                    is_active=True,
                    created_at=utc_now_timestamp(),
                    trigger=trigger,
                    details_json=dict(details or {}),
                )
            ]
        )
        self._node_active_state_ids[node_name] = node_state_id
        return node_state_id

    def begin_run(self, *, selected_node_names: set[str]) -> None:
        if self._run_started:
            return
        self._run_started = True
        self._pending_stop_request = None
        self._stop_request_handled = False
        self._selected_node_names = set(selected_node_names)
        started_at = utc_now_timestamp()
        if not self._pipeline_started_at:
            self._pipeline_started_at = started_at
        self._start_stop_watcher()
        self._record_pipeline_run(
            PipelineRunRecord(
                run_id=self.run_id,
                run_label=self.run_label,
                log_path=self.log_path,
                compile_id=self.compile_id,
                pipeline_id=self.pipeline_id,
                target=self.spec.target,
                artifact_path=self.duckdb_path,
                started_at=self._pipeline_started_at,
                finished_at=None,
                status="running",
                runtime_vars_json=self.runtime_vars,
                is_final=False,
            )
        )
        if self._node_run_ids:
            return
        node_run_records: list[NodeRunRecord] = []
        node_state_records: list[NodeStateRecord] = []
        for node in self._selected_nodes():
            node_run_id = self._ensure_node_run_id(node.name)
            node_state_id = uuid.uuid4().hex
            self._node_active_state_ids[node.name] = node_state_id
            node_state_records.append(
                NodeStateRecord(
                    node_state_id=node_state_id,
                    run_id=self.run_id,
                    node_run_id=node_run_id,
                    node_name=node.name,
                    state="ready",
                    is_active=True,
                    created_at=started_at,
                    trigger="run_initialized",
                    details_json={},
                )
            )
            node_run_records.append(
                NodeRunRecord(
                    node_run_id=node_run_id,
                    run_id=self.run_id,
                    node_name=node.name,
                    node_kind=node.kind,
                    artifact_name=self._node_artifact_name(node),
                    status="ready",
                    active_node_state_id=node_state_id,
                )
            )
        self._record_node_states(node_state_records)
        self._record_node_runs(node_run_records)

    def mark_node_running(self, node: NodeSpec) -> None:
        started_at = utc_now_timestamp()
        self._node_started_at[node.name] = started_at
        self._node_terminal_statuses[node.name] = "running"
        self._node_progress_row_counts.pop(node.name, None)
        node_state_id = self._append_node_state(
            node_name=node.name,
            state="running",
            trigger="node_started",
            details={"node_kind": node.kind},
        )
        self._record_node_runs(
            [
                NodeRunRecord(
                    node_run_id=self._ensure_node_run_id(node.name),
                    run_id=self.run_id,
                    node_name=node.name,
                    node_kind=node.kind,
                    artifact_name=self._node_artifact_name(node),
                    started_at=started_at,
                    status="running",
                    active_node_state_id=node_state_id,
                )
            ]
        )

    def _normalize_runtime_warnings(
        self,
        warnings: list[NodeWarningEvent | dict[str, Any] | str] | None,
        *,
        default_code: str = WarningCode.RUNTIME_WARNING,
        default_source: str = "queron",
    ) -> list[NodeWarningEvent]:
        seen: set[tuple[str, str, str, str, str, str]] = set()
        normalized: list[NodeWarningEvent] = []
        for item in normalize_warning_events(
            warnings or [],
            default_code=default_code,
            default_source=default_source,
        ):
            details_text = str(sorted(item.details.items()))
            key = (
                item.code,
                item.severity,
                item.source,
                item.message,
                str(item.raw_message or ""),
                details_text,
            )
            if key in seen:
                continue
            seen.add(key)
            normalized.append(item)
        return normalized

    def _artifact_size_bytes_for_node(self, node: NodeSpec) -> int | None:
        import duckdb_core

        target_table = str(node.target_table or "").strip()
        if not target_table:
            return None
        return duckdb_core.get_table_artifact_size_bytes(
            connection_id=self._ensure_duckdb_connection_id(),
            target_table=target_table,
        )

    def _row_count_for_node(self, node: NodeSpec) -> int | None:
        import duckdb_core

        target_table = str(node.target_table or "").strip()
        if not target_table:
            return None
        return duckdb_core.get_table_row_count(
            connection_id=self._ensure_duckdb_connection_id(),
            target_table=target_table,
        )

    def _selected_local_artifact_tables(self) -> list[str]:
        seen: set[str] = set()
        target_tables: list[str] = []
        for node in self._selected_nodes():
            target_table = str(node.target_table or "").strip()
            if not target_table or target_table in seen:
                continue
            seen.add(target_table)
            target_tables.append(target_table)
        return target_tables

    def _completed_local_artifact_tables_for_current_run(self) -> list[str]:
        import duckdb_core

        if not str(self.run_id or "").strip():
            return []
        seen: set[str] = set()
        target_tables: list[str] = []
        for item in duckdb_core.get_node_runs_for_run(
            connection_id=self._ensure_duckdb_connection_id(),
            run_id=self.run_id,
        ):
            status = str(item.get("status") or "").strip().lower()
            if status not in {"complete", "complete_with_warnings", "success", "success_with_warnings"}:
                continue
            target_table = str(item.get("artifact_name") or "").strip()
            if not target_table or target_table in seen:
                continue
            seen.add(target_table)
            target_tables.append(target_table)
        return target_tables

    def mark_node_success(self, node: NodeSpec, result: NodeExecutionResult | dict[str, Any] | None = None) -> None:
        payload = self._normalize_execution_result(node, result)
        row_count_out = payload.row_count_out
        if row_count_out is None:
            row_count_out = self._row_count_for_node(node)
        artifact_size_bytes = payload.artifact_size_bytes
        if artifact_size_bytes is None:
            artifact_size_bytes = self._artifact_size_bytes_for_node(node)
        self._node_terminal_statuses[node.name] = "success_with_warnings" if payload.warnings else "success"
        node_status = "complete_with_warnings" if payload.warnings else "complete"
        for warning in payload.warnings:
            warning_details = dict(warning.details or {})
            warning_details.update(
                {
                    "warning_code": warning.code,
                    "warning_source": warning.source,
                }
            )
            if warning.raw_message is not None:
                warning_details["raw_message"] = warning.raw_message
            self._log_event(
                code=LogCode.NODE_WARNING,
                message=warning.message,
                severity=warning.severity,
                source=warning.source,
                node=node,
                artifact_name=payload.artifact_name,
                details=warning_details,
            )
        node_state_id = self._append_node_state(
            node_name=node.name,
            state=node_status,
            trigger="node_completed",
            details={
                "node_kind": node.kind,
                "artifact_name": payload.artifact_name,
            },
        )
        self._record_node_runs(
            [
                NodeRunRecord(
                    node_run_id=self._ensure_node_run_id(node.name),
                    run_id=self.run_id,
                    node_name=node.name,
                    node_kind=node.kind,
                    artifact_name=payload.artifact_name,
                    started_at=self._node_started_at.get(node.name),
                    finished_at=utc_now_timestamp(),
                    status=node_status,
                    row_count_in=payload.row_count_in,
                    row_count_out=row_count_out,
                    artifact_size_bytes=artifact_size_bytes,
                    warnings_json=payload.warnings,
                    details_json=payload.details,
                    active_node_state_id=node_state_id,
                )
            ]
        )
        self._node_progress_row_counts.pop(node.name, None)

    def mark_node_failed(self, node: NodeSpec, exc: Exception) -> None:
        self._node_terminal_statuses[node.name] = "failed"
        details_json = getattr(exc, "queron_details", None)
        if not isinstance(details_json, dict):
            details_json = {}
        progress_count = self._node_progress_row_counts.pop(node.name, None)
        failure_details = {
            "exception_type": type(exc).__name__,
            **{str(key): value for key, value in details_json.items()},
        }
        if progress_count is not None:
            failure_details["extracted_row_count_so_far"] = int(progress_count)
        self._log_event(
            code=LogCode.NODE_EXECUTION_FAILED,
            message=f"Node '{node.name}' failed: {exc}",
            severity="error",
            node=node,
            details=failure_details,
        )
        node_state_id = self._append_node_state(
            node_name=node.name,
            state="failed",
            trigger="node_failed",
            details=failure_details,
        )
        self._record_node_runs(
            [
                NodeRunRecord(
                    node_run_id=self._ensure_node_run_id(node.name),
                    run_id=self.run_id,
                    node_name=node.name,
                    node_kind=node.kind,
                    artifact_name=self._node_artifact_name(node),
                    started_at=self._node_started_at.get(node.name),
                    finished_at=utc_now_timestamp(),
                    status="failed",
                    error_message=str(exc),
                    warnings_json=self._normalize_runtime_warnings(
                        [
                            build_warning_event(
                                code="node_execution_failed",
                                severity="error",
                                source="queron",
                                message=f"Node '{node.name}' failed during execution.",
                                raw_message=str(exc),
                            )
                        ]
                    ),
                    details_json=failure_details,
                    active_node_state_id=node_state_id,
                )
            ]
        )

    def mark_nodes_skipped(self, nodes: list[NodeSpec], *, reason: str) -> None:
        if not nodes:
            return
        warning = build_warning_event(
            code="node_skipped",
            severity="warning",
            source="queron",
            message=reason,
        )
        for node in nodes:
            self._node_terminal_statuses[node.name] = "skipped"
            self._node_progress_row_counts.pop(node.name, None)
            self._log_event(
                code=LogCode.NODE_SKIPPED,
                message=f"Skipping node '{node.name}': {reason}",
                severity="warning",
                node=node,
            )
        skipped_at = utc_now_timestamp()
        run_records: list[NodeRunRecord] = []
        for node in nodes:
            node_state_id = self._append_node_state(
                node_name=node.name,
                state="skipped",
                trigger="node_skipped",
                details={"reason": reason, "node_kind": node.kind},
            )
            run_records.append(
                NodeRunRecord(
                    node_run_id=self._ensure_node_run_id(node.name),
                    run_id=self.run_id,
                    node_name=node.name,
                    node_kind=node.kind,
                    artifact_name=self._node_artifact_name(node),
                    started_at=self._node_started_at.get(node.name),
                    finished_at=skipped_at,
                    status="skipped",
                    warnings_json=[warning],
                    active_node_state_id=node_state_id,
                )
            )
        self._record_node_runs(run_records)

    def mark_run_success(self) -> None:
        status = (
            "success_with_warnings"
            if any(value == "success_with_warnings" for value in self._node_terminal_statuses.values())
            else "success"
        )
        archived_artifact_path: str | None = None
        try:
            import duckdb_core

            archived = duckdb_core.archive_pipeline_targets(
                connection_id=self._ensure_duckdb_connection_id(),
                run_id=self.run_id,
                target_tables=self._completed_local_artifact_tables_for_current_run(),
            )
            if archived:
                archived_artifact_path = duckdb_core.archived_artifact_path_for_run(
                    database_path=self.duckdb_path,
                    run_id=self.run_id,
                )
        except Exception as exc:
            self._record_pipeline_run(
                PipelineRunRecord(
                    run_id=self.run_id,
                    run_label=self.run_label,
                    log_path=self.log_path,
                    compile_id=self.compile_id,
                    pipeline_id=self.pipeline_id,
                    target=self.spec.target,
                    artifact_path=self.duckdb_path,
                    archived_artifact_path=archived_artifact_path,
                    started_at=self._pipeline_started_at,
                    finished_at=utc_now_timestamp(),
                    status="failed",
                    error_message=f"Artifact archive failed: {exc}",
                    runtime_vars_json=self.runtime_vars,
                    is_final=False,
                )
            )
            raise
        self._cleanup_lookup_tables()
        self._record_pipeline_run(
            PipelineRunRecord(
                run_id=self.run_id,
                run_label=self.run_label,
                log_path=self.log_path,
                compile_id=self.compile_id,
                pipeline_id=self.pipeline_id,
                target=self.spec.target,
                artifact_path=self.duckdb_path,
                archived_artifact_path=archived_artifact_path,
                started_at=self._pipeline_started_at,
                finished_at=utc_now_timestamp(),
                status=status,
                runtime_vars_json=self.runtime_vars,
                is_final=True,
            )
        )
        self._shutdown_stop_watcher()

    def mark_run_failed(self, exc: Exception) -> None:
        self._cleanup_lookup_tables()
        self._record_pipeline_run(
            PipelineRunRecord(
                run_id=self.run_id,
                run_label=self.run_label,
                log_path=self.log_path,
                compile_id=self.compile_id,
                pipeline_id=self.pipeline_id,
                target=self.spec.target,
                artifact_path=self.duckdb_path,
                started_at=self._pipeline_started_at,
                finished_at=utc_now_timestamp(),
                status="failed",
                error_message=str(exc),
                runtime_vars_json=self.runtime_vars,
                is_final=False,
            )
        )
        self._shutdown_stop_watcher()

    def clear_pipeline_outputs(self) -> None:
        self.clear_selected_outputs({node.name for node in self.spec.nodes})

    def reset_selected_node_states(self, node_names: set[str], *, trigger: str) -> None:
        nodes = self._node_lookup()
        selected = [nodes[name] for name in sorted(node_names) if name in nodes]
        if not selected or not self.run_id:
            return
        records: list[NodeRunRecord] = []
        state_records: list[NodeStateRecord] = []
        timestamp = utc_now_timestamp()
        for node in selected:
            node_run_id = self._ensure_node_run_id(node.name)
            cleared_state_id = uuid.uuid4().hex
            ready_state_id = uuid.uuid4().hex
            state_records.append(
                NodeStateRecord(
                    node_state_id=cleared_state_id,
                    run_id=self.run_id,
                    node_run_id=node_run_id,
                    node_name=node.name,
                    state="cleared",
                    is_active=True,
                    created_at=timestamp,
                    trigger=trigger,
                    details_json={"phase": "cleared"},
                )
            )
            state_records.append(
                NodeStateRecord(
                    node_state_id=ready_state_id,
                    run_id=self.run_id,
                    node_run_id=node_run_id,
                    node_name=node.name,
                    state="ready",
                    is_active=True,
                    created_at=timestamp,
                    trigger=trigger,
                    details_json={"phase": "ready"},
                )
            )
            self._node_active_state_ids[node.name] = ready_state_id
            records.append(
                NodeRunRecord(
                    node_run_id=node_run_id,
                    run_id=self.run_id,
                    node_name=node.name,
                    node_kind=node.kind,
                    artifact_name=self._node_artifact_name(node),
                    started_at=None,
                    finished_at=None,
                    status="ready",
                    row_count_in=None,
                    row_count_out=None,
                    artifact_size_bytes=None,
                    error_message=None,
                    warnings_json=[],
                    details_json={"trigger": trigger},
                    active_node_state_id=ready_state_id,
                )
            )
        self._record_node_states(state_records)
        self._record_node_runs(records)

    def existing_output_tables(self, node_names: set[str] | None = None) -> list[str]:
        import duckdb_core

        nodes = self._node_lookup()
        selected_names = set(node_names or nodes.keys())
        target_tables = [
            str(nodes[node_name].target_table or "").strip()
            for node_name in selected_names
            if node_name in nodes and str(nodes[node_name].target_table or "").strip()
        ]
        if not target_tables:
            return []
        return duckdb_core.list_existing_pipeline_targets(
            connection_id=self._ensure_duckdb_connection_id(),
            target_tables=target_tables,
        )

    def clear_selected_outputs(self, node_names: set[str]) -> list[str]:
        import duckdb_core

        nodes = self._node_lookup()
        target_tables = [
            str(nodes[node_name].target_table or "").strip()
            for node_name in node_names
            if node_name in nodes and str(nodes[node_name].target_table or "").strip()
        ]
        if not target_tables:
            self._log_event(
                code=LogCode.PIPELINE_CLEAN_FINISHED,
                message="No existing pipeline outputs to clear.",
                details={"target_tables": []},
            )
            return []
        self._log_event(
            code=LogCode.PIPELINE_CLEAN_STARTED,
            message=f"Dropping {len(target_tables)} existing output table(s).",
            details={"target_tables": list(target_tables)},
        )
        duckdb_core.clear_pipeline_targets(
            connection_id=self._ensure_duckdb_connection_id(),
            target_tables=target_tables,
        )
        self._log_event(
            code=LogCode.PIPELINE_OUTPUTS_CLEARED,
            message=f"Cleared {len(target_tables)} existing output table(s).",
            details={"target_tables": list(target_tables)},
        )
        return target_tables

    def _build_ingress_lineage(self, node: NodeSpec) -> list[TableLineageRecord]:
        lineage: list[TableLineageRecord] = []
        connector_type = node.kind.split(".", 1)[0] if "." in node.kind else node.kind
        for source_name in node.sources:
            parent_database, parent_schema, parent_table = _parse_resolved_relation(node.resolved_sources.get(source_name) or "")
            lineage.append(
                TableLineageRecord(
                    parent_kind="source",
                    parent_name=source_name,
                    parent_database=parent_database,
                    parent_schema=parent_schema,
                    parent_table=parent_table,
                    connector_type=connector_type,
                    via_node=node.name,
                )
            )
        for lookup_name in node.lookups:
            parent_database, parent_schema, parent_table = _parse_resolved_relation(node.resolved_lookups.get(lookup_name) or "")
            lineage.append(
                TableLineageRecord(
                    parent_kind="lookup",
                    parent_name=lookup_name,
                    parent_database=parent_database,
                    parent_schema=parent_schema,
                    parent_table=parent_table,
                    connector_type=connector_type,
                    via_node=node.name,
                )
            )
        if not lineage:
            lineage.append(
                TableLineageRecord(
                    parent_kind="external_query",
                    parent_name=node.name,
                    connector_type=connector_type,
                    via_node=node.name,
                )
            )
        return lineage

    def _build_python_ingress_lineage(self, node: NodeSpec) -> list[TableLineageRecord]:
        return [
            TableLineageRecord(
                parent_kind="python",
                parent_name=node.function_name or node.name,
                connector_type="python",
                via_node=node.name,
            )
        ]

    def _build_file_ingress_lineage(self, node: NodeSpec, *, resolved_path: Path, file_format: str) -> list[TableLineageRecord]:
        return [
            TableLineageRecord(
                parent_kind="file",
                parent_name=str(resolved_path),
                parent_database=str(resolved_path.parent),
                parent_table=resolved_path.name,
                connector_type=f"file.{file_format}",
                via_node=node.name,
            )
        ]

    def _file_format_for_node(self, node: NodeSpec) -> str:
        if node.kind in _FILE_INGRESS_KIND_TO_FORMAT:
            return _FILE_INGRESS_KIND_TO_FORMAT[node.kind]
        file_format = str(node.file_format or "").strip().lower()
        if not file_format:
            raise RuntimeError(f"File ingress node '{node.name}' is missing a file format.")
        return file_format

    def _python_callable_for_node(self, node: NodeSpec) -> Callable[..., Any]:
        fn = self.module_globals.get(node.function_name)
        if not callable(fn):
            fn = self.module_globals.get(node.name)
        if not callable(fn):
            raise RuntimeError(
                f"Python ingress node '{node.name}' could not locate callable '{node.function_name}'."
            )
        return fn

    def _materialize_python_ingress_value(self, node: NodeSpec, value: Any) -> tuple[int, str]:
        import duckdb_core

        try:
            import pandas as pd  # type: ignore
        except Exception:
            pd = None  # type: ignore[assignment]
        try:
            import pyarrow as pa  # type: ignore
        except Exception:
            pa = None  # type: ignore[assignment]

        source_type = None
        if pd is not None and isinstance(value, pd.DataFrame):
            source_type = "pandas.DataFrame"
        elif pa is not None and isinstance(value, pa.Table):
            source_type = "pyarrow.Table"
        else:
            raise RuntimeError(
                "Python ingress functions must return a pandas.DataFrame or pyarrow.Table."
            )

        target_table = str(node.target_table or "").strip()
        if not target_table:
            raise RuntimeError(f"Python ingress node '{node.name}' is missing a target table.")
        target_ident = _quote_compound_identifier(target_table)
        temp_name = f"__queron_python_ingress_{uuid.uuid4().hex[:12]}"
        temp_ident = _quote_identifier(temp_name)
        conn = connect_duckdb(self.duckdb_path)
        try:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier('main')}")
            conn.register(temp_name, value)
            conn.execute(f"CREATE TABLE {target_ident} AS SELECT * FROM {temp_ident}")
            row = conn.execute(f"SELECT COUNT(*) FROM {target_ident}").fetchone()
            column_names = [str(item[0]) for item in (conn.execute(f"DESCRIBE {target_ident}").fetchall() or [])]
        finally:
            try:
                conn.unregister(temp_name)
            except Exception:
                pass
            conn.close()

        duckdb_core.record_table_lineage(
            connection_id=self._ensure_duckdb_connection_id(),
            target_table=target_table,
            lineage=self._build_python_ingress_lineage(node),
        )
        row_count = int(row[0]) if row and row[0] is not None else 0
        return row_count, source_type if not column_names else source_type

    def _build_model_lineage(self, node: NodeSpec) -> list[TableLineageRecord]:
        lineage: list[TableLineageRecord] = []
        nodes = self._node_lookup()
        for dependency_name in node.dependencies:
            parent = nodes.get(dependency_name)
            if parent is None:
                continue
            parent_schema, parent_table = _split_target_table_name(str(parent.target_table or parent.out or parent.name))
            lineage.append(
                TableLineageRecord(
                    parent_kind="artifact",
                    parent_name=str(parent.out or parent.name),
                    parent_schema=parent_schema,
                    parent_table=parent_table,
                    via_node=node.name,
                )
            )
        return lineage

    def _collect_ingress_warnings(self, response: Any) -> list[NodeWarningEvent]:
        warning_events: list[NodeWarningEvent | dict[str, Any] | str] = []
        warning_events.extend(list(getattr(response, "warnings", []) or []))
        for mapping in list(getattr(response, "column_mappings", []) or []):
            if hasattr(mapping, "warning_events"):
                warning_events.extend(mapping.warning_events())
            else:
                warning_events.extend(
                    normalize_warning_events(
                        getattr(mapping, "warnings", []) or [],
                        default_code=WarningCode.COLUMN_MAPPING_WARNING,
                        default_source="connector",
                    )
                )
        return self._normalize_runtime_warnings(
            warning_events,
            default_code=WarningCode.CONNECTOR_WARNING,
            default_source="connector",
        )

    def execute_node(self, node: NodeSpec) -> NodeExecutionResult:
        target_label = str(node.target_table or "").strip() or "(check)"
        target_label = self._node_artifact_name(node) or target_label
        self._log_event(
            code=LogCode.NODE_EXECUTION_STARTED,
            message=f"Running {node.kind} node '{node.name}' -> {target_label}",
            node=node,
        )
        self._begin_active_node(node)
        try:
            if node.kind in {"postgres.ingress", "db2.ingress", "mssql.ingress"}:
                return self._execute_ingress(node)
            if node.kind == "python.ingress":
                return self._execute_python_ingress(node)
            if node.kind in _ALL_FILE_INGRESS_KINDS:
                return self._execute_file_ingress(node)
            if node.kind == "postgres.egress":
                return self._execute_postgres_egress(node)
            if node.kind == "db2.egress":
                return self._execute_db2_egress(node)
            if node.kind == "mssql.egress":
                return self._execute_mssql_egress(node)
            if node.kind == "postgres.lookup":
                return self._execute_postgres_lookup(node)
            if node.kind == "db2.lookup":
                return self._execute_db2_lookup(node)
            if node.kind == "mssql.lookup":
                return self._execute_mssql_lookup(node)
            if node.kind == "model.sql":
                return self._execute_model(node)
            if node.kind == "parquet.egress":
                return self._execute_parquet_egress(node)
            if node.kind == "csv.egress":
                return self._execute_csv_egress(node)
            if node.kind == "jsonl.egress":
                return self._execute_jsonl_egress(node)
            if node.kind == "check.count":
                return self._execute_check_count(node)
            if node.kind == "check.boolean":
                return self._execute_check_boolean(node)
            raise RuntimeError(f"Unsupported Phase 1 node kind '{node.kind}'.")
        finally:
            self._end_active_node()

    def _execute_ingress(self, node: NodeSpec) -> NodeExecutionResult:
        binding = self._binding_for_node(node)
        duckdb_connection_id = self._ensure_duckdb_connection_id()
        sql, sql_params = self._render_node_sql(
            node,
            placeholder_style="format" if node.kind == "postgres.ingress" else "qmark",
        )
        if not sql:
            raise RuntimeError(f"Ingress node '{node.name}' is missing SQL.")
        if node.sources:
            self._log_event(
                code=LogCode.NODE_SOURCES_RESOLVED,
                message=f"Resolved {len(node.sources)} source(s) for node '{node.name}'.",
                node=node,
                details={"sources": list(node.sources), "resolved_sources": dict(node.resolved_sources)},
            )

        if node.kind == "postgres.ingress":
            import duckdb_core
            import postgres_core

            source_connection_id = self._resolve_postgres_source_connection_id(node, binding)
            response = postgres_core.ingest_query_to_duckdb(
                source_connection_id=source_connection_id,
                duckdb_connection_id=duckdb_connection_id,
                sql=sql,
                sql_params=sql_params,
                target_table=str(node.target_table or ""),
                replace=False,
                chunk_size=200,
                pipeline_id=self.pipeline_id,
                on_progress=lambda progress: self._update_node_extract_progress(
                    node,
                    row_count=int(progress.get("row_count") or 0),
                    chunk_size=int(progress.get("chunk_size")) if progress.get("chunk_size") is not None else None,
                ),
                on_interrupt_open=self.register_active_interruptor,
                on_interrupt_close=self.unregister_active_interruptor,
            )
            duckdb_core.record_ingest_column_mappings(
                connection_id=duckdb_connection_id,
                target_table=str(node.target_table or ""),
                node_name=node.name,
                node_kind=node.kind,
                column_mappings=response.column_mappings,
            )
            duckdb_core.record_table_lineage(
                connection_id=duckdb_connection_id,
                target_table=str(node.target_table or ""),
                lineage=self._build_ingress_lineage(node),
            )
        elif node.kind == "db2.ingress":
            import duckdb_core
            import db2_core

            source_connection_id = self._resolve_db2_source_connection_id(node, binding)
            response = db2_core.ingest_query_to_duckdb(
                source_connection_id=source_connection_id,
                duckdb_connection_id=duckdb_connection_id,
                sql=sql,
                sql_params=sql_params,
                target_table=str(node.target_table or ""),
                replace=False,
                chunk_size=200,
                pipeline_id=self.pipeline_id,
                on_progress=lambda progress: self._update_node_extract_progress(
                    node,
                    row_count=int(progress.get("row_count") or 0),
                    chunk_size=int(progress.get("chunk_size")) if progress.get("chunk_size") is not None else None,
                ),
            )
            duckdb_core.record_ingest_column_mappings(
                connection_id=duckdb_connection_id,
                target_table=str(node.target_table or ""),
                node_name=node.name,
                node_kind=node.kind,
                column_mappings=response.column_mappings,
            )
            duckdb_core.record_table_lineage(
                connection_id=duckdb_connection_id,
                target_table=str(node.target_table or ""),
                lineage=self._build_ingress_lineage(node),
            )
        else:
            import duckdb_core
            import mssql_core

            source_payload = adapters.build_mssql_request_payload(binding, str(node.config or node.name))
            source_payload["connection_id"] = self._resolve_mssql_source_connection_id(node, binding)
            response = mssql_core.ingest_query_to_duckdb(
                mssql_core.MssqlConnectRequest(**source_payload),
                sql=sql,
                sql_params=sql_params,
                duckdb_path=self.duckdb_path,
                target_table=str(node.target_table or ""),
                replace=False,
                chunk_size=200,
                on_progress=lambda progress: self._update_node_extract_progress(
                    node,
                    row_count=int(progress.get("row_count") or 0),
                    chunk_size=int(progress.get("chunk_size")) if progress.get("chunk_size") is not None else None,
                ),
                on_interrupt_open=self.register_active_interruptor,
                on_interrupt_close=self.unregister_active_interruptor,
            )
            duckdb_core.record_ingest_column_mappings(
                connection_id=duckdb_connection_id,
                target_table=str(node.target_table or ""),
                node_name=node.name,
                node_kind=node.kind,
                column_mappings=response.column_mappings,
            )
            duckdb_core.record_table_lineage(
                connection_id=duckdb_connection_id,
                target_table=str(node.target_table or ""),
                lineage=self._build_ingress_lineage(node),
            )
        self._log_event(
            code=LogCode.NODE_ROWS_WRITTEN,
            message=f"Wrote {response.row_count} row(s) to {node.target_table}.",
            node=node,
            details={"row_count": int(response.row_count)},
        )
        return NodeExecutionResult(
            node_name=node.name,
            node_kind=node.kind,
            artifact_name=str(node.target_table or "").strip() or None,
            row_count_in=int(response.row_count),
            row_count_out=int(response.row_count),
            warnings=self._collect_ingress_warnings(response),
            details={"column_mappings": [mapping.model_dump() for mapping in response.column_mappings]},
        )

    def _execute_python_ingress(self, node: NodeSpec) -> NodeExecutionResult:
        fn = self._python_callable_for_node(node)
        value = fn()
        row_count, source_type = self._materialize_python_ingress_value(node, value)
        self._log_event(
            code=LogCode.NODE_ROWS_WRITTEN,
            message=f"Materialized {row_count} row(s) from python ingress into {node.target_table}.",
            node=node,
            details={"row_count": row_count, "source_type": source_type},
        )
        return NodeExecutionResult(
            node_name=node.name,
            node_kind=node.kind,
            artifact_name=str(node.target_table or "").strip() or None,
            row_count_in=row_count,
            row_count_out=row_count,
            warnings=[],
            details={"return_type": source_type},
        )

    def _execute_file_ingress(self, node: NodeSpec) -> NodeExecutionResult:
        import duckdb_core

        resolved_path = self._resolve_input_path(node.input_path)
        file_format = self._file_format_for_node(node)
        target_table = str(node.target_table or "").strip()
        if not target_table:
            raise RuntimeError(f"File ingress node '{node.name}' is missing a target table.")

        response = duckdb_core.ingest_file_to_duckdb(
            database=self.duckdb_path,
            input_path=str(resolved_path),
            target_table=target_table,
            file_format=file_format,
            header=bool(node.header if node.header is not None else True),
            delimiter=str(node.delimiter or ","),
            quote=node.quote,
            escape=node.escape,
            skip_rows=max(0, int(node.skip_rows or 0)),
            columns=dict(node.columns or {}) if isinstance(node.columns, dict) else None,
            replace=False,
            on_interrupt_open=self.register_active_interruptor,
            on_interrupt_close=self.unregister_active_interruptor,
        )
        duckdb_core.record_table_lineage(
            connection_id=self._ensure_duckdb_connection_id(),
            target_table=target_table,
            lineage=self._build_file_ingress_lineage(node, resolved_path=resolved_path, file_format=file_format),
        )
        self._log_event(
            code=LogCode.NODE_ROWS_WRITTEN,
            message=f"Wrote {response['row_count']} row(s) from file ingress into {node.target_table}.",
            node=node,
            details={
                "row_count": int(response["row_count"]),
                "format": file_format,
                "path": str(resolved_path),
            },
        )
        return NodeExecutionResult(
            node_name=node.name,
            node_kind=node.kind,
            artifact_name=str(node.target_table or "").strip() or None,
            row_count_in=int(response["row_count"]),
            row_count_out=int(response["row_count"]),
            warnings=[],
            details={
                "path": str(resolved_path),
                "format": file_format,
                "header": bool(node.header if node.header is not None else True) if file_format == "csv" else None,
                "delimiter": str(node.delimiter or ",") if file_format == "csv" else None,
                "columns": list(response.get("columns") or []),
            },
        )

    def _execute_model(self, node: NodeSpec) -> NodeExecutionResult:
        import duckdb_core

        sql, sql_params = self._render_node_sql(node, placeholder_style="qmark")
        sql = sql.strip().rstrip(";")
        if not sql:
            raise RuntimeError(f"Model node '{node.name}' is missing SQL.")
        if node.refs:
            self._log_event(
                code=LogCode.NODE_REFS_RESOLVED,
                message=f"Resolved {len(node.refs)} ref(s) for node '{node.name}'.",
                node=node,
                details={"refs": list(node.refs), "dependencies": list(node.dependencies)},
            )
        target_ident = _quote_compound_identifier(str(node.target_table or ""))
        conn = self._open_tracked_duckdb_connection()
        try:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier('main')}")
            conn.execute(f"CREATE TABLE {target_ident} AS SELECT * FROM ({sql}) AS queron_runtime_model", sql_params)
            row = conn.execute(f"SELECT COUNT(*) FROM {target_ident}").fetchone()
        finally:
            self._close_tracked_duckdb_connection(conn)
        row_count_out = int(row[0]) if row and row[0] is not None else None
        duckdb_core.record_table_lineage(
            connection_id=self._ensure_duckdb_connection_id(),
            target_table=str(node.target_table or ""),
            lineage=self._build_model_lineage(node),
        )
        self._log_event(
            code=LogCode.NODE_ARTIFACT_CREATED,
            message=f"Created artifact {node.target_table}.",
            node=node,
            details={"row_count_out": row_count_out},
        )
        return NodeExecutionResult(
            node_name=node.name,
            node_kind=node.kind,
            artifact_name=str(node.target_table or "").strip() or None,
            row_count_in=None,
            row_count_out=row_count_out,
            warnings=[],
        )

    def _execute_postgres_egress(self, node: NodeSpec) -> NodeExecutionResult:
        import postgres_core

        binding = self._binding_for_node(node)
        sql, sql_params = self._render_node_sql(node, placeholder_style="qmark")
        if not sql:
            raise RuntimeError(f"Egress node '{node.name}' is missing SQL.")
        if node.refs:
            self._log_event(
                code=LogCode.NODE_REFS_RESOLVED,
                message=f"Resolved {len(node.refs)} ref(s) for node '{node.name}'.",
                node=node,
                details={"refs": list(node.refs), "dependencies": list(node.dependencies)},
            )
        local_artifact_name = str(node.target_table or "").strip() or None
        source_connection_id = self._resolve_postgres_source_connection_id(node, binding)
        response = postgres_core.egress_query_from_duckdb(
            target_connection_id=source_connection_id,
            duckdb_database=self.duckdb_path,
            sql=sql,
            sql_params=sql_params,
            target_table=str(node.target_relation or ""),
            mode=str(node.mode or "replace"),
            artifact_table=local_artifact_name,
            on_interrupt_open=self.register_active_interruptor,
            on_interrupt_close=self.unregister_active_interruptor,
        )
        self._log_event(
            code=LogCode.NODE_EGRESS_WRITTEN,
            message=f"Wrote {response.row_count} row(s) to PostgreSQL target {response.target_name}.",
            node=node,
            artifact_name=local_artifact_name or response.target_name,
            details={"row_count": int(response.row_count), "mode": str(node.mode or 'replace').lower()},
        )
        return NodeExecutionResult(
            node_name=node.name,
            node_kind=node.kind,
            artifact_name=local_artifact_name or response.target_name,
            row_count_in=int(response.row_count),
            row_count_out=int(response.row_count),
            warnings=self._normalize_runtime_warnings(
                list(response.warnings or []),
                default_code=WarningCode.EGRESS_WARNING,
                default_source="connector",
            ),
            details={"mode": str(node.mode or "replace").lower()},
        )

    def _execute_db2_egress(self, node: NodeSpec) -> NodeExecutionResult:
        import db2_core

        binding = self._binding_for_node(node)
        sql, sql_params = self._render_node_sql(node, placeholder_style="qmark")
        if not sql:
            raise RuntimeError(f"Egress node '{node.name}' is missing SQL.")
        if node.refs:
            self._log_event(
                code=LogCode.NODE_REFS_RESOLVED,
                message=f"Resolved {len(node.refs)} ref(s) for node '{node.name}'.",
                node=node,
                details={"refs": list(node.refs), "dependencies": list(node.dependencies)},
            )
        local_artifact_name = str(node.target_table or "").strip() or None
        source_connection_id = self._resolve_db2_source_connection_id(node, binding)
        response = db2_core.egress_query_from_duckdb(
            target_connection_id=source_connection_id,
            duckdb_database=self.duckdb_path,
            sql=sql,
            sql_params=sql_params,
            target_table=str(node.target_relation or ""),
            mode=str(node.mode or "replace"),
            artifact_table=local_artifact_name,
        )
        self._log_event(
            code=LogCode.NODE_EGRESS_WRITTEN,
            message=f"Wrote {response.row_count} row(s) to DB2 target {response.target_name}.",
            node=node,
            artifact_name=local_artifact_name or response.target_name,
            details={"row_count": int(response.row_count), "mode": str(node.mode or 'replace').lower()},
        )
        return NodeExecutionResult(
            node_name=node.name,
            node_kind=node.kind,
            artifact_name=local_artifact_name or response.target_name,
            row_count_in=int(response.row_count),
            row_count_out=int(response.row_count),
            warnings=self._normalize_runtime_warnings(
                list(response.warnings or []),
                default_code=WarningCode.EGRESS_WARNING,
                default_source="connector",
            ),
            details={"mode": str(node.mode or "replace").lower()},
        )

    def _execute_mssql_egress(self, node: NodeSpec) -> NodeExecutionResult:
        import mssql_core

        binding = self._binding_for_node(node)
        sql, sql_params = self._render_node_sql(node, placeholder_style="qmark")
        if not sql:
            raise RuntimeError(f"Egress node '{node.name}' is missing SQL.")
        if node.refs:
            self._log_event(
                code=LogCode.NODE_REFS_RESOLVED,
                message=f"Resolved {len(node.refs)} ref(s) for node '{node.name}'.",
                node=node,
                details={"refs": list(node.refs), "dependencies": list(node.dependencies)},
            )
        local_artifact_name = str(node.target_table or "").strip() or None
        target_payload = adapters.build_mssql_request_payload(binding, str(node.config or node.name))
        target_payload["connection_id"] = self._resolve_mssql_source_connection_id(node, binding)
        response = mssql_core.egress_query_from_duckdb(
            target_request=mssql_core.MssqlConnectRequest(**target_payload),
            duckdb_database=self.duckdb_path,
            sql=sql,
            sql_params=sql_params,
            target_table=str(node.target_relation or ""),
            mode=str(node.mode or "replace"),
            artifact_table=local_artifact_name,
            on_interrupt_open=self.register_active_interruptor,
            on_interrupt_close=self.unregister_active_interruptor,
        )
        self._log_event(
            code=LogCode.NODE_EGRESS_WRITTEN,
            message=f"Wrote {response.row_count} row(s) to MSSQL target {response.target_name}.",
            node=node,
            artifact_name=local_artifact_name or response.target_name,
            details={"row_count": int(response.row_count), "mode": str(node.mode or 'replace').lower()},
        )
        return NodeExecutionResult(
            node_name=node.name,
            node_kind=node.kind,
            artifact_name=local_artifact_name or response.target_name,
            row_count_in=int(response.row_count),
            row_count_out=int(response.row_count),
            warnings=self._normalize_runtime_warnings(
                list(response.warnings or []),
                default_code=WarningCode.EGRESS_WARNING,
                default_source="connector",
            ),
            details={"mode": str(node.mode or "replace").lower()},
        )

    def _execute_postgres_lookup(self, node: NodeSpec) -> NodeExecutionResult:
        import postgres_core

        binding = self._binding_for_node(node)
        sql, sql_params = self._render_node_sql(node, placeholder_style="qmark")
        if not sql:
            raise RuntimeError(f"Lookup node '{node.name}' is missing SQL.")
        if node.refs:
            self._log_event(
                code=LogCode.NODE_REFS_RESOLVED,
                message=f"Resolved {len(node.refs)} ref(s) for node '{node.name}'.",
                node=node,
                details={"refs": list(node.refs), "dependencies": list(node.dependencies)},
            )
        local_artifact_name = str(node.target_table or "").strip() or None
        source_connection_id = self._resolve_postgres_source_connection_id(node, binding)
        self._register_lookup_table_cleanup(node, connector="postgres")
        response = postgres_core.egress_query_from_duckdb(
            target_connection_id=source_connection_id,
            duckdb_database=self.duckdb_path,
            sql=sql,
            sql_params=sql_params,
            target_table=str(node.target_relation or ""),
            mode=str(node.mode or "replace"),
            artifact_table=local_artifact_name,
            on_interrupt_open=self.register_active_interruptor,
            on_interrupt_close=self.unregister_active_interruptor,
        )
        self._log_event(
            code=LogCode.NODE_EGRESS_WRITTEN,
            message=f"Wrote {response.row_count} row(s) to PostgreSQL lookup table {response.target_name}.",
            node=node,
            artifact_name=local_artifact_name or response.target_name,
            details={
                "row_count": int(response.row_count),
                "mode": str(node.mode or 'replace').lower(),
                "lookup_table": response.target_name,
                "retain": bool(node.retain),
            },
        )
        return NodeExecutionResult(
            node_name=node.name,
            node_kind=node.kind,
            artifact_name=local_artifact_name or response.target_name,
            row_count_in=int(response.row_count),
            row_count_out=int(response.row_count),
            warnings=self._normalize_runtime_warnings(
                list(response.warnings or []),
                default_code=WarningCode.EGRESS_WARNING,
                default_source="connector",
            ),
            details={"mode": str(node.mode or "replace").lower(), "lookup_table": response.target_name, "retain": bool(node.retain)},
        )

    def _execute_db2_lookup(self, node: NodeSpec) -> NodeExecutionResult:
        import db2_core

        binding = self._binding_for_node(node)
        sql, sql_params = self._render_node_sql(node, placeholder_style="qmark")
        if not sql:
            raise RuntimeError(f"Lookup node '{node.name}' is missing SQL.")
        if node.refs:
            self._log_event(
                code=LogCode.NODE_REFS_RESOLVED,
                message=f"Resolved {len(node.refs)} ref(s) for node '{node.name}'.",
                node=node,
                details={"refs": list(node.refs), "dependencies": list(node.dependencies)},
            )
        local_artifact_name = str(node.target_table or "").strip() or None
        source_connection_id = self._resolve_db2_source_connection_id(node, binding)
        self._register_lookup_table_cleanup(node, connector="db2")
        response = db2_core.egress_query_from_duckdb(
            target_connection_id=source_connection_id,
            duckdb_database=self.duckdb_path,
            sql=sql,
            sql_params=sql_params,
            target_table=str(node.target_relation or ""),
            mode=str(node.mode or "replace"),
            artifact_table=local_artifact_name,
        )
        self._log_event(
            code=LogCode.NODE_EGRESS_WRITTEN,
            message=f"Wrote {response.row_count} row(s) to DB2 lookup table {response.target_name}.",
            node=node,
            artifact_name=local_artifact_name or response.target_name,
            details={
                "row_count": int(response.row_count),
                "mode": str(node.mode or 'replace').lower(),
                "lookup_table": response.target_name,
                "retain": bool(node.retain),
            },
        )
        return NodeExecutionResult(
            node_name=node.name,
            node_kind=node.kind,
            artifact_name=local_artifact_name or response.target_name,
            row_count_in=int(response.row_count),
            row_count_out=int(response.row_count),
            warnings=self._normalize_runtime_warnings(
                list(response.warnings or []),
                default_code=WarningCode.EGRESS_WARNING,
                default_source="connector",
            ),
            details={"mode": str(node.mode or "replace").lower(), "lookup_table": response.target_name, "retain": bool(node.retain)},
        )

    def _execute_mssql_lookup(self, node: NodeSpec) -> NodeExecutionResult:
        import mssql_core

        binding = self._binding_for_node(node)
        sql, sql_params = self._render_node_sql(node, placeholder_style="qmark")
        if not sql:
            raise RuntimeError(f"Lookup node '{node.name}' is missing SQL.")
        if node.refs:
            self._log_event(
                code=LogCode.NODE_REFS_RESOLVED,
                message=f"Resolved {len(node.refs)} ref(s) for node '{node.name}'.",
                node=node,
                details={"refs": list(node.refs), "dependencies": list(node.dependencies)},
            )
        local_artifact_name = str(node.target_table or "").strip() or None
        target_payload = adapters.build_mssql_request_payload(binding, str(node.config or node.name))
        target_payload["connection_id"] = self._resolve_mssql_source_connection_id(node, binding)
        self._register_lookup_table_cleanup(node, connector="mssql")
        response = mssql_core.egress_query_from_duckdb(
            target_request=mssql_core.MssqlConnectRequest(**target_payload),
            duckdb_database=self.duckdb_path,
            sql=sql,
            sql_params=sql_params,
            target_table=str(node.target_relation or ""),
            mode=str(node.mode or "replace"),
            artifact_table=local_artifact_name,
            on_interrupt_open=self.register_active_interruptor,
            on_interrupt_close=self.unregister_active_interruptor,
        )
        self._log_event(
            code=LogCode.NODE_EGRESS_WRITTEN,
            message=f"Wrote {response.row_count} row(s) to MSSQL lookup table {response.target_name}.",
            node=node,
            artifact_name=local_artifact_name or response.target_name,
            details={
                "row_count": int(response.row_count),
                "mode": str(node.mode or 'replace').lower(),
                "lookup_table": response.target_name,
                "retain": bool(node.retain),
            },
        )
        return NodeExecutionResult(
            node_name=node.name,
            node_kind=node.kind,
            artifact_name=local_artifact_name or response.target_name,
            row_count_in=int(response.row_count),
            row_count_out=int(response.row_count),
            warnings=self._normalize_runtime_warnings(
                list(response.warnings or []),
                default_code=WarningCode.EGRESS_WARNING,
                default_source="connector",
            ),
            details={"mode": str(node.mode or "replace").lower(), "lookup_table": response.target_name, "retain": bool(node.retain)},
        )

    def _execute_parquet_egress(self, node: NodeSpec) -> NodeExecutionResult:
        import duckdb_core

        sql, sql_params = self._render_node_sql(node, placeholder_style="qmark")
        if not sql:
            raise RuntimeError(f"Export node '{node.name}' is missing SQL.")
        if node.refs:
            self._log_event(
                code=LogCode.NODE_REFS_RESOLVED,
                message=f"Resolved {len(node.refs)} ref(s) for node '{node.name}'.",
                node=node,
                details={"refs": list(node.refs), "dependencies": list(node.dependencies)},
            )
        local_artifact_name = str(node.target_table or "").strip() or None
        if local_artifact_name:
            response = duckdb_core.export_query_to_parquet_with_artifact(
                database=self.duckdb_path,
                sql=sql,
                sql_params=sql_params,
                output_path=str(node.output_path or ""),
                target_table=local_artifact_name,
                overwrite=bool(node.overwrite),
                compression=str(node.compression or "").strip() or None,
                working_dir=self.working_dir,
                on_interrupt_open=self.register_active_interruptor,
                on_interrupt_close=self.unregister_active_interruptor,
            )
        else:
            response = duckdb_core.export_query_to_parquet(
                database=self.duckdb_path,
                sql=sql,
                sql_params=sql_params,
                output_path=str(node.output_path or ""),
                overwrite=bool(node.overwrite),
                compression=str(node.compression or "").strip() or None,
                working_dir=self.working_dir,
                on_interrupt_open=self.register_active_interruptor,
                on_interrupt_close=self.unregister_active_interruptor,
            )
        self._log_event(
            code=LogCode.NODE_EXPORT_WRITTEN,
            message=f"Exported {response.row_count} row(s) to {response.output_path}.",
            node=node,
            artifact_name=local_artifact_name or response.output_path,
            details={
                "row_count": int(response.row_count),
                "format": response.export_format,
                "output_path": response.output_path,
            },
        )
        return NodeExecutionResult(
            node_name=node.name,
            node_kind=node.kind,
            artifact_name=local_artifact_name or response.output_path,
            row_count_in=int(response.row_count),
            row_count_out=int(response.row_count),
            artifact_size_bytes=response.file_size_bytes,
            warnings=self._normalize_runtime_warnings(
                list(response.warnings or []),
                default_code=WarningCode.EXPORT_WARNING,
                default_source="connector",
            ),
            details={"format": response.export_format, "output_path": response.output_path},
        )

    def _execute_csv_egress(self, node: NodeSpec) -> NodeExecutionResult:
        import duckdb_core

        sql, sql_params = self._render_node_sql(node, placeholder_style="qmark")
        if not sql:
            raise RuntimeError(f"Export node '{node.name}' is missing SQL.")
        if node.refs:
            self._log_event(
                code=LogCode.NODE_REFS_RESOLVED,
                message=f"Resolved {len(node.refs)} ref(s) for node '{node.name}'.",
                node=node,
                details={"refs": list(node.refs), "dependencies": list(node.dependencies)},
            )
        local_artifact_name = str(node.target_table or "").strip() or None
        if local_artifact_name:
            response = duckdb_core.export_query_to_csv_with_artifact(
                database=self.duckdb_path,
                sql=sql,
                sql_params=sql_params,
                output_path=str(node.output_path or ""),
                target_table=local_artifact_name,
                overwrite=bool(node.overwrite),
                header=bool(node.header if node.header is not None else True),
                delimiter=str(node.delimiter or ","),
                working_dir=self.working_dir,
                on_interrupt_open=self.register_active_interruptor,
                on_interrupt_close=self.unregister_active_interruptor,
            )
        else:
            response = duckdb_core.export_query_to_csv(
                database=self.duckdb_path,
                sql=sql,
                sql_params=sql_params,
                output_path=str(node.output_path or ""),
                overwrite=bool(node.overwrite),
                header=bool(node.header if node.header is not None else True),
                delimiter=str(node.delimiter or ","),
                working_dir=self.working_dir,
                on_interrupt_open=self.register_active_interruptor,
                on_interrupt_close=self.unregister_active_interruptor,
            )
        self._log_event(
            code=LogCode.NODE_EXPORT_WRITTEN,
            message=f"Exported {response.row_count} row(s) to {response.output_path}.",
            node=node,
            artifact_name=local_artifact_name or response.output_path,
            details={
                "row_count": int(response.row_count),
                "format": response.export_format,
                "output_path": response.output_path,
            },
        )
        return NodeExecutionResult(
            node_name=node.name,
            node_kind=node.kind,
            artifact_name=local_artifact_name or response.output_path,
            row_count_in=int(response.row_count),
            row_count_out=int(response.row_count),
            artifact_size_bytes=response.file_size_bytes,
            warnings=self._normalize_runtime_warnings(
                list(response.warnings or []),
                default_code=WarningCode.EXPORT_WARNING,
                default_source="connector",
            ),
            details={"format": response.export_format, "output_path": response.output_path},
        )

    def _execute_jsonl_egress(self, node: NodeSpec) -> NodeExecutionResult:
        import duckdb_core

        sql, sql_params = self._render_node_sql(node, placeholder_style="qmark")
        if not sql:
            raise RuntimeError(f"Export node '{node.name}' is missing SQL.")
        if node.refs:
            self._log_event(
                code=LogCode.NODE_REFS_RESOLVED,
                message=f"Resolved {len(node.refs)} ref(s) for node '{node.name}'.",
                node=node,
                details={"refs": list(node.refs), "dependencies": list(node.dependencies)},
            )
        local_artifact_name = str(node.target_table or "").strip() or None
        if local_artifact_name:
            response = duckdb_core.export_query_to_jsonl_with_artifact(
                database=self.duckdb_path,
                sql=sql,
                sql_params=sql_params,
                output_path=str(node.output_path or ""),
                target_table=local_artifact_name,
                overwrite=bool(node.overwrite),
                working_dir=self.working_dir,
                on_interrupt_open=self.register_active_interruptor,
                on_interrupt_close=self.unregister_active_interruptor,
            )
        else:
            response = duckdb_core.export_query_to_jsonl(
                database=self.duckdb_path,
                sql=sql,
                sql_params=sql_params,
                output_path=str(node.output_path or ""),
                overwrite=bool(node.overwrite),
                working_dir=self.working_dir,
                on_interrupt_open=self.register_active_interruptor,
                on_interrupt_close=self.unregister_active_interruptor,
            )
        self._log_event(
            code=LogCode.NODE_EXPORT_WRITTEN,
            message=f"Exported {response.row_count} row(s) to {response.output_path}.",
            node=node,
            artifact_name=local_artifact_name or response.output_path,
            details={
                "row_count": int(response.row_count),
                "format": response.export_format,
                "output_path": response.output_path,
            },
        )
        return NodeExecutionResult(
            node_name=node.name,
            node_kind=node.kind,
            artifact_name=local_artifact_name or response.output_path,
            row_count_in=int(response.row_count),
            row_count_out=int(response.row_count),
            artifact_size_bytes=response.file_size_bytes,
            warnings=self._normalize_runtime_warnings(
                list(response.warnings or []),
                default_code=WarningCode.EXPORT_WARNING,
                default_source="connector",
            ),
            details={"format": response.export_format, "output_path": response.output_path},
        )

    def _execute_scalar_duckdb_query(self, node: NodeSpec) -> Any:
        sql, sql_params = self._render_node_sql(node, placeholder_style="qmark")
        sql = sql.strip().rstrip(";")
        if not sql:
            raise RuntimeError(f"Check node '{node.name}' is missing SQL.")
        if node.refs:
            self._log_event(
                code=LogCode.NODE_REFS_RESOLVED,
                message=f"Resolved {len(node.refs)} ref(s) for node '{node.name}'.",
                node=node,
                details={"refs": list(node.refs), "dependencies": list(node.dependencies)},
            )
        rows: list[tuple[Any, ...]] = []
        conn = self._open_tracked_duckdb_connection()
        try:
            rows = conn.execute(sql, sql_params).fetchall()
        finally:
            self._close_tracked_duckdb_connection(conn)
        if len(rows) != 1:
            raise RuntimeError(
                f"Check node '{node.name}' must return exactly one row, but returned {len(rows)}."
            )
        row = rows[0]
        if len(row) != 1:
            raise RuntimeError(
                f"Check node '{node.name}' must return exactly one column, but returned {len(row)}."
            )
        return row[0]

    def _execute_check_count(self, node: NodeSpec) -> NodeExecutionResult:
        actual_value = _coerce_numeric_scalar(self._execute_scalar_duckdb_query(node))
        expected_value = _coerce_numeric_scalar(node.value)
        operator = _normalize_count_operator(node.operator)

        comparisons = {
            "==": actual_value == expected_value,
            "!=": actual_value != expected_value,
            ">": actual_value > expected_value,
            ">=": actual_value >= expected_value,
            "<": actual_value < expected_value,
            "<=": actual_value <= expected_value,
        }
        if operator not in comparisons:
            raise RuntimeError(
                f"Count check node '{node.name}' uses unsupported operator '{node.operator}'."
            )
        if comparisons[operator]:
            raise _build_check_failure(
                f"Count check '{node.name}' failed: result {actual_value:g} {operator} {expected_value:g}.",
                details={"actual_value": actual_value, "operator": operator, "expected_value": expected_value},
            )
        self._log_event(
            code=LogCode.NODE_CHECK_PASSED,
            message=f"Count check '{node.name}' passed with result {actual_value:g}.",
            node=node,
            details={"actual_value": actual_value, "operator": operator, "expected_value": expected_value},
        )
        return NodeExecutionResult(
            node_name=node.name,
            node_kind=node.kind,
            artifact_name=None,
            row_count_in=None,
            row_count_out=None,
            warnings=[],
            details={"actual_value": actual_value, "operator": operator, "expected_value": expected_value},
        )

    def _execute_check_boolean(self, node: NodeSpec) -> NodeExecutionResult:
        actual_value = _coerce_boolean_scalar(self._execute_scalar_duckdb_query(node))
        if actual_value:
            raise _build_check_failure(
                f"Boolean check '{node.name}' failed: result was TRUE.",
                details={"actual_value": actual_value},
            )
        self._log_event(
            code=LogCode.NODE_CHECK_PASSED,
            message=f"Boolean check '{node.name}' passed with result FALSE.",
            node=node,
            details={"actual_value": actual_value},
        )
        return NodeExecutionResult(
            node_name=node.name,
            node_kind=node.kind,
            artifact_name=None,
            row_count_in=None,
            row_count_out=None,
            warnings=[],
            details={"actual_value": actual_value},
        )

    def _render_node_sql(self, node: NodeSpec, *, placeholder_style: str) -> tuple[str, list[Any]]:
        sql_text = str(node.resolved_sql or node.sql or "")
        if placeholder_style == "format":
            return render_runtime_sql(
                sql_text,
                runtime_vars=self.runtime_vars,
                placeholder_factory=lambda _index: "%s",
            )
        return render_runtime_sql(
            sql_text,
            runtime_vars=self.runtime_vars,
            placeholder_factory=lambda _index: "?",
        )
