from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from queue import Empty, Queue
import tempfile
import threading
from datetime import date, datetime, time
from typing import Any
from urllib.parse import parse_qs, urlparse

from duckdb_driver import connect_duckdb
from .api import (
    _inspect_pipeline_logs,
    _inspect_pipeline_runs,
    has_compile_errors,
    inspect_dag,
    inspect_node,
    inspect_node_history,
    inspect_node_logs,
    inspect_node_query,
    list_existing_outputs_for_file,
    reset_all,
    reset_node,
    reset_upstream,
    resume_pipeline,
    run_pipeline,
    stop_pipeline,
    force_stop_pipeline,
)
from .cli import _load_pipeline_namespace, _pipeline_id_from_file, _runtime_bindings_from_file
from .runtime_models import LogCode, PipelineLogEvent


@dataclass
class GraphLiveContext:
    pipeline_path: str
    artifact_path: str
    pipeline_id: str


class _EventBroker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._subscribers: set[Queue[dict[str, Any]]] = set()

    def subscribe(self) -> Queue[dict[str, Any]]:
        queue: Queue[dict[str, Any]] = Queue()
        with self._lock:
            self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: Queue[dict[str, Any]]) -> None:
        with self._lock:
            self._subscribers.discard(queue)

    def publish(self, payload: dict[str, Any]) -> None:
        with self._lock:
            subscribers = list(self._subscribers)
        for queue in subscribers:
            queue.put(payload)


def _graph_event_from_log(event: PipelineLogEvent) -> dict[str, Any]:
    return {
        "type": "runtime_log",
        "timestamp": event.timestamp,
        "code": event.code,
        "severity": event.severity,
        "message": event.message,
        "details": dict(event.details or {}),
        "run_id": event.run_id,
        "node_id": event.node_id,
        "node_name": event.node_name,
        "node_kind": event.node_kind,
        "artifact_name": event.artifact_name,
    }
def resolve_graph_live_context(pipeline_path: str | Path) -> GraphLiveContext:
    resolved_pipeline_path = Path(pipeline_path).expanduser().resolve()
    if not resolved_pipeline_path.exists() or not resolved_pipeline_path.is_file():
        raise RuntimeError(f"Pipeline file '{resolved_pipeline_path}' was not found.")
    pipeline_id = _pipeline_id_from_file(resolved_pipeline_path)
    artifact_path = (resolved_pipeline_path.parent / ".queron" / pipeline_id / "artifact.duckdb").resolve()
    if not artifact_path.exists() or not artifact_path.is_file():
        raise RuntimeError(
            f"Artifact database '{artifact_path}' was not found. Compile or run the pipeline first."
        )
    return GraphLiveContext(
        pipeline_path=str(resolved_pipeline_path),
        artifact_path=str(artifact_path),
        pipeline_id=pipeline_id,
    )


def _normalized_selection(run_id: str | None = None, run_label: str | None = None) -> tuple[str | None, str | None]:
    normalized_run_id = str(run_id or "").strip() or None
    normalized_run_label = str(run_label or "").strip() or None
    return normalized_run_id, normalized_run_label


def _quote_identifier(value: str) -> str:
    return f"\"{str(value or '').replace('\"', '\"\"')}\""


def _qualified_artifact_name(artifact_name: str) -> str:
    text = str(artifact_name or "").strip()
    if not text or "." not in text:
        raise RuntimeError("Selected node does not have a queryable local artifact table.")
    schema_name, table_name = text.split(".", 1)
    schema_name = schema_name.strip().strip('"')
    table_name = table_name.strip().strip('"')
    if not schema_name or not table_name:
        raise RuntimeError("Selected node does not have a queryable local artifact table.")
    return f"{_quote_identifier(schema_name)}.{_quote_identifier(table_name)}"


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def _selected_node_artifact_record(
    artifact_path: str | Path,
    node_name: str,
    *,
    run_id: str | None = None,
    run_label: str | None = None,
) -> dict[str, Any]:
    normalized_run_id, normalized_run_label = _normalized_selection(run_id, run_label)
    selection = inspect_node(
        artifact_path,
        node_name,
        run_id=normalized_run_id,
        run_label=normalized_run_label,
    )
    selected_node = next(
        (item for item in selection.nodes if str(item.get("name") or "").strip() == str(node_name).strip()),
        selection.nodes[0] if selection.nodes else None,
    )
    if not selected_node:
        raise RuntimeError(f"Node '{node_name}' was not found.")
    artifact_name = str(selected_node.get("artifact_name") or "").strip() or None
    if not artifact_name:
        raise RuntimeError(f"Node '{node_name}' does not have a materialized artifact for the selected run.")
    return {
        "artifact_name": artifact_name,
        "artifact_path": str(selected_node.get("artifact_path") or "").strip()
        or str(selected_node.get("archived_artifact_path") or "").strip()
        or str(Path(artifact_path).resolve()),
        "archived_artifact_name": str(selected_node.get("archived_artifact_name") or "").strip() or None,
        "logical_artifact": str(selected_node.get("logical_artifact") or "").strip() or None,
        "run_id": selection.run_id,
        "run_label": selection.run_label,
        "run_status": selection.run_status,
    }


def _artifact_preview_query(artifact_name: str, *, limit: int) -> str:
    resolved_limit = max(1, min(int(limit), 100))
    return f"SELECT * FROM {_qualified_artifact_name(artifact_name)} LIMIT {resolved_limit}"


def _validate_read_only_sql(sql: str) -> str:
    text = str(sql or "").strip().rstrip(";").strip()
    if not text:
        raise RuntimeError("sql is required.")
    if not text.lower().startswith("select"):
        raise RuntimeError("Only SELECT queries are supported.")
    return text


def _execute_artifact_query(database_path: str | Path, sql: str) -> dict[str, Any]:
    conn = connect_duckdb(str(Path(database_path).resolve()))
    try:
        cursor = conn.execute(sql)
        column_names = [str(item[0]) for item in list(cursor.description or [])]
        rows = cursor.fetchall()
        row_dicts = [
            {column_names[index]: _json_value(value) for index, value in enumerate(row)}
            for row in rows
        ]
        return {
            "columns": column_names,
            "rows": row_dicts,
            "row_count": len(row_dicts),
        }
    finally:
        conn.close()


def _artifact_query_context(
    artifact_path: str | Path,
    node_name: str,
    *,
    sql: str,
    run_id: str | None = None,
    run_label: str | None = None,
) -> dict[str, Any]:
    selected = _selected_node_artifact_record(
        artifact_path,
        node_name,
        run_id=run_id,
        run_label=run_label,
    )
    validated_sql = _validate_read_only_sql(sql).replace(
        "{{artifact}}",
        _qualified_artifact_name(selected["artifact_name"]),
    )
    return {
        **selected,
        "sql": validated_sql,
    }


def _download_filename(*, node_name: str, run_id: str | None, export_format: str) -> str:
    extension = "json" if export_format == "json" else export_format
    suffix = f"_{str(run_id).strip()}" if str(run_id or "").strip() else ""
    return f"{str(node_name or 'artifact').strip()}{suffix}.{extension}"


def export_node_artifact_query_panel(
    artifact_path: str | Path,
    node_name: str,
    *,
    sql: str,
    format: str,
    run_id: str | None = None,
    run_label: str | None = None,
) -> dict[str, Any]:
    query_context = _artifact_query_context(
        artifact_path,
        node_name,
        sql=sql,
        run_id=run_id,
        run_label=run_label,
    )
    export_format = str(format or "").strip().lower()
    if export_format not in {"csv", "parquet", "json"}:
        raise RuntimeError("format must be one of: csv, parquet, json.")

    import duckdb_core

    temp_dir = Path(tempfile.gettempdir()).resolve()
    output_path = temp_dir / f"queron_graph_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}.{ 'json' if export_format == 'json' else export_format }"
    try:
        if export_format == "csv":
            export = duckdb_core.export_query_to_csv(
                database=query_context["artifact_path"],
                sql=query_context["sql"],
                output_path=str(output_path),
                overwrite=True,
            )
            content_type = "text/csv; charset=utf-8"
        elif export_format == "parquet":
            export = duckdb_core.export_query_to_parquet(
                database=query_context["artifact_path"],
                sql=query_context["sql"],
                output_path=str(output_path),
                overwrite=True,
            )
            content_type = "application/octet-stream"
        else:
            export = duckdb_core.export_query_to_json(
                database=query_context["artifact_path"],
                sql=query_context["sql"],
                output_path=str(output_path),
                overwrite=True,
            )
            content_type = "application/json; charset=utf-8"
        file_bytes = Path(export.output_path).read_bytes()
        return {
            "filename": _download_filename(node_name=node_name, run_id=query_context["run_id"], export_format=export_format),
            "content_type": content_type,
            "body": file_bytes,
        }
    finally:
        if output_path.exists():
            output_path.unlink(missing_ok=True)


def get_graph_panel(
    artifact_path: str | Path,
    *,
    run_id: str | None = None,
    run_label: str | None = None,
) -> dict[str, Any]:
    normalized_run_id, normalized_run_label = _normalized_selection(run_id, run_label)
    result = inspect_dag(artifact_path, run_id=normalized_run_id, run_label=normalized_run_label)
    return {
        "ok": True,
        "pipeline_path": result.pipeline_path,
        "artifact_path": result.artifact_path,
        "pipeline_id": result.pipeline_id,
        "compile_id": result.compile_id,
        "run_id": result.run_id,
        "run_label": result.run_label,
        "run_status": result.run_status,
        "is_final": result.is_final,
        "node_count": len(result.nodes),
        "nodes": result.nodes,
        "edges": result.edges,
    }


def get_runs_panel(artifact_path: str | Path) -> dict[str, Any]:
    resolved_artifact_path, _contract, runs = _inspect_pipeline_runs(artifact_path, limit=100)
    items: list[dict[str, Any]] = []
    for item in runs:
        items.append(
            {
                "run_id": str(item.get("run_id") or "").strip() or None,
                "run_label": str(item.get("run_label") or "").strip() or None,
                "status": str(item.get("status") or "").strip() or None,
                "is_final": bool(item.get("is_final")),
                "started_at": item.get("started_at"),
                "finished_at": item.get("finished_at"),
            }
        )
    return {
        "ok": True,
        "artifact_path": str(resolved_artifact_path),
        "runs": items,
    }


def get_pipeline_logs_panel(
    artifact_path: str | Path,
    *,
    run_id: str | None = None,
    run_label: str | None = None,
    tail: int | None = 200,
) -> dict[str, Any]:
    normalized_run_id, normalized_run_label = _normalized_selection(run_id, run_label)
    resolved_artifact_path, active_contract, selected_run, lines = _inspect_pipeline_logs(
        artifact_path,
        run_id=normalized_run_id,
        run_label=normalized_run_label,
        tail=tail,
    )
    logs: list[dict[str, Any]] = []
    for line in lines:
        text = str(line or "").strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
            if isinstance(payload, dict):
                logs.append(payload)
                continue
        except Exception:
            pass
        logs.append({"message": text})
    return {
        "ok": True,
        "pipeline_path": str(Path(active_contract.pipeline_path).expanduser().resolve()),
        "artifact_path": str(resolved_artifact_path),
        "run_id": str(selected_run.get("run_id") or "").strip() or None,
        "run_label": str(selected_run.get("run_label") or "").strip() or None,
        "run_status": str(selected_run.get("status") or "").strip() or None,
        "is_final": bool(selected_run.get("is_final")),
        "log_path": str(selected_run.get("log_path") or "").strip() or None,
        "logs": logs,
    }


def get_node_panel(
    artifact_path: str | Path,
    node_name: str,
    *,
    run_id: str | None = None,
    run_label: str | None = None,
) -> dict[str, Any]:
    normalized_run_id, normalized_run_label = _normalized_selection(run_id, run_label)
    selection = inspect_node(
        artifact_path,
        node_name,
        run_id=normalized_run_id,
        run_label=normalized_run_label,
    )
    selected_node = next(
        (item for item in selection.nodes if str(item.get("name") or "") == node_name),
        selection.nodes[0] if selection.nodes else None,
    )
    return {
        "ok": True,
        "pipeline_path": selection.pipeline_path,
        "artifact_path": selection.artifact_path,
        "compile_id": selection.compile_id,
        "run_id": selection.run_id,
        "run_label": selection.run_label,
        "run_status": selection.run_status,
        "is_final": selection.is_final,
        "node_name": node_name,
        "selected": selected_node,
    }


def get_node_upstream_panel(
    artifact_path: str | Path,
    node_name: str,
    *,
    run_id: str | None = None,
    run_label: str | None = None,
) -> dict[str, Any]:
    normalized_run_id, normalized_run_label = _normalized_selection(run_id, run_label)
    upstream = inspect_node(
        artifact_path,
        node_name,
        run_id=normalized_run_id,
        run_label=normalized_run_label,
        upstream=True,
    )
    return {
        "ok": True,
        "node_name": node_name,
        "run_id": upstream.run_id,
        "run_label": upstream.run_label,
        "run_status": upstream.run_status,
        "is_final": upstream.is_final,
        "nodes": upstream.nodes,
    }


def get_node_downstream_panel(
    artifact_path: str | Path,
    node_name: str,
    *,
    run_id: str | None = None,
    run_label: str | None = None,
) -> dict[str, Any]:
    normalized_run_id, normalized_run_label = _normalized_selection(run_id, run_label)
    downstream = inspect_node(
        artifact_path,
        node_name,
        run_id=normalized_run_id,
        run_label=normalized_run_label,
        downstream=True,
    )
    return {
        "ok": True,
        "node_name": node_name,
        "run_id": downstream.run_id,
        "run_label": downstream.run_label,
        "run_status": downstream.run_status,
        "is_final": downstream.is_final,
        "nodes": downstream.nodes,
    }


def get_node_history_panel(
    artifact_path: str | Path,
    node_name: str,
    *,
    run_id: str | None = None,
    run_label: str | None = None,
) -> dict[str, Any]:
    normalized_run_id, normalized_run_label = _normalized_selection(run_id, run_label)
    history = inspect_node_history(
        artifact_path,
        node_name,
        run_id=normalized_run_id,
        run_label=normalized_run_label,
    )
    return {
        "ok": True,
        "node_name": node_name,
        "run_id": history.run_id,
        "run_label": history.run_label,
        "run_status": history.run_status,
        "is_final": history.is_final,
        "history": {
            "node_name": history.node_name,
            "node_kind": history.node_kind,
            "node_run_id": history.node_run_id,
            "node_run_status": history.node_run_status,
            "logical_artifact": history.logical_artifact,
            "artifact_name": history.artifact_name,
            "started_at": history.started_at,
            "finished_at": history.finished_at,
            "error_message": history.error_message,
            "states": history.states,
        },
    }


def get_node_logs_panel(
    artifact_path: str | Path,
    node_name: str,
    *,
    run_id: str | None = None,
    run_label: str | None = None,
    tail: int | None = None,
) -> dict[str, Any]:
    normalized_run_id, normalized_run_label = _normalized_selection(run_id, run_label)
    logs = inspect_node_logs(
        artifact_path,
        node_name,
        run_id=normalized_run_id,
        run_label=normalized_run_label,
        tail=tail,
    )
    return {
        "ok": True,
        "node_name": node_name,
        "run_id": logs.run_id,
        "run_label": logs.run_label,
        "run_status": logs.run_status,
        "is_final": logs.is_final,
        "logs": logs.logs,
    }


def get_node_query_panel(
    artifact_path: str | Path,
    node_name: str,
    *,
    run_id: str | None = None,
    run_label: str | None = None,
) -> dict[str, Any]:
    normalized_run_id, normalized_run_label = _normalized_selection(run_id, run_label)
    query = inspect_node_query(
        artifact_path,
        node_name,
        run_id=normalized_run_id,
        run_label=normalized_run_label,
    )
    return {
        "ok": True,
        "node_name": node_name,
        "run_id": query.run_id,
        "run_label": query.run_label,
        "run_status": query.run_status,
        "is_final": query.is_final,
        "query": {
            "pipeline_path": query.pipeline_path,
            "artifact_path": query.artifact_path,
            "effective_artifact_path": query.effective_artifact_path,
            "pipeline_id": query.pipeline_id,
            "compile_id": query.compile_id,
            "run_id": query.run_id,
            "run_label": query.run_label,
            "run_status": query.run_status,
            "is_final": query.is_final,
            "node_name": query.node_name,
            "node_kind": query.node_kind,
            "logical_artifact": query.logical_artifact,
            "artifact_name": query.artifact_name,
            "archived_artifact_name": query.archived_artifact_name,
            "sql": query.sql,
            "resolved_sql": query.resolved_sql,
            "dependencies": query.dependencies,
        },
    }


def get_node_artifact_preview_panel(
    artifact_path: str | Path,
    node_name: str,
    *,
    run_id: str | None = None,
    run_label: str | None = None,
    limit: int = 5,
    ) -> dict[str, Any]:
    selected = _selected_node_artifact_record(artifact_path, node_name, run_id=run_id, run_label=run_label)
    query_result = _execute_artifact_query(
        selected["artifact_path"],
        _artifact_preview_query(selected["artifact_name"], limit=limit),
    )
    return {
        "ok": True,
        "node_name": node_name,
        "run_id": selected["run_id"],
        "run_label": selected["run_label"],
        "run_status": selected["run_status"],
        "artifact_path": selected["artifact_path"],
        "artifact_name": selected["artifact_name"],
        "logical_artifact": selected["logical_artifact"],
        **query_result,
    }


def query_node_artifact_panel(
    artifact_path: str | Path,
    node_name: str,
    *,
    sql: str,
    run_id: str | None = None,
    run_label: str | None = None,
) -> dict[str, Any]:
    query_context = _artifact_query_context(
        artifact_path,
        node_name,
        sql=sql,
        run_id=run_id,
        run_label=run_label,
    )
    query_result = _execute_artifact_query(query_context["artifact_path"], query_context["sql"])
    return {
        "ok": True,
        "node_name": node_name,
        "run_id": query_context["run_id"],
        "run_label": query_context["run_label"],
        "run_status": query_context["run_status"],
        "artifact_path": query_context["artifact_path"],
        "artifact_name": query_context["artifact_name"],
        "logical_artifact": query_context["logical_artifact"],
        "sql": query_context["sql"],
        **query_result,
    }


def get_run_artifacts_panel(
    artifact_path: str | Path,
    *,
    run_id: str | None = None,
    run_label: str | None = None,
) -> dict[str, Any]:
    graph = inspect_dag(artifact_path, run_id=run_id, run_label=run_label)
    artifacts: list[dict[str, Any]] = []
    seen: set[str] = set()
    for node in graph.nodes:
        node_name = str(node.get("name") or "").strip()
        if not node_name:
            continue
        node_status = str(node.get("node_run_status") or "").strip().lower()
        if node_status not in {"complete", "complete_with_warnings"}:
            continue
        artifact_name = str(node.get("artifact_name") or "").strip()
        logical_artifact = str(node.get("logical_artifact") or "").strip() or None
        if not artifact_name or artifact_name in seen:
            continue
        seen.add(artifact_name)
        artifacts.append(
            {
                "artifact_name": artifact_name,
                "artifact_path": str(node.get("artifact_path") or "").strip() or graph.archived_artifact_path or graph.artifact_path,
                "logical_artifact": logical_artifact,
                "node_name": node_name,
                "node_kind": str(node.get("kind") or "").strip() or None,
                "current_state": str(node.get("current_state") or "").strip() or None,
                "node_run_status": str(node.get("node_run_status") or "").strip() or None,
            }
        )
    return {
        "ok": True,
        "run_id": graph.run_id,
        "run_label": graph.run_label,
        "run_status": graph.run_status,
        "artifacts": artifacts,
    }


def _requires_clean_existing(exc: Exception) -> bool:
    message = str(exc or "")
    lowered = message.lower()
    if "already exists" not in lowered:
        return False
    return any(
        phrase in lowered
        for phrase in (
            "table with name",
            "table ",
            "schema with name",
            "view with name",
        )
    )


def run_graph_pipeline(
    pipeline_path: str | Path,
    *,
    clean_existing: bool = False,
    on_log: Any = None,
) -> dict[str, Any]:
    runtime_bindings = _runtime_bindings_from_file(pipeline_path)
    if not bool(clean_existing):
        compiled, existing_outputs, artifact_path = list_existing_outputs_for_file(
            pipeline_path,
            runtime_bindings=runtime_bindings,
        )
        if has_compile_errors(compiled):
            diagnostics = list(compiled.diagnostics)
            message = diagnostics[0] if diagnostics else "Compile failed."
            raise RuntimeError(message)
        if existing_outputs:
            return {
                "ok": False,
                "artifact_path": artifact_path,
                "requires_clean_existing": True,
                "purge_targets": list(existing_outputs),
                "error": (
                    f"Run will purge {len(existing_outputs)} existing output table"
                    f"{'' if len(existing_outputs) == 1 else 's'} before execution."
                ),
            }
    result = run_pipeline(
        pipeline_path,
        clean_existing=bool(clean_existing),
        set_final=True,
        runtime_bindings=runtime_bindings,
        on_log=on_log,
    )
    return {
        "ok": True,
        "artifact_path": result.artifact_path,
        "run_id": result.run_id,
        "run_label": result.run_label,
        "log_path": result.log_path,
        "executed_nodes": result.executed_nodes,
        "diagnostics": list(result.compiled.diagnostics),
    }


def resume_graph_pipeline(pipeline_path: str | Path, *, on_log: Any = None) -> dict[str, Any]:
    result = resume_pipeline(
        pipeline_path,
        runtime_bindings=_runtime_bindings_from_file(pipeline_path),
        on_log=on_log,
    )
    return {
        "ok": True,
        "artifact_path": result.artifact_path,
        "run_id": result.run_id,
        "run_label": result.run_label,
        "log_path": result.log_path,
        "executed_nodes": result.executed_nodes,
        "diagnostics": list(result.compiled.diagnostics),
    }


def reset_graph_all(pipeline_path: str | Path, *, on_log: Any = None) -> dict[str, Any]:
    result = reset_all(pipeline_path, on_log=on_log)
    return {
        "ok": True,
        "artifact_path": result.artifact_path,
        "reset_nodes": result.reset_nodes,
        "reset_tables": result.reset_tables,
        "diagnostics": list(result.compiled.diagnostics),
    }


def reset_graph_node(pipeline_path: str | Path, node_name: str, *, on_log: Any = None) -> dict[str, Any]:
    result = reset_node(pipeline_path, node_name=node_name, on_log=on_log)
    return {
        "ok": True,
        "artifact_path": result.artifact_path,
        "reset_nodes": result.reset_nodes,
        "reset_tables": result.reset_tables,
        "diagnostics": list(result.compiled.diagnostics),
    }


def reset_graph_upstream(pipeline_path: str | Path, node_name: str, *, on_log: Any = None) -> dict[str, Any]:
    result = reset_upstream(pipeline_path, node_name=node_name, on_log=on_log)
    return {
        "ok": True,
        "artifact_path": result.artifact_path,
        "reset_nodes": result.reset_nodes,
        "reset_tables": result.reset_tables,
        "diagnostics": list(result.compiled.diagnostics),
    }


def stop_graph_pipeline(pipeline_path: str | Path, *, reason: str | None = None) -> dict[str, Any]:
    result = stop_pipeline(
        pipeline_path,
        run_id=None,
        reason=reason,
    )
    return {
        "ok": True,
        "artifact_path": result.artifact_path,
        "run_id": result.run_id,
        "run_label": result.run_label,
        "stop_requested": result.stop_requested,
        "stop_mode": result.stop_mode,
        "request_path": result.request_path,
        "message": result.message,
    }


def force_stop_graph_pipeline(pipeline_path: str | Path, *, reason: str | None = None) -> dict[str, Any]:
    result = force_stop_pipeline(
        pipeline_path,
        run_id=None,
        reason=reason,
    )
    return {
        "ok": True,
        "artifact_path": result.artifact_path,
        "run_id": result.run_id,
        "run_label": result.run_label,
        "stop_requested": result.stop_requested,
        "stop_mode": result.stop_mode,
        "request_path": result.request_path,
        "message": result.message,
    }


def resolve_graph_live_web_root(web_root: str | Path | None = None) -> Path:
    resolved = ((Path(__file__).resolve().parents[1] / "web" / "dist") if web_root is None else Path(web_root)).resolve()
    if not resolved.exists() or not resolved.is_dir():
        raise RuntimeError(
            f"Graph web assets were not found at '{resolved}'. Build web first with `npm install` and `npm run build`."
        )
    index_path = resolved / "index.html"
    if not index_path.exists():
        raise RuntimeError(
            f"Graph web assets at '{resolved}' are incomplete. Build web first with `npm run build`."
        )
    return resolved


class _GraphLiveHandler(SimpleHTTPRequestHandler):
    server_version = "QueronGraphLive/0.2"

    def _write_json(self, payload: dict[str, Any], *, status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_download(self, *, body: bytes, content_type: str, filename: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.end_headers()
        self.wfile.write(body)

    def _write_error_json(self, exc: Exception, *, status: int = 400) -> None:
        self._write_json({"ok": False, "error": str(exc)}, status=status)

    def _read_json_body(self) -> dict[str, Any]:
        content_length = int(self.headers.get("Content-Length") or "0")
        if content_length <= 0:
            return {}
        raw_body = self.rfile.read(content_length)
        if not raw_body:
            return {}
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON body: {exc}") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("Request body must be a JSON object.")
        return payload

    def _graph_context(self) -> GraphLiveContext:
        return self.server.graph_context  # type: ignore[attr-defined]

    def _event_broker(self) -> _EventBroker:
        return self.server.event_broker  # type: ignore[attr-defined]

    def _emit_runtime_log(self, event: PipelineLogEvent) -> None:
        self._event_broker().publish(_graph_event_from_log(event))

    def _write_sse_event(self, payload: dict[str, Any]) -> None:
        body = f"data: {json.dumps(payload)}\n\n".encode("utf-8")
        self.wfile.write(body)
        self.wfile.flush()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            context = self._graph_context()
            self._write_json(
                {
                    "ok": True,
                    "pipeline_path": context.pipeline_path,
                    "artifact_path": context.artifact_path,
                    "pipeline_id": context.pipeline_id,
                }
            )
            return
        if parsed.path == "/api/events":
            queue = self._event_broker().subscribe()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache, no-transform")
            self.send_header("Connection", "keep-alive")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()
            try:
                self._write_sse_event({"type": "connected", "pipeline_id": self._graph_context().pipeline_id})
                while True:
                    try:
                        payload = queue.get(timeout=15)
                    except Empty:
                        payload = {"type": "ping"}
                    self._write_sse_event(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                self._event_broker().unsubscribe(queue)
            return
        if parsed.path == "/api/graph":
            params = parse_qs(parsed.query, keep_blank_values=False)
            run_id = str((params.get("run_id") or [None])[0] or "").strip() or None
            run_label = str((params.get("run_label") or [None])[0] or "").strip() or None
            try:
                self._write_json(get_graph_panel(self._graph_context().artifact_path, run_id=run_id, run_label=run_label))
            except Exception as exc:
                self._write_error_json(exc)
            return
        if parsed.path == "/api/runs":
            try:
                self._write_json(get_runs_panel(self._graph_context().artifact_path))
            except Exception as exc:
                self._write_error_json(exc)
            return
        if parsed.path == "/api/run/artifacts":
            params = parse_qs(parsed.query, keep_blank_values=False)
            run_id = str((params.get("run_id") or [None])[0] or "").strip() or None
            run_label = str((params.get("run_label") or [None])[0] or "").strip() or None
            try:
                self._write_json(
                    get_run_artifacts_panel(
                        self._graph_context().artifact_path,
                        run_id=run_id,
                        run_label=run_label,
                    )
                )
            except Exception as exc:
                self._write_error_json(exc)
            return
        if parsed.path == "/api/pipeline/logs":
            params = parse_qs(parsed.query, keep_blank_values=False)
            run_id = str((params.get("run_id") or [None])[0] or "").strip() or None
            run_label = str((params.get("run_label") or [None])[0] or "").strip() or None
            tail = int(str((params.get("tail") or [200])[0] or "200").strip() or "200")
            try:
                self._write_json(
                    get_pipeline_logs_panel(
                        self._graph_context().artifact_path,
                        run_id=run_id,
                        run_label=run_label,
                        tail=tail,
                    )
                )
            except Exception as exc:
                self._write_error_json(exc)
            return
        if parsed.path == "/api/node":
            params = parse_qs(parsed.query, keep_blank_values=False)
            node_name = str((params.get("node_name") or [None])[0] or "").strip()
            run_id = str((params.get("run_id") or [None])[0] or "").strip() or None
            run_label = str((params.get("run_label") or [None])[0] or "").strip() or None
            if not node_name:
                self._write_json({"ok": False, "error": "node_name is required."}, status=400)
                return
            try:
                self._write_json(
                    get_node_panel(
                        self._graph_context().artifact_path,
                        node_name,
                        run_id=run_id,
                        run_label=run_label,
                    )
                )
            except Exception as exc:
                self._write_error_json(exc)
            return
        if parsed.path in {
            "/api/node/query",
            "/api/node/history",
            "/api/node/logs",
            "/api/node/upstream",
            "/api/node/downstream",
            "/api/node/artifact-preview",
        }:
            params = parse_qs(parsed.query, keep_blank_values=False)
            node_name = str((params.get("node_name") or [None])[0] or "").strip()
            run_id = str((params.get("run_id") or [None])[0] or "").strip() or None
            run_label = str((params.get("run_label") or [None])[0] or "").strip() or None
            if not node_name:
                self._write_json({"ok": False, "error": "node_name is required."}, status=400)
                return
            try:
                if parsed.path == "/api/node/query":
                    payload = get_node_query_panel(self._graph_context().artifact_path, node_name, run_id=run_id, run_label=run_label)
                elif parsed.path == "/api/node/history":
                    payload = get_node_history_panel(self._graph_context().artifact_path, node_name, run_id=run_id, run_label=run_label)
                elif parsed.path == "/api/node/logs":
                    tail = int(str((params.get("tail") or [200])[0] or "200").strip() or "200")
                    payload = get_node_logs_panel(
                        self._graph_context().artifact_path,
                        node_name,
                        run_id=run_id,
                        run_label=run_label,
                        tail=tail,
                    )
                elif parsed.path == "/api/node/upstream":
                    payload = get_node_upstream_panel(self._graph_context().artifact_path, node_name, run_id=run_id, run_label=run_label)
                elif parsed.path == "/api/node/artifact-preview":
                    limit = int(str((params.get("limit") or [5])[0] or "5").strip() or "5")
                    payload = get_node_artifact_preview_panel(
                        self._graph_context().artifact_path,
                        node_name,
                        run_id=run_id,
                        run_label=run_label,
                        limit=limit,
                    )
                else:
                    payload = get_node_downstream_panel(self._graph_context().artifact_path, node_name, run_id=run_id, run_label=run_label)
                self._write_json(payload)
            except Exception as exc:
                self._write_error_json(exc)
            return
        if parsed.path.startswith("/api/"):
            self._write_json({"ok": False, "error": "not_found"}, status=404)
            return
        if parsed.path == "/":
            self.path = "/index.html"
            return super().do_GET()
        static_root = Path(self.directory).resolve()
        candidate = (static_root / parsed.path.lstrip("/")).resolve()
        if str(candidate).startswith(str(static_root)) and candidate.exists():
            return super().do_GET()
        self.path = "/index.html"
        return super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        context = self._graph_context()
        try:
            payload = self._read_json_body()
            if parsed.path == "/api/run":
                clean_existing = bool(payload.get("clean_existing"))
                try:
                    self._write_json(
                        run_graph_pipeline(
                            context.pipeline_path,
                            clean_existing=clean_existing,
                            on_log=self._emit_runtime_log,
                        )
                    )
                except Exception as exc:
                    if _requires_clean_existing(exc):
                        self._write_json(
                            {
                                "ok": False,
                                "error": str(exc),
                                "requires_clean_existing": True,
                            },
                            status=409,
                        )
                        return
                    raise
                return
            if parsed.path == "/api/resume":
                self._write_json(resume_graph_pipeline(context.pipeline_path, on_log=self._emit_runtime_log))
                return
            if parsed.path == "/api/stop":
                reason = str(payload.get("reason") or "").strip() or None
                self._write_json(stop_graph_pipeline(context.pipeline_path, reason=reason))
                return
            if parsed.path == "/api/force-stop":
                reason = str(payload.get("reason") or "").strip() or None
                self._write_json(force_stop_graph_pipeline(context.pipeline_path, reason=reason))
                return
            if parsed.path == "/api/reset-all":
                self._write_json(reset_graph_all(context.pipeline_path, on_log=self._emit_runtime_log))
                return
            if parsed.path == "/api/reset-node":
                node_name = str(payload.get("node_name") or "").strip()
                if not node_name:
                    raise RuntimeError("node_name is required.")
                self._write_json(reset_graph_node(context.pipeline_path, node_name, on_log=self._emit_runtime_log))
                return
            if parsed.path == "/api/reset-upstream":
                node_name = str(payload.get("node_name") or "").strip()
                if not node_name:
                    raise RuntimeError("node_name is required.")
                self._write_json(reset_graph_upstream(context.pipeline_path, node_name, on_log=self._emit_runtime_log))
                return
            if parsed.path == "/api/node/artifact-query":
                node_name = str(payload.get("node_name") or "").strip()
                if not node_name:
                    raise RuntimeError("node_name is required.")
                sql = str(payload.get("sql") or "").strip()
                run_id = str(payload.get("run_id") or "").strip() or None
                run_label = str(payload.get("run_label") or "").strip() or None
                self._write_json(
                    query_node_artifact_panel(
                        context.artifact_path,
                        node_name,
                        sql=sql,
                        run_id=run_id,
                        run_label=run_label,
                    )
                )
                return
            if parsed.path == "/api/node/artifact-download":
                node_name = str(payload.get("node_name") or "").strip()
                if not node_name:
                    raise RuntimeError("node_name is required.")
                sql = str(payload.get("sql") or "").strip()
                run_id = str(payload.get("run_id") or "").strip() or None
                run_label = str(payload.get("run_label") or "").strip() or None
                export_format = str(payload.get("format") or "csv").strip() or "csv"
                download = export_node_artifact_query_panel(
                    context.artifact_path,
                    node_name,
                    sql=sql,
                    run_id=run_id,
                    run_label=run_label,
                    format=export_format,
                )
                self._write_download(
                    body=download["body"],
                    content_type=str(download["content_type"]),
                    filename=str(download["filename"]),
                )
                return
            self._write_json({"ok": False, "error": "not_found"}, status=404)
        except Exception as exc:
            self._write_error_json(exc)

    def log_message(self, format: str, *args: Any) -> None:
        return


def build_graph_live_server(
    context: GraphLiveContext,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    web_root: str | Path | None = None,
) -> ThreadingHTTPServer:
    resolved_web_root = resolve_graph_live_web_root(web_root)
    server = ThreadingHTTPServer((host, port), partial(_GraphLiveHandler, directory=str(resolved_web_root)))
    server.graph_context = context  # type: ignore[attr-defined]
    server.event_broker = _EventBroker()  # type: ignore[attr-defined]
    return server
