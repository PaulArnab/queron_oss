from __future__ import annotations

import json
from typing import Any, Callable
from urllib import error, request

from .runtime_models import PipelineLogEvent, normalize_log_event


def graph_log_publisher(
    graph_url: str,
    *,
    timeout_seconds: float = 1.0,
    raise_errors: bool = False,
) -> Callable[[PipelineLogEvent], None]:
    base_url = str(graph_url or "").strip().rstrip("/")
    if not base_url:
        raise RuntimeError("graph_url is required.")
    endpoint = f"{base_url}/api/events/publish"

    def _publish(event: PipelineLogEvent) -> None:
        normalized = normalize_log_event(event)
        body = json.dumps(normalized.model_dump(), ensure_ascii=True).encode("utf-8")
        req = request.Request(
            endpoint,
            data=body,
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=float(timeout_seconds)) as response:
                response.read()
        except (OSError, error.URLError, error.HTTPError):
            if raise_errors:
                raise

    return _publish


def fanout_log_handlers(*handlers: Callable[[PipelineLogEvent], Any] | None) -> Callable[[PipelineLogEvent], None] | None:
    active_handlers = [handler for handler in handlers if handler is not None]
    if not active_handlers:
        return None

    def _fanout(event: PipelineLogEvent) -> None:
        for handler in active_handlers:
            try:
                handler(event)
            except Exception:
                pass

    return _fanout
