from __future__ import annotations

import inspect
from typing import Any, Callable


def _require_non_empty_string(name: str, value: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} is required and must be a non-empty string.")
    return text


def _node_decorator(kind: str, payload: dict[str, Any]) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        setattr(
            fn,
            "__queron_node__",
            {
                **payload,
                "kind": kind,
                "function_name": fn.__name__,
            },
        )
        return fn

    return decorator


def _require_zero_argument_callable(fn: Callable[..., Any], *, kind: str) -> None:
    signature = inspect.signature(fn)
    required_params = [
        param
        for param in signature.parameters.values()
        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        )
        and param.default is inspect._empty
    ]
    if required_params:
        raise ValueError(f"{kind} functions must not declare required parameters.")


class _PostgresNamespace:
    def ingress(self, *, config: str, name: str, out: str, sql: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "postgres.ingress",
            {
                "config": _require_non_empty_string("config", config),
                "name": _require_non_empty_string("name", name),
                "out": _require_non_empty_string("out", out),
                "sql": _require_non_empty_string("sql", sql),
            },
        )

    def egress(
        self,
        *,
        config: str,
        name: str,
        table: str,
        sql: str,
        mode: str = "replace",
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "postgres.egress",
            {
                "config": _require_non_empty_string("config", config),
                "name": _require_non_empty_string("name", name),
                "table": _require_non_empty_string("table", table),
                "sql": _require_non_empty_string("sql", sql),
                "mode": _require_non_empty_string("mode", mode),
            },
        )


class _Db2Namespace:
    def ingress(self, *, config: str, name: str, out: str, sql: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "db2.ingress",
            {
                "config": _require_non_empty_string("config", config),
                "name": _require_non_empty_string("name", name),
                "out": _require_non_empty_string("out", out),
                "sql": _require_non_empty_string("sql", sql),
            },
        )

    def egress(
        self,
        *,
        config: str,
        name: str,
        table: str,
        sql: str,
        mode: str = "replace",
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "db2.egress",
            {
                "config": _require_non_empty_string("config", config),
                "name": _require_non_empty_string("name", name),
                "table": _require_non_empty_string("table", table),
                "sql": _require_non_empty_string("sql", sql),
                "mode": _require_non_empty_string("mode", mode),
            },
        )


class _ModelNamespace:
    def sql(
        self,
        *,
        name: str,
        query: str,
        out: str,
        materialized: str = "artifact",
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "model.sql",
            {
                "name": _require_non_empty_string("name", name),
                "out": _require_non_empty_string("out", out),
                "query": _require_non_empty_string("query", query),
                "materialized": _require_non_empty_string("materialized", materialized),
            },
        )


def _build_file_ingress_payload(
    *,
    kind: str,
    name: str,
    out: str,
    path: str,
    header: bool | None = None,
    delimiter: str | None = None,
    quote: str | None = None,
    escape: str | None = None,
    skip_rows: int | None = None,
    columns: dict[str, str] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": _require_non_empty_string("name", name),
        "out": _require_non_empty_string("out", out),
        "path": _require_non_empty_string("path", path),
    }
    if kind == "csv.ingress":
        payload["header"] = bool(True if header is None else header)
        payload["delimiter"] = _require_non_empty_string("delimiter", delimiter or ",")
        payload["skip_rows"] = max(0, int(skip_rows or 0))
        if quote is not None:
            payload["quote"] = _require_non_empty_string("quote", quote)
        if escape is not None:
            payload["escape"] = _require_non_empty_string("escape", escape)
        if columns is not None:
            payload["columns"] = dict(columns)
    return payload


class _CsvNamespace:
    def ingress(
        self,
        *,
        name: str,
        out: str,
        path: str,
        header: bool = True,
        delimiter: str = ",",
        quote: str | None = None,
        escape: str | None = None,
        skip_rows: int = 0,
        columns: dict[str, str] | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "csv.ingress",
            _build_file_ingress_payload(
                kind="csv.ingress",
                name=name,
                out=out,
                path=path,
                header=header,
                delimiter=delimiter,
                quote=quote,
                escape=escape,
                skip_rows=skip_rows,
                columns=columns,
            ),
        )

    def egress(
        self,
        *,
        name: str,
        path: str,
        sql: str,
        overwrite: bool = False,
        header: bool = True,
        delimiter: str = ",",
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "csv.egress",
            {
                "name": _require_non_empty_string("name", name),
                "path": _require_non_empty_string("path", path),
                "sql": _require_non_empty_string("sql", sql),
                "overwrite": bool(overwrite),
                "header": bool(header),
                "delimiter": _require_non_empty_string("delimiter", delimiter),
            },
        )


class _JsonlNamespace:
    def ingress(
        self,
        *,
        name: str,
        out: str,
        path: str,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "jsonl.ingress",
            _build_file_ingress_payload(
                kind="jsonl.ingress",
                name=name,
                out=out,
                path=path,
            ),
        )

    def egress(
        self,
        *,
        name: str,
        path: str,
        sql: str,
        overwrite: bool = False,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "jsonl.egress",
            {
                "name": _require_non_empty_string("name", name),
                "path": _require_non_empty_string("path", path),
                "sql": _require_non_empty_string("sql", sql),
                "overwrite": bool(overwrite),
            },
        )


class _ParquetNamespace:
    def ingress(
        self,
        *,
        name: str,
        out: str,
        path: str,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "parquet.ingress",
            _build_file_ingress_payload(
                kind="parquet.ingress",
                name=name,
                out=out,
                path=path,
            ),
        )

    def egress(
        self,
        *,
        name: str,
        path: str,
        sql: str,
        overwrite: bool = False,
        compression: str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        payload: dict[str, Any] = {
            "name": _require_non_empty_string("name", name),
            "path": _require_non_empty_string("path", path),
            "sql": _require_non_empty_string("sql", sql),
            "overwrite": bool(overwrite),
        }
        if compression is not None:
            payload["compression"] = _require_non_empty_string("compression", compression)
        return _node_decorator("parquet.egress", payload)


class _FileNamespace:
    def ingress(
        self,
        *,
        name: str,
        out: str,
        path: str,
        format: str | None = None,
        header: bool = True,
        delimiter: str = ",",
        quote: str | None = None,
        escape: str | None = None,
        skip_rows: int = 0,
        columns: dict[str, str] | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        payload: dict[str, Any] = _build_file_ingress_payload(
            kind="csv.ingress",
            name=name,
            out=out,
            path=path,
            header=header,
            delimiter=delimiter,
            quote=quote,
            escape=escape,
            skip_rows=skip_rows,
            columns=columns,
        )
        if format is not None:
            payload["format"] = _require_non_empty_string("format", format)
        return _node_decorator("file.ingress", payload)


class _CheckNamespace:
    def fail_if_count(
        self,
        *,
        name: str,
        query: str,
        operator: str,
        value: int | float,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "check.count",
            {
                "name": _require_non_empty_string("name", name),
                "query": _require_non_empty_string("query", query),
                "operator": _require_non_empty_string("operator", operator),
                "value": value,
            },
        )

    def fail_if_true(
        self,
        *,
        name: str,
        query: str,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "check.boolean",
            {
                "name": _require_non_empty_string("name", name),
                "query": _require_non_empty_string("query", query),
            },
        )


class _PythonNamespace:
    def ingress(
        self,
        fn: Callable[..., Any] | None = None,
        *,
        name: str | None = None,
        out: str,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]] | Callable[..., Any]:
        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            _require_zero_argument_callable(fn, kind="queron.python.ingress")
            return _node_decorator(
                "python.ingress",
                {
                    "name": _require_non_empty_string("name", name or fn.__name__),
                    "out": _require_non_empty_string("out", out),
                },
            )(fn)

        if fn is None:
            return decorator
        if not callable(fn):
            raise ValueError("queron.python.ingress(...) requires a callable or must be used as a decorator.")
        return decorator(fn)


def ref(name: str) -> str:
    return f'{{{{ queron.ref("{name}") }}}}'


def source(name: str) -> str:
    return f'{{{{ queron.source("{name}") }}}}'


postgres = _PostgresNamespace()
db2 = _Db2Namespace()
csv = _CsvNamespace()
jsonl = _JsonlNamespace()
parquet = _ParquetNamespace()
file = _FileNamespace()
model = _ModelNamespace()
check = _CheckNamespace()
python = _PythonNamespace()

from .compiler import CompiledPipeline, compile_pipeline_code  # noqa: E402
from .bindings import Db2Binding, PostGresBinding, PostgresBinding, RuntimeBinding  # noqa: E402
from .config import load_connections_config, resolve_connection_binding  # noqa: E402
from .executor import execute_pipeline  # noqa: E402
from .api import (  # noqa: E402
    InitPipelineProjectResult,
    ResetPipelineResult,
    RunPipelineResult,
    compile_pipeline_text,
    compile_pipeline_file,
    execute_compiled_pipeline,
    has_compile_errors,
    init_pipeline_project,
    reset_compiled_all,
    reset_compiled_downstream,
    reset_compiled_upstream,
    reset_compiled_node,
    reset_all_file,
    reset_downstream_file,
    reset_upstream_file,
    reset_node_file,
    resume_compiled_pipeline,
    resume_pipeline_file,
    run_pipeline_file,
)
from .runtime import PipelineRuntime  # noqa: E402
from .runtime_models import (  # noqa: E402
    ColumnMappingRecord,
    LogCode,
    NodeExecutionResult,
    NodeRunRecord,
    NodeStateRecord,
    NodeStatus,
    NodeWarningEvent,
    PipelineLogEvent,
    PipelineRunRecord,
    RunPolicy,
    RunStatus,
    TableLineageRecord,
    WarningCode,
    build_log_event,
    build_warning_event,
    format_log_event,
    normalize_log_event,
    normalize_warning_events,
    utc_now_timestamp,
)

__all__ = [
    "ColumnMappingRecord",
    "CompiledPipeline",
    "Db2Binding",
    "InitPipelineProjectResult",
    "LogCode",
    "NodeExecutionResult",
    "NodeRunRecord",
    "NodeStateRecord",
    "NodeStatus",
    "NodeWarningEvent",
    "PipelineLogEvent",
    "PipelineRuntime",
    "PipelineRunRecord",
    "PostGresBinding",
    "PostgresBinding",
    "ResetPipelineResult",
    "RunPipelineResult",
    "RunPolicy",
    "RunStatus",
    "RuntimeBinding",
    "TableLineageRecord",
    "WarningCode",
    "build_log_event",
    "build_warning_event",
    "check",
    "csv",
    "compile_pipeline_code",
    "compile_pipeline_text",
    "compile_pipeline_file",
    "db2",
    "execute_compiled_pipeline",
    "execute_pipeline",
    "file",
    "has_compile_errors",
    "init_pipeline_project",
    "load_connections_config",
    "format_log_event",
    "jsonl",
    "model",
    "normalize_log_event",
    "normalize_warning_events",
    "parquet",
    "postgres",
    "python",
    "ref",
    "reset_all_file",
    "reset_compiled_all",
    "reset_compiled_downstream",
    "reset_compiled_upstream",
    "reset_compiled_node",
    "reset_downstream_file",
    "reset_upstream_file",
    "reset_node_file",
    "resolve_connection_binding",
    "resume_compiled_pipeline",
    "resume_pipeline_file",
    "run_pipeline_file",
    "source",
    "utc_now_timestamp",
]
