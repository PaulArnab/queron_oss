from __future__ import annotations

import inspect
from typing import Any, Callable


def _require_non_empty_string(name: str, value: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} is required and must be a non-empty string.")
    return text


def _normalize_depends_on(depends_on: list[str] | tuple[str, ...] | str | None) -> list[str]:
    if depends_on is None:
        return []
    raw_items = [depends_on] if isinstance(depends_on, str) else list(depends_on)
    normalized: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        text = _require_non_empty_string("depends_on", str(item))
        if text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _add_depends_on(payload: dict[str, Any], depends_on: list[str] | tuple[str, ...] | str | None) -> dict[str, Any]:
    normalized = _normalize_depends_on(depends_on)
    if normalized:
        payload["depends_on"] = normalized
    return payload


def _node_decorator(kind: str, payload: dict[str, Any]) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        normalized_payload = dict(payload)
        depends_on = normalized_payload.pop("depends_on", None)
        _add_depends_on(normalized_payload, depends_on)
        setattr(
            fn,
            "__queron_node__",
            {
                **normalized_payload,
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
    def ingress(self, *, config: str, name: str, out: str, sql: str, depends_on: list[str] | tuple[str, ...] | str | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "postgres.ingress",
            {
                "config": _require_non_empty_string("config", config),
                "name": _require_non_empty_string("name", name),
                "out": _require_non_empty_string("out", out),
                "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
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
        retain: bool = False,
        out: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        payload = {
            "config": _require_non_empty_string("config", config),
            "name": _require_non_empty_string("name", name),
            "table": _require_non_empty_string("table", table),
            "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
            "mode": _require_non_empty_string("mode", mode),
            "retain": bool(retain),
            "out": _require_non_empty_string("out", out),
        }
        return _node_decorator("postgres.egress", payload)

    def lookup(
        self,
        *,
        config: str,
        name: str,
        table: str,
        sql: str,
        mode: str = "replace",
        retain: bool = False,
        out: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        payload = {
            "config": _require_non_empty_string("config", config),
            "name": _require_non_empty_string("name", name),
            "table": _require_non_empty_string("table", table),
            "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
            "mode": _require_non_empty_string("mode", mode),
            "retain": bool(retain),
            "out": _require_non_empty_string("out", out),
        }
        return _node_decorator("postgres.lookup", payload)


class _Db2Namespace:
    def ingress(self, *, config: str, name: str, out: str, sql: str, depends_on: list[str] | tuple[str, ...] | str | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "db2.ingress",
            {
                "config": _require_non_empty_string("config", config),
                "name": _require_non_empty_string("name", name),
                "out": _require_non_empty_string("out", out),
                "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
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
        retain: bool = False,
        out: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        payload = {
            "config": _require_non_empty_string("config", config),
            "name": _require_non_empty_string("name", name),
            "table": _require_non_empty_string("table", table),
            "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
            "mode": _require_non_empty_string("mode", mode),
            "retain": bool(retain),
            "out": _require_non_empty_string("out", out),
        }
        return _node_decorator("db2.egress", payload)

    def lookup(
        self,
        *,
        config: str,
        name: str,
        table: str,
        sql: str,
        mode: str = "replace",
        out: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        payload = {
            "config": _require_non_empty_string("config", config),
            "name": _require_non_empty_string("name", name),
            "table": _require_non_empty_string("table", table),
            "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
            "mode": _require_non_empty_string("mode", mode),
            "out": _require_non_empty_string("out", out),
        }
        return _node_decorator("db2.lookup", payload)


class _MssqlNamespace:
    def ingress(self, *, config: str, name: str, out: str, sql: str, depends_on: list[str] | tuple[str, ...] | str | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "mssql.ingress",
            {
                "config": _require_non_empty_string("config", config),
                "name": _require_non_empty_string("name", name),
                "out": _require_non_empty_string("out", out),
                "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
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
        out: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        payload = {
            "config": _require_non_empty_string("config", config),
            "name": _require_non_empty_string("name", name),
            "table": _require_non_empty_string("table", table),
            "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
            "mode": _require_non_empty_string("mode", mode),
            "out": _require_non_empty_string("out", out),
        }
        return _node_decorator("mssql.egress", payload)

    def lookup(
        self,
        *,
        config: str,
        name: str,
        table: str,
        sql: str,
        mode: str = "replace",
        out: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        payload = {
            "config": _require_non_empty_string("config", config),
            "name": _require_non_empty_string("name", name),
            "table": _require_non_empty_string("table", table),
            "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
            "mode": _require_non_empty_string("mode", mode),
            "out": _require_non_empty_string("out", out),
        }
        return _node_decorator("mssql.lookup", payload)


class _MysqlNamespace:
    def ingress(self, *, config: str, name: str, out: str, sql: str, depends_on: list[str] | tuple[str, ...] | str | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "mysql.ingress",
            {
                "config": _require_non_empty_string("config", config),
                "name": _require_non_empty_string("name", name),
                "out": _require_non_empty_string("out", out),
                "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
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
        retain: bool = False,
        out: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        payload = {
            "config": _require_non_empty_string("config", config),
            "name": _require_non_empty_string("name", name),
            "table": _require_non_empty_string("table", table),
            "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
            "mode": _require_non_empty_string("mode", mode),
            "retain": bool(retain),
            "out": _require_non_empty_string("out", out),
        }
        return _node_decorator("mysql.egress", payload)

    def lookup(
        self,
        *,
        config: str,
        name: str,
        table: str,
        sql: str,
        mode: str = "replace",
        retain: bool = False,
        out: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        payload = {
            "config": _require_non_empty_string("config", config),
            "name": _require_non_empty_string("name", name),
            "table": _require_non_empty_string("table", table),
            "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
            "mode": _require_non_empty_string("mode", mode),
            "retain": bool(retain),
            "out": _require_non_empty_string("out", out),
        }
        return _node_decorator("mysql.lookup", payload)


class _MariaDbNamespace:
    def ingress(self, *, config: str, name: str, out: str, sql: str, depends_on: list[str] | tuple[str, ...] | str | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "mariadb.ingress",
            {
                "config": _require_non_empty_string("config", config),
                "name": _require_non_empty_string("name", name),
                "out": _require_non_empty_string("out", out),
                "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
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
        retain: bool = False,
        out: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        payload = {
            "config": _require_non_empty_string("config", config),
            "name": _require_non_empty_string("name", name),
            "table": _require_non_empty_string("table", table),
            "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
            "mode": _require_non_empty_string("mode", mode),
            "retain": bool(retain),
            "out": _require_non_empty_string("out", out),
        }
        return _node_decorator("mariadb.egress", payload)

    def lookup(
        self,
        *,
        config: str,
        name: str,
        table: str,
        sql: str,
        mode: str = "replace",
        retain: bool = False,
        out: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        payload = {
            "config": _require_non_empty_string("config", config),
            "name": _require_non_empty_string("name", name),
            "table": _require_non_empty_string("table", table),
            "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
            "mode": _require_non_empty_string("mode", mode),
            "retain": bool(retain),
            "out": _require_non_empty_string("out", out),
        }
        return _node_decorator("mariadb.lookup", payload)


class _OracleNamespace:
    def ingress(self, *, config: str, name: str, out: str, sql: str, depends_on: list[str] | tuple[str, ...] | str | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "oracle.ingress",
            {
                "config": _require_non_empty_string("config", config),
                "name": _require_non_empty_string("name", name),
                "out": _require_non_empty_string("out", out),
                "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
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
        retain: bool = False,
        out: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        payload = {
            "config": _require_non_empty_string("config", config),
            "name": _require_non_empty_string("name", name),
            "table": _require_non_empty_string("table", table),
            "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
            "mode": _require_non_empty_string("mode", mode),
            "retain": bool(retain),
            "out": _require_non_empty_string("out", out),
        }
        return _node_decorator("oracle.egress", payload)

    def lookup(
        self,
        *,
        config: str,
        name: str,
        table: str,
        sql: str,
        mode: str = "replace",
        retain: bool = False,
        out: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        payload = {
            "config": _require_non_empty_string("config", config),
            "name": _require_non_empty_string("name", name),
            "table": _require_non_empty_string("table", table),
            "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
            "mode": _require_non_empty_string("mode", mode),
            "retain": bool(retain),
            "out": _require_non_empty_string("out", out),
        }
        return _node_decorator("oracle.lookup", payload)


class _ModelNamespace:
    def sql(
        self,
        *,
        name: str,
        query: str,
        out: str,
        materialized: str = "artifact",
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "model.sql",
            {
                "name": _require_non_empty_string("name", name),
                "out": _require_non_empty_string("out", out),
                "query": _require_non_empty_string("query", query),
            "depends_on": depends_on,
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
    depends_on: list[str] | tuple[str, ...] | str | None = None,
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
    return _add_depends_on(payload, depends_on)


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
        depends_on: list[str] | tuple[str, ...] | str | None = None,
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
                depends_on=depends_on,
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
        out: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        payload = {
            "name": _require_non_empty_string("name", name),
            "path": _require_non_empty_string("path", path),
            "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
            "overwrite": bool(overwrite),
            "header": bool(header),
            "delimiter": _require_non_empty_string("delimiter", delimiter),
            "out": _require_non_empty_string("out", out),
        }
        return _node_decorator("csv.egress", payload)


class _JsonlNamespace:
    def ingress(
        self,
        *,
        name: str,
        out: str,
        path: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "jsonl.ingress",
            _build_file_ingress_payload(
                kind="jsonl.ingress",
                name=name,
                out=out,
                path=path,
                depends_on=depends_on,
            ),
        )

    def egress(
        self,
        *,
        name: str,
        path: str,
        sql: str,
        overwrite: bool = False,
        out: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        payload = {
            "name": _require_non_empty_string("name", name),
            "path": _require_non_empty_string("path", path),
            "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
            "overwrite": bool(overwrite),
            "out": _require_non_empty_string("out", out),
        }
        return _node_decorator("jsonl.egress", payload)


class _ParquetNamespace:
    def ingress(
        self,
        *,
        name: str,
        out: str,
        path: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "parquet.ingress",
            _build_file_ingress_payload(
                kind="parquet.ingress",
                name=name,
                out=out,
                path=path,
                depends_on=depends_on,
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
        out: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        payload: dict[str, Any] = {
            "name": _require_non_empty_string("name", name),
            "path": _require_non_empty_string("path", path),
            "sql": _require_non_empty_string("sql", sql),
            "depends_on": depends_on,
            "overwrite": bool(overwrite),
            "out": _require_non_empty_string("out", out),
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
        depends_on: list[str] | tuple[str, ...] | str | None = None,
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
            depends_on=depends_on,
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
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "check.count",
            {
                "name": _require_non_empty_string("name", name),
                "query": _require_non_empty_string("query", query),
                "depends_on": depends_on,
                "operator": _require_non_empty_string("operator", operator),
                "value": value,
            },
        )

    def fail_if_true(
        self,
        *,
        name: str,
        query: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return _node_decorator(
            "check.boolean",
            {
                "name": _require_non_empty_string("name", name),
                "query": _require_non_empty_string("query", query),
                "depends_on": depends_on,
            },
        )


class _PythonNamespace:
    def ingress(
        self,
        fn: Callable[..., Any] | None = None,
        *,
        name: str | None = None,
        out: str,
        depends_on: list[str] | tuple[str, ...] | str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]] | Callable[..., Any]:
        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            _require_zero_argument_callable(fn, kind="queron.python.ingress")
            return _node_decorator(
                "python.ingress",
                {
                    "name": _require_non_empty_string("name", name or fn.__name__),
                    "out": _require_non_empty_string("out", out),
                    "depends_on": depends_on,
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


def lookup(name: str) -> str:
    return f'{{{{ queron.lookup("{name}") }}}}'


_RUNTIME_VAR_DEFAULT_UNSET = object()


def var(
    name: str,
    *,
    log_value: bool = False,
    mutable_after_start: bool = False,
    default: Any = _RUNTIME_VAR_DEFAULT_UNSET,
) -> str:
    text = str(name or "").strip()
    if not text:
        raise ValueError("queron.var(...) requires a non-empty variable name.")
    escaped = text.replace('"', '\\"')
    options: list[str] = []
    if log_value:
        options.append("log_value=True")
    if mutable_after_start:
        options.append("mutable_after_start=True")
    if default is not _RUNTIME_VAR_DEFAULT_UNSET:
        options.append(f"default={repr(default)}")
    if options:
        return f'{{{{ queron.var("{escaped}", {", ".join(options)}) }}}}'
    return f'{{{{ queron.var("{escaped}") }}}}'


postgres = _PostgresNamespace()
db2 = _Db2Namespace()
mssql = _MssqlNamespace()
mysql = _MysqlNamespace()
mariadb = _MariaDbNamespace()
oracle = _OracleNamespace()
csv = _CsvNamespace()
jsonl = _JsonlNamespace()
parquet = _ParquetNamespace()
file = _FileNamespace()
model = _ModelNamespace()
check = _CheckNamespace()
python = _PythonNamespace()

from .api import (  # noqa: E402
    compile_pipeline,
    export_artifact,
    inspect_node,
    inspect_node_logs,
    inspect_node_query,
    inspect_node_history,
    inspect_dag,
    init_pipeline_project,
    reset_all,
    reset_downstream,
    reset_upstream,
    reset_node,
    resume_pipeline,
    run_pipeline,
    stop_pipeline,
    force_stop_pipeline,
)
from .bindings import Db2Binding, MariaDbBinding, MssqlBinding, MysqlBinding, OracleBinding, PostgresBinding  # noqa: E402
from .graph_client import fanout_log_handlers, graph_log_publisher  # noqa: E402

__all__ = [
    "Db2Binding",
    "MariaDbBinding",
    "MssqlBinding",
    "MysqlBinding",
    "OracleBinding",
    "PostgresBinding",
    "check",
    "csv",
    "compile_pipeline",
    "db2",
    "export_artifact",
    "file",
    "fanout_log_handlers",
    "graph_log_publisher",
    "inspect_node",
    "inspect_node_logs",
    "inspect_node_query",
    "inspect_node_history",
    "inspect_dag",
    "init_pipeline_project",
    "jsonl",
    "lookup",
    "model",
    "mariadb",
    "mysql",
    "oracle",
    "parquet",
    "postgres",
    "python",
    "ref",
    "var",
    "reset_all",
    "reset_downstream",
    "reset_upstream",
    "reset_node",
    "resume_pipeline",
    "run_pipeline",
    "stop_pipeline",
    "force_stop_pipeline",
    "source",
]





