from __future__ import annotations

import re
from typing import Any, Callable

from .runtime_models import PipelineVarRecord


_VAR_PATTERN = re.compile(
    r"\{\{\s*queron\.var\(\s*(?P<quote>['\"])(?P<name>[^'\"]+)(?P=quote)\s*\)\s*\}\}",
    re.IGNORECASE,
)


def validate_runtime_var_values(
    contract_vars: list[PipelineVarRecord] | None,
    runtime_vars: dict[str, Any] | None,
) -> dict[str, Any]:
    provided = {
        str(key).strip(): value
        for key, value in dict(runtime_vars or {}).items()
        if str(key).strip()
    }
    if not contract_vars:
        if provided:
            names = ", ".join(sorted(provided))
            raise RuntimeError(f"Runtime vars were provided but the compiled pipeline does not declare any: {names}.")
        return {}

    records_by_name = {str(item.name).strip(): item for item in contract_vars if str(item.name).strip()}
    unknown_names = sorted(name for name in provided if name not in records_by_name)
    if unknown_names:
        raise RuntimeError(f"Unknown runtime vars: {', '.join(unknown_names)}.")

    resolved: dict[str, Any] = {}
    for name, record in records_by_name.items():
        if name in provided:
            value = provided[name]
        elif not record.required:
            value = record.default
        else:
            raise RuntimeError(f"Missing required runtime var '{name}'.")
        if str(record.kind or "scalar") == "list":
            if not isinstance(value, (list, tuple)):
                raise RuntimeError(f"Runtime var '{name}' must be a list.")
            values = list(value)
            if not values:
                raise RuntimeError(f"Runtime var '{name}' must not be empty.")
            resolved[name] = values
        else:
            if isinstance(value, (list, tuple)):
                raise RuntimeError(f"Runtime var '{name}' must be a scalar value.")
            resolved[name] = value
    return resolved


def render_runtime_sql(
    sql: str,
    *,
    runtime_vars: dict[str, Any] | None,
    placeholder_factory: Callable[[int], str],
) -> tuple[str, list[Any]]:
    rendered_parts: list[str] = []
    params: list[Any] = []
    cursor = 0
    resolved_vars = dict(runtime_vars or {})
    for match in _VAR_PATTERN.finditer(str(sql or "")):
        rendered_parts.append(str(sql or "")[cursor:match.start()])
        name = str(match.group("name") or "").strip()
        if name not in resolved_vars:
            raise RuntimeError(f"Missing required runtime var '{name}'.")
        value = resolved_vars[name]
        if isinstance(value, (list, tuple)):
            values = list(value)
            if not values:
                raise RuntimeError(f"Runtime var '{name}' must not be empty.")
            placeholders: list[str] = []
            for item in values:
                params.append(item)
                placeholders.append(placeholder_factory(len(params)))
            rendered_parts.append(", ".join(placeholders))
        else:
            params.append(value)
            rendered_parts.append(placeholder_factory(len(params)))
        cursor = match.end()
    rendered_parts.append(str(sql or "")[cursor:])
    return "".join(rendered_parts), params
