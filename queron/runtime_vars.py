from __future__ import annotations

import ast
import re
from typing import Any, Callable

from .runtime_models import PipelineVarRecord


_VAR_PATTERN = re.compile(
    r"\{\{\s*queron\.var\(\s*(?P<quote>['\"])(?P<name>[^'\"]+)(?P=quote)(?P<options>(?:\s*,\s*.*?)?)\s*\)\s*\}\}",
    re.IGNORECASE | re.DOTALL,
)


def parse_runtime_var_options(
    name: str,
    options_text: str | None,
) -> tuple[dict[str, Any], list[str], str | None]:
    resolved = {
        "log_value": False,
        "mutable_after_start": False,
        "default": None,
        "has_default": False,
    }
    options_payload = str(options_text or "").strip()
    if not options_payload:
        return resolved, [], None
    escaped_name = str(name or "").replace("\\", "\\\\").replace('"', '\\"')
    expression = f'queron.var("{escaped_name}"{options_payload})'
    try:
        parsed = ast.parse(expression, mode="eval")
    except SyntaxError:
        return resolved, [], "Invalid queron.var(...) options."
    call = parsed.body
    if not isinstance(call, ast.Call):
        return resolved, [], "Invalid queron.var(...) options."
    unknown: list[str] = []
    for keyword in call.keywords:
        option_name = str(keyword.arg or "").strip()
        if option_name == "default":
            try:
                resolved["default"] = ast.literal_eval(keyword.value)
                resolved["has_default"] = True
            except Exception:
                return resolved, [], "queron.var(..., default=...) must use a Python literal value."
            continue
        if option_name in {"log_value", "mutable_after_start"}:
            try:
                value = ast.literal_eval(keyword.value)
            except Exception:
                return resolved, [], f"queron.var(..., {option_name}=...) must use True or False."
            if not isinstance(value, bool):
                return resolved, [], f"queron.var(..., {option_name}=...) must use True or False."
            resolved[option_name] = value
            continue
        unknown.append(option_name)
    return resolved, unknown, None


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


def resolve_runtime_var_values_for_existing_run(
    contract_vars: list[PipelineVarRecord] | None,
    *,
    stored_runtime_vars: dict[str, Any] | None,
    requested_runtime_vars: dict[str, Any] | None,
) -> dict[str, Any]:
    stored_payload = {
        str(key).strip(): value
        for key, value in dict(stored_runtime_vars or {}).items()
        if str(key).strip()
    }
    if requested_runtime_vars is None:
        return validate_runtime_var_values(contract_vars, stored_payload)

    requested_resolved = validate_runtime_var_values(contract_vars, requested_runtime_vars)
    if not stored_payload:
        return requested_resolved

    stored_resolved = validate_runtime_var_values(contract_vars, stored_payload)
    records_by_name = {
        str(item.name).strip(): item
        for item in list(contract_vars or [])
        if str(item.name).strip()
    }
    for name, requested_value in requested_resolved.items():
        if name not in stored_resolved:
            continue
        if stored_resolved[name] == requested_value:
            continue
        if bool(getattr(records_by_name.get(name), "mutable_after_start", False)):
            continue
        raise RuntimeError(f"Runtime var '{name}' cannot be changed after run start.")
    return requested_resolved


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
