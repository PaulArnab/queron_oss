from __future__ import annotations

from typing import Any
import os
from pathlib import Path

import yaml


def _load_yaml_object(yaml_text: str | None, *, label: str) -> dict[str, Any]:
    text = str(yaml_text or "").strip()
    if not text:
        return {}
    parsed = yaml.safe_load(text)
    if parsed is None:
        return {}
    if not isinstance(parsed, dict):
        raise RuntimeError(f"{label} must deserialize to an object.")
    return parsed


def load_config(yaml_text: str | None) -> dict[str, Any]:
    return _load_yaml_object(yaml_text, label="Pipeline configuration YAML")


def load_connections_config(
    yaml_text: str | None = None,
    *,
    yaml_path: str | None = None,
) -> dict[str, Any]:
    text = str(yaml_text or "")
    if not text.strip():
        resolved_path = _resolve_connections_path(yaml_path)
        if resolved_path is None:
            return {}
        text = resolved_path.read_text(encoding="utf-8")
    return _load_yaml_object(text, label="connections.yaml")


def _resolve_connections_path(explicit_path: str | None = None) -> Path | None:
    for candidate in (
        str(explicit_path or "").strip(),
        str(os.environ.get("QUERON_CONNECTIONS_FILE") or "").strip(),
        "connections.yaml",
    ):
        if not candidate:
            continue
        path = Path(candidate)
        if path.exists() and path.is_file():
            return path
    return None


def _resolve_binding_value(binding_name: str, field_name: str, value: Any) -> Any:
    if field_name.endswith("_env"):
        env_name = str(value or "").strip()
        if not env_name:
            raise RuntimeError(
                f"Connection '{binding_name}' has an empty environment variable reference for '{field_name}'."
            )
        resolved = os.environ.get(env_name)
        if resolved is None:
            raise RuntimeError(
                f"Connection '{binding_name}' requires environment variable '{env_name}' for '{field_name}'."
            )
        return resolved
    return value


def resolve_connection_binding(binding_name: str, config: dict[str, Any]) -> dict[str, Any]:
    connections = config.get("connections") or {}
    if not isinstance(connections, dict):
        raise RuntimeError("connections.yaml must define connections as an object.")

    raw_binding = connections.get(binding_name)
    if raw_binding is None:
        raise RuntimeError(f"Connection '{binding_name}' is not defined in connections.yaml.")
    if not isinstance(raw_binding, dict):
        raise RuntimeError(f"Connection '{binding_name}' in connections.yaml is invalid.")

    resolved: dict[str, Any] = {}
    for key, value in raw_binding.items():
        if str(key).endswith("_env"):
            resolved[str(key)[:-4]] = _resolve_binding_value(binding_name, str(key), value)
        else:
            resolved[str(key)] = value

    raw_type = str(resolved.get("type") or "").strip().lower()
    if raw_type in {"postgres", "postgresql"}:
        resolved["type"] = "postgresql"
    elif raw_type == "db2":
        resolved["type"] = "db2"
    else:
        raise RuntimeError(
            f"Connection '{binding_name}' must declare a supported type ('postgres' or 'db2')."
        )

    resolved.setdefault("name", binding_name)
    return resolved


def resolve_target(config: dict[str, Any], explicit_target: str | None = None) -> str | None:
    for candidate in (
        str(explicit_target or "").strip(),
        str(os.environ.get("QUERON_TARGET") or "").strip(),
        str(config.get("target") or "").strip(),
    ):
        if candidate:
            return candidate
    return None


def _relation_entries(config: dict[str, Any], section_name: str, *, label: str) -> dict[str, Any]:
    entries = config.get(section_name) or {}
    if not isinstance(entries, dict):
        raise RuntimeError(f"Pipeline configuration must define {label} as an object.")
    return entries


def _resolve_named_relation(
    entries: dict[str, Any],
    relation_name: str,
    *,
    target: str | None,
    label: str,
) -> str:
    branch: Any = entries.get(relation_name)
    if branch is None:
        raise RuntimeError(f"{label} '{relation_name}' is not defined in configurations.yaml.")
    if isinstance(branch, dict) and target:
        if target in branch:
            branch = branch[target]
        elif isinstance(branch.get("relation"), dict) and target in branch["relation"]:
            branch = {"relation": branch["relation"][target]}

    if isinstance(branch, str):
        return quote_relation(branch)
    if not isinstance(branch, dict):
        raise RuntimeError(f"{label} '{relation_name}' for target '{target or 'default'}' is invalid.")

    relation = branch.get("relation")
    if isinstance(relation, str) and relation.strip():
        return quote_relation(relation)

    table = str(branch.get("table") or "").strip()
    if not table:
        raise RuntimeError(f"{label} '{relation_name}' must define a table for target '{target or 'default'}'.")

    parts = []
    database = str(branch.get("database") or "").strip()
    schema = str(branch.get("schema") or "").strip()
    if database:
        parts.append(database)
    if schema:
        parts.append(schema)
    parts.append(table)
    return ".".join(_quote_identifier(part) for part in parts)


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def quote_relation(relation: str) -> str:
    raw = str(relation or "").strip()
    if not raw:
        raise RuntimeError("Source relation is empty.")
    if '"' in raw:
        return raw
    parts = [part.strip() for part in raw.split(".") if part.strip()]
    if not parts:
        raise RuntimeError("Source relation is empty.")
    return ".".join(_quote_identifier(part) for part in parts)


def resolve_source_relation(source_name: str, config: dict[str, Any], target: str | None) -> str:
    sources = _relation_entries(config, "sources", label="sources")
    return _resolve_named_relation(sources, source_name, target=target, label="Source")


def try_resolve_egress_relation(egress_name: str, config: dict[str, Any], target: str | None) -> str | None:
    egress_entries = config.get("egress")
    if egress_entries is None:
        return None
    if not isinstance(egress_entries, dict):
        raise RuntimeError("Pipeline configuration must define egress as an object.")
    relation_name = str(egress_name or "").strip()
    if not relation_name or relation_name not in egress_entries:
        return None
    return _resolve_named_relation(egress_entries, relation_name, target=target, label="Egress target")
