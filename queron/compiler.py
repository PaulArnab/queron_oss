from __future__ import annotations

import ast
from dataclasses import dataclass
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import site
import sys
import sysconfig
from typing import Any

from .config import load_config, resolve_source_relation, resolve_target, try_resolve_egress_relation
from .runtime_models import CompiledContractRecord, PipelineVarRecord
from .runtime_vars import _VAR_PATTERN, parse_runtime_var_options
from .specs import NodeSpec, PipelineSpec
from .templates import extract_refs, extract_sources, find_raw_compound_relations, has_raw_reference_to_name, render_sql

_FILE_INGRESS_KIND_TO_FORMAT = {
    "csv.ingress": "csv",
    "jsonl.ingress": "jsonl",
    "parquet.ingress": "parquet",
}
_ALL_FILE_INGRESS_KINDS = set(_FILE_INGRESS_KIND_TO_FORMAT) | {"file.ingress"}
_SQL_VAR_VALUE_PRECEDERS = {"=", "<>", "!=", ">", ">=", "<", "<=", "(", ",", "between", "and", "like", "ilike"}
_SQL_VAR_FORBIDDEN_PRECEDERS = {"from", "join", "update", "into", "table", "select", "order", "group", "by", "as"}
_SQL_WORD_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*$")
_SQL_OPERATOR_RE = re.compile(r"(<>|!=|>=|<=|=|>|<|\(|,)$")


@dataclass
class CompiledPipeline:
    spec: PipelineSpec | None
    diagnostics: list[dict[str, Any]]
    module_globals: dict[str, Any]
    contract: CompiledContractRecord | None = None


_PROJECT_DIR_EXCLUDES = {".git", ".venv", ".queron", "__pycache__"}


def _has_error_diagnostics(diagnostics: list[dict[str, Any]]) -> bool:
    return any(str(item.get("level") or "").strip().lower() == "error" for item in diagnostics)


def _normalize_for_hash(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _normalize_for_hash(val) for key, val in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_normalize_for_hash(item) for item in value]
    if isinstance(value, set):
        return [_normalize_for_hash(item) for item in sorted(value, key=lambda item: json.dumps(_normalize_for_hash(item), sort_keys=True))]
    return value


def _hash_json(value: Any) -> str:
    normalized = _normalize_for_hash(value)
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _hash_file(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _resolve_path(value: str | Path | None) -> Path | None:
    if value is None:
        return None
    try:
        return Path(value).expanduser().resolve()
    except Exception:
        return None


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _display_path(path: Path, *, project_root: Path | None) -> str:
    if project_root is not None and _is_relative_to(path, project_root):
        return path.relative_to(project_root).as_posix()
    return str(path)


def _iter_project_python_files(project_root: Path) -> list[Path]:
    files: list[Path] = []
    for path in sorted(project_root.rglob("*.py")):
        if any(part in _PROJECT_DIR_EXCLUDES for part in path.parts):
            continue
        if not path.is_file():
            continue
        files.append(path.resolve())
    return files


def _module_name_for_project_file(project_root: Path, file_path: Path) -> str:
    relative = file_path.relative_to(project_root).with_suffix("")
    parts = list(relative.parts)
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _resolve_import_module_name(*, current_module: str, module_name: str | None, level: int) -> str | None:
    if level <= 0:
        return str(module_name or "").strip() or None

    current_parts = [part for part in str(current_module or "").split(".") if part]
    package_parts = current_parts[:-1]
    if level > 1:
        if level - 1 > len(package_parts):
            return None
        package_parts = package_parts[: len(package_parts) - (level - 1)]
    base = ".".join(package_parts)
    suffix = str(module_name or "").strip()
    if base and suffix:
        return f"{base}.{suffix}"
    if base:
        return base
    return suffix or None


def _collect_import_module_names(project_root: Path, file_path: Path) -> list[str]:
    try:
        tree = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
    except Exception:
        return []

    current_module = _module_name_for_project_file(project_root, file_path)
    module_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = str(alias.name or "").strip()
                if name:
                    module_names.add(name)
        elif isinstance(node, ast.ImportFrom):
            resolved_name = _resolve_import_module_name(
                current_module=current_module,
                module_name=node.module,
                level=int(node.level or 0),
            )
            if resolved_name:
                module_names.add(resolved_name)
    return sorted(module_names)


def _library_roots() -> list[Path]:
    roots: list[Path] = []
    for key in ("stdlib", "platstdlib", "purelib", "platlib"):
        resolved = _resolve_path(sysconfig.get_paths().get(key))
        if resolved is not None:
            roots.append(resolved)
    try:
        for item in site.getsitepackages():
            resolved = _resolve_path(item)
            if resolved is not None:
                roots.append(resolved)
    except Exception:
        pass
    try:
        user_site = site.getusersitepackages()
        resolved = _resolve_path(user_site)
        if resolved is not None:
            roots.append(resolved)
    except Exception:
        pass
    roots.append(Path(__file__).resolve().parents[1])
    deduped: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        key = str(root)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(root)
    return deduped


def _resolve_module_origin(module_name: str, *, project_root: Path) -> str | None:
    original_sys_path = list(sys.path)
    try:
        project_root_text = str(project_root)
        if project_root_text not in sys.path:
            sys.path.insert(0, project_root_text)
        spec = importlib.util.find_spec(module_name)
    except Exception:
        spec = None
    finally:
        sys.path[:] = original_sys_path
    if spec is None:
        return None
    origin = getattr(spec, "origin", None)
    if origin in {"built-in", "frozen"}:
        return str(origin)
    if origin is None:
        return None
    return str(origin)


def _validate_project_imports(project_root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    diagnostics: list[dict[str, Any]] = []
    external_dependencies: list[dict[str, Any]] = []
    seen_dependencies: set[tuple[str, str, str]] = set()
    library_roots = _library_roots()

    for file_path in _iter_project_python_files(project_root):
        for module_name in _collect_import_module_names(project_root, file_path):
            origin = _resolve_module_origin(module_name, project_root=project_root)
            if origin in {"built-in", "frozen"}:
                key = (module_name, str(origin), "library")
                if key not in seen_dependencies:
                    seen_dependencies.add(key)
                    external_dependencies.append({"module": module_name, "path": str(origin), "kind": "library"})
                continue
            resolved_origin = _resolve_path(origin)
            if resolved_origin is None:
                continue
            if _is_relative_to(resolved_origin, project_root):
                continue
            if any(_is_relative_to(resolved_origin, root) for root in library_roots):
                key = (module_name, str(resolved_origin), "library")
                if key not in seen_dependencies:
                    seen_dependencies.add(key)
                    external_dependencies.append(
                        {"module": module_name, "path": str(resolved_origin), "kind": "library"}
                    )
                continue
            diagnostics.append(
                {
                    "level": "error",
                    "code": "external_python_import_not_allowed",
                    "message": (
                        f"Project file '{_display_path(file_path, project_root=project_root)}' imports local Python "
                        f"module '{module_name}' from '{resolved_origin}', which is outside the pipeline project root."
                    ),
                }
            )
    return external_dependencies, diagnostics


def _tracked_config_entry(
    *,
    config_path: Path | None,
    config_text: str | None,
    project_root: Path,
) -> tuple[str, list[dict[str, Any]]]:
    exists = config_path is not None and config_path.exists()
    tracked_files: list[dict[str, Any]] = []
    if config_path is not None:
        tracked_files.append(
            {
                "path": _display_path(config_path, project_root=project_root),
                "kind": "config",
                "hash": _hash_json({"exists": exists, "content": config_text or ""}),
            }
        )
    return _hash_json({"exists": exists, "content": config_text or ""}), tracked_files


def _previous_meaningful_sql_token(sql_text: str, start_index: int) -> str | None:
    prefix = str(sql_text or "")[: max(0, int(start_index))]
    stripped = prefix.rstrip()
    if not stripped:
        return None
    operator_match = _SQL_OPERATOR_RE.search(stripped)
    if operator_match:
        return operator_match.group(1).lower()
    word_match = _SQL_WORD_RE.search(stripped)
    if word_match:
        return word_match.group(0).lower()
    return None


def _sql_var_previous_context(sql_text: str, start_index: int) -> tuple[str | None, str | None]:
    previous_token = _previous_meaningful_sql_token(sql_text, start_index)
    previous_non_group_token = previous_token
    if previous_token == "(":
        prefix = str(sql_text or "")[: max(0, int(start_index))]
        open_index = prefix.rstrip().rfind("(")
        if open_index >= 0:
            previous_non_group_token = _previous_meaningful_sql_token(prefix, open_index)
    return previous_token, previous_non_group_token


def _infer_sql_var_kind(sql_text: str, start_index: int) -> str:
    previous_token, previous_non_group_token = _sql_var_previous_context(sql_text, start_index)
    if previous_token == "in" or previous_non_group_token == "in":
        return "list"
    if previous_token in _SQL_VAR_VALUE_PRECEDERS:
        return "scalar"
    return "scalar"


def _extract_node_var_references(node: NodeSpec) -> list[dict[str, Any]]:
    existing = node.metadata.get("queron_vars")
    if isinstance(existing, list):
        return [item for item in existing if isinstance(item, dict)]
    sql_text = str(getattr(node, "sql", "") or "").strip()
    if not sql_text:
        return []
    refs: list[dict[str, Any]] = []
    for match in _VAR_PATTERN.finditer(sql_text):
        name = str(match.group("name") or "").strip()
        if not name:
            continue
        options, _unknown_options, _option_error = parse_runtime_var_options(name, match.group("options"))
        refs.append(
            {
                "name": name,
                "kind": _infer_sql_var_kind(sql_text, match.start()),
                "log_value": bool(options.get("log_value")),
                "mutable_after_start": bool(options.get("mutable_after_start")),
                "default": options.get("default"),
                "has_default": bool(options.get("has_default")),
            }
        )
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for item in refs:
        deduped[(str(item["name"]), str(item["kind"]))] = item
    return list(deduped.values())


def _validate_node_var_references(node: NodeSpec) -> list[dict[str, Any]]:
    sql_text = str(getattr(node, "sql", "") or "").strip()
    node_refs: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    if not sql_text:
        node.metadata["queron_vars"] = node_refs
        return diagnostics

    for match in _VAR_PATTERN.finditer(sql_text):
        name = str(match.group("name") or "").strip()
        if not name:
            continue
        options, unknown_options, option_error = parse_runtime_var_options(name, match.group("options"))
        if option_error:
            diagnostics.append(
                {
                    "level": "error",
                    "code": "invalid_runtime_var_option",
                    "message": f"Node '{node.name}' has invalid queron.var(...) options for '{name}': {option_error}",
                    "node_name": node.name,
                }
            )
            continue
        if unknown_options:
            diagnostics.append(
                {
                    "level": "error",
                    "code": "unsupported_runtime_var_option",
                    "message": (
                        f"Node '{node.name}' uses unsupported queron.var(...) option(s): "
                        f"{', '.join(sorted(set(unknown_options)))}."
                    ),
                    "node_name": node.name,
                }
            )
            continue
        previous_token, previous_non_group_token = _sql_var_previous_context(sql_text, match.start())
        if previous_token in _SQL_VAR_FORBIDDEN_PRECEDERS or previous_non_group_token in _SQL_VAR_FORBIDDEN_PRECEDERS:
            diagnostics.append(
                {
                    "level": "error",
                    "code": "invalid_runtime_var_placement",
                    "message": (
                        f"Node '{node.name}' uses runtime var '{name}' in a forbidden SQL position near '{previous_non_group_token or previous_token}'. "
                        "Runtime vars are allowed only in SQL value positions."
                    ),
                    "node_name": node.name,
                }
            )
            continue
        if previous_non_group_token != "in" and previous_token not in _SQL_VAR_VALUE_PRECEDERS:
            diagnostics.append(
                {
                    "level": "error",
                    "code": "unsupported_runtime_var_placement",
                    "message": (
                        f"Node '{node.name}' uses runtime var '{name}' in an unsupported SQL position. "
                        "Allowed placements are scalar value positions and explicit IN-list positions."
                    ),
                    "node_name": node.name,
                }
            )
            continue
        node_refs.append(
            {
                "name": name,
                "kind": "list" if previous_non_group_token == "in" else "scalar",
                "log_value": bool(options.get("log_value")),
                "mutable_after_start": bool(options.get("mutable_after_start")),
                "default": options.get("default"),
                "has_default": bool(options.get("has_default")),
            }
        )

    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for item in node_refs:
        deduped[(str(item["name"]), str(item["kind"]))] = item
    node.metadata["queron_vars"] = list(deduped.values())
    return diagnostics


def _collect_pipeline_var_records(nodes: list[NodeSpec]) -> tuple[list[PipelineVarRecord], list[dict[str, Any]]]:
    by_name: dict[str, dict[str, Any]] = {}
    diagnostics: list[dict[str, Any]] = []
    for node in nodes:
        for ref in _extract_node_var_references(node):
            name = str(ref.get("name") or "").strip()
            if not name:
                continue
            record = by_name.setdefault(
                name,
                {
                    "name": name,
                    "kind": str(ref.get("kind") or "scalar"),
                    "required": not bool(ref.get("has_default")),
                    "default": ref.get("default"),
                    "log_value": bool(ref.get("log_value")),
                    "mutable_after_start": bool(ref.get("mutable_after_start")),
                    "used_in_nodes": [],
                },
            )
            ref_kind = str(ref.get("kind") or "scalar")
            if str(record["kind"]) != ref_kind:
                diagnostics.append(
                    {
                        "level": "error",
                        "code": "conflicting_runtime_var_kind",
                        "message": (
                            f"Runtime var '{name}' is used as both '{record['kind']}' and '{ref_kind}' in the pipeline. "
                            "A runtime var must keep one usage kind across the whole pipeline."
                        ),
                        "node_name": node.name,
                    }
                )
            if ref_kind == "list":
                record["kind"] = "list"
            ref_log_value = bool(ref.get("log_value"))
            if bool(record["log_value"]) != ref_log_value:
                diagnostics.append(
                    {
                        "level": "error",
                        "code": "conflicting_runtime_var_log_value",
                        "message": (
                            f"Runtime var '{name}' uses conflicting log_value settings in the pipeline. "
                            "A runtime var must keep one log_value setting across the whole pipeline."
                        ),
                        "node_name": node.name,
                    }
                )
            ref_mutable_after_start = bool(ref.get("mutable_after_start"))
            if bool(record["mutable_after_start"]) != ref_mutable_after_start:
                diagnostics.append(
                    {
                        "level": "error",
                        "code": "conflicting_runtime_var_mutability",
                        "message": (
                            f"Runtime var '{name}' uses conflicting mutable_after_start settings in the pipeline. "
                            "A runtime var must keep one mutable_after_start setting across the whole pipeline."
                        ),
                        "node_name": node.name,
                    }
                )
            ref_has_default = bool(ref.get("has_default"))
            if bool(ref_has_default) != (not bool(record["required"])):
                diagnostics.append(
                    {
                        "level": "error",
                        "code": "conflicting_runtime_var_default",
                        "message": (
                            f"Runtime var '{name}' uses conflicting default settings in the pipeline. "
                            "A runtime var must either always declare a default or never declare one."
                        ),
                        "node_name": node.name,
                    }
                )
            if bool(ref_has_default) and record["default"] != ref.get("default"):
                diagnostics.append(
                    {
                        "level": "error",
                        "code": "conflicting_runtime_var_default_value",
                        "message": (
                            f"Runtime var '{name}' uses conflicting default values in the pipeline. "
                            "A runtime var must keep one default value across the whole pipeline."
                        ),
                        "node_name": node.name,
                    }
                )
            if node.name not in record["used_in_nodes"]:
                record["used_in_nodes"].append(node.name)
    records = [
        PipelineVarRecord(
            name=item["name"],
            kind=item["kind"],
            required=bool(item["required"]),
            default=item["default"],
            log_value=bool(item["log_value"]),
            mutable_after_start=bool(item["mutable_after_start"]),
            used_in_nodes=sorted(str(value) for value in item["used_in_nodes"] if str(value).strip()),
        )
        for item in sorted(by_name.values(), key=lambda entry: entry["name"])
    ]
    return records, diagnostics


def _normalized_node_payload(node: NodeSpec) -> dict[str, Any]:
    return {
        "name": node.name,
        "function_name": node.function_name,
        "kind": node.kind,
        "sql": node.sql,
        "resolved_sql": node.resolved_sql,
        "input_path": node.input_path,
        "file_format": node.file_format,
        "operator": node.operator,
        "value": node.value,
        "config": node.config,
        "out": node.out,
        "materialized": node.materialized,
        "target_relation": node.target_relation,
        "output_path": node.output_path,
        "mode": node.mode,
        "overwrite": node.overwrite,
        "compression": node.compression,
        "delimiter": node.delimiter,
        "quote": node.quote,
        "escape": node.escape,
        "skip_rows": node.skip_rows,
        "columns": dict(sorted((node.columns or {}).items())),
        "header": node.header,
        "connection_type": node.connection_type,
        "target_table": node.target_table,
        "dependencies": sorted(node.dependencies),
        "refs": sorted(node.refs),
        "sources": sorted(node.sources),
        "resolved_sources": dict(sorted((node.resolved_sources or {}).items())),
        "vars": _extract_node_var_references(node),
    }


def _build_compile_contract(
    *,
    spec: PipelineSpec,
    diagnostics: list[dict[str, Any]],
    pipeline_path: Path,
    artifact_path: Path,
    config_path: Path | None,
    config_text: str | None,
) -> tuple[CompiledContractRecord | None, list[dict[str, Any]]]:
    project_root = pipeline_path.parent.resolve()
    external_dependencies, import_diagnostics = _validate_project_imports(project_root)
    if _has_error_diagnostics(import_diagnostics):
        return None, import_diagnostics

    tracked_python_files: list[dict[str, Any]] = []
    for file_path in _iter_project_python_files(project_root):
        tracked_python_files.append(
            {
                "path": _display_path(file_path, project_root=project_root),
                "kind": "project_python",
                "hash": _hash_file(file_path),
            }
        )
    project_python_hash = _hash_json(tracked_python_files)
    config_hash, tracked_config_files = _tracked_config_entry(
        config_path=config_path,
        config_text=config_text,
        project_root=project_root,
    )
    tracked_files = [*tracked_python_files, *tracked_config_files]
    node_hashes_json: list[dict[str, Any]] = []
    normalized_nodes: list[dict[str, Any]] = []
    for node in sorted(spec.nodes, key=lambda item: item.name):
        payload = _normalized_node_payload(node)
        normalized_nodes.append(payload)
        node_hashes_json.append(
            {
                "node_name": node.name,
                "node_kind": node.kind,
                "hash": _hash_json(payload),
            }
        )

    edges_json = sorted([[dependency, node.name] for node in spec.nodes for dependency in node.dependencies])
    edge_hash = _hash_json(edges_json)
    vars_json, var_diagnostics = _collect_pipeline_var_records(spec.nodes)
    if _has_error_diagnostics(var_diagnostics):
        return None, var_diagnostics
    spec_json = {
        "pipeline_id": spec.pipeline_id,
        "target": spec.target,
        "nodes": normalized_nodes,
        "vars": [item.model_dump() for item in vars_json],
    }
    contract_hash = _hash_json(
        {
            "pipeline_id": spec.pipeline_id,
            "target": spec.target,
            "node_hashes": node_hashes_json,
            "edge_hash": edge_hash,
            "config_hash": config_hash,
            "project_python_hash": project_python_hash,
            "vars": [item.model_dump() for item in vars_json],
        }
    )
    contract = CompiledContractRecord(
        pipeline_id=str(spec.pipeline_id),
        pipeline_path=str(pipeline_path),
        project_root=str(project_root),
        artifact_path=str(artifact_path),
        config_path=str(config_path) if config_path is not None else None,
        target=spec.target,
        is_active=True,
        contract_hash=contract_hash,
        edge_hash=edge_hash,
        config_hash=config_hash,
        project_python_hash=project_python_hash,
        node_hashes_json=node_hashes_json,
        edges_json=edges_json,
        tracked_files_json=tracked_files,
        external_dependencies_json=external_dependencies,
        vars_json=vars_json,
        spec_json=spec_json,
        diagnostics_json=list(diagnostics),
    )
    return contract, import_diagnostics


def _sanitize_artifact_name(value: str | None, fallback: str) -> str:
    raw = str(value or "").strip().lower()
    cleaned: list[str] = []
    previous_underscore = False
    for char in raw:
        if char.isalnum() or char == "_":
            cleaned.append(char)
            previous_underscore = False
            continue
        if not previous_underscore:
            cleaned.append("_")
        previous_underscore = True
    joined = "".join(cleaned).strip("_")
    if not joined:
        joined = fallback
    if not joined[0].isalpha() and joined[0] != "_":
        joined = f"n_{joined}"
    return joined[:120]


def _normalize_file_format(value: str | None, *, path: str | None = None) -> str | None:
    text = str(value or "").strip().lower()
    if text in {"jsonl", "ndjson"}:
        return "jsonl"
    if text in {"csv", "parquet"}:
        return text
    if text:
        return text
    suffix = str(path or "").strip().lower()
    if suffix.endswith(".jsonl") or suffix.endswith(".ndjson"):
        return "jsonl"
    if suffix.endswith(".csv"):
        return "csv"
    if suffix.endswith(".parquet"):
        return "parquet"
    return None


def _file_format_for_kind(kind: str, *, value: str | None = None, path: str | None = None) -> str | None:
    if kind in _FILE_INGRESS_KIND_TO_FORMAT:
        return _FILE_INGRESS_KIND_TO_FORMAT[kind]
    return _normalize_file_format(value, path=path)


def _load_module_from_code(
    code: str,
    *,
    source_path: str | Path | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    diagnostics: list[dict[str, Any]] = []
    resolved_source_path = _resolve_path(source_path)
    module_globals: dict[str, Any] = {"__name__": "__queron_generated__", "__file__": str(resolved_source_path or "<queron_generated_pipeline>")}
    original_sys_path = list(sys.path)
    try:
        if resolved_source_path is not None:
            project_root_text = str(resolved_source_path.parent)
            if project_root_text not in sys.path:
                sys.path.insert(0, project_root_text)
        compiled = compile(code, str(resolved_source_path or "<queron_generated_pipeline>"), "exec")
        exec(compiled, module_globals, module_globals)
    except SyntaxError as exc:
        diagnostics.append(
            {
                "level": "error",
                "code": "python_syntax_error",
                "message": str(exc.msg or "Generated Python is invalid."),
                "line": exc.lineno,
                "column": exc.offset,
            }
        )
        return module_globals, diagnostics
    except Exception as exc:
        diagnostics.append(
            {
                "level": "error",
                "code": "python_load_error",
                "message": str(exc),
            }
        )
    finally:
        sys.path[:] = original_sys_path
    return module_globals, diagnostics


def _collect_nodes(module_globals: dict[str, Any]) -> tuple[list[NodeSpec], dict[str, Any]]:
    native_metadata = module_globals.get("__queron_native__")
    native = native_metadata if isinstance(native_metadata, dict) else {}
    native_cells = native.get("cells") if isinstance(native.get("cells"), dict) else {}
    nodes: list[NodeSpec] = []

    for attr_name, value in module_globals.items():
        raw_node = getattr(value, "__queron_node__", None)
        if not isinstance(raw_node, dict):
            continue
        logical_name = str(raw_node.get("name") or attr_name).strip() or attr_name
        node_kind = str(raw_node.get("kind") or "")
        native_entry = native_cells.get(logical_name)
        if not isinstance(native_entry, dict):
            native_entry = {}
        sql_text = str(raw_node.get("sql") or raw_node.get("query") or "")
        raw_path = str(raw_node.get("path") or "").strip() or None
        nodes.append(
            NodeSpec(
                name=logical_name,
                function_name=str(raw_node.get("function_name") or attr_name),
                kind=node_kind,
                sql=sql_text,
                input_path=raw_path if node_kind in _ALL_FILE_INGRESS_KINDS else None,
                file_format=_file_format_for_kind(
                    node_kind,
                    value=str(raw_node.get("format") or "").strip() or None,
                    path=raw_path,
                ),
                operator=str(raw_node.get("operator") or "").strip() or None,
                value=raw_node.get("value"),
                config=str(raw_node.get("config") or "").strip() or None,
                out=str(raw_node.get("out") or "").strip() or None,
                materialized=str(raw_node.get("materialized") or "").strip() or None,
                target_relation=str(raw_node.get("table") or "").strip() or None,
                output_path=raw_path if node_kind in {"parquet.egress", "csv.egress", "jsonl.egress"} else None,
                mode=str(raw_node.get("mode") or "").strip() or None,
                overwrite=bool(raw_node["overwrite"]) if "overwrite" in raw_node else None,
                compression=str(raw_node.get("compression") or "").strip() or None,
                delimiter=str(raw_node.get("delimiter") or "").strip() or None,
                quote=(str(raw_node.get("quote")) if "quote" in raw_node and raw_node.get("quote") is not None else None),
                escape=(str(raw_node.get("escape")) if "escape" in raw_node and raw_node.get("escape") is not None else None),
                skip_rows=int(raw_node.get("skip_rows") or 0) if "skip_rows" in raw_node else None,
                columns=dict(raw_node.get("columns") or {}) if isinstance(raw_node.get("columns"), dict) else None,
                header=bool(raw_node["header"]) if "header" in raw_node else None,
                cell_id=int(native_entry.get("cell_id")) if native_entry.get("cell_id") is not None else None,
                connection_id=str(native_entry.get("connection_id") or "").strip() or None,
                connection_type=str(native_entry.get("connection_type") or "").strip() or None,
                metadata=native_entry,
            )
        )
    return nodes, native


def compile_pipeline_code(
    code: str,
    *,
    yaml_text: str | None = None,
    target: str | None = None,
    source_path: str | Path | None = None,
    artifact_path: str | Path | None = None,
    config_path: str | Path | None = None,
) -> CompiledPipeline:
    module_globals, diagnostics = _load_module_from_code(code, source_path=source_path)
    if diagnostics:
        return CompiledPipeline(spec=None, diagnostics=diagnostics, module_globals=module_globals)

    nodes, native_metadata = _collect_nodes(module_globals)
    config = load_config(yaml_text)
    effective_target = resolve_target(config, target)
    spec = PipelineSpec(
        pipeline_id=(
            str(native_metadata.get("pipeline_id") or native_metadata.get("notebook_id") or "").strip() or None
        ),
        target=effective_target,
        nodes=nodes,
        native_metadata=native_metadata,
    )
    diagnostics.extend(_validate_and_enrich_spec(spec, config))
    contract: CompiledContractRecord | None = None
    if not _has_error_diagnostics(diagnostics) and source_path is not None and artifact_path is not None:
        built_contract, contract_diagnostics = _build_compile_contract(
            spec=spec,
            diagnostics=diagnostics,
            pipeline_path=Path(source_path).expanduser().resolve(),
            artifact_path=Path(artifact_path).expanduser().resolve(),
            config_path=_resolve_path(config_path),
            config_text=yaml_text,
        )
        diagnostics.extend(contract_diagnostics)
        if not _has_error_diagnostics(contract_diagnostics):
            contract = built_contract
    return CompiledPipeline(spec=spec, diagnostics=diagnostics, module_globals=module_globals, contract=contract)


def _validate_and_enrich_spec(spec: PipelineSpec, config: dict[str, Any]) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    node_names: set[str] = set()
    out_producers: dict[str, NodeSpec] = {}
    normalized_out_producers: dict[str, tuple[str, NodeSpec]] = {}
    artifact_node_kinds = {
        "postgres.ingress",
        "db2.ingress",
        "mssql.ingress",
        "python.ingress",
        *list(_ALL_FILE_INGRESS_KINDS),
        "model.sql",
        "postgres.egress",
        "db2.egress",
        "mssql.egress",
        "parquet.egress",
        "csv.egress",
        "jsonl.egress",
    }
    query_node_kinds = {"model.sql", "check.count", "check.boolean", *list(artifact_node_kinds)}
    supported_kinds = artifact_node_kinds | query_node_kinds
    valid_count_operators = {"=", "==", "!=", ">", ">=", "<", "<="}
    valid_egress_modes = {"replace", "append", "create", "create_append"}
    valid_file_formats = {"csv", "parquet", "jsonl"}

    if not str(spec.pipeline_id or "").strip():
        diagnostics.append(
            {
                "level": "error",
                "code": "missing_pipeline_id",
                "message": "Pipeline is missing a required pipeline_id in __queron_native__.",
            }
        )

    for node in spec.nodes:
        if node.name in node_names:
            diagnostics.append(
                {
                    "level": "error",
                    "code": "duplicate_node_name",
                    "message": f"Duplicate pipeline node name '{node.name}'.",
                    "node_name": node.name,
                }
            )
        node_names.add(node.name)

        logical_out = str(node.out or "").strip()
        if node.kind in artifact_node_kinds and not logical_out:
            diagnostics.append(
                {
                    "level": "error",
                    "code": "missing_out_name",
                    "message": f"Node '{node.name}' is missing an explicit out value.",
                    "node_name": node.name,
                }
            )
            continue
        if node.kind in artifact_node_kinds:
            if logical_out in out_producers:
                diagnostics.append(
                    {
                        "level": "error",
                        "code": "duplicate_out_name",
                        "message": f"Duplicate pipeline output '{logical_out}'.",
                        "node_name": node.name,
                    }
                )
            else:
                out_producers[logical_out] = node

            normalized_out = _sanitize_artifact_name(logical_out, node.name)
            prior = normalized_out_producers.get(normalized_out)
            if prior is not None:
                prior_out, _prior_node = prior
                diagnostics.append(
                    {
                        "level": "error",
                        "code": "duplicate_out_name",
                        "message": (
                            f"Pipeline outputs '{prior_out}' and '{logical_out}' both resolve to the same target table "
                            f"'main.{normalized_out}'."
                        ),
                        "node_name": node.name,
                    }
                )
            else:
                normalized_out_producers[normalized_out] = (logical_out, node)
            node.target_table = f"main.{normalized_out}"
        else:
            node.target_table = None

    for node in spec.nodes:
        refs = extract_refs(node.sql)
        sources = extract_sources(node.sql)
        node.dependencies = []
        node.refs = list(refs)
        node.sources = list(sources)
        node.resolved_sources = {}

        if node.kind in _ALL_FILE_INGRESS_KINDS:
            node.file_format = _file_format_for_kind(node.kind, value=node.file_format, path=node.input_path)
        if node.kind in {"postgres.egress", "db2.egress", "mssql.egress"}:
            raw_target_relation = str(node.target_relation or "").strip()
            if raw_target_relation:
                try:
                    resolved_target_relation = try_resolve_egress_relation(raw_target_relation, config, spec.target)
                except Exception as exc:
                    diagnostics.append(
                        {
                            "level": "error",
                            "code": "egress_target_resolution_error",
                            "message": f"Egress node '{node.name}' could not resolve target '{raw_target_relation}': {exc}",
                            "node_name": node.name,
                        }
                    )
                else:
                    if resolved_target_relation:
                        node.target_relation = resolved_target_relation

        for ref_name in refs:
            producer = out_producers.get(ref_name)
            if producer is None:
                diagnostics.append(
                    {
                        "level": "error",
                        "code": "unknown_ref",
                        "message": f"Node '{node.name}' references unknown artifact '{ref_name}'.",
                        "node_name": node.name,
                    }
                )
                continue
            node.dependencies.append(producer.name)

        if node.kind in query_node_kinds:
            diagnostics.extend(_validate_node_var_references(node))
            for relation in find_raw_compound_relations(node.sql):
                diagnostics.append(
                    {
                        "level": "error",
                        "code": "raw_relation_reference",
                        "message": f"Node '{node.name}' uses raw relation '{relation}'. Use queron.ref() or queron.source().",
                        "node_name": node.name,
                    }
                )
            for logical_out in out_producers:
                if has_raw_reference_to_name(node.sql, logical_out):
                    diagnostics.append(
                        {
                            "level": "error",
                            "code": "raw_ref_reference",
                            "message": f"Node '{node.name}' references '{logical_out}' directly. Use queron.ref('{logical_out}').",
                            "node_name": node.name,
                        }
                    )
        elif _VAR_PATTERN.search(str(node.sql or "")):
            diagnostics.append(
                {
                    "level": "error",
                    "code": "runtime_vars_not_allowed_in_node_kind",
                    "message": (
                        f"Node '{node.name}' of kind '{node.kind}' uses queron.var(...), "
                        "but runtime vars are only allowed in SQL-bearing nodes."
                    ),
                    "node_name": node.name,
                }
            )

        if node.kind in query_node_kinds or node.kind in {"postgres.ingress", "db2.ingress", "mssql.ingress", "model.sql"}:
            try:
                def _resolve_source(source_name: str) -> str:
                    relation = resolve_source_relation(source_name, config, spec.target)
                    node.resolved_sources[source_name] = relation
                    return relation

                node.resolved_sql = render_sql(
                    node.sql,
                    resolve_ref=lambda ref_name: _resolve_ref_relation(ref_name, out_producers),
                    resolve_source=_resolve_source,
                )
            except Exception as exc:
                diagnostics.append(
                    {
                        "level": "error",
                        "code": "template_resolution_error",
                        "message": f"Node '{node.name}' could not resolve SQL templates: {exc}",
                        "node_name": node.name,
                    }
                )
        else:
            node.resolved_sql = None

        if node.kind in {"postgres.ingress", "db2.ingress", "mssql.ingress", "postgres.egress", "db2.egress", "mssql.egress"} and not node.config:
            diagnostics.append(
                {
                    "level": "error",
                    "code": "missing_config",
                    "message": f"Node '{node.name}' is missing a config binding.",
                    "node_name": node.name,
                }
            )

        if node.kind in {"postgres.egress", "db2.egress", "mssql.egress"}:
            target_relation = str(node.target_relation or "").strip()
            if not target_relation:
                diagnostics.append(
                    {
                        "level": "error",
                        "code": "missing_target_relation",
                        "message": f"Egress node '{node.name}' is missing a target table relation.",
                        "node_name": node.name,
                    }
                )
            mode = str(node.mode or "").strip().lower()
            if mode not in valid_egress_modes:
                diagnostics.append(
                    {
                        "level": "error",
                        "code": "invalid_egress_mode",
                        "message": (
                            f"Egress node '{node.name}' uses unsupported mode '{node.mode}'. "
                            f"Use one of {sorted(valid_egress_modes)}."
                        ),
                        "node_name": node.name,
                    }
                )

        if node.kind in {"parquet.egress", "csv.egress", "jsonl.egress"}:
            output_path = str(node.output_path or "").strip()
            if not output_path:
                diagnostics.append(
                    {
                        "level": "error",
                        "code": "missing_output_path",
                        "message": f"Export node '{node.name}' is missing an output path.",
                        "node_name": node.name,
                    }
                )
            if node.kind == "csv.egress":
                delimiter = str(node.delimiter or "")
                if len(delimiter) != 1:
                    diagnostics.append(
                        {
                            "level": "error",
                            "code": "invalid_csv_delimiter",
                            "message": f"CSV export node '{node.name}' requires a single-character delimiter.",
                            "node_name": node.name,
                        }
                    )

        if node.kind in _ALL_FILE_INGRESS_KINDS:
            input_path = str(node.input_path or "").strip()
            if not input_path:
                diagnostics.append(
                    {
                        "level": "error",
                        "code": "missing_input_path",
                        "message": f"File ingress node '{node.name}' is missing an input path.",
                        "node_name": node.name,
                    }
                )
            if node.file_format not in valid_file_formats:
                diagnostics.append(
                    {
                        "level": "error",
                        "code": "invalid_file_format",
                        "message": (
                            f"File ingress node '{node.name}' uses unsupported format '{node.file_format or ''}'. "
                            f"Use one of {sorted(valid_file_formats)} or a supported file extension."
                        ),
                        "node_name": node.name,
                    }
                )
            if node.file_format == "csv":
                delimiter = str(node.delimiter or ",")
                if len(delimiter) != 1:
                    diagnostics.append(
                        {
                            "level": "error",
                            "code": "invalid_csv_delimiter",
                            "message": f"File ingress node '{node.name}' requires a single-character delimiter.",
                            "node_name": node.name,
                        }
                    )
                for key, value in (("quote", node.quote), ("escape", node.escape)):
                    if value is not None and len(str(value)) != 1:
                        diagnostics.append(
                            {
                                "level": "error",
                                "code": f"invalid_csv_{key}",
                                "message": f"File ingress node '{node.name}' requires a single-character {key}.",
                                "node_name": node.name,
                            }
                        )
                if node.header is False and not node.columns:
                    diagnostics.append(
                        {
                            "level": "error",
                            "code": "missing_csv_columns",
                            "message": f"CSV file ingress node '{node.name}' requires columns when header=False.",
                            "node_name": node.name,
                        }
                    )
            if node.file_format in {"parquet", "jsonl"} and node.columns:
                diagnostics.append(
                    {
                        "level": "error",
                        "code": "unsupported_file_columns",
                        "message": f"File ingress node '{node.name}' does not support manual columns for {node.file_format}.",
                        "node_name": node.name,
                    }
                )
            if node.columns is not None:
                if not isinstance(node.columns, dict) or not node.columns:
                    diagnostics.append(
                        {
                            "level": "error",
                            "code": "invalid_file_columns",
                            "message": f"File ingress node '{node.name}' requires columns to be a non-empty mapping.",
                            "node_name": node.name,
                        }
                    )
                else:
                    normalized_columns: dict[str, str] = {}
                    for raw_name, raw_type in node.columns.items():
                        column_name = str(raw_name or "").strip()
                        column_type = str(raw_type or "").strip().upper()
                        if not column_name or not column_type:
                            diagnostics.append(
                                {
                                    "level": "error",
                                    "code": "invalid_file_column_entry",
                                    "message": f"File ingress node '{node.name}' has an invalid column entry.",
                                    "node_name": node.name,
                                }
                            )
                            continue
                        normalized_columns[column_name] = column_type
                    node.columns = normalized_columns or None

        if node.kind == "check.count":
            operator = str(node.operator or "").strip()
            if operator not in valid_count_operators:
                diagnostics.append(
                    {
                        "level": "error",
                        "code": "invalid_check_operator",
                        "message": (
                            f"Count check node '{node.name}' uses unsupported operator '{operator}'. "
                            f"Use one of {sorted(valid_count_operators)}."
                        ),
                        "node_name": node.name,
                    }
                )
            if not isinstance(node.value, (int, float)) or isinstance(node.value, bool):
                diagnostics.append(
                    {
                        "level": "error",
                        "code": "invalid_check_value",
                        "message": f"Count check node '{node.name}' requires a numeric comparison value.",
                        "node_name": node.name,
                    }
                )

        if node.kind not in supported_kinds:
            diagnostics.append(
                {
                    "level": "error",
                    "code": "unsupported_kind",
                    "message": f"Node '{node.name}' uses unsupported kind '{node.kind}'.",
                    "node_name": node.name,
                }
            )

    diagnostics.extend(_detect_cycles(spec))
    return diagnostics


def _resolve_ref_relation(ref_name: str, out_producers: dict[str, NodeSpec]) -> str:
    producer = out_producers.get(ref_name)
    if producer is None or not producer.target_table:
        raise RuntimeError(f"Unknown ref '{ref_name}'.")
    return ".".join('"' + part.replace('"', '""') + '"' for part in producer.target_table.split("."))


def _detect_cycles(spec: PipelineSpec) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    adjacency = {node.name: list(node.dependencies) for node in spec.nodes}
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node_name: str) -> bool:
        if node_name in visited:
            return False
        if node_name in visiting:
            return True
        visiting.add(node_name)
        for dep in adjacency.get(node_name, []):
            if visit(dep):
                return True
        visiting.remove(node_name)
        visited.add(node_name)
        return False

    for node in spec.nodes:
        if visit(node.name):
            diagnostics.append(
                {
                    "level": "error",
                    "code": "cyclic_dependency",
                    "message": f"Pipeline dependencies contain a cycle involving '{node.name}'.",
                    "node_name": node.name,
                }
            )
            break
    return diagnostics
