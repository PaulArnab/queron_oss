from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import load_config, resolve_source_relation, resolve_target, try_resolve_egress_relation
from .specs import NodeSpec, PipelineSpec
from .templates import extract_refs, extract_sources, find_raw_compound_relations, has_raw_reference_to_name, render_sql

_FILE_INGRESS_KIND_TO_FORMAT = {
    "csv.ingress": "csv",
    "jsonl.ingress": "jsonl",
    "parquet.ingress": "parquet",
}
_ALL_FILE_INGRESS_KINDS = set(_FILE_INGRESS_KIND_TO_FORMAT) | {"file.ingress"}


@dataclass
class CompiledPipeline:
    spec: PipelineSpec | None
    diagnostics: list[dict[str, Any]]
    module_globals: dict[str, Any]


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


def _load_module_from_code(code: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    diagnostics: list[dict[str, Any]] = []
    module_globals: dict[str, Any] = {"__name__": "__queron_generated__"}
    try:
        compiled = compile(code, "<queron_generated_pipeline>", "exec")
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


def compile_pipeline_code(code: str, *, yaml_text: str | None = None, target: str | None = None) -> CompiledPipeline:
    module_globals, diagnostics = _load_module_from_code(code)
    if diagnostics:
        return CompiledPipeline(spec=None, diagnostics=diagnostics, module_globals=module_globals)

    nodes, native_metadata = _collect_nodes(module_globals)
    config = load_config(yaml_text)
    effective_target = resolve_target(config, target)
    spec = PipelineSpec(
        pipeline_id=(
            str(native_metadata.get("pipeline_id") or native_metadata.get("notebook_id") or "").strip() or None
        ),
        pipeline_name=(
            str(native_metadata.get("pipeline_name") or native_metadata.get("notebook_name") or "").strip() or None
        ),
        target=effective_target,
        nodes=nodes,
        native_metadata=native_metadata,
    )
    diagnostics.extend(_validate_and_enrich_spec(spec, config))
    return CompiledPipeline(spec=spec, diagnostics=diagnostics, module_globals=module_globals)


def _validate_and_enrich_spec(spec: PipelineSpec, config: dict[str, Any]) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    node_names: set[str] = set()
    out_producers: dict[str, NodeSpec] = {}
    normalized_out_producers: dict[str, tuple[str, NodeSpec]] = {}
    artifact_node_kinds = {"postgres.ingress", "db2.ingress", "python.ingress", *list(_ALL_FILE_INGRESS_KINDS), "model.sql"}
    query_node_kinds = {
        "model.sql",
        "check.count",
        "check.boolean",
        "postgres.egress",
        "db2.egress",
        "parquet.egress",
        "csv.egress",
        "jsonl.egress",
    }
    supported_kinds = artifact_node_kinds | query_node_kinds
    valid_count_operators = {"=", "==", "!=", ">", ">=", "<", "<="}
    valid_egress_modes = {"replace", "append", "create", "create_append"}
    valid_file_formats = {"csv", "parquet", "jsonl"}

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
        if node.kind in {"postgres.egress", "db2.egress"}:
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

        if node.kind in query_node_kinds or node.kind in {"postgres.ingress", "db2.ingress", "model.sql"}:
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

        if node.kind in {"postgres.ingress", "db2.ingress", "postgres.egress", "db2.egress"} and not node.config:
            diagnostics.append(
                {
                    "level": "error",
                    "code": "missing_config",
                    "message": f"Node '{node.name}' is missing a config binding.",
                    "node_name": node.name,
                }
            )

        if node.kind in {"postgres.egress", "db2.egress"}:
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
