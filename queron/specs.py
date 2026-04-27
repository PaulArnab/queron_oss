from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class NodeSpec:
    name: str
    function_name: str
    kind: str
    sql: str
    input_path: str | None = None
    file_format: str | None = None
    operator: str | None = None
    value: Any | None = None
    config: str | None = None
    out: str | None = None
    materialized: str | None = None
    target_relation: str | None = None
    output_path: str | None = None
    mode: str | None = None
    retain: bool | None = None
    overwrite: bool | None = None
    compression: str | None = None
    delimiter: str | None = None
    quote: str | None = None
    escape: str | None = None
    skip_rows: int | None = None
    columns: dict[str, str] | None = None
    header: bool | None = None
    cell_id: int | None = None
    connection_id: str | None = None
    connection_type: str | None = None
    target_table: str | None = None
    resolved_sql: str | None = None
    dependencies: list[str] = field(default_factory=list)
    auto_dependencies: list[str] = field(default_factory=list)
    manual_dependencies: list[str] = field(default_factory=list)
    refs: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)
    lookups: list[str] = field(default_factory=list)
    resolved_sources: dict[str, str] = field(default_factory=dict)
    resolved_lookups: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineSpec:
    pipeline_id: str | None
    target: str | None
    nodes: list[NodeSpec]
    native_metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def notebook_id(self) -> str | None:
        return self.pipeline_id

    def node_by_name(self) -> dict[str, NodeSpec]:
        return {node.name: node for node in self.nodes}

    def node_by_out(self) -> dict[str, NodeSpec]:
        return {node.out or node.name: node for node in self.nodes}
