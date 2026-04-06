from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class GraphUiFunctionSpec:
    name: str
    signature: str
    scope: str
    purpose: str
    depends_on: tuple[str, ...] = field(default_factory=tuple)
    notes: str | None = None
    status: str = "planned"


GRAPH_UI_FUNCTIONS: tuple[GraphUiFunctionSpec, ...] = (
    GraphUiFunctionSpec(
        name="get_graph_view",
        signature=(
            "get_graph_view(pipeline_path, *, artifact_path=None, run_id=None, "
            "run_label=None)"
        ),
        scope="public",
        purpose=(
            "Build the full graph page payload for the selected run: header metadata, "
            "BFS DAG nodes, edges, node count, and action state."
        ),
        depends_on=("inspect_dag",),
        notes=(
            "This should be the main read entrypoint for the graph page. It should shape "
            "the data the React app needs instead of making the UI understand raw "
            "inspect_dag payloads."
        ),
    ),
    GraphUiFunctionSpec(
        name="get_node_panel",
        signature=(
            "get_node_panel(pipeline_path, node_name, *, artifact_path=None, "
            "run_id=None, run_label=None)"
        ),
        scope="public",
        purpose=(
            "Build the bottom-sheet payload for a selected node: summary, upstream nodes, "
            "downstream nodes, and node history."
        ),
        depends_on=("inspect_node", "inspect_node_history"),
        notes=(
            "The React panel should consume this single UI-shaped payload instead of "
            "composing multiple raw inspect payloads in the browser."
        ),
    ),
    GraphUiFunctionSpec(
        name="_resolve_graph_run_selection",
        signature=(
            "_resolve_graph_run_selection(pipeline_path, *, artifact_path=None, "
            "run_id=None, run_label=None)"
        ),
        scope="private",
        purpose=(
            "Resolve the selected run consistently for all graph UI entrypoints and reject "
            "invalid selector combinations."
        ),
        depends_on=("api._inspect_pipeline_runs", "api._select_pipeline_run_for_inspection"),
    ),
    GraphUiFunctionSpec(
        name="_build_graph_header",
        signature="_build_graph_header(dag_result)",
        scope="private",
        purpose=(
            "Shape pipeline id, node count, run id, run label, and run status into the "
            "compact header payload used by the graph page."
        ),
        depends_on=("get_graph_view",),
    ),
    GraphUiFunctionSpec(
        name="_build_graph_nodes",
        signature="_build_graph_nodes(dag_result)",
        scope="private",
        purpose=(
            "Map inspect_dag node payloads into graph-node view models with title, status, "
            "kind, duration, and artifact metadata."
        ),
        depends_on=("get_graph_view",),
    ),
    GraphUiFunctionSpec(
        name="_build_graph_edges",
        signature="_build_graph_edges(dag_result)",
        scope="private",
        purpose="Map inspect_dag edge pairs into graph-edge view models for React Flow.",
        depends_on=("get_graph_view",),
    ),
    GraphUiFunctionSpec(
        name="_build_node_summary",
        signature="_build_node_summary(node_panel_result)",
        scope="private",
        purpose=(
            "Shape the selected node's status, kind, artifact, runtime, and counts for the "
            "left side of the bottom sheet."
        ),
        depends_on=("get_node_panel",),
    ),
    GraphUiFunctionSpec(
        name="_build_neighbor_rows",
        signature="_build_neighbor_rows(node_entries)",
        scope="private",
        purpose=(
            "Map inspect_node entries into upstream/downstream table rows with node, status, "
            "kind, and duration."
        ),
        depends_on=("get_node_panel",),
    ),
    GraphUiFunctionSpec(
        name="_build_history_rows",
        signature="_build_history_rows(node_history_result)",
        scope="private",
        purpose="Map inspect_node_history states into the node history tab rows.",
        depends_on=("get_node_panel",),
    ),
)


GRAPH_UI_DEFERRED_WORK: tuple[str, ...] = (
    "Artifact explorer backend functions are intentionally deferred.",
    "When we start that work, add list_run_artifact_tables(...) and query_run_artifacts(...).",
    "Run/resume/reset button wiring can continue to call the existing api.py functions directly.",
)


def list_graph_ui_function_specs() -> list[GraphUiFunctionSpec]:
    return list(GRAPH_UI_FUNCTIONS)


def list_public_graph_ui_functions() -> list[str]:
    return [item.name for item in GRAPH_UI_FUNCTIONS if item.scope == "public"]


__all__ = [
    "GRAPH_UI_DEFERRED_WORK",
    "GRAPH_UI_FUNCTIONS",
    "GraphUiFunctionSpec",
    "list_graph_ui_function_specs",
    "list_public_graph_ui_functions",
]
