from __future__ import annotations

from .specs import PipelineSpec
from .runtime_models import LogCode
from .runtime_models import RunPolicy


def _ready_node_key(name: str, *, spec: PipelineSpec, order_index: dict[str, int]) -> tuple[int, int]:
    node = spec.node_by_name()[name]
    kind = str(node.kind or "").strip().lower()
    priority = 0 if kind.startswith("check.") else 1
    return (priority, order_index.get(name, 0))


def _topological_order(spec: PipelineSpec) -> list[str]:
    nodes = spec.node_by_name()
    order_index = {node.name: index for index, node in enumerate(spec.nodes)}
    incoming = {name: set(node.dependencies) for name, node in nodes.items()}
    ready = [name for name, deps in incoming.items() if not deps]
    ready.sort(key=lambda name: _ready_node_key(name, spec=spec, order_index=order_index))
    ordered: list[str] = []

    while ready:
        current = ready.pop(0)
        ordered.append(current)
        for name, deps in incoming.items():
            if current in deps:
                deps.remove(current)
                if not deps and name not in ordered and name not in ready:
                    ready.append(name)
        ready.sort(key=lambda name: _ready_node_key(name, spec=spec, order_index=order_index))
    return ordered


def _select_nodes(spec: PipelineSpec, target_node: str | None) -> set[str]:
    if not target_node:
        return {node.name for node in spec.nodes}

    nodes = spec.node_by_name()
    if target_node not in nodes:
        raise RuntimeError(f"Target node '{target_node}' was not found in the compiled pipeline.")

    selected: set[str] = set()

    def walk(name: str) -> None:
        if name in selected:
            return
        selected.add(name)
        for dep in nodes[name].dependencies:
            walk(dep)

    walk(target_node)
    return selected


def _select_downstream_nodes(spec: PipelineSpec, start_node: str) -> set[str]:
    nodes = spec.node_by_name()
    if start_node not in nodes:
        raise RuntimeError(f"Start node '{start_node}' was not found in the compiled pipeline.")

    downstream: dict[str, list[str]] = {node.name: [] for node in spec.nodes}
    for node in spec.nodes:
        for dep in node.dependencies:
            downstream.setdefault(dep, []).append(node.name)

    selected: set[str] = set()

    def walk(name: str) -> None:
        if name in selected:
            return
        selected.add(name)
        for child in downstream.get(name, []):
            walk(child)

    walk(start_node)
    return selected


def _select_upstream_nodes(spec: PipelineSpec, start_node: str) -> set[str]:
    nodes = spec.node_by_name()
    if start_node not in nodes:
        raise RuntimeError(f"Start node '{start_node}' was not found in the compiled pipeline.")

    selected: set[str] = set()

    def walk(name: str) -> None:
        if name in selected:
            return
        selected.add(name)
        for dep in nodes[name].dependencies:
            walk(dep)

    walk(start_node)
    return selected


def execute_selected_nodes(spec: PipelineSpec, *, runtime, selected_node_names: set[str]) -> list[str]:
    ordered = _topological_order(spec)
    selected = set(selected_node_names)
    nodes = spec.node_by_name()
    executed: list[str] = []
    run_policy = getattr(runtime, "run_policy", RunPolicy())

    def _handle_interrupted_run(exc: Exception, *, failed_node=None) -> None:
        if run_policy.on_exception != "stop":
            raise RuntimeError(f"Unsupported run policy on_exception='{run_policy.on_exception}'.")
        if failed_node is not None and hasattr(runtime, "_log_event"):
            runtime._log_event(
                code=LogCode.PIPELINE_EXECUTION_FAILED,
                message=f"Execution failed: {exc}",
                severity="error",
                details={"exception_type": type(exc).__name__, "phase": "executor"},
            )
        remaining = [
            nodes[name]
            for name in ordered
            if name in selected and name not in executed and (failed_node is None or name != failed_node.name)
        ]
        if failed_node is not None and run_policy.persist_node_outcomes and hasattr(runtime, "mark_node_failed"):
            runtime.mark_node_failed(failed_node, exc)
        if (
            remaining
            and run_policy.persist_node_outcomes
            and run_policy.downstream_on_hard_failure == "skip"
            and hasattr(runtime, "mark_nodes_skipped")
        ):
            if failed_node is None:
                reason = f"Skipped because pipeline stop was requested before node execution continued: {exc}"
            else:
                reason = f"Skipped because node '{failed_node.name}' failed earlier in the run."
            runtime.mark_nodes_skipped(remaining, reason=reason)
        if run_policy.persist_node_outcomes and hasattr(runtime, "mark_run_failed"):
            runtime.mark_run_failed(exc)
        raise exc

    if run_policy.persist_node_outcomes and hasattr(runtime, "begin_run"):
        runtime.begin_run(selected_node_names=selected)

    for node_name in ordered:
        if node_name not in selected:
            continue
        node = nodes[node_name]
        if hasattr(runtime, "raise_if_stop_requested"):
            try:
                runtime.raise_if_stop_requested(node=node, boundary="before_node")
            except Exception as exc:
                _handle_interrupted_run(exc, failed_node=None)
        if run_policy.persist_node_outcomes and hasattr(runtime, "mark_node_running"):
            runtime.mark_node_running(node)
        try:
            result = runtime.execute_node(node)
        except Exception as exc:
            _handle_interrupted_run(exc, failed_node=node)
        if run_policy.on_warning != "continue":
            raise RuntimeError(f"Unsupported run policy on_warning='{run_policy.on_warning}'.")
        if run_policy.persist_node_outcomes and hasattr(runtime, "mark_node_success"):
            runtime.mark_node_success(node, result)
        executed.append(node_name)
        if hasattr(runtime, "raise_if_stop_requested"):
            try:
                runtime.raise_if_stop_requested(node=node, boundary="after_node")
            except Exception as exc:
                _handle_interrupted_run(exc, failed_node=None)

    if run_policy.persist_node_outcomes and hasattr(runtime, "mark_run_success"):
        runtime.mark_run_success()
    return executed


def execute_pipeline(spec: PipelineSpec, *, runtime, target_node: str | None = None) -> list[str]:
    return execute_selected_nodes(spec, runtime=runtime, selected_node_names=_select_nodes(spec, target_node))
