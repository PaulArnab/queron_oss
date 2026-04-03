# Next Implementation Steps

1. Implement `cleared` state.
   - `cleared` is a reset-history marker for a node inside the current `run_id`.
   - It acts as a separator between run activity and reset activity on the same run.
   - Any node selected by a reset operation should append:
     - `cleared`
     - then immediately `ready`
   - This applies regardless of the node's current state.
   - Example:
     - if `a -> b -> c`
     - current state is:
       - `a = complete`
       - `b = failed`
       - `c = ready`
     - `reset-all` should become:
       - `a: complete -> cleared -> ready`
       - `b: failed -> cleared -> ready`
       - `c: ready -> cleared -> ready`
     - `reset-upstream` on `b` should become:
       - `a: complete -> cleared -> ready`
       - `b: failed -> cleared -> ready`
       - `c: ready`

2. Separate compile and run.
   - Compile must become durable state, not just in-memory validation.
   - Compile should write the compiled execution contract to the artifact store.
   - `run` must use the durable compiled contract and should not compile again.
   - Any pipeline code/config change should require a recompile before `run`.
   - `run` should fail clearly if:
     - no compiled contract exists
     - or the pipeline changed after the last compile

3. Add confirmation for reset options.
   - Reset actions should warn before clearing artifacts/tables.
   - The warning/confirmation flow should match the existing run purge confirmation behavior.
   - Support both:
     - interactive CLI confirmation
     - JSON/API confirmation payloads for UI flows

4. Tighten the user-level public API.
   - For pipeline run level, only expose:
     - `compile_pipeline_file(...)`
     - `run_pipeline_file(...)`
     - `resume_pipeline_file(...)`
     - `reset_node_file(...)`
     - `reset_downstream_file(...)`
     - `reset_upstream_file(...)`
     - `reset_all_file(...)`
   - For pipeline node level, only expose:
     - `postgres.ingress(...)`
     - `postgres.egress(...)`
     - `db2.ingress(...)`
     - `db2.egress(...)`
     - `file.ingress(...)`
     - `csv.ingress(...)`
     - `csv.egress(...)`
     - `jsonl.ingress(...)`
     - `jsonl.egress(...)`
     - `parquet.ingress(...)`
     - `parquet.egress(...)`
     - `python.ingress(...)`
     - `model.sql(...)`
     - `check.fail_if_count(...)`
     - `check.fail_if_true(...)`
   - Keep template helpers user-facing:
     - `ref(name)`
     - `source(name)`
   - Everything else should become internal/private.

5. Add user-facing inspection functions.
   - Add one function to inspect the DAG.
     - Suggested shape: `inspect_dag_file(...)`
     - It should return the graph/DAG structure together with the current state of each node on the DAG.
   - Add one function to inspect current status for node/nodes.
     - Suggested shape: `inspect_status_file(...)`
     - It should support selecting:
       - a node
       - upstream of that node
       - downstream of that node
     - It should return the current active status for the selected node/slice.
   - Add one function to inspect node history.
     - Suggested shape: `inspect_node_history_file(...)`
     - It should return the run-scoped state history for a selected node.
     - It should help explain how the node moved through states like:
       - `ready`
       - `running`
       - `complete`
       - `failed`
       - `cleared`
   - These should be user-level functions, not internal helpers.

6. Add explicit testing modules in the library.
   - Testing support should be implemented as a first-class part of the OSS library.
   - It should not rely only on ad hoc sample pipelines or manual verification.

7. Packaging and productization.
   - Add a real `pyproject.toml`.
   - Add an installable console entry point.
   - Define dependency groups.
   - Add a clean README and quickstart.
   - Add versioning.

8. Implement a visual DAG/graph with dynamic run state changes and pipeline control support.
   - Add a user-facing graph view for the compiled DAG.
   - Show dynamic node state changes while runs, resumes, and resets happen.
   - Surface pipeline controls from the graph where appropriate.
   - Keep the graph aligned with the active node state model and pipeline control flow.

Later change if needed:
- State changes on resets should also track artifacts, not just node status.
  - Context:
    - reset currently changes node state history
    - but reset also changes artifact reality
    - later we may want reset to record artifact-level state transitions too, not only node-level transitions
  - Idea:
    - `node_states` tracks execution/recovery state for nodes
    - a future artifact-state mechanism would track what happened to the actual produced artifact(s) during reset
