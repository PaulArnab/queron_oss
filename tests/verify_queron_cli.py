import io
import json
import pathlib
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import queron
import queron.cli
import duckdb_core
from duckdb_driver import load_duckdb
from queron.runtime_models import PipelineVarRecord
from queron.runtime_vars import resolve_runtime_var_values_for_existing_run


class VerifyQueronCliTests(unittest.TestCase):
    def test_package_root_exports_only_user_level_surface(self):
        expected_exports = {
            "init_pipeline_project",
            "compile_pipeline",
            "inspect_node",
            "inspect_node_query",
            "inspect_node_history",
            "inspect_dag",
            "run_pipeline",
            "resume_pipeline",
            "reset_node",
            "reset_downstream",
            "reset_upstream",
            "reset_all",
            "stop_pipeline",
            "postgres",
            "db2",
            "file",
            "csv",
            "jsonl",
            "lookup",
            "parquet",
            "python",
            "model",
            "check",
            "ref",
            "source",
        }
        unexpected_exports = {
            "CompiledPipeline",
            "compile_pipeline_code",
            "compile_pipeline_text",
            "execute_compiled_pipeline",
            "execute_pipeline",
            "PipelineRuntime",
            "reset_compiled_node",
            "reset_compiled_downstream",
            "reset_compiled_upstream",
            "reset_compiled_all",
            "resume_compiled_pipeline",
            "build_log_event",
            "RuntimeBinding",
            "load_connections_config",
        }

        for name in expected_exports:
            self.assertTrue(hasattr(queron, name), msg=f"expected queron.{name} to be public")
        for name in unexpected_exports:
            self.assertFalse(hasattr(queron, name), msg=f"did not expect queron.{name} to be public")

    def test_resume_pipeline_does_not_accept_pipeline_id_override(self):
        with self.assertRaises(TypeError):
            queron.resume_pipeline("pipeline.py", pipeline_id="override-not-allowed")

    def test_compile_pipeline_does_not_accept_artifact_path_override(self):
        with self.assertRaises(TypeError):
            queron.compile_pipeline("pipeline.py", artifact_path="override-not-allowed.duckdb")

    def test_compile_pipeline_collects_runtime_vars_in_contract(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=f\"\"\"
SELECT 1
WHERE dt >= {queron.var("start_date")}
  AND state IN ({queron.var("states")})
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )

            compiled = queron.compile_pipeline(pipeline_path)

            self.assertFalse(any(str(item.get("level")) == "error" for item in compiled.diagnostics))
            self.assertIsNotNone(compiled.contract)
            self.assertEqual(
                [item.model_dump() for item in compiled.contract.vars_json],
                [
                    {
                        "name": "start_date",
                        "kind": "scalar",
                        "required": True,
                        "default": None,
                        "used_in_nodes": ["seed"],
                    },
                    {
                        "name": "states",
                        "kind": "list",
                        "required": True,
                        "default": None,
                        "used_in_nodes": ["seed"],
                    },
                ],
            )

    def test_compile_pipeline_rejects_runtime_var_in_forbidden_sql_position(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=f\"\"\"
SELECT * FROM {queron.var("table_name")}
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )

            compiled = queron.compile_pipeline(pipeline_path)

            self.assertTrue(any(str(item.get("code")) == "invalid_runtime_var_placement" for item in compiled.diagnostics))
            self.assertIsNone(compiled.contract)

    def test_compile_pipeline_rejects_conflicting_runtime_var_kinds(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed_a',
    out='seed_a',
    query=f\"\"\"
SELECT 1
WHERE state IN ({queron.var("states")})
\"\"\",
)
def seed_a():
    pass

@queron.model.sql(
    name='seed_b',
    out='seed_b',
    query=f\"\"\"
SELECT 1
WHERE state = {queron.var("states")}
\"\"\",
)
def seed_b():
    pass
""",
                encoding="utf-8",
            )

            compiled = queron.compile_pipeline(pipeline_path)

            self.assertTrue(any(str(item.get("code")) == "conflicting_runtime_var_kind" for item in compiled.diagnostics))
            self.assertIsNone(compiled.contract)

    def test_run_pipeline_executes_model_with_runtime_vars(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=f\"\"\"
SELECT *
FROM (
    VALUES
        ('TX', DATE '2026-01-01'),
        ('CA', DATE '2026-01-02'),
        ('WA', DATE '2026-01-03')
) AS src(state, dt)
WHERE state IN ({queron.var("states")})
  AND dt >= {queron.var("start_date")}
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )

            compiled = queron.compile_pipeline(pipeline_path)
            self.assertIsNotNone(compiled.contract)

            result = queron.run_pipeline(
                pipeline_path,
                runtime_vars={"states": ["TX", "CA"], "start_date": "2026-01-02"},
            )

            node = queron.inspect_node(result.artifact_path, "seed", run_id=result.run_id)

            self.assertEqual(int(node.nodes[0].get("row_count_out") or 0), 1)

    def test_run_pipeline_requires_missing_runtime_var(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=f\"\"\"
SELECT 1
WHERE 1 = {queron.var("required_value")}
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )

            compiled = queron.compile_pipeline(pipeline_path)
            self.assertIsNotNone(compiled.contract)

            with self.assertRaisesRegex(RuntimeError, "Missing required runtime var 'required_value'"):
                queron.run_pipeline(pipeline_path, runtime_vars={})

    def test_run_pipeline_uses_runtime_var_default(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=f\"\"\"
SELECT 7 AS value
WHERE 7 = {queron.var("default_value", default=7)}
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )

            compiled = queron.compile_pipeline(pipeline_path)
            self.assertIsNotNone(compiled.contract)
            records = {item.name: item for item in compiled.contract.vars_json}
            self.assertFalse(records["default_value"].required)
            self.assertEqual(records["default_value"].default, 7)

            result = queron.run_pipeline(pipeline_path, runtime_vars={})
            node = queron.inspect_node(result.artifact_path, "seed", run_id=result.run_id)
            self.assertEqual(int(node.nodes[0].get("row_count_out") or 0), 1)

    def test_compile_pipeline_collects_mutable_after_start_flag(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=f\"\"\"
SELECT 1
WHERE 1 = {queron.var("required_value", mutable_after_start=True)}
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )

            compiled = queron.compile_pipeline(pipeline_path)
            self.assertIsNotNone(compiled.contract)
            records = {item.name: item for item in compiled.contract.vars_json}
            self.assertTrue(records["required_value"].mutable_after_start)

    def test_existing_run_runtime_var_mutability_is_enforced(self):
        contract_vars = [
            PipelineVarRecord(name="immutable_value", mutable_after_start=False),
            PipelineVarRecord(name="mutable_value", mutable_after_start=True),
        ]

        with self.assertRaisesRegex(RuntimeError, "Runtime var 'immutable_value' cannot be changed after run start."):
            resolve_runtime_var_values_for_existing_run(
                contract_vars,
                stored_runtime_vars={"immutable_value": 1, "mutable_value": 1},
                requested_runtime_vars={"immutable_value": 2, "mutable_value": 2},
            )

        resolved = resolve_runtime_var_values_for_existing_run(
            contract_vars,
            stored_runtime_vars={"immutable_value": 1, "mutable_value": 1},
            requested_runtime_vars={"immutable_value": 1, "mutable_value": 2},
        )
        self.assertEqual(resolved["mutable_value"], 2)

    def test_cli_run_accepts_vars_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=f\"\"\"
SELECT *
FROM (VALUES ('TX'), ('CA'), ('WA')) AS src(state)
WHERE state IN ({queron.var("states")})
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )

            self._compile_pipeline(pipeline_path)
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = queron.cli.main(
                    [
                        "run",
                        str(pipeline_path),
                        "--vars-json",
                        "{\"states\": [\"TX\", \"CA\"]}",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertEqual(stderr.getvalue(), "")
            self.assertIn("Run succeeded.", stdout.getvalue())

    def test_cli_run_accepts_vars_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            vars_path = root / "vars.json"
            vars_path.write_text(json.dumps({"states": ["CA"]}), encoding="utf-8")
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=f\"\"\"
SELECT *
FROM (VALUES ('TX'), ('CA'), ('WA')) AS src(state)
WHERE state IN ({queron.var("states")})
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )

            self._compile_pipeline(pipeline_path)
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = queron.cli.main(
                    [
                        "run",
                        str(pipeline_path),
                        "--vars-file",
                        str(vars_path),
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertEqual(stderr.getvalue(), "")
            self.assertIn("Run succeeded.", stdout.getvalue())

    def test_cli_run_rejects_vars_json_and_vars_file_together(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            vars_path = root / "vars.json"
            vars_path.write_text(json.dumps({"states": ["CA"]}), encoding="utf-8")
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=f\"\"\"
SELECT *
FROM (VALUES ('TX'), ('CA'), ('WA')) AS src(state)
WHERE state IN ({queron.var("states")})
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )

            self._compile_pipeline(pipeline_path)
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = queron.cli.main(
                    [
                        "run",
                        str(pipeline_path),
                        "--vars-json",
                        "{\"states\": [\"TX\"]}",
                        "--vars-file",
                        str(vars_path),
                    ]
                )

            self.assertEqual(exit_code, 1)
            self.assertIn("Use either --vars-json or --vars-file, not both.", stderr.getvalue())

    def test_inspect_functions_accept_artifact_path_input(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )

            compiled = queron.compile_pipeline(pipeline_path)
            artifact_path = compiled.contract.artifact_path

            dag = queron.inspect_dag(artifact_path)
            node = queron.inspect_node(artifact_path, "seed")
            history = queron.inspect_node_history(artifact_path, "seed")

            self.assertEqual(dag.artifact_path, artifact_path)
            self.assertEqual(node.artifact_path, artifact_path)
            self.assertEqual(history.artifact_path, artifact_path)

    def test_cli_compile_does_not_accept_artifact_path_flag(self):
        with self.assertRaises(SystemExit):
            queron.cli.main(["compile", "pipeline.py", "--artifact-path", "override-not-allowed.duckdb"])

    def test_cli_inspect_dag_does_not_accept_artifact_path_flag(self):
        with self.assertRaises(SystemExit):
            queron.cli.main(["inspect_dag", "pipeline.py", "--artifact-path", "override-not-allowed.duckdb"])

    def test_compile_pipeline_requires_pipeline_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            pipeline_path.write_text(
                """import queron

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )

            compiled = queron.compile_pipeline(pipeline_path)

            self.assertTrue(any(str(item.get("code")) == "missing_pipeline_id" for item in compiled.diagnostics))
            self.assertIsNone(compiled.contract)

    def test_compile_command_fails_without_pipeline_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            pipeline_path.write_text(
                """import queron

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = queron.cli.main(["compile", str(pipeline_path)])

            self.assertEqual(exit_code, 1)
            self.assertIn("missing_pipeline_id", stdout.getvalue() + stderr.getvalue())

    def test_compile_pipeline_defaults_artifact_path_from_pipeline_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )

            compiled = queron.compile_pipeline(pipeline_path)

            self.assertFalse(any(str(item.get("level")) == "error" for item in compiled.diagnostics))
            self.assertIsNotNone(compiled.contract)
            self.assertEqual(
                pathlib.Path(compiled.contract.artifact_path),
                (root / ".queron" / "policy123.duckdb").resolve(),
            )

    def test_compile_pipeline_allows_out_on_egress_nodes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass

@queron.csv.egress(
    name='export_seed',
    out='export_seed_snapshot',
    path='exports/output.csv',
    sql=\"\"\"
SELECT * FROM {{ queron.ref("seed") }}
\"\"\",
)
def export_seed():
    pass
""",
                encoding="utf-8",
            )

            compiled = queron.compile_pipeline(pipeline_path)

            self.assertFalse(any(str(item.get("level")) == "error" for item in compiled.diagnostics))
            export_node = compiled.spec.node_by_name()["export_seed"]
            self.assertEqual(export_node.out, "export_seed_snapshot")
            self.assertEqual(export_node.target_table, "main.export_seed_snapshot")
            self.assertEqual(export_node.output_path, "exports/output.csv")

    def test_run_pipeline_materializes_local_artifact_for_csv_egress_out(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / ".queron" / "policy123.duckdb"
            export_path = root / "exports" / "output.csv"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id, 'alpha' AS label
UNION ALL
SELECT 2 AS id, 'beta' AS label
\"\"\",
)
def seed():
    pass

@queron.csv.egress(
    name='export_seed',
    out='export_seed_snapshot',
    path='exports/output.csv',
    sql=\"\"\"
SELECT * FROM {{ queron.ref("seed") }}
\"\"\",
)
def export_seed():
    pass
""",
                encoding="utf-8",
            )

            compiled = queron.compile_pipeline(pipeline_path)
            self.assertFalse(any(str(item.get("level")) == "error" for item in compiled.diagnostics))

            result = queron.run_pipeline(pipeline_path)
            self.assertIsNotNone(result.run_id)
            self.assertTrue(export_path.exists())

            run_schema = f"run_{result.run_id.replace('-', '')}"
            conn = load_duckdb().connect(str(artifact_path))
            try:
                archived_tables = conn.execute(
                    f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{run_schema}' ORDER BY table_name"
                ).fetchall()
                self.assertIn(("export_seed_snapshot",), archived_tables)

                node_artifact = conn.execute(
                    """
                    SELECT artifact_name
                    FROM "_queron_meta"."node_runs"
                    WHERE run_id = ? AND node_name = 'export_seed'
                    ORDER BY finished_at DESC
                    LIMIT 1
                    """,
                    (result.run_id,),
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(node_artifact[0], f"{run_schema}.export_seed_snapshot")

    def test_compile_pipeline_supports_postgres_lookup_nodes_and_lookup_refs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            (root / "configurations.yaml").write_text(
                """
target: dev
sources:
  pg_claim:
    dev:
      schema: public
      table: claim
lookup:
  policy_lookup_keys:
    dev:
      schema: public
      table: queron_lookup_policy_keys
""",
                encoding="utf-8",
            )
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS policy_id
\"\"\",
)
def seed():
    pass

@queron.postgres.lookup(
    config='PostGres',
    name='stage_policy_keys',
    table='policy_lookup_keys',
    out='policy_lookup_keys_snapshot',
    sql=\"\"\"
SELECT DISTINCT policy_id
FROM {{ queron.ref("seed") }}
\"\"\",
    mode='replace',
)
def stage_policy_keys():
    pass

@queron.postgres.ingress(
    config='PostGres',
    name='pull_claims',
    out='claims',
    sql=\"\"\"
SELECT c.*
FROM {{ queron.source("pg_claim") }} c
JOIN {{ queron.lookup("policy_lookup_keys") }} k
  ON c.policy_id = k.policy_id
\"\"\",
)
def pull_claims():
    pass
""",
                encoding="utf-8",
            )

            compiled = queron.compile_pipeline(pipeline_path)

            self.assertFalse(any(str(item.get("level")) == "error" for item in compiled.diagnostics))
            self.assertIsNotNone(compiled.contract)
            lookup_node = compiled.spec.node_by_name()["stage_policy_keys"]
            self.assertEqual(lookup_node.kind, "postgres.lookup")
            self.assertFalse(lookup_node.retain)
            self.assertEqual(lookup_node.target_table, "main.policy_lookup_keys_snapshot")
            self.assertEqual(lookup_node.target_relation, '"public"."queron_lookup_policy_keys"')
            ingress_node = compiled.spec.node_by_name()["pull_claims"]
            self.assertIn("stage_policy_keys", ingress_node.dependencies)
            self.assertIn('"public"."queron_lookup_policy_keys"', ingress_node.resolved_sql)

            dag = queron.inspect_dag(compiled.contract.artifact_path)
            self.assertIn(["stage_policy_keys", "pull_claims"], dag.edges)
            self.assertIn(
                {"name": "stage_policy_keys", "kind": "postgres.lookup"},
                [{"name": item.get("name"), "kind": item.get("kind")} for item in dag.nodes],
            )

    def test_compile_pipeline_rejects_lookup_ref_in_model_sql(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            (root / "configurations.yaml").write_text(
                """
target: dev
lookup:
  policy_lookup_keys:
    dev:
      schema: public
      table: queron_lookup_policy_keys
""",
                encoding="utf-8",
            )
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='bad_model',
    out='bad_model',
    query=\"\"\"
SELECT *
FROM {{ queron.lookup("policy_lookup_keys") }}
\"\"\",
)
def bad_model():
    pass
""",
                encoding="utf-8",
            )

            compiled = queron.compile_pipeline(pipeline_path)

            self.assertTrue(any(str(item.get("code")) == "lookup_not_allowed_in_node_kind" for item in compiled.diagnostics))
            self.assertIsNone(compiled.contract)

    def test_compile_pipeline_allows_external_lookup_config_without_producer_edge(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            (root / "configurations.yaml").write_text(
                """
target: dev
sources:
  pg_claim:
    dev:
      schema: public
      table: claim
lookup:
  external_policy_keys:
    dev:
      schema: public
      table: external_policy_keys
""",
                encoding="utf-8",
            )
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.postgres.ingress(
    config='PostGres',
    name='pull_claims',
    out='claims',
    sql=\"\"\"
SELECT c.*
FROM {{ queron.source("pg_claim") }} c
JOIN {{ queron.lookup("external_policy_keys") }} k
  ON c.policy_id = k.policy_id
\"\"\",
)
def pull_claims():
    pass
""",
                encoding="utf-8",
            )

            compiled = queron.compile_pipeline(pipeline_path)

            self.assertFalse(any(str(item.get("level")) == "error" for item in compiled.diagnostics))
            self.assertEqual(compiled.spec.node_by_name()["pull_claims"].dependencies, [])

    def test_run_pipeline_drops_lookup_table_unless_retained(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            (root / "configurations.yaml").write_text(
                """
target: dev
lookup:
  policy_lookup_keys:
    dev:
      schema: public
      table: queron_lookup_policy_keys
""",
                encoding="utf-8",
            )
            pipeline_path = root / "pipeline.py"

            def write_pipeline(*, retain: bool) -> None:
                pipeline_path.write_text(
                    f'''import queron

__queron_native__ = {{"pipeline_id": "policy123"}}

@queron.model.sql(
    name='seed',
    out='seed',
    query="SELECT 1 AS policy_id",
)
def seed():
    pass

@queron.postgres.lookup(
    config='PostGres',
    name='stage_policy_keys',
    table='policy_lookup_keys',
    out='policy_lookup_keys_snapshot',
    sql="SELECT * FROM {{{{ queron.ref('seed') }}}}",
    mode='replace',
    retain={retain},
)
def stage_policy_keys():
    pass
''',
                    encoding="utf-8",
                )

            class Response:
                connection_id = "fake-pg"

            class EgressResponse:
                target_name = '"public"."queron_lookup_policy_keys"'
                row_count = 1
                warnings = []

            dropped: list[str] = []

            def fake_connect(req):
                import postgres_core

                postgres_core._connections[req.connection_id] = {"uri": "postgresql://fake", "name": req.name}
                return Response()

            def fake_egress_query_from_duckdb(**kwargs):
                self.assertEqual(kwargs["target_table"], '"public"."queron_lookup_policy_keys"')
                return EgressResponse()

            def fake_drop_table_if_exists(*, connection_id, target_table):
                dropped.append(target_table)

            import postgres_core

            with patch.object(postgres_core, "connect", fake_connect), patch.object(
                postgres_core, "egress_query_from_duckdb", fake_egress_query_from_duckdb
            ), patch.object(postgres_core, "drop_table_if_exists", fake_drop_table_if_exists):
                write_pipeline(retain=False)
                queron.compile_pipeline(pipeline_path)
                result = queron.run_pipeline(
                    pipeline_path,
                    runtime_bindings={"PostGres": {"type": "postgres", "host": "fake"}},
                )
                self.assertEqual(result.executed_nodes, ["seed", "stage_policy_keys"])
                self.assertEqual(dropped, ['"public"."queron_lookup_policy_keys"'])

                dropped.clear()
                write_pipeline(retain=True)
                queron.compile_pipeline(pipeline_path)
                result = queron.run_pipeline(
                    pipeline_path,
                    runtime_bindings={"PostGres": {"type": "postgres", "host": "fake"}},
                    set_final=True,
                )
                self.assertEqual(result.executed_nodes, ["seed", "stage_policy_keys"])
                self.assertEqual(dropped, [])

    def test_compile_command_reports_artifact_path_from_pipeline_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = queron.cli.main(["compile", str(pipeline_path)])

            self.assertEqual(exit_code, 0)
            self.assertIn(str((root / ".queron" / "policy123.duckdb").resolve()), stdout.getvalue())

    def test_inspect_functions_use_artifact_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )

            compiled = queron.compile_pipeline(pipeline_path)

            expected_artifact = str((root / ".queron" / "policy123.duckdb").resolve())
            dag = queron.inspect_dag(compiled.contract.artifact_path)
            node = queron.inspect_node(compiled.contract.artifact_path, "seed")
            history = queron.inspect_node_history(compiled.contract.artifact_path, "seed")

            self.assertEqual(dag.artifact_path, expected_artifact)
            self.assertEqual(node.artifact_path, expected_artifact)
            self.assertEqual(history.artifact_path, expected_artifact)

    def test_run_pipeline_writes_logs_under_pipeline_id_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )

            queron.compile_pipeline(pipeline_path)
            result = queron.run_pipeline(pipeline_path)

            self.assertIsNotNone(result.run_id)
            self.assertIsNotNone(result.log_path)

            expected_log_path = (root / ".queron" / "logs" / "policy123" / f"{result.run_id}.jsonl").resolve()
            self.assertEqual(pathlib.Path(result.log_path).resolve(), expected_log_path)
            self.assertTrue(expected_log_path.exists())

    def test_run_pipeline_uses_compiled_pipeline_id_instead_of_runtime_override(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / ".queron" / "policy123.duckdb"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )

            compiled = queron.compile_pipeline(pipeline_path)
            self.assertFalse(any(str(item.get("level")) == "error" for item in compiled.diagnostics))

            result = queron.run_pipeline(pipeline_path, pipeline_id="override-not-used")
            self.assertIsNotNone(result.run_id)

            conn = load_duckdb().connect(str(artifact_path))
            try:
                row = conn.execute(
                    'SELECT pipeline_id FROM "_queron_meta"."pipeline_runs" ORDER BY started_at DESC LIMIT 1'
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(row[0], "policy123")

    def _compile_pipeline(
        self,
        pipeline_path: pathlib.Path,
        *,
        artifact_path: pathlib.Path | None = None,
        config_path: pathlib.Path | None = None,
    ) -> None:
        args = ["compile", str(pipeline_path)]
        if artifact_path is not None:
            args.extend(["--artifact-path", str(artifact_path)])
        if config_path is not None:
            args.extend(["--config", str(config_path)])
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            exit_code = queron.cli.main(args)
        self.assertEqual(exit_code, 0)

    def _wait_for_running_run_id(self, artifact_path: pathlib.Path, *, timeout_seconds: float = 10.0) -> str:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            try:
                conn = load_duckdb().connect(str(artifact_path))
                try:
                    row = conn.execute(
                        """
                        SELECT run_id
                        FROM "_queron_meta"."pipeline_runs"
                        WHERE status = 'running'
                        ORDER BY COALESCE(finished_at, started_at) DESC
                        LIMIT 1
                        """
                    ).fetchone()
                finally:
                    conn.close()
            except Exception:
                row = None
            if row is not None and str(row[0] or "").strip():
                return str(row[0]).strip()
            time.sleep(0.05)
        self.fail("Timed out waiting for a running pipeline run.")

    def _wait_for_node_status(
        self,
        artifact_path: pathlib.Path,
        *,
        run_id: str,
        node_name: str,
        expected_status: str,
        timeout_seconds: float = 10.0,
    ) -> None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            try:
                conn = load_duckdb().connect(str(artifact_path))
                try:
                    row = conn.execute(
                        """
                        SELECT status
                        FROM "_queron_meta"."node_runs"
                        WHERE run_id = ? AND node_name = ?
                        ORDER BY COALESCE(finished_at, started_at) DESC
                        LIMIT 1
                        """,
                        (run_id, node_name),
                    ).fetchone()
                finally:
                    conn.close()
            except Exception:
                row = None
            if row is not None and str(row[0] or "").strip().lower() == str(expected_status).strip().lower():
                return
            time.sleep(0.05)
        self.fail(f"Timed out waiting for node '{node_name}' to reach status '{expected_status}'.")

    def test_stop_pipeline_gracefully_stops_after_current_node_and_resume_completes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / ".queron" / "policy123.duckdb"
            pipeline_path.write_text(
                """import time
import pyarrow as pa
import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.python.ingress(
    name='slow_seed',
    out='slow_seed',
)
def slow_seed():
    time.sleep(0.6)
    return pa.table({"id": [1, 2]})

@queron.model.sql(
    name='mid',
    out='mid',
    query=\"\"\"
SELECT id + 1 AS id
FROM {{ queron.ref("slow_seed") }}
\"\"\",
)
def mid():
    pass

@queron.model.sql(
    name='leaf',
    out='leaf',
    query=\"\"\"
SELECT id + 1 AS id
FROM {{ queron.ref("mid") }}
\"\"\",
)
def leaf():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path)

            captured: dict[str, object] = {}

            def _run_pipeline() -> None:
                try:
                    captured["result"] = queron.run_pipeline(pipeline_path)
                except Exception as exc:  # pragma: no cover - asserted below
                    captured["error"] = exc

            worker = threading.Thread(target=_run_pipeline, daemon=True)
            worker.start()

            running_run_id = self._wait_for_running_run_id(artifact_path)
            self._wait_for_node_status(
                artifact_path,
                run_id=running_run_id,
                node_name="slow_seed",
                expected_status="running",
            )
            stop_result = queron.stop_pipeline(pipeline_path, run_id=running_run_id)

            self.assertTrue(stop_result.stop_requested)
            self.assertEqual(stop_result.run_id, running_run_id)

            worker.join(timeout=10)
            self.assertFalse(worker.is_alive(), msg="pipeline run did not finish after graceful stop")
            self.assertNotIn("result", captured)
            self.assertIn("error", captured)
            self.assertIn("Pipeline stop requested by user.", str(captured["error"]))

            conn = load_duckdb().connect(str(artifact_path))
            try:
                run_row = conn.execute(
                    """
                    SELECT status, error_message
                    FROM "_queron_meta"."pipeline_runs"
                    WHERE run_id = ?
                    """,
                    (running_run_id,),
                ).fetchone()
                node_rows = conn.execute(
                    """
                    SELECT node_name, status
                    FROM "_queron_meta"."node_runs"
                    WHERE run_id = ?
                    ORDER BY node_name
                    """,
                    (running_run_id,),
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(run_row[0], "failed")
            self.assertIn("Pipeline stop requested by user.", str(run_row[1] or ""))
            self.assertEqual(
                {str(name): str(status) for name, status in node_rows},
                {
                    "leaf": "skipped",
                    "mid": "skipped",
                    "slow_seed": "complete",
                },
            )
            self.assertIsNotNone(stop_result.request_path)
            self.assertFalse(pathlib.Path(str(stop_result.request_path)).exists())

            resume_result = queron.resume_pipeline(pipeline_path)
            self.assertEqual(resume_result.executed_nodes, ["mid", "leaf"])

    def test_cli_stop_without_run_id_targets_latest_running_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / ".queron" / "policy123.duckdb"
            pipeline_path.write_text(
                """import time
import pyarrow as pa
import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.python.ingress(
    name='slow_seed',
    out='slow_seed',
)
def slow_seed():
    time.sleep(0.6)
    return pa.table({"id": [1]})

@queron.model.sql(
    name='leaf',
    out='leaf',
    query=\"\"\"
SELECT id
FROM {{ queron.ref("slow_seed") }}
\"\"\",
)
def leaf():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path)

            exit_codes: dict[str, int] = {}

            def _run_cli() -> None:
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    exit_codes["run"] = queron.cli.main(["run", str(pipeline_path)])

            worker = threading.Thread(target=_run_cli, daemon=True)
            worker.start()

            running_run_id = self._wait_for_running_run_id(artifact_path)
            self._wait_for_node_status(
                artifact_path,
                run_id=running_run_id,
                node_name="slow_seed",
                expected_status="running",
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                stop_exit = queron.cli.main(["stop", str(pipeline_path)])

            self.assertEqual(stop_exit, 0)
            self.assertEqual(stderr.getvalue(), "")
            self.assertIn(running_run_id, stdout.getvalue())

            worker.join(timeout=10)
            self.assertFalse(worker.is_alive(), msg="cli pipeline run did not finish after graceful stop")
            self.assertEqual(exit_codes.get("run"), 1)

    def test_init_command_creates_starter_scaffold(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = pathlib.Path(tmpdir) / "starter_project"
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = queron.cli.main(["init", str(project_path)])

            self.assertEqual(exit_code, 0)
            self.assertEqual(stderr.getvalue(), "")
            self.assertIn("Init succeeded.", stdout.getvalue())
            self.assertTrue((project_path / "pipeline.py").exists())
            self.assertTrue((project_path / "configurations.yaml").exists())
            self.assertTrue((project_path / ".gitignore").exists())
            self.assertTrue((project_path / "local_files").is_dir())
            self.assertTrue((project_path / "exports").is_dir())

            pipeline_text = (project_path / "pipeline.py").read_text(encoding="utf-8")
            self.assertIn("queron init . --sample --force", pipeline_text)

    def test_init_command_creates_sample_scaffold(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = pathlib.Path(tmpdir) / "sample_project"
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = queron.cli.main(["init", str(project_path), "--sample"])

            self.assertEqual(exit_code, 0)
            self.assertEqual(stderr.getvalue(), "")
            self.assertIn("Init succeeded.", stdout.getvalue())

            pipeline_text = (project_path / "pipeline.py").read_text(encoding="utf-8")
            self.assertIn('@queron.model.sql(', pipeline_text)
            self.assertIn('@queron.csv.egress(', pipeline_text)
            self.assertIn('exports/enriched_numbers.csv', pipeline_text)

    def test_compile_command_succeeds_for_valid_pipeline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / "artifacts.duckdb"
            pipeline_path.write_text(
                """import queron

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = queron.cli.main(["compile", str(pipeline_path), "--artifact-path", str(artifact_path)])

            conn = load_duckdb().connect(str(artifact_path))
            try:
                compile_rows = conn.execute(
                    'SELECT compile_id, is_active FROM "_queron_meta"."compiled_contracts"'
                ).fetchall()
            finally:
                conn.close()

        self.assertEqual(exit_code, 0)
        self.assertIn("Compile succeeded.", stdout.getvalue())
        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(len(compile_rows), 1)
        self.assertTrue(bool(compile_rows[0][1]))

    def test_run_command_requires_compile_first(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / "artifacts.duckdb"
            pipeline_path.write_text(
                """import queron

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = queron.cli.main(["run", str(pipeline_path), "--artifact-path", str(artifact_path)])

            self.assertEqual(exit_code, 1)
            self.assertIn("compile_required", stderr.getvalue())

    def test_run_command_requires_recompile_after_pipeline_change(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / "artifacts.duckdb"
            pipeline_path.write_text(
                """import queron

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path, artifact_path=artifact_path)
            pipeline_path.write_text(
                """import queron

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 2 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = queron.cli.main(["run", str(pipeline_path), "--artifact-path", str(artifact_path)])

            self.assertEqual(exit_code, 1)
            self.assertIn("recompile_required", stderr.getvalue())

    def test_run_command_executes_with_connections_yaml(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            connections_path = root / "connections.yaml"
            artifact_path = root / "artifacts.duckdb"
            pipeline_path.write_text(
                """import queron

@queron.postgres.ingress(
    config='CDH',
    name='seed',
    out='seed',
    sql=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )
            connections_path.write_text(
                """
connections:
  CDH:
    type: postgres
    host: localhost
    port: 5432
    database: retail_db
    username: admin
    password: password123
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path, artifact_path=artifact_path)
            stdout = io.StringIO()
            stderr = io.StringIO()
            with patch("postgres_core.connect") as connect_mock, patch(
                "postgres_core.ingest_query_to_duckdb",
                return_value=type(
                    "Resp",
                    (),
                    {
                        "row_count": 1,
                        "column_mappings": [],
                    },
                )(),
            ):
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = queron.cli.main(
                        [
                            "run",
                            str(pipeline_path),
                            "--connections",
                            str(connections_path),
                            "--artifact-path",
                            str(artifact_path),
                        ]
                    )

        self.assertEqual(exit_code, 0)
        self.assertIn("Run succeeded.", stdout.getvalue())
        self.assertTrue(connect_mock.called)

    def test_run_yes_recreates_existing_outputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / "artifacts.duckdb"
            pipeline_path.write_text(
                """import queron

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass

@queron.model.sql(
    name='enriched',
    out='enriched',
    query=\"\"\"
SELECT id + 1 AS id
FROM {{ queron.ref("seed") }}
\"\"\",
)
def enriched():
    pass
""",
                encoding="utf-8",
            )

            self._compile_pipeline(pipeline_path, artifact_path=artifact_path)
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                first_exit = queron.cli.main(["run", str(pipeline_path), "--artifact-path", str(artifact_path)])
            stdout = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(io.StringIO()):
                second_exit = queron.cli.main(
                    ["run", str(pipeline_path), "--artifact-path", str(artifact_path), "--yes"]
                )

            self.assertEqual(first_exit, 0)
            self.assertEqual(second_exit, 0)
            self.assertIn("Run succeeded.", stdout.getvalue())

            conn = load_duckdb().connect(str(artifact_path))
            try:
                latest_runs = conn.execute(
                    """
                    SELECT run_id
                    FROM "_queron_meta"."pipeline_runs"
                    ORDER BY COALESCE(finished_at, started_at) DESC
                    LIMIT 2
                    """
                ).fetchall()
                self.assertEqual(len(latest_runs), 2)
                latest_run_id = str(latest_runs[0][0] or "").strip()
                prior_run_id = str(latest_runs[1][0] or "").strip()
                latest_schema = f"run_{latest_run_id.replace('-', '')}"
                prior_schema = f"run_{prior_run_id.replace('-', '')}"

                rows = conn.execute(
                    f'SELECT id FROM "{latest_schema}"."enriched" ORDER BY id'
                ).fetchall()
                prior_rows = conn.execute(
                    f'SELECT id FROM "{prior_schema}"."enriched" ORDER BY id'
                ).fetchall()
            finally:
                conn.close()
            self.assertEqual(rows, [(2,)])
            self.assertEqual(prior_rows, [(2,)])

    def test_resume_command_resumes_from_latest_failed_node(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / "artifacts.duckdb"
            local_files_dir = root / "local_files"
            local_files_dir.mkdir(parents=True, exist_ok=True)
            pipeline_path.write_text(
                """import queron

@queron.csv.ingress(
    name='seed',
    out='seed',
    path='local_files/input.csv',
)
def seed():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path, artifact_path=artifact_path)

            stderr = io.StringIO()
            with redirect_stdout(io.StringIO()), redirect_stderr(stderr):
                failed_exit = queron.cli.main(["run", str(pipeline_path), "--artifact-path", str(artifact_path)])
            self.assertEqual(failed_exit, 1)
            self.assertIn("Run failed:", stderr.getvalue())
            conn = load_duckdb().connect(str(artifact_path))
            try:
                failed_run = conn.execute(
                    'SELECT run_id, status FROM "_queron_meta"."pipeline_runs"'
                ).fetchone()
            finally:
                conn.close()
            self.assertIsNotNone(failed_run)
            failed_run_id = failed_run[0]
            self.assertEqual(failed_run[1], "failed")

            (local_files_dir / "input.csv").write_text("id\n1\n", encoding="utf-8")

            stdout = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(io.StringIO()):
                resume_exit = queron.cli.main(["resume", str(pipeline_path), "--artifact-path", str(artifact_path)])

            self.assertEqual(resume_exit, 0)
            self.assertIn("Resume succeeded.", stdout.getvalue())

            conn = load_duckdb().connect(str(artifact_path))
            try:
                archived_schema = f"run_{str(failed_run_id).replace('-', '')}"
                rows = conn.execute(f'SELECT id FROM "{archived_schema}"."seed" ORDER BY id').fetchall()
                pipeline_runs = conn.execute(
                    'SELECT run_id, status FROM "_queron_meta"."pipeline_runs"'
                ).fetchall()
                active_states = conn.execute(
                    """
                    SELECT node_name, state
                    FROM "_queron_meta"."node_states"
                    WHERE run_id = ? AND is_active = TRUE
                    ORDER BY node_name
                    """,
                    (failed_run_id,),
                ).fetchall()
            finally:
                conn.close()
            self.assertEqual(rows, [(1,)])
            self.assertEqual(pipeline_runs, [(failed_run_id, "success")])
            self.assertEqual(active_states, [("seed", "complete")])

    def test_clean_existing_archives_failed_run_outputs_before_purge(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / "artifacts.duckdb"
            local_files_dir = root / "local_files"
            local_files_dir.mkdir(parents=True, exist_ok=True)
            pipeline_path.write_text(
                """import queron

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass

@queron.csv.ingress(
    name='ext',
    out='ext',
    path='local_files/input.csv',
)
def ext():
    pass

@queron.model.sql(
    name='final',
    out='final',
    query=\"\"\"
SELECT id + 1 AS id
FROM {{ queron.ref("seed") }}
\"\"\",
)
def final():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path, artifact_path=artifact_path)

            stderr = io.StringIO()
            with redirect_stdout(io.StringIO()), redirect_stderr(stderr):
                failed_exit = queron.cli.main(["run", str(pipeline_path), "--artifact-path", str(artifact_path)])
            self.assertEqual(failed_exit, 1)
            self.assertIn("Run failed:", stderr.getvalue())

            conn = load_duckdb().connect(str(artifact_path))
            try:
                failed_run = conn.execute(
                    """
                    SELECT run_id, status
                    FROM "_queron_meta"."pipeline_runs"
                    ORDER BY COALESCE(finished_at, started_at) DESC
                    LIMIT 1
                    """
                ).fetchone()
                self.assertIsNotNone(failed_run)
                failed_run_id = str(failed_run[0] or "").strip()
                self.assertEqual(failed_run[1], "failed")
                main_tables = conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'main'
                    ORDER BY table_name
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(main_tables, [("seed",)])

            (local_files_dir / "input.csv").write_text("id\n5\n", encoding="utf-8")

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                success_exit = queron.cli.main(
                    ["run", str(pipeline_path), "--artifact-path", str(artifact_path), "--yes"]
                )
            self.assertEqual(success_exit, 0)

            conn = load_duckdb().connect(str(artifact_path))
            try:
                run_rows = conn.execute(
                    """
                    SELECT run_id, status
                    FROM "_queron_meta"."pipeline_runs"
                    ORDER BY COALESCE(finished_at, started_at) DESC
                    LIMIT 2
                    """
                ).fetchall()
                self.assertEqual(len(run_rows), 2)
                latest_run_id = str(run_rows[0][0] or "").strip()
                latest_status = str(run_rows[0][1] or "").strip()
                self.assertIn(latest_status, {"success", "success_with_warnings"})
                self.assertNotEqual(latest_run_id, failed_run_id)

                failed_schema = f"run_{failed_run_id.replace('-', '')}"
                success_schema = f"run_{latest_run_id.replace('-', '')}"

                failed_tables = conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = ?
                    ORDER BY table_name
                    """,
                    (failed_schema,),
                ).fetchall()
                success_tables = conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = ? AND table_name IN ('seed', 'ext', 'final')
                    ORDER BY table_name
                    """,
                    (success_schema,),
                ).fetchall()
                main_tables = conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'main'
                    ORDER BY table_name
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(failed_tables, [("seed",)])
            self.assertEqual(success_tables, [("ext",), ("final",), ("seed",)])
            self.assertEqual(main_tables, [])

    def test_failed_run_archive_includes_failed_node_artifacts(self):
        import duckdb_core
        from queron import api as queron_api
        from queron.runtime_models import NodeRunRecord, PipelineRunRecord

        with tempfile.TemporaryDirectory() as tmpdir:
            artifact_path = pathlib.Path(tmpdir) / "artifacts.duckdb"
            connection_id = duckdb_core.connect(duckdb_core.DuckDbConnectRequest(database=str(artifact_path))).connection_id
            run_id = "failedrun123"
            duckdb_core.record_pipeline_run(
                connection_id=connection_id,
                record=PipelineRunRecord(
                    run_id=run_id,
                    pipeline_id="archive_failed_artifacts",
                    artifact_path=str(artifact_path),
                    status="failed",
                    is_final=False,
                ),
            )
            duckdb_core.record_node_runs(
                connection_id=connection_id,
                records=[
                    NodeRunRecord(
                        node_run_id="node-seed",
                        run_id=run_id,
                        node_name="seed",
                        node_kind="model.sql",
                        artifact_name="main.seed",
                        status="complete",
                    ),
                    NodeRunRecord(
                        node_run_id="node-partial",
                        run_id=run_id,
                        node_name="partial",
                        node_kind="model.sql",
                        artifact_name="main.partial",
                        status="failed",
                        error_message="failed after local artifact creation",
                    ),
                ],
            )
            conn = load_duckdb().connect(str(artifact_path))
            try:
                conn.execute('CREATE TABLE "main"."seed" AS SELECT 1 AS id')
                conn.execute('CREATE TABLE "main"."partial" AS SELECT 11 AS id')
            finally:
                conn.close()

            targets = queron_api._local_artifact_tables_for_run(artifact_path=artifact_path, run_id=run_id)
            self.assertEqual(set(targets), {"main.seed", "main.partial"})
            archived = queron_api._archive_run_outputs(
                artifact_path=artifact_path,
                run_id=run_id,
                target_tables=targets,
            )

            archived_db = pathlib.Path(duckdb_core.archived_artifact_path_for_run(database_path=artifact_path, run_id=run_id))
            self.assertTrue(archived_db.exists())
            self.assertEqual(archived, {"main.seed": "run_failedrun123.seed", "main.partial": "run_failedrun123.partial"})

            active_conn = load_duckdb().connect(str(artifact_path))
            archive_conn = load_duckdb().connect(str(archived_db))
            try:
                active_tables = active_conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'main'
                    ORDER BY table_name
                    """
                ).fetchall()
                archived_rows = archive_conn.execute(
                    'SELECT id FROM "run_failedrun123"."partial"'
                ).fetchall()
            finally:
                active_conn.close()
                archive_conn.close()

            self.assertEqual(active_tables, [])
            self.assertEqual(archived_rows, [(11,)])

    def test_failed_run_persists_skipped_node_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / "artifacts.duckdb"
            pipeline_path.write_text(
                """import queron

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass

@queron.model.sql(
    name='broken',
    out='broken',
    query=\"\"\"
SELECT CAST('bad' AS INTEGER) AS id
FROM {{ queron.ref("seed") }}
\"\"\",
)
def broken():
    pass

@queron.model.sql(
    name='leaf',
    out='leaf',
    query=\"\"\"
SELECT id + 1 AS id
FROM {{ queron.ref("broken") }}
\"\"\",
)
def leaf():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path, artifact_path=artifact_path)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                failed_exit = queron.cli.main(["run", str(pipeline_path), "--artifact-path", str(artifact_path)])
            self.assertEqual(failed_exit, 1)

            conn = load_duckdb().connect(str(artifact_path))
            try:
                active_states = conn.execute(
                    """
                    SELECT node_name, state
                    FROM "_queron_meta"."node_states"
                    WHERE is_active = TRUE
                    ORDER BY node_name
                    """
                ).fetchall()
                node_runs = conn.execute(
                    """
                    SELECT node_name, status
                    FROM "_queron_meta"."node_runs"
                    ORDER BY node_name
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(active_states, [("broken", "failed"), ("leaf", "skipped"), ("seed", "complete")])
            self.assertEqual(node_runs, [("broken", "failed"), ("leaf", "skipped"), ("seed", "complete")])

    def test_failed_run_defaults_non_final(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / ".queron" / "policy_non_final.duckdb"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy_non_final"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass

@queron.model.sql(
    name='broken',
    out='broken',
    query=\"\"\"
SELECT CAST('bad' AS INTEGER) AS id
FROM {{ queron.ref("seed") }}
\"\"\",
)
def broken():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                failed_exit = queron.cli.main(["run", str(pipeline_path)])
            self.assertEqual(failed_exit, 1)

            runs = duckdb_core.list_pipeline_runs(database_path=str(artifact_path), limit=1)
            self.assertEqual(len(runs), 1)
            self.assertFalse(bool(runs[0].get("is_final")))

    def test_fresh_run_marks_previous_failed_run_final(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / ".queron" / "policy_fresh_run.duckdb"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy_fresh_run"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass

@queron.model.sql(
    name='broken',
    out='broken',
    query=\"\"\"
SELECT CAST('bad' AS INTEGER) AS id
FROM {{ queron.ref("seed") }}
\"\"\",
)
def broken():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                first_exit = queron.cli.main(["run", str(pipeline_path)])
            self.assertEqual(first_exit, 1)
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                second_exit = queron.cli.main(["run", str(pipeline_path), "--clean-existing"])
            self.assertEqual(second_exit, 1)

            runs = duckdb_core.list_pipeline_runs(database_path=str(artifact_path), limit=2)
            self.assertEqual(len(runs), 2)
            self.assertFalse(bool(runs[0].get("is_final")))
            self.assertTrue(bool(runs[1].get("is_final")))

    def test_compile_marks_failed_run_final_and_blocks_resume_reset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / ".queron" / "policy_compile_final.duckdb"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy_compile_final"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass

@queron.model.sql(
    name='broken',
    out='broken',
    query=\"\"\"
SELECT CAST('bad' AS INTEGER) AS id
FROM {{ queron.ref("seed") }}
\"\"\",
)
def broken():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                failed_exit = queron.cli.main(["run", str(pipeline_path)])
            self.assertEqual(failed_exit, 1)

            self._compile_pipeline(pipeline_path)

            runs = duckdb_core.list_pipeline_runs(database_path=str(artifact_path), limit=1)
            self.assertEqual(len(runs), 1)
            self.assertTrue(bool(runs[0].get("is_final")))

            dag = queron.inspect_dag(artifact_path)
            node = queron.inspect_node(artifact_path, "seed")
            history = queron.inspect_node_history(artifact_path, "seed")
            logs = queron.inspect_node_logs(artifact_path, "seed")
            query = queron.inspect_node_query(artifact_path, "seed")
            self.assertTrue(dag.is_final)
            self.assertTrue(node.is_final)
            self.assertTrue(history.is_final)
            self.assertTrue(logs.is_final)
            self.assertTrue(query.is_final)

            with self.assertRaisesRegex(RuntimeError, "final"):
                queron.resume_pipeline(pipeline_path)
            with self.assertRaisesRegex(RuntimeError, "final"):
                queron.reset_all(pipeline_path)

    def test_reset_commands_drop_expected_tables(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / "artifacts.duckdb"
            pipeline_path.write_text(
                """import queron

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass

@queron.model.sql(
    name='mid',
    out='mid',
    query=\"\"\"
SELECT id + 1 AS id
FROM {{ queron.ref("seed") }}
\"\"\",
)
def mid():
    pass

@queron.model.sql(
    name='leaf',
    out='leaf',
    query=\"\"\"
SELECT id + 1 AS id
FROM {{ queron.ref("mid") }}
\"\"\",
)
def leaf():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path, artifact_path=artifact_path)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                initial_exit = queron.cli.main(["run", str(pipeline_path), "--artifact-path", str(artifact_path)])
            self.assertEqual(initial_exit, 0)

            conn = load_duckdb().connect(str(artifact_path))
            try:
                latest_run = conn.execute(
                    """
                    SELECT run_id
                    FROM "_queron_meta"."pipeline_runs"
                    ORDER BY COALESCE(finished_at, started_at) DESC
                    LIMIT 1
                    """
                ).fetchone()
                self.assertIsNotNone(latest_run)
                archived_schema = f"run_{str(latest_run[0] or '').replace('-', '')}"
            finally:
                conn.close()

            def restore_main_tables() -> None:
                conn = load_duckdb().connect(str(artifact_path))
                try:
                    conn.execute('DROP TABLE IF EXISTS "main"."leaf"')
                    conn.execute('DROP TABLE IF EXISTS "main"."mid"')
                    conn.execute('DROP TABLE IF EXISTS "main"."seed"')
                    conn.execute(f'CREATE TABLE "main"."seed" AS SELECT * FROM "{archived_schema}"."seed"')
                    conn.execute(f'CREATE TABLE "main"."mid" AS SELECT * FROM "{archived_schema}"."mid"')
                    conn.execute(f'CREATE TABLE "main"."leaf" AS SELECT * FROM "{archived_schema}"."leaf"')
                finally:
                    conn.close()

            restore_main_tables()
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                reset_node_exit = queron.cli.main(
                    ["reset-node", str(pipeline_path), "mid", "--artifact-path", str(artifact_path)]
                )
            self.assertEqual(reset_node_exit, 0)
            conn = load_duckdb().connect(str(artifact_path))
            try:
                existing = {
                    row[0]
                    for row in conn.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                    ).fetchall()
                }
            finally:
                conn.close()
            self.assertIn("seed", existing)
            self.assertNotIn("mid", existing)
            self.assertIn("leaf", existing)

            restore_main_tables()
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                reset_upstream_exit = queron.cli.main(
                    ["reset-upstream", str(pipeline_path), "mid", "--artifact-path", str(artifact_path)]
                )
            self.assertEqual(reset_upstream_exit, 0)
            conn = load_duckdb().connect(str(artifact_path))
            try:
                existing = {
                    row[0]
                    for row in conn.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                    ).fetchall()
                }
            finally:
                conn.close()
            self.assertNotIn("seed", existing)
            self.assertNotIn("mid", existing)
            self.assertIn("leaf", existing)

            restore_main_tables()
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                reset_downstream_exit = queron.cli.main(
                    ["reset-downstream", str(pipeline_path), "mid", "--artifact-path", str(artifact_path)]
                )
            self.assertEqual(reset_downstream_exit, 0)
            conn = load_duckdb().connect(str(artifact_path))
            try:
                existing = {
                    row[0]
                    for row in conn.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                    ).fetchall()
                }
            finally:
                conn.close()
            self.assertIn("seed", existing)
            self.assertNotIn("mid", existing)
            self.assertNotIn("leaf", existing)

    def test_reset_history_appends_cleared_then_ready_for_selected_nodes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / "artifacts.duckdb"
            pipeline_path.write_text(
                """import queron

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass

@queron.model.sql(
    name='broken',
    out='broken',
    query=\"\"\"
SELECT CAST('bad' AS INTEGER) AS id
FROM {{ queron.ref("seed") }}
\"\"\",
)
def broken():
    pass

@queron.model.sql(
    name='leaf',
    out='leaf',
    query=\"\"\"
SELECT id + 1 AS id
FROM {{ queron.ref("broken") }}
\"\"\",
)
def leaf():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path, artifact_path=artifact_path)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                failed_exit = queron.cli.main(["run", str(pipeline_path), "--artifact-path", str(artifact_path)])
            self.assertEqual(failed_exit, 1)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                reset_upstream_exit = queron.cli.main(
                    ["reset-upstream", str(pipeline_path), "broken", "--artifact-path", str(artifact_path)]
                )
            self.assertEqual(reset_upstream_exit, 0)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                reset_all_exit = queron.cli.main(
                    ["reset-all", str(pipeline_path), "--artifact-path", str(artifact_path)]
                )
            self.assertEqual(reset_all_exit, 0)

            conn = load_duckdb().connect(str(artifact_path))
            try:
                active_states = conn.execute(
                    """
                    SELECT node_name, state
                    FROM "_queron_meta"."node_states"
                    WHERE is_active = TRUE
                    ORDER BY node_name
                    """
                ).fetchall()
                state_counts = conn.execute(
                    """
                    SELECT node_name, state, COUNT(*) AS count
                    FROM "_queron_meta"."node_states"
                    GROUP BY node_name, state
                    ORDER BY node_name, state
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(active_states, [("broken", "ready"), ("leaf", "ready"), ("seed", "ready")])
            self.assertIn(("seed", "cleared", 2), state_counts)
            self.assertIn(("broken", "cleared", 2), state_counts)
            self.assertIn(("leaf", "cleared", 1), state_counts)

    def test_run_json_returns_confirmation_payload_without_failure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / "artifacts.duckdb"
            pipeline_path.write_text(
                """import queron

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path, artifact_path=artifact_path)

            conn = load_duckdb().connect(str(artifact_path))
            try:
                conn.execute('CREATE TABLE "main"."seed" AS SELECT 1 AS id')
            finally:
                conn.close()

            stdout = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(io.StringIO()):
                second_exit = queron.cli.main(
                    ["run", str(pipeline_path), "--artifact-path", str(artifact_path), "--json"]
                )

            self.assertEqual(second_exit, 0)
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload.get("requires_confirmation"))
            self.assertEqual(payload.get("phase"), "awaiting_confirmation")
            self.assertTrue(payload.get("ok"))

    def test_run_command_rejects_duplicate_non_null_run_label(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / "artifacts.duckdb"
            pipeline_path.write_text(
                """import queron

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path, artifact_path=artifact_path)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                first_exit = queron.cli.main(
                    [
                        "run",
                        str(pipeline_path),
                        "--artifact-path",
                        str(artifact_path),
                        "--run-label",
                        "nightly_customer_refresh",
                    ]
                )
            self.assertEqual(first_exit, 0)

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                second_exit = queron.cli.main(
                    [
                        "run",
                        str(pipeline_path),
                        "--artifact-path",
                        str(artifact_path),
                        "--run-label",
                        "nightly_customer_refresh",
                        "--yes",
                    ]
                )

            self.assertEqual(second_exit, 1)
            self.assertIn("already exists for this pipeline", stderr.getvalue())

            conn = load_duckdb().connect(str(artifact_path))
            try:
                labels = conn.execute(
                    'SELECT run_label FROM "_queron_meta"."pipeline_runs" ORDER BY started_at'
                ).fetchall()
            finally:
                conn.close()
            self.assertEqual(labels, [("nightly_customer_refresh",)])

    def test_inspect_runs_lists_pipeline_runs_and_honors_limit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / ".queron" / "policy123.duckdb"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                first_exit = queron.cli.main(
                    [
                        "run",
                        str(pipeline_path),
                        "--run-label",
                        "nightly_customer_refresh",
                    ]
                )
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                second_exit = queron.cli.main(
                    [
                        "run",
                        str(pipeline_path),
                        "--run-label",
                        "ad_hoc_debug",
                        "--yes",
                    ]
                )

            self.assertEqual(first_exit, 0)
            self.assertEqual(second_exit, 0)

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                inspect_exit = queron.cli.main(["inspect_runs", str(artifact_path)])

            self.assertEqual(inspect_exit, 0)
            self.assertEqual(stderr.getvalue(), "")
            text = stdout.getvalue()
            self.assertIn("Runs", text)
            self.assertIn("nightly_customer_refresh", text)
            self.assertIn("ad_hoc_debug", text)

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                limited_exit = queron.cli.main(["inspect_runs", str(artifact_path), "--limit", "1"])

            self.assertEqual(limited_exit, 0)
            self.assertEqual(stderr.getvalue(), "")
            limited_text = stdout.getvalue()
            self.assertIn("ad_hoc_debug", limited_text)
            self.assertNotIn("nightly_customer_refresh", limited_text)

    def test_run_writes_log_file_without_default_console_streaming(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / "artifacts.duckdb"
            pipeline_path.write_text(
                """import queron

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path, artifact_path=artifact_path)

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = queron.cli.main(["run", str(pipeline_path), "--artifact-path", str(artifact_path)])

            self.assertEqual(exit_code, 0)
            self.assertEqual(stderr.getvalue(), "")
            self.assertIn("Run ID:", stdout.getvalue())
            self.assertIn("Log file:", stdout.getvalue())
            self.assertIn("Run succeeded.", stdout.getvalue())

            conn = load_duckdb().connect(str(artifact_path))
            try:
                row = conn.execute(
                    'SELECT run_id, log_path FROM "_queron_meta"."pipeline_runs" ORDER BY COALESCE(finished_at, started_at) DESC LIMIT 1'
                ).fetchone()
            finally:
                conn.close()
            self.assertIsNotNone(row)
            self.assertTrue(bool(row[0]))
            self.assertTrue(bool(row[1]))
            log_path = pathlib.Path(str(row[1]))
            self.assertTrue(log_path.exists())
            log_text = log_path.read_text(encoding="utf-8")
            self.assertIn("pipeline_execution_started", log_text)

    def test_stream_logs_and_inspect_logs_cli(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / ".queron" / "policy123.duckdb"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path)

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = queron.cli.main(
                    [
                        "run",
                        str(pipeline_path),
                        "--run-label",
                        "stream_test",
                        "--stream-logs",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertIn("[pipeline]", stderr.getvalue())

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                inspect_exit = queron.cli.main(
                    [
                        "inspect_logs",
                        str(artifact_path),
                        "--run-label",
                        "stream_test",
                        "--tail",
                        "5",
                    ]
                )

            self.assertEqual(inspect_exit, 0)
            self.assertEqual(stderr.getvalue(), "")
            inspect_text = stdout.getvalue()
            self.assertIn("Run label: stream_test", inspect_text)
            self.assertIn("Logs", inspect_text)
            self.assertIn("pipeline_execution_finished", inspect_text)

    def test_inspect_logs_by_run_id_for_unlabeled_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / ".queron" / "policy123.duckdb"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path)

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = queron.cli.main(["run", str(pipeline_path)])

            self.assertEqual(exit_code, 0)
            self.assertEqual(stderr.getvalue(), "")

            conn = load_duckdb().connect(str(artifact_path))
            try:
                row = conn.execute(
                    'SELECT run_id FROM "_queron_meta"."pipeline_runs" ORDER BY COALESCE(finished_at, started_at) DESC LIMIT 1'
                ).fetchone()
            finally:
                conn.close()

            self.assertIsNotNone(row)
            run_id = str(row[0] or "").strip()
            self.assertTrue(run_id)

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                inspect_exit = queron.cli.main(
                    [
                        "inspect_logs",
                        str(artifact_path),
                        "--run-id",
                        run_id,
                        "--tail",
                        "5",
                    ]
                )

            self.assertEqual(inspect_exit, 0)
            self.assertEqual(stderr.getvalue(), "")
            inspect_text = stdout.getvalue()
            self.assertIn(f"Run ID: {run_id}", inspect_text)
            self.assertNotIn("Run label:", inspect_text)
            self.assertIn("Logs", inspect_text)
            self.assertIn("pipeline_execution_finished", inspect_text)

    def test_inspect_dag_cli_without_runs_uses_compiled_contract(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / ".queron" / "policy123.duckdb"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass

@queron.model.sql(
    name='final',
    out='final',
    query=\"\"\"
SELECT id FROM {{ queron.ref("seed") }}
\"\"\",
)
def final():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path)

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = queron.cli.main(["inspect_dag", str(artifact_path)])

            self.assertEqual(exit_code, 0)
            self.assertEqual(stderr.getvalue(), "")
            text = stdout.getvalue()
            self.assertIn("Compile ID:", text)
            self.assertNotIn("Run ID:", text)
            self.assertIn("Nodes", text)
            self.assertIn("- seed  model.sql  -  main.seed", text)
            self.assertIn("- final  model.sql  -  main.final", text)
            self.assertLess(
                text.index("- seed  model.sql  -  main.seed"),
                text.index("- final  model.sql  -  main.final"),
            )
            self.assertIn("Edges", text)
            self.assertIn("- seed -> final", text)

    def test_inspect_dag_cli_supports_run_label_and_run_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / ".queron" / "policy123.duckdb"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass

@queron.model.sql(
    name='final',
    out='final',
    query=\"\"\"
SELECT id FROM {{ queron.ref("seed") }}
\"\"\",
)
def final():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                run_exit = queron.cli.main(
                    [
                        "run",
                        str(pipeline_path),
                    ]
                )
            self.assertEqual(run_exit, 0)

            conn = load_duckdb().connect(str(artifact_path))
            try:
                unlabeled_row = conn.execute(
                    'SELECT run_id FROM "_queron_meta"."pipeline_runs" WHERE run_label IS NULL ORDER BY COALESCE(finished_at, started_at) DESC LIMIT 1'
                ).fetchone()
            finally:
                conn.close()
            self.assertIsNotNone(unlabeled_row)
            unlabeled_run_id = str(unlabeled_row[0] or "").strip()
            self.assertTrue(unlabeled_run_id)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                labeled_exit = queron.cli.main(
                    [
                        "run",
                        str(pipeline_path),
                        "--run-label",
                        "dag_label",
                        "--clean-existing",
                        "--yes",
                    ]
                )
            self.assertEqual(labeled_exit, 0)

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                inspect_latest_exit = queron.cli.main(["inspect_dag", str(artifact_path)])
            self.assertEqual(inspect_latest_exit, 0)
            self.assertEqual(stderr.getvalue(), "")
            latest_text = stdout.getvalue()
            self.assertIn("Run label: dag_label", latest_text)
            self.assertIn("Run status: success", latest_text)
            self.assertIn("- seed  model.sql  complete  main.seed", latest_text)
            self.assertIn("- final  model.sql  complete  main.final", latest_text)
            self.assertIn("- seed -> final", latest_text)

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                inspect_by_label_exit = queron.cli.main(
                    [
                        "inspect_dag",
                        str(artifact_path),
                        "--run-label",
                        "dag_label",
                    ]
                )
            self.assertEqual(inspect_by_label_exit, 0)
            self.assertEqual(stderr.getvalue(), "")
            by_label_text = stdout.getvalue()
            self.assertIn("Run label: dag_label", by_label_text)
            self.assertIn("Run status: success", by_label_text)

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                inspect_by_id_exit = queron.cli.main(
                    [
                        "inspect_dag",
                        str(artifact_path),
                        "--run-id",
                        unlabeled_run_id,
                    ]
                )
            self.assertEqual(inspect_by_id_exit, 0)
            self.assertEqual(stderr.getvalue(), "")
            by_id_text = stdout.getvalue()
            self.assertIn(f"Run ID: {unlabeled_run_id}", by_id_text)
            self.assertNotIn("Run label:", by_id_text)
            self.assertIn("Run status: success", by_id_text)
            self.assertIn("- seed  model.sql  complete  main.seed", by_id_text)

    def test_inspect_node_python_and_cli_support_upstream_and_downstream(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / ".queron" / "policy123.duckdb"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass

@queron.model.sql(
    name='mid',
    out='mid',
    query=\"\"\"
SELECT id + 1 AS id
FROM {{ queron.ref("seed") }}
\"\"\",
)
def mid():
    pass

@queron.model.sql(
    name='leaf',
    out='leaf',
    query=\"\"\"
SELECT id + 1 AS id
FROM {{ queron.ref("mid") }}
\"\"\",
)
def leaf():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                unlabeled_exit = queron.cli.main(["run", str(pipeline_path)])
            self.assertEqual(unlabeled_exit, 0)

            conn = load_duckdb().connect(str(artifact_path))
            try:
                unlabeled_row = conn.execute(
                    'SELECT run_id FROM "_queron_meta"."pipeline_runs" WHERE run_label IS NULL ORDER BY COALESCE(finished_at, started_at) DESC LIMIT 1'
                ).fetchone()
            finally:
                conn.close()
            self.assertIsNotNone(unlabeled_row)
            unlabeled_run_id = str(unlabeled_row[0] or "").strip()
            self.assertTrue(unlabeled_run_id)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                labeled_exit = queron.cli.main(
                    [
                        "run",
                        str(pipeline_path),
                        "--run-label",
                        "node_label",
                    ]
                )
            self.assertEqual(labeled_exit, 0)

            conn = load_duckdb().connect(str(artifact_path))
            try:
                labeled_row = conn.execute(
                    'SELECT run_id FROM "_queron_meta"."pipeline_runs" WHERE run_label = ? LIMIT 1',
                    ("node_label",),
                ).fetchone()
            finally:
                conn.close()
            self.assertIsNotNone(labeled_row)
            labeled_run_id = str(labeled_row[0] or "").strip()
            self.assertTrue(labeled_run_id)

            python_result = queron.inspect_node(str(artifact_path), "mid", run_label="node_label")
            self.assertEqual(python_result.selection, "node")
            self.assertEqual(python_result.requested_node, "mid")
            self.assertEqual(len(python_result.nodes), 1)
            node_payload = python_result.nodes[0]
            self.assertEqual(node_payload.get("name"), "mid")
            self.assertEqual(node_payload.get("logical_artifact"), "main.mid")
            self.assertEqual(node_payload.get("artifact_name"), f"run_{labeled_run_id.replace('-', '')}.mid")
            self.assertEqual(node_payload.get("dependencies"), ["seed"])
            self.assertEqual(node_payload.get("dependents"), ["leaf"])

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                inspect_upstream_exit = queron.cli.main(
                    [
                        "inspect_node",
                        str(artifact_path),
                        "mid",
                        "--run-label",
                        "node_label",
                        "--upstream",
                    ]
                )
            self.assertEqual(inspect_upstream_exit, 0)
            self.assertEqual(stderr.getvalue(), "")
            upstream_text = stdout.getvalue()
            self.assertIn("Run label: node_label", upstream_text)
            self.assertIn("Selection: upstream", upstream_text)
            self.assertIn("Requested node: mid", upstream_text)
            self.assertIn(
                f"- seed  model.sql  complete  logical=main.seed  physical=run_{labeled_run_id.replace('-', '')}.seed",
                upstream_text,
            )
            self.assertIn(
                f"- mid  model.sql  complete  logical=main.mid  physical=run_{labeled_run_id.replace('-', '')}.mid",
                upstream_text,
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                inspect_downstream_exit = queron.cli.main(
                    [
                        "inspect_node",
                        str(artifact_path),
                        "mid",
                        "--run-id",
                        unlabeled_run_id,
                        "--downstream",
                    ]
                )
            self.assertEqual(inspect_downstream_exit, 0)
            self.assertEqual(stderr.getvalue(), "")
            downstream_text = stdout.getvalue()
            self.assertIn(f"Run ID: {unlabeled_run_id}", downstream_text)
            self.assertNotIn("Run label:", downstream_text)
            self.assertIn("Selection: downstream", downstream_text)
            self.assertIn(
                f"- mid  model.sql  complete  logical=main.mid  physical=run_{unlabeled_run_id.replace('-', '')}.mid",
                downstream_text,
            )
            self.assertIn(
                f"- leaf  model.sql  complete  logical=main.leaf  physical=run_{unlabeled_run_id.replace('-', '')}.leaf",
                downstream_text,
            )

    def test_inspect_node_history_python_and_cli_show_failed_then_reset_timeline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / ".queron" / "policy123.duckdb"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass

@queron.model.sql(
    name='broken',
    out='broken',
    query=\"\"\"
SELECT CAST('bad' AS INTEGER) AS id
FROM {{ queron.ref("seed") }}
\"\"\",
)
def broken():
    pass

@queron.model.sql(
    name='leaf',
    out='leaf',
    query=\"\"\"
SELECT id + 1 AS id
FROM {{ queron.ref("broken") }}
\"\"\",
)
def leaf():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                failed_exit = queron.cli.main(["run", str(pipeline_path)])
            self.assertEqual(failed_exit, 1)

            conn = load_duckdb().connect(str(artifact_path))
            try:
                failed_row = conn.execute(
                    'SELECT run_id FROM "_queron_meta"."pipeline_runs" ORDER BY COALESCE(finished_at, started_at) DESC LIMIT 1'
                ).fetchone()
            finally:
                conn.close()
            self.assertIsNotNone(failed_row)
            failed_run_id = str(failed_row[0] or "").strip()
            self.assertTrue(failed_run_id)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                reset_exit = queron.cli.main(["reset-upstream", str(pipeline_path), "broken"])
            self.assertEqual(reset_exit, 0)

            python_result = queron.inspect_node_history(str(artifact_path), "broken", run_id=failed_run_id)
            self.assertEqual(python_result.run_id, failed_run_id)
            self.assertEqual(python_result.node_name, "broken")
            self.assertEqual(python_result.node_kind, "model.sql")
            self.assertEqual(python_result.node_run_status, "ready")
            states = [str(item.get("state") or "").strip() for item in python_result.states]
            triggers = [str(item.get("trigger") or "").strip() for item in python_result.states]
            self.assertEqual(states, ["ready", "running", "failed", "cleared", "ready"])
            self.assertEqual(
                triggers,
                ["run_initialized", "node_started", "node_failed", "reset_upstream", "reset_upstream"],
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                inspect_exit = queron.cli.main(
                    [
                        "inspect_node_history",
                        str(artifact_path),
                        "broken",
                        "--run-id",
                        failed_run_id,
                    ]
                )
            self.assertEqual(inspect_exit, 0)
            self.assertEqual(stderr.getvalue(), "")
            text = stdout.getvalue()
            self.assertIn(f"Run ID: {failed_run_id}", text)
            self.assertIn("Node: broken", text)
            self.assertIn("Node status: ready", text)
            self.assertIn("States", text)
            self.assertIn("trigger=run_initialized", text)
            self.assertIn("trigger=node_started", text)
            self.assertIn("trigger=node_failed", text)
            self.assertIn("trigger=reset_upstream", text)
            self.assertIn("- failed", text)
            self.assertIn("- cleared", text)
            self.assertIn("- ready", text)

    def test_inspect_node_query_python_returns_sql_and_resolved_sql(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / ".queron" / "policy123.duckdb"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass

@queron.model.sql(
    name='final',
    out='final',
    query=\"\"\"
SELECT id
FROM {{ queron.ref("seed") }}
\"\"\",
)
def final():
    pass
""",
                encoding="utf-8",
            )

            self._compile_pipeline(pipeline_path)
            result = queron.inspect_node_query(str(artifact_path), "final")

            self.assertEqual(result.pipeline_id, "policy123")
            self.assertEqual(result.node_name, "final")
            self.assertEqual(result.node_kind, "model.sql")
            self.assertEqual(result.logical_artifact, "main.final")
            self.assertIn("{{ queron.ref(\"seed\") }}", str(result.sql))
            self.assertIn('"main"."seed"', str(result.resolved_sql))
            self.assertEqual(result.dependencies, ["seed"])

    def test_inspect_node_query_cli_prints_query_sections(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            pipeline_path = root / "pipeline.py"
            artifact_path = root / ".queron" / "policy123.duckdb"
            pipeline_path.write_text(
                """import queron

__queron_native__ = {"pipeline_id": "policy123"}

@queron.model.sql(
    name='seed',
    out='seed',
    query=\"\"\"
SELECT 1 AS id
\"\"\",
)
def seed():
    pass

@queron.model.sql(
    name='final',
    out='final',
    query=\"\"\"
SELECT id
FROM {{ queron.ref("seed") }}
\"\"\",
)
def final():
    pass
""",
                encoding="utf-8",
            )

            self._compile_pipeline(pipeline_path)

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = queron.cli.main(["inspect_node_query", str(artifact_path), "final"])

            self.assertEqual(exit_code, 0)
            self.assertEqual(stderr.getvalue(), "")
            text = stdout.getvalue()
            self.assertIn("Pipeline ID: policy123", text)
            self.assertIn("Node: final", text)
            self.assertIn("SQL", text)
            self.assertIn("Resolved SQL", text)
            self.assertIn("{{ queron.ref(\"seed\") }}", text)
            self.assertIn('"main"."seed"', text)

    def test_successful_runs_archive_local_duckdb_artifacts_per_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            local_files = root / "local_files"
            local_files.mkdir(parents=True, exist_ok=True)
            (local_files / "input.csv").write_text("id\n1\n2\n", encoding="utf-8")

            pipeline_path = root / "pipeline.py"
            artifact_path = root / "artifacts.duckdb"
            pipeline_path.write_text(
                """import queron

@queron.csv.ingress(
    name='seed',
    out='seed',
    path='local_files/input.csv',
    header=True,
)
def seed():
    pass

@queron.model.sql(
    name='final',
    out='final',
    query=f\"\"\"
SELECT id
FROM {queron.ref("seed")}
\"\"\",
)
def final():
    pass
""",
                encoding="utf-8",
            )
            self._compile_pipeline(pipeline_path, artifact_path=artifact_path)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                first_exit = queron.cli.main(["run", str(pipeline_path), "--artifact-path", str(artifact_path)])
            self.assertEqual(first_exit, 0)

            conn = load_duckdb().connect(str(artifact_path))
            try:
                first_run = conn.execute(
                    'SELECT run_id FROM "_queron_meta"."pipeline_runs" ORDER BY COALESCE(finished_at, started_at) DESC LIMIT 1'
                ).fetchone()
                self.assertIsNotNone(first_run)
                first_run_id = str(first_run[0] or "").strip()
                self.assertTrue(first_run_id)
                first_schema = f"run_{first_run_id.replace('-', '')}"

                main_tables = conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'main' AND table_name IN ('seed', 'final')
                    ORDER BY table_name
                    """
                ).fetchall()
                self.assertEqual(main_tables, [])

                archived_tables = conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = ? AND table_name IN ('seed', 'final')
                    ORDER BY table_name
                    """,
                    (first_schema,),
                ).fetchall()
                self.assertEqual(archived_tables, [("final",), ("seed",)])

                node_run_artifacts = conn.execute(
                    """
                    SELECT artifact_name
                    FROM "_queron_meta"."node_runs"
                    WHERE run_id = ? AND node_name IN ('seed', 'final')
                    ORDER BY node_name
                    """,
                    (first_run_id,),
                ).fetchall()
                self.assertEqual(
                    node_run_artifacts,
                    [(f"{first_schema}.final",), (f"{first_schema}.seed",)],
                )

                lineage_rows = conn.execute(
                    """
                    SELECT child_schema, child_table, parent_kind, parent_schema, parent_table
                    FROM "_queron_meta"."table_lineage"
                    WHERE child_table IN ('seed', 'final')
                    ORDER BY child_table, parent_kind, parent_table
                    """
                ).fetchall()
                self.assertIn((first_schema, "seed", "file", None, "input.csv"), lineage_rows)
                self.assertIn((first_schema, "final", "artifact", first_schema, "seed"), lineage_rows)
            finally:
                conn.close()

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                second_exit = queron.cli.main(["run", str(pipeline_path), "--artifact-path", str(artifact_path)])
            self.assertEqual(second_exit, 0)

            conn = load_duckdb().connect(str(artifact_path))
            try:
                run_rows = conn.execute(
                    'SELECT run_id FROM "_queron_meta"."pipeline_runs" ORDER BY COALESCE(finished_at, started_at) DESC LIMIT 2'
                ).fetchall()
                self.assertEqual(len(run_rows), 2)
                second_run_id = str(run_rows[0][0] or "").strip()
                self.assertNotEqual(second_run_id, first_run_id)
                second_schema = f"run_{second_run_id.replace('-', '')}"

                main_tables = conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'main' AND table_name IN ('seed', 'final')
                    ORDER BY table_name
                    """
                ).fetchall()
                self.assertEqual(main_tables, [])

                first_archived = conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = ? AND table_name IN ('seed', 'final')
                    ORDER BY table_name
                    """,
                    (first_schema,),
                ).fetchall()
                second_archived = conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = ? AND table_name IN ('seed', 'final')
                    ORDER BY table_name
                    """,
                    (second_schema,),
                ).fetchall()
                self.assertEqual(first_archived, [("final",), ("seed",)])
                self.assertEqual(second_archived, [("final",), ("seed",)])
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
