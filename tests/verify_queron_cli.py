import io
import json
import pathlib
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import queron.cli
from duckdb_driver import load_duckdb


class VerifyQueronCliTests(unittest.TestCase):
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
                rows = conn.execute('SELECT id FROM "main"."enriched"').fetchall()
            finally:
                conn.close()
            self.assertEqual(rows, [(2,)])

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
                rows = conn.execute('SELECT id FROM "main"."seed"').fetchall()
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

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                rerun_exit = queron.cli.main(
                    ["run", str(pipeline_path), "--artifact-path", str(artifact_path), "--yes"]
                )
            self.assertEqual(rerun_exit, 0)

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

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                rerun_exit = queron.cli.main(
                    ["run", str(pipeline_path), "--artifact-path", str(artifact_path), "--yes"]
                )
            self.assertEqual(rerun_exit, 0)

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                reset_all_exit = queron.cli.main(
                    ["reset-all", str(pipeline_path), "--artifact-path", str(artifact_path)]
                )
            self.assertEqual(reset_all_exit, 0)
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
            self.assertEqual(existing, set())

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

            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                first_exit = queron.cli.main(["run", str(pipeline_path), "--artifact-path", str(artifact_path)])
            self.assertEqual(first_exit, 0)

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


if __name__ == "__main__":
    unittest.main()
