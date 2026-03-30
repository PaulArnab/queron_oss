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
    def test_compile_command_succeeds_for_valid_pipeline(self):
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

        self.assertEqual(exit_code, 0)
        self.assertIn("Compile succeeded.", stdout.getvalue())
        self.assertEqual(stderr.getvalue(), "")

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
""",
                encoding="utf-8",
            )

            stderr = io.StringIO()
            with redirect_stdout(io.StringIO()), redirect_stderr(stderr):
                failed_exit = queron.cli.main(["run", str(pipeline_path), "--artifact-path", str(artifact_path)])
            self.assertEqual(failed_exit, 1)
            self.assertIn("Run failed:", stderr.getvalue())

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
SELECT id + 1 AS id
FROM {{ queron.ref("seed") }}
\"\"\",
)
def broken():
    pass
""",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(io.StringIO()):
                resume_exit = queron.cli.main(["resume", str(pipeline_path), "--artifact-path", str(artifact_path)])

            self.assertEqual(resume_exit, 0)
            self.assertIn("Resume succeeded.", stdout.getvalue())

            conn = load_duckdb().connect(str(artifact_path))
            try:
                rows = conn.execute('SELECT id FROM "main"."broken"').fetchall()
            finally:
                conn.close()
            self.assertEqual(rows, [(2,)])

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
