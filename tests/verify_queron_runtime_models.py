import pathlib
import sys
import unittest

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from queron.runtime_models import (
    ColumnMappingRecord,
    LogCode,
    NodeExecutionResult,
    NodeWarningEvent,
    NodeStateRecord,
    PipelineLogEvent,
    RunPolicy,
    WarningCode,
    build_log_event,
    build_warning_event,
    format_log_event,
    normalize_log_event,
    normalize_warning_events,
    quote_compound_relation,
)


class VerifyQueronRuntimeModelsTests(unittest.TestCase):
    def test_build_warning_event_creates_structured_warning(self):
        warning = build_warning_event(
            code="type_mapping_warning",
            message="Numeric column widened to VARCHAR.",
            source="connector",
            details={"column": "unit_price"},
        )

        self.assertIsInstance(warning, NodeWarningEvent)
        self.assertEqual(warning.code, "type_mapping_warning")
        self.assertEqual(warning.severity, "warning")
        self.assertEqual(warning.source, "connector")
        self.assertEqual(warning.message, "Numeric column widened to VARCHAR.")
        self.assertEqual(warning.details["column"], "unit_price")

    def test_normalize_warning_events_accepts_strings_and_mappings(self):
        warnings = normalize_warning_events(
            [
                "Fell back to transport metadata.",
                {
                    "code": "driver_warning",
                    "severity": "info",
                    "source": "driver",
                    "message": "Driver returned no precision metadata.",
                },
            ],
            default_code=WarningCode.CONNECTOR_WARNING,
            default_source="connector",
        )

        self.assertEqual(len(warnings), 2)
        self.assertEqual(warnings[0].code, WarningCode.CONNECTOR_WARNING)
        self.assertEqual(warnings[0].source, "connector")
        self.assertEqual(warnings[0].message, "Fell back to transport metadata.")
        self.assertEqual(warnings[1].code, "driver_warning")
        self.assertEqual(warnings[1].severity, "info")
        self.assertEqual(warnings[1].source, "driver")

    def test_column_mapping_record_exposes_normalized_warning_events(self):
        record = ColumnMappingRecord(
            ordinal_position=1,
            source_column="unit_price",
            source_type="numeric(10,2)",
            target_column="unit_price",
            target_type="VARCHAR",
            connector_type="postgresql",
            mapping_mode="transport",
            warnings=["Loaded as VARCHAR before DECIMAL coercion."],
            lossy=True,
        )

        warning_events = record.warning_events()

        self.assertEqual(len(warning_events), 1)
        self.assertEqual(warning_events[0].code, WarningCode.COLUMN_MAPPING_WARNING)
        self.assertEqual(warning_events[0].source, "connector")
        self.assertEqual(warning_events[0].message, "Loaded as VARCHAR before DECIMAL coercion.")

    def test_run_policy_defaults_match_expected_runtime_behavior(self):
        policy = RunPolicy()

        self.assertEqual(policy.on_exception, "stop")
        self.assertEqual(policy.on_warning, "continue")
        self.assertTrue(policy.persist_node_outcomes)
        self.assertEqual(policy.downstream_on_hard_failure, "skip")

    def test_node_execution_result_accepts_structured_warnings(self):
        result = NodeExecutionResult(
            node_name="seed",
            node_kind="model.sql",
            artifact_name="main.seed",
            row_count_out=1,
            warnings=[
                build_warning_event(
                    code="runtime_warning",
                    severity="warning",
                    source="queron",
                    message="Seed node completed with a warning.",
                )
            ],
        )

        self.assertEqual(result.node_name, "seed")
        self.assertEqual(result.node_kind, "model.sql")
        self.assertEqual(result.artifact_name, "main.seed")
        self.assertEqual(result.row_count_out, 1)
        self.assertEqual(len(result.warnings), 1)
        self.assertEqual(result.warnings[0].message, "Seed node completed with a warning.")

    def test_node_state_record_tracks_active_state_for_node_run(self):
        record = NodeStateRecord(
            node_state_id="state-1",
            run_id="run-1",
            node_run_id="run-1:seed",
            node_name="seed",
            state="ready",
            is_active=True,
            trigger="run_initialized",
            details_json={"reason": "fresh run"},
        )

        self.assertEqual(record.run_id, "run-1")
        self.assertEqual(record.node_run_id, "run-1:seed")
        self.assertEqual(record.node_name, "seed")
        self.assertEqual(record.state, "ready")
        self.assertTrue(record.is_active)
        self.assertEqual(record.trigger, "run_initialized")

    def test_node_state_record_accepts_cleared_state(self):
        record = NodeStateRecord(
            node_state_id="state-2",
            run_id="run-1",
            node_run_id="run-1:seed",
            node_name="seed",
            state="cleared",
            is_active=False,
            trigger="reset_all",
            details_json={"phase": "cleared"},
        )

        self.assertEqual(record.state, "cleared")
        self.assertFalse(record.is_active)
        self.assertEqual(record.trigger, "reset_all")

    def test_build_and_normalize_log_event_create_structured_logs(self):
        event = build_log_event(
            code=LogCode.NODE_EXECUTION_STARTED,
            message="Running model node.",
            source="app",
            details={"node_name": "seed"},
            run_id="run-1",
            node_name="seed",
            node_kind="model.sql",
            artifact_name="main.seed",
        )

        self.assertIsInstance(event, PipelineLogEvent)
        self.assertEqual(event.code, LogCode.NODE_EXECUTION_STARTED)
        self.assertEqual(event.source, "app")
        self.assertEqual(event.run_id, "run-1")
        self.assertEqual(event.node_id, "seed")
        self.assertEqual(event.details["node_name"], "seed")

        normalized = normalize_log_event(
            {
                "code": LogCode.PIPELINE_EXECUTION_FINISHED,
                "message": "Finished successfully.",
                "severity": "info",
                "source": "queron",
            }
        )
        self.assertEqual(normalized.code, LogCode.PIPELINE_EXECUTION_FINISHED)
        self.assertEqual(normalized.message, "Finished successfully.")

    def test_format_log_event_preserves_human_readable_terminal_output(self):
        text = format_log_event(
            {
                "code": LogCode.PIPELINE_RUN_STARTED,
                "message": "Starting pipeline run.",
                "severity": "info",
                "source": "app",
            }
        )

        self.assertEqual(text, "[pipeline] Starting pipeline run.\n")

    def test_format_log_event_includes_node_id_tag_when_present(self):
        text = format_log_event(
            {
                "code": LogCode.NODE_EXECUTION_STARTED,
                "message": "Running model node.",
                "severity": "info",
                "source": "queron",
                "node_name": "seed",
            }
        )

        self.assertEqual(text, "[pipeline][seed] Running model node.\n")

    def test_quote_compound_relation_normalizes_display_relations(self):
        self.assertEqual(quote_compound_relation("main.customer_profile"), '"main"."customer_profile"')
        self.assertEqual(quote_compound_relation('"DB2INST1"."POLICY"'), '"DB2INST1"."POLICY"')
        self.assertEqual(quote_compound_relation("exports/policy.csv"), "exports/policy.csv")


if __name__ == "__main__":
    unittest.main()
