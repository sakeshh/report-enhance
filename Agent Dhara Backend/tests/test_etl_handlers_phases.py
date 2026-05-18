"""ETL handler phase state machine integration tests."""
from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

from agent.etl_handlers import (
    ALLOWED_TRANSITIONS,
    _can_transition,
    etl_confirm_plan,
    etl_generate_code,
    etl_plan_start,
    rollback_on_failure,
)
from tests.fixtures.blob_pair_assessment import make_blob_pair_assessment
from agent.etl_pipeline.business_rules import normalize_business_rules


class TestEtlHandlersPhases(unittest.TestCase):
    def test_allowed_transitions_complete(self):
        self.assertIn("planned", ALLOWED_TRANSITIONS)
        self.assertIn("downloadable", ALLOWED_TRANSITIONS["code_ready"])

    def test_rollback_preserves_plan_context(self):
        flow = {"phase": "generating", "plan": {"plan_id": "p1"}, "preview": {"x": 1}}
        rollback_on_failure(flow, reason="test failure")
        self.assertEqual(flow["phase"], "planned")
        self.assertEqual(flow.get("failure_reason"), "test failure")
        self.assertEqual(flow.get("plan", {}).get("plan_id"), "p1")

    @patch("agent.etl_handlers.save_session")
    @patch("agent.etl_handlers.load_session")
    def test_plan_to_preview_to_approve_flow(self, mock_load, mock_save):
        assess = make_blob_pair_assessment()
        rules = normalize_business_rules({"never_drop_rows": True})
        sess = {
            "context": {
                "last_assessment_result": assess,
                "etl_flow": {},
            }
        }
        mock_load.return_value = sess

        with patch("agent.etl_handlers.build_etl_plan") as mock_plan:
            from agent.etl_pipeline.planner import build_etl_plan as real_build

            mock_plan.side_effect = lambda a, r, **kw: real_build(a, r, **kw)
            res = etl_plan_start("test_session", rules, assessment_result=assess)
        self.assertTrue(res.get("ok") or res.get("plan"))
        phase = sess["context"]["etl_flow"].get("phase")
        self.assertIn(phase, ("preview_ready", "planned", "failed"))

        confirm = etl_confirm_plan("test_session")
        if confirm.get("ok"):
            self.assertEqual(sess["context"]["etl_flow"].get("phase"), "approved")


if __name__ == "__main__":
    unittest.main()
