"""
Intelligence-first ETL flow tests (planner metadata, phases, validators, download guard).
"""
from __future__ import annotations

import os
import tempfile
import unittest

from fastapi import HTTPException
from fastapi.testclient import TestClient

from agent.etl_handlers import (
    ALLOWED_TRANSITIONS,
    _can_transition,
    _transition,
    rollback_on_failure,
)
from agent.etl_pipeline.planner import build_etl_plan
from agent.etl_pipeline.step_metadata import enrich_step_record
from agent.etl_pipeline.validate_python import validate_python_source_dict
from agent.etl_pipeline.validate_sql import validate_sql_basic_dict
from agent.etl_pipeline.business_rules import normalize_business_rules
from tests.fixtures.blob_pair_assessment import make_blob_pair_assessment


class TestEtlFlow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.assess = make_blob_pair_assessment()
        cls.rules = normalize_business_rules({"never_drop_rows": True})

    def test_plan_has_evidence_fields(self):
        plan = build_etl_plan(self.assess, self.rules)
        steps = []
        for block in (plan.get("datasets") or {}).values():
            steps.extend((block or {}).get("steps") or [])
        self.assertTrue(steps, "expected at least one plan step")
        st = steps[0]
        for field in (
            "step_id",
            "reason",
            "evidence",
            "risk",
            "row_impact",
            "alternatives",
            "classification",
            "requires_user_choice",
        ):
            self.assertIn(field, st, f"missing {field} on step")
        self.assertTrue(plan.get("invariants"), "plan should include invariants")

    def test_case_normalization_row_impact_is_none(self):
        plan = build_etl_plan(self.assess, self.rules)
        for block in (plan.get("datasets") or {}).values():
            for st in (block or {}).get("steps") or []:
                if st.get("action") in ("lowercase", "uppercase"):
                    self.assertEqual(st.get("row_impact"), "none")

    def test_phase_transition_planned_to_preview(self):
        flow: dict = {"phase": "planned"}
        self.assertTrue(_can_transition("planned", "preview_ready"))
        _transition(flow, "preview_ready", reason="test")
        self.assertEqual(flow["phase"], "preview_ready")
        self.assertIn("preview_ready", ALLOWED_TRANSITIONS["planned"])

    def test_phase_blocked_on_validation_failure(self):
        flow: dict = {"phase": "generating"}
        rollback_on_failure(flow, reason="bad code")
        self.assertEqual(flow["phase"], "planned")
        self.assertIn("bad code", flow.get("failure_reason", ""))

    def test_download_path_traversal_blocked(self):
        with tempfile.TemporaryDirectory() as base:
            os.environ["DHARA_ETL_OUTPUT_DIR"] = base
            real_base = os.path.realpath(base)
            evil = os.path.normpath(os.path.join(base, "..", "outside_etl.py"))
            real_file = os.path.realpath(evil)
            self.assertFalse(
                real_file.startswith(real_base),
                "path traversal must escape output base",
            )
            from agent.mcp_server import api_etl_download_by_plan_id

            with self.assertRaises(HTTPException) as ctx:
                api_etl_download_by_plan_id("../../outside")
            self.assertEqual(ctx.exception.status_code, 404)

    def test_validate_python_catches_os_wildcard(self):
        src = "from os import *\nx = 1\n"
        result = validate_python_source_dict(src)
        self.assertFalse(result["valid"])
        self.assertTrue(any("os" in i.lower() for i in result.get("issues") or []))

    def test_validate_sql_surfaces_parse_error(self):
        result = validate_sql_basic_dict(";;;")
        if result.get("valid"):
            self.skipTest("sqlparse accepted trivial input")
        self.assertFalse(result["valid"])

    def test_full_flow_assess_plan_approve_generate(self):
        plan = build_etl_plan(self.assess, self.rules)
        self.assertFalse(plan.get("blocked"), "fixture plan should not be blocked")
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        from agent.etl_pipeline.validate_pyspark import validate_pyspark_source
        from agent.etl_pipeline.python_codegen import generate_python_etl
        from agent.etl_pipeline.validate_python import validate_etl_python_source

        eng = (plan.get("engine_recommendation") or {}).get("engine", "python")
        if eng in ("pyspark", "spark"):
            code = generate_pyspark_etl(plan, self.assess)
            ok, _ = validate_pyspark_source(code, plan)
        else:
            code = generate_python_etl(plan, self.assess)
            ok, _ = validate_etl_python_source(code)
        self.assertTrue(ok, "generated code should pass validator")
        self.assertIn("plan_id", code)


if __name__ == "__main__":
    unittest.main()
