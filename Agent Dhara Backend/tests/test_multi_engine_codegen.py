"""
Multi-engine codegen: plan params drive Python, SQL, PySpark, ADF consistently.
"""
from __future__ import annotations

import json
import unittest

from tests.fixtures.blob_pair_assessment import blob_session_context, make_blob_pair_assessment

from agent.etl_pipeline.business_rules import normalize_business_rules
from agent.etl_pipeline.connector_manifest import build_connector_manifest
from agent.etl_pipeline.planner import build_etl_plan
from agent.etl_pipeline.python_codegen import generate_python_etl
from agent.etl_pipeline.sql_codegen import generate_sql_etl
from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
from agent.etl_pipeline.adf_codegen import generate_adf_mapping_flow
from agent.etl_pipeline.source_context import build_source_context
from agent.etl_pipeline.validate_python import validate_etl_python_source
from agent.etl_pipeline.validate_sql import validate_sql_basic
from agent.etl_pipeline.validate_pyspark import validate_pyspark_source
from agent.etl_pipeline.validate_adf import validate_adf_bundle


class TestMultiEngineCodegen(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.assess = make_blob_pair_assessment()
        cls.rules = normalize_business_rules(
            {
                "never_drop_rows": True,
                "outlier_strategy": "flag",
                "valid_values": {"department": ["engineering", "sales"]},
            }
        )
        ctx = blob_session_context()
        cls.plan = build_etl_plan(
            cls.assess,
            cls.rules,
            source_context=build_source_context(ctx, cls.assess),
        )
        cls.plan["connector_manifest"] = build_connector_manifest(
            ctx, cls.assess, output_base="cleaned/"
        )
        cls.plan["business_rules"] = cls.rules

    def test_plan_steps_have_params(self):
        for block in (self.plan.get("datasets") or {}).values():
            for st in (block or {}).get("steps") or []:
                self.assertIn("params", st)
                self.assertIsInstance(st["params"], dict)
                self.assertIn("evidence", st)

    def test_python_codegen_valid_and_uses_params(self):
        code = generate_python_etl(self.plan, self.assess)
        ok, errs = validate_etl_python_source(code, self.plan)
        self.assertTrue(ok, errs)
        self.assertIn("Policy:", code)
        self.assertIn("def run_all", code)
        self.assertIn("_prefix_columns_python", code)

    def test_sql_tsql_codegen_valid(self):
        code = generate_sql_etl(self.plan, self.assess, dialect="tsql")
        ok, errs = validate_sql_basic(code)
        self.assertTrue(ok, errs)
        self.assertIn("BEGIN TRY", code)
        self.assertIn("END CATCH", code)

    def test_pyspark_codegen_valid(self):
        code = generate_pyspark_etl(self.plan, self.assess)
        ok, errs = validate_pyspark_source(code, self.plan)
        self.assertTrue(ok, errs)
        self.assertIn("_iqr_bounds", code)
        self.assertIn("run_pipeline", code)

    def test_adf_bundle_and_expressions(self):
        obj = generate_adf_mapping_flow(self.plan, self.assess)
        ok, errs = validate_adf_bundle(obj)
        self.assertTrue(ok, errs)
        bundle = obj.get("bundle") or {}
        flows = bundle.get("flows") or []
        self.assertGreaterEqual(len(flows), 1)
        roles = {f.get("role") for f in flows}
        self.assertIn("clean_only", roles)
        if (self.plan.get("relationships") or {}).get("joins"):
            self.assertIn("clean_and_joined", roles)
        dumped = json.dumps(obj)
        self.assertIn("toLower", dumped)
        self.assertIn("typeProperties", dumped)


if __name__ == "__main__":
    unittest.main()
