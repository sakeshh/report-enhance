"""Intelligence-first ETL plan: evidence, engine recommendation, narration."""
from __future__ import annotations

import unittest

from agent.etl_pipeline.plan_narrator import _fallback_narration, narrate_plan
from agent.etl_pipeline.planner import _build_evidence, _recommend_engine, build_etl_plan
from agent.etl_pipeline.relationship_planner import build_relationship_plan
from agent.etl_pipeline.source_context import build_source_context


class TestIntelligence(unittest.TestCase):
    def test_evidence_from_dq_message_and_nulls(self):
        ev = _build_evidence(
            {
                "column": "age",
                "auto_fixable": True,
                "message": "412 null values in age",
                "row_count_affected": 412,
                "issue_type": "nulls",
                "severity": "medium",
            },
            {"null_percentage": 0.027, "row_count": 15000, "dtype": "float64"},
            "fill_nulls_simple",
            {},
        )
        self.assertIsNotNone(ev["null_pct"])
        self.assertGreaterEqual(ev["confidence"], 0.6)
        self.assertTrue(
            "412" in ev["why_this_action"] or "null" in ev["why_this_action"].lower()
        )

    def test_evidence_high_null_alternatives(self):
        ev = _build_evidence(
            {
                "column": "notes",
                "auto_fixable": True,
                "message": "High null rate",
                "row_count_affected": 9000,
                "issue_type": "nulls",
            },
            {"null_percentage": 0.9, "row_count": 10000},
            "fill_nulls_simple",
            {},
        )
        self.assertEqual(ev["null_pct"], 90.0)
        self.assertGreater(len(ev["alternatives"]), 0)

    def test_evidence_never_drop_rows(self):
        ev = _build_evidence(
            {"column": "id", "auto_fixable": True, "message": "few nulls", "issue_type": "nulls"},
            {"null_percentage": 0.005, "row_count": 1000},
            "fill_nulls_simple",
            {"never_drop_rows": True},
        )
        self.assertTrue(ev["rule_override"])

    def test_recommend_python_small_csv(self):
        rec = _recommend_engine(
            {"type": "csv_file", "size_mb": 2.4, "row_count": 15000, "extension": ".csv"},
            {},
        )
        self.assertEqual(rec["engine"], "python")
        self.assertIsNone(rec["warning"])

    def test_recommend_sql_azure(self):
        rec = _recommend_engine({"type": "azure_sql", "location": "customers"}, {})
        self.assertEqual(rec["engine"], "sql")
        self.assertEqual(rec["dialect"], "tsql")

    def test_recommend_pyspark_large(self):
        rec = _recommend_engine(
            {"type": "csv_file", "size_mb": 800, "row_count": 5_000_000},
            {},
        )
        self.assertEqual(rec["engine"], "pyspark")
        self.assertTrue(rec["warning"])

    def test_recommend_pyspark_from_assessment_rows(self):
        rec = _recommend_engine({}, {"datasets": {"orders": {"row_count": 2_000_000}}})
        self.assertEqual(rec["engine"], "pyspark")

    def test_build_plan_has_evidence_and_engine_rec(self):
        assessment = {
            "datasets": {
                "customers.csv": {
                    "row_count": 100,
                    "columns": {
                        "email": {
                            "dtype": "object",
                            "null_percentage": 0.1,
                            "semantic_type": "email",
                        }
                    },
                }
            },
            "data_quality_issues": {
                "datasets": {
                    "customers.csv": {
                        "issues": [
                            {
                                "type": "invalid_email",
                                "column": "email",
                                "severity": "medium",
                                "message": "12 invalid emails",
                                "count": 12,
                            }
                        ]
                    }
                }
            },
        }
        plan = build_etl_plan(
            assessment,
            {},
            source_context={"type": "csv_file", "extension": ".csv", "row_count": 100},
        )
        self.assertIn("engine_recommendation", plan)
        self.assertEqual(plan["engine_recommendation"]["engine"], "python")
        steps = plan["datasets"]["customers.csv"]["steps"]
        self.assertTrue(steps)
        self.assertTrue(steps[0].get("evidence"))
        self.assertTrue(steps[0]["evidence"].get("why_this_action"))

    def test_source_context_from_session_tables(self):
        ctx = build_source_context(
            {"selected_source": "azure_sql", "selected_tables": ["dbo.Customers"]},
            {"datasets": {"dbo.Customers": {"row_count": 500}}},
        )
        self.assertEqual(ctx["type"], "azure_sql")
        self.assertEqual(len(ctx.get("sources") or []), 1)

    def test_source_context_multi_dataset(self):
        ctx = build_source_context(
            {
                "selected_local_files": ["a.csv", "b.csv"],
                "local_files_root": "/data",
            },
            {
                "datasets": {
                    "a.csv": {"row_count": 100},
                    "b.csv": {"row_count": 200},
                }
            },
        )
        self.assertTrue(ctx.get("is_multi_source"))
        self.assertEqual(ctx.get("source_count"), 2)
        self.assertEqual(len(ctx.get("sources") or []), 2)

    def test_fallback_narration_empty(self):
        result = _fallback_narration({"datasets": {}, "manual_review": []})
        self.assertIn("engine_explanation", result)
        self.assertIn("overall_readiness", result)

    def test_evidence_recommends_median_when_skewed(self):
        ev = _build_evidence(
            {"column": "amount", "auto_fixable": True, "message": "nulls", "issue_type": "nulls"},
            {
                "null_percentage": 0.05,
                "row_count": 1000,
                "mean": 100.0,
                "median": 40.0,
                "skew": 2.5,
            },
            "fill_nulls_simple",
            {},
        )
        self.assertEqual(ev.get("recommended_fill"), "median")

    def test_relationship_plan_detects_join(self):
        assessment = {
            "datasets": {
                "customers": {"row_count": 100, "columns": {"id": {}}},
                "orders": {"row_count": 500, "columns": {"customer_id": {}}},
            },
            "relationships": [
                {
                    "dataset_a": "customers",
                    "dataset_b": "orders",
                    "column_a": "id",
                    "column_b": "customer_id",
                    "cardinality": "one_to_many",
                    "max_rows_per_key_a": 1,
                    "max_rows_per_key_b": 3,
                    "overlap_count": 95,
                    "summary": "1:N from customers to orders",
                }
            ],
            "data_quality_issues": {"global_issues": {}},
        }
        rel = build_relationship_plan(assessment)
        self.assertEqual(rel["join_count"], 1)
        self.assertEqual(rel["joins"][0]["parent_dataset"], "customers")
        self.assertEqual(rel["joins"][0]["child_dataset"], "orders")

    def test_build_plan_includes_relationships(self):
        assessment = {
            "datasets": {
                "a.csv": {
                    "row_count": 10,
                    "columns": {"id": {"dtype": "int64", "null_percentage": 0}},
                },
                "b.csv": {
                    "row_count": 20,
                    "columns": {"id": {"dtype": "int64", "null_percentage": 0}},
                },
            },
            "relationships": [
                {
                    "dataset_a": "a.csv",
                    "dataset_b": "b.csv",
                    "column_a": "id",
                    "column_b": "id",
                    "cardinality": "one_to_one",
                    "max_rows_per_key_a": 1,
                    "max_rows_per_key_b": 1,
                    "overlap_count": 10,
                    "summary": "1:1",
                }
            ],
            "data_quality_issues": {"datasets": {}, "global_issues": {}},
        }
        plan = build_etl_plan(assessment, {}, source_context={"type": "csv_file"})
        self.assertIn("relationships", plan)
        self.assertGreaterEqual(plan["relationships"].get("join_count", 0), 1)

    def test_narrate_plan_fallback_without_llm(self):
        plan = {
            "engine_recommendation": {
                "engine": "python",
                "reason": "small file",
                "alternatives": [],
                "warning": None,
            },
            "datasets": {
                "ds": {
                    "steps": [
                        {
                            "order": 1,
                            "column": "email",
                            "action": "sanitize_email",
                            "bucket": "auto",
                            "evidence": {
                                "why_this_action": "invalid emails",
                                "alternatives": [],
                                "confidence": 0.85,
                            },
                        }
                    ]
                }
            },
            "manual_review": [],
            "blocked": [],
        }
        result = narrate_plan(plan, mode="fallback", use_llm=False)
        self.assertIn("ds", result["dataset_summaries"])

    def test_relationship_plan_many_to_many(self):
        assessment = {
            "datasets": {
                "products": {"row_count": 100, "columns": {"id": {}}},
                "tags": {"row_count": 200, "columns": {"id": {}}},
            },
            "relationships": [
                {
                    "dataset_a": "products",
                    "dataset_b": "tags",
                    "column_a": "id",
                    "column_b": "id",
                    "cardinality": "many_to_many",
                    "max_rows_per_key_a": 5,
                    "max_rows_per_key_b": 8,
                    "overlap_count": 40,
                }
            ],
            "data_quality_issues": {"global_issues": {}},
        }
        rel = build_relationship_plan(assessment)
        self.assertEqual(rel["mn_count"], 1)
        self.assertEqual(len(rel["many_to_many"]), 1)
        self.assertTrue(rel["many_to_many"][0].get("bridge_name"))

    def test_narrate_includes_relationships_summary(self):
        plan = {
            "engine_recommendation": {"engine": "python", "reason": "small"},
            "datasets": {},
            "manual_review": [],
            "relationships": {"join_count": 2, "mn_count": 1, "load_order": ["a", "b"]},
        }
        result = narrate_plan(plan, mode="fallback")
        self.assertIn("join", result["relationships_summary"].lower())


if __name__ == "__main__":
    unittest.main()
