"""
ETL scenarios 2–14 — automated verification (template codegen, no live LLM).

Run: PYTHONPATH=. python -m unittest tests.test_etl_scenarios -v
"""
from __future__ import annotations

import copy
import re
import unittest
from typing import Any, Dict, List, Tuple

from tests.fixtures.blob_pair_assessment import blob_session_context, make_blob_pair_assessment

from agent.etl_handlers import _rehydrate_plan, _template_fallback
from agent.etl_pipeline.business_rules import normalize_business_rules
from agent.etl_pipeline.connector_manifest import build_connector_manifest
from agent.etl_pipeline.etl_gx_checkpoint import _expectations_from_plan
from agent.etl_pipeline.planner import build_etl_plan
from agent.etl_pipeline.preview_impact import build_impact_preview
from agent.etl_pipeline.source_context import build_source_context
from agent.etl_pipeline.validate_plan import validate_etl_plan
from agent.etl_pipeline.validate_python import validate_etl_python_source
from agent.etl_pipeline.validate_pyspark import validate_pyspark_source
from agent.etl_pipeline.validate_sql import validate_sql_basic
from agent.specialists.etl_guidance_specialist import format_etl_guidance


def _attach_manifest(
    plan: Dict[str, Any],
    assess: Dict[str, Any],
    ctx: Dict[str, Any],
    *,
    output_base: str = "cleaned/",
    overwrite: bool = False,
) -> Dict[str, Any]:
    manifest = build_connector_manifest(
        ctx, assess, output_base=output_base, overwrite_in_place=overwrite
    )
    plan = dict(plan)
    plan["connector_manifest"] = manifest
    plan["source_context"] = build_source_context(ctx, assess)
    return plan


def _build_plan(
    assess: Dict[str, Any],
    rules_raw: Dict[str, Any],
    *,
    output_base: str = "cleaned/",
    overwrite: bool = False,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    ctx = blob_session_context()
    rules = normalize_business_rules(rules_raw)
    plan = build_etl_plan(assess, rules, source_context=build_source_context(ctx, assess))
    plan = _attach_manifest(plan, assess, ctx, output_base=output_base, overwrite=overwrite)
    plan["business_rules"] = rules
    return plan, rules


def _steps_for(plan: Dict[str, Any], ds: str) -> List[Dict[str, Any]]:
    return list((plan.get("datasets") or {}).get(ds, {}).get("steps") or [])


def _has_action(steps: List[Dict[str, Any]], col: str, action: str) -> bool:
    return any(s.get("column") == col and s.get("action") == action for s in steps)


def _gen(engine: str, plan: Dict[str, Any], assess: Dict[str, Any]):
    code, ok, errs = _template_fallback(engine, plan, assess, sql_dialect="tsql")
    if engine == "python" and not ok:
        ok2, errs2 = validate_etl_python_source(code)
        if ok2:
            return code, True, []
    return code, ok, errs


class TestEtlScenarios(unittest.TestCase):
    assess = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.assess = make_blob_pair_assessment(overlap_count=15)

    # ── Scenario 2: case normalization + never drop rows ─────────────────

    def test_scenario_02_plan_lowercase_both_datasets(self) -> None:
        plan, rules = _build_plan(
            self.assess,
            {
                "never_drop_rows": True,
                "notes": "Normalize name and department to lowercase on both data_1.json and data_1.xml.",
            },
        )
        ok, errs = validate_etl_plan(plan, self.assess, rules)
        self.assertTrue(ok, errs)
        for ds in ("data_1.json", "data_1.xml"):
            steps = _steps_for(plan, ds)
            self.assertTrue(_has_action(steps, "name", "lowercase"), f"{ds} missing lowercase name")
            self.assertTrue(
                _has_action(steps, "department", "lowercase"),
                f"{ds} missing lowercase department",
            )
            ev = next(s["evidence"] for s in steps if s.get("column") == "name")
            why = (ev.get("why_this_action") or "").lower()
            self.assertTrue(
                "case" in why or "inconsist" in why,
                f"expected case evidence on name for {ds}: {why}",
            )

    def test_scenario_02_python_lowercase_no_row_drop(self) -> None:
        plan, _ = _build_plan(
            self.assess,
            {"never_drop_rows": True, "notes": "Normalize name and department to lowercase on both files."},
        )
        code, ok, errs = _gen("python", plan, self.assess)
        self.assertTrue(ok, errs)
        self.assertIn(".str.lower()", code)
        self.assertIn("transform_data_1_json", code)
        self.assertIn("transform_data_1_xml", code)
        self.assertNotIn("dropna()", code)
        self.assertNotIn('how="inner"', code)

    def test_scenario_02_pyspark_valid_template(self) -> None:
        plan, _ = _build_plan(
            self.assess,
            {"never_drop_rows": True, "notes": "Normalize name and department to lowercase."},
        )
        code, ok, errs = _gen("pyspark", plan, self.assess)
        self.assertTrue(ok, errs)
        self.assertIn("F.lower", code)
        self.assertIn("def _resolve_data_path", code)
        self.assertIn("AZURE_STORAGE_ACCOUNT", code)
        self.assertIn("Joins skipped", code)
        self.assertNotIn("joined_", code)

    # ── Scenario 3: phone privacy + manual review ──────────────────────────

    def test_scenario_03_plan_manual_review_and_hash_step(self) -> None:
        plan, rules = _build_plan(
            self.assess,
            {
                "notes": (
                    "Hash or mask phone on data_1.xml for privacy; "
                    "normalize name and department to lowercase on both files."
                ),
            },
        )
        manual = plan.get("manual_review") or []
        phone_review = [m for m in manual if (m.get("column") or "").lower() == "phone"]
        self.assertGreaterEqual(
            len(phone_review), 1, "expected manual_review entry for phone on xml"
        )
        xml_steps = _steps_for(plan, "data_1.xml")
        self.assertTrue(
            _has_action(xml_steps, "phone", "hash_phone"),
            "expected hash_phone step on data_1.xml from business notes",
        )
        json_steps = _steps_for(plan, "data_1.json")
        self.assertFalse(
            _has_action(json_steps, "phone", "hash_phone"),
            "phone hash should not be on json unless noted",
        )

    def test_scenario_03_pyspark_hashes_phone_on_xml_only(self) -> None:
        plan, _ = _build_plan(
            self.assess,
            {
                "notes": "Hash or mask phone on data_1.xml for privacy; normalize name and department to lowercase on both files.",
            },
        )
        code, ok, errs = _gen("pyspark", plan, self.assess)
        self.assertTrue(ok, errs)
        self.assertIn("sha2", code)
        self.assertIn("transform_data_1_xml", code)

    # ── Scenario 4: multi-dataset join (1:1, overlap 15) ─────────────────

    def test_scenario_04_relationships_join_and_load_order(self) -> None:
        plan, rules = _build_plan(self.assess, {})
        rel = plan.get("relationships") or {}
        self.assertGreaterEqual(rel.get("join_count", 0), 1)
        joins = [j for j in rel.get("joins") or [] if j.get("join_type") != "review"]
        self.assertGreaterEqual(len(joins), 1)
        jt = str(joins[0].get("join_type") or "").lower()
        self.assertIn(jt, ("inner", "left"))
        self.assertEqual(joins[0].get("overlap_count"), 15)
        load_order = rel.get("load_order") or []
        self.assertIn("data_1.json", load_order)
        self.assertIn("data_1.xml", load_order)
        preview = build_impact_preview(self.assess, plan)
        text = " ".join(preview.get("summary_lines") or preview.get("bullets") or [])
        self.assertIn("data_1.json", text)
        self.assertIn("data_1.xml", text)

    def test_scenario_04_python_run_joins(self) -> None:
        plan, _ = _build_plan(self.assess, {})
        code, ok, errs = _gen("python", plan, self.assess)
        self.assertTrue(ok, errs)
        self.assertIn("def run_joins", code)
        self.assertTrue('on="id"' in code or "left_on=" in code, "expected join on id in codegen")

    # ── Scenario 5: required column guard ──────────────────────────────────

    def test_scenario_05_required_columns_plan_ok(self) -> None:
        plan, rules = _build_plan(self.assess, {"required_columns": ["id", "name"]})
        ok, errs = validate_etl_plan(plan, self.assess, rules)
        self.assertTrue(ok, errs)

    def test_scenario_05_python_required_guard(self) -> None:
        plan, _ = _build_plan(self.assess, {"required_columns": ["id", "name"]})
        code, ok, errs = _gen("python", plan, self.assess)
        self.assertTrue(ok, errs)
        self.assertIn("Required columns missing", code)
        self.assertIn("ValueError", code)

    def test_scenario_05_typo_required_column_blocked(self) -> None:
        plan, rules = _build_plan(self.assess, {"required_columns": ["customer_id"]})
        ok, errs = validate_etl_plan(plan, self.assess, rules)
        self.assertFalse(ok)
        self.assertTrue(any("customer_id" in e for e in errs))

    # ── Scenario 6: never drop + valid values ───────────────────────────────

    def test_scenario_06_valid_values_nullify_not_drop(self) -> None:
        plan, rules = _build_plan(
            self.assess,
            {
                "never_drop_rows": True,
                "valid_values": {"department": ["IT", "HR", "Finance"]},
                "notes": "valid values for department: IT, HR, Finance (case insensitive after lowercasing)",
            },
        )
        code, ok, errs = _gen("python", plan, self.assess)
        self.assertTrue(ok, errs)
        self.assertIn("never_drop_rows", code)
        self.assertIn("nullify disallowed", code.lower())
        self.assertIn(".str.lower()", code)
        self.assertIn("str.lower().isin", code)
        self.assertIn("pd.NA", code)
        self.assertNotIn("valid_values rule on", code)  # drop-path log line absent

    # ── Scenario 7: SQL T-SQL ──────────────────────────────────────────────

    def test_scenario_07_sql_tsql_lowercase_and_join_comments(self) -> None:
        plan, _ = _build_plan(self.assess, {})
        code, ok, errs = _gen("sql", plan, self.assess)
        self.assertTrue(ok, errs)
        self.assertRegex(code, r"LOWER\s*\(", re.I)
        self.assertIn("department", code)
        self.assertIn("name", code)
        self.assertIn("JOIN", code)

    # ── Scenario 8: PySpark ─────────────────────────────────────────────────

    def test_scenario_08_pyspark_run_pipeline(self) -> None:
        plan, _ = _build_plan(self.assess, {"never_drop_rows": True})
        code, ok, errs = _gen("pyspark", plan, self.assess)
        self.assertTrue(ok, errs)
        self.assertIn("transform_data_1_json", code)
        self.assertIn("transform_data_1_xml", code)
        self.assertIn("def run_pipeline", code)
        self.assertIn("SparkSession", code)

    # ── Scenario 9: write cleaned paths ─────────────────────────────────────

    def test_scenario_09_manifest_cleaned_output_paths(self) -> None:
        plan, _ = _build_plan(self.assess, {}, output_base="cleaned/")
        m_ds = (plan.get("connector_manifest") or {}).get("datasets") or {}
        self.assertIn("data_1.json", m_ds)
        self.assertTrue(
            str(m_ds["data_1.json"].get("output_path", "")).startswith("cleaned/"),
            m_ds["data_1.json"].get("output_path"),
        )
        code, ok, errs = _gen("python", plan, self.assess)
        self.assertTrue(ok, errs)
        self.assertIn("write_outputs", code)
        self.assertIn("cleaned/", code)

    # ── Scenario 10: overwrite in-place ───────────────────────────────────

    def test_scenario_10_overwrite_manifest_paths(self) -> None:
        plan, _ = _build_plan(self.assess, {}, output_base="__overwrite__", overwrite=True)
        m_ds = (plan.get("connector_manifest") or {}).get("datasets") or {}
        self.assertEqual(m_ds["data_1.json"]["output_path"], "data_1.json")
        self.assertEqual(m_ds["data_1.xml"]["location"], "data_1.xml")

    # ── Scenario 11: plan edit + rehydrate ──────────────────────────────────

    def test_scenario_11_rehydrate_manifest_after_ui_strip(self) -> None:
        plan, _ = _build_plan(self.assess, {})
        ctx = {
            "connector_manifest": plan["connector_manifest"],
            "source_context": plan["source_context"],
            "etl_flow": {"plan": {"relationships": plan["relationships"]}},
        }
        edited = copy.deepcopy(plan)
        edited.pop("connector_manifest", None)
        edited.pop("relationships", None)
        steps = edited["datasets"]["data_1.json"]["steps"]
        edited["datasets"]["data_1.json"]["steps"] = [
            s for s in steps if s.get("column") != "name"
        ]
        restored = _rehydrate_plan(edited, ctx)
        self.assertIn("connector_manifest", restored)
        self.assertIn("relationships", restored)
        self.assertFalse(
            _has_action(_steps_for(restored, "data_1.json"), "name", "lowercase")
        )
        code, ok, errs = _gen("python", restored, self.assess)
        self.assertTrue(ok, errs)
        self.assertIn("run_joins", code)
        self.assertIn("load_all_datasets", code)

    # ── Scenario 13: GX expectations ────────────────────────────────────────

    def test_scenario_13_gx_expectations_for_transformed_columns(self) -> None:
        plan, rules = _build_plan(
            self.assess,
            {"never_drop_rows": True, "required_columns": ["id", "name"]},
        )
        exps = _expectations_from_plan(plan, self.assess, rules)
        types = {e.get("type") for e in exps}
        self.assertIn("expect_column_to_exist", types)
        cols_mentioned = " ".join(
            str(e.get("column") or "") + str(e.get("detail") or "") for e in exps
        )
        self.assertTrue(
            "name" in cols_mentioned or "department" in cols_mentioned,
            "expected expectations referencing transformed columns",
        )

    # ── Scenario 14: guidance snippets vs full plan ─────────────────────────

    def test_scenario_14_guidance_is_snippet_not_full_pipeline(self) -> None:
        msg = format_etl_guidance(
            self.assess,
            "how do I fix case inconsistency in sql for name column?",
            blob_session_context(),
        )
        low = msg.lower()
        self.assertIn("sql", low)
        self.assertNotIn("load_all_datasets", msg)
        self.assertNotIn("def transform_data_1_json", msg)

    def test_scenario_14_build_plan_is_structured(self) -> None:
        plan, rules = _build_plan(self.assess, {})
        self.assertIn("data_1.json", plan.get("datasets") or {})
        self.assertIn("data_1.xml", plan.get("datasets") or {})
        ok, _ = validate_etl_plan(plan, self.assess, rules)
        self.assertTrue(ok)
        steps = _steps_for(plan, "data_1.json")
        if steps:
            self.assertIn("params", steps[0])
            self.assertIn("invariants", plan)


if __name__ == "__main__":
    unittest.main()
