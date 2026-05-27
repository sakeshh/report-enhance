"""Manual review catalog, promote, and confirm gating."""
from __future__ import annotations

import unittest

from agent.etl_pipeline.manual_review_catalog import (
    enrich_manual_review_item,
    get_resolution_options,
)
from agent.etl_pipeline.manual_review_promote import (
    apply_manual_resolutions,
    count_pending_manual_review,
)
from agent.etl_pipeline.python_codegen import generate_python_etl
from agent.etl_pipeline.validate_plan import validate_etl_plan_for_confirm


class TestManualReviewCatalog(unittest.TestCase):
    def test_very_high_cardinality_has_hash_option(self) -> None:
        opts = get_resolution_options("very_high_cardinality")
        ids = [o["id"] for o in opts]
        self.assertIn("hash_sha256", ids)
        self.assertTrue(any(o.get("recommended") for o in opts))

    def test_enrich_adds_id_and_options(self) -> None:
        item = enrich_manual_review_item(
            {
                "dataset": "data_1.xml",
                "column": "phone",
                "issue_type": "very_high_cardinality",
                "message": "High cardinality",
            }
        )
        self.assertIn("id", item)
        self.assertGreaterEqual(len(item.get("resolution_options") or []), 2)
        self.assertEqual(item.get("status"), "pending")


class TestManualReviewPromote(unittest.TestCase):
    def _base_plan(self) -> dict:
        return {
            "plan_id": "test",
            "datasets": {"data_1.xml": {"steps": []}},
            "manual_review": [
                enrich_manual_review_item(
                    {
                        "dataset": "data_1.xml",
                        "column": "phone",
                        "issue_type": "very_high_cardinality",
                        "message": "phone cardinality",
                    }
                )
            ],
            "business_rules": {},
        }

    def test_promote_hash_adds_step(self) -> None:
        plan = self._base_plan()
        item_id = plan["manual_review"][0]["id"]
        updated, errs = apply_manual_resolutions(
            plan,
            [{"item_id": item_id, "resolution_id": "hash_sha256"}],
        )
        self.assertEqual(errs, [])
        self.assertEqual(count_pending_manual_review(updated), 0)
        steps = updated["datasets"]["data_1.xml"]["steps"]
        self.assertTrue(any(s.get("action") == "hash_phone" for s in steps))

    def test_skip_keeps_no_step(self) -> None:
        plan = self._base_plan()
        item_id = plan["manual_review"][0]["id"]
        updated, errs = apply_manual_resolutions(
            plan,
            [{"item_id": item_id, "resolution_id": "keep_as_is"}],
        )
        self.assertEqual(errs, [])
        self.assertEqual(len(updated["datasets"]["data_1.xml"]["steps"]), 0)
        self.assertEqual(count_pending_manual_review(updated), 0)

    def test_codegen_includes_hash_after_promote(self) -> None:
        plan = self._base_plan()
        item_id = plan["manual_review"][0]["id"]
        updated, _ = apply_manual_resolutions(
            plan,
            [{"item_id": item_id, "resolution_id": "hash_sha256"}],
        )
        code = generate_python_etl(updated, {"datasets": {"data_1.xml": {"columns": {"phone": {}}}}})
        self.assertIn("hashlib.sha256", code)
        self.assertIn("phone", code)

    def test_validate_blocks_confirm_while_pending(self) -> None:
        plan = self._base_plan()
        assess = {"datasets": {"data_1.xml": {"columns": {"phone": {}}}}}
        ok, errs = validate_etl_plan_for_confirm(plan, assess, {})
        self.assertFalse(ok)
        self.assertTrue(any("manual review" in e.lower() for e in errs))

    def test_new_active_catalog_options(self) -> None:
        # Verify resolutions are found in catalog
        for issue in ["all_caps_values", "duplicate_insensitive_values", "numeric_outliers_zscore", 
                      "string_length_outlier", "date_format_inconsistency", "mixed_date_formats", "at_least_one"]:
            opts = get_resolution_options(issue)
            self.assertGreaterEqual(len(opts), 1)
            self.assertTrue(any(o.get("recommended") for o in opts), f"No recommended option for {issue}")

    def test_promote_at_least_one(self) -> None:
        plan = {
            "plan_id": "test_at_least_one",
            "datasets": {"dbo.students_raw": {"steps": []}},
            "manual_review": [
                enrich_manual_review_item(
                    {
                        "dataset": "dbo.students_raw",
                        "column": "email,phone",
                        "issue_type": "at_least_one",
                        "message": "At least one must be non-null",
                    }
                )
            ],
            "business_rules": {},
        }
        item_id = plan["manual_review"][0]["id"]
        updated, errs = apply_manual_resolutions(
            plan,
            [{"item_id": item_id, "resolution_id": "quarantine_all_null"}],
        )
        self.assertEqual(errs, [])
        steps = updated["datasets"]["dbo.students_raw"]["steps"]
        self.assertTrue(any(s.get("action") == "at_least_one" for s in steps))

        # Test python codegen
        code_py = generate_python_etl(updated, {"datasets": {"dbo.students_raw": {"columns": {"email": {}, "phone": {}}}}})
        self.assertIn("isna().all(axis=1)", code_py)
        self.assertIn("Quarantining", code_py)

        # Test T-SQL codegen
        from agent.etl_pipeline.sql_codegen import generate_sql_etl
        code_sql = generate_sql_etl(updated, {"datasets": {"dbo.students_raw": {"columns": {"email": {}, "phone": {}}}}})
        self.assertIn("Quarantine rows where all of email,phone are NULL", code_sql)
        self.assertIn("r.[email] IS NULL AND r.[phone] IS NULL", code_sql)


class TestBlockerManualReview(unittest.TestCase):
    def test_missing_required_column_pending_and_resolved(self) -> None:
        plan = {
            "plan_id": "test_missing_col",
            "datasets": {"dbo.customers_raw": {"steps": []}},
            "manual_review": [
                enrich_manual_review_item(
                    {
                        "dataset": "global",
                        "column": "CreatedDate",
                        "issue_type": "missing_required_column",
                        "message": "Required column CreatedDate not found",
                    }
                )
            ],
            "business_rules": {"required_columns": ["CreatedDate", "CustomerID"]},
        }
        assess = {"datasets": {"dbo.customers_raw": {"columns": {"CustomerID": {}}}}}
        
        # Test 1: validate_etl_plan should succeed (ok=True) even if missing_required_column is pending
        from agent.etl_pipeline.validate_plan import validate_etl_plan
        ok, errs = validate_etl_plan(plan, assess, plan["business_rules"])
        self.assertTrue(ok, f"Expected validation to pass while pending: {errs}")
        
        # Test 2: validate_etl_plan_for_confirm should fail (ok=False) while pending
        ok, errs = validate_etl_plan_for_confirm(plan, assess, plan["business_rules"])
        self.assertFalse(ok)
        self.assertTrue(any("manual review" in e.lower() for e in errs))
        
        # Test 3: Apply skip_requirement resolution
        item_id = plan["manual_review"][0]["id"]
        updated, errs = apply_manual_resolutions(
            plan,
            [{"item_id": item_id, "resolution_id": "skip_requirement"}],
        )
        self.assertEqual(errs, [])
        self.assertEqual(count_pending_manual_review(updated), 0)
        self.assertNotIn("CreatedDate", updated["business_rules"]["required_columns"])
        
        # Test 4: validate_etl_plan_for_confirm should pass now
        ok, errs = validate_etl_plan_for_confirm(updated, assess, updated["business_rules"])
        self.assertTrue(ok, f"Expected validation for confirm to pass: {errs}")

    def test_business_key_duplicate_to_custom_deduplicate(self) -> None:
        plan = {
            "plan_id": "test_bk_dup",
            "datasets": {"dbo.customers_raw": {"steps": []}},
            "manual_review": [
                enrich_manual_review_item(
                    {
                        "dataset": "dbo.customers_raw",
                        "column": "CustomerID, Email",
                        "issue_type": "business_key_duplicate",
                        "message": "Duplicates in CustomerID, Email",
                    }
                )
            ],
            "business_rules": {},
        }
        assess = {"datasets": {"dbo.customers_raw": {"columns": {"CustomerID": {}, "Email": {}, "ModifiedDate": {}}}}}
        
        # Apply deduplicate resolution
        item_id = plan["manual_review"][0]["id"]
        updated, errs = apply_manual_resolutions(
            plan,
            [{"item_id": item_id, "resolution_id": "deduplicate"}],
        )
        self.assertEqual(errs, [])
        
        # Test Python codegen custom deduplication columns list formatting
        code_py = generate_python_etl(updated, assess)
        self.assertIn("drop_duplicates(subset=['CustomerID', 'Email']", code_py)
        
        # Test T-SQL codegen custom deduplication partition keys
        from agent.etl_pipeline.sql_codegen import generate_sql_etl
        code_sql = generate_sql_etl(updated, assess)
        self.assertIn("ROW_NUMBER() OVER (PARTITION BY LOWER(LTRIM(RTRIM(CAST([CustomerID] AS NVARCHAR(400))))), LOWER(LTRIM(RTRIM(CAST([Email] AS NVARCHAR(400)))))", code_sql)


if __name__ == "__main__":
    unittest.main()
