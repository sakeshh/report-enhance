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


if __name__ == "__main__":
    unittest.main()
