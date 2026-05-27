import unittest
import re
import pandas as pd
from typing import Any, Dict

from agent.intelligent_data_assessment import detect_semantic_type
from agent.etl_pipeline.planner import build_etl_plan
from agent.etl_pipeline.sql_codegen import generate_sql_etl
from agent.etl_pipeline.manual_review_catalog import enrich_manual_review_item


class TestSemanticEtlUpgrades(unittest.TestCase):
    def test_date_hint_word_boundaries(self) -> None:
        # Regex used for date hints: r'(?:\b|_)(date|time|dt|created|updated|dob|birth|bday|birthday)(?:\b|_)|(_at\b|\bat\b)'
        date_hint_pattern = r'(?:\b|_)(date|time|dt|created|updated|dob|birth|bday|birthday)(?:\b|_)|(_at\b|\bat\b)'
        
        # Valid date hints
        self.assertTrue(bool(re.search(date_hint_pattern, "created_at")))
        self.assertTrue(bool(re.search(date_hint_pattern, "updated_at")))
        self.assertTrue(bool(re.search(date_hint_pattern, "order_date")))
        self.assertTrue(bool(re.search(date_hint_pattern, "dob")))
        self.assertTrue(bool(re.search(date_hint_pattern, "birth_date")))
        self.assertTrue(bool(re.search(date_hint_pattern, "updated at")))
        self.assertTrue(bool(re.search(date_hint_pattern, "dt_created")))

        # Non-date columns containing "at" or similar substrings
        self.assertFalse(bool(re.search(date_hint_pattern, "attendance")))
        self.assertFalse(bool(re.search(date_hint_pattern, "category")))
        self.assertFalse(bool(re.search(date_hint_pattern, "rate")))
        self.assertFalse(bool(re.search(date_hint_pattern, "latitude")))
        self.assertFalse(bool(re.search(date_hint_pattern, "status")))

    def test_auto_resolve_pending_manual_review(self) -> None:
        # Construct assessment result
        assessment = {
            "datasets": {
                "dbo.Orders_Raw": {
                    "columns": {
                        "OrderID": {"dtype": "int", "candidate_primary_key": True},
                        "customer_email": {"dtype": "object"},
                    }
                }
            }
        }
        
        # Enable auto_resolve_pending in business rules
        business_rules = {
            "never_drop_rows": False,
            "auto_resolve_pending": True,
        }
        
        # Force a suggestion that triggers manual review
        plan = build_etl_plan(
            assessment=assessment,
            business_rules_raw=business_rules,
            source_context={
                "suggestions": [
                    {
                        "dataset": "dbo.Orders_Raw",
                        "column": "customer_email",
                        "suggested_action": "review_manually",
                        "issue_type": "duplicate_column_names",
                        "severity": "high",
                        "message": "Duplicate columns detected",
                        "auto_fixable": False,
                    }
                ]
            }
        )
        
        # Since auto_resolve_pending is True, it should have resolved the manual review
        # with the default/recommended action (which is exclude_column)
        self.assertEqual(len(plan["manual_review"]), 0)
        self.assertEqual(len(plan["resolved_manual_review"]), 1)
        resolved = plan["resolved_manual_review"][0]
        self.assertEqual(resolved["status"], "resolved")
        self.assertEqual(resolved["selected_resolution"], "exclude_column")
        
        # And the exclude_column step should be added to steps
        steps = plan["datasets"]["dbo.Orders_Raw"]["steps"]
        self.assertTrue(any(s["action"] == "exclude_column" for s in steps))

    def test_sql_codegen_quarantine_rejects(self) -> None:
        # Construct a plan with parse_dates and sanitize_email steps
        plan = {
            "plan_id": "test_quarantine_plan",
            "business_rules": {
                "never_drop_rows": False,
                "non_nullable": ["Email"],
            },
            "datasets": {
                "dbo.Orders_Raw": {
                    "steps": [
                        {
                            "order": 1,
                            "column": "OrderDate",
                            "action": "parse_dates",
                        },
                        {
                            "order": 2,
                            "column": "Email",
                            "action": "sanitize_email",
                        }
                    ]
                }
            }
        }
        
        assessment = {
            "datasets": {
                "dbo.Orders_Raw": {
                    "columns": {
                        "OrderID": {"dtype": "int", "candidate_primary_key": True},
                        "OrderDate": {"dtype": "varchar"},
                        "Email": {"dtype": "varchar"},
                    }
                }
            }
        }
        
        # Dialect: tsql, never_drop: False
        sql_with_drop = generate_sql_etl(plan, assessment, dialect="tsql")
        
        self.assertIn("dbo.etl_rejects", sql_with_drop)
        self.assertIn("Quarantine invalid dates", sql_with_drop)
        self.assertIn("Quarantine invalid emails", sql_with_drop)
        self.assertIn("DELETE FROM [dbo].[Orders_Clean]", sql_with_drop)
        self.assertIn("FOR JSON PATH", sql_with_drop)
        
        # Dialect: tsql, never_drop: True
        plan_never_drop = {
            "plan_id": "test_quarantine_plan_nd",
            "business_rules": {
                "never_drop_rows": True,
                "non_nullable": ["Email"],
            },
            "datasets": {
                "dbo.Orders_Raw": {
                    "steps": [
                        {
                            "order": 1,
                            "column": "OrderDate",
                            "action": "parse_dates",
                        },
                        {
                            "order": 2,
                            "column": "Email",
                            "action": "sanitize_email",
                        }
                    ]
                }
            }
        }
        sql_no_drop = generate_sql_etl(plan_never_drop, assessment, dialect="tsql")
        self.assertNotIn("Quarantine invalid dates", sql_no_drop)
        self.assertNotIn("Quarantine invalid emails", sql_no_drop)
        self.assertNotIn("DELETE FROM dbo.Orders_Clean", sql_no_drop)

    def test_sql_codegen_exclude_column_action(self) -> None:
        plan = {
            "plan_id": "test_exclude_column_plan",
            "business_rules": {
                "never_drop_rows": False,
            },
            "datasets": {
                "dbo.Orders_Raw": {
                    "steps": [
                        {
                            "order": 1,
                            "column": "CollidingCol",
                            "action": "exclude_column",
                        }
                    ]
                }
            }
        }
        
        assessment = {
            "datasets": {
                "dbo.Orders_Raw": {
                    "columns": {
                        "OrderID": {"dtype": "int", "candidate_primary_key": True},
                        "CollidingCol": {"dtype": "varchar"},
                        "GoodCol": {"dtype": "varchar"},
                    }
                }
            }
        }
        
        sql = generate_sql_etl(plan, assessment, dialect="tsql")
        # CollidingCol should be skipped in insert and comment should mark it excluded
        self.assertIn("GoodCol", sql)
        self.assertNotIn("[CollidingCol]", sql)
        self.assertIn("skipped via exclude_column transform step", sql)


if __name__ == "__main__":
    unittest.main()
