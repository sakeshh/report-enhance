import unittest
from agent.etl_pipeline.sql_codegen import generate_sql_etl
from agent.etl_pipeline.planner import build_etl_plan

class TestDesignPolish(unittest.TestCase):
    def test_deterministic_reject_logging(self) -> None:
        plan = {
            "plan_id": "test_polish_plan",
            "business_rules": {
                "never_drop_rows": False,
                "non_nullable": ["Email"]
            },
            "datasets": {
                "dbo.Customers_Raw": {
                    "steps": [
                        {
                            "order": 1,
                            "column": "Email",
                            "action": "sanitize_email",
                        }
                    ]
                }
            }
        }
        
        assessment = {
            "datasets": {
                "dbo.Customers_Raw": {
                    "columns": {
                        "CustomerID": {"dtype": "int", "candidate_primary_key": True},
                        "Email": {"dtype": "varchar"},
                    }
                }
            }
        }
        
        sql = generate_sql_etl(plan, assessment, dialect="tsql")
        
        # Test 1: The old non-deterministic lookup (SELECT TOP 1 * FROM Customers_Raw_Staging r2 WHERE r2.[CustomerID] = r.[CustomerID]) must NOT be in the SQL
        self.assertNotIn("TOP 1 * FROM #Customers_Raw_Staging r2 WHERE r2.[CustomerID] = r.[CustomerID]", sql)
        self.assertNotIn("r2.[CustomerID] = r.[CustomerID]", sql)
        
        # Test 2: The exact deterministic logging (SELECT r.* FOR JSON PATH...) must be used instead
        self.assertIn("(SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)", sql)

    def test_redundant_string_wrapper_prevention(self) -> None:
        from agent.etl_pipeline.sql_codegen import compile_column_expression
        
        col_meta = {"dtype": "varchar"}
        # If we have trim and lowercase on the same column
        transforms = [
            {"action": "trim"},
            {"action": "trim"},
            {"action": "lowercase"},
            {"action": "lowercase"},
        ]
        
        expr = compile_column_expression("Name", transforms, col_meta, {})
        # Should apply LTRIM(RTRIM(Name)) and LOWER(LTRIM(RTRIM(Name))) but NOT double wrap them!
        
        # Count LTRIM and RTRIM occurrences: should be exactly 1 each
        self.assertEqual(expr.count("LTRIM"), 1)
        self.assertEqual(expr.count("RTRIM"), 1)
        self.assertEqual(expr.count("LOWER"), 1)
        
        # Let's print for manual check in test logs
        print(f"Compiled Expression: {expr}")

    def test_semantic_schema_persisting(self) -> None:
        assessment = {
            "datasets": {
                "dbo.Orders_Raw": {
                    "columns": {
                        "OrderID": {"dtype": "int", "candidate_primary_key": True, "semantic_type": "identifier"},
                        "customer_email": {"dtype": "object", "semantic_type": "email"},
                    }
                }
            }
        }
        
        plan = build_etl_plan(
            assessment=assessment,
            business_rules_raw={"required_columns": ["OrderID"]}
        )
        
        # Test 3: The plan must persist the semantic_schema
        self.assertIn("semantic_schema", plan)
        self.assertEqual(plan["semantic_schema"].get("dbo.Orders_Raw.OrderID"), "identifier")
        self.assertEqual(plan["semantic_schema"].get("dbo.Orders_Raw.customer_email"), "email")

if __name__ == "__main__":
    unittest.main()
