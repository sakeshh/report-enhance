"""Connector manifest and join emitter codegen tests."""
from __future__ import annotations

import unittest

from agent.etl_pipeline.connector_manifest import (
    build_connector_manifest,
    validate_connector_manifest,
)
from agent.etl_pipeline.join_emitters import (
    emit_adf_join_transformations,
    emit_python_load_and_join,
    emit_sql_joins,
)
from agent.etl_pipeline.python_codegen import generate_python_etl


class TestConnectorManifest(unittest.TestCase):
    def test_manifest_from_local_files(self):
        ctx = {
            "selected_local_files": ["orders.csv", "customers.csv"],
            "local_files_root": "/data/in",
        }
        assess = {
            "datasets": {
                "orders.csv": {"row_count": 100},
                "customers.csv": {"row_count": 50},
            }
        }
        m = build_connector_manifest(ctx, assess)
        self.assertEqual(len(m["datasets"]), 2)
        self.assertIn("read_snippet_python", m["datasets"]["orders.csv"])
        loc = m["datasets"]["orders.csv"]["location"].replace("\\", "/")
        self.assertIn("/data/in/orders.csv", loc)

    def test_manifest_sql_table(self):
        ctx = {
            "selected_source": "azure_sql",
            "selected_tables": ["dbo.Customers"],
        }
        assess = {"datasets": {"dbo.Customers": {"row_count": 500}}}
        m = build_connector_manifest(ctx, assess)
        ent = m["datasets"]["dbo.Customers"]
        self.assertEqual(ent["format"], "sql_table")
        self.assertIn("read_sql", ent["read_snippet_python"].lower())

    def test_validate_manifest_missing_dataset(self):
        plan = {"datasets": {"a.csv": {"steps": []}}}
        manifest = {"datasets": {}}
        errs = validate_connector_manifest(plan, manifest)
        self.assertTrue(any("missing entry" in e for e in errs))


class TestJoinEmitters(unittest.TestCase):
    def _sample_plan(self):
        return {
            "datasets": {
                "customers": {"steps": []},
                "orders": {"steps": []},
            },
            "relationships": {
                "load_order": ["customers", "orders"],
                "joins": [
                    {
                        "parent_dataset": "customers",
                        "child_dataset": "orders",
                        "parent_key": "id",
                        "child_key": "customer_id",
                        "join_type": "left",
                    }
                ],
                "many_to_many": [],
            },
            "connector_manifest": {
                "datasets": {
                    "customers": {
                        "location": "/data/customers.csv",
                        "format": "csv",
                        "read_snippet_python": 'pd.read_csv(r"/data/customers.csv")',
                        "output_path": "cleaned/customers_cleaned.csv",
                    },
                    "orders": {
                        "location": "/data/orders.csv",
                        "format": "csv",
                        "read_snippet_python": 'pd.read_csv(r"/data/orders.csv")',
                        "output_path": "cleaned/orders_cleaned.csv",
                    },
                }
            },
        }

    def test_emit_python_has_load_and_main(self):
        lines = emit_python_load_and_join(self._sample_plan(), self._sample_plan()["connector_manifest"])
        text = "\n".join(lines)
        self.assertIn("def load_all_datasets", text)
        self.assertIn("def run_joins", text)
        self.assertIn("if __name__ == '__main__':", text)
        self.assertIn("load_all_datasets()", text)

    def test_emit_sql_joins(self):
        lines = emit_sql_joins(self._sample_plan(), self._sample_plan()["connector_manifest"])
        text = "\n".join(lines)
        self.assertIn("JOIN", text)
        self.assertIn("customer_id", text)

    def test_adf_join_transformations(self):
        rel = self._sample_plan()["relationships"]
        xf, tid, script = emit_adf_join_transformations([], 0, rel)
        self.assertEqual(len(xf), 1)
        self.assertEqual(xf[0]["type"], "join")
        self.assertIn("source_customers", xf[0]["leftStream"])

    def test_generate_python_includes_manifest_pipeline(self):
        plan = self._sample_plan()
        plan["plan_id"] = "test"
        plan["business_rules"] = {}
        code = generate_python_etl(plan, {})
        self.assertIn("load_all_datasets", code)
        self.assertNotIn("your_input_file.csv", code)

    def test_xml_blob_manifest_format(self):
        ctx = {"selected_blob_files": ["data_1.json", "data_1.xml"]}
        assess = {
            "datasets": {
                "data_1.json": {"row_count": 100},
                "data_1.xml": {"row_count": 100},
            }
        }
        m = build_connector_manifest(ctx, assess)
        xml_ent = m["datasets"]["data_1.xml"]
        self.assertEqual(xml_ent["format"], "xml")
        self.assertIn("read_xml", xml_ent["read_snippet_python"])
        self.assertIn("com.databricks.spark.xml", xml_ent["read_snippet_pyspark"])
        self.assertNotIn("read.csv", xml_ent["read_snippet_pyspark"])
        self.assertTrue(xml_ent["output_path"].endswith(".parquet"))

    def test_mn_bridge_in_python_codegen(self):
        plan = self._sample_plan()
        plan["relationships"]["many_to_many"] = [
            {
                "dataset_a": "customers",
                "dataset_b": "orders",
                "column_a": "id",
                "column_b": "customer_id",
                "bridge_name": "bridge_customers_orders_id",
            }
        ]
        lines = emit_python_load_and_join(plan, plan["connector_manifest"])
        self.assertTrue(any("build_bridge_tables" in ln for ln in lines))


if __name__ == "__main__":
    unittest.main()
