"""PySpark I/O validation tests."""
from __future__ import annotations

import unittest

from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
from agent.etl_pipeline.validate_pyspark import validate_pyspark_source


class TestPysparkIoValidation(unittest.TestCase):
    def _plan_with_manifest(self):
        return {
            "plan_id": "test",
            "datasets": {
                "data_1.json": {
                    "steps": [
                        {"order": 1, "column": "name", "action": "lowercase"},
                    ]
                },
                "data_1.xml": {
                    "steps": [
                        {"order": 1, "column": "name", "action": "lowercase"},
                    ]
                },
            },
            "relationships": {
                "joins": [
                    {
                        "parent_dataset": "data_1.json",
                        "child_dataset": "data_1.xml",
                        "parent_key": "id",
                        "child_key": "id",
                        "join_type": "inner",
                    }
                ],
                "load_order": ["data_1.json", "data_1.xml"],
            },
            "connector_manifest": {
                "datasets": {
                    "data_1.json": {
                        "location": "data_1.json",
                        "format": "json",
                        "source_type": "blob_storage",
                        "read_snippet_pyspark": 'spark.read.json(_resolve_data_path("data_1.json"))',
                        "write_snippet_pyspark": 'df.write.mode("overwrite").json(r"cleaned/data_1_json_cleaned.json")',
                        "output_path": "cleaned/data_1_json_cleaned.json",
                    },
                    "data_1.xml": {
                        "location": "data_1.xml",
                        "format": "xml",
                        "source_type": "blob_storage",
                        "read_snippet_pyspark": (
                            'spark.read.format("com.databricks.spark.xml")'
                            '.option("rowTag", "row").load(_resolve_data_path("data_1.xml"))'
                        ),
                        "write_snippet_pyspark": 'df.write.mode("overwrite").parquet(r"cleaned/data_1_xml_cleaned.parquet")',
                        "output_path": "cleaned/data_1_xml_cleaned.parquet",
                    },
                }
            },
            "business_rules": {},
        }

    def test_template_pyspark_uses_resolve_and_xml_reader(self):
        code = generate_pyspark_etl(self._plan_with_manifest(), {})
        self.assertIn("_resolve_data_path", code)
        self.assertIn("com.databricks.spark.xml", code)
        self.assertNotIn('read.csv(r"data_1.xml")', code)
        self.assertIn("_prefix_columns", code)
        ok, errs = validate_pyspark_source(code, self._plan_with_manifest())
        self.assertTrue(ok, errs)

    def test_rejects_inner_join_when_never_drop_rows(self):
        bad = '''
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
df_joined = a.join(b, on="id", how="inner")
'''
        plan = self._plan_with_manifest()
        plan["business_rules"] = {"never_drop_rows": True}
        ok, errs = validate_pyspark_source(bad, plan)
        self.assertFalse(ok)
        self.assertTrue(any("inner join" in e.lower() for e in errs))

    def test_rejects_spark_session_without_import(self):
        bad = '''
from pyspark.sql import functions as F
if __name__ == "__main__":
    spark = SparkSession.builder.appName("x").getOrCreate()
'''
        ok, errs = validate_pyspark_source(bad, self._plan_with_manifest())
        self.assertFalse(ok)
        self.assertTrue(any("SparkSession" in e for e in errs))

    def test_rejects_stub_resolve_path(self):
        bad = '''
def _resolve_data_path(location: str) -> str:
    return f"abfss://{location}"
from pyspark.sql import SparkSession
df = spark.read.json(_resolve_data_path("data_1.json"))
'''
        ok, errs = validate_pyspark_source(bad, self._plan_with_manifest())
        self.assertFalse(ok)
        self.assertTrue(any("Incomplete _resolve_data_path" in e for e in errs))

    def test_rejects_dead_join_variable(self):
        bad = '''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
df_joined = a.join(b, on="id", how="left")
df_json_cleaned.write.json("out.json")
'''
        ok, errs = validate_pyspark_source(bad, self._plan_with_manifest())
        self.assertFalse(ok)
        self.assertTrue(any("Dead join" in e for e in errs))

    def test_template_pipeline_order_load_transform_write(self):
        code = generate_pyspark_etl(self._plan_with_manifest(), {})
        pipeline = code.split("def run_pipeline", 1)[-1]
        load_i = pipeline.find('dfs["data_1.json"] =')
        transform_i = pipeline.find('transform_data_1_json(dfs["data_1.json"])')
        write_i = pipeline.find("Write cleaned")
        self.assertLess(load_i, transform_i, "load before transform in run_pipeline")
        self.assertLess(transform_i, write_i, "transform before write")
        self.assertIn("_require_columns", code)
        self.assertIn("OUTPUT_PATHS", code)
        self.assertIn("approx_count_distinct", code)
        self.assertIn("Joins skipped", code)  # lowercase-only plan skips enrichment join

    def test_rejects_xml_read_as_csv(self):
        bad = '''
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
df = spark.read.csv("data_1.xml")
df.write.csv("cleaned/data_1_xml_cleaned.xml")
'''
        ok, errs = validate_pyspark_source(bad, self._plan_with_manifest())
        self.assertFalse(ok)
        self.assertTrue(any("xml" in e.lower() for e in errs))


if __name__ == "__main__":
    unittest.main()
