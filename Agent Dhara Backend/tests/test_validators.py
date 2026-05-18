from agent.etl_pipeline.validate_python import validate_etl_python_source, validate_python_source
from agent.etl_pipeline.validate_sql import validate_sql_basic
from agent.etl_pipeline.validate_adf import validate_adf_bundle, validate_adf_json


def test_python_valid_passes():
    ok, errs = validate_python_source("import pandas as pd\ndf = pd.DataFrame()")
    assert ok and not errs


def test_python_bare_import_os_blocked():
    ok, errs = validate_python_source("import os\nos.remove('file')")
    assert not ok


def test_python_from_os_import_blocked():
    ok, errs = validate_python_source("from os import system\nsystem('rm -rf /')")
    assert not ok


def test_python_from_os_star_blocked():
    ok, errs = validate_python_source("from os import *")
    assert not ok


def test_python_eval_blocked():
    ok, errs = validate_python_source("x = eval('1+1')")
    assert not ok


def test_python_syntax_error():
    ok, errs = validate_python_source("def broken(:\n    pass")
    assert not ok
    assert any("syntax" in e.lower() for e in errs)


def test_python_empty():
    ok, errs = validate_python_source("")
    assert not ok


def test_etl_python_allows_os_path_helper():
    src = """
import pandas as pd
def _resolve_data_path(location: str) -> str:
    import os
    return os.path.join(".", location)
df = pd.DataFrame()
"""
    ok, errs = validate_etl_python_source(src)
    assert ok, errs


def test_sql_valid_passes():
    ok, errs = validate_sql_basic("SELECT id, name FROM customers WHERE id IS NOT NULL")
    assert ok


def test_sql_drop_table_blocked():
    ok, errs = validate_sql_basic("DROP TABLE customers")
    assert not ok


def test_sql_drop_in_comment_allowed():
    ok, errs = validate_sql_basic("-- DROP TABLE customers (disabled)\nSELECT 1")
    assert ok


def test_sql_truncate_blocked():
    ok, errs = validate_sql_basic("TRUNCATE TABLE orders")
    assert not ok


def test_sql_empty():
    ok, errs = validate_sql_basic("")
    assert not ok


def test_adf_valid_passes():
    obj = {
        "name": "MyFlow",
        "properties": {
            "type": "MappingDataFlow",
            "sources": [{"name": "src1", "dataset": {"referenceName": "DS_in"}}],
            "transformations": [
                {
                    "name": "derive1",
                    "type": "derivedColumn",
                    "upstream": ["src1"],
                    "typeProperties": {"columns": [{"name": "id", "expression": "id"}]},
                }
            ],
            "sinks": [{"name": "sink1", "dataset": {"referenceName": "DS_out"}}],
        },
    }
    ok, errs = validate_adf_json(obj)
    assert ok, errs


def test_adf_missing_name():
    obj = {
        "properties": {
            "type": "MappingDataFlow",
            "sources": [],
            "sinks": [],
            "transformations": [],
        }
    }
    ok, errs = validate_adf_json(obj)
    assert not ok


def test_adf_wrong_type():
    obj = {"name": "x", "properties": {"type": "Pipeline"}}
    ok, errs = validate_adf_json(obj)
    assert not ok


def test_adf_missing_flow_keys():
    obj = {
        "name": "x",
        "properties": {"type": "MappingDataFlow", "sources": []},
    }
    ok, errs = validate_adf_json(obj)
    assert not ok


def test_adf_bundle_valid():
    primary = {
        "name": "flow_clean",
        "properties": {
            "type": "MappingDataFlow",
            "sources": [{"name": "s1", "dataset": {"referenceName": "DS_a"}}],
            "transformations": [
                {
                    "name": "derive1",
                    "type": "derivedColumn",
                    "upstream": ["s1"],
                    "typeProperties": {"columns": [{"name": "id", "expression": "id"}]},
                }
            ],
            "sinks": [{"name": "sink1", "dataset": {"referenceName": "DS_a_cleaned"}, "dependsOn": ["derive1"]}],
        },
    }
    joined = {
        "name": "flow_joined",
        "role": "clean_and_joined",
        "properties": {
            "type": "MappingDataFlow",
            "sources": [{"name": "s1", "dataset": {"referenceName": "DS_a"}}],
            "transformations": [
                {
                    "name": "derive1",
                    "type": "derivedColumn",
                    "upstream": ["s1"],
                    "typeProperties": {"columns": [{"name": "id", "expression": "id"}]},
                },
                {"name": "j1", "type": "join", "upstream": ["derive1", "s1"]},
            ],
            "sinks": [{"name": "sink1", "dataset": {"referenceName": "DS_joined"}, "dependsOn": ["j1"]}],
        },
    }
    obj = {
        **primary,
        "bundle": {"plan_id": "p1", "flows": [{**primary, "role": "clean_only"}, joined]},
    }
    ok, errs = validate_adf_bundle(obj)
    assert ok, errs
