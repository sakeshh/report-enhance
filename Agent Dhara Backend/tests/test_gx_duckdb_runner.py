from __future__ import annotations

import os
import tempfile
import uuid
from typing import Callable, List

import pytest


def _duckdb_file_with_view(view_sql: str) -> tuple[object, str, str, List[Callable[[], None]]]:
    """Return (native_con, db_path, view_name, on_close_callbacks) for a temp on-disk DuckDB DB."""
    import duckdb

    fd, db_path = tempfile.mkstemp(prefix="dhara_test_", suffix=".duckdb")
    os.close(fd)
    try:
        os.unlink(db_path)
    except OSError:
        pass
    view_name = f"vgx_{uuid.uuid4().hex[:8]}"
    con = duckdb.connect(db_path)
    con.execute(f"CREATE VIEW {view_name} AS {view_sql}")

    def _rm_db() -> None:
        try:
            if os.path.isfile(db_path):
                os.unlink(db_path)
        except OSError:
            pass

    return con, db_path, view_name, [_rm_db]


def test_run_gx_validation_duckdb_not_null_passes() -> None:
    pytest.importorskip("duckdb")
    pytest.importorskip("great_expectations")
    pytest.importorskip("sqlalchemy")

    from agent.gx_runner import run_gx_validation
    from connectors.source_connector import SourceConnection, close_source

    con, db_path, view_name, on_close = _duckdb_file_with_view("SELECT 42 AS x")
    sc = SourceConnection(
        con=con,
        view_name=view_name,
        source_type="csv",
        source_path="az://stub/container.csv",
        dataset_name="ds_unit",
        duckdb_database_path=db_path,
        _tmp_paths=[],
        _on_close=on_close,
    )
    try:
        suite = {
            "expectation_suite_name": "unit_suite",
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "x"},
                    "meta": {"auto_generated": True, "source": "test"},
                }
            ],
        }
        result = run_gx_validation(sc, suite, "ds_unit")
        assert result["gx_passed"] is True
        assert result["failed_expectations"] == 0
        assert result["passed_expectations"] >= 1
        assert result["gx_meta"]["engine"] == "duckdb_sqlalchemy"
        assert not result.get("failures")
    finally:
        close_source(sc)


def test_run_gx_validation_duckdb_not_null_fails_on_null() -> None:
    pytest.importorskip("duckdb")
    pytest.importorskip("great_expectations")
    pytest.importorskip("sqlalchemy")

    from agent.gx_runner import run_gx_validation
    from connectors.source_connector import SourceConnection, close_source

    con, db_path, view_name, on_close = _duckdb_file_with_view("SELECT CAST(NULL AS INTEGER) AS x")
    sc = SourceConnection(
        con=con,
        view_name=view_name,
        source_type="json",
        source_path="az://stub/blob.json",
        dataset_name="ds_null",
        duckdb_database_path=db_path,
        _tmp_paths=[],
        _on_close=on_close,
    )
    try:
        suite = {
            "expectation_suite_name": "unit_suite_null",
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "x"},
                    "meta": {},
                }
            ],
        }
        result = run_gx_validation(sc, suite, "ds_null")
        assert result["gx_passed"] is False
        assert result["failed_expectations"] >= 1
        assert result["failures"]
        assert result["failures"][0].get("expectation_type") == "expect_column_values_to_not_be_null"
    finally:
        close_source(sc)


def test_build_gx_suite_high_business_importance_adds_not_null() -> None:
    from agent.gx_suite_builder import build_gx_suite

    assessment = {
        "datasets": {
            "orders": {
                "columns": {
                    "email": {
                        "null_percentage": 0.0,
                        "business_importance": "high",
                        "semantic_type": "email",
                    }
                }
            }
        }
    }
    suite = build_gx_suite("orders", assessment, strictness="standard")
    types = {e["expectation_type"] for e in suite["expectations"]}
    assert "expect_column_values_to_not_be_null" in types
    assert "expect_column_values_to_match_regex" in types


def test_map_gx_failures_to_issues_shape() -> None:
    from agent.gx_issue_mapper import map_gx_failures_to_issues

    gx_result = {
        "dataset": "orders",
        "gx_passed": False,
        "failures": [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "column": "email",
                "unexpected_count": 3,
                "unexpected_percent": 1.5,
                "partial_unexpected_list": [None],
            }
        ],
    }
    issues = map_gx_failures_to_issues(gx_result)
    assert "datasets" in issues
    block = issues["datasets"]["orders"]
    assert len(block["issues"]) == 1
    it = block["issues"][0]
    assert it["type"] == "gx_null_violation"
    assert it["column"] == "email"
    assert it["severity"] == "high"
    assert it["source"] == "great_expectations"
    assert it["expectation_type"] == "expect_column_values_to_not_be_null"


def test_merge_gx_issues_sets_gx_confirmed_when_profiler_null_issue_exists() -> None:
    from agent.gx_issue_mapper import map_gx_failures_to_issues, merge_gx_issues_into_assessment

    assessment = {
        "datasets": {"orders": {"row_count": 100, "columns": {}}},
        "data_quality_issues": {
            "datasets": {
                "orders": {
                    "issues": [
                        {
                            "type": "high_null_rate",
                            "column": "email",
                            "severity": "high",
                            "message": "profiler says nulls",
                        }
                    ]
                }
            },
            "global_issues": {},
        },
    }
    gx_issues = map_gx_failures_to_issues(
        {
            "dataset": "orders",
            "gx_passed": False,
            "failures": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "column": "email",
                    "unexpected_count": 2,
                    "unexpected_percent": 2.0,
                    "partial_unexpected_list": [],
                }
            ],
        }
    )
    merged = merge_gx_issues_into_assessment(assessment, gx_issues)
    issues_out = merged["data_quality_issues"]["datasets"]["orders"]["issues"]
    assert len(issues_out) == 1
    prof = issues_out[0]
    assert prof.get("gx_confirmed") is True
    assert prof["type"] == "high_null_rate"
