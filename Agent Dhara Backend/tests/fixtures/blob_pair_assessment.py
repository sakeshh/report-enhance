"""Synthetic assessment mirroring data_1.json + data_1.xml blob DQ scenarios."""
from __future__ import annotations

from typing import Any, Dict, List


def _column_meta() -> Dict[str, Any]:
    return {
        "id": {"dtype": "object", "null_percentage": 0, "semantic_type": "numeric_id"},
        "name": {"dtype": "object", "null_percentage": 0, "semantic_type": "categorical"},
        "department": {"dtype": "object", "null_percentage": 0, "semantic_type": "categorical"},
        "phone": {"dtype": "object", "null_percentage": 0, "semantic_type": "phone"},
        "email": {"dtype": "object", "null_percentage": 0, "semantic_type": "email"},
        "age": {"dtype": "object", "null_percentage": 19.0, "semantic_type": "categorical"},
        "salary": {"dtype": "object", "null_percentage": 0, "semantic_type": "numeric"},
    }


def make_blob_pair_assessment(*, overlap_count: int = 15) -> Dict[str, Any]:
    cols = _column_meta()
    dq_json: List[Dict[str, Any]] = [
        {
            "type": "case_inconsistency",
            "column": "name",
            "severity": "medium",
            "message": "Mixed case values in name",
            "count": 40,
        },
        {
            "type": "case_inconsistency",
            "column": "department",
            "severity": "medium",
            "message": "Mixed case values in department",
            "count": 30,
        },
    ]
    dq_xml: List[Dict[str, Any]] = list(dq_json) + [
        {
            "type": "very_high_cardinality",
            "column": "phone",
            "severity": "medium",
            "message": "Phone column has very high cardinality (100 unique / 100 rows)",
            "count": 100,
        },
    ]
    return {
        "datasets": {
            "data_1.json": {"row_count": 100, "columns": dict(cols)},
            "data_1.xml": {"row_count": 100, "columns": dict(cols)},
        },
        "relationships": [
            {
                "dataset_a": "data_1.json",
                "dataset_b": "data_1.xml",
                "column_a": "id",
                "column_b": "id",
                "cardinality": "one_to_one",
                "max_rows_per_key_a": 1,
                "max_rows_per_key_b": 1,
                "overlap_count": overlap_count,
                "summary": f"1:1 relationship; {overlap_count} overlapping keys",
            }
        ],
        "data_quality_issues": {
            "datasets": {
                "data_1.json": {"issues": dq_json},
                "data_1.xml": {"issues": dq_xml},
            },
            "global_issues": {},
        },
    }


def blob_session_context() -> Dict[str, Any]:
    return {"selected_blob_files": ["data_1.json", "data_1.xml"]}
