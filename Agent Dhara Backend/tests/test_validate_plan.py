from agent.etl_pipeline.validate_plan import validate_etl_plan
from agent.etl_pipeline.schema_lineage import build_lineage


def _assess():
    return {
        "datasets": {
            "customers": {
                "columns": {
                    "email": {"dtype": "object", "null_percentage": 5},
                    "age": {"dtype": "object"},
                }
            }
        }
    }


def test_validate_ok_plan():
    plan = {
        "datasets": {
            "customers": {
                "steps": [
                    {"order": 1, "column": "email", "action": "trim"},
                ]
            }
        },
        "blocked": [],
    }
    ok, errs = validate_etl_plan(plan, _assess(), {})
    assert ok and not errs


def test_validate_missing_column():
    plan = {
        "datasets": {
            "customers": {
                "steps": [{"order": 1, "column": "missing_col", "action": "trim"}]
            }
        },
        "blocked": [],
    }
    ok, errs = validate_etl_plan(plan, _assess(), {})
    assert not ok
    assert any("missing_col" in e for e in errs)


def test_lineage_builds():
    plan = {
        "datasets": {
            "customers": {
                "steps": [
                    {"order": 1, "column": "email", "action": "trim"},
                    {"order": 2, "column": "email", "action": "sanitize_email"},
                ]
            }
        }
    }
    lin = build_lineage(plan, _assess())
    assert "customers" in lin
    assert lin["customers"]["email"]["transforms"] == ["trim", "sanitize_email"]
