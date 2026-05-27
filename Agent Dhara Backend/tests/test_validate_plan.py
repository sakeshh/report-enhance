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


def test_validate_many_transforms_with_duplicates():
    # 3 unique actions (trim, lowercase, sanitize_email) + deduplicate. This should pass.
    plan_ok = {
        "datasets": {
            "customers": {
                "steps": [
                    {"order": 1, "column": "email", "action": "trim"},
                    {"order": 2, "column": "email", "action": "lowercase"},
                    {"order": 3, "column": "email", "action": "lowercase"},
                    {"order": 4, "column": "email", "action": "sanitize_email"},
                    {"order": 5, "column": "email", "action": "deduplicate"},
                ]
            }
        },
        "blocked": [],
    }
    ok, errs = validate_etl_plan(plan_ok, _assess(), {})
    assert ok, f"Expected validation to pass but got errors: {errs}"

    # 4 unique actions (trim, lowercase, sanitize_email, fill_or_drop) + deduplicate. This should fail.
    plan_fail = {
        "datasets": {
            "customers": {
                "steps": [
                    {"order": 1, "column": "email", "action": "trim"},
                    {"order": 2, "column": "email", "action": "lowercase"},
                    {"order": 3, "column": "email", "action": "sanitize_email"},
                    {"order": 4, "column": "email", "action": "fill_or_drop"},
                    {"order": 5, "column": "email", "action": "deduplicate"},
                ]
            }
        },
        "blocked": [],
    }
    ok, errs = validate_etl_plan(plan_fail, _assess(), {})
    assert not ok
    assert any("has many transforms" in e for e in errs)

