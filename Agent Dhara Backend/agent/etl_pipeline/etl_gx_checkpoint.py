"""
Post-ETL Great Expectations checkpoint — expectation suite + static validation from plan.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("agent.etl")


def _expectations_from_plan(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    business_rules: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Build GX-style expectation definitions (metadata-driven, no execution required)."""
    expectations: List[Dict[str, Any]] = []
    ds_names = set((assessment.get("datasets") or {}).keys())

    for req in business_rules.get("required_columns") or []:
        rc = str(req).strip()
        if not rc:
            continue
        found_in = [d for d in ds_names if rc.lower() in {c.lower() for c in _cols(assessment, d)}]
        expectations.append(
            {
                "type": "expect_column_to_exist",
                "column": rc,
                "datasets": found_in,
                "kwargs": {},
                "severity": "critical" if not found_in else "info",
                "passed": bool(found_in),
                "detail": (
                    f"Required column '{rc}' found in: {', '.join(found_in) or 'none'}"
                ),
            }
        )

    for nn in business_rules.get("non_nullable") or []:
        expectations.append(
            {
                "type": "expect_column_values_to_not_be_null",
                "column": nn,
                "kwargs": {"mostly": 1.0},
                "severity": "high",
                "passed": None,
                "detail": f"After ETL, column '{nn}' should have no nulls",
            }
        )

    for col, allowed in (business_rules.get("valid_values") or {}).items():
        if isinstance(allowed, list) and allowed:
            expectations.append(
                {
                    "type": "expect_column_values_to_be_in_set",
                    "column": col,
                    "kwargs": {"value_set": allowed},
                    "severity": "medium",
                    "passed": None,
                    "detail": f"Column '{col}' should be in {allowed}",
                }
            )

    for ds_name, block in (plan.get("datasets") or {}).items():
        for st in (block or {}).get("steps") or []:
            action = str(st.get("action") or "")
            col = st.get("column")
            if not col:
                continue
            if action == "sanitize_email":
                expectations.append(
                    {
                        "type": "expect_column_values_to_match_regex",
                        "column": col,
                        "dataset": ds_name,
                        "kwargs": {"regex": r"^[^@]+@[^@]+\.[^@]+$"},
                        "severity": "medium",
                        "passed": None,
                        "detail": f"Post-ETL emails in {ds_name}.{col} should match basic format",
                    }
                )
            elif action in ("coerce_numeric", "cast_type"):
                expectations.append(
                    {
                        "type": "expect_column_values_to_be_of_type",
                        "column": col,
                        "dataset": ds_name,
                        "kwargs": {"type_": "numeric"},
                        "severity": "medium",
                        "passed": None,
                        "detail": f"Post-ETL {ds_name}.{col} should be numeric",
                    }
                )

    return expectations


def _cols(assessment: Dict[str, Any], dataset: str) -> List[str]:
    ds = (assessment.get("datasets") or {}).get(dataset) or {}
    return list((ds.get("columns") or {}).keys())


def _static_checks(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    business_rules: Dict[str, Any],
) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []
    manual = plan.get("manual_review") or []
    for item in manual:
        checks.append(
            {
                "check": "manual_review_pending",
                "severity": "warning",
                "passed": False,
                "detail": item.get("message") or item.get("guidance") or str(item),
                "dataset": item.get("dataset"),
                "column": item.get("column"),
            }
        )

    blocked = plan.get("blocked") or []
    for b in blocked:
        checks.append(
            {
                "check": "plan_blocked",
                "severity": "critical",
                "passed": False,
                "detail": b.get("message") if isinstance(b, dict) else str(b),
            }
        )

    if business_rules.get("never_drop_rows"):
        for ds_name, block in (plan.get("datasets") or {}).items():
            for st in (block or {}).get("steps") or []:
                if str(st.get("action") or "") == "drop_rows":
                    checks.append(
                        {
                            "check": "never_drop_rows_violation",
                            "severity": "critical",
                            "passed": False,
                            "detail": f"drop_rows on {ds_name} violates never_drop_rows",
                        }
                    )

    if not checks and not manual:
        checks.append(
            {
                "check": "plan_static_review",
                "severity": "info",
                "passed": True,
                "detail": "No blocking static issues detected from plan metadata",
            }
        )

    return checks


def run_etl_gx_checkpoint(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    business_rules: Optional[Dict[str, Any]] = None,
    lineage: Optional[Dict[str, Any]] = None,
    *,
    run_gx_if_available: bool = False,
) -> Dict[str, Any]:
    """
    Build post-ETL validation checkpoint report.
    Optionally attempts lightweight GX if run_gx_if_available and GX installed (requires DataFrames).
    """
    rules = business_rules or plan.get("business_rules") or {}
    expectations = _expectations_from_plan(plan, assessment, rules)
    static = _static_checks(plan, assessment, rules)

    passed_static = all(c.get("passed") is not False for c in static if c.get("severity") == "critical")
    pre_passed = sum(1 for e in expectations if e.get("passed") is True)
    total_pre = sum(1 for e in expectations if e.get("passed") is not None)

    gx_runtime: Optional[Dict[str, Any]] = None
    if run_gx_if_available:
        try:
            import pandas as pd  # noqa: F401
            import great_expectations as gx  # noqa: F401

            gx_runtime = {
                "available": True,
                "message": (
                    "Great Expectations is installed. Run your generated ETL script on source data, "
                    "then validate with gx_enabled assessment or export this expectation suite."
                ),
            }
        except ImportError:
            gx_runtime = {"available": False, "message": "great_expectations not installed"}

    critical_failures = [c for c in static if c.get("passed") is False and c.get("severity") == "critical"]
    overall_ok = passed_static and not critical_failures

    return {
        "ok": overall_ok,
        "checkpoint_id": f"gx_cp_{plan.get('plan_id', 'unknown')}",
        "summary": {
            "overall_ok": overall_ok,
            "expectations_defined": len(expectations),
            "expectations_pre_validated": pre_passed,
            "static_checks": len(static),
            "critical_failures": len(critical_failures),
            "manual_review_count": len(plan.get("manual_review") or []),
        },
        "expectations": expectations,
        "static_checks": static,
        "lineage_ref": lineage or {},
        "gx_runtime": gx_runtime,
        "next_steps": [
            "Execute generated ETL code against a staging copy of source data",
            "Re-run assessment with Great Expectations enabled to deep-audit post-transform data",
            "Resolve any manual_review items before production deploy",
        ],
    }
