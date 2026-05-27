"""
Validate ETL plan JSON before confirm / codegen.
"""
from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple


def _columns_for_dataset(assessment: Dict[str, Any], dataset: str) -> Set[str]:
    ds = (assessment.get("datasets") or {}).get(dataset) or {}
    cols = ds.get("columns") or {}
    return {str(c) for c in cols.keys()}


def validate_etl_plan(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    business_rules: Dict[str, Any],
) -> Tuple[bool, List[str]]:
    """
    Returns (ok, errors). ok=False means plan must not proceed to confirm without fixes.
    """
    errs: List[str] = []

    if not isinstance(plan, dict):
        return False, ["plan must be an object"]

    blocked = plan.get("blocked") or []
    if blocked:
        for b in blocked[:5]:
            if isinstance(b, dict):
                errs.append(b.get("message") or str(b))
            else:
                errs.append(str(b))

    datasets = plan.get("datasets") or {}
    if not datasets and not (plan.get("global_steps") or []):
        errs.append("plan has no dataset steps and no global steps")

    manifest = plan.get("connector_manifest") or {}
    if manifest:
        from agent.etl_pipeline.connector_manifest import validate_connector_manifest

        errs.extend(validate_connector_manifest(plan, manifest))

    never_drop = bool(business_rules.get("never_drop_rows"))
    known_ds = set((assessment.get("datasets") or {}).keys())
    manual_pending = [
        m
        for m in (plan.get("manual_review") or [])
        if isinstance(m, dict) and str(m.get("status") or "pending") == "pending"
    ]

    for ds_name, block in datasets.items():
        if ds_name not in known_ds:
            errs.append(f"dataset '{ds_name}' not found in assessment")
            continue
        cols = _columns_for_dataset(assessment, ds_name)
        steps = (block or {}).get("steps") or []
        pending_for_ds = any(
            str(m.get("dataset") or "") == ds_name for m in manual_pending
        )
        # Zero steps is acceptable if the dataset requires no transformations
        pass
        seen_cols: Dict[str, List[str]] = {}
        for st in steps:
            if not isinstance(st, dict):
                continue
            action = str(st.get("action") or "")
            col = st.get("column")
            if action == "drop_rows" and never_drop:
                errs.append(f"never_drop_rows: drop_rows on {ds_name}.{col} is not allowed")
            if col and str(col) not in cols and str(col) not in ("*", "[Row-level]"):
                sub_cols = [c.strip() for c in str(col).split(",") if c.strip()]
                if not (sub_cols and all(sc in cols for sc in sub_cols)):
                    errs.append(f"column '{col}' not in assessment schema for dataset '{ds_name}'")
            if col:
                seen_cols.setdefault(str(col), []).append(action)

        for col, actions in seen_cols.items():
            if "deduplicate" in actions and len(actions) > 1:
                non_dedupe = list(set([a for a in actions if a != "deduplicate"]))
                if len(non_dedupe) > 3:
                    errs.append(
                        f"dataset '{ds_name}' column '{col}' has many transforms — review ordering"
                    )

    req = business_rules.get("required_columns") or []
    pending_missing_cols = {
        str(m.get("column")).lower()
        for m in (plan.get("manual_review") or [])
        if isinstance(m, dict)
        and str(m.get("status") or "pending").lower() == "pending"
        and str(m.get("issue_type") or "").lower() == "missing_required_column"
    }
    for rc in req:
        if str(rc).lower() in pending_missing_cols:
            continue
        found = False
        for ds_name in known_ds:
            cols_lower = {c.lower() for c in _columns_for_dataset(assessment, ds_name)}
            if str(rc).lower() in cols_lower:
                found = True
                break
        if not found:
            errs.append(f"required column '{rc}' missing from all assessed datasets")

    return (len(errs) == 0), errs


def validate_etl_plan_for_confirm(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    business_rules: Dict[str, Any],
) -> Tuple[bool, List[str]]:
    """Like validate_etl_plan but also requires all manual_review items resolved."""
    ok, errs = validate_etl_plan(plan, assessment, business_rules)
    manual_pending = [
        m
        for m in (plan.get("manual_review") or [])
        if isinstance(m, dict) and str(m.get("status") or "pending") == "pending"
    ]
    if manual_pending:
        errs.append(
            f"{len(manual_pending)} manual review item(s) still need a resolution before confirm"
        )
        ok = False
    return ok, errs
