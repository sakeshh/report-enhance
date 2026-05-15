from __future__ import annotations

import hashlib
import json
import time
from typing import Any, Dict, List, Optional, Tuple

from agent.transformation_suggester import suggest_transformations
from agent.etl_pipeline.business_rules import normalize_business_rules, column_is_excluded

# Lower number = earlier in pipeline (per column / global)
_ACTION_PRIORITY: Dict[str, int] = {
    "trim": 5,
    "lowercase": 8,
    "uppercase": 8,
    "fill_or_drop": 20,
    "fill_nulls_simple": 20,
    "coerce_numeric": 40,
    "parse_dates": 45,
    "sanitize_email": 50,
    "normalize_phone": 55,
    "regex_replace": 60,
    "range_clip": 65,
    "clip_or_flag": 65,
    "standardize_boolean": 70,
    "replace_values": 75,
    "deduplicate": 200,
    "validate_referential_integrity_or_stage": 300,
}


def _plan_id() -> str:
    return f"plan_{int(time.time())}"


def _assessment_signature(assessment: Dict[str, Any]) -> str:
    try:
        blob = json.dumps(assessment, sort_keys=True, default=str)[:500_000]
    except Exception:
        blob = str(assessment)
    return hashlib.sha256(blob.encode("utf-8", errors="ignore")).hexdigest()[:16]


def _dataset_columns(assessment: Dict[str, Any], dataset: str) -> Dict[str, Any]:
    ds = (assessment.get("datasets") or {}).get(dataset) or {}
    return ds.get("columns") or {}


def _apply_rules_to_action(
    action: str,
    column: Optional[str],
    business_rules: Dict[str, Any],
) -> Tuple[str, Optional[str]]:
    """
    Returns (action, note) where note is a human-readable override reason.
    """
    if business_rules.get("never_drop_rows") and action == "fill_or_drop":
        return "fill_nulls_simple", "never_drop_rows: using fill-only instead of drop/fill choice"
    return action, None


def build_etl_plan(
    assessment: Dict[str, Any],
    business_rules_raw: Any,
    *,
    engine: str = "python",
) -> Dict[str, Any]:
    """
    Build versioned ETL plan JSON from assessment + normalized business rules.
    """
    if not isinstance(assessment, dict) or not assessment.get("datasets"):
        raise ValueError("Invalid assessment: missing datasets")

    rules = normalize_business_rules(business_rules_raw)
    exclude = set(rules.get("exclude_columns") or [])

    sug_pkg = suggest_transformations(assessment)
    suggestions: List[Dict[str, Any]] = list(sug_pkg.get("suggested_transformations") or [])

    manual_review: List[Dict[str, Any]] = []
    blocked: List[Dict[str, Any]] = []
    # (dataset, column, action) -> step record
    step_map: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    datasets_known = set((assessment.get("datasets") or {}).keys())

    for req_col in rules.get("required_columns") or []:
        rc = str(req_col).strip()
        if not rc:
            continue
        found = False
        for ds_name in datasets_known:
            cols = _dataset_columns(assessment, ds_name)
            for ck in cols.keys():
                if ck.lower() == rc.lower():
                    found = True
                    break
            if found:
                break
        if not found:
            blocked.append(
                {
                    "type": "missing_required_column",
                    "column": rc,
                    "message": f"Required column '{rc}' not found in any assessed dataset.",
                }
            )

    for s in suggestions:
        ds = s.get("dataset") or ""
        col = s.get("column")
        action = str(s.get("suggested_action") or "")
        sev = str(s.get("severity") or "medium").lower()

        if ds and ds != "_global" and column_is_excluded(col, exclude):
            continue

        if action == "review_manually" or not s.get("auto_fixable", False):
            manual_review.append(
                {
                    "dataset": ds or None,
                    "column": col,
                    "issue_type": s.get("issue_type"),
                    "severity": sev,
                    "message": s.get("message"),
                    "guidance": s.get("manual_guidance") or "",
                }
            )
            continue

        action2, override_note = _apply_rules_to_action(action, col, rules)

        if col and rules.get("non_nullable") and col.strip().lower() in (rules.get("non_nullable") or []):
            if action2 in ("fill_or_drop", "fill_nulls_simple") and not rules.get("never_drop_rows"):
                manual_review.append(
                    {
                        "dataset": ds,
                        "column": col,
                        "issue_type": s.get("issue_type"),
                        "severity": "medium",
                        "message": f"Column {col} is non-nullable; review fill/drop behavior manually.",
                        "guidance": override_note or "",
                    }
                )
                continue

        key = (ds or "_global", (col or "*"), action2)
        pri = _ACTION_PRIORITY.get(action2, 80)
        row_est = s.get("row_count_affected")
        entry = {
            "dataset": ds or "_global",
            "column": col,
            "action": action2,
            "source_issue_type": s.get("issue_type"),
            "severity": sev,
            "estimated_affected_rows": row_est,
            "priority": pri,
            "note": override_note,
        }
        prev = step_map.get(key)
        if not prev or (row_est and (prev.get("estimated_affected_rows") or 0) < (row_est or 0)):
            step_map[key] = entry

    # Per-dataset ordered steps
    datasets_out: Dict[str, Any] = {}
    global_steps: List[Dict[str, Any]] = []

    for key, st in step_map.items():
        ds_name, _, _ = key
        if ds_name in ("_global", "", None):
            global_steps.append(
                {
                    "order": st["priority"],
                    "column": st.get("column"),
                    "action": st["action"],
                    "estimated_affected_rows": st.get("estimated_affected_rows"),
                    "note": st.get("note"),
                }
            )
            continue
        datasets_out.setdefault(ds_name, []).append(st)

    for ds_name, steps in datasets_out.items():
        steps.sort(key=lambda x: (x["priority"], str(x.get("column") or "")))
        for i, st in enumerate(steps, start=1):
            st["order"] = i

    global_steps.sort(key=lambda x: x["order"])
    for i, st in enumerate(global_steps, start=1):
        st["order"] = i

    plan = {
        "plan_version": 1,
        "plan_id": _plan_id(),
        "engine": (engine or "python").lower(),
        "created_at": time.time(),
        "assessment_signature": _assessment_signature(assessment),
        "business_rules": rules,
        "datasets": {k: {"steps": v} for k, v in datasets_out.items()},
        "global_steps": global_steps,
        "manual_review": manual_review,
        "blocked": blocked,
        "suggestions_summary": sug_pkg.get("summary") or {},
    }
    return plan
