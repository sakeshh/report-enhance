"""
Rich step metadata for intelligence-first ETL plans.
"""
from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

_SEVERITY_TO_RISK = {"high": "high", "medium": "medium", "low": "low", "critical": "high"}


def _norm_severity(sev: Optional[str]) -> str:
    return _SEVERITY_TO_RISK.get(str(sev or "medium").lower(), "medium")


def _row_impact_for_action(action: str, issue_type: str = "") -> str:
    act = (action or "").lower()
    it = (issue_type or "").lower()
    if act in ("drop_rows", "deduplicate_or_alert"):
        return "subset"
    if act in ("deduplicate",) and it == "duplicate_rows":
        return "subset"
    if act in ("inner_join",):
        return "subset"
    return "none"


def _alternatives_for(action: str, issue_type: str, rules: Dict[str, Any]) -> List[str]:
    act = (action or "").lower()
    it = (issue_type or "").lower()
    never_drop = bool(rules.get("never_drop_rows"))

    if act in ("lowercase",) or it == "case_inconsistency":
        return ["lowercase", "uppercase", "title_case"]
    if act in ("uppercase",):
        return ["uppercase", "lowercase", "title_case"]
    if act in ("hash_phone",):
        return ["hash", "mask", "exclude", "keep"]
    if act in ("mask_phone",):
        return ["mask", "hash", "exclude", "keep"]
    if act in ("exclude_column", "drop_column"):
        return ["exclude", "keep", "hash", "mask"]
    if act in ("fill_or_drop", "fill_nulls_simple"):
        alts = ["fill_nulls_simple", "flag_only"]
        if not never_drop:
            alts.append("drop_rows")
        return alts
    if act in ("clip_or_flag", "flag_outliers"):
        return ["flag_outliers", "clip_outliers", "cap_outliers", "keep"]
    return []


def _requires_user_choice(alternatives: List[str], classification: str) -> bool:
    if classification in ("review", "blocked"):
        return True
    return len(alternatives) > 1


def build_plan_invariants(rules: Dict[str, Any]) -> List[Dict[str, Any]]:
    never_drop = bool(rules.get("never_drop_rows"))
    return [
        {
            "name": "never_drop_rows",
            "enabled": never_drop,
            "check": "row_count_preserved",
            "description": "Output row count must not shrink due to transforms unless user explicitly chose subset impact.",
        },
        {
            "name": "no_silent_column_loss",
            "enabled": True,
            "check": "column_audit",
            "description": "Columns removed only via explicit exclude/drop steps approved by user.",
        },
    ]


def enrich_join_step(
    join: Dict[str, Any],
    *,
    rules: Dict[str, Any],
) -> Dict[str, Any]:
    """Attach intelligence metadata to a relationship join record."""
    how = str(join.get("join_type") or "left").lower()
    never_drop = bool(rules.get("never_drop_rows"))
    if never_drop and how == "inner":
        how = "left"
        join = dict(join)
        join["join_type"] = "left"

    row_impact = "subset" if how == "inner" else "none"
    alts = ["left", "full_outer", "inner", "skip_join"]
    if never_drop and "inner" in alts:
        alts = [a for a in alts if a != "inner"] + ["inner"]

    p = join.get("parent_dataset") or join.get("left_dataset")
    c = join.get("child_dataset") or join.get("right_dataset")
    pk = join.get("parent_key") or join.get("left_key")
    ck = join.get("child_key") or join.get("right_key")
    ev = join.get("evidence") if isinstance(join.get("evidence"), dict) else {}

    return {
        **join,
        "step_id": join.get("step_id") or f"join_{uuid.uuid4().hex[:10]}",
        "action": "join_datasets",
        "target_columns": [pk, ck] if pk and ck else [],
        "target_datasets": [p, c] if p and c else [],
        "reason": f"Join {p} to {c} on {pk}={ck} ({join.get('cardinality', 'unknown')}).",
        "evidence": ev.get("why_this_action") or join.get("message") or "",
        "risk": _norm_severity(join.get("severity") or "medium"),
        "row_impact": row_impact,
        "alternatives": alts,
        "classification": "review",
        "requires_user_choice": True,
        "join_required": True,
        "join_type": how,
        "privacy_action": None,
    }


def enrich_step_record(
    st: Dict[str, Any],
    *,
    assessment: Dict[str, Any],
    rules: Dict[str, Any],
) -> Dict[str, Any]:
    """Produce intelligence-first step dict from planner step entry."""
    action = str(st.get("action") or "")
    col = st.get("column")
    ds = st.get("dataset") or ""
    issue_type = str(st.get("source_issue_type") or st.get("issue_type") or "")
    sev = str(st.get("severity") or "medium")
    bucket = str(st.get("bucket") or st.get("classification") or "auto")
    if bucket == "blocked":
        classification = "blocked"
    elif bucket == "review":
        classification = "review"
    else:
        classification = "auto"

    ev = st.get("evidence") if isinstance(st.get("evidence"), dict) else {}
    message = str(st.get("message") or ev.get("why_this_action") or "")
    alternatives = list(ev.get("alternatives") or []) or _alternatives_for(action, issue_type, rules)

    if action == "lowercase" and issue_type == "case_inconsistency":
        alternatives = ["lowercase", "uppercase", "title_case"]
        classification = "auto"
    if action in ("hash_phone", "mask_phone"):
        alternatives = alternatives or ["hash", "mask", "exclude", "keep"]
        classification = "review"

    row_impact = _row_impact_for_action(action, issue_type)
    if action in ("exclude_column", "drop_column"):
        row_impact = "none"
        classification = "review"

    requires = _requires_user_choice(alternatives, classification)
    if action == "lowercase" and issue_type == "case_inconsistency":
        requires = False

    privacy_action = None
    if action == "hash_phone":
        privacy_action = "hash"
    elif action == "mask_phone":
        privacy_action = "mask"
    elif action == "exclude_column":
        privacy_action = "exclude"

    reason_parts = [message] if message else []
    if not reason_parts and ev.get("why_this_action"):
        reason_parts.append(str(ev["why_this_action"]))
    if issue_type:
        reason_parts.append(f"Linked to assessment finding: {issue_type.replace('_', ' ')}")

    evidence_text = message or str(ev.get("why_this_action") or "")
    params = dict(st.get("params") or {})
    out = dict(st)
    out.update(
        {
            "step_id": st.get("step_id") or f"step_{uuid.uuid4().hex[:10]}",
            "action": action,
            "target_columns": [col] if col else [],
            "target_datasets": [ds] if ds else [],
            "reason": " — ".join(p for p in reason_parts if p) or f"Apply {action} per assessment.",
            "evidence": ev,
            "evidence_text": evidence_text,
            "risk": _norm_severity(sev),
            "row_impact": row_impact,
            "alternatives": alternatives,
            "classification": classification,
            "requires_user_choice": requires,
            "privacy_action": privacy_action,
            "join_required": False,
            "params": params,
            "column": col,
            "dataset": ds,
            "bucket": bucket,
        }
    )
    return out


def enrich_relationship_plan_joins(rel_plan: Dict[str, Any], rules: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(rel_plan)
    joins = [enrich_join_step(j, rules=rules) for j in (rel_plan.get("joins") or [])]
    out["joins"] = joins
    out["join_count"] = len(joins)
    return out


def finalize_dataset_steps(steps: List[Dict[str, Any]], assessment: Dict[str, Any], rules: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for st in steps:
        if not isinstance(st, dict):
            continue
        out.append(enrich_step_record(st, assessment=assessment, rules=rules))
    return out
