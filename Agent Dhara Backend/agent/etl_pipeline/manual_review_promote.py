"""
Promote user-selected manual review resolutions into executable plan steps.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from agent.etl_pipeline.classify_steps import classify_step_bucket
from agent.etl_pipeline.manual_review_catalog import (
    action_for_resolution,
    enrich_manual_review_item,
    is_skip_action,
    manual_review_item_id,
)

_ACTION_PRIORITY: Dict[str, int] = {
    "trim": 5,
    "lowercase": 8,
    "uppercase": 8,
    "fill_or_drop": 20,
    "fill_nulls_simple": 20,
    "zero_to_null": 30,
    "cast_type": 35,
    "coerce_numeric": 40,
    "parse_dates": 45,
    "at_least_one": 46,
    "nullify_future_dates": 48,
    "sanitize_email": 50,
    "normalize_phone": 55,
    "hash_phone": 56,
    "mask_phone": 57,
    "regex_replace": 60,
    "range_clip": 65,
    "clip_or_flag": 65,
    "flag_outliers": 65,
    "clip_outliers": 65,
    "cap_outliers": 65,
    "standardize_boolean": 70,
    "replace_values": 75,
    "drop_column": 85,
    "exclude_column": 86,
    "deduplicate": 200,
    "validate_referential_integrity_or_stage": 300,
    "noop": 999,
}


def _pending_items(plan: Dict[str, Any]) -> List[Dict[str, Any]]:
    manual = plan.get("manual_review") or []
    out: List[Dict[str, Any]] = []
    for m in manual:
        if not isinstance(m, dict):
            continue
        st = str(m.get("status") or "pending").lower()
        if st == "pending":
            out.append(m)
    return out


def count_pending_manual_review(plan: Dict[str, Any]) -> int:
    return len(_pending_items(plan))


def apply_manual_resolutions(
    plan: Dict[str, Any],
    resolutions: List[Dict[str, Any]],
    *,
    business_rules: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Apply user resolution picks. Each resolution: { item_id, resolution_id }.
    Returns (updated_plan, errors).
    """
    rules = business_rules or plan.get("business_rules") or {}
    never_drop = bool(rules.get("never_drop_rows"))
    errs: List[str] = []
    plan = dict(plan)
    manual: List[Dict[str, Any]] = [
        enrich_manual_review_item(m) if isinstance(m, dict) else m
        for m in (plan.get("manual_review") or [])
        if isinstance(m, dict)
    ]
    by_id = {str(m.get("id")): m for m in manual}

    res_map: Dict[str, str] = {}
    for r in resolutions or []:
        if not isinstance(r, dict):
            continue
        iid = str(r.get("item_id") or r.get("id") or "").strip()
        rid = str(r.get("resolution_id") or r.get("resolution") or "").strip()
        if iid and rid:
            res_map[iid] = rid

    datasets: Dict[str, Any] = dict(plan.get("datasets") or {})
    resolved_log: List[Dict[str, Any]] = list(plan.get("resolved_manual_review") or [])

    for iid, rid in res_map.items():
        item = by_id.get(iid)
        if not item:
            errs.append(f"Unknown manual review item: {iid}")
            continue
        if str(item.get("status") or "pending") != "pending":
            errs.append(f"Item already resolved: {iid}")
            continue

        issue_type = str(item.get("issue_type") or "")
        action = action_for_resolution(issue_type, rid, item.get("resolution_options"))
        if not action:
            errs.append(f"No action for resolution '{rid}' on issue '{issue_type}'")
            continue

        item["selected_resolution"] = rid
        item["resolved_action"] = action
        col = item.get("column")

        if action == "skip_requirement":
            req_cols = rules.get("required_columns") or []
            rules["required_columns"] = [rc for rc in req_cols if str(rc).lower() != str(col).lower()]
            plan["business_rules"] = rules
            item["status"] = "resolved"
            resolved_log.append({**item, "promoted": True})
            continue

        if is_skip_action(action):
            item["status"] = "skipped"
            resolved_log.append({**item, "promoted": False})
            continue

        ds = str(item.get("dataset") or "").strip()
        col = item.get("column")
        if not ds or ds == "_global":
            errs.append(f"Cannot promote dataset-less item {iid}")
            continue

        block = datasets.setdefault(ds, {"steps": []})
        steps: List[Dict[str, Any]] = list(block.get("steps") or [])
        pri = _ACTION_PRIORITY.get(action, 80)
        step = {
            "order": len(steps) + 1,
            "column": col,
            "action": action,
            "bucket": classify_step_bucket(
                action,
                severity=str(item.get("severity") or "medium"),
                never_drop_rows=never_drop,
            ),
            "source_issue_type": issue_type,
            "severity": item.get("severity") or "medium",
            "priority": pri,
            "note": f"User-selected resolution: {rid}",
            "evidence": {
                "why_this_action": item.get("guidance") or item.get("message") or "",
                "confidence": 0.95,
                "rule_override": True,
                "user_resolution": rid,
            },
            "message": item.get("message"),
        }
        steps.append(step)
        steps.sort(key=lambda s: (_ACTION_PRIORITY.get(str(s.get("action") or ""), 80), str(s.get("column") or "")))
        for i, st in enumerate(steps, start=1):
            st["order"] = i
        block["steps"] = steps
        datasets[ds] = block

        item["status"] = "resolved"
        resolved_log.append({**item, "promoted": True})

    pending_only = [
        enrich_manual_review_item(m)
        for m in manual
        if str(m.get("status") or "pending") == "pending"
    ]
    plan["manual_review"] = pending_only
    prev_resolved = [x for x in (plan.get("resolved_manual_review") or []) if isinstance(x, dict)]
    plan["resolved_manual_review"] = resolved_log + prev_resolved
    plan["datasets"] = datasets
    return plan, errs


def enrich_plan_manual_review(plan: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure every manual_review entry has catalog options."""
    manual = plan.get("manual_review") or []
    enriched = [enrich_manual_review_item(m) for m in manual if isinstance(m, dict)]
    plan = dict(plan)
    plan["manual_review"] = enriched
    return plan
