"""
Classify plan steps and manifest issues into AUTO / REVIEW / BLOCKED buckets.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

_AUTO_ACTIONS = frozenset(
    {
        "trim",
        "lowercase",
        "uppercase",
        "fill_nulls_simple",
        "fill_or_drop",
        "cast_type",
        "coerce_numeric",
        "parse_dates",
        "sanitize_email",
        "normalize_phone",
        "regex_replace",
        "range_clip",
        "clip_or_flag",
        "flag_outliers",
        "clip_outliers",
        "cap_outliers",
        "standardize_boolean",
        "replace_values",
        "zero_to_null",
        "deduplicate",
        "join_datasets",
        "hash_phone",
        "mask_phone",
        "drop_column",
        "exclude_column",
        "nullify_future_dates",
        "noop",
    }
)

_REVIEW_ACTIONS = frozenset({"review_manually", "validate_referential_integrity_or_stage"})


def classify_step_bucket(
    action: str,
    *,
    severity: str = "medium",
    null_percentage: Optional[float] = None,
    never_drop_rows: bool = False,
) -> str:
    """
    Returns: auto | review | blocked
    """
    act = (action or "").strip().lower()
    sev = (severity or "medium").lower()

    if act == "drop_rows" or (act == "fill_or_drop" and never_drop_rows is False and null_percentage is not None and null_percentage > 30):
        if never_drop_rows and act == "fill_or_drop":
            return "auto"
        if act == "drop_rows":
            return "blocked" if never_drop_rows else "review"

    if act in _REVIEW_ACTIONS or sev == "high":
        return "review"

    if act in _AUTO_ACTIONS:
        if act == "fill_or_drop" and null_percentage is not None and null_percentage > 30:
            return "review"
        return "auto"

    return "review"


def tag_plan_step_buckets(plan: Dict[str, Any], business_rules: Dict[str, Any]) -> Dict[str, Any]:
    """Add bucket field to every step in plan.datasets."""
    never_drop = bool(business_rules.get("never_drop_rows"))
    for _ds, block in (plan.get("datasets") or {}).items():
        if not isinstance(block, dict):
            continue
        for st in block.get("steps") or []:
            if not isinstance(st, dict):
                continue
            sev = str(st.get("severity") or "medium")
            st["bucket"] = classify_step_bucket(
                str(st.get("action") or ""),
                severity=sev,
                null_percentage=st.get("null_percentage"),
                never_drop_rows=never_drop,
            )
    return plan
