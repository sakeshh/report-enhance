"""
Structured resolution options for manual_review plan items.
Each option maps to a codegen action (or noop for keep-as-is).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

ResolutionOption = Dict[str, Any]

_SKIP_ACTIONS = frozenset({"noop", "keep_as_is"})


def _opt(
    opt_id: str,
    label: str,
    action: str,
    *,
    recommended: bool = False,
    description: str = "",
) -> ResolutionOption:
    return {
        "id": opt_id,
        "label": label,
        "action": action,
        "recommended": recommended,
        "description": description,
    }


_DEFAULT_OPTIONS: List[ResolutionOption] = [
    _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop", description="No transform; document in runbook."),
]

_CATALOG: Dict[str, List[ResolutionOption]] = {
    "very_high_cardinality": [
        _opt("hash_sha256", "Hash (SHA-256)", "hash_phone", recommended=True, description="One-way hash for PII-like identifiers."),
        _opt("mask_last4", "Mask (last 4 digits)", "mask_phone", description="Show only last four digits."),
        _opt("exclude_column", "Exclude from output", "exclude_column", description="Drop column before write."),
        _opt("keep_as_is", "Keep raw (accept risk)", "noop"),
    ],
    "future_dates": [
        _opt("nullify_future", "Nullify future dates", "nullify_future_dates", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "date_range_violation": [
        _opt("nullify_out_of_range", "Nullify out-of-range dates", "nullify_future_dates", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "constant_column": [
        _opt("drop_column", "Drop column", "drop_column", recommended=True),
        _opt("keep_as_is", "Keep column", "noop"),
    ],
    "potential_primary_key": [
        _opt("deduplicate", "Deduplicate on column", "deduplicate", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "duplicate_column_names": [
        _opt("keep_as_is", "Rename in source (manual)", "noop", recommended=True),
    ],
    "case_insensitive_column_collision": [
        _opt("keep_as_is", "Standardize names in source (manual)", "noop", recommended=True),
    ],
    "very_wide_table": [
        _opt("keep_as_is", "Review with stakeholders (skip ETL)", "noop", recommended=True),
    ],
    "column_name_whitespace": [
        _opt(
            "keep_as_is",
            "Rename columns in source (manual)",
            "noop",
            recommended=True,
            description="Whitespace in column names must be fixed at ingest/schema mapping.",
        ),
    ],
    "dominant_value_skew": [
        _opt("flag_outliers", "Flag for audit column", "flag_outliers", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "skewed_distribution": [
        _opt("flag_outliers", "Flag extreme values", "flag_outliers", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "empty_dataset": [
        _opt("keep_as_is", "Abort pipeline in orchestration (manual)", "noop", recommended=True),
    ],
    "very_wide_date_span": [
        _opt("parse_dates", "Parse dates consistently", "parse_dates", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "non_nullable_fill": [
        _opt("fill_nulls", "Fill nulls (median/mean)", "fill_nulls_simple", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
}


def manual_review_item_id(dataset: Optional[str], column: Optional[str], issue_type: Optional[str]) -> str:
    ds = (dataset or "_global").strip()
    col = (column or "*").strip()
    it = (issue_type or "unknown").strip()
    return f"{ds}|{col}|{it}"


def get_resolution_options(issue_type: Optional[str]) -> List[ResolutionOption]:
    it = (issue_type or "").strip().lower()
    opts = list(_CATALOG.get(it) or _DEFAULT_OPTIONS)
    if not any(o.get("recommended") for o in opts):
        opts[0] = {**opts[0], "recommended": True}
    return opts


def enrich_manual_review_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Attach id, resolution_options, default_resolution, status."""
    out = dict(item)
    iid = out.get("id") or manual_review_item_id(
        out.get("dataset"), out.get("column"), out.get("issue_type")
    )
    out["id"] = iid
    opts = get_resolution_options(str(out.get("issue_type") or ""))
    out["resolution_options"] = opts
    default = next((o["id"] for o in opts if o.get("recommended")), opts[0]["id"] if opts else "keep_as_is")
    out.setdefault("default_resolution", default)
    out.setdefault("status", "pending")
    out.setdefault("selected_resolution", None)
    return out


def action_for_resolution(issue_type: str, resolution_id: str) -> Optional[str]:
    for o in get_resolution_options(issue_type):
        if o.get("id") == resolution_id:
            return str(o.get("action") or "")
    return None


def is_skip_action(action: str) -> bool:
    return (action or "").strip().lower() in _SKIP_ACTIONS
