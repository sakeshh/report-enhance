"""
Build explicit step['params'] dicts from assessment stats, evidence, and business rules.
All engines read params — evidence remains advisory for UI only.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


def _is_numeric_dtype(dtype: str, col_stats: Dict[str, Any]) -> bool:
    d = (dtype or "").lower()
    if d in ("int", "integer", "float", "double", "decimal", "numeric", "number"):
        return True
    inf = str(col_stats.get("dtype_inference") or "").lower()
    return inf in ("numeric", "integer", "float")


def _is_text_dtype(dtype: str, col_stats: Dict[str, Any]) -> bool:
    d = (dtype or "").lower()
    if d in ("str", "string", "text", "object", "varchar"):
        return True
    inf = str(col_stats.get("dtype_inference") or "").lower()
    return inf in ("string", "text", "categorical")


def build_step_params(
    action: str,
    *,
    column: Optional[str],
    col_stats: Dict[str, Any],
    evidence: Dict[str, Any],
    rules: Dict[str, Any],
    issue_type: str = "",
) -> Dict[str, Any]:
    """Return normalized params dict for a plan step."""
    act = (action or "").lower()
    params: Dict[str, Any] = {
        "execution_mode": "in_place",
    }
    dtype = str(col_stats.get("dtype") or col_stats.get("semantic_type") or "")
    strategy = str(rules.get("outlier_strategy") or "flag").lower()

    if act in ("fill_or_drop", "fill_nulls_simple"):
        rec = evidence.get("recommended_fill") if isinstance(evidence, dict) else None
        if rec in ("mean", "median"):
            params["fill_strategy"] = rec
        elif _is_numeric_dtype(dtype, col_stats):
            params["fill_strategy"] = rec or "median"
        elif _is_text_dtype(dtype, col_stats):
            params["fill_strategy"] = "value"
            params["fill_value"] = ""
        else:
            params["fill_strategy"] = "value"
            params["fill_value"] = None
        if evidence.get("median") is not None and params.get("fill_strategy") == "median":
            params["fill_value"] = evidence["median"]
        elif evidence.get("mean") is not None and params.get("fill_strategy") == "mean":
            params["fill_value"] = evidence["mean"]

    elif act in ("flag_outliers", "clip_or_flag", "clip_outliers", "cap_outliers", "range_clip"):
        method = strategy if strategy in ("flag", "clip", "cap") else "flag"
        if act == "clip_outliers":
            method = "clip"
        elif act == "cap_outliers":
            method = "cap"
        elif act in ("flag_outliers", "clip_or_flag"):
            method = "flag"
        params["outlier_method"] = method
        params["outlier_iqr_multiplier"] = float(
            evidence.get("outlier_iqr_multiplier") or 1.5
        )
        if evidence.get("median") is not None:
            params["fill_value"] = evidence["median"]
        if evidence.get("p5") is not None:
            params["p5"] = evidence["p5"]
        if evidence.get("p95") is not None:
            params["p95"] = evidence["p95"]

    elif act in ("hash_phone", "mask_phone"):
        params["privacy"] = "hash" if act == "hash_phone" else "mask"
        params["execution_mode"] = "in_place"

    elif act in ("drop_column", "exclude_column"):
        params["privacy"] = "exclude"
        params["execution_mode"] = "in_place"

    elif act in ("lowercase", "uppercase"):
        params["case_mode"] = act.replace("case", "")

    elif act == "validate_referential_integrity_or_stage":
        params["enforcement_mode"] = "flag"
        params["execution_mode"] = "new_table"

    return params


def build_ri_step_params(rstep: Dict[str, Any], rules: Dict[str, Any]) -> Dict[str, Any]:
    """Params for referential-integrity validation steps from relationship planner."""
    mode = "quarantine" if rules.get("never_drop_rows") else "flag"
    return {
        "related_dataset": rstep.get("related_dataset"),
        "related_column": rstep.get("related_column"),
        "enforcement_mode": mode,
        "execution_mode": "new_table",
    }
