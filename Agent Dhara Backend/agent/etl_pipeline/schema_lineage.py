"""
Build column-level source → transforms → target lineage from plan + assessment.
"""
from __future__ import annotations

from typing import Any, Dict, List


def _infer_target_dtype(action: str, source_dtype: str) -> str:
    act = (action or "").lower()
    sd = (source_dtype or "unknown").lower()
    if act in ("coerce_numeric", "cast_type", "range_clip", "clip_outliers", "cap_outliers"):
        return "int64" if "int" in act else "float64"
    if act == "parse_dates":
        return "datetime64"
    if act in ("sanitize_email", "trim", "lowercase", "uppercase", "normalize_phone", "regex_replace"):
        return "string"
    if act in ("standardize_boolean",):
        return "Int64"
    if act in ("flag_outliers", "clip_or_flag"):
        return "bool"
    return sd or "unknown"


def build_lineage(plan: Dict[str, Any], assessment: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns { dataset: { column: { source_dtype, transforms[], target_dtype, nullable } } }
    """
    out: Dict[str, Any] = {}

    for ds_name, block in (plan.get("datasets") or {}).items():
        ds_meta = (assessment.get("datasets") or {}).get(ds_name) or {}
        col_meta = ds_meta.get("columns") or {}
        ds_lineage: Dict[str, Any] = {}

        steps = sorted(
            (block or {}).get("steps") or [],
            key=lambda x: int(x.get("order") or 0),
        )
        for st in steps:
            col = st.get("column")
            if not col:
                continue
            cname = str(col)
            meta = col_meta.get(cname) or {}
            if cname not in ds_lineage:
                src = meta.get("dtype") or meta.get("inferred_type") or "unknown"
                ds_lineage[cname] = {
                    "source_dtype": src,
                    "transforms": [],
                    "target_dtype": src,
                    "nullable": True,
                }
            action = str(st.get("action") or "")
            ds_lineage[cname]["transforms"].append(action)
            ds_lineage[cname]["target_dtype"] = _infer_target_dtype(
                action, str(ds_lineage[cname].get("source_dtype") or "unknown")
            )

        if ds_lineage:
            out[ds_name] = ds_lineage

    return out
