from __future__ import annotations

from typing import Any, Dict, List, Optional


def _estimate_from_profile(
    assessment: Dict[str, Any], ds_name: str, col: Optional[str], action: str
) -> Optional[int]:
    """Derive rough affected rows from column null % and row count when DQ count is missing."""
    if not col:
        return None
    ds = (assessment.get("datasets") or {}).get(ds_name) or {}
    total = int(ds.get("row_count") or 0)
    if total <= 0:
        return None
    meta = (ds.get("columns") or {}).get(col) or {}
    pct = meta.get("null_percentage")
    if pct is None:
        return None
    try:
        p = float(pct)
    except (TypeError, ValueError):
        return None
    n = int(round((p / 100.0) * total))
    if action in ("trim", "fill_or_drop", "fill_nulls_simple", "coerce_numeric", "parse_dates"):
        return max(0, min(n, total))
    if action in ("sanitize_email", "normalize_phone"):
        return max(0, min(total, int(round(0.15 * total))) or 1)
    return None


def build_impact_preview(assessment: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
    """
    Summarize likely data impact per step using assessment row counts, DQ counts,
    and column profile heuristics when counts are missing.
    """
    rows_by_ds: Dict[str, int] = {}
    for name, meta in (assessment.get("datasets") or {}).items():
        rows_by_ds[name] = int(meta.get("row_count") or 0)

    bullets: List[str] = []
    detail: List[Dict[str, Any]] = []

    ds_plan = plan.get("datasets") or {}
    for ds_name, block in ds_plan.items():
        total = rows_by_ds.get(ds_name, 0)
        for st in block.get("steps") or []:
            action = st.get("action")
            col = st.get("column")
            est = st.get("estimated_affected_rows")
            est_source = "dq_issue_count"
            if est is None or not isinstance(est, (int, float)) or est < 0:
                est2 = _estimate_from_profile(assessment, ds_name, col, str(action or ""))
                if est2 is not None:
                    est = est2
                    est_source = "profile_heuristic"
            if est is not None and isinstance(est, (int, float)) and est >= 0:
                pct = (100.0 * float(est) / total) if total > 0 else None
                line = f"[{ds_name}] {action}" + (f" on `{col}`" if col else "") + f" — ~{int(est)} rows ({est_source})"
                if pct is not None:
                    line += f" (~{pct:.1f}% of {total:,} rows)"
            else:
                line = (
                    f"[{ds_name}] {action}"
                    + (f" on `{col}`" if col else "")
                    + " — impact volume unknown (no DQ count + no usable column profile)"
                )
                est_source = "unknown"
            bullets.append(line)
            detail.append(
                {
                    "dataset": ds_name,
                    "column": col,
                    "action": action,
                    "line": line,
                    "estimate_source": est_source,
                }
            )

    for st in plan.get("global_steps") or []:
        line = f"[global] {st.get('action')}" + (f" — ref: {st.get('column')}" if st.get("column") else "")
        bullets.append(line)
        detail.append(
            {
                "dataset": "_global",
                "column": st.get("column"),
                "action": st.get("action"),
                "line": line,
                "estimate_source": "n/a",
            }
        )

    manual = plan.get("manual_review") or []
    if manual:
        bullets.append(f"⚠ {len(manual)} item(s) flagged for manual review before production run.")

    return {
        "summary_lines": bullets,
        "detail": detail,
        "manual_review_count": len(manual),
        "dataset_row_totals": rows_by_ds,
    }
