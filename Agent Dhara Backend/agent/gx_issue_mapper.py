from __future__ import annotations

import copy
from typing import Any, Dict, List, Set


def _log(msg: str) -> None:
    print(f"[gx_issue_mapper] {msg}")


def _gx_issue_type(expectation_type: str) -> str:
    m = {
        "expect_column_values_to_not_be_null": "gx_null_violation",
        "expect_column_values_to_be_unique": "gx_uniqueness_violation",
        "expect_column_values_to_be_of_type": "gx_type_violation",
        "expect_column_values_to_match_regex": "gx_format_violation",
        "expect_column_min_to_be_between": "gx_range_violation",
        "expect_column_max_to_be_between": "gx_range_violation",
        "expect_column_values_to_be_in_set": "gx_domain_violation",
    }
    return m.get(expectation_type, "gx_unknown_violation")


def _severity_for(expectation_type: str, dhara_type: str) -> str:
    if dhara_type in ("gx_null_violation", "gx_uniqueness_violation", "gx_type_violation"):
        return "high"
    if expectation_type == "expect_column_values_to_match_regex":
        return "medium"
    return "medium"


def _profiler_issue_matches_gx(profiler_issue: dict, gx_issue: dict) -> bool:
    """Heuristic: same column and overlapping DQ category."""
    col_p = profiler_issue.get("column")
    col_g = gx_issue.get("column")
    if col_p is None or col_g is None or str(col_p) != str(col_g):
        return False
    pt = str(profiler_issue.get("type") or "").lower()
    gt = str(gx_issue.get("type") or "")
    if gt == "gx_null_violation":
        return any(x in pt for x in ("null", "missing", "mnar", "mar", "mcar", "sparse"))
    if gt == "gx_uniqueness_violation":
        return any(x in pt for x in ("duplicate", "unique", "pk", "key"))
    if gt == "gx_type_violation":
        return any(x in pt for x in ("type", "dtype", "format", "numeric", "coercion"))
    if gt == "gx_format_violation":
        return any(x in pt for x in ("format", "regex", "email", "phone", "pattern"))
    if gt == "gx_range_violation":
        return any(x in pt for x in ("range", "outlier", "bound", "min", "max"))
    if gt == "gx_domain_violation":
        return any(x in pt for x in ("domain", "enum", "category", "set", "allowed"))
    return False


def map_gx_failures_to_issues(gx_result: dict) -> dict:
    """
    Map ``gx_runner.run_gx_validation`` output into Dhara ``data_quality_issues`` shape.
    """
    ds_name = str(gx_result.get("dataset") or "")
    failures = gx_result.get("failures") or []
    out: Dict[str, Any] = {"datasets": {}}
    if not ds_name:
        return out

    issues: List[dict] = []
    for f in failures:
        if not isinstance(f, dict):
            continue
        et = str(f.get("expectation_type") or "")
        col = f.get("column")
        dhara_type = _gx_issue_type(et)
        sev = _severity_for(et, dhara_type)
        u_count = f.get("unexpected_count")
        u_pct = f.get("unexpected_percent")
        try:
            cnt = int(u_count) if u_count is not None else 0
        except (TypeError, ValueError):
            cnt = 0
        try:
            pct_f = float(u_pct) if u_pct is not None else 0.0
        except (TypeError, ValueError):
            pct_f = 0.0
        msg = (
            f"GX: {cnt} failing rows in column {col!r} "
            f"({pct_f:.2f}% fail {et})"
            if col is not None
            else f"GX: {cnt} failing rows ({pct_f:.2f}% fail {et})"
        )
        issues.append(
            {
                "type": dhara_type,
                "column": col,
                "severity": sev,
                "message": msg,
                "count": cnt,
                "unexpected_percent": pct_f,
                "source": "great_expectations",
                "expectation_type": et,
            }
        )

    out["datasets"][ds_name] = {"issues": issues}
    _log(f"mapped {len(issues)} GX failures for dataset={ds_name!r}")
    return out


def merge_gx_issues_into_assessment(assessment: dict, gx_issues: dict) -> dict:
    """
    Deep-merge GX issues into ``assessment['data_quality_issues']['datasets']``.

    When a profiler issue on the same column appears to describe the same problem class,
    keep the profiler row and set ``gx_confirmed`` instead of duplicating.
    """
    merged = copy.deepcopy(assessment)
    dq = merged.setdefault("data_quality_issues", {})
    ds_block = dq.setdefault("datasets", {})
    gx_ds = (gx_issues or {}).get("datasets") or {}

    for ds_name, gx_pack in gx_ds.items():
        if not isinstance(gx_pack, dict):
            continue
        target = ds_block.setdefault(ds_name, {})
        existing: List[dict] = list(target.get("issues") or [])
        incoming: List[dict] = list(gx_pack.get("issues") or [])
        used_gx: Set[int] = set()

        for i, ex in enumerate(existing):
            for j, gx in enumerate(incoming):
                if j in used_gx:
                    continue
                if _profiler_issue_matches_gx(ex, gx):
                    base = dict(ex)
                    base["gx_confirmed"] = True
                    existing[i] = base
                    used_gx.add(j)

        for j, gx in enumerate(incoming):
            if j not in used_gx:
                existing.append(dict(gx))

        target["issues"] = existing

    _log("merged GX issues into assessment data_quality_issues.datasets")
    return merged
