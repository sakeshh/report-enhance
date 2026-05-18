"""
Azure Data Factory Mapping Data Flow expression builders (ADF expression language).
https://learn.microsoft.com/azure/data-factory/data-flow-expression-functions
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from agent.etl_pipeline.codegen_shared import outlier_multiplier, step_params


def _col(name: Optional[str]) -> str:
    return str(name or "column")


def adf_fill_expression(col: str, params: Dict[str, Any]) -> Tuple[str, str]:
    """Return (target_column, expression) — coalesce / iif fill."""
    c = _col(col)
    strat = params.get("fill_strategy")
    fval = params.get("fill_value")
    if strat == "median":
        if fval is not None:
            return c, f"coalesce({c}, toDouble({fval}))"
        return c, f"coalesce({c}, percentile({c}, 0.5))"
    if strat == "mean":
        if fval is not None:
            return c, f"coalesce({c}, toDouble({fval}))"
        return c, f"coalesce({c}, avg({c}))"
    if strat == "value":
        if fval is None:
            return c, f"coalesce({c}, toString(''))"
        if isinstance(fval, (int, float)):
            return c, f"coalesce({c}, toDouble({fval}))"
        esc = str(fval).replace("'", "''")
        return c, f"coalesce({c}, toString('{esc}'))"
    return c, f"coalesce({c}, {c})"


def adf_outlier_bounds(col: str, params: Dict[str, Any]) -> Tuple[str, str]:
    """Lower/upper bound expressions using params p5/p95 or percentile."""
    c = _col(col)
    mult = outlier_multiplier(params)
    p5 = params.get("p5")
    p95 = params.get("p95")
    if p5 is not None and p95 is not None:
        lower = f"toDouble({p5})"
        upper = f"toDouble({p95})"
        iqr = f"({upper} - {lower})"
        return (
            f"({lower} - {mult} * {iqr})",
            f"({upper} + {mult} * {iqr})",
        )
    q1 = f"percentile({c}, 0.25)"
    q3 = f"percentile({c}, 0.75)"
    iqr = f"({q3} - {q1})"
    return (f"({q1} - {mult} * {iqr})", f"({q3} + {mult} * {iqr})")


def adf_outlier_expression(
    action: str, col: Optional[str], params: Dict[str, Any]
) -> Tuple[str, str, Optional[str]]:
    """
    Returns (target_column, expression, optional_flag_column_name).
    flag column only for flag_outliers / clip_or_flag.
    """
    c = _col(col)
    method = params.get("outlier_method") or (
        "clip" if action == "clip_outliers" else "cap" if action == "cap_outliers" else "flag"
    )
    lo, hi = adf_outlier_bounds(c, params)

    if method == "clip":
        expr = f"iif({c} < {lo}, {lo}, iif({c} > {hi}, {hi}, {c}))"
        return c, expr, None
    if method == "cap":
        med = params.get("fill_value")
        rep = f"toDouble({med})" if med is not None else f"percentile({c}, 0.5)"
        expr = f"iif({c} < {lo} || {c} > {hi}, {rep}, {c})"
        return c, expr, None

    flag = f"{c}_outlier_flagged"
    expr = f"iif({c} < {lo} || {c} > {hi}, true(), false())"
    return flag, expr, flag


def adf_expression_for_step(action: str, col: Optional[str], st: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    One or more ADF derived columns for a plan step.
    Each item: {targetColumn, expression, description}
    """
    params = step_params(st)
    act = (action or "").lower()
    c = _col(col)
    out: List[Dict[str, str]] = []

    if act == "trim":
        out.append({"targetColumn": c, "expression": f"trim({c})", "description": f"trim {c}"})
    elif act == "lowercase":
        out.append({"targetColumn": c, "expression": f"toLower({c})", "description": f"lowercase {c}"})
    elif act == "uppercase":
        out.append({"targetColumn": c, "expression": f"toUpper({c})", "description": f"uppercase {c}"})
    elif act in ("fill_or_drop", "fill_nulls_simple"):
        tc, ex = adf_fill_expression(c, params)
        out.append({"targetColumn": tc, "expression": ex, "description": f"fill nulls {c}"})
    elif act == "coerce_numeric":
        out.append({"targetColumn": c, "expression": f"toDouble({c})", "description": f"coerce numeric {c}"})
    elif act == "cast_type":
        out.append({"targetColumn": c, "expression": f"toInteger({c})", "description": f"cast {c}"})
    elif act == "parse_dates":
        out.append({"targetColumn": c, "expression": f"toTimestamp({c})", "description": f"parse date {c}"})
    elif act == "sanitize_email":
        out.append({"targetColumn": c, "expression": f"toLower(trim({c}))", "description": f"sanitize email {c}"})
        out.append(
            {
                "targetColumn": c,
                "expression": f"iif(contains({c}, '@'), {c}, toString(null()))",
                "description": f"invalidate bad emails {c}",
            }
        )
    elif act == "normalize_phone":
        out.append(
            {
                "targetColumn": c,
                "expression": f"regexpReplace(toString({c}), '[^0-9]', '')",
                "description": f"normalize phone {c}",
            }
        )
    elif act == "hash_phone":
        out.append(
            {
                "targetColumn": c,
                "expression": f"sha2(256, toString({c}))",
                "description": f"one-way hash {c}",
            }
        )
    elif act == "mask_phone":
        out.append(
            {
                "targetColumn": c,
                "expression": (
                    f"concat(toString('***'), "
                    f"right(regexpReplace(toString({c}), '[^0-9]', ''), 4))"
                ),
                "description": f"mask phone {c}",
            }
        )
    elif act in ("flag_outliers", "clip_or_flag", "clip_outliers", "cap_outliers"):
        tc, ex, _ = adf_outlier_expression(act, col, params)
        out.append({"targetColumn": tc, "expression": ex, "description": f"{act} on {c}"})
    elif act == "standardize_boolean":
        out.append(
            {
                "targetColumn": c,
                "expression": (
                    f"iif(in([toLower(toString({c}))], ['1','true','yes','y','t']), 1, 0)"
                ),
                "description": f"standardize boolean {c}",
            }
        )
    elif act == "zero_to_null":
        out.append(
            {
                "targetColumn": c,
                "expression": f"iif({c} == 0, toString(null()), {c})",
                "description": f"zero to null {c}",
            }
        )
    elif act == "range_clip":
        out.append(
            {
                "targetColumn": c,
                "expression": f"iif(toDouble({c}) < 0, toDouble(0), toDouble({c}))",
                "description": f"range clip {c}",
            }
        )
    elif act in ("drop_column", "exclude_column"):
        out.append({"targetColumn": c, "expression": f"/* drop {c} in select */", "description": f"exclude {c}"})
    elif act == "validate_referential_integrity_or_stage":
        mode = params.get("enforcement_mode") or "flag"
        rel_ds = params.get("related_dataset") or "?"
        out.append(
            {
                "targetColumn": f"{c}_ri_ok",
                "expression": f"/* RI check {c} -> {rel_ds} mode={mode} */ true()",
                "description": f"referential integrity {c}",
            }
        )
    else:
        out.append(
            {
                "targetColumn": c,
                "expression": f"/* unsupported in v1: {act} */ {c}",
                "description": f"unsupported {act}",
            }
        )
    return out
