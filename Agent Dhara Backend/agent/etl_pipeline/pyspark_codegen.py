from __future__ import annotations

import re
from typing import Any, Dict, List


def _safe(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", name)
    return (s or "dataset").strip("_")


def _emit_spark(action: str, col: str | None, df: str) -> List[str]:
    out: List[str] = []
    if not col:
        if action == "deduplicate":
            out.append(f"{df} = {df}.dropDuplicates()")
        return out
    c = repr(str(col))
    if action == "trim":
        out.append(f'{df} = {df}.withColumn({c}, F.trim(F.col({c}).cast("string")))')
    elif action in ("fill_or_drop", "fill_nulls_simple"):
        out.append(f"{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit('')))")
    elif action == "coerce_numeric":
        out.append(f"{df} = {df}.withColumn({c}, F.col({c}).cast('double'))")
    elif action == "parse_dates":
        out.append(f"{df} = {df}.withColumn({c}, F.to_timestamp(F.col({c})))")
    elif action == "sanitize_email":
        out.append(f"{df} = {df}.withColumn({c}, F.lower(F.trim(F.col({c}).cast('string'))))")
        out.append(f"{df} = {df}.withColumn({c}, F.when(F.col({c}).contains('@'), F.col({c})).otherwise(None))")
    elif action == "normalize_phone":
        out.append(
            f'{df} = {df}.withColumn({c}, F.regexp_replace(F.col({c}).cast("string"), "\\\\D", ""))'
        )
    elif action == "deduplicate":
        out.append(f"{df} = {df}.dropDuplicates([{c}])")
    else:
        out.append(f"# TODO spark: {action} on {c}")
    return out


def generate_pyspark_etl(plan: Dict[str, Any], assessment: Dict[str, Any]) -> str:
    _ = assessment
    plan_id = str(plan.get("plan_id") or "unknown")
    lines: List[str] = [
        '"""',
        f"PySpark ETL — Agent Dhara — plan_id={plan_id}",
        "Wire your own read/load paths; functions below assume DataFrames exist.",
        '"""',
        "from __future__ import annotations",
        "",
        "from pyspark.sql import functions as F",
        "from pyspark.sql import DataFrame",
        "",
    ]
    notes = (plan.get("business_rules") or {}).get("notes") or ""
    if notes:
        lines.extend(["# Business notes:", "# " + str(notes).replace("\n", "\n# "), ""])

    for ds_name, block in (plan.get("datasets") or {}).items():
        fn = f"transform_{_safe(ds_name)}"
        lines.append(f"def {fn}(df: DataFrame) -> DataFrame:")
        lines.append('    """Apply planned transforms."""')
        var = "out"
        lines.append(f"    {var} = df")
        for st in sorted(block.get("steps") or [], key=lambda x: int(x.get("order") or 0)):
            for sl in _emit_spark(str(st.get("action")), st.get("column"), var):
                lines.append(f"    {sl}")
        lines.append(f"    return {var}")
        lines.append("")

    lines.append("DATASETS = " + repr(list((plan.get("datasets") or {}).keys())))
    return "\n".join(lines)
