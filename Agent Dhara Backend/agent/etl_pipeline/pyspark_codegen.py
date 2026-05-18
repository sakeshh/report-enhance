from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from agent.etl_pipeline.codegen_policy import plan_policy_block
from agent.etl_pipeline.codegen_shared import outlier_multiplier, step_params
from agent.etl_pipeline.join_emitters import (
    emit_pyspark_joins,
    emit_pyspark_load,
    emit_pyspark_output_contract,
    emit_pyspark_write_outputs,
)
from agent.etl_pipeline.io_snippets import (
    pyspark_iqr_bounds_helper,
    pyspark_prefix_non_key_columns_helper,
    pyspark_production_helpers,
    resolve_path_pyspark_helper,
)


def _safe(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", name)
    return (s or "dataset").strip("_")


def _emit_fill_spark(col: str, df: str, params: Dict[str, Any]) -> List[str]:
    c = repr(str(col))
    strat = params.get("fill_strategy")
    fval = params.get("fill_value")
    if strat == "median":
        if fval is not None:
            return [f"{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit({fval})))"]
        return [
            f"_med = {df}.select(F.percentile_approx(F.col({c}), 0.5).alias('m')).first()['m']",
            f"{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit(_med)))",
        ]
    if strat == "mean":
        if fval is not None:
            return [f"{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit({fval})))"]
        return [
            f"_avg = {df}.select(F.avg(F.col({c}).cast('double')).alias('m')).first()['m']",
            f"{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit(_avg)))",
        ]
    if strat == "value" and fval is not None:
        return [f"{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit({repr(fval)})))"]
    if strat == "value":
        return [f'{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit("")))']
    return [f"{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit(None)))"]


def _emit_outliers_spark(action: str, col: str, df: str, params: Dict[str, Any]) -> List[str]:
    c = repr(str(col))
    flag_col = repr(f"{col}_outlier_flagged")
    mult = outlier_multiplier(params)
    method = params.get("outlier_method") or (
        "clip" if action == "clip_outliers" else "cap" if action == "cap_outliers" else "flag"
    )
    lines = [
        f"_stats, _iqr, _lower, _upper = _iqr_bounds({df}, {c}, multiplier={mult})",
    ]
    if method == "clip":
        lines.append(
            f"{df} = {df}.withColumn({c},"
            f" F.when(F.col({c}) < F.lit(_lower), F.lit(_lower))"
            f" .when(F.col({c}) > F.lit(_upper), F.lit(_upper))"
            f" .otherwise(F.col({c})))"
        )
    elif method == "cap":
        med = params.get("fill_value")
        if med is not None:
            lines.append(f"_median = {med}")
        else:
            lines.append(f"_median = _stats['median']")
        lines.append(
            f"{df} = {df}.withColumn({c},"
            f" F.when((F.col({c}) < F.lit(_lower)) | (F.col({c}) > F.lit(_upper)), F.lit(_median))"
            f" .otherwise(F.col({c})))"
        )
    else:
        lines.append(
            f"{df} = {df}.withColumn({flag_col},"
            f" ((F.col({c}) < F.lit(_lower)) | (F.col({c}) > F.lit(_upper))) & F.col({c}).isNotNull())"
        )
    return lines


def _emit_spark(action: str, col: str | None, df: str, step_meta: Optional[Dict[str, Any]] = None) -> List[str]:
    params = step_params(step_meta)
    act = (action or "").lower()
    if not col:
        if act == "deduplicate":
            return [f"{df} = {df}.dropDuplicates()"]
        return []
    c = repr(str(col))
    if act == "trim":
        return [f'{df} = {df}.withColumn({c}, F.trim(F.col({c}).cast("string")))']
    if act in ("fill_or_drop", "fill_nulls_simple"):
        return _emit_fill_spark(col, df, params)
    if act == "coerce_numeric":
        return [f"{df} = {df}.withColumn({c}, F.col({c}).cast('double'))"]
    if act == "cast_type":
        return [f"{df} = {df}.withColumn({c}, F.col({c}).cast('long'))"]
    if act == "parse_dates":
        return [f"{df} = {df}.withColumn({c}, F.to_timestamp(F.col({c})))"]
    if act == "sanitize_email":
        return [
            f'{df} = {df}.withColumn({c}, F.lower(F.trim(F.col({c}).cast("string"))))',
            f"{df} = {df}.withColumn({c}, F.when(F.col({c}).contains('@'), F.col({c})).otherwise(None))",
        ]
    if act == "normalize_phone":
        return [f'{df} = {df}.withColumn({c}, F.regexp_replace(F.col({c}).cast("string"), "\\\\D", ""))']
    if act == "hash_phone":
        return [
            f"# Privacy: one-way hash (params.privacy=hash)",
            f"{df} = {df}.withColumn({c}, F.sha2(F.col({c}).cast('string'), 256))",
        ]
    if act == "mask_phone":
        return [
            f"# Privacy: reversible mask (params.privacy=mask)",
            f'{df} = {df}.withColumn({c}, F.concat(F.lit("***"), F.substring(F.regexp_replace(F.col({c}).cast("string"), "\\\\D", ""), -4, 4)))',
        ]
    if act == "lowercase":
        return [f'{df} = {df}.withColumn({c}, F.lower(F.col({c}).cast("string")))']
    if act == "uppercase":
        return [f'{df} = {df}.withColumn({c}, F.upper(F.col({c}).cast("string")))']
    if act in ("flag_outliers", "clip_or_flag", "clip_outliers", "cap_outliers"):
        return _emit_outliers_spark(act, col, df, params)
    if act == "standardize_boolean":
        return [
            f'{df} = {df}.withColumn({c}, F.when(F.lower(F.col({c}).cast("string")).isin("1","true","yes","y","t"), F.lit(1)).otherwise(F.lit(0)))'
        ]
    if act in ("drop_column", "exclude_column"):
        return [f"{df} = {df}.drop({c})"]
    if act == "noop":
        return [f"# Column {col}: no transform"]
    if act == "validate_referential_integrity_or_stage":
        mode = params.get("enforcement_mode") or "flag"
        rel_ds = params.get("related_dataset") or "?"
        return [f"# RI {col} -> {rel_ds} (mode={mode}); route orphans to quarantine sink"]
    return [f"# Unsupported in pyspark template v1: {act} on {col}"]


def _emit_valid_values_spark(df: str, ds_name: str, rules: Dict[str, Any]) -> List[str]:
    vv = rules.get("valid_values") or {}
    if not vv:
        return []
    never_drop = bool(rules.get("never_drop_rows"))
    lines: List[str] = []
    for col, allowed in vv.items():
        c = repr(str(col))
        sid = _safe(col)
        allowed_lit = repr([str(v).lower() for v in allowed])
        if never_drop:
            lines.extend([
                f"if {c} in {df}.columns:",
                f"    _bad = ~F.lower(F.col({c}).cast('string')).isin({allowed_lit}) & F.col({c}).isNotNull()",
                f"    {df} = {df}.withColumn({c}, F.when(_bad, F.lit(None)).otherwise(F.col({c})))",
            ])
        else:
            lines.extend([
                f"if {c} in {df}.columns:",
                f"    _before = {df}.count()",
                f"    {df} = {df}.filter(F.lower(F.col({c}).cast('string')).isin({allowed_lit}) | F.col({c}).isNull())",
                f"    logging.info(f'valid_values {ds_name}.{col}: dropped %s rows', _before - {df}.count())",
            ])
    return lines


def generate_pyspark_etl(plan: Dict[str, Any], assessment: Dict[str, Any]) -> str:
    _ = assessment
    plan_id = str(plan.get("plan_id") or "unknown")
    rules = plan.get("business_rules") or {}
    never_drop = bool(rules.get("never_drop_rows"))
    rel = plan.get("relationships") or {}
    joins = rel.get("joins") or []
    join_strategy = str(joins[0].get("join_type") or "left") if joins else "none"

    policy = plan_policy_block(plan).replace("\n", "\n# ")
    lines: List[str] = [
        '"""',
        f"PySpark ETL — plan_id={plan_id}",
        "Generated by: Agent Dhara",
        "Policy:",
        policy,
        '"""',
        "from __future__ import annotations",
        "",
        "import logging",
        "import os",
        "from pyspark.sql import functions as F",
        "from pyspark.sql import DataFrame",
        "",
        "logging.basicConfig(level=logging.INFO)",
        "logger = logging.getLogger('agent_dhara')",
        "",
    ]
    notes = str(rules.get("notes") or "").strip()
    if notes:
        lines.extend(["# Business notes:", "# " + notes.replace("\n", "\n# "), ""])

    manifest = plan.get("connector_manifest") or {}
    if manifest.get("datasets"):
        lines.append(resolve_path_pyspark_helper())
        lines.append("")
        lines.append(pyspark_production_helpers())
        lines.append("")
        lines.append(pyspark_iqr_bounds_helper())
        lines.append("")
        lines.append(pyspark_prefix_non_key_columns_helper())
        lines.append("")

    for ds_name, block in (plan.get("datasets") or {}).items():
        fn = f"transform_{_safe(ds_name)}"
        lines.append(f"def {fn}(df: DataFrame) -> DataFrame:")
        var = "out"
        lines.append(f"    {var} = df")
        for st in sorted(block.get("steps") or [], key=lambda x: int(x.get("order") or 0)):
            for sl in _emit_spark(str(st.get("action")), st.get("column"), var, step_meta=st):
                lines.append(f"    {sl}")
        for sl in _emit_valid_values_spark(var, ds_name, rules):
            lines.append(f"    {sl}")
        for col in rules.get("non_nullable") or []:
            lines.append(f'    _warn_nulls_in_columns({var}, [{col!r}], "{ds_name}")')
        lines.append(f"    return {var}")
        lines.append("")

    lines.append("DATASETS = " + repr(list((plan.get("datasets") or {}).keys())))
    lines.append("")

    non_nullable = [str(c) for c in (rules.get("non_nullable") or []) if c]
    if manifest.get("datasets") or rel.get("joins"):
        for sl in emit_pyspark_output_contract(plan, manifest):
            lines.append(sl)
        lines.append("def run_pipeline(spark):")
        lines.append("    dfs = {}")
        for sl in emit_pyspark_load(plan, manifest):
            lines.append(f"    {sl}")
        for ds_name in (plan.get("datasets") or {}):
            fn = f"transform_{_safe(ds_name)}"
            lines.append(f'    if "{ds_name}" in dfs:')
            lines.append(f'        dfs["{ds_name}"] = {fn}(dfs["{ds_name}"])')
            if non_nullable:
                lines.append(f'        _warn_nulls_in_columns(dfs["{ds_name}"], {non_nullable!r}, "{ds_name}")')
            lines.append(f'        _log_row_count(dfs["{ds_name}"], "{ds_name}")')
        for sl in emit_pyspark_joins(plan):
            lines.append(f"    {sl}")
        for sl in emit_pyspark_write_outputs(plan, manifest):
            lines.append(f"    {sl}")
        lines.append("    return dfs, OUTPUT_PATHS")
        lines.append("")
        lines.append("if __name__ == '__main__':")
        lines.append("    from pyspark.sql import SparkSession")
        lines.append('    spark = SparkSession.builder.appName("AgentDharaETL").getOrCreate()')
        lines.append("    _dfs, _paths = run_pipeline(spark)")

    return "\n".join(lines)
