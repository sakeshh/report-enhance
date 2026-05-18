from __future__ import annotations

import re
from typing import Any, Dict, List

from agent.etl_pipeline.join_emitters import (
    emit_pyspark_joins,
    emit_pyspark_load,
    emit_pyspark_output_contract,
    emit_pyspark_write_outputs,
)
from agent.etl_pipeline.io_snippets import (
    pyspark_prefix_non_key_columns_helper,
    pyspark_production_helpers,
    resolve_path_pyspark_helper,
)


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
    flag_col = repr(f"{col}_outlier_flagged")
    if action == "trim":
        out.append(f'{df} = {df}.withColumn({c}, F.trim(F.col({c}).cast("string")))')
    elif action in ("fill_or_drop", "fill_nulls_simple"):
        out.append(f"{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit('')))")
    elif action == "coerce_numeric":
        out.append(f"{df} = {df}.withColumn({c}, F.col({c}).cast('double'))")
    elif action == "cast_type":
        out.append(f"# Cast {c} to nullable long (Int64 equivalent)")
        out.append(f"{df} = {df}.withColumn({c}, F.col({c}).cast('long'))")
    elif action == "parse_dates":
        out.append(f"{df} = {df}.withColumn({c}, F.to_timestamp(F.col({c})))")
    elif action == "sanitize_email":
        out.append(f"{df} = {df}.withColumn({c}, F.lower(F.trim(F.col({c}).cast('string'))))")
        out.append(f"{df} = {df}.withColumn({c}, F.when(F.col({c}).contains('@'), F.col({c})).otherwise(None))")
    elif action == "normalize_phone":
        out.append(
            f'{df} = {df}.withColumn({c}, F.regexp_replace(F.col({c}).cast("string"), "\\\\D", ""))'
        )
    elif action == "hash_phone":
        out.append(f"# Privacy: one-way hash for {c} (business notes / manual review)")
        out.append(
            f"{df} = {df}.withColumn({c}, F.sha2(F.col({c}).cast('string'), 256))"
        )
    elif action == "mask_phone":
        out.append(f"# Privacy: mask {c} — keep last 4 digits only")
        out.append(
            f'{df} = {df}.withColumn({c}, F.concat(F.lit("***"), F.substring(F.regexp_replace(F.col({c}).cast("string"), "\\\\D", ""), -4, 4)))'
        )
    elif action == "lowercase":
        out.append(f'{df} = {df}.withColumn({c}, F.lower(F.col({c}).cast("string")))')
    elif action == "uppercase":
        out.append(f'{df} = {df}.withColumn({c}, F.upper(F.col({c}).cast("string")))')
    elif action in ("flag_outliers", "clip_or_flag"):
        out.append(f"# IQR outlier flagging for {c}")
        out.append(f"_bounds = {df}.select(")
        out.append(f"    F.percentile_approx(F.col({c}), 0.25).alias('q1'),")
        out.append(f"    F.percentile_approx(F.col({c}), 0.75).alias('q3')")
        out.append(f").first()")
        out.append(f"_iqr = _bounds['q3'] - _bounds['q1']")
        out.append(f"_lower = _bounds['q1'] - 1.5 * _iqr")
        out.append(f"_upper = _bounds['q3'] + 1.5 * _iqr")
        out.append(f"{df} = {df}.withColumn({flag_col},")
        out.append(f"    ((F.col({c}) < F.lit(_lower)) | (F.col({c}) > F.lit(_upper))) & F.col({c}).isNotNull())")
    elif action == "clip_outliers":
        out.append(f"# IQR outlier clipping for {c}")
        out.append(f"_bounds = {df}.select(")
        out.append(f"    F.percentile_approx(F.col({c}), 0.25).alias('q1'),")
        out.append(f"    F.percentile_approx(F.col({c}), 0.75).alias('q3')")
        out.append(f").first()")
        out.append(f"_iqr = _bounds['q3'] - _bounds['q1']")
        out.append(f"_lower = _bounds['q1'] - 1.5 * _iqr")
        out.append(f"_upper = _bounds['q3'] + 1.5 * _iqr")
        out.append(f"{df} = {df}.withColumn({c},")
        out.append(f"    F.when(F.col({c}) < F.lit(_lower), F.lit(_lower))")
        out.append(f"     .when(F.col({c}) > F.lit(_upper), F.lit(_upper))")
        out.append(f"     .otherwise(F.col({c})))")
    elif action == "cap_outliers":
        out.append(f"# IQR outlier capping (replace with median) for {c}")
        out.append(f"_stats = {df}.select(")
        out.append(f"    F.percentile_approx(F.col({c}), 0.25).alias('q1'),")
        out.append(f"    F.percentile_approx(F.col({c}), 0.50).alias('median'),")
        out.append(f"    F.percentile_approx(F.col({c}), 0.75).alias('q3')")
        out.append(f").first()")
        out.append(f"_iqr = _stats['q3'] - _stats['q1']")
        out.append(f"_lower = _stats['q1'] - 1.5 * _iqr")
        out.append(f"_upper = _stats['q3'] + 1.5 * _iqr")
        out.append(f"{df} = {df}.withColumn({c},")
        out.append(f"    F.when((F.col({c}) < F.lit(_lower)) | (F.col({c}) > F.lit(_upper)), F.lit(_stats['median']))")
        out.append(f"     .otherwise(F.col({c})))")
    elif action == "standardize_boolean":
        out.append(f'{df} = {df}.withColumn({c}, F.when(F.lower(F.col({c}).cast("string")).isin("1","true","yes","y","t"), F.lit(1)).otherwise(F.lit(0)))')
    elif action == "zero_to_null":
        out.append(f"{df} = {df}.withColumn({c}, F.when(F.col({c}) == 0, None).otherwise(F.col({c})))")
    elif action == "range_clip":
        out.append(f"{df} = {df}.withColumn({c}, F.when(F.col({c}).cast('double') < 0, F.lit(0)).otherwise(F.col({c}).cast('double')))")
    elif action in ("drop_column", "exclude_column"):
        out.append(f"{df} = {df}.drop({c})")
    elif action == "nullify_future_dates":
        out.append(f"{df} = {df}.withColumn({c}, F.when(F.col({c}) > F.current_date(), None).otherwise(F.col({c})))")
    elif action == "deduplicate":
        out.append(f"{df} = {df}.dropDuplicates([{c}])")
    elif action == "noop":
        out.append(f"# Column {col}: no transform (user accepted as-is)")
    elif action == "regex_replace":
        out.append(
            f'{df} = {df}.withColumn({c}, F.regexp_replace(F.col({c}).cast("string"), r"[^\\w\\s]", ""))'
        )
    elif action == "replace_values":
        out.append(f"# replace_values on {c}: map via when/otherwise from business_rules.replace_values")
    elif action == "validate_referential_integrity_or_stage":
        out.append(f"# Referential integrity check for {c} — filter orphans to quarantine DataFrame")
    else:
        out.append(f"# Unsupported in pyspark template v1: {action} on {c}")
    return out


def generate_pyspark_etl(plan: Dict[str, Any], assessment: Dict[str, Any]) -> str:
    _ = assessment
    plan_id = str(plan.get("plan_id") or "unknown")
    lines: List[str] = [
        '"""',
        f"PySpark ETL — Agent Dhara — plan_id={plan_id}",
        "Uses connector_manifest for read/write paths. Set AZURE_STORAGE_ACCOUNT,",
        "DHARA_BLOB_CONTAINER, DHARA_BLOB_BASE_PATH for Azure Blob sources.",
        '"""',
        "from __future__ import annotations",
        "",
        "import os",
        "from pyspark.sql import functions as F",
        "from pyspark.sql import DataFrame",
        "",
    ]
    notes = (plan.get("business_rules") or {}).get("notes") or ""
    if notes:
        lines.extend(["# Business notes:", "# " + str(notes).replace("\n", "\n# "), ""])

    manual = plan.get("manual_review") or []
    if manual:
        lines.append("# Manual review (plan — implement hash/mask per business notes where noted):")
        for item in manual:
            ds = item.get("dataset") or "?"
            col = item.get("column") or "?"
            msg = item.get("message") or item.get("guidance") or ""
            lines.append(f"#   [{ds}] {col}: {msg}")
        lines.append("")

    manifest = plan.get("connector_manifest") or {}
    if manifest.get("datasets"):
        lines.append(resolve_path_pyspark_helper())
        lines.append("")
        lines.append(pyspark_production_helpers())
        lines.append("")
        lines.append(pyspark_prefix_non_key_columns_helper())
        lines.append("")

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
    lines.append("")

    rel = plan.get("relationships") or {}
    rules = plan.get("business_rules") or {}
    non_nullable = [str(c) for c in (rules.get("non_nullable") or []) if c]
    if manifest.get("datasets") or rel.get("joins"):
        for sl in emit_pyspark_output_contract(plan, manifest):
            lines.append(sl)
        lines.append("def run_pipeline(spark):")
        lines.append("    import logging")
        lines.append('    logging.basicConfig(level=logging.INFO)')
        lines.append("    dfs = {}")
        for sl in emit_pyspark_load(plan, manifest):
            lines.append(f"    {sl}")
        for ds_name in (plan.get("datasets") or {}):
            fn = f"transform_{_safe(ds_name)}"
            lines.append(f'    if "{ds_name}" in dfs:')
            lines.append(f'        dfs["{ds_name}"] = {fn}(dfs["{ds_name}"])')
            if non_nullable:
                lines.append(
                    f'        _warn_nulls_in_columns(dfs["{ds_name}"], {non_nullable!r}, "{ds_name}")'
                )
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
        lines.append("  # XML sources need: spark.jars.packages com.databricks:spark-xml_2.12:0.17.0")
        lines.append("    _dfs, _paths = run_pipeline(spark)")
        lines.append('    print("PySpark ETL complete — datasets:", list(_dfs.keys()))')
        lines.append('    print("Output paths:", _paths)')

    return "\n".join(lines)
