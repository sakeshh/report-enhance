from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


def _safe_ident(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", name)
    if s and s[0].isdigit():
        s = "ds_" + s
    return s or "dataset"


def _col_expr(col: str) -> str:
    return repr(str(col))


def _emit_step(action: str, col: Optional[str], ds_var: str) -> List[str]:
    lines: List[str] = []
    if not col:
        if action == "deduplicate":
            lines.append(f"{ds_var} = {ds_var}.drop_duplicates()")
        return lines

    c = _col_expr(col)
    flag_col = _col_expr(f"{col}_outlier_flagged")
    
    if action == "trim":
        lines.append(f"{ds_var}[{c}] = {ds_var}[{c}].astype(str).str.strip()")
    elif action in ("fill_or_drop", "fill_nulls_simple"):
        lines.append(f"{ds_var}[{c}] = {ds_var}[{c}].fillna(pd.NA)")
    elif action == "coerce_numeric":
        lines.append(f"{ds_var}[{c}] = pd.to_numeric({ds_var}[{c}], errors='coerce')")
    elif action == "cast_type":
        # Supports Int64 for nullable integers as requested by reviewer
        lines.append(f"{ds_var}[{c}] = {ds_var}[{c}].astype('Int64')")
    elif action == "parse_dates":
        lines.append(f"{ds_var}[{c}] = pd.to_datetime({ds_var}[{c}], errors='coerce')")
    elif action == "sanitize_email":
        lines.append(f"{ds_var}[{c}] = {ds_var}[{c}].astype(str).str.strip().str.lower()")
        lines.append(f"_mask = ~{ds_var}[{c}].str.contains('@', na=False)")
        lines.append(f"{ds_var}.loc[_mask, {c}] = pd.NA")
    elif action == "normalize_phone":
        lines.append(f"{ds_var}[{c}] = {ds_var}[{c}].astype(str).str.replace(r'\\D', '', regex=True)")
    elif action == "lowercase":
        lines.append(f"{ds_var}[{c}] = {ds_var}[{c}].astype(str).str.lower()")
    elif action == "uppercase":
        lines.append(f"{ds_var}[{c}] = {ds_var}[{c}].astype(str).str.upper()")
    elif action == "standardize_boolean":
        lines.append(f"_v = {ds_var}[{c}].astype(str).str.strip().str.lower()")
        lines.append(f"{ds_var}[{c}] = _v.isin(('1', 'true', 'yes', 'y', 't')).astype('Int64')")
    elif action == "deduplicate":
        lines.append(f"{ds_var} = {ds_var}.drop_duplicates(subset=[{c}], keep='first')")
    elif action == "zero_to_null":
        lines.append(f"{ds_var}[{c}] = {ds_var}[{c}].replace(0, pd.NA)")
    elif action in ("clip_or_flag", "flag_outliers"):
        # For numeric outliers detected by IQR: flag them in a separate column
        lines.append(f"_q1 = {ds_var}[{c}].quantile(0.25)")
        lines.append(f"_q3 = {ds_var}[{c}].quantile(0.75)")
        lines.append(f"_iqr = _q3 - _q1")
        lines.append(f"_lower = _q1 - 1.5 * _iqr")
        lines.append(f"_upper = _q3 + 1.5 * _iqr")
        lines.append(f"{ds_var}[{flag_col}] = ((({ds_var}[{c}] < _lower) | ({ds_var}[{c}] > _upper)) & {ds_var}[{c}].notna()).astype(bool)")
    elif action == "clip_outliers":
        lines.append(f"_q1 = {ds_var}[{c}].quantile(0.25)")
        lines.append(f"_q3 = {ds_var}[{c}].quantile(0.75)")
        lines.append(f"_iqr = _q3 - _q1")
        lines.append(f"{ds_var}[{c}] = {ds_var}[{c}].clip(lower=_q1 - 1.5 * _iqr, upper=_q3 + 1.5 * _iqr)")
    elif action == "cap_outliers":
        lines.append(f"_median = {ds_var}[{c}].median()")
        lines.append(f"_q1 = {ds_var}[{c}].quantile(0.25)")
        lines.append(f"_q3 = {ds_var}[{c}].quantile(0.75)")
        lines.append(f"_iqr = _q3 - _q1")
        lines.append(f"_mask = ({ds_var}[{c}] < _q1 - 1.5 * _iqr) | ({ds_var}[{c}] > _q3 + 1.5 * _iqr)")
        lines.append(f"{ds_var}.loc[_mask, {c}] = _median")
    elif action == "range_clip":
        lines.append(f"# Range clip on {c}: preserving rows but bounding values")
        lines.append(f"{ds_var}[{c}] = pd.to_numeric({ds_var}[{c}], errors='coerce').clip(lower=0)")
    elif action == "replace_values":
        lines.append(f"# Replace specific values on column {c} — define mapping in business_rules")
    elif action == "regex_replace":
        lines.append(f"{ds_var}[{c}] = {ds_var}[{c}].astype(str).str.replace(r'[^\\w\\s]', '', regex=True)")
    elif action == "validate_referential_integrity_or_stage":
        lines.append(f"# Referential integrity involving {c} — validate in staging/warehouse")
    else:
        lines.append(f"# Unsupported in codegen v1: {action} ({c})")
    return lines


def generate_python_etl(plan: Dict[str, Any], assessment: Dict[str, Any]) -> str:
    """Generate one transform_* function per dataset in the plan."""
    _ = assessment  # reserved for future: dtypes / paths from assessment
    plan_id = str(plan.get("plan_id") or "unknown")
    business_rules: Dict[str, Any] = plan.get("business_rules") or {}
    valid_values_rules: Dict[str, List[Any]] = business_rules.get("valid_values") or {}
    non_nullable_cols: List[str] = business_rules.get("non_nullable") or []
    required_columns: List[str] = business_rules.get("required_columns") or []

    lines: List[str] = [
        '"""',
        f"ETL generated by Agent Dhara — plan_id={plan_id}",
        "Review manual_review items before running in production.",
        '"""',
        "from __future__ import annotations",
        "",
        "import sys",
        "import pandas as pd",
        "import logging",
        "",
        "logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')",
        "",
    ]

    notes_raw = business_rules.get("notes") or ""
    notes = "\n".join(
        line for line in str(notes_raw).strip().splitlines() if line.strip()
    )
    if notes:
        lines.extend(["# Business notes (from user):", "# " + notes.replace("\n", "\n# "), ""])

    manual = plan.get("manual_review") or []
    if manual:
        lines.append("# Manual review items (resolve before production):")
        for item in manual:
            ds = item.get("dataset") or "?"
            col = item.get("column") or "?"
            msg = item.get("message") or item.get("guidance") or ""
            lines.append(f"#   [{ds}] {col}: {msg}")
        lines.append("")

    ds_plan = plan.get("datasets") or {}
    for ds_name, block in ds_plan.items():
        fn = f"transform_{_safe_ident(ds_name)}"
        lines.append(f"def {fn}(df: pd.DataFrame) -> pd.DataFrame:")
        lines.append(f'    """Clean transforms for dataset: {ds_name}"""')
        ds_var = "out"
        lines.append(f"    {ds_var} = df.copy()")

        # --- Runtime guard: required columns must exist before any transform ---
        if required_columns:
            lines.append("")
            lines.append("    # Required-column guard (business rule)")
            lines.append(f"    _required = {repr(required_columns)}")
            lines.append(f"    _missing = [c for c in _required if c not in {ds_var}.columns]")
            lines.append("    if _missing:")
            lines.append(f"        logging.error(f'Required columns missing in {ds_name}: {{_missing}}')")
            lines.append("        raise ValueError(f\"Required columns missing: {_missing}\"  )")

        # --- Runtime guard: non_nullable columns must not be all-null ---
        if non_nullable_cols:
            lines.append("")
            lines.append("    # Non-nullable guard (business rule)")
            for col in non_nullable_cols:
                c = _col_expr(col)
                lines.append(f"    if {c} in {ds_var}.columns and {ds_var}[{c}].isna().all():")
                lines.append(f"        logging.error(f'Column {col} in {ds_name} is entirely null')")
                lines.append(f"        raise ValueError(\"Column {col} is entirely null — violates non_nullable rule\")")

        lines.append("")
        steps = sorted(block.get("steps") or [], key=lambda x: int(x.get("order") or 0))
        if not steps:
            lines.append("    # No auto-fixable steps found — check manual_review items above")
        for st in steps:
            action = str(st.get("action") or "")
            note = st.get("note")
            if note:
                lines.append(f"    # Note: {note}")
            # Log destructive or major actions
            if action in ("exclude_column", "drop_nulls", "deduplicate", "cast_type"):
                lines.append(f"    logging.info(f'Applying {action} to {st.get('column')} in {ds_name}')")
            for sl in _emit_step(action, st.get("column"), ds_var):
                lines.append(f"    {sl}")

        # --- Exclude columns: drop unwanted columns from output ---
        exclude_cols = business_rules.get("exclude_columns") or []
        if exclude_cols:
            lines.append("")
            lines.append("    # Exclude columns (business rule): remove unwanted columns from output")
            exclude_cols_exist = [c for c in exclude_cols if c]
            if exclude_cols_exist:
                lines.append(f"    _exclude = {repr(exclude_cols_exist)}")
                lines.append(f"    _to_drop = [c for c in _exclude if c in {ds_var}.columns]")
                lines.append(f"    if _to_drop:")
                lines.append(f"        logging.info(f'Dropping excluded columns in {ds_name}: {{_to_drop}}')")
                lines.append(f"        {ds_var} = {ds_var}.drop(columns=_to_drop)")

        # --- valid_values filter: drop rows with disallowed values ---
        if valid_values_rules:
            lines.append("")
            lines.append("    # Valid-values filter (business rule): removes rows with disallowed values")
            for col, allowed in valid_values_rules.items():
                c = _col_expr(col)
                lines.append(f"    if {c} in {ds_var}.columns:")
                lines.append(f"        _allowed_{_safe_ident(col)} = {repr(list(allowed))}")
                lines.append(f"        _before = len({ds_var})")
                lines.append(f"        {ds_var} = {ds_var}[{ds_var}[{c}].isin(_allowed_{_safe_ident(col)}) | {ds_var}[{c}].isna()]")
                lines.append(f"        _dropped = _before - len({ds_var})")
                lines.append(f"        if _dropped > 0:")
                lines.append(f"            logging.info(f'Dropped {{_dropped}} rows in {ds_name} due to valid_values rule on {col}')")

        lines.append(f"    return {ds_var}")
        lines.append("")

    if not ds_plan:
        lines.append("# No datasets found in plan — ensure assessment has datasets with DQ issues.")
        lines.append("")

    lines.append("DATASET_NAMES = " + repr(list(ds_plan.keys())))
    lines.append("")

    # --- HOW TO USE: copy-paste ready entry point ---
    lines.append("# ─────────────────────────────────────────────────────────────")
    lines.append("# HOW TO USE — copy-paste the block below into your pipeline")
    lines.append("# ─────────────────────────────────────────────────────────────")
    lines.append("if __name__ == '__main__':")
    if ds_plan:
        first_ds = next(iter(ds_plan))
        fn_first = f"transform_{_safe_ident(first_ds)}"
        lines.append(f"    # Example: run the transform for '{first_ds}'")
        lines.append(f"    import pandas as pd")
        lines.append(f"    df_raw = pd.read_csv('your_input_file.csv')   # ← replace with your source")
        lines.append(f"    df_clean = {fn_first}(df_raw)")
        lines.append(f"    df_clean.to_csv('output_cleaned.csv', index=False)")
        lines.append(f"    print(f'Done — {{len(df_clean)}} rows written to output_cleaned.csv')")
        if len(ds_plan) > 1:
            lines.append(f"")
            lines.append(f"    # To run ALL datasets:")
            for ds_nm in list(ds_plan.keys()):
                fn = f"transform_{_safe_ident(ds_nm)}"
                lines.append(f"    # {fn}(pd.read_csv('{ds_nm}.csv')).to_csv('{ds_nm}_clean.csv', index=False)")
    else:
        lines.append("    print('No datasets were included in this plan.')")
    lines.append("")

    return "\n".join(lines)
