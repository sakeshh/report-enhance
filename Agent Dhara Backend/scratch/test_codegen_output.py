import sys
import os

sys.path.insert(0, r"c:\Users\ssakesh\Downloads\DHARA-GX\Agent Dhara Backend")

from agent.session_store import load_session
from agent.etl_pipeline.sql_codegen import _classify_column, compile_column_expression, _get_clean_table_name, _brk

session_id = "5881a9d5-c5fe-4e17-b50a-bb18b358ae2b"
session = load_session(session_id)
context = session.get("context", {})
etl_flow = context.get("etl_flow", {})
plan = etl_flow.get("approved_plan") or etl_flow.get("plan") or {}
assessment = context.get("last_assessment_result") or {}

ds_name = "dbo.Orders_Raw"
block = plan.get("datasets", {}).get(ds_name, {})
steps = block.get("steps", [])

# Let's run a simplified version of sql_codegen loop to see intermediate variables
ds_info = assessment.get("datasets", {}).get(ds_name) or {}
cols_info = ds_info.get("columns") or {}
excluded_columns = []
local_excluded_columns = set()

# Deduplicate steps
filtered_steps = []
seen_operations = set()
for st in steps:
    action = str(st.get("action") or "").strip().lower()
    col = st.get("column")
    if action in ("trim", "lowercase", "uppercase", "sanitize_email"):
        if col:
            col_meta = cols_info.get(col) or {}
            col_class = _classify_column(col, col_meta)
            if col_class in ("metric", "date"):
                continue
    norm_action = action
    if action in ("clip_or_flag", "flag_outliers"):
        norm_action = "flag_outliers"
    elif action in ("fill_nulls_simple", "fill_or_drop"):
        norm_action = "fill_or_drop"
    op_key = (norm_action, str(col).lower() if col else None)
    if op_key in seen_operations:
        continue
    seen_operations.add(op_key)
    filtered_steps.append(st)

priority = {
    "trim": 10, "lowercase": 11, "uppercase": 11, "sanitize_email": 12,
    "coerce_numeric": 20, "cast_type": 21, "zero_to_null": 30,
    "fill_or_drop": 40, "fill_nulls_simple": 40, "parse_dates": 50,
    "regex_replace": 60, "replace_values": 61, "standardize_boolean": 62,
    "normalize_phone": 63, "hash_phone": 64, "mask_phone": 65,
    "range_clip": 70, "clip_or_flag": 71, "flag_outliers": 72,
    "clip_outliers": 73, "cap_outliers": 74, "deduplicate": 80,
    "validate_referential_integrity_or_stage": 90
}
steps = sorted(filtered_steps, key=lambda x: priority.get(str(x.get("action") or "").strip().lower(), 99))

pk_col = "OrderID"
never_drop = False
step_lines = []

print("Running Step Generation Loop for Orders_Raw:")
# Phase 1: Validations and Quarantines
for st in steps:
    col = st.get("column")
    action = str(st.get("action") or "")
    c = _brk(col)
    if action == "parse_dates":
        print(f"Adding parse_dates quarantine for {col} to step_lines")
        step_lines.append(f"-- Quarantine invalid dates from staging to rejects")
        step_lines.append(f"DELETE FROM staging WHERE {c} IS NOT NULL...")

# Phase 2: Expression-Based Transformations
column_transforms = {}
for st in steps:
    col = st.get("column")
    action = st.get("action")
    if action in ("trim", "lowercase", "uppercase", "sanitize_email", "normalize_phone", "hash_phone", "mask_phone", "standardize_boolean", "regex_replace", "range_clip", "coerce_numeric", "cast_type", "parse_dates", "replace_values"):
        if col not in column_transforms:
            column_transforms[col] = []
        column_transforms[col].append(st)

update_clauses = []
for col_name, col_steps in column_transforms.items():
    print(f"Col: {col_name}, Steps in transform: {[s.get('action') for s in col_steps]}")
    base_steps = [st for st in col_steps if st.get("action") != "cast_type"]
    col_meta = cols_info.get(col_name) or {}
    compiled_expr = compile_column_expression(col_name, base_steps, col_meta, {})
    update_clauses.append(f"[{col_name}] = {compiled_expr}")
    print(f"Compiled expr for {col_name}: {compiled_expr}")

if update_clauses:
    print("Update clauses are populated!")
else:
    print("WARNING: Update clauses are empty!")
