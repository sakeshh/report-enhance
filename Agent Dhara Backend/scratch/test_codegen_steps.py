import sys
import os

sys.path.insert(0, r"c:\Users\ssakesh\Downloads\DHARA-GX\Agent Dhara Backend")

from agent.session_store import load_session
from agent.etl_pipeline.sql_codegen import _classify_column, compile_column_expression

session_id = "5881a9d5-c5fe-4e17-b50a-bb18b358ae2b"
session = load_session(session_id)
context = session.get("context", {})
etl_flow = context.get("etl_flow", {})
plan = etl_flow.get("approved_plan") or etl_flow.get("plan") or {}
assessment = context.get("last_assessment_result") or {}

# Test sql_codegen behavior on Orders
ds_name = "dbo.Orders_Raw"
block = plan.get("datasets", {}).get(ds_name, {})
steps = block.get("steps", [])

print("All steps for dbo.Orders_Raw in plan:")
for st in steps:
    print(f"  Col: {st.get('column')}, Action: {st.get('action')}")

ds_info = assessment.get("datasets", {}).get(ds_name) or {}
cols_info = ds_info.get("columns") or {}

filtered_steps = []
seen_operations = set()

for st in steps:
    action = str(st.get("action") or "").strip().lower()
    col = st.get("column")
    
    # Type-aware filtering for string operations
    if action in ("trim", "lowercase", "uppercase", "sanitize_email"):
        if col:
            col_meta = cols_info.get(col) or {}
            col_class = _classify_column(col, col_meta)
            if col_class in ("metric", "date"):
                print(f"Skipping string operation {action} on {col} (class={col_class})")
                continue
                
    norm_action = action
    if action in ("clip_or_flag", "flag_outliers"):
        norm_action = "flag_outliers"
    elif action in ("fill_nulls_simple", "fill_or_drop"):
        norm_action = "fill_or_drop"
        
    op_key = (norm_action, str(col).lower() if col else None)
    if op_key in seen_operations:
        print(f"Skipping duplicate operation {op_key}")
        continue
    seen_operations.add(op_key)
    filtered_steps.append(st)

print("Filtered steps for dbo.Orders_Raw:")
for st in filtered_steps:
    print(f"  Col: {st.get('column')}, Action: {st.get('action')}")
