import sys
import os

sys.path.insert(0, r"c:\Users\ssakesh\Downloads\DHARA-GX\Agent Dhara Backend")

from agent.session_store import load_session
from agent.etl_pipeline.sql_codegen import generate_sql_etl

session_id = "5881a9d5-c5fe-4e17-b50a-bb18b358ae2b"
session = load_session(session_id)
context = session.get("context", {})
etl_flow = context.get("etl_flow", {})
plan = etl_flow.get("approved_plan") or etl_flow.get("plan") or {}
assessment = context.get("last_assessment_result") or {}

sql_code = generate_sql_etl(plan, assessment, dialect="tsql")

with open(r"c:\Users\ssakesh\Downloads\DHARA-GX\Agent Dhara Backend\scratch\sql_raw_output.sql", "w", encoding="utf-8") as f:
    f.write(sql_code)
print("SQL written successfully to scratch/sql_raw_output.sql")
