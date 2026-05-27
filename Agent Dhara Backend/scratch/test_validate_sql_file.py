import sys
sys.path.insert(0, r"c:\Users\ssakesh\Downloads\DHARA-GX\Agent Dhara Backend")
from agent.etl_pipeline.validate_sql import validate_sql_basic

sql_path = r"c:\Users\ssakesh\Downloads\DHARA-GX\Agent Dhara Backend\output\etl_code\5881a9d5-c5fe-4e17-b50a-bb18b358ae2b\etl_plan_1779779734_sql_v6_1779779781.sql"
with open(sql_path, "r", encoding="utf-8") as f:
    sql = f.read()

ok, errs = validate_sql_basic(sql)
print("Validation OK:", ok)
print("Errors:", errs)
