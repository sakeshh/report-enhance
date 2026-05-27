import sys
import os

sys.path.insert(0, r"c:\Users\ssakesh\Downloads\DHARA-GX\Agent Dhara Backend")

from agent.etl_pipeline.validate_sql import validate_sql_basic

# Test 1: Redundant casting
bad_sql_cast = """
SELECT CAST(CAST(col AS VARCHAR(50)) AS VARCHAR(100)) FROM tbl;
"""
ok, issues = validate_sql_basic(bad_sql_cast)
print("Test 1 (Redundant cast) - OK:", not ok, "Issues:", issues)
assert not ok
assert "redundant double CAST" in issues[0]

# Test 2: Missing email validation
bad_sql_email = """
CREATE TABLE dbo.Customers_Clean ( Email VARCHAR(255) );
-- LTRIM/RTRIM/LOWER email but no pattern match validation
UPDATE #Staging SET Email = LOWER(LTRIM(RTRIM(Email)));
"""
ok, issues = validate_sql_basic(bad_sql_email)
print("Test 2 (Missing email validation) - OK:", not ok, "Issues:", issues)
assert not ok
assert "Email column detected but missing format check" in issues[0]

# Test 3: Missing phone symbol cleaning
bad_sql_phone = """
CREATE TABLE dbo.Customers_Clean ( Phone VARCHAR(255) );
UPDATE #Staging SET Phone = Phone;
"""
ok, issues = validate_sql_basic(bad_sql_phone)
print("Test 3 (Missing phone symbol cleaning) - OK:", not ok, "Issues:", issues)
assert not ok
assert "Phone column detected but missing symbol cleaning" in issues[0]

# Test 4: Missing date parsing
bad_sql_date = """
CREATE TABLE dbo.Orders_Clean ( OrderDate DATETIME );
UPDATE #Staging SET OrderDate = OrderDate;
"""
ok, issues = validate_sql_basic(bad_sql_date)
print("Test 4 (Missing date parsing) - OK:", not ok, "Issues:", issues)
assert not ok
assert "Date columns detected but missing TRY_CAST/TRY_CONVERT" in issues[0]

print("ALL STRICT VALIDATION CHECKS WORKED PERFECTLY!")
