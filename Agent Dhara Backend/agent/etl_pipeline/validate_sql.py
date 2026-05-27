from __future__ import annotations

import re
from typing import List, Tuple

_DANGEROUS = [
    (r"\bdrop\s+table\s+(?!.*\b\w*(?:_clean|_stg|temp_|_temp)\b|.*#)", "contains DROP TABLE on non-staging/clean table — remove for safety"),
    (r"\btruncate\s+table\s+(?!.*\b\w*(?:_clean|_stg|temp_|_temp)\b|.*#)", "contains TRUNCATE TABLE on non-staging/clean table — remove for safety"),
    (r"\bdelete\s+from\s+(?!.*\b\w*(?:_clean|_stg|_dedup|temp_|etl_log|cte|_temp)\b|.*#)", "contains DELETE FROM on non-staging/clean table — remove for safety"),
]


def _bracket_balance(source: str) -> List[str]:
    issues: List[str] = []
    depth = 0
    for ch in source:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                issues.append("unbalanced parentheses")
                break
    if depth != 0:
        issues.append("unclosed parentheses")
    return issues


def validate_sql_basic_dict(source: str) -> dict:
    """Parse SQL and return structured validation result."""
    if not source or not source.strip():
        return {"valid": False, "error": "Empty SQL", "issues": ["empty sql"]}

    issues: List[str] = []
    low = source.lower()
    for pattern, msg in _DANGEROUS:
        if re.search(pattern, low):
            for line in low.splitlines():
                stripped = line.strip()
                if re.search(pattern, stripped) and not stripped.startswith("--"):
                    issues.append(msg)
                    break

    try:
        import sqlparse  # type: ignore

        parsed = sqlparse.parse(source)
        if not parsed or not parsed[0].tokens:
            return {"valid": False, "error": "Empty or unparseable SQL", "issues": ["Empty or unparseable SQL"]}
    except ImportError:
        pass  # structural checks only when sqlparse unavailable
    except Exception as e:
        return {"valid": False, "error": f"SQL validation error: {str(e)}", "issues": [str(e)]}

    issues.extend(_bracket_balance(source))
    issues.extend(_tsql_transaction_blocks(source))

    # Strict DQ Rules Validation
    # 1. Reject pipeline defined but not used
    if "etl_rejects" in low:
        pattern_rejects = r"insert\s+into\s+(?:dbo\s*\.\s*)?\[?etl_rejects\]?"
        if not re.search(pattern_rejects, low):
            issues.append("etl_rejects table is defined but never inserted into (reject pipeline not used)")
        
    # 2. Fake default values
    if "'99999'" in low or "'10120631.5'" in low or "'1900-01-01'" in low or "'19000101'" in low:
        issues.append("contains hardcoded fake default values ('99999', '10120631.5', or '1900-01-01')")
        
    # 3. Wrong deduplication ordering/columns
    if re.search(r"over\s*\([^\)]*etl_created_at", low):
        issues.append("deduplication partitions or orders by etl_created_at instead of a business column")
        
    # 4. SELECT DISTINCT * used for dedup
    if "select distinct *" in low:
        issues.append("contains SELECT DISTINCT * instead of key-aware CTE deduplication")
        
    # 5. Non-production safe SELECT INTO
    into_matches = re.finditer(r"\bselect\b(?:(?!insert|update|delete|create|procedure|\bgo\b|declare|begin|end|commit|rollback)\b[\s\S])*?\binto\s+([\w\.\_\[\]#]+)", low)
    for match in into_matches:
        tbl = match.group(1).strip("[]")
        if not tbl.startswith("#") and not any(x in tbl for x in ("temp_", "staging", "log", "watermark", "reject")):
            issues.append(f"contains SELECT INTO on clean/joined table '{tbl}' instead of CREATE VIEW or INSERT INTO")

    # 6. Destructive multi-column NULL update (data wipe pattern)
    if re.search(r"set\s+[\w\.\_\[\]#]+\s*=\s*null\s*,\s*[\w\.\_\[\]#]+\s*=\s*null", low):
        issues.append("contains destructive multi-column NULL update statement (data wipe pattern)")

    # 7. Redundant/double casting
    if re.search(r"cast\(\s*(?:try_)?cast\(", low) or re.search(r"try_cast\(\s*(?:try_)?cast\(", low):
        issues.append("contains redundant double CAST statements (e.g. CAST(CAST(...)))")
    if re.search(r"lower\(\s*cast\(\s*(?:ltrim|rtrim|replace|lower|upper|coalesce)", low):
        issues.append("contains redundant nested CAST operations inside LOWER/LTRIM string wrappers")
        
    # 8. Email validation constraint check
    if "email" in low:
        if not any(pat in low for pat in ("%_@_%._%", "%_@_%._%")):
            issues.append("Email column detected but missing format check constraint (e.g. Email LIKE '%_@_%._%')")
            
    # 9. Phone normalization & validation check
    if "phone" in low:
        if "replace" not in low:
            issues.append("Phone column detected but missing symbol cleaning operations (nested REPLACE for spaces/dashes)")
        if not any(x in low for x in ("len(", "length(", "[^0-9]")):
            issues.append("Phone column detected but missing validation checks (length >= 7 or only numeric digits)")

    # 10. Date parsing checks for OrderDate / CreatedDate
    if "orderdate" in low or "createddate" in low:
        if not any(x in low for x in ("try_convert(", "try_cast(", "to_date(", "to_datetime(")):
            issues.append("Date columns detected but missing TRY_CAST/TRY_CONVERT date parsing or validation")

    if issues:
        return {"valid": False, "issues": issues}
    return {"valid": True, "issues": []}


def _tsql_transaction_blocks(source: str) -> List[str]:
    """Warn when BEGIN TRY appears without END CATCH (cheap structural check)."""
    low = source.lower()
    issues: List[str] = []
    if "begin try" in low and "end catch" not in low:
        issues.append("BEGIN TRY without matching END CATCH")
        
    has_begin_tran = "begin tran" in low or "begin transaction" in low
    has_commit = "commit" in low or "commit transaction" in low
    has_rollback = "rollback" in low or "rollback transaction" in low
    
    if has_begin_tran and not has_commit:
        issues.append("BEGIN TRANSACTION without COMMIT TRANSACTION")
    if (has_commit or has_rollback) and not has_begin_tran:
        issues.append("COMMIT/ROLLBACK TRANSACTION without BEGIN TRANSACTION")
    return issues


def validate_sql_basic(source: str) -> Tuple[bool, List[str]]:
    result = validate_sql_basic_dict(source)
    if result.get("valid"):
        return True, []
    errs = list(result.get("issues") or [])
    if result.get("error") and not errs:
        errs.append(str(result["error"]))
    return False, errs
