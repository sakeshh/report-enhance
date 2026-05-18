from __future__ import annotations

import re
from typing import List, Tuple

_DANGEROUS = [
    (r"\bdrop\s+table\b", "contains DROP TABLE — remove for safety"),
    (r"\btruncate\s+table\b", "contains TRUNCATE TABLE — remove for safety"),
    (r"\bdelete\s+from\b", "contains DELETE FROM — remove for safety"),
]


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
        return {"valid": False, "error": "sqlparse not installed", "issues": ["sqlparse not installed"]}
    except Exception as e:
        return {"valid": False, "error": f"SQL validation error: {str(e)}", "issues": [str(e)]}

    if issues:
        return {"valid": False, "issues": issues}
    return {"valid": True, "issues": []}


def validate_sql_basic(source: str) -> Tuple[bool, List[str]]:
    result = validate_sql_basic_dict(source)
    if result.get("valid"):
        return True, []
    errs = list(result.get("issues") or [])
    if result.get("error") and not errs:
        errs.append(str(result["error"]))
    return False, errs
