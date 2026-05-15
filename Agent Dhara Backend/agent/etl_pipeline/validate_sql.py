from __future__ import annotations

import re
from typing import List, Tuple


def validate_sql_basic(source: str) -> Tuple[bool, List[str]]:
    errs: List[str] = []
    if not source or not source.strip():
        return False, ["empty sql"]
    low = source.lower()
    for bad in ("drop table", "truncate table", "delete from", "alter table"):
        if bad in low and not low.strip().startswith("--"):
            # allow commented DELETE hints only
            pass
    if re.search(r"\bdrop\s+table\b", low):
        errs.append("contains DROP TABLE — remove for safety")
    if re.search(r"\btruncate\s+table\b", low):
        errs.append("contains TRUNCATE — remove for safety")
    try:
        import sqlparse  # type: ignore

        sqlparse.parse(source)
    except Exception:
        pass
    if errs:
        return False, errs
    return True, []
