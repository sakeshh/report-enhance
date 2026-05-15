from __future__ import annotations

import re
from typing import Any, Dict, List


def normalize_business_rules(raw: Any) -> Dict[str, Any]:
    """
    Normalize UI / JSON payload into a canonical rules dict used by planner + codegen.

    Supported keys (all optional):
    - never_drop_rows: bool
    - required_columns: list[str] — global display names; matched case-insensitively per dataset
    - non_nullable: list[str] — columns that must not be nulled by transforms (best-effort)
    - exclude_columns: list[str] — skip any auto step touching these columns
    - valid_values: dict[str, list[str]] — column -> allowed values (metadata for future codegen)
    - notes: str — free-text business context (stored on plan, not auto-executed)
    """
    if not isinstance(raw, dict):
        raw = {}

    def _bool(v: Any, default: bool = False) -> bool:
        if isinstance(v, bool):
            return v
        if v in (1, "1", "true", "True", "yes", "on"):
            return True
        if v in (0, "0", "false", "False", "no", "off", ""):
            return False
        return default

    def _str_list(v: Any) -> List[str]:
        if v is None:
            return []
        if isinstance(v, str):
            parts = re.split(r"[\s,;]+", v.strip())
            return [p for p in parts if p]
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
        return []

    req = _str_list(raw.get("required_columns") or raw.get("requiredColumns"))
    excl = _str_list(raw.get("exclude_columns") or raw.get("excludeColumns"))
    nn = _str_list(raw.get("non_nullable") or raw.get("nonNullable"))

    vv = raw.get("valid_values") or raw.get("validValues")
    if not isinstance(vv, dict):
        vv = {}

    notes = raw.get("notes") or raw.get("business_notes") or ""
    notes = str(notes).strip() if notes else ""

    return {
        "never_drop_rows": _bool(raw.get("never_drop_rows") or raw.get("neverDropRows"), False),
        "required_columns": req,
        "non_nullable": [c.lower() for c in nn],
        "exclude_columns": sorted({c.lower() for c in excl}),
        "valid_values": {str(k).lower(): list(v) if isinstance(v, list) else [str(v)] for k, v in vv.items()},
        "notes": notes,
    }


def column_is_excluded(column: str | None, exclude: Any) -> bool:
    if not column or not exclude:
        return False
    ex = exclude if isinstance(exclude, (set, frozenset)) else set(exclude)
    return column.strip().lower() in ex
