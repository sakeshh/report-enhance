from __future__ import annotations

import re
from typing import Any, Dict, List


def _brk(ident: str) -> str:
    """T-SQL style bracket quoting."""
    s = str(ident or "").replace("]", "]]")
    return f"[{s}]"


def generate_sql_etl(plan: Dict[str, Any], assessment: Dict[str, Any], *, dialect: str = "tsql") -> str:
    """
    Generate commented SQL scripts (T-SQL biased: UPDATE / TRY_CAST / QUOTENAME patterns).
    `dialect`: 'tsql' | 'ansi' (ansi uses portable comments only for risky bits).
    """
    _ = assessment
    dialect = (dialect or "tsql").lower()
    plan_id = str(plan.get("plan_id") or "unknown")
    lines: List[str] = [
        f"-- ETL SQL — Agent Dhara — plan_id={plan_id}",
        f"-- dialect={dialect} — review before executing against production.",
        "",
    ]
    notes = (plan.get("business_rules") or {}).get("notes") or ""
    if notes:
        lines.extend(["-- Business notes:", "-- " + str(notes).replace("\n", "\n-- "), ""])

    for ds_name, block in (plan.get("datasets") or {}).items():
        tbl = _brk(ds_name)
        lines.append(f"-- === dataset: {ds_name} ===")
        for st in sorted(block.get("steps") or [], key=lambda x: int(x.get("order") or 0)):
            col = st.get("column")
            action = str(st.get("action") or "")
            if not col:
                if action == "deduplicate":
                    lines.append(f"-- Deduplicate {tbl} (example: use ROW_NUMBER in CTE; verify keys first)")
                    lines.append(
                        f"-- ;WITH d AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY (SELECT NULL)) AS rn FROM {tbl}) DELETE FROM d WHERE rn > 1"
                    )
                continue
            c = _brk(str(col))
            if action == "trim":
                lines.append(f"UPDATE {tbl} SET {c} = LTRIM(RTRIM(CAST({c} AS NVARCHAR(MAX)))) WHERE {c} IS NOT NULL;")
            elif action in ("fill_or_drop", "fill_nulls_simple"):
                lines.append(f"UPDATE {tbl} SET {c} = COALESCE({c}, N'') WHERE {c} IS NULL;")
            elif action == "coerce_numeric":
                if dialect == "tsql":
                    lines.append(
                        f"UPDATE {tbl} SET {c} = TRY_CAST(CAST({c} AS NVARCHAR(MAX)) AS BIGINT) WHERE {c} IS NOT NULL;"
                    )
                else:
                    lines.append(f"-- CAST {c} to numeric (adjust type per engine)")
            elif action == "parse_dates":
                if dialect == "tsql":
                    lines.append(f"UPDATE {tbl} SET {c} = TRY_CONVERT(date, {c}, 120) WHERE {c} IS NOT NULL;")
                else:
                    lines.append(f"-- Parse dates for {c}")
            elif action == "sanitize_email":
                lines.append(f"UPDATE {tbl} SET {c} = LOWER(LTRIM(RTRIM(CAST({c} AS NVARCHAR(MAX))))) WHERE {c} IS NOT NULL;")
                lines.append(
                    f"UPDATE {tbl} SET {c} = NULL WHERE {c} IS NOT NULL AND CHARINDEX('@', CAST({c} AS NVARCHAR(MAX))) = 0;"
                )
            elif action == "normalize_phone":
                lines.append(
                    f"-- Phone cleanup for {tbl}.{c}: extend with regex UDF if you need digits-only."
                )
                lines.append(
                    f"UPDATE {tbl} SET {c} = REPLACE(REPLACE(REPLACE(REPLACE(CAST({c} AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'') "
                    f"WHERE {c} IS NOT NULL;"
                )
            elif action == "deduplicate":
                lines.append(f"-- Deduplicate {tbl} on {c} — add business key to PARTITION BY")
            else:
                lines.append(f"-- TODO: {action} on {tbl}.{c}")
        lines.append("")

    for st in plan.get("global_steps") or []:
        lines.append(f"-- global: {st.get('action')} {st.get('column') or ''}")

    return "\n".join(lines).strip() + "\n"
