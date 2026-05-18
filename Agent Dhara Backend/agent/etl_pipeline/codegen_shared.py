"""Shared helpers for reading plan step params across codegen engines."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def step_params(st: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not st:
        return {}
    p = st.get("params")
    return dict(p) if isinstance(p, dict) else {}


def evidence_dict(st: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not st:
        return {}
    ev = st.get("evidence")
    if isinstance(ev, dict):
        return ev
    ep = st.get("evidence_profile")
    return dict(ep) if isinstance(ep, dict) else {}


def outlier_multiplier(params: Dict[str, Any]) -> float:
    try:
        return float(params.get("outlier_iqr_multiplier") or 1.5)
    except (TypeError, ValueError):
        return 1.5


def parse_schema_table(ds_name: str) -> tuple[str, str]:
    parts = str(ds_name or "dbo").split(".", 1)
    if len(parts) == 2:
        return parts[0].strip() or "dbo", parts[1].strip()
    return "dbo", parts[0].strip()


def tsql_qualified_name(ds_name: str) -> str:
    schema, table = parse_schema_table(ds_name)
    return f"[{schema.replace(']', ']]')}].[{table.replace(']', ']]')}]"


def plan_actions(plan: Dict[str, Any]) -> List[str]:
    actions: List[str] = []
    for block in (plan.get("datasets") or {}).values():
        for st in (block or {}).get("steps") or []:
            actions.append(str(st.get("action") or ""))
    return actions


def sql_fill_update_lines(
    table_sql: str,
    col_bracket: str,
    st: Dict[str, Any],
    *,
    dialect: str = "tsql",
) -> List[str]:
    """T-SQL/ANSI UPDATE lines for fill_nulls / fill_or_drop from step params."""
    p = step_params(st)
    strat = p.get("fill_strategy")
    fval = p.get("fill_value")
    col_safe = str(st.get("column") or "col").replace("'", "''")
    lines: List[str] = []

    if strat == "median" and fval is not None:
        lines.append(f"UPDATE {table_sql} SET {col_bracket} = COALESCE({col_bracket}, {fval}) WHERE {col_bracket} IS NULL;")
    elif strat == "mean" and fval is not None:
        lines.append(f"UPDATE {table_sql} SET {col_bracket} = COALESCE({col_bracket}, {fval}) WHERE {col_bracket} IS NULL;")
    elif strat == "mean" and dialect == "tsql":
        lines.append(
            f"UPDATE {table_sql} SET {col_bracket} = COALESCE({col_bracket}, "
            f"(SELECT AVG(CAST({col_bracket} AS FLOAT)) FROM {table_sql} WHERE {col_bracket} IS NOT NULL)) "
            f"WHERE {col_bracket} IS NULL;"
        )
    elif strat == "median" and dialect == "tsql":
        var = f"@fill_{col_safe.replace('.', '_')[:40]}"
        lines.append(f"DECLARE {var} FLOAT;")
        lines.append(
            f"SELECT {var} = PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_bracket}) "
            f"FROM {table_sql} WHERE {col_bracket} IS NOT NULL;"
        )
        lines.append(
            f"UPDATE {table_sql} SET {col_bracket} = COALESCE({col_bracket}, {var}) WHERE {col_bracket} IS NULL;"
        )
    elif fval is not None:
        if isinstance(fval, (int, float)):
            lines.append(
                f"UPDATE {table_sql} SET {col_bracket} = COALESCE({col_bracket}, {fval}) WHERE {col_bracket} IS NULL;"
            )
        else:
            fv = str(fval).replace("'", "''")
            lines.append(
                f"UPDATE {table_sql} SET {col_bracket} = COALESCE({col_bracket}, N'{fv}') WHERE {col_bracket} IS NULL;"
            )
    else:
        lines.append(
            f"UPDATE {table_sql} SET {col_bracket} = COALESCE({col_bracket}, N'') WHERE {col_bracket} IS NULL;"
        )
    return lines
