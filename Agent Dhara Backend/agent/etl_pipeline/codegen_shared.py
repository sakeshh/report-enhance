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


def get_sql_cast_type(col_type: Optional[str], col_name: str) -> str:
    col_lower = col_name.lower()
    t = col_type.lower() if col_type else ""
    is_generic_string = not t or t in ("object", "string", "varchar", "nvarchar", "char", "text")
    
    # 1. Date/Time checks
    if not is_generic_string:
        if "datetime" in t or "timestamp" in t:
            return "DATETIME"
        if "date" in t:
            return "DATE"
        if "time" in t:
            return "TIME"
    else:
        if "date" in col_lower:
            if "time" in col_lower or "stamp" in col_lower or col_lower.endswith("_at"):
                return "DATETIME"
            return "DATE"
        if col_lower.endswith("_at") or "time" in col_lower or "stamp" in col_lower:
            return "DATETIME"

    # 2. Numeric checks
    if not is_generic_string:
        if "int" in t:
            return "BIGINT"
        if "float" in t or "real" in t or "double" in t:
            return "FLOAT"
        if "decimal" in t or "numeric" in t:
            if "amount" in col_lower or "price" in col_lower:
                return "DECIMAL(18,2)"
            return "DECIMAL(18,4)"
    else:
        if "amount" in col_lower or "price" in col_lower:
            return "DECIMAL(18,2)"
        if "quantity" in col_lower or "qty" in col_lower or "count" in col_lower:
            return "DECIMAL(18,4)"

    # 3. String columns checks
    if col_lower == "email":
        return "NVARCHAR(255)"
    if col_lower == "phone":
        return "NVARCHAR(50)"
    if any(x in col_lower for x in ("name", "city", "status", "state", "country", "zip", "postal", "category")):
        return "NVARCHAR(255)"

    return "NVARCHAR(MAX)"


def sql_fill_update_lines(
    table_sql: str,
    col_bracket: str,
    st: Dict[str, Any],
    *,
    dialect: str = "tsql",
    col_type: Optional[str] = None,
    default_values_to_seed: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """T-SQL/ANSI UPDATE lines for fill_nulls / fill_or_drop from step params."""
    p = step_params(st)
    strat = p.get("fill_strategy")
    fval = p.get("fill_value")
    col_safe = str(st.get("column") or "col").replace("'", "''")
    col_name = str(st.get("column") or "col")
    col_lower = col_safe.lower()
    lines: List[str] = []

    is_date = (col_type and ("date" in col_type.lower() or "time" in col_type.lower() or "timestamp" in col_type.lower())) or \
              ("date" in col_lower or "time" in col_lower or "stamp" in col_lower or col_lower.endswith("_at"))
    
    is_numeric = (col_type and ("int" in col_type.lower() or "float" in col_type.lower() or "double" in col_type.lower() or "decimal" in col_type.lower() or "numeric" in col_type.lower() or "real" in col_type.lower())) or \
                 ("amount" in col_lower or "price" in col_lower or "quantity" in col_lower or "qty" in col_lower or "count" in col_lower)

    if strat == "mean" and fval is None and dialect == "tsql":
        lines.append(
            f"UPDATE {table_sql} SET {col_bracket} = COALESCE({col_bracket}, "
            f"(SELECT AVG(CAST({col_bracket} AS FLOAT)) FROM {table_sql} WHERE {col_bracket} IS NOT NULL)) "
            f"WHERE {col_bracket} IS NULL;"
        )
    elif strat == "median" and fval is None and dialect == "tsql":
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
        tbl_key = table_sql.replace("[", "").replace("]", "")
        key = f"{tbl_key}.{col_name}"
        if default_values_to_seed is not None:
            default_values_to_seed[key] = fval
        
        sql_type = get_sql_cast_type(col_type, col_name)
        cast_func = "TRY_CAST" if dialect == "tsql" else "CAST"
        lines.append(
            f"UPDATE {table_sql} SET {col_bracket} = COALESCE({col_bracket}, (SELECT {cast_func}(default_value AS {sql_type}) FROM dbo.etl_default_values WHERE column_name = '{key}')) WHERE {col_bracket} IS NULL;"
        )
    else:
        if is_date:
            lines.append(
                f"UPDATE {table_sql} SET {col_bracket} = COALESCE({col_bracket}, '1900-01-01') WHERE {col_bracket} IS NULL;"
            )
        elif is_numeric:
            lines.append(
                f"UPDATE {table_sql} SET {col_bracket} = COALESCE({col_bracket}, 0) WHERE {col_bracket} IS NULL;"
            )
        else:
            empty_str = "N''" if dialect == "tsql" else "''"
            lines.append(
                f"UPDATE {table_sql} SET {col_bracket} = COALESCE({col_bracket}, {empty_str}) WHERE {col_bracket} IS NULL;"
            )
    return lines
