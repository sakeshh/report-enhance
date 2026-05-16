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
    business_rules: Dict[str, Any] = plan.get("business_rules") or {}
    excluded_columns: List[str] = business_rules.get("exclude_columns") or []

    lines: List[str] = [
        f"-- ETL SQL — Agent Dhara — plan_id={plan_id}",
        f"-- dialect={dialect} — review before executing against production.",
        "",
    ]

    notes_raw = business_rules.get("notes") or ""
    notes = "\n".join(
        line for line in str(notes_raw).strip().splitlines() if line.strip()
    )
    if notes:
        lines.extend(["-- Business notes:", "-- " + notes.replace("\n", "\n-- "), ""])

    # Emit excluded columns as an audit block so the user knows what was skipped
    if excluded_columns:
        lines.append(f"-- Excluded columns (business rule — no transforms generated for these):")
        for col in excluded_columns:
            lines.append(f"--   [{col}]")
        lines.append("")

    manual = plan.get("manual_review") or []
    if manual:
        lines.append(f"-- ⚠ {len(manual)} item(s) flagged for manual review before production run.")
        for item in manual:
            ds = item.get("dataset") or "?"
            col = item.get("column") or "?"
            msg = (item.get("message") or "")[:200]
            lines.append(f"--   [{ds}] {col}: {msg}")
        lines.append("")

    ds_plan = plan.get("datasets") or {}

    # Emit reusable stored procedure for IQR outlier flagging (DRY pattern)
    has_outlier_steps = any(
        any(str(st.get("action") or "") in ("flag_outliers", "clip_or_flag") for st in (b.get("steps") or []))
        for b in ds_plan.values()
    )
    if has_outlier_steps and dialect == "tsql":
        lines.append("-- ============================================================")
        lines.append("-- Reusable stored procedure: IQR outlier flagging")
        lines.append("-- Usage: EXEC sp_flag_outliers_iqr 'dbo.Orders_Raw', 'CustomerID'")
        lines.append("-- ============================================================")
        lines.append("IF OBJECT_ID('sp_flag_outliers_iqr', 'P') IS NOT NULL DROP PROCEDURE sp_flag_outliers_iqr;")
        lines.append("GO")
        lines.append("CREATE PROCEDURE sp_flag_outliers_iqr")
        lines.append("    @table_name NVARCHAR(256),")
        lines.append("    @column_name NVARCHAR(256)")
        lines.append("AS BEGIN")
        lines.append("    SET NOCOUNT ON;")
        lines.append("    DECLARE @flag_col NVARCHAR(270) = @column_name + N'_outlier_flagged';")
        lines.append("    DECLARE @sql NVARCHAR(MAX);")
        lines.append("")
        lines.append("    -- Add flag column if missing")
        lines.append("    SET @sql = N'IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(''' + @table_name + ''') AND name = ''' + @flag_col + ''')'")
        lines.append("        + N' ALTER TABLE ' + QUOTENAME(PARSENAME(@table_name,2)) + N'.' + QUOTENAME(PARSENAME(@table_name,1))")
        lines.append("        + N' ADD ' + QUOTENAME(@flag_col) + N' BIT NOT NULL DEFAULT 0;';")
        lines.append("    EXEC sp_executesql @sql;")
        lines.append("")
        lines.append("    -- Compute IQR and flag")
        lines.append("    SET @sql = N'DECLARE @q1 FLOAT, @q3 FLOAT; '")
        lines.append("        + N'SELECT @q1 = PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ' + QUOTENAME(@column_name) + N'), '")
        lines.append("        + N'       @q3 = PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ' + QUOTENAME(@column_name) + N') '")
        lines.append("        + N'FROM ' + QUOTENAME(PARSENAME(@table_name,2)) + N'.' + QUOTENAME(PARSENAME(@table_name,1))")
        lines.append("        + N' WHERE ' + QUOTENAME(@column_name) + N' IS NOT NULL; '")
        lines.append("        + N'UPDATE ' + QUOTENAME(PARSENAME(@table_name,2)) + N'.' + QUOTENAME(PARSENAME(@table_name,1))")
        lines.append("        + N' SET ' + QUOTENAME(@flag_col) + N' = CASE'")
        lines.append("        + N' WHEN ' + QUOTENAME(@column_name) + N' < (@q1 - 1.5 * (@q3 - @q1)) THEN 1'")
        lines.append("        + N' WHEN ' + QUOTENAME(@column_name) + N' > (@q3 + 1.5 * (@q3 - @q1)) THEN 1'")
        lines.append("        + N' ELSE 0 END'")
        lines.append("        + N' WHERE ' + QUOTENAME(@column_name) + N' IS NOT NULL;';")
        lines.append("    EXEC sp_executesql @sql;")
        lines.append("END;")
        lines.append("GO")
        lines.append("")

    if not ds_plan:
        lines.append("-- No datasets found in plan.")

    for ds_name, block in ds_plan.items():
        tbl = _brk(ds_name)
        lines.append(f"-- === dataset: {ds_name} ===")
        if dialect == "tsql":
            lines.append(f"BEGIN TRY")
            lines.append(f"    BEGIN TRAN;")
        steps = sorted(block.get("steps") or [], key=lambda x: int(x.get("order") or 0))
        if not steps:
            lines.append(f"-- No auto-fixable steps for {ds_name}.")
        for st in steps:
            col = st.get("column")
            action = str(st.get("action") or "")
            note = st.get("note")
            if note:
                lines.append(f"-- Note: {note}")
            if not col:
                if action == "deduplicate":
                    lines.append(f"-- Deduplicate {tbl} (example: use ROW_NUMBER in CTE; verify keys first)")
                    lines.append(
                        f";WITH d AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY (SELECT NULL)) AS rn FROM {tbl})"
                    )
                    lines.append(f"DELETE FROM d WHERE rn > 1;")
                continue
            # Skip if this column was excluded by business rules
            if col in excluded_columns:
                lines.append(f"-- [EXCLUDED] {tbl}.[{col}] — skipped by exclude_columns business rule")
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
                    lines.append(f"-- Parse dates for {c} (adjust to your engine's date parse function)")
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
            elif action == "lowercase":
                lines.append(f"UPDATE {tbl} SET {c} = LOWER(CAST({c} AS NVARCHAR(MAX))) WHERE {c} IS NOT NULL;")
            elif action == "uppercase":
                lines.append(f"UPDATE {tbl} SET {c} = UPPER(CAST({c} AS NVARCHAR(MAX))) WHERE {c} IS NOT NULL;")
            elif action == "cast_type":
                # Safe cast: add new typed column, populate, then swap
                col_clean = str(col).replace("'", "''")
                if dialect == "tsql":
                    lines.append(f"-- Cast {c} from float to integer (safe add-column approach)")
                    lines.append(f"IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('{ds_name}') AND name = '{col_clean}_int')")
                    lines.append(f"    ALTER TABLE {tbl} ADD [{col_clean}_int] BIGINT;")
                    lines.append(f"UPDATE {tbl} SET [{col_clean}_int] = TRY_CAST({c} AS BIGINT);")
                    lines.append(f"-- After verification, swap: ALTER TABLE {tbl} DROP COLUMN {c};")
                    lines.append(f"-- EXEC sp_rename '{ds_name}.{col_clean}_int', '{col_clean}', 'COLUMN';")
                else:
                    lines.append(f"-- ANSI: ALTER COLUMN {c} to INTEGER (adjust DDL per engine)")
            elif action in ("flag_outliers", "clip_or_flag"):
                # IQR-based outlier flagging using variables for performance
                col_clean = str(col).replace("'", "''")
                lines.append(f"-- Flag IQR outliers for {c}")
                if dialect == "tsql":
                    lines.append(f"IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('{ds_name}') AND name = '{col_clean}_outlier_flagged')")
                    lines.append(f"    ALTER TABLE {tbl} ADD [{col_clean}_outlier_flagged] BIT NOT NULL DEFAULT 0;")
                    lines.append(f"")
                    lines.append(f"DECLARE @q1_{col_clean} FLOAT, @q3_{col_clean} FLOAT;")
                    lines.append(f"SELECT @q1_{col_clean} = PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {c}),")
                    lines.append(f"       @q3_{col_clean} = PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {c})")
                    lines.append(f"FROM {tbl} WHERE {c} IS NOT NULL;")
                    lines.append(f"")
                    lines.append(f"UPDATE {tbl} SET [{col_clean}_outlier_flagged] = CASE")
                    lines.append(f"    WHEN {c} < (@q1_{col_clean} - 1.5 * (@q3_{col_clean} - @q1_{col_clean})) THEN 1")
                    lines.append(f"    WHEN {c} > (@q3_{col_clean} + 1.5 * (@q3_{col_clean} - @q1_{col_clean})) THEN 1")
                    lines.append(f"    ELSE 0 END")
                    lines.append(f"WHERE {c} IS NOT NULL;")
                else:
                    lines.append(f"-- ANSI: compute Q1/Q3 using PERCENTILE_CONT, then flag rows outside 1.5*IQR")
            elif action == "clip_outliers":
                col_clean = str(col).replace("'", "''")
                lines.append(f"-- Clip outliers for {c} to IQR bounds")
                if dialect == "tsql":
                    lines.append(f"DECLARE @q1_{col_clean} FLOAT, @q3_{col_clean} FLOAT;")
                    lines.append(f"SELECT @q1_{col_clean} = PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {c}),")
                    lines.append(f"       @q3_{col_clean} = PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {c})")
                    lines.append(f"FROM {tbl} WHERE {c} IS NOT NULL;")
                    lines.append(f"")
                    lines.append(f"UPDATE {tbl} SET {c} = CASE")
                    lines.append(f"    WHEN {c} < (@q1_{col_clean} - 1.5 * (@q3_{col_clean} - @q1_{col_clean})) THEN @q1_{col_clean} - 1.5 * (@q3_{col_clean} - @q1_{col_clean})")
                    lines.append(f"    WHEN {c} > (@q3_{col_clean} + 1.5 * (@q3_{col_clean} - @q1_{col_clean})) THEN @q3_{col_clean} + 1.5 * (@q3_{col_clean} - @q1_{col_clean})")
                    lines.append(f"    ELSE {c} END")
                    lines.append(f"WHERE {c} IS NOT NULL;")
                else:
                    lines.append(f"-- ANSI: clip {c} to [Q1 - 1.5*IQR, Q3 + 1.5*IQR]")
            elif action == "cap_outliers":
                col_clean = str(col).replace("'", "''")
                lines.append(f"-- Cap outliers for {c} with median replacement")
                if dialect == "tsql":
                    lines.append(f"DECLARE @q1_{col_clean} FLOAT, @q3_{col_clean} FLOAT, @median_{col_clean} FLOAT;")
                    lines.append(f"SELECT @q1_{col_clean} = PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {c}),")
                    lines.append(f"       @q3_{col_clean} = PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {c}),")
                    lines.append(f"       @median_{col_clean} = PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {c})")
                    lines.append(f"FROM {tbl} WHERE {c} IS NOT NULL;")
                    lines.append(f"")
                    lines.append(f"UPDATE {tbl} SET {c} = @median_{col_clean}")
                    lines.append(f"WHERE {c} < (@q1_{col_clean} - 1.5 * (@q3_{col_clean} - @q1_{col_clean}))")
                    lines.append(f"   OR {c} > (@q3_{col_clean} + 1.5 * (@q3_{col_clean} - @q1_{col_clean}));")
                else:
                    lines.append(f"-- ANSI: replace outliers in {c} with median value")
            elif action == "standardize_boolean":
                lines.append(f"UPDATE {tbl} SET {c} = CASE")
                lines.append(f"    WHEN LOWER(CAST({c} AS NVARCHAR(10))) IN ('1', 'true', 'yes', 'y', 't') THEN 1")
                lines.append(f"    ELSE 0 END")
                lines.append(f"WHERE {c} IS NOT NULL;")
            elif action == "zero_to_null":
                lines.append(f"UPDATE {tbl} SET {c} = NULL WHERE {c} = 0;")
            elif action == "range_clip":
                lines.append(f"UPDATE {tbl} SET {c} = CASE WHEN TRY_CAST({c} AS FLOAT) < 0 THEN 0 ELSE TRY_CAST({c} AS FLOAT) END WHERE {c} IS NOT NULL;")
            elif action == "deduplicate":
                lines.append(f"-- Deduplicate {tbl} on {c} -- add business key to PARTITION BY")
            else:
                lines.append(f"-- TODO: {action} on {tbl}.{c}")
        if dialect == "tsql":
            lines.append(f"    COMMIT;")
            lines.append(f"END TRY")
            lines.append(f"BEGIN CATCH")
            lines.append(f"    IF @@TRANCOUNT > 0 ROLLBACK;")
            lines.append(f"    THROW;")
            lines.append(f"END CATCH;")
        lines.append("")

    for st in plan.get("global_steps") or []:
        lines.append(f"-- global: {st.get('action')} {st.get('column') or ''}")

    return "\n".join(lines).strip() + "\n"
