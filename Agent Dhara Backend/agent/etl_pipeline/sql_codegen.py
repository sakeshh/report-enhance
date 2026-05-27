from __future__ import annotations

import re
from typing import Any, Dict, List

from agent.etl_pipeline.codegen_shared import (
    get_sql_cast_type,
    outlier_multiplier,
    sql_fill_update_lines,
    step_params,
    tsql_qualified_name,
)
from agent.etl_pipeline.join_emitters import emit_sql_joins


def _brk(ident: str) -> str:
    """T-SQL style bracket quoting."""
    s = str(ident or "").replace("]", "]]")
    return f"[{s}]"


def _get_clean_table_name(ds_name: str) -> str:
    """Resolve raw table name to its clean table equivalent."""
    parts = ds_name.split(".", 1)
    if len(parts) == 2:
        schema, tbl_name = parts[0], parts[1]
    else:
        schema, tbl_name = "dbo", ds_name
    
    schema = schema.strip("[]")
    tbl_name = tbl_name.strip("[]")
    
    if tbl_name.lower().endswith("_raw"):
        clean_tbl = tbl_name[:-4] + "_Clean"
    else:
        clean_tbl = tbl_name + "_Clean"
        
    return f"{schema}.{clean_tbl}"


def _indent(lines_list: List[str], spaces: int = 8) -> List[str]:
    """Prefix all non-empty lines with indentation spaces."""
    prefix = " " * spaces
    return [prefix + line if line.strip() else line for line in lines_list]


def _classify_column(col_name: str, col_meta: dict) -> str:
    """
    Classify column as 'date', 'id', 'metric', 'categorical', or 'string'.
    """
    # 0. Check approved semantic_type first if available
    approved_tag = (col_meta.get("semantic_type") or "").lower().strip()
    if approved_tag in ("id", "metric", "categorical", "date", "text"):
        if approved_tag == "text":
            return "string"
        return approved_tag

    c_lower = str(col_name).lower()
    
    # 1. Date checks
    dtype = str(col_meta.get("dtype") or col_meta.get("inferred_type") or "").lower()
    target_dtype = str(col_meta.get("target_dtype") or "").lower()
    
    if any(x in dtype for x in ("date", "time", "stamp")) or \
       any(x in target_dtype for x in ("date", "time", "stamp")):
        return "date"
    if any(x in c_lower for x in ("date", "time", "dob", "stamp")) or c_lower.endswith("_at"):
        return "date"
        
    # 2. ID / Identifier checks
    if any(x in c_lower for x in ("phone", "email", "ssn", "zip", "postal")):
        return "id"
    if c_lower.endswith("id") or c_lower.endswith("key") or c_lower.endswith("code") or c_lower.endswith("num"):
        return "id"
    if any(x in c_lower for x in ("student_id", "course_id", "instructor_id", "batch_id", "run_id")):
        return "id"
        
    # 3. Metric checks
    if any(x in dtype for x in ("int", "float", "double", "decimal", "numeric", "real")) or \
       any(x in target_dtype for x in ("int", "float", "double", "decimal", "numeric", "real")):
        return "metric"
    if any(x in c_lower for x in ("credit", "fee", "amount", "price", "quantity", "qty", "count", "score", "grade", "val")):
        return "metric"
            
    # 4. Categorical checks
    if any(x in c_lower for x in ("status", "gender", "category", "type", "state", "country", "city", "active", "flag")):
        return "categorical"
        
    # 5. String check/Fallback
    if any(x in dtype for x in ("char", "str", "object", "string", "text")):
        return "string"
        
    return "string"


def compile_column_expression(col_name: str, transforms: List[dict], col_meta: dict, business_rules: dict) -> str:
    # Start with the raw column identifier
    expr = f"[{col_name}]"
    
    def is_already_string(val: str) -> bool:
        v = val.strip().upper()
        return any(v.startswith(prefix) for prefix in (
            "LOWER(", "UPPER(", "LTRIM(", "RTRIM(", "REPLACE(", "CAST(", "TRY_CAST(", 
            "TRY_CONVERT(", "COALESCE(", "CONVERT(", "SUBSTRING(", "RIGHT(", "LEFT(", 
            "N'", "'"
        ))
        
    def _wrap_string_cast(val: str) -> str:
        if is_already_string(val):
            return val
        return f"CAST({val} AS NVARCHAR(MAX))"
        
    for st in transforms:
        action = st.get("action")
        col_class = _classify_column(col_name, col_meta)
        
        if action == "trim":
            if col_class not in ("metric", "date"):
                expr = f"LTRIM(RTRIM({_wrap_string_cast(expr)}))"
        elif action == "lowercase":
            if col_class not in ("metric", "date"):
                expr = f"LOWER({_wrap_string_cast(expr)})"
        elif action == "uppercase":
            if col_class not in ("metric", "date"):
                expr = f"UPPER({_wrap_string_cast(expr)})"
        elif action == "sanitize_email":
            if col_class not in ("metric", "date"):
                expr = f"LOWER(LTRIM(RTRIM({_wrap_string_cast(expr)})))"
        elif action == "normalize_phone":
            inner_cast = expr if is_already_string(expr) else f"CAST({expr} AS NVARCHAR(200))"
            expr = f"REPLACE(REPLACE(REPLACE(REPLACE({inner_cast}, N'-', N''), N' ', N''), N'(', N''), N')', N'')"
        elif action == "hash_phone":
            expr = f"CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', {_wrap_string_cast(expr)}), 2)"
        elif action == "mask_phone":
            inner_cast = expr if is_already_string(expr) else f"CAST({expr} AS NVARCHAR(200))"
            expr = f"N'***' + RIGHT(REPLACE(REPLACE({inner_cast}, N'-', N''), N' ', N''), 4)"
        elif action == "standardize_boolean":
            expr = f"CASE WHEN LOWER(CAST({expr} AS NVARCHAR(10))) IN ('1', 'true', 'yes', 'y', 't') THEN 1 ELSE 0 END"
        elif action == "regex_replace":
            expr = f"REPLACE(REPLACE({_wrap_string_cast(expr)}, N'''', N''), N'\"', N'')"
        elif action == "range_clip":
            expr = f"CASE WHEN TRY_CAST({expr} AS FLOAT) < 0 THEN 0 ELSE TRY_CAST({expr} AS FLOAT) END"
        elif action == "coerce_numeric":
            is_decimal = False
            col_lower = col_name.lower()
            col_type = col_meta.get("dtype")
            if col_type:
                is_decimal = any(x in str(col_type).lower() for x in ("float", "decimal", "double", "numeric", "real"))
            if not is_decimal:
                is_decimal = any(x in col_lower for x in ("fee", "price", "amount", "rate", "cost", "total", "balance", "tax", "decimal"))
            cast_target = "DECIMAL(18, 2)" if is_decimal else "BIGINT"
            expr = f"TRY_CAST({_wrap_string_cast(expr)} AS {cast_target})"
        elif action == "parse_dates":
            expr = f"COALESCE(TRY_CONVERT(date, {expr}, 120), TRY_CONVERT(date, {expr}, 103), TRY_CONVERT(date, {expr}, 101), TRY_CONVERT(date, {expr}, 111))"
        elif action == "replace_values":
            mapping = (business_rules.get("replace_values") or {}).get(col_name) or {}
            if isinstance(mapping, dict) and mapping:
                case_expr = f"CASE"
                for old_v, new_v in list(mapping.items())[:20]:
                    ov = str(old_v).replace("'", "''")
                    nv = str(new_v).replace("'", "''")
                    case_expr += f" WHEN {_wrap_string_cast(expr)} = N'{ov}' THEN N'{nv}'"
                case_expr += f" ELSE {expr} END"
                expr = case_expr
                
    return expr


def generate_sql_etl(plan: Dict[str, Any], assessment: Dict[str, Any], *, dialect: str = "tsql") -> str:
    """
    Generate commented SQL scripts (T-SQL biased: UPDATE / TRY_CAST / QUOTENAME patterns).
    `dialect`: 'tsql' | 'ansi' (ansi uses portable comments only for risky bits).
    """
    dialect = (dialect or "tsql").lower()
    default_values_to_seed = {}
    invalid_values_to_seed = {}
    plan_id = str(plan.get("plan_id") or "unknown")
    business_rules: Dict[str, Any] = plan.get("business_rules") or {}
    excluded_columns: List[str] = business_rules.get("exclude_columns") or []

    lines: List[str] = [
        f"-- ETL SQL — Agent Dhara — plan_id={plan_id}",
        f"-- dialect={dialect} — review before executing against production.",
        "",
    ]
    if dialect != "tsql":
        lines.insert(
            1,
            "-- ANSI mode is template-only; adjust statements to your SQL engine before running.",
        )

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

    # Emit logging table definition for T-SQL dialect
    if dialect == "tsql":
        lines.append("-- ============================================================")
        lines.append("-- Create configuration, watermark and logging tables if not exists")
        lines.append("-- ============================================================")
        lines.append("IF OBJECT_ID('dbo.etl_log', 'U') IS NULL")
        lines.append("BEGIN")
        lines.append("    CREATE TABLE dbo.etl_log (")
        lines.append("        id INT IDENTITY(1,1) PRIMARY KEY,")
        lines.append("        process_name VARCHAR(100) NOT NULL,")
        lines.append("        start_time DATETIME NOT NULL,")
        lines.append("        end_time DATETIME NULL,")
        lines.append("        status VARCHAR(20) NOT NULL,")
        lines.append("        error_message VARCHAR(MAX) NULL")
        lines.append("    );")
        lines.append("END;")
        lines.append("GO")
        lines.append("")
        lines.append("IF OBJECT_ID('dbo.etl_default_values', 'U') IS NULL")
        lines.append("BEGIN")
        lines.append("    CREATE TABLE dbo.etl_default_values (")
        lines.append("        column_name VARCHAR(256) PRIMARY KEY,")
        lines.append("        default_value VARCHAR(256) NOT NULL,")
        lines.append("        data_type VARCHAR(50) NOT NULL")
        lines.append("    );")
        lines.append("END;")
        lines.append("GO")
        lines.append("")
        lines.append("IF OBJECT_ID('dbo.etl_invalid_values', 'U') IS NULL")
        lines.append("BEGIN")
        lines.append("    CREATE TABLE dbo.etl_invalid_values (")
        lines.append("        column_name VARCHAR(256),")
        lines.append("        invalid_value VARCHAR(256),")
        lines.append("        PRIMARY KEY (column_name, invalid_value)")
        lines.append("    );")
        lines.append("END;")
        lines.append("GO")
        lines.append("")
        lines.append("IF OBJECT_ID('dbo.etl_rejects', 'U') IS NULL")
        lines.append("BEGIN")
        lines.append("    CREATE TABLE dbo.etl_rejects (")
        lines.append("        id INT IDENTITY(1,1) PRIMARY KEY,")
        lines.append("        process_name VARCHAR(100) NOT NULL,")
        lines.append("        table_name VARCHAR(100) NOT NULL,")
        lines.append("        row_data VARCHAR(MAX) NOT NULL,")
        lines.append("        error_reason VARCHAR(MAX) NOT NULL,")
        lines.append("        rejected_at DATETIME DEFAULT GETDATE()")
        lines.append("    );")
        lines.append("END;")
        lines.append("GO")
        lines.append("")
        lines.append("IF OBJECT_ID('dbo.etl_watermark', 'U') IS NULL")
        lines.append("BEGIN")
        lines.append("    CREATE TABLE dbo.etl_watermark (")
        lines.append("        process_name VARCHAR(256) PRIMARY KEY,")
        lines.append("        last_run_time DATETIME NOT NULL")
        lines.append("    );")
        lines.append("END;")
        lines.append("GO")
        lines.append("")
        lines.append("-- __DEFAULT_VALUES_SEED_PLACEHOLDER__")
        lines.append("")

    # Emit reusable stored procedure for IQR outlier flagging (DRY pattern)
    has_outlier_steps = any(
        any(str(st.get("action") or "") in ("flag_outliers", "clip_or_flag") for st in (b.get("steps") or []))
        for b in ds_plan.values()
    )
    if has_outlier_steps and dialect == "tsql":
        lines.append("-- ============================================================")
        lines.append("-- Reusable stored procedure: IQR outlier flagging")
        lines.append("-- Usage: EXEC sp_flag_outliers_iqr 'dbo.Orders_Clean', 'CustomerID'")
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
        lines.append("    DECLARE @obj_id INT;")
        lines.append("")
        lines.append("    -- Support temporary tables in tempdb or permanent tables in current DB")
        lines.append("    IF LEFT(@table_name, 1) = '#'")
        lines.append("        SET @obj_id = OBJECT_ID('tempdb..' + @table_name);")
        lines.append("    ELSE")
        lines.append("        SET @obj_id = OBJECT_ID(@table_name);")
        lines.append("")
        lines.append("    IF @obj_id IS NULL")
        lines.append("    BEGIN")
        lines.append("        RAISERROR('Table %s does not exist.', 16, 1, @table_name);")
        lines.append("        RETURN;")
        lines.append("    END")
        lines.append("")
        lines.append("    -- Validate column existence")
        lines.append("    DECLARE @col_exists BIT = 0;")
        lines.append("    IF LEFT(@table_name, 1) = '#'")
        lines.append("        SELECT @col_exists = 1 FROM tempdb.sys.columns WHERE object_id = @obj_id AND name = @column_name;")
        lines.append("    ELSE")
        lines.append("        SELECT @col_exists = 1 FROM sys.columns WHERE object_id = @obj_id AND name = @column_name;")
        lines.append("")
        lines.append("    IF @col_exists = 0")
        lines.append("    BEGIN")
        lines.append("        RAISERROR('Column %s does not exist in table %s.', 16, 1, @column_name, @table_name);")
        lines.append("        RETURN;")
        lines.append("    END")
        lines.append("")
        lines.append("    -- Add flag column if missing")
        lines.append("    IF LEFT(@table_name, 1) = '#'")
        lines.append("    BEGIN")
        lines.append("        SET @sql = N'IF NOT EXISTS (SELECT 1 FROM tempdb.sys.columns WHERE object_id = OBJECT_ID(''tempdb..' + @table_name + ''') AND name = ''' + @flag_col + ''')'")
        lines.append("            + N' ALTER TABLE ' + @table_name + N' ADD ' + QUOTENAME(@flag_col) + N' BIT NOT NULL DEFAULT 0;';")
        lines.append("    END")
        lines.append("    ELSE")
        lines.append("    BEGIN")
        lines.append("        SET @sql = N'IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(''' + @table_name + ''') AND name = ''' + @flag_col + ''')'")
        lines.append("            + N' ALTER TABLE ' + QUOTENAME(PARSENAME(@table_name,2)) + N'.' + QUOTENAME(PARSENAME(@table_name,1))")
        lines.append("            + N' ADD ' + QUOTENAME(@flag_col) + N' BIT NOT NULL DEFAULT 0;';")
        lines.append("    END")
        lines.append("    EXEC sp_executesql @sql;")
        lines.append("")
        lines.append("    -- Compute IQR and flag")
        lines.append("    IF LEFT(@table_name, 1) = '#'")
        lines.append("    BEGIN")
        lines.append("        SET @sql = N'DECLARE @q1 FLOAT, @q3 FLOAT; '")
        lines.append("            + N'SELECT @q1 = PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ' + QUOTENAME(@column_name) + N'), '")
        lines.append("            + N'       @q3 = PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ' + QUOTENAME(@column_name) + N') '")
        lines.append("            + N'FROM ' + @table_name + N' WHERE ' + QUOTENAME(@column_name) + N' IS NOT NULL; '")
        lines.append("            + N'UPDATE ' + @table_name")
        lines.append("            + N' SET ' + QUOTENAME(@flag_col) + N' = CASE'")
        lines.append("            + N' WHEN ' + QUOTENAME(@column_name) + N' < (@q1 - 1.5 * (@q3 - @q1)) THEN 1'")
        lines.append("            + N' WHEN ' + QUOTENAME(@column_name) + N' > (@q3 + 1.5 * (@q3 - @q1)) THEN 1'")
        lines.append("            + N' ELSE 0 END'")
        lines.append("            + N' WHERE ' + QUOTENAME(@column_name) + N' IS NOT NULL;';")
        lines.append("    END")
        lines.append("    ELSE")
        lines.append("    BEGIN")
        lines.append("        SET @sql = N'DECLARE @q1 FLOAT, @q3 FLOAT; '")
        lines.append("            + N'SELECT @q1 = PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ' + QUOTENAME(@column_name) + N'), '")
        lines.append("            + N'       @q3 = PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ' + QUOTENAME(@column_name) + N') '")
        lines.append("            + N'FROM ' + QUOTENAME(PARSENAME(@table_name,2)) + N'.' + QUOTENAME(PARSENAME(@table_name,1))")
        lines.append("            + N' WHERE ' + QUOTENAME(@column_name) + N' IS NOT NULL; '")
        lines.append("            + N'UPDATE ' + QUOTENAME(PARSENAME(@table_name,2)) + N'.' + QUOTENAME(PARSENAME(@table_name,1))")
        lines.append("            + N' SET ' + QUOTENAME(@flag_col) + N' = CASE'")
        lines.append("            + N' WHEN ' + QUOTENAME(@column_name) + N' < (@q1 - 1.5 * (@q3 - @q1)) THEN 1'")
        lines.append("            + N' WHEN ' + QUOTENAME(@column_name) + N' > (@q3 + 1.5 * (@q3 - @q1)) THEN 1'")
        lines.append("            + N' ELSE 0 END'")
        lines.append("            + N' WHERE ' + QUOTENAME(@column_name) + N' IS NOT NULL;';")
        lines.append("    END")
        lines.append("    EXEC sp_executesql @sql;")
        lines.append("END;")
        lines.append("GO")
        lines.append("")

    if not ds_plan:
        lines.append("-- No datasets found in plan.")

    for ds_name, block in ds_plan.items():
        tbl_base = ds_name.split(".")[-1].strip("[]")
        tbl_clean = _get_clean_table_name(ds_name)
        
        raw_tbl = tsql_qualified_name(ds_name) if dialect == "tsql" else _brk(ds_name)
        clean_tbl = tsql_qualified_name(tbl_clean) if dialect == "tsql" else _brk(tbl_clean)
        tbl_staging = f"#{tbl_base}_Staging"
        
        # Consolidate, filter, and sort steps for this dataset
        ds_info = assessment.get("datasets", {}).get(ds_name) or {} if assessment else {}
        cols_info = ds_info.get("columns") or {}
        
        filtered_steps = []
        seen_operations = set()
        
        for st in (block.get("steps") or []):
            if not isinstance(st, dict):
                continue
            action = str(st.get("action") or "").strip().lower()
            col = st.get("column")
            
            # Type-aware filtering for string operations
            if action in ("trim", "lowercase", "uppercase", "sanitize_email"):
                if col:
                    col_meta = cols_info.get(col) or {}
                    col_class = _classify_column(col, col_meta)
                    if col_class in ("metric", "date"):
                        continue
                        
            # Type-aware filtering for outlier actions
            if action in ("flag_outliers", "clip_or_flag", "clip_outliers", "cap_outliers"):
                if col:
                    col_meta = cols_info.get(col) or {}
                    col_class = _classify_column(col, col_meta)
                    if col_class != "metric":
                        continue
                        
            # Operation deduplication / normalization
            norm_action = action
            if action in ("clip_or_flag", "flag_outliers"):
                norm_action = "flag_outliers"
            elif action in ("fill_nulls_simple", "fill_or_drop"):
                norm_action = "fill_or_drop"
            elif action in ("clip_outliers", "cap_outliers"):
                norm_action = "modify_outliers"
                
            op_key = (norm_action, str(col).lower() if col else None)
            if op_key in seen_operations:
                continue
            seen_operations.add(op_key)
            
            filtered_steps.append(st)
            
        priority = {
            "trim": 10,
            "lowercase": 11,
            "uppercase": 11,
            "sanitize_email": 12,
            "coerce_numeric": 20,
            "cast_type": 21,
            "zero_to_null": 30,
            "fill_or_drop": 40,
            "fill_nulls_simple": 40,
            "parse_dates": 50,
            "regex_replace": 60,
            "replace_values": 61,
            "standardize_boolean": 62,
            "normalize_phone": 63,
            "hash_phone": 64,
            "mask_phone": 65,
            "range_clip": 70,
            "clip_or_flag": 71,
            "flag_outliers": 72,
            "clip_outliers": 73,
            "cap_outliers": 74,
            "deduplicate": 80,
            "validate_referential_integrity_or_stage": 90
        }
        
        def get_step_priority(st):
            act = str(st.get("action") or "").strip().lower()
            return priority.get(act, 99)
            
        steps = sorted(filtered_steps, key=get_step_priority)
        for idx, st in enumerate(steps):
            st_copy = dict(st)
            st_copy["order"] = idx + 1
            steps[idx] = st_copy

        never_drop = bool(business_rules.get("never_drop_rows"))
        local_excluded_columns = set(excluded_columns)
        for st in steps:
            if st.get("action") in ("exclude_column", "drop_column") and st.get("column"):
                local_excluded_columns.add(st.get("column"))
        
        # Meta extraction for keys, indexing and incremental loading
        cols = []
        pk_col = None
        watermark_col = None
        if assessment and "datasets" in assessment:
            ds_info = assessment["datasets"].get(ds_name) or {}
            cols_info = ds_info.get("columns") or {}
            cols = [c for c in cols_info.keys() if c not in local_excluded_columns]
            for col_name, cmeta in cols_info.items():
                if cmeta.get("candidate_primary_key") and not pk_col:
                    pk_col = col_name
            
            # Prioritized watermark selection
            for col_name in cols_info.keys():
                col_lower = col_name.lower()
                if any(x in col_lower for x in ("modified", "update", "changed")) or col_lower.endswith("_at"):
                    if "date" in col_lower or "time" in col_lower or "stamp" in col_lower or col_lower.endswith("_at"):
                        watermark_col = col_name
                        break
            if not watermark_col:
                for col_name in cols_info.keys():
                    col_lower = col_name.lower()
                    if "date" in col_lower or "time" in col_lower or "stamp" in col_lower:
                        watermark_col = col_name
                        break
        
        # Fallback key search
        if cols and not pk_col:
            for c in cols:
                if c.lower().endswith("id") or c.lower().endswith("key"):
                    pk_col = c
                    break
            if not pk_col:
                pk_col = cols[0]
                
        col_list = ", ".join(f"[{c}]" for c in cols) if cols else "*"
        
        lines.append(f"-- === dataset: {ds_name} === ")
        
        # Stored Procedure boundary setup for T-SQL
        proc_lines: List[str] = []
        if dialect == "tsql":
            lines.append(f"IF OBJECT_ID('dbo.etl_clean_{tbl_base}', 'P') IS NOT NULL DROP PROCEDURE dbo.etl_clean_{tbl_base};")
            lines.append("GO")
            lines.append(f"CREATE PROCEDURE dbo.etl_clean_{tbl_base}")
            lines.append("    @load_type VARCHAR(20) = 'FULL',")
            lines.append("    @last_run DATETIME = NULL")
            lines.append("AS BEGIN")
            lines.append("    SET NOCOUNT ON;")
            lines.append("    -- Retrieve last run watermark if not provided")
            lines.append("    IF @load_type = 'INCREMENTAL' AND @last_run IS NULL")
            lines.append("    BEGIN")
            lines.append(f"        SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = 'etl_clean_{tbl_base}';")
            lines.append("    END;")
            lines.append("")
            lines.append(f"    INSERT INTO dbo.etl_log (process_name, start_time, status)")
            lines.append(f"    VALUES ('etl_clean_{tbl_base}', GETDATE(), 'RUNNING');")
            lines.append("    DECLARE @run_id INT = SCOPE_IDENTITY();")
            lines.append("")
            lines.append("    BEGIN TRY")
            lines.append("        BEGIN TRAN;")
            lines.append("")
            
        # Check if row-level deduplication step exists
        has_row_dedup = any(
            str(st.get("action") or "").lower() == "deduplicate" and str(st.get("column") or "").lower() in ("row-level", "[row-level]")
            for st in steps
        )

        if dialect == "tsql":
            proc_lines.append(f"-- Initialize Clean Table Structure")
            proc_lines.append(f"IF OBJECT_ID('{tbl_clean}', 'U') IS NULL")
            proc_lines.append("BEGIN")
            proc_lines.append(f"    SELECT * INTO {clean_tbl} FROM {raw_tbl} WHERE 1=0;")
            proc_lines.append(f"    ALTER TABLE {clean_tbl} ADD etl_created_at DATETIME DEFAULT GETDATE();")
            proc_lines.append(f"    ALTER TABLE {clean_tbl} ADD etl_updated_at DATETIME DEFAULT GETDATE();")
            proc_lines.append(f"    ALTER TABLE {clean_tbl} ADD etl_batch_id INT;")
            if pk_col:
                proc_lines.append(f"    ALTER TABLE {clean_tbl} ADD CONSTRAINT [PK_{tbl_base}_Clean] PRIMARY KEY ([{pk_col}]);")
            
            # Setup index keys
            index_keys = []
            rel = plan.get("relationships") or {}
            for j in rel.get("joins") or []:
                if j.get("parent_dataset") == ds_name:
                    index_keys.append(j.get("parent_key"))
                elif j.get("child_dataset") == ds_name:
                    index_keys.append(j.get("child_key"))
            if watermark_col:
                index_keys.append(watermark_col)
                
            for ik in sorted(list(set(index_keys))):
                proc_lines.append(f"    CREATE NONCLUSTERED INDEX idx_{tbl_base}_Clean_{ik} ON {clean_tbl}([{ik}]);")
            proc_lines.append("END")
            proc_lines.append("")
            
        # DDL alterations (run before staging setup)
        ddl_lines = []
        for st in steps:
            col = st.get("column")
            action = str(st.get("action") or "")
            if not col or col in excluded_columns:
                continue
            col_clean = str(col).replace("'", "''")
            if action in ("flag_outliers", "clip_or_flag"):
                ddl_lines.append(f"IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('{tbl_clean}') AND name = '{col_clean}_outlier_flagged')")
                ddl_lines.append(f"    ALTER TABLE {clean_tbl} ADD [{col_clean}_outlier_flagged] BIT NOT NULL DEFAULT 0;")
            elif action == "cast_type":
                ddl_lines.append(f"IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('{tbl_clean}') AND name = '{col_clean}_int')")
                ddl_lines.append(f"    ALTER TABLE {clean_tbl} ADD [{col_clean}_int] BIGINT;")
                
        if ddl_lines:
            if dialect == "tsql":
                proc_lines.append("-- Add generated transformation columns to Clean Table")
                proc_lines.extend(ddl_lines)
                proc_lines.append("")
            else:
                lines.extend(ddl_lines)
                lines.append("")

        if dialect == "tsql":
            proc_lines.append(f"-- Create Staging Table matching Clean structure")
            proc_lines.append(f"IF OBJECT_ID('tempdb..{tbl_staging}') IS NOT NULL DROP TABLE {tbl_staging};")
            proc_lines.append(f"SELECT * INTO {tbl_staging} FROM {clean_tbl} WHERE 1=0;")
            proc_lines.append("")

        # Data copy logic
        if dialect == "tsql":
            proc_lines.append("-- Copy data from Raw to Staging")
            
            # Determine deduplication partition & order keys
            explicit_dedup_step = next(
                (st for st in steps if str(st.get("action") or "").lower() == "deduplicate"),
                None
            )
            should_dedup_on_insert = has_row_dedup or (pk_col is not None) or (explicit_dedup_step is not None)
            if should_dedup_on_insert:
                partition_keys = []
                if explicit_dedup_step:
                    col_val = str(explicit_dedup_step.get("column") or "")
                    if col_val and col_val.lower() not in ("row-level", "[row-level]"):
                        partition_keys = [c.strip() for c in col_val.split(",") if c.strip()]
                
                if not partition_keys:
                    if pk_col:
                        partition_keys.append(pk_col)
                    for c_name in cols:
                        c_lower = c_name.lower()
                        if c_name != pk_col and any(x in c_lower for x in ("id", "key", "email", "code")):
                            if not any(x in c_lower for x in ("row_number", "_rn", "row_num")):
                                partition_keys.append(c_name)
                if not partition_keys:
                    if pk_col:
                        partition_keys = [pk_col]
                    elif cols:
                        partition_keys = [cols[0]]
                    else:
                        partition_keys = ["column1", "column2"]
                
                partition_by = ", ".join(f"LOWER(LTRIM(RTRIM(CAST([{pk}] AS NVARCHAR(400)))))" for pk in partition_keys)
                order_by_clause = f"[{watermark_col}] DESC" if watermark_col else "(SELECT NULL)"
            
            def get_copy_sql_lines(where_cond: str = "") -> List[str]:
                select_cols = ", ".join(f"[{c}]" for c in cols) if cols else "*"
                where_clause = f" WHERE {where_cond}" if where_cond else ""
                col_target_list = f" ({col_list}, etl_batch_id)" if col_list != "*" else ""
                select_list = f"{col_list}, @run_id" if col_list != "*" else "*, @run_id"
                if should_dedup_on_insert:
                    return [
                        f"    ;WITH _raw_dedup AS (",
                        f"        SELECT {select_cols}, ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY {order_by_clause}) AS _rn",
                        f"        FROM {raw_tbl}{where_clause}",
                        f"    )",
                        f"    INSERT INTO {tbl_staging}{col_target_list}",
                        f"    SELECT {select_cols}, @run_id FROM _raw_dedup WHERE _rn = 1;"
                    ]
                else:
                    return [
                        f"    INSERT INTO {tbl_staging}{col_target_list}",
                        f"    SELECT {select_list} FROM {raw_tbl}{where_clause};"
                    ]

            if pk_col and watermark_col:
                proc_lines.append("IF @load_type = 'FULL' OR @last_run IS NULL")
                proc_lines.append("BEGIN")
                proc_lines.extend(get_copy_sql_lines())
                proc_lines.append("END")
                proc_lines.append("ELSE")
                proc_lines.append("BEGIN")
                proc_lines.extend(get_copy_sql_lines(f"[{watermark_col}] > @last_run"))
                proc_lines.append("END")
            else:
                proc_lines.extend(get_copy_sql_lines())
            proc_lines.append("")
        else:
            lines.append(f"-- ANSI Init Target: CREATE TABLE {tbl_staging} AS SELECT * FROM {raw_tbl};")
            lines.append("")
        
        step_lines = []
        
        # Phase 1: Validations and Quarantines (deletes/rejects) on the Staging Table
        if pk_col and dialect == "tsql" and not never_drop:
            step_lines.append(f"-- Quarantine rows where primary key [{pk_col}] is NULL to dbo.etl_rejects")
            step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
            step_lines.append(f"SELECT 'etl_clean_{tbl_base}', '{tbl_clean}',")
            step_lines.append(f"       (SELECT TOP 1 * FROM {tbl_staging} r2 WHERE r2.[{pk_col}] IS NULL FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
            step_lines.append(f"       'Primary key [{pk_col}] is NULL'")
            step_lines.append(f"FROM {tbl_staging} r")
            step_lines.append(f"WHERE r.[{pk_col}] IS NULL;")
            step_lines.append(f"")
            step_lines.append(f"DELETE FROM {tbl_staging} WHERE [{pk_col}] IS NULL;")
            step_lines.append(f"")
            
        non_nullable_cols = business_rules.get("non_nullable") or []
        for nn_col in non_nullable_cols:
            if nn_col in cols and nn_col != pk_col:
                nn_c = _brk(nn_col)
                if not never_drop:
                    step_lines.append(f"-- Quarantine rows where non-nullable [{nn_col}] is NULL to dbo.etl_rejects")
                    step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                    step_lines.append(f"SELECT 'etl_clean_{tbl_base}', '{tbl_clean}',")
                    step_lines.append(f"       (SELECT TOP 1 * FROM {tbl_staging} r2 WHERE r2.[{pk_col}] = r.[{pk_col}] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                    step_lines.append(f"       'Required column [{nn_col}] is NULL'")
                    step_lines.append(f"FROM {tbl_staging} r")
                    step_lines.append(f"WHERE r.{nn_c} IS NULL;")
                    step_lines.append(f"")
                    step_lines.append(f"DELETE FROM {tbl_staging} WHERE {nn_c} IS NULL;")
                    step_lines.append(f"")
                    
        for st in steps:
            col = st.get("column")
            action = str(st.get("action") or "")
            if not col or col in local_excluded_columns:
                continue
            c = _brk(col)
            
            if action == "parse_dates":
                if dialect == "tsql":
                    if not never_drop:
                        step_lines.append(f"-- Quarantine invalid dates from {tbl_staging}.{c} to dbo.etl_rejects")
                        step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                        step_lines.append(f"SELECT 'etl_clean_{tbl_base}', '{tbl_clean}',")
                        step_lines.append(f"       (SELECT TOP 1 * FROM {tbl_staging} r2 WHERE r2.[{pk_col}] = r.[{pk_col}] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                        step_lines.append(f"       'Column [{col}] with value ' + CAST(r.{c} AS NVARCHAR(MAX)) + ' is not a valid date format'")
                        step_lines.append(f"FROM {tbl_staging} r")
                        step_lines.append(f"WHERE r.{c} IS NOT NULL AND COALESCE(")
                        step_lines.append(f"    TRY_CONVERT(date, r.{c}, 120),")
                        step_lines.append(f"    TRY_CONVERT(date, r.{c}, 103),")
                        step_lines.append(f"    TRY_CONVERT(date, r.{c}, 101),")
                        step_lines.append(f"    TRY_CONVERT(date, r.{c}, 111)")
                        step_lines.append(f") IS NULL;")
                        step_lines.append(f"")
                        step_lines.append(f"DELETE FROM {tbl_staging}")
                        step_lines.append(f"WHERE {c} IS NOT NULL AND COALESCE(")
                        step_lines.append(f"    TRY_CONVERT(date, {c}, 120),")
                        step_lines.append(f"    TRY_CONVERT(date, {c}, 103),")
                        step_lines.append(f"    TRY_CONVERT(date, {c}, 101),")
                        step_lines.append(f"    TRY_CONVERT(date, {c}, 111)")
                        step_lines.append(f") IS NULL;")
                        step_lines.append(f"")
            elif action == "sanitize_email":
                if dialect == "tsql":
                    if not never_drop:
                        step_lines.append(f"-- Quarantine invalid emails from {tbl_staging}.{c} to dbo.etl_rejects")
                        step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                        step_lines.append(f"SELECT 'etl_clean_{tbl_base}', '{tbl_clean}',")
                        step_lines.append(f"       (SELECT TOP 1 * FROM {tbl_staging} r2 WHERE r2.[{pk_col}] = r.[{pk_col}] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                        step_lines.append(f"       'Column [{col}] with value ' + CAST(r.{c} AS NVARCHAR(MAX)) + ' is not a valid email format'")
                        step_lines.append(f"FROM {tbl_staging} r")
                        step_lines.append(f"WHERE r.{c} IS NOT NULL AND NOT (CAST(r.{c} AS NVARCHAR(MAX)) LIKE '%_@_%._%');")
                        step_lines.append(f"")
                        step_lines.append(f"DELETE FROM {tbl_staging} WHERE {c} IS NOT NULL AND NOT (CAST({c} AS NVARCHAR(MAX)) LIKE '%_@_%._%');")
                        step_lines.append(f"")
                    else:
                        step_lines.append(f"UPDATE {tbl_staging} SET {c} = NULL WHERE {c} IS NOT NULL AND NOT (CAST({c} AS NVARCHAR(MAX)) LIKE '%_@_%._%');")
            elif action == "normalize_phone":
                if dialect == "tsql":
                    cleaned_expr = f"REPLACE(REPLACE(REPLACE(REPLACE(CAST(r.{c} AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'')"
                    cleaned_expr_del = f"REPLACE(REPLACE(REPLACE(REPLACE(CAST({c} AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'')"
                    if not never_drop:
                        step_lines.append(f"-- Quarantine invalid phones from {tbl_staging}.{c} to dbo.etl_rejects")
                        step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                        step_lines.append(f"SELECT 'etl_clean_{tbl_base}', '{tbl_clean}',")
                        step_lines.append(f"       (SELECT TOP 1 * FROM {tbl_staging} r2 WHERE r2.[{pk_col}] = r.[{pk_col}] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                        step_lines.append(f"       'Column [{col}] with value ' + CAST(r.{c} AS NVARCHAR(MAX)) + ' is not a valid phone format'")
                        step_lines.append(f"FROM {tbl_staging} r")
                        step_lines.append(f"WHERE r.{c} IS NOT NULL AND (LEN({cleaned_expr}) < 7 OR {cleaned_expr} LIKE '%[^0-9]%');")
                        step_lines.append(f"")
                        step_lines.append(f"DELETE FROM {tbl_staging} WHERE {c} IS NOT NULL AND (LEN({cleaned_expr_del}) < 7 OR {cleaned_expr_del} LIKE '%[^0-9]%');")
                        step_lines.append(f"")
                    else:
                        step_lines.append(f"UPDATE {tbl_staging} SET {c} = NULL WHERE {c} IS NOT NULL AND (LEN({cleaned_expr_del}) < 7 OR {cleaned_expr_del} LIKE '%[^0-9]%');")
            elif action == "at_least_one":
                cols_split = [str(x).strip() for x in str(col).split(",")]
                cols_brackets = [_brk(x) for x in cols_split]
                all_null_cond = " AND ".join(f"r.{cb} IS NULL" for cb in cols_brackets)
                all_null_cond_delete = " AND ".join(f"{cb} IS NULL" for cb in cols_brackets)
                if dialect == "tsql":
                    if not never_drop:
                        step_lines.append(f"-- Quarantine rows where all of {col} are NULL to dbo.etl_rejects")
                        step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                        step_lines.append(f"SELECT 'etl_clean_{tbl_base}', '{tbl_clean}',")
                        step_lines.append(f"       (SELECT * FROM {tbl_staging} r2 WHERE r2.[{pk_col}] = r.[{pk_col}] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                        step_lines.append(f"       'All of columns [{col}] are NULL'")
                        step_lines.append(f"FROM {tbl_staging} r")
                        step_lines.append(f"WHERE {all_null_cond};")
                        step_lines.append(f"")
                        step_lines.append(f"DELETE FROM {tbl_staging} WHERE {all_null_cond_delete};")
                        step_lines.append(f"")
            elif action == "validate_referential_integrity_or_stage":
                p = step_params(st)
                rel_ds = p.get("related_dataset") or "?"
                rel_col = p.get("related_column") or "?"
                fk_action = p.get("fk_action") or "flag"
                parent_tbl = _get_clean_table_name(rel_ds)
                
                if dialect == "tsql":
                    step_lines.append(f"-- Referential integrity check: {col} -> {rel_ds}.{rel_col} (action={fk_action})")
                    if fk_action == "reject_orphans":
                        if not never_drop:
                            step_lines.append(f"INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)")
                            step_lines.append(f"SELECT 'etl_clean_{tbl_base}', '{tbl_clean}',")
                            step_lines.append(f"       (SELECT * FROM {tbl_staging} r2 WHERE r2.[{pk_col}] = r.[{pk_col}] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),")
                            step_lines.append(f"       'Referential integrity violation: [{col}] value ' + CAST(r.{c} AS NVARCHAR(MAX)) + ' does not exist in {parent_tbl}.[{rel_col}]'")
                            step_lines.append(f"FROM {tbl_staging} r")
                            step_lines.append(f"WHERE r.{c} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {parent_tbl} p WHERE p.[{rel_col}] = r.{c});")
                            step_lines.append(f"")
                            step_lines.append(f"DELETE FROM {tbl_staging}")
                            step_lines.append(f"WHERE {c} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {parent_tbl} p WHERE p.[{rel_col}] = {c});")
                            step_lines.append(f"")
                        else:
                            step_lines.append(f"UPDATE {tbl_staging} SET {c} = NULL WHERE {c} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {parent_tbl} p WHERE p.[{rel_col}] = {c});")
                    elif fk_action == "null_fill_fk":
                        step_lines.append(f"UPDATE {tbl_staging} SET {c} = NULL WHERE {c} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {parent_tbl} p WHERE p.[{rel_col}] = {c});")
                    elif fk_action == "create_unknown_dim_record":
                        step_lines.append(f"INSERT INTO {parent_tbl} ([{rel_col}])")
                        step_lines.append(f"SELECT DISTINCT r.{c}")
                        step_lines.append(f"FROM {tbl_staging} r")
                        step_lines.append(f"WHERE r.{c} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {parent_tbl} p WHERE p.[{rel_col}] = r.{c});")
                        step_lines.append(f"")

        # Phase 2: Expression-Based Chained Transformations (Single-Pass Update)
        column_transforms = {}
        for st in steps:
            col = st.get("column")
            if not col or col in local_excluded_columns:
                continue
            action = st.get("action")
            # Only expressions
            if action in ("trim", "lowercase", "uppercase", "sanitize_email", "normalize_phone", "hash_phone", "mask_phone", "standardize_boolean", "regex_replace", "range_clip", "coerce_numeric", "cast_type", "parse_dates", "replace_values"):
                if col not in column_transforms:
                    column_transforms[col] = []
                column_transforms[col].append(st)
                
        update_clauses = []
        for col_name, col_steps in column_transforms.items():
            has_cast_type = any(st.get("action") == "cast_type" for st in col_steps)
            base_steps = [st for st in col_steps if st.get("action") != "cast_type"]
            col_meta = cols_info.get(col_name) or {}
            
            if base_steps:
                compiled_expr = compile_column_expression(col_name, base_steps, col_meta, business_rules)
                update_clauses.append(f"[{col_name}] = {compiled_expr}")
            if has_cast_type:
                col_clean = str(col_name).replace("'", "''")
                update_clauses.append(f"[{col_clean}_int] = TRY_CAST([{col_name}] AS BIGINT)")
                
        if update_clauses:
            step_lines.append(f"-- Single-Pass expression updates on {tbl_staging}")
            update_sql = f"UPDATE {tbl_staging}\nSET " + ",\n    ".join(update_clauses) + "\nWHERE 1=1;"
            step_lines.extend(update_sql.splitlines())
            step_lines.append("")

        # Phase 3: Outlier Flags dynamically processed via stored procedure
        for st in steps:
            col = st.get("column")
            action = str(st.get("action") or "")
            if not col or col in local_excluded_columns:
                continue
            col_clean = str(col).replace("'", "''")
            if action in ("flag_outliers", "clip_or_flag"):
                step_lines.append(f"-- Flag IQR outliers for {col}")
                if dialect == "tsql":
                    step_lines.append(f"EXEC dbo.sp_flag_outliers_iqr '{tbl_staging}', '{col_clean}';")
                else:
                    step_lines.append(f"-- ANSI outlier flagging placeholder")

        # Phase 4: Config-driven Default Seed & Single-Pass Join-based updates (defaults + invalid value replacements)
        config_fill_columns = []
        config_invalid_columns = []
        
        for st in steps:
            col = st.get("column")
            if not col or col in local_excluded_columns:
                continue
            action = st.get("action")
            if action in ("zero_to_null",):
                config_invalid_columns.append(col)
            elif action in ("fill_or_drop", "fill_nulls_simple"):
                config_fill_columns.append(st)
                
        config_columns = list(set([st.get("column") for st in config_fill_columns] + config_invalid_columns))
        if config_columns:
            pre_queries = []
            set_clauses = []
            join_clauses = []
            where_clauses = []
            fill_step_map = {st.get("column"): st for st in config_fill_columns}
            
            for col in sorted(config_columns):
                col_bracket = _brk(col)
                col_clean_name = str(col)
                col_meta = cols_info.get(col) or {}
                col_type = col_meta.get("dtype")
                col_class = _classify_column(col, col_meta)
                
                cast_type = get_sql_cast_type(col_type, col_clean_name)
                cast_func = "TRY_CAST" if dialect == "tsql" else "CAST"
                
                fval = None
                strat = None
                if col in fill_step_map:
                    p = step_params(fill_step_map[col])
                    strat = p.get("fill_strategy")
                    fval = p.get("fill_value")
                    
                if fval is not None:
                    val = fval
                else:
                    if col_class in ("date", "id", "categorical"):
                        val = None
                    elif col_class == "metric":
                        val = '0'
                    else:
                        val = None
                        
                has_invalid = col in config_invalid_columns
                tbl_key = f"{tbl_base}_Clean"
                key = f"{tbl_key}.{col_clean_name}"
                expr = f"c.{col_bracket}"
                
                if has_invalid:
                    replace_vals = ["0", "-999", "999999", "9999999", "###"]
                    if col in fill_step_map:
                        replace_vals = step_params(fill_step_map[col]).get("replace_values") or replace_vals
                    invalid_values_to_seed[key] = [str(v) for v in replace_vals]
                    
                    alias_iv = f"iv_{col_clean_name.replace('.', '_').replace(' ', '_')[:35]}"
                    join_clauses.append(
                        f"LEFT JOIN dbo.etl_invalid_values {alias_iv} ON {alias_iv}.column_name = '{key}' "
                        f"AND {cast_func}({alias_iv}.invalid_value AS {cast_type}) = c.{col_bracket}"
                    )
                    expr = f"CASE WHEN {alias_iv}.invalid_value IS NOT NULL THEN NULL ELSE {expr} END"
                    where_clauses.append(f"{alias_iv}.invalid_value IS NOT NULL")
                    
                if col in fill_step_map:
                    if strat == "mean" and fval is None and dialect == "tsql":
                        expr = f"COALESCE({expr}, (SELECT AVG(CAST(c2.{col_bracket} AS FLOAT)) FROM {tbl_staging} c2 WHERE c2.{col_bracket} IS NOT NULL))"
                        where_clauses.append(f"c.{col_bracket} IS NULL")
                    elif strat == "median" and fval is None and dialect == "tsql":
                        var_name = f"@fill_{col_clean_name.replace('.', '_').replace(' ', '_')[:40]}"
                        pre_queries.append(f"DECLARE {var_name} FLOAT;")
                        pre_queries.append(
                            f"SELECT {var_name} = PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_bracket}) FROM {tbl_staging} WHERE {col_bracket} IS NOT NULL;"
                        )
                        expr = f"COALESCE({expr}, {var_name})"
                        where_clauses.append(f"c.{col_bracket} IS NULL")
                    else:
                        if val is not None:
                            default_values_to_seed[key] = {"default_value": val, "data_type": cast_type}
                            alias_dv = f"dv_{col_clean_name.replace('.', '_').replace(' ', '_')[:35]}"
                            join_clauses.append(
                                f"LEFT JOIN dbo.etl_default_values {alias_dv} ON {alias_dv}.column_name = '{key}'"
                            )
                            expr = f"COALESCE({expr}, {cast_func}({alias_dv}.default_value AS {cast_type}))"
                            where_clauses.append(f"c.{col_bracket} IS NULL")
                            
                set_clauses.append(f"c.{col_bracket} = {expr}")
                
            if set_clauses:
                step_lines.append(f"-- Grouped config and null updates on {tbl_staging}")
                step_lines.extend(pre_queries)
                update_sql = f"UPDATE c\nSET " + ",\n    ".join(set_clauses)
                update_sql += f"\nFROM {tbl_staging} c"
                if join_clauses:
                    unique_joins = []
                    for j in join_clauses:
                        if j not in unique_joins:
                            unique_joins.append(j)
                    update_sql += "\n" + "\n".join(unique_joins)
                unique_where = []
                for w in where_clauses:
                    if w not in unique_where:
                        unique_where.append(w)
                if unique_where:
                    update_sql += "\nWHERE " + " OR ".join(unique_where) + ";"
                else:
                    update_sql += ";"
                step_lines.extend(update_sql.splitlines())
                step_lines.append("")

        # Phase 5: Final Copy from Staging to target Clean table
        if dialect == "tsql":
            clean_cols = cols + [f"{c}_outlier_flagged" for st in steps if st.get("action") in ("flag_outliers", "clip_or_flag") for c in [st.get("column")] if c and c not in excluded_columns]
            for st in steps:
                if st.get("action") == "cast_type" and st.get("column"):
                    cc = str(st.get("column")).replace("'", "''")
                    if cc not in excluded_columns:
                        clean_cols.append(f"{cc}_int")
            clean_cols = sorted(list(set(clean_cols)))
            col_list_clean = ", ".join(f"[{c}]" for c in clean_cols)
            
            step_lines.append("-- Copy fully transformed data from Staging to target Clean table")
            step_lines.append("IF @load_type = 'FULL' OR @last_run IS NULL")
            step_lines.append("BEGIN")
            step_lines.append(f"    TRUNCATE TABLE {clean_tbl};")
            step_lines.append(f"    INSERT INTO {clean_tbl} ({col_list_clean}, etl_batch_id, etl_created_at, etl_updated_at)")
            step_lines.append(f"    SELECT {col_list_clean}, etl_batch_id, GETDATE(), GETDATE() FROM {tbl_staging};")
            step_lines.append("END")
            step_lines.append("ELSE")
            step_lines.append("BEGIN")
            if pk_col:
                step_lines.append(f"    DELETE FROM {clean_tbl} WHERE [{pk_col}] IN (SELECT [{pk_col}] FROM {tbl_staging});")
            step_lines.append(f"    INSERT INTO {clean_tbl} ({col_list_clean}, etl_batch_id, etl_created_at, etl_updated_at)")
            step_lines.append(f"    SELECT {col_list_clean}, etl_batch_id, GETDATE(), GETDATE() FROM {tbl_staging};")
            step_lines.append("END;")
            step_lines.append("")
            
            # Clean up the staging table
            step_lines.append(f"IF OBJECT_ID('tempdb..{tbl_staging}') IS NOT NULL DROP TABLE {tbl_staging};")
            step_lines.append("")

        if dialect == "tsql":
            proc_lines.extend(step_lines)
            proc_lines.append("")
            proc_lines.append("-- Update process watermark")
            proc_lines.append("IF @load_type = 'INCREMENTAL' OR @last_run IS NULL")
            proc_lines.append("BEGIN")
            proc_lines.append("    MERGE INTO dbo.etl_watermark AS target")
            proc_lines.append(f"    USING (SELECT 'etl_clean_{tbl_base}' AS process_name) AS source")
            proc_lines.append("    ON target.process_name = source.process_name")
            proc_lines.append("    WHEN MATCHED THEN")
            proc_lines.append("        UPDATE SET last_run_time = GETDATE()")
            proc_lines.append("    WHEN NOT MATCHED THEN")
            proc_lines.append("        INSERT (process_name, last_run_time) VALUES (source.process_name, GETDATE());")
            proc_lines.append("END")
            
            # Indent and append child procedure body
            lines.extend(_indent(proc_lines, spaces=8))
            lines.append("        COMMIT;")
            lines.append("")
            lines.append("        -- Log success")
            lines.append("        UPDATE dbo.etl_log")
            lines.append("        SET end_time = GETDATE(), status = 'SUCCESS'")
            where_id_str = "        WHERE id = @run_id;"
            lines.append(where_id_str)
            lines.append("    END TRY")
            lines.append("    BEGIN CATCH")
            lines.append("        IF @@TRANCOUNT > 0 ROLLBACK;")
            lines.append("        DECLARE @err VARCHAR(MAX) = ERROR_MESSAGE();")
            lines.append("        UPDATE dbo.etl_log")
            lines.append("        SET end_time = GETDATE(), status = 'FAILED', error_message = @err")
            lines.append(where_id_str)
            lines.append("        THROW;")
            lines.append("    END CATCH;")
            lines.append("END;")
            lines.append("GO")
            lines.append("")
        else:
            lines.extend(step_lines)
            lines.append("")

    # Generate master orchestrator procedure for T-SQL
    if dialect == "tsql":
        lines.append("-- ============================================================")
        lines.append("-- Master Orchestrator Stored Procedure")
        lines.append("-- ============================================================")
        lines.append("IF OBJECT_ID('dbo.etl_main', 'P') IS NOT NULL DROP PROCEDURE dbo.etl_main;")
        lines.append("GO")
        lines.append("CREATE PROCEDURE dbo.etl_main")
        lines.append("    @load_type VARCHAR(20) = 'FULL',")
        lines.append("    @last_run DATETIME = NULL")
        lines.append("AS BEGIN")
        lines.append("    SET NOCOUNT ON;")
        lines.append("    -- Retrieve last run watermark if not provided")
        lines.append("    IF @load_type = 'INCREMENTAL' AND @last_run IS NULL")
        lines.append("    BEGIN")
        lines.append("        SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = 'etl_main';")
        lines.append("    END;")
        lines.append("")
        lines.append("    INSERT INTO dbo.etl_log (process_name, start_time, status)")
        lines.append("    VALUES ('etl_main', GETDATE(), 'RUNNING');")
        lines.append("    DECLARE @run_id INT = SCOPE_IDENTITY();")
        lines.append("")
        lines.append("    BEGIN TRY")
        
        # Execute each clean stored procedure
        for ds_name in ds_plan.keys():
            tbl_base = ds_name.split(".")[-1].strip("[]")
            lines.append(f"        EXEC dbo.etl_clean_{tbl_base} @load_type = @load_type, @last_run = @last_run;")
            
        lines.append("")
        lines.append("        -- Update master process watermark")
        lines.append("        IF @load_type = 'INCREMENTAL' OR @last_run IS NULL")
        lines.append("        BEGIN")
        lines.append("            MERGE INTO dbo.etl_watermark AS target")
        lines.append("            USING (SELECT 'etl_main' AS process_name) AS source")
        lines.append("            ON target.process_name = source.process_name")
        lines.append("            WHEN MATCHED THEN")
        lines.append("                UPDATE SET last_run_time = GETDATE()")
        lines.append("            WHEN NOT MATCHED THEN")
        lines.append("                INSERT (process_name, last_run_time) VALUES (source.process_name, GETDATE());")
        lines.append("        END")
        lines.append("")
        lines.append("        UPDATE dbo.etl_log")
        lines.append("        SET end_time = GETDATE(), status = 'SUCCESS'")
        lines.append("        WHERE id = @run_id;")
        lines.append("    END TRY")
        lines.append("    BEGIN CATCH")
        lines.append("        DECLARE @err VARCHAR(MAX) = ERROR_MESSAGE();")
        lines.append("        UPDATE dbo.etl_log")
        lines.append("        SET end_time = GETDATE(), status = 'FAILED', error_message = @err")
        lines.append("        WHERE id = @run_id;")
        lines.append("        THROW;")
        lines.append("    END CATCH;")
        lines.append("END;")
        lines.append("GO")
        lines.append("")

    for st in plan.get("global_steps") or []:
        lines.append(f"-- global: {st.get('action')} {st.get('column') or ''}")

    manifest = plan.get("connector_manifest") or {}
    rel = plan.get("relationships") or {}
    if rel.get("joins") or rel.get("many_to_many") or manifest.get("datasets"):
        lines.append("")
        # Call emit_sql_joins and map output lines from Raw to Clean for views
        join_lines = emit_sql_joins(plan, manifest, assessment=assessment, dialect=dialect)
        if dialect == "tsql":
            clean_join_lines = []
            for line in join_lines:
                new_line = line
                for ds_name in ds_plan.keys():
                    tbl_raw = tsql_qualified_name(ds_name)
                    tbl_cl = tsql_qualified_name(_get_clean_table_name(ds_name))
                    new_line = new_line.replace(tbl_raw, tbl_cl)
                    
                    raw_base = ds_name.split(".")[-1].strip("[]")
                    cl_base = _get_clean_table_name(ds_name).split(".")[-1].strip("[]")
                    new_line = new_line.replace(f"[{raw_base}]", f"[{cl_base}]")
                    new_line = new_line.replace(raw_base, cl_base)
                clean_join_lines.append(new_line)
            join_lines = clean_join_lines
            
        lines.extend(join_lines)

    # Generate seed lines for default_values and invalid_values
    seed_lines = []
    if dialect == "tsql":
        if default_values_to_seed:
            seed_lines.append("-- Seed ETL default configuration values")
            for key, info in default_values_to_seed.items():
                val_escaped = str(info["default_value"]).replace("'", "''")
                dt_escaped = str(info["data_type"]).replace("'", "''")
                seed_lines.append(f"IF NOT EXISTS (SELECT 1 FROM dbo.etl_default_values WHERE column_name = '{key}')")
                seed_lines.append(f"    INSERT INTO dbo.etl_default_values (column_name, default_value, data_type) VALUES ('{key}', N'{val_escaped}', '{dt_escaped}');")
        if invalid_values_to_seed:
            seed_lines.append("-- Seed ETL invalid/sentinel configuration values")
            for key, vals in invalid_values_to_seed.items():
                for val in vals:
                    val_escaped = str(val).replace("'", "''")
                    seed_lines.append(f"IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = '{key}' AND invalid_value = '{val_escaped}')")
                    seed_lines.append(f"    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('{key}', N'{val_escaped}');")
        if seed_lines:
            seed_lines.append("GO")
        seed_sql = "\n".join(seed_lines)
    else:
        seed_sql = ""
        
    full_sql = "\n".join(lines)
    full_sql = full_sql.replace("-- __DEFAULT_VALUES_SEED_PLACEHOLDER__", seed_sql)
    return full_sql.strip() + "\n"
