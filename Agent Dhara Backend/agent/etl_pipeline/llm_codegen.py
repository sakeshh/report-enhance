"""
LLM-based ETL Code Generator.
Translates an approved ETL plan + assessment metadata into production-ready code
for Python, SQL, PySpark, and Azure Data Factory.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    from openai import AzureOpenAI, OpenAI
except ImportError:
    AzureOpenAI = None
    OpenAI = None

from agent.model_config import load_llm_config
from agent.etl_pipeline.codegen_policy import llm_codegen_extra_context, plan_policy_block
from agent.etl_pipeline.io_snippets import resolve_path_pyspark_helper

LLM_ERROR_PREFIX = "# Error"

# Actions the planner may emit — LLM must implement each one correctly for the target engine.
_PLAN_PARAMS = """
Each plan step includes a "params" dict — use it as the source of truth (not only evidence):
- params.fill_strategy: "mean" | "median" | "value" — for fill_or_drop / fill_nulls_simple
- params.fill_value: scalar when fill_strategy is "value" or precomputed mean/median
- params.outlier_method: "flag" | "clip" | "cap"
- params.outlier_iqr_multiplier: float (default 1.5)
- params.privacy: "hash" | "mask" | "exclude" for phone/privacy columns
- params.enforcement_mode: "flag" | "quarantine" for referential integrity steps
- params.execution_mode: "in_place" | "new_column" | "new_table"
"""

_PLAN_ACTIONS = """
Supported plan step actions (implement ALL steps in order per dataset):
- trim: strip whitespace on strings
- lowercase / uppercase: case normalization
- fill_or_drop / fill_nulls_simple: fill nulls (if never_drop_rows in business_rules, NEVER delete rows)
- cast_type: nullable integer use Int64 (pandas) / long (spark); preserve nulls
- coerce_numeric: safe numeric conversion
- parse_dates: safe datetime parsing
- sanitize_email: trim, lower, invalid emails -> null
- normalize_phone: digits only
- hash_phone: F.sha2(column.cast('string'), 256) for privacy (per business notes / manual_review)
- mask_phone: keep last 4 digits with *** prefix
- regex_replace: clean per plan note if present
- range_clip: bound numeric values (e.g. lower bound 0)
- clip_or_flag / flag_outliers: IQR-based outlier flag column (suffix _outlier_flagged)
- clip_outliers: IQR clip values to bounds
- cap_outliers: IQR replace outliers with median
- standardize_boolean: map yes/no/1/0/true/false to 0/1
- replace_values: map values per business_rules.valid_values when provided
- zero_to_null: replace 0 with null
- deduplicate: drop duplicate rows (subset column if column set, else full row)
- validate_referential_integrity_or_stage: emit validation/staging comments + checks, do not skip
"""

_BASE_RULES = """
UNIVERSAL RULES (mandatory):
1. Implement EVERY step in plan.datasets[*].steps in ascending "order". Do not skip or merge steps.
2. Read step["params"] for fill/outlier/privacy — match template codegen semantics.
3. Honor business_rules: never_drop_rows, required_columns, exclude_columns, non_nullable, valid_values, notes.
4. Preserve exact column name casing from the plan.
5. Add clear comments for manual_review items from the plan.
6. Production quality: logging.getLogger("agent_dhara"), guards for required columns, no placeholder TODOs for listed actions.
7. Output ONLY the artifact — no markdown fences, no prose before/after.
"""

SYSTEM_PROMPTS: Dict[str, str] = {
    "python": f"""You are a senior data engineer writing production Python ETL with pandas.

{_BASE_RULES}
{_PLAN_PARAMS}
{_PLAN_ACTIONS}

PYTHON REQUIREMENTS:
- Module docstring with plan_id summary.
- Imports: pandas, logging (and sys if needed). No os/subprocess/socket/shutil/ctypes/eval/exec.
- One transform_<dataset> function per dataset; each receives pd.DataFrame and returns pd.DataFrame.
- Start each function with df.copy(); use nullable Int64 for integer columns.
- Required columns: raise ValueError with clear message if missing.
- never_drop_rows: use fillna only, never dropna on rows.
- I/O: use connector_manifest read_snippet_python and write_snippet_python EXACTLY per dataset.
- NEVER read .xml with read_csv. Use read_xml for format=xml. NEVER write CSV to a .xml path.
- Use _resolve_data_path(location) helper when manifest shows blob paths (not bare filenames).
- if __name__ == "__main__": load_all_datasets / transform_all / run_joins / write_outputs from manifest.
- Executable, syntactically valid Python 3.10+.
""",
    "sql-tsql": f"""You are a senior data engineer writing production T-SQL ETL scripts.

{_BASE_RULES}
{_PLAN_PARAMS}
{_PLAN_ACTIONS}

T-SQL REQUIREMENTS:
- Header comment block with plan_id.
- Raw -> Staging -> Transform -> Clean Architecture: Raw tables are completely immutable. Never write updates/modifications/deletions directly on raw tables. Create the target clean table (e.g., `dbo.Orders_Clean` for `dbo.Orders_Raw`) if it does not exist with standard audit columns: `etl_created_at DATETIME DEFAULT GETDATE()`, `etl_updated_at DATETIME DEFAULT GETDATE()`, and `etl_batch_id INT`. 
  Inside each table-cleaning stored procedure, initialize a temporary staging table (e.g. `#Orders_Staging`) by doing `SELECT * INTO #Orders_Staging FROM dbo.Orders_Clean WHERE 1=0;`. Copy the raw data (utilizing candidate key CTE deduplication and watermarking filters) into the staging table (populating `@run_id` to `etl_batch_id`). Execute all transformations, updates, and validations directly on the staging table `#Orders_Staging`. Finally, truncate/delete records in the target clean table `dbo.Orders_Clean` and insert the fully transformed records from the staging table into `dbo.Orders_Clean`.
- Modular Stored Procedures: Wrap all cleaning steps for each table into dedicated stored procedures named `dbo.etl_clean_<table_base_name>`.
- Master Orchestration: Generate a master coordinator procedure named `dbo.etl_main` that calls all the individual table cleaning stored procedures.
- Execution Logging & Log ID Bugfix: Output DDL to create a logging table named `dbo.etl_log` with columns `id INT IDENTITY(1,1) PRIMARY KEY`, `process_name VARCHAR(100) NOT NULL`, `start_time DATETIME NOT NULL`, `end_time DATETIME NULL`, `status VARCHAR(20) NOT NULL`, `error_message VARCHAR(MAX) NULL`. 
  Inside each stored procedure's TRY block, you MUST first run the `INSERT INTO dbo.etl_log (process_name, start_time, status) VALUES ('...', GETDATE(), 'RUNNING');` statement. IMMEDIATELY AFTER this insert, define and set the batch run ID: `DECLARE @run_id INT = SCOPE_IDENTITY();`. NEVER declare `@run_id = SCOPE_IDENTITY();` before any insert statement has occurred in the procedure, as this returns NULL and breaks audit batch tracking. Wrap the block in a transaction. Commit on success and rollback on failure.
- Balanced Transactions: Every Try-Catch block MUST wrap data modifications in an explicit transaction block. Begin the transaction inside `BEGIN TRY` using `BEGIN TRANSACTION;` immediately after defining `@run_id`. Commit the transaction using `COMMIT TRANSACTION;` at the very end of the `BEGIN TRY` block (after all updates and logging are completed). At the beginning of the `BEGIN CATCH` block, you MUST verify if a transaction is still active and roll it back using: `IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;`. Never omit `BEGIN TRANSACTION` if `COMMIT/ROLLBACK` are used, or vice versa, to ensure compilation succeeds.
- Incremental Loading, Watermarking & Watermark Storage: Stored procedures and the main procedure must accept parameters `@load_type VARCHAR(20) = 'FULL'` and `@last_run DATETIME = NULL`. Generate DDL for `dbo.etl_watermark (process_name VARCHAR(100) PRIMARY KEY, last_run_time DATETIME NOT NULL)`. 
  If `@load_type = 'INCREMENTAL'` and `@last_run IS NULL`, retrieve it using `SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = '...';`. Ensure this `@last_run` filter is fully applied to the copy queries (e.g. `WHERE watermark_col > @last_run`). Update the watermark using `MERGE` upon successful completion.
- Performance Indexing: When creating the clean target table, add DDL commands to create NONCLUSTERED indexes on the primary keys, join/relationship keys, and watermark columns.
- Outlier Mitigation Safety (Catalog Checks): Implement the outlier flagging logic using a reusable stored procedure `dbo.sp_flag_outliers_iqr` that computes IQR and updates outlier flags dynamically. The procedure takes exactly two arguments: `@table_name` and `@column_name`. Validate that input table and column parameters exist in `sys.tables` and `sys.columns` (or `tempdb.sys.columns` for `#` temp tables) before dynamic SQL executions. NEVER call the procedure with extra parameters.
- Reusable Outlier Procedure Call: When executing IQR flagging on a column, invoke it exactly as `EXEC dbo.sp_flag_outliers_iqr '#TableName_Staging', 'ColumnName'`. Do not repeat the execution or call it multiple times for the same column. Only run outlier stored procedures on numeric/metric columns. NEVER execute it on string identifiers, phones, or emails.
- Rule Merging & Single-Scan Consolidation: NEVER generate separate, sequential `UPDATE` statements for each plan step on the same table. Instead:
  1. Merge and consolidate all expression-based updates (like `LTRIM/RTRIM`, `LOWER/UPPER`, case normalization, formatting, phone normalization, date parsing, and range clipping) into a **single multi-column `UPDATE` statement** on the staging table `#<TableName>_Staging`.
  2. Merge all default value fillings and invalid/sentinel replacements into a **single join-based `UPDATE` statement** joining `dbo.etl_default_values` and `dbo.etl_invalid_values` via `LEFT JOIN`s.
- Zero Redundant Operations: Do not output duplicate or redundant CTE statements, updates, or procedure calls. Verify that any deduplication logic, outlier checks, or date/email validation is written once per column.
- Type-Safe String Transformations: If applying `LTRIM`, `RTRIM`, `LOWER`, or `UPPER` on a non-string column (such as numeric/date columns), first cast the column explicitly to a string type (e.g. `CAST(col AS NVARCHAR(MAX))`) before calling the string function, then cast back to the target type. (e.g. `TRY_CAST(NULLIF(LTRIM(RTRIM(CAST(credits AS NVARCHAR(50)))), '') AS INT)`).
- Rejects & Quarantine Logging: Enforce the use of `dbo.etl_rejects` table. Generate DDL to create the rejects logging table `dbo.etl_rejects` if it does not exist:
  ```sql
  CREATE TABLE dbo.etl_rejects (
      id INT IDENTITY(1,1) PRIMARY KEY,
      table_name VARCHAR(100) NOT NULL,
      rejected_row_data NVARCHAR(MAX) NOT NULL,
      reject_reason VARCHAR(255) NOT NULL,
      etl_batch_id INT NOT NULL,
      rejected_at DATETIME DEFAULT GETDATE()
  );
  ```
  For any row that fails validation format constraints (e.g. invalid date formats, invalid email patterns) or referential integrity (joins), you MUST insert the violating records into `dbo.etl_rejects` and delete them from the staging table prior to any transformation/cast steps. Use `(SELECT * FROM staging_alias r2 WHERE r2.[pk] = r.[pk] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)` to serialize the violating row data. Perform validation deletes before applying transformations/casts to preserve the original invalid values.
- Default Value Sanity & No Fake/Placeholder Defaults: Seed defaults/invalid values dynamically. Use `NULL` as the default value strategy for date, email, and phone/identifier columns to prevent downstream data pollution. NEVER hardcode placeholder default values (like `'10120631.5'` for dates, or `'99999'` for Phone/IDs) when filling nulls; un-defaulted values must remain `NULL`. Replace literal default values with lookup queries using `TRY_CAST(default_value AS <type>)` from `dbo.etl_default_values` (dynamic casting based on column data type).
- Multi-Format Date Parsing: When parsing date columns, use a coalesced chain of `TRY_CONVERT` with different format styles (e.g. style 120, 103, 101, 111) to check if the date is valid. For example: `COALESCE(TRY_CONVERT(DATETIME, [date_col], 120), TRY_CONVERT(DATETIME, [date_col], 103), TRY_CONVERT(DATETIME, [date_col], 101), TRY_CONVERT(DATETIME, [date_col], 111))`. If all conversion attempts fail and the value is not empty/null, treat it as a format validation failure, insert it into `dbo.etl_rejects`, and delete it from the staging table.
- Business-Key Deduplication: For row-level deduplication, partition by the candidate primary key and business keys (names containing `id`, `key`, `email`, `code`) instead of all non-key columns, and order the partition descending by watermark column (`ORDER BY <watermark> DESC`) to preserve the latest record. Perform deduplication inside the initial staging `INSERT INTO ... SELECT` statement using a CTE.
- Index-Friendly Numeric Checks: For `zero_to_null` step, check numeric column placeholders without casting (e.g., `WHERE Quantity IN (-999, 999999)`). Avoid casting string columns to `NVARCHAR(MAX)` as well.
- Active Curated Views: Generate active view layers `CREATE VIEW dbo.vw_<table_base>_Fact AS` (instead of commented templates) explicitly listing selected fields from Clean tables and prefixing duplicate fields as `[parent_prefix_col]` to prevent duplicate column view compilation errors.
- Use bracket quoting [column] and TRY_CAST / TRY_CONVERT for safe casts.
- never_drop_rows: UPDATE/SET only, no DELETE FROM for data quality fixes (except when logging to rejects table).
- Email Validation: For Email columns, check for valid email syntax using the exact SQL pattern `NOT LIKE '%_@_%._%'`. If invalid, quarantine any invalid emails into `dbo.etl_rejects` and delete them from the staging table.
- Phone Normalization and Validation: For Phone columns, strip symbols `-`, ` `, `(`, `)` using nested `REPLACE` functions. If the cleaned phone number length is less than 7 or contains non-numeric characters (tested using `LIKE '%[^0-9]%'`), treat it as an invalid phone validation failure, quarantine it to `dbo.etl_rejects`, and delete it from the staging table.
- No Redundant Casts: Prohibit redundant string castings. Do not emit nested castings like `LOWER(CAST(LTRIM(RTRIM(CAST(col AS NVARCHAR(MAX)))) AS NVARCHAR(MAX)))`. If a column is already cast to a string type, or is the result of string functions (`LOWER`, `LTRIM`, `REPLACE`), do not wrap it in additional `CAST` statements.
- Orders Pipeline: Do not make the Orders pipeline a simple copy. Enforce strict date parsing (via coalesced `TRY_CONVERT` chains), status normalization (trim and case normalization), and invalid/null values handling.
- Duplicates Deduplication ordering: Never use non-existent columns (like `etl_created_at`) in `ROW_NUMBER() OVER (PARTITION BY LOWER(LTRIM(RTRIM(CAST([pk] AS NVARCHAR(400))))) ORDER BY ...)` inside the staging copy CTE. Use a business column (like `CreatedDate DESC` or `OrderDate DESC`) for ordering.
- No SELECT DISTINCT * for deduplication: Avoid using expensive, non-key-aware `SELECT DISTINCT` statements. Deduplication must be key-aware using CTE `ROW_NUMBER()`.
- Idempotent and Production-Safe views/joins: Ensure join views use `CREATE VIEW` instead of `SELECT ... INTO` to prevent duplicate view compilation failures or schema write conflicts.
""",
    "sql-ansi": f"""You are a senior data engineer writing portable ANSI SQL ETL scripts.

{_BASE_RULES}
{_PLAN_ACTIONS}

ANSI SQL REQUIREMENTS:
- Header comment block with plan_id.
- Raw -> Staging -> Transform -> Clean Architecture: Raw tables are completely immutable. Never write updates/modifications/deletions directly on raw tables. Create the target clean table (e.g., `Orders_Clean` for `Orders_Raw`) if it does not exist with standard audit columns: `etl_created_at`, `etl_updated_at`, and `etl_batch_id`. 
  Inside each table-cleaning stored procedure, initialize a temporary staging table (e.g. `#Orders_Staging`) by doing `SELECT * INTO #Orders_Staging FROM Orders_Clean WHERE 1=0;`. Copy the raw data (utilizing candidate key CTE deduplication and watermarking filters) into the staging table (populating `@run_id` to `etl_batch_id`). Execute all transformations, updates, and validations directly on the staging table `#Orders_Staging`. Finally, truncate/delete records in the target clean table `Orders_Clean` and insert the fully transformed records from the staging table into `Orders_Clean`.
- Modular Stored Procedures: Wrap all cleaning steps for each table into dedicated stored procedures named `etl_clean_<table_name>`.
- Master Orchestration: Generate a master coordinator procedure named `etl_main` that calls all the individual table cleaning stored procedures.
- Execution Logging & Log ID Bugfix: Output DDL to create a logging table named `etl_log` and log the start, success, and failure status (with errors) within exception blocks. Always insert the log row first, and then capture the generated identity/auto-increment variable immediately to define the batch run ID safely without race conditions.
- Balanced Transactions: Every block/procedure performing data modifications MUST wrap them in an explicit transaction. Start with `BEGIN TRANSACTION;` inside the try block immediately after capturing `@run_id`. End with `COMMIT TRANSACTION;` at the end of the success path. In exception handlers, verify and rollback using `ROLLBACK TRANSACTION;`.
- Incremental Loading, Watermarking & Watermark Storage: Accept `@load_type` and `@last_run` parameters. If incremental, retrieve the watermark value from `etl_watermark` if not provided, and filter raw source rows using the watermark. Prior to inserting the incremental batch, delete matching clean rows by primary key to prevent duplicate records.
- Performance Indexing: Add statements or comments recommending indexes on primary keys, join keys, and watermark columns.
- Rule Merging & Single-Scan Consolidation: NEVER generate separate, sequential `UPDATE` statements for each plan step on the same table. Instead:
  1. Merge and consolidate all expression-based updates (like `LTRIM/RTRIM`, `LOWER/UPPER`, case normalization, formatting, phone normalization, date parsing, and range clipping) into a **single multi-column `UPDATE` statement** on the staging table.
  2. Merge all default value fillings and invalid/sentinel replacements into a **single join-based `UPDATE` statement** joining `etl_default_values` and `etl_invalid_values` via `LEFT JOIN`s.
- Zero Redundant Operations: Do not output duplicate or redundant CTE statements, updates, or procedure calls. Verify that any deduplication logic, outlier checks, or date/email validation is written once per column.
- Type-Safe String Transformations: When trimming or lowercasing non-string columns, first cast the column explicitly to a string type (e.g. `CAST(col AS VARCHAR(50))`) before applying the string function.
- Rejects & Quarantine Logging: Validate date and email constraints and quarantine violating records to an `etl_rejects` table before removing them from the staging table. Perform validation deletes before applying transformations/casts to preserve the original invalid values. Create the table DDL if not exists: `CREATE TABLE etl_rejects (id INT, table_name VARCHAR(100), rejected_row_data VARCHAR(MAX), reject_reason VARCHAR(255), etl_batch_id INT, rejected_at TIMESTAMP)`.
- Default Value Sanity & No Fake/Placeholder Defaults: Use `NULL` as the default value strategy for date, email, and phone/identifier columns to prevent downstream data pollution. NEVER hardcode placeholder default values (like `'10120631.5'` for dates, or `'99999'` for Phone/IDs) when filling nulls; un-defaulted values must remain `NULL`. Use `etl_default_values` lookup table queries (with type-safe dynamic casting based on column data type) and `etl_invalid_values` lookup table queries instead of hardcoded default/sentinel values.
- Multi-Format Date Parsing: Cascaded parsing using conditional conversion attempts (e.g., trying style 120, 103, 101, 111). If all fail, quarantine the row as invalid format into `etl_rejects`.
- Business-Key Deduplication: Partition row-level deduplication by business keys/primary keys, sorting descending by the watermark column to preserve the latest record. Perform deduplication inside the initial staging `INSERT` statement using a CTE rather than doing standalone `DELETE` statements.
- Index-Friendly Numeric Checks: Avoid casting columns for sentinel/placeholder checks where possible (especially for numeric columns).
- Active Curated Views: Generate active view layers `CREATE VIEW vw_<table_base>_Fact AS` explicitly listing selected fields and renaming duplicate joined fields to prevent duplicate column errors.
- Safe casts (CAST/TRY semantics via CASE WHERE not available).
- IQR outlier logic with subqueries or CTEs, not dialect-specific hacks unless noted in comments. Only run outlier checks on numeric/metric columns.
- never_drop_rows: no DELETE for quality fixes (except when logging to rejects table).
- Email Validation: For Email columns, check for valid email syntax using the exact SQL pattern `NOT LIKE '%_@_%._%'`. If invalid, quarantine any invalid emails into `etl_rejects` and delete them from the staging table.
- Phone Normalization and Validation: For Phone columns, strip symbols `-`, ` `, `(`, `)` using nested `REPLACE` functions. If the cleaned phone number length is less than 7 or contains non-numeric characters (tested using `LIKE '%[^0-9]%'`), treat it as an invalid phone validation failure, quarantine it to `etl_rejects`, and delete it from the staging table.
- No Redundant Casts: Prohibit redundant string castings. Do not emit nested castings like `LOWER(CAST(LTRIM(RTRIM(CAST(col AS VARCHAR(MAX)))) AS VARCHAR(MAX)))`. If a column is already cast to a string type, or is the result of string functions (`LOWER`, `LTRIM`, `REPLACE`), do not wrap it in additional `CAST` statements.
- Orders Pipeline: Do not make the Orders pipeline a simple copy. Enforce strict date parsing (via coalesced `TRY_CONVERT` chains), status normalization (trim and case normalization), and invalid/null values handling.
- Duplicates Deduplication ordering: Never use non-existent columns (like `etl_created_at`) in `ROW_NUMBER() OVER (PARTITION BY LOWER(LTRIM(RTRIM(CAST([pk] AS VARCHAR(400))))) ORDER BY ...)` inside the staging copy CTE. Use a business column (like `CreatedDate DESC` or `OrderDate DESC`) for ordering.
- No SELECT DISTINCT * for deduplication: Avoid using expensive, non-key-aware `SELECT DISTINCT` statements. Deduplication must be key-aware using CTE `ROW_NUMBER()`.
- Idempotent and Production-Safe views/joins: Ensure join views use `CREATE VIEW` instead of `SELECT ... INTO` to prevent duplicate view compilation failures or schema write conflicts.
""",
    "pyspark": f"""You are a senior data engineer writing production PySpark ETL.

{_BASE_RULES}
{_PLAN_PARAMS}
{_PLAN_ACTIONS}

PYSPARK REQUIREMENTS:
- Module docstring with plan_id.
- from pyspark.sql import DataFrame; from pyspark.sql import functions as F
- One transform_<dataset>(df: DataFrame) -> DataFrame per dataset.
- Use withColumn, dropDuplicates, percentile_approx for IQR — same semantics as pandas plan.
- never_drop_rows: coalesce/fill only, no filter that drops null-quality rows.
- I/O: use connector_manifest read_snippet_pyspark and write_snippet_pyspark per dataset.
- NEVER use spark.read.csv for .xml files. Use format("com.databricks.spark.xml") for format=xml.
- NEVER write .csv() to a path ending in .xml — use parquet or json matching manifest output_path.
- Load paths via _resolve_data_path(manifest location), not bare filenames like "data_1.json".
- COPY the full _resolve_data_path helper from the user message (uses AZURE_STORAGE_ACCOUNT, DHARA_BLOB_CONTAINER, DHARA_BLOB_BASE_PATH). NEVER return only f"abfss://{{location}}".
- Pipeline order: load -> transform each dataset -> join (if needed) -> write ALL outputs (per-dataset + joined_* if joined).
- Joins: prefix right-hand columns with _prefix_columns before join; store result in dfs["joined_<parent>_<child>"] and WRITE it to parquet.
- Do NOT assign a join to a variable that is never written (no dead df_joined).
- Pre-join: _require_columns for business_rules.required_columns; _warn_duplicate_keys on join keys.
- When plan is per-dataset normalization only (lowercase/hash, no enrichment need): SKIP joins; write each cleaned dataset only.
- never_drop_rows + joins: use how="left" only, never inner.
- if __name__ == "__main__": SparkSession + run_pipeline(spark) with logging.basicConfig(INFO).
- Valid Python 3.10+ invoking PySpark APIs only.
""",
    "adf": f"""You are a senior Azure Data Factory engineer.

{_BASE_RULES}
{_PLAN_PARAMS}
{_PLAN_ACTIONS}

ADF REQUIREMENTS:
- Output JSON with bundle.flows: [clean_only flow, clean_and_joined flow] when relationships.joins exist.
- Use ADF expression language: toLower, toUpper, trim, coalesce, iif, percentile, sha2, regexpReplace.
- derivedColumn transformations: typeProperties.columns[] with name + expression per step params.
- Join transforms: joinType left (never inner when never_drop_rows), leftStream/rightStream from upstream chain.
- Linked services: LS_AzureBlob, datasets DS_<dataset>, DS_<dataset>_cleaned.
- Valid JSON only (no markdown).
""",
}


def is_llm_generation_error(text: str) -> bool:
    return (text or "").strip().startswith(LLM_ERROR_PREFIX)


def normalize_codegen_engine(engine: str, sql_dialect: str = "tsql") -> str:
    e = (engine or "python").lower().strip()
    d = (sql_dialect or "tsql").lower().strip()
    if e in ("spark", "pyspark"):
        return "pyspark"
    if e == "adf":
        return "adf"
    if e in ("sql", "tsql", "ansi") or "sql" in e:
        if e == "ansi" or d == "ansi":
            return "sql-ansi"
        return "sql-tsql"
    return "python"


def _get_llm_client():
    cfg = load_llm_config(purpose="etl_codegen")
    if not cfg:
        return None, None
    if cfg.provider == "azure_openai" and AzureOpenAI and cfg.endpoint:
        client = AzureOpenAI(
            azure_endpoint=cfg.endpoint,
            api_key=cfg.api_key,
            api_version=cfg.api_version or "2024-02-01",
        )
        return client, cfg.model
    if cfg.provider == "openai" and OpenAI:
        return OpenAI(api_key=cfg.api_key), cfg.model
    return None, None


def _strip_markdown_fences(text: str) -> str:
    code = (text or "").strip()
    code = re.sub(r"^```[a-zA-Z0-9_-]*\s*\n?", "", code)
    code = re.sub(r"\n?```\s*$", "", code)
    return code.strip()


def _estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


def _safe_max_tokens(payload_json: str, engine_key: str) -> int:
    context_window = 16000 if engine_key != "adf" else 32000
    system_overhead = 800
    input_tokens = _estimate_tokens(payload_json)
    available = context_window - input_tokens - system_overhead
    cap = 8000 if engine_key != "adf" else 6000
    return max(1500, min(cap, available))


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


def _is_numeric_column(col_name: str, col_meta: dict) -> bool:
    approved_tag = (col_meta.get("semantic_type") or "").lower().strip()
    if approved_tag == "metric":
        return True
    if approved_tag in ("id", "date", "categorical", "text"):
        return False

    c_lower = str(col_name).lower()
    # Identifiers, phones, emails, and dates are semantically NOT numeric measures
    if any(x in c_lower for x in ("phone", "email", "name", "date", "time", "dob", "student_id", "course_id", "instructor", "department")):
        return False
    dtype = str(col_meta.get("dtype") or col_meta.get("inferred_type") or "").lower()
    target_dtype = str(col_meta.get("target_dtype") or "").lower()
    if any(x in dtype for x in ("int", "float", "double", "decimal", "numeric", "real")):
        return True
    if any(x in target_dtype for x in ("int", "float", "double", "decimal", "numeric", "real")):
        return True
    # Fallback keyword checks
    if any(x in c_lower for x in ("credit", "fee", "amount", "price", "quantity", "qty", "count")):
        return True
    return False


def _consolidate_and_filter_datasets(
    datasets: Dict[str, Any],
    source_metadata: Dict[str, Any]
) -> Dict[str, Any]:
    cleaned_datasets = {}
    
    # Priority for sorting steps
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
    
    for ds_name, block in (datasets or {}).items():
        if not isinstance(block, dict):
            cleaned_datasets[ds_name] = block
            continue
            
        steps = block.get("steps") or []
        if not steps:
            cleaned_datasets[ds_name] = block
            continue
            
        ds_meta = source_metadata.get(ds_name) or {}
        columns_meta = ds_meta.get("columns") or {}
        
        filtered_steps = []
        seen_operations = set()
        
        for step in steps:
            if not isinstance(step, dict):
                continue
            action = str(step.get("action") or "").strip().lower()
            col = step.get("column")
            
            # 1. Type-aware filtering: trim/lower/upper/sanitize_email are string-only
            if action in ("trim", "lowercase", "uppercase", "sanitize_email"):
                if col:
                    col_meta = columns_meta.get(col) or {}
                    col_class = _classify_column(col, col_meta)
                    if col_class in ("metric", "date"):
                        continue
            
            # Type-aware filtering: outlier logic is numeric-only
            if action in ("flag_outliers", "clip_or_flag", "clip_outliers", "cap_outliers"):
                if col:
                    col_meta = columns_meta.get(col) or {}
                    if not _is_numeric_column(col, col_meta):
                        continue
                        
            # 2. Operation Deduplication / Normalization
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
            
            filtered_steps.append(step)
            
        # 3. Sort steps according to transform priority order
        def get_step_priority(st):
            act = str(st.get("action") or "").strip().lower()
            return priority.get(act, 99)
            
        sorted_steps = sorted(filtered_steps, key=get_step_priority)
        
        # Re-assign order field
        for idx, st in enumerate(sorted_steps):
            st_copy = dict(st)
            st_copy["order"] = idx + 1
            sorted_steps[idx] = st_copy
            
        cleaned_block = dict(block)
        cleaned_block["steps"] = sorted_steps
        cleaned_datasets[ds_name] = cleaned_block
        
    return cleaned_datasets


def _build_codegen_payload(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    *,
    output_mode: str = "dataframe_only",
    output_path: Optional[str] = None,
) -> Dict[str, Any]:
    source_metadata: Dict[str, Any] = {}
    for ds_name, meta in (assessment.get("datasets") or {}).items():
        cols = meta.get("columns") or {}
        source_metadata[ds_name] = {
            "row_count": meta.get("row_count"),
            "columns": {
                col: {
                    "dtype": cmeta.get("dtype") or cmeta.get("inferred_type"),
                    "null_percentage": cmeta.get("null_percentage"),
                    "semantic_type": cmeta.get("semantic_type", "unknown"),
                }
                for col, cmeta in cols.items()
                if isinstance(cmeta, dict)
            },
        }
    
    # Consolidate, deduplicate, type-filter and sort plan steps before passing to LLM
    raw_datasets = plan.get("datasets") or {}
    cleaned_datasets = _consolidate_and_filter_datasets(raw_datasets, source_metadata)
    
    base = {
        "plan_id": plan.get("plan_id"),
        "engine": plan.get("engine"),
        "output_mode": output_mode,
        "output_path": output_path,
        "business_rules": plan.get("business_rules"),
        "datasets": cleaned_datasets,
        "global_steps": plan.get("global_steps"),
        "manual_review": plan.get("manual_review"),
        "blocked": plan.get("blocked"),
        "source_metadata": source_metadata,
        "source_context": plan.get("source_context") or {},
        "connector_manifest": plan.get("connector_manifest") or {},
        "engine_recommendation": plan.get("engine_recommendation") or {},
        "relationships": plan.get("relationships") or {},
        "etl_intent": plan.get("etl_intent") or {},
    }
    base.update(llm_codegen_extra_context(plan))
    return base


_READ_TEMPLATES: Dict[str, str] = {
    "csv_file": 'df = pd.read_csv(r"{loc}")',
    "excel": 'df = pd.read_excel(r"{loc}", sheet_name=0)',
    "json": 'df = pd.read_json(r"{loc}")',
    "parquet": 'df = pd.read_parquet(r"{loc}")',
    "sql_server": (
        'engine = create_engine("mssql+pyodbc://...")\n'
        'df = pd.read_sql("SELECT * FROM {loc}", engine)'
    ),
    "azure_sql": (
        'engine = create_engine("mssql+pyodbc://...database.windows.net/...")\n'
        'df = pd.read_sql("SELECT * FROM {loc}", engine)'
    ),
    "postgres": (
        'engine = create_engine("postgresql://...")\n'
        'df = pd.read_sql("SELECT * FROM {loc}", engine)'
    ),
    "blob_storage": (
        "# Read from Azure Blob — configure connection string\n"
        'df = pd.read_csv("downloaded_{loc}")'
    ),
    "unknown": 'df = pd.read_csv(r"{loc}")  # TODO: adjust read for your source',
}

_PYSPARK_READ_TEMPLATES: Dict[str, str] = {
    "csv_file": 'df = spark.read.option("header","true").csv(r"{loc}")',
    "parquet": 'df = spark.read.parquet(r"{loc}")',
    "json": 'df = spark.read.json(r"{loc}")',
    "blob_storage": 'df = spark.read.csv("wasbs://container@account.blob.core.windows.net/{loc}")',
    "sql_server": (
        'df = spark.read.format("jdbc").option("dbtable", "{loc}").load()'
    ),
    "unknown": 'df = spark.read.option("header","true").csv(r"{loc}")',
}


def _read_hint_for_payload(engine_key: str, payload: Dict[str, Any]) -> str:
    ctx = payload.get("source_context") or {}
    src_type = str(ctx.get("type") or "unknown")
    loc = str(ctx.get("location") or "data_file")
    if engine_key == "pyspark":
        tmpl = _PYSPARK_READ_TEMPLATES.get(src_type, _PYSPARK_READ_TEMPLATES["unknown"])
    else:
        tmpl = _READ_TEMPLATES.get(src_type, _READ_TEMPLATES["unknown"])
    return tmpl.format(loc=loc)


def _call_llm(
    engine_key: str,
    payload: Dict[str, Any],
    *,
    fix_errors: Optional[List[str]] = None,
    previous_output: Optional[str] = None,
) -> str:
    client, model = _get_llm_client()
    if not client or not model:
        return f"{LLM_ERROR_PREFIX} No LLM credentials (configure AZURE_OPENAI_* or OPENAI_API_KEY)."

    system = SYSTEM_PROMPTS.get(engine_key, SYSTEM_PROMPTS["python"])
    user_parts = [
        f"Target engine: {engine_key}",
        f"ETL policy (must follow):\n{payload.get('policy') or ''}",
        f"Generate complete ETL for this approved plan:\n{json.dumps(payload, indent=2, default=str)}",
    ]
    manifest = payload.get("connector_manifest") or {}
    m_ds = manifest.get("datasets") or {}
    if m_ds:
        read_lines = []
        for ds_name, ent in m_ds.items():
            if not isinstance(ent, dict):
                continue
            snip = ent.get("read_snippet_python") or ent.get("read_snippet_pyspark") or ""
            read_lines.append(f"- {ds_name}: {ent.get('source_type')} @ {ent.get('location')}")
            if snip:
                read_lines.append(f"  read: {snip}")
            if ent.get("output_path"):
                read_lines.append(f"  write: {ent.get('output_path')}")
            fmt = ent.get("format")
            if fmt == "xml":
                read_lines.append(
                    "  CRITICAL: format=xml — do NOT use read_csv or write.csv; use XML read + parquet/json write"
                )
        user_parts.append(
            "CONNECTOR MANIFEST (use these exact read/write patterns per dataset):\n"
            + "\n".join(read_lines[:40])
        )
        if engine_key == "pyspark":
            user_parts.append(
                "REQUIRED _resolve_data_path helper (copy verbatim into generated code):\n"
                f"```python\n{resolve_path_pyspark_helper()}\n```"
            )
            user_parts.append(
                "REQUIRED production helpers (copy if you emit joins or required_columns):\n"
                "```python\n"
                "def _require_columns(df, required, label): ...\n"
                "def _warn_duplicate_keys(df, key_col, label): ...\n"
                "def _prefix_columns(df, prefix, except_cols): ...\n"
                "```\n"
                "Use the template implementations from Agent Dhara io_snippets — do not stub paths."
            )
    elif payload.get("source_context"):
        ctx = payload["source_context"]
        sources = ctx.get("sources") or []
        if len(sources) > 1:
            src_lines = [
                f"- {s.get('dataset')}: {s.get('type')} @ {s.get('location')} ({s.get('row_count', 0):,} rows)"
                for s in sources[:15]
            ]
            user_parts.append(
                "MULTI-SOURCE CONTEXT (one loader per dataset):\n" + "\n".join(src_lines)
            )
        read_hint = _read_hint_for_payload(engine_key, payload)
        user_parts.append(
            f"PRIMARY READ PATTERN:\n```python\n{read_hint}\n```"
        )
    br = payload.get("business_rules") or {}
    if br.get("notes"):
        user_parts.append(
            "BUSINESS NOTES (must honor in generated transforms):\n" + str(br.get("notes"))
        )
    manual = payload.get("manual_review") or []
    if manual:
        mr_lines = []
        for item in manual[:12]:
            ds = item.get("dataset") or "?"
            col = item.get("column") or "?"
            msg = item.get("message") or item.get("guidance") or ""
            mr_lines.append(f"- [{ds}] {col}: {msg}")
        user_parts.append(
            "MANUAL REVIEW (implement in code when business notes require it, especially phone hash/mask):\n"
            + "\n".join(mr_lines)
        )
    if br.get("never_drop_rows"):
        user_parts.append(
            "NEVER_DROP_ROWS (mandatory): preserve every input row. "
            "No inner join, dropna(), or row-filtering that removes records. "
            "For normalization-only plans, transform and write each dataset separately — "
            "skip joins unless business_rules.notes explicitly require a join."
        )
    rel = payload.get("relationships") or {}
    joins = rel.get("joins") or []
    if joins:
        join_lines = []
        for j in joins[:8]:
            join_lines.append(
                f"- {j.get('left_dataset')}.{j.get('left_key')} "
                f"{j.get('join_type', 'inner')} join "
                f"{j.get('right_dataset')}.{j.get('right_key')} "
                f"({j.get('cardinality')}, overlap={j.get('overlap_count')})"
            )
        per_ds_only = all(
            str(st.get("action") or "")
            in (
                "lowercase",
                "uppercase",
                "trim",
                "sanitize_email",
                "normalize_phone",
                "hash_phone",
                "mask_phone",
            )
            for block in (payload.get("datasets") or {}).values()
            for st in (block or {}).get("steps") or []
        )
        if per_ds_only and br.get("never_drop_rows"):
            user_parts.append(
                "JOIN POLICY: Per-dataset normalization only — do NOT emit joins unless "
                "business_rules.notes explicitly require enrichment. Write each cleaned dataset."
            )
        else:
            user_parts.append(
                "DETECTED JOINS (after per-dataset transforms; write joined_* parquet):\n"
                + "\n".join(join_lines)
            )
        if rel.get("load_order"):
            user_parts.append(f"LOAD ORDER: {rel.get('load_order')}")
    if fix_errors:
        user_parts.append(
            "PREVIOUS ATTEMPT FAILED VALIDATION. Fix these specific issues:\n"
            + "\n".join(f"  - {e}" for e in fix_errors)
            + "\nDo NOT repeat these errors."
        )
        if previous_output:
            user_parts.append(f"Previous output (truncated):\n{previous_output[:12000]}")

    payload_json = json.dumps(payload, indent=2, default=str)
    max_tokens = _safe_max_tokens(payload_json, engine_key)

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": "\n\n".join(user_parts)},
            ],
            temperature=0.05,
            max_tokens=max_tokens,
        )
        return _strip_markdown_fences(response.choices[0].message.content or "")
    except Exception as e:
        return f"{LLM_ERROR_PREFIX} generating code with LLM: {e}"


def parse_adf_json_from_llm(text: str) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """Parse ADF mapping JSON from LLM text; returns (object, errors)."""
    errs: List[str] = []
    raw = _strip_markdown_fences(text)
    if not raw:
        return None, ["empty ADF response"]
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj, []
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{[\s\S]*\}", raw)
    if match:
        try:
            obj = json.loads(match.group(0))
            if isinstance(obj, dict):
                return obj, []
        except json.JSONDecodeError as e:
            errs.append(f"JSON parse: {e}")
    else:
        errs.append("no JSON object found in LLM response")
    return None, errs or ["invalid ADF JSON"]


def generate_etl_with_llm(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    engine: str = "python",
    *,
    sql_dialect: str = "tsql",
    output_mode: str = "dataframe_only",
    output_path: Optional[str] = None,
    validation_errors: Optional[List[str]] = None,
    validate_fn: Optional[Callable[[str], Tuple[bool, List[str]]]] = None,
) -> str:
    """
    Generate ETL source text via LLM for python | sql-* | pyspark.
    For ADF use generate_adf_with_llm instead.
    """
    engine_key = normalize_codegen_engine(engine, sql_dialect)
    if engine_key == "adf":
        return f"{LLM_ERROR_PREFIX} Use generate_adf_with_llm for ADF engine."

    payload = _build_codegen_payload(
        plan, assessment, output_mode=output_mode, output_path=output_path
    )
    prev: Optional[str] = None
    if validation_errors:
        prev = "(retry — see validation errors in user message)"
    code = _call_llm(
        engine_key,
        payload,
        fix_errors=validation_errors,
        previous_output=prev,
    )
    if is_llm_generation_error(code):
        return code

    # Single LLM call per generate request; outer handler falls back to template on failure.
    return code


def generate_adf_with_llm(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    *,
    validation_errors: Optional[List[str]] = None,
    validate_fn: Optional[Callable[[Dict[str, Any]], Tuple[bool, List[str]]]] = None,
) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Returns (adf_object, error_message).
    error_message empty on success; on LLM failure error_message is set and first element may be None.
    """
    payload = _build_codegen_payload(plan, assessment)
    raw = _call_llm("adf", payload, fix_errors=validation_errors)
    if is_llm_generation_error(raw):
        return None, raw

    obj, parse_errs = parse_adf_json_from_llm(raw)
    if obj is None:
        fixed_raw = _call_llm(
            "adf",
            payload,
            fix_errors=parse_errs or ["invalid JSON"],
            previous_output=raw,
        )
        if not is_llm_generation_error(fixed_raw):
            obj, parse_errs = parse_adf_json_from_llm(fixed_raw)
            raw = fixed_raw

    if obj is None:
        return None, f"{LLM_ERROR_PREFIX} ADF JSON parse failed: {'; '.join(parse_errs)}"

    if validate_fn:
        ok, errs = validate_fn(obj)
        if not ok and errs:
            fixed_raw = _call_llm(
                "adf",
                payload,
                fix_errors=errs,
                previous_output=raw,
            )
            if not is_llm_generation_error(fixed_raw):
                obj2, _ = parse_adf_json_from_llm(fixed_raw)
                if obj2 is not None:
                    ok2, _ = validate_fn(obj2)
                    if ok2:
                        return obj2, ""
                    obj = obj2
    return obj, ""
