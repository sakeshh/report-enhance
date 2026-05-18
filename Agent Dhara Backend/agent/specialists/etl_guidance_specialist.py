"""ETL Guidance specialist — SQL/Python cleaning code and step-by-step Azure guidance."""
from __future__ import annotations
from typing import Any, Dict, List, Optional


def _generate_sql_for_issue(dataset: str, column: str, issue_type: str) -> str:
    """Generate a simple SQL snippet to clean a specific issue."""
    t = issue_type.lower()
    col = f"[{column}]" if column else "*"
    ds = f"[{dataset}]"
    
    if "null" in t or "missing" in t:
        return f"-- Fix NULLs in {column}\nUPDATE {ds} SET {col} = 'UNKNOWN' WHERE {col} IS NULL;"
    if "duplicate" in t:
        return f"-- Identify duplicates in {column}\nSELECT {col}, COUNT(*) FROM {ds} GROUP BY {col} HAVING COUNT(*) > 1;"
    if "email" in t:
        return f"-- Filter invalid emails\nSELECT * FROM {ds} WHERE {col} NOT LIKE '%@%.%';"
    if "format" in t or "type" in t:
        return f"-- Cast {column} to correct type (example: INT)\nSELECT TRY_CAST({col} AS INT) AS {col}_Cleaned FROM {ds};"
    if "case" in t or "inconsist" in t:
        return f"-- Normalize case on {column}\nUPDATE {ds} SET {col} = LOWER({col}) WHERE {col} IS NOT NULL;"

    return f"-- General clean-up for {issue_type}\nSELECT DISTINCT {col} FROM {ds};"


def _generate_pandas_for_issue(dataset: str, column: str, issue_type: str) -> str:
    """Generate a simple Pandas snippet to clean a specific issue."""
    t = issue_type.lower()
    col = f"'{column}'" if column else None
    
    if "null" in t or "missing" in t:
        return f"# Fix NULLs in {column}\ndf[{col}] = df[{col}].fillna('UNKNOWN')"
    if "duplicate" in t:
        return f"# Identify duplicates in {column}\nduplicates = df[df.duplicated(subset=[{col}], keep=False)]"
    if "email" in t:
        return f"# Filter invalid emails\ndf = df[df[{col}].str.contains(r'[^@]+@[^@]+\\.[^@]+', na=False)]"
    if "format" in t or "type" in t:
        return f"# Cast {column} to correct type (example: numeric)\ndf[{col}] = pd.to_numeric(df[{col}], errors='coerce')"
    
    return f"# General clean-up for {issue_type}\nunique_vals = df[{col}].unique()"


def format_etl_guidance(assessment: Dict[str, Any], message: str = "", context: Optional[Dict[str, Any]] = None) -> str:
    if not assessment:
        return (
            "I don't have a data assessment to work with yet. "
            "Please select a dataset and run 'Generate Report' first so I can give you specific ETL guidance."
        )

    dq = assessment.get("data_quality_issues") or {}
    per_ds = dq.get("datasets") or {}
    
    if not per_ds:
        return "The last assessment didn't find any major data quality issues, so no specific cleaning code is required!"

    msg_low = (message or "").lower()
    # User explicitly asking for SQL snippets (scenario 14) — not full pipeline codegen
    force_sql = "sql" in msg_low or "t-sql" in msg_low or "tsql" in msg_low

    source_type = "sql" if force_sql else "sql"

    # 1. Check if dataset names look like files
    first_ds = list(per_ds.keys())[0].lower() if per_ds else ""
    if not force_sql and (
        any(first_ds.endswith(ext) for ext in [".csv", ".json", ".xml", ".parquet", ".txt", ".xlsx", ".jsonl"])
        or "abfss://" in first_ds
    ):
        source_type = "file"
        
    # 2. Check context if available (unless user asked for SQL snippets explicitly)
    if context and not force_sql:
        if len(context.get("selected_blob_files") or []) > 0:
            source_type = "file"
        elif len(context.get("selected_local_files") or []) > 0:
            source_type = "file"
        elif len(context.get("selected_tables") or []) > 0:
            source_type = "sql"

    lines = []
    
    if source_type == "sql":
        lines.extend([
            "### 🛠️ SQL Remediation Code",
            "Based on the assessment, here are the recommended SQL scripts to clean your tables:",
            ""
        ])
        
        count = 0
        for ds_name, block in per_ds.items():
            issues = block.get("issues") or []
            if not issues: continue
            lines.append(f"**Table:** `{ds_name}`")
            lines.append("```sql")
            for iss in issues[:3]:
                col = iss.get("column")
                itype = iss.get("type") or iss.get("issue_type") or "issue"
                if col and itype:
                    lines.append(_generate_sql_for_issue(ds_name, col, itype))
                    lines.append("")
                    count += 1
            lines.append("```\n")
            if count >= 10: break

        lines.extend([
            "*(Run these statements in Azure Data Studio, SSMS, or the Azure Portal Query Editor.)*"
        ])

    else:
        lines.extend([
            "### 💻 Python/Pandas Remediation Code",
            "Based on the assessment, here is the Python logic to clean your files:",
            ""
        ])
        
        count = 0
        for ds_name, block in per_ds.items():
            issues = block.get("issues") or []
            if not issues: continue
            lines.append(f"**File:** `{ds_name}`")
            lines.append("```python")
            lines.append(f"import pandas as pd\n\n# Load your data\ndf = pd.read_csv('{ds_name}') # Adjust reading method if needed\n")
            for iss in issues[:3]:
                col = iss.get("column")
                itype = iss.get("type") or iss.get("issue_type") or "issue"
                if col and itype:
                    lines.append(_generate_pandas_for_issue(ds_name, col, itype))
                    count += 1
            lines.append("\n# Save cleaned data\ndf.to_csv('cleaned_data.csv', index=False)")
            lines.append("```\n")
            if count >= 10: break

        lines.extend([
            "*(You can run this in a Jupyter Notebook, Azure Synapse, Databricks, or locally.)*"
        ])

    return "\n".join(lines)

