"""
Transformation Suggester (Stage 3: Smart Data Transformation).

Consumes the output of load_and_profile() and produces a structured list of
suggested transformations (e.g. trim whitespace, parse dates, fill nulls).
Can be extended to call an Azure AI Foundry agent to generate SQL/Spark snippets.
"""

from __future__ import annotations

from typing import Any, Dict, List


# Map DQ issue types to suggested actions (rule-based)
# Full action set: trim, parse_dates, fill_or_drop, coerce_numeric, clip_or_flag, deduplicate,
# currency_normalize, replace_values, uppercase, lowercase, fill_forward, fill_backward, fill_sequence,
# standardize_boolean, word_to_number, normalize_phone, regex_replace, range_clip,
# sanitize_email, flatten_nested, zero_to_null
ISSUE_TO_ACTION = {
    "whitespace": "trim",
    "nulls": "fill_or_drop",
    "invalid_date_format": "parse_dates",
    "invalid_email": "sanitize_email",
    "invalid_phone": "normalize_phone",
    "invalid_numeric": "coerce_numeric",
    "negative_values": "clip_or_flag",
    "suspicious_zero": "zero_to_null",
    "mixed_types": "coerce_numeric",
    "nested_structure": "flatten_nested",
    "duplicate_rows": "deduplicate",
    "duplicate_primary_key": "deduplicate_or_alert",
    "custom_one_of": "replace_values",
    "custom_range": "range_clip",
    "custom_regex": "regex_replace",
    "custom_not_null": "fill_or_drop",
    "potential_primary_key": "review_manually",
    "numeric_outliers_iqr": "clip_or_flag",
    "extremely_long_strings": "regex_replace",
    "control_characters_in_text": "regex_replace",
    "empty_string_values": "fill_or_drop",
    "date_range_violation": "review_manually",
    "future_dates": "review_manually",
    "ancient_dates": "parse_dates",
    "constant_column": "review_manually",
    "dominant_value_skew": "review_manually",
    "skewed_distribution": "review_manually",
    "integer_stored_as_float": "review_manually",
    "empty_dataset": "review_manually",
    "duplicate_column_names": "review_manually",
    "case_insensitive_column_collision": "review_manually",
    # Values differ only by casing — normalize in ETL (aligns with assessment recommendation)
    "case_inconsistency": "lowercase",
    "very_wide_table": "review_manually",
    "column_name_whitespace": "review_manually",
    "very_high_cardinality": "review_manually",
    "binary_like_column": "standardize_boolean",
    "very_wide_date_span": "review_manually",
}

# Columns we should NOT coerce to numeric (semantic string columns)
_SKIP_COERCE_SEMANTIC = {"email", "categorical", "free_text"}
# Column names that suggest numeric (apply coerce)
_NUMERIC_COL_PATTERNS = ("_id", "id", "amount", "price", "total", "count", "quantity", "qty", "extent", "number")
# Column names that suggest string (skip coerce for invalid_numeric)
_STRING_COL_NAMES = {"name", "email", "sku", "category", "status", "description", "order_id"}

MANUAL_GUIDANCE: Dict[str, str] = {
    "orphan_foreign_key": (
        "Validate referential integrity in staging: reject orphan keys or load dimension tables first "
        "(SCD/type-2 patterns if applicable)."
    ),
    "constant_column": (
        "Drop this column in ETL — it has zero information variance across all rows. "
        "Including it wastes storage and adds no value to downstream queries."
    ),
    "dominant_value_skew": (
        "Investigate before ETL — check if the dominant value is a legitimate default "
        "or a pipeline fill artifact. If artificial, trace back to source system."
    ),
    "skewed_distribution": (
        "Flag for business review — extreme skew may indicate a data collection issue "
        "or a valid rare-event column. Confirm before applying any ETL transformation."
    ),
    "integer_stored_as_float": (
        "Cast to INT in ETL schema definition if no legitimate decimal values exist. "
        "Reduces storage and prevents float precision issues in aggregations."
    ),
    "duplicate_column_names": (
        "Rename or drop duplicate columns BEFORE any ETL load step. "
        "Duplicate column names will cause silent data loss or errors in most warehouses."
    ),
    "case_insensitive_column_collision": (
        "Standardize ALL column names to lowercase snake_case in ETL schema definition. "
        "e.g. CustomerID and customerid → customer_id"
    ),
    "very_wide_table": (
        "Review with stakeholders — tables with 200+ columns are a strong signal of "
        "poor schema design. Consider vertical partitioning in ETL (split into entity tables)."
    ),
    "column_name_whitespace": (
        "Strip and normalize column names in ETL ingestion step before schema mapping. "
        "Whitespace in column names causes failures in most SQL engines."
    ),
    "very_high_cardinality": (
        "If this is a free-text or UUID column, consider hashing or bucketing in ETL. "
        "High cardinality string columns are expensive for GROUP BY and JOIN operations."
    ),
    "potential_primary_key": (
        "Validate and promote to explicit PRIMARY KEY constraint in warehouse schema. "
        "Add a NOT NULL + UNIQUE constraint in the ETL target table DDL."
    ),
    "date_range_violation": (
        "Validate acceptable date range with business team before writing ETL filter logic. "
        "Add a range guard in ETL: reject or flag rows outside the agreed date window."
    ),
    "future_dates": (
        "Nullify or reject in ETL — future dates in this column are almost certainly "
        "data entry errors. Add an ETL validation rule: date <= current_date."
    ),
    "ancient_dates": (
        "Flag for review — dates before 1900 are likely sentinel/default values "
        "(e.g. 1970-01-01 epoch, 1900-01-01 placeholder). Handle explicitly in ETL."
    ),
    "very_wide_date_span": (
        "Investigate date range span — a 100+ year date span in one column may indicate "
        "mixed date formats being parsed differently. Verify ETL date parser handles all formats."
    ),
    "binary_like_column": (
        "Cast to BOOLEAN in ETL schema — map Y/N, 1/0, yes/no, true/false to native BOOLEAN. "
        "Avoids ambiguity in downstream analytics and BI tools."
    ),
    "empty_dataset": (
        "Do NOT load — dataset is empty. Add an ETL pre-check that aborts the pipeline "
        "and raises an alert if row count = 0 at ingestion."
    ),
}


def _get_manual_guidance(issue_type: str) -> str:
    return MANUAL_GUIDANCE.get(
        issue_type,
        "Review manually before writing ETL logic for this column/dataset.",
    )


def _should_coerce_numeric(ds_name: str, col: str, issue_type: str, assessment_result: Dict[str, Any]) -> bool:
    """Only coerce when column is semantically numeric. Skip name, email, sku, category, etc."""
    col_lower = (col or "").lower().strip()
    if col_lower in _STRING_COL_NAMES:
        return False
    if col_lower in ("id",) or (col_lower.endswith("_id") and col_lower not in ("order_id",)):
        return True
    if col_lower in ("price", "amount", "total", "quantity", "count", "qty") and issue_type in ("invalid_numeric", "mixed_types", "negative_values"):
        return True
    if any(col_lower.endswith(p) or col_lower == p for p in ("name", "email", "sku", "category", "status")):
        return False
    meta = (assessment_result.get("datasets", {}) or {}).get(ds_name, {}).get("columns", {}).get(col, {})
    semantic = (meta.get("semantic_type") or "unknown").lower()
    if semantic in _SKIP_COERCE_SEMANTIC:
        return False
    if semantic == "numeric_id":
        return True
    if any(p in col_lower for p in _NUMERIC_COL_PATTERNS):
        return True
    return False


def suggest_transformations(assessment_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a list of suggested transformations from assessment + DQ output.

    assessment_result: dict returned by load_and_profile() (datasets, relationships,
                       data_quality_issues).

    Returns a structure:
    {
      "suggested_transformations": [
        {
          "dataset": str,
          "column": str | None,
          "issue_type": str,
          "severity": str,
          "message": str,
          "suggested_action": str,
          "row_count_affected": int | None
        },
        ...
      ],
      "summary": { "by_action": {...}, "by_dataset": {...} }
    }
    """
    dq = assessment_result.get("data_quality_issues", {})
    by_dataset = dq.get("datasets", {})
    suggested: List[Dict[str, Any]] = []

    datasets_meta = assessment_result.get("datasets", {}) or {}
    trim_columns: set = set()
    for ds_name, dq_block in by_dataset.items():
        for issue in dq_block.get("issues", []):
            issue_type = issue.get("type", "")
            col = issue.get("column")
            action = ISSUE_TO_ACTION.get(issue_type, "review_manually")
            if action == "review_manually":
                suggested.append(
                    {
                        "dataset": ds_name,
                        "column": col,
                        "issue_type": issue_type,
                        "severity": issue.get("severity", "medium"),
                        "message": issue.get("message", ""),
                        "suggested_action": "review_manually",
                        "manual_guidance": _get_manual_guidance(issue_type),
                        "row_count_affected": issue.get("count"),
                        "auto_fixable": False,
                    }
                )
                continue
            if action == "coerce_numeric" and not _should_coerce_numeric(ds_name, col, issue_type, assessment_result):
                action = "trim"
            suggested.append(
                {
                    "dataset": ds_name,
                    "column": col,
                    "issue_type": issue_type,
                    "severity": issue.get("severity", "medium"),
                    "message": issue.get("message", ""),
                    "suggested_action": action,
                    "manual_guidance": "",
                    "row_count_affected": issue.get("count"),
                    "auto_fixable": True,
                }
            )
            if col and action in ("coerce_numeric", "parse_dates"):
                trim_columns.add((ds_name, col))

    # Robust: add trim before coerce/parse (handles " 123 ", " 2024-01-15 ")
    for (ds_name, col) in trim_columns:
        if not any(s["dataset"] == ds_name and s["column"] == col and s["suggested_action"] == "trim" for s in suggested):
            suggested.append(
                {
                    "dataset": ds_name,
                    "column": col,
                    "issue_type": "proactive_trim",
                    "severity": "low",
                    "message": "Trim before coerce/parse",
                    "suggested_action": "trim",
                    "manual_guidance": "",
                    "row_count_affected": None,
                    "auto_fixable": True,
                }
            )

    # Global issues: orphan FKs → suggest referential cleanup or staging checks
    global_issues = dq.get("global_issues", {})
    for orphan in global_issues.get("orphan_foreign_keys", []):
        suggested.append(
            {
                "dataset": None,
                "column": None,
                "issue_type": "orphan_foreign_key",
                "severity": "medium",
                "message": f"Orphans: {orphan.get('from')} → {orphan.get('to')}",
                "suggested_action": "validate_referential_integrity_or_stage",
                "manual_guidance": _get_manual_guidance("orphan_foreign_key"),
                "row_count_affected": orphan.get("orphan_count"),
                "auto_fixable": True,
            }
        )

    # Summary
    by_action: Dict[str, int] = {}
    by_dataset_count: Dict[str, int] = {}
    for s in suggested:
        by_action[s["suggested_action"]] = by_action.get(s["suggested_action"], 0) + 1
        ds = s["dataset"] or "global"
        by_dataset_count[ds] = by_dataset_count.get(ds, 0) + 1

    return {
        "suggested_transformations": suggested,
        "summary": {
            "by_action": by_action,
            "by_dataset": by_dataset_count,
            "total_suggestions": len(suggested),
            "auto_fixable_count": sum(1 for s in suggested if s.get("suggested_action") != "review_manually"),
            "manual_review_count": sum(1 for s in suggested if s.get("suggested_action") == "review_manually"),
        },
    }


def get_transformation_manifest_for_etl(assessment_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Produce a minimal manifest that an ETL job (e.g. ADF mapping data flow,
    Synapse notebook) can consume: one entry per dataset/column with at least
    one suggested action.
    """
    raw = suggest_transformations(assessment_result)
    manifest: Dict[str, List[Dict[str, Any]]] = {}
    for s in raw["suggested_transformations"]:
        ds = s["dataset"] or "_global"
        if ds not in manifest:
            manifest[ds] = []
        manifest[ds].append({
            "column": s["column"],
            "suggested_action": s["suggested_action"],
            "issue_type": s["issue_type"],
        })
    return {"datasets": manifest, "summary": raw["summary"]}


def build_dq_summary_for_agent(assessment_result: Dict[str, Any]) -> str:
    """
    Build a concise text summary of DQ issues for the Azure Foundry agent prompt.
    Includes dataset, column, issue type, severity, message, sample values, and suggested action.
    """
    lines = ["# Data quality issues (for transformation rule generation)\n"]
    dq = assessment_result.get("data_quality_issues", {})
    suggested = suggest_transformations(assessment_result)
    by_issue = {f"{s['dataset']}|{s['column']}|{s['issue_type']}": s for s in suggested["suggested_transformations"]}

    for ds_name, dq_block in dq.get("datasets", {}).items():
        for issue in dq_block.get("issues", []):
            key = f"{ds_name}|{issue.get('column')}|{issue.get('type')}"
            sug = by_issue.get(key, {})
            action = sug.get("suggested_action", "review_manually")
            samples = issue.get("sample_values") or []
            samples_str = ", ".join(str(x)[:50] for x in samples[:5])
            lines.append(
                f"- Dataset: {ds_name} | Column: {issue.get('column')} | "
                f"Issue: {issue.get('type')} | Severity: {issue.get('severity')} | "
                f"Count: {issue.get('count')} | Suggested action: {action}"
            )
            lines.append(f"  Message: {issue.get('message')}")
            if samples_str:
                lines.append(f"  Sample values: {samples_str}")
            lines.append("")

    for orphan in dq.get("global_issues", {}).get("orphan_foreign_keys", []):
        lines.append(
            f"- [Global] Orphan FK: {orphan.get('from')} -> {orphan.get('to')} "
            f"(count: {orphan.get('orphan_count')}) | Suggested: validate_referential_integrity_or_stage"
        )
        lines.append("")

    return "\n".join(lines).strip() or "No issues found."
