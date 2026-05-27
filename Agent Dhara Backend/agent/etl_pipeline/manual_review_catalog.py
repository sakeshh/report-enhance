"""
Structured resolution options for manual_review plan items.
Each option maps to a codegen action (or noop for keep-as-is).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

ResolutionOption = Dict[str, Any]

_SKIP_ACTIONS = frozenset({"noop", "keep_as_is"})


def _opt(
    opt_id: str,
    label: str,
    action: str,
    *,
    recommended: bool = False,
    description: str = "",
) -> ResolutionOption:
    return {
        "id": opt_id,
        "label": label,
        "action": action,
        "recommended": recommended,
        "description": description,
    }


_DEFAULT_OPTIONS: List[ResolutionOption] = [
    _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop", description="No transform; document in runbook."),
]

_CATALOG: Dict[str, List[ResolutionOption]] = {
    "very_high_cardinality": [
        _opt("hash_sha256", "Hash (SHA-256)", "hash_phone", recommended=True, description="One-way hash for PII-like identifiers."),
        _opt("mask_last4", "Mask (last 4 digits)", "mask_phone", description="Show only last four digits."),
        _opt("exclude_column", "Exclude from output", "exclude_column", description="Drop column before write."),
        _opt("keep_as_is", "Keep raw (accept risk)", "noop"),
    ],
    "future_dates": [
        _opt("nullify_future", "Nullify future dates", "nullify_future_dates", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "date_range_violation": [
        _opt("nullify_out_of_range", "Nullify out-of-range dates", "nullify_future_dates", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "constant_column": [
        _opt("drop_column", "Drop column", "drop_column", recommended=True),
        _opt("keep_as_is", "Keep column", "noop"),
    ],
    "potential_primary_key": [
        _opt("deduplicate", "Deduplicate on column", "deduplicate", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "duplicate_column_names": [
        _opt("exclude_column", "Exclude duplicate columns", "exclude_column", recommended=True, description="Drop colliding columns to prevent ETL load crashes."),
        _opt("keep_as_is", "Rename in source (manual)", "noop"),
    ],
    "case_insensitive_column_collision": [
        _opt("exclude_column", "Exclude colliding columns", "exclude_column", recommended=True, description="Exclude duplicate case-insensitive colliding columns."),
        _opt("keep_as_is", "Standardize names in source (manual)", "noop"),
    ],
    "very_wide_table": [
        _opt("keep_as_is", "Review with stakeholders (skip ETL)", "noop", recommended=True),
    ],
    "column_name_whitespace": [
        _opt(
            "keep_as_is",
            "Rename columns in source (manual)",
            "noop",
            recommended=True,
            description="Whitespace in column names must be fixed at ingest/schema mapping.",
        ),
    ],
    "dominant_value_skew": [
        _opt("flag_outliers", "Flag for audit column", "flag_outliers", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "skewed_distribution": [
        _opt("flag_outliers", "Flag extreme values", "flag_outliers", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "empty_dataset": [
        _opt("keep_as_is", "Abort pipeline in orchestration (manual)", "noop", recommended=True),
    ],
    "very_wide_date_span": [
        _opt("parse_dates", "Parse dates consistently", "parse_dates", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "non_nullable_fill": [
        _opt("fill_nulls", "Fill nulls (median/mean)", "fill_nulls_simple", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "near_duplicate_rows": [
        _opt("deduplicate", "Deduplicate (keep first)", "deduplicate", recommended=True, description="Deduplicate rows to keep only one copy."),
        _opt("keep_as_is", "Keep as-is (allow duplicates)", "noop", description="Do not filter near-duplicates."),
    ],
    "sentinel_numeric_value": [
        _opt("zero_to_null", "Nullify sentinel values", "zero_to_null", recommended=True, description="Replace numeric sentinel values (e.g. -999, 999999) with NULL."),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "punctuation_only_value": [
        _opt("zero_to_null", "Nullify punctuation placeholders", "zero_to_null", recommended=True, description="Replace punctuation-only text (e.g. '###') with NULL."),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "multivariate_outliers": [
        _opt("flag_outliers", "Flag multivariate outliers", "flag_outliers", recommended=True, description="Add boolean audit column flagging row outlier status."),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "all_caps_values": [
        _opt("lowercase", "Standardize to lowercase", "lowercase", recommended=True, description="Convert all values in this column to lowercase."),
        _opt("uppercase", "Standardize to uppercase", "uppercase", description="Convert all values in this column to uppercase."),
        _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop"),
    ],
    "duplicate_insensitive_values": [
        _opt("lowercase", "Standardize case (lowercase) and trim", "lowercase", recommended=True, description="Trim whitespace and standardize case to lowercase to eliminate duplicates."),
        _opt("uppercase", "Standardize case (uppercase) and trim", "uppercase", description="Trim whitespace and standardize case to uppercase to eliminate duplicates."),
        _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop"),
    ],
    "numeric_outliers_zscore": [
        _opt("flag_outliers", "Flag extreme z-score outliers", "flag_outliers", recommended=True, description="Add a boolean audit column flagging statistical outliers."),
        _opt("clip_outliers", "Clip outliers to IQR bounds", "clip_outliers", description="Cap values at statistical IQR boundaries."),
        _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop"),
    ],
    "string_length_outlier": [
        _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop", recommended=True, description="Keep long strings as-is."),
        _opt("exclude_column", "Exclude column from output", "exclude_column", description="Drop column before writing to target."),
    ],
    "date_format_inconsistency": [
        _opt("parse_dates", "Standardize date formats", "parse_dates", recommended=True, description="Convert and parse mixed date strings into standard ISO dates."),
        _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop"),
    ],
    "mixed_date_formats": [
        _opt("parse_dates", "Standardize mixed date formats", "parse_dates", recommended=True, description="Convert and parse mixed date strings into standard ISO dates."),
        _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop"),
    ],
    "at_least_one": [
        _opt("quarantine_all_null", "Quarantine rows where all are null", "at_least_one", recommended=True, description="Move rows to rejects table where all specified columns are NULL."),
        _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop"),
    ],
    "missing_required_column": [
        _opt("skip_requirement", "Skip requirement (remove from rules)", "skip_requirement", recommended=True, description="Remove this column from the list of required columns for this ingestion."),
        _opt("keep_as_is", "Keep as-is (requires external fix)", "noop", description="Accept the requirement without resolving it in the pipeline (will fail validation)."),
    ],
    "business_key_duplicate": [
        _opt("deduplicate", "Deduplicate on business key", "deduplicate", recommended=True, description="Add a deduplication step to group by this key and keep the first/last record."),
        _opt("keep_as_is", "Keep duplicates (allow in output)", "noop", description="Accept duplicates and pass them to target without filtering."),
    ],
    "high_null_percentage": [
        _opt("fill_nulls", "Fill nulls (median/mean/mode)", "fill_nulls_simple", recommended=True, description="Impute null values using median, mean, or mode based on column type."),
        _opt("keep_as_is", "Keep as-is (accept risk)", "noop", description="Allow nulls in the output column."),
    ],
    "orphan_foreign_keys": [
        _opt("reject_orphans", "Validate referential integrity (reject/stage)", "validate_referential_integrity_or_stage", recommended=True, description="Filter out/delete records where foreign key does not exist in target."),
        _opt("keep_as_is", "Keep raw (allow orphans)", "noop", description="Accept orphan keys and pass them to target."),
    ],
}


def manual_review_item_id(dataset: Optional[str], column: Optional[str], issue_type: Optional[str]) -> str:
    ds = (dataset or "_global").strip()
    col = (column or "*").strip()
    it = (issue_type or "unknown").strip()
    return f"{ds}|{col}|{it}"


def get_resolution_options(issue_type: Optional[str]) -> List[ResolutionOption]:
    it = (issue_type or "").strip().lower()
    opts = list(_CATALOG.get(it) or _DEFAULT_OPTIONS)
    if not any(o.get("recommended") for o in opts):
        opts[0] = {**opts[0], "recommended": True}
    return opts


def enrich_manual_review_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Attach id, resolution_options, default_resolution, status."""
    out = dict(item)
    iid = out.get("id") or manual_review_item_id(
        out.get("dataset"), out.get("column"), out.get("issue_type")
    )
    out["id"] = iid
    issue_type = str(out.get("issue_type") or "")
    if issue_type.strip().lower() in _CATALOG:
        opts = get_resolution_options(issue_type)
    else:
        opts = get_dynamic_resolution_options(issue_type, out)
    out["resolution_options"] = opts
    default = next((o["id"] for o in opts if o.get("recommended")), opts[0]["id"] if opts else "keep_as_is")
    out.setdefault("default_resolution", default)
    out.setdefault("status", "pending")
    out.setdefault("selected_resolution", None)
    return out


def get_dynamic_resolution_options(issue_type: str, item: Dict[str, Any]) -> List[ResolutionOption]:
    """
    Queries LLM to generate resolution options for an unmapped anomaly type.
    Maps options to standard actions: noop, drop_column, exclude_column, deduplicate,
    flag_outliers, fill_nulls_simple, zero_to_null, lowercase, uppercase, parse_dates.
    """
    import json
    import os
    import logging
    from agent.model_config import load_llm_config

    opts = [
        _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop", description="No transform; document in runbook.")
    ]

    cfg = load_llm_config()
    if not cfg:
        opts[0]["recommended"] = True
        return opts

    client = None
    try:
        if cfg.provider == "azure_openai":
            from openai import AzureOpenAI
            client = AzureOpenAI(
                azure_endpoint=cfg.endpoint,
                api_key=cfg.api_key,
                api_version=cfg.api_version or "2024-02-01",
            )
        elif cfg.provider == "openai":
            from openai import OpenAI
            client = OpenAI(api_key=cfg.api_key)
    except Exception as e:
        logger = logging.getLogger("agent.manual_review_catalog")
        logger.error(f"Failed to initialize OpenAI client: {e}")
        opts[0]["recommended"] = True
        return opts

    if not client:
        opts[0]["recommended"] = True
        return opts

    system_prompt = (
        "You are an expert ETL engineer. We detected a data quality issue in a dataset.\n"
        "Generate 1 or 2 appropriate cleanup actions from the following allowed set of standard actions:\n"
        "- noop: Keep as-is, do not modify or transform the data.\n"
        "- drop_column: Drop the entire column from the dataset.\n"
        "- exclude_column: Exclude this column from output.\n"
        "- deduplicate: Deduplicate rows based on this column.\n"
        "- flag_outliers: Add a boolean audit column to flag these outlier values.\n"
        "- fill_nulls_simple: Fill missing values with a default value (e.g. median/mean/mode/constant).\n"
        "- zero_to_null: Replace sentinel/magic/placeholder values (e.g. -999, '###') with NULL.\n"
        "- lowercase: Standardize string case to lowercase.\n"
        "- uppercase: Standardize string case to uppercase.\n"
        "- parse_dates: Standardize/parse mixed date strings into clean ISO dates.\n\n"
        "Return a JSON object exactly formatted like this:\n"
        "{\n"
        "  \"options\": [\n"
        "    {\n"
        "      \"id\": \"unique_id_for_option\",\n"
        "      \"label\": \"User-friendly label\",\n"
        "      \"action\": \"one of the allowed standard actions\",\n"
        "      \"recommended\": true/false,\n"
        "      \"description\": \"Description of the cleanup action\"\n"
        "    }\n"
        "  ]\n"
        "}"
    )

    user_prompt = f"Issue Type: {issue_type}\nIssue Context: {json.dumps(item)}"

    try:
        resp = client.chat.completions.create(
            model=cfg.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        raw = resp.choices[0].message.content or "{}"
        parsed = json.loads(raw)
        llm_opts = parsed.get("options") or []
        for opt in llm_opts:
            if isinstance(opt, dict) and opt.get("id") and opt.get("label") and opt.get("action"):
                action = opt.get("action")
                allowed_actions = {
                    "noop", "drop_column", "exclude_column", "deduplicate",
                    "flag_outliers", "fill_nulls_simple", "zero_to_null",
                    "lowercase", "uppercase", "parse_dates"
                }
                if action not in allowed_actions:
                    action = "noop"

                if opt.get("id") == "keep_as_is" or action == "noop":
                    continue

                opts.append({
                    "id": opt.get("id"),
                    "label": opt.get("label"),
                    "action": action,
                    "recommended": bool(opt.get("recommended")),
                    "description": opt.get("description", "")
                })
    except Exception as e:
        logger = logging.getLogger("agent.manual_review_catalog")
        logger.error(f"Failed to generate dynamic resolution options: {e}")

    if not any(o.get("recommended") for o in opts):
        opts[0]["recommended"] = True

    return opts


def action_for_resolution(issue_type: str, resolution_id: str, options: Optional[List[ResolutionOption]] = None) -> Optional[str]:
    if options:
        for o in options:
            if o.get("id") == resolution_id:
                return str(o.get("action") or "")
    for o in get_resolution_options(issue_type):
        if o.get("id") == resolution_id:
            return str(o.get("action") or "")
    return None


def is_skip_action(action: str) -> bool:
    return (action or "").strip().lower() in _SKIP_ACTIONS
