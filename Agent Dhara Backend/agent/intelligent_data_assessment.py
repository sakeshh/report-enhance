"""Intelligent Data Assessment Engine.

This module profiles datasets, detects data quality issues, relationships, and generates reports.
Supported data sources: Azure SQL, filesystem (CSV/TSV/JSON/JSONL/XML/Parquet/XLSX), Azure Blob Storage
"""
from __future__ import annotations

import hashlib
import json
import os
import re

import concurrent.futures
import numpy as np
import xml.etree.ElementTree as ET
import pandas as pd
from typing import Any, Collection, Dict, List, Optional, Tuple

# Import connectors

# ============================================================
# PERFORMANCE CONFIG
# ============================================================
SAMPLING_THRESHOLD = 10_000_000
DEFAULT_SAMPLE_SIZE = 100_000
HEAVY_OPERATION_THRESHOLD = 10_000_000
try:
    from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector
except ImportError:
    AzureSQLPythonNetConnector = None


# ============================================================
# DQ THRESHOLDS (config-driven)
# ============================================================

def load_dq_thresholds(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load DQ thresholds from YAML. If path is None, use env DQ_THRESHOLDS_PATH or config/dq_thresholds.yaml."""
    path = config_path or os.environ.get("DQ_THRESHOLDS_PATH")
    if not path and os.path.isdir("config"):
        path = os.path.join("config", "dq_thresholds.yaml")
    if not path or not os.path.isfile(path):
        return {}
    try:
        import yaml
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _get_threshold(thresholds: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Get nested key from thresholds, e.g. _get_threshold(t, 'severity', 'null_pct_high', default=0.25)."""
    d = thresholds
    for k in keys:
        d = (d or {}).get(k)
        if d is None:
            return default
    return d if d is not None else default


# ============================================================
# SAFE HELPERS (prevent "unhashable type: 'list'" in pandas)
# ============================================================

def _to_key(x: Any) -> Any:
    """Convert list/dict/unhashable objects into stable strings for hashing."""
    try:
        hash(x)
        return x
    except Exception:
        try:
            return json.dumps(x, sort_keys=True, ensure_ascii=False, default=str)
        except Exception:
            return repr(x)


def safe_nunique(series: pd.Series) -> int:
    """Safe nunique even when values are lists/dicts/objects."""
    try:
        if len(series) > SAMPLING_THRESHOLD:
            # For very large datasets, we estimate or use a sample to avoid OOM/freeze.
            sample = series.dropna()
            if len(sample) > DEFAULT_SAMPLE_SIZE:
                sample = sample.sample(DEFAULT_SAMPLE_SIZE, random_state=42)
            return int(sample.map(_to_key).nunique(dropna=True))
        return int(series.nunique(dropna=True))
    except Exception:
        # Fallback for unhashable types (list, dict).
        if len(series) > SAMPLING_THRESHOLD:
            sample = series.dropna().sample(DEFAULT_SAMPLE_SIZE, random_state=42)
            return int(sample.map(_to_key).nunique(dropna=True))
        return int(series.dropna().map(_to_key).nunique(dropna=True))


def safe_is_unique(series: pd.Series) -> bool:
    """Safe uniqueness check on unhashables."""
    try:
        if len(series) > SAMPLING_THRESHOLD:
            # Check if nulls exist first (fast)
            if series.isna().any():
                return False
            sample = series.sample(DEFAULT_SAMPLE_SIZE, random_state=42)
            return bool(sample.map(_to_key).is_unique)
        return bool(series.is_unique and series.notna().all())
    except Exception:
        # Fallback for unhashable
        if len(series) > SAMPLING_THRESHOLD:
            # If it's a large unhashable column, it's very unlikely to be a PK candidate 
            # if it contains complex objects. We'll check a sample.
            sample = series.dropna().sample(DEFAULT_SAMPLE_SIZE, random_state=42)
            coerced = sample.map(_to_key)
            return bool(coerced.is_unique and series.notna().all())
        coerced = series.map(_to_key)
        return bool(coerced.is_unique and series.notna().all())


# ============================================================
# SEMANTIC & DTYPE INFERENCE
# ============================================================

def _strip(x: Any) -> Any:
    return x.strip() if isinstance(x, str) else x


def _is_text_dtype(dtype) -> bool:
    ds = str(dtype).lower()
    return "object" in ds or "string" in ds or "str" in ds or "category" in ds


def _is_actual_numeric_column(col_name: str, approved_semantic_tag: Optional[str] = None) -> bool:
    """
    Check if a column is semantically numeric, filtering out identifiers,
    phones, emails, zipcodes, dates, etc.
    If approved_semantic_tag is provided, it overrides the default heuristics.
    """
    if approved_semantic_tag is not None:
        tag_lower = approved_semantic_tag.lower()
        if tag_lower == "metric":
            return True
        if tag_lower in ("id", "categorical", "date", "text"):
            return False

    c_lower = str(col_name).lower()
    if any(x in c_lower for x in ("phone", "email", "ssn", "zip", "postal", "date", "time", "dob", "stamp")) or c_lower.endswith("_at"):
        return False
    if c_lower.endswith("id") or c_lower.endswith("key") or c_lower.endswith("code"):
        return False
    if any(x in c_lower for x in ("student_id", "course_id", "instructor_id", "batch_id", "run_id")):
        return False
    return True


def scalar_type_distribution(series: pd.Series, max_sample: int = 2000) -> Dict[str, Any]:
    """
    Summarize Python scalar types present in a column.
    Useful for JSON-loaded datasets where pandas dtype is 'object' but values mix int/str/etc.
    """
    try:
        s = series.dropna()
    except Exception:
        s = series
    if len(s) > max_sample:
        try:
            s = s.sample(max_sample, random_state=42)
        except Exception:
            s = s.head(max_sample)

    counts: Dict[str, int] = {
        "str": 0,
        "int": 0,
        "float": 0,
        "bool": 0,
        "dict": 0,
        "list": 0,
        "other": 0,
    }
    total = 0
    for v in s.tolist():
        if v is None:
            continue
        total += 1
        if isinstance(v, bool):
            counts["bool"] += 1
        elif isinstance(v, int):
            counts["int"] += 1
        elif isinstance(v, float):
            counts["float"] += 1
        elif isinstance(v, str):
            counts["str"] += 1
        elif isinstance(v, dict):
            counts["dict"] += 1
        elif isinstance(v, list):
            counts["list"] += 1
        else:
            counts["other"] += 1

    pct = {k: (counts[k] / total if total else 0.0) for k in counts}
    return {"counts": counts, "pct": pct, "sample_size": int(total)}


def detect_semantic_type(values: pd.Series, col_name: str = "") -> str:
    """
    Detect semantic type from values + column name.
    Returns one of:
    date | email | uuid | url | ip_address | boolean_like |
    numeric_id | phone | free_text | categorical | unknown
    """
    col_lower = col_name.lower() if col_name else ""
    # Column-name hint (fastest — no value scan needed)
    if any(hint in col_lower for hint in _PHONE_NAME_HINTS):
        return "phone"

    non_null_vals = values.dropna()
    if len(non_null_vals) > 200:
        sample = non_null_vals.sample(n=200, random_state=42).astype(str)
    else:
        sample = non_null_vals.astype(str)

    if sample.empty:
        return "unknown"

    total = len(sample)

    # UUID — check before numeric_id (UUIDs contain digits)
    if (sample.str.match(_UUID_RE).sum() / total) >= 0.7:
        return "uuid"

    # IP address
    if (sample.str.match(_IP4_RE).sum() / total) >= 0.6:
        return "ip_address"

    # URL
    if (sample.str.match(_URL_RE).sum() / total) >= 0.5:
        return "url"

    # Email
    if sample.str.contains("@", na=False).sum() / total >= 0.5:
        return "email"

    # Boolean-like
    if (sample.str.strip().str.lower().isin(_BOOL_VALS).sum() / total) >= 0.8:
        return "boolean_like"

    # Date (ISO-8601 first, then broader)
    if sample.str.match(r'^\d{4}-\d{2}-\d{2}').sum() / total >= 0.5:
        return "date"

    # Broader date detection using dateutil
    try:
        from dateutil import parser as du_parser
        parsed_ok = 0
        is_date_hint = bool(re.search(r'(?:\b|_)(date|time|dt|created|updated|dob|birth|bday|birthday)(?:\b|_)|(_at\b|\bat\b)', col_lower))
        for v in sample.head(30):
            # If it's a simple numeric value, don't parse as date unless column name hints date or len is 8 (YYYYMMDD)
            val_strip = v.strip()
            if val_strip.replace(".", "", 1).isdigit():
                val_clean = val_strip.split(".")[0]
                if not (is_date_hint or len(val_clean) == 8):
                    continue
            try:
                du_parser.parse(v, fuzzy=False)
                parsed_ok += 1
            except Exception:
                pass
        if parsed_ok / min(30, total) >= 0.7:
            return "date"
    except ImportError:
        pass

    # Numeric ID
    if sample.str.fullmatch(r'\d+').sum() / total >= 0.9:
        return "numeric_id"

    # Free text vs categorical: use mean length
    mean_len = sample.str.len().mean()
    if mean_len > 50:
        return "free_text"

    return "categorical"


def _validate_phone_phonenumbers(val: str, default_region: str = "IN") -> bool:
    """
    Validate phone using Google's libphonenumber.
    Falls back to regex if library not available.
    default_region: ISO 3166-1 alpha-2 (e.g. "IN", "US", "GB").
    Used when number has no + prefix.
    """
    val = str(val).strip()
    if val.endswith(".0"):
        val = val[:-2]
    elif "." in val:
        try:
            parts = val.split(".")
            if len(parts) == 2 and all(c == '0' for c in parts[1]):
                val = parts[0]
        except Exception:
            pass
    try:
        import phonenumbers
        # Try parsing with + prefix (international) first
        try:
            pn = phonenumbers.parse(val, None)
        except Exception:
            # Try with default region fallback
            try:
                pn = phonenumbers.parse(val, default_region)
            except Exception:
                return False
        return phonenumbers.is_valid_number(pn)
    except ImportError:
        # Graceful fallback to existing regex
        return bool(PHONE_RE.match(val))


def _detect_phone_formats(series: pd.Series) -> Dict[str, int]:
    """
    Categorize phone values into format buckets.
    Returns counts per format type.
    """
    try:
        import phonenumbers
        buckets = {"e164": 0, "national": 0, "invalid": 0, "empty": 0}
        for val in series.dropna().astype(str).head(500):
            v = val.strip()
            if v.endswith(".0"):
                v = v[:-2]
            elif "." in v:
                try:
                    parts = v.split(".")
                    if len(parts) == 2 and all(c == '0' for c in parts[1]):
                        v = parts[0]
                except Exception:
                    pass
            if not v:
                buckets["empty"] += 1
                continue
            try:
                pn = phonenumbers.parse(v, "IN")
                fmt = phonenumbers.format_number(pn, phonenumbers.PhoneNumberFormat.E164)
                if v == fmt:
                    buckets["e164"] += 1
                else:
                    buckets["national"] += 1
            except Exception:
                buckets["invalid"] += 1
        return buckets
    except ImportError:
        return {}


def _detect_date_formats(series: pd.Series) -> Dict[str, Any]:
    """
    Analyzes date patterns using regex-based buckets to flag inconsistencies.
    """
    formats = {
        "YYYY-MM-DD": r"^\d{4}-\d{2}-\d{2}$",
        "MM/DD/YYYY": r"^\d{1,2}/\d{1,2}/\d{4}$",
        "DD-MM-YYYY": r"^\d{2}-\d{2}-\d{4}$",
        "YYYY/MM/DD": r"^\d{4}/\d{2}/\d{2}$",
        "other/timestamp": r".+"
    }
    counts = {k: 0 for k in formats}
    unparsed_cnt = 0
    total_non_null = 0
    
    # We run dateutil parser to confirm it is actually parseable as date
    from dateutil import parser as du_parser
    for val in series.dropna().astype(str).head(1000):
        v = val.strip()
        if not v: continue
        total_non_null += 1
        try:
            du_parser.parse(v, fuzzy=False)
            matched = False
            for fmt_name, regex in formats.items():
                if fmt_name != "other/timestamp" and re.match(regex, v):
                    counts[fmt_name] += 1
                    matched = True
                    break
            if not matched:
                counts["other/timestamp"] += 1
        except Exception:
            unparsed_cnt += 1
            
    return {
        "counts": counts,
        "unparsed_count": unparsed_cnt,
        "total_non_null": total_non_null
    }


def _dtype_inference_for_object(series: pd.Series) -> Optional[str]:
    """
    For object dtype, give a human hint for UI:
    - "string" | "numeric_like" | "datetime_like" | "boolean_like" | "nested" | "mixed" | "unknown"
    """
    s = series.dropna().map(_strip)
    if len(s) > 10000:
        s = s.sample(10000, random_state=42)

    # nested?
    try:
        if s.apply(lambda v: isinstance(v, (list, dict))).any():
            return "nested"
    except Exception:
        pass

    # boolean-like
    booleans = {"true", "false", "yes", "no", "0", "1"}
    try:
        if (s.astype(str).str.lower().isin(booleans).mean() > 0.8):
            return "boolean_like"
    except Exception:
        pass

    # numeric-like
    try:
        num = pd.to_numeric(s, errors="coerce")
        if (1.0 - float(num.isna().mean())) > 0.8:
            return "numeric_like"
    except Exception:
        pass

    # datetime-like (guarded: require date-ish separators; avoid numeric IDs being miscast)
    try:
        as_str = s.astype(str)
        # Require at least some obvious date delimiters in the sample.
        # This prevents numeric IDs like 1/2/3... from being interpreted as datetimes by pandas.
        if (as_str.str.contains(r"[-/:T]", regex=True).mean() >= 0.20):
            import warnings
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message="Could not infer format, so each element will be parsed individually",
                )
                dt_coerced = pd.to_datetime(s, errors="coerce")
            if (1.0 - float(dt_coerced.isna().mean())) > 0.8:
                return "datetime_like"
    except Exception:
        pass

    # plain strings?
    try:
        if s.apply(lambda v: isinstance(v, str)).mean() > 0.8:
            return "string"
    except Exception:
        pass

    try:
        if not s.empty:
            return "mixed"
    except Exception:
        pass
    return "unknown"


# ============================================================
# ACCURACY UPGRADE HELPERS
# ============================================================

def count_file_lines(fp: str) -> int:
    try:
        with open(fp, "rb") as f:
            lines = 0
            buf_size = 1024 * 1024
            read_f = f.raw.read
            buf = read_f(buf_size)
            while buf:
                lines += buf.count(b"\n")
                buf = read_f(buf_size)
            return lines
    except Exception:
        return 0


def load_csv_sampled(fp: str, sep: str = ",", max_rows: Optional[int] = None) -> pd.DataFrame:
    if not max_rows:
        return pd.read_csv(fp, sep=sep, low_memory=False)
    
    total_lines = count_file_lines(fp)
    if total_lines <= max_rows:
        return pd.read_csv(fp, sep=sep, low_memory=False)
        
    chunk_size = 50000
    sample_rate = max_rows / max(1, total_lines)
    chunks = []
    
    try:
        for chunk in pd.read_csv(fp, sep=sep, chunksize=chunk_size, low_memory=False):
            target_n = int(round(len(chunk) * sample_rate))
            if target_n > 0:
                sampled_chunk = chunk.sample(n=min(len(chunk), target_n), random_state=42)
                chunks.append(sampled_chunk)
        if chunks:
            df = pd.concat(chunks, ignore_index=True)
            if len(df) > max_rows:
                df = df.sample(n=max_rows, random_state=42).reset_index(drop=True)
            return df
        else:
            return pd.read_csv(fp, sep=sep, nrows=max_rows)
    except Exception:
        try:
            return pd.read_csv(fp, sep=sep, nrows=max_rows)
        except Exception:
            return pd.DataFrame()


def match_column_key(col_key: str, dataset_name: str, column_name: str) -> bool:
    col_key_parts = [p.strip().lower() for p in col_key.split(".")]
    ds_parts = [p.strip().lower() for p in dataset_name.split(".")]
    col_name = column_name.strip().lower()
    
    if not col_key_parts or not col_name:
        return False
        
    if col_key_parts[-1] != col_name:
        return False
        
    if len(col_key_parts) == 1:
        return True
        
    prefix_parts = col_key_parts[:-1]
    min_len = min(len(ds_parts), len(prefix_parts))
    for idx in range(1, min_len + 1):
        if ds_parts[-idx] != prefix_parts[-idx]:
            return False
            
    return True


def get_valid_values_for_column(
    business_rules: Optional[Dict[str, Any]],
    dataset_name: str,
    column_name: str
) -> Optional[List[str]]:
    if not business_rules:
        return None
    vv = business_rules.get("valid_values")
    if not vv or not isinstance(vv, dict):
        return None
    for col_key, vals in vv.items():
        if match_column_key(col_key, dataset_name, column_name):
            if isinstance(vals, list):
                return vals
            return [str(vals)]
    return None


def check_custom_assertions(df: pd.DataFrame, rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Evaluates custom/formula cross-column assertions.
    Unlike check_formula_rules, this does not coerce columns to numeric automatically
    unless they are already numeric, supporting string evaluations (e.g. col == 'IT').
    """
    issues = []
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        assertion = rule.get("assertion")
        if not assertion:
            continue
        severity = str(rule.get("severity", "medium")).lower()
        custom_msg = rule.get("message")
        
        try:
            ref_cols = []
            for col in df.columns:
                pattern = r'\b' + re.escape(col) + r'\b'
                if re.search(pattern, assertion):
                    ref_cols.append(col)
                    
            if not ref_cols:
                continue
                
            res = df.eval(assertion, engine='python')
            
            if isinstance(res, pd.Series):
                viol_mask = ~res.fillna(False)
                viol_cnt = int(viol_mask.sum())
                if viol_cnt > 0:
                    rows = df.index[viol_mask].tolist()
                    msg = custom_msg or f"Custom rule violation: '{assertion}' ({viol_cnt} violations)"
                    issues.append(dq_issue(
                        severity,
                        "custom_rule_violation",
                        msg,
                        column=",".join(ref_cols),
                        count=viol_cnt,
                        rows=rows,
                        sample=df.loc[viol_mask, ref_cols].head(5).to_dict(orient="records")
                    ))
        except Exception as e:
            issues.append(dq_issue(
                "low",
                "custom_rule_error",
                f"Failed to evaluate custom assertion '{assertion}': {str(e)}"
            ))
    return issues


def profile_database_table_full(
    connector: Any,
    table: str,
    df_sample: pd.DataFrame,
    job_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Run aggregate SELECT query in-place to profile 100% of database rows instead of downloading them.
    Exclude text/blob columns. Cast bit to int.
    """
    from agent.jobs_store import add_event
    if job_id:
        add_event(job_id=job_id, level="info", message=f"Performing in-database profiling for table: {table}")
        
    try:
        schema = connector.get_table_schema(table)
    except Exception as e:
        if job_id:
            add_event(job_id=job_id, level="warning", message=f"Failed to get table schema for database profiling: {e}")
        schema = [{"name": c, "type": "varchar", "nullable": "YES"} for c in df_sample.columns]

    if not schema:
        return {}

    unsafe_types = {"text", "ntext", "image", "xml", "geography", "geometry", "varbinary", "binary"}
    select_items = ["COUNT(*) AS [__total_rows__]"]
    profiled_cols = []
    
    for col in schema:
        col_name = col["name"]
        col_type = str(col.get("type", "varchar")).lower()
        if col_type in unsafe_types:
            continue
            
        profiled_cols.append((col_name, col_type))
        col_quoted = f"[{col_name}]"
        
        select_items.append(f"SUM(CASE WHEN {col_quoted} IS NULL THEN 1 ELSE 0 END) AS [{col_name}__null_cnt]")
        select_items.append(f"COUNT(DISTINCT {col_quoted}) AS [{col_name}__distinct_cnt]")
        
        if col_type == "bit":
            select_items.append(f"MIN(CAST({col_quoted} AS INT)) AS [{col_name}__min_val]")
            select_items.append(f"MAX(CAST({col_quoted} AS INT)) AS [{col_name}__max_val]")
        else:
            select_items.append(f"MIN({col_quoted}) AS [{col_name}__min_val]")
            select_items.append(f"MAX({col_quoted}) AS [{col_name}__max_val]")
            
    table_quoted = connector._quote_two_part_name(table)
    sql = f"SELECT {', '.join(select_items)} FROM {table_quoted}"
    
    try:
        res_df = connector.execute_select(sql)
        if res_df.empty:
            return {}
        row_data = res_df.iloc[0].to_dict()
    except Exception as e:
        if job_id:
            add_event(job_id=job_id, level="warning", message=f"In-database profiling SQL failed: {e}")
        return {}
        
    total_rows = int(row_data.get("__total_rows__", 0))
    db_profile = {
        "row_count": total_rows,
        "columns": {}
    }
    
    for col_name, col_type in profiled_cols:
        null_cnt = row_data.get(f"{col_name}__null_cnt")
        distinct_cnt = row_data.get(f"{col_name}__distinct_cnt")
        min_val = row_data.get(f"{col_name}__min_val")
        max_val = row_data.get(f"{col_name}__max_val")
        
        try:
            null_cnt = int(null_cnt) if null_cnt is not None else 0
        except (ValueError, TypeError):
            null_cnt = 0
            
        try:
            distinct_cnt = int(distinct_cnt) if distinct_cnt is not None else 0
        except (ValueError, TypeError):
            distinct_cnt = 0
            
        null_pct = null_cnt / max(1, total_rows)
        is_cpk = (null_cnt == 0 and distinct_cnt == total_rows and total_rows > 0)
        
        db_profile["columns"][col_name] = {
            "null_count": null_cnt,
            "null_percentage": null_pct,
            "unique_count": distinct_cnt,
            "min": min_val,
            "max": max_val,
            "candidate_primary_key": is_cpk
        }
        
    return db_profile


def merge_in_db_profile(sample_profile: Dict[str, Any], db_profile: Dict[str, Any]) -> Dict[str, Any]:
    """
    Overwrites statistical counts, bounds, and PK flags in the sample profile with full DB stats.
    """
    if not db_profile:
        return sample_profile
        
    sample_profile["row_count"] = db_profile.get("row_count", sample_profile.get("row_count", 0))
    sample_profile["sampling_info"] = f"Full dataset has {sample_profile['row_count']:,} rows. Statistics (nulls, min/max, uniqueness) profiled in-database on 100% of rows."
    
    db_cols = db_profile.get("columns") or {}
    sample_cols = sample_profile.setdefault("columns", {})
    
    for col_name, db_col_info in db_cols.items():
        if col_name not in sample_cols:
            sample_cols[col_name] = {}
        col_prof = sample_cols[col_name]
        
        col_prof["null_percentage"] = db_col_info.get("null_percentage", col_prof.get("null_percentage", 0.0))
        col_prof["unique_count"] = db_col_info.get("unique_count", col_prof.get("unique_count", 0))
        col_prof["candidate_primary_key"] = db_col_info.get("candidate_primary_key", col_prof.get("candidate_primary_key", False))
        
        if "min" in db_col_info:
            col_prof["min"] = db_col_info["min"]
        if "max" in db_col_info:
            col_prof["max"] = db_col_info["max"]
            
        if "null_count" in db_col_info:
            col_prof["null_count"] = db_col_info["null_count"]
            
    return sample_profile


# ============================================================
# DATA PROFILING (pandas dtypes + inference hint for object)
# ============================================================

def profile_dataframe(df: pd.DataFrame, job_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Returns a consistent profiling dictionary for a DataFrame, including:
    - row_count, column_count, data_volume_bytes
    - columns: { col: { dtype, dtype_inference?, null_percentage, unique_count, semantic_type, candidate_primary_key }}
    """
    from agent.jobs_store import add_event
    if job_id:
        add_event(job_id=job_id, level="info", message="Profiling columns...")
    row_count = int(len(df))
    col_count = int(len(df.columns))

    # Fast memory usage estimate for large DataFrames
    if row_count > SAMPLING_THRESHOLD:
        # Shallow usage
        shallow = df.memory_usage(deep=False).sum()
        # Estimate deep overhead by sampling object columns
        obj_cols = df.select_dtypes(include=["object"]).columns
        deep_overhead = 0
        if not obj_cols.empty:
            sample_size = min(row_count, DEFAULT_SAMPLE_SIZE // 10) # Smaller sample for memory estimate
            sample = df[obj_cols].sample(sample_size, random_state=42)
            # Subtract shallow size of the sample to get deep overhead
            deep_sample = sample.memory_usage(deep=True).sum()
            shallow_sample = sample.memory_usage(deep=False).sum()
            overhead_per_row = (deep_sample - shallow_sample) / sample_size
            deep_overhead = overhead_per_row * row_count
        data_volume_bytes = int(shallow + deep_overhead)
    else:
        data_volume_bytes = int(df.memory_usage(deep=True).sum())

    profile: Dict[str, Any] = {
        "row_count": row_count,
        "column_count": col_count,
        "data_volume_bytes": data_volume_bytes,
        "sampling_info": f"Full dataset has {row_count:,} rows. Analysis performed on a representative sample of {min(row_count, SAMPLING_THRESHOLD):,} rows." if row_count > SAMPLING_THRESHOLD else "Analysis performed on 100% of rows.",
        "columns": {}
    }

    def profile_col(col: str) -> Tuple[str, Dict[str, Any]]:
        s = df[col]
        dtype_str = str(s.dtype)
        semantic = detect_semantic_type(s, col)
        hint = _dtype_inference_for_object(s) if _is_text_dtype(dtype_str) else None
        if semantic == "numeric_id" and hint == "datetime_like":
            hint = "numeric_like"
        type_dist = scalar_type_distribution(s) if _is_text_dtype(dtype_str) else None

        raw_smp = s.dropna().head(20).astype(str).tolist()

        col_profile = {
            "dtype": dtype_str,
            "dtype_inference": hint,
            "type_distribution": type_dist,
            "null_percentage": float(s.isna().mean()),
            "unique_count": safe_nunique(s),
            "semantic_type": semantic,
            "candidate_primary_key": safe_is_unique(s),
            "raw_samples": raw_smp,
        }
        n_nonnull = int(s.notna().sum())
        if n_nonnull > 0:
            dupes = int(s.duplicated(keep=False).sum())
            if dupes > 0:
                col_profile["duplicate_value_count"] = dupes
            try:
                num = pd.to_numeric(s, errors="coerce")
                nn = num.dropna()
                if len(nn) >= 3:
                    col_profile["mean"] = float(nn.mean())
                    col_profile["median"] = float(nn.median())
                    col_profile["std"] = float(nn.std())
                    if len(nn) >= 8:
                        sk = float(nn.skew())
                        col_profile["skew"] = round(sk, 4)
                        col_profile["p5"] = float(nn.quantile(0.05))
                        col_profile["p95"] = float(nn.quantile(0.95))
            except Exception:
                pass
        return col, col_profile

    # Parallelize column profiling for speed on large datasets
    processed_cols = 0
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(profile_col, col): col for col in df.columns}
        for future in concurrent.futures.as_completed(futures):
            col, col_prof = future.result()
            profile["columns"][col] = col_prof
            processed_cols += 1
            if job_id and col_count > 0:
                pct = int((processed_cols / col_count) * 40) # 0-40% for profiling
                add_event(job_id=job_id, level="info", message=f"Profiling: {pct}% complete")

    return profile


# ============================================================
# SQL LOADER
# ============================================================

def _sql_location_key_prefix(loc: Dict[str, Any], conn: Dict[str, Any], db_index: int, multi_db: bool) -> str:
    """Prefix for dataset keys when multiple database locations are configured."""
    if not multi_db:
        return ""
    for k in ("id", "label", "name"):
        v = loc.get(k)
        if v and str(v).strip():
            s = re.sub(r"[^\w\-]+", "_", str(v).strip())[:48].strip("_")
            if s:
                return s + "__"
    db = str(conn.get("database") or conn.get("Database") or f"db{db_index}")
    srv = str(conn.get("server") or conn.get("Server") or "")
    h = hashlib.md5(f"{srv}|{db}".encode("utf-8")).hexdigest()[:8]
    tail = re.sub(r"[^\w]+", "_", db)[:24].strip("_") or "db"
    return f"{tail}_{h}__"


def load_sql_datasets(
    connection_cfg: Dict[str, Any],
    dataset_key_prefix: str = "",
    max_rows: Optional[int] = None,
    db_connectors_by_dataset: Optional[Dict[str, Tuple[Any, str]]] = None,
    only_tables: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """
    Loads all discovered tables from Azure SQL using the provided connector configuration.
    Returns a dict: { "<schema>.<table>": DataFrame, ... } or prefixed keys if dataset_key_prefix set.
    """
    if AzureSQLPythonNetConnector is None:
        print("[INFO] AzureSQLPythonNetConnector not available, skipping SQL datasets")
        return {}

    p = (dataset_key_prefix or "").strip()
    if p and not p.endswith("__"):
        p = p + "__"

    datasets: Dict[str, pd.DataFrame] = {}
    try:
        connector = AzureSQLPythonNetConnector(connection_cfg)
        tables = connector.discover_tables()

        if only_tables is not None:
            allowed_set = {t.lower() for t in only_tables}
            filtered_tables = []
            for t in tables:
                key = f"{p}{t}" if p else t
                if key.lower() in allowed_set:
                    filtered_tables.append(t)
            tables = filtered_tables

        for table in tables:
            key = f"{p}{table}" if p else table
            try:
                datasets[key] = connector.load_table(table, max_rows=max_rows)
                if db_connectors_by_dataset is not None:
                    db_connectors_by_dataset[key] = (connector, table)
            except Exception as e:
                print(f"[ERROR] Failed to load table {table}: {e}")
    except Exception as e:
        print(f"[INFO] Failed to connect to SQL database: {e}")

    return datasets


# ============================================================
# JSON DEEP-FLATTEN HELPERS
# ============================================================

def _find_record_path(obj: Any, path: Optional[List[str]] = None, max_depth: int = 4) -> Optional[List[str]]:
    """Find nested list-of-dicts path for record_path (e.g., ['departments','employees'])."""
    if path is None:
        path = []
    if max_depth < 0:
        return None
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            return path
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            rp = _find_record_path(v, path + [k], max_depth - 1)
            if rp:
                return rp
    return None


def _json_deep_flatten(data: Any) -> pd.DataFrame:
    from pandas import json_normalize

    if isinstance(data, list):
        if not data:
            return pd.DataFrame()
        if isinstance(data[0], dict):
            return json_normalize(data, max_level=1)
        return pd.DataFrame({"value": data})

    if not isinstance(data, dict):
        return pd.DataFrame([{"value": data}])

    record_path = _find_record_path(data, max_depth=4)
    if not record_path:
        return json_normalize(data, max_level=1)

    meta_keys: List[str] = []

    def collect_scalars(d: Dict[str, Any]) -> None:
        for k, v in d.items():
            if not isinstance(v, (list, dict)):
                if k not in meta_keys:
                    meta_keys.append(k)

    parent: Any = data
    for k in record_path[:-1]:
        if isinstance(parent, dict):
            collect_scalars(parent)
            parent = parent.get(k, {})
        else:
            break

    try:
        return json_normalize(
            data,
            record_path=record_path,
            meta=meta_keys if meta_keys else None,
            errors="ignore"
        )
    except Exception:
        return json_normalize(data, max_level=1)


def _load_json_to_df(path: str, max_rows: Optional[int] = None) -> pd.DataFrame:
    if path.lower().endswith(".jsonl"):
        if max_rows is not None:
            import random
            reservoir = []
            count = 0
            rng = random.Random(42)
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if len(reservoir) < max_rows:
                        reservoir.append(line)
                    else:
                        j = rng.randint(0, count)
                        if j < max_rows:
                            reservoir[j] = line
                    count += 1
            rows = []
            for line in reservoir:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    rows.append({"value": line})
        else:
            rows = []
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        rows.append({"value": line})
        if not rows:
            return pd.DataFrame()
        return pd.json_normalize(rows, max_level=1)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return _json_deep_flatten(data)


# ============================================================
# XML EXPLODE (one row per <Item> container when consistent)
# ============================================================

def _xml_to_df_exploded(path: str) -> pd.DataFrame:
    root = ET.parse(path).getroot()
    nodes = list(root)
    if not nodes:
        return pd.DataFrame()

    if len(set(n.tag for n in nodes)) == 1:
        records: List[Dict[str, Any]] = []
        for node in nodes:
            base: Dict[str, Any] = {}
            containers: List[ET.Element] = []
            for child in node:
                g = list(child)
                if g:
                    containers.append(child)
                else:
                    base[child.tag] = child.text

            exploded = False
            for container in containers:
                items = list(container)
                if not items:
                    continue
                if len({c.tag for c in items}) == 1:
                    exploded = True
                    for item in items:
                        row = dict(base)
                        for sub in item:
                            row[f"{container.tag}_{sub.tag}"] = sub.text
                        records.append(row)
            if not exploded:
                records.append(base)

        return pd.DataFrame(records)

    return pd.DataFrame([{c.tag: c.text for c in node} for node in nodes])


# ============================================================
# FILE LOADER (CSV, TSV, JSON, JSONL, XML, PARQUET, XLSX)
# ============================================================

def load_file_datasets(
    path: str,
    max_rows: Optional[int] = None,
    only_files: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """
    Reads supported files from a local folder and returns a dict: { "<file_name>": DataFrame }
    """
    data: Dict[str, pd.DataFrame] = {}

    if not os.path.isdir(path):
        print("[INFO] Filesystem path not found:", path)
        return data

    files_to_load = os.listdir(path)
    if only_files is not None:
        allowed_set = {f.lower() for f in only_files}
        files_to_load = [f for f in files_to_load if f.lower() in allowed_set]

    for file in files_to_load:
        fp = os.path.join(path, file)
        if not os.path.isfile(fp):
            continue

        try:
            low = file.lower()
            if low.endswith(".csv"):
                data[file] = load_csv_sampled(fp, sep=",", max_rows=max_rows)
            elif low.endswith(".tsv"):
                data[file] = load_csv_sampled(fp, sep="\t", max_rows=max_rows)
            elif low.endswith(".json") or low.endswith(".jsonl"):
                data[file] = _load_json_to_df(fp, max_rows=max_rows)
            elif low.endswith(".xml"):
                data[file] = _xml_to_df_exploded(fp) # XML is harder to sample early
            elif low.endswith(".parquet"):
                # Parquet can be sampled early if we use a different engine, but for now:
                data[file] = pd.read_parquet(fp).head(max_rows) if max_rows else pd.read_parquet(fp)
            elif low.endswith(".xlsx"):
                data[file] = pd.read_excel(fp, engine="openpyxl", nrows=max_rows)
            elif low.endswith(".html") or low.endswith(".htm"):
                tables = pd.read_html(fp)
                data[file] = tables[0] if tables else pd.DataFrame()
        except Exception as e:
            print(f"[ERROR] Reading {file}: {e}")

    return data


# ============================================================
# RELATIONSHIP DETECTION (cardinality + row-level orphan checks)
# ============================================================

def _guess_parent_child_tables(
    n1: str, df1: pd.DataFrame, c1: str,
    n2: str, df2: pd.DataFrame, c2: str,
    meta1: Dict[str, Any], meta2: Dict[str, Any],
) -> Optional[Tuple[str, pd.DataFrame, str, str, pd.DataFrame, str]]:
    """
    Return (parent_ds, parent_df, parent_col, child_ds, child_df, child_col) for FK-style checks, or None.
    """
    nn1 = int(df1[c1].notna().sum())
    nn2 = int(df2[c2].notna().sum())
    if nn1 == 0 or nn2 == 0:
        return None
    u1, u2 = safe_nunique(df1[c1]), safe_nunique(df2[c2])
    r1, r2 = u1 / max(nn1, 1), u2 / max(nn2, 1)

    # Use sampling for large datasets when checking for overlap and cardinality
    if len(df1) > 100_000 or len(df2) > 100_000:
        sample_size = 50_000
        s1 = df1[c1].dropna().sample(min(len(df1[c1].dropna()), sample_size), random_state=42)
        s2 = df2[c2].dropna().sample(min(len(df2[c2].dropna()), sample_size), random_state=42)
        k1 = s1.map(_to_key)
        k2 = s2.map(_to_key)
        try:
            vc1 = k1.value_counts()
            vc2 = k2.value_counts()
            common = vc1.index.intersection(vc2.index)
        except Exception:
            return None
        if len(common) == 0:
            return None
        m1 = int(vc1.reindex(common).fillna(0).max())
        m2 = int(vc2.reindex(common).fillna(0).max())
    else:
        k1 = df1[c1].map(_to_key)
        k2 = df2[c2].map(_to_key)
        try:
            vc1 = k1.dropna().value_counts()
            vc2 = k2.dropna().value_counts()
            common = vc1.index.intersection(vc2.index)
        except Exception:
            return None
        if len(common) == 0:
            return None
        m1 = int(vc1.reindex(common).fillna(0).max())
        m2 = int(vc2.reindex(common).fillna(0).max())

    pk1 = (meta1.get("columns") or {}).get(c1, {}).get("candidate_primary_key")
    pk2 = (meta2.get("columns") or {}).get(c2, {}).get("candidate_primary_key")
    if pk1 and not pk2:
        return (n1, df1, c1, n2, df2, c2)
    if pk2 and not pk1:
        return (n2, df2, c2, n1, df1, c1)
    if r1 >= 0.995 and r2 < 0.97:
        return (n1, df1, c1, n2, df2, c2)
    if r2 >= 0.995 and r1 < 0.97:
        return (n2, df2, c2, n1, df1, c1)
    if m1 == 1 and m2 > 1:
        return (n1, df1, c1, n2, df2, c2)
    if m2 == 1 and m1 > 1:
        return (n2, df2, c2, n1, df1, c1)
    return None


def _classify_cardinality(m1: int, m2: int) -> Tuple[str, str]:
    """
    m1 = max rows per shared key in table A; m2 = max in table B.
    Returns (cardinality_code, human_summary).
    """
    if m1 <= 1 and m2 <= 1:
        return ("one_to_one", "Each key appears at most once in both tables (1:1 on overlapping keys).")
    if m1 <= 1 < m2:
        return ("one_to_many", f"Table A has at most one row per key; table B has up to {m2} rows per key (1:N from A to B).")
    if m2 <= 1 < m1:
        return ("many_to_one", f"Table B has at most one row per key; table A has up to {m1} rows per key (N:1 from A to B).")
    return ("many_to_many", f"Keys repeat on both sides (up to {m1} vs {m2} rows per key) - M:N or bridge-style.")


MAX_REL_ROW_INDEXES = 200


def analyze_cross_dataset_relationships(
    datasets: Dict[str, pd.DataFrame],
    metadata: Dict[str, Any],
    thresholds: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    For each pair of datasets sharing a column name (case-insensitive):
    - overlap count, cardinality (one_to_one / one_to_many / many_to_one / many_to_many)
    - Row-level orphan FK issues (child rows whose key is missing from parent)
    - Warnings for ambiguous M:N on id-like columns
    """
    relationships: List[Dict[str, Any]] = []
    row_issues: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    thresholds = thresholds or {}
    rel_cfg = thresholds.get("relationships") or {}
    include_non_key = bool(rel_cfg.get("include_non_key_columns", False))
    orphan_only_if_same_data = bool(rel_cfg.get("orphan_only_if_same_dataset", True))
    same_data_min_id_overlap = float(rel_cfg.get("same_dataset_min_id_overlap_ratio", 0.90))
    same_data_max_row_diff = float(rel_cfg.get("same_dataset_max_rowcount_diff_ratio", 0.10))

    def _is_key_like(col_lower: str) -> bool:
        return any(x in col_lower for x in ("_id", "id", "key", "code", "sku"))

    def _same_dataset_representation(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
        """
        Heuristic guard: only treat datasets as join-compatible for orphan/FK checks when
        they look like different serializations of the SAME records.
        """
        try:
            cols1 = {str(c).strip().lower() for c in df1.columns}
            cols2 = {str(c).strip().lower() for c in df2.columns}
            if cols1 != cols2 or "id" not in cols1:
                return False
            c1 = next(c for c in df1.columns if str(c).lower() == "id")
            c2 = next(c for c in df2.columns if str(c).lower() == "id")
            k1 = set(df1[c1].map(_to_key).dropna().tolist())
            k2 = set(df2[c2].map(_to_key).dropna().tolist())
            if not k1 or not k2:
                return False
            inter = k1 & k2
            overlap_ratio = len(inter) / max(1, min(len(k1), len(k2)))
            r1, r2 = len(df1), len(df2)
            row_diff_ratio = abs(r1 - r2) / max(1, max(r1, r2))
            if not (overlap_ratio >= same_data_min_id_overlap and row_diff_ratio <= same_data_max_row_diff):
                return False

            # Stronger check: do rows actually match on shared IDs?
            # This avoids falsely treating independent sources with same id range as identical datasets.
            inter_list = list(inter)
            if len(inter_list) > 50:
                inter_list = inter_list[:50]
            # prefer email if present, else compare full row signature excluding id
            cols = [c for c in df1.columns if str(c).lower() != "id"]
            if not cols:
                return True
            # Map id -> normalized tuple of values
            def _row_sig(df: pd.DataFrame, id_col: str) -> Dict[Any, Tuple[Any, ...]]:
                out = {}
                for _, row in df.iterrows():
                    ik = _to_key(row[id_col])
                    if ik is None or ik in out:
                        continue
                    out[ik] = tuple(_to_key(row[c]) for c in cols)
                return out
            m1 = _row_sig(df1, c1)
            m2 = _row_sig(df2, c2)
            if not m1 or not m2:
                return False
            matches = 0
            total = 0
            for ik in inter_list:
                if ik in m1 and ik in m2:
                    total += 1
                    if m1[ik] == m2[ik]:
                        matches += 1
            if total == 0:
                return False
            return (matches / total) >= 0.80
        except Exception:
            return False

    names = list(datasets.keys())

    for i in range(len(names)):
        n1, df1 = names[i], datasets[names[i]]
        meta1 = metadata.get(n1, {}) or {}
        for j in range(i + 1, len(names)):
            n2, df2 = names[j], datasets[names[j]]
            meta2 = metadata.get(n2, {}) or {}
            if df1.empty or df2.empty:
                continue
            common = set(map(str.lower, df1.columns)) & set(map(str.lower, df2.columns))
            for col_lower in common:
                if (not include_non_key) and (not _is_key_like(col_lower)):
                    continue
                c1 = next(x for x in df1.columns if str(x).lower() == col_lower)
                c2 = next(x for x in df2.columns if str(x).lower() == col_lower)
                try:
                    k1_full = df1[c1]
                    k2_full = df2[c2]
                    # No sampling, user wants full analysis
                    k1 = k1_full.map(_to_key)
                    k2 = k2_full.map(_to_key)
                    s1k = set(k1.dropna().tolist())
                    s2k = set(k2.dropna().tolist())
                    overlap = s1k & s2k
                except Exception:
                    continue
                if not overlap:
                    continue
                vc1 = k1.dropna().value_counts()
                vc2 = k2.dropna().value_counts()
                common_idx = vc1.index.intersection(vc2.index)
                m1 = int(vc1.reindex(common_idx).fillna(0).max()) if len(common_idx) else 1
                m2 = int(vc2.reindex(common_idx).fillna(0).max()) if len(common_idx) else 1
                card, summary = _classify_cardinality(m1, m2)
                rel = {
                    "from": f"{n1}.{c1}",
                    "to": f"{n2}.{c2}",
                    "dataset_a": n1,
                    "dataset_b": n2,
                    "column_a": c1,
                    "column_b": c2,
                    "overlap_count": len(overlap),
                    "cardinality": card,
                    "max_rows_per_key_a": m1,
                    "max_rows_per_key_b": m2,
                    "summary": summary,
                    "from_a_to_b": (
                        "one_to_many" if m1 <= 1 < m2 else
                        "many_to_one" if m2 <= 1 < m1 else
                        "one_to_one" if m1 <= 1 and m2 <= 1 else
                        "many_to_many"
                    ),
                }
                relationships.append(rel)

                if m1 > 1 and m2 > 1:
                    id_like = any(
                        x in col_lower for x in ("_id", "id", "key", "code", "sku")
                    )
                    sev = "medium" if id_like else "low"
                    warnings.append({
                        "severity": sev,
                        "type": "many_to_many_relationship",
                        "datasets": [n1, n2],
                        "columns": [c1, c2],
                        "message": (
                            f"{n1}.{c1} <-> {n2}.{c2}: keys repeat on both sides "
                            f"(max {m1} rows per key in {n1}, max {m2} in {n2})."
                        ),
                        "recommendation": (
                            "If you expected a parent-child (1:N) model, deduplicate keys on the 'one' side "
                            "or fix source extraction. If M:N is correct (e.g. orders-products), model it with "
                            "a junction table and FK constraints."
                        ),
                    })

                guess = _guess_parent_child_tables(n1, df1, c1, n2, df2, c2, meta1, meta2)
                if guess:
                    _pn, pdf, pc, cn, cdf, cc = guess
                    if orphan_only_if_same_data and not _same_dataset_representation(pdf, cdf):
                        continue
                    try:
                        parent_keys = set(_to_key(x) for x in pdf[pc].dropna())
                    except Exception:
                        parent_keys = set()
                    if not parent_keys:
                        continue
                    ck = cdf[cc].map(lambda x: _to_key(x) if pd.notna(x) else None)
                    orphan = cdf[cc].notna() & ~ck.isin(parent_keys)
                    oc = int(orphan.sum())
                    if oc > 0:
                        oidx = cdf.index[orphan].tolist()[:MAX_REL_ROW_INDEXES]
                        samples = list(cdf.loc[orphan, cc].head(8))
                        row_issues.append({
                            "severity": "high",
                            "type": "orphan_foreign_key_rows",
                            "dataset": cn,
                            "column": cc,
                            "related_dataset": _pn,
                            "related_column": pc,
                            "count": oc,
                            "row_indexes": oidx,
                            "sample_values": samples,
                            "message": (
                                f"{oc} row(s) in '{cn}' column '{cc}' reference value(s) not found in "
                                f"'{_pn}'.'{pc}' (orphan / broken FK)."
                            ),
                            "recommendation": (
                                f"1) Add missing keys to '{_pn}' or remove bad rows from '{cn}'. "
                                f"2) Enforce FK in the source DB or pipeline. "
                                f"3) Trim/normalize keys (whitespace, type) if mismatch is format-only."
                            ),
                        })

    return {
        "relationships": relationships,
        "relationship_row_issues": row_issues,
        "relationship_warnings": warnings,
    }


def detect_relationships(
    datasets: Dict[str, pd.DataFrame],
    metadata: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Returns enriched relationship list (cardinality, summaries)."""
    return analyze_cross_dataset_relationships(datasets, metadata or {})["relationships"]


def analyze_cross_dataset_consistency(
    datasets: Dict[str, pd.DataFrame],
    metadata: Dict[str, Any],
    thresholds: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Cross-dataset insights for data engineers:
    - ID type drift across datasets (e.g., JSON mixed str/int vs CSV int)
    - Likely duplicate representations (same schema + high ID overlap)
    """
    thresholds = thresholds or {}
    out: List[Dict[str, Any]] = []

    # ID type drift
    try:
        id_summaries: Dict[str, Any] = {}
        for name, df in datasets.items():
            id_col = None
            for c in df.columns:
                cl = str(c).lower()
                if cl == "id" or cl.endswith("_id"):
                    id_col = c
                    break
            if id_col is None:
                continue
            td = scalar_type_distribution(df[id_col])
            id_summaries[name] = {"column": str(id_col), "type_distribution": td}

        if len(id_summaries) >= 2:
            def _bucket(td: Dict[str, Any]) -> str:
                pct = (td.get("pct") or {})
                strp = float(pct.get("str", 0.0))
                nump = float(pct.get("int", 0.0)) + float(pct.get("float", 0.0))
                if strp >= 0.10 and nump >= 0.10:
                    return "mixed_str_num"
                if nump >= 0.80:
                    return "mostly_numeric"
                if strp >= 0.80:
                    return "mostly_string"
                return "other"

            buckets = {ds: _bucket(v["type_distribution"]) for ds, v in id_summaries.items()}
            if len(set(buckets.values())) >= 2:
                out.append({
                    "severity": "high",
                    "type": "id_type_drift_across_datasets",
                    "message": "ID column uses inconsistent scalar types across datasets (serialization/type drift).",
                    "details": {"buckets": buckets, "samples": id_summaries},
                })
    except Exception:
        pass

    # Duplicate representation candidates: schema match + high ID overlap
    try:
        dupe_cfg = thresholds.get("duplicate_detection") or {}
        min_overlap = float(dupe_cfg.get("min_id_overlap_ratio", 0.95))
        max_row_diff = float(dupe_cfg.get("max_rowcount_diff_ratio", 0.05))

        def _schema_sig(df: pd.DataFrame) -> Tuple[str, ...]:
            return tuple(sorted({str(c).strip().lower() for c in df.columns}))

        groups: Dict[Tuple[str, ...], List[str]] = {}
        for name, df in datasets.items():
            groups.setdefault(_schema_sig(df), []).append(name)

        for sig, names in groups.items():
            if len(names) < 2 or len(sig) == 0:
                continue
            for i in range(len(names)):
                for j in range(i + 1, len(names)):
                    a, b = names[i], names[j]
                    dfa, dfb = datasets[a], datasets[b]
                    if "id" not in [str(c).lower() for c in dfa.columns] or "id" not in [str(c).lower() for c in dfb.columns]:
                        continue
                    ca = next(c for c in dfa.columns if str(c).lower() == "id")
                    cb = next(c for c in dfb.columns if str(c).lower() == "id")
                    inter = set()
                    # Sampling for large datasets in duplicate representation check
                    if len(dfa) > 100_000 or len(dfb) > 100_000:
                        sample_a = dfa[ca].dropna().sample(min(len(dfa), 50_000), random_state=42).map(_to_key)
                        sample_b = dfb[cb].dropna().sample(min(len(dfb), 50_000), random_state=42).map(_to_key)
                        ka = set(sample_a.tolist())
                        kb = set(sample_b.tolist())
                    else:
                        ka = set(dfa[ca].map(_to_key).dropna().tolist())
                        kb = set(dfb[cb].map(_to_key).dropna().tolist())
                    
                    if not ka or not kb:
                        continue
                    inter = ka & kb
                    overlap_ratio = len(inter) / max(1, min(len(ka), len(kb)))
                    ra, rb = len(dfa), len(dfb)
                    row_diff_ratio = abs(ra - rb) / max(1, max(ra, rb))
                    if overlap_ratio >= min_overlap and row_diff_ratio <= max_row_diff:
                        out.append({
                            "severity": "medium",
                            "type": "duplicate_representation_candidate",
                            "message": f"Datasets '{a}' and '{b}' likely represent the same records in different formats.",
                            "details": {
                                "schema_columns": list(sig)[:30],
                                "id_overlap_ratio": round(overlap_ratio, 4),
                                "id_overlap_count": len(inter),
                                "row_counts": {a: ra, b: rb},
                            },
                        })
    except Exception:
        pass

    # Enrich recommendations
    for it in out:
        enrich_issue_with_recommendation(it)
    return out


def build_executive_summary_items(
    per_dataset_dq: Dict[str, Any],
    global_issues: Dict[str, Any],
    thresholds: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Business-first summary: rank the most impactful signals into a small list.
    Uses a lightweight scoring model (severity * datasets affected).
    """
    thresholds = thresholds or {}
    cfg = thresholds.get("executive_summary") or {}
    max_items = int(cfg.get("max_items", 8))
    sev_w = {"high": 3.0, "medium": 2.0, "low": 1.0}

    rollup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for ds, block in (per_dataset_dq or {}).items():
        issues = (block or {}).get("issues") or []
        for it in issues:
            typ = str(it.get("type") or "")
            col = str(it.get("column") or "")
            key = (typ, col)
            r = rollup.setdefault(key, {"type": typ, "column": col, "datasets": set(), "sev_max": "low", "rows": 0})
            r["datasets"].add(ds)
            r["rows"] += int(it.get("count") or 0)
            if sev_w.get(str(it.get("severity") or "low"), 1) > sev_w.get(r["sev_max"], 1):
                r["sev_max"] = str(it.get("severity") or "low")

    # add cross-dataset consistency signals
    for it in (global_issues.get("cross_dataset_consistency") or []):
        if not isinstance(it, dict):
            continue
        key = (str(it.get("type") or ""), "")
        r = rollup.setdefault(key, {"type": key[0], "column": "", "datasets": set(), "sev_max": "low", "rows": 0})
        if sev_w.get(str(it.get("severity") or "low"), 1) > sev_w.get(r["sev_max"], 1):
            r["sev_max"] = str(it.get("severity") or "low")

    ranked = []
    for r in rollup.values():
        ds_count = len(r["datasets"]) if r["datasets"] else 1
        score = sev_w.get(r["sev_max"], 1.0) * (1.0 + min(3.0, ds_count / 2.0))
        ranked.append({**r, "datasets_affected": ds_count, "score": float(score)})
    ranked.sort(key=lambda x: (-x.get("score", 0.0), -x.get("datasets_affected", 0), -x.get("rows", 0)))

    items = []
    for x in ranked[:max_items]:
        items.append({
            "title": x["type"] + (f" ({x['column']})" if x.get("column") else ""),
            "severity": x.get("sev_max"),
            "datasets_affected": x.get("datasets_affected"),
            "estimated_rows_affected": x.get("rows"),
            "recommendation": DQ_ISSUE_RECOMMENDATIONS.get(x["type"], _DEFAULT_REC),
        })
    return items


# ============================================================
# DATA QUALITY CHECKS (with row indexes, config-driven thresholds)
# ============================================================

PLACEHOLDERS = {
    "", " ", "-", "--", "---", "n/a", "na", "none", "null", "nil",
    "unknown", "not available", "missing", "undefined", "not applicable",
    "tbd", "tba", "n.a.", "n.a", "#n/a", "#null!", "#value!", "#ref!",
    "#div/0!", "error", "nan", "inf", "-inf", "0000-00-00", "1900-01-01",
    "9999-12-31", "00", "000", "0000", "?", "??", "???", "!",
    "temp", "test", "dummy", "placeholder", "na.", "na,", "not set",
    "unknown unknown", "n.d.", "nd", "not known",
}
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PHONE_RE = re.compile(r"^[+()\-\.\s0-9]{7,}$")
URL_RE = re.compile(
    r"^(https?://|ftp://|www\.)[^\s/$.?#][^\s]*$",
    re.IGNORECASE,
)
INVALID_URL_RE = re.compile(
    r"^(https?://|ftp://|www\.).*",
    re.IGNORECASE,
)
HTML_TAG_RE = re.compile(r"<[a-zA-Z][^>]*>|</[a-zA-Z]+>")
UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
PUNCTUATION_ONLY_RE = re.compile(r"^[\W_]+$")
LEADING_ZERO_RE = re.compile(r"^0[0-9]+$")
MULTI_SPACE_RE = re.compile(r"  +")  # two or more consecutive spaces

_UUID_RE = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
)
_URL_RE = re.compile(r'^https?://', re.IGNORECASE)
_IP4_RE = re.compile(r'^\d{1,3}(?:\.\d{1,3}){3}$')
_IP6_RE = re.compile(r'^[0-9a-fA-F:]{7,39}$')
_BOOL_VALS = frozenset({"true","false","yes","no","y","n","1","0","t","f","on","off"})
_PHONE_NAME_HINTS = frozenset({
    "phone","mobile","contact","tel","cell","fax",
    "whatsapp","landline","ph_no","phno","phone_no",
    "telephone","phn","mob","cellphone","handphone"
})
SENTINEL_NUMBERS = {
    -999, -9999, -99999, -999999, -9999999,
    999, 9999, 99999, 999999, 9999999,
    -1, -99, -100, -1000,
    0.0, -0.0,
    1111, 1234, 12345, 123456, 1234567,
    9876, 98765, 9876543,
    11111, 22222, 33333, 44444, 55555, 66666, 77777, 88888,
}
BOOL_VARIANTS: Dict[str, set] = {
    "true_false": {"true", "false"},
    "yes_no": {"yes", "no"},
    "y_n": {"y", "n"},
    "1_0": {"1", "0"},
    "on_off": {"on", "off"},
    "active_inactive": {"active", "inactive"},
    "enabled_disabled": {"enabled", "disabled"},
}

_DEFAULT_REC = (
    "Review with domain owners; document the expected rule; add validation at ingest or in the warehouse."
)
DQ_ISSUE_RECOMMENDATIONS: Dict[str, str] = {
    "nulls": "Map source placeholders to NULL; fix upstream capture; use defaults only where business-approved.",
    "whitespace": "Trim strings in ETL (e.g. TRIM in SQL, str.strip in pandas) before load or constraint checks.",
    "invalid_email": "Reject or quarantine bad emails; validate with regex or a mailbox API at entry.",
    "invalid_phone": "Normalize to E.164 or national format; strip junk characters in staging.",
    "invalid_date_format": "Standardize to ISO-8601 in pipeline; use robust parse with explicit format/locale.",
    "invalid_numeric": "Coerce after trim; fix type in source; quarantine non-numeric rows for manual fix.",
    "negative_values": "Clip to zero if business allows, or flag rows; verify sign convention in source.",
    "suspicious_zero": "Treat 0 as missing if appropriate, or validate IDs never zero at source.",
    "mixed_types": "Cast column to single type in ETL; split into two columns if genuinely mixed semantics.",
    "nested_structure": "Flatten JSON/XML to scalar columns or child tables before relational load.",
    "duplicate_rows": "Deduplicate on business key (keep latest by timestamp); add uniqueness constraint.",
    "duplicate_primary_key": "Resolve duplicates before load; enforce PRIMARY KEY in database.",
    "potential_primary_key": "Promote column to natural key in modeling docs; add UNIQUE constraint if stable.",
    "empty_dataset": "Verify extract scope and filters; re-run load or fix source path.",
    "duplicate_column_names": "Rename duplicate columns in extract; use explicit aliases in SQL SELECT.",
    "case_insensitive_column_collision": "Rename to a single convention (snake_case); avoid Windows/Excel collisions.",
    "very_wide_table": "Split wide tables by domain or normalize repeating groups.",
    "column_name_whitespace": "Rename columns to strip/replace spaces for SQL compatibility.",
    "date_range_violation": "Swap dates if reversed by mistake; invalidate rows that violate business window.",
    "constant_column": "Drop column if no variance, or fix extract if value should vary.",
    "dominant_value_skew": "Investigate default/fill behavior; segment by dimensions to see real spread.",
    "very_high_cardinality": "Confirm not free-text in ID column; consider hashing or surrogate keys for privacy.",
    "binary_like_column": "Encode as boolean 0/1; document semantics for both values.",
    "numeric_outliers_iqr": "Winsorize, cap, or investigate fraud/measurement errors; document exclusion rules.",
    "skewed_distribution": "Apply log transform for analytics or stratify reporting; check for contamination.",
    "integer_stored_as_float": "Cast to integer type (Int64 nullable) to avoid float drift.",
    "future_dates": "Correct clock skew or data entry; set max date validation at source.",
    "ancient_dates": "Fix century typos or replace sentinel dates with NULL.",
    "very_wide_date_span": "Split historical vs operational feeds if span is implausible for one entity.",
    "extremely_long_strings": "Truncate with audit trail, or move large text to blob/document store.",
    "empty_string_values": "Normalize empty string to NULL for consistent SQL semantics.",
    "control_characters_in_text": "Strip non-printable chars in ETL; fix export encoding (UTF-8).",
    "mixed_scalar_types": "Standardize to a single scalar type in ETL (e.g. cast all IDs to string or integer) and enforce it at ingest.",
    "case_inconsistency": "Normalize case in ETL (e.g. UPPER/LOWER) and consider adding a canonical mapping table for display.",
    "name_format_inconsistency": "Normalize name presentation (trim, collapse spaces, title-case) in staging; preserve raw in an audit column if needed.",
    "mixed_phone_formats": "Normalize phones to a single canonical format (prefer E.164) and validate length/country rules at capture time.",
    "systematic_placeholder": "Investigate upstream defaults; replace placeholders with NULL and enforce domain constraints at source.",
    "out_of_range": "Clip/correct out-of-range values or quarantine rows; confirm expected business bounds with domain owners.",
    "id_type_drift_across_datasets": "Align serialization across formats: IDs should use a consistent type (string or integer) across JSON/CSV/XML exports.",
    "duplicate_representation_candidate": "These datasets likely represent the same entities in different formats; avoid double-counting and choose a system-of-record.",
    "custom_one_of": "Map invalid values to allowed enum or reject rows per data contract.",
    "custom_range": "Clip to bounds or reject; align with business limits.",
    "custom_regex": "Fix format at source or apply regex replace in staging.",
    "custom_not_null": "Backfill from upstream or drop incomplete rows per policy.",
    # --- NEW CHECKS ---
    "invalid_url": "Validate URL format at entry; normalize with urllib.parse; reject structurally invalid URLs.",
    "html_tags_in_text": "Strip HTML/XML tags with BeautifulSoup or regex before storing in a plain-text column.",
    "punctuation_only_value": "Replace symbol-only strings with NULL; investigate upstream export bugs.",
    "sentinel_numeric_value": "Replace sentinel values (-999, 9999999, etc.) with NULL; enforce domain constraints at source.",
    "boolean_inconsistency": "Standardize to a single boolean representation (True/False or 1/0) across the pipeline.",
    "internal_whitespace": "Collapse consecutive spaces with REGEXP_REPLACE or str.strip in ETL.",
    "non_ascii_characters": "Normalize to UTF-8; strip or transliterate unexpected non-ASCII if column is meant to be ASCII.",
    "invalid_uuid": "Enforce UUID v4 format at source; regenerate malformed UUIDs.",
    "leading_zeros_on_numeric_id": "Store leading-zero IDs as VARCHAR/TEXT to prevent data loss on integer cast.",
    "numeric_outliers_zscore": "Investigate extreme z-score outliers (>4 std devs); likely data entry errors, test records, or fraud signals.",
    "round_number_anomaly": "Suspiciously round numbers may indicate estimates or placeholders; validate exact source values.",
    "date_clumping_jan1": "Dates clustering on Jan 1 often indicate default/dummy dates; replace with actual dates or NULL.",
    "date_clumping_month_end": "Dates clustering on month-end may indicate estimated or rolled-up dates; verify with source.",
    "all_caps_values": "Inconsistent all-caps entries may indicate data entry from legacy systems; normalize case in ETL.",
    "string_length_outlier": "Strings significantly longer than the column average may contain concatenated data or free-text errors.",
    "numeric_precision_anomaly": "Mixing integers with high-precision floats suggests inconsistent data capture; standardize precision.",
    "impossible_date": "Dates like 2000-02-30 or 2001-13-01 are structurally invalid; fix upstream date parsing.",
    "weekend_date_anomaly": "Business transactions on weekends may be legitimate or may indicate date errors; verify with domain.",
    "duplicate_insensitive_values": "Values that differ only by case/whitespace produce false uniqueness; deduplicate after normalization.",
    "low_variance_numeric": "Near-constant numeric column may indicate fill/default behavior rather than real data.",
    "high_null_ratio_in_key_column": "Key or ID columns with high null rates cannot reliably join; backfill or reject at ingest.",
    "mixed_date_formats": "Multiple date formats in the same column (e.g. DD/MM/YYYY vs YYYY-MM-DD) cause silent parse errors.",
    "implausible_age": "Age values outside the range 0-150 are likely data entry errors; validate at source.",
    "implausible_percentage": "Percentage values outside 0-100 are structurally invalid; add range constraint at ingest.",
    "timezone_inconsistency": "Datetime values mixing timezone-aware and timezone-naive records cause comparison errors.",
    "string_with_only_digits_in_text_column": "Text columns containing only digits may indicate a schema mismatch or misrouted data.",
    "repeated_token_in_string": "Values with repeated words/tokens (e.g. 'test test') often indicate data entry errors.",
    "near_duplicate_rows": "Rows that are identical except for one or two fields may be erroneous duplicates; deduplicate or merge.",
}


def enrich_issue_with_recommendation(issue: Dict[str, Any]) -> None:
    if issue.get("recommendation"):
        return
    issue["recommendation"] = DQ_ISSUE_RECOMMENDATIONS.get(
        issue.get("type") or "", _DEFAULT_REC
    )


FIXABILITY_BY_ISSUE_TYPE: Dict[str, str] = {
    # deterministic transforms
    "whitespace": "FIXABLE",
    "empty_string_values": "FIXABLE",
    "case_inconsistency": "FIXABLE",
    "mixed_scalar_types": "FIXABLE",
    "integer_stored_as_float": "FIXABLE",
    "control_characters_in_text": "FIXABLE",
    "nested_structure": "FIXABLE",
    "internal_whitespace": "FIXABLE",
    "non_ascii_characters": "FIXABLE",
    "html_tags_in_text": "FIXABLE",
    "boolean_inconsistency": "FIXABLE",
    "all_caps_values": "FIXABLE",
    "duplicate_insensitive_values": "FIXABLE",
    "id_type_drift_across_datasets": "FIXABLE",
    # complex / requires domain decision
    "invalid_email": "NOT_FIXABLE",
    "invalid_phone": "COMPLEX",
    "mixed_phone_formats": "COMPLEX",
    "invalid_numeric": "COMPLEX",
    "mixed_types": "COMPLEX",
    "negative_values": "COMPLEX",
    "out_of_range": "COMPLEX",
    "numeric_outliers_iqr": "COMPLEX",
    "numeric_outliers_zscore": "COMPLEX",
    "dominant_value_skew": "COMPLEX",
    "name_format_inconsistency": "COMPLEX",
    "systematic_placeholder": "COMPLEX",
    "sentinel_numeric_value": "COMPLEX",
    "punctuation_only_value": "COMPLEX",
    "round_number_anomaly": "COMPLEX",
    "date_clumping_jan1": "COMPLEX",
    "date_clumping_month_end": "COMPLEX",
    "invalid_url": "COMPLEX",
    "leading_zeros_on_numeric_id": "COMPLEX",
    "string_length_outlier": "COMPLEX",
    "numeric_precision_anomaly": "COMPLEX",
    "impossible_date": "COMPLEX",
    "weekend_date_anomaly": "COMPLEX",
    "mixed_date_formats": "COMPLEX",
    "implausible_age": "COMPLEX",
    "implausible_percentage": "COMPLEX",
    "string_with_only_digits_in_text_column": "COMPLEX",
    "repeated_token_in_string": "COMPLEX",
    "near_duplicate_rows": "COMPLEX",
    "low_variance_numeric": "COMPLEX",
    "timezone_inconsistency": "COMPLEX",
    # cannot be auto-repaired without authoritative source
    "duplicate_primary_key": "NOT_FIXABLE",
    "duplicate_rows": "COMPLEX",
    "orphan_foreign_key_rows": "NOT_FIXABLE",
    "orphan_foreign_key": "NOT_FIXABLE",
    "invalid_uuid": "NOT_FIXABLE",
    "impossible_date": "NOT_FIXABLE",
    "high_null_ratio_in_key_column": "NOT_FIXABLE",
    "duplicate_representation_candidate": "COMPLEX",
}


def enrich_issue_with_fixability(issue: Dict[str, Any]) -> None:
    if issue.get("fixability"):
        return
    issue["fixability"] = FIXABILITY_BY_ISSUE_TYPE.get(issue.get("type") or "", "COMPLEX")


def dq_issue(
    sev: str,
    typ: str,
    msg: str,
    *,
    column: Optional[str] = None,
    count: Optional[int] = None,
    rows: Optional[List[int]] = None,
    sample: Optional[List[Any]] = None
) -> Dict[str, Any]:
    """
    Create a normalized DQ issue record.
    - severity: "low" | "medium" | "high"
    - row_indexes: list of 0-based indexes (capped to 50)
    - sample_values: capped to 10
    """
    return {
        "severity": sev,
        "type": typ,
        "column": column,
        "count": count,
        "row_indexes": rows[:50] if rows else [],
        "sample_values": sample[:10] if sample else [],
        "message": msg,
        "fixability": FIXABILITY_BY_ISSUE_TYPE.get(typ, "COMPLEX"),
    }


def analyze_column(
    series: pd.Series,
    col: str,
    semantic: str,
    thresholds: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Per-column data quality checks (uses thresholds when provided).
    """
    thresholds = thresholds or {}
    sev = thresholds.get("severity", {})
    null_pct_high = _get_threshold(thresholds, "severity", "null_pct_high", default=0.25)
    null_pct_medium = _get_threshold(thresholds, "severity", "null_pct_medium", default=0.10)
    invalid_numeric_pct_high = _get_threshold(thresholds, "severity", "invalid_numeric_pct_high", default=0.10)
    invalid_date_pct_high = _get_threshold(thresholds, "severity", "invalid_date_pct_high", default=0.20)
    mixed_low = _get_threshold(thresholds, "mixed_types", "parse_rate_low", default=0.20)
    mixed_high = _get_threshold(thresholds, "mixed_types", "parse_rate_high", default=0.80)

    # Load custom placeholders or use global default set
    placeholders_list = thresholds.get("placeholders")
    if placeholders_list is not None:
        local_placeholders = set(str(p).lower() for p in placeholders_list)
    else:
        local_placeholders = PLACEHOLDERS
        
    # Load custom sentinels or use global default set
    sentinels_list = thresholds.get("sentinels")
    if sentinels_list is not None:
        local_sentinels = set(float(s) for s in sentinels_list if s is not None)
    else:
        local_sentinels = SENTINEL_NUMBERS

    issues: List[Dict[str, Any]] = []
    n = len(series)
    
    # If the dataset is huge, sample early for memory-intensive operations
    if n > HEAVY_OPERATION_THRESHOLD:
        s_working = series.sample(DEFAULT_SAMPLE_SIZE, random_state=42)
        working_n = len(s_working)
    else:
        s_working = series
        working_n = n

    s = s_working
    s_stripped = s_working.map(_strip)
    is_phone_col = isinstance(col, str) and any(p in col.lower() for p in ["phone", "mobile", "contact"])

    # null/placeholder
    null_like_mask = s_stripped.isna() | s_stripped.astype(object).map(
        lambda v: isinstance(v, str) and v.lower() in local_placeholders
    )
    null_cnt = int(null_like_mask.sum())
    if null_cnt > 0:
        ratio = null_cnt / max(working_n, 1)
        sev_str = "high" if ratio > null_pct_high else ("medium" if ratio > null_pct_medium else "low")
        rows = s.index[null_like_mask].tolist()
        issues.append(dq_issue(sev_str, "nulls", f"{null_cnt} null/placeholder", column=col,
                               count=null_cnt, rows=rows, sample=list(s[null_like_mask].head(5))))

    # whitespace
    ws_mask = s.astype(object).map(lambda v: isinstance(v, str) and v != v.strip())
    ws_cnt = int(ws_mask.sum())
    if ws_cnt > 0:
        rows = s.index[ws_mask].tolist()
        issues.append(dq_issue("low", "whitespace", f"{ws_cnt} leading/trailing spaces",
                               column=col, count=ws_cnt, rows=rows, sample=list(s[ws_mask].head(5))))

    # mixed scalar types (common in JSON IDs: alternating "0", 1, "2"...)
    try:
        if _is_text_dtype(s.dtype) and n > 0:
            td = scalar_type_distribution(s)
            pct = (td.get("pct") or {})
            has_str = float(pct.get("str", 0.0)) >= 0.05
            has_num = (float(pct.get("int", 0.0)) + float(pct.get("float", 0.0))) >= 0.05
            if has_str and has_num:
                sev_str = "medium" if (semantic == "numeric_id" or (isinstance(col, str) and col.lower().endswith("id"))) else "low"
                issues.append(dq_issue(
                    sev_str,
                    "mixed_scalar_types",
                    f"Mixed scalar types (str~{round(100*pct.get('str',0.0),1)}%, num~{round(100*(pct.get('int',0.0)+pct.get('float',0.0)),1)}%)",
                    column=col,
                    sample=[td.get("counts", {})],
                ))
    except Exception:
        pass

    # email
    if semantic == "email":
        bad_email_mask = s_stripped.astype(object).map(
            lambda v: isinstance(v, str) and not EMAIL_RE.match(v)
        ) & (~null_like_mask)
        bad_cnt = int(bad_email_mask.sum())
        if bad_cnt > 0:
            rows = s.index[bad_email_mask].tolist()
            issues.append(dq_issue("medium", "invalid_email", f"{bad_cnt} invalid email(s)",
                                   column=col, count=bad_cnt, rows=rows, sample=list(s[bad_email_mask].head(5))))

    # phone
    if semantic == "phone": # Now triggers on name OR detected type
        # 1. Validity check using phonenumbers
        bad_phone_mask = s_stripped.astype(object).map(
            lambda v: isinstance(v, str) and not _validate_phone_phonenumbers(v)
        ) & (~null_like_mask)
        bad_cnt = int(bad_phone_mask.sum())
        if bad_cnt > 0:
            rows = s.index[bad_phone_mask].tolist()
            issues.append(dq_issue("medium", "invalid_phone",
                                   f"{bad_cnt} invalid phone number(s) (failed libphonenumber validation)",
                                   column=col, count=bad_cnt, rows=rows,
                                   sample=list(s[bad_phone_mask].head(5))))

        # 2. Mixed format detection
        fmt_buckets = _detect_phone_formats(s[~null_like_mask])
        nonzero_fmts = {k: v for k, v in fmt_buckets.items() if v > 0 and k not in ("empty", "invalid")}
        if len(nonzero_fmts) >= 2:
            issues.append(dq_issue("medium", "mixed_phone_formats",
                                   f"Multiple phone formats: {nonzero_fmts}. Standardize to E.164 in ETL.",
                                   column=col, count=sum(nonzero_fmts.values()),
                                   sample=[nonzero_fmts]))

    # date
    if semantic == "date":
        # 1. Broad parse via dateutil
        res = _detect_date_formats(s_stripped)
        bad_cnt = res["unparsed_count"]
        
        # Locate row indexes of unparsed dates
        from dateutil import parser as du_parser
        bad_rows = []
        for idx, v in s_stripped.items():
            if null_like_mask.loc[idx]:
                continue
            if pd.isna(v) or not isinstance(v, str) or not v.strip():
                continue
            try:
                du_parser.parse(v.strip(), fuzzy=False)
            except Exception:
                bad_rows.append(idx)
                
        if bad_cnt > 0:
            sev_str = "medium" if bad_cnt / max(n, 1) <= invalid_date_pct_high else "high"
            issues.append(dq_issue(sev_str, "invalid_date_format",
                                   f"{bad_cnt} bad date(s) (failed dateutil parsing)",
                                   column=col, count=bad_cnt, rows=bad_rows[:50],
                                   sample=list(s.loc[bad_rows[:5]])))

        # 2. Date format inconsistency
        counts = res["counts"]
        nonzero_fmts = {k: v for k, v in counts.items() if v > 0 and k != "other/timestamp"}
        if len(nonzero_fmts) >= 2:
            issues.append(dq_issue("medium", "date_format_inconsistency",
                                   f"Mixed date formats in same column: {nonzero_fmts}. Standardize to ISO-8601.",
                                   column=col, count=sum(nonzero_fmts.values()),
                                   sample=[nonzero_fmts]))

    if (not is_phone_col) and (semantic not in ("date", "email")) and (
        (semantic in ("numeric_id",)) or
        (not _is_text_dtype(s.dtype)) or
        (_is_text_dtype(s.dtype) and (
            (1.0 - pd.to_numeric(s_stripped, errors="coerce").isna().mean()) > 0.2
        ))
    ):
        num = pd.to_numeric(s_stripped, errors="coerce")
        invalid_mask = num.isna() & (~null_like_mask)
        invalid_cnt = int(invalid_mask.sum())
        if invalid_cnt > 0:
            rows = s.index[invalid_mask].tolist()
            sev_str = "medium" if invalid_cnt / max(n, 1) <= invalid_numeric_pct_high else "high"
            issues.append(dq_issue(sev_str, "invalid_numeric",
                                   f"{invalid_cnt} non-numeric value(s)",
                                   column=col, count=invalid_cnt, rows=rows, sample=list(s[invalid_mask].head(5))))

            # Systematic placeholder detection among invalid tokens (e.g., "50k", "twenty")
            try:
                ph = (thresholds or {}).get("systematic_placeholders") or {}
                min_count = int(ph.get("min_count", 5))
                min_pct = float(ph.get("min_pct", 0.02))
                top_k = int(ph.get("top_k", 5))
                inval = s_stripped[invalid_mask].astype(str)
                vc = inval.value_counts()
                if len(vc) > 0:
                    top = vc.head(top_k)
                    top_items = [{"value": k, "count": int(v), "pct": float(v) / max(1, n)} for k, v in top.items()]
                    biggest = int(top.iloc[0])
                    if biggest >= min_count and (biggest / max(1, n)) >= min_pct:
                        issues.append(dq_issue(
                            "medium",
                            "systematic_placeholder",
                            f"Repeated invalid token(s) detected (top='{str(top.index[0])}' appears {biggest} times)",
                            column=col,
                            count=int(vc.sum()),
                            sample=top_items,
                        ))
            except Exception:
                pass

        neg_mask = num < 0
        neg_cnt = int(neg_mask.sum())
        if neg_cnt > 0:
            rows = s.index[neg_mask].tolist()
            issues.append(dq_issue("high", "negative_values",
                                   f"{neg_cnt} negative value(s)", column=col,
                                   count=neg_cnt, rows=rows, sample=list(s[neg_mask].head(5))))

        if semantic == "numeric_id":
            zero_mask = num == 0
            zero_cnt = int(zero_mask.sum())
            if zero_cnt > 0:
                rows = s.index[zero_mask].tolist()
                issues.append(dq_issue("medium", "suspicious_zero",
                                       f"{zero_cnt} zero(s) in ID-like column", column=col,
                                       count=zero_cnt, rows=rows, sample=list(s[zero_mask].head(5))))

        parse_rate = 1.0 - float(num.isna().mean())
        if mixed_low < parse_rate < mixed_high:
            # "mixed_types" means a material number of values do not conform to the dominant numeric parse.
            # Use the same invalid_mask used for invalid_numeric (excludes null-like placeholders).
            mixed_cnt = int((num.isna() & (~null_like_mask)).sum())
            issues.append(dq_issue("medium", "mixed_types",
                                   f"Mixed numeric/text (parse={round(parse_rate*100,1)}%)",
                                   column=col, count=mixed_cnt,
                                   rows=s.index[num.isna() & (~null_like_mask)].tolist()[:50],
                                   sample=list(s[num.isna() & (~null_like_mask)].head(5))))

        # Domain/range rules (config-driven; applied to parsed numeric values)
        try:
            dr = (thresholds or {}).get("domain_rules") or {}
            rules = dr.get("rules") or []
            if isinstance(rules, list) and isinstance(col, str):
                for r in rules:
                    if not isinstance(r, dict):
                        continue
                    rcol = str(r.get("column") or "").strip().lower()
                    if not rcol or rcol != col.lower():
                        continue
                    rmin = r.get("min", None)
                    rmax = r.get("max", None)
                    if rmin is None and rmax is None:
                        continue
                    ok = num.notna() & (~null_like_mask)
                    out_mask = ok.copy()
                    if rmin is not None:
                        out_mask = out_mask & (num < float(rmin))
                    if rmax is not None:
                        out_mask = out_mask & (num > float(rmax))
                    oc = int(out_mask.sum())
                    if oc > 0:
                        rows = s.index[out_mask].tolist()
                        issues.append(dq_issue(
                            str(r.get("severity") or "medium").lower(),
                            "out_of_range",
                            f"{oc} value(s) outside expected range",
                            column=col,
                            count=oc,
                            rows=rows[:50],
                            sample=list(s[out_mask].head(5)),
                        ))
        except Exception:
            pass

    # structural leftovers
    struct_mask = s.astype(object).map(lambda v: isinstance(v, (list, dict)))
    struct_cnt = int(struct_mask.sum())
    if struct_cnt > 0:
        rows = s.index[struct_mask].tolist()
        issues.append(dq_issue("medium", "nested_structure",
                               f"{struct_cnt} nested list/dict values", column=col,
                               count=struct_cnt, rows=rows, sample=list(s[struct_mask].head(5))))

    # ----------------------------------------------------------------
    # STRING-SPECIFIC EXTENDED CHECKS
    # ----------------------------------------------------------------
    str_mask = s.astype(object).map(lambda v: isinstance(v, str))
    str_series = s[str_mask].astype(str)
    stripped_str = str_series.str.strip()
    non_empty_str = stripped_str[stripped_str != ""]

    # Empty string values (distinct from null)
    try:
        empty_str_mask = str_series.str.strip() == ""
        esc = int(empty_str_mask.sum())
        if esc > 0:
            rows = empty_str_mask[empty_str_mask].index.tolist()
            issues.append(dq_issue("low", "empty_string_values",
                                   f"{esc} empty string value(s) (use NULL instead)",
                                   column=col, count=esc, rows=rows))
    except Exception:
        pass

    # Internal (multi) whitespace
    try:
        if len(non_empty_str) > 0:
            mws_mask = non_empty_str.str.contains(r"  +", regex=True, na=False)
            mws_cnt = int(mws_mask.sum())
            if mws_cnt > 0:
                rows = non_empty_str.index[mws_mask].tolist()
                issues.append(dq_issue("low", "internal_whitespace",
                                       f"{mws_cnt} value(s) with multiple consecutive spaces",
                                       column=col, count=mws_cnt, rows=rows,
                                       sample=list(non_empty_str[mws_mask].head(5))))
    except Exception:
        pass

    # Non-ASCII characters (only flag if column name suggests ASCII content)
    try:
        col_lower = str(col).lower()
        ascii_hinted = any(k in col_lower for k in (
            "name", "code", "id", "status", "type", "category", "label", "tag", "flag", "key"
        ))
        if ascii_hinted and len(non_empty_str) > 0:
            non_ascii_mask = non_empty_str.str.contains(r"[^\x00-\x7F]", regex=True, na=False)
            na_cnt = int(non_ascii_mask.sum())
            if na_cnt > 0:
                rows = non_empty_str.index[non_ascii_mask].tolist()
                issues.append(dq_issue("low", "non_ascii_characters",
                                       f"{na_cnt} value(s) containing non-ASCII characters",
                                       column=col, count=na_cnt, rows=rows,
                                       sample=list(non_empty_str[non_ascii_mask].head(5))))
    except Exception:
        pass

    # HTML / XML tags in text
    try:
        if len(non_empty_str) > 0:
            html_mask = non_empty_str.str.contains(r"<[a-zA-Z][^>]*>|</[a-zA-Z]+>", regex=True, na=False)
            html_cnt = int(html_mask.sum())
            if html_cnt > 0:
                rows = non_empty_str.index[html_mask].tolist()
                issues.append(dq_issue("medium", "html_tags_in_text",
                                       f"{html_cnt} value(s) containing HTML/XML tags",
                                       column=col, count=html_cnt, rows=rows,
                                       sample=list(non_empty_str[html_mask].head(5))))
    except Exception:
        pass

    # Punctuation-only strings
    try:
        if len(non_empty_str) > 0:
            punc_mask = non_empty_str.str.match(r"^[\W_]+$", na=False)
            punc_cnt = int(punc_mask.sum())
            if punc_cnt > 0:
                rows = non_empty_str.index[punc_mask].tolist()
                issues.append(dq_issue("low", "punctuation_only_value",
                                       f"{punc_cnt} value(s) consisting only of punctuation/symbols",
                                       column=col, count=punc_cnt, rows=rows,
                                       sample=list(non_empty_str[punc_mask].head(5))))
    except Exception:
        pass

    # URL column: invalid URL format
    try:
        col_lower = str(col).lower()
        is_url_col = any(k in col_lower for k in ("url", "link", "href", "uri", "website", "webpage"))
        if is_url_col and len(non_empty_str) > 0:
            url_like = non_empty_str.str.contains(r"^(https?://|ftp://|www\.)", regex=True, na=False)
            url_valid = non_empty_str.str.match(
                r"^(https?://|ftp://|www\.)[^\s/$.?#][^\s]*$", na=False
            )
            bad_url_mask = url_like & ~url_valid
            bad_cnt = int(bad_url_mask.sum())
            if bad_cnt > 0:
                rows = non_empty_str.index[bad_url_mask].tolist()
                issues.append(dq_issue("medium", "invalid_url",
                                       f"{bad_cnt} structurally invalid URL(s)",
                                       column=col, count=bad_cnt, rows=rows,
                                       sample=list(non_empty_str[bad_url_mask].head(5))))
    except Exception:
        pass

    # UUID column: invalid UUID format
    try:
        col_lower = str(col).lower()
        is_uuid_col = any(k in col_lower for k in ("uuid", "guid", "uid"))
        if is_uuid_col and len(non_empty_str) > 0:
            uuid_valid = non_empty_str.str.match(
                r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
                na=False,
            )
            invalid_uuid_cnt = int((~uuid_valid).sum())
            if invalid_uuid_cnt > 0:
                rows = non_empty_str.index[~uuid_valid].tolist()
                issues.append(dq_issue("high", "invalid_uuid",
                                       f"{invalid_uuid_cnt} value(s) do not match UUID format",
                                       column=col, count=invalid_uuid_cnt, rows=rows,
                                       sample=list(non_empty_str[~uuid_valid].head(5))))
    except Exception:
        pass

    # Leading zeros on numeric-ID-like columns
    try:
        col_lower = str(col).lower()
        if "id" in col_lower and len(non_empty_str) > 0:
            lz_mask = non_empty_str.str.match(r"^0[0-9]+$", na=False)
            lz_cnt = int(lz_mask.sum())
            if lz_cnt > 0:
                rows = non_empty_str.index[lz_mask].tolist()
                issues.append(dq_issue("medium", "leading_zeros_on_numeric_id",
                                       f"{lz_cnt} ID value(s) with leading zeros (risk of data loss on int cast)",
                                       column=col, count=lz_cnt, rows=rows,
                                       sample=list(non_empty_str[lz_mask].head(5))))
    except Exception:
        pass

    # Boolean inconsistency: column mixes multiple boolean-like vocabularies
    try:
        if len(non_empty_str) > 0 and safe_nunique(s) <= 10:
            vals_lower = set(non_empty_str.str.strip().str.lower().dropna().unique())
            matched_groups = [
                name for name, vocab in BOOL_VARIANTS.items()
                if len(vals_lower & vocab) >= 2 or (len(vals_lower & vocab) >= 1 and len(vals_lower) > len(vocab))
            ]
            if len(matched_groups) >= 2:
                issues.append(dq_issue(
                    "medium", "boolean_inconsistency",
                    f"Column mixes multiple boolean vocabularies: {matched_groups} (values: {sorted(list(vals_lower))[:10]})",
                    column=col, count=len(non_empty_str),
                    sample=sorted(list(vals_lower))[:10],
                ))
    except Exception:
        pass

    # All-caps values mixed with non-caps (inconsistent casing in categorical)
    try:
        col_lower_name = str(col).lower()
        is_text_like = any(k in col_lower_name for k in ("name", "title", "label", "desc", "comment", "remark", "status", "type"))
        if is_text_like and len(non_empty_str) > 5:
            all_caps = non_empty_str.str.isupper()
            all_caps_cnt = int(all_caps.sum())
            not_all_caps_cnt = int((~all_caps).sum())
            if all_caps_cnt > 0 and not_all_caps_cnt > 0 and all_caps_cnt < len(non_empty_str) * 0.9:
                issues.append(dq_issue(
                    "low", "all_caps_values",
                    f"{all_caps_cnt} ALL-CAPS value(s) mixed with {not_all_caps_cnt} mixed/lower-case",
                    column=col, count=all_caps_cnt,
                    rows=non_empty_str.index[all_caps].tolist()[:50],
                    sample=list(non_empty_str[all_caps].head(5)),
                ))
    except Exception:
        pass

    # String-only-digits in a text column (possible schema mismatch)
    try:
        col_lower_name = str(col).lower()
        is_text_col = any(k in col_lower_name for k in ("name", "desc", "comment", "note", "title", "remark", "address", "city"))
        if is_text_col and len(non_empty_str) > 0:
            digits_only_mask = non_empty_str.str.match(r"^\d+$", na=False)
            d_cnt = int(digits_only_mask.sum())
            if d_cnt > 0 and d_cnt / max(len(non_empty_str), 1) > 0.05:
                rows = non_empty_str.index[digits_only_mask].tolist()
                issues.append(dq_issue("low", "string_with_only_digits_in_text_column",
                                       f"{d_cnt} value(s) in text column contain only digits",
                                       column=col, count=d_cnt, rows=rows,
                                       sample=list(non_empty_str[digits_only_mask].head(5))))
    except Exception:
        pass

    # Repeated tokens in string values (e.g. "test test", "abc abc")
    try:
        if len(non_empty_str) > 0:
            def _has_repeated_token(v: str) -> bool:
                parts = v.strip().split()
                return len(parts) >= 2 and len(set(p.lower() for p in parts)) < len(parts) * 0.6
            rpt_mask = non_empty_str.map(_has_repeated_token)
            rpt_cnt = int(rpt_mask.sum())
            if rpt_cnt > 0 and rpt_cnt / max(len(non_empty_str), 1) > 0.03:
                rows = non_empty_str.index[rpt_mask].tolist()
                issues.append(dq_issue("low", "repeated_token_in_string",
                                       f"{rpt_cnt} value(s) with repeated word tokens (possible data entry error)",
                                       column=col, count=rpt_cnt, rows=rows,
                                       sample=list(non_empty_str[rpt_mask].head(5))))
    except Exception:
        pass

    # High null ratio in key/ID columns
    try:
        col_lower_name = str(col).lower()
        is_key_col = any(k in col_lower_name for k in ("_id", "id", "key", "code", "ref", "pk"))
        null_ratio = float(s.isna().mean())
        if is_key_col and null_ratio > 0.05:
            issues.append(dq_issue(
                "high" if null_ratio > 0.20 else "medium",
                "high_null_ratio_in_key_column",
                f"Key/ID column has {round(null_ratio*100, 1)}% null values (unreliable for joins)",
                column=col, count=int(s.isna().sum()),
                rows=s.index[s.isna()].tolist()[:50],
            ))
    except Exception:
        pass

    # Implausible age values
    try:
        col_lower_name = str(col).lower()
        if "age" in col_lower_name:
            num_age = pd.to_numeric(s_stripped, errors="coerce")
            age_bad = num_age.notna() & ((num_age < 0) | (num_age > 150))
            age_cnt = int(age_bad.sum())
            if age_cnt > 0:
                rows = s.index[age_bad].tolist()
                issues.append(dq_issue("high", "implausible_age",
                                       f"{age_cnt} age value(s) outside range 0-150",
                                       column=col, count=age_cnt, rows=rows,
                                       sample=list(s[age_bad].head(5))))
    except Exception:
        pass

    # Implausible percentage values
    try:
        col_lower_name = str(col).lower()
        if any(k in col_lower_name for k in ("percent", "pct", "rate", "ratio", "share")):
            num_pct = pd.to_numeric(s_stripped, errors="coerce")
            pct_bad = num_pct.notna() & ((num_pct < 0) | (num_pct > 100))
            pct_cnt = int(pct_bad.sum())
            if pct_cnt > 0:
                rows = s.index[pct_bad].tolist()
                issues.append(dq_issue("medium", "implausible_percentage",
                                       f"{pct_cnt} value(s) outside expected 0-100% range",
                                       column=col, count=pct_cnt, rows=rows,
                                       sample=list(s[pct_bad].head(5))))
    except Exception:
        pass

    # Sentinel numeric values (-999, 9999999, etc.)
    try:
        if (not _is_text_dtype(s.dtype)) or (semantic in ("numeric_id",)):
            num_chk = pd.to_numeric(s_stripped, errors="coerce").dropna()
            if len(num_chk) > 0:
                sentinel_mask = num_chk.apply(lambda v: float(v) in local_sentinels)
                sent_cnt = int(sentinel_mask.sum())
                if sent_cnt > 0 and sent_cnt / max(len(num_chk), 1) > 0.01:
                    rows = num_chk.index[sentinel_mask].tolist()
                    issues.append(dq_issue("medium", "sentinel_numeric_value",
                                           f"{sent_cnt} sentinel/magic number(s) detected (e.g. -999, 9999999)",
                                           column=col, count=sent_cnt, rows=rows,
                                           sample=list(num_chk[sentinel_mask].head(5))))
    except Exception:
        pass

    # Mixed date formats (e.g. YYYY-MM-DD mixed with DD/MM/YYYY)
    try:
        if semantic == "date" and len(non_empty_str) >= 10:
            fmt_iso = non_empty_str.str.match(r"^\d{4}-\d{2}-\d{2}", na=False)
            fmt_us = non_empty_str.str.match(r"^\d{1,2}/\d{1,2}/\d{2,4}", na=False)
            fmt_eu = non_empty_str.str.match(r"^\d{1,2}\.\d{1,2}\.\d{2,4}", na=False)
            active_fmts = [(c, v) for c, v in [("ISO(YYYY-MM-DD)", int(fmt_iso.sum())),
                                                ("US(MM/DD/YYYY)", int(fmt_us.sum())),
                                                ("EU(DD.MM.YYYY)", int(fmt_eu.sum()))]
                           if v > 0]
            if len(active_fmts) >= 2:
                detail = ", ".join(f"{n}={c}" for n, c in active_fmts)
                issues.append(dq_issue("medium", "mixed_date_formats",
                                       f"Multiple date formats detected: {detail}",
                                       column=col, count=sum(c for _, c in active_fmts),
                                       sample=[n for n, _ in active_fmts]))
    except Exception:
        pass

    return issues




# Known start/end column pairs for cross-column date logic (lowercase keys).
_DATE_RANGE_PAIRS = (
    ("start_date", "end_date"),
    ("start_dt", "end_dt"),
    ("valid_from", "valid_to"),
    ("from_date", "to_date"),
    ("begin_date", "end_date"),
    ("effective_date", "expiry_date"),
    ("effective_from", "effective_to"),
    ("period_start", "period_end"),
    ("open_date", "close_date"),
    ("order_date", "ship_date"),
    ("order_date", "delivery_date"),
    ("ship_date", "delivery_date"),
    ("created_at", "updated_at"),
    ("birth_date", "death_date"),
)


def run_extended_dq_checks(
    df: pd.DataFrame,
    profile: Dict[str, Any],
    thresholds: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Additional DQ: empty dataset, duplicate headers, case collisions, wide table,
    constant/dominant columns, IQR outliers, skew, string length & control chars,
    future/ancient dates, cross-column date ordering, high cardinality, binary hint.
    Respects thresholds['extended_checks'] (disabled, numeric limits).
    """
    thresholds = thresholds or {}
    ext = thresholds.get("extended_checks") or {}
    if ext.get("disabled"):
        return []

    issues: List[Dict[str, Any]] = []
    n = len(df)
    dominate_pct = float(ext.get("dominant_value_pct", 0.92))
    outlier_frac = float(ext.get("outlier_row_fraction_warn", 0.003))
    max_heavy = int(ext.get("max_rows_heavy", 10_000_000))
    extreme_len = int(ext.get("extreme_string_len", 4000))
    wide_cols = int(ext.get("wide_table_columns", 200))
    skew_th = float(ext.get("skew_threshold", 2.0))

    if n == 0:
        issues.append(dq_issue("high", "empty_dataset", "Dataset has zero rows"))
        return issues

    # Duplicate pandas column labels
    if df.columns.duplicated().any():
        dups = list(dict.fromkeys(df.columns[df.columns.duplicated()].tolist()))
        issues.append(dq_issue(
            "high", "duplicate_column_names",
            f"Duplicate column label(s): {dups[:8]}{'…' if len(dups) > 8 else ''}",
        ))

    lower_map: Dict[str, List[str]] = {}
    for c in df.columns:
        k = str(c).lower()
        lower_map.setdefault(k, []).append(str(c))
    for _k, cols in lower_map.items():
        if len(cols) > 1:
            issues.append(dq_issue(
                "medium", "case_insensitive_column_collision",
                f"Columns differ only by case: {cols}",
            ))

    if len(df.columns) > wide_cols:
        issues.append(dq_issue(
            "low", "very_wide_table",
            f"{len(df.columns)} columns; consider narrowing or documenting schema",
        ))

    cols_meta = profile.get("columns", {})

    def analyze_col_dq(col: str) -> List[Dict[str, Any]]:
        col_issues = []
        if col not in df.columns:
            return []
        s = df[col]
        col_lower = str(col).lower()
        meta = cols_meta.get(col, {})
        meta_semantic = (meta.get("semantic_type") or "unknown").lower()
        semantic = meta_semantic if meta_semantic != "unknown" else detect_semantic_type(df[col], col_name=col)
        null_pct = float(meta.get("null_percentage") or 0)
        uq = int(meta.get("unique_count") or 0)
        non_null = int(s.notna().sum())

        if non_null == 0:
            return []

        # Whitespace in column names
        cn = str(col)
        if " " in cn.strip() or cn != cn.strip():
            col_issues.append(dq_issue(
                "low", "column_name_whitespace",
                f"Column name has leading/trailing/embedded spaces: {cn!r}",
                column=cn,
            ))

        if uq == 1:
            col_issues.append(dq_issue(
                "low", "constant_column",
                "Single distinct non-null value",
                column=col,
            ))
        elif uq > 1 and non_null > 0:
            sub = s.dropna()
            if len(sub) > max_heavy:
                sub = sub.sample(max_heavy, random_state=42)
            try:
                vc = sub.value_counts()
                if len(vc) > 0:
                    top_share = float(vc.iloc[0]) / float(len(sub))
                    if top_share >= dominate_pct:
                        col_issues.append(dq_issue(
                            "medium", "dominant_value_skew",
                            f"~{round(top_share*100,1)}% rows share one value (top category)",
                            column=col,
                            count=int(top_share * non_null),
                            sample=[vc.index[0]],
                        ))
            except Exception:
                pass

        if n > 20 and uq >= max(2, int(0.98 * non_null)) and semantic not in ("numeric_id", "email"):
            if uq > 50:
                col_issues.append(dq_issue(
                    "low", "very_high_cardinality",
                    f"{uq} distinct values (~{round(100*uq/max(non_null,1),1)}% of non-null rows)",
                    column=col,
                    count=int(uq),
                ))

        if uq == 2 and non_null > 10:
            col_issues.append(dq_issue(
                "low", "binary_like_column",
                "Only two distinct values; suitable for boolean encoding",
                column=col,
            ))

        # Numeric: IQR outliers + skew + integer-stored-as-float
        # User wants full report, no sampling here unless requested via max_rows
        s_eff = s
        
        s_str = s_eff.astype(str).str.strip() if _is_text_dtype(s_eff.dtype) else s_eff
        num = pd.to_numeric(s_str, errors="coerce")
        parse_ok = int(num.notna().sum())
        if parse_ok >= max(10, int(0.85 * non_null)) and _is_actual_numeric_column(col, meta_semantic):
            v = num.dropna()
            if len(v) > max_heavy:
                v = v.sample(max_heavy, random_state=42)
            q1, q3 = v.quantile(0.25), v.quantile(0.75)
            iqr = float(q3 - q1)
            if iqr > 0:
                lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
                out_mask = num.notna() & ((num < lo) | (num > hi))
                oc = int(out_mask.sum())
                if oc > 0 and (oc / max(n, 1)) >= outlier_frac:
                    col_issues.append(dq_issue(
                        "medium", "numeric_outliers_iqr",
                        f"{oc} row(s) outside 1.5*IQR [{lo:.6g}, {hi:.6g}]",
                        column=col, count=oc,
                        rows=df.index[out_mask].tolist()[:50],
                        sample=list(num[out_mask].head(5)),
                    ))
            if len(v) >= 8:
                try:
                    sk = float(v.skew())
                    if abs(sk) >= skew_th:
                        col_issues.append(dq_issue(
                            "low", "skewed_distribution",
                            f"Skewness ~ {round(sk, 2)} (heavy tail on one side)",
                            column=col,
                        ))
                except Exception:
                    pass
            if str(s.dtype).startswith("float") and non_null > 0:
                nn = num.dropna()
                if len(nn) > 0 and bool(np.allclose(nn.to_numpy(), np.round(nn.to_numpy()), rtol=0, atol=1e-9)):
                    col_issues.append(dq_issue(
                        "low", "integer_stored_as_float",
                        "All non-null values are whole numbers; consider Int64 dtype",
                        column=col,
                    ))

        # Dates: future / ancient / span
        parsed = pd.to_datetime(s_str, errors="coerce")
        date_ok = int(parsed.notna().sum())
        if (semantic == "date" or "datetime" in str(s.dtype).lower() or (any(h in col_lower for h in ["date", "time", "dt", "created", "updated"]) and "id" not in col_lower)) and date_ok >= max(5, int(0.45 * non_null)):
            valid = parsed.dropna()
            if len(valid) > 0:
                now = pd.Timestamp.now(tz=None).normalize()
                fut = (parsed > now) & parsed.notna()
                fc = int(fut.sum())
                if fc > 0:
                    col_issues.append(dq_issue(
                        "medium", "future_dates",
                        f"{fc} date(s) after today",
                        column=col, count=fc,
                        rows=df.index[fut].tolist()[:50],
                        sample=[v.isoformat() if hasattr(v, "isoformat") else v for v in parsed[fut].head(3).tolist()],
                    ))
                ancient = parsed.notna() & (parsed < pd.Timestamp("1900-01-01"))
                ac = int(ancient.sum())
                if ac > 0:
                    col_issues.append(dq_issue(
                        "low", "ancient_dates",
                        f"{ac} date(s) before 1900-01-01",
                        column=col, count=ac,
                        rows=df.index[ancient].tolist()[:30],
                    ))
        # String-specific checks: length, casing, control chars
        if _is_text_dtype(s.dtype) or semantic in ("email", "free_text", "categorical"):
            try:
                sub = s.dropna()
                if len(sub) > max_heavy:
                    sub = sub.sample(max_heavy, random_state=42)
                
                # Length extremes
                lens = sub.astype(str).str.len()
                if lens.max() >= extreme_len:
                    col_issues.append(dq_issue(
                        "medium", "extremely_long_strings",
                        f"Max length {int(lens.max())} chars (>={extreme_len})",
                        column=col,
                    ))
                
                # Case inconsistency
                if uq > 1 and uq <= max(250, int(0.6 * non_null)):
                    norm = sub.astype(str).str.strip().str.lower()
                    groups: Dict[str, set] = {}
                    for raw, key in zip(sub.tolist(), norm.tolist()):
                        if not isinstance(raw, str) or not key: continue
                        groups.setdefault(key, set()).add(raw.strip())
                    collisions = {k: sorted(list(v)) for k, v in groups.items() if len(v) >= 2}
                    if collisions:
                        top = sorted(collisions.items(), key=lambda kv: len(kv[1]), reverse=True)[:5]
                        col_issues.append(dq_issue(
                            "medium", "case_inconsistency",
                            f"Values differ only by casing (e.g., {top[0][1][0]} vs {top[0][1][1]})",
                            column=col, count=len(collisions),
                            sample=[{k: v} for k, v in top],
                        ))
                
                # Control chars
                ctrl_pat = r"[\x00-\x08\x0b\x0c\x0e-\x1f]"
                has_ctrl = sub.astype(str).str.contains(ctrl_pat, regex=True, na=False)
                if has_ctrl.any():
                    col_issues.append(dq_issue(
                        "medium", "control_characters_in_text",
                        f"Detected control characters in text values",
                        column=col, count=int(has_ctrl.sum()),
                    ))
            except Exception:
                pass
        # UUID uniqueness check
        if semantic == "uuid":
            dup_uuids = df[col].dropna()[df[col].dropna().duplicated()]
            if len(dup_uuids) > 0:
                col_issues.append(dq_issue("high", "duplicate_uuid",
                    f"{len(dup_uuids)} duplicate UUID(s) - UUIDs must be globally unique",
                    column=col, count=len(dup_uuids),
                    rows=df.index[df[col].duplicated(keep=False) & df[col].notna()].tolist()[:50],
                    sample=list(dup_uuids.head(5))))

        # Boolean-like: detect ambiguous mixed representation (True/1/yes all present)
        if semantic == "boolean_like":
            vals = df[col].dropna().astype(str).str.strip().str.lower().value_counts()
            bool_groups = {
                "true_variants": [v for v in vals.index if v in {"true","yes","y","1","t"}],
                "false_variants": [v for v in vals.index if v in {"false","no","n","0","f"}]
            }
            total_variants = len(bool_groups["true_variants"]) + len(bool_groups["false_variants"])
            if total_variants >= 3: # e.g. True, 1, yes all present
                col_issues.append(dq_issue("medium", "ambiguous_boolean",
                    f"Multiple representations of boolean: {dict(vals.head(6).to_dict())}",
                    column=col, count=int(non_null),
                    sample=list(vals.index[:6])))

        return col_issues

    # Parallelize column profiling
    with concurrent.futures.ThreadPoolExecutor() as executor:
        col_futures = [executor.submit(analyze_col_dq, col) for col in df.columns]
        for future in concurrent.futures.as_completed(col_futures):
            issues.extend(future.result())

    # Cross-column date range violations
    cmap = {str(c).lower(): c for c in df.columns}
    for a, b in _DATE_RANGE_PAIRS:
        ca, cb = cmap.get(a), cmap.get(b)
        if not ca or not cb:
            continue
        # Robust date parsing via dateutil.parser instead of strict pandas to_datetime
        from dateutil import parser as du_parser
        
        def _robust_date_parse(val: Any):
            if pd.isna(val) or not isinstance(val, str) or not val.strip():
                return None
            try:
                return du_parser.parse(val.strip(), fuzzy=False)
            except Exception:
                return None

        # Parse columns element-wise
        d1 = df[ca].map(_robust_date_parse)
        d2 = df[cb].map(_robust_date_parse)
        
        bad = d2.notna() & d1.notna() & (d2 < d1)
        bc = int(bad.sum())
        if bc > 0:
            issues.append(dq_issue("high", "date_range_violation",
                                   f"{bc} row(s): {cb!r} is before {ca!r} (detected via robust dateutil parser)",
                                   count=bc, rows=df.index[bad].tolist()[:50],
                                   sample=df.loc[bad, [ca, cb]].head(5).to_dict(orient="records")))

    # ----------------------------------------------------------------
    # DATASET-LEVEL EXTENDED CHECKS
    # ----------------------------------------------------------------

    for col in df.columns:
        try:
            s = df[col]
            non_null = int(s.notna().sum())
            if non_null < 10:
                continue
            s_str = s.astype(str).str.strip() if _is_text_dtype(s.dtype) else s
            num = pd.to_numeric(s_str, errors="coerce")
            parse_ok = int(num.notna().sum())
            meta = cols_meta.get(col, {})
            semantic = (meta.get("semantic_type") or "unknown").lower()

            # Z-Score outliers (> 4 std deviations)
            if parse_ok >= max(10, int(0.85 * non_null)) and _is_actual_numeric_column(col, semantic):
                v = num.dropna()
                if len(v) >= 10:
                    try:
                        mean_v = float(v.mean())
                        std_v = float(v.std())
                        if std_v > 0:
                            z_scores = (v - mean_v) / std_v
                            z_out_mask = num.notna() & ((num - mean_v).abs() / std_v > 4.0)
                            zoc = int(z_out_mask.sum())
                            if zoc > 0 and (zoc / max(n, 1)) >= outlier_frac:
                                issues.append(dq_issue(
                                    "medium", "numeric_outliers_zscore",
                                    f"{zoc} row(s) with |z-score| > 4 (extreme statistical outliers)",
                                    column=col, count=zoc,
                                    rows=df.index[z_out_mask].tolist()[:50],
                                    sample=list(num[z_out_mask].head(5)),
                                ))
                    except Exception:
                        pass

            # Round number anomaly: >30% of values are multiples of 1000 (or 100 for smaller values)
            if parse_ok >= max(10, int(0.85 * non_null)) and _is_actual_numeric_column(col, semantic):
                v = num.dropna()
                if len(v) >= 20:
                    try:
                        max_abs = float(v.abs().max())
                        divisor = 1000.0 if max_abs >= 10000 else 100.0 if max_abs >= 1000 else 10.0
                        round_mask = (v % divisor == 0) & (v != 0)
                        round_pct = float(round_mask.mean())
                        if round_pct > 0.50:
                            issues.append(dq_issue(
                                "low", "round_number_anomaly",
                                f"{round(round_pct*100, 1)}% of values are multiples of {int(divisor)} - may indicate estimates",
                                column=col, count=int(round_mask.sum()),
                                sample=list(v[round_mask].head(5)),
                            ))
                    except Exception:
                        pass

            # Low-variance numeric: coefficient of variation < 1%
            if parse_ok >= max(10, int(0.85 * non_null)):
                v = num.dropna()
                if len(v) >= 10:
                    try:
                        mean_v = float(v.mean())
                        std_v = float(v.std())
                        if mean_v != 0 and std_v / abs(mean_v) < 0.01:
                            issues.append(dq_issue(
                                "low", "low_variance_numeric",
                                f"Very low coefficient of variation ({round(100*std_v/abs(mean_v), 3)}%) - near-constant column",
                                column=col,
                            ))
                    except Exception:
                        pass

            # Numeric precision anomaly: mix of integers and high-precision floats
            if parse_ok >= max(10, int(0.85 * non_null)) and str(s.dtype).startswith("float"):
                v = num.dropna()
                if len(v) >= 10:
                    try:
                        is_int_like = np.isclose(v.to_numpy(), np.round(v.to_numpy()), rtol=0, atol=1e-9)
                        int_like_pct = float(is_int_like.mean())
                        if 0.1 < int_like_pct < 0.9:
                            issues.append(dq_issue(
                                "low", "numeric_precision_anomaly",
                                f"{round(int_like_pct*100, 1)}% of float values are whole numbers - mixed precision data",
                                column=col, count=int(is_int_like.sum()),
                            ))
                    except Exception:
                        pass

            # Date: Jan-1 clumping
            parsed_dates = pd.to_datetime(s_str, errors="coerce")
            date_ok_cnt = int(parsed_dates.notna().sum())
            if (semantic == "date" or "datetime" in str(s.dtype).lower()) and date_ok_cnt >= max(5, int(0.45 * non_null)):
                valid_dates = parsed_dates.dropna()
                try:
                    jan1_mask = parsed_dates.notna() & (parsed_dates.dt.month == 1) & (parsed_dates.dt.day == 1)
                    jan1_cnt = int(jan1_mask.sum())
                    jan1_pct = jan1_cnt / max(date_ok_cnt, 1)
                    if jan1_pct > 0.20 and jan1_cnt > 3:
                        issues.append(dq_issue(
                            "medium", "date_clumping_jan1",
                            f"{jan1_cnt} date(s) ({round(jan1_pct*100, 1)}%) fall on Jan 1 - possible default/dummy date",
                            column=col, count=jan1_cnt,
                            rows=df.index[jan1_mask].tolist()[:50],
                            sample=[v.isoformat() if hasattr(v, "isoformat") else v
                                    for v in parsed_dates[jan1_mask].head(3).tolist()],
                        ))
                except Exception:
                    pass

                # Month-end date clumping
                try:
                    import calendar
                    month_end_mask = parsed_dates.notna() & parsed_dates.apply(
                        lambda d: d is not pd.NaT and d.day == calendar.monthrange(d.year, d.month)[1]
                        if pd.notna(d) else False
                    )
                    me_cnt = int(month_end_mask.sum())
                    me_pct = me_cnt / max(date_ok_cnt, 1)
                    if me_pct > 0.30 and me_cnt > 5:
                        issues.append(dq_issue(
                            "low", "date_clumping_month_end",
                            f"{me_cnt} date(s) ({round(me_pct*100, 1)}%) fall on month-end - possible rolled-up dates",
                            column=col, count=me_cnt,
                            rows=df.index[month_end_mask].tolist()[:50],
                        ))
                except Exception:
                    pass

                # Weekend date anomaly (for business-context columns)
                try:
                    col_lower_name = str(col).lower()
                    is_biz_col = any(k in col_lower_name for k in (
                        "order", "invoice", "transaction", "payment", "shipment", "delivery", "process"
                    ))
                    if is_biz_col:
                        weekend_mask = parsed_dates.notna() & (parsed_dates.dt.dayofweek >= 5)
                        wkd_cnt = int(weekend_mask.sum())
                        wkd_pct = wkd_cnt / max(date_ok_cnt, 1)
                        if wkd_pct > 0.20 and wkd_cnt > 5:
                            issues.append(dq_issue(
                                "low", "weekend_date_anomaly",
                                f"{wkd_cnt} date(s) ({round(wkd_pct*100, 1)}%) fall on weekend for a business-context column",
                                column=col, count=wkd_cnt,
                                rows=df.index[weekend_mask].tolist()[:50],
                            ))
                except Exception:
                    pass

            # String length outliers (> mean + 4*std for string columns)
            if _is_text_dtype(s.dtype) or semantic in ("email", "free_text", "categorical"):
                try:
                    sub = s.dropna().astype(str)
                    if len(sub) >= 10:
                        lens = sub.str.len()
                        mean_l = float(lens.mean())
                        std_l = float(lens.std())
                        if std_l > 0:
                            long_mask = lens > mean_l + 4 * std_l
                            long_cnt = int(long_mask.sum())
                            if long_cnt > 0 and (long_cnt / max(n, 1)) >= 0.005:
                                orig_idx = sub.index[long_mask]
                                issues.append(dq_issue(
                                    "low", "string_length_outlier",
                                    f"{long_cnt} value(s) significantly longer than average "
                                    f"(mean={round(mean_l, 0)}, threshold>{round(mean_l+4*std_l, 0)} chars)",
                                    column=col, count=long_cnt,
                                    rows=orig_idx.tolist()[:50],
                                    sample=list(sub.loc[orig_idx].head(3)),
                                ))
                except Exception:
                    pass

            # Duplicate-insensitive values: values differing only by case/whitespace
            if _is_text_dtype(s.dtype) and non_null >= 5:
                try:
                    uq = int(meta.get("unique_count") or 0)
                    if 1 < uq <= max(500, int(0.8 * non_null)):
                        normalized = s.dropna().astype(str).str.strip().str.lower()
                        norm_uq = int(normalized.nunique())
                        raw_uq = int(s.dropna().astype(str).nunique())
                        if norm_uq < raw_uq:
                            diff = raw_uq - norm_uq
                            issues.append(dq_issue(
                                "medium", "duplicate_insensitive_values",
                                f"{diff} value group(s) differ only by case/whitespace "
                                f"({raw_uq} raw -> {norm_uq} after normalization)",
                                column=col, count=diff,
                            ))
                except Exception:
                    pass

        except Exception:
            pass

    # ------------------------------------------------------------
    # 5. Near-Duplicate Row Detection using rapidfuzz
    # ------------------------------------------------------------
    nd_cfg = (thresholds or {}).get("near_duplicate") or {}
    if nd_cfg.get("enabled", True):
        from rapidfuzz import fuzz
        
        # We construct a string representation for each row (excluding ID/Timestamp cols to be smart)
        text_cols = [c for c in df.columns if _is_text_dtype(df[c].dtype) and not c.lower().endswith("id")]
        if len(text_cols) >= 2:
            max_rows = int(nd_cfg.get("max_rows", 50000))
            sub_df = df[text_cols].dropna(how="all")
            if len(sub_df) > max_rows:
                sub_df = sub_df.sample(max_rows, random_state=42)
                
            row_strings = sub_df.apply(lambda row: " | ".join(str(val) for val in row), axis=1).tolist()
            row_indices = sub_df.index.tolist()
            
            threshold = float(nd_cfg.get("threshold", 0.92)) * 100
            near_dups = []
            
            # If the dataset is small, do full comparison
            if len(row_strings) <= 300:
                for i in range(len(row_strings)):
                    for j in range(i + 1, len(row_strings)):
                        ratio = fuzz.token_sort_ratio(row_strings[i], row_strings[j])
                        if ratio >= threshold:
                            near_dups.append((row_indices[i], row_indices[j], ratio / 100.0))
            else:
                # Group by blocking keys (first two chars of first two significant words)
                buckets = {}
                for idx, r_str in enumerate(row_strings):
                    words = [w for w in re.findall(r'\w+', r_str.lower()) if len(w) > 2]
                    keys = set()
                    if len(words) >= 1:
                        keys.add(words[0][:2])
                    if len(words) >= 2:
                        keys.add(words[1][:2])
                    if not keys:
                        keys.add(f"len_{len(r_str) // 10}")
                        
                    for key in keys:
                        buckets.setdefault(key, []).append(idx)
                
                # Pairwise comparison only within buckets
                compared_pairs = set()
                for key, idx_list in buckets.items():
                    if len(idx_list) < 2:
                        continue
                    bucket_limit = min(len(idx_list), 200)
                    for i in range(bucket_limit):
                        for j in range(i + 1, bucket_limit):
                            ii, jj = idx_list[i], idx_list[j]
                            pair = (min(ii, jj), max(ii, jj))
                            if pair in compared_pairs:
                                continue
                            compared_pairs.add(pair)
                            
                            ratio = fuzz.token_sort_ratio(row_strings[ii], row_strings[jj])
                            if ratio >= threshold:
                                near_dups.append((row_indices[ii], row_indices[jj], ratio / 100.0))
                        
            if len(near_dups) > 0:
                issues.append(dq_issue("medium", "near_duplicate_rows",
                                       f"Found {len(near_dups)} pair(s) of near-duplicate rows with string similarity >= {threshold/100:.2f}",
                                       column="[Row-level]",
                                       count=len(near_dups),
                                       sample=[{"row_index_a": int(a), "row_index_b": int(b), "similarity": float(s)}
                                               for a, b, s in near_dups[:10]]))

    # ------------------------------------------------------------
    # 6. Multivariate Outlier Detection using IsolationForest
    # ------------------------------------------------------------
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and not c.lower().endswith("id")]
    if len(num_cols) >= 2 and len(df) >= 10:
        outlier_cfg = (thresholds or {}).get("multivariate_outliers") or {}
        if outlier_cfg.get("enabled", True):
            clean_df = df[num_cols].dropna()
            if len(clean_df) >= 10:
                try:
                    from sklearn.ensemble import IsolationForest
                    contamination = float(outlier_cfg.get("contamination", 0.02))
                    
                    model = IsolationForest(contamination=contamination, random_state=42)
                    preds = model.fit_predict(clean_df)
                    
                    outlier_mask = preds == -1
                    outliers = clean_df[outlier_mask]
                    
                    if len(outliers) > 0:
                        issues.append(dq_issue("medium", "multivariate_outliers",
                                               f"Detected {len(outliers)} multivariate outlier(s) using IsolationForest (contamination={contamination})",
                                               column="[Row-level]",
                                               count=len(outliers),
                                               rows=outliers.index.tolist()[:50],
                                               sample=outliers.head(5).to_dict(orient="records")))
                except Exception:
                    pass

    # ------------------------------------------------------------
    # 7. Intra-Dataset Self-Referencing FK Check
    # ------------------------------------------------------------
    id_cols = [c for c in df.columns if c.lower().endswith("id")]
    if len(id_cols) >= 2:
        for col_pk in id_cols:
            if df[col_pk].dropna().is_unique and df[col_pk].notna().any():
                pk_lower = col_pk.lower()
                # Extract PK base name: e.g. "order" from "OrderID" or "order_id"
                pk_base = pk_lower[:-2].strip("_")
                for col_fk in id_cols:
                    if col_fk == col_pk:
                        continue
                    
                    fk_lower = col_fk.lower()
                    
                    # Smart self-referencing check:
                    # If PK is order_id and FK is customer_id, they are different entities and not hierarchical.
                    # We only check if fk contains pk_base (e.g., parent_order_id) or known hierarchy keywords.
                    is_hierarchical = any(w in fk_lower for w in ["parent", "manager", "prev", "next", "reports", "sub", "master", "hierarchy", "ancestor", "descendant"])
                    if pk_base and pk_base not in fk_lower and not is_hierarchical:
                        continue
                        
                    fk_vals = df[col_fk].dropna()
                    if len(fk_vals) > 0:
                        pk_vals = set(df[col_pk].dropna())
                        orphans = [v for v in fk_vals if v not in pk_vals]
                        if len(orphans) > 0:
                            issues.append(dq_issue("high", "intra_dataset_orphan_fk",
                                                   f"{len(orphans)} self-referencing orphan value(s) in '{col_fk}' (referring to '{col_pk}')",
                                                   column=col_fk, count=len(orphans),
                                                   rows=df.index[df[col_fk].isin(orphans)].tolist()[:50],
                                                   sample=list(set(orphans))[:5]))

    # ------------------------------------------------------------
    # 11. Functional Dependency Validation
    # ------------------------------------------------------------
    _DEFAULT_FUNCTIONAL_DEPENDENCIES = [
        {"determinant": "zip", "dependent": "city"},
        {"determinant": "zip", "dependent": "state"},
        {"determinant": "postal_code", "dependent": "city"},
        {"determinant": "postal_code", "dependent": "state"},
        {"determinant": "country_code", "dependent": "country"},
        {"determinant": "store_id", "dependent": "store_name"},
        {"determinant": "product_id", "dependent": "product_name"},
        {"determinant": "customer_id", "dependent": "customer_name"},
    ]
    fd_cfg = (thresholds or {}).get("functional_dependencies")
    if not fd_cfg:
        fd_cfg = _DEFAULT_FUNCTIONAL_DEPENDENCIES
    if isinstance(fd_cfg, list):
        for rule in fd_cfg:
            det = str(rule.get("determinant", "")).lower().strip()
            dep = str(rule.get("dependent", "")).lower().strip()
            if not det or not dep:
                continue
                
            cmap = {str(c).lower().strip(): c for c in df.columns}
            det_col = cmap.get(det)
            dep_col = cmap.get(dep)
            
            if det_col and dep_col:
                if len(df) >= 10:
                    clean = df[[det_col, dep_col]].dropna()
                    if len(clean) >= 10:
                        gp = clean.groupby(det_col)[dep_col].nunique()
                        violations = gp[gp > 1]
                        if len(violations) > 0:
                            violation_keys = list(violations.index)
                            sample_violations = []
                            for vk in violation_keys[:5]:
                                distinct_deps = list(clean[clean[det_col] == vk][dep_col].unique())
                                sample_violations.append({
                                    "determinant_value": vk,
                                    "distinct_dependent_values": distinct_deps
                                })
                            v_mask = df[det_col].isin(violation_keys)
                            violating_rows = df.index[v_mask].tolist()
                            
                            issues.append(dq_issue("high", "functional_dependency_violation",
                                                   f"Functional dependency violation: '{det_col}' -> '{dep_col}'. "
                                                   f"Found {len(violations)} value(s) of '{det_col}' mapping to multiple distinct values of '{dep_col}'.",
                                                   column=det_col, count=len(violations),
                                                   rows=violating_rows[:50], sample=sample_violations))

    return issues






def check_conditional_rules(df: pd.DataFrame, rules: List[Dict[str, Any]], ds_name: str) -> List[Dict[str, Any]]:
    """
    Evaluates cross-column conditional rules from dq_thresholds.yaml (`conditional_rules`).
    Returns DQ-shaped issue dicts.
    """
    issues: List[Dict[str, Any]] = []

    for rule in rules or []:
        if not isinstance(rule, dict):
            continue
        rule_type = rule.get("type")
        severity = str(rule.get("severity", "medium")).lower()

        try:
            if rule_type == "conditional_not_null":
                when_col = rule["when_column"]
                when_val = rule["when_value"]
                then_col = rule["then_column"]
                if when_col not in df.columns or then_col not in df.columns:
                    continue
                mask = (
                    df[when_col].astype(str).str.strip().eq(str(when_val)) & df[then_col].isna()
                )
                bad_rows = df.loc[mask]
                if len(bad_rows) > 0:
                    issues.append(
                        dq_issue(
                            severity,
                            "conditional_not_null",
                            f"'{then_col}' must not be null when '{when_col}' = '{when_val}'. "
                            f"{len(bad_rows)} violations found.",
                            column=then_col,
                            count=int(len(bad_rows)),
                            rows=bad_rows.index.tolist(),
                            sample=[v.isoformat() if hasattr(v, "isoformat") else v for v in bad_rows[then_col].head(5).tolist()],
                        )
                    )
                    issues[-1]["recommended_action"] = "add_conditional_not_null_check_in_etl"
                    issues[-1]["auto_fixable"] = False
                    issues[-1]["manual_guidance"] = (
                        f"Add ETL guard: when {when_col} = '{when_val}', "
                        f"reject or flag rows where {then_col} IS NULL."
                    )
                    issues[-1]["rule_context"] = rule

            elif rule_type == "conditional_format":
                when_col = rule["when_column"]
                when_val = rule["when_value"]
                then_col = rule["then_column"]
                then_regex = str(rule["then_regex"])
                if when_col not in df.columns or then_col not in df.columns:
                    continue
                pattern = re.compile(then_regex)
                m_when = df[when_col].astype(str).str.strip().eq(str(when_val))
                then_series = df[then_col]

                def _ok(cell: Any) -> bool:
                    if pd.isna(cell):
                        return False
                    return pattern.match(str(cell)) is not None

                mask = m_when & ~then_series.map(_ok)
                bad_rows = df.loc[mask]
                if len(bad_rows) > 0:
                    issues.append(
                        dq_issue(
                            severity,
                            "conditional_format",
                            f"'{then_col}' must match '{then_regex}' when '{when_col}' = '{when_val}'. "
                            f"{len(bad_rows)} violations.",
                            column=then_col,
                            count=int(len(bad_rows)),
                            rows=bad_rows.index.tolist(),
                            sample=[v.isoformat() if hasattr(v, "isoformat") else v for v in bad_rows[then_col].head(5).tolist()],
                        )
                    )
                    issues[-1]["recommended_action"] = "add_conditional_format_check_in_etl"
                    issues[-1]["auto_fixable"] = False
                    issues[-1]["manual_guidance"] = (
                        f"Add ETL format branch: when {when_col} = '{when_val}', "
                        f"validate {then_col} against pattern: {then_regex}"
                    )
                    issues[-1]["rule_context"] = rule

            elif rule_type == "conditional_range":
                when_col = rule["when_column"]
                when_val = rule["when_value"]
                then_col = rule["then_column"]
                min_val = rule.get("min")
                max_val = rule.get("max")
                if when_col not in df.columns or then_col not in df.columns:
                    continue
                cond = df[when_col].astype(str).str.strip().eq(str(when_val))
                sub = df.loc[cond]
                if sub.empty:
                    continue
                numeric = pd.to_numeric(sub[then_col], errors="coerce")
                viol = numeric.isna() & sub[then_col].notna()
                if min_val is not None:
                    viol |= numeric.notna() & (numeric < float(min_val))
                if max_val is not None:
                    viol |= numeric.notna() & (numeric > float(max_val))
                bad_rows = sub.loc[viol]
                if len(bad_rows) > 0:
                    issues.append(
                        dq_issue(
                            severity,
                            "conditional_range",
                            f"'{then_col}' must be between {min_val} and {max_val} when "
                            f"'{when_col}' = '{when_val}'. {len(bad_rows)} violations.",
                            column=then_col,
                            count=int(len(bad_rows)),
                            rows=bad_rows.index.tolist(),
                            sample=[v.isoformat() if hasattr(v, "isoformat") else v for v in bad_rows[then_col].head(5).tolist()],
                        )
                    )
                    issues[-1]["recommended_action"] = "add_conditional_range_check_in_etl"
                    issues[-1]["auto_fixable"] = False
                    issues[-1]["manual_guidance"] = (
                        f"Add ETL range guard: when {when_col} = '{when_val}', "
                        f"reject rows where {then_col} < {min_val} or > {max_val}."
                    )
                    issues[-1]["rule_context"] = rule

            elif rule_type == "mutual_exclusion":
                columns = rule.get("columns") or []
                existing = [c for c in columns if c in df.columns]
                if len(existing) < 2:
                    continue
                mask = df[existing].notna().all(axis=1)
                bad_rows = df.loc[mask]
                if len(bad_rows) > 0:
                    col_label = ",".join(existing)
                    issues.append(
                        dq_issue(
                            severity,
                            "mutual_exclusion",
                            f"Columns {existing} must not all be filled simultaneously. "
                            f"{len(bad_rows)} rows violate mutual exclusion.",
                            column=col_label,
                            count=int(len(bad_rows)),
                            rows=bad_rows.index.tolist(),
                        )
                    )
                    issues[-1]["recommended_action"] = "add_mutual_exclusion_check_in_etl"
                    issues[-1]["auto_fixable"] = False
                    issues[-1]["manual_guidance"] = (
                        f"Add ETL validation: only one of {existing} should be non-null per row."
                    )
                    issues[-1]["rule_context"] = rule

            elif rule_type == "at_least_one":
                columns = rule.get("columns") or []
                existing = [c for c in columns if c in df.columns]
                if not existing:
                    continue
                mask = df[existing].isna().all(axis=1)
                bad_rows = df.loc[mask]
                if len(bad_rows) > 0:
                    col_label = ",".join(existing)
                    issues.append(
                        dq_issue(
                            severity,
                            "at_least_one",
                            f"At least one of {existing} must be non-null. "
                            f"{len(bad_rows)} rows have all null.",
                            column=col_label,
                            count=int(len(bad_rows)),
                            rows=bad_rows.index.tolist(),
                        )
                    )
                    issues[-1]["recommended_action"] = "add_at_least_one_check_in_etl"
                    issues[-1]["auto_fixable"] = False
                    issues[-1]["manual_guidance"] = (
                        f"Add ETL guard: reject rows where ALL of {existing} are null."
                    )
                    issues[-1]["rule_context"] = rule

        except Exception as e:
            issues.append(
                {
                    "severity": "low",
                    "type": "conditional_rule_error",
                    "column": None,
                    "count": None,
                    "row_indexes": [],
                    "sample_values": [],
                    "message": f"Rule evaluation error ({rule_type}): {str(e)}",
                    "fixability": "COMPLEX",
                    "recommended_action": "review_manually",
                    "auto_fixable": False,
                }
            )

    return issues


def check_formula_rules(df: pd.DataFrame, rules: List[Dict[str, Any]], issue_type: str = "formula_rule_violation") -> List[Dict[str, Any]]:
    """
    Evaluates multi-column math/logical formula assertions from dq_thresholds.yaml (`formula_rules`).
    """
    issues = []
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        assertion = rule.get("assertion")
        if not assertion:
            continue
        severity = str(rule.get("severity", "medium")).lower()
        custom_msg = rule.get("message")
        
        try:
            # Find which columns are in the assertion using word-boundary regex
            ref_cols = []
            for col in df.columns:
                pattern = r'\b' + re.escape(col) + r'\b'
                if re.search(pattern, assertion):
                    ref_cols.append(col)
                    
            if not ref_cols:
                continue
                
            # Create evaluation dataframe where referenced columns are coerced to numeric
            eval_df = pd.DataFrame(index=df.index)
            valid_rows = pd.Series(True, index=df.index)
            
            for col in ref_cols:
                eval_df[col] = pd.to_numeric(df[col], errors='coerce')
                valid_rows &= eval_df[col].notna()
                
            if not valid_rows.any():
                continue
                
            # Evaluate assertion on valid rows
            res = eval_df.eval(assertion)
            
            # Mask of violations (valid rows where assertion evaluated to False or nan)
            viol_mask = valid_rows & (~res.fillna(False))
            viol_cnt = int(viol_mask.sum())
            
            if viol_cnt > 0:
                rows = df.index[viol_mask].tolist()
                msg = custom_msg or f"Formula assertion failed: '{assertion}' ({viol_cnt} violations)"
                issues.append(dq_issue(
                    severity,
                    issue_type,
                    msg,
                    column=",".join(ref_cols),
                    count=viol_cnt,
                    rows=rows,
                    sample=df.loc[viol_mask, ref_cols].head(5).to_dict(orient="records")
                ))
        except Exception as e:
            issues.append(dq_issue(
                "low",
                "formula_rule_error",
                f"Failed to evaluate formula '{assertion}': {str(e)}"
            ))
            
    return issues


def analyze_dataset_quality(
    name: str,
    df: pd.DataFrame,
    profile: Dict[str, Any],
    thresholds: Optional[Dict[str, Any]] = None,
    job_id: Optional[str] = None,
    business_rules: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Return a dict with dataset-level issues + summary (uses config-driven thresholds for duplicate severity).
    """
    thresholds = thresholds or {}
    dup_pct_high = _get_threshold(thresholds, "severity", "duplicate_row_pct_high", default=0.05)
    dup_pct_warn = _get_threshold(thresholds, "severity", "duplicate_row_pct_warn", default=0.02)

    issues: List[Dict[str, Any]] = []
    n = len(df)

    try:
        dup_mask = df.duplicated()

        dup_rows = int(dup_mask.sum())
    except Exception:
        dup_rows, dup_mask = 0, pd.Series(False, index=df.index)

    if dup_rows > 0:
        ratio = dup_rows / max(n, 1)
        sev = "high" if ratio > dup_pct_high else ("medium" if ratio > dup_pct_warn else "low")
        rows = df.index[dup_mask].tolist()
        issues.append(dq_issue(sev, "duplicate_rows", f"{dup_rows} duplicate row(s)",
                               column="[Row-level]", count=dup_rows, rows=rows))

    # Parallelized column-level DQ checks
    with concurrent.futures.ThreadPoolExecutor() as executor:
        col_futures = [
            executor.submit(
                analyze_column,
                df[col],
                col,
                profile.get("columns", {}).get(col, {}).get("semantic_type", "unknown"),
                thresholds
            )
            for col in df.columns
        ]
        for future in concurrent.futures.as_completed(col_futures):
            issues.extend(future.result())

    issues.extend(run_extended_dq_checks(df, profile, thresholds))

    cpk_cols = [c for c, m in profile.get("columns", {}).items() if m.get("candidate_primary_key", False)]
    for cpk in cpk_cols:
        if cpk not in df.columns: continue
        cdup_mask = df[cpk].duplicated()
        if cdup_mask.any():
            dup_count = int(cdup_mask.sum())
            rows = df.index[cdup_mask].tolist()[:50]
            issues.append(dq_issue("high", "duplicate_primary_key",
                                   f"{dup_count} duplicate in candidate PK",
                                   column=cpk, count=dup_count, rows=rows,
                                   sample=[v.isoformat() if hasattr(v, "isoformat") else v for v in df.loc[cdup_mask, cpk].head(5).tolist()]))

    if not cpk_cols and n > 0:
        for col, m in profile.get("columns", {}).items():
            if m.get("null_percentage", 1.0) <= 0.05 and m.get("unique_count", 0) >= int(0.98 * n):
                issues.append(dq_issue("low", "potential_primary_key",
                                       "Highly unique and low-null; consider as PK",
                                       column=col))

    conditional_rules = (thresholds or {}).get("conditional_rules") or []
    if conditional_rules:
        cond_iss = check_conditional_rules(df, conditional_rules, name)
        for ci in cond_iss:
            enrich_issue_with_fixability(ci)
        issues.extend(cond_iss)

    formula_rules = (thresholds or {}).get("formula_rules") or []
    if formula_rules:
        form_iss = check_formula_rules(df, formula_rules)
        for fi in form_iss:
            enrich_issue_with_fixability(fi)
        issues.extend(form_iss)

    if business_rules:
        # 1. Valid values validation
        for col in df.columns:
            vv = get_valid_values_for_column(business_rules, name, col)
            if vv is not None:
                lower_vv = {str(v).lower() for v in vv}
                col_values_lower = df[col].astype(str).str.lower()
                invalid_mask = df[col].notna() & (~col_values_lower.isin(lower_vv))
                invalid_cnt = int(invalid_mask.sum())
                if invalid_cnt > 0:
                    rows = df.index[invalid_mask].tolist()
                    sample = df.loc[invalid_mask, col].head(5).tolist()
                    iss = dq_issue(
                        "high",
                        "invalid_lookup_value",
                        f"Value not in allowed lookup list for {col} ({invalid_cnt} invalid value(s))",
                        column=col,
                        count=invalid_cnt,
                        rows=rows,
                        sample=[str(v) for v in sample]
                    )
                    enrich_issue_with_fixability(iss)
                    issues.append(iss)

        # 2. Custom assertions validation
        rules_list = []
        if isinstance(business_rules, dict):
            rules_list.extend(business_rules.get("custom_assertions") or [])
            rules_list.extend(business_rules.get("assertions") or [])
        
        seen_assertions = set()
        deduped_rules = []
        for r in rules_list:
            if isinstance(r, dict) and r.get("assertion"):
                ast = r.get("assertion")
                if ast not in seen_assertions:
                    seen_assertions.add(ast)
                    deduped_rules.append(r)
        
        if deduped_rules:
            custom_iss = check_custom_assertions(df, deduped_rules)
            for ci in custom_iss:
                enrich_issue_with_fixability(ci)
            issues.extend(custom_iss)

    # Filter out suppressed rules
    suppressed = (thresholds or {}).get("suppressed_rules") or []
    if suppressed:
        issues = [it for it in issues if it.get("type") not in suppressed]

    # DQ score (0-100) and clean row estimates
    try:
        score_cfg = (thresholds or {}).get("dq_score") or {}
        w = (score_cfg.get("weights") or {})
        wh = float(w.get("high", 3.0))
        wm = float(w.get("medium", 1.0))
        wl = float(w.get("low", 0.3))
        sev_w = {"high": wh, "medium": wm, "low": wl}

        # penalize by DISTINCT affected rows per severity.
        # This avoids double-penalizing the same bad rows across multiple issue types.
        high_rows, med_rows, low_rows = set(), set(), set()
        for it in issues:
            sev = str(it.get("severity") or "low").lower()
            rows = it.get("row_indexes") or []
            if rows:
                if sev == "high":
                    high_rows.update(rows)
                elif sev == "medium":
                    med_rows.update(rows)
                else:
                    low_rows.update(rows)

        # Remove overlap so a row counted as HIGH doesn't also count against MED/LOW
        med_rows = set(med_rows) - set(high_rows)
        low_rows = set(low_rows) - set(high_rows) - set(med_rows)

        frac_h = len(high_rows) / max(1, n)
        frac_m = len(med_rows) / max(1, n)
        frac_l = len(low_rows) / max(1, n)

        raw_penalty = (sev_w["high"] * frac_h) + (sev_w["medium"] * frac_m) + (sev_w["low"] * frac_l)
        max_penalty = sev_w["high"] + sev_w["medium"] + sev_w["low"]
        dq_score = 100.0 * max(0.0, 1.0 - (raw_penalty / max(1e-9, max_penalty)))

        clean_est_high = max(0, n - len(high_rows))
        clean_est_high_med = max(0, n - len(high_rows.union(med_rows)))
    except Exception:
        dq_score = None
        clean_est_high = None
        clean_est_high_med = None

    return {
        "issues": issues,
        "summary": {
            "issue_count": len(issues),
            "high_severity": sum(1 for i in issues if i["severity"] == "high"),
            "medium_severity": sum(1 for i in issues if i["severity"] == "medium"),
            "low_severity": sum(1 for i in issues if i["severity"] == "low"),
            "dq_score_0_100": dq_score,
            "estimated_clean_rows_after_high": clean_est_high,
            "estimated_clean_rows_after_high_and_medium": clean_est_high_med,
        }
    }


# ============================================================
# GLOBAL ISSUES (orphans, cross-dataset inconsistencies)
# ============================================================

def detect_global_issues(datasets: Dict[str, pd.DataFrame], thresholds: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    - Orphan foreign keys: values present in one dataset.column but not in the counterpart
    - Cross-dataset inconsistencies: coarse mixed numeric/text indicator per column by parse-rate
    """
    thresholds = thresholds or {}
    rel_cfg = thresholds.get("relationships") or {}
    orphan_only_if_same_data = bool(rel_cfg.get("orphan_only_if_same_dataset", True))
    same_data_min_id_overlap = float(rel_cfg.get("same_dataset_min_id_overlap_ratio", 0.90))
    same_data_max_row_diff = float(rel_cfg.get("same_dataset_max_rowcount_diff_ratio", 0.10))

    def _same_dataset_representation(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
        try:
            cols1 = {str(c).strip().lower() for c in df1.columns}
            cols2 = {str(c).strip().lower() for c in df2.columns}
            if cols1 != cols2 or "id" not in cols1:
                return False
            c1 = next(c for c in df1.columns if str(c).lower() == "id")
            c2 = next(c for c in df2.columns if str(c).lower() == "id")
            k1 = set(df1[c1].map(_to_key).dropna().tolist())
            k2 = set(df2[c2].map(_to_key).dropna().tolist())
            if not k1 or not k2:
                return False
            inter = k1 & k2
            overlap_ratio = len(inter) / max(1, min(len(k1), len(k2)))
            r1, r2 = len(df1), len(df2)
            row_diff_ratio = abs(r1 - r2) / max(1, max(r1, r2))
            if not (overlap_ratio >= same_data_min_id_overlap and row_diff_ratio <= same_data_max_row_diff):
                return False

            # Stronger check: do rows actually match on shared IDs?
            inter_list = list(inter)
            if len(inter_list) > 50:
                inter_list = inter_list[:50]
            cols = [c for c in df1.columns if str(c).lower() != "id"]
            if not cols:
                return True

            def _row_sig(df: pd.DataFrame, id_col: str) -> Dict[Any, Tuple[Any, ...]]:
                out = {}
                for _, row in df.iterrows():
                    ik = _to_key(row[id_col])
                    if ik is None or ik in out:
                        continue
                    out[ik] = tuple(_to_key(row[c]) for c in cols)
                return out

            m1 = _row_sig(df1, c1)
            m2 = _row_sig(df2, c2)
            if not m1 or not m2:
                return False
            matches = 0
            total = 0
            for ik in inter_list:
                if ik in m1 and ik in m2:
                    total += 1
                    if m1[ik] == m2[ik]:
                        matches += 1
            if total == 0:
                return False
            return (matches / total) >= 0.80
        except Exception:
            return False

    global_issues = {
        "orphan_foreign_keys": [],
        "cross_dataset_inconsistencies": [],
        "schema_drift": []
    }

    names = list(datasets.keys())
    for i in range(len(names)):
        df1 = datasets[names[i]]
        for j in range(i + 1, len(names)):
            df2 = datasets[names[j]]
            same_data = _same_dataset_representation(df1, df2)

            common = set(map(str.lower, df1.columns)) & set(map(str.lower, df2.columns))
            for col in common:
                if not col.endswith("id"):
                    continue
                c1 = next(x for x in df1.columns if x.lower() == col)
                c2 = next(x for x in df2.columns if x.lower() == col)

                s1 = df1[c1].dropna()
                s2 = df2[c2].dropna()

                try:
                    set1 = set(s1.map(_to_key).dropna())
                    set2 = set(s2.map(_to_key).dropna())
                except Exception:
                    continue

                only_left = list(set1 - set2)
                only_right = list(set2 - set1)

                _orph_rec = (
                    "Align keys between datasets (trim, type cast). Add missing reference rows or remove "
                    "orphan facts in the child extract. Prefer FK constraints in the source system."
                )
                if (not orphan_only_if_same_data) or same_data:
                    if only_left:
                        global_issues["orphan_foreign_keys"].append({
                            "from": f"{names[i]}.{c1}",
                            "to": f"{names[j]}.{c2}",
                            "orphan_count": len(only_left),
                            "sample_values": only_left[:10],
                            "recommendation": _orph_rec,
                        })
                    if only_right:
                        global_issues["orphan_foreign_keys"].append({
                            "from": f"{names[j]}.{c2}",
                            "to": f"{names[i]}.{c1}",
                            "orphan_count": len(only_right),
                            "sample_values": only_right[:10],
                            "recommendation": _orph_rec,
                        })

            for nm, df in ((names[i], df1), (names[j], df2)):
                for col in df.columns:
                    s = df[col].map(_strip)
                    num = pd.to_numeric(s, errors="coerce")
                    parse_rate = 1.0 - float(num.isna().mean())
                    if 0.2 < parse_rate < 0.8:
                        global_issues["cross_dataset_inconsistencies"].append({
                            "dataset": nm,
                            "column": col,
                            "issue_type": "mixed_types",
                            "message": f"Mixed numeric/text values (parse={round(parse_rate*100,1)}%)",
                            "recommendation": (
                                "Standardize to one type in staging: coerce numerics after validation, "
                                "or split into _raw and _numeric columns."
                            ),
                        })

    # Deduplicate cross-dataset inconsistencies: one row per (dataset, column, issue_type)
    try:
        seen = set()
        deduped = []
        for x in global_issues.get("cross_dataset_inconsistencies", []) or []:
            key = (x.get("dataset"), x.get("column"), x.get("issue_type"))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(x)
        global_issues["cross_dataset_inconsistencies"] = deduped
    except Exception:
        pass

    # ------------------------------------------------------------
    # 8. Schema Drift Detection Across Runs
    # ------------------------------------------------------------
    import os
    import json
    
    schema_cache_file = os.path.join("config", "schema_cache.json")
    # Build current schema representation
    current_schema = {}
    for name, df in datasets.items():
        current_schema[name] = {
            col: str(df[col].dtype) for col in df.columns
        }
        
    prev_schema = {}
    if os.path.exists(schema_cache_file):
        try:
            with open(schema_cache_file, "r", encoding="utf-8") as f:
                prev_schema = json.load(f)
        except Exception:
            pass
            
    # Save current schema for next runs
    try:
        os.makedirs(os.path.dirname(schema_cache_file), exist_ok=True)
        with open(schema_cache_file, "w", encoding="utf-8") as f:
            json.dump(current_schema, f, indent=4)
    except Exception:
        pass
        
    # Compare current schema with previous schema
    if prev_schema:
        for ds_name, curr_cols in current_schema.items():
            if ds_name in prev_schema:
                prev_cols = prev_schema[ds_name]
                added = [c for c in curr_cols if c not in prev_cols]
                removed = [c for c in prev_cols if c not in curr_cols]
                type_changed = []
                for c in curr_cols:
                    if c in prev_cols and curr_cols[c] != prev_cols[c]:
                        type_changed.append({"column": c, "from": prev_cols[c], "to": curr_cols[c]})
                        
                if added or removed or type_changed:
                    global_issues["schema_drift"].append({
                        "dataset": ds_name,
                        "added_columns": added,
                        "removed_columns": removed,
                        "type_changes": type_changed,
                        "message": f"Schema drift detected on '{ds_name}'. Added: {added}, Removed: {removed}, Type changes: {type_changed}"
                    })

    return global_issues


# ============================================================
# CUSTOM RULES (config-driven, applied after standard DQ)
# ============================================================

def run_custom_rules(
    datasets: Dict[str, pd.DataFrame],
    custom_rules: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Apply custom rules from config. Each rule: dataset (or "*"), column, rule, params.
    rule: one_of, not_one_of, range, regex, not_null.
    Returns extra issues per dataset name.
    """
    extra: Dict[str, List[Dict[str, Any]]] = {}
    if not custom_rules:
        return extra

    for rule_cfg in custom_rules:
        dataset_pattern = (rule_cfg.get("dataset") or "*").strip()
        column = rule_cfg.get("column")
        rule_type = (rule_cfg.get("rule") or "").strip().lower()
        params = rule_cfg.get("params")
        if not column or not rule_type:
            continue

        for ds_name, df in datasets.items():
            if dataset_pattern != "*" and dataset_pattern != ds_name:
                continue
            if column not in df.columns:
                continue
            s = df[column].dropna().astype(str)
            if s.empty:
                continue
            issues: List[Dict[str, Any]] = []
            if rule_type == "one_of" and isinstance(params, list):
                allowed = set(str(x).strip().lower() for x in params)
                bad = ~s.str.strip().str.lower().isin(allowed)
                if bad.any():
                    cnt = int(bad.sum())
                    issues.append(dq_issue("medium", "custom_one_of",
                        f"Value not in allowed list ({cnt} rows)", column=column, count=cnt,
                        rows=df.index[bad].tolist()[:50], sample=list(s[bad].head(5))))
            elif rule_type == "range" and isinstance(params, dict):
                try:
                    num = pd.to_numeric(s, errors="coerce")
                    min_v = params.get("min")
                    max_v = params.get("max")
                    bad = pd.Series(False, index=s.index)
                    if min_v is not None:
                        bad = bad | (num < float(min_v))
                    if max_v is not None:
                        bad = bad | (num > float(max_v))
                    if bad.any():
                        cnt = int(bad.sum())
                        issues.append(dq_issue("high", "custom_range",
                            f"Value outside range ({cnt} rows)", column=column, count=cnt,
                            rows=df.index[bad].tolist()[:50], sample=list(s[bad].head(5))))
                except (TypeError, ValueError):
                    pass
            elif rule_type == "regex" and isinstance(params, (str, dict)):
                pattern = params if isinstance(params, str) else params.get("pattern", "")
                if not pattern:
                    continue
                try:
                    import re as re_mod
                    pat = re_mod.compile(pattern)
                    bad = ~s.str.strip().apply(lambda v: bool(pat.match(v)) if isinstance(v, str) else False)
                    if bad.any():
                        cnt = int(bad.sum())
                        issues.append(dq_issue("medium", "custom_regex",
                            f"Value does not match pattern ({cnt} rows)", column=column, count=cnt,
                            rows=df.index[bad].tolist()[:50], sample=list(s[bad].head(5))))
                except Exception:
                    pass
            elif rule_type == "not_null":
                null_mask = df[column].isna() | (df[column].astype(str).str.strip() == "")
                if null_mask.any():
                    cnt = int(null_mask.sum())
                    issues.append(dq_issue("high", "custom_not_null",
                        f"Null or empty not allowed ({cnt} rows)", column=column, count=cnt,
                        rows=df.index[null_mask].tolist()[:50], sample=list(df.loc[null_mask, column].head(5))))
            for i in issues:
                if ds_name not in extra:
                    extra[ds_name] = []
                extra[ds_name].append(i)
    return extra


def detect_date_format_variants(series: pd.Series) -> list[dict]:
    """
    For object/string columns suspected as dates, count format variants.
    Returns list of {"format": str, "count": int, "pct": float}
    """
    import re
    patterns = {
        "DD/MM/YYYY": r"^\d{2}/\d{2}/\d{4}$",
        "YYYY-MM-DD": r"^\d{4}-\d{2}-\d{2}$",
        "MM-DD-YYYY": r"^\d{2}-\d{2}-\d{4}$",
        "YYYY/MM/DD": r"^\d{4}/\d{2}/\d{2}$",
        "Mon D YYYY": r"^[A-Za-z]+ \d{1,2} \d{4}$",
        "DD-Mon-YYYY": r"^\d{2}-[A-Za-z]+-\d{4}$",
    }
    sample = series.dropna().astype(str).str.strip().head(5000)
    total = len(sample)
    results = []
    if total == 0:
        return []
    for fmt_name, pattern in patterns.items():
        count = sample.str.match(pattern).sum()
        if count > 0:
            results.append({"format": fmt_name, "count": int(count), "pct": round(float(count / total), 4)})
    return sorted(results, key=lambda x: -x["count"])


def confirm_business_key_duplicates(df: pd.DataFrame, pk_cols: list[str]) -> dict:
    """
    Given LLM-suggested PK columns, confirm actual duplicate count.
    """
    available = [c for c in pk_cols if c in df.columns]
    if not available:
        return {"confirmed": False, "reason": "pk_cols not found in dataframe"}
    dup_count = int(df.duplicated(subset=available).sum())
    return {
        "confirmed": True,
        "business_key_cols": available,
        "business_key_duplicate_count": dup_count,
        "dedup_strategy_hint": "keep_last" if dup_count > 0 else "no_action_needed"
    }


def detect_null_pattern(df: pd.DataFrame, col_name: str) -> dict:
    """
    Check if nulls in col_name correlate with a specific categorical column (MNAR detection).
    Caps at top-5 categorical columns to keep performance O(n).
    """
    null_mask = df[col_name].isnull()
    total_nulls = null_mask.sum()
    if total_nulls == 0:
        return {"type": "none"}
    cat_cols = [c for c in df.columns if c != col_name and df[c].dtype == object][:5]
    for cat_col in cat_cols:
        try:
            null_by_cat = df.groupby(cat_col)[col_name].apply(lambda x: x.isnull().mean())
            if not null_by_cat.empty and null_by_cat.max() > 0.8: # 80%+ nulls concentrated in one category
                return {
                    "type": "MNAR",
                    "concentrated_in_col": cat_col,
                    "concentrated_in_value": str(null_by_cat.idxmax()),
                    "fill_strategy_hint": "flag_only"
                }
        except Exception:
            pass
    return {"type": "MCAR", "fill_strategy_hint": "median_or_mode"}


# ============================================================
# MAIN ENTRYPOINT
# ============================================================

def load_and_profile(
    source_cfg: Dict[str, Any],
    *,
    additional_data: Optional[Dict[str, pd.DataFrame]] = None,
    dq_thresholds_path: Optional[str] = None,
    dq_thresholds: Optional[Dict[str, Any]] = None,
    return_datasets: bool = False,
    location_types: Optional[Collection[str]] = None,
    job_id: Optional[str] = None,
    max_rows: Optional[int] = None,
    business_rules: Optional[Dict[str, Any]] = None,
    db_connectors: Optional[Dict[str, Any]] = None,
    approved_semantics: Optional[Dict[str, Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """
    Orchestrator:
    - Iterate over source_cfg["locations"]: all database + filesystem entries (azure_blob via additional_data)
    - Multiple databases: table keys prefixed (id/label or db hash) so names never collide
    - Merge with additional_data if provided (e.g., from Azure Blob Storage)
    - Profile each dataset; per-dataset DQ; relationships; global issues.
    - dq_thresholds: optional dict (if None, loaded from dq_thresholds_path or config).
    - return_datasets: if True, add result["_datasets"] = raw DataFrames (pop before JSON serialize).
    - location_types: optional set/list of lowercase location type strings (e.g. {"database","azure_blob"}).
      If set, only those location blocks are loaded from YAML. Blob data still comes only via additional_data
      (caller should pass {} when blob is excluded). If None, all location types are processed.
    """
    thresholds = dq_thresholds
    if thresholds is None:
        thresholds = load_dq_thresholds(dq_thresholds_path)

    datasets: Dict[str, pd.DataFrame] = {}
    source_root_by_dataset: Dict[str, str] = {}

    db_connectors_by_dataset: Dict[str, Tuple[Any, str]] = {}
    if db_connectors:
        for k, v in db_connectors.items():
            if isinstance(v, tuple) and len(v) == 2:
                db_connectors_by_dataset[k] = v
            else:
                table_name = k.split("__")[-1]
                db_connectors_by_dataset[k] = (v, table_name)

    locations = list(source_cfg.get("locations", []) or [])
    if location_types is not None:
        allowed = {str(t).lower() for t in location_types}
        locations = [loc for loc in locations if (loc.get("type") or "").lower() in allowed]
    db_locs = [loc for loc in locations if (loc.get("type") or "").lower() == "database"]
    multi_db = len(db_locs) > 1
    db_seen = 0

    for loc in locations:
        typ = (loc.get("type") or "").lower()

        if typ == "database":
            conn = loc.get("connection", {}) or {}
            prefix = _sql_location_key_prefix(loc, conn, db_seen, multi_db)
            label = (prefix.rstrip("_") if prefix else "") or "__default__"
            for table_key, df in load_sql_datasets(
                conn, dataset_key_prefix=prefix, max_rows=max_rows, db_connectors_by_dataset=db_connectors_by_dataset
            ).items():
                datasets[table_key] = df
                source_root_by_dataset[table_key] = (
                    f"__database__:{label}" if multi_db else "__database__"
                )
            db_seen += 1

        elif typ == "filesystem":
            fp = loc.get("path")
            if fp:
                root = os.path.abspath(os.path.normpath(fp))
                for fname, df in load_file_datasets(root, max_rows=max_rows).items():
                    key = fname
                    if key in datasets:
                        key = f"{os.path.basename(root.rstrip(os.sep))}__{fname}"
                    if key in datasets:
                        key = f"{hashlib.md5(root.encode('utf-8')).hexdigest()[:8]}__{fname}"
                    datasets[key] = df
                    source_root_by_dataset[key] = root

    if additional_data:
        for name, df in additional_data.items():
            datasets[name] = df
            norm = (name or "").replace("\\", "/")
            parent = os.path.dirname(norm).strip("/")
            source_root_by_dataset[name] = (
                f"azure_blob:{parent}" if parent else "azure_blob:"
            )

    metadata = {}
    for name, df in datasets.items():
        if job_id:
            from agent.jobs_store import add_event
            add_event(job_id=job_id, level="info", message=f"Profiling dataset: {name}")
        meta = profile_dataframe(df, job_id=job_id)
        try:
            from agent.specialists.ydata_profiler import enrich_assessment_with_profile
            meta = enrich_assessment_with_profile(df, meta)
        except Exception:
            pass # ydata-profiling optional — graceful skip
            
        if name in db_connectors_by_dataset:
            connector, table = db_connectors_by_dataset[name]
            try:
                db_prof = profile_database_table_full(connector, table, df, job_id=job_id)
                meta = merge_in_db_profile(meta, db_prof)
            except Exception as e:
                if job_id:
                    from agent.jobs_store import add_event
                    add_event(job_id=job_id, level="warning", message=f"Full database profiling failed for {name}: {e}")
                    
        meta["source_root"] = source_root_by_dataset.get(name, "")
        metadata[name] = meta

    if approved_semantics:
        import re
        def _norm_key(k: str) -> str:
            return re.sub(r"[^\w]+", "", str(k).lower())

        norm_metadata = {_norm_key(k): k for k in metadata.keys()}
        for name, table_sem in approved_semantics.items():
            norm_name = _norm_key(name)
            if norm_name in norm_metadata:
                meta = metadata[norm_metadata[norm_name]]
                norm_cols = {_norm_key(c): c for c in meta.get("columns", {})}
                for col, tag in table_sem.items():
                    norm_col = _norm_key(col)
                    if norm_col in norm_cols:
                        meta["columns"][norm_cols[norm_col]]["semantic_type"] = tag

    if len(datasets) >= 2:
        try:
            from agent.specialists.cross_dataset_agent import generate_sweetviz_comparison
            names = list(datasets.keys())
            generate_sweetviz_comparison(
                datasets[names[0]], datasets[names[1]], names[0], names[1]
            )
        except Exception:
            pass

    per_dataset_dq = {}
    for name, df in datasets.items():
        if job_id:
            from agent.jobs_store import add_event
            add_event(job_id=job_id, level="info", message=f"Analyzing data quality: {name}")
        per_dataset_dq[name] = analyze_dataset_quality(name, df, metadata[name], thresholds, job_id=job_id, business_rules=business_rules)
        metadata[name]["quality"] = per_dataset_dq[name]
        if job_id:
            add_event(job_id=job_id, level="info", message=f"Quality check complete for {name}")

    # Apply custom rules from config and merge into per_dataset_dq
    custom_rules = (thresholds or {}).get("custom_rules") or []
    if isinstance(custom_rules, list):
        extra_issues = run_custom_rules(datasets, custom_rules)
        for ds_name, issues in extra_issues.items():
            if ds_name in per_dataset_dq:
                per_dataset_dq[ds_name]["issues"].extend(issues)
                per_dataset_dq[ds_name]["summary"]["issue_count"] = len(per_dataset_dq[ds_name]["issues"])
                per_dataset_dq[ds_name]["summary"]["medium_severity"] = sum(
                    1 for i in per_dataset_dq[ds_name]["issues"] if i.get("severity") == "medium"
                )
                per_dataset_dq[ds_name]["summary"]["high_severity"] = sum(
                    1 for i in per_dataset_dq[ds_name]["issues"] if i.get("severity") == "high"
                )

    rel_bundle = analyze_cross_dataset_relationships(datasets, metadata, thresholds)
    relationships = rel_bundle["relationships"]
    global_issues = detect_global_issues(datasets, thresholds)
    global_issues["relationship_row_issues"] = rel_bundle["relationship_row_issues"]
    global_issues["relationship_warnings"] = rel_bundle["relationship_warnings"]
    global_issues["cross_dataset_consistency"] = analyze_cross_dataset_consistency(datasets, metadata, thresholds)

    is_sampled = (max_rows is not None)
    for ds_name, block in per_dataset_dq.items():
        ds_sampled = is_sampled or (metadata.get(ds_name, {}).get("row_count", 0) > HEAVY_OPERATION_THRESHOLD)
        for iss in block.get("issues", []):
            iss.setdefault("dataset", ds_name)
            enrich_issue_with_recommendation(iss)
            enrich_issue_with_fixability(iss)
            if ds_sampled and iss.get("row_indexes"):
                iss["row_indexes_estimated"] = True
                if "estimated" not in str(iss.get("message")).lower():
                    iss["message"] = f"{iss['message']} (Row indexes are estimated based on a sampled subset of the data)"

    # Enrich global/cross-dataset issues
    try:
        for iss in (global_issues.get("relationship_row_issues") or []):
            enrich_issue_with_recommendation(iss)
            enrich_issue_with_fixability(iss)
            if is_sampled and iss.get("row_indexes"):
                iss["row_indexes_estimated"] = True
                if "estimated" not in str(iss.get("message")).lower():
                    iss["message"] = f"{iss['message']} (Row indexes are estimated based on a sampled subset of the data)"
        for iss in (global_issues.get("relationship_warnings") or []):
            enrich_issue_with_recommendation(iss)
            enrich_issue_with_fixability(iss)
            if is_sampled and iss.get("row_indexes"):
                iss["row_indexes_estimated"] = True
                if "estimated" not in str(iss.get("message")).lower():
                    iss["message"] = f"{iss['message']} (Row indexes are estimated based on a sampled subset of the data)"
        for iss in (global_issues.get("cross_dataset_consistency") or []):
            enrich_issue_with_recommendation(iss)
            enrich_issue_with_fixability(iss)
            if is_sampled and iss.get("row_indexes"):
                iss["row_indexes_estimated"] = True
                if "estimated" not in str(iss.get("message")).lower():
                    iss["message"] = f"{iss['message']} (Row indexes are estimated based on a sampled subset of the data)"
        for iss in (global_issues.get("cross_dataset_inconsistencies") or []):
            # these use issue_type, not type
            if isinstance(iss, dict) and iss.get("issue_type") and not iss.get("fixability"):
                iss["fixability"] = FIXABILITY_BY_ISSUE_TYPE.get(str(iss.get("issue_type")), "COMPLEX")
    except Exception:
        pass

    out = {
        "datasets": metadata,
        "relationships": relationships,
        "data_quality_issues": {
            "datasets": per_dataset_dq,
            "global_issues": global_issues
        },
        "executive_summary_items": build_executive_summary_items(per_dataset_dq, global_issues, thresholds),
    }

    # 1. Run LLM Schema Enrichment first
    try:
        from agent.llm_schema_enricher import enrich_assessment_with_schema_llm
        out = enrich_assessment_with_schema_llm(out)
    except Exception as e:
        logger.error(f"Enrichment error: {e}")

    # 2. Run the Pandas Confirmation Pass using the loaded dataframes
    for name, df in datasets.items():
        if name not in out["datasets"]:
            continue
        ds_meta = out["datasets"][name]
        
        # A. Business Key duplicate confirmation
        llm_ds_hints = ds_meta.setdefault("llm_hints", {})
        probable_pks = llm_ds_hints.get("probable_pk_columns") or []
        if probable_pks:
            dup_info = confirm_business_key_duplicates(df, probable_pks)
            llm_ds_hints["business_key_confirmation"] = dup_info
            
        # B. Date variant and Null patterns per column
        for col_name, col_meta in ds_meta.get("columns", {}).items():
            if col_name not in df.columns:
                continue
            hints = col_meta.setdefault("llm_hints", {})
            
            # Date check
            if hints.get("mixed_formats_suspected") or hints.get("semantic_type") == "date":
                fmt_vars = detect_date_format_variants(df[col_name])
                hints["format_variants"] = fmt_vars
                if len(fmt_vars) > 1:
                    hints["mixed_formats_suspected"] = True
                    
            # Null pattern check
            if col_meta.get("null_percentage", 0) > 0:
                null_pat = detect_null_pattern(df, col_name)
                hints["null_pattern"] = null_pat

    if return_datasets:
        out["_datasets"] = datasets

    return out
