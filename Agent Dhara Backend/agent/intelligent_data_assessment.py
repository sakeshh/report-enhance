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


def detect_semantic_type(values: pd.Series) -> str:
    """
    Lightweight semantic type detector using a small sample.
    Returns: "date" | "email" | "numeric_id" | "free_text" | "categorical" | "unknown"
    """
    sample = values.dropna().astype(str).head(100)
    if sample.empty:
        return "unknown"
    if sample.str.match(r"^\d{4}-\d{2}-\d{2}$").any():
        return "date"
    if sample.str.contains("@").any():
        return "email"
    if sample.str.fullmatch(r"\d+").all():
        return "numeric_id"
    if sample.str.len().mean() > 50:
        return "free_text"
    return "categorical"


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
        semantic = detect_semantic_type(s)
        hint = _dtype_inference_for_object(s) if dtype_str == "object" else None
        if semantic == "numeric_id" and hint == "datetime_like":
            hint = "numeric_like"
        type_dist = scalar_type_distribution(s) if dtype_str == "object" else None

        col_profile = {
            "dtype": dtype_str,
            "dtype_inference": hint,
            "type_distribution": type_dist,
            "null_percentage": float(s.isna().mean()),
            "unique_count": safe_nunique(s),
            "semantic_type": semantic,
            "candidate_primary_key": safe_is_unique(s),
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
    connection_cfg: Dict[str, Any], dataset_key_prefix: str = "", max_rows: Optional[int] = None
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

        for table in tables:
            key = f"{p}{table}" if p else table
            try:
                datasets[key] = connector.load_table(table, max_rows=max_rows)
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
                if max_rows and len(rows) >= int(max_rows):
                    break
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

def load_file_datasets(path: str, max_rows: Optional[int] = None) -> Dict[str, pd.DataFrame]:
    """
    Reads supported files from a local folder and returns a dict: { "<file_name>": DataFrame }
    """
    data: Dict[str, pd.DataFrame] = {}

    if not os.path.isdir(path):
        print("[INFO] Filesystem path not found:", path)
        return data

    for file in os.listdir(path):
        fp = os.path.join(path, file)
        if not os.path.isfile(fp):
            continue

        try:
            low = file.lower()
            if low.endswith(".csv"):
                data[file] = pd.read_csv(fp, low_memory=False, nrows=max_rows)
            elif low.endswith(".tsv"):
                data[file] = pd.read_csv(fp, sep="\t", low_memory=False, nrows=max_rows)
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
    return ("many_to_many", f"Keys repeat on both sides (up to {m1} vs {m2} rows per key) — M:N or bridge-style.")


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
                            f"{n1}.{c1} ↔ {n2}.{c2}: keys repeat on both sides "
                            f"(max {m1} rows per key in {n1}, max {m2} in {n2})."
                        ),
                        "recommendation": (
                            "If you expected a parent–child (1:N) model, deduplicate keys on the 'one' side "
                            "or fix source extraction. If M:N is correct (e.g. orders–products), model it with "
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
    Uses a lightweight scoring model (severity × datasets affected).
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
    "unknown", "not available", "missing"
}
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PHONE_RE = re.compile(r"^[+()\-\.\s0-9]{7,}$")

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
    # complex / requires domain decision
    # invalid emails can't be deterministically corrected (only quarantine/drop)
    "invalid_email": "NOT_FIXABLE",
    "invalid_phone": "COMPLEX",
    "mixed_phone_formats": "COMPLEX",
    "invalid_numeric": "COMPLEX",
    "mixed_types": "COMPLEX",
    "negative_values": "COMPLEX",
    "out_of_range": "COMPLEX",
    "numeric_outliers_iqr": "COMPLEX",
    "dominant_value_skew": "COMPLEX",
    "name_format_inconsistency": "COMPLEX",
    "systematic_placeholder": "COMPLEX",
    # cannot be auto-repaired without authoritative source
    "duplicate_primary_key": "NOT_FIXABLE",
    "duplicate_rows": "COMPLEX",
    # orphan keys generally require upstream reference data; cannot be deterministically "fixed" in-place
    "orphan_foreign_key_rows": "NOT_FIXABLE",
    "orphan_foreign_key": "NOT_FIXABLE",
    "id_type_drift_across_datasets": "FIXABLE",
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
        lambda v: isinstance(v, str) and v.lower() in PLACEHOLDERS
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
        if str(s.dtype) == "object" and n > 0:
            td = scalar_type_distribution(s)
            pct = (td.get("pct") or {})
            has_str = float(pct.get("str", 0.0)) >= 0.05
            has_num = (float(pct.get("int", 0.0)) + float(pct.get("float", 0.0))) >= 0.05
            if has_str and has_num:
                sev_str = "medium" if (semantic == "numeric_id" or (isinstance(col, str) and col.lower().endswith("id"))) else "low"
                issues.append(dq_issue(
                    sev_str,
                    "mixed_scalar_types",
                    f"Mixed scalar types (str≈{round(100*pct.get('str',0.0),1)}%, num≈{round(100*(pct.get('int',0.0)+pct.get('float',0.0)),1)}%)",
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
    if is_phone_col:
        bad_phone_mask = s_stripped.astype(object).map(
            lambda v: isinstance(v, str) and not PHONE_RE.match(v)
        ) & (~null_like_mask)
        bad_cnt = int(bad_phone_mask.sum())
        if bad_cnt > 0:
            rows = s.index[bad_phone_mask].tolist()
            issues.append(dq_issue("medium", "invalid_phone", f"{bad_cnt} invalid phone(s)",
                                   column=col, count=bad_cnt, rows=rows, sample=list(s[bad_phone_mask].head(5))))

        # Mixed phone formats (e.g., "+91-..." vs digits-only); exclude junk/short from this
        # to avoid double-counting with invalid_phone.
        try:
            def _digits(v: Any) -> str:
                return re.sub(r"\D+", "", v) if isinstance(v, str) else ""

            non_null = s_stripped[~null_like_mask]
            e164ish = non_null.astype(object).map(lambda v: isinstance(v, str) and v.strip().startswith("+") and 10 <= len(_digits(v)) <= 15)
            digits_only = non_null.astype(object).map(lambda v: isinstance(v, str) and v.strip().isdigit() and 10 <= len(v.strip()) <= 15)
            short_or_junk = non_null.astype(object).map(lambda v: isinstance(v, str) and 0 < len(_digits(v)) < 10)
            buckets = {
                "e164ish": int(e164ish.sum()),
                "digits_only_valid_len": int(digits_only.sum()),
                "short_or_junk": int(short_or_junk.sum()),
            }
            nonzero_valid = [k for k in ("e164ish", "digits_only_valid_len") if buckets.get(k, 0) > 0]
            if len(nonzero_valid) >= 2:
                samples = []
                for k in nonzero_valid:
                    mask = {"e164ish": e164ish, "digits_only_valid_len": digits_only, "short_or_junk": short_or_junk}[k]
                    samples.append({k: list(non_null[mask].head(3))})
                issues.append(dq_issue(
                    "medium",
                    "mixed_phone_formats",
                    f"Multiple phone formats detected among valid-length values: {', '.join([f'{k}={buckets[k]}' for k in nonzero_valid])}. Junk/short={buckets.get('short_or_junk',0)}.",
                    column=col,
                    count=sum(buckets[k] for k in nonzero_valid),
                    sample=samples,
                ))
        except Exception:
            pass

    # date
    if semantic == "date":
        parsed = pd.to_datetime(s_stripped, errors="coerce")
        bad_mask = parsed.isna() & (~null_like_mask)
        bad_cnt = int(bad_mask.sum())
        if bad_cnt > 0:
            rows = s.index[bad_mask].tolist()
            sev_str = "medium" if bad_cnt / max(n, 1) <= invalid_date_pct_high else "high"
            issues.append(dq_issue(sev_str, "invalid_date_format", f"{bad_cnt} bad date(s)",
                                   column=col, count=bad_cnt, rows=rows, sample=list(s[bad_mask].head(5))))

    # numeric-like validations
    if (not is_phone_col) and ((semantic in ("numeric_id",)) or (str(s.dtype) != "object") or (
        (str(s.dtype) == "object") and (
            (1.0 - pd.to_numeric(s_stripped, errors="coerce").isna().mean()) > 0.2
        )
    )):
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
        meta = cols_meta.get(col, {})
        semantic = (meta.get("semantic_type") or "unknown").lower()
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
        
        s_str = s_eff.astype(str).str.strip() if str(s_eff.dtype) == "object" else s_eff
        num = pd.to_numeric(s_str, errors="coerce")
        parse_ok = int(num.notna().sum())
        if parse_ok >= max(10, int(0.85 * non_null)):
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
                        f"{oc} row(s) outside 1.5×IQR [{lo:.6g}, {hi:.6g}]",
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
                            f"Skewness ≈ {round(sk, 2)} (heavy tail on one side)",
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
        if semantic == "date" or date_ok >= max(5, int(0.45 * non_null)):
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
        if str(s.dtype) == "object" or semantic in ("email", "free_text", "categorical"):
            try:
                sub = s.dropna()
                if len(sub) > max_heavy:
                    sub = sub.sample(max_heavy, random_state=42)
                
                # Length extremes
                lens = sub.astype(str).str.len()
                if lens.max() >= extreme_len:
                    col_issues.append(dq_issue(
                        "medium", "extremely_long_strings",
                        f"Max length {int(lens.max())} chars (≥{extreme_len})",
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
        if not ca or not cb: continue
        d1 = pd.to_datetime(df[ca], errors="coerce")
        d2 = pd.to_datetime(df[cb], errors="coerce")
        bad = d2.notna() & d1.notna() & (d2 < d1)
        bc = int(bad.sum())
        if bc > 0:
            issues.append(dq_issue(
                "high", "date_range_violation",
                f"{bc} row(s): {cb!r} before {ca!r}",
                count=bc, rows=df.index[bad].tolist()[:50],
                sample=[f"{d1[i]} → {d2[i]}" for i in list(df.index[bad])[:5]],
            ))

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


def analyze_dataset_quality(
    name: str,
    df: pd.DataFrame,
    profile: Dict[str, Any],
    thresholds: Optional[Dict[str, Any]] = None,
    job_id: Optional[str] = None,
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
                               count=dup_rows, rows=rows))

    # Parallelized column-level DQ checks

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
        "cross_dataset_inconsistencies": []
    }

    names = list(datasets.keys())
    for i in range(len(names)):
        df1 = datasets[names[i]]
        for j in range(i + 1, len(names)):
            df2 = datasets[names[j]]
            same_data = _same_dataset_representation(df1, df2)

            common = set(map(str.lower, df1.columns)) & set(map(str.lower, df2.columns))
            for col in common:
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
    gx_enabled: bool = False,
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
    if gx_enabled:
        import logging
        logging.getLogger(__name__).info("Great Expectations (GX) audit layer ENABLED for this run.")

    thresholds = dq_thresholds
    if thresholds is None:
        thresholds = load_dq_thresholds(dq_thresholds_path)

    datasets: Dict[str, pd.DataFrame] = {}
    source_root_by_dataset: Dict[str, str] = {}

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
            for table_key, df in load_sql_datasets(conn, dataset_key_prefix=prefix, max_rows=max_rows).items():
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
        meta["source_root"] = source_root_by_dataset.get(name, "")
        metadata[name] = meta

    per_dataset_dq = {}
    for name, df in datasets.items():
        if job_id:
            from agent.jobs_store import add_event
            add_event(job_id=job_id, level="info", message=f"Analyzing data quality: {name}")
        per_dataset_dq[name] = analyze_dataset_quality(name, df, metadata[name], thresholds, job_id=job_id)
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

    for ds_name, block in per_dataset_dq.items():
        for iss in block.get("issues", []):
            iss.setdefault("dataset", ds_name)
            enrich_issue_with_recommendation(iss)
            enrich_issue_with_fixability(iss)

    # Enrich global/cross-dataset issues
    try:
        for iss in (global_issues.get("relationship_row_issues") or []):
            enrich_issue_with_recommendation(iss)
            enrich_issue_with_fixability(iss)
        for iss in (global_issues.get("relationship_warnings") or []):
            enrich_issue_with_recommendation(iss)
            enrich_issue_with_fixability(iss)
        for iss in (global_issues.get("cross_dataset_consistency") or []):
            enrich_issue_with_recommendation(iss)
            enrich_issue_with_fixability(iss)
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
    if return_datasets:
        out["_datasets"] = datasets

    if gx_enabled:
        out["gx_results"] = {} # Default for UI
        try:
            from agent.specialists.gx_validation_specialist import run_gx_validation
            if job_id:
                from agent.jobs_store import add_event
                add_event(job_id=job_id, level="info", message="Running Great Expectations (GX) deep audit...")
            out["gx_results"] = run_gx_validation(datasets, out)
        except Exception as e:
            if job_id:
                from agent.jobs_store import add_event
                add_event(job_id=job_id, level="error", message=f"GX Audit failed: {e}")
            out["gx_results"] = {"error": str(e), "success": False}

    return out
