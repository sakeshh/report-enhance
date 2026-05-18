"""
Shared I/O path resolution and read/write snippet builders for ETL codegen.
"""
from __future__ import annotations

import os
import re
from typing import Any, Dict


def _escape_path(p: str) -> str:
    return str(p or "").replace("\\", "\\\\").replace('"', '\\"')


def resolve_path_python_helper() -> str:
    """Python helper emitted once at top of generated scripts using blob paths."""
    return '''
def _resolve_data_path(location: str) -> str:
    """Resolve blob/SQL/file path from connector manifest location."""
    import os
    loc = (location or "").strip()
    if not loc or loc == "unknown":
        raise ValueError("connector_manifest location is missing")
    low = loc.lower()
    if low.startswith(("abfss://", "wasbs://", "https://", "http://", "s3://")):
        return loc
    base = os.environ.get("DHARA_BLOB_BASE_PATH") or os.environ.get("DHARA_BLOB_MOUNT") or "."
    account = os.environ.get("AZURE_STORAGE_ACCOUNT", "").strip()
    container = os.environ.get("DHARA_BLOB_CONTAINER", "").strip()
    if account and container and not os.path.isabs(loc):
        return f"abfss://{container}@{account}.dfs.core.windows.net/{loc.lstrip('/')}"
    return os.path.join(base, loc) if not os.path.isabs(loc) else loc
'''.strip()


def resolve_path_pyspark_helper() -> str:
    return '''
def _resolve_data_path(location: str) -> str:
    import os
    loc = (location or "").strip()
    if not loc or loc == "unknown":
        raise ValueError("connector_manifest location is missing")
    low = loc.lower()
    if low.startswith(("abfss://", "wasbs://", "https://", "http://")):
        return loc
    base = os.environ.get("DHARA_BLOB_BASE_PATH") or os.environ.get("DHARA_BLOB_MOUNT") or "."
    account = os.environ.get("AZURE_STORAGE_ACCOUNT", "").strip()
    container = os.environ.get("DHARA_BLOB_CONTAINER", "").strip()
    if account and container and not os.path.isabs(loc):
        return f"abfss://{container}@{account}.dfs.core.windows.net/{loc.lstrip('/')}"
    return os.path.join(base, loc) if not os.path.isabs(loc) else loc
'''.strip()


def infer_format_from_ext(ext: str, source_type: str) -> str:
    ext = (ext or "").lower()
    if ext in (".csv",):
        return "csv"
    if ext in (".tsv",):
        return "tsv"
    if ext in (".parquet",):
        return "parquet"
    if ext in (".json", ".jsonl"):
        return "json"
    if ext in (".xml",):
        return "xml"
    if ext in (".xlsx", ".xls"):
        return "excel"
    if source_type in ("sql_server", "azure_sql", "postgres", "mysql"):
        return "sql_table"
    return "csv"


def output_extension_for_format(fmt: str, fallback_ext: str) -> str:
    mapping = {
        "json": ".json",
        "xml": ".parquet",
        "csv": ".csv",
        "tsv": ".tsv",
        "parquet": ".parquet",
        "excel": ".parquet",
    }
    if fmt == "xml":
        return ".parquet"
    return mapping.get(fmt, fallback_ext or ".parquet")


def python_read_snippet(entry: Dict[str, Any]) -> str:
    loc = _escape_path(entry["location"])
    fmt = entry.get("format") or "csv"
    if fmt == "sql_table":
        cref = entry.get("connection_ref") or "DHARA_SQL_CONNECTION_STRING"
        table = entry["location"]
        return (
            f'pd.read_sql("SELECT * FROM {table}", '
            f'create_engine(os.environ["{cref}"]))'
        )
    path_expr = f'_resolve_data_path("{loc}")'
    if fmt == "parquet":
        return f"pd.read_parquet({path_expr})"
    if fmt == "excel":
        return f"pd.read_excel({path_expr}, sheet_name=0)"
    if fmt == "json":
        return f"pd.read_json({path_expr})"
    if fmt == "xml":
        return (
            f"pd.read_xml({path_expr}, parser='lxml') "
            f"if 'read_xml' in dir(pd) else pd.read_xml({path_expr})"
        )
    if fmt == "tsv":
        return f"pd.read_csv({path_expr}, sep='\\t')"
    return f"pd.read_csv({path_expr})"


def python_write_snippet(entry: Dict[str, Any]) -> str:
    fmt = entry.get("format") or "csv"
    op = _escape_path(entry.get("output_path") or "cleaned/out.parquet")
    path_expr = f'r"{op}"'
    if fmt == "json":
        return f"df.to_json({path_expr}, orient='records', lines=True, index=False)"
    if fmt in ("xml", "parquet"):
        return f"df.to_parquet({path_expr}, index=False)"
    if fmt == "excel":
        return f"df.to_excel({path_expr}, index=False)"
    return f"df.to_csv({path_expr}, index=False)"


def pyspark_read_snippet(entry: Dict[str, Any]) -> str:
    loc = _escape_path(entry["location"])
    fmt = entry.get("format") or "csv"
    if fmt == "sql_table":
        cref = entry.get("connection_ref") or "DHARA_SQL_CONNECTION_STRING"
        table = entry["location"]
        return (
            'spark.read.format("jdbc").option("url", os.environ["'
            + cref
            + f'"]).option("dbtable", "{table}").load()'
        )
    path_expr = f'_resolve_data_path("{loc}")'
    if fmt == "parquet":
        return f"spark.read.parquet({path_expr})"
    if fmt == "json":
        return f"spark.read.json({path_expr})"
    if fmt == "xml":
        return (
            'spark.read.format("com.databricks.spark.xml")'
            f'.option("rowTag", "row").load({path_expr})  '
            f"# requires spark-xml / Maven: com.databricks:spark-xml_2.12"
        )
    return (
        f"spark.read.option('header', 'true').option('inferSchema', 'true').csv({path_expr})"
    )


def pyspark_write_snippet(entry: Dict[str, Any]) -> str:
    fmt = entry.get("format") or "csv"
    op = entry.get("output_path") or "cleaned/out.parquet"
    if fmt == "xml" and str(op).lower().endswith(".xml"):
        op = str(op)[:-4] + ".parquet"
    op_esc = _escape_path(op)
    path_expr = f'r"{op_esc}"'
    if fmt == "json":
        return f'df.write.mode("overwrite").json({path_expr})'
    if fmt in ("parquet", "xml"):
        return f'df.write.mode("overwrite").parquet({path_expr})'
    return f'df.write.mode("overwrite").option("header", "true").csv({path_expr})'


def pyspark_iqr_bounds_helper() -> str:
    return '''
def _iqr_bounds(df, col: str, multiplier: float = 1.5):
    """Return (stats_row, iqr, lower, upper) for outlier transforms."""
    row = df.select(
        F.percentile_approx(F.col(col), 0.25).alias("q1"),
        F.percentile_approx(F.col(col), 0.75).alias("q3"),
        F.percentile_approx(F.col(col), 0.50).alias("median"),
    ).first()
    iqr = float(row["q3"] - row["q1"])
    lower = float(row["q1"] - multiplier * iqr)
    upper = float(row["q3"] + multiplier * iqr)
    return row, iqr, lower, upper
'''.strip()


def pyspark_production_helpers() -> str:
    return '''
def _require_columns(df, required: list, label: str) -> None:
  """Fail fast if required columns are missing."""
  missing = [c for c in required if c not in df.columns]
  if missing:
    raise ValueError(f"{label}: missing required columns: {missing}")


def _warn_duplicate_keys(df, key_col: str, label: str) -> None:
  """Log possible duplicate join keys (single scan — approx distinct, no full shuffle)."""
  import logging
  import os
  if key_col not in df.columns:
    return
  if os.environ.get("DHARA_ETL_CHECK_DUP_KEYS", "1").strip().lower() in ("0", "false", "no"):
    return
  row = (
    df.agg(
      F.count(F.col(key_col)).alias("_n"),
      F.approx_count_distinct(F.col(key_col)).alias("_d"),
    )
    .first()
  )
  if not row:
    return
  n, d = int(row["_n"] or 0), int(row["_d"] or 0)
  if n > 0 and d > 0 and n > d:
    logging.getLogger("agent_dhara").warning(
      "%s: ~%d possible duplicate key value(s) on %s (approx_count_distinct)",
      label,
      n - d,
      key_col,
    )


def _warn_nulls_in_columns(df, columns: list, label: str) -> None:
  """Cheap null probe: limit(1) per column — does not scan full table."""
  import logging
  for col in columns or []:
    if col not in df.columns:
      continue
    if df.filter(F.col(col).isNull()).limit(1).count() > 0:
      logging.getLogger("agent_dhara").warning(
        "%s: column %s has null values after transform", label, col
      )


def _log_row_count(df, label: str) -> None:
  """Optional row count log (set DHARA_ETL_LOG_ROW_COUNTS=1)."""
  import logging
  import os
  if os.environ.get("DHARA_ETL_LOG_ROW_COUNTS", "0").strip().lower() not in ("1", "true", "yes"):
    return
  logging.getLogger("agent_dhara").info("%s: row_count=%s", label, df.count())
'''.strip()


def pyspark_prefix_non_key_columns_helper() -> str:
    return '''
def _prefix_columns(df, prefix: str, except_cols: list):
    """Prefix right-side columns before join to avoid duplicate names."""
    for c in df.columns:
        if c not in except_cols:
            df = df.withColumnRenamed(c, f"{prefix}_{c}")
    return df
'''.strip()
