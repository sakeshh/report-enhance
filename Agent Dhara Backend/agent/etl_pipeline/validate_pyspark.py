"""
PySpark ETL validation: syntax + no pandas + plan column references + I/O sanity.
"""
from __future__ import annotations

import ast
import re
from typing import Any, Dict, List, Set, Tuple

from agent.etl_pipeline.validate_python import validate_etl_python_source


def _plan_columns(plan: Dict[str, Any]) -> Set[str]:
    cols: Set[str] = set()
    for block in (plan.get("datasets") or {}).values():
        for st in (block or {}).get("steps") or []:
            c = st.get("column")
            if c:
                cols.add(str(c))
    return cols


def _check_spark_session_import(source: str, tree: ast.AST) -> List[str]:
    errs: List[str] = []
    uses_session = "SparkSession" in source and (
        "SparkSession.builder" in source or "SparkSession(" in source
    )
    if not uses_session:
        return errs
    imported = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("pyspark.sql"):
            for n in node.names or []:
                if n.name == "SparkSession":
                    imported = True
        elif isinstance(node, ast.Import):
            for n in node.names or []:
                if (n.name or "").endswith("SparkSession") or n.name == "pyspark.sql":
                    imported = True
    if not imported:
        errs.append(
            "SparkSession is used but not imported — add: from pyspark.sql import SparkSession"
        )
    return errs


def _check_never_drop_rows(source: str, plan: Dict[str, Any] | None) -> List[str]:
    if not plan:
        return []
    rules = plan.get("business_rules") or {}
    if not rules.get("never_drop_rows"):
        return []
    errs: List[str] = []
    low = source.lower()
    if re.search(r'how\s*=\s*["\']inner["\']', low) or re.search(
        r'\.join\s*\([^)]*how\s*=\s*["\']inner["\']', low, re.I
    ):
        errs.append(
            "never_drop_rows: do not use inner join — write per-dataset outputs or use left join only"
        )
    if re.search(r"\.dropna\s*\(\s*\)", source) or re.search(
        r"\.dropna\s*\(\s*subset\s*=", source
    ):
        errs.append("never_drop_rows: do not use dropna() — use fill/coalesce instead")
    if re.search(r"\.filter\s*\(\s*F\.col", source) and "isNotNull" in source:
        pass  # allow null checks on columns without dropping all rows — hard to prove
    return errs


def _check_resolve_helper_defined(source: str) -> List[str]:
    if "_resolve_data_path(" not in source:
        return []
    if re.search(r"def\s+_resolve_data_path\s*\(", source):
        return _check_resolve_helper_quality(source)
    return [
        "_resolve_data_path() is used but not defined — include the connector_manifest "
        "_resolve_data_path helper or use full abfss:// paths"
    ]


def _check_resolve_helper_quality(source: str) -> List[str]:
    """Reject stub helpers that cannot resolve Azure blob paths."""
    errs: List[str] = []
    if re.search(
        r'return\s+f?["\']abfss://\{location\}["\']',
        source,
        re.I,
    ) or re.search(r'return\s+f?["\']abfss://\{loc\}["\']', source, re.I):
        errs.append(
            "Incomplete _resolve_data_path: must use AZURE_STORAGE_ACCOUNT + "
            "DHARA_BLOB_CONTAINER (abfss://container@account.dfs.core.windows.net/...) "
            "or DHARA_BLOB_BASE_PATH — not abfss://{location} only"
        )
    if "_resolve_data_path" in source and "def _resolve_data_path" in source:
        body = source.split("def _resolve_data_path", 1)[-1][:800]
        if "AZURE_STORAGE_ACCOUNT" not in body and "dfs.core.windows.net" not in body:
            if re.search(r"abfss://", body, re.I):
                errs.append(
                    "_resolve_data_path must build full abfss URLs with storage account and container"
                )
    return errs


def _check_dead_join_variables(source: str) -> List[str]:
    """Flag join results that are assigned but never written or returned."""
    errs: List[str] = []
    for m in re.finditer(r"(\w+)\s*=\s*[^=\n]*?\.join\s*\(", source):
        var = m.group(1)
        if var in ("dfs", "spark", "df"):
            continue
        uses = len(re.findall(rf"\b{re.escape(var)}\b", source))
        if uses <= 1:
            errs.append(
                f"Dead join: `{var}` is assigned from .join() but never written or returned — "
                "write it (e.g. joined_*.parquet) or remove the join"
            )
    return errs


def _check_io_antipatterns(source: str, plan: Dict[str, Any] | None) -> List[str]:
    errs: List[str] = []
    low = source.lower()

    # .xml read as csv
    if re.search(r'read\.csv\s*\([^)]*\.xml', low, re.I) or re.search(
        r'\.csv\s*\(\s*r?["\'][^"\']*\.xml', low, re.I
    ):
        errs.append("Do not use read.csv for .xml files — use spark-xml or manifest read_snippet_pyspark")

    if re.search(r'write\.csv\s*\([^)]*\.xml', low, re.I):
        errs.append("Do not use write.csv for .xml output paths — use parquet/json per manifest")

    manifest = (plan or {}).get("connector_manifest") or {}
    m_ds = manifest.get("datasets") or {}
    for ds_name, ent in m_ds.items():
        if not isinstance(ent, dict):
            continue
        if ent.get("format") == "xml" and "read.csv" in source and ds_name in source:
            errs.append(f"Dataset '{ds_name}' is XML — remove read.csv usage for this dataset")

    # Bare filename loads without resolve helper when manifest has blob sources
    if m_ds and any(
        isinstance(e, dict) and e.get("source_type") == "blob_storage" for e in m_ds.values()
    ):
        if "_resolve_data_path" not in source and not re.search(
            r"abfss://|wasbs://", source, re.I
        ):
            if re.search(r'read\.(json|csv)\s*\(\s*r?["\'][\w./-]+\.(json|csv|xml)', source, re.I):
                errs.append(
                    "Blob sources detected — use _resolve_data_path(manifest location) "
                    "or abfss:// paths from connector_manifest, not bare filenames"
                )

    return errs


def validate_pyspark_source(
    source: str,
    plan: Dict[str, Any] | None = None,
) -> Tuple[bool, List[str]]:
    errs: List[str] = []
    if not source or not source.strip():
        return False, ["empty source"]

    try:
        ast.parse(source)
    except SyntaxError as e:
        return False, [f"syntax: {e.msg} at line {e.lineno}"]

    ok_py, py_errs = validate_etl_python_source(source)
    if not ok_py:
        errs.extend(
            e
            for e in py_errs
            if not e.startswith("disallowed import: os")
            and "disallowed import from 'os'" not in e
        )

    low = source.lower()
    if re.search(r"\bimport\s+pandas\b", low) or re.search(r"\bfrom\s+pandas\b", low):
        errs.append("PySpark script must not import pandas")
    if "pd." in source and "pyspark" not in low:
        errs.append("pandas-style pd.* usage detected — use pyspark.sql.functions")

    has_spark = False
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names or []:
                if (n.name or "").startswith("pyspark"):
                    has_spark = True
        elif isinstance(node, ast.ImportFrom):
            if (node.module or "").startswith("pyspark"):
                has_spark = True

    if not has_spark and "SparkSession" not in source:
        errs.append("expected pyspark import or SparkSession usage")

    errs.extend(_check_resolve_helper_defined(source))
    errs.extend(_check_dead_join_variables(source))
    errs.extend(_check_io_antipatterns(source, plan))
    errs.extend(_check_spark_session_import(source, tree))
    errs.extend(_check_never_drop_rows(source, plan))

    if plan:
        allowed = _plan_columns(plan)
        if allowed:
            quoted = set(re.findall(r"['\"]([a-zA-Z_][\w]*)['\"]", source))
            suspicious = [c for c in quoted if c.endswith("_outlier_flagged")]
            for c in suspicious:
                base = c.replace("_outlier_flagged", "")
                if base not in allowed and c not in allowed:
                    errs.append(f"column '{c}' not in plan — verify spelling")

    if errs:
        return False, errs
    return True, []
