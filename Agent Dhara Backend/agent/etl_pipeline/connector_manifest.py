"""
Per-dataset I/O manifest for production-shaped ETL codegen.
Built from session context + assessment dataset names.
"""
from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional

from agent.etl_pipeline.io_snippets import (
    infer_format_from_ext,
    output_extension_for_format,
    python_read_snippet,
    python_write_snippet,
    pyspark_read_snippet,
    pyspark_write_snippet,
)


def _ext(name: str) -> str:
    return os.path.splitext(name or "")[1].lower()


def _safe_var(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", name or "dataset")
    if s and s[0].isdigit():
        s = "ds_" + s
    return s or "dataset"


def build_connector_manifest(
    session_context: Optional[Dict[str, Any]],
    assessment: Dict[str, Any],
    *,
    output_base: str = "cleaned/",
    overwrite_in_place: bool = False,
) -> Dict[str, Any]:
    """
    Returns {
      version, datasets: { name: { source_type, location, format, read_hint, write_hint, ... } },
      connection_refs: { sql: env var names, blob: ... }
    }
    """
    ctx = session_context or {}
    ds_names = list((assessment.get("datasets") or {}).keys())
    selected = str(ctx.get("selected_source") or "").lower().strip()
    tables: List[str] = list(ctx.get("selected_tables") or [])
    blob_files: List[str] = list(ctx.get("selected_blob_files") or [])
    local_files: List[str] = list(ctx.get("selected_local_files") or [])
    local_root = str(ctx.get("local_files_root") or "").strip()

    default_sql_type = "azure_sql" if "azure" in selected else (
        "postgres" if "postgres" in selected else "sql_server"
    )

    datasets: Dict[str, Any] = {}
    for ds_name in ds_names:
        loc = ds_name
        source_type = "unknown"
        connection_ref: Optional[str] = None
        ext = _ext(ds_name)

        if ds_name in tables or (tables and ds_name.split(".")[-1] in tables):
            source_type = default_sql_type
            loc = ds_name if ds_name in tables else ds_name
            connection_ref = "DHARA_SQL_CONNECTION_STRING"
        elif ds_name in blob_files:
            source_type = "blob_storage"
            loc = ds_name
            connection_ref = "AZURE_STORAGE_CONNECTION_STRING"
        elif ds_name in local_files:
            source_type = _file_type_from_extension_local(ext)
            loc = os.path.join(local_root, ds_name) if local_root else ds_name
        elif blob_files and len(blob_files) == len(ds_names):
            idx = ds_names.index(ds_name)
            if idx < len(blob_files):
                source_type = "blob_storage"
                loc = blob_files[idx]
                connection_ref = "AZURE_STORAGE_CONNECTION_STRING"
        elif local_files and len(local_files) == len(ds_names):
            idx = ds_names.index(ds_name)
            if idx < len(local_files):
                loc = (
                    os.path.join(local_root, local_files[idx])
                    if local_root
                    else local_files[idx]
                )
                source_type = _file_type_from_extension_local(_ext(local_files[idx]))
        else:
            if ext in (".csv", ".tsv", ".parquet", ".json", ".jsonl", ".xml", ".xlsx", ".xls"):
                source_type = _file_type_from_extension_local(ext)
            elif "abfss://" in ds_name.lower() or "wasbs://" in ds_name.lower():
                source_type = "blob_storage"
                connection_ref = "AZURE_STORAGE_CONNECTION_STRING"
                loc = ds_name
            else:
                source_type = "csv_file"

        fmt = infer_format_from_ext(ext if ext else _ext(loc), source_type)
        out_ext = output_extension_for_format(fmt, ext or ".parquet")
        if overwrite_in_place or output_base == "__overwrite__":
            out_path = loc
        else:
            base = output_base.rstrip("/") or "cleaned"
            out_path = f"{base}/{_safe_var(ds_name)}_cleaned{out_ext}"

        entry = {
            "dataset": ds_name,
            "source_type": source_type,
            "location": loc,
            "format": fmt,
            "extension": ext or _ext(loc),
            "connection_ref": connection_ref,
            "output_path": out_path,
            "row_count": int(
                ((assessment.get("datasets") or {}).get(ds_name) or {}).get("row_count") or 0
            ),
        }
        entry["read_snippet_python"] = python_read_snippet(entry)
        entry["read_snippet_pyspark"] = pyspark_read_snippet(entry)
        entry["write_snippet_python"] = python_write_snippet(entry)
        entry["write_snippet_pyspark"] = pyspark_write_snippet(entry)
        entry["read_snippet_sql"] = _sql_read_snippet(entry)
        datasets[ds_name] = entry

    return {
        "version": 1,
        "datasets": datasets,
        "connection_refs": {
            "sql": "DHARA_SQL_CONNECTION_STRING",
            "azure_sql": "DHARA_SQL_CONNECTION_STRING",
            "blob": "AZURE_STORAGE_CONNECTION_STRING",
            "postgres": "DHARA_POSTGRES_CONNECTION_STRING",
        },
        "output_base": output_base,
    }


def _file_type_from_extension_local(ext: str) -> str:
    if ext in (".xlsx", ".xls"):
        return "excel"
    if ext == ".parquet":
        return "parquet"
    if ext in (".json", ".jsonl"):
        return "json"
    if ext == ".xml":
        return "xml_file"
    return "csv_file"


def _sql_read_snippet(entry: Dict[str, Any]) -> str:
    loc = entry["location"]
    fmt = entry.get("format") or "csv"
    if fmt == "sql_table":
        return f"-- Source table/view: {loc}\nSELECT * FROM {loc};"
    return f"-- Load from file staging table stg_{_safe_var(entry['dataset'])} (populate from {loc})"


def validate_connector_manifest(
    plan: Dict[str, Any],
    manifest: Dict[str, Any],
) -> List[str]:
    """Return errors if plan datasets lack manifest entries or joins reference unknown datasets."""
    errs: List[str] = []
    m_ds = (manifest or {}).get("datasets") or {}
    plan_ds = set((plan.get("datasets") or {}).keys())
    for name in plan_ds:
        if name not in m_ds:
            errs.append(f"connector_manifest: missing entry for dataset '{name}'")
        else:
            ent = m_ds[name]
            loc = str(ent.get("location") or "")
            if not loc or loc == "unknown":
                errs.append(f"connector_manifest: dataset '{name}' has no resolved location")
            fmt = ent.get("format")
            if fmt == "xml" and "read.csv" in str(ent.get("read_snippet_pyspark") or ""):
                errs.append(f"connector_manifest: dataset '{name}' XML must not use read.csv")
    rel = plan.get("relationships") or {}
    for j in rel.get("joins") or []:
        for side in ("left_dataset", "right_dataset", "parent_dataset", "child_dataset"):
            d = j.get(side)
            if d and d not in plan_ds and d not in m_ds:
                errs.append(f"join references unknown dataset '{d}'")
    return errs
