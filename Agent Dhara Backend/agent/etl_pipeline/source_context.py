"""
Derive source_context for ETL engine recommendation from session + assessment.
Supports multi-dataset sessions via `sources` list (connector manifest is authoritative for I/O).
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional


def _ext(name: str) -> str:
    return os.path.splitext(name or "")[1].lower()


def _file_type_from_extension(ext: str) -> str:
    if ext in (".csv", ".tsv"):
        return "csv_file"
    if ext in (".xlsx", ".xls"):
        return "excel"
    if ext in (".json", ".jsonl"):
        return "json"
    if ext == ".parquet":
        return "parquet"
    if ext == ".xml":
        return "xml_file"
    return "csv_file"


def _totals_from_assessment(assessment: Dict[str, Any]) -> tuple[int, float]:
    row_count = 0
    for ds in (assessment.get("datasets") or {}).values():
        if isinstance(ds, dict):
            row_count = max(row_count, int(ds.get("row_count") or 0))
    size_mb = round(row_count * 0.0005, 2) if row_count > 0 else 0.0
    return row_count, size_mb


def _resolve_dataset_source(
    ds_name: str,
    ctx: Dict[str, Any],
    assessment: Dict[str, Any],
    *,
    selected: str,
) -> Dict[str, Any]:
    """Map one assessment dataset name to a source descriptor."""
    tables: List[str] = list(ctx.get("selected_tables") or [])
    blob_files: List[str] = list(ctx.get("selected_blob_files") or [])
    local_files: List[str] = list(ctx.get("selected_local_files") or [])
    local_root = str(ctx.get("local_files_root") or "").strip()
    ext = _ext(ds_name)
    loc = ds_name
    stype = "unknown"

    if ds_name in tables or (tables and ds_name.split(".")[-1] in tables):
        if "azure" in selected:
            stype = "azure_sql"
        elif "postgres" in selected:
            stype = "postgres"
        elif "mysql" in selected:
            stype = "mysql"
        else:
            stype = "sql_server"
        loc = ds_name
    elif ds_name in blob_files:
        stype = "blob_storage"
        loc = ds_name
    elif ds_name in local_files:
        stype = _file_type_from_extension(_ext(ds_name))
        loc = os.path.join(local_root, ds_name) if local_root else ds_name
    elif blob_files and len(blob_files) == len((assessment.get("datasets") or {})):
        ds_names = list((assessment.get("datasets") or {}).keys())
        if ds_name in ds_names:
            idx = ds_names.index(ds_name)
            if idx < len(blob_files):
                stype = "blob_storage"
                loc = blob_files[idx]
    elif local_files and len(local_files) == len((assessment.get("datasets") or {})):
        ds_names = list((assessment.get("datasets") or {}).keys())
        if ds_name in ds_names:
            idx = ds_names.index(ds_name)
            if idx < len(local_files):
                stype = _file_type_from_extension(_ext(local_files[idx]))
                loc = (
                    os.path.join(local_root, local_files[idx])
                    if local_root
                    else local_files[idx]
                )
    else:
        if ext in (".csv", ".tsv", ".parquet", ".json", ".xlsx", ".xls"):
            stype = _file_type_from_extension(ext)
        elif "abfss://" in ds_name.lower():
            stype = "blob_storage"
        else:
            stype = "csv_file"
        loc = ds_name

    ds_meta = (assessment.get("datasets") or {}).get(ds_name) or {}
    return {
        "dataset": ds_name,
        "type": stype,
        "location": loc,
        "extension": ext or _ext(loc),
        "row_count": int(ds_meta.get("row_count") or 0),
    }


def _build_sources_list(
    ctx: Dict[str, Any],
    assessment: Dict[str, Any],
) -> List[Dict[str, Any]]:
    selected = str(ctx.get("selected_source") or "").lower().strip()
    ds_names = list((assessment.get("datasets") or {}).keys())
    return [_resolve_dataset_source(ds, ctx, assessment, selected=selected) for ds in ds_names]


def build_source_context(
    session_context: Optional[Dict[str, Any]],
    assessment: Dict[str, Any],
    override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build source_context for planner / codegen.
    override wins for explicit API fields; session fills gaps.
    Always includes `sources` (per-dataset) when assessment has datasets.
    """
    ctx = session_context or {}
    ovr = override or {}
    sources = _build_sources_list(ctx, assessment)
    row_count, size_mb = _totals_from_assessment(assessment)

    if ovr.get("type"):
        base = dict(ovr)
        base.setdefault("row_count", row_count)
        base.setdefault("size_mb", size_mb)
        if not base.get("extension") and base.get("location"):
            base["extension"] = _ext(str(base["location"]))
        if sources:
            base["sources"] = sources
            base["source_count"] = len(sources)
            base["is_multi_source"] = len(sources) > 1
        return base

    if sources:
        primary = sources[0]
        types = {s["type"] for s in sources}
        mix = "mixed" if len(types) > 1 else (primary["type"] if primary else "unknown")
        return {
            "type": primary["type"],
            "location": primary["location"],
            "size_mb": size_mb,
            "row_count": row_count,
            "extension": primary.get("extension") or "",
            "sources": sources,
            "source_count": len(sources),
            "is_multi_source": len(sources) > 1,
            "source_mix": mix,
        }

    return {
        "type": "unknown",
        "location": "unknown",
        "size_mb": size_mb,
        "row_count": row_count,
        "extension": "",
        "sources": [],
        "source_count": 0,
        "is_multi_source": False,
    }
