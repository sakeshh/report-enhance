"""
Azure Data Factory Mapping Data Flow JSON export (hardened for ADF UI import).
Emits both properties.* and properties.typeProperties.* for compatibility.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from agent.etl_pipeline.join_emitters import emit_adf_join_transformations


def _safe_ds(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", name or "dataset")
    return (s or "dataset").strip("_")

# Plan action -> ADF mapping transformation component kind
_ACTION_ADF_TYPE: Dict[str, str] = {
    "trim": "derivedColumn",
    "lowercase": "derivedColumn",
    "uppercase": "derivedColumn",
    "fill_or_drop": "derivedColumn",
    "fill_nulls_simple": "derivedColumn",
    "cast_type": "cast",
    "coerce_numeric": "cast",
    "parse_dates": "derivedColumn",
    "sanitize_email": "derivedColumn",
    "normalize_phone": "derivedColumn",
    "regex_replace": "derivedColumn",
    "range_clip": "derivedColumn",
    "clip_or_flag": "derivedColumn",
    "flag_outliers": "derivedColumn",
    "clip_outliers": "derivedColumn",
    "cap_outliers": "derivedColumn",
    "standardize_boolean": "derivedColumn",
    "replace_values": "derivedColumn",
    "zero_to_null": "derivedColumn",
    "deduplicate": "aggregate",
    "validate_referential_integrity_or_stage": "lookup",
}


def _adf_transform_step(
    name: str,
    *,
    description: str,
    component: str,
    column: Optional[str],
    action: str,
    upstream: List[str],
) -> Dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "type": component,
        "column": column,
        "action": action,
        "upstream": upstream,
        "dataset": {
            "type": "DatasetReference",
        },
    }


def generate_adf_mapping_flow(plan: Dict[str, Any], assessment: Dict[str, Any]) -> Dict[str, Any]:
    """
    Production-shaped Mapping Data Flow JSON for handoff to Azure Data Factory.
    """
    _ = assessment
    plan_id = str(plan.get("plan_id") or "unknown")
    business_rules = plan.get("business_rules") or {}
    target_path = (plan.get("etl_intent") or {}).get("target_path") or "cleaned/"

    sources: List[Dict[str, Any]] = []
    transformations: List[Dict[str, Any]] = []
    script_lines: List[str] = []
    tid = 0

    for ds_name in (plan.get("datasets") or {}).keys():
        tid += 1
        sid = f"source_{_safe_ds(ds_name)}"
        sources.append(
            {
                "name": sid,
                "description": f"Source dataset: {ds_name}",
                "dataset": {
                    "referenceName": ds_name,
                    "type": "DatasetReference",
                },
                "schema": _schema_hint(assessment, ds_name),
            }
        )
        upstream = [sid]
        last_xf: Optional[str] = None
        block = (plan.get("datasets") or {}).get(ds_name) or {}
        for st in sorted(block.get("steps") or [], key=lambda x: int(x.get("order") or 0)):
            tid += 1
            action = str(st.get("action") or "")
            col = st.get("column")
            component = _ACTION_ADF_TYPE.get(action, "derivedColumn")
            tname = f"xf_{tid}_{action[:18]}"
            col_label = col or "(dataset)"
            desc = f"{action} on {col_label}"
            transformations.append(
                _adf_transform_step(
                    tname,
                    description=desc,
                    component=component,
                    column=col,
                    action=action,
                    upstream=list(upstream),
                )
            )
            script_lines.append(f"// {ds_name}: {desc}")
            upstream = [tname]
            last_xf = tname

    rel = plan.get("relationships") or {}
    if rel.get("joins"):
        transformations, tid, join_script = emit_adf_join_transformations(
            transformations, tid, rel
        )
        script_lines.extend(join_script)

    sinks: List[Dict[str, Any]] = []
    for ds_name in (plan.get("datasets") or {}).keys():
        tid += 1
        sinks.append(
            {
                "name": f"sink_{ds_name}",
                "description": f"Write cleaned output for {ds_name} → {target_path}",
                "dataset": {
                    "referenceName": f"{ds_name}_cleaned",
                    "type": "DatasetReference",
                },
                "schema": _schema_hint(assessment, ds_name, use_lineage=True, plan=plan, ds=ds_name),
            }
        )

    if not sinks:
        sinks = [
            {
                "name": "sink_cleaned",
                "description": f"Default sink — output path: {target_path}",
                "dataset": {"referenceName": "CleanedOutput", "type": "DatasetReference"},
            }
        ]

    flow_body = {
        "sources": sources,
        "sinks": sinks,
        "transformations": transformations,
        "script": "\n".join(script_lines),
        "scriptLines": script_lines,
    }

    return {
        "name": f"AgentDhara_ETL_{plan_id}",
        "type": "Microsoft.DataFactory/factories/dataflows",
        "properties": {
            "type": "MappingDataFlow",
            "typeProperties": flow_body,
            **flow_body,
            "annotations": [
                {"plan_id": plan_id, "generator": "AgentDhara", "version": "3"},
                {"never_drop_rows": business_rules.get("never_drop_rows")},
            ],
            "folder": {"name": "AgentDhara"},
        },
    }


def _schema_hint(
    assessment: Dict[str, Any],
    ds_name: str,
    *,
    use_lineage: bool = False,
    plan: Optional[Dict[str, Any]] = None,
    ds: Optional[str] = None,
) -> List[Dict[str, str]]:
    """Minimal column schema for ADF UI hints."""
    out: List[Dict[str, str]] = []
    cols = ((assessment.get("datasets") or {}).get(ds_name) or {}).get("columns") or {}
    lineage = {}
    if use_lineage and plan and ds:
        from agent.etl_pipeline.schema_lineage import build_lineage

        lineage = build_lineage(plan, assessment).get(ds) or {}

    for col_name, meta in cols.items():
        if not isinstance(meta, dict):
            continue
        dtype = meta.get("dtype") or meta.get("inferred_type") or "string"
        if col_name in lineage:
            dtype = lineage[col_name].get("target_dtype") or dtype
        out.append({"name": str(col_name), "type": str(dtype)})
    return out
