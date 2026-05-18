"""
Azure Data Factory Mapping Data Flow JSON export (hardened for ADF UI import).
"""
from __future__ import annotations

import copy
import re
from typing import Any, Dict, List, Optional

from agent.etl_pipeline.adf_expressions import adf_expression_for_step
from agent.etl_pipeline.codegen_shared import step_params
from agent.etl_pipeline.join_emitters import emit_adf_join_transformations


def _safe_ds(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", name or "dataset")
    return (s or "dataset").strip("_")


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
    "drop_column": "select",
    "exclude_column": "select",
    "validate_referential_integrity_or_stage": "conditionalSplit",
}


def _adf_derived_transform(
    name: str,
    *,
    upstream: List[str],
    columns: List[Dict[str, str]],
    action: str,
    description: str,
) -> Dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "type": "derivedColumn",
        "action": action,
        "upstream": list(upstream),
        "typeProperties": {
            "columns": [
                {
                    "name": c["targetColumn"],
                    "expression": c["expression"],
                }
                for c in columns
            ],
        },
        "columns": columns,
        "expression": "; ".join(f"{c['targetColumn']}={c['expression']}" for c in columns),
    }


def _build_flow_body(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    *,
    include_joins: bool,
    target_path: str,
) -> Dict[str, Any]:
    sources: List[Dict[str, Any]] = []
    transformations: List[Dict[str, Any]] = []
    script_lines: List[str] = []
    tid = 0
    stream_upstream: Dict[str, str] = {}

    for ds_name in (plan.get("datasets") or {}).keys():
        tid += 1
        sid = f"source_{_safe_ds(ds_name)}"
        stream_upstream[ds_name] = sid
        sources.append(
            {
                "name": sid,
                "description": f"Source: {ds_name}",
                "dataset": {
                    "referenceName": f"DS_{_safe_ds(ds_name)}",
                    "type": "DatasetReference",
                    "linkedServiceName": {"referenceName": "LS_AzureBlob", "type": "LinkedServiceReference"},
                },
                "schema": _schema_hint(assessment, ds_name),
            }
        )
        upstream = [sid]
        block = (plan.get("datasets") or {}).get(ds_name) or {}
        for st in sorted(block.get("steps") or [], key=lambda x: int(x.get("order") or 0)):
            action = str(st.get("action") or "")
            col = st.get("column")
            if action in ("drop_column", "exclude_column"):
                continue
            cols_exprs = adf_expression_for_step(action, col, st)
            if not cols_exprs:
                continue
            tid += 1
            tname = f"derive_{tid}_{_safe_ds(ds_name)}_{action[:12]}"
            desc = f"{ds_name}: {action} on {col or 'dataset'}"
            transformations.append(
                _adf_derived_transform(
                    tname,
                    upstream=upstream,
                    columns=cols_exprs,
                    action=action,
                    description=desc,
                )
            )
            for ce in cols_exprs:
                script_lines.append(f"// {desc} => {ce['targetColumn']} = {ce['expression']}")
            upstream = [tname]
        stream_upstream[ds_name] = upstream[-1] if upstream else sid

    rel = plan.get("relationships") or {}
    if include_joins and rel.get("joins"):
        transformations, tid, join_script = emit_adf_join_transformations(
            transformations, tid, rel, stream_upstream=stream_upstream
        )
        script_lines.extend(join_script)

    sinks: List[Dict[str, Any]] = []
    if include_joins and rel.get("joins"):
        for j in rel.get("joins") or []:
            if j.get("join_type") == "review":
                continue
            p, c = j.get("parent_dataset"), j.get("child_dataset")
            jname = f"joined_{_safe_ds(p)}_{_safe_ds(c)}"
            sinks.append(
                {
                    "name": f"sink_{jname}",
                    "description": f"Joined output {jname}",
                    "dataset": {
                        "referenceName": f"DS_{jname}_cleaned",
                        "type": "DatasetReference",
                        "linkedServiceName": {"referenceName": "LS_AzureBlob", "type": "LinkedServiceReference"},
                    },
                    "dependsOn": [f"join_{_safe_ds(p)}_{_safe_ds(c)}"],
                }
            )
    for ds_name in (plan.get("datasets") or {}).keys():
        tid += 1
        last = stream_upstream.get(ds_name, f"source_{_safe_ds(ds_name)}")
        sinks.append(
            {
                "name": f"sink_{_safe_ds(ds_name)}",
                "description": f"Cleaned {ds_name} -> {target_path}",
                "dataset": {
                    "referenceName": f"DS_{_safe_ds(ds_name)}_cleaned",
                    "type": "DatasetReference",
                    "linkedServiceName": {"referenceName": "LS_AzureBlob", "type": "LinkedServiceReference"},
                },
                "dependsOn": [last],
                "schema": _schema_hint(assessment, ds_name, use_lineage=True, plan=plan, ds=ds_name),
            }
        )

    if not sinks:
        sinks = [
            {
                "name": "sink_cleaned",
                "dataset": {"referenceName": "CleanedOutput", "type": "DatasetReference"},
            }
        ]

    return {
        "sources": sources,
        "sinks": sinks,
        "transformations": transformations,
        "script": "\n".join(script_lines),
        "scriptLines": script_lines,
    }


def _wrap_mapping_dataflow(
    name: str,
    flow_body: Dict[str, Any],
    *,
    plan_id: str,
    business_rules: Dict[str, Any],
    role: str,
) -> Dict[str, Any]:
    return {
        "name": name,
        "role": role,
        "type": "Microsoft.DataFactory/factories/dataflows",
        "properties": {
            "type": "MappingDataFlow",
            "description": f"Agent Dhara ETL ({role})",
            "typeProperties": flow_body,
            **flow_body,
            "annotations": [
                {"plan_id": plan_id, "generator": "AgentDhara", "version": "4", "role": role},
                {"never_drop_rows": business_rules.get("never_drop_rows")},
            ],
            "folder": {"name": "AgentDhara"},
        },
    }


def generate_adf_mapping_flow(plan: Dict[str, Any], assessment: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns primary Mapping Data Flow (clean_only) plus bundle.flows when joins exist.
    Top-level keys remain backward-compatible (name, type, properties = primary flow).
    """
    plan_id = str(plan.get("plan_id") or "unknown")
    business_rules = plan.get("business_rules") or {}
    target_path = (plan.get("etl_intent") or {}).get("target_path") or "cleaned/"
    rel = plan.get("relationships") or {}

    clean_body = _build_flow_body(
        plan, assessment, include_joins=False, target_path=target_path
    )
    primary = _wrap_mapping_dataflow(
        f"AgentDhara_ETL_{plan_id}_clean",
        clean_body,
        plan_id=plan_id,
        business_rules=business_rules,
        role="clean_only",
    )

    flows: List[Dict[str, Any]] = [copy.deepcopy(primary)]
    if int(rel.get("join_count") or 0) > 0 or (rel.get("joins")):
        joined_body = _build_flow_body(
            plan, assessment, include_joins=True, target_path=target_path
        )
        joined = _wrap_mapping_dataflow(
            f"AgentDhara_ETL_{plan_id}_joined",
            joined_body,
            plan_id=plan_id,
            business_rules=business_rules,
            role="clean_and_joined",
        )
        flows.append(joined)

    result = {
        **primary,
        "bundle": {
            "plan_id": plan_id,
            "flows": flows,
        },
    }
    return result


def _schema_hint(
    assessment: Dict[str, Any],
    ds_name: str,
    *,
    use_lineage: bool = False,
    plan: Optional[Dict[str, Any]] = None,
    ds: Optional[str] = None,
) -> List[Dict[str, str]]:
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
