from __future__ import annotations

from typing import Any, Dict, List


def generate_adf_mapping_flow(plan: Dict[str, Any], assessment: Dict[str, Any]) -> Dict[str, Any]:
    """
    Minimal Mapping Data Flow–shaped JSON for handoff to Azure Data Factory.
    Not a full ADF export; enough structure to import/extend in ADF UI.
    """
    _ = assessment
    plan_id = str(plan.get("plan_id") or "unknown")
    sources: List[Dict[str, Any]] = []
    transforms: List[Dict[str, Any]] = []
    tid = 0

    for ds_name in (plan.get("datasets") or {}).keys():
        tid += 1
        sid = f"source_{tid}"
        sources.append(
            {
                "name": sid,
                "description": f"Source for {ds_name}",
                "dataset": {"referenceName": ds_name, "type": "DatasetReference"},
            }
        )
        prev = sid
        block = (plan.get("datasets") or {}).get(ds_name) or {}
        for st in sorted(block.get("steps") or [], key=lambda x: int(x.get("order") or 0)):
            tid += 1
            tname = f"xf_{tid}_{str(st.get('action'))[:20]}"
            transforms.append(
                {
                    "name": tname,
                    "description": f"{st.get('action')} on {st.get('column')}",
                    "type": "DerivedColumn" if st.get("column") else "Aggregate",
                    "dependsOn": [{"activity": prev, "dependencyConditions": ["Succeeded"]}],
                    "column": st.get("column"),
                    "action": st.get("action"),
                }
            )
            prev = tname

    last_activity = transforms[-1]["name"] if transforms else (sources[-1]["name"] if sources else None)
    sink_dep = (
        [{"activity": last_activity, "dependencyConditions": ["Succeeded"]}]
        if last_activity
        else []
    )
    return {
        "name": f"AgentDhara_ETL_{plan_id}",
        "type": "Microsoft.DataFactory/factories/dataflows",
        "properties": {
            "type": "MappingDataFlow",
            "sources": sources,
            "transformations": transforms,
            "sinks": [
                {
                    "name": "sink_cleaned",
                    "description": "Replace with your sink dataset",
                    "dependsOn": sink_dep,
                }
            ],
            "annotations": [{"plan_id": plan_id, "generator": "AgentDhara"}],
        },
    }
