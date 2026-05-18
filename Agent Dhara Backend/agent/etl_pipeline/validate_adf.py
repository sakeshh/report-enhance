from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple

_REQUIRED_FLOW_KEYS = {"sources", "sinks", "transformations"}


def _flow_section(props: Dict[str, Any]) -> Dict[str, Any]:
    """Agent Dhara template stores flow under properties; ADF export may use typeProperties."""
    if not isinstance(props, dict):
        return {}
    tp = props.get("typeProperties")
    if isinstance(tp, dict) and tp:
        return tp
    return props


def validate_adf_json(obj: Any) -> Tuple[bool, List[str]]:
    errs: List[str] = []

    if not isinstance(obj, dict):
        return False, ["ADF payload must be a JSON object"]

    if "name" not in obj:
        errs.append("ADF JSON missing required 'name' field")

    props = obj.get("properties")
    if props is None:
        errs.append("ADF JSON missing 'properties' object")
        return False, errs

    if not isinstance(props, dict):
        errs.append("'properties' must be an object")
        return False, errs

    if props.get("type") != "MappingDataFlow":
        errs.append(
            f"expected properties.type 'MappingDataFlow', got '{props.get('type')}'"
        )

    section = _flow_section(props)
    missing: Set[str] = _REQUIRED_FLOW_KEYS - set(section.keys())
    if missing:
        errs.append(
            f"ADF MappingDataFlow missing required fields: {sorted(missing)}"
        )

    for key in _REQUIRED_FLOW_KEYS:
        val = section.get(key)
        if val is not None and not isinstance(val, list):
            errs.append(f"'{key}' must be an array")

    sources = section.get("sources") or []
    if isinstance(sources, list):
        for i, src in enumerate(sources):
            if not isinstance(src, dict):
                errs.append(f"sources[{i}] must be an object")
                continue
            if not src.get("name"):
                errs.append(f"sources[{i}] missing name")
            ds = src.get("dataset")
            if not isinstance(ds, dict) or not ds.get("referenceName"):
                errs.append(f"sources[{i}] missing dataset.referenceName")

    transforms = section.get("transformations") or []
    if isinstance(transforms, list) and len(sources) > 0 and len(transforms) == 0:
        errs.append("transformations array is empty but sources exist")

    sinks = section.get("sinks") or []
    if isinstance(sinks, list) and not sinks:
        errs.append("at least one sink is required")

    return (len(errs) == 0), errs
