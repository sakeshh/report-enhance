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

    if isinstance(transforms, list):
        names = {t.get("name") for t in transforms if isinstance(t, dict)}
        for t in transforms:
            if not isinstance(t, dict):
                continue
            for up in t.get("upstream") or []:
                if up not in names and up not in {s.get("name") for s in sources if isinstance(s, dict)}:
                    errs.append(f"transformation '{t.get('name')}' references unknown upstream '{up}'")

    return (len(errs) == 0), errs


def validate_adf_bundle(obj: Any) -> Tuple[bool, List[str]]:
    """Validate primary ADF flow and any flows in bundle.flows."""
    if not isinstance(obj, dict):
        return False, ["ADF payload must be a JSON object"]
    all_errs: List[str] = []
    ok_primary, errs = validate_adf_json(obj)
    if not ok_primary:
        all_errs.extend([f"primary: {e}" for e in errs])
    bundle = obj.get("bundle")
    if isinstance(bundle, dict):
        flows = bundle.get("flows") or []
        if len(flows) < 1:
            all_errs.append("bundle.flows is empty")
        for i, flow in enumerate(flows):
            ok_f, err_f = validate_adf_json(flow)
            if not ok_f:
                role = flow.get("role", f"flow_{i}") if isinstance(flow, dict) else f"flow_{i}"
                all_errs.extend([f"{role}: {e}" for e in err_f])
        roles = [f.get("role") for f in flows if isinstance(f, dict)]
        if "clean_only" not in roles:
            all_errs.append("bundle missing clean_only flow")
    return (len(all_errs) == 0), all_errs
