from __future__ import annotations

from typing import Any, Dict, List, Tuple


def validate_adf_json(obj: Any) -> Tuple[bool, List[str]]:
    errs: List[str] = []
    if not isinstance(obj, dict):
        return False, ["ADF payload must be a JSON object"]
    props = obj.get("properties")
    if props is not None and not isinstance(props, dict):
        errs.append("properties must be an object")
    if isinstance(props, dict):
        if props.get("type") != "MappingDataFlow":
            errs.append("expected properties.type MappingDataFlow")
    if errs:
        return False, errs
    return True, []
