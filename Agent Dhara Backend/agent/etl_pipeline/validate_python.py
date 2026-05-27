from __future__ import annotations

import ast
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.etl_pipeline.codegen_shared import plan_actions

FORBIDDEN_IMPORTS = {"os", "subprocess", "sys", "shlex", "pty", "socket", "shutil", "ctypes"}
_BANNED_MODULES = FORBIDDEN_IMPORTS
_BANNED_CALLS = {"system", "popen", "run"}
_BANNED_BUILTINS = {"eval", "exec"}


def validate_python_source_dict(source: str) -> dict:
    """AST validation returning a structured dict (spec-friendly)."""
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        return {"valid": False, "error": f"Syntax error: {e}", "issues": [f"Syntax error: {e}"]}

    issues: List[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root_mod = (alias.name or "").split(".")[0]
                if root_mod in FORBIDDEN_IMPORTS:
                    issues.append(f"Forbidden import: {alias.name}")
        elif isinstance(node, ast.ImportFrom):
            if node.module and node.module.split(".")[0] in FORBIDDEN_IMPORTS:
                issues.append(f"Forbidden import from: {node.module}")
            if node.module in FORBIDDEN_IMPORTS and any(a.name == "*" for a in (node.names or [])):
                issues.append(f"Forbidden wildcard import from: {node.module}")
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute) and node.func.attr in _BANNED_CALLS:
                issues.append(f"Forbidden call: .{node.func.attr}()")
            elif isinstance(node.func, ast.Name) and node.func.id in _BANNED_BUILTINS:
                issues.append(f"Forbidden builtin: {node.func.id}()")

    return {"valid": len(issues) == 0, "issues": issues}


_ACTION_CODE_MARKERS: Dict[str, List[str]] = {
    "lowercase": [".str.lower()", "F.lower(", "lower("],
    "uppercase": [".str.upper()", "F.upper(", "upper("],
    "trim": [".str.strip()", "F.trim("],
    "fill_nulls_simple": [".fillna(", "coalesce("],
    "fill_or_drop": [".fillna(", "coalesce("],
    "hash_phone": ["hashlib.sha256", "F.sha2("],
    "mask_phone": ["'***'", 'F.lit("***")'],
    "flag_outliers": ["_outlier_flagged", "_iqr_bounds", "_lower"],
    "clip_outliers": [".clip(", "F.lit(_lower)"],
    "cap_outliers": ["_median", "_iqr_bounds"],
    "coerce_numeric": ["to_numeric", "cast('double')"],
    "parse_dates": ["to_datetime", "to_timestamp"],
    "sanitize_email": ["contains('@'"],
    "normalize_phone": ["regexp_replace", r"\D"],
    "deduplicate": ["drop_duplicates", "dropDuplicates"],
    "exclude_column": [".drop(columns=", ".drop("],
    "drop_column": [".drop(columns=", ".drop("],
}


def _action_reflected_in_source(source: str, action: str) -> bool:
    if f"Unsupported in codegen v1: {action}" in source:
        return True
    if action in source:
        return True
    for marker in _ACTION_CODE_MARKERS.get(action, []):
        if marker in source:
            return True
    if action == "validate_referential_integrity_or_stage":
        return "Referential integrity" in source or "RI " in source
    return False


def validate_python_implements_plan(source: str, plan: Optional[Dict[str, Any]] = None) -> List[str]:
    """Ensure each plan action is implemented or marked unsupported in generated code."""
    if not plan:
        return []
    missing: List[str] = []
    seen: Set[str] = set()
    for action in plan_actions(plan):
        if not action or action in seen:
            continue
        seen.add(action)
        if not _action_reflected_in_source(source, action):
            missing.append(f"plan action not reflected in code: {action}")
    return missing


def validate_etl_python_source(source: str, plan: Optional[Dict[str, Any]] = None) -> Tuple[bool, List[str]]:
    """
    ETL template scripts may import os for path resolution (connector manifest).
    Still blocks eval/exec/subprocess and dangerous os calls.
    """
    result = validate_python_source_dict(source)
    if not result.get("valid") and result.get("error") and not result.get("issues"):
        return False, [str(result["error"])]

    issues = list(result.get("issues") or [])
    etl_allowed_roots = {"os", "sys"}
    filtered = []
    for e in issues:
        if any(e == f"Forbidden import: {m}" for m in etl_allowed_roots):
            continue
        if e.startswith("Forbidden import from:"):
            mod = e.split(":", 1)[-1].strip().split(".")[0]
            if mod in etl_allowed_roots:
                continue
        filtered.append(e)
    dangerous = (
        "os.system",
        "os.popen",
        "os.remove",
        "os.unlink",
        "os.rmdir",
        "shutil",
        "subprocess",
    )
    low = source or ""
    for d in dangerous:
        if d in low:
            filtered.append(f"disallowed usage: {d}")
    filtered.extend(validate_python_implements_plan(source, plan))
    return (len(filtered) == 0), filtered


def validate_python_source(source: str) -> Tuple[bool, List[str]]:
    """Strict validation for untrusted Python (no os allowance)."""
    if not source or not source.strip():
        return False, ["empty source"]

    result = validate_python_source_dict(source)
    if not result.get("valid") and result.get("error"):
        return False, list(result.get("issues") or [str(result["error"])])

    issues = list(result.get("issues") or [])
    if issues:
        return False, issues
    return True, []
