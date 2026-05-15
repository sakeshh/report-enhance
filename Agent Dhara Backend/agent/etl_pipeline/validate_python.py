from __future__ import annotations

import ast
from typing import List, Tuple


def validate_python_source(source: str) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    if not source or not source.strip():
        return False, ["empty source"]
    try:
        ast.parse(source)
    except SyntaxError as e:
        return False, [f"syntax: {e.msg} at line {e.lineno}"]
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return False, errors

    # Disallow obvious risky constructs in generated ETL v1
    for node in ast.walk(tree):
        if isinstance(node, ast.Import) or isinstance(node, ast.ImportFrom):
            for n in getattr(node, "names", []) or []:
                mod = getattr(n, "name", "") or ""
                low = mod.lower()
                if low in ("os", "subprocess", "socket", "shutil") or low.startswith("ctypes"):
                    errors.append(f"disallowed import pattern: {mod}")
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                if node.func.attr in ("system", "popen", "run"):
                    errors.append("disallowed call")
    if errors:
        return False, errors
    return True, []
