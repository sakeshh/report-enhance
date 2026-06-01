from __future__ import annotations

import copy
from typing import Any, Dict, List

_EMAIL_RE = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
_E164_RE = r"^\+[1-9]\d{1,14}$"


def _log(msg: str) -> None:
    print(f"[gx_suite_builder] {msg}")


def _col_meta(col: dict) -> dict:
    return col if isinstance(col, dict) else {}


def _inferred_type(col: dict) -> str:
    c = _col_meta(col)
    if c.get("inferred_type"):
        return str(c["inferred_type"]).lower()
    st = str(c.get("semantic_type") or "").lower()
    if st in ("email", "phone", "integer", "float", "numeric"):
        return st
    dtype = str(c.get("dtype") or "").lower()
    if "int" in dtype:
        return "integer"
    if "float" in dtype or "double" in dtype:
        return "float"
    return st or dtype or "unknown"


def _business_importance(col: dict) -> str:
    c = _col_meta(col)
    if c.get("business_importance"):
        return str(c["business_importance"]).lower()
    hints = c.get("llm_hints") or {}
    if hints.get("business_importance"):
        return str(hints["business_importance"]).lower()
    st = str(c.get("semantic_type") or "").lower()
    if st in ("email", "phone", "id", "pk", "metric", "numeric_id"):
        return "high"
    return "medium"


def _strictness_margin(strictness: str, base: float) -> float:
    s = (strictness or "standard").lower()
    if s == "strict":
        return base * 0.5
    if s == "relaxed":
        return base * 1.5
    return base


def build_gx_suite(
    dataset_name: str,
    assessment: dict,
    strictness: str = "standard",
) -> dict:
    """
    Build a Great Expectations ExpectationSuite as a serializable dict from profiler output.

    Reads ``assessment["datasets"][dataset_name]["columns"]`` and maps profiler fields
    (``semantic_type``, ``dtype``, ``llm_hints``, etc.) to GX expectations.
    """
    ds = (assessment.get("datasets") or {}).get(dataset_name) or {}
    columns = ds.get("columns") or {}
    if not isinstance(columns, dict):
        columns = {}

    expectations: List[Dict[str, Any]] = []
    suite_name = f"{dataset_name}_auto_suite"

    for col_name, col_raw in columns.items():
        col = _col_meta(col_raw)
        column = str(col_name)
        inf = _inferred_type(col)
        biz = _business_importance(col)
        null_pct = float(col.get("null_percentage") or 0.0)
        meta = {"auto_generated": True, "source": "dhara_profiler"}

        if biz == "high":
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": column},
                    "meta": copy.deepcopy(meta),
                }
            )
        elif null_pct > 0 and biz != "high":
            # Spec: only high business importance for null expectation when null_pct > 0
            pass

        if inf in ("integer", "int", "bigint"):
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_be_of_type",
                    "kwargs": {"column": column, "type_": "BIGINT"},
                    "meta": copy.deepcopy(meta),
                }
            )
        elif inf in ("float", "double", "numeric", "number"):
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_be_of_type",
                    "kwargs": {"column": column, "type_": "DOUBLE"},
                    "meta": copy.deepcopy(meta),
                }
            )

        stats = col.get("statistics") or col.get("stats") or {}
        if isinstance(stats, dict):
            lo = stats.get("min")
            hi = stats.get("max")
            if lo is not None and hi is not None and inf in ("integer", "int", "float", "double", "numeric", "number"):
                try:
                    lo_f = float(lo)
                    hi_f = float(hi)
                    margin = _strictness_margin(strictness, max(abs(lo_f), abs(hi_f), 1.0) * 0.01)
                    expectations.append(
                        {
                            "expectation_type": "expect_column_min_to_be_between",
                            "kwargs": {
                                "column": column,
                                "min_value": lo_f - margin,
                                "max_value": lo_f + margin,
                            },
                            "meta": copy.deepcopy(meta),
                        }
                    )
                    expectations.append(
                        {
                            "expectation_type": "expect_column_max_to_be_between",
                            "kwargs": {
                                "column": column,
                                "min_value": hi_f - margin,
                                "max_value": hi_f + margin,
                            },
                            "meta": copy.deepcopy(meta),
                        }
                    )
                except (TypeError, ValueError):
                    pass

        if inf == "email" or str(col.get("semantic_type") or "").lower() == "email":
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_match_regex",
                    "kwargs": {"column": column, "regex": _EMAIL_RE},
                    "meta": copy.deepcopy(meta),
                }
            )
        if inf == "phone" or str(col.get("semantic_type") or "").lower() in ("phone", "mobile"):
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_match_regex",
                    "kwargs": {"column": column, "regex": _E164_RE},
                    "meta": copy.deepcopy(meta),
                }
            )

        uniq = col.get("uniqueness_ratio")
        if uniq is None and col.get("unique_count") is not None and col.get("row_count"):
            try:
                rc = float(col.get("row_count") or 0)
                if rc > 0:
                    uniq = float(col.get("unique_count") or 0) / rc
            except (TypeError, ValueError):
                uniq = None
        if uniq is not None and float(uniq) >= 1.0:
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": column},
                    "meta": copy.deepcopy(meta),
                }
            )

        distinct = col.get("distinct_values")
        if isinstance(distinct, list) and len(distinct) < 20 and distinct:
            try:
                value_set = list(distinct)
                expectations.append(
                    {
                        "expectation_type": "expect_column_values_to_be_in_set",
                        "kwargs": {"column": column, "value_set": value_set},
                        "meta": copy.deepcopy(meta),
                    }
                )
            except (TypeError, ValueError):
                pass

    out = {
        "expectation_suite_name": suite_name,
        "expectations": _dedupe_expectations(expectations),
    }
    _log(f"built suite {suite_name} with {len(expectations)} expectations for dataset={dataset_name!r}")
    return out


def _dedupe_expectations(exps: List[dict]) -> List[dict]:
    seen: set[tuple[Any, ...]] = set()
    out: List[dict] = []
    for e in exps:
        et = e.get("expectation_type")
        kw = e.get("kwargs") or {}
        key = (et, tuple(sorted((k, str(v)) for k, v in kw.items())))
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out
