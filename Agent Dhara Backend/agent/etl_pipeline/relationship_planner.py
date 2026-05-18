"""
Derive join keys, load order, M:N bridge models, and relationship steps from assessment.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def _parent_child_from_rel(rel: Dict[str, Any]) -> Optional[Tuple[str, str, str, str]]:
    card = str(rel.get("cardinality") or "").lower()
    n1 = str(rel.get("dataset_a") or "")
    n2 = str(rel.get("dataset_b") or "")
    c1 = str(rel.get("column_a") or "")
    c2 = str(rel.get("column_b") or "")
    m1 = int(rel.get("max_rows_per_key_a") or 1)
    m2 = int(rel.get("max_rows_per_key_b") or 1)

    if card == "one_to_many" or (m1 <= 1 < m2):
        return n1, c1, n2, c2
    if card == "many_to_one" or (m2 <= 1 < m1):
        return n2, c2, n1, c1
    if card == "one_to_one" and m1 <= 1 and m2 <= 1:
        return n1, c1, n2, c2
    return None


def _build_join_evidence(rel: Dict[str, Any], orphan_count: int = 0) -> Dict[str, Any]:
    why = [
        rel.get("summary") or "",
        f"{rel.get('overlap_count', 0):,} overlapping key values",
        f"cardinality: {rel.get('cardinality', 'unknown')}",
    ]
    alts: List[str] = []
    card = str(rel.get("cardinality") or "")
    if card == "many_to_many":
        alts.append("Use bridge table (implemented as model_many_to_many step)")
        alts.append("Keep datasets separate and aggregate before reporting")
    if orphan_count > 0:
        alts.append("Left join + orphan quarantine table to preserve child rows")
    else:
        alts.append("Inner join if only matching keys should survive")
    conf = 0.88 if orphan_count == 0 and card in ("one_to_many", "many_to_one", "one_to_one") else 0.62
    return {
        "why_this_action": " | ".join(x for x in why if x),
        "alternatives": [a for a in alts if a],
        "confidence": conf,
        "overlap_count": rel.get("overlap_count"),
        "cardinality": card,
        "orphan_row_count": orphan_count or None,
    }


def _bridge_name(a: str, b: str, col: str) -> str:
    import re

    sa = re.sub(r"[^0-9a-zA-Z_]+", "_", a)[:20]
    sb = re.sub(r"[^0-9a-zA-Z_]+", "_", b)[:20]
    sc = re.sub(r"[^0-9a-zA-Z_]+", "_", col)[:20]
    return f"bridge_{sa}_{sb}_{sc}"


def build_relationship_plan(assessment: Dict[str, Any]) -> Dict[str, Any]:
    rels: List[Dict[str, Any]] = list(assessment.get("relationships") or [])
    dq = assessment.get("data_quality_issues") or {}
    global_issues = dq.get("global_issues") if isinstance(dq, dict) else {}
    if not isinstance(global_issues, dict):
        global_issues = {}
    row_issues: List[Dict[str, Any]] = list(global_issues.get("relationship_row_issues") or [])
    warnings: List[Dict[str, Any]] = list(global_issues.get("relationship_warnings") or [])

    orphan_by_pair: Dict[Tuple[str, str, str], int] = {}
    for iss in row_issues:
        if str(iss.get("type") or "") != "orphan_foreign_key_rows":
            continue
        key = (
            str(iss.get("related_dataset") or ""),
            str(iss.get("related_column") or ""),
            str(iss.get("dataset") or ""),
        )
        orphan_by_pair[key] = int(iss.get("count") or 0)

    joins: List[Dict[str, Any]] = []
    many_to_many: List[Dict[str, Any]] = []
    parent_scores: Dict[str, int] = {}

    for rel in rels:
        card = str(rel.get("cardinality") or "").lower()
        n1 = str(rel.get("dataset_a") or "")
        n2 = str(rel.get("dataset_b") or "")
        c1 = str(rel.get("column_a") or "")
        c2 = str(rel.get("column_b") or "")

        if card == "many_to_many" or (
            int(rel.get("max_rows_per_key_a") or 1) > 1
            and int(rel.get("max_rows_per_key_b") or 1) > 1
        ):
            bname = _bridge_name(n1, n2, c1)
            many_to_many.append(
                {
                    "dataset_a": n1,
                    "dataset_b": n2,
                    "column_a": c1,
                    "column_b": c2,
                    "bridge_name": bname,
                    "cardinality": "many_to_many",
                    "overlap_count": rel.get("overlap_count"),
                    "evidence": _build_join_evidence(rel),
                    "recommended_action": "model_many_to_many",
                    "resolution_options": [
                        "bridge_table",
                        "keep_separate",
                        "aggregate_then_join",
                    ],
                    "default_resolution": "bridge_table",
                }
            )
            joins.append(
                {
                    "left_dataset": n1,
                    "right_dataset": n2,
                    "left_key": c1,
                    "right_key": c2,
                    "join_type": "review",
                    "cardinality": "many_to_many",
                    "overlap_count": rel.get("overlap_count"),
                    "evidence": _build_join_evidence(rel),
                    "note": "M:N — use bridge table codegen, not direct join",
                    "auto_fixable": False,
                    "bridge_name": bname,
                }
            )
            continue

        pc = _parent_child_from_rel(rel)
        if not pc:
            joins.append(
                {
                    "left_dataset": n1,
                    "right_dataset": n2,
                    "left_key": c1,
                    "right_key": c2,
                    "join_type": "review",
                    "cardinality": rel.get("cardinality"),
                    "overlap_count": rel.get("overlap_count"),
                    "evidence": _build_join_evidence(rel),
                    "note": "Ambiguous cardinality — review before joining",
                    "auto_fixable": False,
                }
            )
            continue

        p_ds, p_col, c_ds, c_col = pc
        orphan = orphan_by_pair.get((p_ds, p_col, c_ds), 0)
        join_type = "left" if orphan > 0 else "inner"
        parent_scores[p_ds] = parent_scores.get(p_ds, 0) + 1

        joins.append(
            {
                "parent_dataset": p_ds,
                "child_dataset": c_ds,
                "parent_key": p_col,
                "child_key": c_col,
                "left_dataset": p_ds,
                "right_dataset": c_ds,
                "left_key": p_col,
                "right_key": c_col,
                "join_type": join_type,
                "cardinality": rel.get("cardinality"),
                "from_a_to_b": rel.get("from_a_to_b"),
                "overlap_count": rel.get("overlap_count"),
                "orphan_row_count": orphan or None,
                "evidence": _build_join_evidence(rel, orphan_count=orphan),
                "recommended_action": "join_after_cleaning",
                "auto_fixable": True,
            }
        )

    ds_names = list((assessment.get("datasets") or {}).keys())
    load_order: List[str] = []
    seen = set()
    for ds, _ in sorted(parent_scores.items(), key=lambda x: -x[1]):
        if ds in ds_names and ds not in seen:
            load_order.append(ds)
            seen.add(ds)
    for ds in ds_names:
        if ds not in seen:
            load_order.append(ds)

    relationship_steps: List[Dict[str, Any]] = []
    order = 0
    for b in many_to_many:
        order += 1
        relationship_steps.append(
            {
                "order": order,
                "action": "model_many_to_many",
                "dataset_a": b.get("dataset_a"),
                "dataset_b": b.get("dataset_b"),
                "column_a": b.get("column_a"),
                "column_b": b.get("column_b"),
                "bridge_name": b.get("bridge_name"),
                "bucket": "review",
                "evidence": b.get("evidence"),
            }
        )

    for j in joins:
        if j.get("join_type") == "review":
            continue
        order += 1
        relationship_steps.append(
            {
                "order": order,
                "action": "join_datasets",
                "parent_dataset": j.get("parent_dataset"),
                "child_dataset": j.get("child_dataset"),
                "parent_key": j.get("parent_key"),
                "child_key": j.get("child_key"),
                "join_type": j.get("join_type"),
                "bucket": "review" if j.get("orphan_row_count") else "auto",
                "evidence": j.get("evidence"),
            }
        )

    for iss in row_issues:
        if str(iss.get("type") or "") != "orphan_foreign_key_rows":
            continue
        order += 1
        relationship_steps.append(
            {
                "order": order,
                "action": "validate_referential_integrity_or_stage",
                "dataset": iss.get("dataset"),
                "column": iss.get("column"),
                "related_dataset": iss.get("related_dataset"),
                "related_column": iss.get("related_column"),
                "estimated_affected_rows": iss.get("count"),
                "bucket": "review",
                "evidence": {
                    "why_this_action": iss.get("message") or "Orphan foreign keys detected",
                    "alternatives": [
                        "Reject orphan rows",
                        "Load orphans to quarantine table",
                        iss.get("recommendation") or "",
                    ],
                    "confidence": 0.75,
                    "orphan_row_count": iss.get("count"),
                },
            }
        )

    return {
        "joins": joins,
        "many_to_many": many_to_many,
        "load_order": load_order,
        "relationship_steps": relationship_steps,
        "orphan_issues": row_issues,
        "warnings": warnings,
        "join_count": len([j for j in joins if j.get("join_type") != "review"]),
        "mn_count": len(many_to_many),
    }
