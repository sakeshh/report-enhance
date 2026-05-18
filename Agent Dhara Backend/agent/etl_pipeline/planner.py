from __future__ import annotations

import hashlib
import json
import time
from typing import Any, Dict, List, Optional, Tuple

from agent.transformation_suggester import suggest_transformations
from agent.etl_pipeline.business_rules import normalize_business_rules, column_is_excluded
from agent.etl_pipeline.classify_steps import classify_step_bucket
from agent.etl_pipeline.relationship_planner import build_relationship_plan
from agent.etl_pipeline.manual_review_catalog import enrich_manual_review_item
from agent.etl_pipeline.step_metadata import (
    build_plan_invariants,
    enrich_relationship_plan_joins,
    finalize_dataset_steps,
)
from agent.etl_pipeline.step_params import build_ri_step_params, build_step_params

# Lower number = earlier in pipeline (per column / global)
_ACTION_PRIORITY: Dict[str, int] = {
    "trim": 5,
    "lowercase": 8,
    "uppercase": 8,
    "fill_or_drop": 20,
    "fill_nulls_simple": 20,
    "cast_type": 35,
    "coerce_numeric": 40,
    "parse_dates": 45,
    "sanitize_email": 50,
    "normalize_phone": 55,
    "hash_phone": 56,
    "mask_phone": 57,
    "drop_column": 85,
    "exclude_column": 86,
    "nullify_future_dates": 48,
    "regex_replace": 60,
    "range_clip": 65,
    "clip_or_flag": 65,
    "flag_outliers": 65,
    "clip_outliers": 65,
    "cap_outliers": 65,
    "standardize_boolean": 70,
    "replace_values": 75,
    "deduplicate": 200,
    "validate_referential_integrity_or_stage": 300,
}


def _plan_id() -> str:
    return f"plan_{int(time.time())}"


def _assessment_signature(assessment: Dict[str, Any]) -> str:
    try:
        blob = json.dumps(assessment, sort_keys=True, default=str)[:500_000]
    except Exception:
        blob = str(assessment)
    return hashlib.sha256(blob.encode("utf-8", errors="ignore")).hexdigest()[:16]


def _dataset_columns(assessment: Dict[str, Any], dataset: str) -> Dict[str, Any]:
    ds = (assessment.get("datasets") or {}).get(dataset) or {}
    return ds.get("columns") or {}


def _col_stats_for_step(
    assessment: Dict[str, Any], dataset: str, column: Optional[str]
) -> Dict[str, Any]:
    if not dataset or not column:
        return {}
    ds_data = (assessment.get("datasets") or {}).get(dataset) or {}
    stats = dict((ds_data.get("columns") or {}).get(column) or {})
    total = int(ds_data.get("row_count") or 0)
    stats["row_count"] = total
    null_pct = stats.get("null_percentage")
    if null_pct is not None and total > 0:
        try:
            stats["null_count"] = int(round(float(null_pct) * total))
        except (TypeError, ValueError):
            pass
    return stats


def _build_evidence(
    suggestion: Dict[str, Any],
    col_stats: Dict[str, Any],
    action: str,
    rules: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Structured evidence from assessment DQ + column profile (no invented stats).
    """
    col = suggestion.get("column")
    issue_type = str(suggestion.get("issue_type") or "")
    message = str(suggestion.get("message") or "").strip()
    affected = suggestion.get("row_count_affected")
    sev = str(suggestion.get("severity") or "medium").lower()

    stype = col_stats.get("dtype") or col_stats.get("semantic_type") or "unknown"
    total = int(col_stats.get("row_count") or 0)
    nulls = col_stats.get("null_count")
    if nulls is None and col_stats.get("null_percentage") is not None and total > 0:
        try:
            nulls = int(round(float(col_stats["null_percentage"]) * total))
        except (TypeError, ValueError):
            nulls = 0
    nulls = int(nulls or 0)
    null_pct = round((nulls / max(total, 1)) * 100, 2) if nulls else 0.0
    if col_stats.get("null_percentage") is not None:
        try:
            null_pct = round(float(col_stats["null_percentage"]) * 100, 2)
        except (TypeError, ValueError):
            pass

    why_parts: List[str] = []
    alternatives: List[str] = []
    confidence = 0.55

    if message:
        why_parts.append(message)
        confidence += 0.12
    if affected is not None and isinstance(affected, (int, float)) and affected >= 0:
        why_parts.append(f"~{int(affected):,} rows affected per DQ scan")
        confidence += 0.15
    if issue_type:
        why_parts.append(f"issue_type={issue_type}")

    mean = col_stats.get("mean")
    median = col_stats.get("median")
    std = col_stats.get("std")
    skew = col_stats.get("skew")
    p5 = col_stats.get("p5")
    p95 = col_stats.get("p95")
    recommended_fill: Optional[str] = None

    act = (action or "").lower()
    if act in ("fill_nulls_simple", "fill_or_drop"):
        if null_pct > 0:
            why_parts.append(f"{nulls:,} nulls ({null_pct}% of {total:,} rows)")
        if skew is not None and abs(float(skew)) > 1.0 and median is not None:
            recommended_fill = "median"
            why_parts.append(
                f"skewed distribution (skew={round(float(skew), 2)}) — "
                f"fill with median ({round(float(median), 4)}) not mean"
            )
            confidence = min(confidence + 0.1, 0.92)
        elif median is not None and mean is not None:
            recommended_fill = "median" if abs(float(skew or 0)) > 0.5 else "mean"
            why_parts.append(
                f"near-normal spread — fill with {recommended_fill} "
                f"(mean={round(float(mean), 4)}, median={round(float(median), 4)})"
            )
        if null_pct < 1.0:
            alternatives.append("Drop rows — null rate is very low, minimal data loss")
        if null_pct > 20.0:
            alternatives.append(
                f"Consider dropping column — {null_pct}% missing may be unreliable"
            )
            confidence = min(confidence, 0.62)
        if col_stats.get("semantic_type"):
            why_parts.append(f"semantic_type={col_stats['semantic_type']}")

    elif act in ("cast_type", "coerce_numeric"):
        if col_stats.get("dtype_inference"):
            why_parts.append(f"inferred type hint: {col_stats['dtype_inference']}")
        alternatives.append("Use try-cast to preserve rows with conversion errors")

    elif act == "deduplicate":
        dupes = col_stats.get("duplicate_value_count")
        if dupes:
            why_parts.append(f"{int(dupes):,} duplicate values in column")
        if affected:
            why_parts.append(f"{int(affected):,} duplicate-related rows flagged")
        alternatives.append("Keep duplicates if source intentionally produces them (e.g. event logs)")

    elif act in ("flag_outliers", "clip_outliers", "cap_outliers", "clip_or_flag"):
        if p5 is not None and p95 is not None:
            why_parts.append(f"outliers outside p5–p95 range [{round(float(p5), 4)}, {round(float(p95), 4)}]")
        if std is not None and mean is not None:
            why_parts.append(f"std={round(float(std), 2)}, mean={round(float(mean), 2)}")
        if issue_type in ("numeric_outliers_iqr", "negative_values", "suspicious_zero"):
            why_parts.append("numeric outlier pattern detected in assessment")
        alternatives.append("Flag outliers instead of clipping — preserves values for audit")
        alternatives.append("Cap at p1/p99 for less aggressive trimming")

    elif act in ("sanitize_email", "normalize_phone"):
        if affected:
            why_parts.append(f"{int(affected):,} values failed format checks")

    elif act in ("trim", "lowercase", "uppercase"):
        if issue_type == "whitespace" or issue_type == "case_inconsistency":
            why_parts.append("format inconsistency detected in column values")

    elif act == "parse_dates":
        if col_stats.get("dtype_inference") == "datetime_like":
            why_parts.append("mixed or string dates inferred from profile")
        alternatives.append("Specify target date format if downstream requires strict typing")

    if rules.get("never_drop_rows") and act == "fill_nulls_simple":
        why_parts.append("'never_drop_rows' rule active — drop option removed")
        confidence = min(confidence + 0.05, 0.95)

    if sev == "high" and act not in ("deduplicate",):
        confidence = min(confidence, 0.68)
    if suggestion.get("auto_fixable"):
        confidence = min(confidence + 0.08, 0.94)
    if not why_parts:
        why_parts.append(f"Action '{action}' recommended from assessment profile")

    confidence = round(max(0.35, min(confidence, 0.95)), 2)

    out: Dict[str, Any] = {
        "null_count": nulls if nulls else None,
        "null_pct": null_pct if null_pct else None,
        "dtype": stype,
        "row_count": total if total else None,
        "issue_type": issue_type or None,
        "severity": sev,
        "why_this_action": " | ".join(why_parts),
        "alternatives": alternatives,
        "confidence": confidence,
        "rule_override": bool(rules.get("never_drop_rows") and act == "fill_nulls_simple"),
    }
    if mean is not None:
        out["mean"] = round(float(mean), 4)
    if median is not None:
        out["median"] = round(float(median), 4)
    if std is not None:
        out["std"] = round(float(std), 4)
    if skew is not None:
        out["skew"] = round(float(skew), 4)
    if recommended_fill:
        out["recommended_fill"] = recommended_fill
    return out


def _recommend_engine(
    source_context: Optional[Dict[str, Any]],
    assessment: Dict[str, Any],
) -> Dict[str, Any]:
    """Engine recommendation from source type + data scale."""
    ctx = source_context or {}
    src_type = str(ctx.get("type") or "unknown").lower()
    size_mb = float(ctx.get("size_mb") or 0)
    row_count = int(ctx.get("row_count") or 0)

    if row_count == 0:
        for ds in (assessment.get("datasets") or {}).values():
            if isinstance(ds, dict):
                row_count = max(row_count, int(ds.get("row_count") or 0))
    if size_mb == 0 and row_count > 0:
        size_mb = round(row_count * 0.0005, 2)

    if src_type in ("sql_server", "azure_sql"):
        return {
            "engine": "sql",
            "dialect": "tsql",
            "reason": (
                f"Source is {src_type} — T-SQL runs in-database with no file export."
            ),
            "alternatives": [
                "Python via SQLAlchemy — script-based ETL",
                "ADF — if part of a larger Azure pipeline",
            ],
            "warning": None,
        }

    if src_type in ("postgres", "mysql"):
        return {
            "engine": "sql",
            "dialect": "ansi",
            "reason": f"Source is {src_type} — ANSI SQL is portable across engines.",
            "alternatives": ["Python/Pandas via SQLAlchemy"],
            "warning": None,
        }

    if src_type in ("blob_storage", "adls") or size_mb > 500 or row_count > 1_000_000:
        return {
            "engine": "pyspark",
            "dialect": None,
            "reason": (
                f"Large or cloud-backed data ({row_count:,} rows, ~{size_mb}MB) — "
                f"PySpark scales beyond single-node Pandas."
            ),
            "alternatives": [
                "ADF — existing Azure Data Factory pipelines",
                "Python — only for samples or small subsets",
            ],
            "warning": (
                "PySpark needs a cluster (Databricks, Synapse, or local Spark)."
            ),
        }

    if src_type in ("adf_pipeline", "databricks"):
        return {
            "engine": "adf",
            "dialect": None,
            "reason": "ADF-native source — Mapping Data Flow JSON fits your pipeline.",
            "alternatives": ["PySpark — notebook-based transforms"],
            "warning": "ADF JSON needs linked services configured in your factory.",
        }

    ext = str(ctx.get("extension") or ".csv").lower()
    file_notes = {
        ".xlsx": "Excel — pd.read_excel()",
        ".xls": "Excel — pd.read_excel()",
        ".json": "JSON — pd.read_json()",
        ".jsonl": "JSON lines — pd.read_json(lines=True)",
        ".parquet": "Parquet — pd.read_parquet()",
        ".csv": "CSV — pd.read_csv()",
        ".tsv": "TSV — pd.read_csv(sep='\\t')",
    }
    file_note = file_notes.get(ext, f"{ext or 'file'} detected")

    warn = None
    if size_mb >= 200:
        warn = f"At ~{size_mb}MB, Pandas may be slow — consider PySpark."

    return {
        "engine": "python",
        "dialect": None,
        "reason": (
            f"{file_note}. {row_count:,} rows (~{size_mb}MB) — "
            f"suitable for Pandas on a single machine."
        ),
        "alternatives": [
            "PySpark — if data grows past ~500MB or moves to lake storage",
            "SQL — after loading into a database",
        ],
        "warning": warn,
    }


def _steps_from_business_notes(
    rules: Dict[str, Any],
    assessment: Dict[str, Any],
) -> List[Tuple[str, str, str, str]]:
    """
    Promote explicit business-note instructions into plan steps (e.g. hash phone on data_1.xml).
    Returns (dataset, column, action, note) tuples.
    """
    notes = (rules.get("notes") or "").lower()
    if "phone" not in notes:
        return []
    if not any(w in notes for w in ("hash", "mask", "privacy")):
        return []
    use_hash = "hash" in notes
    use_mask = "mask" in notes and not use_hash
    action = "hash_phone" if use_hash else ("mask_phone" if use_mask else "hash_phone")

    ds_names = list((assessment.get("datasets") or {}).keys())
    targets: List[str] = []
    for ds in ds_names:
        dsl = ds.lower()
        if dsl in notes or dsl.replace("_", "") in notes.replace("_", "").replace(" ", ""):
            targets.append(ds)
    if not targets:
        for ds in ds_names:
            if ".xml" in ds.lower() and "xml" in notes:
                targets.append(ds)
    if not targets and len(ds_names) == 1:
        targets = ds_names

    out: List[Tuple[str, str, str, str]] = []
    for ds in targets:
        for col in _dataset_columns(assessment, ds).keys():
            if str(col).lower() == "phone":
                out.append(
                    (
                        ds,
                        col,
                        action,
                        f"business_rules.notes: {'hash' if use_hash else 'mask'} phone for privacy",
                    )
                )
    return out


def _apply_rules_to_action(
    action: str,
    column: Optional[str],
    business_rules: Dict[str, Any],
) -> Tuple[str, Optional[str]]:
    """
    Returns (action, note) where note is a human-readable override reason.
    """
    if business_rules.get("never_drop_rows") and action == "fill_or_drop":
        return "fill_nulls_simple", "never_drop_rows: using fill-only instead of drop/fill choice"
    return action, None


def build_etl_plan(
    assessment: Dict[str, Any],
    business_rules_raw: Any,
    *,
    engine: str = "python",
    source_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build versioned ETL plan JSON from assessment + normalized business rules.
    """
    if not isinstance(assessment, dict) or not assessment.get("datasets"):
        raise ValueError("Invalid assessment: missing datasets")

    rules = normalize_business_rules(business_rules_raw)
    exclude = set(rules.get("exclude_columns") or [])

    sug_pkg = suggest_transformations(assessment)
    suggestions: List[Dict[str, Any]] = list(sug_pkg.get("suggested_transformations") or [])

    manual_review: List[Dict[str, Any]] = []
    blocked: List[Dict[str, Any]] = []
    # (dataset, column, action) -> step record
    step_map: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    datasets_known = set((assessment.get("datasets") or {}).keys())

    for req_col in rules.get("required_columns") or []:
        rc = str(req_col).strip()
        if not rc:
            continue
        found = False
        for ds_name in datasets_known:
            cols = _dataset_columns(assessment, ds_name)
            for ck in cols.keys():
                if ck.lower() == rc.lower():
                    found = True
                    break
            if found:
                break
        if not found:
            blocked.append(
                {
                    "type": "missing_required_column",
                    "column": rc,
                    "message": f"Required column '{rc}' not found in any assessed dataset.",
                }
            )

    for s in suggestions:
        ds = s.get("dataset") or ""
        col = s.get("column")
        action = str(s.get("suggested_action") or "")
        sev = str(s.get("severity") or "medium").lower()

        if ds and ds != "_global" and column_is_excluded(col, exclude):
            continue

        if action == "review_manually" or not s.get("auto_fixable", False):
            manual_review.append(
                enrich_manual_review_item(
                    {
                        "dataset": ds or None,
                        "column": col,
                        "issue_type": s.get("issue_type"),
                        "severity": sev,
                        "message": s.get("message"),
                        "guidance": s.get("manual_guidance") or "",
                    }
                )
            )
            continue

        action2, override_note = _apply_rules_to_action(action, col, rules)

        # Route outlier strategy
        if action2 == "clip_or_flag":
            strategy = rules.get("outlier_strategy", "flag")
            if strategy == "clip":
                action2 = "clip_outliers"
            elif strategy == "cap":
                action2 = "cap_outliers"
            else:
                action2 = "flag_outliers"

        if col and rules.get("non_nullable") and col.strip().lower() in (rules.get("non_nullable") or []):
            if action2 in ("fill_or_drop", "fill_nulls_simple") and not rules.get("never_drop_rows"):
                manual_review.append(
                    enrich_manual_review_item(
                        {
                            "dataset": ds,
                            "column": col,
                            "issue_type": s.get("issue_type") or "non_nullable_fill",
                            "severity": "medium",
                            "message": f"Column {col} is non-nullable; review fill/drop behavior manually.",
                            "guidance": override_note or "",
                        }
                    )
                )
                continue

        key = (ds or "_global", (col or "*"), action2)
        pri = _ACTION_PRIORITY.get(action2, 80)
        row_est = s.get("row_count_affected")
        col_stats = _col_stats_for_step(assessment, ds or "", col)
        evidence = _build_evidence(s, col_stats, action2, rules)
        params = build_step_params(
            action2,
            column=col,
            col_stats=col_stats,
            evidence=evidence,
            rules=rules,
            issue_type=str(s.get("issue_type") or ""),
        )
        entry = {
            "dataset": ds or "_global",
            "column": col,
            "action": action2,
            "source_issue_type": s.get("issue_type"),
            "severity": sev,
            "estimated_affected_rows": row_est,
            "priority": pri,
            "note": override_note,
            "params": params,
            "evidence": evidence,
            "message": s.get("message"),
        }
        prev = step_map.get(key)
        if not prev or (row_est and (prev.get("estimated_affected_rows") or 0) < (row_est or 0)):
            step_map[key] = entry

    for ds, col, act, note in _steps_from_business_notes(rules, assessment):
        if column_is_excluded(col, exclude):
            continue
        key = (ds, col, act)
        if key not in step_map:
            cstats = _col_stats_for_step(assessment, ds, col)
            ev = {"why_this_action": note, "confidence": 0.9}
            step_map[key] = {
                "dataset": ds,
                "column": col,
                "action": act,
                "source_issue_type": "business_notes",
                "severity": "medium",
                "priority": _ACTION_PRIORITY.get(act, 56),
                "note": note,
                "params": build_step_params(act, column=col, col_stats=cstats, evidence=ev, rules=rules),
                "evidence": ev,
                "message": note,
            }

    # Per-dataset ordered steps
    datasets_out: Dict[str, Any] = {}
    global_steps: List[Dict[str, Any]] = []

    for key, st in step_map.items():
        ds_name = key[0]
        # Route to global if sentinel or empty string
        if not ds_name or ds_name == "_global":
            global_steps.append(
                {
                    "order": st["priority"],
                    "column": st.get("column"),
                    "action": st["action"],
                    "estimated_affected_rows": st.get("estimated_affected_rows"),
                    "note": st.get("note"),
                }
            )
            continue
        datasets_out.setdefault(ds_name, []).append(st)

    rel_plan = build_relationship_plan(assessment)
    if rules.get("never_drop_rows"):
        for j in rel_plan.get("joins") or []:
            if str(j.get("join_type") or "").lower() == "inner":
                j["join_type"] = "left"
                j["note"] = (
                    (j.get("note") or "")
                    + " Upgraded inner→left: never_drop_rows business rule."
                ).strip()
    for rstep in rel_plan.get("relationship_steps") or []:
        act = str(rstep.get("action") or "")
        if act == "validate_referential_integrity_or_stage":
            ds = rstep.get("dataset") or "_global"
            col = rstep.get("column")
            key = (ds, (col or "*"), act)
            if key not in step_map:
                ri_ev = rstep.get("evidence") or {}
                step_map[key] = {
                    "dataset": ds,
                    "column": col,
                    "action": act,
                    "severity": "high",
                    "estimated_affected_rows": rstep.get("estimated_affected_rows"),
                    "priority": _ACTION_PRIORITY.get(act, 300),
                    "note": f"FK to {rstep.get('related_dataset')}.{rstep.get('related_column')}",
                    "params": build_ri_step_params(rstep, rules),
                    "evidence": ri_ev,
                    "message": ri_ev.get("why_this_action"),
                }
                datasets_out.setdefault(ds, []).append(step_map[key])

    global_steps.sort(key=lambda x: x.get("order") or 0)
    for i, st in enumerate(global_steps, start=1):
        st["order"] = i

    for ds_name, steps in datasets_out.items():
        steps.sort(key=lambda x: (x["priority"], str(x.get("column") or "")))
        enriched_steps: List[Dict[str, Any]] = []
        for i, st in enumerate(steps, start=1):
            st["order"] = i
            col = st.get("column")
            cstats = _col_stats_for_step(assessment, ds_name, col)
            null_pct = None
            if cstats.get("null_percentage") is not None:
                try:
                    null_pct = float(cstats["null_percentage"]) * 100.0
                except (TypeError, ValueError):
                    pass
            st["bucket"] = classify_step_bucket(
                str(st.get("action") or ""),
                severity=str(st.get("severity") or "medium"),
                null_percentage=null_pct,
                never_drop_rows=bool(rules.get("never_drop_rows")),
            )
            enriched_steps.append(st)
        datasets_out[ds_name] = finalize_dataset_steps(enriched_steps, assessment, rules)

    rel_plan = enrich_relationship_plan_joins(rel_plan, rules)

    engine_rec = _recommend_engine(source_context, assessment)
    if rel_plan.get("join_count", 0) > 0 and engine_rec.get("engine") == "python":
        ds_count = len(datasets_known)
        if ds_count > 1:
            engine_rec = dict(engine_rec)
            engine_rec["reason"] = (
                str(engine_rec.get("reason", ""))
                + f" Multi-dataset ({ds_count} sources, {rel_plan['join_count']} join(s) detected)."
            )

    plan = {
        "plan_version": 1,
        "plan_id": _plan_id(),
        "engine": (engine or "python").lower(),
        "created_at": time.time(),
        "assessment_signature": _assessment_signature(assessment),
        "business_rules": rules,
        "datasets": {k: {"steps": v} for k, v in datasets_out.items()},
        "global_steps": global_steps,
        "manual_review": manual_review,
        "blocked": blocked,
        "invariants": build_plan_invariants(rules),
        "suggestions_summary": sug_pkg.get("summary") or {},
        "engine_recommendation": engine_rec,
        "source_context": source_context or {},
        "relationships": rel_plan,
    }
    return plan
