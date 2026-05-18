from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional

from agent.business_rules_loader import (
    list_tenant_ids,
    merge_business_rules_for_datasets,
    pending_rules_from_session,
    tenant_id_from_session,
)
from agent.etl_pipeline.etl_gx_checkpoint import run_etl_gx_checkpoint
from agent.session_store import load_session, save_session
from agent.etl_pipeline import (
    build_etl_plan,
    build_impact_preview,
    generate_python_etl,
    normalize_business_rules,
)
from agent.etl_pipeline.llm_codegen import (
    generate_adf_with_llm,
    generate_etl_with_llm,
    is_llm_generation_error,
    parse_adf_json_from_llm,
)
from agent.etl_pipeline.schema_lineage import build_lineage
from agent.etl_pipeline.validate_plan import validate_etl_plan, validate_etl_plan_for_confirm
from agent.etl_pipeline.validate_python import validate_etl_python_source, validate_python_source
from agent.etl_pipeline.validate_pyspark import validate_pyspark_source
from agent.etl_pipeline.source_context import build_source_context
from agent.etl_pipeline.connector_manifest import build_connector_manifest
from agent.etl_pipeline.plan_narrator import narrate_plan
from agent.etl_pipeline.manual_review_promote import (
    apply_manual_resolutions,
    count_pending_manual_review,
    enrich_plan_manual_review,
)

logger = logging.getLogger("agent.etl")


def _resolve_codegen_mode(
    engine: str,
    *,
    requested: Optional[str] = None,
) -> str:
    """
    template | llm | llm_then_template
    Default: template for pyspark (fast, manifest-aware); llm_then_template for others.
    Override with ETL_CODEGEN_MODE env or API codegen_mode.
    """
    if requested and str(requested).strip().lower() in ("template", "llm", "llm_then_template"):
        return str(requested).strip().lower()
    env = os.getenv("ETL_CODEGEN_MODE", "").strip().lower()
    if env in ("template", "llm", "llm_then_template"):
        return env
    eng = (engine or "python").lower()
    if eng in ("spark", "pyspark"):
        fast = os.getenv("DHARA_ETL_FAST_PYSPARK", "1").strip().lower() in ("1", "true", "yes")
        if fast:
            return "template"
    return "llm_then_template"


# ── Phase state machine ───────────────────────────────────────────────────────

ETL_PHASES = [
    "planned",
    "preview_ready",
    "approved",
    "generating",
    "validated",
    "code_ready",
    "downloadable",
    "failed",
]

ALLOWED_TRANSITIONS: Dict[str, List[str]] = {
    "planned": ["preview_ready", "failed"],
    "preview_ready": ["approved", "failed"],
    "approved": ["generating", "failed"],
    "generating": ["validated", "failed"],
    "validated": ["code_ready", "failed"],
    "code_ready": ["downloadable", "failed"],
    "failed": ["planned"],
    "downloadable": [],
}

_LEGACY_PHASE_MAP = {
    "no_plan": "planned",
    "plan_built": "planned",
    "plan_validated": "preview_ready",
    "preview_shown": "preview_ready",
    "code_failed": "failed",
}


def _migrate_phase(flow: dict) -> None:
    current = flow.get("phase")
    if current in _LEGACY_PHASE_MAP:
        flow["phase"] = _LEGACY_PHASE_MAP[current]


def _can_transition(from_phase: str, to_phase: str) -> bool:
    _migrate_phase({"phase": from_phase})
    from_phase = _LEGACY_PHASE_MAP.get(from_phase, from_phase)
    if from_phase == to_phase:
        return True
    return to_phase in ALLOWED_TRANSITIONS.get(from_phase, [])


def _transition(flow: dict, to_phase: str, *, by: str = "system", reason: str = "") -> None:
    if to_phase not in ETL_PHASES:
        raise ValueError(f"Unknown phase: {to_phase}")
    _migrate_phase(flow)
    from_phase = flow.get("phase") or "planned"
    if from_phase not in ETL_PHASES:
        from_phase = _LEGACY_PHASE_MAP.get(from_phase, "planned")
        flow["phase"] = from_phase
    if not _can_transition(from_phase, to_phase):
        raise ValueError(f"Invalid ETL phase transition: {from_phase} -> {to_phase}")
    history = flow.setdefault("phase_history", [])
    history.append(
        {
            "from": from_phase,
            "to": to_phase,
            "ts": time.time(),
            "by": by,
            "reason": reason,
        }
    )
    flow["phase"] = to_phase


def rollback_on_failure(flow: dict, *, reason: str = "") -> None:
    """Reset flow to planned while preserving plan, assessment context, and history."""
    flow["failure_reason"] = reason
    flow["last_failure_reason"] = reason
    try:
        _transition(flow, "failed", by="system", reason=reason)
    except ValueError:
        flow["phase"] = "failed"
    flow["approved_plan"] = None
    flow["validation_ok"] = False
    try:
        _transition(flow, "planned", by="system", reason="rollback_on_failure")
    except ValueError:
        flow["phase"] = "planned"


def _plan_all_auto(plan: Dict[str, Any]) -> bool:
    for block in (plan.get("datasets") or {}).values():
        for st in (block or {}).get("steps") or []:
            if str(st.get("classification") or st.get("bucket") or "auto").lower() != "auto":
                return False
            if st.get("requires_user_choice"):
                return False
    if plan.get("blocked"):
        return False
    if count_pending_manual_review(plan) > 0:
        return False
    return True


def _invariants_pass(plan: Dict[str, Any]) -> bool:
    inv = plan.get("invariants") or []
    for item in inv:
        if item.get("enabled") and item.get("name") == "never_drop_rows":
            rules = plan.get("business_rules") or {}
            if not rules.get("never_drop_rows"):
                return False
    return True


def _ctx(session: Dict[str, Any]) -> Dict[str, Any]:
    return session.setdefault("context", {})


def _get_assessment(session: Dict[str, Any], override: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if isinstance(override, dict) and override.get("datasets"):
        return override
    raw = (_ctx(session) or {}).get("last_assessment_result")
    return raw if isinstance(raw, dict) and raw.get("datasets") else None


def _safe_segment(s: str) -> str:
    t = re.sub(r"[^a-zA-Z0-9_-]+", "_", (s or "default").strip())[:80]
    return t or "default"


def _rehydrate_plan(plan: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Restore session-owned fields stripped by UI plan edits."""
    out = dict(plan)
    if not out.get("connector_manifest") and ctx.get("connector_manifest"):
        out["connector_manifest"] = ctx["connector_manifest"]
    if not out.get("source_context") and ctx.get("source_context"):
        out["source_context"] = ctx["source_context"]
    if not out.get("relationships") and (ctx.get("etl_flow") or {}).get("plan", {}).get("relationships"):
        out["relationships"] = (ctx.get("etl_flow") or {})["plan"]["relationships"]
    flow = ctx.get("etl_flow") or {}
    if not out.get("etl_intent") and flow.get("etl_intent"):
        out["etl_intent"] = flow["etl_intent"]
    if not out.get("engine_recommendation") and flow.get("plan", {}).get("engine_recommendation"):
        out["engine_recommendation"] = flow["plan"]["engine_recommendation"]
    if not out.get("narration") and flow.get("plan", {}).get("narration"):
        out["narration"] = flow["plan"]["narration"]
    return out


def _engine_rec_to_codegen(rec: Dict[str, Any]) -> tuple[str, str]:
    """Map engine_recommendation to (codegen_engine, sql_dialect)."""
    eng = str(rec.get("engine") or "python").lower()
    dialect = str(rec.get("dialect") or "tsql").lower()
    if eng == "pyspark":
        return "pyspark", dialect
    if eng == "adf":
        return "adf", dialect
    if eng == "sql":
        return "sql", dialect if dialect in ("ansi", "tsql") else "tsql"
    return "python", dialect


def etl_plan_start(
    session_id: str,
    business_rules: Any,
    assessment_result: Optional[Dict[str, Any]] = None,
    engine: str = "python",
    codegen_engine: Optional[str] = None,
    sql_dialect: str = "tsql",
    target_destination: str = "dataframe_only",
    target_path: Optional[str] = None,
    tenant_id: Optional[str] = None,
    source_context: Optional[Dict[str, Any]] = None,
    engine_user_override: bool = False,
) -> Dict[str, Any]:
    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    ctx = _ctx(sess)
    assess = _get_assessment(sess, assessment_result)

    if not assess:
        return {
            "ok": False,
            "error": "NO_ASSESSMENT",
            "message": "Run an assessment first, or pass assessment_result in the request body.",
        }

    if isinstance(assessment_result, dict) and assessment_result.get("datasets"):
        ctx["last_assessment_result"] = assessment_result

    ds_names = list((assess.get("datasets") or {}).keys())
    tid = (tenant_id or tenant_id_from_session(ctx) or "default").strip() or "default"
    ctx["etl_tenant_id"] = tid
    pending = pending_rules_from_session(ctx)
    merged_raw = business_rules
    if pending:
        merged_raw = {**(pending or {}), **(business_rules if isinstance(business_rules, dict) else {})}
    rules_merged = merge_business_rules_for_datasets(merged_raw, ds_names, tenant_id=tid)

    src_ctx = build_source_context(ctx, assess, override=source_context)
    ctx["source_context"] = src_ctx
    if target_destination == "overwrite":
        out_base = "__overwrite__"
    elif target_destination == "new_path" and target_path:
        out_base = target_path
    else:
        out_base = "cleaned/"
    manifest = build_connector_manifest(
        ctx, assess, output_base=out_base, overwrite_in_place=(target_destination == "overwrite")
    )
    ctx["connector_manifest"] = manifest

    t0 = time.time()
    plan = build_etl_plan(
        assess,
        rules_merged,
        engine=engine,
        source_context=src_ctx,
    )
    plan = enrich_plan_manual_review(plan)
    plan["connector_manifest"] = manifest
    plan["source_context"] = src_ctx
    plan["etl_intent"] = {
        "engine": (engine or "python").lower(),
        "target_destination": target_destination or "dataframe_only",
        "target_path": target_path,
    }

    flow = ctx.setdefault("etl_flow", {})
    narr_mode = os.getenv("ETL_NARRATOR_MODE", "tiered").strip().lower()
    use_llm_full = narr_mode in ("llm", "full") or os.getenv(
        "ETL_NARRATOR_USE_LLM", "0"
    ).strip().lower() in ("1", "true", "yes")
    cache_key = f"narr_{plan.get('plan_id')}_{plan.get('assessment_signature')}"
    cached = (flow.get("narration_cache") or {}).get(cache_key)
    if isinstance(cached, dict) and cached.get("engine_explanation"):
        plan["narration"] = cached
    else:
        plan["narration"] = narrate_plan(plan, mode=narr_mode, use_llm=use_llm_full)
        flow.setdefault("narration_cache", {})[cache_key] = plan["narration"]

    plan_ok, plan_errs = validate_etl_plan(plan, assess, rules_merged)

    eng_rec = plan.get("engine_recommendation") or {}
    if engine_user_override:
        ce = (codegen_engine or engine or "python").lower()
        sd = (sql_dialect or "tsql").lower()
        ctx["etl_engine_override"] = True
    else:
        ce, sd = _engine_rec_to_codegen(eng_rec)
        if codegen_engine:
            ce = codegen_engine.lower()
        ctx.pop("etl_engine_override", None)
    _migrate_phase(flow)
    _transition(flow, "planned", by="system", reason="etl_plan_start")
    preview = None
    if plan_ok and not (plan.get("blocked") or []):
        preview = build_impact_preview(assess, plan)
        flow["preview"] = preview
        _transition(flow, "preview_ready", by="system", reason="plan_enriched_with_evidence")
    elif plan.get("blocked"):
        flow["failure_reason"] = "Plan has blocking issues"
        _transition(flow, "failed", by="system", reason="plan_blocked")

    if pending:
        ctx.pop("pending_business_rules", None)

    flow.update(
        {
            "plan": plan,
            "plan_validation_ok": plan_ok,
            "plan_validation_errors": plan_errs,
            "target_engine": (engine or "python").lower(),
            "codegen_engine": ce,
            "sql_dialect": sd,
            "business_rules": rules_merged,
            "etl_intent": {
                "engine": (engine or "python").lower(),
                "sql_dialect": sd,
                "target_destination": target_destination or "dataframe_only",
                "target_path": target_path,
            },
            "approved_plan": None,
            "preview": None,
            "code": None,
            "validation_ok": None,
            "validation_errors": [],
            "generated_by": None,
            "artifact_rel_path": None,
            "is_draft": False,
            "lineage": None,
            "artifact_version": flow.get("artifact_version") or 0,
        }
    )

    save_session(sess)
    logger.info(
        "etl_plan_start session=%s plan_id=%s ok=%s steps=%s latency_ms=%.0f",
        sid,
        plan.get("plan_id"),
        plan_ok,
        sum(len((v or {}).get("steps") or []) for v in (plan.get("datasets") or {}).values()),
        (time.time() - t0) * 1000,
    )
    blocked = plan.get("blocked") or []
    plan_success = plan_ok and not blocked
    pending_manual = count_pending_manual_review(plan)
    return {
        "ok": plan_success,
        "session_id": sid,
        "plan": plan,
        "blocked": blocked,
        "pending_manual_review": pending_manual,
        "plan_validation_ok": plan_ok,
        "plan_validation_errors": plan_errs,
        "engine_recommendation": plan.get("engine_recommendation"),
        "source_context": src_ctx,
        "recommended_codegen_engine": ce,
        "recommended_sql_dialect": sd,
        "message": (
            None
            if plan_success
            else (
                "Plan has blocking issues."
                if blocked
                else "Plan built with validation warnings — review plan_validation_errors."
            )
        ),
    }


def etl_apply_manual_resolutions(
    session_id: str,
    resolutions: List[Dict[str, Any]],
    plan_override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Apply user picks for manual_review items; promotes steps into plan.datasets."""
    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    ctx = _ctx(sess)
    assess = _get_assessment(sess, None)
    flow = ctx.get("etl_flow") or {}

    plan = (
        plan_override
        if isinstance(plan_override, dict) and plan_override.get("datasets") is not None
        else flow.get("plan")
    )
    if not isinstance(plan, dict):
        return {"ok": False, "error": "NO_PLAN", "message": "Create a plan first (POST /etl/plan)."}

    plan = enrich_plan_manual_review(_rehydrate_plan(plan, ctx))
    rules = flow.get("business_rules") or plan.get("business_rules") or {}
    updated, apply_errs = apply_manual_resolutions(plan, resolutions, business_rules=rules)

    if apply_errs and not resolutions:
        return {
            "ok": False,
            "error": "NO_RESOLUTIONS",
            "message": "Provide at least one resolution.",
            "errors": apply_errs,
        }

    struct_ok, plan_errs = validate_etl_plan(updated, assess or {}, rules)
    pending = count_pending_manual_review(updated)
    plan_ok = struct_ok and pending == 0
    flow = ctx.setdefault("etl_flow", {})
    flow["plan"] = updated
    flow["plan_validation_ok"] = plan_ok
    flow["plan_validation_errors"] = plan_errs
    flow["approved_plan"] = None
    save_session(sess)

    return {
        "ok": len(apply_errs) == 0 and pending == 0,
        "session_id": sid,
        "plan": updated,
        "pending_manual_review": pending,
        "plan_validation_ok": plan_ok,
        "plan_validation_errors": plan_errs,
        "errors": apply_errs,
        "message": (
            None
            if pending == 0 and not apply_errs
            else (
                f"{pending} manual review item(s) still pending."
                if pending
                else "Some resolutions could not be applied."
            )
        ),
    }


def etl_confirm_plan(session_id: str, plan_override: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    ctx = _ctx(sess)
    assess = _get_assessment(sess, None)
    flow = ctx.get("etl_flow") or {}
    _migrate_phase(flow)

    plan = (
        plan_override
        if isinstance(plan_override, dict) and plan_override.get("datasets") is not None
        else flow.get("plan")
    )
    if not isinstance(plan, dict) or not plan.get("datasets"):
        return {"ok": False, "error": "NO_PLAN", "message": "Create a plan first (POST /etl/plan)."}

    plan = enrich_plan_manual_review(_rehydrate_plan(plan, ctx))

    pending_manual = count_pending_manual_review(plan)
    if pending_manual > 0:
        return {
            "ok": False,
            "error": "MANUAL_REVIEW_PENDING",
            "message": f"Resolve {pending_manual} manual review item(s) in the UI before confirming.",
            "pending_manual_review": pending_manual,
            "manual_review": plan.get("manual_review") or [],
        }

    blocked = plan.get("blocked") or []
    if blocked:
        return {
            "ok": False,
            "error": "PLAN_BLOCKED",
            "message": "Plan has blocking issues; resolve required columns or rules first.",
            "blocked": blocked,
        }

    rules = flow.get("business_rules") or plan.get("business_rules") or {}
    plan_ok, plan_errs = validate_etl_plan_for_confirm(plan, assess or {}, rules)
    if not plan_ok:
        return {
            "ok": False,
            "error": "PLAN_VALIDATION_FAILED",
            "message": "Plan failed validation. Fix issues before confirming.",
            "plan_validation_errors": plan_errs,
        }

    _migrate_phase(flow)
    phase = flow.get("phase", "planned")
    if phase not in ("preview_ready", "planned"):
        return {
            "ok": False,
            "error": "INVALID_PHASE",
            "message": f"Cannot approve plan in phase '{phase}'. Build plan first.",
            "phase": phase,
        }

    auto_ok = _plan_all_auto(plan) and _invariants_pass(plan)
    if phase == "planned":
        flow["preview"] = flow.get("preview") or build_impact_preview(assess or {}, plan)
        _transition(flow, "preview_ready", by="system", reason="preview_before_approve")
        phase = "preview_ready"

    preview = flow.get("preview") or build_impact_preview(assess or {}, plan)
    lineage = build_lineage(plan, assess or {})
    flow = ctx.setdefault("etl_flow", {})
    flow["approved_plan"] = plan
    flow["preview"] = preview
    flow["lineage"] = lineage
    flow["plan_validation_ok"] = True
    _transition(
        flow,
        "approved",
        by="user" if not auto_ok else "system",
        reason="confirm_plan_called" if not auto_ok else "auto_approved_all_steps_safe",
    )

    save_session(sess)
    logger.info(
        "etl_confirm_plan session=%s plan_id=%s lineage_cols=%s",
        sid,
        plan.get("plan_id"),
        sum(len(v) for v in lineage.values()),
    )
    return {
        "ok": True,
        "session_id": sid,
        "preview": preview,
        "approved_plan": plan,
        "lineage": lineage,
    }


def _generate_for_engine(
    eng: str,
    plan: Dict[str, Any],
    assess: Dict[str, Any],
    *,
    sql_dialect: str,
    output_mode: str,
    output_path: Optional[str],
    inject_errors: Optional[List[str]],
) -> tuple[str, bool, List[str], str]:
    """Returns (code, ok, errs, generated_by)."""
    generated_by = "llm"

    if eng == "python":
        code = generate_etl_with_llm(
            plan,
            assess,
            engine="python",
            output_mode=output_mode,
            output_path=output_path,
            validation_errors=inject_errors,
            validate_fn=lambda src: validate_etl_python_source(src),
        )
        if is_llm_generation_error(code):
            return code, False, [code], generated_by
        ok, errs = validate_etl_python_source(code)
        return code, ok, errs, generated_by

    if eng in ("sql", "tsql", "ansi"):
        from agent.etl_pipeline.validate_sql import validate_sql_basic

        dialect = "ansi" if eng == "ansi" else (sql_dialect or "tsql")
        code = generate_etl_with_llm(
            plan,
            assess,
            engine=f"sql-{dialect}",
            sql_dialect=dialect,
            output_mode=output_mode,
            validation_errors=inject_errors,
        )
        if is_llm_generation_error(code):
            return code, False, [code], generated_by
        ok, errs = validate_sql_basic(code)
        return code, ok, errs, generated_by

    if eng in ("spark", "pyspark"):
        code = generate_etl_with_llm(
            plan,
            assess,
            engine="pyspark",
            output_mode=output_mode,
            output_path=output_path,
            validation_errors=inject_errors,
            validate_fn=lambda src: validate_pyspark_source(src, plan),
        )
        if is_llm_generation_error(code):
            return code, False, [code], generated_by
        ok, errs = validate_pyspark_source(code, plan)
        return code, ok, errs, generated_by

    if eng == "adf":
        from agent.etl_pipeline.validate_adf import validate_adf_json

        if inject_errors:
            raw = generate_etl_with_llm(plan, assess, engine="adf", validation_errors=inject_errors)
            if is_llm_generation_error(raw):
                return raw, False, [raw], generated_by
            obj, parse_errs = parse_adf_json_from_llm(raw)
            if obj is None:
                return raw, False, parse_errs, generated_by
            code = json.dumps(obj, indent=2)
            ok, errs = validate_adf_json(obj)
            return code, ok, errs, generated_by

        obj, llm_err = generate_adf_with_llm(plan, assess, validate_fn=validate_adf_json)
        if obj is None:
            return llm_err or "# Error: ADF generation failed", False, [llm_err or "ADF failed"], generated_by
        code = json.dumps(obj, indent=2)
        ok, errs = validate_adf_json(obj)
        return code, ok, errs, generated_by

    return "", False, [f"Unsupported engine: {eng}"], generated_by


def _template_fallback(
    eng: str,
    plan: Dict[str, Any],
    assess: Dict[str, Any],
    *,
    sql_dialect: str,
) -> tuple[str, bool, List[str]]:
    if eng == "python":
        code = generate_python_etl(plan, assess)
        return code, *validate_etl_python_source(code, plan)

    if eng in ("sql", "tsql", "ansi"):
        from agent.etl_pipeline.sql_codegen import generate_sql_etl
        from agent.etl_pipeline.validate_sql import validate_sql_basic

        dialect = "ansi" if eng == "ansi" else (sql_dialect or "tsql")
        code = generate_sql_etl(plan, assess, dialect=dialect)
        return code, *validate_sql_basic(code)

    if eng in ("spark", "pyspark"):
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl

        code = generate_pyspark_etl(plan, assess)
        return code, *validate_pyspark_source(code, plan)

    if eng == "adf":
        from agent.etl_pipeline.adf_codegen import generate_adf_mapping_flow
        from agent.etl_pipeline.validate_adf import validate_adf_bundle

        obj = generate_adf_mapping_flow(plan, assess)
        code = json.dumps(obj, indent=2)
        return code, *validate_adf_bundle(obj)

    return "", False, [f"Unsupported engine: {eng}"]


def etl_generate_code(
    session_id: str,
    engine: str = "python",
    sql_dialect: str = "tsql",
    *,
    codegen_mode: Optional[str] = None,
    run_gx_on_generate: Optional[bool] = None,
) -> Dict[str, Any]:
    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    ctx = _ctx(sess)
    assess = _get_assessment(sess, None) or {}
    flow = ctx.get("etl_flow") or {}
    _migrate_phase(flow)

    _migrate_phase(flow)
    current_phase = flow.get("phase", "planned")
    allowed_phases = {"approved", "failed", "code_ready", "generating", "validated"}
    if current_phase not in allowed_phases:
        return {
            "ok": False,
            "error": "PLAN_NOT_APPROVED",
            "http_status": 409,
            "message": (
                f"Cannot generate code: phase is '{current_phase}'. "
                "Approve the plan first via POST /etl/confirm."
            ),
            "phase": current_phase,
        }

    plan = flow.get("approved_plan")
    if not isinstance(plan, dict) or not plan.get("datasets"):
        return {
            "ok": False,
            "error": "NO_APPROVED_PLAN",
            "message": "Confirm the plan first (POST /etl/confirm).",
        }
    plan = _rehydrate_plan(plan, ctx)

    eng = (engine or flow.get("codegen_engine") or "python").lower()
    intent = flow.get("etl_intent") or {}
    output_mode = intent.get("target_destination", "dataframe_only")
    output_path = intent.get("target_path")
    sd = (sql_dialect or flow.get("sql_dialect") or "tsql").lower()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_dir = os.path.join(root, "output", "etl_code", _safe_segment(sid))
    os.makedirs(out_dir, exist_ok=True)
    pid = _safe_segment(str(plan.get("plan_id") or "plan"))
    ts = int(time.time())
    version = int(flow.get("artifact_version") or 0) + 1

    _transition(flow, "generating", by="system")
    save_session(sess)

    mode = _resolve_codegen_mode(eng, requested=codegen_mode)
    t_gen = time.time()
    ok = False
    errs: List[str] = []
    code = ""
    generated_by = "template" if mode == "template" else "llm"

    try:
        if mode == "template":
            code, ok, errs = _template_fallback(eng, plan, assess, sql_dialect=sd)
            generated_by = "template"
        elif mode == "llm":
            code, ok, errs, generated_by = _generate_for_engine(
                eng,
                plan,
                assess,
                sql_dialect=sd,
                output_mode=output_mode,
                output_path=output_path,
                inject_errors=None,
            )
        else:
            code, ok, errs, generated_by = _generate_for_engine(
                eng,
                plan,
                assess,
                sql_dialect=sd,
                output_mode=output_mode,
                output_path=output_path,
                inject_errors=None,
            )
            if not ok and not is_llm_generation_error(code):
                logger.info(
                    "etl_generate_code LLM validation failed session=%s — using template fallback",
                    sid,
                )
            if not ok:
                generated_by = "template"
                code, ok, errs = _template_fallback(eng, plan, assess, sql_dialect=sd)
    except Exception as exc:
        logger.exception("etl_generate_code failed session=%s", sid)
        code = code or f"# Generation failed: {exc}"
        ok = False
        errs = [str(exc)]
        generated_by = "error"

    ext_map = {
        "python": "py",
        "sql": "sql",
        "tsql": "sql",
        "ansi": "sql",
        "pyspark": "py",
        "spark": "py",
        "adf": "json",
    }
    ext = ext_map.get(eng, "py")
    fname = f"etl_{pid}_{eng}_v{version}_{ts}.{ext}"

    abs_path = os.path.join(out_dir, fname)
    with open(abs_path, "w", encoding="utf-8") as f:
        f.write(code)

    rel = os.path.relpath(abs_path, root).replace("\\", "/")

    flow = ctx.setdefault("etl_flow", {})
    if ok:
        _transition(flow, "validated", by="system", reason=f"validator_passed generated_by={generated_by}")
        _transition(flow, "code_ready", by="system", reason="artifact_written")
    else:
        rollback_on_failure(flow, reason=f"validation_failed: {(errs or ['unknown'])[:3]}")
    latency_ms = (time.time() - t_gen) * 1000
    flow.update(
        {
            "code": code,
            "target_engine": eng,
            "validation_ok": ok,
            "validation_errors": errs or [],
            "generated_by": generated_by,
            "artifact_rel_path": rel,
            "is_draft": not ok,
            "artifact_version": version,
            "last_generate_latency_ms": round(latency_ms, 1),
        }
    )
    save_session(sess)

    logger.info(
        "etl_generate_code session=%s plan_id=%s engine=%s by=%s ok=%s version=%s latency_ms=%.0f",
        sid,
        plan.get("plan_id"),
        eng,
        generated_by,
        ok,
        version,
        latency_ms,
    )

    gx_report: Optional[Dict[str, Any]] = None
    try:
        if run_gx_on_generate is None:
            auto_gx = os.getenv("DHARA_ETL_GX_AUTO", "1").strip().lower() in ("1", "true", "yes")
        else:
            auto_gx = bool(run_gx_on_generate)
        gx_result = run_etl_gx_checkpoint(
            plan,
            assess,
            flow.get("business_rules") or plan.get("business_rules"),
            flow.get("lineage"),
            run_gx_if_available=auto_gx,
        )
        flow["gx_checkpoint"] = gx_report = gx_result
        flow["gx_checkpoint_at"] = time.time()
        save_session(sess)
    except Exception as gx_exc:
        logger.warning("gx_checkpoint_after_generate failed: %s", gx_exc)

    return {
        "ok": ok,
        "session_id": sid,
        "engine": eng,
        "format": ext,
        "code": code,
        "validation_ok": ok,
        "validation_errors": errs or [],
        "generated_by": generated_by,
        "is_draft": not ok,
        "label": "Validated" if ok else "UNVALIDATED — do not deploy",
        "artifact_rel_path": rel,
        "artifact_version": version,
        "latency_ms": round(latency_ms, 1),
        "codegen_mode": mode,
        "gx_checkpoint": gx_report,
        "message": (
            None
            if ok
            else "Code generated as draft — fix validation_errors before production deploy."
        ),
        "gx_checkpoint_hint": (
            "GX expectation suite attached in gx_checkpoint. "
            "Re-run POST /etl/gx-checkpoint with run_gx_if_available=true after executing ETL on staging data."
        ),
    }


def etl_get_lineage(session_id: str) -> Dict[str, Any]:
    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    flow = (_ctx(sess).get("etl_flow") or {})
    lineage = flow.get("lineage")
    if not isinstance(lineage, dict):
        return {"ok": False, "error": "NO_LINEAGE", "message": "Confirm the plan first to build lineage."}
    return {"ok": True, "session_id": sid, "lineage": lineage, "plan_id": (flow.get("approved_plan") or {}).get("plan_id")}


def etl_run_gx_checkpoint(session_id: str, *, run_gx_if_available: bool = False) -> Dict[str, Any]:
    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    ctx = _ctx(sess)
    flow = ctx.get("etl_flow") or {}
    plan = flow.get("approved_plan") or flow.get("plan")
    assess = _get_assessment(sess, None)

    if not isinstance(plan, dict) or not assess:
        return {
            "ok": False,
            "error": "NO_PLAN_OR_ASSESSMENT",
            "message": "Build and confirm an ETL plan after assessment first.",
        }

    report = run_etl_gx_checkpoint(
        plan,
        assess,
        flow.get("business_rules") or plan.get("business_rules"),
        flow.get("lineage"),
        run_gx_if_available=run_gx_if_available,
    )
    flow["gx_checkpoint"] = report
    flow["gx_checkpoint_at"] = time.time()
    save_session(sess)

    logger.info(
        "etl_gx_checkpoint session=%s plan_id=%s overall_ok=%s",
        sid,
        plan.get("plan_id"),
        report.get("summary", {}).get("overall_ok"),
    )
    return {"ok": True, "session_id": sid, "checkpoint": report}


def etl_list_tenants() -> Dict[str, Any]:
    return {"ok": True, "tenants": list_tenant_ids()}
