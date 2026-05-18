"""
ETL Chat Router — full pipeline parity with Pipeline UI via etl_handlers.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from agent.etl_handlers import etl_confirm_plan, etl_generate_code, etl_plan_start
from agent.session_store import load_session, save_session


def _flow(session_id: str) -> Dict[str, Any]:
    sess = load_session(session_id)
    return (sess.get("context") or {}).get("etl_flow") or {}


def _get_assessment(session_id: str) -> Optional[Dict[str, Any]]:
    sess = load_session(session_id)
    raw = (sess.get("context") or {}).get("last_assessment_result")
    return raw if isinstance(raw, dict) and raw.get("datasets") else None


def _ensure_etl_plan(
    session_id: str,
    *,
    engine: str = "python",
) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    """Build plan from session assessment + pending rules if missing."""
    flow = _flow(session_id)
    if flow.get("plan") or flow.get("approved_plan"):
        return True, None, ""

    assess = _get_assessment(session_id)
    if not assess:
        return (
            False,
            None,
            "No assessment in this session. Run a **data quality assessment** first, then say **'build ETL plan'**.",
        )

    sess = load_session(session_id)
    ctx = sess.get("context") or {}
    pending = ctx.get("pending_business_rules") or {}
    eng_rec = (flow.get("plan") or {}).get("engine_recommendation") or {}
    codegen = str(flow.get("codegen_engine") or eng_rec.get("engine") or engine or "python")

    result = etl_plan_start(
        session_id,
        business_rules=pending,
        assessment_result=assess,
        engine=codegen if codegen != "pyspark" else "python",
        codegen_engine=codegen,
        target_destination=(flow.get("etl_intent") or {}).get("target_destination", "dataframe_only"),
        target_path=(flow.get("etl_intent") or {}).get("target_path"),
    )
    if not result.get("ok"):
        errs = result.get("plan_validation_errors") or []
        blocked = result.get("blocked") or []
        if blocked:
            items = "\n".join(f"  - {b.get('message', '')}" for b in blocked[:3])
            return False, result, f"❌ Plan blocked:\n{items}"
        err_txt = "\n".join(f"  - {e}" for e in errs[:5]) if errs else result.get("message", "Plan validation failed")
        return False, result, f"⚠️ Plan built with warnings:\n{err_txt}\n\nSay **'show ETL plan'** to review."

    return True, result, ""


def chat_build_etl_plan(
    session_id: str,
    *,
    engine: str = "python",
    business_rules: Optional[Dict[str, Any]] = None,
) -> str:
    assess = _get_assessment(session_id)
    if not assess:
        return (
            "No assessment found. Select data and run **assess selected tables/files** first, "
            "then say **'build ETL plan'**."
        )

    sess = load_session(session_id)
    ctx = sess.get("context") or {}
    rules = business_rules if business_rules is not None else (ctx.get("pending_business_rules") or {})

    result = etl_plan_start(
        session_id,
        business_rules=rules,
        assessment_result=assess,
        engine=engine if engine not in ("pyspark", "spark", "adf") else "python",
        codegen_engine=engine,
    )

    if result.get("blocked"):
        items = "\n".join(f"  - {b.get('message', '')}" for b in result["blocked"][:3])
        return f"❌ Plan blocked — resolve before continuing:\n{items}"

    plan = result.get("plan") or {}
    summary = _format_plan_summary(plan)
    eng = (result.get("engine_recommendation") or {}).get("engine", "python")
    val_ok = result.get("plan_validation_ok", False)

    header = "✅ **ETL plan built**" if result.get("ok") else "⚠️ **ETL plan built (needs review)**"
    lines = [
        header,
        f"Recommended engine: **{str(eng).upper()}**",
        "",
        summary,
        "",
    ]
    if not val_ok:
        perrs = result.get("plan_validation_errors") or []
        if perrs:
            lines.append("Validation notes:")
            lines.extend(f"  - {e}" for e in perrs[:5])
            lines.append("")
    lines.append("Next: say **'approve the plan'**, then **'generate ETL code'**.")
    return "\n".join(lines)


def chat_generate_etl_code(
    session_id: str,
    engine: str = "python",
    sql_dialect: str = "tsql",
) -> str:
    flow = _flow(session_id)
    phase = flow.get("phase", "no_plan")

    if phase == "no_plan" or not flow.get("plan"):
        ok, _, msg = _ensure_etl_plan(session_id, engine=engine)
        if not ok:
            return msg
        flow = _flow(session_id)
        phase = flow.get("phase", "no_plan")

    if phase not in ("approved", "code_failed", "code_ready", "generating"):
        if phase in ("plan_built", "plan_validated", "preview_shown", "planned", "preview_ready"):
            plan_summary = _format_plan_summary(flow.get("plan") or {})
            return (
                "Plan is ready — please **approve it** before I generate code.\n\n"
                f"{plan_summary}\n\n"
                "Say **'approve the plan'** to proceed."
            )
        return (
            f"Cannot generate code (phase: `{phase}`). "
            "Say **'build ETL plan'** after assessment, then **'approve the plan'**."
        )

    result = etl_generate_code(session_id, engine=engine, sql_dialect=sql_dialect)

    if not result.get("ok"):
        error = result.get("error", "UNKNOWN")
        if error == "PLAN_NOT_APPROVED":
            return "The plan hasn't been approved yet. Say **'approve the plan'** first."
        code = result.get("code") or ""
        errs = result.get("validation_errors") or []
        err_list = "\n".join(f"  - {e}" for e in errs[:5])
        preview = code[:800]
        more = "..." if len(code) > 800 else ""
        return (
            f"⚠️ **{result.get('label', 'Draft code')}**\n\n"
            f"Validation failed:\n{err_list}\n\n"
            f"```{engine}\n{preview}{more}\n```\n\n"
            "Fix the plan or regenerate. Download is blocked until validation passes."
        )

    validation_ok = result.get("validation_ok", False)
    generated_by = result.get("generated_by", "llm")
    engine_used = result.get("engine", engine)
    preview = (result.get("code") or "")[:800]
    more = "..." if len(result.get("code") or "") > 800 else ""
    gx = result.get("gx_checkpoint") or {}
    gx_sum = (gx.get("summary") or {}) if isinstance(gx, dict) else {}

    lines = [
        f"✅ **ETL code generated** ({engine_used.upper()}, via {generated_by})\n",
        f"```{engine_used}\n{preview}{more}\n```\n",
        f"Download: `GET /etl/download?session_id={session_id}` or **Pipeline UI → Download**.",
    ]
    if gx_sum:
        lines.append(
            f"\n📋 **GX checkpoint:** {gx_sum.get('passed', 0)} passed, "
            f"{gx_sum.get('failed', 0)} failed expectations (metadata suite)."
        )
    return "\n".join(lines)


def chat_show_etl_plan(session_id: str) -> str:
    flow = _flow(session_id)
    plan = flow.get("plan") or flow.get("approved_plan")

    if not plan:
        ok, _, msg = _ensure_etl_plan(session_id)
        if not ok:
            return msg
        flow = _flow(session_id)
        plan = flow.get("plan")

    if not plan:
        return "No ETL plan yet. Say **'build ETL plan'** after running an assessment."

    phase = flow.get("phase", "no_plan")
    header = f"_(phase: {phase})_\n\n" if phase != "no_plan" else ""
    return header + _format_plan_summary(plan)


def chat_confirm_etl_plan(
    session_id: str,
    plan_override: Optional[Dict[str, Any]] = None,
) -> str:
    flow = _flow(session_id)

    if not flow.get("plan") and not plan_override:
        ok, _, msg = _ensure_etl_plan(session_id)
        if not ok:
            return msg

    result = etl_confirm_plan(session_id, plan_override=plan_override)

    if not result.get("ok"):
        error = result.get("error", "")
        if error == "PLAN_BLOCKED":
            blocked = result.get("blocked") or []
            items = "\n".join(f"  - {b.get('message', '')}" for b in blocked[:3])
            return f"❌ Plan has blocking issues:\n{items}"
        perrs = result.get("plan_validation_errors") or []
        if perrs:
            return "❌ Plan validation failed:\n" + "\n".join(f"  - {e}" for e in perrs[:5])
        return f"Could not confirm plan: {result.get('message', error)}"

    steps_count = sum(
        len(v.get("steps", []))
        for v in (result["approved_plan"].get("datasets") or {}).values()
    )
    return (
        f"✅ **Plan approved** ({steps_count} transformation steps)\n\n"
        "Say **'generate ETL code'** (python / sql / pyspark / adf) to produce the script."
    )


def chat_capture_business_rules(session_id: str, rules_text: str) -> str:
    from agent.etl_pipeline.business_rules import normalize_business_rules

    sess = load_session(session_id)
    ctx = sess.setdefault("context", {})
    rules = normalize_business_rules(rules_text)
    ctx["pending_business_rules"] = rules
    save_session(sess)
    return (
        "✅ Business rules captured.\n\n"
        f"Rules: `{rules}`\n\n"
        "Say **'build ETL plan'** to create a plan with these rules, then approve and generate."
    )


def chat_download_etl_code(session_id: str) -> str:
    flow = _flow(session_id)

    if not flow.get("artifact_rel_path"):
        return "No ETL script yet. Say **'build ETL plan'** → **'approve the plan'** → **'generate ETL code'**."

    if not flow.get("validation_ok"):
        return (
            "⚠️ Generated code did not pass validation — download is blocked for safety. "
            "Review validation errors and regenerate."
        )

    return (
        f"✅ Validated script ready.\n\n"
        f"**Download:** `GET /etl/download?session_id={session_id}` "
        "or use **Pipeline UI → Download**."
    )


def _format_plan_summary(plan: Dict[str, Any]) -> str:
    if not plan:
        return "_(empty plan)_"

    datasets = plan.get("datasets") or {}
    manual = plan.get("manual_review") or []
    blocked = plan.get("blocked") or []
    plan_id = plan.get("plan_id", "—")
    rel = plan.get("relationships") or {}

    lines = [f"**ETL Plan** `{plan_id}`\n"]

    for ds_name, ds_obj in datasets.items():
        steps = ds_obj.get("steps") or []
        lines.append(f"**Dataset: {ds_name}** ({len(steps)} steps)")
        for s in steps[:8]:
            col = s.get("column") or "*(global)*"
            act = s.get("action", "?")
            bucket = s.get("bucket", "auto")
            lines.append(f"  {s.get('order', '?')}. `{col}` → **{act}** [{bucket}]")
        if len(steps) > 8:
            lines.append(f"  _...and {len(steps) - 8} more steps_")

    if rel.get("join_count"):
        lines.append(f"\n🔗 **{rel['join_count']} join(s)** — load order: {rel.get('load_order', [])}")
    if rel.get("mn_count"):
        lines.append(f"⚠️ **{rel['mn_count']} M:N** relationship(s) — bridge modeling required")

    if manual:
        lines.append(f"\n⚠️ **{len(manual)} manual review item(s)**")
    if blocked:
        lines.append(f"\n❌ **{len(blocked)} blocked** — resolve before confirming")

    return "\n".join(lines)
