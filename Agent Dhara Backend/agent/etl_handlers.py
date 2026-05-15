from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, Optional

from agent.session_store import load_session, save_session
from agent.etl_pipeline import (
    build_etl_plan,
    build_impact_preview,
    generate_python_etl,
    normalize_business_rules,
)
from agent.etl_pipeline.validate_python import validate_python_source


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


def etl_plan_start(
    session_id: str,
    business_rules: Any,
    assessment_result: Optional[Dict[str, Any]] = None,
    engine: str = "python",
    codegen_engine: Optional[str] = None,
    sql_dialect: str = "tsql",
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

    plan = build_etl_plan(assess, business_rules, engine=engine)
    ce = (codegen_engine or engine or "python").lower()
    sd = (sql_dialect or "tsql").lower()
    flow = ctx.setdefault("etl_flow", {})
    flow.update(
        {
            "phase": "planned",
            "plan": plan,
            "target_engine": (engine or "python").lower(),
            "codegen_engine": ce,
            "sql_dialect": sd,
            "business_rules": normalize_business_rules(business_rules),
            "approved_plan": None,
            "preview": None,
            "code": None,
            "validation_ok": None,
            "validation_errors": [],
            "artifact_rel_path": None,
        }
    )
    save_session(sess)
    return {"ok": True, "session_id": sid, "plan": plan, "blocked": plan.get("blocked") or []}


def etl_confirm_plan(session_id: str, plan_override: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    ctx = _ctx(sess)
    assess = _get_assessment(sess, None)
    flow = ctx.get("etl_flow") or {}
    plan = plan_override if isinstance(plan_override, dict) and plan_override.get("datasets") is not None else flow.get("plan")
    if not isinstance(plan, dict) or not plan.get("datasets"):
        return {"ok": False, "error": "NO_PLAN", "message": "Create a plan first (POST /etl/plan)."}

    blocked = plan.get("blocked") or []
    if blocked:
        return {
            "ok": False,
            "error": "PLAN_BLOCKED",
            "message": "Plan has blocking issues; resolve required columns or rules first.",
            "blocked": blocked,
        }

    preview = build_impact_preview(assess or {}, plan)
    flow = ctx.setdefault("etl_flow", {})
    flow["phase"] = "preview_ready"
    flow["approved_plan"] = plan
    flow["preview"] = preview
    save_session(sess)
    return {"ok": True, "session_id": sid, "preview": preview, "approved_plan": plan}


def etl_generate_code(session_id: str, engine: str = "python", sql_dialect: str = "tsql") -> Dict[str, Any]:
    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    ctx = _ctx(sess)
    assess = _get_assessment(sess, None)
    flow = ctx.get("etl_flow") or {}
    plan = flow.get("approved_plan")
    if not isinstance(plan, dict) or not plan.get("datasets"):
        return {"ok": False, "error": "NO_APPROVED_PLAN", "message": "Confirm the plan first (POST /etl/confirm)."}

    eng = (engine or "python").lower()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_dir = os.path.join(root, "output", "etl_code", _safe_segment(sid))
    os.makedirs(out_dir, exist_ok=True)
    pid = _safe_segment(str(plan.get("plan_id") or "plan"))

    if eng == "python":
        code = generate_python_etl(plan, assess or {})
        ok, errs = validate_python_source(code)
        fname = f"etl_{pid}.py"
        ext = "python"
    elif eng in ("sql", "tsql", "ansi"):
        from agent.etl_pipeline.sql_codegen import generate_sql_etl
        from agent.etl_pipeline.validate_sql import validate_sql_basic

        dialect = (sql_dialect or "tsql").lower()
        if eng == "ansi":
            dialect = "ansi"
        elif eng == "tsql":
            dialect = "tsql"
        code = generate_sql_etl(plan, assess or {}, dialect=dialect)
        ok, errs = validate_sql_basic(code)
        fname = f"etl_{pid}.sql"
        ext = "sql"
    elif eng in ("spark", "pyspark"):
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl

        code = generate_pyspark_etl(plan, assess or {})
        ok, errs = validate_python_source(code)
        fname = f"etl_{pid}_spark.py"
        ext = "pyspark"
    elif eng == "adf":
        from agent.etl_pipeline.adf_codegen import generate_adf_mapping_flow
        from agent.etl_pipeline.validate_adf import validate_adf_json

        obj = generate_adf_mapping_flow(plan, assess or {})
        code = json.dumps(obj, indent=2)
        ok, errs = validate_adf_json(obj)
        fname = f"etl_{pid}.adf.json"
        ext = "adf"
    else:
        return {"ok": False, "error": "UNSUPPORTED_ENGINE", "message": f"Unknown engine: {eng}"}

    abs_path = os.path.join(out_dir, fname)
    with open(abs_path, "w", encoding="utf-8") as f:
        f.write(code)

    rel = os.path.relpath(abs_path, root).replace("\\", "/")
    flow = ctx.setdefault("etl_flow", {})
    flow["phase"] = "code_ready"
    flow["code"] = code
    flow["target_engine"] = eng
    flow["validation_ok"] = ok
    flow["validation_errors"] = errs
    flow["artifact_rel_path"] = rel
    save_session(sess)

    return {
        "ok": True,
        "session_id": sid,
        "engine": eng,
        "format": ext,
        "code": code,
        "validation_ok": ok,
        "validation_errors": errs,
        "artifact_rel_path": rel,
    }
