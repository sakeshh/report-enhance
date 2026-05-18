"""
Legacy module — delegates to etl_chat_router (full etl_handlers pipeline).
Used when importing _node_convo_etl_guidance for assessment-based quick codegen.
"""
from __future__ import annotations

from typing import Any, Dict


def _node_convo_etl_guidance(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Full ETL plan + validated codegen path (same as Pipeline UI).
    Prefer chat action `build_etl_plan` + `generate_etl_code` in chat_graph.
    """
    sid = str(state.get("session_id") or "default")
    from agent.etl_chat_router import chat_build_etl_plan, chat_confirm_etl_plan, chat_generate_etl_code

    ctx: Dict[str, Any] = (state.get("session") or {}).get("context") or {}
    if not isinstance(ctx.get("last_assessment_result"), dict):
        return {
            "reply": (
                "No assessment found. Run a **data quality assessment** first, "
                "then ask for ETL code."
            ),
            "payload": {"step": "etl_guidance", "code": None},
        }

    plan_msg = chat_build_etl_plan(sid, engine="python")
    confirm_msg = chat_confirm_etl_plan(sid)
    if "Could not confirm" in confirm_msg or confirm_msg.startswith("❌"):
        return {"reply": f"{plan_msg}\n\n{confirm_msg}", "payload": {"step": "etl_guidance", "code": None}}

    gen_msg = chat_generate_etl_code(sid, engine="python")
    from agent.session_store import load_session

    flow = ((load_session(sid).get("context") or {}).get("etl_flow") or {})
    code = flow.get("code")

    return {
        "reply": f"{plan_msg}\n\n{confirm_msg}\n\n{gen_msg}",
        "payload": {
            "step": "etl_guidance",
            "etl_code": code,
            "plan_id": str((flow.get("approved_plan") or {}).get("plan_id") or ""),
            "validation_ok": flow.get("validation_ok"),
        },
    }
