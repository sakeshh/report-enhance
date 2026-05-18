"""
Router Orchestrator — unified entry point for all intent routing.

Layered routing strategy:
  Layer 0  → Fresh-session fallback guard    (NEW — catches empty-context turns)
  Layer 0b → Source keyword normalisation    (NEW — "blob" → "select source blob")
  Layer 1  → Adversarial / safety guard      (rule-based, unbypassable)
  Layer 1b → Code generation guard           (rule-based, unbypassable)
  Layer 1c → General OOD keyword guard       (rule-based)
  Layer 2a → Primary keyword classifier      (existing code, free, 0ms)
  Layer 2b → Fallback keyword heuristics     (free, 0ms)
  Layer 3  → LangChain ToolCallingAgent      (PRIMARY agentic router, ~100 tokens)
             └─ Falls back to legacy LLM JSON router if LangChain unavailable
  Layer 4  → Final fallback (return None)

FIX LOG (2026-05-07)
---------------------
- Added Layer 0: guard_fresh_session_fallback() fires before any routing when
  the session has no selected_source, no assessment, and last_step is unknown.
  This prevents the clarification-mode dead-end on fresh sessions.
- Added Layer 0b: normalize_source_message() rewrites bare source keywords
  ("blob", "azure", "db", "local" …) to their canonical deterministic commands
  before keyword matching so Layer 2a always receives a parseable message.
- Both helpers are imported from routing_guards to keep logic centralised.

Usage:
    from agent.router_orchestrator import route_message
    result = route_message(user_message, context)
"""
from __future__ import annotations
import logging
from typing import Any, Dict, Optional

from agent.conversational_intents import (
    classify_intent,
    fallback_router_intent,
    _is_adversarial,
    _is_ood,
    _is_etl_generate,
    _is_etl_show_plan,
    _is_etl_approve,
    _is_etl_download,
    _is_etl_capture_rules,
)
from agent.llm_router import classify_intent_for_chat
from agent.agent_system_prompt import OUT_OF_SCOPE_REPLY, ADVERSARIAL_REPLY
from agent.routing_guards import (
    normalize_source_message,
    guard_fresh_session_fallback,
)

logger = logging.getLogger(__name__)

# ── Hardcoded code-generation guard (triple safety net) ─────────────────────
_CODE_KEYWORDS = (
    "generate code", "write code", "etl code", "generate etl code",
    "generate etl", "write etl", "create etl", "build etl",
    "python code", "python script", "write python", "write a python",
    "write sql", "generate sql", "sql script",
    "write script", "generate script", "create script",
    "write a script", "give me code", "give code", "show me code",
    "write me code", "code for this", "code to fix", "code to clean",
    "write pipeline", "build pipeline", "create pipeline",
    "generate pipeline", "build a pipeline", "write a pipeline",
    "pyspark", "spark code", "write pandas", "pandas code",
    "write dbt", "generate dbt", "dbt model",
    "write airflow", "generate airflow", "airflow dag",
    "write dag", "generate dag",
    "automate this", "automate the fix", "write automation",
)

_CODE_OOS_REPLY = (
    "I can't generate ETL code or scripts. "
    "I only analyse data quality issues from your assessment.\n\n"
    "Try asking:\n"
    "- 'What are the top issues in my data?'\n"
    "- 'Which datasets should I fix first?'\n"
    "- 'Show me null issues only'\n"
    "- 'Generate a DQ report'"
)


def route_message(
    message: str,
    context: Dict[str, Any],
    use_llm_fallback: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Route a user message through all intent layers.

    Returns dict with: intent, tool, reason, source
    Returns None if no layer matched.
    """
    if not message or not message.strip():
        return None

    # ── Layer 0: Fresh-session fallback (NEW) ────────────────────────────────
    # Intercept ambiguous messages in completely empty sessions before any
    # routing occurs.  Sends the user to source selection immediately.
    guard_reply = guard_fresh_session_fallback(message, context)
    if guard_reply is not None:
        logger.info("Router: fresh-session fallback fired")
        return {
            "intent": -1,
            "tool": "none",
            "reason": "fresh_session_fallback",
            "source": "routing_guard",
            **guard_reply,
        }

    # ── Layer 0b: Source keyword normalisation (NEW) ─────────────────────────
    # Rewrite "blob", "azure", "database", "local" etc. to their canonical
    # deterministic commands so Layer 2a can always match them.
    message = normalize_source_message(message, context)

    low = message.lower().strip()

    # ── Layer 1a: Adversarial guard ──────────────────────────────────────────
    if _is_adversarial(low):
        logger.info("Router: adversarial detected")
        return {
            "intent": 8,
            "tool": "none",
            "reason": "adversarial_policy",
            "source": "safety_guard",
            "reply": ADVERSARIAL_REPLY,
        }

    # ── Layer 1b: ETL pipeline intents (route to etl_handlers via chat graph) ─
    if _is_etl_download(low):
        return {"intent": 13, "reason": "etl_download", "source": "etl_router"}
    if _is_etl_approve(low):
        return {"intent": 12, "reason": "etl_approve", "source": "etl_router"}
    if _is_etl_show_plan(low):
        return {"intent": 11, "reason": "etl_show_plan", "source": "etl_router"}
    if _is_etl_capture_rules(low):
        return {"intent": 14, "reason": "etl_capture_rules", "source": "etl_router"}
    if _is_etl_generate(low) or any(k in low for k in _CODE_KEYWORDS):
        return {"intent": 10, "reason": "etl_generate", "source": "etl_router"}

    # ── Layer 1c: General OOD keyword guard ──────────────────────────────────
    if _is_ood(low):
        logger.info("Router: out-of-domain keyword detected")
        return {
            "intent": 7,
            "tool": "none",
            "reason": "out_of_domain_keyword",
            "source": "safety_guard",
            "reply": OUT_OF_SCOPE_REPLY,
        }

    # ── Layer 2a: Primary keyword classifier ─────────────────────────────────
    result = classify_intent(message, context)
    if result:
        result.setdefault("source", "keyword")
        logger.info("Router: keyword match → intent=%d", result.get("intent"))
        return result

    # ── Layer 2b: Fallback keyword heuristics ────────────────────────────────
    result = fallback_router_intent(message, context)
    if result:
        result.setdefault("source", "keyword_fallback")
        logger.info("Router: keyword fallback → intent=%d", result.get("intent"))
        return result

    # ── Layer 3: Agentic LLM Router (fires only on keyword miss) ─────────────
    # Uses LangChain ToolCallingAgent as primary; legacy JSON router as fallback.
    # ~100-150 tokens per call. NEVER sends raw dataset rows to the LLM.
    if use_llm_fallback:
        logger.info("Router: keyword missed → calling agentic LLM router for: %s", message[:80])
        result = classify_intent_for_chat(message, context)
        if result:
            if result.get("tool") == "none" or result.get("intent") == 7:
                result.setdefault("reply", OUT_OF_SCOPE_REPLY)
            return result

    # ── Layer 4: No match ────────────────────────────────────────────────────
    logger.info("Router: no layer matched → message: %s", message[:80])
    return None


def route_and_get_reply(
    specialist_fn,
    message: str,
    context: Dict[str, Any],
    assessment: Dict[str, Any],
    use_llm_formatter: bool = True,
) -> str:
    """
    Convenience wrapper: route → call specialist → optionally format with LLM.
    """
    raw = specialist_fn(assessment, message)

    if use_llm_formatter and raw:
        try:
            from agent.llm_formatter import format_specialist_output
            return format_specialist_output(raw, message)
        except Exception as exc:
            logger.warning("Formatter error, using raw output: %s", exc)

    return raw
