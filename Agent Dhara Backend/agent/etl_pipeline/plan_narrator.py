"""
Plan narrator — plain-English summaries from structured plan evidence.
Tiered: fallback always; LLM for review/joins/M:N when mode=tiered; full LLM when mode=llm.
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

try:
    from openai import AzureOpenAI, OpenAI
except ImportError:
    AzureOpenAI = OpenAI = None  # type: ignore


NARRATOR_SYSTEM = """You are a Senior Data Engineer explaining an ETL plan to a colleague.
Use ONLY numbers from the evidence field. Return JSON:
{
  "engine_explanation": "string",
  "dataset_summaries": { "<dataset>": { "summary": "string", "steps": { "<order>": "string" } } },
  "manual_review_explanations": [{"column": "string", "explanation": "string"}],
  "relationships_summary": "string",
  "overall_readiness": "string"
}
"""


def _get_client():
    az_ep = os.getenv("AZURE_OPENAI_ENDPOINT")
    az_key = os.getenv("AZURE_OPENAI_API_KEY")
    if az_ep and az_key and AzureOpenAI:
        return AzureOpenAI(
            azure_endpoint=az_ep,
            api_key=az_key,
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
        )
    ok = os.getenv("OPENAI_API_KEY")
    if ok and OpenAI:
        return OpenAI(api_key=ok)
    return None


def _build_narrator_payload(plan: Dict[str, Any], *, subset: Optional[str] = None) -> Dict[str, Any]:
    datasets_payload: Dict[str, Any] = {}
    for ds_name, ds_obj in (plan.get("datasets") or {}).items():
        steps = ds_obj.get("steps") or []
        datasets_payload[ds_name] = {
            "steps": [
                {
                    "order": s.get("order"),
                    "column": s.get("column"),
                    "action": s.get("action"),
                    "bucket": s.get("bucket"),
                    "evidence": {
                        k: v
                        for k, v in (s.get("evidence") or {}).items()
                        if v is not None and k != "alternatives"
                    },
                }
                for s in steps[:20]
            ]
        }
    rel = plan.get("relationships") or {}
    payload: Dict[str, Any] = {
        "engine_recommendation": plan.get("engine_recommendation") or {},
        "source_context": plan.get("source_context") or {},
        "relationships": {
            "joins": (rel.get("joins") or [])[:10],
            "many_to_many": (rel.get("many_to_many") or [])[:5],
            "load_order": rel.get("load_order"),
        },
        "datasets": datasets_payload,
        "manual_review": [
            {
                "column": m.get("column"),
                "issue_type": m.get("issue_type"),
                "message": m.get("message"),
            }
            for m in (plan.get("manual_review") or [])[:10]
        ],
        "blocked": plan.get("blocked") or [],
    }
    if subset == "hard":
        payload["datasets"] = {}
        payload["manual_review"] = payload.get("manual_review") or []
    return payload


def _fallback_narration(plan: Dict[str, Any], error: Optional[str] = None) -> Dict[str, Any]:
    eng_rec = plan.get("engine_recommendation") or {}
    datasets = plan.get("datasets") or {}
    rel = plan.get("relationships") or {}

    engine_explanation = (
        f"Recommended engine: {str(eng_rec.get('engine', 'python')).upper()}. "
        f"{eng_rec.get('reason', 'Based on data profile.')}"
    )
    if eng_rec.get("warning"):
        engine_explanation += f" Note: {eng_rec['warning']}"

    dataset_summaries: Dict[str, Any] = {}
    for ds_name, ds_obj in datasets.items():
        steps = ds_obj.get("steps") or []
        step_explanations: Dict[str, str] = {}
        for s in steps:
            ev = s.get("evidence") or {}
            why = ev.get("why_this_action") or f"Apply {s.get('action')} transformation"
            fill = ev.get("recommended_fill")
            fill_note = f" Use {fill} for imputation." if fill else ""
            alts = ev.get("alternatives") or []
            alt_text = f" Alternative: {alts[0]}" if alts else ""
            conf = ev.get("confidence")
            conf_text = f" ({int(float(conf) * 100)}% confidence)" if conf is not None else ""
            step_explanations[str(s.get("order", 0))] = f"{why}.{fill_note}{alt_text}{conf_text}"

        auto = sum(1 for s in steps if s.get("bucket") == "auto")
        rev = sum(1 for s in steps if s.get("bucket") == "review")
        blk = sum(1 for s in steps if s.get("bucket") == "blocked")
        dataset_summaries[ds_name] = {
            "summary": f"{ds_name}: {len(steps)} steps — {auto} auto, {rev} review, {blk} blocked.",
            "steps": step_explanations,
        }

    manual = plan.get("manual_review") or []
    manual_explanations = [
        {
            "column": m.get("column", "unknown"),
            "explanation": m.get("message") or m.get("guidance") or "Manual review required.",
        }
        for m in manual
    ]

    total_auto = sum(
        sum(1 for s in (v.get("steps") or []) if s.get("bucket") == "auto")
        for v in datasets.values()
    )
    total_steps = sum(len(v.get("steps") or []) for v in datasets.values())
    pct = round((total_auto / max(total_steps, 1)) * 100)

    rel_parts: List[str] = []
    if rel.get("join_count"):
        rel_parts.append(f"{rel['join_count']} join(s)")
    if rel.get("mn_count"):
        rel_parts.append(f"{rel['mn_count']} M:N pair(s) need bridge modeling")
    if rel.get("load_order"):
        rel_parts.append(f"load order: {' → '.join(rel['load_order'][:5])}")

    return {
        "engine_explanation": engine_explanation,
        "dataset_summaries": dataset_summaries,
        "manual_review_explanations": manual_explanations,
        "relationships_summary": "; ".join(rel_parts) if rel_parts else "Single-dataset or no joins detected.",
        "overall_readiness": (
            f"{pct}% of transformations are auto-fixable. "
            f"{len(manual)} manual review item(s)."
            + (f" Relationships: {', '.join(rel_parts)}." if rel_parts else "")
        ),
        "_narrator_source": "fallback" if not error else f"fallback_error:{error}",
    }


def _llm_narrate(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    client = _get_client()
    if not client:
        return None
    payload_json = json.dumps(payload, indent=2, default=str)
    if len(payload_json) > 12000:
        payload_json = payload_json[:12000] + "\n... (truncated)"
    try:
        resp = client.chat.completions.create(
            model=os.getenv("AZURE_OPENAI_DEPLOYMENT")
            or os.getenv("OPENAI_MODEL", "gpt-4o"),
            messages=[
                {"role": "system", "content": NARRATOR_SYSTEM},
                {"role": "user", "content": f"Narrate this ETL plan:\n{payload_json}"},
            ],
            temperature=0.2,
            max_tokens=2000,
            response_format={"type": "json_object"},
        )
        raw = resp.choices[0].message.content or "{}"
        parsed = json.loads(raw)
        parsed["_narrator_source"] = "llm"
        return parsed
    except Exception:
        return None


def _merge_narration(base: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in extra.items():
        if k.startswith("_"):
            continue
        if k == "dataset_summaries" and isinstance(v, dict):
            out.setdefault("dataset_summaries", {}).update(v)
        elif v:
            out[k] = v
    return out


def narrate_plan(
    plan: Dict[str, Any],
    *,
    mode: str = "tiered",
    use_llm: bool = False,
) -> Dict[str, Any]:
    """
    mode: fallback | tiered | llm
    tiered = fallback + LLM for manual_review / relationships only (if credentials exist)
    """
    mode = (mode or "tiered").lower()
    base = _fallback_narration(plan)

    if mode == "fallback" and not use_llm:
        return base

    if mode == "llm" or use_llm:
        full = _llm_narrate(_build_narrator_payload(plan))
        return full if full else base

    # tiered: enhance hard sections with LLM
    hard = _llm_narrate(_build_narrator_payload(plan, subset="hard"))
    if hard:
        if hard.get("engine_explanation"):
            base["engine_explanation"] = hard["engine_explanation"]
        if hard.get("relationships_summary"):
            base["relationships_summary"] = hard["relationships_summary"]
        if hard.get("manual_review_explanations"):
            base["manual_review_explanations"] = hard["manual_review_explanations"]
        if hard.get("overall_readiness"):
            base["overall_readiness"] = hard["overall_readiness"]
        base["_narrator_source"] = "tiered_llm"
    else:
        base["_narrator_source"] = "tiered_fallback"

    return base
