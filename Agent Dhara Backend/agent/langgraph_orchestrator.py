"""
LangGraph-based multi-agent orchestration for Agent Dhara Backend.

This module defines a small LangGraph workflow:
- Route user request (MasterAgent.plan)
- Extract per selected source location (ExtractionAgent, parallel)

**Note:** Interactive chat uses a separate graph in `agent.chat_graph` (including
`classify_intent` / conversational specialists); this orchestrator covers batch extraction/assessment flows.

The workflow is designed to be callable from:
- CLI glue code (future)
- FastAPI endpoints (future)
- other Python code (unit tests, scripts)
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional, Sequence, TypedDict

try:
    from langgraph.graph import END, StateGraph
except Exception as e:  # pragma: no cover
    END = None  # type: ignore
    StateGraph = None  # type: ignore
    _LANGGRAPH_IMPORT_ERROR = e
else:
    _LANGGRAPH_IMPORT_ERROR = None

from agent.master_agent import MasterAgent
from agent.data_quality_agent import DataQualityAgent, dq_result_to_dict
from agent.dq_recommendations_agent import DQRecommendationsAgent, dq_recommendations_to_dict
from agent.transformation_suggester import suggest_transformations


class OrchestratorState(TypedDict, total=False):
    """
    Shared state passed between LangGraph nodes.
    """

    # Inputs
    user_request: str
    sources_path: str
    selected_sources: List[str]
    stream_records: List[Dict[str, Any]]
    stream_name: str
    job_id: str
    session_id: str
    source_connections: Dict[str, Any]

    # Derived / intermediate
    plan: Dict[str, Any]
    selected_location_count: int

    # Outputs
    extractions: List[Dict[str, Any]]
    extraction_errors: List[Dict[str, Any]]
    data_quality: Dict[str, Any]
    dq_recommendations: Dict[str, Any]
    transform_suggestions: Dict[str, Any]
    timings: Dict[str, Any]
    request_id: str
    approved_semantics: Dict[str, Dict[str, str]]


def _merge_timings(state: OrchestratorState, extra: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(state.get("timings") or {})
    out.update(extra or {})
    return out
def _node_route(state: OrchestratorState) -> OrchestratorState:
    t0 = time.time()
    job_id = state.get("job_id")
    if job_id:
        try:
            from agent.jobs_store import update_job_progress
            update_job_progress(job_id, 15)
        except Exception:
            pass
    master = MasterAgent()
    p = master.plan(state.get("user_request", ""))
    out: OrchestratorState = {
        "plan": {
            "do_extract": p.do_extract,
            "do_dq_check": p.do_dq_check,
            "do_dq_recommendations": p.do_dq_recommendations,
            "do_transform": p.do_transform,
        },
    }
    out["timings"] = _merge_timings(state, {"route_ms": int((time.time() - t0) * 1000)})
    return out


async def _node_extract_async(state: OrchestratorState) -> OrchestratorState:
    t0 = time.time()
    job_id = state.get("job_id")
    if job_id:
        try:
            from agent.jobs_store import update_job_progress
            update_job_progress(job_id, 40)
        except Exception:
            pass

    session_id = state.get("session_id")
    if not session_id and job_id:
        try:
            from agent.jobs_store import fetch_job
            job = fetch_job(job_id)
            if job and "input" in job:
                session_id = job["input"].get("sessionId") or job["input"].get("session_id")
        except Exception:
            pass

    context = {}
    if session_id:
        try:
            from agent.session_store import load_session
            sess = load_session(session_id)
            context = sess.get("context") or {}
        except Exception as e:
            print(f"[ERROR] Failed to load session {session_id}: {e}")

    user_req = (state.get("user_request") or "").lower()
    is_sql = "table" in user_req or "sql" in user_req or "database" in user_req
    is_local = "local file" in user_req or "local_file" in user_req or "filesystem" in user_req
    is_blob = ("file" in user_req and not is_local) or "blob" in user_req or "azure_blob" in user_req

    sources_path = state.get("sources_path", "config/sources.yaml")
    from agent.master_agent import load_sources_config
    source_root = load_sources_config(sources_path)

    results = []
    errors = []
    location_count = 0

    if is_sql and (context.get("selected_tables") or context.get("last_table_list")):
        selected_tables = context.get("selected_tables") or context.get("last_table_list") or []
        if isinstance(selected_tables, str):
            selected_tables = [selected_tables]
        db_locs = [loc for loc in (source_root.get("locations") or []) if (loc.get("type") or "").lower() == "database"]
        if selected_tables and db_locs:
            db_idx = int(context.get("selected_db_location_index") or 0)
            db_idx = max(0, min(db_idx, len(db_locs) - 1))
            loc = db_locs[db_idx]
            conn_cfg = loc.get("connection") or {}
            
            from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector
            from agent.intelligent_data_assessment import load_and_profile
            from agent.chat_graph import _override_source_root_for_datasets
            
            try:
                conn = AzureSQLPythonNetConnector(conn_cfg)
                dfs = {}
                for t in selected_tables:
                    dfs[t] = conn.load_table(t, max_rows=None)
                    
                approved_sem = context.get("approved_semantics") or state.get("approved_semantics")
                result = load_and_profile(
                    {"name": source_root.get("name") or "source", "locations": []},
                    additional_data=dfs,
                    job_id=job_id,
                    max_rows=None,
                    db_connectors={t: conn for t in selected_tables},
                    approved_semantics=approved_sem,
                )
                
                label = (
                    (loc.get("id") or loc.get("label") or loc.get("name") or "").strip()
                    or (conn_cfg.get("database") or "").strip()
                    or "__default__"
                )
                _override_source_root_for_datasets(result, list(dfs.keys()), f"__database__:{label}")
                
                from agent.extraction_agent import ExtractionResult
                results.append(
                    ExtractionResult(
                        source_name=label,
                        location_type="database",
                        result=result,
                    )
                )
                location_count = 1
            except Exception as e:
                errors.append({"source": "database", "type": "database", "error": str(e)})

    elif is_blob and (context.get("selected_blob_files") or context.get("last_blob_list")):
        selected_blob_files = context.get("selected_blob_files") or context.get("last_blob_list") or []
        if isinstance(selected_blob_files, str):
            selected_blob_files = [selected_blob_files]
        blob_locs = [loc for loc in (source_root.get("locations") or []) if (loc.get("type") or "").lower() == "azure_blob"]
        if selected_blob_files and blob_locs:
            blob_idx = int(context.get("selected_blob_location_index") or 0)
            blob_idx = max(0, min(blob_idx, len(blob_locs) - 1))
            loc = blob_locs[blob_idx]
            
            from agent.mcp_clients import _single_location_config
            from agent.mcp_interface import load_selected_blob_datasets, run_assessment
            
            try:
                cfg_text = _single_location_config({"name": source_root.get("name") or "source"}, loc)
                dfs = load_selected_blob_datasets(
                    cfg_text,
                    location_index=0,
                    blob_names=list(selected_blob_files),
                    max_rows=None,
                    max_bytes=10_737_418_240, # 10GB
                )
                
                approved_sem = context.get("approved_semantics") or state.get("approved_semantics")
                result = run_assessment(cfg_text, additional_data=dfs, job_id=job_id, approved_semantics=approved_sem)
                
                label = (
                    (loc.get("id") or loc.get("label") or loc.get("name") or "").strip()
                    or "blob_data"
                )
                
                from agent.extraction_agent import ExtractionResult
                results.append(
                    ExtractionResult(
                        source_name=label,
                        location_type="azure_blob",
                        result=result,
                    )
                )
                location_count = 1
            except Exception as e:
                errors.append({"source": "azure_blob", "type": "azure_blob", "error": str(e)})

    elif is_local and (context.get("selected_local_files") or context.get("last_local_file_list")):
        selected_local_files = context.get("selected_local_files") or context.get("last_local_file_list") or []
        if isinstance(selected_local_files, str):
            selected_local_files = [selected_local_files]
        fs_locs = [loc for loc in (source_root.get("locations") or []) if (loc.get("type") or "").lower() in ("filesystem", "local_fs")]
        if selected_local_files:
            fs_idx = int(context.get("selected_fs_location_index") or 0)
            if fs_locs:
                fs_idx = max(0, min(fs_idx, len(fs_locs) - 1))
                loc = fs_locs[fs_idx]
                root = loc.get("path") or ""
            else:
                root = context.get("local_files_root") or ""
                
            import os
            import json
            import pandas as pd
            from agent.intelligent_data_assessment import load_and_profile
            from agent.chat_graph import _override_source_root_for_datasets
            
            try:
                dfs = {}
                for name in selected_local_files:
                    p = os.path.join(root, name) if root else name
                    if not os.path.isfile(p):
                        p = os.path.abspath(name)
                        if not os.path.isfile(p):
                            raise FileNotFoundError(f"Local file not found: {name}")
                    low = p.lower()
                    if low.endswith(".csv"):
                        df = pd.read_csv(p, low_memory=False)
                    elif low.endswith(".tsv"):
                        df = pd.read_csv(p, sep="\t", low_memory=False)
                    elif low.endswith(".jsonl"):
                        rows = []
                        with open(p, "r", encoding="utf-8") as f:
                            for line in f:
                                line = line.strip()
                                if not line:
                                    continue
                                try:
                                    rows.append(json.loads(line))
                                except Exception:
                                    rows.append({"value": line})
                        df = pd.json_normalize(rows, max_level=1) if rows else pd.DataFrame()
                    else:
                        if low.endswith((".xlsx", ".xls")):
                            df = pd.read_excel(p)
                        elif low.endswith(".parquet"):
                            df = pd.read_parquet(p)
                        else:
                            df = pd.read_json(p)
                    dfs[name] = df
                    
                approved_sem = context.get("approved_semantics") or state.get("approved_semantics")
                result = load_and_profile(
                    {"name": "local", "locations": []},
                    additional_data=dfs,
                    max_rows=None,
                    approved_semantics=approved_sem,
                )
                if root:
                    _override_source_root_for_datasets(result, list(dfs.keys()), os.path.abspath(root))
                    
                from agent.extraction_agent import ExtractionResult
                results.append(
                    ExtractionResult(
                        source_name="local_files",
                        location_type="filesystem",
                        result=result,
                    )
                )
                location_count = 1
            except Exception as e:
                errors.append({"source": "local_files", "type": "filesystem", "error": str(e)})

    # Default fallback
    if not results and not errors:
        master = MasterAgent()
        source_root, locations = master.load_and_select_sources(
            sources_path=sources_path,
            selected_sources=state.get("selected_sources") or None,
            user_request=state.get("user_request") or "",
        )
        location_count = len(locations)

        extraction_agent = master.registry["extraction"]
        results, errors = await extraction_agent.extract_many(
            source_root=source_root,
            locations=locations,
            parallel=True,
            stream_records=state.get("stream_records"),
            stream_name=state.get("stream_name") or "stream",
            job_id=state.get("job_id"),
            approved_semantics=state.get("approved_semantics"),
        )

    # Normalize to JSON-serializable output (no dataclasses)
    extractions_out: List[Dict[str, Any]] = []
    for r in results:
        extractions_out.append(
            {
                "source": r.source_name,
                "location_type": r.location_type,
                "result": r.result,
            }
        )

    return {
        "selected_location_count": location_count,
        "extractions": extractions_out,
        "extraction_errors": errors,
        "timings": _merge_timings(state, {"extract_ms": int((time.time() - t0) * 1000)}),
    }


def _node_extract(state: OrchestratorState) -> OrchestratorState:
    """
    Sync wrapper around the async extraction node for LangGraph.
    """
    return asyncio.run(_node_extract_async(state))


def _node_dq_check(state: OrchestratorState) -> OrchestratorState:
    t0 = time.time()
    job_id = state.get("job_id")
    if job_id:
        try:
            from agent.jobs_store import update_job_progress
            update_job_progress(job_id, 70)
        except Exception:
            pass
    dq_agent = DataQualityAgent()
    extractions = state.get("extractions") or []
    merged = dq_agent.run_from_extractions(extractions)
    return {
        "data_quality": dq_result_to_dict(merged),
        "timings": _merge_timings(state, {"dq_check_ms": int((time.time() - t0) * 1000)}),
    }


def _node_dq_recommend(state: OrchestratorState) -> OrchestratorState:
    """
    LLM-assisted cleaning recommendations based on merged DQ issues.
    """
    t0 = time.time()
    job_id = state.get("job_id")
    if job_id:
        try:
            from agent.jobs_store import update_job_progress
            update_job_progress(job_id, 85)
        except Exception:
            pass
    dq = state.get("data_quality") or {}
    agent = DQRecommendationsAgent()
    rec, _usage = agent.recommend(merged_dq=dq, user_intent=state.get("user_request") or "")
    return {
        "dq_recommendations": dq_recommendations_to_dict(rec),
        "timings": _merge_timings(state, {"dq_recommend_ms": int((time.time() - t0) * 1000)}),
    }


def _node_transform_suggest(state: OrchestratorState) -> OrchestratorState:
    """
    Build transformation suggestions per extracted source.
    """
    t0 = time.time()
    job_id = state.get("job_id")
    if job_id:
        try:
            from agent.jobs_store import update_job_progress
            update_job_progress(job_id, 95)
        except Exception:
            pass
    extractions = state.get("extractions") or []
    out_by_source: Dict[str, Any] = {}
    for ex in extractions:
        source = str(ex.get("source") or ex.get("source_name") or "source")
        res = ex.get("result") if isinstance(ex.get("result"), dict) else {}
        if not isinstance(res, dict):
            out_by_source[source] = {"error": "Missing extraction result"}
            continue
        try:
            out_by_source[source] = suggest_transformations(res)
        except Exception as e:
            out_by_source[source] = {"error": str(e)}
    return {
        "transform_suggestions": {"sources": out_by_source},
        "timings": _merge_timings(state, {"transform_suggest_ms": int((time.time() - t0) * 1000)}),
    }


def _route_after_plan(state: OrchestratorState) -> str:
    plan = state.get("plan") or {}
    do_extract = bool(plan.get("do_extract", True))
    do_dq = bool(plan.get("do_dq_check", True))
    do_rec = bool(plan.get("do_dq_recommendations", False))
    do_transform = bool(plan.get("do_transform", False))

    # If extraction is requested, always do it first.
    if do_extract:
        return "extract"

    # If caller provided extractions in state (e.g. from a session), allow DQ/transform without re-extract.
    if (state.get("extractions") or []) and do_dq:
        return "dq_check"
    if (state.get("extractions") or []) and do_rec:
        return "dq_recommend"
    if (state.get("extractions") or []) and do_transform:
        return "transform_suggest"
    return END  # type: ignore[return-value]


def _route_after_extract(state: OrchestratorState) -> str:
    plan = state.get("plan") or {}
    do_dq = bool(plan.get("do_dq_check", True))
    do_rec = bool(plan.get("do_dq_recommendations", False))
    do_transform = bool(plan.get("do_transform", False))
    if do_dq:
        return "dq_check"
    if do_rec:
        # Recommendations depend on merged DQ output.
        return "dq_check"
    if do_transform:
        return "transform_suggest"
    return END  # type: ignore[return-value]


def _route_after_dq(state: OrchestratorState) -> str:
    plan = state.get("plan") or {}
    do_rec = bool(plan.get("do_dq_recommendations", False))
    do_transform = bool(plan.get("do_transform", False))
    if do_rec:
        return "dq_recommend"
    return "transform_suggest" if do_transform else END  # type: ignore[return-value]


def _route_after_dq_recommend(state: OrchestratorState) -> str:
    plan = state.get("plan") or {}
    do_transform = bool(plan.get("do_transform", False))
    return "transform_suggest" if do_transform else END  # type: ignore[return-value]


def build_orchestrator_graph():
    """
    Build and compile the LangGraph orchestrator.
    """
    if _LANGGRAPH_IMPORT_ERROR is not None or StateGraph is None:
        raise ImportError(
            "LangGraph is not installed (or failed to import). "
            "Install with: pip install -r requirements.txt"
        ) from _LANGGRAPH_IMPORT_ERROR
    g = StateGraph(OrchestratorState)
    g.add_node("route", _node_route)
    g.add_node("extract", _node_extract)
    g.add_node("dq_check", _node_dq_check)
    g.add_node("dq_recommend", _node_dq_recommend)
    g.add_node("transform_suggest", _node_transform_suggest)

    g.set_entry_point("route")
    g.add_conditional_edges("route", _route_after_plan)
    g.add_conditional_edges("extract", _route_after_extract)
    g.add_conditional_edges("dq_check", _route_after_dq)
    g.add_conditional_edges("dq_recommend", _route_after_dq_recommend)
    g.add_edge("transform_suggest", END)
    return g.compile()


def run_orchestrator(
    *,
    user_request: str,
    sources_path: str = "config/sources.yaml",
    selected_sources: Optional[Sequence[str]] = None,
    stream_records: Optional[List[Dict[str, Any]]] = None,
    stream_name: str = "stream",
    request_id: str = "",
    job_id: str = "",
    approved_semantics: Optional[Dict[str, Dict[str, str]]] = None,
    session_id: str = "",
    source_connections: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    High-level convenience wrapper.
    """
    graph = build_orchestrator_graph()
    final = graph.invoke(
        {
            "user_request": user_request,
            "sources_path": sources_path,
            "selected_sources": list(selected_sources or []),
            "stream_records": stream_records,
            "stream_name": stream_name,
            "request_id": request_id or "",
            "job_id": job_id or "",
            "timings": {},
            "approved_semantics": approved_semantics or {},
            "session_id": session_id,
            "source_connections": dict(source_connections or {}),
        }
    )
    if job_id:
        try:
            from agent.jobs_store import update_job_progress
            update_job_progress(job_id, 100)
        except Exception:
            pass
    return dict(final)

