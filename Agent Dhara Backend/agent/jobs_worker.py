from __future__ import annotations

import threading
import time
from typing import Any, Dict, Optional

from agent.jobs_store import add_event, claim_next_job, update_job_status


def _run_job(job: Dict[str, Any]) -> Dict[str, Any]:
    job_id = job.get("job_id")
    kind = job.get("kind")
    inp = job.get("input") or {}
    add_event(job_id=job_id, level="debug", message=f"JobWorker processing {kind}")
    
    if kind == "assess":
        from agent.langgraph_orchestrator import run_orchestrator
        from connectors.source_connector import (
            SourceConnection,
            close_source,
            connect_source,
            validate_view_accessible,
        )

        user_req = inp.get("user_request")
        if not user_req and inp.get("messages"):
            msgs = inp.get("messages")
            if isinstance(msgs, list) and len(msgs) > 0:
                user_req = msgs[-1].get("content")

        source_connections: dict[str, SourceConnection] = {}
        try:
            for cfg in inp.get("source_configs") or []:
                if not isinstance(cfg, dict):
                    continue
                ds_key = str(cfg.get("dataset_name") or "").strip()
                if not ds_key:
                    print("[jobs_worker] skipping source_config without dataset_name")
                    continue
                conn = connect_source(cfg)
                validate_view_accessible(conn)
                source_connections[ds_key] = conn
                print(f"[jobs_worker] Step 0: connected source dataset={ds_key!r}")
        except Exception as e:
            for _c in source_connections.values():
                try:
                    close_source(_c)
                except Exception:
                    pass
            raise RuntimeError(f"Source connector Step 0 failed: {e}") from e

        try:
            state = run_orchestrator(
                user_request=str(user_req or ""),
                sources_path=str(inp.get("sources_path") or "config/sources.yaml"),
                selected_sources=inp.get("selected_sources") or [],
                job_id=job_id,
                session_id=str(inp.get("sessionId") or inp.get("session_id") or "default").strip(),
                source_connections=source_connections or None,
            )

            extractions = state.get("extractions") or []
            merged_result = {
                "datasets": {},
                "relationships": [],
                "data_quality_issues": {
                    "datasets": {},
                    "global_issues": {},
                },
            }
            for ex in extractions:
                res = ex.get("result")
                if isinstance(res, dict):
                    merged_result["datasets"].update(res.get("datasets") or {})
                    merged_result["relationships"].extend(res.get("relationships") or [])
                    dq = res.get("data_quality_issues") or {}
                    merged_result["data_quality_issues"]["datasets"].update(dq.get("datasets") or {})
                    merged_result["data_quality_issues"]["global_issues"].update(dq.get("global_issues") or {})

            try:
                from agent.gx_issue_mapper import map_gx_failures_to_issues, merge_gx_issues_into_assessment
                from agent.gx_runner import run_gx_validation
                from agent.gx_suite_builder import build_gx_suite

                for _ds_name, _src_conn in source_connections.items():
                    try:
                        _suite = build_gx_suite(
                            _ds_name,
                            merged_result,
                            strictness=str(inp.get("gx_strictness") or "standard"),
                        )
                        _gx_result = run_gx_validation(_src_conn, _suite, _ds_name)
                        _gx_issues = map_gx_failures_to_issues(_gx_result)
                        merged_result = merge_gx_issues_into_assessment(merged_result, _gx_issues)
                    except ImportError as _gx_ie:
                        print(f"[jobs_worker] GX step skipped (missing dependency): {_gx_ie}")
                    except Exception as _gx_err:
                        print(f"[jobs_worker] GX step non-fatal error for {_ds_name!r}: {_gx_err}")
            except Exception as _gx_outer:
                print(f"[jobs_worker] GX pipeline wrapper error (non-fatal): {_gx_outer}")

            from agent.chat_graph import _build_report_tables_markdown, _render_report_html, _write_report_artifacts

            report_md = _build_report_tables_markdown(merged_result)
            report_html = _render_report_html(merged_result)
            artifacts = _write_report_artifacts(
                result=merged_result, report_markdown=report_md, report_html=report_html
            )

            try:
                from agent.session_store import load_session, save_session

                sid = str(inp.get("sessionId") or inp.get("session_id") or "default").strip()
                sess = load_session(sid)
                ctx = sess.setdefault("context", {})
                ctx["last_assessment_result"] = merged_result
                save_session(sess)
            except Exception:
                pass

            return {
                "result": merged_result,
                "report_markdown": report_md,
                "report_html": report_html,
                "report_files": artifacts,
            }
        finally:
            for _c in source_connections.values():
                try:
                    close_source(_c)
                except Exception:
                    pass

    if kind == "chat":
        from agent.chat_graph import run_chat

        return run_chat(
            session_id=str(inp.get("session_id") or "default"), 
            message=str(inp.get("message") or ""),
            job_id=job_id,
        )

    raise ValueError(f"Unknown job kind: {kind}")


class JobWorker:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            job = claim_next_job()
            if not job:
                time.sleep(0.25)
                continue
            job_id = job["job_id"]
            try:
                add_event(job_id=job_id, level="info", message="started")
                result = _run_job(job)
                update_job_status(job_id, status="succeeded", result=result)
                add_event(job_id=job_id, level="info", message="succeeded")
            except Exception as e:
                update_job_status(job_id, status="failed", error=str(e))
                add_event(job_id=job_id, level="error", message="failed", data={"error": str(e)})

