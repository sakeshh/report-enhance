from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional


class SessionJSONEncoder(json.JSONEncoder):
    """Handles pandas Timestamps, datetime objects, and numpy scalars."""

    def default(self, obj: Any) -> Any:
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        if hasattr(obj, "item") and callable(obj.item):
            # Handles numpy types (int64, float64, etc.)
            try:
                return obj.item()
            except Exception:
                pass
        return super().default(obj)


def _db_path() -> str:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_dir = os.path.join(root, "output")
    os.makedirs(out_dir, exist_ok=True)
    return os.path.join(out_dir, "chat_sessions.sqlite3")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path(), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
          session_id TEXT PRIMARY KEY,
          created_at REAL NOT NULL,
          updated_at REAL NOT NULL,
          payload_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS experiences (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          session_id TEXT NOT NULL,
          ts REAL NOT NULL,
          user_text TEXT,
          action TEXT,
          success INTEGER,
          notes TEXT,
          FOREIGN KEY(session_id) REFERENCES sessions(session_id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_experiences_session_ts ON experiences(session_id, ts DESC)")
    return conn


# FIX (2026-05-07): New sessions now initialise last_step as
# 'awaiting_source_selection' instead of 'unknown'.  This ensures the
# guard_fresh_session_fallback() in routing_guards.py fires correctly and
# the frontend status display never shows 'unknown'.
_DEFAULT_SESSION_PAYLOAD: Dict[str, Any] = {
    "selected_source": None,
    "selected_blob_files": [],
    "selected_local_files": [],
    "selected_tables": [],
    "selected_table": None,
    "last_assessment_result": None,
    "last_assessment_signature": None,
    "last_assessment_datasets": [],
    "last_step": "awaiting_source_selection",  # FIX: was absent / 'unknown'
    "selected_db_location_index": None,
    "selected_blob_location_index": None,
    "selected_fs_location_index": None,
}


def load_session(session_id: str) -> Dict[str, Any]:
    sid = (session_id or "default").strip() or "default"
    now = time.time()
    conn = _connect()
    try:
        row = conn.execute("SELECT payload_json FROM sessions WHERE session_id = ?", (sid,)).fetchone()
        if not row:
            # Brand-new session — persist immediately with correct defaults
            payload = dict(_DEFAULT_SESSION_PAYLOAD)
            payload["session_id"] = sid
            conn.execute(
                "INSERT INTO sessions (session_id, created_at, updated_at, payload_json) VALUES (?,?,?,?)",
                (sid, now, now, json.dumps(payload, cls=SessionJSONEncoder)),
            )
            conn.commit()
            return payload
        payload = json.loads(row[0])
        payload["session_id"] = sid
        # Back-fill missing last_step for sessions created before this fix
        if payload.get("last_step") in (None, "", "unknown") and not payload.get("selected_source"):
            payload["last_step"] = "awaiting_source_selection"
        return payload
    finally:
        conn.close()


def save_session(session_id_or_payload: str | Dict[str, Any], payload: Optional[Dict[str, Any]] = None) -> None:
    if isinstance(session_id_or_payload, dict):
        payload = session_id_or_payload
        sid = str(payload.get("session_id") or "default")
    else:
        sid = str(session_id_or_payload or "default").strip() or "default"
        if payload is None:
            payload = {}

    now = time.time()
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO sessions (session_id, created_at, updated_at, payload_json)
            VALUES (?,?,?,?)
            ON CONFLICT(session_id) DO UPDATE SET updated_at=excluded.updated_at,
                                                   payload_json=excluded.payload_json
            """,
            (sid, now, now, json.dumps(payload, cls=SessionJSONEncoder)),
        )
        conn.commit()
    finally:
        conn.close()


def list_sessions(limit: int = 50) -> List[Dict[str, Any]]:
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT session_id, created_at, updated_at FROM sessions ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [
            {
                "session_id": r[0],
                "created_at": r[1],
                "updated_at": r[2],
            }
            for r in rows
        ]
    finally:
        conn.close()


def reset_session(session_id: str) -> Dict[str, Any]:
    """Wipe a session back to clean defaults and persist."""
    payload = dict(_DEFAULT_SESSION_PAYLOAD)
    save_session(session_id, payload)
    return payload


def add_experience(
    session_id: str,
    user_text: Optional[str],
    action: Optional[str],
    success: bool,
    notes: Optional[str] = None,
) -> None:
    sid = (session_id or "default").strip() or "default"
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO experiences (session_id, ts, user_text, action, success, notes) VALUES (?,?,?,?,?,?)",
            (sid, time.time(), user_text, action, int(success), notes),
        )
        conn.commit()
    finally:
        conn.close()


def list_recent_experiences(
    session_id: str,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    sid = (session_id or "default").strip() or "default"
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT ts, user_text, action, success, notes FROM experiences "
            "WHERE session_id = ? ORDER BY ts DESC LIMIT ?",
            (sid, limit),
        ).fetchall()
        return [
            {
                "ts": r[0],
                "user_text": r[1],
                "action": r[2],
                "success": bool(r[3]),
                "notes": r[4],
            }
            for r in rows
        ]
    finally:
        conn.close()


# Backward compatibility aliases
log_experience = add_experience
get_experiences = list_recent_experiences
