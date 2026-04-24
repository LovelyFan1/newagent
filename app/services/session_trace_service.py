from __future__ import annotations

import time
import uuid
from typing import Any


class SessionTraceService:
    def __init__(self) -> None:
        self._store: dict[str, dict[str, Any]] = {}

    def ensure_session(self, session_id: str | None = None) -> str:
        sid = (session_id or "").strip() or uuid.uuid4().hex
        if sid not in self._store:
            self._store[sid] = {
                "uploaded_files": [],
                "file_content": "",
                "latest_report_summary": "",
                "latest_report_sections": {},
                "updated_at": time.time(),
            }
        return sid

    def add_uploaded_file(self, *, session_id: str, filename: str, content: str) -> None:
        sid = self.ensure_session(session_id)
        session = self._store[sid]
        files = session.get("uploaded_files")
        if not isinstance(files, list):
            files = []
        files.append(
            {
                "filename": filename,
                "content_preview": (content or "")[:1000],
                "content_length": len(content or ""),
                "uploaded_at": time.time(),
            }
        )
        session["uploaded_files"] = files
        existing = str(session.get("file_content") or "")
        merged = (existing + "\n\n" + (content or "")).strip()
        session["file_content"] = merged[:20000]
        session["updated_at"] = time.time()

    def get_file_content(self, session_id: str | None) -> str:
        sid = (session_id or "").strip()
        if not sid:
            return ""
        session = self._store.get(sid) or {}
        return str(session.get("file_content") or "")

    def set_latest_report(self, *, session_id: str, summary: str, sections: dict[str, Any] | None = None) -> None:
        sid = self.ensure_session(session_id)
        session = self._store[sid]
        session["latest_report_summary"] = (summary or "")[:10000]
        session["latest_report_sections"] = sections or {}
        session["updated_at"] = time.time()

    def get_latest_report(self, session_id: str) -> dict[str, Any]:
        sid = self.ensure_session(session_id)
        session = self._store[sid]
        return {
            "summary": str(session.get("latest_report_summary") or ""),
            "sections": session.get("latest_report_sections") or {},
            "uploaded_files": session.get("uploaded_files") or [],
        }


session_trace_service = SessionTraceService()

