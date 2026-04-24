from __future__ import annotations

from pydantic import BaseModel
from fastapi import APIRouter

from app.api.response import ok
from app.services.agent.orchestrator import AgentOrchestrator
from app.services.session_trace_service import session_trace_service


router = APIRouter()
orchestrator = AgentOrchestrator()


class AgentQueryIn(BaseModel):
    question: str
    session_id: str | None = None
    force: bool = False


@router.post("/query")
async def agent_query(payload: AgentQueryIn):
    result = await orchestrator.process_query(
        question=payload.question,
        session_id=payload.session_id,
        force=payload.force,
    )
    sid = result.get("session_id")
    report = result.get("report") if isinstance(result.get("report"), dict) else {}
    if sid and report:
        session_trace_service.set_latest_report(
            session_id=sid,
            summary=str(report.get("summary") or ""),
            sections=report.get("sections") if isinstance(report.get("sections"), dict) else {},
        )
    return ok(result)

