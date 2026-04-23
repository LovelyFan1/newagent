from __future__ import annotations

from pydantic import BaseModel
from fastapi import APIRouter

from app.api.response import ok
from app.services.agent.orchestrator import AgentOrchestrator


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
    return ok(result)

