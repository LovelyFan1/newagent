from __future__ import annotations

import pytest


def test_intent_detector_gibberish():
    from app.services.agent.intent import IntentDetector

    d = IntentDetector()
    assert d.is_gibberish("") is True
    assert d.is_gibberish("嗯") is True
    assert d.is_gibberish("比亚迪分析") is False


def test_intent_detector_extracts():
    from app.services.agent.intent import IntentDetector

    d = IntentDetector()
    assert "比亚迪" in d.extract_enterprises("比亚迪 2022 年财务风险分析")
    tr = d.extract_time_range("比亚迪 2022 年财务风险分析")
    assert tr is not None and tr.kind == "year" and tr.year == 2022


@pytest.mark.anyio
async def test_agent_orchestrator_clarification_no_enterprise(monkeypatch):
    from app.services.agent.orchestrator import AgentOrchestrator
    from app.services.agent.llm_gateway import LLMGateway

    orch = AgentOrchestrator(llm=LLMGateway(timeout_s=1, max_retries=1))
    # Force LLM disabled to avoid network calls
    orch.llm._enabled = False  # type: ignore[attr-defined]
    orch.llm._client = None  # type: ignore[attr-defined]

    result = await orch.process_query(question="帮我做个风险分析", session_id="s1")
    assert result["status"] == "needs_clarification"
    assert result["clarification"]["required"] is True


@pytest.mark.anyio
async def test_agent_api_e2e_offline(client):
    r = await client.post("/api/v1/agent/query", json={"question": "比亚迪 2022 年财务风险分析", "session_id": "s2"})
    assert r.status_code == 200
    body = r.json()
    assert body["code"] == 0
    data = body["data"]
    assert data["status"] in {"completed", "needs_clarification"}
    # offline mode should still complete when year+enterprise present
    assert data["status"] == "completed"
    assert data["report"] is not None
    assert "summary" in data["report"]


@pytest.mark.anyio
async def test_agent_api_needs_clarification_when_missing_time_range(client):
    r = await client.post("/api/v1/agent/query", json={"question": "比亚迪的营收", "session_id": "s3"})
    assert r.status_code == 200
    body = r.json()
    assert body["code"] == 0
    data = body["data"]
    assert data["status"] == "needs_clarification"
    qs = data["clarification"]["questions"]
    assert any(("时间" in q.get("question", "")) or ("年份" in q.get("question", "")) for q in qs)

