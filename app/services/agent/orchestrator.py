from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from app.services.agent.evidence import Evidence, EvidenceRetriever
from app.services.agent.intent import IntentDetector
from app.services.agent.llm_gateway import LLMGateway, default_llm_gateway
from app.services.agent.response import EnhancedReport, ResponseComposer, offline_report_from_evidence
from app.services.agent.utils import TimeRange, extract_json_object, safe_text


class AgentOrchestrator:
    def __init__(self, *, llm: LLMGateway | None = None):
        self.intent = IntentDetector()
        self.evidence = EvidenceRetriever()
        self.llm = llm or default_llm_gateway()
        self.composer = ResponseComposer(self.llm)
        self._prompt_env = Environment(
            loader=FileSystemLoader(str(Path(__file__).resolve().parent / "prompts")),
            undefined=StrictUndefined,
            autoescape=False,
        )

    async def process_query(self, *, question: str, session_id: str | None = None) -> dict[str, Any]:
        q = (question or "").strip()
        intent = self.intent.detect(q)
        enterprises = self.intent.extract_enterprises(q)
        tr = self.intent.extract_time_range(q)
        time_range = tr

        if intent == "chat" and self.intent.is_gibberish(q):
            # short-circuit
            return self._format_compat_response(
                status="completed",
                report=EnhancedReport(summary="你好，我在。你可以直接问：比亚迪 2022 年财务风险分析。", sections={"mode": "chat"}),
                evidence=[],
                charts={},
            )

        # IMPORTANT: if analysis/decision but missing explicit time range, do NOT fetch evidence yet.
        # Let composer return needs_clarification as required by the API contract.
        if time_range is None and intent in {"analysis", "decision"}:
            ev: list[Evidence] = []
        else:
            ev = await self.evidence.retrieve(
                enterprises,
                time_range or TimeRange(kind="LAST_3_YEARS"),
                intent,
                query=q,
            )

        async def run_analysis():
            return await self.run_analysis(enterprises=enterprises, time_range=time_range or TimeRange(kind="LAST_3_YEARS"), evidence=ev, query=q, intent=intent)

        resp = await self.composer.compose(
            intent=intent,
            query=q,
            enterprises=enterprises,
            time_range=str(time_range.year) if (time_range and time_range.kind == "year") else (time_range.kind if time_range else None),
            evidence=ev,
            analysis_runner=run_analysis,
        )

        return self._format_compat_response(
            status=resp.status,
            report=resp.report,
            evidence=[e.model_dump() for e in resp.evidence],
            charts=resp.charts,
            clarification=resp.clarification.model_dump(),
            intent=intent,
            session_id=session_id,
        )

    async def run_analysis(self, *, enterprises: list[str], time_range: TimeRange, evidence: list[Evidence], query: str, intent: str) -> EnhancedReport:
        years = time_range.years(default_year=2022)
        if not self.llm.enabled:
            return offline_report_from_evidence(intent=intent, query=query, enterprises=enterprises, years=years, evidence=evidence)

        # parallel role agents
        credit_t = self._run_role_agent("credit_analyst.j2", role="credit", query=query, enterprises=enterprises, years=years, evidence=evidence)
        industry_t = self._run_role_agent("industry_analyst.j2", role="industry", query=query, enterprises=enterprises, years=years, evidence=evidence)
        risk_t = self._run_role_agent("risk_analyst.j2", role="risk", query=query, enterprises=enterprises, years=years, evidence=evidence)
        invest_t = self._run_role_agent("investment_analyst.j2", role="investment", query=query, enterprises=enterprises, years=years, evidence=evidence)

        role_outputs = await asyncio.gather(credit_t, industry_t, risk_t, invest_t)

        chief = await self._run_chief_agent(
            query=query,
            enterprises=enterprises,
            years=years,
            evidence=evidence,
            role_outputs=role_outputs,
            intent=intent,
        )
        return chief

    async def _run_role_agent(
        self,
        template_name: str,
        *,
        role: str,
        query: str,
        enterprises: list[str],
        years: list[int],
        evidence: list[Evidence],
    ) -> dict[str, Any]:
        tmpl = self._prompt_env.get_template(template_name)
        prompt = tmpl.render(query=query, enterprises=enterprises, years=years, evidence=[e.model_dump() for e in evidence])
        try:
            r = await self.llm.chat(system="你只输出合法 JSON（对象）。中文输出。", user=prompt, temperature=0.2)
            obj = extract_json_object(r.content) or {"role": role, "error": "invalid_json", "raw": safe_text(r.content, 500)}
            obj["role"] = role
            return obj
        except Exception as e:
            return {"role": role, "error": f"{type(e).__name__}: {e}", "raw": ""}

    async def _run_chief_agent(
        self,
        *,
        query: str,
        enterprises: list[str],
        years: list[int],
        evidence: list[Evidence],
        role_outputs: list[dict[str, Any]],
        intent: str,
    ) -> EnhancedReport:
        tmpl = self._prompt_env.get_template("chief_analyst.j2")
        prompt = tmpl.render(
            query=query,
            intent=intent,
            enterprises=enterprises,
            years=years,
            evidence=[e.model_dump() for e in evidence],
            role_outputs=role_outputs,
        )
        try:
            r = await self.llm.chat(system="你只输出合法 JSON（对象）。中文输出。", user=prompt, temperature=0.2)
            obj = extract_json_object(r.content)
            if not obj:
                return EnhancedReport(
                    summary="首席 Agent 输出解析失败，已返回降级报告。",
                    sections={"role_outputs": role_outputs, "raw": safe_text(r.content, 800)},
                )
            return EnhancedReport(
                summary=str(obj.get("summary") or "（无摘要）"),
                sections=obj.get("sections") if isinstance(obj.get("sections"), dict) else {"data": obj},
            )
        except Exception:
            # hard fallback: offline report style (still returns evidence_trail in ResponseComposer)
            return EnhancedReport(
                summary="LLM 调用失败，已返回基于本地证据的降级报告。",
                sections={"role_outputs": role_outputs, "charts": {}},
            )

    def _format_compat_response(
        self,
        *,
        status: str,
        report: EnhancedReport | None,
        evidence: list[dict[str, Any]],
        charts: dict[str, Any],
        clarification: dict[str, Any] | None = None,
        intent: str | None = None,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        # Compatible envelope for frontend: status/report/charts/evidence (+ optional clarification)
        return {
            "status": status,
            "session_id": session_id,
            "intent": intent,
            "report": report.model_dump() if report else None,
            "charts": charts or {},
            "evidence": evidence or [],
            "clarification": clarification or {"required": False, "questions": []},
        }

