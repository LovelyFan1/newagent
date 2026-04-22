from __future__ import annotations

import json
from typing import Any, Literal

from pydantic import BaseModel, Field

from app.services.agent.evidence import Evidence
from app.services.agent.llm_gateway import LLMGateway, default_llm_gateway
from app.services.agent.utils import extract_json_object, safe_text


class ClarificationQuestion(BaseModel):
    question_id: str
    question: str
    reason: str


class ClarificationBlock(BaseModel):
    required: bool
    questions: list[ClarificationQuestion] = Field(default_factory=list)


class EnhancedReport(BaseModel):
    summary: str
    sections: dict[str, Any] = Field(default_factory=dict)


class AgentResponse(BaseModel):
    status: Literal["completed", "needs_clarification"]
    report: EnhancedReport | None = None
    clarification: ClarificationBlock = Field(default_factory=lambda: ClarificationBlock(required=False, questions=[]))
    charts: dict[str, Any] = Field(default_factory=dict)
    evidence: list[Evidence] = Field(default_factory=list)


class ResponseComposer:
    def __init__(self, llm: LLMGateway | None = None):
        self.llm = llm or default_llm_gateway()

    async def compose(self, *, intent: str, query: str, enterprises: list[str], time_range: str | None, evidence: list[Evidence], analysis_runner):
        # clarification for analysis
        if intent in {"analysis", "decision"}:
            if not enterprises:
                return AgentResponse(
                    status="needs_clarification",
                    report=None,
                    clarification=ClarificationBlock(
                        required=True,
                        questions=[
                            ClarificationQuestion(
                                question_id="q_enterprise",
                                question="请明确企业名称/股票代码（如：比亚迪/002594）。",
                                reason="当前问题未指向具体企业，无法检索证据并生成可追溯结论。",
                            )
                        ],
                    ),
                    evidence=evidence,
                    charts={},
                )
            if (time_range is None) or (isinstance(time_range, str) and not time_range.strip()):
                return AgentResponse(
                    status="needs_clarification",
                    report=None,
                    clarification=ClarificationBlock(
                        required=True,
                        questions=[
                            ClarificationQuestion(
                                question_id="q_time_range",
                                question="请确认分析时间范围（例如 2022 年 / 近三年）。",
                                reason="时间范围会影响证据窗口与结论有效性。",
                            )
                        ],
                    ),
                    evidence=evidence,
                    charts={},
                )

        if intent == "chat":
            if self.llm.enabled:
                r = await self.llm.chat(
                    system="你是简洁的中文助手，控制在120字以内。",
                    user=query,
                    temperature=0.3,
                )
                return AgentResponse(
                    status="completed",
                    report=EnhancedReport(summary=safe_text(r.content, 280), sections={"mode": "chat"}),
                    clarification=ClarificationBlock(required=False, questions=[]),
                    evidence=[],
                    charts={},
                )
            return AgentResponse(
                status="completed",
                report=EnhancedReport(
                    summary="你好，我可以做企业风险分析。请提供企业名称/股票代码 + 年份（例如：比亚迪 2022）。",
                    sections={"mode": "chat_offline"},
                ),
                evidence=[],
                charts={},
            )

        # analysis/decision
        report = await analysis_runner()
        return AgentResponse(
            status="completed",
            report=report,
            clarification=ClarificationBlock(required=False, questions=[]),
            evidence=evidence,
            charts=report.sections.get("charts", {}) if report else {},
        )


def offline_report_from_evidence(*, intent: str, query: str, enterprises: list[str], years: list[int], evidence: list[Evidence]) -> EnhancedReport:
    key = "、".join(enterprises) if enterprises else "（未识别企业）"
    time_desc = ",".join(str(y) for y in years) if years else "（未指定）"
    ev_excerpt = "\n".join([f"- {e.title}: {e.excerpt}" for e in evidence[:6]]) or "（暂无本地证据）"
    kb_evidence = [e for e in evidence if e.source == "knowledge_base"]
    if kb_evidence:
        kb_fact = safe_text(kb_evidence[0].excerpt, 120)
        kb_note = f"知识库引用：{kb_fact}"
        kb_attr = f"结论归因包含 knowledge_base 证据：{kb_evidence[0].title}"
    else:
        kb_note = "知识库中未找到直接相关信息（离线模板占位提示）。"
        kb_attr = "无 knowledge_base 归因（离线模板占位提示）。"
    summary = f"已基于本地数据完成{key}在{time_desc}的{intent}分析。证据条数={len(evidence)}。"
    return EnhancedReport(
        summary=summary,
        sections={
            "query": query,
            "enterprises": enterprises,
            "years": years,
            "evidence_excerpt": ev_excerpt,
            "key_findings": [kb_note],
            "attributions": [kb_attr],
            "note": "离线模式：未配置 LLM，报告为基于本地证据的结构化摘要。",
        },
    )

