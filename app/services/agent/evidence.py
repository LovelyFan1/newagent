from __future__ import annotations

import asyncio
from typing import Any, List

from pydantic import BaseModel, Field

from app.services.agent.utils import TimeRange, new_evidence_id, safe_text
from app.services.indicator_calc import calculate_indicators
from app.services.scoring_service import scoring_service
from app.services.vector_retriever import vector_retriever


class Evidence(BaseModel):
    evidence_id: str
    source: str
    title: str
    excerpt: str
    confidence: float = Field(ge=0.0, le=1.0)


class EvidenceRetriever:
    def __init__(self):
        pass

    async def retrieve(self, enterprises: List[str], time_range: TimeRange, intent: str, query: str | None = None) -> List[Evidence]:
        years = time_range.years(default_year=2022)
        tasks = []
        for ent in enterprises:
            for y in years:
                tasks.append(self._retrieve_local_indicators(ent, y))
                tasks.append(self._retrieve_scoring(ent, y))

        results: list[list[Evidence]] = []
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)

        evidence: list[Evidence] = []
        for r in results:
            if isinstance(r, Exception):
                continue
            evidence.extend(r)

        # RAG retrieval for analysis/decision
        rag_evidence: list[Evidence] = []
        if intent in {"analysis", "decision"} and query:
            rag_evidence = await self._retrieve_vector(query=query, top_k=5)

        # merge strategy:
        # - if structured is weak/empty, rag as primary
        # - if structured is sufficient, rag as supplementary
        merged = evidence + rag_evidence
        merged.sort(key=lambda x: x.confidence, reverse=True)

        # web fallback only if still no evidence
        if not merged:
            web = await self._retrieve_web(enterprises, time_range, intent)
            merged.extend(web)

        return merged

    async def _retrieve_vector(self, query: str, top_k: int = 5) -> list[Evidence]:
        docs = await vector_retriever.retrieve(query=query, top_k=top_k)
        out: list[Evidence] = []
        for d in docs:
            out.append(
                Evidence(
                    evidence_id=new_evidence_id("kb"),
                    source="knowledge_base",
                    title=d.title or f"Document #{d.id}",
                    excerpt=safe_text(d.content, 560),
                    confidence=max(0.1, min(0.95, d.score)),
                )
            )
        return out

    async def _retrieve_web(self, enterprises: list[str], time_range: TimeRange, intent: str) -> list[Evidence]:
        # Optional: web fallback. Keep empty by default to avoid network coupling.
        return []

    async def _retrieve_local_indicators(self, enterprise: str, year: int) -> list[Evidence]:
        try:
            data = await calculate_indicators(enterprise, year)
        except Exception:
            return []
        fin = data.get("indicators", {}).get("financial_health", {})
        legal = data.get("indicators", {}).get("legal_risk", {})
        excerpt = (
            f"{enterprise} {year} 指标摘要："
            f"营收={fin.get('revenue')}, 净利润={fin.get('net_profit')}, 流动比率={fin.get('current_ratio')}; "
            f"诉讼次数={legal.get('lawsuit_count')}, 涉案金额={legal.get('lawsuit_total_amount')}。"
        )
        return [
            Evidence(
                evidence_id=new_evidence_id("local_ind"),
                source="local_indicator_engine",
                title=f"{enterprise} {year} 指标引擎结果",
                excerpt=safe_text(excerpt, 520),
                confidence=0.85,
            )
        ]

    async def _retrieve_scoring(self, enterprise: str, year: int) -> list[Evidence]:
        try:
            score = await scoring_service.calculate_score(enterprise, year)
        except Exception:
            return []
        if not score:
            return []
        excerpt = (
            f"{enterprise} {year} 风险评分：total={score.get('total_score')}, rating={score.get('rating')}, "
            f"dimension_scores={score.get('dimension_scores')}。"
        )
        return [
            Evidence(
                evidence_id=new_evidence_id("score"),
                source="local_scoring_service",
                title=f"{enterprise} {year} 风险评分结果",
                excerpt=safe_text(excerpt, 520),
                confidence=0.9,
            )
        ]


