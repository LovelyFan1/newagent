from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, List

from pydantic import BaseModel, Field

from app.services.agent.utils import TimeRange, new_evidence_id, safe_text
from app.services.indicator_calc import calculate_indicators
from app.services.scoring_service import scoring_service
from app.services.vector_retriever import vector_retriever

logger = logging.getLogger(__name__)

# 本地 calculate_indicators / 评分引擎使用的名称（简称在库里常匹配不到 → 无评分 JSON）
_INDICATOR_STOCK_NAME_MAP: dict[str, str] = {
    "长城": "长城汽车",
    "长安汽车": "长安汽车",
    "长安": "长安汽车",
    "广汽集团": "广汽集团",
    "广汽": "广汽集团",
    "比亚迪汽车": "比亚迪",
    "比亚迪": "比亚迪",
    "理想": "理想汽车",
    "力帆": "力帆科技",
    "力帆科技": "力帆科技",
    "中汽": "中汽股份",
    "中汽股份": "中汽股份",
    "一汽解放": "一汽解放",
    "万向": "万向钱潮",
    "万向钱潮": "万向钱潮",
    "东风汽车": "东风汽车",
    "东风科技": "东风科技",
    "重汽": "中国重汽",
    "中国重汽": "中国重汽",
    "宇通": "宇通客车",
    "宇通客车": "宇通客车",
    "江铃": "江铃汽车",
    "江铃汽车": "江铃汽车",
    "东安": "东安动力",
    "东安动力": "东安动力",
    "云意": "云意电气",
    "云意电气": "云意电气",
    "京威": "京威股份",
    "京威股份": "京威股份",
    "伯特利": "伯特利",
    "信隆": "信隆健康",
    "信隆健康": "信隆健康",
    "旷达": "旷达科技",
    "旷达科技": "旷达科技",
    "汉马": "汉马科技",
    "汉马科技": "汉马科技",
    "索菱": "索菱股份",
    "索菱股份": "索菱股份",
    "贝斯特": "贝斯特",
    "路畅": "路畅科技",
    "路畅科技": "路畅科技",
    "亚星": "亚星客车",
    "亚星客车": "亚星客车",
    "安凯": "安凯客车",
    "安凯客车": "安凯客车",
    "福田": "福田汽车",
    "福田汽车": "福田汽车",
}


def _resolve_indicator_stock_name(name: str) -> str:
    n = (name or "").strip()
    if not n:
        return n
    return _INDICATOR_STOCK_NAME_MAP.get(n, n)


class Evidence(BaseModel):
    evidence_id: str
    source_type: str | None = None
    source: str
    title: str
    excerpt: str
    url_or_path: str | None = None
    confidence: float = Field(ge=0.0, le=1.0)


class EvidenceRetriever:
    def __init__(self):
        pass

    async def retrieve(self, enterprises: List[str], time_range: TimeRange, intent: str, query: str | None = None) -> List[Evidence]:
        years = time_range.years(default_year=2022)
        tasks = []
        for ent in enterprises:
            api_ent = _resolve_indicator_stock_name(ent)
            for y in years:
                tasks.append(self._retrieve_local_indicators(api_ent, y))
                tasks.append(self._retrieve_scoring(api_ent, y))

        results: list[list[Evidence]] = []
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)

        evidence: list[Evidence] = []
        for r in results:
            if isinstance(r, Exception):
                continue
            evidence.extend(r)

        has_scoring_evidence = any(e.source == "local_scoring_service" for e in evidence)

        # Comparison-oriented analysis should rely on structured local data first.
        # Skip RAG to reduce latency and avoid introducing noisy context.
        q_text = (query or "")
        has_comparison_keyword = bool(re.search(r"(对比|比较|谁更|哪个好|排名)", q_text))
        is_multi_enterprise_comparison = intent in {"analysis", "decision"} and (
            len(enterprises) >= 2 or has_comparison_keyword
        )

        skip_rag = True
        use_rag = (
            bool(query)
            and intent in {"analysis", "decision"}
            and self._has_complex_analysis_keyword(query)
            and (not skip_rag)
            and (not is_multi_enterprise_comparison)
        )

        # RAG retrieval only for truly complex analysis queries
        rag_evidence: list[Evidence] = []
        if use_rag:
            logger.info("[RAG] retrieving knowledge evidence for query=%s", safe_text(query, 120))
            rag_evidence = await self._retrieve_vector(query=query, top_k=5)
        else:
            logger.info(
                "[RAG] skipped intent=%s has_scoring=%s skip_rag=%s query=%s",
                intent,
                has_scoring_evidence,
                skip_rag,
                safe_text(query, 120) if query else "",
            )

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

    def _is_simple_metric_query(self, query: str | None) -> bool:
        if not query:
            return False
        q = query.strip()
        simple_metric_keywords = (
            "销量",
            "销售",
            "营收",
            "收入",
            "净利润",
            "利润",
            "总资产",
            "负债",
            "ROE",
            "roe",
            "流动比率",
        )
        complex_keywords = ("分析", "评估", "风险", "竞争力", "前景", "建议", "对比", "策略", "投资")
        return any(k in q for k in simple_metric_keywords) and not any(k in q for k in complex_keywords)

    def _has_complex_analysis_keyword(self, query: str) -> bool:
        return bool(re.search(r"(风险|竞争力|前景|评估|分析|建议|对比|策略|投资)", query))

    async def _retrieve_vector(self, query: str, top_k: int = 5) -> list[Evidence]:
        docs = await vector_retriever.retrieve(query=query, top_k=top_k)
        out: list[Evidence] = []
        for d in docs:
            out.append(
                Evidence(
                    evidence_id=new_evidence_id("kb"),
                    source_type="knowledge",
                    source="knowledge_base",
                    title=d.title or f"Document #{d.id}",
                    excerpt=safe_text(d.content, 560),
                    url_or_path=d.source,
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
        ind = data.get("indicators", {}).get("industry_position", {})
        excerpt = (
            f"{enterprise} {year} 指标摘要："
            f"销量={ind.get('sales_volume')}, 新能源销量={ind.get('nev_sales_volume')}, "
            f"营收={fin.get('revenue')}, 净利润={fin.get('net_profit')}, 总资产={fin.get('total_assets')}, "
            f"ROE={fin.get('roe')}, 流动比率={fin.get('current_ratio')}; "
            f"诉讼次数={legal.get('lawsuit_count')}, 涉案金额={legal.get('lawsuit_total_amount')}。"
        )
        return [
            Evidence(
                evidence_id=new_evidence_id("local_ind"),
                source_type="local",
                source="local_indicator_engine",
                title=f"{enterprise} {year} 指标引擎结果",
                excerpt=safe_text(excerpt, 520),
                url_or_path=None,
                confidence=0.85,
            )
        ]

    async def _retrieve_scoring(self, enterprise: str, year: int) -> list[Evidence]:
        try:
            raw = await scoring_service.get_raw_data(enterprise, year)
            if raw:
                scored = scoring_service.calculate_score_from_raw_data(raw_data=raw, year=year)
            else:
                score = await scoring_service.calculate_score(enterprise, year)
                if not score:
                    return []
                excerpt = (
                    f"{enterprise} {year} 风险评分：total={score.get('total_score')}, rating={score.get('rating')}, "
                    f"dimension_scores={score.get('dimension_scores')}。"
                )
                return [
                    Evidence(
                        evidence_id=new_evidence_id("score"),
                        source_type="local",
                        source="local_scoring_service",
                        title=f"{enterprise} {year} 风险评分结果",
                        excerpt=safe_text(excerpt, 520),
                        url_or_path=None,
                        confidence=0.9,
                    )
                ]
        except Exception:
            return []
        payload: dict[str, Any] = {
            "enterprise": enterprise,
            "year": year,
            "deterministic_scoring": {
                "total_score": scored.get("total_score"),
                "rating": scored.get("rating"),
                "confidence": scored.get("confidence"),
                "effective_weights": scored.get("effective_weights"),
                "dimension_scores": scored.get("dimension_scores"),
                "indicator_scores": scored.get("indicator_scores"),
            },
            "indicator_attribution": raw.get("attribution"),
            "all_indicator_scores": raw.get("all_indicator_scores"),
        }
        # Keep full JSON for LLM + downstream parsers; avoid mid-JSON truncation (breaks json.loads).
        excerpt = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        if len(excerpt) > 12000:
            excerpt = excerpt[:11997] + "..."
        return [
            Evidence(
                evidence_id=new_evidence_id("score"),
                source_type="local",
                source="local_scoring_service",
                title=f"{enterprise} {year} 风险评分结果",
                excerpt=excerpt,
                url_or_path=None,
                confidence=0.9,
            )
        ]


