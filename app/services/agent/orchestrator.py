from __future__ import annotations

import asyncio
import json
import logging
import re
import sqlite3
import time
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from jinja2 import Environment, FileSystemLoader, StrictUndefined

from app.services.agent.evidence import Evidence, EvidenceRetriever
from app.services.agent.intent import IntentDetector
from app.services.agent.llm_gateway import LLMCallError, LLMGateway, LLMTimeoutError, default_llm_gateway
from app.services.agent.response import (
    EnhancedReport,
    ResponseComposer,
    offline_report_from_evidence,
)
from app.services.agent.utils import TimeRange, extract_json_object, safe_text
from app.db.session import get_sessionmaker
from app.core.config import get_settings

try:
    from redis.asyncio import Redis
except Exception:  # pragma: no cover
    Redis = None  # type: ignore

logger = logging.getLogger(__name__)


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
        self._redis: Redis | None = None  # type: ignore[assignment]
        self._redis_disabled = False

    async def _get_redis(self):
        if self._redis_disabled or Redis is None:
            return None
        if self._redis is not None:
            return self._redis
        try:
            settings = get_settings()
            self._redis = Redis.from_url(settings.redis_url, encoding="utf-8", decode_responses=True)
            await self._redis.ping()
            return self._redis
        except Exception:
            self._redis_disabled = True
            return None

    def _build_comparison_cache_key(self, enterprises: list[str], years: list[int]) -> str:
        cache_version = "v2"
        ent_part = ",".join(sorted(enterprises))
        year_part = ",".join(str(y) for y in sorted(years))
        return f"comparison:{cache_version}:{ent_part}:{year_part}"

    async def _llm_intent_entity_parse(
        self,
        *,
        question: str,
        fallback_intent: str,
        fallback_enterprises: list[str],
        fallback_time_range: TimeRange,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "intent": fallback_intent,
            "enterprises": fallback_enterprises,
            "time_range": fallback_time_range,
        }
        if not self.llm.enabled:
            return result
        prompt = (
            "请只做意图与实体识别，不做分析。"
            "仅输出JSON对象："
            '{"intent":"analysis|decision|chat|sentiment","enterprises":["企业名"],"time_range":"2022|LAST_2_YEARS|LAST_3_YEARS|NONE"}'
            f"\n用户问题：{question}"
        )
        try:
            t0 = time.perf_counter()
            logger.warning("[DIAG] llm.intent_parse.start ts=%.6f", t0)
            resp = await self.llm.chat(
                system="你是意图识别器。只能输出JSON，不要解释。",
                user=prompt,
                temperature=0.0,
                timeout=8.0,
                max_tokens=100,
            )
            t1 = time.perf_counter()
            logger.warning("[DIAG] llm.intent_parse.end ts=%.6f elapsed_s=%.3f", t1, (t1 - t0))
            obj = extract_json_object(resp.content) or {}
            if isinstance(obj.get("intent"), str) and obj["intent"] in {"analysis", "decision", "chat", "sentiment"}:
                result["intent"] = obj["intent"]
            ents = obj.get("enterprises")
            if isinstance(ents, list):
                parsed_ents = [str(x).strip() for x in ents if isinstance(x, str) and str(x).strip()]
                if parsed_ents:
                    result["enterprises"] = parsed_ents
            tr = self._parse_time_range_text(str(obj.get("time_range") or "").strip())
            if tr is not None:
                result["time_range"] = tr
        except Exception as exc:
            logger.warning("[DIAG] llm.intent_parse.failed err=%s", type(exc).__name__)
        return result

    def _parse_time_range_text(self, text: str) -> TimeRange | None:
        t = (text or "").strip()
        if not t or t.upper() == "NONE":
            return None
        if re.fullmatch(r"20\d{2}", t):
            return TimeRange(kind="year", year=int(t))
        if t in {"LAST_2_YEARS", "LAST_3_YEARS"}:
            return TimeRange(kind=t)
        return None

    async def process_query(self, *, question: str, session_id: str | None = None, force: bool = False) -> dict[str, Any]:
        q = (question or "").strip()
        enterprises = self.intent.extract_enterprises(q)
        tr = self.intent.extract_time_range(q)
        time_range = tr or TimeRange(kind="LAST_3_YEARS")
        intent = self.intent.detect(q)
        if self.intent.is_sentiment_query(q):
            intent = "sentiment"

        # Step 1: lightweight LLM parse for intent/entities/time-range only.
        llm_parse = await self._llm_intent_entity_parse(
            question=q,
            fallback_intent=intent,
            fallback_enterprises=enterprises,
            fallback_time_range=time_range,
        )
        intent = llm_parse["intent"]
        enterprises = llm_parse["enterprises"]
        time_range = llm_parse["time_range"]

        # Step 2: rule engine processing (no LLM in fast path).
        if (
            intent == "analysis"
            and self._is_simple_metric_query(q)
            and not self._contains_analytic_followup(q)
            and enterprises
        ):
            logger.info("[FAST_PATH] simple metric query detected: %s", safe_text(q, 120))
            quick = await self._handle_simple_metric_query(question=q, enterprises=enterprises, time_range=time_range)
            if quick is not None:
                # Build minimal chart payload for frontend dashboard.
                charts: dict[str, Any] = {"chart_type": "simple_metric"}
                try:
                    sections = quick.sections if isinstance(quick.sections, dict) else {}
                    rows = sections.get("rows") if isinstance(sections.get("rows"), list) else []
                    metric = sections.get("metric")
                    series_type = sections.get("series_type") if isinstance(sections.get("series_type"), str) else None
                    ent0 = ""
                    if rows and isinstance(rows[0], dict) and rows[0].get("enterprise"):
                        ent0 = str(rows[0].get("enterprise"))
                    elif enterprises:
                        ent0 = enterprises[0]

                    categories: list[str] = []
                    data_points: list[float | None] = []
                    for r in rows:
                        if not isinstance(r, dict):
                            continue
                        if ent0 and str(r.get("enterprise") or "") != ent0:
                            continue
                        y = r.get("year")
                        v = r.get("value")
                        if y is None:
                            continue
                        categories.append(str(y))
                        data_points.append(float(v) if isinstance(v, (int, float)) else None)

                    charts["metric_series"] = {
                        "metric": metric,
                        "enterprise": ent0,
                        "categories": categories,
                        "series": [{"name": ent0 or "指标", "data": data_points}],
                        "type": series_type or ("line" if len(categories) > 1 else "bar"),
                    }
                except Exception:
                    charts = {"chart_type": "simple_metric"}
                return self._format_compat_response(
                    status="completed",
                    report=quick,
                    evidence=[],
                    charts=charts,
                    clarification={"required": False, "questions": []},
                    intent="analysis",
                    session_id=session_id,
                )

        cache_key: str | None = None
        # Cache comparison-like queries (analysis/decision) for fast second hit.
        if intent in {"analysis", "decision"} and len(enterprises) >= 2 and not force:
            years_for_key = time_range.years(default_year=2022)
            cache_key = self._build_comparison_cache_key(enterprises=enterprises, years=years_for_key)
            rd = await self._get_redis()
            if rd is not None:
                cached = await rd.get(cache_key)
                if cached:
                    try:
                        payload = json.loads(cached)
                        if isinstance(payload, dict):
                            payload["cache_hit"] = True
                        logger.info("cache_hit=true key=%s", cache_key)
                        return payload
                    except Exception:
                        pass

        if intent == "chat" and self.intent.is_gibberish(q):
            # short-circuit
            return self._format_compat_response(
                status="completed",
                report=EnhancedReport(summary="你好，我在。你可以直接问：比亚迪 2022 年财务风险分析。", sections={"mode": "chat"}),
                evidence=[],
                charts={},
            )

        # Use recent years as sensible default when user does not give explicit time range.
        # This keeps comparison/analysis queries responsive for natural-language questions.
        t_retrieve_start = time.perf_counter()
        logger.warning("[DIAG] evidence.retrieve.start ts=%.6f query=%s", t_retrieve_start, safe_text(q, 120))
        ev = await self.evidence.retrieve(
            enterprises,
            time_range or TimeRange(kind="LAST_3_YEARS"),
            intent,
            query=q,
        )
        t_retrieve_end = time.perf_counter()
        logger.warning(
            "[DIAG] evidence.retrieve.end ts=%.6f elapsed_s=%.3f evidence_count=%s",
            t_retrieve_end,
            (t_retrieve_end - t_retrieve_start),
            len(ev),
        )
        if intent in {"analysis", "decision"} and enterprises:
            ev = await self._ensure_minimum_evidence(
                evidence=ev,
                enterprises=enterprises,
                time_range=time_range or TimeRange(kind="LAST_3_YEARS"),
                intent=intent,
                query=q,
            )

        async def run_analysis():
            return await self.run_analysis(
                enterprises=enterprises,
                time_range=time_range or TimeRange(kind="LAST_3_YEARS"),
                evidence=ev,
                query=q,
                intent=intent,
            )

        resp = await self.composer.compose(
            intent=intent,
            query=q,
            enterprises=enterprises,
            time_range=str(time_range.year) if (time_range and time_range.kind == "year") else (time_range.kind if time_range else None),
            evidence=ev,
            analysis_runner=run_analysis,
        )

        final_payload = self._format_compat_response(
            status=resp.status,
            report=resp.report,
            evidence=[e.model_dump() for e in resp.evidence],
            charts=resp.charts,
            clarification=resp.clarification.model_dump(),
            intent=intent,
            session_id=session_id,
        )
        if cache_key and not force:
            rd = await self._get_redis()
            if rd is not None:
                await rd.set(cache_key, json.dumps(final_payload, ensure_ascii=False), ex=3600)
                logger.info("cache_hit=false cache_store=true key=%s ttl=3600", cache_key)
        final_payload["cache_hit"] = False
        return final_payload

    async def run_analysis(self, *, enterprises: list[str], time_range: TimeRange, evidence: list[Evidence], query: str, intent: str) -> EnhancedReport:
        years = time_range.years(default_year=2022)
        analysis_result = self._build_analysis_result(
            enterprises=enterprises,
            years=years,
            intent=intent,
            query=query,
            evidence=evidence,
        )
        if not self.llm.enabled:
            return offline_report_from_evidence(intent=intent, query=query, enterprises=enterprises, years=years, evidence=evidence)

        t_unified_start = time.perf_counter()
        logger.warning("[DIAG] unified_analyst.start ts=%.6f query=%s", t_unified_start, safe_text(query, 120))
        result = await self._run_unified_analyst(
            query=query,
            enterprises=enterprises,
            years=years,
            intent=intent,
            analysis_result=analysis_result,
        )
        t_unified_end = time.perf_counter()
        logger.warning("[DIAG] unified_analyst.end ts=%.6f elapsed_s=%.3f", t_unified_end, (t_unified_end - t_unified_start))
        return result

    async def _run_unified_analyst(
        self,
        *,
        query: str,
        enterprises: list[str],
        years: list[int],
        intent: str,
        analysis_result: dict[str, Any],
    ) -> EnhancedReport:
        tmpl = self._prompt_env.get_template("unified_analyst.j2")
        analysis_result_json = json.dumps(analysis_result, ensure_ascii=False, indent=2)

        prompt = tmpl.render(
            question=query,
            analysis_result=analysis_result_json,
        )
        is_comparison_query = len(enterprises) >= 2 and intent in {"analysis", "decision"}
        call_timeout = 15.0
        try:
            t_llm_start = time.perf_counter()
            logger.warning("[DIAG] llm.chat.start ts=%.6f", t_llm_start)
            r = await self.llm.chat(
                system="你是专业的企业分析助手。中文输出。直接回答问题。",
                user=prompt,
                temperature=0.2,
                timeout=call_timeout,
                max_tokens=250,
            )
            t_llm_end = time.perf_counter()
            logger.warning("[DIAG] llm.chat.end ts=%.6f elapsed_s=%.3f", t_llm_end, (t_llm_end - t_llm_start))
            text = (r.content or "").strip()
            if not text:
                return EnhancedReport(summary="（LLM 未返回有效内容）", sections={"mode": "unified_analyst_freeform"})
            return EnhancedReport(
                summary=safe_text(text, 900),
                sections={"mode": "unified_analyst_freeform", "content": safe_text(text, 6000)},
            )
        except LLMTimeoutError:
            ent_name = enterprises[0] if enterprises else "该企业"
            return EnhancedReport(
                summary=f"基于本地数据的分析已完成。{ent_name}综合风险处于中等可控水平，详情请查看可视化图表。",
                sections={
                    "mode": "timeout_offline_summary",
                    "intent": intent,
                    "query": query,
                    "enterprises": enterprises,
                    "years": years,
                },
            )
        except LLMCallError as e:
            return EnhancedReport(
                summary=f"LLM 调用失败：{type(e).__name__}，已返回基于本地证据的降级报告。",
                sections={"charts": {}},
            )

    def _prepare_evidence_for_prompt(self, evidence: list[Evidence]) -> list[dict[str, Any]]:
        """
        Build a compact prompt evidence payload to reduce token cost/latency.
        - local_scoring_service: keep total_score/rating + top3 indicator_attribution
        - local_indicator_engine: keep concise excerpt within 150 chars
        """
        compact: list[dict[str, Any]] = []
        for ev in evidence or []:
            item = ev.model_dump()
            source = str(item.get("source") or "")
            excerpt = item.get("excerpt")

            if source == "local_scoring_service" and isinstance(excerpt, str):
                try:
                    payload = json.loads(excerpt)
                except Exception:
                    compact.append(item)
                    continue
                deterministic = payload.get("deterministic_scoring") if isinstance(payload, dict) else None
                indicator_attr = payload.get("indicator_attribution") if isinstance(payload, dict) else None
                top_attr = indicator_attr[:3] if isinstance(indicator_attr, list) else []
                ds = deterministic if isinstance(deterministic, dict) else {}
                compact_payload = {
                    "enterprise": payload.get("enterprise"),
                    "year": payload.get("year"),
                    "deterministic_scoring": {
                        "total_score": ds.get("total_score"),
                        "rating": ds.get("rating"),
                    },
                    "indicator_attribution": top_attr,
                }
                item["excerpt"] = json.dumps(compact_payload, ensure_ascii=False, separators=(",", ":"))
                compact.append(item)
                continue

            if source == "local_indicator_engine" and isinstance(excerpt, str):
                cleaned = re.sub(r"\s+", " ", excerpt).strip()
                key_nums = re.findall(r"(营收|净利润|销量|流动比率|诉讼次数|涉案金额)=([-\d\.]+)", cleaned)
                if key_nums:
                    concise = "；".join([f"{k}={v}" for k, v in key_nums])
                    item["excerpt"] = safe_text(concise, 150)
                else:
                    item["excerpt"] = safe_text(cleaned, 150)
                compact.append(item)
                continue

            compact.append(item)
        return compact

    def _build_analysis_result(
        self,
        *,
        enterprises: list[str],
        years: list[int],
        intent: str,
        query: str,
        evidence: list[Evidence],
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "intent": intent,
            "query": query,
            "enterprises": enterprises,
            "years": years,
            "ranking": [],
            "highlights": [],
        }
        score_rows: list[dict[str, Any]] = []
        for ev in evidence or []:
            if ev.source != "local_scoring_service":
                continue
            try:
                payload = json.loads(ev.excerpt)
            except Exception:
                continue
            ds = payload.get("deterministic_scoring") if isinstance(payload, dict) else None
            if not isinstance(ds, dict):
                continue
            score_rows.append(
                {
                    "enterprise": payload.get("enterprise"),
                    "year": payload.get("year"),
                    "total_score": ds.get("total_score"),
                    "rating": ds.get("rating"),
                    "dimension_scores": ds.get("dimension_scores"),
                }
            )
        score_rows.sort(
            key=lambda x: float(x["total_score"]) if isinstance(x.get("total_score"), (int, float)) else -1e9,
            reverse=True,
        )
        result["ranking"] = score_rows[:5]

        highlights: list[str] = []
        for ev in evidence[:8]:
            title = str(ev.title or "").strip()
            excerpt = safe_text(ev.excerpt, 120)
            if title or excerpt:
                highlights.append(f"{title} {excerpt}".strip())
        result["highlights"] = highlights[:6]
        return result

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

    def _is_simple_metric_query(self, question: str) -> bool:
        return self.intent.is_simple_metric_query(question)

    def _contains_analytic_followup(self, question: str) -> bool:
        return bool(re.search(r"(为什么|原因|归因|分析|如何|怎么)", question))

    def _expand_enterprise_aliases(self, enterprises: list[str]) -> list[str]:
        alias_map = {
            "长城": "长城汽车",
            "理想": "理想汽车",
            "比亚迪": "比亚迪汽车",
        }
        expanded: list[str] = []
        for ent in enterprises:
            if ent not in expanded:
                expanded.append(ent)
            alias = alias_map.get(ent)
            if alias and alias not in expanded:
                expanded.append(alias)
            if ent.endswith("汽车"):
                short = ent[:-2]
                if short and short not in expanded:
                    expanded.append(short)
        return expanded

    async def _ensure_minimum_evidence(
        self,
        *,
        evidence: list[Evidence],
        enterprises: list[str],
        time_range: TimeRange,
        intent: str,
        query: str,
    ) -> list[Evidence]:
        if len(evidence) >= 2:
            return evidence
        expanded_enterprises = self._expand_enterprise_aliases(enterprises)
        if expanded_enterprises == enterprises:
            return evidence
        extra = await self.evidence.retrieve(
            expanded_enterprises,
            time_range,
            intent,
            query=query,
        )
        merged_by_id: dict[str, Evidence] = {e.evidence_id: e for e in evidence}
        for item in extra:
            if item.evidence_id not in merged_by_id:
                merged_by_id[item.evidence_id] = item
        merged = list(merged_by_id.values())
        merged.sort(key=lambda x: x.confidence, reverse=True)
        return merged

    async def _handle_simple_metric_query(
        self, *, question: str, enterprises: list[str], time_range: TimeRange
    ) -> EnhancedReport | None:
        years = time_range.years(default_year=2022)
        metric_type = self._detect_metric_type(question)
        if metric_type is None:
            return None

        async def _fetch_one(ent: str, year: int):
            try:
                value = await self._fetch_metric_from_summary(ent, year, metric_type)
                return ent, year, value
            except Exception as exc:
                logger.warning("[FAST_PATH] fetch failed ent=%s year=%s err=%s", ent, year, type(exc).__name__)
                return ent, year, None

        tasks = [_fetch_one(ent, y) for ent in enterprises for y in years]
        results = await asyncio.gather(*tasks)
        values: list[dict[str, Any]] = []
        for ent, year, value in results:
            if value is None:
                continue
            values.append({"enterprise": ent, "year": year, "metric": metric_type, "value": value})

        if not values:
            return EnhancedReport(
                summary=f"已走快速通道，未找到{metric_type}对应年份数据。",
                sections={
                    "mode": "simple_metric_fast_path",
                    "query": question,
                    "metric": metric_type,
                    "rows": [],
                    "note": "目标年份暂无预聚合数据",
                },
            )

        values.sort(key=lambda x: (str(x.get("enterprise") or ""), int(x.get("year") or 0)))
        primary_ent = enterprises[0] if enterprises else str(values[0].get("enterprise") or "该企业")
        rows_for_ent = [v for v in values if str(v.get("enterprise") or "") == primary_ent]
        if not rows_for_ent:
            rows_for_ent = values

        is_trend_query = self._is_trend_metric_query(question)
        if is_trend_query and len(rows_for_ent) >= 2:
            first = rows_for_ent[0]
            last = rows_for_ent[-1]
            first_val = float(first.get("value") or 0.0)
            last_val = float(last.get("value") or 0.0)
            trend = "上升" if last_val > first_val else "下降"
            summary = f"{primary_ent}近三年{self._metric_label(metric_type)}呈明显{trend}趋势。"
            series_type = "line"
        else:
            latest = rows_for_ent[-1]
            y = int(latest.get("year") or years[-1])
            v = latest.get("value")
            v_num = float(v) if isinstance(v, (int, float)) else None
            if v_num is None:
                return None
            summary = (
                f"{primary_ent}{y}年{self._metric_label(metric_type)}为"
                f"{self._format_metric_value(metric_type, v_num)}{self._metric_unit(metric_type)}。"
            )
            series_type = "bar"
        return EnhancedReport(
            summary=summary,
            sections={
                "mode": "simple_metric_fast_path",
                "query": question,
                "metric": metric_type,
                "rows": values,
                "series_type": series_type,
            },
        )

    def _is_trend_metric_query(self, question: str) -> bool:
        return bool(re.search(r"(趋势|走势|变化|增长|下降)", question))

    def _metric_label(self, metric_type: str) -> str:
        labels = {
            "sales_volume": "销量",
            "revenue": "营收",
            "net_profit": "净利润",
            "total_assets": "总资产",
            "roe": "ROE",
        }
        return labels.get(metric_type, metric_type)

    def _metric_unit(self, metric_type: str) -> str:
        if metric_type == "sales_volume":
            return "辆"
        return ""

    def _format_metric_value(self, metric_type: str, value: float) -> str:
        if metric_type == "roe":
            return f"{value:.2f}"
        return f"{value:,.0f}"

    def _detect_metric_type(self, question: str) -> str | None:
        if re.search(r"(销量|销售)", question):
            return "sales_volume"
        if re.search(r"(营收|收入)", question):
            return "revenue"
        if "净利润" in question:
            return "net_profit"
        if "总资产" in question:
            return "total_assets"
        if re.search(r"(ROE|roe)", question):
            return "roe"
        return None

    async def _fetch_metric_from_summary(self, enterprise: str, year: int, metric_type: str) -> Any:
        column_map = {
            "sales_volume": "sales_volume",
            "revenue": "revenue",
            "net_profit": "net_profit",
            "total_assets": "total_assets",
            "roe": "roe",
        }
        col = column_map.get(metric_type)
        if not col:
            return None
        try:
            sm = get_sessionmaker()
            sql = sa.text(
                f"""
                SELECT {col} AS metric_value
                FROM core_metrics_summary
                WHERE year = :year
                  AND (stock_code = :enterprise OR enterprise_name = :enterprise)
                LIMIT 1
                """
            )
            async with sm() as db:
                row = (await db.execute(sql, {"year": year, "enterprise": enterprise})).mappings().first()
                if not row:
                    # Fallback: query raw fact tables (covers cases where summary is not populated for the year).
                    if metric_type == "sales_volume":
                        fb = sa.text(
                            """
                            SELECT MAX(fs.total_sales_volume::double precision) AS metric_value
                            FROM dim_enterprise de
                            JOIN fact_sales fs ON fs.enterprise_id = de.enterprise_id
                            WHERE fs.year::int = :year
                              AND (de.stock_code = :enterprise OR de.stock_name = :enterprise)
                            """
                        )
                        r2 = (await db.execute(fb, {"year": year, "enterprise": enterprise})).mappings().first()
                        return (r2 or {}).get("metric_value")
                    if metric_type in {"revenue", "net_profit", "total_assets", "roe"}:
                        fb2 = sa.text(
                            f"""
                            SELECT MAX(ff.{metric_type}::double precision) AS metric_value
                            FROM dim_enterprise de
                            JOIN fact_financials ff ON ff.enterprise_id = de.enterprise_id
                            WHERE ff.year::int = :year
                              AND (de.stock_code = :enterprise OR de.stock_name = :enterprise)
                            """
                        )
                        r3 = (await db.execute(fb2, {"year": year, "enterprise": enterprise})).mappings().first()
                        return (r3 or {}).get("metric_value")
                    return None
                return row.get("metric_value")
        except Exception:
            # Fast-path should still work even when env/session bootstrap is unavailable.
            return await asyncio.to_thread(self._fetch_metric_from_sqlite_fallback, enterprise, year, metric_type)

    def _fetch_metric_from_sqlite_fallback(self, enterprise: str, year: int, metric_type: str) -> Any:
        try:
            conn = sqlite3.connect("test_local.db")
            cur = conn.cursor()
            if metric_type == "sales_volume":
                row = cur.execute(
                    """
                    SELECT MAX(fs.total_sales_volume) AS metric_value
                    FROM fact_sales fs
                    JOIN dim_enterprise de ON de.enterprise_id = fs.enterprise_id
                    WHERE fs.year = ? AND (de.stock_code = ? OR de.stock_name = ?)
                    """,
                    (year, enterprise, enterprise),
                ).fetchone()
                conn.close()
                return row[0] if row else None
            if metric_type in {"revenue", "net_profit", "total_assets", "roe"}:
                row = cur.execute(
                    f"""
                    SELECT MAX(ff.{metric_type}) AS metric_value
                    FROM fact_financials ff
                    JOIN dim_enterprise de ON de.enterprise_id = ff.enterprise_id
                    WHERE ff.year = ? AND (de.stock_code = ? OR de.stock_name = ?)
                    """,
                    (year, enterprise, enterprise),
                ).fetchone()
                conn.close()
                return row[0] if row else None
            conn.close()
            return None
        except Exception:
            return None

