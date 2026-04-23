from __future__ import annotations

import json
import re
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
    evidence_trail: list[dict[str, Any]] = Field(default_factory=list)


class AgentResponse(BaseModel):
    status: Literal["completed", "needs_clarification"]
    report: EnhancedReport | None = None
    clarification: ClarificationBlock = Field(default_factory=lambda: ClarificationBlock(required=False, questions=[]))
    charts: dict[str, Any] = Field(default_factory=dict)
    evidence: list[Evidence] = Field(default_factory=list)


def build_comparison_table(enterprises: list[str], metrics: list[str], data: list[list[Any]]) -> str:
    if not enterprises or not metrics or not data:
        return ""
    header = "| 指标 | " + " | ".join(enterprises) + " |"
    separator = "|------|" + "|".join(["------"] * len(enterprises)) + "|"
    rows: list[str] = []
    for i, metric in enumerate(metrics):
        row_vals = []
        for j in range(len(enterprises)):
            v = data[j][i] if j < len(data) and i < len(data[j]) else "-"
            row_vals.append(str(v))
        rows.append(f"| {metric} | " + " | ".join(row_vals) + " |")
    return "\n".join([header, separator] + rows)


def build_comparison_snapshot_from_evidence(enterprises: list[str], evidence: list[Evidence]) -> dict[str, Any] | None:
    if len(enterprises) <= 1:
        return None

    by_ent: dict[str, dict[str, Any]] = {}
    ent_alias = {e: e for e in enterprises}

    def _norm_ent(name: str | None) -> str | None:
        if not name:
            return None
        for ent in enterprises:
            if ent in name or name in ent:
                return ent
        return name if name in ent_alias else None

    for ev in evidence or []:
        if ev.source == "local_scoring_service":
            try:
                payload = json.loads(ev.excerpt)
            except Exception:
                continue
            ent = _norm_ent(str(payload.get("enterprise") or ""))
            if not ent:
                continue
            rec = by_ent.setdefault(ent, {})
            ds = payload.get("deterministic_scoring") if isinstance(payload.get("deterministic_scoring"), dict) else {}
            rec["total_score"] = ds.get("total_score")
            rec["rating"] = ds.get("rating")
            ind_scores = ds.get("indicator_scores") if isinstance(ds.get("indicator_scores"), dict) else {}
            rec["roe_score"] = ind_scores.get("financial_health.roe")
            rec["current_ratio_score"] = ind_scores.get("financial_health.current_ratio")

        if ev.source == "local_indicator_engine":
            m = re.search(r"^(.*?)\s+\d{4}", ev.title or "")
            ent = _norm_ent(m.group(1).strip() if m else "")
            if not ent:
                continue
            rec = by_ent.setdefault(ent, {})
            revenue = re.search(r"营收=([-\d.]+)", ev.excerpt)
            profit = re.search(r"净利润=([-\d.]+)", ev.excerpt)
            if revenue:
                rec["revenue_raw"] = float(revenue.group(1))
            if profit:
                rec["net_profit_raw"] = float(profit.group(1))

    snapshot_rows: list[dict[str, Any]] = []
    for ent in enterprises:
        rec = by_ent.get(ent, {})
        snapshot_rows.append(
            {
                "enterprise": ent,
                "total_score": rec.get("total_score"),
                "rating": rec.get("rating", "-"),
                "revenue_100m": round(float(rec["revenue_raw"]) / 1e8, 2) if rec.get("revenue_raw") is not None else "-",
                "net_profit_100m": round(float(rec["net_profit_raw"]) / 1e8, 2) if rec.get("net_profit_raw") is not None else "-",
                "roe_percent": rec.get("roe_score", "-"),
                "current_ratio": rec.get("current_ratio_score", "-"),
            }
        )

    snapshot_rows.sort(key=lambda x: float(x["total_score"]) if isinstance(x["total_score"], (int, float)) else -1e9, reverse=True)
    sorted_enterprises = [r["enterprise"] for r in snapshot_rows]
    scores = [r["total_score"] for r in snapshot_rows]
    metrics = ["营收(亿)", "净利润(亿)", "ROE(%)", "流动比率", "风险评级"]
    data = [[r["revenue_100m"], r["net_profit_100m"], r["roe_percent"], r["current_ratio"], r["rating"]] for r in snapshot_rows]

    return {
        "enterprises": sorted_enterprises,
        "scores": scores,
        "metrics": metrics,
        "data": data,
        "table_markdown": build_comparison_table(sorted_enterprises, metrics, data),
    }


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
        # post-process to satisfy acceptance contract
        trail = [
            {
                "evidence_id": e.evidence_id,
                "source": e.source,
                "source_type": getattr(e, "source_type", None),
                "title": e.title,
                "excerpt": e.excerpt,
                "url_or_path": getattr(e, "url_or_path", None),
                "confidence": e.confidence,
            }
            for e in (evidence or [])
        ]
        if report is not None:
            report.evidence_trail = trail
            sections = report.sections if isinstance(report.sections, dict) else {}
            charts: dict[str, Any] = {}
            q = (query or "").strip()
            is_sentiment_query = bool(re.search(r"(舆情|新闻|口碑|舆论)", q))
            is_comparison_query = len(enterprises) > 1 and bool(re.search(r"(对比|比较|vs|VS|哪个好|谁更|高于|低于|排名)", q, flags=re.IGNORECASE))

            # Backend hint for frontend: decide which charts to render.
            mode = str(sections.get("mode") or "")
            if mode == "simple_metric_fast_path":
                charts["chart_type"] = "simple_metric"
            elif is_sentiment_query or intent == "sentiment":
                charts["chart_type"] = "sentiment"
            elif is_comparison_query:
                charts["chart_type"] = "comparison_ranking"
            elif re.search(r"(司法|诉讼|仲裁|案件|判决|执行|行政处罚)", q):
                charts["chart_type"] = "legal_risk"
            else:
                charts["chart_type"] = "general" if intent == "sentiment" else (intent or "analysis")

            if intent == "sentiment" or is_sentiment_query:
                # Hard guard: sentiment intent must not produce ranking/table/recommend-invest artifacts.
                charts["chart_type"] = "sentiment"
                key_findings = sections.get("key_findings") if isinstance(sections.get("key_findings"), list) else []
                sections["key_findings"] = [
                    item for item in key_findings if not (isinstance(item, str) and "| 指标 |" in item)
                ]
                recs = sections.get("recommendations") if isinstance(sections.get("recommendations"), list) else []
                sections["recommendations"] = [
                    r
                    for r in recs
                    if not (isinstance(r, str) and re.search(r"(推荐投资|买入|加仓|第一值得投资)", r))
                ]
                report.summary = re.sub(r"(第一值得投资的是.*)$", "", report.summary or "").strip() or "已完成舆情分析。"
                report.sections = sections
                return AgentResponse(
                    status="completed",
                    report=report,
                    clarification=ClarificationBlock(required=False, questions=[]),
                    evidence=evidence,
                    charts=charts,
                )

            # Simple metric: build a minimal series from fast-path rows.
            if charts.get("chart_type") == "simple_metric":
                rows = sections.get("rows") if isinstance(sections.get("rows"), list) else []
                metric = sections.get("metric")
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
                }

            # Comparison ranking: build ranking bar data (categories already sorted by score desc).
            if charts.get("chart_type") == "comparison_ranking" and is_comparison_query:
                snapshot_for_charts = build_comparison_snapshot_from_evidence(enterprises=enterprises, evidence=evidence)
                if snapshot_for_charts:
                    cats = list(snapshot_for_charts.get("enterprises") or [])
                    vals = list(snapshot_for_charts.get("scores") or [])
                    charts["ranking_bar"] = {
                        "categories": cats,
                        "series": [{"name": "综合评分", "data": vals}],
                    }
                # Build comparison radar from deterministic scoring dimensions.
                radar_indicators = [
                    {"name": "财务健康", "max": 100},
                    {"name": "行业地位", "max": 100},
                    {"name": "法律风险", "max": 100},
                    {"name": "运营能力", "max": 100},
                ]
                by_ent_dim: dict[str, list[float]] = {}
                def _norm_to_input_ent(name: str) -> str | None:
                    for ent in enterprises:
                        if ent in name or name in ent:
                            return ent
                    return None
                for ev in evidence or []:
                    if ev.source != "local_scoring_service":
                        continue
                    try:
                        payload = json.loads(ev.excerpt)
                    except Exception:
                        continue
                    ent_raw = str(payload.get("enterprise") or "")
                    ent = _norm_to_input_ent(ent_raw) or ent_raw
                    if not ent:
                        continue
                    ds = payload.get("deterministic_scoring") if isinstance(payload, dict) else None
                    dims = ds.get("dimension_scores") if isinstance(ds, dict) else None
                    if not isinstance(dims, dict):
                        continue
                    def _pick_val(key: str) -> float:
                        raw = dims.get(key)
                        if isinstance(raw, dict):
                            raw = raw.get("score")
                        return float(raw) if isinstance(raw, (int, float)) else 0.0
                    by_ent_dim[ent] = [
                        _pick_val("financial_health"),
                        _pick_val("industry_position"),
                        _pick_val("legal_risk"),
                        _pick_val("operation"),
                    ]
                radar_series: list[dict[str, Any]] = []
                for ent in enterprises:
                    vals = by_ent_dim.get(ent)
                    if vals:
                        radar_series.append({"name": ent, "value": vals})
                if not radar_series and snapshot_for_charts:
                    ents = list(snapshot_for_charts.get("enterprises") or [])
                    scores = list(snapshot_for_charts.get("scores") or [])
                    for idx, ent in enumerate(ents):
                        s = scores[idx] if idx < len(scores) and isinstance(scores[idx], (int, float)) else 0.0
                        radar_series.append({"name": ent, "value": [float(s), float(s), float(s), float(s)]})
                if radar_series:
                    charts["radar"] = {"indicators": radar_indicators, "series": radar_series}
                if snapshot_for_charts and snapshot_for_charts.get("enterprises"):
                    scatter_points = []
                    for idx, ent in enumerate(snapshot_for_charts.get("enterprises") or []):
                        total_score = (snapshot_for_charts.get("scores") or [None])[idx] if idx < len(snapshot_for_charts.get("scores") or []) else None
                        risk_score = self._extract_enterprise_risk_score(ent, evidence)
                        if isinstance(total_score, (int, float)) and isinstance(risk_score, (int, float)):
                            scatter_points.append({"name": ent, "value": [risk_score, float(total_score)]})
                    if scatter_points:
                        charts["scatter"] = {"series": [{"name": "企业风险收益分布", "data": scatter_points}]}
            if charts.get("chart_type") == "analysis":
                analysis_radar = self._build_radar_from_scoring(enterprises=enterprises, evidence=evidence)
                if analysis_radar:
                    charts["radar"] = analysis_radar
                analysis_scatter = self._build_scatter_from_scoring(enterprises=enterprises, evidence=evidence)
                if analysis_scatter:
                    charts["scatter"] = analysis_scatter
            if is_comparison_query:
                snapshot = build_comparison_snapshot_from_evidence(enterprises=enterprises, evidence=evidence)
                if snapshot:
                    ranked_ents = snapshot.get("enterprises") or enterprises
                    ord_words = ["第一", "第二", "第三", "第四", "第五", "第六", "第七", "第八", "第九", "第十"]
                    rank_parts: list[str] = []
                    for i, ent in enumerate(ranked_ents):
                        prefix = ord_words[i] if i < len(ord_words) else f"第{i+1}"
                        if i == 0:
                            rank_parts.append(f"{prefix}值得投资的是{ent}")
                        else:
                            rank_parts.append(f"{prefix}是{ent}")
                    ranking_sentence = "，".join(rank_parts) + "。"
                    if ranking_sentence not in report.summary:
                        report.summary = f"{report.summary} {ranking_sentence}".strip()

                    table_md = snapshot.get("table_markdown") or ""
                    kf_list = sections.get("key_findings") if isinstance(sections.get("key_findings"), list) else []
                    if table_md and (not any(isinstance(x, str) and "| 指标 |" in x for x in kf_list)):
                        kf_list = [table_md] + kf_list
                    sections["key_findings"] = kf_list

                    recs = sections.get("recommendations") if isinstance(sections.get("recommendations"), list) else []
                    top_ent = ranked_ents[0] if ranked_ents else enterprises[0]
                    must_rec = f"推荐投资：{top_ent}"
                    if recs:
                        if "推荐投资：" not in str(recs[0]):
                            recs[0] = must_rec + "。"
                    else:
                        recs = [must_rec + "。"]
                    sections["recommendations"] = recs
            # recommendations must exist
            recs = sections.get("recommendations")
            if not isinstance(recs, list) or not recs:
                sections["recommendations"] = ["建议补充更多时间范围/同业对比数据以提高结论可靠性。"]
            # attributions must be structured
            atts = sections.get("attributions")
            if not isinstance(atts, list):
                atts = []
            ev_ids = [e.evidence_id for e in (evidence or [])][:3]

            def _build_specific_attribution_candidates() -> list[dict[str, Any]]:
                candidates: list[dict[str, Any]] = []
                for ev in evidence or []:
                    if ev.source != "local_scoring_service":
                        continue
                    try:
                        payload = json.loads(ev.excerpt)
                    except Exception:
                        continue
                    attrs = payload.get("indicator_attribution")
                    if not isinstance(attrs, list):
                        continue
                    for row in attrs:
                        if not isinstance(row, dict):
                            continue
                        indicator = str(row.get("indicator") or row.get("name") or "指标")
                        score = row.get("score")
                        value = row.get("value")
                        zh_name = indicator.split(".")[-1]
                        val_text = "-" if value in (None, "") else str(value)
                        score_text = "-" if score in (None, "") else str(score)
                        obs = f"{zh_name}（得分 {score_text}，数值 {val_text}）"
                        cause = f"{zh_name}表现偏弱，需结合同周期经营与行业证据做归因校验。（evidence_id: {ev.evidence_id})"
                        candidates.append(
                            {
                                "observation": obs,
                                "causes": [cause, f"建议优先修复{zh_name}相关短板并持续跟踪。（evidence_id: {ev.evidence_id})"],
                                "evidence_ids": [ev.evidence_id],
                                "_score_sort": float(score) if isinstance(score, (int, float)) else 1e9,
                            }
                        )
                candidates.sort(key=lambda x: x.get("_score_sort", 1e9))
                return candidates[:3]

            specific_candidates = _build_specific_attribution_candidates()
            norm_atts: list[dict[str, Any]] = []
            for a in atts:
                if isinstance(a, dict):
                    obs = a.get("observation") or ""
                    causes = a.get("causes") if isinstance(a.get("causes"), list) else []
                    eids = a.get("evidence_ids") if isinstance(a.get("evidence_ids"), list) else []
                    if len(causes) < 2:
                        causes = (causes + ["直接原因：证据显示关键指标波动。", "根本原因：竞争/成本结构变化影响。"])[:2]
                    if len(eids) < 2:
                        eids = (eids + ev_ids)[:2]
                    norm_atts.append({"observation": str(obs)[:260], "causes": causes, "evidence_ids": eids})
                elif isinstance(a, str):
                    norm_atts.append(
                        {
                            "observation": a[:260],
                            "causes": ["直接原因：证据支持该现象。", "根本原因：结构性因素叠加。"],
                            "evidence_ids": ev_ids[:2],
                        }
                    )
            # replace vague attributions with specific indicator-based candidates
            def _is_vague(att: dict[str, Any]) -> bool:
                text = f"{att.get('observation', '')} {' '.join(att.get('causes', []))}"
                return ("关键指标存在变化" in text) or ("利润/现金流指标波动" in text)

            if specific_candidates and (not norm_atts or any(_is_vague(x) for x in norm_atts)):
                norm_atts = [
                    {
                        "observation": c["observation"],
                        "causes": c["causes"],
                        "evidence_ids": c["evidence_ids"],
                    }
                    for c in specific_candidates
                ]

            if not norm_atts and ev_ids:
                if specific_candidates:
                    norm_atts = [
                        {
                            "observation": c["observation"],
                            "causes": c["causes"],
                            "evidence_ids": c["evidence_ids"],
                        }
                        for c in specific_candidates
                    ]
                else:
                    norm_atts = [
                        {
                            "observation": "评分归因字段缺失，已退化为证据级归因。",
                            "causes": [f"请补充 indicator_attribution 后输出更细粒度归因。（evidence_id: {ev_ids[0]})", "当前先按可见指标数值进行风险解释。"],
                            "evidence_ids": ev_ids[:2],
                        }
                    ]
            sections["attributions"] = norm_atts
            # key_findings normalize
            if not isinstance(sections.get("key_findings"), list):
                sections["key_findings"] = []
            # Frontend can choose summary-only rendering mode.
            sections["dialogue_summary_only"] = True
            report.sections = sections
        return AgentResponse(
            status="completed",
            report=report,
            clarification=ClarificationBlock(required=False, questions=[]),
            evidence=evidence,
            charts=charts if report else {},
        )

    def _extract_enterprise_risk_score(self, enterprise: str, evidence: list[Evidence]) -> float | None:
        for ev in evidence or []:
            if ev.source != "local_scoring_service":
                continue
            try:
                payload = json.loads(ev.excerpt)
            except Exception:
                continue
            ent = str(payload.get("enterprise") or "")
            if enterprise not in ent and ent not in enterprise:
                continue
            ds = payload.get("deterministic_scoring") if isinstance(payload, dict) else None
            dims = ds.get("dimension_scores") if isinstance(ds, dict) else None
            if not isinstance(dims, dict):
                continue
            raw = dims.get("legal_risk")
            if isinstance(raw, dict):
                raw = raw.get("score")
            if isinstance(raw, (int, float)):
                return float(raw)
        return None

    def _build_radar_from_scoring(self, enterprises: list[str], evidence: list[Evidence]) -> dict[str, Any] | None:
        radar_indicators = [
            {"name": "财务健康", "max": 100},
            {"name": "行业地位", "max": 100},
            {"name": "法律风险", "max": 100},
            {"name": "运营能力", "max": 100},
        ]
        series: list[dict[str, Any]] = []
        for ev in evidence or []:
            if ev.source != "local_scoring_service":
                continue
            try:
                payload = json.loads(ev.excerpt)
            except Exception:
                continue
            ent_raw = str(payload.get("enterprise") or "")
            ent = next((x for x in enterprises if x in ent_raw or ent_raw in x), ent_raw)
            ds = payload.get("deterministic_scoring") if isinstance(payload, dict) else None
            dims = ds.get("dimension_scores") if isinstance(ds, dict) else None
            if not isinstance(dims, dict):
                continue

            def _score(k: str) -> float:
                v = dims.get(k)
                if isinstance(v, dict):
                    v = v.get("score")
                return float(v) if isinstance(v, (int, float)) else 0.0

            vals = [_score("financial_health"), _score("industry_position"), _score("legal_risk"), _score("operation")]
            series.append({"name": ent, "value": vals})
        if not series:
            return None
        return {"indicators": radar_indicators, "series": series}

    def _build_scatter_from_scoring(self, enterprises: list[str], evidence: list[Evidence]) -> dict[str, Any] | None:
        points: list[dict[str, Any]] = []
        for ev in evidence or []:
            if ev.source != "local_scoring_service":
                continue
            try:
                payload = json.loads(ev.excerpt)
            except Exception:
                continue
            ent_raw = str(payload.get("enterprise") or "")
            ent = next((x for x in enterprises if x in ent_raw or ent_raw in x), ent_raw)
            ds = payload.get("deterministic_scoring") if isinstance(payload, dict) else None
            if not isinstance(ds, dict):
                continue
            total = ds.get("total_score")
            dims = ds.get("dimension_scores") if isinstance(ds.get("dimension_scores"), dict) else {}
            risk = dims.get("legal_risk")
            if isinstance(risk, dict):
                risk = risk.get("score")
            if isinstance(total, (int, float)) and isinstance(risk, (int, float)):
                points.append({"name": ent, "value": [float(risk), float(total)]})
        if not points:
            return None
        return {"series": [{"name": "企业风险收益分布", "data": points}]}


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
        evidence_trail=[
            {
                "evidence_id": e.evidence_id,
                "source": e.source,
                "source_type": getattr(e, "source_type", None),
                "title": e.title,
                "excerpt": e.excerpt,
                "url_or_path": getattr(e, "url_or_path", None),
                "confidence": e.confidence,
            }
            for e in (evidence or [])
        ],
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

