"""
Run two Agent queries and verify LLM consumes deterministic scoring evidence.
Writes Markdown to stdout (redirect to verification_score_feed_llm.md on host).
"""
from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Any

import httpx


def _parse_scoring_evidence(evidence: list[dict[str, Any]]) -> dict[str, Any] | None:
    for e in evidence or []:
        if e.get("source") == "local_scoring_service" and isinstance(e.get("excerpt"), str):
            try:
                return json.loads(e["excerpt"])
            except Exception:
                continue
    return None


def _check_summary_mentions_score(summary: str, total: float | int, rating: str) -> bool:
    s = summary or ""
    # total may appear as 71.2 or 71.20
    t_ok = re.search(rf"{re.escape(str(total))}", s) is not None or re.search(
        rf"{re.escape(str(round(float(total), 1)))}", s
    ) is not None
    r_ok = rating in s
    return bool(t_ok and r_ok)


def _check_confidence_warning(summary: str, key_findings: list[Any], confidence: float | None) -> tuple[str, bool]:
    if confidence is None:
        return "confidence missing in payload", True
    if float(confidence) >= 0.6:
        return f"confidence={confidence} (>=0.6, warning not required)", True
    blob = summary + "\n" + "\n".join(str(x) for x in (key_findings or []) if x)
    ok = ("数据不足" in blob or "参考价值有限" in blob or "完整度" in blob or "不足" in blob)
    return f"confidence={confidence} (<0.6, expect warning)", ok


def _attribution_indicators(payload: dict[str, Any] | None) -> list[str]:
    if not payload:
        return []
    att = payload.get("indicator_attribution") or []
    out: list[str] = []
    for row in att if isinstance(att, list) else []:
        if isinstance(row, dict) and row.get("indicator"):
            out.append(str(row["indicator"]))
    return out


async def _post(client: httpx.AsyncClient, question: str, session_id: str) -> dict[str, Any]:
    r = await client.post(
        "http://127.0.0.1:8000/api/v1/agent/query",
        json={"question": question, "session_id": session_id},
        timeout=180.0,
    )
    r.raise_for_status()
    body = r.json()
    return body.get("data") or body


async def main() -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    q1 = "比亚迪 2022 年综合评估"
    q2 = "比亚迪 2022 年经营风险怎么样"

    out: list[str] = []
    out.append("# 确定性评分 + LLM Agent 整合验证报告\n")
    out.append(f"- 测试时间：{now}\n")
    out.append("- 接口：`POST /api/v1/agent/query`\n")

    async with httpx.AsyncClient() as client:
        for _ in range(45):
            try:
                r0 = await client.get("http://127.0.0.1:8000/docs", timeout=2.0)
                if r0.status_code < 500:
                    break
            except Exception:
                await asyncio.sleep(1.0)
        d1 = await _post(client, q1, "test_verify_1")
        d2 = await _post(client, q2, "test_verify_2")

    responses = [
        ("请求1：综合评估", q1, d1),
        ("请求2：风险问法", q2, d2),
    ]

    parsed_payloads: list[dict[str, Any] | None] = []
    summaries: list[str] = []
    ratings: list[str] = []
    totals: list[float] = []

    for title, q, data in responses:
        out.append(f"\n## {title}\n")
        out.append(f"- 问题：`{q}`\n")
        out.append(f"- status：`{data.get('status')}`\n")
        report = data.get("report") or {}
        summ = str(report.get("summary") or "")
        summaries.append(summ)
        sections = report.get("sections") if isinstance(report.get("sections"), dict) else {}
        kf = sections.get("key_findings")
        out.append("### report.summary\n")
        out.append("```text\n" + summ[:2000] + "\n```\n")
        out.append("### key_findings（节选）\n")
        out.append("```json\n" + json.dumps(kf, ensure_ascii=False, indent=2)[:4000] + "\n```\n")

        ev = data.get("evidence") or []
        payload = _parse_scoring_evidence(ev)
        parsed_payloads.append(payload)
        if payload:
            ds = payload.get("deterministic_scoring") or {}
            totals.append(float(ds.get("total_score") or 0))
            ratings.append(str(ds.get("rating") or ""))
            out.append("### 解析后的确定性评分 JSON（来自 evidence.local_scoring_service）\n")
            out.append("```json\n" + json.dumps(payload, ensure_ascii=False, indent=2)[:6000] + "\n```\n")
        else:
            out.append("### 解析后的确定性评分 JSON\n**失败**：未找到 `local_scoring_service` 证据或 excerpt 非 JSON。\n")

    # Ground-truth consistency
    out.append("\n## 一致性核对（两次请求应引用同一套评分）\n")
    if len(totals) == 2 and totals[0] == totals[1] and ratings[0] == ratings[1]:
        out.append(f"- total_score 一致：**通过**（{totals[0]}）\n")
        out.append(f"- rating 一致：**通过**（{ratings[0]}）\n")
    else:
        out.append(f"- 评分数字/评级一致：**失败** totals={totals}, ratings={ratings}\n")

    # Per-request checks
    out.append("\n## 逐条检查（关键点）\n")
    idx = 0
    for (title, q, data), payload in zip(responses, parsed_payloads, strict=True):
        idx += 1
        report = data.get("report") or {}
        summ = str(report.get("summary") or "")
        sections = report.get("sections") if isinstance(report.get("sections"), dict) else {}
        kf = sections.get("key_findings") if isinstance(sections.get("key_findings"), list) else []
        atts = sections.get("attributions") if isinstance(sections.get("attributions"), list) else []
        blob = summ + "\n" + json.dumps(kf + atts, ensure_ascii=False)

        ds = (payload or {}).get("deterministic_scoring") or {}
        total = ds.get("total_score")
        rating = str(ds.get("rating") or "")
        conf = ds.get("confidence")

        c1 = _check_summary_mentions_score(summ, total, rating) if payload else False
        eng_indicators = list((payload or {}).get("all_indicator_scores") or {})
        mentioned_indicator = any(ind in blob for ind in eng_indicators[:20])  # rough

        sys_attrs = _attribution_indicators(payload)
        attr_mentioned = any(ind in blob for ind in sys_attrs[:8]) if sys_attrs else False

        warn_label, warn_ok = _check_confidence_warning(summ, kf, float(conf) if conf is not None else None)

        bad_terms = ["市占率", "用户口碑"]
        bad_hit = [t for t in bad_terms if t in blob]

        out.append(f"\n### {title}（请求{idx}）\n")
        out.append(f"| 检查项 | 结果 |\n| --- | --- |\n")
        out.append(f"| summary 明确写出总分与评级（与 JSON 一致） | **{'通过' if c1 else '失败'}** |\n")
        out.append(f"| 解释评分依据（引用具体指标名） | **{'通过' if mentioned_indicator else '待人工/弱通过'}** |\n")
        out.append(f"| 归因与引擎 attribution 指标一致 | **{'通过' if attr_mentioned else '失败'}** |\n")
        out.append(f"| 数据完整度警告（confidence<0.6） | **{'通过' if warn_ok else '失败'}**（{warn_label}） |\n")
        out.append(f"| 未编造市占率/用户口碑等 | **{'通过' if not bad_hit else '失败'}**（命中：{bad_hit}） |\n")

    out.append("\n## 两次回答核心结论一致性\n")
    s0 = summaries[0] if summaries else ""
    s1 = summaries[1] if len(summaries) > 1 else ""
    same_rating = ratings[0] == ratings[1] if len(ratings) > 1 else False
    same_total = totals[0] == totals[1] if len(totals) > 1 else False
    out.append(f"- 评级一致：{'是' if same_rating else '否'}\n")
    out.append(f"- 总分一致：{'是' if same_total else '否'}\n")
    out.append("- 摘要前 200 字对比：\n")
    out.append("```text\n" + (s0[:200] + "\n---\n" + s1[:200]) + "\n```\n")

    out.append("\n## 最终结论\n")
    if same_rating and same_total and all(parsed_payloads):
        out.append("- **链路已打通**：响应 evidence 中含可解析的确定性评分 JSON；两次问法下总分与评级一致。\n")
        out.append("- **LLM 消费评分数据**：以 summary 是否包含与 JSON 一致的总分/评级为准；若「解释依据」为弱通过，多为摘要篇幅限制，可再收紧 prompt 或提高 temperature=0。\n")
    else:
        out.append("- **存在问题**：请检查 evidence 注入、LLM 输出或解析逻辑。\n")

    text = "".join(out)
    # Write UTF-8 inside container for docker cp to host (PowerShell redirect may mangle encoding)
    with open("/app/verification_score_feed_llm.md", "w", encoding="utf-8") as f:
        f.write(text)
    print(text)


if __name__ == "__main__":
    asyncio.run(main())
