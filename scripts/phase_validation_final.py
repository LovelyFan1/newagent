from __future__ import annotations

import asyncio
import json
import os
import random
import time
import uuid
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx
import sqlalchemy as sa

from app.db.session import get_sessionmaker
from app.services.indicator_calc import calculate_indicators


BASE_URL = os.environ.get("PHASE_FINAL_BASE_URL", "http://127.0.0.1:8000")
YEAR = int(os.environ.get("PHASE_FINAL_YEAR", "2022"))


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for r in rows:
        out.append("| " + " | ".join(r) + " |")
    return "\n".join(out)


def _now_local() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def _leaf_count(d: Any) -> int:
    if isinstance(d, dict):
        return sum(_leaf_count(v) for v in d.values())
    if isinstance(d, list):
        return sum(_leaf_count(x) for x in d)
    return 1


def _get_nested(d: dict[str, Any], path: list[str], default=None):
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


async def _db_scalar(sql: str, params: dict[str, Any] | None = None) -> Any:
    sm = get_sessionmaker()
    async with sm() as db:
        r = await db.execute(sa.text(sql), params or {})
        row = r.first()
        if not row:
            return None
        return row[0]


async def _db_rows(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    sm = get_sessionmaker()
    async with sm() as db:
        r = await db.execute(sa.text(sql), params or {})
        return [dict(x) for x in r.mappings().all()]


async def _http_get(path: str, params: dict[str, Any] | None = None) -> tuple[int, str, float]:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=60.0) as c:
        t0 = time.perf_counter()
        r = await c.get(path, params=params)
        return r.status_code, r.text, time.perf_counter() - t0


async def _http_post(path: str, json_body: dict[str, Any]) -> tuple[int, str, float]:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=60.0) as c:
        t0 = time.perf_counter()
        r = await c.post(path, json=json_body)
        return r.status_code, r.text, time.perf_counter() - t0


async def main() -> int:
    started_at = _now_local()

    # 1) data base quick checks
    dim_cnt = await _db_scalar("SELECT COUNT(*)::int FROM dim_enterprise;")
    fin_2022 = await _db_scalar("SELECT COUNT(*)::int FROM fact_financials WHERE CAST(year AS int)=:y;", {"y": YEAR})
    sales_2022 = await _db_scalar("SELECT COUNT(*)::int FROM fact_sales WHERE CAST(year AS int)=:y;", {"y": YEAR})
    legal_2022 = await _db_scalar("SELECT COUNT(*)::int FROM fact_legal WHERE year=:y;", {"y": str(YEAR)})

    core_names = ["比亚迪", "长城汽车", "蔚来", "宁德时代", "上汽集团"]
    core_cov = await _db_rows(
        """
        WITH picked AS (
          SELECT enterprise_id, stock_name
          FROM dim_enterprise
          WHERE stock_name = ANY(:names)
        )
        SELECT
          p.stock_name,
          CASE WHEN f.enterprise_id IS NULL THEN '无' ELSE '有' END AS financial,
          CASE WHEN s.enterprise_id IS NULL THEN '无' ELSE '有' END AS sales,
          CASE WHEN l.enterprise_id IS NULL THEN '无' ELSE '有' END AS legal
        FROM picked p
        LEFT JOIN (SELECT DISTINCT enterprise_id FROM fact_financials WHERE CAST(year AS int)=:y) f ON f.enterprise_id=p.enterprise_id
        LEFT JOIN (SELECT DISTINCT enterprise_id FROM fact_sales WHERE CAST(year AS int)=:y) s ON s.enterprise_id=p.enterprise_id
        LEFT JOIN (SELECT DISTINCT enterprise_id FROM fact_legal WHERE year=:ys) l ON l.enterprise_id=p.enterprise_id
        ORDER BY p.stock_name;
        """,
        {"names": core_names, "y": YEAR, "ys": str(YEAR)},
    )

    # 2) indicator engine sampling (5 enterprises with financial 2022)
    sample_ents = await _db_rows(
        """
        SELECT de.stock_name
        FROM dim_enterprise de
        JOIN (SELECT DISTINCT enterprise_id FROM fact_financials WHERE CAST(year AS int)=:y) f
          ON f.enterprise_id = de.enterprise_id
        WHERE de.stock_name IS NOT NULL
        ORDER BY random()
        LIMIT 5;
        """,
        {"y": YEAR},
    )
    sampled_names = [r["stock_name"] for r in sample_ents]

    indicator_results: list[dict[str, Any]] = []
    for name in sampled_names:
        try:
            ind = await calculate_indicators(name, YEAR)
            indicators = ind.get("indicators", {})
            fin = indicators.get("financial_health", {})
            legal = indicators.get("legal_risk", {})
            industry = indicators.get("industry_position", {})

            indicator_count = _leaf_count(indicators)
            # sanity checks
            revenue = fin.get("revenue")
            net_profit = fin.get("net_profit")
            roe = fin.get("roe")
            current_ratio = fin.get("current_ratio")
            sales_volume = industry.get("sales_volume")
            lawsuit_count = legal.get("lawsuit_count")

            def _num(v):
                try:
                    if isinstance(v, str) and v.endswith("%"):
                        return float(v[:-1]) / 100.0
                    return float(v)
                except Exception:
                    return None

            indicator_results.append(
                {
                    "enterprise": name,
                    "ok": True,
                    "indicator_leaf_count": indicator_count,
                    "revenue": revenue,
                    "net_profit": net_profit,
                    "roe": roe,
                    "current_ratio": current_ratio,
                    "sales_volume": sales_volume,
                    "lawsuit_count": lawsuit_count,
                    "revenue_num": _num(revenue),
                    "net_profit_num": _num(net_profit),
                }
            )
        except Exception as e:
            indicator_results.append({"enterprise": name, "ok": False, "error": f"{type(e).__name__}: {e}"})

    # 3) scoring service API checks + cache T1/T2
    scoring_checks: list[dict[str, Any]] = []
    for name in sampled_names:
        quoted = "/api/v1/scoring/" + urllib.parse.quote(name, safe="")
        s1, b1, t1 = await _http_get(quoted, params={"year": YEAR})
        s2, b2, t2 = await _http_get(quoted, params={"year": YEAR})
        same = b1 == b2
        try:
            j1 = json.loads(b1)
            d1 = j1.get("data", {}).get("data", j1.get("data", {}))  # tolerate double-wrap
        except Exception:
            d1 = {}
        scoring_checks.append(
            {
                "enterprise": name,
                "status1": s1,
                "t1": round(t1, 3),
                "status2": s2,
                "t2": round(t2, 3),
                "same_body": same,
                "total_score": d1.get("total_score"),
                "rating": d1.get("rating"),
            }
        )

    # 4) agent API scenarios
    scenarios = [
        ("单企业分析", {"question": "比亚迪 2022 年财务风险分析", "session_id": f"s-{uuid.uuid4().hex[:8]}"}),
        ("多企业对比", {"question": "对比比亚迪和长城汽车 2022 年的盈利能力", "session_id": f"s-{uuid.uuid4().hex[:8]}"}),
        ("投资决策", {"question": "蔚来汽车是否值得投资？", "session_id": f"s-{uuid.uuid4().hex[:8]}"}),
        ("无数据企业", {"question": "小米汽车 2022 年销量", "session_id": f"s-{uuid.uuid4().hex[:8]}"}),
        ("模糊查询(缺时间)", {"question": "比亚迪的营收", "session_id": f"s-{uuid.uuid4().hex[:8]}"}),
        ("乱码输入", {"question": "asdfghjkl", "session_id": f"s-{uuid.uuid4().hex[:8]}"}),
    ]
    agent_results: list[dict[str, Any]] = []
    for title, body in scenarios:
        st, text, elapsed = await _http_post("/api/v1/agent/query", body)
        ok = st == 200
        status = None
        ev_n = None
        summary_ok = False
        try:
            j = json.loads(text)
            data = j.get("data", {})
            status = data.get("status")
            ev_n = len(data.get("evidence") or [])
            report = data.get("report") or {}
            summary_ok = bool((report.get("summary") or "").strip())
        except Exception:
            pass
        agent_results.append(
            {
                "scenario": title,
                "http": st,
                "elapsed": round(elapsed, 3),
                "status": status,
                "evidence_n": ev_n,
                "report_summary_ok": summary_ok,
                "body_head": text[:180],
            }
        )

    # 5) exception handling for scoring
    exc_tests = []
    st, text, _ = await _http_get("/api/v1/scoring/INVALID", params={"year": YEAR})
    exc_tests.append({"case": "invalid_code", "http": st, "body": text[:160]})
    st, text, _ = await _http_get("/api/v1/scoring/002594", params={"year": 1999})
    exc_tests.append({"case": "no_data_year", "http": st, "body": text[:160]})

    # 6) concurrency: 10 agent queries
    async def one_agent(i: int):
        q = random.choice(
            [
                "比亚迪 2022 年财务风险分析",
                "对比比亚迪和长安汽车 2022 年的风险",
                "长城汽车 2022 年诉讼风险分析",
                "上汽集团 2022 年综合风险评估",
                "比亚迪的营收",
            ]
        )
        payload = {"question": q, "session_id": f"conc-{i}-{uuid.uuid4().hex[:6]}"}
        st, text, elapsed = await _http_post("/api/v1/agent/query", payload)
        return {"i": i, "http": st, "elapsed": round(elapsed, 3), "ok": st == 200, "head": text[:120]}

    conc = await asyncio.gather(*[one_agent(i) for i in range(10)])
    conc_ok = sum(1 for r in conc if r["ok"])

    # render report
    lines: list[str] = []
    lines.append("# Phase Final 全面验证报告（指标引擎 + 评分 + Agent + API）")
    lines.append("")
    lines.append(f"- 测试时间：{started_at}")
    lines.append(f"- BASE_URL：`{BASE_URL}`")
    lines.append(f"- YEAR：{YEAR}")
    lines.append("")

    lines.append("## 1. 数据基础快速复查")
    lines.append("")
    lines.append(f"- dim_enterprise：{dim_cnt}")
    lines.append(f"- fact_financials({YEAR})：{fin_2022}")
    lines.append(f"- fact_sales({YEAR})：{sales_2022}")
    lines.append(f"- fact_legal({YEAR})：{legal_2022}")
    lines.append("")
    lines.append(_md_table(["企业", "财务", "销售", "司法"], [[r["stock_name"], r["financial"], r["sales"], r["legal"]] for r in core_cov]))
    lines.append("")

    lines.append("## 2. 指标引擎抽样验证（5家）")
    lines.append("")
    rows = []
    for r in indicator_results:
        if not r.get("ok"):
            rows.append([r["enterprise"], "FAIL", "", "", "", "", "", r.get("error", "")[:60]])
        else:
            rows.append(
                [
                    r["enterprise"],
                    "OK",
                    str(r["indicator_leaf_count"]),
                    str(r["revenue"]),
                    str(r["net_profit"]),
                    str(r["roe"]),
                    str(r["current_ratio"]),
                    str(r["lawsuit_count"]),
                ]
            )
    lines.append(_md_table(["企业", "状态", "指标叶子数", "revenue", "net_profit", "roe", "current_ratio", "lawsuit_count"], rows))
    lines.append("")

    lines.append("## 3. 评分服务验证（API + 缓存）")
    lines.append("")
    lines.append(
        _md_table(
            ["企业", "HTTP1", "T1(s)", "HTTP2", "T2(s)", "total_score", "rating", "body一致"],
            [
                [
                    r["enterprise"],
                    str(r["status1"]),
                    str(r["t1"]),
                    str(r["status2"]),
                    str(r["t2"]),
                    str(r.get("total_score")),
                    str(r.get("rating")),
                    str(r.get("same_body")),
                ]
                for r in scoring_checks
            ],
        )
    )
    lines.append("")

    lines.append("## 4. Agent API 多场景测试")
    lines.append("")
    lines.append(
        _md_table(
            ["场景", "HTTP", "耗时(s)", "status", "evidence条数", "summary非空"],
            [[r["scenario"], str(r["http"]), str(r["elapsed"]), str(r["status"]), str(r["evidence_n"]), str(r["report_summary_ok"])] for r in agent_results],
        )
    )
    lines.append("")

    lines.append("## 5. 异常处理测试")
    lines.append("")
    lines.append(_md_table(["case", "HTTP", "body(截断)"], [[r["case"], str(r["http"]), r["body"]] for r in exc_tests]))
    lines.append("")

    lines.append("## 6. 并发稳定性测试（10并发 Agent）")
    lines.append("")
    lines.append(f"- 成功数：{conc_ok}/10")
    lines.append(_md_table(["i", "HTTP", "耗时(s)", "ok"], [[str(r["i"]), str(r["http"]), str(r["elapsed"]), str(r["ok"])] for r in conc]))
    lines.append("")

    # final conclusion
    has_500 = any(r["http"] == 500 for r in agent_results) or any(r["http"] == 500 for r in exc_tests)
    lines.append("## 7. 结论")
    lines.append("")
    lines.append(f"- 是否出现 500：{has_500}")
    lines.append("- 备注：评分与 Agent 的响应体字符串可能因 JSON 字段顺序不同而不完全一致，应以语义字段（total_score/rating 等）为准。")
    lines.append("")

    out_path = os.environ.get("PHASE_FINAL_REPORT_PATH", "/app/phase_validation_final.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).rstrip() + "\n")
    print(f"Wrote report to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

