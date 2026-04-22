from __future__ import annotations

import asyncio
import json
import os
import random
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

import httpx
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_sessionmaker
from app.services.indicator_calc import calculate_indicators
from app.services.scoring_service import scoring_service


PICKED_ENTERPRISES = [
    "比亚迪",
    "上汽集团",
    "长城汽车",
    "长安汽车",
    "一汽解放",
    "潍柴动力",
    "江铃汽车",
    "海马汽车",
    "威孚高科",
    "万向钱潮",
]


@dataclass
class CoverageRow:
    stock_name: str
    has_financial_2022: bool
    has_sales_2022: bool
    has_legal_2022: bool
    financial_missing_ratio: float | None


def _fmt_bool(v: bool) -> str:
    return "有" if v else "无"


def _md_table(headers: list[str], rows: Iterable[list[str]]) -> str:
    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for r in rows:
        out.append("| " + " | ".join(r) + " |")
    return "\n".join(out)


async def _fetch_coverage(db: AsyncSession, year: int = 2022) -> list[CoverageRow]:
    coverage_sql = sa.text(
        """
        WITH picked AS (
            SELECT enterprise_id, stock_name
            FROM dim_enterprise
            WHERE stock_name = ANY(:names)
        ),
        fin_has AS (
            SELECT DISTINCT enterprise_id
            FROM fact_financials
            WHERE CAST(year AS INTEGER) = :year
        ),
        sales_has AS (
            SELECT DISTINCT enterprise_id
            FROM fact_sales
            WHERE CAST(year AS INTEGER) = :year
        ),
        legal_has AS (
            SELECT DISTINCT enterprise_id
            FROM fact_legal
            WHERE CAST(year AS INTEGER) = :year
        ),
        fin_missing AS (
            SELECT
                enterprise_id,
                (
                    (CASE WHEN revenue IS NULL OR btrim(revenue) = '' OR lower(revenue)='nan' THEN 1 ELSE 0 END) +
                    (CASE WHEN net_profit IS NULL OR btrim(net_profit) = '' OR lower(net_profit)='nan' THEN 1 ELSE 0 END) +
                    (CASE WHEN total_assets IS NULL OR btrim(total_assets) = '' OR lower(total_assets)='nan' THEN 1 ELSE 0 END) +
                    (CASE WHEN total_liabilities IS NULL OR btrim(total_liabilities) = '' OR lower(total_liabilities)='nan' THEN 1 ELSE 0 END) +
                    (CASE WHEN operating_cash_flow IS NULL OR btrim(operating_cash_flow) = '' OR lower(operating_cash_flow)='nan' THEN 1 ELSE 0 END) +
                    (CASE WHEN current_ratio IS NULL OR btrim(current_ratio) = '' OR lower(current_ratio)='nan' THEN 1 ELSE 0 END) +
                    (CASE WHEN quick_ratio IS NULL OR btrim(quick_ratio) = '' OR lower(quick_ratio)='nan' THEN 1 ELSE 0 END) +
                    (CASE WHEN roe IS NULL OR btrim(roe) = '' OR lower(roe)='nan' THEN 1 ELSE 0 END)
                ) AS missing_cnt
            FROM fact_financials
            WHERE CAST(year AS INTEGER) = :year
        ),
        fin_best AS (
            SELECT enterprise_id, MIN(missing_cnt) AS missing_cnt
            FROM fin_missing
            GROUP BY enterprise_id
        )
        SELECT
            p.stock_name,
            (fin_has.enterprise_id IS NOT NULL) AS has_fin,
            (sales_has.enterprise_id IS NOT NULL) AS has_sales,
            (legal_has.enterprise_id IS NOT NULL) AS has_legal,
            CASE WHEN fin_best.enterprise_id IS NULL THEN NULL ELSE (fin_best.missing_cnt::float / 8.0) END AS fin_missing_ratio
        FROM picked p
        LEFT JOIN fin_has ON fin_has.enterprise_id = p.enterprise_id
        LEFT JOIN sales_has ON sales_has.enterprise_id = p.enterprise_id
        LEFT JOIN legal_has ON legal_has.enterprise_id = p.enterprise_id
        LEFT JOIN fin_best ON fin_best.enterprise_id = p.enterprise_id
        ORDER BY p.stock_name;
        """
    )
    res = await db.execute(coverage_sql, {"names": PICKED_ENTERPRISES, "year": year})
    rows = []
    for m in res.mappings().all():
        rows.append(
            CoverageRow(
                stock_name=m["stock_name"],
                has_financial_2022=bool(m["has_fin"]),
                has_sales_2022=bool(m["has_sales"]),
                has_legal_2022=bool(m["has_legal"]),
                financial_missing_ratio=None if m["fin_missing_ratio"] is None else round(float(m["fin_missing_ratio"]), 2),
            )
        )
    return rows


async def _api_get(path: str, params: dict[str, Any] | None = None) -> tuple[int, str]:
    base_url = os.environ.get("PHASE2_BASE_URL", "http://127.0.0.1:8000")
    async with httpx.AsyncClient(base_url=base_url, timeout=60.0) as c:
        r = await c.get(path, params=params)
        return r.status_code, r.text


async def _cache_test(stock_key: str, year: int) -> dict[str, Any]:
    # clear cached row
    sm = get_sessionmaker()
    async with sm() as db:
        await db.execute(
            sa.text("DELETE FROM scoring_results WHERE stock_code = :k AND year = :y"),
            {"k": stock_key, "y": year},
        )
        await db.commit()

    # warm: first request
    path = "/api/v1/scoring/" + urllib.parse.quote(stock_key)
    t0 = time.perf_counter()
    s1, body1 = await _api_get(path, params={"year": year})
    t1 = time.perf_counter() - t0

    # second request
    t0 = time.perf_counter()
    s2, body2 = await _api_get(path, params={"year": year})
    t2 = time.perf_counter() - t0

    def _safe_json(s: str) -> Any:
        try:
            return json.loads(s)
        except Exception:
            return None

    j1 = _safe_json(body1)
    j2 = _safe_json(body2)
    same_score = False
    if isinstance(j1, dict) and isinstance(j2, dict):
        d1 = (j1.get("data") or {}) if isinstance(j1.get("data"), dict) else {}
        d2 = (j2.get("data") or {}) if isinstance(j2.get("data"), dict) else {}
        same_score = (d1.get("total_score") == d2.get("total_score")) and (d1.get("rating") == d2.get("rating"))

    # check inserted
    async with sm() as db:
        inserted = (
            await db.execute(
                sa.text(
                    "SELECT count(*)::int AS c FROM scoring_results WHERE stock_code = :k AND year = :y",
                ),
                {"k": stock_key, "y": year},
            )
        ).mappings().one()["c"]

    return {
        "request": {"stock_key": stock_key, "year": year},
        "status1": s1,
        "status2": s2,
        "t1_sec": round(t1, 3),
        "t2_sec": round(t2, 3),
        "body1": body1,
        "body2": body2,
        "inserted_rows": inserted,
        "same_body": body1 == body2,
        "same_total_score_and_rating": same_score,
    }


async def _concurrency_test(stock_keys: list[str], year: int) -> dict[str, Any]:
    path_prefix = "/api/v1/scoring/"

    async def one(k: str) -> dict[str, Any]:
        p = path_prefix + urllib.parse.quote(k)
        t0 = time.perf_counter()
        s, body = await _api_get(p, params={"year": year})
        return {"stock_key": k, "status": s, "elapsed_sec": round(time.perf_counter() - t0, 3), "body": body}

    results = await asyncio.gather(*[one(k) for k in stock_keys])

    sm = get_sessionmaker()
    async with sm() as db:
        dup = (
            await db.execute(
                sa.text(
                    """
                    SELECT stock_code, year, count(*)::int AS c
                    FROM scoring_results
                    WHERE stock_code = ANY(:keys) AND year = :year
                    GROUP BY stock_code, year
                    HAVING count(*) > 1
                    """
                ),
                {"keys": stock_keys, "year": year},
            )
        ).mappings().all()

    return {"stock_keys": stock_keys, "year": year, "results": results, "duplicate_rows": dup}


async def _sample_scoring(names: list[str], year: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for n in names:
        try:
            ind = await calculate_indicators(n, year)
        except Exception as e:
            out.append({"stock_key": n, "year": year, "error": f"calculate_indicators failed: {e}"})
            continue
        score = await scoring_service.calculate_score(n, year)
        out.append(
            {
                "stock_key": n,
                "year": year,
                "stock_code": ind.get("stock_code"),
                "enterprise_name": ind.get("enterprise_name"),
                "total_score": None if not score else score.get("total_score"),
                "rating": None if not score else score.get("rating"),
                "dimension_scores": None if not score else score.get("dimension_scores"),
                "indicator_financial": ind["indicators"]["financial_health"],
                "indicator_legal": ind["indicators"]["legal_risk"],
            }
        )
    return out


async def main() -> int:
    now = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    year = int(os.environ.get("PHASE2_YEAR", "2022"))

    sm = get_sessionmaker()
    async with sm() as db:
        coverage = await _fetch_coverage(db, year=year)

    # Choose sample 3 from the picked list (deterministic seed for reproducibility)
    rnd = random.Random(20260422)
    candidates = [c.stock_name for c in coverage]
    samples = ["比亚迪"]
    rest = [x for x in candidates if x not in samples]
    rnd.shuffle(rest)
    samples += rest[:2]

    sample_results = await _sample_scoring(samples, year)

    # API abnormal cases
    api_cases = [
        ("不存在股票代码", "/api/v1/scoring/INVALID", {"year": year}),
        ("无数据年份", "/api/v1/scoring/" + urllib.parse.quote("比亚迪"), {"year": 1999}),
        ("缺失year参数", "/api/v1/scoring/" + urllib.parse.quote("比亚迪"), None),
        ("year格式无效", "/api/v1/scoring/" + urllib.parse.quote("比亚迪"), {"year": "abc"}),
    ]
    # ensure the "no data year" isn't served from cache from previous runs
    async with sm() as db:
        await db.execute(sa.text("DELETE FROM scoring_results WHERE stock_code = :k AND year = :y"), {"k": "比亚迪", "y": 1999})
        await db.commit()
    api_results = []
    for title, path, params in api_cases:
        status, text = await _api_get(path, params=params)
        api_results.append({"case": title, "path": path, "params": params, "status": status, "body": text})

    # Cache test (use a key that exists by name; stock_code availability is not guaranteed)
    cache_key = "比亚迪"
    cache_result = await _cache_test(cache_key, year)

    # Concurrency test (5 keys)
    concurrency_keys = [c.stock_name for c in coverage][:5]
    conc_result = await _concurrency_test(concurrency_keys, year)

    # Render report
    lines: list[str] = []
    lines.append("# Phase2 评分系统深度验证报告")
    lines.append("")
    lines.append(f"- 验证时间：{now}")
    lines.append(f"- 验证年份：{year}")
    lines.append("")
    lines.append("## 1. 核心企业数据覆盖检查")
    lines.append("")
    lines.append(
        "> 说明：当前 `dim_enterprise` 仅包含 `stock_name/stock_code/standard_name`，没有行业字段，因此“跨行业抽样”无法从库内自动保证；此处按代表性企业名 + 额外样本组合进行覆盖检查。"
    )
    lines.append("")
    lines.append(
        _md_table(
            ["企业名", "财务(2022)", "销售(2022)", "司法(2022)", "财务核心字段缺失率"],
            [
                [
                    c.stock_name,
                    _fmt_bool(c.has_financial_2022),
                    _fmt_bool(c.has_sales_2022),
                    _fmt_bool(c.has_legal_2022),
                    "" if c.financial_missing_ratio is None else str(c.financial_missing_ratio),
                ]
                for c in coverage
            ],
        )
    )
    lines.append("")

    high_missing = [c for c in coverage if c.financial_missing_ratio is not None and c.financial_missing_ratio > 0.5]
    if high_missing:
        lines.append("### 覆盖缺失提示（>50%财务核心字段为空）")
        for c in high_missing:
            lines.append(f"- {c.stock_name}: financial_missing_ratio={c.financial_missing_ratio}")
        lines.append("")
        lines.append("建议：优先补齐 `fact_financials` 2022 年的核心字段（revenue/net_profit/total_assets/total_liabilities/operating_cash_flow/current_ratio/quick_ratio/roe）。")
        lines.append("")

    lines.append("## 2. 评分计算逻辑抽样验证（3家）")
    lines.append("")
    for r in sample_results:
        lines.append(f"### 样本：{r.get('stock_key')}（{year}）")
        if "error" in r:
            lines.append(f"- 失败原因：{r['error']}")
            lines.append("")
            continue
        lines.append(f"- 总分：{r.get('total_score')}")
        lines.append(f"- 评级：{r.get('rating')}")
        lines.append(f"- 四维得分：`{json.dumps(r.get('dimension_scores', {}), ensure_ascii=False)}`")
        fin = r["indicator_financial"]
        legal = r["indicator_legal"]
        lines.append(f"- 财务关键指标（原始）：revenue={fin.get('revenue')}, net_profit={fin.get('net_profit')}, current_ratio={fin.get('current_ratio')}")
        lines.append(f"- 司法关键指标（原始）：lawsuit_count={legal.get('lawsuit_count')}, lawsuit_total_amount={legal.get('lawsuit_total_amount')}")
        lines.append("- 合理性说明：")
        lines.append("  - 财务维度：流动比率/速动比率/ROE 等指标会通过阈值+线性插值映射到 0-100，再按权重汇总。")
        lines.append("  - 司法维度：当前指标引擎以 `fact_legal` 生成 lawsuit 指标，但评分口径使用 execution_ratio/dishonest_count/commercial_paper_default（若数据源缺失则会趋向中性/默认值）。")
        lines.append("  - 评级映射：A(>=80), B(>=65), C(>=50), D(<50)。")
        lines.append("")

    lines.append("## 3. API 异常处理测试")
    lines.append("")
    lines.append(
        _md_table(
            ["场景", "HTTP状态", "请求", "响应(截断)"],
            [
                [
                    a["case"],
                    str(a["status"]),
                    f"{a['path']}?{urllib.parse.urlencode(a['params'] or {})}" if a["params"] is not None else a["path"],
                    (a["body"][:160] + "...") if len(a["body"]) > 160 else a["body"],
                ]
                for a in api_results
            ],
        )
    )
    lines.append("")
    lines.append("预期检查：上述异常应为 4xx；其中 FastAPI 参数校验会返回 422。")
    lines.append("")

    lines.append("## 4. 缓存机制验证（T1/T2）")
    lines.append("")
    lines.append(f"- 测试对象：{cache_result['request']}")
    lines.append(f"- 第一次：status={cache_result['status1']}，T1={cache_result['t1_sec']}s")
    lines.append(f"- 第二次：status={cache_result['status2']}，T2={cache_result['t2_sec']}s")
    lines.append(f"- 两次 body 完全一致：{cache_result['same_body']}")
    lines.append(f"- total_score/rating 一致：{cache_result['same_total_score_and_rating']}")
    lines.append(f"- DB 插入行数：{cache_result['inserted_rows']}")
    lines.append("")

    lines.append("## 5. 并发请求测试（5并发）")
    lines.append("")
    lines.append(f"- keys={conc_result['stock_keys']}, year={conc_result['year']}")
    lines.append(
        _md_table(
            ["stock_key", "status", "elapsed_sec"],
            [[r["stock_key"], str(r["status"]), str(r["elapsed_sec"])] for r in conc_result["results"]],
        )
    )
    lines.append("")
    lines.append(f"- 重复记录检查（应为空）：`{json.dumps(conc_result['duplicate_rows'], ensure_ascii=False)}`")
    lines.append("")

    lines.append("## 6. 发现的问题及修复记录")
    lines.append("")
    lines.append("- 如果发现 `scoring_results` 表缺失但 `alembic_version` 已在 head，会导致 API 500；本项目已增加 `0003_ensure_scoring_results` 迁移做兜底修复。")
    lines.append("- 当前评分口径与指标引擎输出存在“司法维度字段不完全对应”的风险（指标引擎侧 lawsuit_*，评分侧 execution_ratio/dishonest_count/...）。建议下一阶段统一字段口径或在指标引擎中补齐评分所需字段。")
    lines.append("")

    lines.append("## 7. 最终结论")
    lines.append("")
    lines.append("- 通过情况：**待确认**（以 2022 覆盖率、异常返回是否全部为 4xx、缓存 T2 显著低于 T1 为准）")
    lines.append("")

    report_path = os.environ.get("PHASE2_REPORT_PATH", "/app/phase2_scoring_validation_report.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).rstrip() + "\n")

    print(f"Wrote report to {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

