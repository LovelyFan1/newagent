from __future__ import annotations

import asyncio
import json
import os
import random
import statistics
import time
from dataclasses import dataclass
from typing import Any

import asyncpg
import httpx


def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    if v is not None and v.strip() != "":
        return v.strip()
    return default


def _normalize_pg_dsn(dsn: str) -> str:
    # Accept SQLAlchemy async DSN like: postgresql+asyncpg://...
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn[len("postgresql+asyncpg://") :]
    return dsn


@dataclass(frozen=True)
class QueryResult:
    enterprise: str
    elapsed_ms: float
    ok: bool
    cache_hit: bool | None
    status: str | None
    note: str | None = None


async def _wait_api_ready(client: httpx.AsyncClient, base_url: str) -> None:
    for _ in range(60):
        try:
            r = await client.get(f"{base_url}/docs", timeout=2.0)
            if r.status_code < 500:
                return
        except Exception:
            pass
        await asyncio.sleep(1.0)
    raise RuntimeError("API not ready: /docs not reachable")


async def _pick_random_enterprises(conn: asyncpg.Connection, *, year: int, n: int) -> list[str]:
    # Prefer picking enterprises that actually have sales data for the target year (2023).
    rows = await conn.fetch(
        """
        SELECT DISTINCT de.stock_name AS enterprise_name
        FROM dim_enterprise de
        JOIN fact_sales fs ON fs.enterprise_id = de.enterprise_id
        WHERE de.stock_name IS NOT NULL
          AND fs.year::int = $1
          AND fs.total_sales_volume IS NOT NULL
        """,
        int(year),
    )
    names = [str(r["enterprise_name"]) for r in rows if r and r.get("enterprise_name")]
    if len(names) < n:
        # Fallback: any-year sales data
        rows2 = await conn.fetch(
            """
            SELECT DISTINCT de.stock_name AS enterprise_name
            FROM dim_enterprise de
            JOIN fact_sales fs ON fs.enterprise_id = de.enterprise_id
            WHERE de.stock_name IS NOT NULL
              AND fs.total_sales_volume IS NOT NULL
            """
        )
        names = [str(r["enterprise_name"]) for r in rows2 if r and r.get("enterprise_name")]
    if not names:
        raise RuntimeError("No enterprises found with fact_sales data")
    random.shuffle(names)
    return names[:n]


async def _agent_query(client: httpx.AsyncClient, *, base_url: str, question: str) -> tuple[dict[str, Any], float]:
    payload = {"question": question, "session_id": "sample_perf"}
    t0 = time.perf_counter()
    r = await client.post(f"{base_url}/api/v1/agent/query", json=payload, timeout=180.0)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    r.raise_for_status()
    body = r.json()
    data = body.get("data") if isinstance(body, dict) else None
    if isinstance(data, dict):
        return data, elapsed_ms
    if isinstance(body, dict):
        return body, elapsed_ms
    raise RuntimeError("unexpected response shape")


async def _try_clear_comparison_cache(
    *,
    redis_url: str | None,
    enterprises: list[str],
    years: list[int],
) -> bool:
    if not redis_url:
        return False
    try:
        import redis.asyncio as redis  # type: ignore

        ent_part = ",".join(sorted(enterprises))
        year_part = ",".join(str(y) for y in sorted(years))
        key = f"comparison:{ent_part}:{year_part}"
        rd = redis.Redis.from_url(redis_url, encoding="utf-8", decode_responses=True)
        await rd.delete(key)
        await rd.aclose()
        return True
    except Exception:
        return False


def _fmt_ms(x: float) -> str:
    return f"{x:.1f}ms"


async def main() -> None:
    year = int(_env("PERF_YEAR", "2023") or "2023")
    n = int(_env("PERF_N", "10") or "10")
    expect_ms = float(_env("EXPECT_SIMPLE_MS", "200") or "200")
    expect_cache_ms = float(_env("EXPECT_CACHE_MS", "50") or "50")

    base_url = _env("API_BASE_URL", "http://127.0.0.1:8000") or "http://127.0.0.1:8000"
    db_url = _env("DATABASE_URL") or _env("POSTGRES_URL") or _env("DB_URL")
    if not db_url:
        raise RuntimeError("Missing DATABASE_URL (or POSTGRES_URL/DB_URL)")
    db_dsn = _normalize_pg_dsn(db_url)
    redis_url = _env("REDIS_URL")

    async with httpx.AsyncClient() as client:
        await _wait_api_ready(client, base_url)

        conn = await asyncpg.connect(dsn=db_dsn)
        try:
            enterprises = await _pick_random_enterprises(conn, year=year, n=n)
        finally:
            await conn.close()

        print("# 随机抽查10家企业性能验证报告")
        print()
        print(f"- API：`{base_url}/api/v1/agent/query`（实际为 POST）")
        print(f"- 抽样：{len(enterprises)} 家（目标年份：{year}）")
        print(f"- 简单查询阈值：<{int(expect_ms)}ms（单次）")
        print(f"- 对比缓存阈值：第二次 <{int(expect_cache_ms)}ms 且 cache_hit=true")
        print()

        results: list[QueryResult] = []
        for ent in enterprises:
            q = f"{ent}{year}年销量"
            try:
                data, ms = await _agent_query(client, base_url=base_url, question=q)
                cache_hit = data.get("cache_hit") if isinstance(data, dict) else None
                status = data.get("status") if isinstance(data, dict) else None
                results.append(QueryResult(enterprise=ent, elapsed_ms=ms, ok=True, cache_hit=cache_hit, status=status))
            except Exception as e:
                results.append(QueryResult(enterprise=ent, elapsed_ms=999999.0, ok=False, cache_hit=None, status=None, note=str(e)))

        ok_ms = [r.elapsed_ms for r in results if r.ok]
        min_ms = min(ok_ms) if ok_ms else float("inf")
        max_ms = max(ok_ms) if ok_ms else float("inf")
        avg_ms = statistics.mean(ok_ms) if ok_ms else float("inf")
        all_under = all((r.ok and r.elapsed_ms < expect_ms) for r in results)

        print("## 简单查询抽查明细（{企业}2023年销量）")
        for r in results:
            if r.ok:
                print(f"- {r.enterprise}: {_fmt_ms(r.elapsed_ms)} status={r.status}")
            else:
                print(f"- {r.enterprise}: FAILED err={r.note}")
        print()
        print("## 简单查询统计")
        print(f"- min: {_fmt_ms(min_ms)}")
        print(f"- max: {_fmt_ms(max_ms)}")
        print(f"- avg: {_fmt_ms(avg_ms)}")
        print(f"- 是否全部 <{int(expect_ms)}ms: {'PASS' if all_under else 'FAIL'}")
        print()

        print("## 对比查询缓存命中验证")
        cmp_ents = ["比亚迪", "长城", "理想"]
        # NOTE: cache key years are derived from backend TimeRange.years(default_year=2022).
        # For queries without an explicit year, TimeRange defaults to LAST_3_YEARS -> [2020, 2021, 2022].
        years_for_key = [2020, 2021, 2022]
        cleared = await _try_clear_comparison_cache(redis_url=redis_url, enterprises=cmp_ents, years=years_for_key)
        print(f"- 预清理缓存：{'成功' if cleared else '跳过/失败（无 REDIS_URL 或连接失败）'}")
        print(f"- 预清理 key 年份：{years_for_key}")

        cmp_q = "、".join(cmp_ents) + " 哪个更值得投资"
        first_data, first_ms = await _agent_query(client, base_url=base_url, question=cmp_q)
        first_hit = bool(first_data.get("cache_hit")) if isinstance(first_data, dict) else False

        second_data, second_ms = await _agent_query(client, base_url=base_url, question=cmp_q)
        second_hit = bool(second_data.get("cache_hit")) if isinstance(second_data, dict) else False

        cache_pass = (not first_hit) and second_hit and (second_ms < expect_cache_ms)
        print(f"- 第一次: {_fmt_ms(first_ms)} cache_hit={first_hit}")
        print(f"- 第二次: {_fmt_ms(second_ms)} cache_hit={second_hit}")
        print(f"- 结论: {'PASS' if cache_pass else 'FAIL'}（要求：第一次 miss，第二次 hit 且 <{int(expect_cache_ms)}ms）")
        print()

        print("## 总体结论")
        overall_pass = all_under and cache_pass
        print(f"- 简单查询达标：{'PASS' if all_under else 'FAIL'}")
        print(f"- 对比缓存达标：{'PASS' if cache_pass else 'FAIL'}")
        print(f"- 最终：{'PASS' if overall_pass else 'FAIL'}")


if __name__ == "__main__":
    asyncio.run(main())

