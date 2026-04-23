from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Any

import asyncpg


TARGET_ENTS = [
    "比亚迪",
    "长城汽车",
    "蔚来",
    "理想",
    "小鹏",
    "上汽集团",
    "广汽集团",
    "长安汽车",
    "吉利汽车",
    "宁德时代",
    "国轩高科",
    "亿纬锂能",
    "潍柴动力",
    "均胜电子",
    "华域汽车",
    "福耀玻璃",
    "德赛西威",
    "中科创达",
    "万向钱潮",
    "一汽解放",
]

YEARS = [2020, 2021, 2022, 2023]


def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    if v is not None and v.strip() != "":
        return v.strip()
    return default


def _normalize_pg_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn[len("postgresql+asyncpg://") :]
    return dsn


@dataclass(frozen=True)
class EntRow:
    enterprise_id: Any
    stock_code: str | None
    stock_name: str


def _md_table(headers: list[str], rows: list[list[Any]]) -> str:
    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("|" + "|".join(["---"] * len(headers)) + "|")
    for r in rows:
        out.append("| " + " | ".join("" if v is None else str(v) for v in r) + " |")
    return "\n".join(out)


async def _load_enterprises(conn: asyncpg.Connection) -> dict[str, EntRow | None]:
    # 1) exact match on stock_name
    rows = await conn.fetch(
        """
        SELECT enterprise_id, stock_code, stock_name
        FROM dim_enterprise
        WHERE stock_name = ANY($1::text[])
        ORDER BY (stock_code IS NULL OR stock_code = '') ASC, stock_code ASC, stock_name ASC
        """,
        TARGET_ENTS,
    )
    found: dict[str, asyncpg.Record] = {}
    for r in rows:
        found[str(r["stock_name"])] = r

    # 2) fallback: contains match (e.g. "比亚迪股份" vs "比亚迪")
    missing = [n for n in TARGET_ENTS if n not in found]
    if missing:
        rows2 = await conn.fetch(
            """
            SELECT enterprise_id, stock_code, stock_name
            FROM dim_enterprise
            WHERE EXISTS (
              SELECT 1
              FROM unnest($1::text[]) AS t(name)
              WHERE dim_enterprise.stock_name ILIKE '%' || t.name || '%'
                 OR t.name ILIKE '%' || dim_enterprise.stock_name || '%'
            )
            """,
            missing,
        )
        # keep best per missing token: prefer longer stock_name match
        for token in missing:
            cand = [r for r in rows2 if (token in str(r["stock_name"])) or (str(r["stock_name"]) in token)]
            if not cand:
                continue
            cand.sort(key=lambda x: len(str(x["stock_name"])), reverse=True)
            found[token] = cand[0]

    out: dict[str, EntRow | None] = {}
    for token in TARGET_ENTS:
        r = found.get(token)
        if not r:
            out[token] = None
            continue
        out[token] = EntRow(enterprise_id=r["enterprise_id"], stock_code=r["stock_code"], stock_name=r["stock_name"])
    return out


async def _diagnose_financials(conn: asyncpg.Connection, ent: EntRow) -> list[tuple[int, list[str]]]:
    # For each year, find row and null fields.
    rows = await conn.fetch(
        """
        SELECT year::int AS y,
               revenue, net_profit, total_assets, total_liabilities,
               current_ratio, quick_ratio, roe
        FROM fact_financials
        WHERE enterprise_id = $1
          AND year::int = ANY($2::int[])
        """,
        ent.enterprise_id,
        YEARS,
    )
    by_year: dict[int, asyncpg.Record] = {int(r["y"]): r for r in rows}
    missing: list[tuple[int, list[str]]] = []
    fields = ["revenue", "net_profit", "total_assets", "total_liabilities", "current_ratio", "quick_ratio", "roe"]
    for y in YEARS:
        r = by_year.get(y)
        if not r:
            missing.append((y, ["ROW_MISSING"]))
            continue
        nulls = [f for f in fields if r.get(f) is None]
        if nulls:
            missing.append((y, nulls))
    return missing


async def _diagnose_sales(conn: asyncpg.Connection, ent: EntRow) -> list[int]:
    rows = await conn.fetch(
        """
        SELECT year::int AS y, total_sales_volume
        FROM fact_sales
        WHERE enterprise_id = $1
          AND year::int = ANY($2::int[])
        """,
        ent.enterprise_id,
        YEARS,
    )
    by_year: dict[int, asyncpg.Record] = {int(r["y"]): r for r in rows}
    missing: list[int] = []
    for y in YEARS:
        r = by_year.get(y)
        if not r:
            missing.append(y)
            continue
        if r.get("total_sales_volume") is None:
            missing.append(y)
    return missing


async def main() -> None:
    dsn = _env("DATABASE_URL") or "postgresql://app_v2:app_v2@127.0.0.1:5432/app_v2"
    dsn = _normalize_pg_dsn(dsn)
    conn = await asyncpg.connect(dsn=dsn)
    try:
        mapping = await _load_enterprises(conn)
        lines: list[str] = []
        lines.append("## 企业映射（dim_enterprise）\n")
        map_rows: list[list[Any]] = []
        for token in TARGET_ENTS:
            e = mapping.get(token)
            if e is None:
                map_rows.append([token, "NOT_FOUND", "", ""])
            else:
                map_rows.append([token, e.enterprise_id, e.stock_code or "", e.stock_name])
        lines.append(_md_table(["token", "enterprise_id", "stock_code", "stock_name"], map_rows))
        lines.append("")

        fin_rows: list[list[Any]] = []
        sales_rows: list[list[Any]] = []

        for token in TARGET_ENTS:
            e = mapping.get(token)
            if e is None:
                fin_rows.append([token, "ALL", "ENTERPRISE_NOT_FOUND_IN_DIM"])
                sales_rows.append([token, "ALL"])
                continue
            fin_missing = await _diagnose_financials(conn, e)
            for y, fields in fin_missing:
                fin_rows.append([e.stock_name, y, ", ".join(fields)])
            s_missing = await _diagnose_sales(conn, e)
            if s_missing:
                sales_rows.append([e.stock_name, ", ".join(str(x) for x in s_missing)])

        lines.append("## 财务缺失报告（fact_financials）\n")
        lines.append(_md_table(["企业", "缺失年份", "缺失字段"], fin_rows) if fin_rows else "（无缺失）")
        lines.append("")

        lines.append("## 销售缺失报告（fact_sales）\n")
        lines.append(_md_table(["企业", "缺失年份"], sales_rows) if sales_rows else "（无缺失）")
        lines.append("")

        text = "\n".join(lines)
        report_path = os.path.join(os.path.dirname(__file__), "diagnose_missing_coverage_report.md")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(text)
        print(text)

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())

