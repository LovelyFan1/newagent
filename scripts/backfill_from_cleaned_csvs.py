from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import asyncpg
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
CLEANED = ROOT / "data" / "cleaned"

FIN_PATH = CLEANED / "financials.csv"
SALES_PATH = CLEANED / "sales.csv"
LEGAL_PATH = CLEANED / "legal.csv"


def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    if v is not None and v.strip() != "":
        return v.strip()
    return default


def _normalize_pg_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn[len("postgresql+asyncpg://") :]
    return dsn


def _as_text(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s if s != "" else None


def _as_text_int(v: Any) -> str | None:
    if v is None or v == "":
        return None
    try:
        return str(int(float(v)))
    except Exception:
        s = str(v).strip()
        return s if s else None


def _as_year(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except Exception:
        return None


@dataclass(frozen=True)
class Ent:
    enterprise_id: str
    stock_name: str
    stock_code: str | None


async def _load_dim_enterprise(conn: asyncpg.Connection) -> tuple[dict[str, Ent], dict[str, Ent]]:
    rows = await conn.fetch("select enterprise_id, stock_name, stock_code from dim_enterprise")
    by_name: dict[str, Ent] = {}
    by_code: dict[str, Ent] = {}
    for r in rows:
        e = Ent(enterprise_id=str(r["enterprise_id"]), stock_name=str(r["stock_name"]), stock_code=(str(r["stock_code"]) if r["stock_code"] else None))
        by_name[e.stock_name] = e
        if e.stock_code:
            by_code[e.stock_code] = e
    return by_name, by_code


async def _sales_id_has_default(conn: asyncpg.Connection) -> bool:
    r = await conn.fetchrow(
        """
        select column_default
        from information_schema.columns
        where table_schema='public'
          and table_name='fact_sales'
          and column_name='sales_id'
        """
    )
    if not r:
        return False
    return bool(r["column_default"])


async def backfill_financials(conn: asyncpg.Connection) -> dict[str, Any]:
    df = pd.read_csv(FIN_PATH, dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]

    by_name, by_code = await _load_dim_enterprise(conn)

    # Map to enterprise_id; only keep rows that map.
    mapped = []
    for _, row in df.iterrows():
        stock_code = str(row.get("stock_code") or "").strip()
        stock_name = str(row.get("stock_name") or "").strip()
        y = _as_year(row.get("year"))
        if not y:
            continue
        ent = by_code.get(stock_code) or by_name.get(stock_name)
        if not ent:
            continue
        mapped.append(
            {
                "enterprise_id": ent.enterprise_id,
                "stock_code": stock_code or ent.stock_code or "",
                "stock_name": ent.stock_name,
                "year": str(y),
                "revenue": _as_text(row.get("revenue")),
                "net_profit": _as_text(row.get("net_profit")),
                "total_assets": _as_text(row.get("total_assets")),
                "total_liabilities": _as_text(row.get("total_liabilities")),
                "operating_cash_flow": _as_text(row.get("operating_cash_flow")),
                "current_ratio": _as_text(row.get("current_ratio")),
                "quick_ratio": _as_text(row.get("quick_ratio")),
                "debt_asset_ratio": _as_text(row.get("debt_asset_ratio")),
                "roe": _as_text(row.get("roe")),
                "net_margin": _as_text(row.get("net_margin")),
                "rd_expense": _as_text(row.get("rd_expense")),
                "time_id": None,
            }
        )

    await conn.execute("create temp table tmp_financials (like fact_financials including defaults) on commit drop")
    if mapped:
        await conn.executemany(
            """
            insert into tmp_financials
            (stock_code, stock_name, year, revenue, net_profit, total_assets, total_liabilities, operating_cash_flow,
             current_ratio, quick_ratio, debt_asset_ratio, roe, net_margin, rd_expense, time_id, enterprise_id)
            values
            ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
            """,
            [
                (
                    r["stock_code"],
                    r["stock_name"],
                    r["year"],
                    r["revenue"],
                    r["net_profit"],
                    r["total_assets"],
                    r["total_liabilities"],
                    r["operating_cash_flow"],
                    r["current_ratio"],
                    r["quick_ratio"],
                    r["debt_asset_ratio"],
                    r["roe"],
                    r["net_margin"],
                    r["rd_expense"],
                    r["time_id"],
                    r["enterprise_id"],
                )
                for r in mapped
            ],
        )

    # Validate: duplicates per (enterprise_id, year)
    dup_cnt = await conn.fetchval(
        "select count(*) from (select enterprise_id, year, count(*) c from tmp_financials group by enterprise_id, year having count(*)>1) x"
    )
    if dup_cnt and int(dup_cnt) > 0:
        raise RuntimeError(f"tmp_financials contains duplicate (enterprise_id,year) rows: {dup_cnt}")

    # Merge strategy: delete then insert for the impacted keys.
    impacted = await conn.fetchval("select count(*) from (select distinct enterprise_id, year from tmp_financials) x")
    await conn.execute(
        """
        delete from fact_financials f
        using (select distinct enterprise_id, year from tmp_financials) s
        where f.enterprise_id = s.enterprise_id and f.year::int = s.year::int
        """
    )
    inserted = await conn.fetchval(
        """
        insert into fact_financials
        (stock_code, stock_name, year, revenue, net_profit, total_assets, total_liabilities, operating_cash_flow,
         current_ratio, quick_ratio, debt_asset_ratio, roe, net_margin, rd_expense, time_id, enterprise_id)
        select stock_code, stock_name, year, revenue, net_profit, total_assets, total_liabilities, operating_cash_flow,
               current_ratio, quick_ratio, debt_asset_ratio, roe, net_margin, rd_expense, time_id, enterprise_id
        from tmp_financials
        returning 1
        """
    )
    # inserted is 1 or None due to returning; compute count separately.
    ins_cnt = await conn.fetchval("select count(*) from tmp_financials")
    return {"table": "fact_financials", "rows_stage": len(mapped), "keys_impacted": int(impacted or 0), "rows_inserted": int(ins_cnt or 0)}


async def backfill_sales(conn: asyncpg.Connection) -> dict[str, Any]:
    df = pd.read_csv(SALES_PATH, dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]

    by_name, by_code = await _load_dim_enterprise(conn)
    mapped = []
    for _, row in df.iterrows():
        stock_code = str(row.get("stock_code") or "").strip()
        stock_name = str(row.get("stock_name") or "").strip()
        y = _as_year(row.get("year"))
        if not y:
            continue
        ent = by_code.get(stock_code) or by_name.get(stock_name)
        if not ent:
            continue
        mapped.append(
            {
                "enterprise_id": ent.enterprise_id,
                "year": str(y),
                "total_sales_volume": _as_text(row.get("total_sales_volume")),
                "nev_sales_volume": _as_text(row.get("nev_sales_volume")),
                "time_id": None,
            }
        )

    await conn.execute("create temp table tmp_sales (like fact_sales including defaults) on commit drop")
    if mapped:
        # Insert without sales_id: allow default/identity to generate if exists.
        await conn.executemany(
            """
            insert into tmp_sales (enterprise_id, year, total_sales_volume, nev_sales_volume, time_id)
            values ($1,$2,$3,$4,$5)
            """,
            [(r["enterprise_id"], r["year"], r["total_sales_volume"], r["nev_sales_volume"], r["time_id"]) for r in mapped],
        )

    dup_cnt = await conn.fetchval(
        "select count(*) from (select enterprise_id, year, count(*) c from tmp_sales group by enterprise_id, year having count(*)>1) x"
    )
    if dup_cnt and int(dup_cnt) > 0:
        raise RuntimeError(f"tmp_sales contains duplicate (enterprise_id,year) rows: {dup_cnt}")

    impacted = await conn.fetchval("select count(*) from (select distinct enterprise_id, year from tmp_sales) x")
    await conn.execute(
        """
        delete from fact_sales f
        using (select distinct enterprise_id, year from tmp_sales) s
        where f.enterprise_id = s.enterprise_id and f.year::int = s.year::int
        """
    )

    has_default = await _sales_id_has_default(conn)
    if has_default:
        await conn.execute(
            """
            insert into fact_sales (enterprise_id, year, total_sales_volume, nev_sales_volume, time_id)
            select enterprise_id, year, total_sales_volume, nev_sales_volume, time_id
            from tmp_sales
            """
        )
    else:
        # Fallback: if sales_id has no default, we must provide one. Use max+row_number.
        max_id = await conn.fetchval(
            "select coalesce(max((sales_id)::bigint), 0) from fact_sales where sales_id ~ '^[0-9]+$'"
        )
        await conn.execute(
            """
            insert into fact_sales (sales_id, enterprise_id, year, total_sales_volume, nev_sales_volume, time_id)
            select ($1::bigint + row_number() over (order by enterprise_id, year))::text as sales_id,
                   enterprise_id, year, total_sales_volume, nev_sales_volume, time_id
            from tmp_sales
            """,
            int(max_id or 0),
        )

    ins_cnt = await conn.fetchval("select count(*) from tmp_sales")
    return {"table": "fact_sales", "rows_stage": len(mapped), "keys_impacted": int(impacted or 0), "rows_inserted": int(ins_cnt or 0), "sales_id_default": bool(has_default)}


async def backfill_legal(conn: asyncpg.Connection) -> dict[str, Any]:
    df = pd.read_csv(LEGAL_PATH, dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]

    by_name, by_code = await _load_dim_enterprise(conn)
    mapped = []
    for _, row in df.iterrows():
        stock_code = str(row.get("stock_code") or "").strip()
        stock_name = str(row.get("stock_name") or "").strip()
        y = _as_year(row.get("year"))
        if not y:
            continue
        ent = by_code.get(stock_code) or by_name.get(stock_name)
        if not ent:
            continue
        mapped.append(
            {
                "enterprise_id": ent.enterprise_id,
                "stock_code": stock_code or ent.stock_code or "",
                "stock_name": ent.stock_name,
                "year": str(y),
                "lawsuit_count": _as_text_int(row.get("lawsuit_count")),
                "lawsuit_total_amount": _as_text(row.get("lawsuit_total_amount")),
                "time_id": None,
            }
        )

    await conn.execute("create temp table tmp_legal (like fact_legal including defaults) on commit drop")
    if mapped:
        await conn.executemany(
            """
            insert into tmp_legal
            (stock_code, stock_name, year, lawsuit_count, lawsuit_total_amount, time_id, enterprise_id)
            values ($1,$2,$3,$4,$5,$6,$7)
            """,
            [
                (
                    r["stock_code"],
                    r["stock_name"],
                    r["year"],
                    r["lawsuit_count"],
                    r["lawsuit_total_amount"],
                    r["time_id"],
                    r["enterprise_id"],
                )
                for r in mapped
            ],
        )

    dup_cnt = await conn.fetchval(
        "select count(*) from (select enterprise_id, year, count(*) c from tmp_legal group by enterprise_id, year having count(*)>1) x"
    )
    if dup_cnt and int(dup_cnt) > 0:
        raise RuntimeError(f"tmp_legal contains duplicate (enterprise_id,year) rows: {dup_cnt}")

    # Use real upsert if unique index exists (it does: ux_fact_legal_enterprise_year).
    await conn.execute(
        """
        insert into fact_legal (stock_code, stock_name, year, lawsuit_count, lawsuit_total_amount, time_id, enterprise_id)
        select stock_code, stock_name, year, lawsuit_count, lawsuit_total_amount, time_id, enterprise_id
        from tmp_legal
        on conflict (enterprise_id, year)
        do update set
          stock_code = excluded.stock_code,
          stock_name = excluded.stock_name,
          lawsuit_count = excluded.lawsuit_count,
          lawsuit_total_amount = excluded.lawsuit_total_amount,
          time_id = excluded.time_id
        """
    )

    ins_cnt = await conn.fetchval("select count(*) from tmp_legal")
    impacted = await conn.fetchval("select count(*) from (select distinct enterprise_id, year from tmp_legal) x")
    return {"table": "fact_legal", "rows_stage": len(mapped), "keys_impacted": int(impacted or 0), "rows_upserted": int(ins_cnt or 0)}


async def main() -> None:
    dsn = _env("DATABASE_URL") or "postgresql://app_v2:app_v2@127.0.0.1:5432/app_v2"
    dsn = _normalize_pg_dsn(dsn)
    conn = await asyncpg.connect(dsn=dsn)
    try:
        async with conn.transaction():
            s1 = await backfill_financials(conn)
            s2 = await backfill_sales(conn)
            s3 = await backfill_legal(conn)
        print("## backfill_from_cleaned_csvs done")
        print(s1)
        print(s2)
        print(s3)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())

