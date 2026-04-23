from __future__ import annotations

import asyncio
import os

import asyncpg
import pandas as pd


TARGET_NOT_FOUND = [
    "蔚来",
    "理想",
    "小鹏",
    "吉利汽车",
    "宁德时代",
    "国轩高科",
    "亿纬锂能",
    "德赛西威",
    "中科创达",
]


def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    if v is not None and v.strip() != "":
        return v.strip()
    return default


def _normalize_pg_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn[len("postgresql+asyncpg://") :]
    return dsn


def _pick_stock_code(token: str, ent_df: pd.DataFrame) -> str | None:
    # Try exact match on stock_name, then contains match on company_name.
    df = ent_df
    df1 = df[df["stock_name"].fillna("").astype(str) == token]
    if not df1.empty:
        code = str(df1.iloc[0].get("stock_code") or "").strip()
        return code or None
    df2 = df[df["company_name"].fillna("").astype(str).str.contains(token, na=False)]
    if not df2.empty:
        code = str(df2.iloc[0].get("stock_code") or "").strip()
        return code or None
    return None


async def main() -> None:
    dsn = _env("DATABASE_URL") or "postgresql://app_v2:app_v2@127.0.0.1:5432/app_v2"
    dsn = _normalize_pg_dsn(dsn)

    ent_path = os.path.join(os.path.dirname(__file__), "..", "data", "cleaned", "enterprise_basic.csv")
    ent_df = pd.read_csv(ent_path, dtype=str).fillna("")
    ent_df.columns = [c.strip() for c in ent_df.columns]

    conn = await asyncpg.connect(dsn=dsn)
    try:
        # enterprise_id is stored as text in this dataset; use integer-like ids for new rows.
        max_id = await conn.fetchval(
            "select coalesce(max((enterprise_id)::int), 0) from dim_enterprise where enterprise_id ~ '^[0-9]+$'"
        )
        next_id = int(max_id or 0) + 1

        inserted = 0
        skipped = 0
        for token in TARGET_NOT_FOUND:
            exists = await conn.fetchval("select 1 from dim_enterprise where stock_name=$1 limit 1", token)
            if exists:
                skipped += 1
                continue
            stock_code = _pick_stock_code(token, ent_df)
            await conn.execute(
                """
                insert into dim_enterprise (standard_name, enterprise_id, stock_code, stock_name)
                values ($1, $2, $3, $4)
                """,
                token,
                str(next_id),
                stock_code,
                token,
            )
            inserted += 1
            next_id += 1

        # report
        rows = await conn.fetch(
            """
            select stock_name, enterprise_id, stock_code
            from dim_enterprise
            where stock_name = any($1::text[])
            order by stock_name
            """,
            TARGET_NOT_FOUND,
        )
        print("## backfill_enterprise_mapping")
        print("inserted=", inserted, "skipped_existing=", skipped)
        for r in rows:
            print(r["stock_name"], r["enterprise_id"], r["stock_code"] or "")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())

