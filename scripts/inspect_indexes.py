from __future__ import annotations

import asyncio

import asyncpg


async def main() -> None:
    conn = await asyncpg.connect("postgresql://app_v2:app_v2@127.0.0.1:5432/app_v2")
    try:
        for t in ["dim_enterprise", "fact_financials", "fact_sales", "fact_legal"]:
            rows = await conn.fetch(
                "select indexname, indexdef from pg_indexes where schemaname='public' and tablename=$1",
                t,
            )
            uniques = [r["indexdef"] for r in rows if "unique" in str(r["indexdef"]).lower()]
            print(f"\n## {t}")
            if uniques:
                for u in uniques:
                    print(u)
            else:
                print("(no unique indexes found)")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())

