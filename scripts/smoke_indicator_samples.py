from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import sqlalchemy as sa

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.db import get_sessionmaker
from services.indicator_calc import calculate_indicators


async def main() -> None:
    sm = get_sessionmaker()
    async with sm() as db:
        rows = (
            await db.execute(
                sa.text(
                    """
                    SELECT de.stock_code, de.stock_name, CAST(ff.year AS INTEGER) AS year
                    FROM dim_enterprise de
                    JOIN fact_financials ff ON ff.enterprise_id = de.enterprise_id
                    WHERE de.stock_name IS NOT NULL
                    LIMIT 3
                    """
                )
            )
        ).mappings().all()

    print("samples:", [dict(r) for r in rows])
    ok = 0
    for r in rows:
        code_or_name = str(r["stock_code"] or r["stock_name"])
        res = await calculate_indicators(code_or_name, int(r["year"]))
        print(
            "sample_ok:",
            code_or_name,
            r["year"],
            res["overall"]["risk_level_color"],
            res["scores"]["total_score"],
        )
        ok += 1
    print("ok_count:", ok)


if __name__ == "__main__":
    asyncio.run(main())

