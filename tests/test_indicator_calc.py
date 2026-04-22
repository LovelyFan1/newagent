from __future__ import annotations

import sys
from pathlib import Path

import pytest
import sqlalchemy as sa

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.db import get_sessionmaker
from services.indicator_calc import calculate_indicators


@pytest.mark.asyncio
async def test_calculate_indicators_returns_expected_structure():
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as db:
        row = (
            await db.execute(
                sa.text(
                    """
                    SELECT de.stock_code, de.stock_name, CAST(ff.year AS INTEGER) AS year
                    FROM dim_enterprise de
                    JOIN fact_financials ff ON ff.enterprise_id = de.enterprise_id
                    LIMIT 1
                    """
                )
            )
        ).mappings().first()

    if not row:
        pytest.skip("No enterprise financial data found")

    code = row["stock_code"] or row["stock_name"]
    year = int(row["year"])
    result = await calculate_indicators(str(code), year)

    assert "indicators" in result
    assert "financial_health" in result["indicators"]
    assert "industry_position" in result["indicators"]
    assert "legal_risk" in result["indicators"]
    assert "operation" in result["indicators"]
    assert "overall" in result
    assert "scores" in result
    assert "total_score" in result["scores"]

