from __future__ import annotations

import pytest


@pytest.mark.anyio
async def test_scoring_service_calculate_score_monkeypatched(monkeypatch):
    from app.services import scoring_service as scoring_mod

    async def fake_calculate_indicators(stock_code: str, year: int):
        return {
            "stock_code": stock_code,
            "enterprise_name": "TestCo",
            "report_date": f"{year}-12-31",
            "indicators": {
                "financial_health": {
                    "current_ratio": 1.5,
                    "quick_ratio": 1.0,
                    "cashflow_coverage": 0.2,
                    "debt_ebitda_ratio": 2.0,
                    "roe": "15%",
                    "operating_profit_margin": "10%",
                    "asset_turnover": 1.0,
                    "inventory_turnover": 8.0,
                    "receivables_turnover": 12.0,
                },
                "industry_position": {
                    "nev_penetration": "80%",
                    "nev_gap": 20.0,
                    "sales_deviation_rate": 0.1,
                    "revenue_per_vehicle": "50万元",
                },
                "legal_risk": {"execution_ratio": "0%", "dishonest_count": 0, "commercial_paper_default": 0},
                "operation": {
                    "rd_capitalization_ratio": "30%",
                    "free_cashflow": 1.0,
                    "guarantee_ratio": "30%",
                    "recall_density": 1.0,
                },
            },
            "overall": {"risk_level_color": "GREEN", "key_warnings": []},
            "data_quality": {"completeness": 1.0},
        }

    monkeypatch.setattr(scoring_mod, "calculate_indicators", fake_calculate_indicators)

    svc = scoring_mod.ScoringService()
    res = await svc.calculate_score("002594", 2022)
    assert res is not None
    assert res["stock_code"] == "002594"
    assert res["stock_name"] == "TestCo"
    assert res["year"] == 2022
    assert 0 <= res["total_score"] <= 100
    assert res["rating"] in {"A", "B", "C", "D"}
    assert set(res["dimension_scores"].keys()) == {
        "financial_health",
        "industry_position",
        "legal_risk",
        "operation",
    }

