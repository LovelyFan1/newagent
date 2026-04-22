"""
企业综合评分服务 - 基于31指标的ABCD评级
通过 app.services.indicator_calc.calculate_indicators 获取指标数据
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from app.services.indicator_calc import calculate_indicators


# ===== 指标权重配置 =====
DIMENSION_WEIGHTS = {
    "financial_health": 0.30,  # 财务健康度
    "industry_position": 0.25,  # 行业地位
    "legal_risk": 0.25,  # 法律风险
    "operation": 0.20,  # 运营能力
}

# 各维度指标权重
INDICATOR_WEIGHTS = {
    "financial_health": {
        "current_ratio": 0.15,
        "quick_ratio": 0.10,
        "cashflow_coverage": 0.15,
        "debt_ebitda_ratio": 0.10,
        "roe": 0.15,
        "operating_profit_margin": 0.10,
        "asset_turnover": 0.08,
        "inventory_turnover": 0.07,
        "receivables_turnover": 0.10,
    },
    "industry_position": {
        "nev_penetration": 0.30,
        "nev_gap": 0.25,
        "sales_deviation_rate": 0.20,
        "revenue_per_vehicle": 0.25,
    },
    "legal_risk": {
        "execution_ratio": 0.35,
        "dishonest_count": 0.35,
        "commercial_paper_default": 0.30,
    },
    "operation": {
        "rd_capitalization_ratio": 0.20,
        "free_cashflow": 0.25,
        "guarantee_ratio": 0.25,
        "recall_density": 0.30,
    },
}

# 指标阈值（用于计算单指标得分 0-100）
INDICATOR_THRESHOLDS = {
    "current_ratio": {"best": (1.5, 100), "good": (1.0, 80), "ok": (0.8, 60), "bad": (0.0, 30)},
    "quick_ratio": {"best": (1.0, 100), "good": (0.8, 80), "ok": (0.5, 60), "bad": (0.0, 30)},
    "cashflow_coverage": {"best": (0.2, 100), "good": (0.1, 80), "ok": (0.05, 60), "bad": (0.0, 30)},
    "debt_ebitda_ratio": {"best": (2.0, 100), "good": (4.0, 80), "ok": (6.0, 60), "bad": (999.0, 30)},
    "roe": {"best": (0.15, 100), "good": (0.10, 80), "ok": (0.05, 60), "bad": (-999.0, 30)},
    "operating_profit_margin": {"best": (0.10, 100), "good": (0.05, 80), "ok": (0.0, 60), "bad": (-999.0, 20)},
    "asset_turnover": {"best": (1.0, 100), "good": (0.7, 80), "ok": (0.4, 60), "bad": (0.0, 30)},
    "inventory_turnover": {"best": (8.0, 100), "good": (5.0, 80), "ok": (3.0, 60), "bad": (0.0, 30)},
    "receivables_turnover": {"best": (12.0, 100), "good": (8.0, 80), "ok": (4.0, 60), "bad": (0.0, 30)},
    "nev_penetration": {"best": (0.80, 100), "good": (0.50, 80), "ok": (0.30, 60), "bad": (0.0, 30)},
    "nev_gap": {"best": (20.0, 100), "good": (0.0, 80), "ok": (-10.0, 60), "bad": (-999.0, 30)},
    "sales_deviation_rate": {"best": (0.10, 100), "good": (0.20, 80), "ok": (0.30, 60), "bad": (999.0, 30)},
    "revenue_per_vehicle": {"best": (50.0, 100), "good": (20.0, 80), "ok": (10.0, 60), "bad": (0.0, 30)},
    "execution_ratio": {"best": (0.0, 100), "good": (0.01, 80), "ok": (0.05, 60), "bad": (999.0, 30)},
    "dishonest_count": {"best": (0.0, 100), "good": (0.0, 80), "ok": (1.0, 60), "bad": (999.0, 30)},
    "commercial_paper_default": {"best": (0.0, 100), "good": (0.0, 80), "ok": (0.0, 60), "bad": (999.0, 30)},
    "rd_capitalization_ratio": {"best": (0.3, 100), "good": (0.5, 80), "ok": (0.7, 60), "bad": (999.0, 30)},
    "free_cashflow": {"best": (1.0, 100), "good": (0.0, 80), "ok": (0.0, 60), "bad": (-999.0, 30)},
    "guarantee_ratio": {"best": (0.3, 100), "good": (0.5, 80), "ok": (0.7, 60), "bad": (999.0, 30)},
    "recall_density": {"best": (1.0, 100), "good": (5.0, 80), "ok": (10.0, 60), "bad": (999.0, 30)},
}


def parse_percent(val) -> float:
    if val is None or val == "N/A":
        return 0.0
    if isinstance(val, str):
        val = val.replace("%", "").strip()
        try:
            return float(val) / 100
        except ValueError:
            return 0.0
    return float(val)


def parse_value(val) -> float:
    if val is None or val == "N/A":
        return 0.0
    if isinstance(val, str):
        val = val.replace("万元", "").replace("元", "").replace(",", "").strip()
        try:
            return float(val)
        except ValueError:
            return 0.0
    return float(val)


def score_indicator(key: str, raw_val) -> float:
    if raw_val is None or raw_val == "N/A":
        return 50.0

    threshold = INDICATOR_THRESHOLDS.get(key)
    if threshold is None:
        return 50.0

    try:
        val = float(raw_val)
    except (ValueError, TypeError):
        return 50.0

    t = threshold
    if val >= t["best"][0]:
        return t["best"][1]
    elif val >= t["good"][0]:
        ratio = (val - t["good"][0]) / (t["best"][0] - t["good"][0])
        return t["good"][1] + ratio * (t["best"][1] - t["good"][1])
    elif val >= t["ok"][0]:
        ratio = (val - t["ok"][0]) / (t["good"][0] - t["ok"][0])
        return t["ok"][1] + ratio * (t["good"][1] - t["ok"][1])
    else:
        return t["bad"][1]


class ScoringService:
    async def calculate_score(self, stock_code: str, year: int) -> Optional[Dict[str, Any]]:
        try:
            ent_data = await calculate_indicators(stock_code, year)
        except ValueError:
            return None
        indicators = ent_data["indicators"]

        dimension_scores: dict[str, float] = {}
        indicator_scores: dict[str, float] = {}

        for dim_key, _dim_weight in DIMENSION_WEIGHTS.items():
            dim_data = indicators.get(dim_key, {})
            dim_score = 0.0
            total_w = 0.0

            for ind_key, ind_weight in INDICATOR_WEIGHTS.get(dim_key, {}).items():
                raw = dim_data.get(ind_key, "N/A")

                if ind_key in (
                    "roe",
                    "operating_profit_margin",
                    "nev_penetration",
                    "execution_ratio",
                    "rd_capitalization_ratio",
                    "guarantee_ratio",
                ):
                    clean_val = parse_percent(raw)
                else:
                    clean_val = parse_value(raw)

                ind_score = score_indicator(ind_key, clean_val)
                indicator_scores[f"{dim_key}.{ind_key}"] = round(ind_score, 1)
                dim_score += ind_score * ind_weight
                total_w += ind_weight

            dimension_scores[dim_key] = round(dim_score / total_w, 1) if total_w > 0 else 50.0

        total_score = sum(dimension_scores[d] * w for d, w in DIMENSION_WEIGHTS.items())

        if total_score >= 80:
            rating, desc = "A", "经营优秀"
        elif total_score >= 65:
            rating, desc = "B", "经营良好"
        elif total_score >= 50:
            rating, desc = "C", "经营一般"
        else:
            rating, desc = "D", "经营风险"

        return {
            "stock_code": ent_data["stock_code"],
            "stock_name": ent_data["enterprise_name"],
            "year": year,
            "total_score": round(total_score, 1),
            "rating": rating,
            "rating_desc": desc,
            "dimension_scores": dimension_scores,
            "indicator_scores": indicator_scores,
        }


scoring_service = ScoringService()

