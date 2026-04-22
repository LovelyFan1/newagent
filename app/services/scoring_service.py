"""
企业综合评分服务 - 基于31指标的ABCD评级
通过 app.services.indicator_calc.calculate_indicators 获取指标数据
"""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
from typing import Any, Dict, Optional

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.scoring import ScoringResult
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

DIMENSION_INDICATORS = {
    "financial_health": [
        "current_ratio",
        "quick_ratio",
        "cashflow_coverage",
        "debt_ebitda_ratio",
        "roe",
        "operating_profit_margin",
        "asset_turnover",
        "inventory_turnover",
        "receivables_turnover",
    ],
    "industry_position": ["nev_penetration", "nev_gap", "sales_deviation_rate", "revenue_per_vehicle"],
    "legal_risk": ["execution_ratio", "dishonest_count", "commercial_paper_default"],
    "operation": ["rd_capitalization_ratio", "free_cashflow", "guarantee_ratio", "recall_density"],
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
    async def get_raw_data(self, stock_code: str, year: int) -> Optional[Dict[str, Any]]:
        try:
            return await calculate_indicators(stock_code, year)
        except ValueError:
            return None

    def calculate_score_from_raw_data(self, *, raw_data: Dict[str, Any], year: int) -> Dict[str, Any]:
        indicators = raw_data["indicators"]
        # Prefer completeness/weights from indicator engine if present; otherwise compute here.
        scores_meta = raw_data.get("scores") if isinstance(raw_data.get("scores"), dict) else {}
        completeness = scores_meta.get("completeness") if isinstance(scores_meta.get("completeness"), dict) else None
        effective_weights = scores_meta.get("effective_weights") if isinstance(scores_meta.get("effective_weights"), dict) else None

        if completeness is None:
            completeness = {}
            for dim, inds in DIMENSION_INDICATORS.items():
                if not inds:
                    completeness[dim] = 0.0
                    continue
                dim_data = indicators.get(dim, {}) if isinstance(indicators.get(dim), dict) else {}
                filled = 0
                for ind in inds:
                    v = dim_data.get(ind)
                    if v is None:
                        continue
                    if isinstance(v, str) and v.strip().upper() == "N/A":
                        continue
                    filled += 1
                completeness[dim] = filled / len(inds)

        if effective_weights is None:
            base_weights = dict(DIMENSION_WEIGHTS)
            valid_dims = {k for k, v in completeness.items() if float(v or 0.0) > 0.3}
            if valid_dims:
                total_w = sum(base_weights.get(k, 0.0) for k in valid_dims) or 0.0
                if total_w > 0:
                    effective_weights = {
                        k: (round(base_weights[k] / total_w, 4) if k in valid_dims else 0.0) for k in base_weights
                    }
                else:
                    effective_weights = base_weights
            else:
                effective_weights = base_weights

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

        total_score = sum(float(dimension_scores.get(d, 50.0)) * float(effective_weights.get(d, 0.0)) for d in DIMENSION_WEIGHTS)

        if total_score >= 80:
            rating, desc = "A", "经营优秀"
        elif total_score >= 65:
            rating, desc = "B", "经营良好"
        elif total_score >= 50:
            rating, desc = "C", "经营一般"
        else:
            rating, desc = "D", "经营风险"

        dimension_scores_detail = {
            dim: {
                "score": round(float(score), 2),
                "weight": float(effective_weights.get(dim, 0.0) or 0.0),
                "completeness": float(completeness.get(dim, 0.0) or 0.0),
                "warning": None
                if float(completeness.get(dim, 0.0) or 0.0) > 0.3
                else "数据完整度不足，分数参考价值有限",
            }
            for dim, score in dimension_scores.items()
        }

        confidence = (
            round(sum(float(v or 0.0) for v in completeness.values()) / len(completeness), 2) if completeness else 0.0
        )

        return {
            "stock_code": raw_data.get("stock_code"),
            "year": year,
            "total_score": round(float(total_score), 2),
            "rating": rating,
            "dimension_scores": dimension_scores_detail,
            "effective_weights": effective_weights,
            "confidence": confidence,
            "updated_at": datetime.now().isoformat(),
            # Keep extra info for debugging/compat if callers still need it
            "rating_desc": desc,
            "indicator_scores": indicator_scores,
        }

    async def calculate(self, db: AsyncSession, stock_code: str, year: int, force: bool = False) -> Optional[Dict[str, Any]]:
        raw_data = await self.get_raw_data(stock_code, year)
        if raw_data is None:
            return None

        raw_data_str = json.dumps(raw_data, sort_keys=True, default=str)
        data_hash = hashlib.md5(raw_data_str.encode()).hexdigest()

        existing = (
            await db.execute(select(ScoringResult).where(ScoringResult.stock_code == stock_code, ScoringResult.year == year))
        ).scalar_one_or_none()

        if (not force) and existing is not None and existing.data_hash == data_hash:
            # Return a fresh response (includes confidence/effective_weights) but keep cache as source of truth.
            return self.calculate_score_from_raw_data(raw_data=raw_data, year=year)

        result = self.calculate_score_from_raw_data(raw_data=raw_data, year=year)

        if existing is not None:
            existing.stock_code = stock_code
            existing.stock_name = str(raw_data.get("enterprise_name") or stock_code)
            existing.year = year
            existing.dimension_scores = result["dimension_scores"]
            existing.total_score = float(result["total_score"])
            existing.rating = str(result["rating"])
            existing.data_hash = data_hash
            try:
                await db.commit()
            except IntegrityError:
                await db.rollback()
        else:
            record = ScoringResult(
                stock_code=stock_code,
                stock_name=str(raw_data.get("enterprise_name") or stock_code),
                year=year,
                dimension_scores=result["dimension_scores"],
                total_score=float(result["total_score"]),
                rating=str(result["rating"]),
                data_hash=data_hash,
            )
            db.add(record)
            try:
                await db.commit()
            except IntegrityError:
                # concurrent requests may insert same (stock_code, year)
                await db.rollback()
        return result

    async def calculate_score(self, stock_code: str, year: int) -> Optional[Dict[str, Any]]:
        raw_data = await self.get_raw_data(stock_code, year)
        if raw_data is None:
            return None
        return self.calculate_score_from_raw_data(raw_data=raw_data, year=year)


scoring_service = ScoringService()

