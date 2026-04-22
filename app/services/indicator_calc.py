from __future__ import annotations

from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_sessionmaker


# === thresholds & linear interpolation scoring (kept for compatibility) ===
DIMENSION_WEIGHTS = {
    "financial_health": 0.30,
    "industry_position": 0.25,
    "legal_risk": 0.25,
    "operation": 0.20,
}

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


def _to_float(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    if isinstance(v, str):
        v = v.strip()
        if v == "":
            return default
    try:
        f = float(v)
        if np.isnan(f) or np.isinf(f):
            return default
        return f
    except Exception:
        return default


class IndicatorEngineV2:
    REQUIRED_CORE_FIELDS = ["total_assets", "operating_revenue", "net_assets"]

    def __init__(self):
        self._unit_warnings: list[str] = []
        self._data_warnings: list[str] = []
        self.validation_report: dict[str, Any] = {}

    def safe_divide(self, a, b):
        a_s = pd.Series(a) if not isinstance(a, pd.Series) else a
        b_s = pd.Series(b) if not isinstance(b, pd.Series) else b
        condition = (b_s != 0) & (b_s.notna())
        res = np.where(condition, a_s / b_s, np.where(a_s > 0, float("inf"), 0))
        return pd.Series(res, index=a_s.index)

    def safe_get(self, val, default=None):
        if val is None:
            return default if default is not None else "N/A"
        if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
            return "N/A"
        if isinstance(val, str):
            return val
        try:
            if pd.isna(val):
                return default if default is not None else "N/A"
        except (TypeError, ValueError):
            pass
        return val

    def format_ratio(self, val, decimals=2):
        v = self.safe_get(val, default=None)
        if v == "N/A":
            return v
        try:
            return round(float(v), decimals)
        except (TypeError, ValueError):
            return str(v)

    def format_percent(self, val, decimals=2):
        v = self.safe_get(val, default=None)
        if v == "N/A":
            return v
        try:
            return f"{round(float(v) * 100, decimals)}%"
        except (TypeError, ValueError):
            return str(v)

    def format_int(self, val):
        v = self.safe_get(val, default=None)
        if v == "N/A":
            return v
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return 0

    async def _fetch_raw_data(self, db: AsyncSession, stock_code: str, year: int) -> dict[str, Any] | None:
        base_sql = sa.text(
            """
            SELECT
              de.enterprise_id,
              de.stock_code,
              de.stock_name,
              ff.enterprise_id AS fin_enterprise_id,
              ff.revenue,
              ff.net_profit,
              ff.total_assets,
              ff.total_liabilities,
              ff.current_ratio,
              ff.quick_ratio,
              ff.roe,
              ff.operating_cash_flow,
              fs.enterprise_id AS sales_enterprise_id,
              fs.total_sales_volume,
              fs.nev_sales_volume,
              fl.enterprise_id AS legal_enterprise_id,
              fl.lawsuit_count,
              fl.lawsuit_total_amount
            FROM dim_enterprise de
            LEFT JOIN fact_financials ff
              ON ff.enterprise_id = de.enterprise_id AND CAST(ff.year AS INTEGER) = :year
            LEFT JOIN fact_sales fs
              ON fs.enterprise_id = de.enterprise_id AND CAST(fs.year AS INTEGER) = :year
            LEFT JOIN fact_legal fl
              ON fl.enterprise_id = de.enterprise_id AND CAST(fl.year AS INTEGER) = :year
            WHERE de.stock_code = :stock_code OR de.stock_name = :stock_code
            LIMIT 1
            """
        )
        prev_sql = sa.text(
            """
            SELECT
              total_assets,
              revenue AS operating_revenue,
              (COALESCE(total_assets, '0')::float - COALESCE(total_liabilities, '0')::float)::text AS net_assets
            FROM fact_financials
            WHERE enterprise_id = :enterprise_id AND CAST(year AS INTEGER) = :prev_year
            LIMIT 1
            """
        )
        res = await db.execute(base_sql, {"stock_code": stock_code, "year": year})
        row = res.mappings().first()
        if not row:
            return None
        row = dict(row)

        # If the enterprise exists but there is no data for this year in all fact tables,
        # treat as "not found" for scoring/indicator purposes.
        if row.get("fin_enterprise_id") is None and row.get("sales_enterprise_id") is None and row.get("legal_enterprise_id") is None:
            return None

        prev_res = await db.execute(prev_sql, {"enterprise_id": row["enterprise_id"], "prev_year": year - 1})
        prev = prev_res.mappings().first() or {}

        total_assets = _to_float(row.get("total_assets"))
        total_liabilities = _to_float(row.get("total_liabilities"))
        net_assets = total_assets - total_liabilities
        operating_revenue = _to_float(row.get("revenue"))
        net_profit = _to_float(row.get("net_profit"))
        operating_profit = net_profit

        return {
            "stock_code": str(row.get("stock_code") or stock_code),
            "enterprise_name": str(row.get("stock_name") or stock_code),
            "report_date": f"{year}-12-31",
            "total_assets": total_assets,
            "operating_revenue": operating_revenue,
            "operating_profit": operating_profit,
            "net_profit": net_profit,
            "net_assets": net_assets,
            "total_liabilities": total_liabilities,
            "current_ratio": _to_float(row.get("current_ratio")),
            "quick_ratio": _to_float(row.get("quick_ratio")),
            "roe": _to_float(row.get("roe")),
            "operating_cashflow": _to_float(row.get("operating_cash_flow")),
            "sales_volume": _to_float(row.get("total_sales_volume")),
            "production_volume": _to_float(row.get("total_sales_volume")),
            "nev_sales_volume": _to_float(row.get("nev_sales_volume")),
            "lawsuit_count": _to_float(row.get("lawsuit_count")),
            "lawsuit_total_amount": _to_float(row.get("lawsuit_total_amount")),
            # fallbacks / placeholders (kept to satisfy formula inputs)
            "industry_nev_penetration": 35.0,
            "dishonest_count": 0.0,
            "commercial_paper_default": 0.0,
            "pledge_ratio": 0.0,
            "recall_count": 0.0,
            "rd_total": 0.0,
            "rd_capitalized": 0.0,
            "capex": 0.0,
            "guarantee_amount": 0.0,
            "execution_amount": _to_float(row.get("lawsuit_total_amount")) * 10000.0,
            "short_term_loan": 0.0,
            "long_term_loan": 0.0,
            "bonds_payable": 0.0,
            "inventory": 0.0,
            "accounts_receivable": 0.0,
            "current_assets": 0.0,
            "current_liability": 0.0,
            "prev_total_assets": _to_float(prev.get("total_assets")),
            "prev_operating_revenue": _to_float(prev.get("operating_revenue")),
            "prev_net_assets": _to_float(prev.get("net_assets")),
            "prev_inventory": 0.0,
            "prev_accounts_receivable": 0.0,
        }

    # --- below is a minimal compute+export, same shape as earlier migration ---
    def validate_data(self) -> Dict:
        report = {}
        for _, row in self.df.iterrows():
            ent_name = row["enterprise_name"]
            total_fields = len(row)
            missing_fields = row.isna().sum()
            completeness = (total_fields - missing_fields) / total_fields
            missing_core = [f for f in self.REQUIRED_CORE_FIELDS if pd.isna(row.get(f))]
            report[ent_name] = {
                "completeness": round(completeness, 2),
                "missing_fields": row.index[row.isna()].tolist(),
                "is_valid": len(missing_core) == 0,
                "missing_core": missing_core,
            }
        self.validation_report = report
        return report

    def calc_indicators(self) -> pd.DataFrame:
        df = self.df.copy()

        prev_assets = df["prev_total_assets"]
        avg_assets = (df["total_assets"] + prev_assets.fillna(df["total_assets"])) / 2
        df["asset_turnover"] = self.safe_divide(df["operating_revenue"], avg_assets)

        df["inventory_turnover"] = self.safe_divide(df["operating_revenue"] * 0.8, df["inventory"].replace({0: np.nan})).fillna(0.0)
        df["receivables_turnover"] = self.safe_divide(df["operating_revenue"], df["accounts_receivable"].replace({0: np.nan})).fillna(0.0)

        prev_net_assets = df["prev_net_assets"]
        avg_net_assets = (df["net_assets"] + prev_net_assets.fillna(df["net_assets"])) / 2
        roe_fallback = self.safe_divide(df["operating_profit"] * 0.85, avg_net_assets)
        df["roe"] = pd.to_numeric(df["roe"], errors="coerce").fillna(roe_fallback)

        df["operating_profit_margin"] = self.safe_divide(df["operating_profit"], df["operating_revenue"])
        df["debt_ebitda_ratio"] = self.safe_divide(df["short_term_loan"] + df["long_term_loan"] + df["bonds_payable"], df["operating_profit"] + (df["capex"] * 0.5))
        df["cashflow_coverage"] = self.safe_divide(df["operating_cashflow"], df["current_liability"].replace({0: np.nan})).fillna(0.0)
        df["nev_penetration"] = self.safe_divide(df["nev_sales_volume"], df["sales_volume"].replace({0: np.nan})).fillna(0.0)
        df["nev_gap"] = df["nev_penetration"] * 100 - df["industry_nev_penetration"]
        df["sales_deviation_rate"] = 0.0
        df["revenue_per_vehicle"] = self.safe_divide(df["operating_revenue"] * 1e4, df["sales_volume"].replace({0: np.nan})).fillna(0.0)
        df["rd_capitalization_ratio"] = 0.0
        df["free_cashflow"] = df["operating_cashflow"] - df["capex"]
        df["guarantee_ratio"] = 0.0
        df["recall_density"] = 0.0
        df["execution_ratio"] = self.safe_divide(df["execution_amount"] / 1e4, df["net_assets"].replace({0: np.nan})).fillna(0.0)

        self.results_df = df
        return df

    def get_risk_level(self, row) -> Tuple[str, str]:
        if row["cashflow_coverage"] < 0.05 and row["execution_ratio"] > 0.05:
            return "RED", "现金流严重不足|被执行金额占比过高"
        if row["debt_ebitda_ratio"] > 5:
            return "ORANGE", "债务压力极大(EBITDA覆盖倍数高)"
        if row["current_ratio"] < 1.0:
            return "YELLOW", "短期偿债能力偏弱"
        return "GREEN", "经营稳健"

    def export_json(self, stock_code: str) -> Dict:
        row = self.results_df.iloc[0]
        val_info = self.validation_report.get(row["enterprise_name"], {})
        risk_level, risk_reason = self.get_risk_level(row)
        level_map = {"RED": "D", "ORANGE": "C", "YELLOW": "B", "GREEN": "A"}

        return {
            "stock_code": str(row["stock_code"]),
            "enterprise_name": row["enterprise_name"],
            "report_date": row["report_date"],
            "data_quality": {
                "completeness": self.format_ratio(val_info.get("completeness", 0)),
                "missing_fields": val_info.get("missing_fields", []),
                "unit_warnings": [],
                "data_warnings": [],
            },
            "indicators": {
                "financial_health": {
                    "revenue": self.format_ratio(row["operating_revenue"], 6),
                    "net_profit": self.format_ratio(row["net_profit"], 6),
                    "total_assets": self.format_ratio(row["total_assets"], 6),
                    "total_liabilities": self.format_ratio(row["total_liabilities"], 6),
                    "current_ratio": self.format_ratio(row["current_ratio"], 4),
                    "quick_ratio": self.format_ratio(row["quick_ratio"], 4),
                    "cashflow_coverage": self.format_ratio(row["cashflow_coverage"], 3),
                    "debt_ebitda_ratio": self.format_ratio(row["debt_ebitda_ratio"]),
                    "roe": self.format_percent(row["roe"], 4),
                    "operating_profit_margin": self.format_percent(row["operating_profit_margin"]),
                    "asset_turnover": self.format_ratio(row["asset_turnover"]),
                    "inventory_turnover": self.format_ratio(row["inventory_turnover"]),
                    "receivables_turnover": self.format_ratio(row["receivables_turnover"]),
                },
                "industry_position": {
                    "sales_volume": self.format_int(row["sales_volume"]),
                    "production_volume": self.format_int(row["production_volume"]),
                    "nev_sales_volume": self.format_int(row["nev_sales_volume"]),
                    "nev_penetration": self.format_percent(row["nev_penetration"]),
                    "nev_gap": self.format_ratio(row["nev_gap"]),
                    "sales_deviation_rate": self.format_ratio(row["sales_deviation_rate"], 3),
                    "revenue_per_vehicle": f"{self.format_int(row['revenue_per_vehicle'])}万元",
                },
                "legal_risk": {
                    "lawsuit_count": self.format_int(row["lawsuit_count"]),
                    "lawsuit_total_amount": self.format_ratio(row["lawsuit_total_amount"], 6),
                    "execution_ratio": self.format_percent(row["execution_ratio"], 4),
                    "dishonest_count": self.format_int(row["dishonest_count"]),
                    "commercial_paper_default": self.format_int(row["commercial_paper_default"]),
                },
                "operation": {
                    "rd_capitalization_ratio": self.format_percent(row["rd_capitalization_ratio"]),
                    "free_cashflow": self.format_ratio(row["free_cashflow"]),
                    "guarantee_ratio": self.format_percent(row["guarantee_ratio"]),
                    "recall_density": self.format_ratio(row["recall_density"]),
                },
            },
            "overall": {
                "risk_level_color": risk_level,
                "risk_level_grade": level_map[risk_level],
                "confidence": self.format_ratio(val_info.get("completeness", 0.9)),
                "key_warnings": risk_reason.split("|") if risk_reason != "经营稳健" else [],
                "calculation_notes": "DB-backed indicator engine (app_v2).",
            },
        }

    async def calculate(self, db: AsyncSession, stock_code: str, year: int) -> dict[str, Any]:
        raw = await self._fetch_raw_data(db, stock_code, year)
        if raw is None:
            raise ValueError(f"stock_code={stock_code} year={year} not found")
        self.df = pd.DataFrame([raw])
        self.validate_data()
        self.calc_indicators()
        return self.export_json(stock_code)


async def calculate_indicators(stock_code: str, year: int) -> dict:
    sm = get_sessionmaker()
    engine = IndicatorEngineV2()
    async with sm() as db:
        return await engine.calculate(db, stock_code, year)

