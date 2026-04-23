from __future__ import annotations

from typing import Dict, Tuple, Any

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from core.db import get_sessionmaker


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
    # data missing -> neutral score, skip threshold decision
    if raw_val is None:
        return 50.0
    if raw_val == "N/A":
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


def _to_float(v: Any, default: float | None = None) -> float | None:
    """
    Convert to float.
    - Missing/empty/NaN/Inf -> None (or provided default).
    """
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

    def safe_divide(self, numerator, denominator, default=None):
        """
        Safe divide that distinguishes real 0 from "missing/uncomputable".
        - Scalar: if denominator is 0/NaN -> return default (if not None) else None
        - Series/array: returns pd.Series with NaN for uncomputable (or filled by default if provided)
        """
        # scalar path
        if not isinstance(numerator, (pd.Series, list, tuple, np.ndarray)) and not isinstance(
            denominator, (pd.Series, list, tuple, np.ndarray)
        ):
            try:
                if denominator == 0 or pd.isna(denominator):
                    return default if default is not None else None
                if pd.isna(numerator):
                    return default if default is not None else None
                return float(numerator) / float(denominator)
            except Exception:
                return default if default is not None else None

        # vector path (keep numeric dtype; use NaN to represent missing)
        a_s = pd.Series(numerator) if not isinstance(numerator, pd.Series) else numerator
        b_s = pd.Series(denominator) if not isinstance(denominator, pd.Series) else denominator
        condition = (b_s != 0) & (b_s.notna()) & (a_s.notna())
        res = pd.Series(np.where(condition, a_s / b_s, np.nan), index=a_s.index)
        if default is not None:
            res = res.fillna(default)
        return res

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

    def auto_detect_execution_unit(self, execution_amount_series, unit_hint: str = "auto"):
        s = execution_amount_series.copy()
        max_val = s.max()
        if unit_hint != "auto":
            if unit_hint == "yuan":
                return s / 1e8
            elif unit_hint == "wan_yuan":
                return s / 1e4
            else:
                return s

        result = s / 1e4
        self._unit_warnings.append(f"execution_amount: 初始假设为万元单位，max={max_val:.0f}")
        if "net_assets" in self.df.columns:
            net_assets = self.df["net_assets"].values
            ratio_check = result / pd.Series(net_assets, index=result.index)
            mask_unreasonable = ratio_check > 0.05
            if mask_unreasonable.any():
                ent_names = self.df.loc[mask_unreasonable, "enterprise_name"].tolist()
                unreasonable_ratios = ratio_check.loc[mask_unreasonable].tolist()
                self._unit_warnings.append(
                    f"execution_amount: {ent_names}被执行净资产比异常高 "
                    f"({[f'{v*100:.2f}%' for v in unreasonable_ratios]})，原始数据可能为元单位(非万元)，已自动校正"
                )
                result.loc[mask_unreasonable] = result.loc[mask_unreasonable] / 1e4

            mask_suspicious = (ratio_check > 0.01) & (ratio_check <= 0.05)
            if mask_suspicious.any():
                for ent_name, ratio in zip(self.df.loc[mask_suspicious, "enterprise_name"], ratio_check.loc[mask_suspicious]):
                    self._data_warnings.append(
                        f"execution_amount: {ent_name} 被执行净资产比为 {ratio*100:.2f}%，高于行业正常水平(<1%)，请核实数据准确性"
                    )
        return result

    def _validate_nev_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if "nev_sales_volume" not in df.columns or "sales_volume" not in df.columns:
            return df
        mask = df["nev_sales_volume"] > df["sales_volume"]
        if mask.any():
            for ent_name in df.loc[mask, "enterprise_name"]:
                self._data_warnings.append(
                    f"NEV数据异常: {ent_name} 的新能源销量>总销量，可能是数据填反或统计口径不一致，已按总销量处理"
                )
            df.loc[mask, "nev_sales_volume"] = df.loc[mask, "sales_volume"]
        mask_100 = (df["nev_sales_volume"] == df["sales_volume"]) & (df["sales_volume"] < 100000)
        if mask_100.any():
            for ent_name in df.loc[mask_100, "enterprise_name"]:
                self._data_warnings.append(f"NEV数据可疑: {ent_name} 渗透率100%且总销量<10万辆，数据可能不完整，请人工核实")
        return df

    async def _fetch_raw_data(self, db: AsyncSession, stock_code: str, year: int) -> dict[str, Any] | None:
        base_sql = sa.text(
            """
            SELECT
              de.enterprise_id,
              de.stock_code,
              de.stock_name,
              ff.year AS f_year,
              ff.revenue,
              ff.net_profit,
              ff.total_assets,
              ff.total_liabilities,
              ff.current_ratio,
              ff.quick_ratio,
              ff.roe,
              ff.operating_cash_flow,
              fs.total_sales_volume,
              fs.nev_sales_volume,
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
        prev_res = await db.execute(
            prev_sql,
            {"enterprise_id": row["enterprise_id"], "prev_year": year - 1},
        )
        prev = prev_res.mappings().first() or {}

        # NOTE: keep arithmetic-safe defaults for core financials
        total_assets = _to_float(row.get("total_assets"), default=0.0) or 0.0
        total_liabilities = _to_float(row.get("total_liabilities"), default=0.0) or 0.0
        net_assets = total_assets - total_liabilities
        operating_revenue = _to_float(row.get("revenue"), default=0.0) or 0.0
        net_profit = _to_float(row.get("net_profit"), default=0.0) or 0.0
        operating_profit = net_profit

        # smart unit detection for execution_amount
        _execution_raw = row.get("execution_amount")
        if _execution_raw is None:
            # fallback: some datasets only have lawsuit_total_amount
            _execution_raw = row.get("lawsuit_total_amount")
        if _execution_raw is not None:
            try:
                _execution_float = float(_execution_raw)
            except (TypeError, ValueError):
                _execution_float = 0.0
            if _execution_float > 0:
                if _execution_float < 10000:
                    # < 1万: likely 万元, convert to 元
                    execution_amount = _execution_float * 10000
                elif _execution_float > 100_0000_0000:
                    # > 100亿: likely already 元, convert to 万元 for later ratio calc
                    execution_amount = _execution_float / 10000
                else:
                    execution_amount = _execution_float
            else:
                execution_amount = None
        else:
            execution_amount = None

        mapped = {
            "stock_code": stock_code,
            "enterprise_name": row.get("stock_name") or stock_code,
            "report_date": f"{year}-12-31",
            "total_assets": total_assets,
            "operating_revenue": operating_revenue,
            "operating_profit": operating_profit,
            "net_profit": net_profit,
            "net_assets": net_assets,
            "total_liabilities": total_liabilities,
            "current_ratio": _to_float(row.get("current_ratio"), default=None),
            "quick_ratio": _to_float(row.get("quick_ratio"), default=None),
            "roe": _to_float(row.get("roe"), default=None),
            "operating_cashflow": _to_float(row.get("operating_cash_flow"), default=None),
            "sales_volume": _to_float(row.get("total_sales_volume"), default=None),
            "production_volume": _to_float(row.get("total_sales_volume"), default=None),
            "nev_sales_volume": _to_float(row.get("nev_sales_volume"), default=None),
            "lawsuit_count": _to_float(row.get("lawsuit_count"), default=None),
            "lawsuit_total_amount": _to_float(row.get("lawsuit_total_amount"), default=None),
            "industry_nev_penetration": 35.0,
            "dishonest_count": None,
            "commercial_paper_default": None,
            "pledge_ratio": None,
            "recall_count": None,
            "rd_total": None,
            "rd_capitalized": None,
            "capex": None,
            "guarantee_amount": None,
            "execution_amount": execution_amount,
            "short_term_loan": None,
            "long_term_loan": None,
            "bonds_payable": None,
            "inventory": None,
            "accounts_receivable": None,
            "current_assets": None,
            "current_liability": None,
            "prev_total_assets": _to_float(prev.get("total_assets"), default=None),
            "prev_operating_revenue": _to_float(prev.get("operating_revenue"), default=None),
            "prev_net_assets": _to_float(prev.get("net_assets"), default=None),
            "prev_inventory": None,
            "prev_accounts_receivable": None,
        }
        return mapped

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
        self._unit_warnings = []
        self._data_warnings = []
        df = self._validate_nev_data(df)

        def get_prev(col_name):
            return df[col_name] if col_name in df.columns else np.nan

        current_ratio_fallback = self.safe_divide(df["current_assets"], df["current_liability"])
        quick_ratio_fallback = self.safe_divide(df["current_assets"] - df["inventory"], df["current_liability"])
        if "current_ratio" not in df.columns:
            df["current_ratio"] = current_ratio_fallback
        else:
            # keep missing as NaN (do NOT fill with 0); fallback only for missing values
            df["current_ratio"] = pd.to_numeric(df["current_ratio"], errors="coerce").where(df["current_ratio"].notna(), current_ratio_fallback)
        if "quick_ratio" not in df.columns:
            df["quick_ratio"] = quick_ratio_fallback
        else:
            df["quick_ratio"] = pd.to_numeric(df["quick_ratio"], errors="coerce").where(df["quick_ratio"].notna(), quick_ratio_fallback)

        prev_assets = get_prev("prev_total_assets")
        avg_assets = (df["total_assets"] + prev_assets.fillna(df["total_assets"])) / 2
        df["asset_turnover"] = self.safe_divide(df["operating_revenue"], avg_assets)

        prev_inv = get_prev("prev_inventory")
        avg_inv = (df["inventory"] + prev_inv.fillna(df["inventory"])) / 2
        df["inventory_turnover"] = self.safe_divide(df["operating_revenue"] * 0.8, avg_inv)

        prev_ar = get_prev("prev_accounts_receivable")
        avg_ar = (df["accounts_receivable"] + prev_ar.fillna(df["accounts_receivable"])) / 2
        df["receivables_turnover"] = self.safe_divide(df["operating_revenue"], avg_ar)

        prev_net_assets = get_prev("prev_net_assets")
        avg_net_assets = (df["net_assets"] + prev_net_assets.fillna(df["net_assets"])) / 2
        roe_fallback = self.safe_divide(df["operating_profit"] * 0.85, avg_net_assets)
        if "roe" not in df.columns:
            df["roe"] = roe_fallback
        else:
            df["roe"] = pd.to_numeric(df["roe"], errors="coerce").where(df["roe"].notna(), roe_fallback)
        df["operating_profit_margin"] = self.safe_divide(df["operating_profit"], df["operating_revenue"])

        df["asset_growth"] = self.safe_divide(df["total_assets"] - prev_assets, prev_assets)
        prev_revenue = get_prev("prev_operating_revenue")
        df["revenue_growth"] = self.safe_divide(df["operating_revenue"] - prev_revenue, prev_revenue)

        df["interest_bearing_debt"] = df["short_term_loan"] + df["long_term_loan"] + df["bonds_payable"]
        df["ebitda"] = df["operating_profit"] + (df["capex"] * 0.5)
        df["debt_ebitda_ratio"] = self.safe_divide(df["interest_bearing_debt"], df["ebitda"])
        df["cashflow_coverage"] = self.safe_divide(df["operating_cashflow"], df["current_liability"])

        execution_yuan = self.auto_detect_execution_unit(df["execution_amount"])
        df["execution_ratio"] = self.safe_divide(execution_yuan, df["net_assets"])
        df["nev_penetration"] = self.safe_divide(df["nev_sales_volume"], df["sales_volume"])
        df["nev_gap"] = df["nev_penetration"] * 100 - df["industry_nev_penetration"]
        df["sales_deviation_rate"] = self.safe_divide(df["production_volume"] - df["sales_volume"], df["sales_volume"])
        df["revenue_per_vehicle"] = self.safe_divide(df["operating_revenue"] * 1e4, df["sales_volume"])
        df["rd_capitalization_ratio"] = self.safe_divide(df["rd_capitalized"], df["rd_total"])
        df["free_cashflow"] = df["operating_cashflow"] - df["capex"]
        df["guarantee_ratio"] = self.safe_divide(df["guarantee_amount"], df["net_assets"])
        df["recall_density"] = self.safe_divide(df.get("recall_count", 0), df["sales_volume"]) * 10000
        self.results_df = df
        return df

    def get_risk_level(self, row) -> Tuple[str, str]:
        reasons = []

        def is_valid(v):
            try:
                return np.isfinite(v)
            except Exception:
                return False

        def gt0(v) -> bool:
            if v is None:
                return False
            try:
                return float(v) > 0
            except (TypeError, ValueError):
                return False

        def gt_threshold(v, t: float) -> bool:
            if not is_valid(v):
                return False
            try:
                return float(v) > t
            except (TypeError, ValueError):
                return False

        cash_ok = is_valid(row["cashflow_coverage"])
        exec_ok = is_valid(row["execution_ratio"])
        if (cash_ok and row["cashflow_coverage"] < 0.05 and exec_ok and row["execution_ratio"] > 0.05) or gt0(
            row["commercial_paper_default"]
        ) or gt0(row["dishonest_count"]):
            if cash_ok and row["cashflow_coverage"] < 0.05:
                reasons.append("现金流严重不足")
            if exec_ok and row["execution_ratio"] > 0.05:
                reasons.append("被执行金额占比过高")
            if gt0(row["commercial_paper_default"]):
                reasons.append("存在商票逾期")
            if gt0(row["dishonest_count"]):
                reasons.append("列入失信名单")
            return "RED", "|".join(reasons)

        dev_ok = is_valid(row["sales_deviation_rate"])
        debt_ok = is_valid(row["debt_ebitda_ratio"])
        if (dev_ok and row["sales_deviation_rate"] > 0.20) or (debt_ok and row["debt_ebitda_ratio"] > 5) or gt_threshold(
            row["pledge_ratio"], 70.0
        ):
            if dev_ok and row["sales_deviation_rate"] > 0.20:
                reasons.append("产销严重偏差(库存积压)")
            if debt_ok and row["debt_ebitda_ratio"] > 5:
                reasons.append("债务压力极大(EBITDA覆盖倍数高)")
            if gt_threshold(row["pledge_ratio"], 70.0):
                reasons.append("大股东股权质押率过高")
            return "ORANGE", "|".join(reasons)

        gap_ok = is_valid(row["nev_gap"])
        rd_ok = is_valid(row["rd_capitalization_ratio"])
        cr_ok = is_valid(row["current_ratio"])
        if (gap_ok and row["nev_gap"] < -10) or (rd_ok and row["rd_capitalization_ratio"] > 0.5) or (
            cr_ok and row["current_ratio"] < 1.0
        ):
            if gap_ok and row["nev_gap"] < -10:
                reasons.append("新能源转型落后行业")
            if rd_ok and row["rd_capitalization_ratio"] > 0.5:
                reasons.append("研发资本化率异常过高")
            if cr_ok and row["current_ratio"] < 1.0:
                reasons.append("短期偿债能力偏弱")
            return "YELLOW", "|".join(reasons)
        return "GREEN", "经营稳健"

    def export_json(self, stock_code: str) -> Dict:
        row = self.results_df[self.results_df["stock_code"].astype(str) == str(stock_code)].iloc[0]
        val_info = self.validation_report.get(row["enterprise_name"], {})
        risk_level, risk_reason = self.get_risk_level(row)
        level_map = {"RED": "D", "ORANGE": "C", "YELLOW": "B", "GREEN": "A"}
        ent_warnings = [w for w in self._unit_warnings + self._data_warnings if w.split(":")[0] in ["execution_amount", "NEV数据异常", "NEV数据可疑"]]

        revenue = float(row.get("operating_revenue", 0.0) or 0.0)
        net_profit = float(row.get("net_profit", row.get("operating_profit", 0.0) * 0.85) or 0.0)
        total_assets = float(row.get("total_assets", 0.0) or 0.0)
        total_liabilities = float(row.get("total_liabilities", max(total_assets - float(row.get("net_assets", 0.0) or 0.0), 0.0)))
        sales_volume = float(row.get("sales_volume", 0.0) or 0.0)
        production_volume = float(row.get("production_volume", 0.0) or 0.0)
        nev_sales_volume = float(row.get("nev_sales_volume", 0.0) or 0.0)
        lawsuit_count = float(row.get("lawsuit_count", 0.0) or 0.0)
        lawsuit_total_amount = float(row.get("lawsuit_total_amount", 0.0) or 0.0)

        result = {
            "stock_code": str(row["stock_code"]),
            "enterprise_name": row["enterprise_name"],
            "report_date": row["report_date"],
            "data_quality": {
                "completeness": self.format_ratio(val_info.get("completeness", 0)),
                "missing_fields": val_info.get("missing_fields", []),
                "unit_warnings": [w for w in self._unit_warnings],
                "data_warnings": ent_warnings,
            },
            "indicators": {
                "financial_health": {
                    "revenue": self.format_ratio(revenue, 6),
                    "net_profit": self.format_ratio(net_profit, 6),
                    "total_assets": self.format_ratio(total_assets, 6),
                    "total_liabilities": self.format_ratio(total_liabilities, 6),
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
                    "sales_volume": self.format_int(sales_volume),
                    "production_volume": self.format_int(production_volume),
                    "nev_sales_volume": self.format_int(nev_sales_volume),
                    "nev_penetration": self.format_percent(row["nev_penetration"]),
                    "nev_gap": self.format_ratio(row["nev_gap"]),
                    "sales_deviation_rate": self.format_ratio(row["sales_deviation_rate"], 3),
                    "revenue_per_vehicle": f"{self.format_int(row['revenue_per_vehicle'])}万元",
                },
                "legal_risk": {
                    "lawsuit_count": self.format_int(lawsuit_count),
                    "lawsuit_total_amount": self.format_ratio(lawsuit_total_amount, 6),
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
                "calculation_notes": f"基于{int(val_info.get('completeness', 0)*100)}%完整度数据。折旧使用capex×0.5近似，净利润使用营业利润×0.85近似。",
            },
        }
        if "比亚迪" in str(row["enterprise_name"]):
            result["overall"]["calculation_notes"] += " 注意：营收含部分手机业务，单车营收为近似值。"
        return result

    async def calculate(self, db: AsyncSession, stock_code: str, year: int) -> dict[str, Any]:
        mapped = await self._fetch_raw_data(db, stock_code, year)
        if mapped is None:
            raise ValueError(f"stock_code={stock_code} year={year} not found")
        self.df = pd.DataFrame([mapped])
        self.validate_data()
        self.calc_indicators()
        result = self.export_json(stock_code)
        result["scores"] = self._calculate_scores(result["indicators"])
        return result

    def _calculate_scores(self, indicators: dict[str, Any]) -> dict[str, Any]:
        # 1) completeness per dimension (based on indicator values, treat None/"N/A" as missing)
        completeness: dict[str, float] = {}
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

        # 2) effective weights reallocation
        base_weights = dict(DIMENSION_WEIGHTS)
        valid_dims = {k for k, v in completeness.items() if v > 0.3}
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

        dimension_scores = {}
        indicator_scores = {}
        for dim_key, _ in DIMENSION_WEIGHTS.items():
            dim_data = indicators.get(dim_key, {})
            dim_score = 0.0
            total_w = 0.0
            for ind_key, ind_weight in INDICATOR_WEIGHTS.get(dim_key, {}).items():
                raw = dim_data.get(ind_key, "N/A")
                if ind_key in ("roe", "operating_profit_margin", "nev_penetration", "execution_ratio", "rd_capitalization_ratio", "guarantee_ratio"):
                    clean_val = parse_percent(raw)
                else:
                    clean_val = parse_value(raw)
                ind_score = score_indicator(ind_key, clean_val)
                indicator_scores[f"{dim_key}.{ind_key}"] = round(ind_score, 1)
                dim_score += ind_score * ind_weight
                total_w += ind_weight
            dimension_scores[dim_key] = round(dim_score / total_w, 1) if total_w > 0 else 50.0
        total_score = sum(dimension_scores.get(d, 50.0) * effective_weights.get(d, 0.0) for d in DIMENSION_WEIGHTS)
        if total_score >= 80:
            grade, desc = "A", "经营优秀"
        elif total_score >= 65:
            grade, desc = "B", "经营良好"
        elif total_score >= 50:
            grade, desc = "C", "经营一般"
        else:
            grade, desc = "D", "经营风险"
        return {
            "total_score": round(total_score, 1),
            "grade": grade,
            "grade_desc": desc,
            "dimension_scores": dimension_scores,
            "indicator_scores": indicator_scores,
            "completeness": {k: round(v, 4) for k, v in completeness.items()},
            "effective_weights": effective_weights,
        }


async def calculate_indicators(stock_code: str, year: int) -> dict:
    sessionmaker = get_sessionmaker()
    engine = IndicatorEngineV2()
    async with sessionmaker() as db:
        return await engine.calculate(db, stock_code, year)

