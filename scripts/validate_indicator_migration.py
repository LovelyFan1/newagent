from __future__ import annotations

import asyncio
import importlib.util
import random
import sys
from pathlib import Path

import sqlalchemy as sa

ROOT = Path(__file__).resolve().parents[1]
PROJECT_PARENT = ROOT.parent
if str(PROJECT_PARENT) not in sys.path:
    sys.path.insert(0, str(PROJECT_PARENT))

from app_v2.core.db import get_sessionmaker
from app_v2.services.indicator_calc import calculate_indicators


OLD_INDICATOR_FILE = ROOT.parent / "比赛项目初版第一版" / "比赛项目1.1" / "指标计算引擎" / "indicator_calc.py"


def load_old_engine():
    spec = importlib.util.spec_from_file_location("old_indicator_calc", OLD_INDICATOR_FILE)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load old indicator file: {OLD_INDICATOR_FILE}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod.IndicatorEngine


def rel_diff(a: float, b: float) -> float:
    denom = max(abs(a), 1e-9)
    return abs(a - b) / denom


async def pick_samples(n: int = 3):
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as db:
        q = sa.text(
            """
            SELECT de.stock_code, de.stock_name, CAST(ff.year AS INTEGER) AS year
            FROM dim_enterprise de
            JOIN fact_financials ff ON ff.enterprise_id = de.enterprise_id
            WHERE ff.year IS NOT NULL
            LIMIT 100
            """
        )
        rows = (await db.execute(q)).mappings().all()
    if not rows:
        return []
    random.shuffle(rows)
    return rows[:n]


def get_old_result(OldEngine, stock_code_or_name: str, year: int) -> dict:
    data_root = OLD_INDICATOR_FILE.parent.parent / "data" / "new_data_source"
    engine = OldEngine(data_root=str(data_root), target_year=year)
    engine.validate_data()
    engine.calc_indicators()
    return engine.export_json(stock_code_or_name)


def to_num(v):
    if isinstance(v, str):
        s = v.strip().replace("%", "").replace("万元", "")
        if s in ("N/A", ""):
            return None
        try:
            if "%" in v:
                return float(s) / 100.0
            return float(s)
        except Exception:
            return None
    if isinstance(v, (int, float)):
        return float(v)
    return None


async def main():
    if not OLD_INDICATOR_FILE.exists():
        raise FileNotFoundError(str(OLD_INDICATOR_FILE))

    OldEngine = load_old_engine()
    samples = await pick_samples(3)
    if not samples:
        print("No samples found in DB.")
        return

    compare_paths = [
        ("indicators", "financial_health", "current_ratio"),
        ("indicators", "financial_health", "quick_ratio"),
        ("indicators", "financial_health", "roe"),
        ("indicators", "industry_position", "nev_penetration"),
        ("indicators", "legal_risk", "execution_ratio"),
    ]

    print("=== Indicator Migration Validation ===")
    for s in samples:
        code = s["stock_code"] or s["stock_name"]
        year = int(s["year"])
        new_res = await calculate_indicators(str(code), year)
        old_res = get_old_result(OldEngine, str(code), year)

        diffs = []
        for p in compare_paths:
            n = new_res
            o = old_res
            for k in p:
                n = n.get(k, {}) if isinstance(n, dict) else None
                o = o.get(k, {}) if isinstance(o, dict) else None
            n_v = to_num(n)
            o_v = to_num(o)
            if n_v is None or o_v is None:
                continue
            diffs.append(rel_diff(n_v, o_v))

        avg_diff = (sum(diffs) / len(diffs)) if diffs else 0.0
        status = "PASS" if avg_diff < 0.01 else "WARN"
        print(f"{status} sample code={code} year={year} avg_diff={avg_diff:.4f}")


if __name__ == "__main__":
    asyncio.run(main())

