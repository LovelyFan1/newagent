from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
CLEANED_DIR = ROOT / "data" / "cleaned"

SALES_SRC = CLEANED_DIR / "sales.csv"
ENTERPRISE_DIM = CLEANED_DIR / "企业维度表.csv"
TIME_DIM = CLEANED_DIR / "时间维度表.csv"

OUT_CSV = CLEANED_DIR / "fact_sales_rebuilt.csv"
UNMATCHED_LOG = CLEANED_DIR / "fact_sales_rebuilt_unmatched.csv"


def _norm_str(x: object) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower() == "nan":
        return ""
    return s


def _norm_stock_code(x: object) -> str:
    s = _norm_str(x).upper()
    return s


def _norm_stock_name(x: object) -> str:
    s = _norm_str(x)
    # remove common spaces/punct
    return s.replace(" ", "").replace("　", "")


def main() -> None:
    for p in (SALES_SRC, ENTERPRISE_DIM, TIME_DIM):
        if not p.exists():
            raise FileNotFoundError(str(p))

    sales = pd.read_csv(SALES_SRC)
    total_rows = len(sales)

    ent = pd.read_csv(ENTERPRISE_DIM, dtype=str)
    ent["enterprise_id"] = ent["enterprise_id"].astype(int)
    ent["stock_code"] = ent["stock_code"].map(_norm_stock_code)
    ent["stock_name"] = ent["stock_name"].map(_norm_stock_name)
    ent["standard_name"] = ent["standard_name"].map(_norm_stock_name)

    UNKNOWN_TOKENS = {"δ֪", "未知", "UNK", "UNKNOWN"}

    code_map = {c: int(eid) for c, eid in zip(ent["stock_code"], ent["enterprise_id"], strict=False) if c}
    name_map = {
        n: int(eid)
        for n, eid in zip(ent["stock_name"], ent["enterprise_id"], strict=False)
        if n and n not in UNKNOWN_TOKENS
    }
    std_map = {n: int(eid) for n, eid in zip(ent["standard_name"], ent["enterprise_id"], strict=False) if n}

    tdim = pd.read_csv(TIME_DIM, dtype=str)
    # prefer granularity == 'year'
    year_rows = tdim.loc[tdim["granularity"].astype(str) == "year", ["year", "time_id"]]
    year_to_time_id = {}
    if not year_rows.empty:
        for y, tid in zip(year_rows["year"], year_rows["time_id"], strict=False):
            y = _norm_str(y)
            if y and y.isdigit():
                year_to_time_id[int(y)] = int(tid)

    def to_year(v: object) -> int | None:
        if pd.isna(v):
            return None
        try:
            return int(float(v))
        except Exception:
            s = _norm_str(v)
            if s.isdigit():
                return int(s)
        return None

    rebuilt_rows = []
    unmatched_rows = []

    for _, r in sales.iterrows():
        stock_code = _norm_stock_code(r.get("stock_code"))
        stock_name = _norm_stock_name(r.get("stock_name"))
        year = to_year(r.get("year"))
        if year is None:
            unmatched_rows.append({**r.to_dict(), "reason": "missing_year"})
            continue

        eid = None
        if stock_code and stock_code in code_map:
            eid = code_map[stock_code]
        elif stock_name and stock_name in name_map:
            eid = name_map[stock_name]
        elif stock_name and stock_name in std_map:
            eid = std_map[stock_name]

        if eid is None:
            unmatched_rows.append({**r.to_dict(), "reason": "unmatched_enterprise"})
            continue

        time_id = year_to_time_id.get(year, year)  # fallback to year if mapping missing

        rebuilt_rows.append(
            {
                "enterprise_id": eid,
                "year": year,
                "total_sales_volume": r.get("total_sales_volume"),
                "nev_sales_volume": r.get("nev_sales_volume"),
                "time_id": time_id,
            }
        )

    rebuilt = pd.DataFrame(rebuilt_rows)
    if rebuilt.empty:
        raise RuntimeError("No rows matched enterprise_id; cannot build fact_sales_rebuilt.csv")

    rebuilt.insert(0, "sales_id", range(1, len(rebuilt) + 1))

    # stats
    matched = len(rebuilt)
    match_rate = matched / max(total_rows, 1) * 100
    print(f"sales.csv rows: {total_rows}")
    print(f"matched enterprise_id rows: {matched}")
    print(f"match rate: {match_rate:.2f}%")
    print(f"unmatched rows: {len(unmatched_rows)} (logged to {UNMATCHED_LOG.name})")

    rebuilt.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    if unmatched_rows:
        pd.DataFrame(unmatched_rows).to_csv(UNMATCHED_LOG, index=False, encoding="utf-8-sig")

    print(f"wrote: {OUT_CSV}")


if __name__ == "__main__":
    main()

