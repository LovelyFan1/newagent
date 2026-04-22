from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import sqlalchemy as sa

from app.db.session import get_sessionmaker


ROOT = Path(__file__).resolve().parents[1]  # /app (in container) or app_v2 (local)
DATA_CLEANED = ROOT / "data" / "cleaned"
LEGAL_CSV = DATA_CLEANED / "legal.csv"
LEGAL_XLSX_GLOB = "*诉讼仲裁*.xlsx"


YEAR = 2022


ALIASES = {
    "比亚迪股份": "比亚迪",
}


def _norm_stock_code(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    # keep digits only
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 6:
        return digits[-6:]
    return digits or None


def _norm_name(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    s = ALIASES.get(s, s)
    s = re.sub(r"\s+", "", s)
    return s


_AMOUNT_RE = re.compile(r"[-+]?\d+(?:\.\d+)?")


def _parse_amount_to_yuan(v: Any) -> float:
    """
    Parse amount field into yuan.
    - Accepts '元/万元/亿元' suffixes
    - Strips commas and non-numeric chars
    """
    if v is None:
        return 0.0
    if isinstance(v, (int, float, np.number)):
        if np.isnan(v):
            return 0.0
        return float(v)
    s = str(v).replace(",", "").strip()
    if not s or s.lower() == "nan":
        return 0.0

    unit = 1.0
    if "亿元" in s:
        unit = 1e8
    elif "万元" in s:
        unit = 1e4
    elif "元" in s:
        unit = 1.0

    m = _AMOUNT_RE.search(s)
    if not m:
        return 0.0
    try:
        return float(m.group(0)) * unit
    except ValueError:
        return 0.0


def _extract_year_from_any(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, (int, np.integer)):
        y = int(v)
        return y if 1900 <= y <= 2100 else None
    if isinstance(v, float):
        if np.isnan(v):
            return None
        y = int(v)
        return y if 1900 <= y <= 2100 else None
    s = str(v).strip()
    if not s:
        return None
    # pandas Timestamp
    try:
        ts = pd.to_datetime(v, errors="raise")
        if not pd.isna(ts):
            return int(ts.year)
    except Exception:
        pass
    m = re.search(r"(20\d{2}|19\d{2})", s)
    if not m:
        return None
    return int(m.group(1))


@dataclass
class EnterpriseRow:
    enterprise_id: str
    stock_code: str | None
    stock_name: str | None
    standard_name: str | None


async def _load_enterprises() -> list[EnterpriseRow]:
    sm = get_sessionmaker()
    async with sm() as db:
        rows = (
            await db.execute(
                sa.text(
                    """
                    SELECT enterprise_id, stock_code, stock_name, standard_name
                    FROM dim_enterprise
                    """
                )
            )
        ).mappings().all()
    return [
        EnterpriseRow(
            enterprise_id=str(r["enterprise_id"]),
            stock_code=_norm_stock_code(r.get("stock_code")),
            stock_name=_norm_name(r.get("stock_name")),
            standard_name=_norm_name(r.get("standard_name")),
        )
        for r in rows
    ]


def _build_enterprise_maps(ents: Iterable[EnterpriseRow]):
    by_code: dict[str, EnterpriseRow] = {}
    by_name: dict[str, EnterpriseRow] = {}
    for e in ents:
        if e.stock_code:
            by_code[e.stock_code] = e
        for n in (e.stock_name, e.standard_name):
            if n:
                by_name[n] = e
    return by_code, by_name


def _read_legal_csv_2022() -> pd.DataFrame:
    if not LEGAL_CSV.exists():
        raise FileNotFoundError(str(LEGAL_CSV))
    df = pd.read_csv(LEGAL_CSV, dtype=str)
    # expected columns: stock_code, stock_name, year, lawsuit_count, lawsuit_total_amount
    df["year_i"] = df["year"].apply(_extract_year_from_any)
    df = df[df["year_i"] == YEAR].copy()
    df["stock_code_norm"] = df["stock_code"].apply(_norm_stock_code)
    df["stock_name_norm"] = df["stock_name"].apply(_norm_name)
    df["lawsuit_count_i"] = pd.to_numeric(df.get("lawsuit_count"), errors="coerce").fillna(0).astype(int)
    df["lawsuit_total_amount_yuan"] = df.get("lawsuit_total_amount").apply(_parse_amount_to_yuan)
    return df


def _guess_excel_columns(df: pd.DataFrame) -> dict[str, str | None]:
    cols = list(df.columns)
    col_lower = {c: str(c).lower() for c in cols}

    def pick(preds: list[str]) -> str | None:
        for c in cols:
            name = str(c)
            low = col_lower[c]
            if any(p in name for p in preds) or any(p in low for p in preds):
                return c
        return None

    return {
        "stock_code": pick(["股票代码", "代码", "证券代码", "stock_code", "stock code"]),
        "stock_name": pick(["公司简称", "公司名称", "企业名称", "证券简称", "stock_name", "stock name"]),
        "date": pick(["公告日期", "起诉日期", "立案日期", "日期", "发生日期"]),
        "year": pick(["年份", "年度", "year"]),
        "amount": pick(["涉案金额", "金额", "标的金额", "诉讼金额", "仲裁金额"]),
    }


def _read_excel_2022() -> pd.DataFrame:
    paths = sorted(DATA_CLEANED.glob(LEGAL_XLSX_GLOB))
    if not paths:
        raise FileNotFoundError(f"no xlsx matched: {DATA_CLEANED}/{LEGAL_XLSX_GLOB}")
    xlsx_path = paths[0]

    xls = pd.ExcelFile(xlsx_path)
    sheet = None
    for cand in ["诉讼仲裁", "Sheet1", xls.sheet_names[0]]:
        if cand in xls.sheet_names:
            sheet = cand
            break
    if sheet is None:
        sheet = xls.sheet_names[0]

    df = pd.read_excel(xlsx_path, sheet_name=sheet)
    # normalize column names by stripping
    df.columns = [str(c).strip() for c in df.columns]

    m = _guess_excel_columns(df)
    # year filter
    if m["year"] is not None:
        df["year_i"] = df[m["year"]].apply(_extract_year_from_any)
    elif m["date"] is not None:
        df["year_i"] = df[m["date"]].apply(_extract_year_from_any)
    else:
        # fallback: try any column contains date-like values
        df["year_i"] = None
        for c in df.columns:
            if "日期" in str(c):
                tmp = df[c].apply(_extract_year_from_any)
                if tmp.notna().any():
                    df["year_i"] = tmp
                    break

    df = df[df["year_i"] == YEAR].copy()

    code_col = m["stock_code"]
    name_col = m["stock_name"]
    amt_col = m["amount"]

    df["stock_code_norm"] = df[code_col].apply(_norm_stock_code) if code_col else None
    df["stock_name_norm"] = df[name_col].apply(_norm_name) if name_col else None
    df["lawsuit_total_amount_yuan"] = df[amt_col].apply(_parse_amount_to_yuan) if amt_col else 0.0
    df["lawsuit_count_i"] = 1
    return df


def _map_to_enterprise_id(df: pd.DataFrame, by_code: dict[str, EnterpriseRow], by_name: dict[str, EnterpriseRow]) -> pd.DataFrame:
    ent_ids: list[str | None] = []
    ent_name: list[str | None] = []
    ent_code: list[str | None] = []

    for _, r in df.iterrows():
        code = r.get("stock_code_norm")
        name = r.get("stock_name_norm")

        e = None
        if code and code in by_code:
            e = by_code[code]
        elif name and name in by_name:
            e = by_name[name]
        else:
            # try fuzzy: contains match on dim names (very limited)
            if name:
                for k, v in by_name.items():
                    if k and name == k:
                        e = v
                        break

        if e is None:
            ent_ids.append(None)
            ent_name.append(name)
            ent_code.append(code)
        else:
            ent_ids.append(e.enterprise_id)
            ent_name.append(e.stock_name or e.standard_name or name)
            ent_code.append(e.stock_code or code)

    out = df.copy()
    out["enterprise_id"] = ent_ids
    out["mapped_stock_name"] = ent_name
    out["mapped_stock_code"] = ent_code
    return out


def _aggregate(df: pd.DataFrame) -> pd.DataFrame:
    df_ok = df[df["enterprise_id"].notna()].copy()
    if df_ok.empty:
        return pd.DataFrame(columns=["enterprise_id", "year", "stock_code", "stock_name", "lawsuit_count", "lawsuit_total_amount"])

    g = (
        df_ok.groupby("enterprise_id", as_index=False)
        .agg(
            lawsuit_count=("lawsuit_count_i", "sum"),
            lawsuit_total_amount=("lawsuit_total_amount_yuan", "sum"),
            stock_code=("mapped_stock_code", "first"),
            stock_name=("mapped_stock_name", "first"),
        )
        .copy()
    )
    g["year"] = str(YEAR)
    # store as text to match existing schema
    g["lawsuit_count"] = g["lawsuit_count"].astype(int).astype(str)
    g["lawsuit_total_amount"] = g["lawsuit_total_amount"].round(2).astype(float).astype(str)
    return g[["enterprise_id", "stock_code", "stock_name", "year", "lawsuit_count", "lawsuit_total_amount"]]


def _merge_sources(csv_agg: pd.DataFrame, xlsx_agg: pd.DataFrame) -> pd.DataFrame:
    if csv_agg.empty and xlsx_agg.empty:
        return csv_agg
    if csv_agg.empty:
        return xlsx_agg
    if xlsx_agg.empty:
        return csv_agg

    merged = csv_agg.merge(xlsx_agg, on=["enterprise_id", "year"], how="outer", suffixes=("_csv", "_xlsx"))

    def pick_row(r):
        # prefer xlsx when present; if both present, take max to avoid undercount (and avoid double counting)
        def pick_text(a, b):
            return b if (b is not None and str(b) != "nan") else a

        def pick_num(a, b):
            try:
                fa = float(a) if a not in (None, "", "nan") else 0.0
            except Exception:
                fa = 0.0
            try:
                fb = float(b) if b not in (None, "", "nan") else 0.0
            except Exception:
                fb = 0.0
            return str(max(fa, fb))

        return {
            "enterprise_id": r["enterprise_id"],
            "year": r["year"],
            "stock_code": pick_text(r.get("stock_code_csv"), r.get("stock_code_xlsx")),
            "stock_name": pick_text(r.get("stock_name_csv"), r.get("stock_name_xlsx")),
            "lawsuit_count": pick_num(r.get("lawsuit_count_csv"), r.get("lawsuit_count_xlsx")),
            "lawsuit_total_amount": pick_num(r.get("lawsuit_total_amount_csv"), r.get("lawsuit_total_amount_xlsx")),
        }

    rows = [pick_row(r) for _, r in merged.iterrows()]
    return pd.DataFrame(rows)


async def _ensure_unique_index(db) -> None:
    # fact_legal was imported as a raw table (text columns) and may contain duplicates.
    # Deduplicate first so we can create the unique index required by UPSERT.
    await db.execute(
        sa.text(
            """
            WITH ranked AS (
                SELECT
                    ctid,
                    enterprise_id,
                    year,
                    row_number() OVER (
                        PARTITION BY enterprise_id, year
                        ORDER BY
                            COALESCE(NULLIF(lawsuit_count, '')::float, 0) DESC,
                            COALESCE(NULLIF(lawsuit_total_amount, '')::float, 0) DESC
                    ) AS rn
                FROM fact_legal
                WHERE enterprise_id IS NOT NULL AND year IS NOT NULL
            )
            DELETE FROM fact_legal
            WHERE ctid IN (SELECT ctid FROM ranked WHERE rn > 1);
            """
        )
    )
    await db.execute(
        sa.text(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_fact_legal_enterprise_year
            ON fact_legal (enterprise_id, year);
            """
        )
    )
    await db.commit()


async def _upsert_fact_legal(db, df: pd.DataFrame) -> None:
    if df.empty:
        return
    await _ensure_unique_index(db)

    insert_sql = sa.text(
        """
        INSERT INTO fact_legal (enterprise_id, stock_code, stock_name, year, lawsuit_count, lawsuit_total_amount)
        VALUES (:enterprise_id, :stock_code, :stock_name, :year, :lawsuit_count, :lawsuit_total_amount)
        ON CONFLICT (enterprise_id, year) DO UPDATE SET
          stock_code = EXCLUDED.stock_code,
          stock_name = EXCLUDED.stock_name,
          lawsuit_count = EXCLUDED.lawsuit_count,
          lawsuit_total_amount = EXCLUDED.lawsuit_total_amount
        """
    )

    payload = df.to_dict(orient="records")
    await db.execute(insert_sql, payload)
    await db.commit()


async def _validate(db) -> dict[str, int]:
    r1 = (await db.execute(sa.text("SELECT COUNT(*)::int AS c FROM fact_legal WHERE year = :y"), {"y": str(YEAR)})).mappings().one()["c"]
    r2 = (
        await db.execute(
            sa.text("SELECT COUNT(DISTINCT enterprise_id)::int AS c FROM fact_legal WHERE year = :y"),
            {"y": str(YEAR)},
        )
    ).mappings().one()["c"]
    return {"rows_2022": int(r1), "distinct_enterprise_2022": int(r2)}


async def main() -> int:
    # 1) read sources
    df_csv = _read_legal_csv_2022()
    df_xlsx = _read_excel_2022()

    # 2) load dim_enterprise mapping
    ents = await _load_enterprises()
    by_code, by_name = _build_enterprise_maps(ents)

    # 3) map
    csv_m = _map_to_enterprise_id(df_csv, by_code, by_name)
    xlsx_m = _map_to_enterprise_id(df_xlsx, by_code, by_name)

    # 4) aggregate
    csv_agg = _aggregate(csv_m)
    xlsx_agg = _aggregate(xlsx_m)
    final_agg = _merge_sources(csv_agg, xlsx_agg)

    # log unmatched (do not import)
    unmatched_csv = int(csv_m["enterprise_id"].isna().sum())
    unmatched_xlsx = int(xlsx_m["enterprise_id"].isna().sum())
    print(f"[legal.csv] 2022 rows={len(df_csv)} mapped={len(csv_m)-unmatched_csv} unmatched={unmatched_csv}")
    print(f"[xlsx] 2022 rows={len(df_xlsx)} mapped={len(xlsx_m)-unmatched_xlsx} unmatched={unmatched_xlsx}")
    print(f"[agg] csv={len(csv_agg)} xlsx={len(xlsx_agg)} merged={len(final_agg)}")

    # 5) write to DB
    sm = get_sessionmaker()
    async with sm() as db:
        await _upsert_fact_legal(db, final_agg)
        stats = await _validate(db)

    print("Validation SQL results:", stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

