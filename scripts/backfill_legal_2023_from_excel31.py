from __future__ import annotations

import asyncio
import os
import re
from pathlib import Path
from typing import Any

import asyncpg
import pandas as pd


EXCEL_31 = Path(r"c:\Users\0\Desktop\31汽车上市公司诉讼仲裁数据(200308-202303).xlsx")


def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    if v is not None and v.strip() != "":
        return v.strip()
    return default


def _normalize_pg_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn[len("postgresql+asyncpg://") :]
    return dsn


def _to_year(v: Any) -> int | None:
    if v is None or v == "":
        return None
    s = str(v)
    m = re.search(r"(20\d{2})", s)
    if m:
        return int(m.group(1))
    # datetime
    try:
        return int(getattr(v, "year"))
    except Exception:
        return None


def _to_float(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        s = str(v)
        s = s.replace(",", "").replace("，", "").strip()
        try:
            return float(s)
        except Exception:
            return None


async def main() -> None:
    if not EXCEL_31.exists():
        raise FileNotFoundError(str(EXCEL_31))

    dsn = _env("DATABASE_URL") or "postgresql://app_v2:app_v2@127.0.0.1:5432/app_v2"
    dsn = _normalize_pg_dsn(dsn)

    # Read excel (guess sheet 0)
    xls = pd.ExcelFile(EXCEL_31)
    sheet = xls.sheet_names[0]
    df = pd.read_excel(EXCEL_31, sheet_name=sheet)
    df.columns = [str(c).strip() for c in df.columns]

    # Detect columns robustly: prefer keyword match on headers (works even if console can't render),
    # fallback to value-pattern inference.
    def _first_col_with_keywords(keywords: list[str]) -> str | None:
        for c in df.columns:
            for k in keywords:
                if k in str(c):
                    return c
        return None

    # 1) keyword-based
    date_col = _first_col_with_keywords(["公告日期", "立案日期", "裁判日期", "判决日期", "发布日期", "日期"])
    code_col = _first_col_with_keywords(["公司代码", "证券代码", "股票代码", "代码"])
    name_col = _first_col_with_keywords(["公司简称", "证券简称", "公司名称", "企业名称", "简称"])
    amt_col = _first_col_with_keywords(["涉案金额（元）", "涉案金额(元)", "涉案金额", "金额（元）", "金额(元)", "金额"])

    # 2) value-pattern inference (if keyword fails)
    def _match_rate(col: str, pattern: str, sample_n: int = 500) -> float:
        s = df[col].dropna().astype(str).head(sample_n)
        if s.empty:
            return 0.0
        hits = s.str.contains(pattern, regex=True).sum()
        return float(hits) / float(len(s))

    best = 0.0
    # First try: direct 2023-xx-xx presence (most reliable)
    if not date_col:
        for c in df.columns:
            s = df[c].dropna().astype(str).head(800)
            if s.empty:
                continue
            if s.str.contains(r"2023[-/\\.]", regex=True).any():
                date_col = c
                best = 1.0
                break
    if not date_col:
        for c in df.columns:
            r = _match_rate(c, r"20\\d{2}[-/\\.](?:0?[1-9]|1[0-2])[-/\\.](?:0?[1-9]|[12]\\d|3[01])")
            if r > best:
                best = r
                date_col = c

    # code-like: has 6 digits frequently
    if not code_col:
        best_code = 0.0
        for c in df.columns:
            s = df[c].dropna().astype(str).head(800)
            if s.empty:
                continue
            # Prefer pure 6-digit codes (optionally ending with .0)
            hits = s.str.match(r"^\\d{6}(?:\\.0+)?$").sum()
            r = float(hits) / float(len(s))
            if r > best_code:
                best_code = r
                code_col = c

    # name-like: Chinese company short name tends to be 2-8 CJK chars, but avoid picking long text columns
    if not name_col:
        best_name = 0.0
        for c in df.columns:
            if c in {date_col, code_col}:
                continue
            s = df[c].dropna().astype(str).head(800)
            if s.empty:
                continue
            avg_len = s.map(len).mean()
            if avg_len and avg_len > 24:
                continue
            hits = s.str.contains(r"^[\\u4e00-\\u9fa5]{2,12}$", regex=True).sum()
            r = float(hits) / float(len(s))
            if r > best_name:
                best_name = r
                name_col = c

    # amount-like: numeric with big magnitude sometimes, pick column with highest numeric parse rate
    if not amt_col:
        best_amt = 0.0
        for c in df.columns:
            if c in {date_col, code_col, name_col}:
                continue
            s = df[c].dropna().astype(str).head(800)
            if s.empty:
                continue
            s2 = s.str.replace(",", "", regex=False).str.replace("，", "", regex=False).str.strip()
            hits = s2.str.match(r"^-?\\d+(?:\\.\\d+)?$").sum()
            r = float(hits) / float(len(s2))
            if r > best_amt:
                best_amt = r
                amt_col = c

    # Fallback: if strict date pattern fails (some cells may be stored as datetime objects or mixed),
    # pick any column that contains "2023" frequently.
    if (not date_col) or best < 0.01:
        date_col2 = None
        best2 = 0.0
        for c in df.columns:
            r2 = _match_rate(c, r"2023")
            if r2 > best2:
                best2 = r2
                date_col2 = c
        if date_col2 and best2 >= 0.01:
            date_col = date_col2
        else:
            raise RuntimeError(f"Cannot infer date column in sheet={sheet} (best_rate={best:.3f})")
    if not (code_col or name_col):
        raise RuntimeError(f"Cannot infer company code/name column in sheet={sheet}")

    df["__year"] = df[date_col].apply(_to_year)
    df2023 = df[df["__year"] == 2023].copy()
    if df2023.empty:
        print("No 2023 rows found in excel31; nothing to backfill.")
        return

    # normalize stock_code if present (pad to 6 digits + suffix if possible)
    def norm_code(x: Any) -> str:
        s = str(x).strip()
        s = re.sub(r"\\s+", "", s)
        # already has suffix
        if re.fullmatch(r"\\d{6}\\.\\w{2}", s):
            return s
        # digits only
        m = re.search(r"(\\d{6})", s)
        if m:
            return m.group(1)
        return s

    if code_col:
        df2023["__code"] = df2023[code_col].apply(norm_code)
    else:
        df2023["__code"] = ""
    if name_col:
        df2023["__name"] = df2023[name_col].astype(str).str.strip()
    else:
        df2023["__name"] = ""
    if amt_col:
        df2023["__amt"] = df2023[amt_col].apply(_to_float)
    else:
        df2023["__amt"] = None

    grp = (
        df2023.groupby(["__code", "__name"], dropna=False)
        .agg(lawsuit_count=("__year", "size"), lawsuit_total_amount=("__amt", "sum"))
        .reset_index()
    )

    conn = await asyncpg.connect(dsn=dsn)
    try:
        # dim maps
        dim = await conn.fetch("select enterprise_id, stock_code, stock_name from dim_enterprise")
        by_code = {}
        by_name = {}
        for r in dim:
            if r["stock_code"]:
                by_code[str(r["stock_code"]).strip()] = r
            by_name[str(r["stock_name"]).strip()] = r

        staged = []
        unmatched = []
        for _, r in grp.iterrows():
            code = str(r["__code"] or "").strip()
            name = str(r["__name"] or "").strip()
            rec = None
            # Try code match: exact; also try adding .SZ/.SH based on first digit (best-effort)
            if code and code in by_code:
                rec = by_code[code]
            if not rec and code and re.fullmatch(r"\\d{6}", code):
                for suf in [".SZ", ".SH"]:
                    k = code + suf
                    if k in by_code:
                        rec = by_code[k]
                        break
            if not rec and name and name in by_name:
                rec = by_name[name]
            if not rec:
                unmatched.append({"code": code, "name": name})
                continue

            staged.append(
                (
                    str(rec["stock_code"] or code),
                    str(rec["stock_name"]),
                    "2023",
                    str(int(r["lawsuit_count"])),
                    str(float(r["lawsuit_total_amount"])) if r["lawsuit_total_amount"] == r["lawsuit_total_amount"] else None,
                    None,
                    str(rec["enterprise_id"]),
                )
            )

        # Note: asyncpg autocommits each statement; using "ON COMMIT DROP" would drop the temp table immediately.
        await conn.execute("create temp table tmp_legal_2023 (like fact_legal including defaults)")
        if staged:
            await conn.executemany(
                """
                insert into tmp_legal_2023
                (stock_code, stock_name, year, lawsuit_count, lawsuit_total_amount, time_id, enterprise_id)
                values ($1,$2,$3,$4,$5,$6,$7)
                """,
                staged,
            )
            await conn.execute(
                """
                insert into fact_legal (stock_code, stock_name, year, lawsuit_count, lawsuit_total_amount, time_id, enterprise_id)
                select stock_code, stock_name, year, lawsuit_count, lawsuit_total_amount, time_id, enterprise_id
                from tmp_legal_2023
                on conflict (enterprise_id, year)
                do update set
                  stock_code = excluded.stock_code,
                  stock_name = excluded.stock_name,
                  lawsuit_count = excluded.lawsuit_count,
                  lawsuit_total_amount = excluded.lawsuit_total_amount,
                  time_id = excluded.time_id
                """
            )

        print("## backfill_legal_2023_from_excel31")
        print("sheet=", sheet)
        print("rows_2023=", int(len(df2023)))
        print("groups_2023=", int(len(grp)))
        print("matched_groups=", int(len(staged)))
        print("unmatched_groups=", int(len(unmatched)))
        if unmatched:
            for u in unmatched[:30]:
                print("UNMATCHED", u)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
