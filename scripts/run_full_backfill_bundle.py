from __future__ import annotations

import asyncio
import os
import re
from pathlib import Path
from typing import Any

import asyncpg
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
CLEANED = ROOT / "data" / "cleaned"
REPORT_PATH = ROOT / "scripts" / "diagnose_missing_coverage_report.md"
DOC_PATH = CLEANED / "数据文件说明.md"

# Files user provided (absolute paths)
RAW_FILES = [
    Path(r"c:\Users\0\Desktop\23汽车A股上市公司基本信息（269家，63个指标）.xlsx"),
    Path(r"c:\Users\0\Desktop\24汽车A股上市公司财务摘要（269家，10个指标，2006-2022）.xlsx"),
    Path(r"c:\Users\0\Desktop\25汽车A股上市公司营运能力指标（269家，9个指标，2006-2022）.xlsx"),
    Path(r"c:\Users\0\Desktop\26汽车A股上市公司偿债能力指标（269家，12个指标，2006-2022）.xlsx"),
    Path(r"c:\Users\0\Desktop\27汽车A股上市公司成长能力指标（269家，14个指标，2006-2022）.xlsx"),
    Path(r"c:\Users\0\Desktop\28汽车A股上市公司盈利能力指标（269家，6个指标，2006-2022）.xlsx"),
    Path(r"c:\Users\0\Desktop\29汽车A股上市公司研发投入（269家，9个指标，2006-2022）.xlsx"),
    Path(r"c:\Users\0\Desktop\1汽车分品牌产销(95家车企，768个车型，201512-202210月度数据).xlsx"),
    Path(r"c:\Users\0\Desktop\2新能源汽车分厂商产销(207家厂商，201812-202210月度数据).xlsx"),
    Path(r"c:\Users\0\Desktop\3新能源汽车总体产销(201812-202210月度数据).xlsx"),
    Path(r"c:\Users\0\Desktop\31汽车上市公司诉讼仲裁数据(200308-202303).xlsx"),
    Path(r"c:\Users\0\Desktop\锂电-新能源汽车产业数据库240427_0714230635.xlsx"),
    Path(r"c:\Users\0\Desktop\新能源车行业数据库-20250529.xlsx"),
    Path(r"c:\Users\0\Desktop\29汽车上市公司并购重组数据(201012-202303).xlsx"),
    Path(r"c:\Users\0\Desktop\30汽车上市公司IPO发行数据(199012-202303).xlsx"),
    Path(r"c:\Users\0\Desktop\32汽车产量统计分省（月，200204-202212，112个指标，不是新能源汽车产量，是所有汽车产量总和）.xls"),
    Path(r"c:\Users\0\Desktop\充电桩数量（月，201602-202212，全国各省份）.xls"),
    Path(r"c:\Users\0\Desktop\22汽车上市公司专利数量-实用新型（年，1995-2022，72家）.xls"),
    Path(r"c:\Users\0\Desktop\21汽车上市公司专利数量-发明专利（年，1999-2022，68家）.xls"),
    Path(r"c:\Users\0\Desktop\20汽车上市公司专利数量-发明授权（年，2001-2022，53家）.xls"),
    Path(r"c:\Users\0\Desktop\19汽车上市公司专利数量-外观设计（年，1997-2022，36家）.xls"),
    Path(r"c:\Users\0\Desktop\18动力电池销量分材料类型（月，201906-202212，10个指标）.xls"),
    Path(r"c:\Users\0\Desktop\17动力电池装车量分材料类型（月，201701-202301，10个指标）.xls"),
    Path(r"c:\Users\0\Desktop\16动力电池装车量分车型（月，201909-202301，10个指标）.xls"),
    Path(r"c:\Users\0\Desktop\15配套动力电池企业数量（月，201701-202301）.xls"),
    Path(r"c:\Users\0\Desktop\14动力电池产量分材料类型（月，201901-202301，10个指标）.xls"),
    Path(r"c:\Users\0\Desktop\13公共充电桩数量分公司（123个指标，月，202005-202301）.xls"),
    Path(r"c:\Users\0\Desktop\12公共充电桩运营商充电电量（21家运营商，月，202012-202301）.xls"),
    Path(r"c:\Users\0\Desktop\11全球各国充电桩保有量（年，2007-2021，66个国家）.xls"),
    Path(r"c:\Users\0\Desktop\10国内充电设施数量（省份，月，201602-202302）.xls"),
    Path(r"c:\Users\0\Desktop\9全球电动汽车市场份额（20个国家，2005-2021）.xls"),
    Path(r"c:\Users\0\Desktop\8全球电动汽车保有量（20个国家，纯电动汽车，2005-2021）.xls"),
    Path(r"c:\Users\0\Desktop\7全球电动汽车保有量（20个国家，插电式混合动力汽车，2009-2021）.xls"),
    Path(r"c:\Users\0\Desktop\6全球电动汽车新车销量（20个国家，插电式混合动力，2009-2021）.xls"),
    Path(r"c:\Users\0\Desktop\5全球电动汽车新车销量（20个国家，纯电动汽车，2005-2021）.xls"),
    Path(r"c:\Users\0\Desktop\4支付交易数据（周，20210103-20230122，蔚来，理想，小鹏，比亚迪，特斯拉）.xls"),
]


def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    if v is not None and v.strip() != "":
        return v.strip()
    return default


def _normalize_pg_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn[len("postgresql+asyncpg://") :]
    return dsn


def _as_year(v: Any) -> int | None:
    if v is None or v == "":
        return None
    s = str(v)
    m = re.search(r"(20\d{2})", s)
    if m:
        return int(m.group(1))
    try:
        return int(float(s))
    except Exception:
        return None


def _as_text(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


async def _upsert_dim_from_alias(conn: asyncpg.Connection, aliases: set[str]) -> int:
    max_id = await conn.fetchval(
        "select coalesce(max((enterprise_id)::int), 0) from dim_enterprise where enterprise_id ~ '^[0-9]+$'"
    )
    next_id = int(max_id or 0) + 1
    existing = await conn.fetch("select stock_name from dim_enterprise")
    exists = {str(r["stock_name"]).strip() for r in existing}

    inserted = 0
    for name in sorted(aliases):
        if not name or name in exists:
            continue
        await conn.execute(
            "insert into dim_enterprise (standard_name, enterprise_id, stock_code, stock_name) values ($1,$2,$3,$4)",
            name,
            str(next_id),
            None,
            name,
        )
        next_id += 1
        inserted += 1
    return inserted


async def _load_dim_maps(conn: asyncpg.Connection) -> tuple[dict[str, str], dict[str, tuple[str, str]]]:
    rows = await conn.fetch("select enterprise_id, stock_name, stock_code from dim_enterprise")
    by_name = {}
    by_code = {}
    for r in rows:
        eid = str(r["enterprise_id"])
        name = str(r["stock_name"]).strip()
        code = str(r["stock_code"]).strip() if r["stock_code"] else ""
        by_name[name] = eid
        if code:
            by_code[code] = (eid, name)
    return by_name, by_code


async def backfill_from_cleaned_csv(conn: asyncpg.Connection) -> tuple[int, int, int]:
    # Reuse earlier cleaned CSV-first logic in compact form.
    fin = pd.read_csv(CLEANED / "financials.csv", dtype=str).fillna("")
    sal = pd.read_csv(CLEANED / "sales.csv", dtype=str).fillna("")
    leg = pd.read_csv(CLEANED / "legal.csv", dtype=str).fillna("")
    fin.columns = [c.strip() for c in fin.columns]
    sal.columns = [c.strip() for c in sal.columns]
    leg.columns = [c.strip() for c in leg.columns]

    by_name, by_code = await _load_dim_maps(conn)

    fin_rows = []
    for _, r in fin.iterrows():
        code = str(r.get("stock_code") or "").strip()
        name = str(r.get("stock_name") or "").strip()
        y = _as_year(r.get("year"))
        if not y:
            continue
        if code in by_code:
            eid, std_name = by_code[code]
        elif name in by_name:
            eid, std_name = by_name[name], name
        else:
            continue
        fin_rows.append(
            (
                code,
                std_name,
                str(y),
                _as_text(r.get("revenue")),
                _as_text(r.get("net_profit")),
                _as_text(r.get("total_assets")),
                _as_text(r.get("total_liabilities")),
                _as_text(r.get("operating_cash_flow")),
                _as_text(r.get("current_ratio")),
                _as_text(r.get("quick_ratio")),
                _as_text(r.get("debt_asset_ratio")),
                _as_text(r.get("roe")),
                _as_text(r.get("net_margin")),
                _as_text(r.get("rd_expense")),
                None,
                eid,
            )
        )

    sal_rows = []
    for _, r in sal.iterrows():
        code = str(r.get("stock_code") or "").strip()
        name = str(r.get("stock_name") or "").strip()
        y = _as_year(r.get("year"))
        if not y:
            continue
        if code in by_code:
            eid = by_code[code][0]
        elif name in by_name:
            eid = by_name[name]
        else:
            continue
        sal_rows.append((eid, str(y), _as_text(r.get("total_sales_volume")), _as_text(r.get("nev_sales_volume")), None))

    leg_rows = []
    for _, r in leg.iterrows():
        code = str(r.get("stock_code") or "").strip()
        name = str(r.get("stock_name") or "").strip()
        y = _as_year(r.get("year"))
        if not y:
            continue
        if code in by_code:
            eid, std_name = by_code[code]
        elif name in by_name:
            eid, std_name = by_name[name], name
        else:
            continue
        leg_rows.append((code, std_name, str(y), _as_text(r.get("lawsuit_count")), _as_text(r.get("lawsuit_total_amount")), None, eid))

    # stage + merge
    await conn.execute("create temp table tmp_fin (like fact_financials including defaults)")
    if fin_rows:
        await conn.executemany(
            """
            insert into tmp_fin
            (stock_code, stock_name, year, revenue, net_profit, total_assets, total_liabilities, operating_cash_flow,
             current_ratio, quick_ratio, debt_asset_ratio, roe, net_margin, rd_expense, time_id, enterprise_id)
            values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
            """,
            fin_rows,
        )
        await conn.execute(
            """
            delete from fact_financials f using (select distinct enterprise_id, year from tmp_fin) s
            where f.enterprise_id=s.enterprise_id and f.year::text=s.year::text
            """
        )
        await conn.execute(
            """
            insert into fact_financials
            (stock_code, stock_name, year, revenue, net_profit, total_assets, total_liabilities, operating_cash_flow,
             current_ratio, quick_ratio, debt_asset_ratio, roe, net_margin, rd_expense, time_id, enterprise_id)
            select stock_code, stock_name, year, revenue, net_profit, total_assets, total_liabilities, operating_cash_flow,
                   current_ratio, quick_ratio, debt_asset_ratio, roe, net_margin, rd_expense, time_id, enterprise_id
            from tmp_fin
            """
        )

    await conn.execute("create temp table tmp_sal (like fact_sales including defaults)")
    if sal_rows:
        await conn.executemany(
            "insert into tmp_sal (enterprise_id, year, total_sales_volume, nev_sales_volume, time_id) values ($1,$2,$3,$4,$5)",
            sal_rows,
        )
        await conn.execute(
            """
            delete from fact_sales f using (select distinct enterprise_id, year from tmp_sal) s
            where f.enterprise_id=s.enterprise_id and f.year::text=s.year::text
            """
        )
        max_id = await conn.fetchval(
            "select coalesce(max((sales_id)::bigint), 0) from fact_sales where sales_id ~ '^[0-9]+$'"
        )
        await conn.execute(
            """
            insert into fact_sales (sales_id, enterprise_id, year, total_sales_volume, nev_sales_volume, time_id)
            select ($1::bigint + row_number() over (order by enterprise_id, year))::text,
                   enterprise_id, year, total_sales_volume, nev_sales_volume, time_id
            from tmp_sal
            """,
            int(max_id or 0),
        )

    await conn.execute("create temp table tmp_leg (like fact_legal including defaults)")
    if leg_rows:
        await conn.executemany(
            "insert into tmp_leg (stock_code, stock_name, year, lawsuit_count, lawsuit_total_amount, time_id, enterprise_id) values ($1,$2,$3,$4,$5,$6,$7)",
            leg_rows,
        )
        await conn.execute(
            """
            insert into fact_legal (stock_code, stock_name, year, lawsuit_count, lawsuit_total_amount, time_id, enterprise_id)
            select stock_code, stock_name, year, lawsuit_count, lawsuit_total_amount, time_id, enterprise_id
            from tmp_leg
            on conflict (enterprise_id, year)
            do update set
              stock_code=excluded.stock_code,
              stock_name=excluded.stock_name,
              lawsuit_count=excluded.lawsuit_count,
              lawsuit_total_amount=excluded.lawsuit_total_amount,
              time_id=excluded.time_id
            """
        )

    return len(fin_rows), len(sal_rows), len(leg_rows)


async def backfill_legal_2023_from_31(conn: asyncpg.Connection, path31: Path) -> tuple[int, int]:
    if not path31.exists():
        return 0, 0
    xls = pd.ExcelFile(path31)
    sheet = xls.sheet_names[0]
    df = pd.read_excel(path31, sheet_name=sheet)
    df.columns = [str(c).strip() for c in df.columns]

    # date col: first column containing '2023-' sample
    date_col = None
    for c in df.columns:
        s = df[c].dropna().astype(str).head(800)
        if not s.empty and s.str.contains(r"2023[-/\.]", regex=True).any():
            date_col = c
            break
    if not date_col:
        return 0, 0

    # code col with many 6-digit values
    code_col = None
    best = 0.0
    for c in df.columns:
        s = df[c].dropna().astype(str).head(800)
        if s.empty:
            continue
        r = float(s.str.match(r"^\d{6}(?:\.0+)?$").sum()) / float(len(s))
        if r > best:
            best = r
            code_col = c
    amt_col = None
    best_amt = 0.0
    for c in df.columns:
        if c == date_col or c == code_col:
            continue
        s = df[c].dropna().astype(str).head(800)
        if s.empty:
            continue
        s2 = s.str.replace(",", "", regex=False).str.replace("，", "", regex=False).str.strip()
        r = float(s2.str.match(r"^-?\d+(?:\.\d+)?$").sum()) / float(len(s2))
        if r > best_amt:
            best_amt = r
            amt_col = c

    df["__year"] = df[date_col].apply(_as_year)
    d23 = df[df["__year"] == 2023].copy()
    if d23.empty:
        return 0, 0

    def norm_code(x: Any) -> str:
        s = str(x).strip()
        m = re.search(r"(\d{6})", s)
        return m.group(1) if m else ""

    d23["__code"] = d23[code_col].apply(norm_code) if code_col else ""
    d23["__amt"] = d23[amt_col].apply(_as_text) if amt_col else None
    grp = d23.groupby(["__code"], dropna=False).agg(lawsuit_count=("__year", "size"), lawsuit_total_amount=("__amt", "sum")).reset_index()

    by_name, by_code = await _load_dim_maps(conn)
    _ = by_name
    staged = []
    unmatched = 0
    for _, r in grp.iterrows():
        code6 = str(r["__code"] or "").strip()
        rec = None
        for suf in [".SZ", ".SH", ""]:
            k = code6 + suf
            if k in by_code:
                rec = by_code[k]
                break
        if not rec:
            unmatched += 1
            continue
        eid, std_name = rec
        staged.append((code6, std_name, "2023", str(int(r["lawsuit_count"])), _as_text(r["lawsuit_total_amount"]), None, eid))

    if staged:
        await conn.execute("create temp table tmp_leg_2023 (like fact_legal including defaults)")
        await conn.executemany(
            "insert into tmp_leg_2023 (stock_code, stock_name, year, lawsuit_count, lawsuit_total_amount, time_id, enterprise_id) values ($1,$2,$3,$4,$5,$6,$7)",
            staged,
        )
        await conn.execute(
            """
            insert into fact_legal (stock_code, stock_name, year, lawsuit_count, lawsuit_total_amount, time_id, enterprise_id)
            select stock_code, stock_name, year, lawsuit_count, lawsuit_total_amount, time_id, enterprise_id
            from tmp_leg_2023
            on conflict (enterprise_id, year)
            do update set
              stock_code=excluded.stock_code,
              stock_name=excluded.stock_name,
              lawsuit_count=excluded.lawsuit_count,
              lawsuit_total_amount=excluded.lawsuit_total_amount,
              time_id=excluded.time_id
            """
        )
    return len(staged), unmatched


async def coverage_counts(conn: asyncpg.Connection, year: int) -> tuple[int, int, int, list[str]]:
    y = str(year)
    fin = await conn.fetch("select distinct enterprise_id from fact_financials where year=$1", y)
    sal = await conn.fetch("select distinct enterprise_id from fact_sales where year=$1", y)
    leg = await conn.fetch("select distinct enterprise_id from fact_legal where year=$1", y)
    fin_set = {str(r["enterprise_id"]) for r in fin}
    sal_set = {str(r["enterprise_id"]) for r in sal}
    leg_set = {str(r["enterprise_id"]) for r in leg}
    rows = await conn.fetch("select enterprise_id, stock_name from dim_enterprise")
    full = [str(r["stock_name"]) for r in rows if str(r["enterprise_id"]) in fin_set and str(r["enterprise_id"]) in sal_set and str(r["enterprise_id"]) in leg_set]
    return len(fin_set), len(sal_set), len(leg_set), sorted(full)


def generate_data_doc() -> str:
    lines = ["# 数据文件说明", ""]
    lines.append("## 口径对齐规则")
    lines.append("- 企业名标准：以 `23汽车A股上市公司基本信息.xlsx` 中证券简称作为基准，其他名称做别名映射。")
    lines.append("- 财务金额统一按元口径存储；销量按辆；比率按小数。")
    lines.append("- 时间字段统一提取为 `YYYY`。")
    lines.append("")
    lines.append("## 文件清单与用途")
    lines.append("| 文件 | 用途 | 时间范围(按文件名推断) |")
    lines.append("|---|---|---|")
    for p in RAW_FILES:
        use = "其他"
        n = p.name
        if "基本信息" in n:
            use = "企业维度/别名基准"
        elif "财务摘要" in n or "偿债能力" in n or "盈利能力" in n or "营运能力" in n or "成长能力" in n or "研发投入" in n:
            use = "财务补充"
        elif "产销" in n:
            use = "销售/产量补充"
        elif "诉讼仲裁" in n:
            use = "法律补充"
        elif "专利" in n:
            use = "专利补充"
        elif "充电" in n or "动力电池" in n or "电动汽车" in n:
            use = "行业扩展数据"
        elif "支付交易" in n:
            use = "支付行为扩展数据"
        m = re.search(r"(\d{4})[-~—](\d{4})|(\d{6})-(\d{6})", n)
        rng = m.group(0) if m else "见文件名"
        lines.append(f"| {n} | {use} | {rng} |")
    lines.append("")
    lines.append("## 说明")
    lines.append("- 优先使用 cleaned CSV 回填事实表；原始 Excel 作为补充与覆盖增强。")
    lines.append("- 若源文件年份不覆盖（如 2006-2022），对应 2023 无法强行回填。")
    return "\n".join(lines) + "\n"


async def main() -> None:
    dsn = _env("DATABASE_URL") or "postgresql://app_v2:app_v2@127.0.0.1:5432/app_v2"
    dsn = _normalize_pg_dsn(dsn)
    conn = await asyncpg.connect(dsn=dsn)
    try:
        # Alias base from 23 file + cleaned enterprise_basic
        aliases = set()
        f23 = RAW_FILES[0]
        if f23.exists():
            x = pd.read_excel(f23)
            x.columns = [str(c).strip() for c in x.columns]
            for col in x.columns:
                if "证券简称" in col or "公司简称" in col or "公司名称" in col:
                    aliases.update({str(v).strip() for v in x[col].dropna().tolist() if str(v).strip()})
        eb = pd.read_csv(CLEANED / "enterprise_basic.csv", dtype=str).fillna("")
        aliases.update({str(v).strip() for v in eb.get("stock_name", []).tolist() if str(v).strip()})

        async with conn.transaction():
            inserted_dim = await _upsert_dim_from_alias(conn, aliases)
            fin_n, sal_n, leg_n = await backfill_from_cleaned_csv(conn)
            legal23_n, legal23_unmatched = await backfill_legal_2023_from_31(conn, RAW_FILES[10])

        # docs
        DOC_PATH.write_text(generate_data_doc(), encoding="utf-8")

        # stats
        f22, s22, l22, full22 = await coverage_counts(conn, 2022)
        f23, s23, l23, full23 = await coverage_counts(conn, 2023)
        summary = []
        summary.append("\n\n## 全量原始数据回填结果（本轮）\n")
        summary.append(f"- dim_enterprise 新增企业数：**{inserted_dim}**\n")
        summary.append(f"- 财务回填（CSV主）写入行数：**{fin_n}**\n")
        summary.append(f"- 销售回填（CSV主）写入行数：**{sal_n}**\n")
        summary.append(f"- 法律回填（CSV主）写入行数：**{leg_n}**\n")
        summary.append(f"- 司法 2023（31号Excel）新增/更新组数：**{legal23_n}**，未匹配组：**{legal23_unmatched}**\n")
        summary.append("\n### 2022 覆盖统计\n")
        summary.append(f"- 财务覆盖企业数：**{f22}**\n")
        summary.append(f"- 销售覆盖企业数：**{s22}**\n")
        summary.append(f"- 法律覆盖企业数：**{l22}**\n")
        summary.append(f"- 三领域完全覆盖企业数：**{len(full22)}**\n")
        if full22:
            summary.append("- 三领域完全覆盖企业名单（前20）：\n")
            for n in full22[:20]:
                summary.append(f"  - {n}\n")
        else:
            summary.append("- 三领域完全覆盖企业名单（前20）：（无）\n")
        summary.append("\n### 2023 覆盖统计\n")
        summary.append(f"- 财务覆盖企业数：**{f23}**\n")
        summary.append(f"- 销售覆盖企业数：**{s23}**\n")
        summary.append(f"- 法律覆盖企业数：**{l23}**\n")
        summary.append(f"- 三领域完全覆盖企业数：**{len(full23)}**\n")
        REPORT_PATH.write_text((REPORT_PATH.read_text(encoding="utf-8") if REPORT_PATH.exists() else "") + "".join(summary), encoding="utf-8")

        print("## full_backfill_done")
        print("inserted_dim", inserted_dim)
        print("financial_rows", fin_n)
        print("sales_rows", sal_n)
        print("legal_rows", leg_n)
        print("legal_2023_rows", legal23_n, "unmatched", legal23_unmatched)
        print("coverage_2022", {"finance": f22, "sales": s22, "legal": l22, "full": len(full22)})
        print("coverage_2023", {"finance": f23, "sales": s23, "legal": l23, "full": len(full23)})
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())

