from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


BASE_DIR = Path(r"C:\Users\0\Desktop\项目\app_v2.2")
RAW_DIR = BASE_DIR / "data" / "原始数据"
OUT_DIR = BASE_DIR / "data" / "cleaned"
REPORT_PATH = BASE_DIR / "data" / "CLEANING_REPORT.md"
GUIDE_PATH = BASE_DIR / "data" / "QUERY_GUIDE.md"
UNMATCHED_LOG = BASE_DIR / "data" / "unmatched.log"


def normalize_name(name: Any) -> str:
    s = "" if name is None else str(name).strip()
    if not s:
        return ""
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[（(].*?[）)]", "", s)
    s = re.sub(r"(股份有限公司|有限责任公司|股份公司|集团股份有限公司|控股股份有限公司|集团|公司)$", "", s)
    s = re.sub(r"[·•\-—_]", "", s)
    return s


def parse_amount_to_yuan(v: Any) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(",", "").replace("，", "")
    mul = 1.0
    if "亿元" in s or re.search(r"(?<!万)亿", s):
        mul = 1e8
    elif "万元" in s or "万" in s:
        mul = 1e4
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    return float(m.group(0)) * mul


def parse_ratio_to_decimal(v: Any, force_percent: bool = False) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, (int, float)):
        x = float(v)
    else:
        s = str(v).strip().replace("%", "")
        m = re.search(r"-?\d+(?:\.\d+)?", s)
        if not m:
            return None
        x = float(m.group(0))
        force_percent = force_percent or ("%" in str(v))
    if force_percent or x > 1.0:
        return x / 100.0
    return x


def classify_sheet(excel_path: Path, sheet_name: str) -> tuple[str, str]:
    low = sheet_name.lower()
    if "目录" in sheet_name or "首页" in sheet_name or "index" in low:
        return "[跳过]", "sheet名称命中跳过规则"
    try:
        probe = pd.read_excel(excel_path, sheet_name=sheet_name, header=None, nrows=5)
        vals = [str(v).strip() for v in probe.fillna("").values.flatten().tolist() if str(v).strip()]
        if vals and all(not any(ch.isdigit() for ch in v) for v in vals):
            return "[跳过]", "前5行均为文本说明"
    except Exception:
        pass
    return "[数据]", "满足数据页条件"


def pick_file_by_prefix(prefix: str) -> Path:
    patt = re.compile(rf"^{re.escape(prefix)}(?!\d)")
    files = [p for p in RAW_DIR.iterdir() if p.is_file() and p.suffix.lower() in {".xlsx", ".xls"} and patt.search(p.name)]
    if not files:
        raise FileNotFoundError(f"未找到前缀为 {prefix} 的文件")
    return sorted(files, key=lambda p: p.name)[0]


@dataclass
class Mapper:
    base_df: pd.DataFrame
    canonical_col: str
    company_col: str
    code_col: str

    def __post_init__(self) -> None:
        self.alias_to_canonical: dict[str, str] = {}
        self.canonical_to_code: dict[str, str] = {}
        self.canonical_to_company: dict[str, str] = {}
        for _, r in self.base_df.iterrows():
            canonical = str(r.get(self.canonical_col, "")).strip()
            if not canonical:
                continue
            company = str(r.get(self.company_col, "")).strip()
            code = str(r.get(self.code_col, "")).strip()
            self.canonical_to_code[canonical] = code
            self.canonical_to_company[canonical] = company
            aliases = {canonical, company, normalize_name(canonical), normalize_name(company)}
            if code:
                aliases.add(code)
                aliases.add(code.split(".")[0])
            for a in aliases:
                if a:
                    self.alias_to_canonical[a] = canonical

    def match(self, raw_name: Any) -> tuple[str | None, str]:
        n = "" if raw_name is None else str(raw_name).strip()
        if not n:
            return None, "empty"
        cand = [n, normalize_name(n)]
        for c in cand:
            if c in self.alias_to_canonical:
                return self.alias_to_canonical[c], "exact"
        nn = normalize_name(n)
        if nn:
            for alias, canonical in self.alias_to_canonical.items():
                if len(alias) < 2:
                    continue
                if nn in alias or alias in nn:
                    return canonical, "fuzzy"
        return None, "unmatched"


def detect_base_columns(df: pd.DataFrame) -> tuple[str, str, str, str]:
    cols = list(df.columns)
    code_col = cols[0]
    short_col = cols[1] if len(cols) > 1 else cols[0]
    company_col = cols[6] if len(cols) > 6 else short_col
    founded_col = cols[7] if len(cols) > 7 else cols[-1]
    for c in cols:
        s = str(c)
        if "证券代码" in s or "通联代码" in s:
            code_col = c
        if "证券简称" in s or "股票简称" in s:
            short_col = c
        if "中文名称" in s:
            company_col = c
        elif (("公司" in s and "名称" in s) or "公司全称" in s) and ("英文" not in s):
            company_col = c
        if "成立" in s and "日期" in s:
            founded_col = c
    return code_col, short_col, company_col, founded_col


def longify_financial(df: pd.DataFrame, code_col: str, short_col: str) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for c in df.columns:
        cs = str(c)
        ym = re.search(r"(20\d{2})", cs)
        if not ym:
            continue
        year = int(ym.group(1))
        metric = cs.split("\n")[0].strip()
        force_percent = ("%" in cs) or ("[单位]%" in cs)
        for _, r in df.iterrows():
            raw_val = r.get(c)
            if pd.isna(raw_val):
                val = None
            else:
                val = raw_val
            records.append(
                {
                    "stock_code": str(r.get(code_col, "")).strip(),
                    "source_name": str(r.get(short_col, "")).strip(),
                    "year": year,
                    "metric_raw": metric,
                    "value_raw": val,
                    "header_raw": cs,
                    "force_percent": force_percent,
                }
            )
    return pd.DataFrame(records)


def map_metric_name(m: str) -> str | None:
    mm = str(m)
    checks = [
        ("revenue", [r"营业总收入", r"营业收入", r"营收"]),
        ("net_profit", [r"净利润", r"归母.*净利润", r"归属于母公司.*净利润"]),
        ("total_assets", [r"资产总计", r"总资产"]),
        ("total_liabilities", [r"负债合计", r"总负债"]),
        ("current_ratio", [r"流动比率"]),
        ("quick_ratio", [r"速动比率"]),
        ("roe", [r"净资产收益率", r"ROE", r"资产净利率"]),
        ("gross_margin", [r"毛利率", r"销售毛利率"]),
    ]
    for field, pats in checks:
        if any(re.search(p, mm, flags=re.IGNORECASE) for p in pats):
            return field
    return None


def summarize_df(df: pd.DataFrame, name: str) -> str:
    return f"- {name}: 行数={len(df)}, 列数={len(df.columns)}"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    UNMATCHED_LOG.write_text("", encoding="utf-8")

    # Phase 1: audit all files/sheets
    sheet_audit: list[dict[str, Any]] = []
    all_files = sorted([p for p in RAW_DIR.iterdir() if p.is_file()])
    for f in all_files:
        info = {"file": f.name, "sheets": []}
        if f.suffix.lower() in {".xlsx", ".xls"}:
            xls = pd.ExcelFile(f)
            for s in xls.sheet_names:
                mark, reason = classify_sheet(f, s)
                info["sheets"].append({"sheet": s, "mark": mark, "reason": reason})
        sheet_audit.append(info)
    print(f"[Phase1] 文件总数={len(all_files)}; Excel文件数={sum(1 for f in all_files if f.suffix.lower() in {'.xlsx','.xls'})}")

    # Phase 2: build enterprise map from file 23
    file23 = pick_file_by_prefix("23")
    df23 = pd.read_excel(file23, sheet_name=0)
    code_col, short_col, company_col, founded_col = detect_base_columns(df23)
    base_map = df23[[code_col, short_col, company_col, founded_col]].copy()
    base_map.columns = ["证券代码", "证券简称", "公司中文名称", "成立日期"]
    mapper = Mapper(base_df=base_map, canonical_col="证券简称", company_col="公司中文名称", code_col="证券代码")
    print(f"[Phase2] 基准企业数={len(base_map)}; 映射别名数={len(mapper.alias_to_canonical)}")

    unmatched_rows: list[str] = []
    conversion_notes: list[str] = []
    file_stats: list[dict[str, Any]] = []

    # Phase 3.1 financial clean
    fin_files = [pick_file_by_prefix("24"), pick_file_by_prefix("26"), pick_file_by_prefix("28")]
    fin_long_parts: list[pd.DataFrame] = []
    for fp in fin_files:
        df = pd.read_excel(fp, sheet_name=0)
        before_rows = len(df)
        code_col_f, short_col_f, _, _ = detect_base_columns(df)
        long_df = longify_financial(df, code_col_f, short_col_f)
        long_df["field"] = long_df["metric_raw"].map(map_metric_name)
        long_df = long_df[long_df["field"].notna()].copy()
        # amount/ratio conversion
        def _convert_row(r: pd.Series) -> float | None:
            field = str(r["field"])
            raw = r["value_raw"]
            if field in {"revenue", "net_profit", "total_assets", "total_liabilities"}:
                return parse_amount_to_yuan(raw)
            if field in {"roe", "gross_margin"}:
                force_pct = bool(r.get("force_percent", False))
                return parse_ratio_to_decimal(raw, force_percent=force_pct)
            if field in {"current_ratio", "quick_ratio"}:
                return parse_ratio_to_decimal(raw, force_percent=False)
            return None

        long_df["value"] = long_df.apply(_convert_row, axis=1)
        conv_count = int(long_df["value"].notna().sum())
        conversion_notes.append(f"{fp.name}: 转换记录={conv_count}")
        fin_long_parts.append(long_df[["stock_code", "source_name", "year", "field", "value"]])
        file_stats.append({"file": fp.name, "before_rows": before_rows, "after_rows": len(long_df)})
        print(f"[Phase3-Fin] {fp.name}: 原始行={before_rows}, 长表行={len(long_df)}, 非空值={conv_count}")

    fin_all = pd.concat(fin_long_parts, ignore_index=True) if fin_long_parts else pd.DataFrame()
    fin_pivot = (
        fin_all.pivot_table(index=["stock_code", "source_name", "year"], columns="field", values="value", aggfunc="first")
        .reset_index()
        .rename_axis(None, axis=1)
    )
    fin_pivot["enterprise_name"] = fin_pivot["source_name"].apply(lambda x: mapper.match(x)[0])
    for _, r in fin_pivot[fin_pivot["enterprise_name"].isna()].iterrows():
        unmatched_rows.append(f"[financial] {r.get('source_name','')} -> unmatched")
    fin_pivot = fin_pivot[fin_pivot["enterprise_name"].notna()].copy()
    fin_pivot["security_code"] = fin_pivot["enterprise_name"].map(mapper.canonical_to_code)
    financials = fin_pivot[
        [
            "security_code",
            "enterprise_name",
            "year",
            "revenue",
            "net_profit",
            "total_assets",
            "total_liabilities",
            "current_ratio",
            "quick_ratio",
            "roe",
            "gross_margin",
        ]
    ].copy()

    # Phase 3.2 sales clean
    # file 1
    f1 = pick_file_by_prefix("1")
    d1 = pd.read_excel(f1, sheet_name=0)
    c_manu_1 = d1.columns[2] if len(d1.columns) > 2 else d1.columns[0]
    c_type_1 = d1.columns[1] if len(d1.columns) > 1 else d1.columns[0]
    c_date_1 = d1.columns[5] if len(d1.columns) > 5 else d1.columns[-1]
    c_val_1 = d1.columns[6] if len(d1.columns) > 6 else d1.columns[-1]
    s1 = d1.copy()
    s1 = s1[s1[c_type_1].astype(str).str.contains("销", na=False)].copy()
    s1["year"] = pd.to_datetime(s1[c_date_1], errors="coerce").dt.year
    s1["sales_volume"] = pd.to_numeric(s1[c_val_1], errors="coerce")
    s1["source_name"] = s1[c_manu_1].astype(str).str.strip()
    s1 = s1[["source_name", "year", "sales_volume"]].dropna(subset=["year"])
    file_stats.append({"file": f1.name, "before_rows": len(d1), "after_rows": len(s1)})
    print(f"[Phase3-Sales] {f1.name}: 原始行={len(d1)}, 保留行={len(s1)}")

    # file 2
    f2 = pick_file_by_prefix("2")
    d2 = pd.read_excel(f2, sheet_name=0, header=[0, 1])
    flat_cols = []
    for a, b in d2.columns:
        aa = "" if a is None else str(a).strip()
        bb = "" if b is None else str(b).strip()
        flat_cols.append((aa + "_" + bb).strip("_"))
    d2.columns = flat_cols
    c_manu_2 = d2.columns[1] if len(d2.columns) > 1 else d2.columns[0]
    c_model_2 = d2.columns[2] if len(d2.columns) > 2 else d2.columns[0]
    c_detail_2 = d2.columns[3] if len(d2.columns) > 3 else d2.columns[0]
    c_date_2 = d2.columns[5] if len(d2.columns) > 5 else d2.columns[-1]
    c_val_2 = d2.columns[6] if len(d2.columns) > 6 else d2.columns[-1]
    s2 = d2.copy()
    s2 = s2[
        s2[c_model_2].astype(str).str.contains("总计", na=False) | s2[c_detail_2].astype(str).str.contains("总计", na=False)
    ].copy()
    s2["year"] = pd.to_datetime(s2[c_date_2], errors="coerce").dt.year
    s2["nev_sales_volume"] = pd.to_numeric(s2[c_val_2], errors="coerce")
    s2["source_name"] = s2[c_manu_2].astype(str).str.strip()
    s2 = s2[["source_name", "year", "nev_sales_volume"]].dropna(subset=["year"])
    file_stats.append({"file": f2.name, "before_rows": len(d2), "after_rows": len(s2)})
    print(f"[Phase3-Sales] {f2.name}: 原始行={len(d2)}, 保留行={len(s2)}")

    # file 3 (overall NEV market)
    f3 = pick_file_by_prefix("3")
    d3 = pd.read_excel(f3, sheet_name=0, header=[0, 1])
    d3.columns = [(("" if a is None else str(a).strip()) + "_" + ("" if b is None else str(b).strip())).strip("_") for a, b in d3.columns]
    c_type_3 = d3.columns[1] if len(d3.columns) > 1 else d3.columns[0]
    c_fuel_3 = d3.columns[3] if len(d3.columns) > 3 else d3.columns[0]
    c_date_3 = d3.columns[4] if len(d3.columns) > 4 else d3.columns[-1]
    c_sales_3 = d3.columns[9] if len(d3.columns) > 9 else d3.columns[-1]
    s3 = d3.copy()
    s3 = s3[s3[c_type_3].astype(str).str.contains("总计", na=False) & s3[c_fuel_3].astype(str).str.contains("总计", na=False)].copy()
    s3["year"] = pd.to_datetime(s3[c_date_3], errors="coerce").dt.year
    s3["nev_sales_volume"] = pd.to_numeric(s3[c_sales_3], errors="coerce")
    s3["source_name"] = "全国新能源总体"
    s3 = s3[["source_name", "year", "nev_sales_volume"]].dropna(subset=["year"])
    file_stats.append({"file": f3.name, "before_rows": len(d3), "after_rows": len(s3)})
    print(f"[Phase3-Sales] {f3.name}: 原始行={len(d3)}, 保留行={len(s3)}")

    sales_merge = pd.merge(
        s1.groupby(["source_name", "year"], as_index=False)["sales_volume"].sum(),
        s2.groupby(["source_name", "year"], as_index=False)["nev_sales_volume"].sum(),
        on=["source_name", "year"],
        how="outer",
    )
    sales_merge = pd.concat([sales_merge, s3.groupby(["source_name", "year"], as_index=False)["nev_sales_volume"].sum()], ignore_index=True)
    sales_merge["enterprise_name"] = sales_merge["source_name"].apply(lambda x: mapper.match(x)[0])
    sales_merge["match_mode"] = sales_merge["source_name"].apply(lambda x: mapper.match(x)[1])
    for _, r in sales_merge[sales_merge["enterprise_name"].isna()].iterrows():
        unmatched_rows.append(f"[sales] {r.get('source_name','')} -> unmatched")
    sales = sales_merge[sales_merge["enterprise_name"].notna()].copy()
    sales["security_code"] = sales["enterprise_name"].map(mapper.canonical_to_code)
    sales["sales_volume"] = sales["sales_volume"].round(0).astype("Int64")
    sales["nev_sales_volume"] = sales["nev_sales_volume"].round(0).astype("Int64")
    sales = sales[["security_code", "enterprise_name", "year", "sales_volume", "nev_sales_volume"]]

    # Phase 3.3 legal clean
    f31 = pick_file_by_prefix("31")
    l31 = pd.read_excel(f31, sheet_name=0)
    c_code_31 = l31.columns[1] if len(l31.columns) > 1 else l31.columns[0]
    c_name_31 = l31.columns[2] if len(l31.columns) > 2 else l31.columns[0]
    c_date_31 = l31.columns[3] if len(l31.columns) > 3 else l31.columns[0]
    c_amt_31 = l31.columns[8] if len(l31.columns) > 8 else l31.columns[-1]
    c_ccy_31 = l31.columns[9] if len(l31.columns) > 9 else l31.columns[-1]
    legal_raw = l31.copy()
    legal_raw["year"] = pd.to_datetime(legal_raw[c_date_31], errors="coerce").dt.year
    legal_raw["source_name"] = legal_raw[c_name_31].astype(str).str.strip()
    legal_raw["source_code"] = legal_raw[c_code_31].astype(str).str.strip()
    legal_raw["lawsuit_amount_yuan"] = legal_raw[c_amt_31].apply(parse_amount_to_yuan)
    legal_raw["currency"] = legal_raw[c_ccy_31].astype(str).str.upper()
    # Non-CNY amounts are kept as numeric yuan-equivalent assumption unavailable; keep NULL instead of guessing.
    legal_raw.loc[~legal_raw["currency"].str.contains("CNY|人民币|RMB", na=False), "lawsuit_amount_yuan"] = None
    legal_raw["enterprise_name"] = legal_raw["source_name"].apply(lambda x: mapper.match(x)[0])
    for _, r in legal_raw[legal_raw["enterprise_name"].isna()].iterrows():
        if r.get("source_code"):
            direct = mapper.match(r["source_code"])[0]
            if direct:
                legal_raw.at[r.name, "enterprise_name"] = direct
                continue
        unmatched_rows.append(f"[legal] {r.get('source_name','')} / {r.get('source_code','')} -> unmatched")
    legal = (
        legal_raw[legal_raw["enterprise_name"].notna()]
        .groupby(["enterprise_name", "year"], as_index=False)
        .agg(lawsuit_count=("enterprise_name", "size"), lawsuit_total_amount=("lawsuit_amount_yuan", "sum"))
    )
    legal["security_code"] = legal["enterprise_name"].map(mapper.canonical_to_code)
    legal = legal[["security_code", "enterprise_name", "year", "lawsuit_count", "lawsuit_total_amount"]]
    file_stats.append({"file": f31.name, "before_rows": len(l31), "after_rows": len(legal_raw)})
    print(f"[Phase3-Legal] {f31.name}: 原始行={len(l31)}, 清洗行={len(legal_raw)}, 聚合行={len(legal)}")

    # Phase 4: write outputs + report
    financials_out = OUT_DIR / "financials.csv"
    sales_out = OUT_DIR / "sales.csv"
    legal_out = OUT_DIR / "legal.csv"
    financials.to_csv(financials_out, index=False, encoding="utf-8-sig")
    sales.to_csv(sales_out, index=False, encoding="utf-8-sig")
    legal.to_csv(legal_out, index=False, encoding="utf-8-sig")
    UNMATCHED_LOG.write_text("\n".join(sorted(set(unmatched_rows))), encoding="utf-8")

    # Phase 5: guide stats
    fin2022 = set(financials.loc[financials["year"] == 2022, "enterprise_name"].dropna().unique())
    sales2022 = set(sales.loc[sales["year"] == 2022, "enterprise_name"].dropna().unique())
    legal2022 = set(legal.loc[legal["year"] == 2022, "enterprise_name"].dropna().unique())
    full2022 = sorted(fin2022 & sales2022 & legal2022)
    fin_all_ents = sorted(financials["enterprise_name"].dropna().unique())
    sales_all_ents = sorted(sales["enterprise_name"].dropna().unique())
    legal_all_ents = sorted(legal["enterprise_name"].dropna().unique())
    sentiment_ents = sorted(set(legal_all_ents))  # litigation/news event proxies

    guide = []
    guide.append("# QUERY_GUIDE")
    guide.append("")
    guide.append("## 2022年三领域完全覆盖企业")
    guide.append(f"- 数量: {len(full2022)}")
    guide.append("- 名单(前30): " + ("、".join(full2022[:30]) if full2022 else "无"))
    guide.append("")
    guide.append("## 财务分析可用企业")
    guide.append(f"- 数量: {len(fin_all_ents)}")
    guide.append("- 名单(前100): " + ("、".join(fin_all_ents[:100]) if fin_all_ents else "无"))
    guide.append("")
    guide.append("## 销售分析可用企业")
    guide.append(f"- 数量: {len(sales_all_ents)}")
    guide.append("- 名单(前100): " + ("、".join(sales_all_ents[:100]) if sales_all_ents else "无"))
    guide.append("")
    guide.append("## 司法分析可用企业")
    guide.append(f"- 数量: {len(legal_all_ents)}")
    guide.append("- 名单(前100): " + ("、".join(legal_all_ents[:100]) if legal_all_ents else "无"))
    guide.append("")
    guide.append("## 舆情分析可用企业（基于司法/重大事件代理）")
    guide.append(f"- 数量: {len(sentiment_ents)}")
    guide.append("- 名单(前100): " + ("、".join(sentiment_ents[:100]) if sentiment_ents else "无"))
    GUIDE_PATH.write_text("\n".join(guide), encoding="utf-8")

    report = []
    report.append("# CLEANING_REPORT")
    report.append("")
    report.append("## 第一阶段：文件结构审计与分页确认")
    report.append(f"- 原始目录: `{RAW_DIR}`")
    report.append(f"- 文件总数: {len(all_files)}")
    for item in sheet_audit:
        report.append(f"- 文件: `{item['file']}`")
        if item["sheets"]:
            for s in item["sheets"]:
                report.append(f"  - {s['mark']} `{s['sheet']}`（{s['reason']}）")
        else:
            report.append("  - 非Excel文件")
    report.append("")
    report.append("## 第二阶段：企业全量地图")
    report.append(f"- 基准文件: `{file23.name}`")
    report.append(f"- 企业基准行数: {len(base_map)}")
    report.append(f"- 映射别名数量: {len(mapper.alias_to_canonical)}")
    report.append(f"- 基准字段: 证券代码=`{code_col}`，证券简称=`{short_col}`，公司中文名称=`{company_col}`，成立日期=`{founded_col}`")
    report.append("")
    report.append("## 第三阶段：逐文件清洗统计")
    for st in file_stats:
        report.append(f"- `{st['file']}`: 清洗前行数={st['before_rows']}，清洗后行数={st['after_rows']}")
    report.append("")
    report.append("## 单位换算与空值策略")
    report.append("- 金额字段统一换算到“元”；包含“万/亿”按系数转换。")
    report.append("- 比率字段统一为小数；带`%`或数值>1时按百分比除以100。")
    report.append("- 缺失值保持为NULL，未做0/均值填充。")
    for n in conversion_notes:
        report.append(f"- {n}")
    report.append("")
    report.append("## 未匹配企业")
    report.append(f"- 详见 `{UNMATCHED_LOG}`，总条数={len(set(unmatched_rows))}")
    report.append("")
    report.append("## 输出文件")
    report.append(f"- `{financials_out}`")
    report.append(f"- `{sales_out}`")
    report.append(f"- `{legal_out}`")
    report.append(f"- `{GUIDE_PATH}`")
    report.append("")
    report.append("## 数据规模摘要")
    report.append(summarize_df(financials, "financials.csv"))
    report.append(summarize_df(sales, "sales.csv"))
    report.append(summarize_df(legal, "legal.csv"))
    REPORT_PATH.write_text("\n".join(report), encoding="utf-8")

    print("[DONE] outputs:")
    print(str(financials_out))
    print(str(sales_out))
    print(str(legal_out))
    print(str(REPORT_PATH))
    print(str(GUIDE_PATH))
    print(str(UNMATCHED_LOG))


if __name__ == "__main__":
    main()

