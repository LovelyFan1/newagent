from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd


BASE = Path(r"C:\Users\0\Desktop\项目\app_v2.2")
RAW = BASE / "data" / "原始数据"
CLEANED = BASE / "data" / "cleaned"
UNMATCHED_LOG = BASE / "data" / "unmatched.log"
REPORT_PATH = BASE / "data" / "CLEANING_REPORT.md"


def pick_file(prefix: str) -> Path:
    patt = re.compile(rf"^{re.escape(prefix)}(?!\d)")
    hits = [p for p in RAW.iterdir() if p.is_file() and p.suffix.lower() in {".xlsx", ".xls"} and patt.search(p.name)]
    if not hits:
        raise FileNotFoundError(prefix)
    return sorted(hits, key=lambda x: x.name)[0]


def normalize_name(x: Any) -> str:
    s = "" if x is None else str(x).strip()
    if not s:
        return ""
    s = s.replace("（", "(").replace("）", ")")
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"\s+", "", s)
    s = re.sub(
        r"(汽车制造公司|汽车科技公司|汽车工业公司|汽车股份公司|汽车集团公司|汽车公司|股份有限公司|有限责任公司|股份公司|集团公司|集团控股|控股公司|集团)$",
        "",
        s,
    )
    s = re.sub(r"(公司|股份)$", "", s)
    return s


def parse_unmatched_sales_aliases() -> list[str]:
    if not UNMATCHED_LOG.exists():
        return []
    lines = UNMATCHED_LOG.read_text(encoding="utf-8").splitlines()
    aliases = []
    for ln in lines:
        m = re.search(r"^\[sales\]\s*(.*?)\s*->\s*unmatched", ln)
        if m:
            aliases.append(m.group(1).strip())
    return sorted(set(a for a in aliases if a))


def load_enterprise_basic() -> pd.DataFrame:
    f23 = pick_file("23")
    df = pd.read_excel(f23, sheet_name=0)
    code_col, short_col, company_col = df.columns[0], df.columns[1], df.columns[6]
    for c in df.columns:
        s = str(c)
        if "证券代码" in s or "通联代码" in s:
            code_col = c
        if "证券简称" in s or "股票简称" in s:
            short_col = c
        if "中文名称" in s:
            company_col = c
    out = df[[code_col, short_col, company_col]].copy()
    out.columns = ["security_code", "security_short_name", "company_name"]
    out["security_short_name"] = out["security_short_name"].astype(str).str.strip()
    out["company_name"] = out["company_name"].astype(str).str.strip()
    out["norm_short"] = out["security_short_name"].map(normalize_name)
    out["norm_company"] = out["company_name"].map(normalize_name)
    out.to_csv(CLEANED / "enterprise_basic.csv", index=False, encoding="utf-8-sig")
    return out


def build_manual_seed(alias_list: list[str], enterprise_df: pd.DataFrame) -> pd.DataFrame:
    # helper to find canonical by keyword from enterprise short names
    names = enterprise_df["security_short_name"].dropna().astype(str).tolist()

    def find_name(keyword: str, default: str = "UNMATCHED") -> str:
        for n in names:
            if keyword in n:
                return n
        return default

    hard_rules = {
        "特斯拉": "UNMATCHED",
        "蔚来": "UNMATCHED",
        "理想": "UNMATCHED",
        "小鹏": "UNMATCHED",
        "威马": "UNMATCHED",
        "观致": "UNMATCHED",
        "东风": find_name("东风汽车"),
        "上汽": find_name("上汽集团"),
        "北京奔驰": find_name("北汽蓝谷"),
        "北京现代": find_name("北汽蓝谷"),
        "北汽": find_name("北汽蓝谷"),
        "华晨宝马": "UNMATCHED",
        "华晨": "UNMATCHED",
        "东风日产": find_name("东风汽车"),
        "东风本田": find_name("东风汽车"),
        "东风裕隆": find_name("东风汽车"),
        "东风小康": find_name("赛力斯"),
        "东风神龙": find_name("东风汽车"),
        "中国一汽": find_name("一汽解放"),
        "一汽": find_name("一汽解放"),
        "长安": find_name("长安汽车"),
        "长城": find_name("长城汽车"),
        "广汽": find_name("广汽集团"),
        "比亚迪": find_name("比亚迪"),
        "江铃": find_name("江铃汽车"),
        "江淮": find_name("江淮汽车"),
        "福田": find_name("福田汽车"),
        "海马": find_name("海马汽车"),
        "宇通": find_name("宇通客车"),
        "中通": find_name("中通客车"),
        "安凯": find_name("安凯客车"),
        "亚星": find_name("亚星客车"),
        "金龙": find_name("金龙汽车"),
        "赛力斯": find_name("赛力斯"),
        "金康": find_name("赛力斯"),
        "瑞驰": find_name("赛力斯"),
        "力帆": find_name("力帆科技"),
        "重汽": find_name("中国重汽"),
        "吉利": "UNMATCHED",
        "奇瑞": "UNMATCHED",
        "岚图": "UNMATCHED",
    }
    rows = []
    for a in alias_list:
        target = None
        note = ""
        for k, v in hard_rules.items():
            if k in a:
                target = v
                note = f"manual_seed_by_keyword:{k}"
                break
        if target is not None:
            rows.append({"raw_alias": a, "mapped_enterprise": target, "note": note})
    # add explicit entries requested by user
    for a, t, note in [
        ("特斯拉(上海)", "UNMATCHED", "上海工厂归入集团但不在A股基准库"),
        ("理想汽车", "UNMATCHED", "非A股上市主体，保留UNMATCHED"),
        ("蔚来汽车", "UNMATCHED", "非A股上市主体，保留UNMATCHED"),
        ("小鹏汽车", "UNMATCHED", "非A股上市主体，保留UNMATCHED"),
    ]:
        rows.append({"raw_alias": a, "mapped_enterprise": t, "note": note})
    return pd.DataFrame(rows).drop_duplicates(subset=["raw_alias"], keep="first")


def build_enhanced_alias_mapping(aliases: list[str], ent: pd.DataFrame, manual_map: pd.DataFrame) -> pd.DataFrame:
    exact_map: dict[str, str] = {}
    for _, r in ent.iterrows():
        short, company = str(r["security_short_name"]), str(r["company_name"])
        exact_map[normalize_name(short)] = short
        exact_map[normalize_name(company)] = short

    manual_dict = {str(r["raw_alias"]): str(r["mapped_enterprise"]) for _, r in manual_map.iterrows()}
    manual_note = {str(r["raw_alias"]): str(r["note"]) for _, r in manual_map.iterrows()}
    names_norm = [(normalize_name(n), n) for n in ent["security_short_name"].dropna().astype(str)]

    records: list[dict[str, Any]] = []
    for alias in aliases:
        clean = normalize_name(alias)
        level = "UNMATCHED"
        mapped = f"UNMATCHED::{clean or alias}"
        note = "no_rule_match"

        # Level 1 exact
        if clean in exact_map:
            mapped = exact_map[clean]
            level = "L1_EXACT"
            note = "exact_match_to_enterprise_basic"
        else:
            # Level 2 fuzzy contains
            candidates = []
            for norm_name, orig in names_norm:
                if not norm_name or len(norm_name) < 2:
                    continue
                if norm_name in clean or clean in norm_name:
                    candidates.append((len(norm_name), orig))
            if candidates:
                candidates.sort(reverse=True)
                mapped = candidates[0][1]
                level = "L2_FUZZY"
                note = "substring_fuzzy_match"

        # Level 3 manual override
        if alias in manual_dict:
            target = manual_dict[alias]
            mapped = target if target != "UNMATCHED" else f"UNMATCHED::{clean or alias}"
            level = "L3_MANUAL"
            note = manual_note.get(alias, "manual_map")

        records.append(
            {
                "raw_alias": alias,
                "clean_alias": clean,
                "mapped_enterprise": mapped,
                "match_level": level,
                "note": note,
            }
        )
    out = pd.DataFrame(records).drop_duplicates(subset=["raw_alias"], keep="first")
    out.to_csv(CLEANED / "enhanced_alias_mapping.csv", index=False, encoding="utf-8-sig")
    manual_map.to_csv(CLEANED / "manual_alias_map.csv", index=False, encoding="utf-8-sig")
    return out


def load_sales_raw_1() -> pd.DataFrame:
    f1 = pick_file("1")
    d1 = pd.read_excel(f1, sheet_name=0)
    c_manu = d1.columns[2] if len(d1.columns) > 2 else d1.columns[0]
    c_type = d1.columns[1] if len(d1.columns) > 1 else d1.columns[0]
    c_date = d1.columns[5] if len(d1.columns) > 5 else d1.columns[-1]
    c_val = d1.columns[6] if len(d1.columns) > 6 else d1.columns[-1]
    s = d1[d1[c_type].astype(str).str.contains("销", na=False)].copy()
    s["year"] = pd.to_datetime(s[c_date], errors="coerce").dt.year
    s["raw_alias"] = s[c_manu].astype(str).str.strip()
    s["sales_volume"] = pd.to_numeric(s[c_val], errors="coerce")
    return s[["raw_alias", "year", "sales_volume"]].dropna(subset=["year"])


def load_sales_raw_2() -> pd.DataFrame:
    f2 = pick_file("2")
    d2 = pd.read_excel(f2, sheet_name=0, header=[0, 1])
    d2.columns = [(("" if a is None else str(a).strip()) + "_" + ("" if b is None else str(b).strip())).strip("_") for a, b in d2.columns]
    c_manu = d2.columns[1] if len(d2.columns) > 1 else d2.columns[0]
    c_model = d2.columns[2] if len(d2.columns) > 2 else d2.columns[0]
    c_detail = d2.columns[3] if len(d2.columns) > 3 else d2.columns[0]
    c_date = d2.columns[5] if len(d2.columns) > 5 else d2.columns[-1]
    c_val = d2.columns[6] if len(d2.columns) > 6 else d2.columns[-1]
    s = d2[
        d2[c_model].astype(str).str.contains("总计", na=False) | d2[c_detail].astype(str).str.contains("总计", na=False)
    ].copy()
    s["year"] = pd.to_datetime(s[c_date], errors="coerce").dt.year
    s["raw_alias"] = s[c_manu].astype(str).str.strip()
    s["nev_sales_volume"] = pd.to_numeric(s[c_val], errors="coerce")
    return s[["raw_alias", "year", "nev_sales_volume"]].dropna(subset=["year"])


def rebuild_sales(mapping: pd.DataFrame) -> pd.DataFrame:
    m = mapping[["raw_alias", "mapped_enterprise", "match_level"]].copy()
    s1 = load_sales_raw_1().merge(m, on="raw_alias", how="left")
    s2 = load_sales_raw_2().merge(m, on="raw_alias", how="left")
    s1["enterprise_name"] = s1["mapped_enterprise"].fillna(s1["raw_alias"].map(lambda x: f"UNMATCHED::{normalize_name(x)}"))
    s2["enterprise_name"] = s2["mapped_enterprise"].fillna(s2["raw_alias"].map(lambda x: f"UNMATCHED::{normalize_name(x)}"))
    s1_agg = s1.groupby(["enterprise_name", "year"], as_index=False)["sales_volume"].sum(min_count=1)
    s2_agg = s2.groupby(["enterprise_name", "year"], as_index=False)["nev_sales_volume"].sum(min_count=1)
    out = s1_agg.merge(s2_agg, on=["enterprise_name", "year"], how="outer")
    # prevent duplicate enterprise-year
    out = out.groupby(["enterprise_name", "year"], as_index=False).agg(
        sales_volume=("sales_volume", "sum"),
        nev_sales_volume=("nev_sales_volume", "sum"),
    )
    out["sales_volume"] = out["sales_volume"].round(0).astype("Int64")
    out["nev_sales_volume"] = out["nev_sales_volume"].round(0).astype("Int64")
    out.to_csv(CLEANED / "sales.csv", index=False, encoding="utf-8-sig")
    return out


def update_report(new_stats: str) -> None:
    if REPORT_PATH.exists():
        content = REPORT_PATH.read_text(encoding="utf-8")
    else:
        content = "# CLEANING_REPORT\n\n"
    section = "\n## 销售增强映射（第二轮）\n" + new_stats + "\n"
    if "## 销售增强映射（第二轮）" in content:
        content = re.sub(r"\n## 销售增强映射（第二轮）[\s\S]*$", section, content)
    else:
        content += section
    REPORT_PATH.write_text(content, encoding="utf-8")


def main() -> None:
    ent = load_enterprise_basic()
    unmatched_aliases = parse_unmatched_sales_aliases()
    # include aliases directly seen in raw sales files to avoid missing new names
    aliases_all = sorted(
        set(unmatched_aliases)
        | set(load_sales_raw_1()["raw_alias"].dropna().astype(str).tolist())
        | set(load_sales_raw_2()["raw_alias"].dropna().astype(str).tolist())
    )

    manual = build_manual_seed(aliases_all, ent)
    mapping = build_enhanced_alias_mapping(aliases_all, ent, manual)
    sales = rebuild_sales(mapping)

    matched = sales[~sales["enterprise_name"].astype(str).str.startswith("UNMATCHED::")]
    unmatched = sales[sales["enterprise_name"].astype(str).str.startswith("UNMATCHED::")]
    cover_cnt = matched["enterprise_name"].nunique()
    total_rows = len(sales)
    unmatched_alias_cnt = mapping[mapping["mapped_enterprise"].astype(str).str.startswith("UNMATCHED::")].shape[0]
    top30 = sorted(matched["enterprise_name"].dropna().unique())[:30]

    stats = (
        f"- sales.csv总行数: {total_rows}\n"
        f"- 覆盖企业数(非UNMATCHED): {cover_cnt}\n"
        f"- UNMATCHED企业-年份记录数: {len(unmatched)}\n"
        f"- 未匹配别名数: {unmatched_alias_cnt}\n"
        f"- 覆盖企业前30: {'、'.join(top30)}"
    )
    update_report(stats)
    print(stats)


if __name__ == "__main__":
    main()

