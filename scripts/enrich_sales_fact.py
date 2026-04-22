from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
CLEANED_DIR = ROOT / "data" / "cleaned"

SALES_CSV = CLEANED_DIR / "销售事实表.csv"
ENTERPRISE_CSV = CLEANED_DIR / "企业维度表.csv"
PRODUCT_CSV = CLEANED_DIR / "产品维度表.csv"
REGION_CSV = CLEANED_DIR / "区域维度表.csv"

OUT_CSV = CLEANED_DIR / "销售事实表_enriched.csv"


_SUFFIXES = (
    "股份有限公司",
    "有限责任公司",
    "有限公司",
    "股份公司",
    "集团股份有限公司",
    "集团有限公司",
    "集团",
    "公司",
)


def normalize_name(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", "", s)
    s = s.replace("（", "(").replace("）", ")")
    for suf in _SUFFIXES:
        if s.endswith(suf) and len(s) > len(suf):
            s = s[: -len(suf)]
            break
    # remove common punctuation
    s = re.sub(r"[·•，,。\.、\-\(\)（）]", "", s)
    return s.upper()


def build_enterprise_map(dim_enterprise: pd.DataFrame) -> dict[str, int]:
    m: dict[str, int] = {}

    def _add(key: str | None, eid: int):
        if not key:
            return
        nk = normalize_name(str(key))
        if not nk:
            return
        m.setdefault(nk, eid)

    for _, row in dim_enterprise.iterrows():
        eid = int(row["enterprise_id"])
        _add(row.get("standard_name"), eid)
        _add(row.get("stock_name"), eid)

    # minimal alias examples (can extend later)
    alias_pairs = [
        ("比亚迪股份公司", "比亚迪"),
        ("特斯拉汽车", "特斯拉"),
        ("上汽通用五菱汽车", "上汽通用五菱"),
    ]
    for a, b in alias_pairs:
        na, nb = normalize_name(a), normalize_name(b)
        if na in m and nb not in m:
            m[nb] = m[na]
        if nb in m and na not in m:
            m[na] = m[nb]

    return m


def build_keyword_alias_map(dim_enterprise: pd.DataFrame) -> dict[str, int]:
    """
    Best-effort alias mapping for common short names -> enterprise_id
    by substring matching against enterprise dimension.
    """

    keywords = ["比亚迪", "特斯拉", "蔚来", "小鹏", "理想", "长城", "长安", "广汽", "上汽通用五菱"]
    out: dict[str, int] = {}

    # ensure string columns exist
    dim = dim_enterprise.copy()
    dim["standard_name"] = dim.get("standard_name", "").astype(str)
    dim["stock_name"] = dim.get("stock_name", "").astype(str)
    dim["enterprise_id"] = dim.get("enterprise_id").astype(int)

    for kw in keywords:
        # prefer exact stock_name match
        exact = dim.loc[dim["stock_name"] == kw, "enterprise_id"]
        if not exact.empty:
            out[kw] = int(exact.iloc[0])
            continue
        hit = dim.loc[dim["standard_name"].str.contains(kw, na=False) | dim["stock_name"].str.contains(kw, na=False), "enterprise_id"]
        if not hit.empty:
            out[kw] = int(hit.iloc[0])

    return out


def pick_china_region_id(dim_region: pd.DataFrame) -> int:
    if "region_name" in dim_region.columns and "region_id" in dim_region.columns:
        hit = dim_region.loc[dim_region["region_name"].astype(str) == "中国", "region_id"]
        if not hit.empty:
            return int(hit.iloc[0])
    return 1


def main() -> None:
    for p in (SALES_CSV, ENTERPRISE_CSV, PRODUCT_CSV, REGION_CSV):
        if not p.exists():
            raise FileNotFoundError(str(p))

    sales = pd.read_csv(SALES_CSV, dtype=str)
    before_rows = len(sales)

    dim_enterprise = pd.read_csv(ENTERPRISE_CSV, dtype=str)
    dim_region = pd.read_csv(REGION_CSV, dtype=str)
    _ = pd.read_csv(PRODUCT_CSV, dtype=str)  # kept for future use; 车型列缺失时无法映射

    ent_map = build_enterprise_map(dim_enterprise)
    kw_alias = build_keyword_alias_map(dim_enterprise)
    china_region_id = pick_china_region_id(dim_region)

    if "车企" in sales.columns:
        ent_raw = sales["车企"].astype(str).fillna("").str.strip()
        ent_norm = ent_raw.map(normalize_name)
        eid = ent_norm.map(ent_map)
        # keyword-based fallback (handles short aliases)
        if kw_alias:
            for kw, kw_eid in kw_alias.items():
                mask = eid.isna() & ent_raw.str.contains(kw, na=False)
                if mask.any():
                    eid.loc[mask] = kw_eid

        # If still missing, try inferring from "maker columns" (wide-format sheets):
        # e.g. columns like "比亚迪/特斯拉/蔚来..." may contain sales values per row.
        maker_cols = [kw for kw in kw_alias.keys() if kw in sales.columns]
        if maker_cols:
            maker_vals = sales[maker_cols].fillna("").astype(str).apply(lambda c: c.str.strip())
            non_empty = maker_vals.ne("")
            one_hot = non_empty.sum(axis=1) == 1
            if one_hot.any():
                # pick the first non-empty maker column name per row
                picked = non_empty[one_hot].idxmax(axis=1)
                for kw, kw_eid in kw_alias.items():
                    mask = eid.isna() & one_hot & (picked == kw)
                    if mask.any():
                        eid.loc[mask] = kw_eid

        sales["enterprise_id"] = eid.fillna(-1).astype(int)
        matched = int((sales["enterprise_id"] != -1).sum())
        match_rate = matched / max(before_rows, 1) * 100
        print(f"企业匹配率: {match_rate:.2f}% ({matched}/{before_rows})")
    else:
        sales["enterprise_id"] = -1
        print("企业匹配率: 0.00% (销售事实表无 `车企` 列)")

    # product enrichment
    if "车型" in sales.columns:
        # placeholder: if the column exists in future, map with dim_product.product_name
        dim_product = pd.read_csv(PRODUCT_CSV, dtype=str)
        prod_map = {
            normalize_name(str(n)): int(pid)
            for n, pid in zip(dim_product["product_name"].astype(str), dim_product["product_id"].astype(int), strict=False)
            if str(n).strip()
        }
        prod_norm = sales["车型"].astype(str).map(normalize_name)
        sales["product_id"] = prod_norm.map(prod_map).fillna(-1).astype(int)
        matched = int((sales["product_id"] != -1).sum())
        match_rate = matched / max(before_rows, 1) * 100
        print(f"产品匹配率: {match_rate:.2f}% ({matched}/{before_rows})")
    else:
        sales["product_id"] = -1
        print("产品匹配率: N/A (销售事实表无 `车型` 列，已全部置为 -1)")

    if "region_id" not in sales.columns:
        sales["region_id"] = china_region_id
        print(f"region_id 缺失，默认填充为: {china_region_id}")

    after_rows = len(sales)
    print(f"行数: {before_rows} -> {after_rows}")

    sales.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"已输出: {OUT_CSV}")


if __name__ == "__main__":
    main()

