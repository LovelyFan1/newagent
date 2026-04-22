from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "cleaned"


FILE_TO_TABLE: dict[str, str] = {
    "时间维度表.csv": "dim_time",
    "企业维度表.csv": "dim_enterprise",
    "产品维度表.csv": "dim_product",
    "区域维度表.csv": "dim_region",
    "财务事实表.csv": "fact_financials",
    "fact_sales_rebuilt.csv": "fact_sales",
    "法律事实表.csv": "fact_legal",
    "充电桩事实表.csv": "fact_charging_piles",
    "专利事实表.csv": "fact_patents",
    "字段映射表.csv": "field_mapping",
}

DEDUP_KEY_MAP: dict[str, list[str]] = {
    # requested unique key; will be used only if columns exist in the CSV
    "fact_sales": ["enterprise_id", "year"],
}


def _pick_dedup_subset(columns: list[str]) -> list[str]:
    cols = set(columns)
    if "id" in cols:
        return ["id"]
    if "enterprise_id" in cols and "time_id" in cols:
        return ["enterprise_id", "time_id"]
    if "enterprise_id" in cols and "year" in cols:
        return ["enterprise_id", "year"]
    if "enterprise_id" in cols:
        return ["enterprise_id"]
    if "time_id" in cols:
        return ["time_id"]
    return columns


def _dedup_subset_for_table(table_name: str, columns: list[str]) -> list[str]:
    cols = set(columns)
    preferred = DEDUP_KEY_MAP.get(table_name)
    if preferred:
        if all(c in cols for c in preferred):
            return preferred
        # fallback for current cleaned fact_sales schema (no enterprise_id/product_id/region_id columns)
        if table_name == "fact_sales":
            if "sales_record_id" in cols and "time_id" in cols:
                return ["sales_record_id", "time_id"]
            if "sales_record_id" in cols:
                return ["sales_record_id"]
            # avoid over-dedup as a last resort
            return columns
    return _pick_dedup_subset(columns)


def _build_table(table_name: str, df: pd.DataFrame, metadata: sa.MetaData) -> sa.Table:
    columns: list[sa.Column[Any]] = []
    for col in df.columns:
        # To avoid dtype inference issues across heterogeneous CSVs,
        # we store everything as TEXT for this import step.
        columns.append(sa.Column(col, sa.Text(), nullable=True))
    return sa.Table(table_name, metadata, *columns)


async def _replace_and_load(engine: AsyncEngine, table_name: str, df: pd.DataFrame) -> dict[str, Any]:
    # normalize nulls (empty string / NA-like -> NULL)
    df = df.copy()
    df = df.replace({pd.NA: None})
    df = df.where(pd.notna(df), None)
    df = df.replace({"": None})

    subset = _dedup_subset_for_table(table_name, list(df.columns))
    if table_name == "fact_sales":
        for k in ("enterprise_id", "product_id", "region_id"):
            if k in df.columns:
                df[k] = df[k].fillna("-1")
    before = len(df)
    df = df.drop_duplicates(subset=subset, keep="first")
    after = len(df)

    metadata = sa.MetaData()
    table = _build_table(table_name, df, metadata)

    async with engine.begin() as conn:
        await conn.execute(sa.text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
        await conn.run_sync(metadata.create_all)

        if after:
            records = df.to_dict(orient="records")
            chunk_size = 5000
            for i in range(0, len(records), chunk_size):
                await conn.execute(sa.insert(table), records[i : i + chunk_size])

    return {
        "table": table_name,
        "rows_before_dedup": before,
        "rows_after_dedup": after,
        "dedup_subset": subset,
    }


async def main() -> None:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL env var is required")

    file_list = list(FILE_TO_TABLE.keys())
    missing = [fn for fn in file_list if not (DATA_DIR / fn).exists()]
    if missing:
        raise FileNotFoundError(f"Missing CSV files under {DATA_DIR}: {missing}")

    only_table = None
    if len(sys.argv) >= 3 and sys.argv[1] == "--only":
        only_table = sys.argv[2]

    engine = create_async_engine(database_url, pool_pre_ping=True)
    try:
        stats: list[dict[str, Any]] = []
        for filename in file_list:
            table_name = FILE_TO_TABLE[filename]
            if only_table and table_name != only_table:
                continue
            path = DATA_DIR / filename
            # Read as strings to keep schema stable and avoid NaN float leakage.
            df = pd.read_csv(path, dtype=str)
            df.columns = [str(c).strip() for c in df.columns]
            s = await _replace_and_load(engine, table_name, df)
            s["file"] = str(path)
            stats.append(s)
            print(f"[OK] {filename} -> {table_name}: {s['rows_after_dedup']}/{s['rows_before_dedup']} rows (dedup={s['dedup_subset']})")

        # validation queries
        validate_tables = ["dim_enterprise", "fact_financials", "fact_sales", "fact_legal"]
        async with engine.connect() as conn:
            for t in validate_tables:
                if only_table and t != only_table:
                    continue
                res = await conn.execute(sa.text(f'SELECT COUNT(*) AS cnt FROM "{t}"'))
                cnt = res.scalar_one()
                print(f"[COUNT] {t}: {cnt}")

        print("\n=== Import summary ===")
        for s in stats:
            print(
                f"- {s['table']}: {s['rows_after_dedup']} rows (from {s['rows_before_dedup']}), dedup={s['dedup_subset']}"
            )
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())

