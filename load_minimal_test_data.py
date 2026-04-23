from __future__ import annotations

import csv
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "test_local.db"
DATA_DIR = ROOT / "data" / "cleaned"


def to_int_year(raw: str) -> int | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def to_float(raw: str):
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def main() -> None:
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS dim_enterprise (
            enterprise_id INTEGER PRIMARY KEY,
            stock_code TEXT,
            stock_name TEXT
        );

        CREATE TABLE IF NOT EXISTS fact_financials (
            enterprise_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            revenue REAL,
            net_profit REAL,
            total_assets REAL,
            total_liabilities REAL,
            current_ratio REAL,
            quick_ratio REAL,
            roe REAL,
            operating_cash_flow REAL
        );

        CREATE TABLE IF NOT EXISTS fact_sales (
            enterprise_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            total_sales_volume REAL,
            total_production_volume REAL,
            nev_sales_volume REAL,
            nev_production_volume REAL
        );

        CREATE TABLE IF NOT EXISTS fact_legal (
            enterprise_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            lawsuit_count REAL,
            lawsuit_total_amount REAL
        );

        CREATE TABLE IF NOT EXISTS core_metrics_summary (
            stock_code TEXT NOT NULL,
            enterprise_name TEXT NOT NULL,
            year INTEGER NOT NULL,
            revenue REAL,
            net_profit REAL,
            total_assets REAL,
            sales_volume REAL,
            roe REAL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (stock_code, year)
        );
        """
    )

    # 清空旧数据，确保重复执行幂等
    for table in ("dim_enterprise", "fact_financials", "fact_sales", "fact_legal", "core_metrics_summary"):
        cur.execute(f"DELETE FROM {table}")

    # 按需求写入固定 enterprise_id
    enterprises = [
        (59, "002594.SZ", "比亚迪"),
        (889, "LI", "理想汽车"),
    ]
    cur.executemany("INSERT INTO dim_enterprise (enterprise_id, stock_code, stock_name) VALUES (?, ?, ?)", enterprises)

    ent_by_name = {"比亚迪": 59, "理想汽车": 889}

    # financials: 仅导入 2020-2022 且属于比亚迪/理想汽车
    with (DATA_DIR / "financials.csv").open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            year = to_int_year(row.get("year"))
            name = (row.get("stock_name") or "").strip()
            if year not in (2020, 2021, 2022):
                continue
            if name not in ent_by_name:
                continue
            cur.execute(
                """
                INSERT INTO fact_financials
                (enterprise_id, year, revenue, net_profit, total_assets, total_liabilities, current_ratio, quick_ratio, roe, operating_cash_flow)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ent_by_name[name],
                    year,
                    to_float(row.get("revenue")),
                    to_float(row.get("net_profit")),
                    to_float(row.get("total_assets")),
                    to_float(row.get("total_liabilities")),
                    to_float(row.get("current_ratio")),
                    to_float(row.get("quick_ratio")),
                    to_float(row.get("roe")),
                    to_float(row.get("operating_cash_flow")),
                ),
            )

    # sales: stock_name 命中比亚迪/理想汽车
    with (DATA_DIR / "sales.csv").open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            year = to_int_year(row.get("year"))
            name = (row.get("stock_name") or "").strip()
            if year not in (2020, 2021, 2022):
                continue
            if name not in ent_by_name:
                continue
            cur.execute(
                """
                INSERT INTO fact_sales
                (enterprise_id, year, total_sales_volume, total_production_volume, nev_sales_volume, nev_production_volume)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    ent_by_name[name],
                    year,
                    to_float(row.get("total_sales_volume")),
                    to_float(row.get("total_production_volume")),
                    to_float(row.get("nev_sales_volume")),
                    to_float(row.get("nev_production_volume")),
                ),
            )

    # legal: stock_name 命中比亚迪/理想汽车（理想可能无记录，允许为空）
    with (DATA_DIR / "legal.csv").open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            year = to_int_year(row.get("year"))
            name = (row.get("stock_name") or "").strip()
            if year not in (2020, 2021, 2022):
                continue
            if name not in ent_by_name:
                continue
            cur.execute(
                """
                INSERT INTO fact_legal
                (enterprise_id, year, lawsuit_count, lawsuit_total_amount)
                VALUES (?, ?, ?, ?)
                """,
                (
                    ent_by_name[name],
                    year,
                    to_float(row.get("lawsuit_count")),
                    to_float(row.get("lawsuit_total_amount")),
                ),
            )

    # 预聚合核心指标，用于快速通道
    cur.executescript(
        """
        INSERT OR REPLACE INTO core_metrics_summary
        (stock_code, enterprise_name, year, revenue, net_profit, total_assets, sales_volume, roe, updated_at)
        SELECT
            de.stock_code,
            de.stock_name,
            ff.year,
            ff.revenue,
            ff.net_profit,
            ff.total_assets,
            fs.total_sales_volume,
            ff.roe,
            CURRENT_TIMESTAMP
        FROM dim_enterprise de
        LEFT JOIN fact_financials ff ON ff.enterprise_id = de.enterprise_id
        LEFT JOIN fact_sales fs ON fs.enterprise_id = de.enterprise_id AND fs.year = ff.year
        WHERE ff.year IN (2020, 2021, 2022);
        """
    )

    conn.commit()

    for table in ("dim_enterprise", "fact_financials", "fact_sales", "fact_legal", "core_metrics_summary"):
        cnt = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"{table}: {cnt}")

    conn.close()


if __name__ == "__main__":
    main()

