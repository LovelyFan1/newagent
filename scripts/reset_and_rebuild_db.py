import sqlite3
from pathlib import Path

import pandas as pd


BASE = Path(r"C:\Users\0\Desktop\项目\app_v2.2")
DB = BASE / "test_local.db"
CLEANED = BASE / "data" / "cleaned"


def to_num(v):
    if pd.isna(v):
        return None
    return float(v)


def main() -> None:
    enterprise_basic = pd.read_csv(CLEANED / "enterprise_basic.csv")
    financials = pd.read_csv(CLEANED / "financials.csv")
    sales = pd.read_csv(CLEANED / "sales.csv")
    legal = pd.read_csv(CLEANED / "legal.csv")

    conn = sqlite3.connect(str(DB))
    cur = conn.cursor()

    # 1) clear old data
    cur.execute("DELETE FROM fact_financials;")
    cur.execute("DELETE FROM fact_sales;")
    cur.execute("DELETE FROM fact_legal;")
    cur.execute("DELETE FROM dim_enterprise;")
    conn.commit()

    # 2) rebuild dim_enterprise from enterprise_basic
    dim_rows = []
    for i, r in enumerate(enterprise_basic.itertuples(index=False), start=1):
        stock_code = str(r.security_code).strip() if pd.notna(r.security_code) else ""
        stock_name = str(r.security_short_name).strip() if pd.notna(r.security_short_name) else ""
        if not stock_name:
            continue
        dim_rows.append((i, stock_code, stock_name))

    cur.executemany(
        "INSERT INTO dim_enterprise (enterprise_id, stock_code, stock_name) VALUES (?, ?, ?)",
        dim_rows,
    )
    conn.commit()

    dim = pd.read_sql_query("SELECT enterprise_id, stock_code, stock_name FROM dim_enterprise", conn)
    by_code = {str(r.stock_code).strip(): int(r.enterprise_id) for r in dim.itertuples(index=False)}
    by_name = {str(r.stock_name).strip(): int(r.enterprise_id) for r in dim.itertuples(index=False)}

    # 3) import financials with unified enterprise_id
    fin_rows = []
    for r in financials.itertuples(index=False):
        eid = by_code.get(str(r.security_code).strip()) or by_name.get(str(r.enterprise_name).strip())
        if not eid:
            continue
        fin_rows.append(
            (
                eid,
                int(r.year),
                to_num(r.revenue),
                to_num(r.net_profit),
                to_num(r.total_assets),
                to_num(r.total_liabilities),
                to_num(r.current_ratio),
                to_num(r.quick_ratio),
                to_num(r.roe),
                None,  # operating_cash_flow not provided in cleaned CSV
            )
        )
    cur.executemany(
        """
        INSERT INTO fact_financials
        (enterprise_id, year, revenue, net_profit, total_assets, total_liabilities, current_ratio, quick_ratio, roe, operating_cash_flow)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        fin_rows,
    )

    # 4) import sales with unified enterprise_id
    sales_rows = []
    for r in sales.itertuples(index=False):
        name = str(r.enterprise_name).strip()
        if name.startswith("UNMATCHED::"):
            continue
        eid = by_name.get(name)
        if not eid:
            continue
        sales_rows.append(
            (
                eid,
                int(r.year),
                to_num(r.sales_volume),
                None,
                to_num(r.nev_sales_volume),
                None,
            )
        )
    # aggregate duplicate enterprise-year before insert
    if sales_rows:
        sdf = pd.DataFrame(
            sales_rows,
            columns=[
                "enterprise_id",
                "year",
                "total_sales_volume",
                "total_production_volume",
                "nev_sales_volume",
                "nev_production_volume",
            ],
        )
        sdf = (
            sdf.groupby(["enterprise_id", "year"], as_index=False)
            .agg(
                total_sales_volume=("total_sales_volume", "sum"),
                total_production_volume=("total_production_volume", "first"),
                nev_sales_volume=("nev_sales_volume", "sum"),
                nev_production_volume=("nev_production_volume", "first"),
            )
        )
        cur.executemany(
            """
            INSERT INTO fact_sales
            (enterprise_id, year, total_sales_volume, total_production_volume, nev_sales_volume, nev_production_volume)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [tuple(x) for x in sdf.itertuples(index=False, name=None)],
        )

    # 5) import legal with unified enterprise_id
    legal_rows = []
    for r in legal.itertuples(index=False):
        eid = by_code.get(str(r.security_code).strip()) or by_name.get(str(r.enterprise_name).strip())
        if not eid:
            continue
        legal_rows.append(
            (
                eid,
                int(r.year),
                int(r.lawsuit_count) if pd.notna(r.lawsuit_count) else None,
                to_num(r.lawsuit_total_amount),
            )
        )
    cur.executemany(
        """
        INSERT INTO fact_legal (enterprise_id, year, lawsuit_count, lawsuit_total_amount)
        VALUES (?, ?, ?, ?)
        """,
        legal_rows,
    )
    conn.commit()

    print("rebuild complete")
    print("dim_enterprise:", cur.execute("SELECT COUNT(*) FROM dim_enterprise").fetchone()[0])
    print("fact_financials:", cur.execute("SELECT COUNT(*) FROM fact_financials").fetchone()[0])
    print("fact_sales:", cur.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0])
    print("fact_legal:", cur.execute("SELECT COUNT(*) FROM fact_legal").fetchone()[0])
    conn.close()


if __name__ == "__main__":
    main()

