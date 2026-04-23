import sqlite3
from pathlib import Path

import pandas as pd


BASE = Path(r"C:\Users\0\Desktop\项目\app_v2.2")
DB = BASE / "test_local.db"
SALES_CSV = BASE / "data" / "cleaned" / "sales.csv"


def main() -> None:
    sales = pd.read_csv(SALES_CSV)
    sales = sales[~sales["enterprise_name"].astype(str).str.startswith("UNMATCHED::")].copy()
    sales["year"] = pd.to_numeric(sales["year"], errors="coerce").astype("Int64")
    sales = sales[sales["year"].notna()].copy()

    conn = sqlite3.connect(str(DB))
    cur = conn.cursor()
    dim = pd.read_sql_query("SELECT enterprise_id, stock_name FROM dim_enterprise", conn)
    dim["stock_name"] = dim["stock_name"].astype(str).str.strip()
    sales["enterprise_name"] = sales["enterprise_name"].astype(str).str.strip()

    merged = sales.merge(dim, left_on="enterprise_name", right_on="stock_name", how="left")
    mapped = merged[merged["enterprise_id"].notna()].copy()
    mapped["enterprise_id"] = mapped["enterprise_id"].astype(int)
    mapped = (
        mapped.groupby(["enterprise_id", "year"], as_index=False)
        .agg(
            total_sales_volume=("sales_volume", "sum"),
            nev_sales_volume=("nev_sales_volume", "sum"),
        )
    )
    mapped["total_production_volume"] = None
    mapped["nev_production_volume"] = None

    cur.execute("DELETE FROM fact_sales")
    insert_sql = """
        INSERT INTO fact_sales
        (enterprise_id, year, total_sales_volume, total_production_volume, nev_sales_volume, nev_production_volume)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    cur.executemany(
        insert_sql,
        [
            (
                int(r.enterprise_id),
                int(r.year),
                None if pd.isna(r.total_sales_volume) else float(r.total_sales_volume),
                None,
                None if pd.isna(r.nev_sales_volume) else float(r.nev_sales_volume),
                None,
            )
            for r in mapped.itertuples(index=False)
        ],
    )
    conn.commit()

    c1 = cur.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    c2 = cur.execute("SELECT COUNT(DISTINCT enterprise_id) FROM fact_sales WHERE year=2022").fetchone()[0]
    print(f"fact_sales重建完成: rows={c1}, enterprises_2022={c2}")
    conn.close()


if __name__ == "__main__":
    main()

