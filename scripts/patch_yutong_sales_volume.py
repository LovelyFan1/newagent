"""Backfill total_sales_volume from nev when total is 0 for 宇通客车 (CSV + sqlite + core_metrics_summary)."""
from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = ROOT / "data" / "cleaned" / "sales.csv"
DB_PATH = ROOT / "test_local.db"


def patch_csv() -> int:
    if not CSV_PATH.is_file():
        return 0
    rows: list[list[str]] = []
    changed = 0
    with CSV_PATH.open(encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows.append(header)
        for row in reader:
            if len(row) < 4:
                rows.append(row)
                continue
            name, year, sv, nev = row[0], row[1], row[2], row[3]
            if name == "宇通客车":
                try:
                    sv_f = float(sv or 0)
                    nev_f = float(nev or 0)
                except ValueError:
                    rows.append(row)
                    continue
                if sv_f == 0 and nev_f > 0:
                    row[2] = str(int(nev_f))
                    changed += 1
            rows.append(row)
    with CSV_PATH.open("w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerows(rows)
    return changed


def patch_sqlite() -> tuple[int, int]:
    if not DB_PATH.is_file():
        return 0, 0
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE fact_sales
        SET total_sales_volume = nev_sales_volume
        WHERE (total_sales_volume IS NULL OR total_sales_volume = 0)
          AND nev_sales_volume IS NOT NULL
          AND nev_sales_volume > 0
          AND enterprise_id IN (
            SELECT enterprise_id FROM dim_enterprise WHERE stock_name = '宇通客车'
          )
        """
    )
    n_fs = cur.rowcount or 0
    cur.execute(
        """
        UPDATE core_metrics_summary
        SET sales_volume = (
          SELECT fs.nev_sales_volume
          FROM fact_sales fs
          INNER JOIN dim_enterprise de ON de.enterprise_id = fs.enterprise_id
          WHERE de.stock_name = '宇通客车'
            AND CAST(fs.year AS INTEGER) = core_metrics_summary.year
          LIMIT 1
        )
        WHERE enterprise_name = '宇通客车'
          AND (sales_volume IS NULL OR sales_volume = 0)
        """
    )
    n_cms = cur.rowcount or 0
    conn.commit()
    conn.close()
    return n_fs, n_cms


if __name__ == "__main__":
    c1 = patch_csv()
    c2, c3 = patch_sqlite()
    print(f"csv_rows_updated={c1} fact_sales_rows={c2} core_metrics_summary_rows={c3}")
