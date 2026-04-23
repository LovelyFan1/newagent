import sqlite3
from pathlib import Path


BASE = Path(r"C:\Users\0\Desktop\项目\app_v2.2")
DB = BASE / "test_local.db"
OUT = BASE / "data" / "core_25_enterprises.txt"

SQL = """
SELECT DISTINCT d.stock_name
FROM dim_enterprise d
JOIN fact_financials f
  ON f.enterprise_id = d.enterprise_id
 AND f.year = 2022
JOIN fact_sales s
  ON s.enterprise_id = d.enterprise_id
 AND s.year = 2022
JOIN fact_legal l
  ON l.enterprise_id = d.enterprise_id
 AND l.year = 2022
WHERE d.stock_name NOT LIKE '%UNMATCHED%'
ORDER BY d.stock_name
"""


def main() -> None:
    conn = sqlite3.connect(str(DB))
    cur = conn.cursor()
    names = [r[0] for r in cur.execute(SQL).fetchall()]
    conn.close()

    OUT.write_text("\n".join(names), encoding="utf-8")

    print(f"三领域完全覆盖的核心企业共有 {len(names)} 家")
    for n in names:
        print(n)
    print("\n推荐演示查询：")
    if len(names) >= 3:
        print(f"对比{names[0]}、{names[1]}、{names[2]}，谁更值得投资")
    if names:
        print(f"{names[0]}近三年的销量和净利润变化趋势")
    if len(names) >= 2:
        print(f"{names[0]}和{names[1]}在2022年的法律风险与偿债能力对比")


if __name__ == "__main__":
    main()

