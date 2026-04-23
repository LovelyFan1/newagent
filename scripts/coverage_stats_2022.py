from __future__ import annotations

import asyncio
import os
from pathlib import Path

import asyncpg


ROOT = Path(__file__).resolve().parents[1]
REPORT_PATH = ROOT / "scripts" / "diagnose_missing_coverage_report.md"


def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    if v is not None and v.strip() != "":
        return v.strip()
    return default


def _normalize_pg_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn[len("postgresql+asyncpg://") :]
    return dsn


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("|" + "|".join(["---"] * len(headers)) + "|")
    for r in rows:
        out.append("| " + " | ".join(r) + " |")
    return "\n".join(out)


async def main() -> None:
    dsn = _env("DATABASE_URL") or "postgresql://app_v2:app_v2@127.0.0.1:5432/app_v2"
    dsn = _normalize_pg_dsn(dsn)
    conn = await asyncpg.connect(dsn=dsn)
    try:
        year = int(_env("COVERAGE_YEAR", "2022") or "2022")
        y = str(year)
        fin = await conn.fetch("select distinct enterprise_id from fact_financials where year=$1", y)
        sal = await conn.fetch("select distinct enterprise_id from fact_sales where year=$1", y)
        leg = await conn.fetch("select distinct enterprise_id from fact_legal where year=$1", y)
        fin_set = {str(r["enterprise_id"]) for r in fin}
        sal_set = {str(r["enterprise_id"]) for r in sal}
        leg_set = {str(r["enterprise_id"]) for r in leg}

        all_ents = await conn.fetch("select enterprise_id, stock_name from dim_enterprise")
        rows = []
        full = []
        for r in all_ents:
            eid = str(r["enterprise_id"])
            name = str(r["stock_name"])
            has_fin = "1" if eid in fin_set else "0"
            has_sal = "1" if eid in sal_set else "0"
            has_leg = "1" if eid in leg_set else "0"
            rows.append([name, has_fin, has_sal, has_leg])
            if has_fin == "1" and has_sal == "1" and has_leg == "1":
                full.append(name)

        fin_cnt = len(fin_set)
        sal_cnt = len(sal_set)
        leg_cnt = len(leg_set)
        full_cnt = len(full)

        section = []
        section.append(f"\n\n## 回填后：{year} 三领域覆盖统计\n")
        section.append(f"- 财务覆盖企业数（year={year}）：**{fin_cnt}**\n")
        section.append(f"- 销售覆盖企业数（year={year}）：**{sal_cnt}**\n")
        section.append(f"- 法律覆盖企业数（year={year}）：**{leg_cnt}**\n")
        section.append(f"- 三领域完全覆盖企业数（year={year}）：**{full_cnt}**\n")
        section.append(f"\n### 三领域完全覆盖企业名单（year={year}）\n")
        if full:
            for n in sorted(full):
                section.append(f"- {n}\n")
        else:
            section.append("（无）\n")

        section.append(f"\n### 覆盖明细（year={year}）\n")
        section.append(_md_table(["企业", "has_finance", "has_sales", "has_legal"], rows))
        text = "".join(section)

        # append to report
        old = REPORT_PATH.read_text(encoding="utf-8") if REPORT_PATH.exists() else ""
        REPORT_PATH.write_text(old + text, encoding="utf-8")
        print(text)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())

