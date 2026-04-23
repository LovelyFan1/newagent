import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.indicator_calc import calculate_indicators


def load_env() -> None:
    env_path = ROOT / ".env"
    for line in env_path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        os.environ[k.strip()] = v.strip()


async def run_one(code: str, year: int):
    return await asyncio.wait_for(calculate_indicators(code, year), timeout=20)


async def main() -> None:
    load_env()
    byd = await run_one("比亚迪", 2022)
    gw = await run_one("长城汽车", 2022)
    for name, payload in [("BYD", byd), ("GWM", gw)]:
        inds = payload.get("indicators", {})
        print(
            name,
            payload.get("enterprise_name"),
            inds.get("financial_health", {}).get("revenue"),
            inds.get("industry_position", {}).get("sales_volume"),
            inds.get("legal_risk", {}).get("lawsuit_count"),
            inds.get("legal_risk", {}).get("lawsuit_total_amount"),
        )


if __name__ == "__main__":
    asyncio.run(main())

