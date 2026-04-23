import asyncio
import time
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.main import app


def _unwrap_response(payload: dict) -> dict:
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    return data if isinstance(data, dict) else payload


async def test():
    load_dotenv(".env")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test", timeout=60.0) as client:
        t0 = time.time()
        r = await client.post(
            "/api/v1/agent/query",
            json={"question": "分析比亚迪2022年的综合风险", "session_id": "timing_test"},
        )
        elapsed = time.time() - t0

        raw = r.json()
        data = _unwrap_response(raw)
        report = data.get("report", {}) if isinstance(data, dict) else {}
        summary = report.get("summary", "") if isinstance(report, dict) else ""

        print(f"总耗时: {elapsed:.2f}s")
        print(f"HTTP状态: {r.status_code}")
        print(f"Summary长度: {len(summary)} 字符")
        print(f"Summary预览: {summary[:200]}...")

        # 合规检查
        has_number = any(char.isdigit() for char in summary)
        has_metric_words = any(word in summary for word in ["流动比率", "得分", "evidence_id", "ROE", "净利润"])
        print(f"包含数字: {has_number}")
        print(f"包含指标词: {has_metric_words}")
        print(f"合规状态: {'通过' if not has_metric_words else '需改进'}")


if __name__ == "__main__":
    asyncio.run(test())
