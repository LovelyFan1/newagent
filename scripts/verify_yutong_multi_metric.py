"""Smoke: 宇通 2022 销量 + 多指标离线摘要."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
    s = line.strip()
    if not s or s.startswith("#") or "=" not in s:
        continue
    k, v = s.split("=", 1)
    os.environ[k.strip()] = v.strip()

from fastapi.testclient import TestClient
from app.main import app
from app.api.v1.routes import agent as agent_routes


def _off():
    o = agent_routes.orchestrator
    o.llm._enabled = False
    o.llm._client = None
    o.composer.llm._enabled = False
    o.composer.llm._client = None


def main() -> int:
    _off()
    with TestClient(app) as client:
        r1 = client.post(
            "/api/v1/agent/query",
            json={"question": "宇通客车2022年销量", "session_id": "yut1", "force": True},
        )
        d1 = r1.json().get("data") or {}
        s1 = str((d1.get("report") or {}).get("summary") or "")
        r2 = client.post(
            "/api/v1/agent/query",
            json={"question": "宇通客车2022年销量和净利润", "session_id": "yut2", "force": True},
        )
        d2 = r2.json().get("data") or {}
        s2 = str((d2.get("report") or {}).get("summary") or "")
    print("single:", s1[:200])
    print("multi:", s2[:400])
    ok1 = "37,268" in s1 or "37268" in s1.replace(",", "")
    ok2 = ("净利润" in s2 or "净利润=" in s2) and ("销量" in s2 or "销量=" in s2)
    if not ok1:
        print("FAIL single metric expected non-zero sales")
        return 1
    if not ok2:
        print("FAIL multi metric missing sales or net profit in summary")
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
