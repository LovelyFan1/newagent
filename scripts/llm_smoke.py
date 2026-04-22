from __future__ import annotations

import json
import uuid

import requests


BASE = "http://127.0.0.1:8000"


def main() -> int:
    email = f"demo_{uuid.uuid4().hex[:6]}@example.com"
    pw = "DemoPass123"
    requests.post(f"{BASE}/api/v1/auth/register", json={"email": email, "password": pw}, timeout=30)
    r = requests.post(
        f"{BASE}/api/v1/auth/login",
        data={"username": email, "password": pw},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    token = r.json()["data"]["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    q = {"question": "比亚迪 2022 年财务风险分析", "session_id": f"smoke-{uuid.uuid4().hex[:6]}"}
    resp = requests.post(f"{BASE}/api/v1/agent/query", json=q, headers=headers, timeout=180)
    payload = resp.json().get("data", {})
    report = payload.get("report") or {}
    sections = report.get("sections") or {}
    role_outputs = sections.get("role_outputs") or []
    print("http=", resp.status_code)
    print("status=", payload.get("status"))
    print("summary=", (report.get("summary") or "")[:200])
    print("fallback=", "LLM 调用失败" in json.dumps(report, ensure_ascii=False))
    print("role_error=", role_outputs[0].get("error") if role_outputs else None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

