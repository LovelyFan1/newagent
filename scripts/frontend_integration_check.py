from __future__ import annotations

import json
import time
import uuid

import requests


BASE = "http://127.0.0.1:8000"


def main() -> int:
    out: dict[str, object] = {"base": BASE, "steps": []}
    username = f"demo_{uuid.uuid4().hex[:8]}"
    email = f"{username}@example.com"
    password = "DemoPass123"
    token = ""

    def add(name: str, ok: bool, detail: str):
        out["steps"].append({"name": name, "ok": ok, "detail": detail})

    s = requests.Session()
    t0 = time.time()
    for path in ["/web/login.html", "/web/index.html"]:
        r = s.get(BASE + path, timeout=15)
        add(f"static:{path}", r.status_code == 200, f"status={r.status_code}")

    r = s.post(BASE + "/api/v1/auth/register", json={"email": email, "password": password}, timeout=20)
    ok = r.status_code == 200 and r.json().get("code") == 0
    add("register", ok, f"status={r.status_code}")

    r = s.post(
        BASE + "/api/v1/auth/login",
        data={"username": email, "password": password},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=20,
    )
    jd = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    token = ((jd.get("data") or {}).get("access_token") or "")
    add("login", r.status_code == 200 and bool(token), f"status={r.status_code}")

    auth = {"Authorization": f"Bearer {token}"}
    r = s.get(BASE + "/api/v1/auth/me", headers=auth, timeout=20)
    add("me", r.status_code == 200 and r.json().get("code") == 0, f"status={r.status_code}")

    r = s.get(BASE + "/api/v1/scoring/比亚迪", params={"year": 2022}, headers=auth, timeout=60)
    add("scoring", r.status_code == 200 and r.json().get("code") == 0, f"status={r.status_code}")

    r = s.post(
        BASE + "/api/v1/agent/query",
        json={"question": "分析比亚迪 2022 年财务风险", "session_id": f"s-{uuid.uuid4().hex[:6]}"},
        headers=auth,
        timeout=60,
    )
    jd = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    status = ((jd.get("data") or {}).get("status") or "")
    add("agent_analysis", r.status_code == 200 and status == "completed", f"http={r.status_code},status={status}")

    r = s.post(
        BASE + "/api/v1/agent/query",
        json={"question": "比亚迪的营收", "session_id": f"s-{uuid.uuid4().hex[:6]}"},
        headers=auth,
        timeout=60,
    )
    jd = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    status = ((jd.get("data") or {}).get("status") or "")
    add("agent_clarification", r.status_code == 200 and status == "needs_clarification", f"http={r.status_code},status={status}")

    out["elapsed_s"] = round(time.time() - t0, 3)
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

