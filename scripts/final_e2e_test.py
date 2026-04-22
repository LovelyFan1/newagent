from __future__ import annotations

import json
import random
import time
import uuid

import requests


BASE = "http://127.0.0.1:8000"


def ok_wrap(resp_json: dict) -> bool:
    return isinstance(resp_json, dict) and resp_json.get("code") == 0


def must(cond: bool, msg: str):
    if not cond:
        raise AssertionError(msg)


def main() -> int:
    started = time.time()
    report: dict[str, object] = {"base": BASE, "started_at": time.strftime("%Y-%m-%d %H:%M:%S"), "cases": []}

    def record(name: str, ok: bool, detail: str):
        report["cases"].append({"name": name, "ok": ok, "detail": detail})

    s = requests.Session()
    # wait for server ready (avoid rebuild race)
    for _ in range(40):
        try:
            r0 = s.get(BASE + "/", allow_redirects=False, timeout=2)
            if r0.status_code in (200, 307, 404):
                break
        except Exception:
            time.sleep(0.5)
    # 0) static
    for path in ["/", "/web/login.html", "/web/index.html", "/web/js/app.js", "/web/vendor/echarts.min.js"]:
        r = s.get(BASE + path, allow_redirects=False, timeout=20)
        record(f"static:{path}", r.status_code in (200, 307), f"status={r.status_code}")

    # 1) register/login
    email = f"demo_{uuid.uuid4().hex[:8]}@example.com"
    password = "DemoPass123"
    r = s.post(BASE + "/api/v1/auth/register", json={"email": email, "password": password}, timeout=30)
    j = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    record("register", r.status_code == 200 and ok_wrap(j), f"status={r.status_code}")
    must(r.status_code == 200 and ok_wrap(j), f"register failed: {r.status_code} {r.text[:200]}")

    r = s.post(
        BASE + "/api/v1/auth/login",
        data={"username": email, "password": password},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    j = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    token = ((j.get("data") or {}).get("access_token") or "")
    record("login", r.status_code == 200 and bool(token), f"status={r.status_code}")
    must(token, f"login failed: {r.status_code} {r.text[:200]}")

    headers = {"Authorization": f"Bearer {token}"}
    r = s.get(BASE + "/api/v1/auth/me", headers=headers, timeout=30)
    j = r.json()
    record("me", r.status_code == 200 and ok_wrap(j), f"status={r.status_code}")

    # 2) scoring
    r = s.get(BASE + "/api/v1/scoring/比亚迪", params={"year": 2022}, headers=headers, timeout=90)
    j = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    record("scoring", r.status_code == 200 and ok_wrap(j), f"status={r.status_code}")
    must(r.status_code == 200 and ok_wrap(j), f"scoring failed: {r.status_code} {r.text[:200]}")

    # 3) agent intents
    questions = [
        ("compare", "对比比亚迪和长城汽车 2022 年的盈利能力"),
        ("investment", "比亚迪 2022 年是否值得投资？"),
        ("legal", "分析比亚迪 2022 年司法诉讼风险"),
        ("sentiment", "比亚迪 2022 年舆情风险与市场情绪趋势"),
        ("clarification", "比亚迪的营收"),
        ("analysis", "比亚迪2022年盈利能力下降原因"),
    ]
    for tag, q in questions:
        body = {"question": q, "session_id": f"e2e-{tag}-{uuid.uuid4().hex[:6]}"}
        r = s.post(BASE + "/api/v1/agent/query", json=body, headers=headers, timeout=120)
        j = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        data = (j.get("data") or {}) if isinstance(j, dict) else {}
        status = data.get("status")
        ok = r.status_code == 200 and ok_wrap(j) and status in ("completed", "needs_clarification")
        record(f"agent:{tag}", ok, f"http={r.status_code},status={status}")
        must(ok, f"agent {tag} failed: {r.status_code} {r.text[:200]}")

        if tag == "clarification":
            must(status == "needs_clarification", "clarification should return needs_clarification")
        else:
            must(status == "completed", f"{tag} should be completed")
            report_obj = data.get("report") or {}
            # evidence trail
            must("evidence_trail" in report_obj, "report.evidence_trail missing")
            trail = report_obj.get("evidence_trail") or []
            must(isinstance(trail, list), "evidence_trail must be list")
            # attributions structure
            sections = report_obj.get("sections") or {}
            atts = sections.get("attributions") or []
            must(isinstance(atts, list) and len(atts) >= 1, "attributions missing/empty")
            a0 = atts[0]
            must(isinstance(a0, dict), "attributions[0] must be object")
            must(isinstance(a0.get("observation"), str) and len(a0["observation"]) > 0, "observation missing")
            causes = a0.get("causes")
            must(isinstance(causes, list) and len(causes) >= 2, "causes must have >=2 items")
            eids = a0.get("evidence_ids")
            must(isinstance(eids, list) and len(eids) >= 2, "evidence_ids must have >=2 items")
            trail_ids = {t.get("evidence_id") for t in trail if isinstance(t, dict)}
            must(set(eids).issubset(trail_ids), "attribution evidence_ids not found in evidence_trail")

    report["elapsed_s"] = round(time.time() - started, 3)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

