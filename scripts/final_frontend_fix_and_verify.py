"""
Final frontend cache-bust + API verification.

What it does:
1) Inject a unique time-based cache buster into web/index.html static assets:
   - app.js, api-client.js, (and any *.css link tags if present)
2) Headless verification via API:
   - register/login (idempotent)
   - POST /api/v1/agent/query with "比亚迪近三年的销量"
   - assert charts.chart_type == "simple_metric"
3) Print a manual verification checklist.
"""

from __future__ import annotations

import datetime as _dt
import json
import re
import sys
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[1]
WEB_INDEX = ROOT / "web" / "index.html"


def _ts_tag() -> str:
    # e.g. 20260423_153000
    return _dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def _inject_cache_buster(html: str, tag: str) -> str:
    """
    Add/replace ?t=... for:
      - ./js/app.js
      - ./js/api-client.js
      - any .css href
    Keep other query params intact when possible.
    """

    def rewrite_attr(value: str) -> str:
        if not value:
            return value
        # only touch local static assets
        targets = (
            "./js/app.js",
            "./js/api-client.js",
        )
        is_css = value.endswith(".css") or ".css?" in value
        is_target = any(value.startswith(t) for t in targets) or is_css
        if not is_target:
            return value

        # split path/query
        if "?" in value:
            path, qs = value.split("?", 1)
            # remove existing t=
            qs = re.sub(r"(?:^|&)(t=[^&]*)", "", qs).strip("&")
            if qs:
                return f"{path}?{qs}&t={tag}"
            return f"{path}?t={tag}"
        return f"{value}?t={tag}"

    # rewrite <script src="..."> and <link href="...">
    def sub_script(m: re.Match) -> str:
        before, val, after = m.group(1), m.group(2), m.group(3)
        return f'{before}{rewrite_attr(val)}{after}'

    html = re.sub(r'(<script[^>]+src=")([^"]+)(")', sub_script, html, flags=re.IGNORECASE)
    html = re.sub(r'(<link[^>]+href=")([^"]+)(")', sub_script, html, flags=re.IGNORECASE)
    return html


def _api_register_login(base: str, email: str, password: str) -> str:
    # register (idempotent)
    try:
        r = requests.post(
            f"{base}/api/v1/auth/register",
            json={"email": email, "password": password},
            timeout=15,
        )
        # allow "already exists" (usually non-200)
        _ = r.text
    except Exception:
        pass

    # login (form)
    form = {"username": email, "password": password}
    r2 = requests.post(f"{base}/api/v1/auth/login", data=form, timeout=20)
    r2.raise_for_status()
    payload = r2.json()
    if isinstance(payload, dict) and "data" in payload:
        payload = payload["data"]
    token = (payload or {}).get("access_token")
    if not token:
        raise RuntimeError(f"login_failed: {r2.text[:400]}")
    return token


def _api_query(base: str, token: str, question: str) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.post(
        f"{base}/api/v1/agent/query",
        json={"question": question, "force": True},
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    payload = r.json()
    if isinstance(payload, dict) and "data" in payload:
        return payload["data"]
    return payload


def main() -> int:
    tag = _ts_tag()
    if not WEB_INDEX.exists():
        print(json.dumps({"ok": False, "error": f"missing {str(WEB_INDEX)}"}, ensure_ascii=False))
        return 2

    raw = WEB_INDEX.read_text(encoding="utf-8")
    patched = _inject_cache_buster(raw, tag)
    if patched != raw:
        WEB_INDEX.write_text(patched, encoding="utf-8")

    base = "http://127.0.0.1:8000"
    email = "demo@example.com"
    password = "DemoPass123"

    try:
        token = _api_register_login(base, email, password)
        data = _api_query(base, token, "比亚迪近三年的销量")
        charts = data.get("charts") if isinstance(data, dict) else {}
        chart_type = (charts or {}).get("chart_type")
        ok = chart_type == "simple_metric"
        print(
            json.dumps(
                {
                    "ok": ok,
                    "cache_buster_tag": tag,
                    "chart_type": chart_type,
                    "hint": "PASS" if ok else "FAIL: chart_type != simple_metric",
                },
                ensure_ascii=False,
            )
        )
        if not ok:
            return 1
    except Exception as e:
        print(json.dumps({"ok": False, "cache_buster_tag": tag, "error": str(e)}, ensure_ascii=False))
        return 1

    print("\n=== 浏览器强制刷新核查清单 ===")
    print("1) 完全关闭所有 Chrome/Edge 窗口")
    print("2) 用无痕/隐私窗口打开: http://127.0.0.1:8000")
    print("3) 登录后，按 F12 打开控制台，执行：")
    print("   - typeof echarts")
    print("   - window.__LAST_CHARTS__")
    print("4) 发送：比亚迪近三年的销量")
    print("   预期：chart_type=simple_metric，左侧显示趋势图卡片（2020/2021/2022 三个点）")
    print(f"（本次已写入 cache buster：t={tag}）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

