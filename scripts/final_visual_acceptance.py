from __future__ import annotations

import json
import re
import socket
import statistics
import time
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
APP_JS = ROOT / "web" / "js" / "app.js"
OUT_JSON = ROOT / "scripts" / "final_visual_acceptance_result.json"


def _unwrap(data: dict[str, Any]) -> dict[str, Any]:
    if isinstance(data.get("data"), dict):
        return data["data"]
    return data


def _post(port: int, question: str, session_id: str | None = None, timeout: int = 60) -> tuple[int, dict[str, Any], float]:
    payload: dict[str, Any] = {"question": question}
    if session_id:
        payload["session_id"] = session_id
    t0 = time.perf_counter()
    r = requests.post(f"http://127.0.0.1:{port}/api/v1/agent/query", json=payload, timeout=timeout)
    elapsed = time.perf_counter() - t0
    body = _unwrap(r.json())
    return r.status_code, body, elapsed


def _docs_ok(port: int) -> bool:
    try:
        return requests.get(f"http://127.0.0.1:{port}/docs", timeout=3).status_code == 200
    except Exception:
        return False


def run(port: int = 8002) -> dict[str, Any]:
    result: dict[str, Any] = {"port": port, "scenarios": []}
    scenarios = result["scenarios"]

    # 1 深度分析
    s, d, t = _post(port, "分析比亚迪2022年的综合风险")
    sum1 = ((d.get("report") or {}).get("summary") or "")
    c1 = d.get("charts") or {}
    ok = s == 200 and isinstance(c1.get("radar"), dict) and isinstance(c1.get("scatter"), dict) and ("报告摘要：" not in sum1)
    scenarios.append({"id": 1, "name": "三领域深度分析", "pass": ok, "status": s, "time_s": round(t, 3), "summary": sum1[:120]})

    # 2 多企业对比
    s, d, t = _post(port, "对比长城汽车、长安汽车、广汽集团的财务和销售表现")
    sum2 = ((d.get("report") or {}).get("summary") or "")
    c2 = d.get("charts") or {}
    ok = s == 200 and c2.get("chart_type") == "comparison_ranking" and isinstance(c2.get("ranking_bar"), dict) and isinstance(c2.get("radar"), dict) and bool(re.search(r"(第一|排名)", sum2))
    scenarios.append({"id": 2, "name": "多企业对比", "pass": ok, "status": s, "time_s": round(t, 3), "summary": sum2[:120]})

    # 3 趋势
    s, d, t = _post(port, "比亚迪近三年销量趋势")
    c3 = d.get("charts") or {}
    sum3 = ((d.get("report") or {}).get("summary") or "")
    ms3 = c3.get("metric_series") or {}
    ok = s == 200 and c3.get("chart_type") == "simple_metric" and ms3.get("type") == "line" and ("趋势" in sum3)
    scenarios.append({"id": 3, "name": "简单趋势查询", "pass": ok, "status": s, "time_s": round(t, 3), "summary": sum3[:120]})

    # 4 归因追问（先对比再为什么）
    s_base, d_base, _ = _post(port, "对比长城汽车、长安汽车、广汽集团的财务和销售表现")
    sid = d_base.get("session_id")
    s, d, t = _post(port, "为什么", session_id=sid, timeout=80)
    sum4 = ((d.get("report") or {}).get("summary") or "")
    ok = s_base == 200 and s == 200 and d.get("status") == "completed" and ("evidence_id" not in sum4.lower())
    scenarios.append({"id": 4, "name": "归因追问", "pass": ok, "status": s, "time_s": round(t, 3), "summary": sum4[:120]})

    # 5 缺时间澄清
    s, d, t = _post(port, "比亚迪的营收")
    ok = s == 200 and d.get("status") == "needs_clarification"
    scenarios.append({"id": 5, "name": "缺时间澄清", "pass": ok, "status": s, "time_s": round(t, 3), "detail": d.get("clarification")})

    # 6 舆情
    s, d, t = _post(port, "理想汽车的舆情怎么样")
    sum6 = ((d.get("report") or {}).get("summary") or "")
    c6 = d.get("charts") or {}
    ok = s == 200 and c6.get("chart_type") in {"sentiment", "general"} and ("推荐投资" not in sum6) and ("| 指标 |" not in sum6)
    scenarios.append({"id": 6, "name": "舆情查询", "pass": ok, "status": s, "time_s": round(t, 3), "summary": sum6[:120]})

    # 7 快速通道 + 性能
    times = []
    detail = []
    for _ in range(3):
        s, d, t = _post(port, "比亚迪2022年销量")
        times.append(t)
        detail.append({"status": s, "time_s": round(t, 3), "summary": ((d.get("report") or {}).get("summary") or "")[:80]})
    avg_fast = statistics.mean(times) if times else 999.0
    ok = all(x["status"] == 200 for x in detail) and avg_fast < 1.0
    scenarios.append({"id": 7, "name": "快速通道数值查询", "pass": ok, "avg_time_s": round(avg_fast, 3), "runs": detail})
    result["fast_avg_s"] = round(avg_fast, 3)

    # 8 评分接口
    t0 = time.perf_counter()
    r = requests.get(f"http://127.0.0.1:{port}/api/v1/scoring/比亚迪?year=2022", timeout=30)
    dt = time.perf_counter() - t0
    body = _unwrap(r.json())
    ok = r.status_code == 200 and isinstance(body.get("total_score"), (int, float)) and bool(body.get("rating")) and isinstance(body.get("dimension_scores"), dict)
    scenarios.append({"id": 8, "name": "评分接口", "pass": ok, "status": r.status_code, "time_s": round(dt, 3)})

    # 9 可视化自适应（静态前端逻辑检查）
    js = APP_JS.read_text(encoding="utf-8")
    ok = "if (cards.length === 1)" in js and "fullscreen-card" in js and "cards.forEach" in js
    scenarios.append({"id": 9, "name": "可视化自适应", "pass": ok, "detail": "static-js-check"})

    # 10 证据面板非空（通过分析响应 evidence）
    s, d, t = _post(port, "分析比亚迪2022年的综合风险")
    ev = d.get("evidence") or []
    ok = s == 200 and isinstance(ev, list) and len(ev) >= 1
    scenarios.append({"id": 10, "name": "证据面板非空", "pass": ok, "status": s, "time_s": round(t, 3), "evidence_count": len(ev)})

    # 11 前端页面
    t0 = time.perf_counter()
    r = requests.get(f"http://127.0.0.1:{port}", timeout=10)
    dt = time.perf_counter() - t0
    ok = r.status_code in {200, 307, 302}
    scenarios.append({"id": 11, "name": "前端页面", "pass": ok, "status": r.status_code, "time_s": round(dt, 3)})

    # 12 端口切换（检查 8003 可用性）
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 8003))
        ok = True
        detail = "8003 可绑定，可作为 8002 占用时的回退端口。"
    except OSError:
        ok = _docs_ok(8003)
        detail = "8003 已占用，但该端口已有服务可访问。"
    finally:
        try:
            sock.close()
        except Exception:
            pass
    scenarios.append({"id": 12, "name": "端口切换", "pass": ok, "detail": detail})

    result["passed"] = sum(1 for x in scenarios if x["pass"])
    result["failed"] = len(scenarios) - result["passed"]
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


if __name__ == "__main__":
    out = run(8002)
    print(json.dumps({"passed": out["passed"], "failed": out["failed"], "fast_avg_s": out.get("fast_avg_s")}, ensure_ascii=False))
