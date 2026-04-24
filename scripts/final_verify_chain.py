from __future__ import annotations

import json
import re
import time
from pathlib import Path
from statistics import mean
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[1]
REPORT_PATH = ROOT / "FINAL_VERIFICATION_REPORT.md"
JS_PATH = ROOT / "web" / "js" / "app.js"


def unwrap(resp_json: dict[str, Any]) -> dict[str, Any]:
    if isinstance(resp_json, dict) and isinstance(resp_json.get("data"), dict):
        return resp_json["data"]
    return resp_json


def post_query(port: int, question: str, session_id: str | None = None) -> tuple[dict[str, Any], float, int]:
    payload = {"question": question}
    if session_id:
        payload["session_id"] = session_id
    t0 = time.perf_counter()
    r = requests.post(f"http://127.0.0.1:{port}/api/v1/agent/query", json=payload, timeout=60)
    elapsed = time.perf_counter() - t0
    body = {}
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text}
    return unwrap(body), elapsed, r.status_code


def has_banned_prefix(text: str) -> bool:
    banned = ["摘要：", "报告摘要：", "归因依据：", "证据：", "已走快速通道", "LLM未返回有效内容"]
    return any(x in (text or "") for x in banned)


def check_docs_port() -> tuple[int | None, list[str]]:
    logs: list[str] = []
    for p in (8000, 8001, 8002):
        try:
            r = requests.get(f"http://127.0.0.1:{p}/docs", timeout=3)
            logs.append(f"- 端口 {p}: /docs -> {r.status_code}")
            if r.status_code == 200:
                return p, logs
        except Exception as exc:
            logs.append(f"- 端口 {p}: /docs 失败 ({type(exc).__name__})")
    return None, logs


def main() -> None:
    port, port_logs = check_docs_port()
    if port is None:
        REPORT_PATH.write_text(
            "# FINAL_VERIFICATION_REPORT\n\n- 失败：8000/8001/8002 均无法访问 /docs，无法继续自动验收。\n",
            encoding="utf-8",
        )
        print("NO_PORT")
        return

    results: list[tuple[str, bool, str]] = []
    perf_fast: list[float] = []
    perf_deep: list[float] = []

    # 2.1 深度分析
    q1, t1, s1 = post_query(port, "分析比亚迪2022年的综合风险")
    perf_deep.append(t1)
    sum1 = ((q1.get("report") or {}).get("summary") or "").strip()
    charts1 = q1.get("charts") or {}
    ev1 = q1.get("evidence") or []
    ok_21 = (
        s1 == 200
        and (not has_banned_prefix(sum1))
        and isinstance(charts1.get("radar"), dict)
        and isinstance(charts1.get("scatter"), dict)
        and isinstance(ev1, list)
        and len(ev1) >= 1
    )
    results.append(("2.1 三领域深度分析", ok_21, f"status={s1}, t={t1:.2f}s, chart_type={charts1.get('chart_type')}"))

    # 2.2 多企业对比
    q2, t2, s2 = post_query(port, "对比长城汽车、长安汽车、广汽集团的财务和销售表现")
    perf_deep.append(t2)
    sum2 = ((q2.get("report") or {}).get("summary") or "").strip()
    charts2 = q2.get("charts") or {}
    ev2 = q2.get("evidence") or []
    rank_ok = bool(re.search(r"(第一|排名)", sum2))
    ok_22 = (
        s2 == 200
        and rank_ok
        and charts2.get("chart_type") == "comparison_ranking"
        and isinstance(charts2.get("ranking_bar"), dict)
        and isinstance(charts2.get("radar"), dict)
        and len(ev2) >= 1
    )
    results.append(("2.2 多企业对比", ok_22, f"status={s2}, t={t2:.2f}s"))

    # 2.3 简单趋势
    q3, t3, s3 = post_query(port, "比亚迪近三年销量趋势")
    perf_fast.append(t3)
    sum3 = ((q3.get("report") or {}).get("summary") or "").strip()
    charts3 = q3.get("charts") or {}
    ms3 = charts3.get("metric_series") or {}
    mode3 = ((q3.get("report") or {}).get("sections") or {}).get("mode")
    ok_23 = (
        s3 == 200
        and ("趋势" in sum3)
        and charts3.get("chart_type") == "simple_metric"
        and ms3.get("type") == "line"
        and mode3 == "simple_metric_fast_path"
    )
    results.append(("2.3 简单趋势查询", ok_23, f"status={s3}, t={t3:.2f}s, type={ms3.get('type')}"))

    # 2.4 原因追问 + 2.8 上下文
    first, _, _ = post_query(port, "比亚迪近三年销量趋势")
    sid = first.get("session_id")
    q4, t4, s4 = post_query(port, "为什么", session_id=sid)
    perf_deep.append(t4)
    sum4 = ((q4.get("report") or {}).get("summary") or "").strip()
    ok_24 = (
        s4 == 200
        and q4.get("status") != "needs_clarification"
        and len(sum4) >= 10
        and ("evidence_id" not in sum4.lower())
    )
    results.append(("2.4 原因追问", ok_24, f"status={s4}, t={t4:.2f}s, session={bool(sid)}"))
    results.append(("2.8 上下文记忆", ok_24, "与2.4同轮验证"))

    # 2.5 缺时间澄清
    q5, t5, s5 = post_query(port, "比亚迪的营收")
    perf_fast.append(t5)
    ok_25 = s5 == 200 and q5.get("status") == "needs_clarification"
    results.append(("2.5 缺时间澄清", ok_25, f"status={s5}, t={t5:.2f}s, api_status={q5.get('status')}"))

    # 2.6 舆情
    q6, t6, s6 = post_query(port, "理想汽车的舆情怎么样")
    perf_deep.append(t6)
    sum6 = ((q6.get("report") or {}).get("summary") or "").strip()
    c6 = (q6.get("charts") or {}).get("chart_type")
    ok_26 = s6 == 200 and c6 in {"sentiment", "general"} and (not re.search(r"(推荐投资|排名第一|对比表)", sum6))
    results.append(("2.6 舆情查询", ok_26, f"status={s6}, t={t6:.2f}s, chart_type={c6}"))

    # 2.7 快速通道数值
    q7, t7, s7 = post_query(port, "比亚迪2022年销量")
    perf_fast.append(t7)
    sum7 = ((q7.get("report") or {}).get("summary") or "").strip()
    c7 = q7.get("charts") or {}
    ms7 = c7.get("metric_series") or {}
    ok_27 = (
        s7 == 200
        and ("1,305,447" in sum7 or "1305447" in sum7)
        and c7.get("chart_type") == "simple_metric"
        and ms7.get("type") == "bar"
        and t7 < 1.0
    )
    results.append(("2.7 快速通道数值查询", ok_27, f"status={s7}, t={t7:.2f}s"))

    # 2.9 可视化自适应（静态代码保障）
    js = JS_PATH.read_text(encoding="utf-8")
    ok_29 = ("fullscreen-card" in js) and ("if (cards.length === 1)" in js) and ("cards.forEach" in js)
    results.append(("2.9 可视化自适应", ok_29, "通过前端逻辑静态检查"))

    # 2.10 证据面板与归因关联
    sec1 = ((q1.get("report") or {}).get("sections") or {})
    atts = sec1.get("attributions") if isinstance(sec1.get("attributions"), list) else []
    ids = {str((e or {}).get("evidence_id")) for e in ev1 if isinstance(e, dict)}
    trace_ok = False
    for a in atts:
        if isinstance(a, dict):
            eids = a.get("evidence_ids") if isinstance(a.get("evidence_ids"), list) else []
            if any(str(x) in ids for x in eids):
                trace_ok = True
                break
    ok_210 = len(ev1) >= 1 and trace_ok
    results.append(("2.10 证据面板与归因关联", ok_210, f"evidence={len(ev1)}, trace={trace_ok}"))

    # 3 性能
    fast_samples = []
    for _ in range(3):
        _, t, _ = post_query(port, "比亚迪2022年销量")
        fast_samples.append(t)
    deep_samples = []
    for _ in range(3):
        _, t, _ = post_query(port, "分析比亚迪2022年的综合风险")
        deep_samples.append(t)
    fast_avg = mean(fast_samples) if fast_samples else 999.0
    deep_avg = mean(deep_samples) if deep_samples else 999.0
    results.append(("3.1 快速通道平均响应<1s", fast_avg < 1.0, f"avg={fast_avg:.2f}s, samples={[round(x,2) for x in fast_samples]}"))
    results.append(("3.2 深度分析平均响应<20s", deep_avg < 20.0, f"avg={deep_avg:.2f}s, samples={[round(x,2) for x in deep_samples]}"))

    # 4 文案净化（静态 + 动态）
    dyn_texts = [sum1, sum2, sum3, sum4, sum6, sum7]
    dynamic_ok = all(not has_banned_prefix(t) for t in dyn_texts)
    title_ok = all(token in js for token in ["🏆 综合排名", "📈 ", "欢迎使用，请直接输入企业问题。"])
    results.append(("4 交互文案净化", dynamic_ok and title_ok, f"dynamic_ok={dynamic_ok}, title_ok={title_ok}"))

    passed = [x for x in results if x[1]]
    failed = [x for x in results if not x[1]]

    lines = [
        "# FINAL_VERIFICATION_REPORT",
        "",
        "## 1) 环境准备与端口检测",
        *port_logs,
        f"- 采用端口: `{port}`",
        "",
        "## 2) 全场景功能验证结果",
    ]
    for name, ok, detail in results:
        if name.startswith("3.") or name.startswith("4 "):
            continue
        lines.append(f"- {'PASS' if ok else 'FAIL'} | {name} | {detail}")
    lines += [
        "",
        "## 3) 性能验证",
    ]
    for name, ok, detail in results:
        if name.startswith("3."):
            lines.append(f"- {'PASS' if ok else 'FAIL'} | {name} | {detail}")
    lines += [
        "",
        "## 4) 交互文案净化",
    ]
    for name, ok, detail in results:
        if name.startswith("4 "):
            lines.append(f"- {'PASS' if ok else 'FAIL'} | {name} | {detail}")

    lines += [
        "",
        "## 自动修复记录",
        "- 修复会话上下文：支持 `session_id` 记忆上一轮企业与时间范围，保障“为什么”追问可延续主题。",
        "- 修复缺时间澄清：单指标数值查询且未给年份时，返回 `needs_clarification`。",
        "- 修复文案净化：移除前端“报告摘要：”前缀，欢迎语改为自然表达。",
        "- 修复图表标题友好度：新增中文友好标题（如 `🏆 综合排名`、`📈 指标趋势`）。",
        "",
        "## 结论",
        f"- 通过项：{len(passed)}",
        f"- 未通过项：{len(failed)}",
    ]
    if failed:
        lines.append("- 未通过明细：")
        for name, _, detail in failed:
            lines.append(f"  - {name}: {detail}")
    else:
        lines.append("- 全部验收项已通过。")

    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"port": port, "passed": len(passed), "failed": len(failed)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
