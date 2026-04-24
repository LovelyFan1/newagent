"""
回归：核心别名、全量筛选路由、极简追问上下文。
使用 TestClient 调用 /api/v1/agent/query（等价 HTTP 200），并关闭 LLM 以保证稳定。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_env() -> None:
    import os

    env_file = ROOT / ".env"
    if not env_file.is_file():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        os.environ[k.strip()] = v.strip()


def _disable_llm() -> None:
    from app.api.v1.routes import agent as agent_routes

    o = agent_routes.orchestrator
    if hasattr(o.llm, "_enabled"):
        o.llm._enabled = False
    if hasattr(o.llm, "_client"):
        o.llm._client = None
    if hasattr(o.composer.llm, "_enabled"):
        o.composer.llm._enabled = False
    if hasattr(o.composer.llm, "_client"):
        o.composer.llm._client = None


def _payload_summary(data: dict) -> tuple[str, bool, str]:
    inner = data.get("data") if isinstance(data.get("data"), dict) else {}
    status = str(inner.get("status") or "")
    clar = inner.get("clarification") if isinstance(inner.get("clarification"), dict) else {}
    need = bool(clar.get("required"))
    rep = inner.get("report") if isinstance(inner.get("report"), dict) else {}
    summary = str(rep.get("summary") or "")
    return status, need, summary


def main() -> int:
    _load_env()
    from fastapi.testclient import TestClient

    from app.main import app

    _disable_llm()
    sid_main = "verify_alias_fix_main"
    sid_futian = "verify_alias_fix_futian"

    cases: list[tuple[str, str, str | None, list[str]]] = [
        ("分析力帆科技2022年的综合风险", sid_main, "力帆科技", []),
        ("对比江铃汽车、中国重汽、宇通客车的财务表现", sid_main, None, ["江铃汽车", "中国重汽", "宇通客车"]),
        ("汉马科技最近的司法风险", sid_main, "汉马科技", []),
        ("索菱股份的财务压力与司法风险是否相关", sid_main, "索菱股份", []),
        ("福田汽车近三年销量趋势", sid_futian, "福田汽车", []),
        ("为什么", sid_futian, "福田汽车", []),
        ("哪些企业ROE最高", sid_main, None, []),
        ("流动比率低于1.0的企业有哪些", sid_main, None, []),
        ("亚星客车的销量和司法风险对比", sid_main, "亚星客车", []),
    ]

    failures: list[str] = []
    with TestClient(app, raise_server_exceptions=True) as client:
        for question, sid, must_contain, must_all in cases:
            r = client.post(
                "/api/v1/agent/query",
                json={"question": question, "session_id": sid, "force": True},
            )
            try:
                body = r.json()
            except Exception:
                failures.append(f"{question!r}: 非 JSON 响应 raw={r.text[:200]}")
                continue
            code = body.get("code")
            if r.status_code != 200 or code != 0:
                failures.append(
                    f"{question!r}: HTTP={r.status_code} code={code} body={json.dumps(body, ensure_ascii=False)[:400]}"
                )
                continue
            status, need_clar, summary = _payload_summary(body)
            if status == "needs_clarification" or need_clar:
                qs = body.get("data", {}).get("clarification", {}).get("questions", [])
                failures.append(f"{question!r}: needs_clarification questions={qs}")
                continue
            if must_contain and must_contain not in summary:
                failures.append(f"{question!r}: 摘要未包含期望词 {must_contain!r} snip={summary[:180]!r}")
            for w in must_all:
                if w not in summary:
                    failures.append(f"{question!r}: 摘要未同时包含 {w!r} snip={summary[:220]!r}")
            ent_q = "请补充企业名称" in summary or "请明确企业名称" in summary
            if ent_q:
                failures.append(f"{question!r}: 出现企业补全话术 summary={summary[:200]!r}")

    if failures:
        print("回归失败：")
        for f in failures:
            print(" -", f)
        return 1
    print("[PASS] 所有核心企业别名映射生效，路由、上下文、可视化回归正常。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
