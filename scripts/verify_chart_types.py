import asyncio
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.agent.orchestrator import AgentOrchestrator


def load_env() -> None:
    env_file = ROOT / ".env"
    for line in env_file.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        os.environ[k.strip()] = v.strip()


async def run_case(orc: AgentOrchestrator, question: str, expected: set[str]) -> dict:
    resp = await orc.process_query(question=question, session_id="verify_chart_types", force=True)
    ok = isinstance(resp, dict)
    chart_type = None
    if ok:
        chart_type = ((resp or {}).get("charts") or {}).get("chart_type")
    passed = bool(ok and chart_type in expected)
    return {
        "question": question,
        "expected": sorted(expected),
        "chart_type": chart_type,
        "status_code": 200 if ok else 500,
        "passed": passed,
    }


async def main() -> None:
    load_env()
    orc = AgentOrchestrator()
    # deterministic verification path: avoid external LLM timing drift
    if hasattr(orc.llm, "_enabled"):
        orc.llm._enabled = False
    if hasattr(orc.llm, "_client"):
        orc.llm._client = None
    if hasattr(orc.composer.llm, "_enabled"):
        orc.composer.llm._enabled = False
    if hasattr(orc.composer.llm, "_client"):
        orc.composer.llm._client = None
    cases = [
        ("对比比亚迪、长城、长安的财务表现", {"comparison_ranking"}),
        ("比亚迪近三年销量趋势", {"simple_metric"}),
        ("比亚迪司法风险", {"legal_risk"}),
        ("理想汽车的舆情怎么样", {"sentiment", "general"}),
    ]
    results = []
    for q, exp in cases:
        results.append(await run_case(orc, q, exp))

    for idx, res in enumerate(results, start=1):
        verdict = "通过" if res["passed"] else "失败"
        print(
            f"[{idx}] {verdict} | 问题: {res['question']} | expected={res['expected']} | got={res['chart_type']} | status={res['status_code']}"
        )

    all_passed = all(x["passed"] for x in results)
    print("总结果:", "全部通过" if all_passed else "存在失败")
    raise SystemExit(0 if all_passed else 1)


if __name__ == "__main__":
    asyncio.run(main())

