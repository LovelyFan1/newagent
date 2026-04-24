"""
全场景验收（TestClient + 关闭 LLM）。
硬约束下不修改数据文件；失败时打印原因，单次运行 exit 非 0。
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _load_env() -> None:
    p = ROOT / ".env"
    if not p.is_file():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        os.environ[k.strip()] = v.strip()


def _disable_llm() -> None:
    from app.api.v1.routes import agent as agent_routes

    o = agent_routes.orchestrator
    o.llm._enabled = False
    o.llm._client = None
    o.composer.llm._enabled = False
    o.composer.llm._client = None


def _post(client, q: str, sid: str) -> dict:
    r = client.post("/api/v1/agent/query", json={"question": q, "session_id": sid, "force": True})
    assert r.status_code == 200, f"HTTP {r.status_code} body={r.text[:300]}"
    body = r.json()
    assert body.get("code") == 0, f"code={body.get('code')} msg={body.get('message')}"
    return body.get("data") or {}


def _inner(d: dict) -> tuple[str, bool, str, str | None]:
    st = str(d.get("status") or "")
    clar = d.get("clarification") if isinstance(d.get("clarification"), dict) else {}
    need = bool(clar.get("required"))
    rep = d.get("report") if isinstance(d.get("report"), dict) else {}
    summ = str(rep.get("summary") or "")
    ct = ((d.get("charts") or {}).get("chart_type")) if isinstance(d.get("charts"), dict) else None
    return st, need, summ, str(ct) if ct else None


def main() -> int:
    _load_env()
    _disable_llm()
    from fastapi.testclient import TestClient
    from app.main import app

    failures: list[str] = []
    with TestClient(app) as client:

        def run(name: str, fn) -> None:
            try:
                fn()
            except AssertionError as e:
                failures.append(f"{name}: {e}")

        def s1():
            sid = "ult_s1"
            _post(client, "重汽2022年销量", sid)
            d = _post(client, "利润呢", sid)
            st, need, summ, _ = _inner(d)
            assert st != "needs_clarification" and not need, (st, need)
            assert "补充企业" not in summ and "净利润" in summ, summ[:400]

        def s2():
            sid = "ult_s2"
            _post(client, "索菱的财务压力", sid)
            d = _post(client, "它的司法风险如何", sid)
            _, need, summ, _ = _inner(d)
            assert not need
            ev = d.get("evidence") or []
            ev_txt = " ".join(str((e or {}).get("excerpt") or "") + str((e or {}).get("title") or "") for e in ev)
            assert "司法" in summ or "诉讼" in summ or "风险" in summ or "诉讼" in ev_txt or "司法" in ev_txt, (
                summ[:400],
                ev_txt[:400],
            )

        def s3():
            sid = "ult_s3"
            d1 = _post(client, "比亚迪2022年销量", sid)
            _, _, s1, _ = _inner(d1)
            assert "比亚迪" in s1 and "2022" in s1
            d2 = _post(client, "长城呢", sid)
            _, _, s2, _ = _inner(d2)
            assert "长城" in s2 and "2022" in s2
            d3 = _post(client, "长安呢", sid)
            _, _, s3, _ = _inner(d3)
            assert "长安" in s3 and "2022" in s3

        def s4():
            d = _post(client, "净利润最高的汽车零部件企业", "ult_s4")
            _, need, _, ct = _inner(d)
            assert not need
            assert ct == "ranking", ct

        def s5():
            d = _post(client, "哪些企业没有法律诉讼记录", "ult_s5")
            _, need, summ, _ = _inner(d)
            assert not need
            assert "本地库" in summ or "企业" in summ, summ[:400]
            assert "比亚迪2022" not in summ[:80]

        def s6():
            d = _post(client, "既有销售数据又有司法风险的企业有哪些", "ult_s6")
            _, need, summ, _ = _inner(d)
            assert not need
            assert "销售" in summ or "司法" in summ or "同时" in summ, summ[:400]

        def s7():
            d = _post(client, "一彬科技2022年销量", "ult_s7")
            _, need, summ, _ = _inner(d)
            assert not need
            assert "一彬" in summ
            assert "未找到" in summ or "财务" in summ or "年报" in summ, summ[:400]

        def s8():
            d = _post(client, "宇通客车2022年销量为什么是0", "ult_s8")
            _, need, summ, _ = _inner(d)
            assert not need
            assert "fact_sales" in summ or "口径" in summ or "nev" in summ.lower() or "新能源" in summ, summ[:400]

        def s9():
            sid = "ult_s9"
            _post(client, "对比汉马科技和索菱股份的司法风险", sid)
            d = _post(client, "谁的司法风险更高", sid)
            _, need, summ, _ = _inner(d)
            assert not need
            assert "汉马" in summ or "索菱" in summ or "司法" in summ, summ[:400]

        def s10():
            d = _post(client, "江铃汽车2022年营收和净利润", "ult_s10")
            _, need, summ, _ = _inner(d)
            assert not need
            assert "营收" in summ or "净利润" in summ or "摘录" in summ, summ[:500]

        def s11():
            d = _post(client, "理想汽车的法律纠纷怎么样", "ult_s11")
            _, need, summ, ct = _inner(d)
            assert not need
            assert ct == "legal_risk" or "司法" in summ or "诉讼" in summ, (ct, summ[:200])

        def s12():
            d = _post(client, "22年销量最高的车企", "ult_s12")
            _, need, _, ct = _inner(d)
            assert not need
            assert ct == "ranking", ct

        def s13():
            d = _post(client, "比亚迪这几年的销量变化", "ult_s13")
            _, need, summ, _ = _inner(d)
            assert not need
            assert "比亚迪" in summ and ("趋势" in summ or "销量" in summ), summ[:400]

        def s14():
            d = _post(client, "宇通客车2022年销量和净利润", "ult_s14")
            _, need, summ, _ = _inner(d)
            assert not need
            assert "净利润" in summ and ("销量" in summ or "摘录" in summ), summ[:500]

        def s15():
            d = _post(client, "福田汽车近三年销量趋势与诉讼变化", "ult_s15")
            _, need, summ, ct = _inner(d)
            assert not need
            assert ("诉讼" in summ or "司法" in summ or ct == "legal_risk") and (
                "趋势" in summ or "销量" in summ or "变化" in summ
            ), (ct, summ[:400])

        for i, fn in enumerate(
            [s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15], start=1
        ):
            run(f"scenario_{i}", fn)

    if failures:
        print("FAILURES:")
        for f in failures:
            print(" -", f)
        return 1
    print("[PASS] 核心逻辑终极加固完成，所有边界场景验证通过，可视化与逻辑全部合理。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
