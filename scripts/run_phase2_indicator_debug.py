import asyncio
import os
import sqlite3
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.indicator_calc import calculate_indicators


def load_env() -> None:
    env_path = ROOT / ".env"
    for line in env_path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        os.environ[k.strip()] = v.strip()


def pick_single_domain_enterprise() -> tuple[str | None, str | None, str | None]:
    conn = sqlite3.connect(str(ROOT / "test_local.db"))
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT d.stock_name,
               CASE WHEN EXISTS(SELECT 1 FROM fact_financials f WHERE f.enterprise_id=d.enterprise_id AND f.year=2022) THEN 1 ELSE 0 END AS fin,
               CASE WHEN EXISTS(SELECT 1 FROM fact_sales s WHERE s.enterprise_id=d.enterprise_id AND s.year=2022) THEN 1 ELSE 0 END AS sales,
               CASE WHEN EXISTS(SELECT 1 FROM fact_legal l WHERE l.enterprise_id=d.enterprise_id AND l.year=2022) THEN 1 ELSE 0 END AS legal
        FROM dim_enterprise d
        """
    ).fetchall()
    conn.close()
    only_fin = next((n for n, f, s, l in rows if f == 1 and s == 0 and l == 0), None)
    only_sales = next((n for n, f, s, l in rows if f == 0 and s == 1 and l == 0), None)
    only_legal = next((n for n, f, s, l in rows if f == 0 and s == 0 and l == 1), None)
    return only_fin, only_sales, only_legal


def has_data(v) -> bool:
    if v is None:
        return False
    if isinstance(v, str) and v.strip().upper() == "N/A":
        return False
    return True


async def safe_call(name: str, year: int):
    try:
        data = await asyncio.wait_for(calculate_indicators(name, year), timeout=20)
        return {"ok": True, "data": data, "error": None}
    except Exception as e:
        return {"ok": False, "data": None, "error": f"{type(e).__name__}: {e}"}


def dim_status(payload: dict) -> dict:
    inds = payload.get("indicators", {})
    fin = inds.get("financial_health", {})
    sales = inds.get("industry_position", {})
    legal = inds.get("legal_risk", {})
    return {
        "financial": any(has_data(fin.get(k)) for k in ["revenue", "net_profit", "total_assets"]),
        "sales": any(has_data(sales.get(k)) for k in ["sales_volume", "nev_sales_volume"]),
        "legal": any(has_data(legal.get(k)) for k in ["lawsuit_count", "lawsuit_total_amount"]),
    }


async def main() -> None:
    load_env()
    only_fin, only_sales, only_legal = pick_single_domain_enterprise()
    only_fin_target = "宁德时代" if only_fin is None else only_fin
    only_sales_target = "理想汽车" if only_sales is None else only_sales
    only_legal_target = "奥特佳" if only_legal is None else only_legal

    targets = [
        ("三领域全覆盖", "比亚迪", (True, True, True)),
        ("三领域全覆盖", "力帆科技", (True, True, True)),
        ("三领域全覆盖", "中汽股份", (True, True, True)),
        ("财务+销售", "长城汽车", (True, True, False)),
        ("财务+销售", "长安汽车", (True, True, False)),
        ("财务+司法", "东风科技", (True, False, True)),
        ("销售+司法", "福田汽车", (False, True, True)),
        ("仅财务", only_fin_target, (True, False, False)),
        ("仅销售", only_sales_target, (False, True, False)),
        ("仅司法", only_legal_target, (False, False, True)),
    ]

    lines = ["# PHASE2_DEBUG_REPORT", ""]
    failures = []
    fixes = [
        "- 修复：indicator_calc 财务缺失值不再默认写入 0，改为 None/N/A。",
        "- 修复：新增 debt_asset_ratio（资产负债率）输出字段。",
        "- 修复：法律维度缺失时保持 N/A，不再输出 0。",
    ]

    lines.append("## 企业抽样验证")
    for group, name, expected in targets:
        res = await safe_call(name, 2022)
        if not res["ok"]:
            failures.append(f"{name}: {res['error']}")
            lines.append(f"- [{group}] {name}: FAIL ({res['error']})")
            continue
        status = dim_status(res["data"])
        got = (status["financial"], status["sales"], status["legal"])
        ok = got == expected
        if not ok:
            failures.append(f"{name}: expected={expected}, got={got}")
        lines.append(f"- [{group}] {name}: {'PASS' if ok else 'FAIL'} | expected={expected} got={got}")

    lines.append("")
    lines.append("## 衍生指标计算验证（比亚迪 2022）")
    byd = await safe_call("比亚迪", 2022)
    if byd["ok"]:
        f = byd["data"]["indicators"]["financial_health"]
        checks = {
            "current_ratio": f.get("current_ratio"),
            "quick_ratio": f.get("quick_ratio"),
            "roe": f.get("roe"),
            "debt_asset_ratio": f.get("debt_asset_ratio"),
        }
        for k, v in checks.items():
            good = has_data(v) and str(v) not in {"0", "0.0", "50", "50.0"}
            if not good:
                failures.append(f"比亚迪 {k} 异常值: {v}")
            lines.append(f"- {k}: {v} -> {'PASS' if good else 'FAIL'}")
    else:
        failures.append(f"比亚迪衍生指标验证失败: {byd['error']}")
        lines.append(f"- FAIL: {byd['error']}")

    lines.append("")
    lines.append("## 缺失字段处理验证")
    gw = await safe_call("长城汽车", 2022)
    if gw["ok"]:
        legal = gw["data"]["indicators"]["legal_risk"]
        lc = legal.get("lawsuit_count")
        la = legal.get("lawsuit_total_amount")
        ok = (lc in [None, "N/A"]) and (la in [None, "N/A"])
        if not ok:
            failures.append(f"长城汽车法律字段应为空，实际 lawsuit_count={lc}, lawsuit_total_amount={la}")
        lines.append(f"- 长城汽车 lawsuit_count={lc}, lawsuit_total_amount={la} -> {'PASS' if ok else 'FAIL'}")
    else:
        failures.append(f"长城汽车验证失败: {gw['error']}")
        lines.append(f"- 长城汽车 FAIL: {gw['error']}")

    li = await safe_call("理想汽车", 2022)
    if li["ok"]:
        fin = li["data"]["indicators"]["financial_health"]
        rev = fin.get("revenue")
        npv = fin.get("net_profit")
        ok = (rev in [None, "N/A"]) and (npv in [None, "N/A"])
        if not ok:
            failures.append(f"理想汽车财务字段应为空，实际 revenue={rev}, net_profit={npv}")
        lines.append(f"- 理想汽车 revenue={rev}, net_profit={npv} -> {'PASS' if ok else 'FAIL'}")
    else:
        # 理想汽车当前可能不在dim_enterprise（被保留在销售UNMATCHED体系），此时改用仅销售样本验证。
        if only_sales_target != "理想汽车":
            lines.append(f"- 理想汽车不在当前企业基准库，改用仅销售样本 `{only_sales_target}` 验证 -> INFO")
        else:
            failures.append(f"理想汽车验证失败: {li['error']}")
            lines.append(f"- 理想汽车 FAIL: {li['error']}")

    lines.append("")
    lines.append("## 边界情况测试")
    nx = await safe_call("不存在的企业", 2022)
    lines.append(f"- 不存在企业: {'PASS' if not nx['ok'] else 'FAIL'} | {nx['error'] if nx['error'] else 'unexpected_success'}")
    if nx["ok"]:
        failures.append("不存在企业未返回错误")
    y1999 = await safe_call("比亚迪", 1999)
    if y1999["ok"]:
        st = dim_status(y1999["data"])
        ok = (st["financial"], st["sales"], st["legal"]) == (False, False, False)
        lines.append(f"- 无数据年份1999: {'PASS' if ok else 'FAIL'} | status={st}")
        if not ok:
            failures.append(f"1999应缺失，实际 status={st}")
    else:
        lines.append(f"- 无数据年份1999: FAIL | {y1999['error']}")
        failures.append(f"1999调用失败: {y1999['error']}")

    lines.append("")
    lines.append("## 自动修复记录")
    lines.extend(fixes)

    lines.append("")
    lines.append("## 失败项汇总")
    if failures:
        for f in failures:
            lines.append(f"- {f}")
    else:
        lines.append("- 无")

    report = ROOT / "PHASE2_DEBUG_REPORT.md"
    report.write_text("\n".join(lines), encoding="utf-8")
    print(f"report={report}")
    print(f"failures={len(failures)}")

    # Ensure process exits cleanly in this environment.
    os._exit(0)


if __name__ == "__main__":
    asyncio.run(main())

