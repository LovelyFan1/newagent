# FINAL ACCEPTANCE REPORT

## 测试时间与环境

- 测试时间：2026-04-23（本地）
- 工作目录：`app_v2.2`
- 操作系统：Windows
- 数据库：SQLite（`sqlite+aiosqlite:///./test_local.db`）
- 服务启动：`uvicorn app.main:app --reload --port 8000`
- 说明：当前机器无 Docker，`docker compose -f app_v2/docker-compose.yml ps` 无法执行（`docker` 命令缺失）

## 核心修复项

- `app/main.py`：增加数据库类型检测，SQLite 下跳过 PostgreSQL 扩展初始化，但保留基础表初始化（`users`、`scoring_results`、`core_metrics_summary`）。
- `app/core/db.py`：`ensure_vector_extension()` 仅在 PostgreSQL 执行，异常静默捕获。
- `app/services/indicator_calc.py`：将 PostgreSQL 专用 `::float` 改为兼容写法（`CAST(... AS REAL)`）。
- `app/services/scoring_service.py`：评分调用前自动确保 `scoring_results` 存在；SQLite 走原生 SQL 缓存读写路径。
- `app/services/agent/intent.py`：增加企业别名映射与提取日志（含“理想汽车”“长城汽车”等）。
- `app/services/vector_retriever.py`：SQLite 下不执行 `pg_trgm/similarity/ILIKE`，改为安全回退检索并处理 `documents` 表缺失。
- `app/services/agent/evidence.py`：保持“本地评分为空不阻断 RAG”的检索策略。

## 最终验收结果

| 用例 | 预期 | 结果 | 耗时(ms) | 备注 |
|---|---|---|---:|---|
| 注册 | 200，返回 token | 通过 | 265.79 | 返回 bearer token |
| 登录 | 200，返回 token | 通过 | 2304.34 | 返回 bearer token |
| 获取用户信息 | 200，返回用户名 | 通过 | 1754.79 | email 含 `finaltest` |
| 比亚迪2022年销量 | 200，返回销量数字 | 通过 | 878.54 | `status=completed`，`chart_type=simple_metric` |
| 对比比亚迪、长城、理想 | 200，`evidence > 0` | 通过 | 421.54 | `evidence_len=6`，`chart_type=comparison_ranking` |
| 评分比亚迪2022 | 200，返回 `total_score` | 通过 | 565.25 | `total_score=77.17` |
| 前端页面加载 | 200，返回 HTML | 通过 | 293.25 / 258.89 | 根路径与登录页均正常 |

## 汇总

- 本轮总用例：7
- 通过：7
- 失败：0
- 步骤 5 证据数量：`evidence_len=6`

## 最终结论

**本地验收通过，项目可演示。**

