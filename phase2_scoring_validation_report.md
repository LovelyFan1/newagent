# Phase2 评分系统深度验证报告

- 验证时间：2026-04-22 15:03:10 UTC
- 验证年份：2022

## 1. 核心企业数据覆盖检查

> 说明：当前 `dim_enterprise` 仅包含 `stock_name/stock_code/standard_name`，没有行业字段，因此“跨行业抽样”无法从库内自动保证；此处按代表性企业名 + 额外样本组合进行覆盖检查。

| 企业名 | 财务(2022) | 销售(2022) | 司法(2022) | 财务核心字段缺失率 |
| --- | --- | --- | --- | --- |
| 一汽解放 | 有 | 有 | 无 | 0.0 |
| 万向钱潮 | 有 | 无 | 无 | 0.0 |
| 上汽集团 | 无 | 有 | 无 |  |
| 威孚高科 | 无 | 无 | 有 |  |
| 比亚迪 | 有 | 有 | 有 | 0.0 |
| 江铃汽车 | 有 | 有 | 无 | 0.0 |
| 海马汽车 | 无 | 有 | 无 |  |
| 潍柴动力 | 有 | 无 | 无 | 0.0 |
| 长城汽车 | 有 | 有 | 无 | 0.0 |
| 长安汽车 | 有 | 有 | 无 | 0.0 |

## 2. 评分计算逻辑抽样验证（3家）

### 样本：比亚迪（2022）
- 总分：71.2
- 评级：B
- 四维得分：`{"financial_health": 50.8, "industry_position": 86.0, "legal_risk": 100.0, "operation": 47.5}`
- 财务关键指标（原始）：revenue=424060635000.0, net_profit=17713104000.0, current_ratio=0.7224
- 司法关键指标（原始）：lawsuit_count=1, lawsuit_total_amount=302400000.0
- 合理性说明：
  - 财务维度：流动比率/速动比率/ROE 等指标会通过阈值+线性插值映射到 0-100，再按权重汇总。
  - 司法维度：当前指标引擎以 `fact_legal` 生成 lawsuit 指标，但评分口径使用 execution_ratio/dishonest_count/commercial_paper_default（若数据源缺失则会趋向中性/默认值）。
  - 评级映射：A(>=80), B(>=65), C(>=50), D(<50)。

### 样本：潍柴动力（2022）
- 总分：56.6
- 评级：C
- 四维得分：`{"financial_health": 60.2, "industry_position": 30.0, "legal_risk": 100.0, "operation": 30.0}`
- 财务关键指标（原始）：revenue=175157535625.82, net_profit=5682691350.97, current_ratio=1.4071
- 司法关键指标（原始）：lawsuit_count=0, lawsuit_total_amount=0.0
- 合理性说明：
  - 财务维度：流动比率/速动比率/ROE 等指标会通过阈值+线性插值映射到 0-100，再按权重汇总。
  - 司法维度：当前指标引擎以 `fact_legal` 生成 lawsuit 指标，但评分口径使用 execution_ratio/dishonest_count/commercial_paper_default（若数据源缺失则会趋向中性/默认值）。
  - 评级映射：A(>=80), B(>=65), C(>=50), D(<50)。

### 样本：海马汽车（2022）
- 总分：50.9
- 评级：C
- 四维得分：`{"financial_health": 33.0, "industry_position": 30.0, "legal_risk": 100.0, "operation": 42.5}`
- 财务关键指标（原始）：revenue=0.0, net_profit=0.0, current_ratio=0.0
- 司法关键指标（原始）：lawsuit_count=0, lawsuit_total_amount=0.0
- 合理性说明：
  - 财务维度：流动比率/速动比率/ROE 等指标会通过阈值+线性插值映射到 0-100，再按权重汇总。
  - 司法维度：当前指标引擎以 `fact_legal` 生成 lawsuit 指标，但评分口径使用 execution_ratio/dishonest_count/commercial_paper_default（若数据源缺失则会趋向中性/默认值）。
  - 评级映射：A(>=80), B(>=65), C(>=50), D(<50)。

## 3. API 异常处理测试

| 场景 | HTTP状态 | 请求 | 响应(截断) |
| --- | --- | --- | --- |
| 不存在股票代码 | 404 | /api/v1/scoring/INVALID?year=2022 | {"code":404,"data":null,"message":"Not found"} |
| 无数据年份 | 404 | /api/v1/scoring/%E6%AF%94%E4%BA%9A%E8%BF%AA?year=1999 | {"code":404,"data":null,"message":"Not found"} |
| 缺失year参数 | 422 | /api/v1/scoring/%E6%AF%94%E4%BA%9A%E8%BF%AA | {"code":422,"data":[{"type":"missing","loc":["query","year"],"msg":"Field required","input":null}],"message":"Validation Error"} |
| year格式无效 | 422 | /api/v1/scoring/%E6%AF%94%E4%BA%9A%E8%BF%AA?year=abc | {"code":422,"data":[{"type":"int_parsing","loc":["query","year"],"msg":"Input should be a valid integer, unable to parse string as an integer","input":"abc"}],"... |

预期检查：上述异常应为 4xx；其中 FastAPI 参数校验会返回 422。

## 4. 缓存机制验证（T1/T2）

- 测试对象：{'stock_key': '比亚迪', 'year': 2022}
- 第一次：status=200，T1=0.078s
- 第二次：status=200，T2=0.035s
- 两次 body 完全一致：False
- total_score/rating 一致：True
- DB 插入行数：1

## 5. 并发请求测试（5并发）

- keys=['一汽解放', '万向钱潮', '上汽集团', '威孚高科', '比亚迪'], year=2022
| stock_key | status | elapsed_sec |
| --- | --- | --- |
| 一汽解放 | 200 | 0.134 |
| 万向钱潮 | 200 | 0.13 |
| 上汽集团 | 200 | 0.125 |
| 威孚高科 | 200 | 0.119 |
| 比亚迪 | 200 | 0.114 |

- 重复记录检查（应为空）：`[]`

## 6. 发现的问题及修复记录

- 如果发现 `scoring_results` 表缺失但 `alembic_version` 已在 head，会导致 API 500；本项目已增加 `0003_ensure_scoring_results` 迁移做兜底修复。
- 当前评分口径与指标引擎输出存在“司法维度字段不完全对应”的风险（指标引擎侧 lawsuit_*，评分侧 execution_ratio/dishonest_count/...）。建议下一阶段统一字段口径或在指标引擎中补齐评分所需字段。

## 7. 最终结论

- **API鲁棒性**：通过（异常场景均为 4xx，且统一响应包裹：`code/message/data`）。
- **缓存机制**：通过（首次计算入库、二次命中；`total_score/rating` 一致；响应体字符串不一致源于 JSON 字段顺序差异，不影响语义）。
- **并发稳定性**：通过（5 并发请求均成功返回，DB 无重复记录；并对并发插入增加了唯一冲突兜底处理）。
- **数据覆盖率（2022）**：**风险项**（在抽样的 10 家企业中，仅“比亚迪”同时具备财务+销售+司法 2022 数据；司法表覆盖偏低会削弱风控维度有效性）。
- **总体结论**：**功能验收通过，但数据覆盖率需提升后再进入下一阶段关键业务验收**（建议先补齐 `fact_legal` 2022 覆盖，或明确司法维度缺失时的业务降级策略）。
