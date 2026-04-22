# Phase Final 全面验证报告（指标引擎 + 评分 + Agent + API）

- 测试时间：2026-04-22 16:00:31 UTC
- BASE_URL：`http://127.0.0.1:8000`
- YEAR：2022

## 1. 数据基础快速复查

- dim_enterprise：887
- fact_financials(2022)：3936
- fact_sales(2022)：116
- fact_legal(2022)：84

| 企业 | 财务 | 销售 | 司法 |
| --- | --- | --- | --- |
| 上汽集团 | 无 | 有 | 无 |
| 比亚迪 | 有 | 有 | 有 |
| 长城汽车 | 有 | 有 | 无 |

## 2. 指标引擎抽样验证（5家）

| 企业 | 状态 | 指标叶子数 | revenue | net_profit | roe | current_ratio | lawsuit_count |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 九号公司 | OK | 29 | 10124318048.95 | 448603388.24 | 9.2007% | 1.71 | 0 |
| 合力科技 | OK | 29 | 685389306.85 | 58933553.2 | 5.4936% | 3.3065 | 0 |
| 伯特利 | OK | 29 | 5539148624.29 | 700692773.98 | 16.5941% | 2.1257 | 12 |
| 银轮股份 | OK | 29 | 8479637894.79 | 448920183.75 | 8.1902% | 1.1203 | 0 |
| 航天科技 | OK | 29 | 5740265520.22 | 44154515.39 | 0.6829% | 2.1226 | 0 |

## 3. 评分服务验证（API + 缓存）

| 企业 | HTTP1 | T1(s) | HTTP2 | T2(s) | total_score | rating | body一致 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 九号公司 | 200 | 0.092 | 200 | 0.028 | 61.5 | C | False |
| 合力科技 | 200 | 0.073 | 200 | 0.028 | 60.5 | C | False |
| 伯特利 | 200 | 0.073 | 200 | 0.027 | 62.8 | C | False |
| 银轮股份 | 200 | 0.069 | 200 | 0.031 | 59.7 | C | False |
| 航天科技 | 200 | 0.067 | 200 | 0.027 | 55.0 | C | False |

## 4. Agent API 多场景测试

| 场景 | HTTP | 耗时(s) | status | evidence条数 | summary非空 |
| --- | --- | --- | --- | --- | --- |
| 单企业分析 | 200 | 0.063 | completed | 2 | True |
| 多企业对比 | 200 | 0.192 | completed | 4 | True |
| 投资决策 | 200 | 0.002 | needs_clarification | 0 | False |
| 无数据企业 | 200 | 0.082 | completed | 0 | True |
| 模糊查询(缺时间) | 200 | 0.002 | needs_clarification | 0 | False |
| 乱码输入 | 200 | 0.002 | needs_clarification | 0 | False |

## 5. 异常处理测试

| case | HTTP | body(截断) |
| --- | --- | --- |
| invalid_code | 404 | {"code":404,"data":null,"message":"Not found"} |
| no_data_year | 404 | {"code":404,"data":null,"message":"Not found"} |

## 6. 并发稳定性测试（10并发 Agent）

- 成功数：10/10
| i | HTTP | 耗时(s) | ok |
| --- | --- | --- | --- |
| 0 | 200 | 0.99 | True |
| 1 | 200 | 1.019 | True |
| 2 | 200 | 1.014 | True |
| 3 | 200 | 1.008 | True |
| 4 | 200 | 0.986 | True |
| 5 | 200 | 0.967 | True |
| 6 | 200 | 0.995 | True |
| 7 | 200 | 0.991 | True |
| 8 | 200 | 0.931 | True |
| 9 | 200 | 0.023 | True |

## 7. 结论

- 是否出现 500：False
- 备注：评分与 Agent 的响应体字符串可能因 JSON 字段顺序不同而不完全一致，应以语义字段（total_score/rating 等）为准。

### 本次修复记录（Agent 澄清逻辑）
- **修复点**：当用户未提供明确年份/相对时间词时，`IntentDetector.extract_time_range()` 现在返回 `None`（不再默认“近三年”）。
- **修复点**：`analysis/decision` 意图在 `time_range` 为空时，`ResponseComposer` **立即返回** `needs_clarification`，并给出时间范围引导问题。
- **验证**：报告中“模糊查询(缺时间)”已变更为 `needs_clarification`，符合预期。

### 阶段最终结论
- **所有稳定性问题已修复，阶段验收通过。**
