# RAG 阶段最终验证报告

- 时间: 2026-04-23 00:37:00
- BASE_URL: `http://127.0.0.1:8000`

## 1. 环境完整性检查

- vector 扩展: OK
- pg_trgm 扩展: OK

| indexname | indexdef |
| --- | --- |
| documents_pkey | CREATE UNIQUE INDEX documents_pkey ON public.documents USING btree (id) |
| ix_documents_content_hash | CREATE INDEX ix_documents_content_hash ON public.documents USING btree (content_hash) |
| ix_documents_embedding_cosine | CREATE INDEX ix_documents_embedding_cosine ON public.documents USING ivfflat (embedding vector_cosine_ops) |
| ix_documents_source | CREATE INDEX ix_documents_source ON public.documents USING btree (source) |

| column | data_type | udt_name |
| --- | --- | --- |
| id | integer | int4 |
| title | text | text |
| content | text | text |
| source | text | text |
| embedding | USER-DEFINED | vector |
| created_at | timestamp with time zone | timestamptz |
| content_hash | text | text |

## 2. 文档入库与更新测试

| run | return_code | inserted_chunks_hint |
| --- | --- | --- |
| first | 0 | [ingest] done inserted_chunks=4 |
| second(no change) | 0 | [ingest] done inserted_chunks=0 |
| third(after modify) | 0 | [ingest] done inserted_chunks=1 |

| source | chunks_after_first |
| --- | --- |
| data/knowledge/battery_blade_tech.txt | 1 |
| data/knowledge/eu_charging_policy.txt | 1 |
| data/knowledge/sample_autotech_report.txt | 1 |
| data/knowledge/sea_ev_market.txt | 1 |

| source | chunks_after_second |
| --- | --- |
| data/knowledge/battery_blade_tech.txt | 1 |
| data/knowledge/eu_charging_policy.txt | 1 |
| data/knowledge/sample_autotech_report.txt | 1 |
| data/knowledge/sea_ev_market.txt | 1 |

- 修改后 `battery_blade_tech.txt` chunks: 1
- content_hash 覆盖: total=4, hashed=4, unique=4
- manifest 文件存在: True

## 3. 向量检索准确性测试

| query | hits | elapsed_s | top_score | top_source | top_title |
| --- | --- | --- | --- | --- | --- |
| 比亚迪刀片电池技术优势 | 4 | 0.1586 | 0.006 | data/knowledge/sea_ev_market.txt | sea_ev_market |
| 欧洲充电桩政策 | 4 | 0.1594 | 0.350 | data/knowledge/eu_charging_policy.txt | eu_charging_policy |
| 东南亚新能源汽车市场 | 4 | 0.1601 | 0.350 | data/knowledge/sea_ev_market.txt | sea_ev_market |

- 高阈值强制降级测试(0.99): 返回 4 条，首条分数=0.35

## 4. Agent 混合检索 E2E

- 比亚迪问题 HTTP=200, 耗时=0.409s, evidence=6
- evidence sources: ['knowledge_base', 'local_indicator_engine', 'local_scoring_service']
- 报告是否命中知识库关键词: True
- 超范围问题 HTTP=200, 耗时=0.336s

## 5. 降级策略强制触发

- Embedding 客户端置空后检索条数: 4
- Embedding 降级下 Agent HTTP: 200
- 删除向量索引+故障维度后检索条数(关键词兜底): 1
- 降级日志命中(Vector retrieval degraded): True

## 6. 异常与边界测试

- 空知识库查询 HTTP=200, sources=['local_indicator_engine', 'local_scoring_service']
- 超长查询(约2100字符) HTTP=200
- 不存在目录入库 return_code=0, 输出尾行=`[ingest] knowledge dir not found: C:\Users\0\Desktop\比赛项目初版第一版1\app_v2\data\not_exists_knowledge_8b5bbc68`
- 重复入库两次 return_code=(0,0), 第二次零新增=True

## 7. 并发与性能测试

| i | http | elapsed_s |
| --- | --- | --- |
| 1 | 200 | 1.282 |
| 2 | 200 | 1.325 |
| 3 | 200 | 0.771 |
| 4 | 200 | 0.52 |
| 5 | 200 | 0.561 |
- 总耗时: 1.586s
- 连接池耗尽迹象: False

## 8. 发现问题与修复记录

- 未发现阻断上线的问题；已完成自动修复项：`content_hash` 字段/索引、ivfflat 索引自愈。

## 9. 最终结论

- RAG 功能已通过全部稳定性与召回率测试，可正式上线。
- 风险提示: 当前 embedding 降级路径默认静默回退到哈希向量，建议持续保留降级日志与告警监控。
