# RAG 集成验证报告

- 验证时间：2026-04-22
- 目标：pgvector + 文档向量化 + Agent 混合检索生效

## 环境与迁移

- PostgreSQL 镜像：`pgvector/pgvector:pg16`
- Alembic 版本：`0004_documents_vector`
- `documents` 表已创建，当前文档块数：`2`

## 文档入库

- 入库脚本：`scripts/ingest_documents.py`
- 示例输出：
  - `[ingest] data/knowledge/sample_autotech_report.txt chunks=1`
  - `[ingest] done inserted_chunks=1`

## 检索与Agent验证

- 向量检索服务：`app/services/vector_retriever.py`
- Agent 混合检索验证请求：
  - `POST /api/v1/agent/query`
  - `question=比亚迪 2022 年行业竞争风险分析`
- 关键结果：
  - `status=completed`
  - `evidence.source` 包含：
    - `local_scoring_service`
    - `local_indicator_engine`
    - `knowledge_base`

## 测试结果

- `pytest tests/test_rag.py`：`3 passed`

## 降级策略

- 若 OpenAI Embedding 不可用：`EmbeddingService` 自动降级为本地哈希向量。
- 若 pgvector 查询失败或结果为空：`VectorRetriever` 自动降级到关键词检索（ILIKE）。
- 若数据库未安装 vector 扩展：系统仍可继续使用结构化证据链（指标+评分）。

