from __future__ import annotations

import pytest
import sqlalchemy as sa


@pytest.mark.anyio
async def test_embedding_service_fallback_dimension(monkeypatch):
    from app.services.embedding_service import EmbeddingService

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    monkeypatch.setenv("EMBEDDING_DIM", "1536")

    svc = EmbeddingService()
    vecs = await svc.embed(["比亚迪财报分析"])
    assert len(vecs) == 1
    assert len(vecs[0]) == 1536


@pytest.mark.anyio
async def test_vector_retriever_with_mocked_session(monkeypatch):
    from app.services.vector_retriever import VectorRetriever

    class _Res:
        def mappings(self):
            return self

        def all(self):
            return [
                {"id": 1, "title": "A", "content": "新能源汽车行业增长", "source": "kb/a", "score": 0.91},
                {"id": 2, "title": "B", "content": "传统燃油车承压", "source": "kb/b", "score": 0.72},
            ]

    class _Session:
        async def execute(self, *_args, **_kwargs):
            return _Res()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _SM:
        def __call__(self):
            return _Session()

    async def _fake_embed(_texts):
        return [[0.1, 0.2, 0.3]]

    monkeypatch.setattr("app.services.vector_retriever.embedding_service.embed", _fake_embed)
    monkeypatch.setattr("app.services.vector_retriever.get_sessionmaker", lambda: _SM())

    r = VectorRetriever()
    docs = await r.retrieve("新能源汽车", top_k=2)
    assert len(docs) == 2
    assert docs[0].score >= docs[1].score
    assert "新能源" in docs[0].content


@pytest.mark.anyio
async def test_agent_query_includes_knowledge_base_evidence_when_available(client, engine, monkeypatch):
    # try to ensure pgvector/documents exists for e2e; skip if unavailable
    try:
        async with engine.begin() as conn:
            await conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS vector;"))
            await conn.execute(
                sa.text(
                    """
                    CREATE TABLE IF NOT EXISTS documents (
                      id SERIAL PRIMARY KEY,
                      title TEXT,
                      content TEXT,
                      source TEXT,
                      embedding vector(1536),
                      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    """
                )
            )
            await conn.execute(sa.text("DELETE FROM documents WHERE source = 'test_kb';"))
            await conn.execute(
                sa.text(
                    """
                    INSERT INTO documents (title, content, source, embedding)
                    VALUES (
                      '测试知识文档',
                      '比亚迪在供应链整合方面具有优势，行业竞争仍在加剧。',
                      'test_kb',
                      CAST(:embedding AS vector)
                    )
                    """
                ),
                {"embedding": "[" + ",".join(["0.0"] * 1536) + "]"},
            )
    except Exception as e:
        pytest.skip(f"pgvector/documents unavailable in test env: {e!s}")

    async def _fake_embed(_texts):
        return [[0.0] * 1536]

    monkeypatch.setattr("app.services.vector_retriever.embedding_service.embed", _fake_embed)

    r = await client.post("/api/v1/agent/query", json={"question": "比亚迪 2022 年财务风险分析", "session_id": "rag-e2e"})
    assert r.status_code == 200
    data = r.json()["data"]
    assert data["status"] == "completed"
    ev = data.get("evidence") or []
    assert any((item.get("source") == "knowledge_base") for item in ev)

