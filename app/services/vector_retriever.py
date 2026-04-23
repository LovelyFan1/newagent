from __future__ import annotations

import logging
import os
import re
from typing import List

import sqlalchemy as sa
from pydantic import BaseModel

from app.db.session import get_sessionmaker
from app.services.embedding_service import embedding_service

logger = logging.getLogger(__name__)


class DocumentChunk(BaseModel):
    id: int
    title: str | None
    content: str
    source: str | None
    score: float


class VectorRetriever:
    def _keyword_tokens(self, query: str) -> list[str]:
        raw = [t for t in query.split() if len(t.strip()) >= 2]
        if raw:
            return raw[:12]
        cjk = "".join(re.findall(r"[\u4e00-\u9fff]+", query))
        if cjk:
            try:
                import jieba  # type: ignore

                toks = [t.strip() for t in jieba.cut(cjk) if t.strip()]
                if toks:
                    return toks[:20]
            except Exception:
                pass
            # fallback: single-char split as required
            return [ch for ch in cjk][:20]
        return []

    async def retrieve(self, query: str, top_k: int = 5) -> List[DocumentChunk]:
        sm = get_sessionmaker()
        vec_rows: list[dict] = []
        trigram_rows: list[dict] = []
        cand_k = 10
        threshold = float(os.environ.get("RAG_VECTOR_SIM_THRESHOLD", "0.5"))
        vec_literal = None
        if embedding_service._client is None:
            logger.warning(
                "Vector retrieval degraded",
                extra={"reason": "embedding_client_unavailable", "query": query, "fallback": "trgm+keyword"},
            )
        vecs = await embedding_service.embed([query])
        if vecs:
            vec = vecs[0]
            vec_literal = "[" + ",".join(f"{x:.8f}" for x in vec) + "]"

        async with sm() as db:
            bind = db.get_bind()
            dialect_name = bind.dialect.name if bind is not None else ""
            is_pg = dialect_name == "postgresql"

            if is_pg:
                try:
                    await db.execute(sa.text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
                    await db.commit()
                except Exception:
                    await db.rollback()
                    logger.warning(
                        "Vector retrieval degraded",
                        extra={"reason": "pg_trgm_extension_failed", "query": query, "fallback": "vector+keyword"},
                    )

            if vec_literal is not None:
                vector_sql = sa.text(
                    """
                    SELECT
                      id,
                      title,
                      content,
                      source,
                      1 - (embedding <=> CAST(:vec AS vector)) AS score
                    FROM documents
                    WHERE embedding IS NOT NULL
                    ORDER BY embedding <=> CAST(:vec AS vector)
                    LIMIT :cand_k
                    """
                )
                try:
                    vec_rows = [dict(r) for r in (await db.execute(vector_sql, {"vec": vec_literal, "cand_k": cand_k})).mappings().all()]
                except Exception:
                    await db.rollback()
                    logger.warning(
                        "Vector retrieval degraded",
                        extra={"reason": "pgvector_query_failed", "query": query, "fallback": "trgm+keyword"},
                    )
                    vec_rows = []

            top_score = float(vec_rows[0].get("score") or 0.0) if vec_rows else 0.0
            if top_score < threshold:
                logger.warning(
                    "Vector retrieval degraded",
                    extra={"reason": "low_vector_similarity", "query": query, "fallback": "trgm+keyword"},
                )

            if is_pg:
                trigram_sql = sa.text(
                    """
                    SELECT
                      id,
                      title,
                      content,
                      source,
                      similarity(content, :q) AS score
                    FROM documents
                    WHERE similarity(content, :q) > 0.3
                    ORDER BY similarity(content, :q) DESC
                    LIMIT :cand_k
                    """
                )
                try:
                    trigram_rows = [dict(r) for r in (await db.execute(trigram_sql, {"q": query, "cand_k": cand_k})).mappings().all()]
                except Exception:
                    await db.rollback()
                    logger.warning(
                        "Vector retrieval degraded",
                        extra={"reason": "trigram_query_failed", "query": query, "fallback": "keyword"},
                    )
                    trigram_rows = []

            # merge weighted scores by id
            merged: dict[int, dict] = {}
            for r in vec_rows:
                rid = int(r["id"])
                merged[rid] = {
                    "id": rid,
                    "title": r.get("title"),
                    "content": r.get("content") or "",
                    "source": r.get("source"),
                    "score": 0.65 * float(r.get("score") or 0.0),
                }
            for r in trigram_rows:
                rid = int(r["id"])
                weighted = 0.35 * float(r.get("score") or 0.0)
                if rid in merged:
                    merged[rid]["score"] = max(float(merged[rid]["score"]), float(merged[rid]["score"]) + weighted)
                else:
                    merged[rid] = {
                        "id": rid,
                        "title": r.get("title"),
                        "content": r.get("content") or "",
                        "source": r.get("source"),
                        "score": weighted,
                    }

            ranked_rows = sorted(merged.values(), key=lambda x: float(x.get("score") or 0.0), reverse=True)

            # keyword fallback if still weak/empty
            if (not ranked_rows) or (float(ranked_rows[0].get("score") or 0.0) < 0.2):
                tokens = self._keyword_tokens(query)
                where_clauses = []
                params: dict[str, object] = {"cand_k": cand_k}
                for i, tok in enumerate(tokens):
                    key = f"t{i}"
                    op = "ILIKE" if is_pg else "LIKE"
                    where_clauses.append(f"(title {op} :{key} OR content {op} :{key})")
                    params[key] = f"%{tok}%"
                if where_clauses:
                    kw_sql = sa.text(
                        f"""
                        SELECT id, title, content, source, 0.35 AS score
                        FROM documents
                        WHERE {" OR ".join(where_clauses)}
                        ORDER BY id DESC
                        LIMIT :cand_k
                        """
                    )
                    try:
                        kw_rows = [dict(r) for r in (await db.execute(kw_sql, params)).mappings().all()]
                        for r in kw_rows:
                            rid = int(r["id"])
                            if rid in merged:
                                merged[rid]["score"] = max(float(merged[rid]["score"]), 0.35)
                            else:
                                merged[rid] = {
                                    "id": rid,
                                    "title": r.get("title"),
                                    "content": r.get("content") or "",
                                    "source": r.get("source"),
                                    "score": 0.35,
                                }
                    except Exception:
                        await db.rollback()
                        logger.warning(
                            "Vector retrieval degraded",
                            extra={"reason": "keyword_query_failed", "query": query, "fallback": "recent_docs"},
                        )
                ranked_rows = sorted(merged.values(), key=lambda x: float(x.get("score") or 0.0), reverse=True)

            # ensure at least 1-3 results when KB is non-empty
            if not ranked_rows:
                try:
                    kb_cnt = await db.execute(sa.text("SELECT CAST(COUNT(*) AS INTEGER) FROM documents"))
                    total_docs = int(kb_cnt.scalar() or 0)
                except Exception:
                    await db.rollback()
                    total_docs = 0
                if total_docs > 0:
                    try:
                        recent = (
                            await db.execute(
                                sa.text(
                                    """
                                    SELECT id, title, content, source, 0.15 AS score
                                    FROM documents
                                    ORDER BY id DESC
                                    LIMIT :n
                                    """
                                ),
                                {"n": max(1, min(3, int(top_k)))},
                            )
                        ).mappings().all()
                        ranked_rows = [dict(r) for r in recent]
                    except Exception:
                        await db.rollback()
                        ranked_rows = []
                    logger.warning(
                        "Vector retrieval degraded",
                        extra={"reason": "empty_after_all_fallbacks", "query": query, "fallback": "recent_docs"},
                    )

        final_rows = ranked_rows[: int(top_k)]
        return [
            DocumentChunk(
                id=int(r["id"]),
                title=r.get("title"),
                content=r.get("content") or "",
                source=r.get("source"),
                score=float(r.get("score") or 0.0),
            )
            for r in final_rows
        ]


vector_retriever = VectorRetriever()

