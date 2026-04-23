from __future__ import annotations

import asyncio
import hashlib
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
        cand_k = 10
        threshold = float(os.environ.get("RAG_VECTOR_SIM_THRESHOLD", "0.5"))
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

        async def _ensure_pg_trgm(db) -> None:
            try:
                await db.execute(sa.text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
                if hasattr(db, "commit"):
                    await db.commit()
            except Exception:
                if hasattr(db, "rollback"):
                    await db.rollback()

        async def _vector_strategy() -> list[dict]:
            try:
                vec_literal = None
                if embedding_service._client is None:
                    logger.warning(
                        "Vector retrieval degraded",
                        extra={"reason": "embedding_client_unavailable", "query": query, "fallback": "trgm+keyword+recent"},
                    )
                vecs = await embedding_service.embed([query])
                if vecs:
                    vec = vecs[0]
                    vec_literal = "[" + ",".join(f"{x:.8f}" for x in vec) + "]"
                if vec_literal is None:
                    return []

                async with sm() as db:
                    rows = [dict(r) for r in (await db.execute(vector_sql, {"vec": vec_literal, "cand_k": cand_k})).mappings().all()]
                top_score = float(rows[0].get("score") or 0.0) if rows else 0.0
                if top_score < threshold:
                    logger.warning(
                        "Vector retrieval degraded",
                        extra={"reason": "low_vector_similarity", "query": query, "fallback": "trgm+keyword+recent"},
                    )
                return rows
            except Exception:
                logger.warning(
                    "Vector retrieval degraded",
                    extra={"reason": "pgvector_query_failed", "query": query, "fallback": "trgm+keyword+recent"},
                )
                return []

        async def _trigram_strategy() -> list[dict]:
            try:
                async with sm() as db:
                    await _ensure_pg_trgm(db)
                    return [dict(r) for r in (await db.execute(trigram_sql, {"q": query, "cand_k": cand_k})).mappings().all()]
            except Exception:
                logger.warning(
                    "Vector retrieval degraded",
                    extra={"reason": "trigram_query_failed", "query": query, "fallback": "keyword+recent"},
                )
                return []

        async def _keyword_strategy() -> list[dict]:
            tokens = self._keyword_tokens(query)
            if not tokens:
                return []
            where_clauses = []
            params: dict[str, object] = {"cand_k": cand_k}
            for i, tok in enumerate(tokens):
                key = f"t{i}"
                where_clauses.append(f"(title ILIKE :{key} OR content ILIKE :{key})")
                params[key] = f"%{tok}%"
            if not where_clauses:
                return []

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
                async with sm() as db:
                    return [dict(r) for r in (await db.execute(kw_sql, params)).mappings().all()]
            except Exception:
                logger.warning(
                    "Vector retrieval degraded",
                    extra={"reason": "keyword_query_failed", "query": query, "fallback": "recent_docs"},
                )
                return []

        async def _recent_strategy() -> list[dict]:
            try:
                async with sm() as db:
                    kb_cnt = await db.execute(sa.text("SELECT COUNT(*)::int FROM documents"))
                    total_docs = int(kb_cnt.scalar() or 0)
                    if total_docs <= 0:
                        return []
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
                    return [dict(r) for r in recent]
            except Exception:
                logger.warning(
                    "Vector retrieval degraded",
                    extra={"reason": "recent_docs_failed", "query": query},
                )
                return []

        vec_rows, trigram_rows, kw_rows, recent_rows = await asyncio.gather(
            _vector_strategy(),
            _trigram_strategy(),
            _keyword_strategy(),
            _recent_strategy(),
        )

        # merge + dedupe (id first; then content hash), keep highest score
        merged_by_id: dict[int, dict] = {}
        for r in vec_rows:
            rid = int(r["id"])
            merged_by_id[rid] = {
                "id": rid,
                "title": r.get("title"),
                "content": r.get("content") or "",
                "source": r.get("source"),
                "score": 0.65 * float(r.get("score") or 0.0),
            }
        for r in trigram_rows:
            rid = int(r["id"])
            weighted = 0.35 * float(r.get("score") or 0.0)
            if rid in merged_by_id:
                merged_by_id[rid]["score"] = max(float(merged_by_id[rid]["score"]), float(merged_by_id[rid]["score"]) + weighted)
            else:
                merged_by_id[rid] = {
                    "id": rid,
                    "title": r.get("title"),
                    "content": r.get("content") or "",
                    "source": r.get("source"),
                    "score": weighted,
                }
        for r in kw_rows:
            rid = int(r["id"])
            if rid in merged_by_id:
                merged_by_id[rid]["score"] = max(float(merged_by_id[rid]["score"]), 0.35)
            else:
                merged_by_id[rid] = {
                    "id": rid,
                    "title": r.get("title"),
                    "content": r.get("content") or "",
                    "source": r.get("source"),
                    "score": float(r.get("score") or 0.35),
                }
        for r in recent_rows:
            rid = int(r["id"])
            if rid in merged_by_id:
                merged_by_id[rid]["score"] = max(float(merged_by_id[rid]["score"]), float(r.get("score") or 0.15))
            else:
                merged_by_id[rid] = {
                    "id": rid,
                    "title": r.get("title"),
                    "content": r.get("content") or "",
                    "source": r.get("source"),
                    "score": float(r.get("score") or 0.15),
                }

        # second-pass dedupe by content hash (avoid near-duplicate docs with different ids)
        merged: list[dict] = []
        seen_hash: dict[str, dict] = {}
        for r in merged_by_id.values():
            content = (r.get("content") or "").strip()
            h = hashlib.sha1(content.encode("utf-8")).hexdigest() if content else f"id:{int(r['id'])}"
            existing = seen_hash.get(h)
            if existing is None or float(r.get("score") or 0.0) > float(existing.get("score") or 0.0):
                seen_hash[h] = r
        merged = list(seen_hash.values())

        ranked_rows = sorted(merged, key=lambda x: float(x.get("score") or 0.0), reverse=True)

        if not ranked_rows:
            logger.warning(
                "Vector retrieval degraded",
                extra={"reason": "empty_after_all_strategies", "query": query, "fallback": "none"},
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

