from __future__ import annotations

import asyncio
import hashlib
import os
from typing import List

from openai import AsyncOpenAI


class EmbeddingService:
    def __init__(self):
        self.model = os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small")
        self.dimension = int(os.environ.get("EMBEDDING_DIM", "1536"))
        api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("LLM_API_KEY")
        self._client = AsyncOpenAI(api_key=api_key) if api_key else None

    async def embed(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        if self._client is None:
            return [self._hash_embedding(t) for t in texts]

        # batch call
        try:
            resp = await self._client.embeddings.create(model=self.model, input=texts)
            vectors = [list(item.embedding) for item in resp.data]
            # normalize dimension in case model dimension differs from config
            return [self._fit_dim(v) for v in vectors]
        except Exception:
            # offline degrade
            return [self._hash_embedding(t) for t in texts]

    def _fit_dim(self, v: List[float]) -> List[float]:
        if len(v) == self.dimension:
            return v
        if len(v) > self.dimension:
            return v[: self.dimension]
        return v + [0.0] * (self.dimension - len(v))

    def _hash_embedding(self, text: str) -> List[float]:
        """
        Deterministic local fallback embedding, no external dependency.
        """
        seed = hashlib.sha256((text or "").encode("utf-8")).digest()
        out: List[float] = []
        i = 0
        while len(out) < self.dimension:
            h = hashlib.sha256(seed + i.to_bytes(4, "little")).digest()
            for j in range(0, len(h), 4):
                if len(out) >= self.dimension:
                    break
                val = int.from_bytes(h[j : j + 4], "little", signed=False)
                out.append((val / 2**32) * 2 - 1)  # [-1, 1)
            i += 1
        # l2 normalize
        norm = sum(x * x for x in out) ** 0.5 or 1.0
        return [x / norm for x in out]


embedding_service = EmbeddingService()

