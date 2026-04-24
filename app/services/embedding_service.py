from __future__ import annotations

import asyncio
from collections import OrderedDict
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
        self._cache_maxsize = int(os.environ.get("EMBEDDING_CACHE_MAXSIZE", "1000"))
        self._cache: OrderedDict[str, List[float]] = OrderedDict()
        self._cache_lock = asyncio.Lock()

    def _cache_key(self, text: str) -> str:
        raw = f"{self.model}|{self.dimension}|{text or ''}".encode("utf-8")
        return hashlib.md5(raw).hexdigest()

    async def _cache_get_many(self, keys: list[str]) -> dict[str, List[float]]:
        if not keys:
            return {}
        async with self._cache_lock:
            out: dict[str, List[float]] = {}
            for k in keys:
                v = self._cache.get(k)
                if v is not None:
                    # refresh LRU order
                    self._cache.move_to_end(k, last=True)
                    out[k] = v
            return out

    async def _cache_put_many(self, items: list[tuple[str, List[float]]]) -> None:
        if not items:
            return
        async with self._cache_lock:
            for k, v in items:
                self._cache[k] = v
                self._cache.move_to_end(k, last=True)
            # evict oldest
            while self._cache_maxsize > 0 and len(self._cache) > self._cache_maxsize:
                self._cache.popitem(last=False)

    async def embed(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        if self._client is None:
            return [self._hash_embedding(t) for t in texts]

        keys = [self._cache_key(t) for t in texts]
        cached = await self._cache_get_many(keys)

        missing_texts: list[str] = []
        missing_keys: list[str] = []
        for t, k in zip(texts, keys):
            if k not in cached:
                missing_texts.append(t)
                missing_keys.append(k)

        # batch call for misses only
        try:
            if missing_texts:
                resp = await self._client.embeddings.create(model=self.model, input=missing_texts)
                vectors = [self._fit_dim(list(item.embedding)) for item in resp.data]
                await self._cache_put_many(list(zip(missing_keys, vectors)))

            # restore original order
            cached2 = await self._cache_get_many(keys)
            return [cached2[k] for k in keys]
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

