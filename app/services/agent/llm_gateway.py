from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Any

from openai import AsyncOpenAI


logger = logging.getLogger(__name__)


@dataclass
class LLMResult:
    content: str
    model: str
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None


class LLMGateway:
    def __init__(self, *, timeout_s: float = 30.0, max_retries: int = 3):
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("LLM_API_KEY")
        self._enabled = bool(api_key and api_key.strip())
        self._client = AsyncOpenAI(api_key=api_key) if self._enabled else None

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def chat(self, *, system: str, user: str, model: str = "gpt-4o-mini", temperature: float = 0.2) -> LLMResult:
        if not self._client:
            raise RuntimeError("LLM not configured (OPENAI_API_KEY/LLM_API_KEY is missing)")

        last_err: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = await asyncio.wait_for(
                    self._client.chat.completions.create(
                        model=model,
                        temperature=temperature,
                        messages=[
                            {"role": "system", "content": system},
                            {"role": "user", "content": user},
                        ],
                    ),
                    timeout=self.timeout_s,
                )
                content = (resp.choices[0].message.content or "").strip()
                usage = getattr(resp, "usage", None)
                result = LLMResult(
                    content=content,
                    model=model,
                    prompt_tokens=getattr(usage, "prompt_tokens", None) if usage else None,
                    completion_tokens=getattr(usage, "completion_tokens", None) if usage else None,
                    total_tokens=getattr(usage, "total_tokens", None) if usage else None,
                )
                if result.total_tokens is not None:
                    logger.info(
                        "llm_call model=%s tokens=%s (prompt=%s completion=%s)",
                        model,
                        result.total_tokens,
                        result.prompt_tokens,
                        result.completion_tokens,
                    )
                return result
            except Exception as e:
                last_err = e
                logger.warning("llm_call_failed attempt=%s/%s err=%s", attempt, self.max_retries, type(e).__name__)
                await asyncio.sleep(0.4 * attempt)
        raise last_err or RuntimeError("llm_call_failed")


def default_llm_gateway() -> LLMGateway:
    return LLMGateway()

