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
        env_timeout = os.environ.get("LLM_TIMEOUT_S")
        env_retries = os.environ.get("LLM_MAX_RETRIES")
        self.timeout_s = float(env_timeout) if env_timeout else timeout_s
        self.max_retries = int(env_retries) if env_retries else max_retries
        self.model = os.environ.get("LLM_MODEL") or os.environ.get("LLM_MODEL_NAME") or "moonshot-v1-8k"
        self.base_url = os.environ.get("LLM_BASE_URL", "").strip() or None
        api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("LLM_API_KEY")
        self._enabled = bool(api_key and api_key.strip())
        self._client = AsyncOpenAI(api_key=api_key, base_url=self.base_url) if self._enabled else None
        if not self._enabled:
            logger.warning("LLM disabled: missing OPENAI_API_KEY/LLM_API_KEY; will use offline reports.")

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def chat(self, *, system: str, user: str, model: str | None = None, temperature: float = 0.2) -> LLMResult:
        if not self._client:
            raise RuntimeError("LLM not configured (OPENAI_API_KEY/LLM_API_KEY is missing)")
        model_name = model or self.model

        last_err: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = await asyncio.wait_for(
                    self._client.chat.completions.create(
                        model=model_name,
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
                    model=model_name,
                    prompt_tokens=getattr(usage, "prompt_tokens", None) if usage else None,
                    completion_tokens=getattr(usage, "completion_tokens", None) if usage else None,
                    total_tokens=getattr(usage, "total_tokens", None) if usage else None,
                )
                if result.total_tokens is not None:
                    logger.info(
                        "llm_call model=%s tokens=%s (prompt=%s completion=%s)",
                        model_name,
                        result.total_tokens,
                        result.prompt_tokens,
                        result.completion_tokens,
                    )
                return result
            except Exception as e:
                last_err = e
                logger.warning("llm_call_failed attempt=%s/%s err=%s", attempt, self.max_retries, type(e).__name__)
                if attempt < self.max_retries:
                    # Simple exponential backoff: 0.5s, 1s, 2s, 4s...
                    await asyncio.sleep(0.5 * (2 ** (attempt - 1)))
        raise last_err or RuntimeError("llm_call_failed")


def default_llm_gateway() -> LLMGateway:
    return LLMGateway()

