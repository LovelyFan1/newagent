from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Any

import httpx
from openai import AsyncOpenAI


logger = logging.getLogger(__name__)


class LLMTimeoutError(RuntimeError):
    pass


class LLMCallError(RuntimeError):
    pass


@dataclass
class LLMResult:
    content: str
    model: str
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None


class LLMGateway:
    def __init__(self, *, timeout_s: float = 20.0, max_retries: int = 3):
        env_timeout = os.environ.get("LLM_TIMEOUT_S")
        env_retries = os.environ.get("LLM_MAX_RETRIES")
        self.timeout_s = float(env_timeout) if env_timeout else timeout_s
        self.max_retries = int(env_retries) if env_retries else max_retries
        self.model = os.environ.get("LLM_MODEL") or os.environ.get("LLM_MODEL_NAME") or "moonshot-v1-8k"
        self.base_url = os.environ.get("LLM_BASE_URL", "").strip() or None
        api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("LLM_API_KEY")
        self._enabled = bool(api_key and api_key.strip())
        self._client = (
            AsyncOpenAI(
                api_key=api_key,
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout_s, connect=5.0),
            )
            if self._enabled
            else None
        )
        if not self._enabled:
            logger.warning("LLM disabled: missing OPENAI_API_KEY/LLM_API_KEY; will use offline reports.")

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def chat(
        self,
        *,
        system: str,
        user: str,
        model: str | None = None,
        temperature: float = 0.2,
        timeout: float | None = 20.0,
        max_tokens: int = 800,
    ) -> LLMResult:
        if not self._client:
            raise LLMCallError("LLM not configured (OPENAI_API_KEY/LLM_API_KEY is missing)")
        model_name = model or self.model

        last_err: Exception | None = None
        call_timeout = float(timeout) if timeout is not None else self.timeout_s
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = await asyncio.wait_for(
                    self._client.chat.completions.create(
                        model=model_name,
                        temperature=temperature,
                        max_tokens=max_tokens,
                        messages=[
                            {"role": "system", "content": system},
                            {"role": "user", "content": user},
                        ],
                    ),
                    timeout=call_timeout,
                )
                logger.warning(
                    "[LLM RAW RESPONSE] status=%s",
                    getattr(resp.choices[0], "finish_reason", None) if getattr(resp, "choices", None) else None,
                )
                logger.warning(
                    "[LLM RAW CONTENT] <<<%s>>>",
                    (resp.choices[0].message.content if getattr(resp, "choices", None) else None),
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
            except asyncio.TimeoutError as e:
                last_err = e
                logger.warning("llm_call_timeout attempt=%s/%s timeout=%ss", attempt, self.max_retries, call_timeout)
                # timeout should fail fast and be handled by orchestrator fallback
                raise LLMTimeoutError(f"LLM timeout after {call_timeout}s") from e
            except Exception as e:
                last_err = e
                logger.warning("llm_call_failed attempt=%s/%s err=%s", attempt, self.max_retries, type(e).__name__)
                await asyncio.sleep(0.4 * attempt)
        raise LLMCallError("llm_call_failed") from (last_err or RuntimeError("llm_call_failed"))


def default_llm_gateway() -> LLMGateway:
    return LLMGateway()

