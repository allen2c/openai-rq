"""OpenAI SDK subclasses that route every request through Redis."""

from __future__ import annotations

import httpx
import openai

from .transport import AsyncRedisQueueTransport, RedisQueueTransport

_PLACEHOLDER_BASE_URL = "http://openai-rq.invalid/v1"


class OpenAIRQ(openai.OpenAI):
    def __init__(
        self,
        *,
        redis_url=None,
        redis_client=None,
        requests_maxlen=10_000,
        result_ttl_s=600,
        **kwargs,
    ):
        transport = RedisQueueTransport(
            redis_url=redis_url,
            redis_client=redis_client,
            requests_maxlen=requests_maxlen,
            result_ttl_s=result_ttl_s,
        )
        kwargs.setdefault("api_key", "unused")
        kwargs.setdefault("base_url", _PLACEHOLDER_BASE_URL)
        kwargs["max_retries"] = 0  # retries are owned by the queue/worker
        kwargs["http_client"] = httpx.Client(transport=transport)
        super().__init__(**kwargs)


class AsyncOpenAIRQ(openai.AsyncOpenAI):
    def __init__(
        self,
        *,
        redis_url=None,
        redis_client=None,
        requests_maxlen=10_000,
        result_ttl_s=600,
        **kwargs,
    ):
        transport = AsyncRedisQueueTransport(
            redis_url=redis_url,
            redis_client=redis_client,
            requests_maxlen=requests_maxlen,
            result_ttl_s=result_ttl_s,
        )
        kwargs.setdefault("api_key", "unused")
        kwargs.setdefault("base_url", _PLACEHOLDER_BASE_URL)
        kwargs["max_retries"] = 0
        kwargs["http_client"] = httpx.AsyncClient(transport=transport)
        super().__init__(**kwargs)
