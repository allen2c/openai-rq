import asyncio
import json

import httpx
import pytest_asyncio

from openai_rq import AsyncOpenAIRQ
from openai_rq.backend import HTTPBackend
from openai_rq.worker import Worker


def _vllm(request: httpx.Request) -> httpx.Response:
    if request.url.path == "/v1/chat/completions":
        payload = json.loads(request.content)
        if payload.get("stream"):
            sse = (
                b'data: {"id":"1","object":"chat.completion.chunk","created":0,"model":"x",'
                b'"choices":[{"index":0,"delta":{"content":"Hel"},"finish_reason":null}]}\n\n'
                b'data: {"id":"1","object":"chat.completion.chunk","created":0,"model":"x",'
                b'"choices":[{"index":0,"delta":{"content":"lo"},"finish_reason":"stop"}]}\n\n'
                b"data: [DONE]\n\n"
            )
            return httpx.Response(
                200, headers={"content-type": "text/event-stream"}, content=sse
            )
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json={
                "id": "1",
                "object": "chat.completion",
                "created": 0,
                "model": "x",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "Hello"},
                        "finish_reason": "stop",
                    }
                ],
            },
        )
    return httpx.Response(404)


@pytest_asyncio.fixture
async def running_worker(async_redis_client):
    backend = HTTPBackend(
        base_url="http://localhost:8000",
        client=httpx.AsyncClient(transport=httpx.MockTransport(_vllm)),
    )
    worker = Worker(
        redis_client=async_redis_client,
        backend=backend,
        consumer="e2e",
        stream_flush_ms=10,
        result_ttl_s=60,
    )
    await worker.ensure_group()
    task = asyncio.create_task(worker.run())
    yield
    worker.stop()
    await asyncio.wait_for(task, timeout=5)


async def test_e2e_non_stream(async_redis_client, running_worker):
    client = AsyncOpenAIRQ(redis_client=async_redis_client)
    completion = await client.chat.completions.create(
        model="x", messages=[{"role": "user", "content": "hi"}]
    )
    assert completion.choices[0].message.content == "Hello"


async def test_e2e_streaming(async_redis_client, running_worker):
    client = AsyncOpenAIRQ(redis_client=async_redis_client)
    stream = await client.chat.completions.create(
        model="x", messages=[{"role": "user", "content": "hi"}], stream=True
    )
    text = ""
    async for chunk in stream:
        if chunk.choices and chunk.choices[0].delta.content:
            text += chunk.choices[0].delta.content
    assert text == "Hello"
