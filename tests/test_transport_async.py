import asyncio
import json

import httpx

from openai_rq import codec
from openai_rq.transport import AsyncRedisQueueTransport


async def _async_worker_non_stream(redis):
    while True:
        entries = await redis.xread({codec.REQUESTS_STREAM: "0"}, count=1, block=3000)
        if entries:
            break
    job = json.loads(entries[0][1][0][1][b"data"])
    payload = codec.encode_response(
        status=200, headers={"content-type": "application/json"}, body=b'{"ok":true}'
    )
    await redis.rpush(codec.result_key(job["id"]), json.dumps(payload))


async def test_async_non_stream_roundtrip(async_redis_client):
    transport = AsyncRedisQueueTransport(redis_client=async_redis_client)
    worker = asyncio.create_task(_async_worker_non_stream(async_redis_client))
    async with httpx.AsyncClient(transport=transport) as client:
        resp = await client.post(
            "http://openai-rq.invalid/v1/chat/completions",
            content=b"{}",
            headers={"content-type": "application/json"},
        )
    await worker
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


async def _async_worker_stream(redis):
    while True:
        entries = await redis.xread({codec.REQUESTS_STREAM: "0"}, count=1, block=3000)
        if entries:
            break
    job = json.loads(entries[0][1][0][1][b"data"])
    sk = codec.stream_key(job["id"])
    await redis.xadd(
        sk,
        codec.encode_stream_field(
            codec.stream_head(status=200, headers={"content-type": "text/event-stream"})
        ),
    )
    await redis.xadd(sk, codec.encode_stream_field(codec.stream_data(b"data: a\n\n")))
    await redis.xadd(sk, codec.encode_stream_field(codec.stream_done()))


async def test_async_streaming_roundtrip(async_redis_client):
    transport = AsyncRedisQueueTransport(redis_client=async_redis_client)
    worker = asyncio.create_task(_async_worker_stream(async_redis_client))
    async with httpx.AsyncClient(transport=transport) as client:
        async with client.stream(
            "POST",
            "http://openai-rq.invalid/v1/chat/completions",
            headers={"content-type": "application/json"},
            content=b'{"stream":true}',
        ) as resp:
            assert resp.status_code == 200
            chunks = [c async for c in resp.aiter_bytes()]
    await worker
    assert b"".join(chunks) == b"data: a\n\n"
