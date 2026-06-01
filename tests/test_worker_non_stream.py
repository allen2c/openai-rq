import asyncio
import json

import httpx

from openai_rq import codec
from openai_rq.backend import HTTPBackend
from openai_rq.worker import Worker


def _mock_app(request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        200,
        json={"echo": json.loads(request.content)},
        headers={"content-type": "application/json"},
    )


async def _enqueue(redis, body: bytes, stream: bool, rid: str):
    req = httpx.Request(
        "POST",
        "http://openai-rq.invalid/v1/chat/completions",
        headers={"content-type": "application/json"},
        content=body,
    )
    job = codec.encode_request(req, rid=rid, stream=stream)
    await redis.xadd(codec.REQUESTS_STREAM, {"data": json.dumps(job)})


async def test_worker_relays_non_stream(async_redis_client):
    backend = HTTPBackend(
        base_url="http://localhost:8000",
        client=httpx.AsyncClient(transport=httpx.MockTransport(_mock_app)),
    )
    worker = Worker(
        redis_client=async_redis_client,
        backend=backend,
        result_ttl_s=60,
        consumer="test-1",
    )
    await worker.ensure_group()
    await _enqueue(async_redis_client, b'{"hello":1}', stream=False, rid="r1")

    runner = asyncio.create_task(worker.run())
    raw = await async_redis_client.blpop([codec.result_key("r1")], timeout=5)
    worker.stop()
    await asyncio.wait_for(runner, timeout=5)

    assert raw is not None
    status, headers, body = codec.decode_response(json.loads(raw[1]))
    assert status == 200
    assert json.loads(body) == {"echo": {"hello": 1}}


async def test_worker_sets_result_ttl(async_redis_client):
    backend = HTTPBackend(
        base_url="http://localhost:8000",
        client=httpx.AsyncClient(transport=httpx.MockTransport(_mock_app)),
    )
    worker = Worker(
        redis_client=async_redis_client,
        backend=backend,
        result_ttl_s=60,
        consumer="test-2",
    )
    await worker.ensure_group()
    await _enqueue(async_redis_client, b"{}", stream=False, rid="r2")
    runner = asyncio.create_task(worker.run())
    # wait until the result key exists, then check its TTL before consuming
    for _ in range(500):
        if await async_redis_client.exists(codec.result_key("r2")):
            break
        await asyncio.sleep(0.01)
    ttl = await async_redis_client.ttl(codec.result_key("r2"))
    worker.stop()
    await asyncio.wait_for(runner, timeout=5)
    assert 0 < ttl <= 60
