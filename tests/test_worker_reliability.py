import json

import httpx

from openai_rq import codec
from openai_rq.backend import HTTPBackend
from openai_rq.worker import Worker


def _ok_app(request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        200, json={"ok": 1}, headers={"content-type": "application/json"}
    )


async def _enqueue(redis, rid: str):
    req = httpx.Request(
        "POST",
        "http://openai-rq.invalid/v1/chat/completions",
        headers={"content-type": "application/json"},
        content=b"{}",
    )
    job = codec.encode_request(req, rid=rid, stream=False)
    await redis.xadd(codec.REQUESTS_STREAM, {"data": json.dumps(job)})


def _worker(redis):
    return Worker(
        redis_client=redis,
        backend=HTTPBackend(
            base_url="http://localhost:8000",
            client=httpx.AsyncClient(transport=httpx.MockTransport(_ok_app)),
        ),
        consumer="live",
        reclaim_min_idle_ms=0,
        max_retries=3,
    )


async def test_reclaim_processes_orphan(async_redis_client):
    # Simulate an orphan: a "dead" consumer reads but never ACKs.
    await async_redis_client.xgroup_create(
        codec.REQUESTS_STREAM, codec.GROUP_DEFAULT, id="0", mkstream=True
    )
    await _enqueue(async_redis_client, "orphan-1")
    await async_redis_client.xreadgroup(
        codec.GROUP_DEFAULT, "dead-consumer", {codec.REQUESTS_STREAM: ">"}, count=1
    )

    worker = _worker(async_redis_client)
    reclaimed = await worker.reclaim_once()
    assert reclaimed >= 1
    raw = await async_redis_client.blpop([codec.result_key("orphan-1")], timeout=5)
    assert raw is not None
    status, _h, body = codec.decode_response(json.loads(raw[1]))
    assert status == 200


async def test_dead_letter_after_max_retries(async_redis_client):
    await async_redis_client.xgroup_create(
        codec.REQUESTS_STREAM, codec.GROUP_DEFAULT, id="0", mkstream=True
    )
    await _enqueue(async_redis_client, "poison-1")
    # Read it, then claim repeatedly under a dead consumer to inflate delivery count.
    await async_redis_client.xreadgroup(
        codec.GROUP_DEFAULT, "dead", {codec.REQUESTS_STREAM: ">"}, count=1
    )
    for _ in range(4):
        pending = await async_redis_client.xpending_range(
            codec.REQUESTS_STREAM, codec.GROUP_DEFAULT, min="-", max="+", count=10
        )
        ids = [p["message_id"] for p in pending]
        if ids:
            await async_redis_client.xclaim(
                codec.REQUESTS_STREAM,
                codec.GROUP_DEFAULT,
                "dead",
                min_idle_time=0,
                message_ids=ids,
            )

    worker = _worker(async_redis_client)
    await worker.reclaim_once()

    # poison routed to dead-letter and a 502 result delivered so the client unblocks
    dl = await async_redis_client.xlen(codec.DEAD_LETTER_STREAM)
    assert dl >= 1
    raw = await async_redis_client.blpop([codec.result_key("poison-1")], timeout=5)
    assert raw is not None
    status, _h, _b = codec.decode_response(json.loads(raw[1]))
    assert status == 502
