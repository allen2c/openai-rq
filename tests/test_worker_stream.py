import asyncio
import json

import httpx

from openai_rq import codec
from openai_rq.backend import HTTPBackend
from openai_rq.worker import Worker


def _sse_app(request: httpx.Request) -> httpx.Response:
    if request.url.path == "/v1/bad":
        return httpx.Response(
            400, json={"error": "nope"}, headers={"content-type": "application/json"}
        )
    return httpx.Response(
        200,
        headers={"content-type": "text/event-stream"},
        content=b"data: a\n\ndata: b\n\ndata: [DONE]\n\n",
    )


async def _enqueue_stream(redis, path: str, rid: str):
    req = httpx.Request(
        "POST",
        f"http://openai-rq.invalid{path}",
        headers={"accept": "text/event-stream"},
        content=b'{"stream":true}',
    )
    job = codec.encode_request(req, rid=rid, stream=True)
    await redis.xadd(codec.REQUESTS_STREAM, {"data": json.dumps(job)})


def _make_worker(redis, app):
    return Worker(
        redis_client=redis,
        backend=HTTPBackend(
            base_url="http://localhost:8000",
            client=httpx.AsyncClient(transport=httpx.MockTransport(app)),
        ),
        consumer="s1",
        stream_flush_ms=20,
        result_ttl_s=60,
    )


async def _drain_stream(redis, rid, *, timeout=5.0):
    sk = codec.stream_key(rid)
    last = "0"
    head = None
    data = b""
    loops = int(timeout / 0.02)
    for _ in range(loops):
        resp = await redis.xread({sk: last}, block=20)
        if not resp:
            continue
        for entry_id, fields in resp[0][1]:
            last = entry_id
            entry = codec.parse_stream_field(fields)
            if entry["t"] == "head":
                head = entry
            elif entry["t"] in ("data", "error"):
                data += codec.stream_data_bytes(entry)
                if entry["t"] == "error":
                    return head, entry, data
            elif entry["t"] == "done":
                return head, entry, data
    raise AssertionError("stream did not terminate")


async def test_worker_streams_coalesced(async_redis_client):
    worker = _make_worker(async_redis_client, _sse_app)
    await worker.ensure_group()
    await _enqueue_stream(async_redis_client, "/v1/chat/completions", "s-ok")
    runner = asyncio.create_task(worker.run())
    head, term, data = await _drain_stream(async_redis_client, "s-ok")
    worker.stop()
    await asyncio.wait_for(runner, timeout=5)
    assert head["status"] == 200
    assert head["headers"]["content-type"] == "text/event-stream"
    assert term["t"] == "done"
    assert data == b"data: a\n\ndata: b\n\ndata: [DONE]\n\n"
    assert await async_redis_client.ttl(codec.stream_key("s-ok")) > 0


async def test_worker_stream_upstream_error(async_redis_client):
    worker = _make_worker(async_redis_client, _sse_app)
    await worker.ensure_group()
    await _enqueue_stream(async_redis_client, "/v1/bad", "s-bad")
    runner = asyncio.create_task(worker.run())
    head, term, data = await _drain_stream(async_redis_client, "s-bad")
    worker.stop()
    await asyncio.wait_for(runner, timeout=5)
    assert term["t"] == "error"
    assert term["status"] == 400
    assert json.loads(data) == {"error": "nope"}
