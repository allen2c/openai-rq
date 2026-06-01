import json
import threading

import httpx

from openai_rq import codec
from openai_rq.transport import RedisQueueTransport


def _fake_worker_once(redis, *, respond):
    """Read one job off the requests stream and write a response (test double)."""
    entries = redis.xread({codec.REQUESTS_STREAM: "0"}, count=1, block=3000)
    assert entries, "no job arrived"
    _stream, items = entries[0]
    _id, fields = items[0]
    job = json.loads(fields[b"data"])
    rid = job["id"]
    redis.rpush(codec.result_key(rid), json.dumps(respond(job)))


def test_non_stream_request_roundtrip(redis_client):
    transport = RedisQueueTransport(redis_client=redis_client, result_ttl_s=60)

    def respond(job):
        assert job["path"] == "/v1/chat/completions"
        return codec.encode_response(
            status=200,
            headers={"content-type": "application/json"},
            body=b'{"ok":true}',
        )

    t = threading.Thread(
        target=_fake_worker_once, args=(redis_client,), kwargs={"respond": respond}
    )
    t.start()

    client = httpx.Client(transport=transport)
    resp = client.post(
        "http://openai-rq.invalid/v1/chat/completions",
        content=b'{"model":"x"}',
        headers={"content-type": "application/json"},
    )
    t.join(timeout=5)

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


def test_non_stream_timeout_raises(redis_client):
    transport = RedisQueueTransport(redis_client=redis_client, result_ttl_s=60)
    client = httpx.Client(transport=transport)
    try:
        client.post(
            "http://openai-rq.invalid/v1/chat/completions",
            content=b"{}",
            headers={"content-type": "application/json"},
            timeout=0.3,  # no worker -> BLPOP times out
        )
        assert False, "expected a timeout"
    except httpx.ReadTimeout:
        pass
