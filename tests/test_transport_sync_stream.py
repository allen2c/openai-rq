import json
import threading

import httpx

from openai_rq import codec
from openai_rq.transport import RedisQueueTransport


def _fake_stream_worker(redis):
    entries = redis.xread({codec.REQUESTS_STREAM: "0"}, count=1, block=3000)
    _stream, items = entries[0]
    job = json.loads(items[0][1][b"data"])
    sk = codec.stream_key(job["id"])
    redis.xadd(
        sk,
        codec.encode_stream_field(
            codec.stream_head(status=200, headers={"content-type": "text/event-stream"})
        ),
    )
    redis.xadd(sk, codec.encode_stream_field(codec.stream_data(b"data: a\n\n")))
    redis.xadd(sk, codec.encode_stream_field(codec.stream_data(b"data: b\n\n")))
    redis.xadd(sk, codec.encode_stream_field(codec.stream_done()))


def test_streaming_roundtrip(redis_client):
    transport = RedisQueueTransport(redis_client=redis_client)
    t = threading.Thread(target=_fake_stream_worker, args=(redis_client,))
    t.start()

    client = httpx.Client(transport=transport)
    with client.stream(
        "POST",
        "http://openai-rq.invalid/v1/chat/completions",
        headers={"accept": "text/event-stream", "content-type": "application/json"},
        content=b'{"stream":true}',
    ) as resp:
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "text/event-stream"
        body = b"".join(resp.iter_bytes())
    t.join(timeout=5)
    assert body == b"data: a\n\ndata: b\n\n"


def test_streaming_error_sentinel_sets_status(redis_client):
    def worker():
        entries = redis_client.xread({codec.REQUESTS_STREAM: "0"}, count=1, block=3000)
        job = json.loads(entries[0][1][0][1][b"data"])
        sk = codec.stream_key(job["id"])
        # Upstream returned a non-2xx before any SSE: head carries the real status,
        # the body arrives as a single error sentinel.
        redis_client.xadd(
            sk,
            codec.encode_stream_field(
                codec.stream_error(
                    status=400,
                    headers={"content-type": "application/json"},
                    body=b'{"error":"bad"}',
                )
            ),
        )

    t = threading.Thread(target=worker)
    t.start()
    client = httpx.Client(transport=RedisQueueTransport(redis_client=redis_client))
    with client.stream(
        "POST",
        "http://openai-rq.invalid/v1/chat/completions",
        headers={"content-type": "application/json"},
        content=b'{"stream":true}',
    ) as resp:
        body = b"".join(resp.iter_bytes())
        assert resp.status_code == 400
        assert body == b'{"error":"bad"}'
    t.join(timeout=5)
