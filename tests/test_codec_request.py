import base64

import httpx

from openai_rq import codec


def test_keys_and_constants():
    assert codec.result_key("abc") == "openai-rq:result:abc"
    assert codec.stream_key("abc") == "openai-rq:stream:abc"
    assert codec.REQUESTS_STREAM == "openai-rq:requests"


def test_encode_decode_request_roundtrip():
    req = httpx.Request(
        "POST",
        "http://openai-rq.invalid/v1/chat/completions",
        headers={"content-type": "application/json", "authorization": "Bearer unused"},
        content=b'{"model":"x","stream":false}',
    )
    job = codec.encode_request(req, rid="job1", stream=False)

    assert job["id"] == "job1"
    assert job["method"] == "POST"
    assert job["path"] == "/v1/chat/completions"
    assert job["stream"] is False
    assert base64.b64decode(job["body_b64"]) == b'{"model":"x","stream":false}'
    # host/content-length are stripped; content-type kept
    assert "host" not in {k.lower() for k in job["headers"]}
    assert job["headers"]["content-type"] == "application/json"

    dec = codec.decode_request(job)
    assert dec["method"] == "POST"
    assert dec["path"] == "/v1/chat/completions"
    assert dec["body"] == b'{"model":"x","stream":false}'
    assert dec["stream"] is False


def test_path_includes_query():
    req = httpx.Request("GET", "http://x.invalid/v1/models?limit=2")
    job = codec.encode_request(req, rid="j", stream=False)
    assert job["path"] == "/v1/models?limit=2"
