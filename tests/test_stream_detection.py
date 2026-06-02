"""Streaming must be detected from the request BODY, not the Accept header.

Regression: the OpenAI SDK sends `Accept: application/json` even when
stream=True (it signals streaming only via `"stream": true` in the body), so
detecting on the Accept header routed every stream request down the non-stream
BLPOP path. These tests build requests with the REAL SDK so they reflect actual
on-the-wire behaviour.
"""

from __future__ import annotations

import httpx
import openai

from openai_rq.transport import _is_stream


def _sdk_request(*, stream: bool) -> httpx.Request:
    captured: dict[str, httpx.Request] = {}

    def capture(req: httpx.Request) -> httpx.Response:
        captured["req"] = req
        # A stream request needs an SSE body so the SDK can parse it; a
        # non-stream request needs JSON. Either way we only care about the
        # request the SDK built.
        if stream:
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                content=b"data: [DONE]\n\n",
            )
        return httpx.Response(200, json={"choices": []})

    client = openai.OpenAI(
        api_key="x",
        base_url="http://x.invalid/v1",
        http_client=httpx.Client(transport=httpx.MockTransport(capture)),
    )
    result = client.chat.completions.create(
        model="m", messages=[{"role": "user", "content": "hi"}], stream=stream
    )
    if stream:
        for _ in result:
            pass
    req = captured["req"]
    req.read()
    return req


def test_sdk_stream_request_is_detected():
    req = _sdk_request(stream=True)
    # The SDK does NOT send text/event-stream; prove the regression assumption.
    assert req.headers.get("accept") == "application/json"
    assert _is_stream(req) is True


def test_sdk_non_stream_request_is_not_detected():
    assert _is_stream(_sdk_request(stream=False)) is False


def test_get_request_without_body_is_not_stream():
    req = httpx.Request("GET", "http://x.invalid/v1/models")
    req.read()
    assert _is_stream(req) is False


def test_accept_event_stream_header_is_detected():
    # A client that signals streaming the other way (SSE Accept header, no
    # stream flag in the body) is also honoured.
    req = httpx.Request(
        "POST",
        "http://x.invalid/v1/chat/completions",
        headers={"accept": "text/event-stream"},
        content=b"{}",
    )
    req.read()
    assert _is_stream(req) is True
