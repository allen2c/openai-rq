import httpx

from openai_rq.backend import HTTPBackend


def _mock_app(request: httpx.Request) -> httpx.Response:
    if request.url.path == "/v1/chat/completions":
        assert request.url.host == "localhost"
        return httpx.Response(
            200,
            json={"echo": request.content.decode()},
            headers={"content-type": "application/json"},
        )
    if request.url.path == "/v1/stream":
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=b"data: a\n\ndata: b\n\n",
        )
    return httpx.Response(404)


async def test_relay_non_stream():
    client = httpx.AsyncClient(transport=httpx.MockTransport(_mock_app))
    backend = HTTPBackend(base_url="http://localhost:8000/v1", client=client)
    status, headers, body = await backend.relay(
        method="POST",
        path="/v1/chat/completions",
        headers={"content-type": "application/json"},
        body=b'{"m":1}',
    )
    assert status == 200
    assert headers["content-type"] == "application/json"
    assert b'"echo"' in body


async def test_relay_stream_yields_status_then_bytes():
    client = httpx.AsyncClient(transport=httpx.MockTransport(_mock_app))
    backend = HTTPBackend(base_url="http://localhost:8000", client=client)
    async with backend.relay_stream(
        method="POST",
        path="/v1/stream",
        headers={},
        body=b"{}",
    ) as (status, headers, body_iter):
        assert status == 200
        assert headers["content-type"] == "text/event-stream"
        chunks = [c async for c in body_iter]
    assert b"".join(chunks) == b"data: a\n\ndata: b\n\n"
