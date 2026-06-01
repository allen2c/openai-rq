"""Backend auth injection: the worker owns the backend credential; it is set on
HTTPBackend (never transits Redis / the client)."""

import httpx

from openai_rq.backend import HTTPBackend


def _capture():
    seen = {}

    def app(request: httpx.Request) -> httpx.Response:
        seen["headers"] = dict(request.headers)
        return httpx.Response(
            200, json={}, headers={"content-type": "application/json"}
        )

    return seen, app


async def test_api_key_injected_as_bearer_and_dummy_stripped():
    seen, app = _capture()
    backend = HTTPBackend(
        base_url="http://localhost:8000/v1",
        api_key="realkey",
        client=httpx.AsyncClient(transport=httpx.MockTransport(app)),
    )
    await backend.relay(
        method="POST",
        path="/v1/chat/completions",
        headers={"authorization": "Bearer unused", "content-type": "application/json"},
        body=b"{}",
    )
    # the client's dummy Bearer is replaced by the worker's real key, exactly once
    assert seen["headers"]["authorization"] == "Bearer realkey"


async def test_default_headers_inject_custom_api_key_header():
    seen, app = _capture()
    backend = HTTPBackend(
        base_url="http://localhost:8000",
        api_key=None,
        default_headers={"api-key": "azkey"},
        client=httpx.AsyncClient(transport=httpx.MockTransport(app)),
    )
    await backend.relay(
        method="POST",
        path="/v1/chat/completions",
        headers={"content-type": "application/json"},
        body=b"{}",
    )
    assert seen["headers"]["api-key"] == "azkey"


async def test_default_headers_override_case_insensitively():
    seen, app = _capture()
    backend = HTTPBackend(
        base_url="http://localhost:8000",
        default_headers={"X-Trace": "on"},
        client=httpx.AsyncClient(transport=httpx.MockTransport(app)),
    )
    await backend.relay(
        method="POST", path="/v1/x", headers={"x-trace": "off"}, body=b"{}"
    )
    # the forwarded x-trace is replaced, not duplicated
    assert seen["headers"]["x-trace"] == "on"


async def test_no_auth_config_passes_headers_through():
    # backward-compat + the free client-side extra_headers passthrough: with no
    # backend credential configured, forwarded headers are untouched.
    seen, app = _capture()
    backend = HTTPBackend(
        base_url="http://localhost:8000",
        client=httpx.AsyncClient(transport=httpx.MockTransport(app)),
    )
    await backend.relay(
        method="POST",
        path="/v1/x",
        headers={"authorization": "Bearer unused", "x-custom": "abc"},
        body=b"{}",
    )
    assert seen["headers"]["authorization"] == "Bearer unused"
    assert seen["headers"]["x-custom"] == "abc"
