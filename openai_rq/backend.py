"""Backend abstraction. v1 = HTTP relay to a local OpenAI-compatible server."""

from __future__ import annotations

import contextlib
from typing import AsyncIterator, Protocol

import httpx


class Backend(Protocol):
    async def relay(
        self, *, method: str, path: str, headers: dict, body: bytes
    ) -> tuple[int, dict, bytes]: ...

    def relay_stream(self, *, method: str, path: str, headers: dict, body: bytes): ...


class HTTPBackend:
    """Relay to a local OpenAI-compatible server.

    Constructor params mirror ``openai.OpenAI(base_url=, api_key=, default_headers=)``:
    the worker is, in effect, an OpenAI client pointed at the backend. The credential
    is OWNED by the worker — it is injected here and never transits Redis or the client.

    - ``api_key``       -> ``Authorization: Bearer <key>`` (replaces the client's dummy)
    - ``default_headers`` -> fixed headers merged on every relay; covers non-Bearer auth
      such as servers that use an ``api-key: <key>`` header instead of Bearer.

    Uses only the ORIGIN of ``base_url``; the request path (which already includes
    ``/v1``) is appended verbatim.
    """

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str | None = None,
        default_headers: dict[str, str] | None = None,
        client: httpx.AsyncClient | None = None,
    ):
        origin = httpx.URL(base_url)
        self._base = httpx.URL(scheme=origin.scheme, host=origin.host, port=origin.port)
        self._api_key = api_key
        self._default_headers = dict(default_headers) if default_headers else None
        self._client = client or httpx.AsyncClient(timeout=None)

    def _target(self, path: str) -> httpx.URL:
        return self._base.copy_with(raw_path=path.encode("ascii"))

    def _apply_auth(self, headers: dict) -> dict:
        out = dict(headers)
        if self._api_key:
            # replace any forwarded Authorization (the client's dummy "Bearer unused")
            out = {k: v for k, v in out.items() if k.lower() != "authorization"}
            out["Authorization"] = f"Bearer {self._api_key}"
        if self._default_headers:
            lowered = {k.lower() for k in self._default_headers}
            out = {k: v for k, v in out.items() if k.lower() not in lowered}
            out.update(self._default_headers)
        return out

    async def relay(self, *, method, path, headers, body) -> tuple[int, dict, bytes]:
        resp = await self._client.request(
            method, self._target(path), headers=self._apply_auth(headers), content=body
        )
        return resp.status_code, dict(resp.headers), resp.content

    @contextlib.asynccontextmanager
    async def relay_stream(
        self, *, method, path, headers, body
    ) -> AsyncIterator[tuple[int, dict, AsyncIterator[bytes]]]:
        req = self._client.build_request(
            method, self._target(path), headers=self._apply_auth(headers), content=body
        )
        resp = await self._client.send(req, stream=True)
        try:
            yield resp.status_code, dict(resp.headers), resp.aiter_bytes()
        finally:
            await resp.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()
