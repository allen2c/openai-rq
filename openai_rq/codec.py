"""Shared wire contract between client and worker. Pure functions, no I/O."""

from __future__ import annotations

import base64
import json
from typing import Any

import httpx

REQUESTS_STREAM = "openai-rq:requests"
DEAD_LETTER_STREAM = "openai-rq:dead-letter"
GROUP_DEFAULT = "openai-rq"

# Hop-by-hop / host-specific headers that must NOT be forwarded; the relaying
# httpx client recomputes them for the real connection.
_STRIP_REQUEST_HEADERS = {
    "host",
    "content-length",
    "connection",
    "accept-encoding",
    "transfer-encoding",
}


def result_key(rid: str) -> str:
    return f"openai-rq:result:{rid}"


def stream_key(rid: str) -> str:
    return f"openai-rq:stream:{rid}"


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _unb64(text: str) -> bytes:
    return base64.b64decode(text.encode("ascii"))


def _filter_headers(headers: httpx.Headers) -> dict[str, str]:
    return {k: v for k, v in headers.items() if k.lower() not in _STRIP_REQUEST_HEADERS}


def encode_request(req: httpx.Request, *, rid: str, stream: bool) -> dict[str, Any]:
    return {
        "id": rid,
        "method": req.method,
        "path": req.url.raw_path.decode("ascii"),  # path + ?query, leading /
        "headers": _filter_headers(req.headers),
        "body_b64": _b64(req.content),
        "stream": stream,
    }


def decode_request(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": job["id"],
        "method": job["method"],
        "path": job["path"],
        "headers": dict(job["headers"]),
        "body": _unb64(job["body_b64"]),
        "stream": bool(job["stream"]),
    }


# Response headers that must be dropped: the client's httpx recomputes framing,
# and we ship already-decoded bytes so any content-encoding no longer applies.
_STRIP_RESPONSE_HEADERS = {
    "content-length",
    "transfer-encoding",
    "content-encoding",
    "connection",
}


def _filter_response_headers(headers: dict[str, str]) -> dict[str, str]:
    return {
        k: v for k, v in headers.items() if k.lower() not in _STRIP_RESPONSE_HEADERS
    }


def encode_response(
    *, status: int, headers: dict[str, str], body: bytes
) -> dict[str, Any]:
    return {
        "status": status,
        "headers": _filter_response_headers(headers),
        "body_b64": _b64(body),
    }


def decode_response(payload: dict[str, Any]) -> tuple[int, dict[str, str], bytes]:
    return (
        int(payload["status"]),
        dict(payload["headers"]),
        _unb64(payload["body_b64"]),
    )


def stream_head(*, status: int, headers: dict[str, str]) -> dict[str, Any]:
    return {"t": "head", "status": status, "headers": _filter_response_headers(headers)}


def stream_data(data: bytes) -> dict[str, Any]:
    return {"t": "data", "b": _b64(data)}


def stream_done() -> dict[str, Any]:
    return {"t": "done"}


def stream_error(
    *, status: int, headers: dict[str, str], body: bytes
) -> dict[str, Any]:
    return {
        "t": "error",
        "status": status,
        "headers": _filter_response_headers(headers),
        "b": _b64(body),
    }


def stream_data_bytes(entry: dict[str, Any]) -> bytes:
    return _unb64(entry["b"])


def encode_stream_field(entry: dict[str, Any]) -> dict[str, str]:
    """Wrap an entry dict into the redis XADD field map."""
    return {"data": json.dumps(entry)}


def _coerce_str(value: Any) -> str:
    return value.decode() if isinstance(value, (bytes, bytearray)) else value


def parse_stream_field(fields: dict[Any, Any]) -> dict[str, Any]:
    """Reverse of encode_stream_field; tolerates bytes keys/values from redis-py."""
    decoded = {_coerce_str(k): _coerce_str(v) for k, v in fields.items()}
    return json.loads(decoded["data"])
