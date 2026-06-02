"""httpx transports that ship requests over Redis and await the result."""

from __future__ import annotations

import json
import math
import uuid

import httpx
import redis
import redis.asyncio as aioredis

from . import codec

_DEFAULT_TIMEOUT_S = 600.0  # used when the SDK request carries no read timeout


def _is_stream(request: httpx.Request) -> bool:
    # Accept BOTH streaming conventions, for maximum client compatibility:
    #   1) Accept: text/event-stream  — used by some clients/SDKs.
    #   2) "stream": true in the JSON body — what the OpenAI SDK uses (it sends
    #      Accept: application/json even when streaming).
    # Order matters: check the cheap header first, then fall back to the body.
    # A false negative only costs an extra non-stream relay (still correct); a
    # false positive can't happen since both signals are explicit.
    if request.headers.get("accept", "").startswith("text/event-stream"):
        return True
    if "application/json" not in request.headers.get("content-type", ""):
        return False
    try:
        return bool(json.loads(request.content).get("stream"))
    except (ValueError, TypeError, AttributeError):
        return False


def _read_timeout(request: httpx.Request) -> float:
    timeout = request.extensions.get("timeout") or {}
    value = timeout.get("read", None)
    return _DEFAULT_TIMEOUT_S if value is None else float(value)


def _blpop_timeout(read_timeout: float) -> int:
    # redis BLPOP wants an integer #seconds; 0 == block forever. Round up so we
    # never time out earlier than the SDK asked.
    return max(1, math.ceil(read_timeout))


class RedisQueueTransport(httpx.BaseTransport):
    def __init__(
        self,
        *,
        redis_url: str | None = None,
        redis_client: redis.Redis | None = None,
        requests_stream: str = codec.REQUESTS_STREAM,
        requests_maxlen: int = 10_000,
        result_ttl_s: int = 600,
    ) -> None:
        if redis_client is None:
            if redis_url is None:
                raise ValueError("provide redis_url or redis_client")
            redis_client = redis.Redis.from_url(redis_url)
        self._redis = redis_client
        self._stream = requests_stream
        self._maxlen = requests_maxlen
        self._result_ttl_s = result_ttl_s

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        request.read()  # ensure request.content is materialized
        rid = uuid.uuid4().hex
        stream = _is_stream(request)
        job = codec.encode_request(request, rid=rid, stream=stream)
        self._redis.xadd(
            self._stream,
            {"data": json.dumps(job)},
            maxlen=self._maxlen,
            approximate=True,
        )
        if stream:
            return self._handle_stream(rid, request)
        return self._handle_non_stream(rid, request)

    def _handle_non_stream(self, rid: str, request: httpx.Request) -> httpx.Response:
        timeout = _read_timeout(request)
        popped = self._redis.blpop(
            [codec.result_key(rid)], timeout=_blpop_timeout(timeout)
        )
        if popped is None:
            raise httpx.ReadTimeout(
                "timed out waiting for openai-rq result", request=request
            )
        _key, raw = popped
        status, headers, body = codec.decode_response(json.loads(raw))
        return httpx.Response(
            status_code=status, headers=headers, content=body, request=request
        )

    def _handle_stream(self, rid: str, request: httpx.Request) -> httpx.Response:
        sk = codec.stream_key(rid)
        # Block for the first entry, which must be a head/error carrying status+headers.
        first = self._redis.xread(
            {sk: "0"}, block=_blpop_timeout(_read_timeout(request)) * 1000
        )
        if not first:
            raise httpx.ReadTimeout(
                "timed out waiting for openai-rq stream", request=request
            )
        _key, items = first[0]
        head_id, head_fields = items[0]
        head = codec.parse_stream_field(head_fields)

        if head["t"] == "error":
            # No streaming body; deliver the error body as the whole response.
            return httpx.Response(
                status_code=head["status"],
                headers=head["headers"],
                content=codec.stream_data_bytes(head),
                request=request,
            )

        body = _SyncRedisByteStream(self._redis, sk, head_id)
        return httpx.Response(
            status_code=head["status"],
            headers=head["headers"],
            stream=body,
            request=request,
        )


class _SyncRedisByteStream(httpx.SyncByteStream):
    def __init__(self, redis_client, stream_k, last_id):
        self._redis = redis_client
        self._key = stream_k
        self._last_id = last_id

    def __iter__(self):
        while True:
            resp = self._redis.xread({self._key: self._last_id}, block=0, count=64)
            if not resp:
                continue
            _key, items = resp[0]
            for entry_id, fields in items:
                self._last_id = entry_id
                entry = codec.parse_stream_field(fields)
                kind = entry["t"]
                if kind == "data":
                    yield codec.stream_data_bytes(entry)
                elif kind == "error":
                    yield codec.stream_data_bytes(entry)
                    return
                elif kind == "done":
                    return

    def close(self) -> None:  # nothing to release; redis client is shared
        pass


class _AsyncRedisByteStream(httpx.AsyncByteStream):
    def __init__(self, redis_client, stream_k, last_id):
        self._redis = redis_client
        self._key = stream_k
        self._last_id = last_id

    async def __aiter__(self):
        while True:
            resp = await self._redis.xread(
                {self._key: self._last_id}, block=0, count=64
            )
            if not resp:
                continue
            _key, items = resp[0]
            for entry_id, fields in items:
                self._last_id = entry_id
                entry = codec.parse_stream_field(fields)
                kind = entry["t"]
                if kind == "data":
                    yield codec.stream_data_bytes(entry)
                elif kind == "error":
                    yield codec.stream_data_bytes(entry)
                    return
                elif kind == "done":
                    return

    async def aclose(self) -> None:
        pass


class AsyncRedisQueueTransport(httpx.AsyncBaseTransport):
    def __init__(
        self,
        *,
        redis_url: str | None = None,
        redis_client: "aioredis.Redis | None" = None,
        requests_stream: str = codec.REQUESTS_STREAM,
        requests_maxlen: int = 10_000,
        result_ttl_s: int = 600,
    ) -> None:
        if redis_client is None:
            if redis_url is None:
                raise ValueError("provide redis_url or redis_client")
            redis_client = aioredis.Redis.from_url(redis_url)
        self._redis = redis_client
        self._stream = requests_stream
        self._maxlen = requests_maxlen
        self._result_ttl_s = result_ttl_s

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        await request.aread()
        rid = uuid.uuid4().hex
        stream = _is_stream(request)
        job = codec.encode_request(request, rid=rid, stream=stream)
        await self._redis.xadd(
            self._stream,
            {"data": json.dumps(job)},
            maxlen=self._maxlen,
            approximate=True,
        )
        if stream:
            return await self._handle_stream(rid, request)
        return await self._handle_non_stream(rid, request)

    async def _handle_non_stream(
        self, rid: str, request: httpx.Request
    ) -> httpx.Response:
        timeout = _read_timeout(request)
        popped = await self._redis.blpop(
            [codec.result_key(rid)], timeout=_blpop_timeout(timeout)
        )
        if popped is None:
            raise httpx.ReadTimeout(
                "timed out waiting for openai-rq result", request=request
            )
        _key, raw = popped
        status, headers, body = codec.decode_response(json.loads(raw))
        return httpx.Response(
            status_code=status, headers=headers, content=body, request=request
        )

    async def _handle_stream(self, rid: str, request: httpx.Request) -> httpx.Response:
        sk = codec.stream_key(rid)
        first = await self._redis.xread(
            {sk: "0"}, block=_blpop_timeout(_read_timeout(request)) * 1000
        )
        if not first:
            raise httpx.ReadTimeout(
                "timed out waiting for openai-rq stream", request=request
            )
        head_id, head_fields = first[0][1][0]
        head = codec.parse_stream_field(head_fields)
        if head["t"] == "error":
            return httpx.Response(
                status_code=head["status"],
                headers=head["headers"],
                content=codec.stream_data_bytes(head),
                request=request,
            )
        body = _AsyncRedisByteStream(self._redis, sk, head_id)
        return httpx.Response(
            status_code=head["status"],
            headers=head["headers"],
            stream=body,
            request=request,
        )
