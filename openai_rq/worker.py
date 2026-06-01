"""Generic OpenAI-HTTP-over-Redis relay worker."""

from __future__ import annotations

import asyncio
import json

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from . import codec
from .backend import Backend, HTTPBackend


class Worker:
    def __init__(
        self,
        *,
        redis_url: str | None = None,
        redis_client: "aioredis.Redis | None" = None,
        backend: Backend | None = None,
        openai_base_url: str = "http://localhost:8000/v1",
        openai_api_key: str | None = None,
        openai_default_headers: dict[str, str] | None = None,
        concurrency: int = 16,
        group: str = codec.GROUP_DEFAULT,
        consumer: str = "worker-1",
        stream_flush_ms: int = 50,
        stream_flush_events: int = 32,
        result_ttl_s: int = 600,
        stream_maxlen: int = 10_000,
        max_retries: int = 3,
        block_ms: int = 1000,
        reclaim_min_idle_ms: int = 60_000,
    ) -> None:
        if redis_client is None:
            if redis_url is None:
                raise ValueError("provide redis_url or redis_client")
            redis_client = aioredis.Redis.from_url(redis_url)
        self._redis = redis_client
        self._backend = backend or HTTPBackend(
            base_url=openai_base_url,
            api_key=openai_api_key,
            default_headers=openai_default_headers,
        )
        self._sem = asyncio.Semaphore(concurrency)
        self._group = group
        self._consumer = consumer
        self._flush_ms = stream_flush_ms
        self._flush_events = stream_flush_events
        self._result_ttl_s = result_ttl_s
        self._stream_maxlen = stream_maxlen
        self._max_retries = max_retries
        self._block_ms = block_ms
        self._reclaim_min_idle_ms = reclaim_min_idle_ms
        self._stopping = asyncio.Event()
        self._tasks: set[asyncio.Task] = set()

    async def ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                codec.REQUESTS_STREAM, self._group, id="0", mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        await self.ensure_group()
        last_reclaim = asyncio.get_running_loop().time()
        while not self._stopping.is_set():
            now = asyncio.get_running_loop().time()
            if now - last_reclaim >= self._reclaim_min_idle_ms / 1000:
                try:
                    await self.reclaim_once()
                except Exception:
                    pass
                last_reclaim = now
            resp = await self._redis.xreadgroup(
                self._group,
                self._consumer,
                {codec.REQUESTS_STREAM: ">"},
                count=self._sem._value or 1,
                block=self._block_ms,
            )
            if not resp:
                continue
            for _stream, items in resp:
                for entry_id, fields in items:
                    job = self._load_job(fields)
                    task = asyncio.create_task(self._dispatch(entry_id, job))
                    self._tasks.add(task)
                    task.add_done_callback(self._tasks.discard)
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    @staticmethod
    def _load_job(fields: dict) -> dict:
        raw = fields.get(b"data", fields.get("data"))
        return json.loads(raw)

    async def _dispatch(self, entry_id, job: dict) -> None:
        async with self._sem:
            try:
                await self._process(job)
            finally:
                await self._redis.xack(codec.REQUESTS_STREAM, self._group, entry_id)

    async def reclaim_once(self) -> int:
        """Claim orphaned entries; process or dead-letter them. Returns #claimed."""
        claimed = 0
        cursor = "0-0"
        while True:
            cursor, entries, _deleted = await self._redis.xautoclaim(
                codec.REQUESTS_STREAM,
                self._group,
                self._consumer,
                min_idle_time=self._reclaim_min_idle_ms,
                start_id=cursor,
                count=32,
            )
            for entry_id, fields in entries:
                claimed += 1
                await self._handle_reclaimed(entry_id, fields)
            if cursor in ("0-0", b"0-0"):
                break
        return claimed

    async def _delivery_count(self, entry_id) -> int:
        info = await self._redis.xpending_range(
            codec.REQUESTS_STREAM, self._group, min=entry_id, max=entry_id, count=1
        )
        return int(info[0]["times_delivered"]) if info else 1

    async def _handle_reclaimed(self, entry_id, fields) -> None:
        job = self._load_job(fields)
        if await self._delivery_count(entry_id) > self._max_retries:
            await self._redis.xadd(codec.DEAD_LETTER_STREAM, {"data": json.dumps(job)})
            # unblock the waiting client with a terminal error
            if job.get("stream"):
                sk = codec.stream_key(job["id"])
                await self._xadd_stream(
                    sk,
                    codec.stream_error(
                        status=502,
                        headers={"content-type": "text/plain"},
                        body=b"openai-rq: dead-lettered",
                    ),
                )
                await self._redis.expire(sk, self._result_ttl_s)
            else:
                err = codec.encode_response(
                    status=502,
                    headers={"content-type": "text/plain"},
                    body=b"openai-rq: job exceeded max_retries (dead-lettered)",
                )
                await self._write_result(job["id"], err)
            await self._redis.xack(codec.REQUESTS_STREAM, self._group, entry_id)
        else:
            await self._dispatch(entry_id, job)

    async def _process(self, job: dict) -> None:
        req = codec.decode_request(job)
        if req["stream"]:
            await self._process_stream(req)
        else:
            await self._process_non_stream(req)

    async def _process_non_stream(self, req: dict) -> None:
        rid = req["id"]
        try:
            status, headers, body = await self._backend.relay(
                method=req["method"],
                path=req["path"],
                headers=req["headers"],
                body=req["body"],
            )
            payload = codec.encode_response(status=status, headers=headers, body=body)
        except Exception as exc:  # backend unreachable, etc. -> synthetic 502
            payload = codec.encode_response(
                status=502,
                headers={"content-type": "text/plain"},
                body=f"openai-rq backend error: {exc}".encode(),
            )
        await self._write_result(rid, payload)

    async def _write_result(self, rid: str, payload: dict) -> None:
        key = codec.result_key(rid)
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.rpush(key, json.dumps(payload))
            pipe.expire(key, self._result_ttl_s)
            await pipe.execute()

    async def _xadd_stream(self, sk: str, entry: dict) -> None:
        await self._redis.xadd(
            sk,
            codec.encode_stream_field(entry),
            maxlen=self._stream_maxlen,
            approximate=True,
        )

    async def _process_stream(self, req: dict) -> None:
        rid = req["id"]
        sk = codec.stream_key(rid)
        try:
            async with self._backend.relay_stream(
                method=req["method"],
                path=req["path"],
                headers=req["headers"],
                body=req["body"],
            ) as (status, headers, body_iter):
                await self._xadd_stream(
                    sk, codec.stream_head(status=status, headers=headers)
                )
                if status // 100 != 2:
                    body = b"".join([chunk async for chunk in body_iter])
                    await self._xadd_stream(
                        sk,
                        codec.stream_error(status=status, headers=headers, body=body),
                    )
                    await self._redis.expire(sk, self._result_ttl_s)
                    return
                await self._coalesce(sk, body_iter)
                await self._xadd_stream(sk, codec.stream_done())
        except Exception as exc:
            # head may or may not have been written; an error sentinel is always safe
            # to append — the client treats it as terminal.
            await self._xadd_stream(
                sk,
                codec.stream_error(
                    status=502,
                    headers={"content-type": "text/plain"},
                    body=f"openai-rq stream error: {exc}".encode(),
                ),
            )
        finally:
            await self._redis.expire(sk, self._result_ttl_s)

    async def _coalesce(self, sk: str, body_iter) -> None:
        """Buffer raw SSE bytes; flush on event-boundary count OR timer OR EOF.
        Never split mid-event: only the bytes up to the last '\\n\\n' are flushed
        on a count/timer flush; any trailing partial event stays buffered."""
        buf = bytearray()
        deadline = None
        loop = asyncio.get_running_loop()

        async def flush(force: bool) -> None:
            nonlocal buf, deadline
            if not buf:
                return
            if force:
                to_send, rest = bytes(buf), b""
            else:
                idx = buf.rfind(b"\n\n")
                if idx == -1:
                    return
                cut = idx + 2
                to_send, rest = bytes(buf[:cut]), bytes(buf[cut:])
            if to_send:
                await self._xadd_stream(sk, codec.stream_data(to_send))
            buf = bytearray(rest)
            deadline = None

        async for chunk in body_iter:
            buf.extend(chunk)
            if deadline is None:
                deadline = loop.time() + self._flush_ms / 1000
            if buf.count(b"\n\n") >= self._flush_events:
                await flush(force=False)
            elif loop.time() >= deadline:
                await flush(force=False)
        await flush(
            force=True
        )  # EOF: send whatever remains, including a trailing partial
