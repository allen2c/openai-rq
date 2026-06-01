"""`openai-rq` CLI (ops surface, runs on the inference box)."""

from __future__ import annotations

import asyncio
import os
import socket

import typer

from .worker import Worker

app = typer.Typer(add_completion=False, help="openai-rq: OpenAI-over-Redis relay.")


@app.callback()
def _main() -> None:
    """openai-rq command group. Keeps `worker` an explicit subcommand
    (spec §5c: `openai-rq worker ...`) rather than collapsing to root."""


def _default_consumer() -> str:
    return f"{socket.gethostname()}/{os.getpid()}"


def _parse_headers(items: list[str] | None) -> dict[str, str]:
    """Parse repeated --openai-header KEY=VALUE into a dict."""
    headers: dict[str, str] = {}
    for item in items or []:
        if "=" not in item:
            raise typer.BadParameter(f"expected KEY=VALUE, got {item!r}")
        key, value = item.split("=", 1)
        headers[key.strip()] = value.strip()
    return headers


def _build_worker(
    *,
    redis_url,
    openai_base_url,
    openai_api_key,
    openai_default_headers,
    concurrency,
    group,
    consumer,
    stream_flush_ms,
    result_ttl_s,
    max_retries,
) -> Worker:
    return Worker(
        redis_url=redis_url,
        openai_base_url=openai_base_url,
        openai_api_key=openai_api_key,
        openai_default_headers=openai_default_headers or None,
        concurrency=concurrency,
        group=group,
        consumer=consumer or _default_consumer(),
        stream_flush_ms=stream_flush_ms,
        result_ttl_s=result_ttl_s,
        max_retries=max_retries,
    )


@app.command()
def worker(
    redis_url: str = typer.Option(
        ..., "--redis-url", help="rediss://...redis.cache.windows.net:6380"
    ),
    openai_base_url: str = typer.Option(
        "http://localhost:8000/v1",
        "--openai-base-url",
        help="local OpenAI-compatible server (vLLM)",
    ),
    openai_api_key: str = typer.Option(
        None,
        "--openai-api-key",
        envvar="OPENAI_API_KEY",
        help="injected as Authorization: Bearer; never transits Redis",
    ),
    openai_header: list[str] = typer.Option(
        None,
        "--openai-header",
        metavar="KEY=VALUE",
        help="extra backend header (repeatable), e.g. api-key=... for Azure",
    ),
    concurrency: int = typer.Option(16, "--concurrency"),
    group: str = typer.Option("openai-rq", "--group"),
    consumer: str = typer.Option("", "--consumer", help="defaults to <host>/<pid>"),
    stream_flush_ms: int = typer.Option(50, "--stream-flush-ms"),
    result_ttl_s: int = typer.Option(600, "--result-ttl-s"),
    max_retries: int = typer.Option(3, "--max-retries"),
) -> None:
    """Run the relay worker (XREADGROUP loop)."""
    w = _build_worker(
        redis_url=redis_url,
        openai_base_url=openai_base_url,
        openai_api_key=openai_api_key,
        openai_default_headers=_parse_headers(openai_header),
        concurrency=concurrency,
        group=group,
        consumer=consumer,
        stream_flush_ms=stream_flush_ms,
        result_ttl_s=result_ttl_s,
        max_retries=max_retries,
    )
    typer.echo(
        f"openai-rq worker: group={w._group} consumer={w._consumer} backend={openai_base_url}"
    )
    asyncio.run(w.run())


if __name__ == "__main__":
    app()
