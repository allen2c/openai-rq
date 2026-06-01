# openai-rq

[![CI](https://github.com/allen2c/openai-rq/actions/workflows/ci.yml/badge.svg)](https://github.com/allen2c/openai-rq/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache_2.0-blue.svg)](https://github.com/allen2c/openai-rq/blob/main/LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![Lint](https://img.shields.io/badge/lint-ruff-261230.svg)](https://github.com/astral-sh/ruff)
[![Docs](https://img.shields.io/badge/docs-live-d92d20.svg)](https://allen2c.github.io/openai-rq/)
[![Deploy docs](https://github.com/allen2c/openai-rq/actions/workflows/pages.yml/badge.svg)](https://github.com/allen2c/openai-rq/actions/workflows/pages.yml)

📊 **Overview deck:** https://allen2c.github.io/openai-rq/

Use the OpenAI SDK from behind a locked-down network where the **only** reachable
outbound endpoint is Redis. `openai-rq` ships each OpenAI HTTP request over Redis
Streams to a worker that replays it against a local OpenAI-compatible server (e.g. vLLM)
and streams the response back — your client code stays identical to normal OpenAI usage.

```text
  your client ──(Redis Streams)──▶  openai-rq worker ──▶  http://localhost:8000/v1
   OpenAIRQ    ◀─(Redis Streams)──                         (vLLM / OpenAI-compatible)
```

Both sides connect **only** to Redis. No direct HTTP between client and the inference box.

## Install

```bash
pip install openai-rq
```

## Client — a drop-in `openai.OpenAI`

Swap `openai.OpenAI` for `openai_rq.OpenAIRQ` and point it at Redis. Everything else —
parameters, response objects, streaming, error handling — works unchanged.

```python
from openai_rq import OpenAIRQ

client = OpenAIRQ(redis_url="redis://localhost:6379/0")

resp = client.chat.completions.create(
    model="openai/gpt-oss-120b",
    messages=[{"role": "user", "content": "Hello!"}],
)
print(resp.choices[0].message.content)
```

### Streaming

```python
stream = client.chat.completions.create(
    model="openai/gpt-oss-120b",
    messages=[{"role": "user", "content": "Write a haiku."}],
    stream=True,
)
for chunk in stream:
    print(chunk.choices[0].delta.content or "", end="", flush=True)
```

### Async

```python
from openai_rq import AsyncOpenAIRQ

client = AsyncOpenAIRQ(redis_url="redis://localhost:6379/0")

resp = await client.chat.completions.create(
    model="openai/gpt-oss-120b",
    messages=[{"role": "user", "content": "Hello!"}],
)
```

`extra_headers` and `extra_body` pass through verbatim, so server-specific options
(guided decoding, etc.) just work:

```python
client.chat.completions.create(
    model="openai/gpt-oss-120b",
    messages=[...],
    extra_body={"guided_json": schema},
)
```

## Worker — run it next to the inference server

On the inference box, run a worker that relays jobs to your local server:

```bash
openai-rq worker \
  --redis-url redis://localhost:6379/0 \
  --openai-base-url http://localhost:8000/v1 \
  --concurrency 16
```

Run as many workers as you like against the same Redis — jobs are load-balanced across
them via a Redis consumer group.

### Backend needs an API key?

The credential lives **only on the worker** — it never transits Redis or the client.

```bash
# Bearer style → Authorization: Bearer <key>
export OPENAI_API_KEY=<key>
openai-rq worker --redis-url redis://localhost:6379/0 --openai-base-url http://localhost:8000/v1

# Server that expects a custom auth header instead of Bearer (repeatable)
openai-rq worker --redis-url redis://localhost:6379/0 \
  --openai-base-url http://localhost:8000/v1 \
  --openai-header api-key=<key>
```

### Embedding the worker

```python
from openai_rq.worker import Worker

worker = Worker(
    redis_url="redis://localhost:6379/0",
    openai_base_url="http://localhost:8000/v1",
    openai_api_key="<key>",         # optional; → Authorization: Bearer
    concurrency=16,
)
await worker.run()
```

## Worker options

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--redis-url` | *(required)* | Redis URL; use `rediss://` for TLS |
| `--openai-base-url` | `http://localhost:8000/v1` | local OpenAI-compatible server |
| `--openai-api-key` | env `OPENAI_API_KEY` | injected as `Authorization: Bearer` |
| `--openai-header` | — | extra backend header `KEY=VALUE` (repeatable) |
| `--concurrency` | `16` | in-flight jobs per worker |
| `--stream-flush-ms` | `50` | streaming coalesce window |
| `--result-ttl-s` | `600` | TTL on result/stream keys |
| `--max-retries` | `3` | queue retries before dead-letter |

## License

Apache-2.0
