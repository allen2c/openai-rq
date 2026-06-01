from typer.main import get_command

from openai_rq.cli import _build_worker, _parse_headers, app


def _worker_command():
    # Inspect the actual Click command tree, NOT the rendered `--help`: Typer renders
    # help via Rich, which truncates option names on narrow terminals (e.g. CI), so
    # asserting substrings on help output is environment-dependent and flaky.
    group = get_command(app)
    return group, group.commands["worker"]


def test_worker_is_a_named_subcommand():
    group, _worker = _worker_command()
    assert "worker" in group.commands


def test_worker_lists_flags():
    _group, worker = _worker_command()
    flags = {opt for param in worker.params for opt in param.opts}
    for flag in [
        "--redis-url",
        "--openai-base-url",
        "--openai-api-key",
        "--openai-header",
        "--concurrency",
        "--stream-flush-ms",
    ]:
        assert flag in flags, f"missing flag: {flag}"


def test_parse_headers():
    assert _parse_headers(["api-key=secret", "x-trace=on"]) == {
        "api-key": "secret",
        "x-trace": "on",
    }
    assert _parse_headers(None) == {}


def test_build_worker_applies_flags():
    worker = _build_worker(
        redis_url="redis://localhost:6379/0",
        openai_base_url="http://localhost:8000/v1",
        openai_api_key="sk-test",
        openai_default_headers={"api-key": "azkey"},
        concurrency=8,
        group="g",
        consumer="c",
        stream_flush_ms=40,
        result_ttl_s=120,
        max_retries=5,
    )
    assert worker._group == "g"
    assert worker._consumer == "c"
    assert worker._flush_ms == 40
    assert worker._result_ttl_s == 120
    assert worker._max_retries == 5
    assert worker._sem._value == 8
    # the backend credential is held by the worker's HTTPBackend
    assert worker._backend._api_key == "sk-test"
    assert worker._backend._default_headers == {"api-key": "azkey"}
