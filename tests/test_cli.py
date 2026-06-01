from typer.testing import CliRunner

from openai_rq.cli import _build_worker, _parse_headers, app

runner = CliRunner()


def test_worker_is_a_named_subcommand():
    # spec §5c documents `openai-rq worker ...`; `worker` must be a real subcommand,
    # not collapsed to root (which happens with a single command + no callback).
    top = runner.invoke(app, ["--help"])
    assert top.exit_code == 0
    assert "worker" in top.output


def test_worker_help_lists_flags():
    result = runner.invoke(app, ["worker", "--help"])
    assert result.exit_code == 0
    assert "--redis-url" in result.output
    assert "--openai-base-url" in result.output
    assert "--openai-api-key" in result.output
    assert "--openai-header" in result.output
    assert "--concurrency" in result.output
    assert "--stream-flush-ms" in result.output


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
