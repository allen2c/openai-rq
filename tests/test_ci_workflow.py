from pathlib import Path

import yaml

WF = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ci.yml"


def _load():
    assert WF.exists(), f"{WF} does not exist"
    data = yaml.safe_load(WF.read_text(encoding="utf-8"))
    # PyYAML parses the bare key `on:` as boolean True (YAML 1.1) — accept either.
    triggers = data.get("on", data.get(True))
    return data, triggers


def test_triggers_push_and_pr_to_main():
    _data, triggers = _load()
    assert "main" in triggers["push"]["branches"]
    assert "main" in triggers["pull_request"]["branches"]


def test_provides_a_real_redis_service():
    data, _ = _load()
    services = data["jobs"]["test"]["services"]
    assert "redis" in services
    assert "redis" in services["redis"]["image"]


def test_runs_lint_and_pytest():
    data, _ = _load()
    steps_text = yaml.safe_dump(data["jobs"]["test"])
    assert ".[dev]" in steps_text  # installs dev extras
    assert "ruff check" in steps_text
    assert "pytest" in steps_text


def test_python_matrix_includes_min_supported():
    data, _ = _load()
    versions = [
        str(v) for v in data["jobs"]["test"]["strategy"]["matrix"]["python-version"]
    ]
    assert "3.11" in versions
