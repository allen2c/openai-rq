from pathlib import Path

import yaml

WF = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "pages.yml"


def _load():
    assert WF.exists(), f"{WF} does not exist"
    data = yaml.safe_load(WF.read_text(encoding="utf-8"))
    # PyYAML parses the bare key `on:` as boolean True (YAML 1.1) — accept either.
    triggers = data.get("on", data.get(True))
    return data, triggers


def test_triggers():
    _data, triggers = _load()
    assert "workflow_dispatch" in triggers
    push = triggers["push"]
    assert "main" in push["branches"]
    assert any("docs/index.html" in p for p in push["paths"])


def test_permissions():
    # Least-privilege: build only reads; deploy holds the Pages/OIDC writes.
    data, _ = _load()
    jobs = data["jobs"]
    assert jobs["build"]["permissions"]["contents"] == "read"
    assert jobs["deploy"]["permissions"]["pages"] == "write"
    assert jobs["deploy"]["permissions"]["id-token"] == "write"


def test_concurrency_group():
    data, _ = _load()
    assert data["concurrency"]["group"] == "pages"
    assert data["concurrency"]["cancel-in-progress"] is False


def test_jobs_and_first_party_actions():
    data, _ = _load()
    jobs = data["jobs"]
    assert "build" in jobs and "deploy" in jobs
    assert jobs["deploy"]["needs"] == "build"
    assert jobs["deploy"]["environment"]["name"] == "github-pages"

    # Pin major versions: an incompatible downgrade (e.g. deploy-pages@v3) must fail here.
    steps_text = yaml.safe_dump(jobs)
    for action in ["actions/checkout@v4", "actions/configure-pages@v5",
                   "actions/upload-pages-artifact@v3", "actions/deploy-pages@v4"]:
        assert action in steps_text, f"missing/wrong first-party action: {action}"


def test_publishes_only_index_html():
    data, _ = _load()
    build = yaml.safe_dump(data["jobs"]["build"])
    assert "_site" in build
    assert "cp docs/index.html _site/" in build
    assert "superpowers" not in build
