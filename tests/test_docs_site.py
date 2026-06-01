import re
from pathlib import Path

import pytest

INDEX = Path(__file__).resolve().parents[1] / "docs" / "index.html"


@pytest.fixture(scope="module")
def html() -> str:
    assert INDEX.exists(), f"{INDEX} does not exist"
    return INDEX.read_text(encoding="utf-8")


def test_doctype_and_title(html):
    assert html.lstrip().lower().startswith("<!doctype html>")
    assert "<title>" in html and "openai-rq" in html


def test_has_exactly_eleven_slides(html):
    assert len(re.findall(r'<section class="slide"', html)) == 11


def test_no_network_fetched_resources(html):
    assert not re.search(r'<script[^>]+src=["\']https?:', html), "external script"
    assert not re.search(
        r'<link[^>]+href=["\']https?:', html
    ), "external stylesheet/font"
    assert not re.search(r'<img[^>]+src=["\']https?:', html), "external image"
    assert "url(http" not in html, "external url() in CSS"


def test_print_media_query(html):
    assert "@media print" in html


def test_keyboard_and_hash_nav(html):
    assert "keydown" in html
    assert "hashchange" in html or "location.hash" in html


def test_progress_and_counter_present(html):
    assert 'class="progress-bar"' in html
    assert 'class="counter"' in html
    assert "slides.length" in html


def test_key_content_markers(html):
    for marker in ["transport", "Redis", "OpenAIRQ", "openai-rq worker"]:
        assert marker in html, f"missing content: {marker}"


def test_deidentified_names_only(html):
    assert "openai/gpt-oss-120b" in html
    assert "localhost:8000" in html
    # brands previously scrubbed from the project (see de-identification pass)
    for brand in ["azure", "windows.net", "qwen", "entra", "dgx"]:
        assert brand not in html.lower(), f"branded term leaked: {brand}"
