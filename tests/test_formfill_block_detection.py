"""Bot-protection detection must FAIL a run, not decorate it with a warning.

Why this is a test and not a live check: SIMAH's Cloudflare block is
INTERMITTENT. Two verify runs against it sailed straight through, so "I saw it
fire once" is not evidence the gate works. These tests pin the behaviour with
fixed inputs so it stays working when nobody is watching.

    venv/Scripts/python.exe -m pytest tests/test_formfill_block_detection.py -q
"""

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dynamic_crawler.formfill import runner, verify as verify_mod  # noqa: E402

# The real thing, trimmed: what Cloudflare served us on 2026-08-02.
CLOUDFLARE_BLOCK = """
<html><head><title>Attention Required! | Cloudflare</title></head><body>
<div id="cf-wrapper">
  <div class="cf-alert cf-cookie-error" id="cookie-alert">Please enable cookies.</div>
  <div id="cf-error-details" class="cf-error-details-wrapper">
    <h1>Sorry, you have been blocked</h1>
    <h2 class="cf-subheadline">You are unable to access simah.com</h2>
    <p>This website is using a security service to protect itself from online attacks.</p>
  </div>
</div></body></html>
"""

REAL_PAGE = """
<html><head><title>Rules and Regulations</title></head><body>
<h1>Rules and Regulations</h1>
<div class="accordion-item"><h2>Article-1</h2>
  <div class="accordion-body">The following words and phrases, wherever mentioned
  in this Law, shall have the meanings assigned to them.</div></div>
<div class="accordion-sub-hd"><h6>The Implementing Regulations for Credit Information Law</h6>
  <a class="btn-blue" href="/files/implementing-regulations.pdf">Download PDF</a></div>
</body></html>
"""


@pytest.fixture(scope="module")
def page():
    pw = pytest.importorskip("playwright.sync_api")
    with pw.sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        pg = browser.new_page()
        yield pg
        browser.close()


def test_detects_a_cloudflare_block(page):
    page.set_content(CLOUDFLARE_BLOCK)
    assert runner._blocked(page), "a Cloudflare challenge page must be recognised"


def test_does_not_fire_on_a_real_page(page):
    """The expensive mistake would be calling a good page blocked and discarding
    a whole crawl, so the false-positive direction matters as much."""
    page.set_content(REAL_PAGE)
    assert runner._blocked(page) == "", "a real document page must not look blocked"


@pytest.mark.parametrize("phrase", [
    "Just a moment...",
    "Checking your browser before accessing",
    "Access Denied",
    "Error 1020",
    "Verify you are human",
])
def test_detects_other_common_walls(page, phrase):
    page.set_content(f"<html><body><h1>{phrase}</h1></body></html>")
    assert runner._blocked(page), f"{phrase!r} should be recognised as a wall"


def test_gate_fails_when_any_page_was_blocked(tmp_path, monkeypatch):
    """The wiring, not the regex: one blocked page must make verify FAIL.

    A blocked run stores a challenge page as document content. If the gate only
    warned, that page would be approved into the library as the regulation.
    """
    hints = {
        "version": 1, "name": "fake", "seed_url": "https://example.gov/x",
        "shape": "list", "scope": "prefix", "row_selector": "div.row",
        "pagination": {"mode": "none", "max_pages": 1},
        "fields": {"title": {"from": "css", "selector": "a", "attr": "text"},
                   "document_url": {"from": "css", "selector": "a", "attr": "href"}},
        "meta": {"approved": False},
    }
    hints_path = tmp_path / "fake.yml"
    hints_path.write_text(json.dumps(hints), encoding="utf-8")   # YAML is a JSON superset

    def fake_run(h, out, **kw):
        out = Path(out)
        out.mkdir(parents=True, exist_ok=True)
        (out / "rows.json").write_text(json.dumps(
            [{"href": "https://example.gov/a", "title": "A", "fields": {}}]), encoding="utf-8")
        return {"rows": 1, "records": 1, "documents": 0, "seconds": 1.0,
                "listing_pages": 1, "plan": {"mode": "none"},
                "fill_rates": {"title": 100.0, "document_url": 100.0},
                "blocked_pages": 1,          # <- the one thing under test
                "warnings": [], "pages": []}

    monkeypatch.setattr(verify_mod.runner, "run", fake_run)
    report = verify_mod.verify(hints_path, tmp_path / "out", runs=2)

    assert report["verdict"] == "FAIL", "a blocked page must fail the gate outright"
    assert any("bot-protection" in f for f in report["failures"]), report["failures"]


def test_gate_passes_when_nothing_was_blocked(tmp_path, monkeypatch):
    """Control case — otherwise the test above passes for the wrong reason."""
    hints = {
        "version": 1, "name": "fake", "seed_url": "https://example.gov/x",
        "shape": "list", "scope": "prefix", "row_selector": "div.row",
        "pagination": {"mode": "none", "max_pages": 1},
        "fields": {"title": {"from": "css", "selector": "a", "attr": "text"},
                   "document_url": {"from": "css", "selector": "a", "attr": "href"}},
        "meta": {"approved": False},
    }
    hints_path = tmp_path / "fake.yml"
    hints_path.write_text(json.dumps(hints), encoding="utf-8")

    def fake_run(h, out, **kw):
        out = Path(out)
        out.mkdir(parents=True, exist_ok=True)
        (out / "rows.json").write_text(json.dumps(
            [{"href": "https://example.gov/a", "title": "A", "fields": {}}]), encoding="utf-8")
        return {"rows": 1, "records": 1, "documents": 0, "seconds": 1.0,
                "listing_pages": 1, "plan": {"mode": "none"},
                "fill_rates": {"title": 100.0, "document_url": 100.0},
                "blocked_pages": 0,
                "warnings": [], "pages": []}

    monkeypatch.setattr(verify_mod.runner, "run", fake_run)
    report = verify_mod.verify(hints_path, tmp_path / "out", runs=2)
    assert report["verdict"] == "PASS", report["failures"] + report["warnings"]
