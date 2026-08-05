"""Snapshots, and the clock that decides when SIMAH may be asked again.

The rule these tests protect: the live site is touched by exactly one function
(`snapshot.capture`). Everything else replays a saved page. Nothing here makes a
network request — that is the point, and a test that did would be the bug.

    venv/Scripts/python.exe -m pytest tests/test_formfill_snapshot.py -q
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dynamic_crawler.formfill import runner, verify as verify_mod  # noqa: E402
from dynamic_crawler.formfill.snapshot import (BACKOFF_HOURS, SnapshotStore,  # noqa: E402
                                              capture)

SEED = "https://www.simah.com/english/Pages/rules-and-regulations.aspx"

# SIMAH's page, reduced to what the form actually reads: the law as collapsible
# articles, plus the Implementing Regulations as an attached PDF with a RELATIVE
# href — which is the case the <base> tag exists for.
PAGE = """<html><head><title>Rules and Regulations</title></head><body>
<h1>Rules and Regulations</h1>
<div class="accordion-item"><button class="accordion-button">Article-1</button>
  <div class="accordion-body">The following words and phrases, wherever mentioned in
  this Law, shall have the meanings assigned to them.</div></div>
<div class="accordion-sub-hd">
  <h6>The Implementing Regulations for Credit Information Law</h6>
  <a class="btn-blue" href="/english/Documents/implementing-regulations.pdf">Download PDF</a>
</div>
</body></html>"""


def _now():
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------- #
# the <base> tag — the silent-wrongness guard
# --------------------------------------------------------------------------- #

def test_base_tag_is_injected_into_head():
    out = runner.snapshot_html(PAGE, SEED)
    assert f'<base href="{SEED}">' in out
    assert out.index("<base") < out.index("<h1>"), "must precede the content it affects"


def test_an_existing_base_is_left_alone():
    """A page that already declares a base knows better than we do; two base tags
    would mean the second is ignored anyway."""
    html = '<html><head><base href="https://elsewhere/"></head><body>x</body></html>'
    assert runner.snapshot_html(html, SEED) == html


def test_base_is_still_added_when_there_is_no_head():
    out = runner.snapshot_html("<body><a href='/x.pdf'>x</a></body>", SEED)
    assert out.startswith(f'<base href="{SEED}">')


# --------------------------------------------------------------------------- #
# the clock
# --------------------------------------------------------------------------- #

def test_a_store_with_no_history_allows_an_attempt(tmp_path):
    allowed, why = SnapshotStore("simah.rules", tmp_path).may_attempt()
    assert allowed and "no attempt" in why


def test_a_block_pushes_the_next_attempt_out(tmp_path):
    store = SnapshotStore("simah.rules", tmp_path)
    m = store.record_block("you have been blocked")
    assert m["consecutive_blocks"] == 1
    assert m["last_attempt_result"] == "blocked"
    allowed, why = store.may_attempt()
    assert not allowed, "a blocked attempt must not be immediately retryable"
    assert "blocked" in why and "backoff" in why, why


def test_the_backoff_lengthens_with_each_block(tmp_path):
    """Retrying a block is what turns a temporary rule into a lasting one, so the
    interval has to grow rather than sit at a constant."""
    store = SnapshotStore("simah.rules", tmp_path)
    seen = []
    for _ in range(len(BACKOFF_HOURS) + 2):
        seen.append(store.next_backoff_hours())
        store.record_block("blocked")
    assert seen[:len(BACKOFF_HOURS)] == list(BACKOFF_HOURS)
    assert seen[-1] == BACKOFF_HOURS[-1], "capped, never unbounded"


def test_a_block_never_writes_html(tmp_path):
    """The 2026-07-30 bug: the challenge page became the stored document."""
    store = SnapshotStore("simah.rules", tmp_path)
    store.record_block("you have been blocked")
    assert not store.html_path.exists()
    assert store.state() == "missing"


def test_a_load_failure_is_not_a_block(tmp_path):
    """A timeout means the site did not refuse us, so it must not earn the block
    backoff — otherwise one flaky night silently freezes refreshes for two weeks."""
    store = SnapshotStore("simah.rules", tmp_path)
    store.record_failure("net::ERR_NAME_NOT_RESOLVED")
    assert store.manifest()["consecutive_blocks"] == 0
    allowed, _ = store.may_attempt()
    assert allowed


def test_a_successful_capture_resets_the_backoff(tmp_path):
    store = SnapshotStore("simah.rules", tmp_path)
    for _ in range(3):
        store.record_block("blocked")
    m = store.save(PAGE, SEED)
    assert m["consecutive_blocks"] == 0
    assert store.state() == "fresh"


# --------------------------------------------------------------------------- #
# fresh / aging / stale
# --------------------------------------------------------------------------- #

def _age(store: SnapshotStore, days: float):
    m = store.manifest()
    m["captured_at"] = (_now() - timedelta(days=days)).isoformat(timespec="seconds")
    store.manifest_path.write_text(json.dumps(m), encoding="utf-8")


@pytest.mark.parametrize("days, expected", [
    (1, "fresh"), (29, "fresh"), (31, "aging"), (89, "aging"), (91, "stale"),
])
def test_snapshot_states_by_age(tmp_path, days, expected):
    store = SnapshotStore("simah.rules", tmp_path, max_age_days=30, grace_days=90)
    store.save(PAGE, SEED)
    _age(store, days)
    assert store.state() == expected


def test_a_change_between_captures_is_reported(tmp_path):
    """This is the monitoring signal — the reason a refresh loop is worth having at
    all rather than just keeping one old copy."""
    store = SnapshotStore("simah.rules", tmp_path)
    first = store.save(PAGE, SEED)
    assert first["changed_on_last_capture"] is False, "nothing to compare against yet"
    second = store.save(PAGE.replace("Article-1", "Article-1 (amended)"), SEED)
    assert second["changed_on_last_capture"] is True
    assert second["previous_sha256"] == first["sha256"]


def test_recapturing_identical_html_reports_no_change(tmp_path):
    store = SnapshotStore("simah.rules", tmp_path)
    store.save(PAGE, SEED)
    assert store.save(PAGE, SEED)["changed_on_last_capture"] is False


# --------------------------------------------------------------------------- #
# capture() refuses inside the backoff window — without opening a browser
# --------------------------------------------------------------------------- #

def test_capture_refuses_inside_the_backoff_window(tmp_path):
    """If this ever opens a browser the test hangs or fails, which is exactly the
    signal we want: the refusal must happen BEFORE any request."""
    store = SnapshotStore("simah.rules", tmp_path)
    store.record_block("you have been blocked")
    hints = {"name": "simah.rules", "seed_url": SEED}
    res = capture(hints, store, headed=False)
    assert res["result"] == "refused"
    assert "backoff" in res["reason"], res["reason"]


# --------------------------------------------------------------------------- #
# a snapshot run makes no requests, and cannot approve a form
# --------------------------------------------------------------------------- #

def test_a_missing_snapshot_is_an_error_not_a_live_crawl(tmp_path):
    """The dangerous fallback would be shrugging and going to the network — the
    surprise that costs an IP block."""
    hints = {"version": 1, "name": "simah.rules", "seed_url": SEED, "shape": "list",
             "scope": "prefix", "row_selector": "div.accordion-sub-hd",
             "pagination": {"mode": "none", "max_pages": 1}, "fields": {}}
    with pytest.raises(FileNotFoundError, match="snapshot not found"):
        runner.run(hints, tmp_path / "out", snapshot=tmp_path / "nope.html")


def test_a_snapshot_verify_cannot_approve_a_form(tmp_path, monkeypatch):
    """Three runs against one saved file agree by construction. Approving on that
    would make the gate theatre for exactly the sites we can check least."""
    hints = {"version": 1, "name": "fake", "seed_url": SEED, "shape": "list",
             "scope": "prefix", "row_selector": "div.row",
             "pagination": {"mode": "none", "max_pages": 1},
             "fields": {"title": {"from": "css", "selector": "a", "attr": "text"},
                        "document_url": {"from": "css", "selector": "a", "attr": "href"}},
             "meta": {"approved": False}}
    hints_path = tmp_path / "fake.yml"
    hints_path.write_text(json.dumps(hints), encoding="utf-8")
    snap = tmp_path / "fake.html"
    snap.write_text(PAGE, encoding="utf-8")

    def fake_run(h, out, **kw):
        assert kw.get("snapshot") == snap, "verify must pass the snapshot through"
        out = Path(out)
        out.mkdir(parents=True, exist_ok=True)
        (out / "rows.json").write_text(json.dumps(
            [{"href": "https://x/a.pdf", "title": "A", "fields": {}}]), encoding="utf-8")
        return {"rows": 1, "records": 1, "documents": 1, "seconds": 0.1,
                "listing_pages": 1, "plan": {"mode": "none"},
                "fill_rates": {"title": 100.0, "document_url": 100.0},
                "blocked_pages": 0, "warnings": [], "pages": [],
                "source": "snapshot"}

    monkeypatch.setattr(verify_mod.runner, "run", fake_run)
    out = tmp_path / "verify"
    rep = verify_mod.verify(hints_path, out, runs=2, snapshot=snap)

    assert rep["source"] == "snapshot"
    assert any("SNAPSHOT" in w for w in rep["warnings"]), rep["warnings"]
    with pytest.raises(SystemExit, match="snapshot"):
        verify_mod.approve(hints_path, out / "verify.json", "tester")


def test_a_forced_snapshot_approval_is_recorded_as_such(tmp_path, monkeypatch):
    """An override is allowed — the gate has --force by design — but it must leave
    evidence in the form rather than looking like a normal approval."""
    hints = {"version": 1, "name": "fake2", "seed_url": SEED, "shape": "list",
             "scope": "prefix", "row_selector": "div.row",
             "pagination": {"mode": "none", "max_pages": 1},
             "fields": {"title": {"from": "css", "selector": "a", "attr": "text"},
                        "document_url": {"from": "css", "selector": "a", "attr": "href"}}}
    hints_path = tmp_path / "fake2.yml"
    hints_path.write_text(json.dumps(hints), encoding="utf-8")
    rep = {"verdict": "WARN", "counts": [1, 1], "spread_pct": 0.0,
           "fill_rates": {"title": 100.0}, "verified_at": "2026-08-04T00:00:00+00:00",
           "source": "snapshot", "snapshot": "x.html", "failures": [], "warnings": []}
    vj = tmp_path / "verify.json"
    vj.write_text(json.dumps(rep), encoding="utf-8")

    stamped = {}
    monkeypatch.setattr("dynamic_crawler.formfill.verify.stamp_meta",
                        lambda p, meta: stamped.update(meta))
    verify_mod.approve(hints_path, vj, "tester", force=True)
    assert stamped["verify"]["source"] == "snapshot"
    assert stamped["verify"]["forced"] is True


# --------------------------------------------------------------------------- #
# one saved page can only replay a one-page form
# --------------------------------------------------------------------------- #

def _hints(**over):
    h = {"version": 1, "name": "x", "seed_url": SEED, "shape": "list",
         "scope": "prefix", "row_selector": "div.row",
         "pagination": {"mode": "none", "max_pages": 1}, "fields": {}}
    h.update(over)
    return h


def test_a_paginated_form_refuses_to_run_from_a_snapshot(tmp_path):
    """Serving the snapshot for page 1 and fetching 2..N would put live traffic
    behind a flag that promises none — the exact foot-gun a blocked site cannot
    afford."""
    snap = tmp_path / "s.html"
    snap.write_text(PAGE, encoding="utf-8")
    hints = _hints(pagination={"mode": "url_offset", "pattern": "x/P{offset}",
                               "step": 30, "max_pages": 200})
    with pytest.raises(ValueError, match="single-page form"):
        runner.run(hints, tmp_path / "out", snapshot=snap)


def test_a_tree_form_refuses_to_run_from_a_snapshot(tmp_path):
    """A tree finds its nodes by visiting them: the seed's menu shows 20 of SAMA's
    40, so a replay would report half a rulebook as all of it."""
    snap = tmp_path / "s.html"
    snap.write_text(PAGE, encoding="utf-8")
    hints = _hints(shape="tree", tree={"menu_selector": "#m", "node_selector": "li",
                                       "link_selector": "a"})
    with pytest.raises(ValueError, match="single-page form"):
        runner.run(hints, tmp_path / "out", snapshot=snap)


def test_a_single_page_form_is_allowed(tmp_path):
    """SIMAH's case, and the only one this feature claims: no pagination, not a
    tree. Reaching the browser at all proves the guard let it through."""
    snap = tmp_path / "s.html"
    snap.write_text(PAGE, encoding="utf-8")
    pytest.importorskip("playwright.sync_api")
    summary = runner.run(_hints(row_selector="div.accordion-sub-hd", include_page=True),
                         tmp_path / "out", snapshot=snap, write_excel=False)
    assert summary["source"] == "snapshot"
    assert summary["rows"] >= 1
