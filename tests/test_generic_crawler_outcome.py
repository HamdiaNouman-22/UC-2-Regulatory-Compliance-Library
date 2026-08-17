"""A run that reached the end is not a run that crawled the site.

`done` used to mean only "the walk reached _finish", and every consumer read it as
success: the UI printed a finish line and main() never set an exit code at all. A
Cloudflare page reaches _finish too, and its ~1,054 characters clear the 200-char
bar that makes a page a document — so it would have been stored as the regulation.

These tests pin the WIRING between the counters and the exit code with fixed
inputs. Nothing here touches the network: SIMAH's block is IP-level and
intermittent, so a live check would prove nothing and cost everyone the site.

    venv/Scripts/python.exe -m pytest tests/test_generic_crawler_outcome.py -q
"""

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
for p in (REPO_ROOT, REPO_ROOT / "generic_crawler"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

# noqa: E402 below — these must follow the sys.path insert above (flake8's E402 is
# "import not at top of file"); same reason as tests/test_formfill_block_detection.py.
from generic_crawler import crawler  # noqa: E402
from generic_crawler.blockcheck import blocked_reason, text_is_blocked  # noqa: E402

# What Cloudflare served us on 2026-08-02, trimmed.
CLOUDFLARE_TITLE = "Attention Required! | Cloudflare"
CLOUDFLARE_BODY = ("Sorry, you have been blocked. You are unable to access "
                   "simah.com. This website is using a security service to "
                   "protect itself from online attacks.")

# A clean generic run. Each test breaks exactly one thing in it.
CLEAN = {"pages": 40, "documents": 7, "blocked_pages": 0, "errors": 0,
         "retries": 0, "cap_hit": False, "seed_loaded": True, "stopped": ""}


# --------------------------------------------------------------------------- #
# detection
# --------------------------------------------------------------------------- #

def test_detects_the_cloudflare_page():
    assert text_is_blocked(CLOUDFLARE_TITLE, CLOUDFLARE_BODY)


def test_detects_a_block_from_the_title_alone():
    """Cloudflare's challenge body can be nearly empty while the title gives it
    away, so the probe reads both."""
    assert text_is_blocked(CLOUDFLARE_TITLE, "")


@pytest.mark.parametrize("phrase", [
    "Just a moment...", "Checking your browser before accessing",
    "Access Denied", "Error 1020", "Verify you are human", "DDoS protection",
])
def test_detects_other_common_walls(phrase):
    assert text_is_blocked("", phrase), phrase


def test_does_not_fire_on_a_real_regulator_page():
    """The expensive false positive: calling a good page blocked throws away a
    whole crawl, so this direction matters as much as the other."""
    body = ("Rules and Regulations. Article 1: The following words and phrases, "
            "wherever mentioned in this Law, shall have the meanings assigned to "
            "them. Article 2: The Bank shall maintain monetary stability.")
    assert text_is_blocked("Rules and Regulations", body) == ""


def test_a_probe_that_throws_is_not_a_block():
    """A page we could not read is a load failure for the caller to count — not
    evidence of a wall. Guessing 'blocked' here would fail clean runs."""
    class Broken:
        def evaluate(self, _js):
            raise RuntimeError("Execution context was destroyed")

    assert blocked_reason(Broken()) == ""


# --------------------------------------------------------------------------- #
# run_status — the classification
# --------------------------------------------------------------------------- #

def test_a_clean_run_is_ok():
    assert crawler.run_status(CLEAN) == "ok"


def test_one_blocked_page_is_blocked():
    assert crawler.run_status(dict(CLEAN, blocked_pages=1)) == "blocked"


def test_blocked_outranks_every_other_signal():
    """Nothing from a blocked run is trustworthy, so no other counter may soften
    it into 'incomplete'."""
    assert crawler.run_status(
        dict(CLEAN, blocked_pages=2, errors=5, cap_hit=True)) == "blocked"


def test_no_pages_is_zero_even_when_a_document_was_scraped():
    """A site we pointed a crawler at is not empty — zero pages means the walk did
    not happen. Deliberately STRICTER than baseline_report.py's ZERO (which needs
    pages AND documents at 0): a few links can be scraped off the seed alone while
    the walk found nothing, which is exactly the shape-misdetection bug in
    MERGE_LOG §8 — crawl_tree on a page with no book menu."""
    assert crawler.run_status(dict(CLEAN, pages=0)) == "zero"
    assert crawler.run_status(dict(CLEAN, pages=0, documents=2)) == "zero"


@pytest.mark.parametrize("broken", [
    {"cap_hit": True},
    {"seed_loaded": False},
    {"stopped": "browser died on listing page 12 of 139 — listing INCOMPLETE"},
])
def test_a_truncated_walk_is_incomplete(broken):
    assert crawler.run_status(dict(CLEAN, **broken)) == "incomplete"


def test_a_page_that_errored_does_not_make_the_run_incomplete():
    """A 404 among 150 pages is skipped by design (README). If it flipped the
    status, every real run would read INCOMPLETE and nobody would look."""
    assert crawler.run_status(dict(CLEAN, errors=1)) == "ok"


def test_incomplete_is_not_fatal_but_blocked_and_zero_are():
    """The decision this whole change rests on: a truncated run keeps its rows
    (MERGE_LOG §13) while a blocked or empty one must stop the caller."""
    assert "incomplete" not in crawler.FATAL_STATUSES
    assert set(crawler.FATAL_STATUSES) == {"blocked", "zero"}


# --------------------------------------------------------------------------- #
# _merge_note — walker findings plus seed findings
# --------------------------------------------------------------------------- #

def test_a_walkers_findings_add_to_the_seeds():
    """The seed check runs in crawl() and the page checks run inside the walker.
    Overwriting instead of adding would lose one of them."""
    note = {"blocked_pages": 1, "errors": 2, "retries": 1, "stopped": "", "resume": {}}
    crawler._merge_note(note, {"blocked_pages": 3, "errors": 1})
    assert (note["blocked_pages"], note["errors"]) == (4, 3)


def test_the_first_reason_for_stopping_wins():
    """A seed that was blocked explains everything after it; a later cap does
    not."""
    note = {"blocked_pages": 1, "errors": 0, "retries": 0,
            "stopped": "seed blocked by bot protection (you have been blocked)",
            "resume": {}}
    crawler._merge_note(note, {"stopped": "page cap: 150 of max_pages=150"})
    assert note["stopped"].startswith("seed blocked")


def test_a_walker_that_reports_nothing_changes_nothing():
    note = {"blocked_pages": 0, "errors": 0, "retries": 0, "stopped": "", "resume": {}}
    crawler._merge_note(note, {})
    assert crawler.run_status(dict(CLEAN, **{k: note[k] for k in
                                             ("blocked_pages", "errors")})) == "ok"


# --------------------------------------------------------------------------- #
# the exit code — what a caller acts on
# --------------------------------------------------------------------------- #

def _wrote(tmp_path: Path, **overrides) -> Path:
    """A pages.json exactly as _finish writes one."""
    counts = dict(CLEAN, **overrides)
    status = crawler.run_status(counts)
    body = {"seed": "https://example.gov/x", "shape": "generic", "status": status,
            "n_pages": counts["pages"], "n_documents": counts["documents"],
            "blocked_pages": counts["blocked_pages"], "errors": counts["errors"],
            "retries": counts["retries"], "cap_hit": counts["cap_hit"],
            "seed_loaded": counts["seed_loaded"], "stopped": counts["stopped"],
            "resume": {}, "pages": [], "documents": [], "chrome_dropped": []}
    (tmp_path / "pages.json").write_text(json.dumps(body), encoding="utf-8")
    return tmp_path


@pytest.mark.parametrize("overrides, expected", [
    ({}, 0),                                  # ok
    ({"blocked_pages": 1}, 1),                # blocked  -> fatal
    ({"pages": 0, "documents": 0}, 1),        # zero     -> fatal
    ({"cap_hit": True}, 0),                   # incomplete -> reported, not fatal
    ({"seed_loaded": False, "stopped": "seed did not load after 3 attempts"}, 0),
])
def test_exit_code_follows_the_outcome(tmp_path, overrides, expected):
    assert crawler._report_outcome(_wrote(tmp_path, **overrides)) == expected


def test_a_missing_pages_json_is_not_a_success(tmp_path):
    """No artifact at all used to exit 0 as well, since main() never checked."""
    assert crawler._report_outcome(tmp_path) == 1


def test_the_operator_is_told_what_to_do_next(tmp_path, capsys):
    out = _wrote(tmp_path, blocked_pages=2,
                 stopped="seed blocked by bot protection (you have been blocked)")
    crawler._report_outcome(out)
    err = capsys.readouterr().err
    assert "BLOCKED" in err
    assert "stopped:" in err, "the reason must be in the banner, not only the file"
    assert "pages.json" in err, "point at the artifact to read"


def test_an_incomplete_run_reports_where_it_stopped(tmp_path, capsys):
    """The resume hook: a truncated run is only useful later if it says where it
    ran out."""
    out = _wrote(tmp_path, cap_hit=True,
                 stopped="page cap: 150 of max_pages=150, 812 URLs still queued")
    assert crawler._report_outcome(out) == 0
    err = capsys.readouterr().err
    assert "INCOMPLETE" in err and "812 URLs still queued" in err
