"""Per-article hashes over a page we already hold. No network, by construction.

WHY THIS FILE LOOKS LIKE THIS

The page this was built for belongs to a host we are blocked from, so every test
here works on markup written inline — the same accordion shape, small enough to
read. One test reads the real saved page when it is present and skips when it is
not, because the snapshot lives under output/ and is not committed.

What is verified here:

  the text        the hash is over TEXT. `collapse show`, `aria-expanded` and an
                  empty `style` land on whichever article was open when the page
                  was captured, so a hash of the markup moves when the law has not
  granularity     an amendment names the ARTICLE that moved, which is the whole
                  point of this over the whole-page sha256 already in the manifest
  coverage        an article that is gone from the page is `missing` with a
                  streak, and an unparseable page RAISES rather than reporting
                  every article withdrawn
  the clock       a snapshot past its grace period is refused: replaying one
                  forever reports `unchanged` while the law moves on

    venv/Scripts/python.exe -m pytest tests/test_snapshot_articles.py -v
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class _Any:
    def __getattr__(self, name):
        return _Any()

    def __call__(self, *a, **kw):
        return _Any()

    def __iter__(self):
        return iter(())

    def __bool__(self):
        return False


_PREFIXES = ("fitz", "pdf2image", "pytesseract", "PIL", "cv2", "paddle",
             "paddleocr", "paddlex", "torch", "transformers", "easyocr",
             "docx", "pptx", "camelot", "pdfplumber", "layoutparser",
             "lingua", "langdetect", "openai", "tiktoken", "selenium",
             "bs4", "httpx", "aiohttp", "tenacity")

for _name in _PREFIXES:
    if _name not in sys.modules:
        try:
            __import__(_name)
        except Exception:
            _m = types.ModuleType(_name)
            _m.__getattr__ = lambda attr: _Any()
            _m.__path__ = []
            sys.modules[_name] = _m

from dynamic_crawler import changesignal as cs                         # noqa: E402
from dynamic_crawler import snapshot_articles as sa                    # noqa: E402
from dynamic_crawler.change_state import ChangeStateStore              # noqa: E402

REAL = Path(__file__).resolve().parents[1] / "output/snapshots/simah.rules.html"


# --------------------------------------------------------------------------- #
#  the markup, as the page renders it                                          #
# --------------------------------------------------------------------------- #

def item(number: int, text: str, open_: bool = False) -> str:
    """One accordion item. `open_` is the state of the one panel that was open
    when the page happened to be captured."""
    shown = ' class="accordion-collapse collapse show" style=""' if open_ else \
            ' class="accordion-collapse collapse"'
    return (f'<div class="accordion-item">'
            f'<h2 class="accordion-header" id="heading{number}">'
            f'<button class="accordion-button" type="button" '
            f'aria-expanded="{str(open_).lower()}"> Article-{number} </button></h2>'
            f'<div id="collapse{number}"{shown} data-bs-parent="#rulesAccordion">'
            f'<div class="accordion-body"><p>{text}</p></div></div></div>')


def page(texts, open_index: int = 1) -> str:
    body = "".join(item(i, t, open_=(i == open_index))
                   for i, t in enumerate(texts, start=1))
    return ('<html><body><div class="accordion" id="rulesAccordion">'
            f'{body}</div></body></html>')


LAW = ["The following words and phrases shall have the meanings assigned.",
       "This Law aims at establishing general principles.",
       "This Law shall apply to companies and members."]


def signal(html, **kw) -> sa.SnapshotArticleSweep:
    kw.setdefault("name", "simah.rules")
    kw.setdefault("page", "rules-and-regulations")
    return sa.SnapshotArticleSweep(html=html, **kw)


def store_at(tmp_path) -> ChangeStateStore:
    return ChangeStateStore(tmp_path / "state.json", source="SIMAH/Rules")


def keyed(buckets, verdict) -> dict:
    return {(o.key if hasattr(o, "key") else o): why
            for o, why in buckets[verdict]}


class FakeSnapshot:
    """SnapshotStore's reading half, which is all this signal touches."""

    def __init__(self, html="", state="fresh", url="https://example/rules"):
        self._html, self._state, self._url = html, state, url

    def state(self):
        return self._state

    def html(self):
        return self._html

    def manifest(self):
        return {"url": self._url, "captured_at": "2026-08-05T02:01:19+00:00"}

    def describe(self):
        return f"simah.rules: {self._state.upper()}"


# --------------------------------------------------------------------------- #
#  what it reads                                                               #
# --------------------------------------------------------------------------- #

def test_one_observation_per_article_keyed_on_the_page_and_the_article():
    obs = signal(page(LAW)).sweep()
    assert [o.key for o in obs] == [
        "page=rules-and-regulations|article=Article-1",
        "page=rules-and-regulations|article=Article-2",
        "page=rules-and-regulations|article=Article-3"]
    assert obs[0].identity_fields == ("page", "article")
    assert obs[0].basis == sa.BASIS_ARTICLE


def test_the_hash_is_over_text_not_markup():
    """The open panel carries `collapse show`, `aria-expanded` and a style
    attribute. Whichever article was open when the page was captured must not
    decide whether the law reads as changed."""
    first = {o.key: o.token for o in signal(page(LAW, open_index=1)).sweep()}
    third = {o.key: o.token for o in signal(page(LAW, open_index=3)).sweep()}
    assert first == third


def test_whitespace_and_entities_do_not_move_a_hash():
    a = signal(page(["The Central   Bank shall oversee &amp; monitor."])).sweep()
    b = signal(page(["The Central Bank shall oversee & monitor.\n"])).sweep()
    assert a[0].token == b[0].token


def test_script_and_style_text_is_not_part_of_the_law():
    noisy = page(LAW).replace("</body>", "<script>var t=Date.now()</script></body>")
    assert [o.token for o in signal(noisy).sweep()] == [
        o.token for o in signal(page(LAW)).sweep()]


def test_an_unlabelled_item_keeps_its_position_rather_than_disappearing():
    html = page(LAW).replace("> Article-2 <", "><")
    keys = [o.key for o in signal(html).sweep()]
    assert "page=rules-and-regulations|article=item-2" in keys
    assert len(keys) == 3


# --------------------------------------------------------------------------- #
#  what it is FOR — which article moved                                        #
# --------------------------------------------------------------------------- #

def test_an_amendment_names_the_article_that_moved(tmp_path):
    store = store_at(tmp_path)
    cs.run_sweep(signal(page(LAW)), store)

    amended = list(LAW)
    amended[1] = "This Law aims at establishing general principles and controls."
    report, buckets = cs.run_sweep(signal(page(amended)), store)

    assert report["counts"][cs.MODIFIED] == 1
    assert report["counts"][cs.UNCHANGED] == 2
    assert "page=rules-and-regulations|article=Article-2" in keyed(buckets,
                                                                  cs.MODIFIED)


def test_a_banner_that_moved_outside_the_articles_changes_nothing(tmp_path):
    store = store_at(tmp_path)
    cs.run_sweep(signal(page(LAW)), store)

    # The whole-page sha256 in the manifest moves here. Per article, nothing did.
    rotated = page(LAW).replace("<body>", '<body><div class="promo">Offer B</div>')
    report, _buckets = cs.run_sweep(signal(rotated), store)
    assert report["counts"][cs.MODIFIED] == 0
    assert report["counts"][cs.UNCHANGED] == 3


def test_a_repealed_article_is_missing_and_the_streak_counts(tmp_path):
    store = store_at(tmp_path)
    cs.run_sweep(signal(page(LAW)), store)

    report, buckets = cs.run_sweep(signal(page(LAW[:2])), store)
    assert report["counts"][cs.MISSING] == 1
    gone = keyed(buckets, cs.MISSING)
    assert "1 consecutive" in gone["page=rules-and-regulations|article=Article-3"]

    _report, buckets = cs.run_sweep(signal(page(LAW[:2])), store)
    assert "2 consecutive" in keyed(buckets, cs.MISSING)[
        "page=rules-and-regulations|article=Article-3"]


def test_a_new_article_is_new(tmp_path):
    store = store_at(tmp_path)
    cs.run_sweep(signal(page(LAW)), store)
    report, buckets = cs.run_sweep(signal(page(LAW + ["Article four."])), store)
    assert report["counts"][cs.NEW] == 1
    assert "page=rules-and-regulations|article=Article-4" in keyed(buckets, cs.NEW)


# --------------------------------------------------------------------------- #
#  the two refusals                                                            #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("html", [
    "",
    "<html><body>Sorry, you have been blocked</body></html>",
    "<div class='accordion-item'><h2>Article-1</h2></div>",      # markup moved
])
def test_a_page_it_cannot_parse_raises_rather_than_emptying_the_law(html):
    with pytest.raises(ValueError):
        signal(html).sweep()


def test_an_unparseable_page_records_nothing(tmp_path):
    store = store_at(tmp_path)
    cs.run_sweep(signal(page(LAW)), store)
    with pytest.raises(ValueError):
        cs.run_sweep(signal("<html>blocked</html>"), store)
    assert len(store.keys()) == 3
    assert all(r["misses"] == 0 for r in store.records.values())


def test_a_stale_snapshot_is_refused_and_can_be_swept_knowingly():
    stale = FakeSnapshot(page(LAW), state="stale")
    with pytest.raises(ValueError, match="stale"):
        sa.SnapshotArticleSweep("simah.rules", store=stale).sweep()

    knowing = sa.SnapshotArticleSweep("simah.rules", store=stale,
                                      allow_stale=True)
    assert len(knowing.sweep()) == 3


def test_a_missing_snapshot_raises():
    with pytest.raises(ValueError, match="no snapshot"):
        sa.SnapshotArticleSweep("simah.rules",
                                store=FakeSnapshot(state="missing")).sweep()


def test_an_aging_snapshot_is_swept_and_its_state_is_reported():
    sig = sa.SnapshotArticleSweep("simah.rules",
                                  store=FakeSnapshot(page(LAW), state="aging"))
    sig.sweep()
    assert sig.stats["snapshot"] == "aging"
    assert sig.stats["captured_at"] == "2026-08-05T02:01:19+00:00"


# --------------------------------------------------------------------------- #
#  the page itself, when it is on this machine                                 #
# --------------------------------------------------------------------------- #

@pytest.mark.skipif(not REAL.exists(),
                    reason="the snapshot lives under output/ and is not committed")
def test_the_saved_page_is_seventeen_articles_of_law():
    found = sa.articles(REAL.read_text(encoding="utf-8"))
    assert len(found) == 17
    assert [label for label, _t in found] == [f"Article-{i}" for i in range(1, 18)]
    assert sum(len(t) for _l, t in found) == 8025
