"""A sitemap is the cheapest signal and the easiest one to adopt wrongly.

WHY THIS FILE LOOKS LIKE THIS

No network. The XML is handed to the adapter directly, shaped as the four
sitemaps measured across the regulators: one with a real per-url edit history,
one carrying its own build time on every url, one whose good timestamps belong
to a newsroom, and one that is an index rather than a urlset.

What is verified here:

  the guards    both questions are counted over the documents WE TRACK, not
                over the sitemap -- a build-time sitemap and a newsroom sitemap
                each fail one of them, and a sitemap that fails either is
                refused by the adapter rather than by a runbook
  matching      stored urls are percent-encoded and the sitemap publishes the
                same paths as literal Arabic; compared raw nothing matches
  absences      a tracked document missing from the sitemap is reported and is
                NOT observed, recorded or counted as withdrawn. Absence from a
                sitemap is not absence from the regulator, measured
  the hash      the trailing "related regulations" view is cut off first, so a
                page whose only movement is its neighbours' cards hashes the
                same. That view is 68-92% of the text
  the tiers     the date shortlists and the hash decides: a CMS operation that
                re-stamps a group and edits no text reads `unchanged` -- once a
                hash is stored, which the FIRST such event pays for

    venv/Scripts/python.exe -m pytest tests/test_sitemap_signal.py -v
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
from dynamic_crawler import sitemap_signal as ss                       # noqa: E402
from dynamic_crawler.change_state import ChangeStateStore              # noqa: E402


# --------------------------------------------------------------------------- #
#  the sitemaps, shaped as measured                                            #
# --------------------------------------------------------------------------- #

BASE = "https://www.hrsd.gov.sa/en/knowledge-centre/decisions-and-regulations/regulation-and-procedures"

#: The library stores this percent-encoded; the sitemap publishes it decoded.
ARABIC = "%D8%A7%D9%84%D8%AF%D9%84%D9%8A%D9%84"
ARABIC_PLAIN = "الدليل"

#: Twenty documents, not four: both guards are ratios, and at four the coarsest
#: possible sitemap still scores 0.25. The measured source has 62.
TRACKED = ([f"{BASE}/{ARABIC}"]
           + [f"{BASE}/regulation-{n:02d}" for n in range(1, 20)])

#: 8 of 20 sharing one timestamp, in the proportion measured (26 of 62): one CMS
#: operation, not 26 amendments. 13 distinct over 20 is a ratio of 0.65 against
#: the 0.60 measured, and against 0.004-0.009 for a sitemap's own build time.
BULK = "2025-05-14T09:00:00+03:00"
STAMPS = ([BULK] * 8
          + [f"2026-0{1 + n // 28}-{1 + n % 28:02d}T12:41:58+03:00"
             for n in range(12)])

NS = 'xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"'

#: What three of the five measured sitemaps carry: one value, repeated.
BUILD_TIME = ["2026-08-05T02:00:00+03:00"] * len(TRACKED)


def urlset(pairs, ns: str = NS) -> str:
    body = "".join(
        f"<url><loc>{loc}</loc>"
        + (f"<lastmod>{stamp}</lastmod>" if stamp else "")
        + "</url>"
        for loc, stamp in pairs)
    return f'<?xml version="1.0" encoding="UTF-8"?><urlset {ns}>{body}</urlset>'


def mhrsd_pairs(stamps=None):
    """Every tracked document, with the Arabic one published decoded."""
    stamps = stamps or STAMPS
    locs = [f"{BASE}/{ARABIC_PLAIN}"] + TRACKED[1:]
    return list(zip(locs, stamps))


def sweep_for(pairs, tracked=None, **kw):
    return ss.SitemapLastmodSweep(
        "https://www.hrsd.gov.sa/sitemap.xml?page=7", "MHRSD/mhrsd.regs",
        TRACKED if tracked is None else tracked,
        fetch_xml=lambda: urlset(pairs), **kw)


# --------------------------------------------------------------------------- #
#  reading the xml                                                             #
# --------------------------------------------------------------------------- #

def test_parses_loc_and_lastmod():
    got = ss.parse_sitemap(urlset(mhrsd_pairs()))
    assert len(got) == len(TRACKED)
    assert got[0][1] == BULK


def test_parses_without_a_namespace():
    got = ss.parse_sitemap(urlset(mhrsd_pairs(), ns=""))
    assert len(got) == len(TRACKED)


def test_a_url_with_no_lastmod_is_kept_with_an_empty_stamp():
    got = ss.parse_sitemap(urlset([(f"{BASE}/a", ""), (f"{BASE}/b", "2026-01-01")]))
    assert got == [(f"{BASE}/a", ""), (f"{BASE}/b", "2026-01-01")]


def test_an_index_raises_and_names_a_child():
    """Read as an empty urlset it would fail the overlap guard, which blames
    the source rather than the url that was asked for."""
    xml = ('<?xml version="1.0" encoding="UTF-8"?>'
           f'<sitemapindex {NS}>'
           "<sitemap><loc>https://x/sitemap.xml?page=7</loc></sitemap>"
           "<sitemap><loc>https://x/sitemap.xml?page=8</loc></sitemap>"
           "</sitemapindex>")
    with pytest.raises(ValueError) as e:
        ss.parse_sitemap(xml)
    assert "sitemapindex of 2" in str(e.value)
    assert "page=7" in str(e.value)


def test_something_that_is_not_a_urlset_raises():
    with pytest.raises(ValueError):
        ss.parse_sitemap('<?xml version="1.0"?><html><body/></html>')


def test_a_bom_does_not_reach_the_parser():
    class _R:
        content = ("﻿" + urlset(mhrsd_pairs())).encode("utf-8")
        status_code = 200

        def raise_for_status(self):
            return None

    class _S:
        def get(self, url, **kw):
            return _R()

    text = ss.fetch_sitemap("https://x/sitemap.xml", session=_S())
    assert not text.startswith("﻿")
    assert len(ss.parse_sitemap(text)) == len(TRACKED)


# --------------------------------------------------------------------------- #
#  matching a stored url to a published one                                    #
# --------------------------------------------------------------------------- #

def test_percent_encoding_and_a_trailing_slash_do_not_decide_a_match():
    assert ss.normalise(f"{BASE}/{ARABIC}/") == ss.normalise(f"{BASE}/{ARABIC_PLAIN}")


def test_the_encoded_document_is_observed():
    obs = sweep_for(mhrsd_pairs()).sweep()
    assert len(obs) == len(TRACKED)
    assert any(ARABIC_PLAIN in o.fields["document_url"] for o in obs)


# --------------------------------------------------------------------------- #
#  the two guards                                                              #
# --------------------------------------------------------------------------- #

def test_a_real_edit_history_passes_both():
    got = ss.assess(mhrsd_pairs(), TRACKED)
    assert got["usable"] and got["why_not"] == []
    assert got["matched"] == 20 and got["overlap"] == 1.0
    assert got["distinct_lastmod"] == 13 and got["distinct_ratio"] == 0.65


def test_one_timestamp_repeated_is_a_build_time_and_is_refused():
    """229 urls, 229 lastmod, 1 distinct value: the sitemap's own build time."""
    pairs = mhrsd_pairs(BUILD_TIME)
    got = ss.assess(pairs, TRACKED)
    assert not got["usable"]
    assert got["overlap"] == 1.0                     # question two says yes
    assert "build time" in " ".join(got["why_not"])


def test_a_newsroom_sitemap_passes_the_timestamps_and_fails_the_overlap():
    """500 of 500 distinct -- better hygiene than the good one -- and none of
    them is a document we track."""
    news = [(f"https://www.moh.gov.sa/Ministry/MediaCenter/News/{n}",
             f"2026-0{n}-01T10:00:00+03:00") for n in range(1, 6)]
    got = ss.assess(news, TRACKED)
    assert not got["usable"]
    assert got["distinct_ratio"] == 0.0 and got["matched"] == 0
    assert "not a sitemap of this source" in " ".join(got["why_not"])


def test_the_ratio_is_counted_over_what_we_track_not_over_the_sitemap():
    """A sitemap can be healthy site-wide and frozen on exactly our subset."""
    others = [(f"{BASE}/../news/{n}", f"2026-01-{n:02d}T10:00:00+03:00")
              for n in range(1, 40)]
    frozen = mhrsd_pairs(BUILD_TIME)
    got = ss.assess(frozen + others, TRACKED)
    assert got["urls_in_sitemap"] == 59
    assert got["with_lastmod"] == 20 and got["distinct_lastmod"] == 1
    assert not got["usable"]


def test_no_stored_urls_is_refused_rather_than_scored_zero():
    got = ss.assess(mhrsd_pairs(), [])
    assert not got["usable"]
    assert "no stored urls" in " ".join(got["why_not"])


def test_the_numbers_are_in_the_refusal():
    why = " ".join(ss.assess(mhrsd_pairs(BUILD_TIME), TRACKED)["why_not"])
    assert "1 distinct lastmod over 20" in why


def test_a_refused_sitemap_stops_the_sweep():
    signal = sweep_for(mhrsd_pairs(BUILD_TIME))
    with pytest.raises(ValueError) as e:
        signal.sweep()
    assert "not a change signal for this source" in str(e.value)


def test_the_thresholds_can_be_moved_per_site():
    assert ss.assess(mhrsd_pairs(BUILD_TIME), TRACKED, min_distinct=0.0)["usable"]


# --------------------------------------------------------------------------- #
#  a document the sitemap does not hold                                        #
# --------------------------------------------------------------------------- #

def test_a_tracked_document_absent_from_the_sitemap_is_reported_not_observed():
    """Measured: one stored url 308s to a slug that 404s while the regulation
    is live at the same slug without /en/. It is not a withdrawal."""
    signal = sweep_for(mhrsd_pairs()[:-1])
    obs = signal.sweep()
    assert len(obs) == len(TRACKED) - 1
    assert signal.stats["not_in_sitemap"] == [ss.normalise(TRACKED[-1])]


def test_an_absence_never_reaches_the_store(tmp_path):
    store = ChangeStateStore(tmp_path / "s.json", source="MHRSD/mhrsd.regs")
    signal = sweep_for(mhrsd_pairs()[:-1])
    report, buckets = cs.run_sweep(signal, store)
    assert cs.MISSING not in report["counts"]
    assert "not measured" in report["missing"]
    assert ss.normalise(TRACKED[-1]) not in " ".join(store.keys())


def test_a_document_the_sitemap_has_and_we_do_not_store_is_an_addition():
    pairs = mhrsd_pairs() + [(f"{BASE}/brand-new-rule", "2026-08-01T10:00:00+03:00")]
    signal = sweep_for(pairs)
    signal.sweep()
    assert signal.stats["unstored_locs"] == [f"{BASE}/brand-new-rule"]


def test_the_addition_prefix_comes_off_the_stored_urls():
    assert ss.candidate_prefix(TRACKED) == f"{BASE}/"


def test_one_stored_url_gives_no_prefix_and_so_no_additions():
    """A prefix guessed from a single url is that url, and everything else on
    the site would read as an addition."""
    assert ss.candidate_prefix(TRACKED[:1]) == ""


# --------------------------------------------------------------------------- #
#  the confirm hash                                                            #
# --------------------------------------------------------------------------- #

OWN = ('<div class="block block-field-block-node-regulation-and-procedure-body">'
       "<p>Article 1. The employer shall.</p></div>")
RELATED = ('<div class="block block-views-block-regulations-and-procedures-related">'
           '<div class="card"><h2>Some other regulation</h2></div></div>')
OTHER_RELATED = ('<div class="block block-views-block-regulations-and-procedures-related">'
                 '<div class="card"><h2>A different other regulation</h2></div></div>')


def test_the_related_view_is_cut_before_hashing():
    text, was_cut = ss.page_text(OWN + RELATED)
    assert was_cut and "Article 1" in text
    assert "other regulation" not in text


def test_a_page_whose_neighbours_changed_hashes_the_same():
    """68-92% of the text is the neighbours' cards, so without the cut every
    page moves whenever the ministry publishes anything."""
    assert ss.page_hash(OWN + RELATED)[0] == ss.page_hash(OWN + OTHER_RELATED)[0]


def test_an_amendment_still_moves_the_hash():
    amended = OWN.replace("The employer shall", "The employer shall not")
    assert ss.page_hash(OWN + RELATED)[0] != ss.page_hash(amended + RELATED)[0]


def test_scripts_and_whitespace_do_not_reach_the_hash():
    noisy = ('<div class="block block-field-block-node-regulation-and-procedure-body">'
             "<script>var t=Date.now()</script>\n\n   <p>Article 1.  The "
             "employer   shall.</p></div>")
    assert ss.page_hash(noisy + RELATED)[0] == ss.page_hash(OWN + RELATED)[0]


def test_a_missing_marker_hashes_the_whole_page_and_says_so():
    digest, was_cut = ss.page_hash(OWN)
    assert digest and not was_cut


def test_the_uncut_pages_are_counted(monkeypatch):
    signal = sweep_for(mhrsd_pairs())
    signal.sweep()
    _serve(monkeypatch, {u: OWN for u in [ss.normalise(t) for t in TRACKED]})
    signal.confirm(cs.Observation(key="k", url=ss.normalise(TRACKED[0])))
    assert signal.stats["confirm_uncut"] == 1


# --------------------------------------------------------------------------- #
#  the two tiers together                                                      #
# --------------------------------------------------------------------------- #

def _serve(monkeypatch, pages: dict):
    """requests.get answering with the page bodies given."""
    class _R:
        def __init__(self, body):
            self.content = body.encode("utf-8")
            self.status_code = 200

        def raise_for_status(self):
            return None

    monkeypatch.setattr(ss.requests, "get",
                        lambda url, **kw: _R(pages.get(url, OWN + RELATED)))


def _run(signal, store):
    report, buckets = cs.run_sweep(signal, store)
    return report["counts"], buckets


def test_first_sweep_is_a_baseline_then_nothing_changes(tmp_path, monkeypatch):
    _serve(monkeypatch, {})
    store = ChangeStateStore(tmp_path / "s.json", source="MHRSD/mhrsd.regs")

    counts, _ = _run(sweep_for(mhrsd_pairs()), store)
    assert counts[cs.NEW] == len(TRACKED)

    counts, _ = _run(sweep_for(mhrsd_pairs()), store)
    assert counts[cs.UNCHANGED] == len(TRACKED) and counts[cs.MODIFIED] == 0


LATER = "2026-08-10T08:00:00+03:00"


def _moved(index=None, when: str = LATER):
    """One timestamp moved, or the bulk group re-stamped together.

    The bulk group is 8 of 20 because the measured event was 26 of 62. A CMS
    operation that re-stamped ALL of them would be indistinguishable from a
    sitemap carrying only its own build time -- see the test below.
    """
    stamps = list(STAMPS)
    if index is None:
        return [when if s == BULK else s for s in stamps]
    stamps[index] = when
    return stamps


def test_a_sitemap_that_restamps_every_document_at_once_is_refused(tmp_path):
    """Deliberate, and the loud half of the trade-off.

    From one sweep, 20 documents sharing one new timestamp and a sitemap that
    only ever publishes its build time are the same reading. The refusal names
    the numbers, so the day it happens a person looks -- which is the right
    answer, because a CMS regression is the commoner cause of the two.
    """
    with pytest.raises(ValueError) as e:
        sweep_for(mhrsd_pairs(BUILD_TIME)).sweep()
    assert "1 distinct lastmod over 20" in str(e.value)


def test_the_first_bulk_republish_shortlists_everything(tmp_path, monkeypatch):
    """Accepted, not unavoidable: a one-shot that pre-hashed every page was
    weighed and refused.

    The confirm hash is taken only for documents already ruled modified, so it
    is never stored at baseline -- and unlike GOSI the text does not arrive with
    the date, so storing one would mean fetching every page on every sweep. This
    event is what lays the hashes down, and it costs fewer fetches than pre-
    hashing the source would. A missing hash reads `modified`, never `unchanged`.
    """
    _serve(monkeypatch, {})
    store = ChangeStateStore(tmp_path / "s.json", source="MHRSD/mhrsd.regs")
    _run(sweep_for(mhrsd_pairs()), store)

    counts, buckets = _run(sweep_for(mhrsd_pairs(_moved())), store)
    assert counts[cs.MODIFIED] == 8
    assert "no stored content hash" in buckets[cs.MODIFIED][0][1]


def test_a_later_bulk_republish_is_absorbed(tmp_path, monkeypatch):
    """Once a hash is stored, the CMS operation that gave 26 of 62 one
    timestamp moves every date and no text, and the tier clears all of them."""
    _serve(monkeypatch, {})
    store = ChangeStateStore(tmp_path / "s.json", source="MHRSD/mhrsd.regs")
    _run(sweep_for(mhrsd_pairs()), store)
    _run(sweep_for(mhrsd_pairs(_moved())), store)      # stores the hashes

    counts, _ = _run(sweep_for(mhrsd_pairs(_moved(when="2026-09-01T08:00:00+03:00"))),
                     store)
    assert counts[cs.MODIFIED] == 0
    assert counts[cs.UNCHANGED] == len(TRACKED)


def test_a_real_amendment_survives_the_confirm_tier(tmp_path, monkeypatch):
    _serve(monkeypatch, {})
    store = ChangeStateStore(tmp_path / "s.json", source="MHRSD/mhrsd.regs")
    _run(sweep_for(mhrsd_pairs()), store)
    _run(sweep_for(mhrsd_pairs(_moved())), store)

    amended = OWN.replace("The employer shall", "The employer shall not")
    _serve(monkeypatch, {ss.normalise(TRACKED[1]): amended + RELATED})
    stamps = _moved()                      # what the store holds after two sweeps
    stamps[1] = "2026-09-01T08:00:00+03:00"
    counts, buckets = _run(sweep_for(mhrsd_pairs(stamps)), store)
    assert counts[cs.MODIFIED] == 1
    assert "content both moved" in buckets[cs.MODIFIED][0][1]


def test_an_unreadable_page_is_unknown_not_unchanged(tmp_path, monkeypatch):
    _serve(monkeypatch, {})
    store = ChangeStateStore(tmp_path / "s.json", source="MHRSD/mhrsd.regs")
    _run(sweep_for(mhrsd_pairs()), store)
    _run(sweep_for(mhrsd_pairs()), store)

    def _boom(url, **kw):
        raise OSError("connection reset")

    monkeypatch.setattr(ss.requests, "get", _boom)
    moved = list(STAMPS)
    moved[0] = "2026-08-10T08:00:00+03:00"
    counts, _ = _run(sweep_for(mhrsd_pairs(moved)), store)
    assert counts[cs.UNKNOWN] == 1 and counts[cs.MODIFIED] == 0


def test_only_the_shortlist_is_fetched(tmp_path, monkeypatch):
    """One request for the sitemap, and a page fetch only for what moved."""
    _serve(monkeypatch, {})
    store = ChangeStateStore(tmp_path / "s.json", source="MHRSD/mhrsd.regs")
    _run(sweep_for(mhrsd_pairs()), store)
    _run(sweep_for(mhrsd_pairs()), store)

    fetched = []
    original = ss.requests.get
    monkeypatch.setattr(ss.requests, "get",
                        lambda url, **kw: (fetched.append(url),
                                           original(url, **kw))[1])
    moved = list(STAMPS)
    moved[2] = "2026-08-10T08:00:00+03:00"
    _run(sweep_for(mhrsd_pairs(moved)), store)
    assert fetched == [ss.normalise(TRACKED[2])]


def test_the_store_keys_on_page_and_document_url(tmp_path, monkeypatch):
    _serve(monkeypatch, {})
    store = ChangeStateStore(tmp_path / "s.json", source="MHRSD/mhrsd.regs")
    _run(sweep_for(mhrsd_pairs()), store)
    key = sorted(store.keys())[0]
    assert key.startswith("page=MHRSD/mhrsd.regs|document_url=")
    assert store.get(key)["identity_fields"] == ["page", "document_url"]
