"""GOSI's CMS returns the whole page, so this is the sweep that can report a loss.

WHY THIS FILE LOOKS LIKE THIS

No network. The JSON is handed to the adapter directly, shaped exactly as the
captured response: 6 instruments / 45 sections for SocialInsurance, 2 / 51 for
Saned, one page date shared by every instrument, and 6 PDF links carrying the
triple slash GOSI actually publishes. `fingerprint.tokens_for` is replaced with
the ETags those documents answered with.

What is verified here:

  identity      instruments key on (page, system_id), documents on
                (page, document_url) -- two shapes in one store, no collision
  the date      only SystemsList[].LastPublishedDate is read. The page's own
                ModifiedDate is frozen at 2022 and the platform date is a
                site-wide cache-buster; neither may reach a verdict
  the guard     a response this sweep cannot read RAISES. It reports absences,
                so an unreadable page must produce no report rather than one
                saying the whole library was withdrawn
  coverage      an instrument that vanished is `missing` with a streak -- and
                reading the page without its documents turns coverage off
                entirely, because the documents skipped would be the absence
  confirm tier  one page date moves all six instruments, so the hash decides:
                same sections is a republish, moved sections is an amendment.
                A document's version counter is proof on its own and is never
                sent for confirmation
  documents     the triple slash survives, links dedupe, non-documents are
                ignored, and a blocked host is still OBSERVED so that being
                banned from a host can never look like a withdrawal

    venv/Scripts/python.exe -m pytest tests/test_gosi_signal.py -v
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
from dynamic_crawler import fingerprint                                # noqa: E402
from dynamic_crawler import gosi_signal as gs                          # noqa: E402
from dynamic_crawler.change_state import ChangeStateStore              # noqa: E402


# --------------------------------------------------------------------------- #
#  the response, as it was captured                                            #
# --------------------------------------------------------------------------- #

PDF = ("https://cmsgosi.gosi.gov.sa///sites/en/SystemsAndRegulations/"
       "SocialInsurance/Documents/vaolition%20rules%20en.pdf")
OSH = ("https://cmsgosi.gosi.gov.sa///sites/en/SystemsAndRegulations/"
       "SocialInsurance/Documents/OSH.pdf")

PAGE_DATE = "2026/06/01 09:45:26 AM"


def section(id, title="Definitions", content="<p>text</p>") -> dict:
    return {"Created": None, "ID": id, "Title": title, "Content": content}


def payload(systems, code=0, **kw) -> dict:
    """The envelope, with the two date fields that must never be read."""
    out = {"PageContent": "...", "PageTitle": "Social Insurance Law",
           "CreatedDate": "2022-07-25 02:53:13 PM",
           "ModifiedDate": "2022-12-20 09:18:25 AM",     # frozen since 2022
           "PublishedDate": "",
           "SystemsList": systems, "ReturnCode": code, "Message": ""}
    out.update(kw)
    return out


def systems(date=PAGE_DATE, count=2, links=(PDF,)) -> list:
    """`count` instruments sharing one publish date, the first one linking out."""
    out = []
    for i in range(1, count + 1):
        content = (f'<p>See <a href="{links[0]}">Here</a></p>'
                   if i == 1 and links else "<p>body</p>")
        out.append({"Created": None, "ID": i, "Title": f"Instrument {i}",
                    "LastPublishedDate": date,
                    "ContentList": [section(1, content=content),
                                    section(2)]})
    return out


def signal(data, **kw) -> gs.GosiJsonSweep:
    kw.setdefault("seed", "SocialInsurance")
    return gs.GosiJsonSweep(fetch_json=lambda: data, **kw)


def answers(tokens: dict):
    return lambda urls, workers=None, timeout=20.0: {
        u: tokens.get(u, ("", fingerprint.BASIS_FAILED)) for u in urls}


@pytest.fixture
def probed(monkeypatch):
    def install(tokens):
        monkeypatch.setattr(gs.fingerprint, "tokens_for", answers(tokens))
    return install


def store_at(tmp_path) -> ChangeStateStore:
    return ChangeStateStore(tmp_path / "state.json", source="GOSI/SocialInsurance")


def keyed(buckets, verdict) -> dict:
    """{identity key: reason} for one bucket, whatever the bucket holds."""
    return {(o.key if hasattr(o, "key") else o): why
            for o, why in buckets[verdict]}


# --------------------------------------------------------------------------- #
#  identity                                                                    #
# --------------------------------------------------------------------------- #

def test_instruments_key_on_the_page_and_the_system_id(probed):
    probed({PDF: ("{G},22", "etag")})
    obs = signal(payload(systems())).sweep()

    instruments = [o for o in obs if "system_id" in o.fields]
    assert [o.key for o in instruments] == [
        "page=SocialInsurance|system_id=1", "page=SocialInsurance|system_id=2"]
    assert instruments[0].identity_fields == ("page", "system_id")
    assert instruments[0].url.endswith("/SocialInsurance#1")


def test_documents_key_on_their_url_and_never_collide_with_an_instrument(probed):
    probed({PDF: ("{G},22", "etag")})
    obs = signal(payload(systems())).sweep()

    documents = [o for o in obs if "document_url" in o.fields]
    assert len(documents) == 1
    assert documents[0].key == f"page=SocialInsurance|document_url={PDF}"
    assert documents[0].identity_fields == ("page", "document_url")
    assert len({o.key for o in obs}) == len(obs)      # two shapes, one store


def test_the_two_seeds_are_two_sources(probed):
    probed({})
    saned = signal(payload(systems(count=2, links=())), seed="Saned").sweep()
    assert all(o.fields["page"] == "Saned" for o in saned)
    assert signal(payload(systems()), seed="Saned").name == "gosi-json:Saned"


# --------------------------------------------------------------------------- #
#  which date                                                                  #
# --------------------------------------------------------------------------- #

def test_the_instrument_date_is_the_token(probed):
    probed({PDF: ("{G},22", "etag")})
    obs = [o for o in signal(payload(systems())).sweep() if "system_id" in o.fields]
    assert {o.token for o in obs} == {PAGE_DATE}
    assert obs[0].basis == gs.BASIS_PAGE_DATE


def test_the_page_and_platform_dates_are_never_read(probed):
    probed({PDF: ("{G},22", "etag")})
    data = payload(systems(), LastPublishedDate="2026/03/14 12:01:21 AM")
    tokens = {o.token for o in signal(data).sweep() if "system_id" in o.fields}

    # ModifiedDate is the container list item, frozen at 2022; the top-level
    # LastPublishedDate is the site-wide cache-buster the app caches.
    assert tokens == {PAGE_DATE}
    assert "2022-12-20 09:18:25 AM" not in tokens
    assert "2026/03/14 12:01:21 AM" not in tokens


def test_an_instrument_with_no_date_says_so_rather_than_reading_unchanged(probed):
    probed({})
    data = payload([{"ID": 1, "Title": "x", "LastPublishedDate": "",
                     "ContentList": [section(1)]}])
    obs = signal(data).sweep()[0]
    assert obs.token == ""
    assert obs.basis == fingerprint.BASIS_NONE


# --------------------------------------------------------------------------- #
#  the guard on a page it cannot read                                          #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("data", [
    payload([], code=0),                        # 200 with an empty tab strip
    payload(systems(), code=1),                 # the API's own error code
    payload(None),                              # the key is there, holding null
    "<html>blocked</html>",                     # a firewall page behind a 200
])
def test_a_response_it_cannot_read_raises_rather_than_reporting_a_loss(data, probed):
    probed({})
    with pytest.raises(ValueError):
        signal(data).sweep()


def test_the_guard_runs_before_anything_is_recorded(tmp_path, probed):
    probed({PDF: ("{G},22", "etag")})
    store = store_at(tmp_path)
    cs.run_sweep(signal(payload(systems())), store)
    assert len(store.keys()) == 3

    # The next sweep cannot read the page. It must not empty the library.
    with pytest.raises(ValueError):
        cs.run_sweep(signal(payload([])), store)
    assert len(store.keys()) == 3
    assert all(r.get("misses") == 0 for r in store.records.values())


# --------------------------------------------------------------------------- #
#  coverage — the half no other signal has                                     #
# --------------------------------------------------------------------------- #

def test_an_instrument_that_vanished_is_missing_and_the_streak_counts(tmp_path,
                                                                     probed):
    probed({PDF: ("{G},22", "etag")})
    store = store_at(tmp_path)
    cs.run_sweep(signal(payload(systems(count=2))), store)

    report, buckets = cs.run_sweep(signal(payload(systems(count=1))), store)
    assert report["counts"][cs.MISSING] == 1
    gone = keyed(buckets, cs.MISSING)
    assert "page=SocialInsurance|system_id=2" in gone
    assert "1 consecutive" in gone["page=SocialInsurance|system_id=2"]

    _report, buckets = cs.run_sweep(signal(payload(systems(count=1))), store)
    assert "2 consecutive" in keyed(buckets, cs.MISSING)[
        "page=SocialInsurance|system_id=2"]


def test_being_seen_again_clears_the_streak(tmp_path, probed):
    probed({PDF: ("{G},22", "etag")})
    store = store_at(tmp_path)
    cs.run_sweep(signal(payload(systems(count=2))), store)
    cs.run_sweep(signal(payload(systems(count=1))), store)
    cs.run_sweep(signal(payload(systems(count=2))), store)
    assert store.get("page=SocialInsurance|system_id=2")["misses"] == 0


def test_reading_the_page_without_its_documents_reports_no_absence_at_all(tmp_path,
                                                                         probed):
    probed({PDF: ("{G},22", "etag")})
    store = store_at(tmp_path)
    cs.run_sweep(signal(payload(systems())), store)

    # The documents are in the store and this sweep did not look at them, so it
    # is no longer entitled to call anything absent.
    page_only = signal(payload(systems()), probe_documents=False)
    assert page_only.covers_inventory is False
    report, _buckets = cs.run_sweep(page_only, store)
    assert cs.MISSING not in report["counts"]
    assert "not measured" in report["missing"]


# --------------------------------------------------------------------------- #
#  the confirm tier                                                            #
# --------------------------------------------------------------------------- #

def test_the_baseline_sweep_stores_a_hash_it_was_never_asked_for(tmp_path, probed):
    """Otherwise the FIRST date move has nothing to compare against and
    shortlists the whole page — the case the tier exists for."""
    probed({PDF: ("{G},22", "etag")})
    store = store_at(tmp_path)
    report, _buckets = cs.run_sweep(signal(payload(systems())), store)

    assert report["confirmed"] == 0                  # nothing was confirmed
    assert store.get("page=SocialInsurance|system_id=1")["confirm_hash"]
    # A document's counter is proof on its own, so it stores no hash.
    assert store.get(f"page=SocialInsurance|document_url={PDF}")["confirm_hash"] == ""


def test_a_republish_that_moved_no_text_is_unchanged(tmp_path, probed):
    probed({PDF: ("{G},22", "etag")})
    store = store_at(tmp_path)
    cs.run_sweep(signal(payload(systems())), store)

    later = payload(systems(date="2026/07/02 11:00:00 AM"))
    report, buckets = cs.run_sweep(signal(later), store)
    assert report["counts"][cs.MODIFIED] == 0
    assert report["counts"][cs.UNCHANGED] == 3
    assert "bulk republish" in keyed(buckets, cs.UNCHANGED)[
        "page=SocialInsurance|system_id=1"]


def test_a_republish_whose_text_moved_is_modified(tmp_path, probed):
    probed({PDF: ("{G},22", "etag")})
    store = store_at(tmp_path)
    cs.run_sweep(signal(payload(systems())), store)

    amended = systems(date="2026/07/02 11:00:00 AM")
    amended[1]["ContentList"][0]["Content"] = "<p>a new article</p>"
    report, buckets = cs.run_sweep(signal(payload(amended)), store)

    assert report["counts"][cs.MODIFIED] == 1
    assert "page=SocialInsurance|system_id=2" in keyed(buckets, cs.MODIFIED)


def test_a_documents_version_counter_is_proof_and_is_never_confirmed(tmp_path,
                                                                     probed):
    probed({PDF: ("{G},22", "etag")})
    store = store_at(tmp_path)
    cs.run_sweep(signal(payload(systems())), store)

    probed({PDF: ("{G},23", "etag")})
    moved = signal(payload(systems()))
    moved.confirm = lambda obs: pytest.fail("a document must not be confirmed")
    report, buckets = cs.run_sweep(moved, store)

    assert report["counts"][cs.MODIFIED] == 1
    assert f"page=SocialInsurance|document_url={PDF}" in keyed(buckets, cs.MODIFIED)
    assert report["confirmed"] == 0


def test_the_confirm_hash_costs_no_second_request(probed):
    probed({PDF: ("{G},22", "etag")})
    sig = signal(payload(systems()))
    obs = [o for o in sig.sweep() if "system_id" in o.fields][0]
    assert sig.confirm(obs) == gs.content_hash(
        payload(systems())["SystemsList"][0]["ContentList"])


def test_a_reordered_section_list_is_not_an_amendment():
    sections = [section(1, "A"), section(2, "B"), section(10, "C")]
    assert gs.content_hash(sections) == gs.content_hash(list(reversed(sections)))
    assert gs.content_hash(sections) != gs.content_hash(
        [section(1, "A"), section(2, "B changed"), section(10, "C")])


# --------------------------------------------------------------------------- #
#  the document links                                                          #
# --------------------------------------------------------------------------- #

def test_the_triple_slash_is_left_exactly_as_published(probed):
    probed({PDF: ("{G},22", "etag")})
    obs = [o for o in signal(payload(systems())).sweep()
           if "document_url" in o.fields][0]
    assert obs.url == PDF
    assert "///sites/" in obs.url


def test_links_dedupe_and_non_documents_are_ignored(probed):
    probed({PDF: ("{G},22", "etag"), OSH: ("{H},12", "etag")})
    sys_list = systems()
    sys_list[0]["ContentList"][1]["Content"] = (
        f'<a href="{PDF}">again</a>'
        f'<a href="{OSH}">osh</a>'
        '<a href="https://www.gosi.gov.sa/en/About">a page, not a document</a>'
        '<a href="mailto:info@gosi.gov.sa">mail</a>')
    urls = gs.document_links(sys_list)
    assert urls == [PDF, OSH]


def test_an_href_is_unescaped_before_it_is_probed():
    content = ('<a href="https://cmsgosi.gosi.gov.sa/x.pdf?a=1&amp;b=2">x</a>')
    assert gs.document_links([{"ContentList": [section(1, content=content)]}]) == [
        "https://cmsgosi.gosi.gov.sa/x.pdf?a=1&b=2"]


def test_a_blocked_host_is_still_observed_so_it_cannot_look_like_a_withdrawal(
        tmp_path, probed):
    probed({})
    sig = signal(payload(systems()), skip_hosts=["cmsgosi.gosi.gov.sa"])
    report, buckets = cs.run_sweep(sig, store_at(tmp_path))

    assert sig.stats["documents_blocked"] == 1
    assert report["observed"] == 3                       # the document is seen
    assert report["counts"][cs.MISSING] == 0
    obs = [o for o in sig.sweep() if "document_url" in o.fields][0]
    assert obs.basis == gs.BASIS_BLOCKED


def test_a_probe_that_failed_is_unknown_not_unchanged(tmp_path, probed):
    probed({PDF: ("{G},22", "etag")})
    store = store_at(tmp_path)
    cs.run_sweep(signal(payload(systems())), store)

    probed({})                                           # every probe fails now
    report, buckets = cs.run_sweep(signal(payload(systems())), store)
    assert f"page=SocialInsurance|document_url={PDF}" in keyed(buckets, cs.UNKNOWN)
    assert store.get(f"page=SocialInsurance|document_url={PDF}")["token"] == "{G},22"


# --------------------------------------------------------------------------- #
#  the shape it was measured with                                              #
# --------------------------------------------------------------------------- #

def test_the_report_carries_the_measured_shape_of_the_page(tmp_path, probed):
    probed({PDF: ("{G},22", "etag")})
    sig = signal(payload(systems(count=6)))
    report, _buckets = cs.run_sweep(sig, store_at(tmp_path))

    assert sig.stats["instruments"] == 6
    assert sig.stats["sections"] == 12
    assert sig.stats["page_dates"] == [PAGE_DATE]
    assert sig.stats["documents_found"] == 1
    assert report["observed"] == 7


def test_the_endpoint_is_the_one_that_was_sniffed():
    assert gs.endpoint("SocialInsurance") == (
        "https://cmsapi.gosi.gov.sa/api/SharePoint/GetSiteContent"
        "?parentSiteUrl=SystemsAndRegulations&siteUrl=SocialInsurance&lang=en")
