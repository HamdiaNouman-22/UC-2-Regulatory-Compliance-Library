"""JS_DETAIL — three ways a page's text vanished while every check still passed.

All three were found on SIMAH's real page, and all three would have produced a
wrong capture on a perfectly successful LIVE crawl. They were invisible until a
snapshot made it possible to run the form repeatedly without touching the site.

    1. <form> was deleted. SharePoint / ASP.NET wrap the WHOLE page in
       <form id="aspnetForm">, so 8,182 characters of law became 0 and left a
       444-character husk of markup behind.
    2. innerText only. SIMAH's law is 17 articles in an EXCLUSIVE bootstrap
       accordion (data-bs-parent), so at most one is ever open and rendered text
       sees one article in seventeen. Clicking cannot defeat that.
    3. No main-content candidate matched, so the fallback was <body> — which is
       where 1 turned fatal.

    venv/Scripts/python.exe -m pytest tests/test_formfill_detail_extraction.py -q
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dynamic_crawler.formfill.runner import JS_DETAIL  # noqa: E402

LAW = ("The following words and phrases, wherever mentioned in this Law, shall have "
       "the meanings assigned to them unless the context requires otherwise.")

# SIMAH's shape, reduced: everything inside an ASP.NET form, the articles in an
# exclusive accordion where only the first panel is open.
SHAREPOINT_PAGE = f"""
<html><head><title>Rules and Regulations</title>
<style>.collapse:not(.show){{display:none}}</style></head><body>
<form id="aspnetForm" method="post">
  <div id="s4-workspace">
    <span id="DeltaPlaceHolderMain">
      <div class="article-content">
        <h1>Rules and Regulations</h1>
        <div class="accordion" id="rulesAccordion">
          <div class="accordion-item"><button class="accordion-button">Article-1</button>
            <div class="accordion-collapse collapse show" data-bs-parent="#rulesAccordion">
              <div class="accordion-body">Article-1 {LAW}</div></div></div>
          <div class="accordion-item"><button class="accordion-button">Article-2</button>
            <div class="accordion-collapse collapse" data-bs-parent="#rulesAccordion">
              <div class="accordion-body">Article-2 {LAW}</div></div></div>
          <div class="accordion-item"><button class="accordion-button">Article-3</button>
            <div class="accordion-collapse collapse" data-bs-parent="#rulesAccordion">
              <div class="accordion-body">Article-3 {LAW}</div></div></div>
        </div>
        <div class="accordion-sub-hd"><h6>The Implementing Regulations</h6>
          <a href="/files/ir.pdf">Download PDF</a></div>
      </div>
    </span>
  </div>
  <footer>Sign In | Turn on more accessible mode | All rights reserved</footer>
</form>
</body></html>"""

# A normal page, to prove the fixes changed nothing for the six forms that work.
NORMAL_PAGE = f"""
<html><head><title>Circular</title></head><body>
<header>Site header</header>
<nav>Menu one Menu two</nav>
<main><h1>BPRD Circular No. 15</h1><p>{LAW}</p>
  <a href="/files/circular.pdf">Download</a></main>
<form><input name="q"><button>Search</button></form>
<footer>Footer text</footer>
</body></html>"""


@pytest.fixture(scope="module")
def page():
    pw = pytest.importorskip("playwright.sync_api")
    with pw.sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        pg = browser.new_page()
        yield pg
        browser.close()


def _detail(page, html):
    page.set_content(html, wait_until="domcontentloaded")
    return page.evaluate(JS_DETAIL)


# --------------------------------------------------------------------------- #
# the SharePoint page
# --------------------------------------------------------------------------- #

def test_a_page_wrapped_in_a_form_still_yields_its_text(page):
    """Defect 1. Deleting <form> deleted the entire document."""
    d = _detail(page, SHAREPOINT_PAGE)
    assert len(d["text"]) > 300, f"text was {len(d['text'])} chars — the form ate it"


def test_all_articles_are_captured_not_just_the_open_one(page):
    """Defects 2 and 3. Two of these three panels are collapsed; a rendered-text
    read sees only Article-1 and reports a healthy-looking result."""
    d = _detail(page, SHAREPOINT_PAGE)
    for n in (1, 2, 3):
        assert f"Article-{n}" in d["text"], f"Article-{n} missing from the capture"


def test_sharepoint_chrome_is_not_part_of_the_document(page):
    """The main-content placeholder is preferred over the container that also holds
    the ribbon and the footer."""
    d = _detail(page, SHAREPOINT_PAGE)
    assert "Sign In" not in d["text"]
    assert "accessible mode" not in d["text"]


def test_the_document_link_survives(page):
    d = _detail(page, SHAREPOINT_PAGE)
    assert any(l["href"].endswith("/files/ir.pdf") for l in d["links"])


# --------------------------------------------------------------------------- #
# the normal page — nothing may regress for the forms that already work
# --------------------------------------------------------------------------- #

def test_a_normal_page_still_prefers_main(page):
    d = _detail(page, NORMAL_PAGE)
    assert "BPRD Circular No. 15" in d["text"]
    assert LAW in d["text"]


def test_chrome_is_still_stripped_on_a_normal_page(page):
    """header / nav / footer removal is unchanged — only <form> handling moved from
    delete to unwrap."""
    d = _detail(page, NORMAL_PAGE)
    for junk in ("Site header", "Menu one", "Footer text"):
        assert junk not in d["text"], f"{junk!r} leaked into the document"


def test_html_and_text_stay_distinct(page):
    """The pipeline branches on content_text vs document_html; a walker that put
    HTML in `text` fed markup to the LLM as prose (MERGE_LOG CHANGE 6)."""
    d = _detail(page, NORMAL_PAGE)
    assert "<" in d["html"]
    assert "<p>" not in d["text"]


# --------------------------------------------------------------------------- #
# document titles — provenance decides, not word lists
# --------------------------------------------------------------------------- #

def test_a_declared_title_beats_an_anchor_scraped_one():
    """SIMAH's PDF is linked from the page it is declared on (include_page), so the
    anchor sighting lands first and used to win: the library got "Download PDF"."""
    from dynamic_crawler.formfill.runner import _add_document
    docs = {}
    url = "https://x.gov/files/ir.pdf"
    _add_document(docs, url, "Download PDF", "https://x.gov/law", "Law", declared=False)
    _add_document(docs, url, "The Implementing Regulations", "https://x.gov/law", "Law",
                  declared=True)
    assert docs[url]["title"] == "The Implementing Regulations"
    assert docs[url]["times_linked"] == 2, "still ONE document, seen twice"


def test_a_declared_title_is_never_downgraded():
    from dynamic_crawler.formfill.runner import _add_document
    docs = {}
    url = "https://x.gov/files/ir.pdf"
    _add_document(docs, url, "The Implementing Regulations", "https://x.gov/a", "Law",
                  declared=True)
    _add_document(docs, url, "Download PDF", "https://x.gov/b", "Law", declared=False)
    assert docs[url]["title"] == "The Implementing Regulations"


def test_anchor_titles_are_untouched_when_nothing_declares_the_file():
    """The case every other form is in — sbp/mhrsd/sama find documents by scanning
    detail pages, so there is no declared sighting and nothing may change."""
    from dynamic_crawler.formfill.runner import _add_document
    docs = {}
    url = "https://x.gov/files/circular.pdf"
    _add_document(docs, url, "Circular 15 of 2026", "https://x.gov/a", "Circulars",
                  declared=False)
    _add_document(docs, url, "Annex A", "https://x.gov/b", "Circulars", declared=False)
    assert docs[url]["title"] == "Circular 15 of 2026", "first sighting still wins"
    assert docs[url]["also_in"] == ""


def test_cross_listing_bookkeeping_survives_the_change():
    """`also_in` and the URL key are what stopped 3 PDFs being reported as 39."""
    from dynamic_crawler.formfill.runner import _add_document
    docs = {}
    url = "https://x.gov/files/save-as.pdf"
    _add_document(docs, url, "Save as PDF", "https://x.gov/a", "Section A")
    _add_document(docs, url, "Save as PDF", "https://x.gov/b", "Section B")
    assert len(docs) == 1
    assert docs[url]["section_path"] == "Section A", "shallowest still wins"
    assert docs[url]["also_in"] == "Section B"
