"""`panels` — one URL holding several documents in fragment-addressed tabs.

GOSI's Social Insurance page is six separate legal instruments behind a tab
strip. Nothing navigates (`href="#3"`), and picking the longest main-content
candidate keeps exactly one panel — so the generic crawl captured 1 of 6
instruments and 9 of 45 sections, and reported `status: ok`.

The trap these tests pin down is that innerText makes it WORSE, not better: the
ACTIVE panel's accordions are collapsed, so on the real page it reports 279
characters against 82,064 of textContent, while the hidden panels report the
full text either way.

    venv/Scripts/python.exe -m pytest tests/test_formfill_panels.py -q
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dynamic_crawler.formfill.runner import (  # noqa: E402
    JS_IN_PAGE_DETAIL, JS_PANELS, JS_ROWS, _scraped_doc_title)
from dynamic_crawler.formfill.schema import validate_hints  # noqa: E402

TABS = "ul.tabs li.tab a"

# GOSI's shape, reduced: three tabs, three panels already in the DOM, only the
# first shown. Each panel is an accordion whose bodies are collapsed.
TABBED_PAGE = """
<html><head><style>
  .panel{display:none} .panel.active{display:block}
  .collapsible-body{display:none}
</style></head><body>
<ul class="tabs">
  <li class="tab"><a href="#1" class="active"><span>Social Insurance Law</span></a></li>
  <li class="tab"><a href="#2"><span>Implementing Regulations</span></a></li>
  <li class="tab"><a href="#3"><span>Medical Board Regulation</span></a></li>
</ul>
<div id="1" class="panel active">
  <ul class="collapsible">
    <li><h1 class="collapsible-header">Scope of Application</h1>
        <div class="collapsible-body"><p>ARTICLE (1) This Law shall be called the
        Social Insurance Law and applies to all workers.</p></div></li>
    <li><h1 class="collapsible-header">Penalties</h1>
        <div class="collapsible-body"><p>Fines are set out in the tables.</p>
        <a href="/docs/violation-rules.pdf">Press&nbsp;Here</a></div></li>
  </ul>
</div>
<div id="2" class="panel">
  <ul class="collapsible">
    <li><h1 class="collapsible-header">Regulations for the implementation</h1>
        <div class="collapsible-body"><p>DECISION OF THE MINISTER.</p></div></li>
  </ul>
</div>
<div id="3" class="panel">
  <ul class="collapsible">
    <li><h1 class="collapsible-header">Medical committees</h1>
        <div class="collapsible-body"><p>Chapter One: the formation of committees.</p>
        <a href="/docs/OSH.pdf">Click here</a>
        <a href="/docs/osh-2.pdf">Click here</a></div></li>
  </ul>
</div>
</body></html>
"""

# Two tabs pointing at ONE panel: every row inside it would otherwise be read
# twice, and the second tab's label would overwrite the first's section path.
ALIASED_TABS = """
<html><body>
<ul class="tabs">
  <li class="tab"><a href="#a">Laws</a></li>
  <li class="tab"><a href="#a">Laws (again)</a></li>
  <li class="tab"><a href="/elsewhere">Not a fragment</a></li>
  <li class="tab"><a href="#missing">Points at nothing</a></li>
</ul>
<div id="a"><p>One panel.</p></div>
</body></html>
"""


@pytest.fixture(scope="module")
def page():
    pw = pytest.importorskip("playwright.sync_api")
    with pw.sync_playwright() as p:
        browser = p.chromium.launch()
        pg = browser.new_page()
        yield pg
        browser.close()


def _load(page, html):
    page.set_content(html, wait_until="domcontentloaded")


# --------------------------------------------------------------------------- #
# Panel discovery
# --------------------------------------------------------------------------- #

def test_every_panel_is_found_not_just_the_visible_one(page):
    _load(page, TABBED_PAGE)
    panels = page.evaluate(JS_PANELS, TABS)
    assert [p["label"] for p in panels] == [
        "Social Insurance Law", "Implementing Regulations", "Medical Board Regulation"]
    assert [p["fragment"] for p in panels] == ["#1", "#2", "#3"]


def test_hidden_panels_report_their_full_text(page):
    """The whole point: a panel nobody has clicked still has all of its content."""
    _load(page, TABBED_PAGE)
    panels = page.evaluate(JS_PANELS, TABS)
    assert all(p["text_len"] > 30 for p in panels), [p["text_len"] for p in panels]


def test_innertext_would_lose_the_visible_panel(page):
    """The active panel's accordion bodies are collapsed, so RENDERED text is a
    fraction of the real content. This is why `panels` has no `read:` knob."""
    _load(page, TABBED_PAGE)
    rendered, real = page.evaluate(
        "()=>{const e=document.getElementById('1');"
        "return [(e.innerText||'').trim().length, (e.textContent||'').trim().length]}")
    assert rendered < real / 2
    panels = page.evaluate(JS_PANELS, TABS)
    assert panels[0]["text_len"] > rendered


def test_two_tabs_one_panel_is_counted_once(page):
    _load(page, ALIASED_TABS)
    panels = page.evaluate(JS_PANELS, TABS)
    assert len(panels) == 1 and panels[0]["label"] == "Laws"


def test_non_fragment_and_dangling_tabs_are_skipped(page):
    _load(page, ALIASED_TABS)
    labels = [p["label"] for p in page.evaluate(JS_PANELS, TABS)]
    assert "Not a fragment" not in labels and "Points at nothing" not in labels


def test_a_selector_matching_nothing_returns_no_panels(page):
    _load(page, TABBED_PAGE)
    assert page.evaluate(JS_PANELS, "ul.nope li a") == []


# --------------------------------------------------------------------------- #
# Rows scoped to a panel
# --------------------------------------------------------------------------- #

def _rows(page, root_sel=None, root_label="", stamp_base=None):
    return page.evaluate(JS_ROWS, {
        "rowSel": "ul.collapsible > li", "linkSel": "a[href]", "cssFields": [
            {"target": "title", "selector": "h1.collapsible-header", "attr": "text"}],
        "sectionLevels": [], "rootSel": root_sel, "rootLabel": root_label,
        "stampBase": stamp_base})


def test_rows_are_scoped_to_one_panel(page):
    _load(page, TABBED_PAGE)
    page.evaluate(JS_PANELS, TABS)
    assert _rows(page)["matched"] == 4                       # unscoped: the whole page
    assert _rows(page, '[data-ff-panel="0"]')["matched"] == 2
    assert _rows(page, '[data-ff-panel="2"]')["matched"] == 1


def test_the_panel_label_becomes_the_outermost_section_level(page):
    _load(page, TABBED_PAGE)
    page.evaluate(JS_PANELS, TABS)
    res = _rows(page, '[data-ff-panel="0"]', "Social Insurance Law")
    assert all(r["section"][0] == "Social Insurance Law" for r in res["rows"])


def test_rows_are_stamped_so_phase_two_can_find_them_again(page):
    _load(page, TABBED_PAGE)
    page.evaluate(JS_PANELS, TABS)
    res = _rows(page, '[data-ff-panel="0"]', "Law", stamp_base=100)
    assert [r["ff_row"] for r in res["rows"]] == [100, 101]
    assert page.evaluate("()=>document.querySelectorAll('[data-ff-row]').length") == 2


def test_a_root_that_matches_nothing_yields_no_rows_and_does_not_throw(page):
    _load(page, TABBED_PAGE)
    assert _rows(page, '[data-ff-panel="99"]') == {"rows": [], "matched": 0}


# --------------------------------------------------------------------------- #
# Reading one panel's content back
# --------------------------------------------------------------------------- #

def test_in_page_detail_returns_the_panel_not_the_page(page):
    _load(page, TABBED_PAGE)
    page.evaluate(JS_PANELS, TABS)
    d = page.evaluate(JS_IN_PAGE_DETAIL, '[data-ff-panel="1"]')
    assert "DECISION OF THE MINISTER" in d["text"]
    assert "Social Insurance Law" not in d["text"]      # panel 0 must not leak in


def test_in_page_detail_reads_collapsed_bodies(page):
    _load(page, TABBED_PAGE)
    page.evaluate(JS_PANELS, TABS)
    d = page.evaluate(JS_IN_PAGE_DETAIL, '[data-ff-panel="0"]')
    assert "This Law shall be called" in d["text"]


def test_in_page_detail_reports_the_files_inside_the_panel(page):
    _load(page, TABBED_PAGE)
    page.evaluate(JS_PANELS, TABS)
    d = page.evaluate(JS_IN_PAGE_DETAIL, '[data-ff-panel="2"]')
    assert sorted(l["href"].rsplit("/", 1)[-1] for l in d["links"]) == ["OSH.pdf", "osh-2.pdf"]


def test_a_links_context_is_the_heading_above_it(page):
    _load(page, TABBED_PAGE)
    page.evaluate(JS_PANELS, TABS)
    d = page.evaluate(JS_IN_PAGE_DETAIL, '[data-ff-panel="0"]')
    assert d["links"][0]["ctx"] == "Penalties"


def test_missing_element_returns_none_rather_than_empty_content(page):
    """An empty capture would be stored as a document with no text; None is the
    signal to keep the row and say so."""
    _load(page, TABBED_PAGE)
    assert page.evaluate(JS_IN_PAGE_DETAIL, '[data-ff-panel="9"]') is None


# --------------------------------------------------------------------------- #
# Titles for files scraped out of a panel
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("text,ctx,expect", [
    ("Press Here", "Penalties", "Penalties"),
    ("Press\xa0Here", "Penalties", "Penalties"),          # GOSI uses a nbsp
    ("Click here", "", "Osh"),                            # no heading -> the slug
    ("Tables of rules for imposing fines", "Penalties",
     "Tables of rules for imposing fines"),               # a real title wins
])
def test_a_call_to_action_is_not_a_title(text, ctx, expect):
    link = {"text": text, "ctx": ctx, "href": "https://x/docs/OSH.pdf"}
    assert _scraped_doc_title(link, "fallback") == expect


# --------------------------------------------------------------------------- #
# The form
# --------------------------------------------------------------------------- #

BASE = {"version": 1, "name": "x.y", "seed_url": "https://x/y", "shape": "list",
        "pagination": {"mode": "none", "max_pages": 1}}


def test_include_panel_alone_is_a_complete_form():
    """The panels ARE the entries, so row_selector and fields are optional —
    the same allowance shape: tree already has."""
    assert validate_hints({**BASE, "panels": {"tabs": TABS, "include_panel": True}}) == []


def test_panels_without_include_panel_still_needs_a_row_selector():
    errs = validate_hints({**BASE, "panels": {"tabs": TABS}})
    assert any("row_selector is required" in e for e in errs)


def test_panels_needs_a_tabs_selector():
    errs = validate_hints({**BASE, "panels": {"include_panel": True}})
    assert any("panels.tabs is required" in e for e in errs)


def test_a_misspelled_sub_key_is_rejected_not_ignored():
    """A silently ignored key is a form that reads as if it does something."""
    errs = validate_hints({**BASE, "panels": {"tabs": TABS, "include_panels": True}})
    assert any("panels.include_panels" in e for e in errs)


def test_read_is_not_a_knob():
    errs = validate_hints({**BASE, "panels": {"tabs": TABS, "include_panel": True,
                                              "read": "innerText"}})
    assert any("panels.read" in e for e in errs)


def test_panels_are_rejected_on_a_tree():
    errs = validate_hints({"version": 1, "name": "x.y", "seed_url": "https://x/y",
                           "shape": "tree", "panels": {"tabs": TABS},
                           "tree": {"menu_selector": "#m", "node_selector": "li",
                                    "link_selector": "a"}})
    assert any("panels does not apply to shape: tree" in e for e in errs)


def test_the_shipped_gosi_forms_are_valid():
    # Exactly these two. The section-level reading of the SocialInsurance page
    # lives in UC-2-Scratch, not here — it drops 1 of 45 sections and 3 of 6 PDFs.
    for name in ("gosi.social_insurance", "gosi.saned"):
        import yaml
        h = yaml.safe_load(
            (REPO_ROOT / "dynamic_crawler" / "hints" / f"{name}.yml").read_text(
                encoding="utf-8"))
        assert validate_hints(h) == [], name
