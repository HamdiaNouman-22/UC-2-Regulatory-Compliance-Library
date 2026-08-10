"""THE FORM — the whole point of this package.

The old onboarding path (`dynamic_crawler/onboarding/` + `dynamic_crawler/auto/`)
asked an LLM to write a crawler. The evidence that it didn't work is still in the
repo: `auto/generated/SAMA/anthropic_claude_sonnet_4_5/adapter.v1.py … v7.py` —
seven attempts at one regulator. The problem was never that the model couldn't
write Python. It's that nobody could tell whether the Python it wrote was right.

So here the LLM's entire job is to fill in the small fixed form defined below.

    the LLM produces        ~12 fields of DATA
    our code produces       the crawl (formfill/runner.py)

Three consequences, and they are the reason this is worth building:

  1. REVIEWABLE — a dozen fields, readable in 30 seconds, vs 300 lines of
     generated Python.
  2. SAFE WHEN WRONG — a bad guess is a selector that matches nothing, which the
     verify gate catches. It is not code that executes.
  3. NO SANDBOX — it's data, not code, so `auto/sandbox.py`'s entire problem
     disappears.

FROZEN, NOT REGENERATED
    A hints file is proposed once, reviewed, committed to git, and then read on
    every run. `runner.py` never calls an LLM — it does not even import the
    proposal code. This is deliberate: a model that can change its mind between
    runs would make change detection report hundreds of documents as
    "disappeared" the first time it picked different rows.

This module is pure data + validation. No network, no browser, no LLM.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import yaml

HINTS_VERSION = 1

# How the page is laid out. These mirror generic_crawler's shapes so a hints file
# and a plain generic crawl are describing the same world.
#
#   list / table  — one page holds many document rows, pagination moves you on
#   tree          — a nested menu; each node is its own page, pages have children
#
# A tree is really the same idea with the menu standing in for the listing: the
# NODES are the rows, and the nesting is the section path. That is why it reuses
# the same field extraction, the same de-duplication and the same verify gate
# instead of being a separate crawler.
SHAPES = ("list", "table", "tree")

# How far the crawl may roam. Only meaningful to the generic engine; carried here
# so a hints file is a complete description of the source.
SCOPES = ("prefix", "host", "breadcrumb", "auto")

PAGER_MODES = (
    "none",        # single page
    "url_offset",  # /circulars/P30, P60 …   pattern + step, offset counts ROWS
    "url_page",    # ?page=2, /page/3 …      pattern + step, counts PAGES
    "click",       # a "Next" control we click
)

# Where an extracted field lands. Anything not in this list is rejected rather
# than silently written, so a hallucinated field name fails review instead of
# quietly producing a column nobody reads.
FIELD_TARGETS = (
    "title",           # required
    "document_url",    # required — the row's link
    "published_date",
    # NOT the same thing as published_date. Several sites print only a "Last
    # Updated" date — Tadawul's cards say "Last Updated: 5th January, 2026" —
    # which is when the document was last revised, not when it was issued.
    # Writing that into published_date would corrupt dedupe, because the
    # orchestrator treats published_date as part of a document's identity.
    # This lands in extra_meta instead.
    "last_updated_date",
    "reference_no",
    "department",
    "category",
    "year",
    "urdu_url",
    # A second-language copy of the same document. Tadawul publishes every rule
    # as an Arabic and an English PDF; the English one is the document and the
    # Arabic one travels in extra_meta, mirroring what the CBB crawler already
    # does with extra_meta["arabic_pdf_link"].
    "arabic_url",
    "status",
)
REQUIRED_FIELDS = ("title", "document_url")

# Only two ways to extract, on purpose. More operations means a bigger surface
# for the model to be creative on and a smaller chance a reviewer spots a
# mistake.
#   css   — a selector INSIDE the row, plus which part of it to take
#   regex — a pattern applied to the row's own visible text
FIELD_SOURCES = ("css", "regex")
CSS_ATTRS = ("text", "href", "src", "title", "value")

_CSS_SAFE = re.compile(r"^[A-Za-z0-9 \.\#\[\]\=\'\"\-\_\*\:\(\)\>\+\~\,\^\$\|]+$")

# The example the LLM is shown, and the example a teammate reads to understand
# the format. One source of truth for both — if they drift, the model is being
# taught something the docs deny.
EXAMPLE_HINTS = """\
version: 1
name: sbp.circulars
seed_url: "https://www.sbp.org.pk/circulars/"
shape: list
scope: prefix

# Which repeated element on the listing page is ONE entry.
row_selector: "h4.mb-2"

# The link inside that row that opens the entry. Relative to the row.
detail_link_selector: "a[href]"

pagination:
  mode: url_offset
  pattern: "https://www.sbp.org.pk/circulars/P{offset}"
  step: 30
  max_offset: 4140        # optional; discovered from page 1 when omitted
  max_pages: 200          # hard stop, always applied

fields:
  title:
    from: css
    selector: "a"
    attr: text
  document_url:
    from: css
    selector: "a[href]"
    attr: href
  reference_no:
    from: regex
    pattern: "([A-Z]{2,6} Circular(?: Letter)? No\\\\.?\\\\s*\\\\d+ of \\\\d{4})"
  published_date:
    from: regex
    pattern: "([A-Z][a-z]+ \\\\d{1,2},? \\\\d{4})"
  department:
    from: regex
    pattern: "\\\\|\\\\s*([A-Z]{2,8})\\\\s*\\\\|"

# Optional. Rebuilds the site's own hierarchy for each row, outermost level
# first: "Laws > Sectoral Legislations > Real estate sector". Each level walks UP
# from the row to the nearest matching ancestor, then reads a title inside it.
section_path:
  prefix: ["Laws"]
  levels:
    - {ancestor: "div.regulationContent", title: "h4"}
    - {ancestor: "div.showLawItems",      title: "h4"}

fetch_details: true       # phase 2: open each entry for its HTML
"""


class HintsError(ValueError):
    pass


# --------------------------------------------------------------------------- #
# VALIDATION — everything checkable without touching the network
#
# Live checks ("does row_selector actually match anything?") belong to
# verify.py, because they need the real page. Keeping the two apart means a
# malformed proposal is rejected in milliseconds, before we open a browser.
# --------------------------------------------------------------------------- #

# Every key the engine reads, plus the ones approve/propose/verify stamp on.
# A form is DATA, and the whole safety argument is that a reviewer can see what it
# says. A key nobody reads breaks that quietly: `row_count` instead of
# `row_count_check` validated clean, did nothing, and cost a debugging session
# before anyone noticed the coverage check had never run.
KNOWN_KEYS = {
    # identity and shape
    "version", "name", "seed_url", "shape", "scope", "requires_headed",
    # list walking
    "row_selector", "detail_link_selector", "pagination", "page_size",
    "row_count_check", "expand_selector", "tabs",
    # tree walking
    "tree",
    # extraction and output
    "fields", "section_path", "fetch_details", "content", "library",
    "include_page", "attachment_is_document",
    # provenance, written by the tooling rather than by hand
    "approved", "approved_at", "approved_by", "verified_at",
    "proposed_at", "proposed_by", "notes", "meta",
}


def validate_hints(h: dict) -> list[str]:
    """Return a list of human-readable problems. Empty list means structurally
    valid — NOT that it works. Only verify.py can say that."""
    errs: list[str] = []

    if not isinstance(h, dict):
        return ["hints must be a mapping"]

    for k in sorted(set(h) - KNOWN_KEYS):
        near = sorted(kk for kk in KNOWN_KEYS
                      if kk.startswith(k[:5]) or k.startswith(kk[:5]))
        hint = f" — did you mean {near[0]!r}?" if near else ""
        errs.append(f"unknown key {k!r}: nothing reads it, so it would do "
                    f"nothing silently{hint}")

    if h.get("version") != HINTS_VERSION:
        errs.append(f"version must be {HINTS_VERSION}, got {h.get('version')!r}")

    if not str(h.get("seed_url", "")).startswith(("http://", "https://")):
        errs.append("seed_url must be an absolute http(s) URL")

    if not h.get("name"):
        errs.append("name is required (e.g. 'sbp.circulars') — it names the output folder")

    shape = h.get("shape")
    if shape not in SHAPES:
        errs.append(f"shape must be one of {SHAPES}, got {shape!r}")

    scope = h.get("scope", "auto")
    if scope not in SCOPES:
        errs.append(f"scope must be one of {SCOPES}, got {scope!r}")

    if "requires_headed" in h and not isinstance(h["requires_headed"], bool):
        errs.append("requires_headed must be true or false")

    if shape == "tree":
        # A tree names its menu instead of a row selector, and its title/link
        # come from the node itself — so neither row_selector nor fields is
        # required. Pagination is meaningless: the menu is the whole index.
        errs += _check_tree(h.get("tree") or {})
        if h.get("row_selector"):
            errs.append("row_selector does not apply to shape: tree — use tree.node_selector")
        if (h.get("pagination") or {}).get("mode", "none") != "none":
            errs.append("pagination does not apply to shape: tree — the menu is the index")
    else:
        errs += _check_selector(h.get("row_selector"), "row_selector", required=True)
        errs += _check_pagination(h.get("pagination") or {"mode": "none"})

    errs += _check_selector(h.get("detail_link_selector"), "detail_link_selector",
                            required=False)
    errs += _check_fields(h.get("fields") or {}, required=(shape != "tree"))
    errs += _check_section_path(h.get("section_path") or {})

    if not isinstance(h.get("fetch_details", True), bool):
        errs.append("fetch_details must be true or false")

    # A tab strip that FILTERS the list rather than extending it. Distinct from
    # expand_selector, which clicks everything at once: clicking every tab here
    # would leave only the last one showing. Each tab is clicked in turn, the
    # rows visible after it are harvested, and the tab's own label becomes the
    # section — which is the only way to learn a category the markup never
    # states. MOE's 119 cards sit flat in one container with no category
    # attribute; the tab is the sole source of that fact.
    errs += _check_tabs(h.get("tabs") or {})

    # A "Show N entries" dropdown. Every DataTables grid has one, and picking
    # its largest option collapses pagination entirely: SAMA's 685 circulars are
    # 69 pages of 10, or ONE page when the select is set to "All". It is a
    # <select>, so it needs a value change and a change event — clicking it, the
    # way expand_selector would, does nothing.
    errs += _check_page_size(h.get("page_size") or {})

    # Many listings STATE their own total — DataTables prints "Showing 1 to 685
    # of 685 entries". Reading it turns coverage from a guess into a check.
    #
    # This exists because a SAMA circulars run harvested 684 rows when the site
    # had 685, silently losing the newest circular. The count was stable across
    # three verify runs, so the gate saw nothing wrong: consistency again failing
    # to imply correctness. The site's own number is the only thing that catches
    # it.
    errs += _check_row_count_check(h.get("row_count_check") or {})

    # Selectors removed from the captured page content. Regulator pages wrap the
    # actual document in their own furniture — SAMA puts a print toolbar
    # ("Entire section | Custom print | Text Only | Rich Text | Print / Save as
    # PDF"), a version switcher and a PDF button above every circular. Stored as
    # part of the document, that text is fed to the LLM as if it were regulation.
    # Links are collected BEFORE stripping, so removing the PDF button here does
    # not lose org_pdf_link.
    errs += _check_content(h.get("content") or {})

    # Who this source belongs to, for the folder trail. Optional, but when set it
    # is the ONE place the naming lives: the runner puts these at the head of
    # every row's section_path (so the crawler's own Excel shows the full path),
    # and the pipeline reuses them instead of prepending its own copy. Naming
    # them here and in the source YAML is what produced duplicate folders.
    errs += _check_library(h.get("library") or {})

    # Is a file hanging off the page THE document, or supporting material?
    #
    #   true  — SAMA. The page is a stub or summary and the PDF is the
    #           regulation, so each attachment becomes its own document with the
    #           page as its source_page_url.
    #   false — SBP. The page IS the regulation (4-5k characters of circular
    #           text) and the PDFs are annexures. One 2022 circular carries 40 of
    #           them; promoting those to documents would replace one circular
    #           with forty annexure fragments. They are kept in
    #           extra_meta["annexures"] instead.
    #
    # Defaults to false because that is the option that never invents documents.
    if not isinstance(h.get("attachment_is_document", False), bool):
        errs.append("attachment_is_document must be true or false")

    # Clicked on every page before anything is read. For accordions, "show more"
    # buttons and tab strips, where the content exists in the DOM but collapsed —
    # capture it closed and you save a page of headings with no substance.
    errs += _check_selector(h.get("expand_selector"), "expand_selector", required=False)

    # The seed page IS a document, not just an index of them. True for any
    # regulation published as one page: SIMAH's Credit Information Law is 17
    # articles of law text on a single URL with nothing to click through to.
    if not isinstance(h.get("include_page", False), bool):
        errs.append("include_page must be true or false")
    if h.get("include_page") and not h.get("fetch_details", True):
        errs.append("include_page needs fetch_details: true — the page's own text and "
                    "HTML are captured by phase 2")

    return errs


def _check_selector(sel, label: str, required: bool) -> list[str]:
    if sel in (None, ""):
        return [f"{label} is required"] if required else []
    if not isinstance(sel, str):
        return [f"{label} must be a string"]
    if len(sel) > 200:
        return [f"{label} is suspiciously long ({len(sel)} chars) — likely not a real selector"]
    if not _CSS_SAFE.match(sel):
        return [f"{label} contains characters that aren't valid in a CSS selector: {sel!r}"]
    return []


def _check_pagination(p: dict) -> list[str]:
    errs: list[str] = []
    if not isinstance(p, dict):
        return ["pagination must be a mapping"]

    mode = p.get("mode", "none")
    if mode not in PAGER_MODES:
        return [f"pagination.mode must be one of {PAGER_MODES}, got {mode!r}"]

    if mode in ("url_offset", "url_page"):
        pattern = p.get("pattern") or ""
        token = "{offset}" if mode == "url_offset" else "{page}"
        if not pattern.startswith(("http://", "https://")):
            errs.append("pagination.pattern must be an absolute URL")
        if token not in pattern:
            errs.append(f"pagination.pattern must contain {token} for mode {mode}")
        if pattern.count("{") != 1 or pattern.count("}") != 1:
            errs.append(f"pagination.pattern must contain exactly one placeholder, {token}")
        step = p.get("step", 1)
        if not isinstance(step, int) or step < 1:
            errs.append("pagination.step must be a positive integer")
        mx = p.get("max_offset")
        if mx is not None and (not isinstance(mx, int) or mx < 1):
            errs.append("pagination.max_offset must be a positive integer when given")

    if mode == "click":
        errs += _check_selector(p.get("next_selector"), "pagination.next_selector",
                                required=True)

    mp = p.get("max_pages", 200)
    if not isinstance(mp, int) or not (1 <= mp <= 5000):
        errs.append("pagination.max_pages must be an integer between 1 and 5000")

    return errs


def _check_content(c: dict) -> list[str]:
    """`content.strip`: selectors deleted from the captured HTML and text."""
    if not c:
        return []
    if not isinstance(c, dict):
        return ["content must be a mapping"]
    strip = c.get("strip", [])
    if not isinstance(strip, list):
        return ["content.strip must be a list of CSS selectors"]
    if len(strip) > 20:
        return ["content.strip: at most 20 selectors"]
    errs = []
    for i, sel in enumerate(strip):
        errs += _check_selector(sel, f"content.strip[{i}]", required=True)
    return errs


def _check_library(lib: dict) -> list[str]:
    """`library.regulator` / `library.source_system` — the head of the folder trail."""
    if not lib:
        return []
    if not isinstance(lib, dict):
        return ["library must be a mapping"]
    errs = []
    for key in ("regulator", "source_system"):
        v = lib.get(key)
        if v is not None and (not isinstance(v, str) or not v.strip() or len(v) > 120):
            errs.append(f"library.{key} must be a non-empty string under 120 chars")
    return errs


def _check_row_count_check(rc: dict) -> list[str]:
    """`row_count_check`: where the page states how many rows it has.

    `total` says WHAT that number counts, and the two are not interchangeable:
      page — the rows on THIS listing page (DataTables showing everything at once)
      list — the rows in the WHOLE list across every page (MOH: "of 75 items"
             while the page shows 10)
    Reading a list-wide total as a per-page count reports a 65-row coverage gap on
    a complete crawl, so this is declared rather than guessed."""
    if not rc:
        return []
    if not isinstance(rc, dict):
        return ["row_count_check must be a mapping"]
    errs = _check_selector(rc.get("selector"), "row_count_check.selector", required=True)
    total = rc.get("total", "page")
    if total not in ("page", "list"):
        errs.append(f"row_count_check.total must be 'page' or 'list', got {total!r}")
    pat = rc.get("pattern")
    if not isinstance(pat, str) or not pat:
        errs.append("row_count_check.pattern is required, with one capture group "
                    "for the number, e.g. 'of ([\d,]+) entries'")
    else:
        try:
            c = re.compile(pat)
            if c.groups != 1:
                errs.append("row_count_check.pattern needs exactly one capture group")
        except re.error as e:
            errs.append(f"row_count_check.pattern does not compile: {e}")
    t = rc.get("tolerance", 0)
    if not isinstance(t, int) or not (0 <= t <= 50):
        errs.append("row_count_check.tolerance must be an integer between 0 and 50")
    return errs


def _check_page_size(ps: dict) -> list[str]:
    """The 'Show N entries' select: pick an option, then everything is on one page."""
    if not ps:
        return []
    if not isinstance(ps, dict):
        return ["page_size must be a mapping"]
    errs = _check_selector(ps.get("selector"), "page_size.selector", required=True)
    val = ps.get("value")
    if not isinstance(val, str) or not val.strip():
        errs.append("page_size.value is required — the option's visible text, e.g. 'All' or '100'")
    w = ps.get("wait_ms", 5000)
    if not isinstance(w, int) or not (100 <= w <= 60000):
        errs.append("page_size.wait_ms must be an integer between 100 and 60000")
    return errs


def _check_tabs(t: dict) -> list[str]:
    """A filter strip: click each control, harvest what it shows, move on."""
    if not t:
        return []
    if not isinstance(t, dict):
        return ["tabs must be a mapping"]
    errs = _check_selector(t.get("selector"), "tabs.selector", required=True)

    skip = t.get("skip_labels", [])
    if not isinstance(skip, list) or any(not isinstance(x, str) for x in skip):
        errs.append("tabs.skip_labels must be a list of strings")
    w = t.get("wait_ms", 1200)
    if not isinstance(w, int) or not (100 <= w <= 15000):
        errs.append("tabs.wait_ms must be an integer between 100 and 15000")
    mx = t.get("max_tabs", 50)
    if not isinstance(mx, int) or not (1 <= mx <= 200):
        errs.append("tabs.max_tabs must be an integer between 1 and 200")
    return errs


def _check_tree(t: dict) -> list[str]:
    """A tree needs four things: where the menu is, what one node looks like,
    where the node's link is, and how deep to go.

    `expand_selector` is the one that makes a tree tractable at all — most
    rulebook menus render collapsed, so the deep nodes do not exist in the DOM
    until something is clicked.
    """
    if not isinstance(t, dict) or not t:
        return ["shape: tree requires a `tree:` block (menu_selector, node_selector, "
                "link_selector)"]
    errs: list[str] = []
    errs += _check_selector(t.get("menu_selector"), "tree.menu_selector", required=True)
    errs += _check_selector(t.get("node_selector"), "tree.node_selector", required=True)
    errs += _check_selector(t.get("link_selector"), "tree.link_selector", required=True)
    errs += _check_selector(t.get("expand_selector"), "tree.expand_selector", required=False)

    md = t.get("max_depth", 8)
    if not isinstance(md, int) or not (1 <= md <= 20):
        errs.append("tree.max_depth must be an integer between 1 and 20")
    mn = t.get("max_nodes", 1000)
    if not isinstance(mn, int) or not (1 <= mn <= 20000):
        errs.append("tree.max_nodes must be an integer between 1 and 20000")
    return errs


def _check_section_path(sp: dict) -> list[str]:
    """Where the document sits in the site's own structure.

    One of the two guiding goals for crawling is replicating the regulator's
    hierarchy — "Laws > Sectoral Legislations > Real estate sector" rather than a
    flat "Laws". A listing page usually encodes that in the row's ANCESTORS: the
    row sits inside a sector block, which sits inside a tab pane. So a level says
    "walk up to the nearest <ancestor>, and take the text of <title> inside it".
    """
    errs: list[str] = []
    if not sp:
        return errs
    if not isinstance(sp, dict):
        return ["section_path must be a mapping"]

    # The page's own breadcrumb, when it has one, beats anything we can infer.
    # It is the site telling us where the document sits, in the site's words.
    # Read on the DETAIL page, so it only applies when phase 2 runs.
    if sp.get("from_breadcrumb"):
        errs += _check_selector(sp["from_breadcrumb"], "section_path.from_breadcrumb",
                                required=True)
        for k in ("drop_first", "drop_last"):
            v = sp.get(k, 0)
            if not isinstance(v, int) or not (0 <= v <= 5):
                errs.append(f"section_path.{k} must be an integer between 0 and 5")

    prefix = sp.get("prefix", [])
    if not isinstance(prefix, list) or len(prefix) > 5:
        errs.append("section_path.prefix must be a list of at most 5 strings")
    else:
        errs += [f"section_path.prefix entries must be short strings, got {p!r}"
                 for p in prefix if not isinstance(p, str) or not p or len(p) > 80]

    levels = sp.get("levels", [])
    if not isinstance(levels, list):
        return errs + ["section_path.levels must be a list"]
    if len(levels) > 4:
        errs.append("section_path.levels: at most 4 levels")
    for i, lv in enumerate(levels):
        if not isinstance(lv, dict):
            errs.append(f"section_path.levels[{i}] must be a mapping")
            continue
        # Two shapes, because pages mark groups in two ways:
        #   {ancestor, title}  the rows are wrapped in a block that names itself
        #   {preceding}        a heading sits before unwrapped rows (SharePoint)
        if lv.get("preceding"):
            errs += _check_selector(lv["preceding"], f"section_path.levels[{i}].preceding",
                                    required=True)
            if lv.get("ancestor") or lv.get("title"):
                errs.append(f"section_path.levels[{i}]: use either 'preceding' or "
                            "'ancestor'+'title', not both")
            continue
        errs += _check_selector(lv.get("ancestor"), f"section_path.levels[{i}].ancestor",
                                required=True)
        errs += _check_selector(lv.get("title"), f"section_path.levels[{i}].title",
                                required=True)
    return errs


def _check_fields(fields: dict, required: bool = True) -> list[str]:
    errs: list[str] = []
    if not isinstance(fields, dict):
        return ["fields must be a mapping of target -> extraction rule"]

    if required:
        for missing in (f for f in REQUIRED_FIELDS if f not in fields):
            errs.append(f"fields.{missing} is required")

    for target, rule in fields.items():
        if target not in FIELD_TARGETS:
            errs.append(f"fields.{target}: unknown target (allowed: {', '.join(FIELD_TARGETS)})")
            continue
        if not isinstance(rule, dict):
            errs.append(f"fields.{target} must be a mapping")
            continue

        src = rule.get("from")
        if src not in FIELD_SOURCES:
            errs.append(f"fields.{target}.from must be one of {FIELD_SOURCES}, got {src!r}")
            continue

        if src == "css":
            errs += _check_selector(rule.get("selector"), f"fields.{target}.selector",
                                    required=True)
            attr = rule.get("attr", "text")
            if attr not in CSS_ATTRS:
                errs.append(f"fields.{target}.attr must be one of {CSS_ATTRS}, got {attr!r}")
            # where_text: pick the match whose own text matches this pattern.
            # For rows holding several indistinguishable links (Arabic/English).
            wt = rule.get("where_text")
            if wt is not None:
                if not isinstance(wt, str) or not wt:
                    errs.append(f"fields.{target}.where_text must be a non-empty string")
                elif len(wt) > 120:
                    errs.append(f"fields.{target}.where_text is over 120 chars")
                else:
                    try:
                        re.compile(wt)
                    except re.error as e:
                        errs.append(f"fields.{target}.where_text is not a valid regex: {e}")
            if target.endswith("_url") or target == "document_url":
                if attr not in ("href", "src"):
                    errs.append(f"fields.{target}: a URL field needs attr href or src, not {attr!r}")
        else:
            pattern = rule.get("pattern")
            if not isinstance(pattern, str) or not pattern:
                errs.append(f"fields.{target}.pattern is required for a regex field")
                continue
            if len(pattern) > 300:
                errs.append(f"fields.{target}.pattern is over 300 chars — too long to review")
            try:
                compiled = re.compile(pattern)
            except re.error as e:
                errs.append(f"fields.{target}.pattern does not compile: {e}")
                continue
            if compiled.groups > 1:
                errs.append(f"fields.{target}.pattern has {compiled.groups} capture groups — "
                            "use exactly one (or none, to take the whole match)")

    return errs


# --------------------------------------------------------------------------- #
# LOAD / SAVE / APPROVE
# --------------------------------------------------------------------------- #

def load_hints(path: str | Path, require_valid: bool = True) -> dict:
    h = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    if require_valid:
        errs = validate_hints(h)
        if errs:
            raise HintsError(f"{path} is not a valid hints file:\n  - " + "\n  - ".join(errs))
    return h


def save_hints(h: dict, path: str | Path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(yaml.safe_dump(h, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return p


def body_hash(h: dict) -> str:
    """Fingerprint of the form itself, ignoring `meta`.

    Without this, a form can be approved on Monday, edited on Tuesday, and still
    say `approved: true` on Wednesday — which would make the whole gate
    decorative. The hash is recorded at approval; anything that reads the form
    afterwards can tell whether it is still the thing that was verified.
    """
    body = {k: v for k, v in h.items() if k != "meta"}
    return hashlib.md5(
        yaml.safe_dump(body, sort_keys=True, allow_unicode=True).encode("utf-8")
    ).hexdigest()[:12]


def approval_state(h: dict) -> tuple[bool, str]:
    """(is_usable, message). 'Approved but since edited' is its own state — it is
    not the same as never approved, and saying so tells you what to do next."""
    meta = h.get("meta") or {}
    if not meta.get("approved"):
        return False, "not approved — run `verify` then `approve`"
    recorded = meta.get("form_hash")
    if not recorded:
        return True, "approved before form hashing existed — re-verify when convenient"
    if recorded != body_hash(h):
        return False, ("APPROVED BUT SINCE EDITED — the form no longer matches what was "
                       "verified. Re-run `verify` and `approve`.")
    return True, "approved"


def is_approved(h: dict) -> bool:
    return approval_state(h)[0]


def stamp_meta(path: str | Path, meta: dict) -> Path:
    """Replace only the `meta:` block, leaving the rest of the file byte-for-byte.

    `save_hints` round-trips through PyYAML, which silently deletes every comment.
    On a form whose comments explain why each selector was chosen, approving it
    would strip exactly the material a future reviewer needs — so approval edits
    the text instead of rewriting it.

    Falls back to a full rewrite if `meta:` is not the last top-level key, which
    is the only layout this can safely patch.
    """
    p = Path(path)
    lines = p.read_text(encoding="utf-8").splitlines()

    start = next((i for i, ln in enumerate(lines) if ln.startswith("meta:")), None)
    if start is not None:
        tail = [ln for ln in lines[start + 1:]
                if ln.strip() and not ln.startswith((" ", "\t", "#"))]
        if tail:                                   # something follows the meta block
            body = yaml.safe_load("\n".join(lines)) or {}
            body["meta"] = meta
            return save_hints(body, p)
        lines = lines[:start]

    block = yaml.safe_dump({"meta": meta}, sort_keys=False, allow_unicode=True)
    p.write_text("\n".join(lines).rstrip() + "\n" + block, encoding="utf-8")
    return p


def compile_field_regexes(h: dict) -> dict:
    """Pre-compile once so the runner isn't recompiling per row."""
    out = {}
    for target, rule in (h.get("fields") or {}).items():
        if rule.get("from") == "regex":
            out[target] = re.compile(rule["pattern"])
    return out


def summarise(h: dict) -> str:
    """The 30-second review. If this doesn't read like the site, stop here."""
    p = h.get("pagination") or {}
    lines = [
        f"name          {h.get('name')}",
        f"seed_url      {h.get('seed_url')}",
        f"shape         {h.get('shape')}    scope: {h.get('scope', 'auto')}",
    ]
    if h.get("shape") == "tree":
        t = h.get("tree") or {}
        lines += [
            f"menu          {t.get('menu_selector')!r}",
            f"node          {t.get('node_selector')!r}   link: {t.get('link_selector')!r}",
            f"expand        {t.get('expand_selector') or '(nothing to click)'}",
            f"depth/nodes   max_depth={t.get('max_depth', 8)}  max_nodes={t.get('max_nodes', 1000)}",
            f"fetch_details {h.get('fetch_details', True)}",
        ]
    else:
        lines += [
            f"row_selector  {h.get('row_selector')!r}",
            f"detail_link   {h.get('detail_link_selector') or '(the row itself)'!r}",
            f"pagination    {p.get('mode')}"
            + (f"  {p.get('pattern')}  step={p.get('step')}  max_pages={p.get('max_pages')}"
               if p.get("mode", "none") != "none" else ""),
            f"fetch_details {h.get('fetch_details', True)}",
        ]
    sp = h.get("section_path") or {}
    if sp:
        trail = list(sp.get("prefix") or []) + [
            f"<after {lv['preceding']}>" if lv.get("preceding")
            else f"<{lv.get('ancestor')} {lv.get('title')}>"
            for lv in (sp.get("levels") or [])]
        lines.append("section_path  " + " > ".join(trail))
    lines.append("fields")
    for target, rule in (h.get("fields") or {}).items():
        if rule.get("from") == "css":
            lines.append(f"  {target:<15} css    {rule.get('selector')!r} -> {rule.get('attr', 'text')}")
        else:
            lines.append(f"  {target:<15} regex  {rule.get('pattern')!r}")
    meta = h.get("meta") or {}
    lines.append(f"approved      {meta.get('approved', False)}"
                 + (f" by {meta.get('approved_by')} on {meta.get('approved_at')}"
                    if meta.get("approved") else ""))
    return "\n".join(lines)
