"""
================================================================================
 Standalone Playwright crawler — the present "dynamic" crawler for regulator sites
================================================================================

NEW HERE?  Read CRAWLING_OVERVIEW.md and README.md in this folder first. They
explain what this UC is for and why this generic crawler is our current approach.
This file is a TEST TOOL and is fully SEPARATE from the live pipeline — it only
writes into output/standalone_crawler/.

--------------------------------------------------------------------------------
WHAT IT DOES (in plain words)
--------------------------------------------------------------------------------
You give it ONE start URL — that's all. It opens the page in a real headless
browser and behaves like a diligent person clicking around:

  1. Loads the page and lets JavaScript finish rendering.
  2. Works out its own SCOPE from the landing page (Alteration 1), so nobody has
     to know in advance whether this is a tree site or a list site.
  3. Reveals hidden links (Alteration 2): maximises "Show N entries" menus, opens
     menus/trees, clicks "Next"/"Load more" and keeps only the clicks that
     actually produced new links. Reads content inside <frame>s and scrolls to
     trigger lazy-loaded lists — so nothing stays hidden.
  4. Walks every level of the site (breadth-first: folder within folder ...),
     following links, until it runs out or hits the page/depth caps.
  5. On each page it records:
       - the BREADCRUMB trail   -> the "folder path" (how we mirror site structure)
       - the page CONTENT       -> rendered HTML + plain text
       - every DOCUMENT link    -> PDFs, DOCX, and "Download" buttons, with titles
  6. Keeps the crawl inside the detected section using that SCOPE
     (breadcrumb / prefix / host) plus a same-host rule and hard caps.
  7. Ignores DOCUMENTS linked from the header/footer, they never land in the 
     documents sheet. Each link carries a `chrome` flag from JS_LINKS; see 
     _merge_links(). A chrome link is still followed as a page if it stays under 
     the seed's path, since some real content is only linked from the nav.

--------------------------------------------------------------------------------
SCOPE (worked out automatically — see SECTION C2)
--------------------------------------------------------------------------------
  auto       : DEFAULT. detect_scope() picks one of the three below by reading the
               landing page. --scope <name> still forces a choice by hand.
  breadcrumb : stay inside the section by matching the breadcrumb trail exactly.
               Best for menu/tree sites with breadcrumbs (SAMA rulebook, CMA).
  prefix     : only follow URLs under the start URL's path (e.g. /circulars/*).
               Best for list-of-documents sites (SBP circulars, SECP acts).
  host       : anything on the same domain (broadest; always use a page cap).

--------------------------------------------------------------------------------
OUTPUTS (all under --out)
--------------------------------------------------------------------------------
  pages.json          full records incl. full html + text
  pages.xlsx          two sheets: "pages" (structure) and "documents" (the files)
  html/<slug>.html    the full readable HTML of each page, one file each
  (schema of these columns is documented in README.md)

Progress is printed to stdout as one JSON object per line so the Streamlit UI
(app.py) — or any wrapper — can stream it live.

--------------------------------------------------------------------------------
HOW TO READ THIS FILE
--------------------------------------------------------------------------------
It is laid out in the order a crawl actually happens:
  A. Small helpers        - URL cleaning, is-this-a-document?, title picking
  B. Browser-side JS       - snippets that run *inside* the page to read it
  C. Page actions          - extract_all(), reveal_all_links() <- the one crawl() uses.
                             collect_paginated_links() / expand_tree() are LEGACY:
                             superseded by reveal_all_links(), kept only for
                             probe_scope() and debug_page.py's --expand/--paginate.
  C2. Auto-scope           - detect_scope(), probe_scope() + the five tuning knobs
  D. crawl()               - the main loop that ties it all together
  E. Excel writer + CLI

Run directly:
  venv/Scripts/python.exe generic_crawler/crawler.py \
      --url https://rulebook.sama.gov.sa/en/regulatory-sandbox \
      --out output/standalone_crawler/sama_sandbox --scope auto --max-pages 150
"""

import argparse
import json
import re
import sys
from collections import deque
from pathlib import Path
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

from playwright.sync_api import sync_playwright

# Windows consoles default to cp1252; force UTF-8 so Arabic/curly chars don't crash logging.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def slugify(text: str) -> str:
    """Filesystem-safe slug (no external dep)."""
    s = re.sub(r"[^a-zA-Z0-9]+", "-", (text or "").lower()).strip("-")
    return s[:80] or "page"

# ============================================================================
# SECTION A — small helpers (URL cleaning, "is this a document?", title picking)
# ============================================================================

# ---- extensions we treat as DOCUMENTS (record, never navigate into) ----
DOC_EXTS = {".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".zip", ".ppt", ".pptx"}
# ---- extensions we simply ignore ----
SKIP_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico", ".css", ".js",
             ".woff", ".woff2", ".ttf", ".mp4", ".mp3", ".webp"}
TRACKING_PREFIXES = ("utm_",)
TRACKING_KEYS = {"fbclid", "gclid", "gclsrc", "dclid", "msclkid", "_ga", "refresh"}

# Aggregator / print pages: "Entire Section", "Custom Print", "Print/Save as PDF".
# These duplicate content already captured page-by-page AND link to everything,
# which is what makes the crawl go in circles. Skip them by URL and by title.
import hashlib
AGGREGATOR_URL_PAT = re.compile(r"/(entiresection|customprint|custom-print|printpdf|print)(/|$|\?|-)", re.I)
AGGREGATOR_TITLE_PAT = re.compile(r"^\s*(entire section|custom print|print\s*/\s*save)", re.I)


def is_aggregator(url: str, title: str = "") -> bool:
    return bool(AGGREGATOR_URL_PAT.search(url)) or bool(AGGREGATOR_TITLE_PAT.search(title or ""))


# Common non-content chrome pages — never worth crawling.
DENY_PATH_PAT = re.compile(
    r"/(search|login|sign-?in|register|contact|sitemap|rss|feed|revision-updates|"
    r"terms-and-conditions|privacy|cookie)s?(/|$|\?)", re.I)


def first_seg(path: str) -> str:
    segs = [s for s in path.split("/") if s]
    return segs[0] if segs else ""


def content_key(text: str) -> str:
    """Hash of whitespace-normalized text, to detect same-content duplicate URLs."""
    norm = re.sub(r"\s+", " ", (text or "")).strip().lower()
    return hashlib.md5(norm.encode("utf-8")).hexdigest() if norm else ""


def emit(event: dict):
    """Print one JSON line to stdout (flushed) for live streaming."""
    sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def normalize_url(url: str) -> str:
    """Drop fragments + tracking params, lowercase host, strip trailing slash."""
    try:
        p = urlparse(url)
    except Exception:
        return url
    if p.scheme not in ("http", "https"):
        return url
    query = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True)
             if not (k.lower() in TRACKING_KEYS or k.lower().startswith(TRACKING_PREFIXES))]
    path = re.sub(r"/{2,}", "/", p.path)      # collapse //circulars -> /circulars (SBP bug)
    path = path.rstrip("/") or "/"
    return urlunparse((p.scheme, p.netloc.lower(), path, "", urlencode(query), ""))


def ext_of(url: str) -> str:
    path = urlparse(url).path.lower()
    dot = path.rfind(".")
    return path[dot:] if dot != -1 else ""


def is_document_link(url: str) -> bool:
    """True if a link points to a downloadable document — not just plain .pdf/.docx,
    but also download-manager links (WordPress Download Manager `wpdmdl=`, /document/,
    /download/ endpoints) that serve a file without a file extension in the URL.
    Very common on gov sites (e.g. SECP acts: /document/<slug>/?wpdmdl=<id>)."""
    if ext_of(url) in DOC_EXTS:
        return True
    p = urlparse(url)
    q = p.query.lower()
    if "wpdmdl=" in q or "download" in q:
        return True
    segs = [s for s in p.path.lower().split("/") if s]
    if any(s in ("document", "documents", "download", "downloads") for s in segs):
        return True
    return False


def doc_type_of(url: str) -> str:
    e = ext_of(url).lstrip(".").upper()
    return e if e else "DOC"


GENERIC_LINK_TEXT = {"", "download", "pdf", "download pdf", "view", "view details",
                     "click here", "read more", "open", "details", "more"}


def title_from_slug(url: str) -> str:
    segs = [s for s in urlparse(url).path.split("/") if s]
    slug = segs[-1] if segs else ""
    return re.sub(r"[-_]+", " ", slug).strip().title()[:180]


def best_doc_title(link: dict, url: str) -> str:
    """Pick a human name for the FILE itself. The anchor text is often a generic
    'Download' button, so fall back to the anchor's title attribute, then the
    row/card context (title + date) and finally to the URL slug."""
    t = (link.get("text") or "").strip()
    if t.lower() not in GENERIC_LINK_TEXT and len(t) > 3:
        return t[:200]
    ta = (link.get("title_attr") or "").strip()
    if len(ta) > 3:
        return ta[:200]
    ctx = (link.get("ctx") or "").strip()
    ctx = re.sub(r"\b(download|pdf|view|click here|read more)\b", "", ctx, flags=re.I).strip(" -|")
    if len(ctx) > 3:
        return ctx[:200]
    return title_from_slug(url) or t


def _norm_heading(s: str) -> str:
    """Case/punctuation-insensitive form, so '&' and 'and' compare equal."""
    s = (s or "").lower().replace("&", " and ")
    s = "".join(c if (c.isalnum() or c.isspace()) else " " for c in s)
    return " ".join(s.split())


def collapse_heading_path(path) -> list:
    """Trim a raw heading path down to the headings that actually name sections.

    Two conventions, no thresholds:
      1. a heading ending in ':' introduces what follows, it doesn't name it
         ("Learn More about the Policies and Regulations:")
      2. a heading that restates its parent adds nothing
    """
    kept = []
    for raw in path or []:
        raw = (raw or "").strip()
        if not raw or raw.endswith((":", "：")):
            continue
        n = _norm_heading(raw)
        if not n:
            continue
        if kept:
            prev = _norm_heading(kept[-1])
            if prev and (prev in n or n in prev):
                continue
        kept.append(raw)
    return kept




# ============================================================================
# SECTION B — browser-side JavaScript snippets
# These strings are handed to the browser and run INSIDE the page (via
# page.evaluate). That is how we read what a real user sees after JS renders.
# ============================================================================
# ---------------- browser-side extraction (runs as JS in the page) --------------

# Pull the breadcrumb trail as a list of visible link/label texts.
JS_BREADCRUMB = r"""
() => {
  const sels = ['.breadcrumb','[class*="readcrumb"]','[class*="rumb"]',
                'nav[aria-label*="readcrumb" i]','ol[class*="crumb"]'];
  for (const s of sels) {
    const el = document.querySelector(s);
    if (el) {
      const parts = Array.from(el.querySelectorAll('a, span, li'))
        .map(n => n.textContent.trim())
        .filter(t => t && t !== '>' && t !== '›' && t.length < 200);
      // de-dupe consecutive repeats
      const out = [];
      for (const p of parts) if (out[out.length-1] !== p) out.push(p);
      if (out.length) return out;
    }
  }
  return [];
}
"""


# Main content: clone body, strip chrome (nav/aside/header/footer/scripts),
# prefer a <main>/<article>/[role=main] if present, else the stripped body.
JS_MAIN_CONTENT = r"""
() => {
  const pick = document.querySelector('main, [role="main"], article, #content, .content, #main');
  const src = pick || document.body || document.documentElement;
  if (!src) return { html: '', text: '' };   // frameset top docs have no <body>
  const clone = src.cloneNode(true);
  clone.querySelectorAll('script,style,noscript,nav,aside,header,footer,form').forEach(n => n.remove());
  return { html: clone.innerHTML, text: (clone.innerText || '').trim() };
}
"""

# All anchors: absolute href + visible text + whether it's a pagination/nav link.
# `nav=true` links (page numbers, next/prev, pager containers) are followed at LOW
# priority so real content/detail pages get crawled before index pages.
#
# Also returns heading_path: the headings this link sits under. We walk the page
# ONCE in document order keeping a stack of headings by rank, so h1..h6 pop each
# other the way the page itself nests them.
JS_LINKS = r"""
() => {
  const root = document.body || document.documentElement;
  if (!root) return [];                       // frameset top docs have no <body>

  // Rank: h1..h6 -> 1..6, ARIA headings -> aria-level, and the widgets that act
  // as headings (accordion <summary>, table <caption>, fieldset <legend>) sort
  // below any real heading.
  const rankOf = (el) => {
    const tag = el.tagName;
    if (/^H[1-6]$/.test(tag)) return parseInt(tag[1], 10);
    if (el.getAttribute('role') === 'heading') {
      const n = parseInt(el.getAttribute('aria-level') || '0', 10);
      if (n >= 1 && n <= 6) return n;
    }
    if (tag === 'SUMMARY' || tag === 'CAPTION' || tag === 'LEGEND') return 7;
    return 0;
  };
  // Both take any element and ask which region it sits in.
  const isChrome = (elem) =>
    !!elem.closest('header, footer, [role="banner"], [role="contentinfo"]');
  // Headings also rule out nav/aside, where a heading labels the widget rather
  // than the content. Links keep <nav>, which can hold real content links.
  const isWidget = (elem) => isChrome(elem) ||
    !!elem.closest('nav, aside, [role="navigation"], [role="complementary"]');

  const out = [];
  const stack = [];                           // [{rank, text}] = current section
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT);
  let node = walker.currentNode;
  while (node) {
    const r = rankOf(node);
    if (r && !isWidget(node)) {
      // innerText is empty for a collapsed accordion, so fall back to textContent
      const ht = (node.innerText || node.textContent || '').replace(/\s+/g, ' ').trim();
      if (ht && ht.length < 300) {
        while (stack.length && stack[stack.length - 1].rank >= r) stack.pop();
        stack.push({ rank: r, text: ht });
      }
    }
    if (node.tagName === 'A' && node.hasAttribute('href')) {
      const a = node;
      const t = (a.textContent || '').trim();
      let nav = false, p = a;
      for (let i = 0; i < 4 && p; i++) {
        const c = ((p.className && p.className.toString ? p.className.toString() : '') + ' ' +
                   (p.id || '')).toLowerCase();
        if (/paginat|pager|page-numbers|page-nav/.test(c)) { nav = true; break; }
        p = p.parentElement;
      }
      const rel = (a.getAttribute('rel') || '').toLowerCase();
      if (rel === 'next' || rel === 'prev') nav = true;
      if (/^(\d+|«|»|<|>|‹|›|\.\.\.|next|previous|prev|first|last)$/i.test(t)) nav = true;
      // Chrome = inside a header/footer landmark, so site-wide furniture rather than
      // this page's content. <nav> is excluded on purpose: it is sometimes a real
      // in-page category sidebar. header/footer are reliable signals; nav isn't.
      const chrome = isChrome(a);
      // Context = text of the nearest row/card/item — this holds the real title + date
      // when the link itself is just a generic "Download" button (e.g. SECP tables).
      let ctx = '', row = a;
      for (let i = 0; i < 5 && row; i++) {
        const tag = row.tagName || '';
        const cn = (row.className && row.className.toString ? row.className.toString() : '');
        if (/^(TR|LI|ARTICLE)$/.test(tag) || /card|item|box|publication|row|result/i.test(cn)) {
          ctx = (row.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 250);
          break;
        }
        row = row.parentElement;
      }
      out.push({ href: a.href, text: t.slice(0, 300), nav: nav, ctx: ctx, chrome: chrome,
                 title_attr: (a.getAttribute('title') || '').trim().slice(0, 300),
                 // a chrome link's outline would be whatever content heading came
                 // before it in document order, which means nothing. Don't claim one.
                 heading_path: chrome ? [] : stack.map(s => s.text) });
    }
    node = walker.nextNode();
  }
  return out;
}
"""


# A "status" chip if the page shows one (e.g. In-Force).
JS_STATUS = r"""
() => {
  const el = Array.from(document.querySelectorAll('*'))
    .find(n => /status\s*:/i.test(n.textContent || '') && n.children.length < 4);
  if (!el) return '';
  const m = (el.textContent || '').match(/status\s*:\s*([A-Za-z\- ]{2,40})/i);
  return m ? m[1].trim() : '';
}
"""


# We don't try to know which element is the "Next" button. We collect anything
# plausible, stamp each one with a temporary id so Python can click it, and let
# the click-and-verify loop in Section C decide what actually worked.
JS_CLICKABLES = r"""
() => {
  // Clear stamps from any previous scan so ids are always fresh and unambiguous.
  document.querySelectorAll('[data-crawl-id]').forEach(
    n => n.removeAttribute('data-crawl-id'));

  // NOTE: these MUST stay on one line each. JavaScript has no /x flag, so a
  // regex literal broken across lines is a SyntaxError — and page.evaluate()
  // would then throw on every call, silently disabling this whole tier.

  // Things that must NEVER be clicked: they log you out, delete data, or leave.
  const NEVER = /log\s*out|sign\s*out|logout|signout|delete|remove|unsubscribe|submit|register|sign\s*up|login|sign\s*in|print|download|share/i;
  // Text that means "show me more of this list".
  const ADVANCE = /^\s*(next|more|load\s*more|show\s*(all|more)|view\s*(all|more)|see\s*more|older|newer|\d{1,4}|»|›|>>|>)\s*$/i;
  const ADVANCE_CLASS = /paginat|pager|page-numbers|page-nav|load-?more|show-?more/i;
  // Classes that mean "this opens a collapsed section".
  const EXPAND_CLASS  = /toggle|expand|collaps|accordion|tree|caret|chevron|has-children|dropdown/i;

  const out = [];
  let id = 0;
  const nodes = document.querySelectorAll(
    'a, button, [role="button"], [aria-expanded], summary, ' +
    '[class*="toggle"], [class*="expand"]');

  for (const el of nodes) {
    // --- must be actually visible on screen ---
    const r = el.getBoundingClientRect();
    if (r.width < 2 || r.height < 2) continue;
    const cs = getComputedStyle(el);
    if (cs.visibility === 'hidden' || cs.display === 'none' || cs.opacity === '0') continue;

    const text  = (el.innerText || el.textContent || '')
                    .replace(/\s+/g, ' ').trim().slice(0, 80);
    const aria  = (el.getAttribute('aria-label') || '').trim().slice(0, 80);
    const cls   = ((el.className && el.className.toString)
                    ? el.className.toString() : '') + ' ' + (el.id || '');
    const label = (text + ' ' + aria).trim();

    if (NEVER.test(label) || NEVER.test(cls)) continue;

    // --- only click things with NO real destination ---
    // A real href is already handled by the crawl queue. Clicking it just
    // navigates away and wastes budget. We want buttons, "#" links and
    // javascript: links — the controls that change the page in place.
    const href = el.getAttribute('href');
    if (el.tagName === 'A' && href && !/^\s*(#|javascript:)/i.test(href)) continue;

    // --- already disabled? nothing to gain ---
    if (/disabled/i.test(cls) || el.getAttribute('aria-disabled') === 'true'
        || el.disabled) continue;

    // --- classify: what kind of control does this look like? ---
    let kind = '';
    if (el.getAttribute('aria-expanded') === 'false' || EXPAND_CLASS.test(cls))
      kind = 'expand';
    if (ADVANCE.test(label) || ADVANCE_CLASS.test(cls))
      kind = 'advance';
    if (!kind) continue;

    el.setAttribute('data-crawl-id', String(id));
    out.push({ cid: id, kind: kind,
               label: label.slice(0, 60), cls: cls.slice(0, 80) });
    id++;
  }
  return out;
}
"""


# ============================================================================
# SECTION C — page actions (run against a loaded page, may click / read frames)
# ============================================================================


def _merge_links(store: dict, links) -> dict:
    """Merge links into an href-keyed store. First sighting wins, except that a
    link seen outside the header/footer even once is never chrome.

    A URL often sits in the content AND in the footer. Every merge here keys on
    href alone, so otherwise whichever copy came first would decide whether a real
    document survives.
    """
    for l in links or []:
        h = l.get("href")
        if not h:
            continue
        if h not in store:
            store[h] = l
            continue
        if not l.get("chrome"):
            store[h]["chrome"] = False
        # A click-revealed state can lose the outline; keep the one we did see.
        if not store[h].get("heading_path") and l.get("heading_path"):
            store[h]["heading_path"] = l["heading_path"]
    return store


def extract_all(page):
    """Frame-aware extraction — the key to handling ALL site types.

    Old sites (e.g. SBP) use <frameset>: the top document has no <body>, and the
    real links/content live inside child <frame>s. Modern sites (e.g. SAMA) are a
    single document. This walks every frame, merges the links, keeps the richest
    content frame, and takes a breadcrumb/status from whichever frame has one — so
    the same code works for both without site-specific config.
    Returns (breadcrumb, content{html,text}, status, links)."""
    breadcrumb, status = [], ""
    best = {"html": "", "text": ""}
    link_map = {}
    for fr in page.frames:                 # page.frames includes the main frame
        try:
            _merge_links(link_map, fr.evaluate(JS_LINKS))
        except Exception:
            pass
        try:
            c = fr.evaluate(JS_MAIN_CONTENT)
            if c and len(c.get("text", "")) > len(best["text"]):
                best = c
        except Exception:
            pass
        if not breadcrumb:
            try:
                b = fr.evaluate(JS_BREADCRUMB)
                if b:
                    breadcrumb = b
            except Exception:
                pass
        if not status:
            try:
                s = fr.evaluate(JS_STATUS)
                if s:
                    status = s
            except Exception:
                pass
    return breadcrumb, best, status, list(link_map.values())


def collect_paginated_links(page, max_clicks=60):
    """Handle IN-PAGE (JavaScript) pagination — e.g. DataTables 'Show N entries' +
    page 1/2 buttons (SECP), where clicking a page re-draws the table with no URL
    change. Strategy: (1) set the page-size select to its largest option so more rows
    show at once, then (2) click Next repeatedly, harvesting links after each draw.
    Returns the union of all anchors seen across the in-page pages."""
    seen = {}

    def harvest():
        for fr in page.frames:
            try:
                for l in fr.evaluate(JS_LINKS):
                    h = l.get("href")
                    if h and h not in seen:
                        seen[h] = l
            except Exception:
                pass

    # (1) maximise a DataTables-style length menu ("Show N entries").
    try:
        for sel in page.query_selector_all("select"):
            vals = sel.evaluate("s => Array.from(s.options).map(o => o.value)")
            nums = [v for v in vals if str(v).lstrip("-").isdigit()]
            if not nums:
                continue
            # prefer "All" (-1) if present, else the biggest number
            best = "-1" if "-1" in nums else max(nums, key=lambda x: int(x))
            if best == "-1" or int(best) >= 50:
                sel.select_option(best)
                page.wait_for_timeout(1200)
                break
    except Exception:
        pass

    harvest()

    # (2) click "Next" until it's gone/disabled or nothing new appears.
    NEXT_SELECTORS = [".paginate_button.next", "a.paginate_button.next", "li.next a",
                      "a.next", "[rel='next']", ".pagination .next a", "button.next"]
    for _ in range(max_clicks):
        before = len(seen)
        el = None
        for s in NEXT_SELECTORS:
            cand = page.query_selector(s)
            if cand:
                cls = (cand.get_attribute("class") or "").lower()
                aria = (cand.get_attribute("aria-disabled") or "").lower()
                if "disabled" in cls or aria == "true":
                    continue
                el = cand
                break
        if el is None:
            break
        try:
            el.scroll_into_view_if_needed(timeout=800)
            el.click(timeout=1000)
            page.wait_for_timeout(800)
        except Exception:
            break
        harvest()
        if len(seen) == before:      # no new links -> we're done
            break
    return list(seen.values())


def expand_tree(page, max_rounds=40):
    """Best-effort: repeatedly click collapsed expanders so child links appear.
    Generic (aria-expanded + common toggle classes). No-ops harmlessly if the
    site doesn't use them — BFS still reaches everything by navigating in."""
    total = 0
    for _ in range(max_rounds):
        toggles = page.query_selector_all(
            "[aria-expanded='false'], .collapsed > .toggle, li.has-children > .expander, "
            ".tree-toggle, .accordion-toggle"
        )
        clicked = 0
        for t in toggles:
            try:
                if t.is_visible():
                    t.click(timeout=800)
                    clicked += 1
            except Exception:
                pass
        total += clicked
        if clicked == 0:
            break
        page.wait_for_timeout(250)
    return total


# ---- ALTERATION 2: one page-action path, with a click-and-verify fallback ----
# Three tiers, cheapest first:
#   0. maximise any "Show N entries" menu — ported from collect_paginated_links()
#   1. the known selectors below — the OLD hardcoded guesses, kept as a fast path
#   2. candidates discovered by JS_CLICKABLES — the site-agnostic fallback

# So tier 0 does the useful work today, and tier 2 is a VERIFIED-SAFE but
# UNEXERCISED fallback.

REVEAL_CLICK_BUDGET = 25   # hard cap on clicks per page — stops runaway loops
REVEAL_SETTLE_MS    = 600  # how long to let the page redraw after a click
REVEAL_MAX_BARREN   = 1    # stop after the FIRST click that gains nothing.

# The old hardcoded guesses, lifted unchanged from collect_paginated_links().
KNOWN_ADVANCE_SELECTORS = [
    ".paginate_button.next", "a.paginate_button.next", "li.next a", "a.next",
    "[rel='next']", ".pagination .next a", "button.next",
]


def _harvest(page):
    """Every link the crawler can currently see, keyed by href.
    Uses extract_all() so we see exactly what the crawl will see — frames included."""
    try:
        _bc, _c, _s, links = extract_all(page)
    except Exception:
        return {}
    return _merge_links({}, links)


def _unstamp(page):
    """Remove our data-crawl-id markers so they never reach the saved HTML."""
    try:
        page.evaluate("() => document.querySelectorAll('[data-crawl-id]')"
                      ".forEach(n => n.removeAttribute('data-crawl-id'))")
    except Exception:
        pass


def _click_and_keep(page, el, seen):
    """Click ONE element; keep the result only if the page gained links.

    Always leaves the browser on the page we started from. Returns True if new
    links appeared. This is the whole idea of change 3.3: we don't need to know
    which element was the right one — we click and check.
    """
    before_url = page.url
    before_n = len(seen)
    try:
        el.scroll_into_view_if_needed(timeout=800)
        el.click(timeout=1200)
        page.wait_for_timeout(REVEAL_SETTLE_MS)
    except Exception:
        return False                       # not clickable / detached — no harm done

    # A click that NAVIGATED is not a reveal. Undo it, or the rest of this page's
    # work happens on the wrong document. (This is the guard expand_tree lacks.)
    if page.url != before_url:
        try:
            page.go_back(wait_until="domcontentloaded", timeout=15000)
            page.wait_for_timeout(300)
        except Exception:
            pass
        return False

    _merge_links(seen, _harvest(page).values())
    return len(seen) > before_n


def reveal_all_links(page, budget=REVEAL_CLICK_BUDGET):
    """Click things that might reveal more links; keep the union of all states.
    It supersedes collect_paginated_links() and expand_tree() (both still defined, 
    for probe_scope() and debug_page.py only).

    Tier 2 adds alternative: instead of guessing which element is the "Next" button, 
    click a plausible candidate and check whether the link count actually grew. Every 
    click is bounded, verified, and undone if it navigated away, so it is safe to run 
    on a site nobody has looked at.
    """
    seen = _harvest(page)
    baseline = len(seen)
    clicks = 0

    # ---- tier 0: maximise a DataTables-style "Show N entries" menu ----
    try:
        for sel in page.query_selector_all("select"):
            vals = sel.evaluate("s => Array.from(s.options).map(o => o.value)")
            nums = [v for v in vals if str(v).lstrip("-").isdigit()]
            if not nums:
                continue
            best = "-1" if "-1" in nums else max(nums, key=lambda x: int(x))
            if best == "-1" or int(best) >= 50:
                sel.select_option(best)
                page.wait_for_timeout(1200)
                break
    except Exception:
        pass
    _merge_links(seen, _harvest(page).values())

    g0 = len(seen) - baseline         # gained by the page-size menu (ported logic)
    _mark = len(seen)


    # ---- tier 1: the known selectors (cheap and already proven) ----
    barren = 0
    for sel in KNOWN_ADVANCE_SELECTORS:
        while clicks < budget and barren < REVEAL_MAX_BARREN:
            try:
                el = page.query_selector(sel)
            except Exception:
                el = None
            if el is None:
                break
            cls = (el.get_attribute("class") or "").lower()
            if "disabled" in cls or (el.get_attribute("aria-disabled") or "") == "true":
                break
            gained = _click_and_keep(page, el, seen)
            clicks += 1
            barren = 0 if gained else barren + 1
            if not gained:
                break

    g1 = len(seen) - _mark             # gained by the known selectors (ported logic)
    c1 = clicks
    _mark = len(seen)


    # ---- tier 2: discovered candidates ----
    tried = set()
    barren = 0
    n_cands = 0        
    js_error = ""

    while clicks < budget and barren < REVEAL_MAX_BARREN:
        try:
            cands = page.evaluate(JS_CLICKABLES)
        except Exception as e:
            # A broken JS_CLICKABLES looks EXACTLY like "this page has no
            # candidates". That silence hid a JS SyntaxError through five sites
            # of testing — so report it rather than letting the tier vanish.
            js_error = f"{type(e).__name__}: {str(e)[:120]}"
            break
        n_cands = max(n_cands, len(cands))
        # Expanders first: they ADD to the page. Pagination REPLACES content, so
        # expanding before advancing captures both states rather than one.
        order = {"expand": 0, "advance": 1}
        cands.sort(key=lambda c: order.get(c.get("kind"), 2))

        pick = None
        for c in cands:
            # Identity is label+class, NOT cid — cid is reassigned on every scan.
            key = (c.get("kind"), c.get("label"), c.get("cls"))
            if key not in tried:
                pick = c
                tried.add(key)
                break
        if pick is None:
            break                          # every candidate has been tried

        try:
            el = page.query_selector(f"[data-crawl-id='{pick['cid']}']")
        except Exception:
            el = None
        if el is None:
            continue                       # vanished between scan and click
        gained = _click_and_keep(page, el, seen)
        clicks += 1
        barren = 0 if gained else barren + 1

    _unstamp(page)
    ev = {"event": "reveal", "links": len(seen), "gained": len(seen) - baseline,
          "t0_select": g0, "t1_known": g1, "t2_discovered": len(seen) - _mark,
          "clicks": clicks, "t1_clicks": c1, "t2_clicks": clicks - c1,
          "cands": n_cands}
    if js_error:                 # tier 2 never ran — this is a code bug, not a site
        ev["js_error"] = js_error
    emit(ev)
    return list(seen.values())


# ============================================================================
# SECTION C2 — AUTO-SCOPE [IMPLEMENTED CHANGE - Dynamic Crawling]
#   Scope used to be a human decision typed in before the crawl. Here the
#   crawler loads the seed page once and works it out from what it can see, so
#   the answer is re-derived on every run instead of remembered from last time.
# ============================================================================

# Tuning knobs — CALIBRATED 2026-07-28 against 6 regulator sites.
# Measured with generic_crawler/calibrate_scope.py. Re-run it before changing these.
#
#   site           docs  under  cands  ratio  share  crumbs   correct scope
#   SECP acts        23      0    164     0%    12%       2   prefix   (rule 3)
#   SBP circulars     0     33    168    20%     0%       2   prefix   (rule 2, count)
#   SAMA sandbox      0      0     43     0%     0%       2   breadcrumb
#   SAMA CB law       1      0     43     0%     2%       3   breadcrumb
#   MISA laws        68      0     27     0%    72%       5   prefix   (rule 3)
#   SDAIA regs       36      0    117     0%    24%       9   breadcrumb (file guard)

PREFIX_MIN_RATIO   = 0.30  # UNVALIDATED: no site fires this rule. Kept as a net for
                           # "few links, mostly children" (high ratio, low count).
PREFIX_MIN_COUNT   = 10    # gap 0 -> 33 (SBP). Well-clear on both sides.
LISTING_DOC_MIN    = 5     # gap 1 (SAMA) -> 23 (SECP). Floor against tiny pages.
LISTING_DOC_SHARE  = 0.07  # gap 2% (SAMA) -> 12% (SECP). NARROW — weakest knob.
                           # Depends on the seed_is_file guard to exclude SDAIA (24%).
BREADCRUMB_MIN_LEN = 2     # DO NOT RAISE: SAMA sandbox has exactly 2 crumbs.


def detect_scope(breadcrumb, links, seed_url, seed_host):
    """Choose 'breadcrumb' | 'prefix' | 'host' from evidence on the seed page.

    Pure function that reads the three things extract_all() already returns

    Returns (scope, reason) — the reason is a plain-English sentence that gets
    logged, so a wrong guess is diagnosable at a glance.
    """
    seed_path = urlparse(seed_url).path.rstrip("/")

    # Real folder depth of the seed. A leading /en/ or /ar/ is a language
    # marker, not a section, so it doesn't count as a level.
    segs = [s for s in seed_path.split("/") if s]
    if segs and re.fullmatch(r"[a-z]{2,3}", segs[0]):
        segs = segs[1:]

    # A seed ending in .aspx/.html/.php is a PAGE, not a folder — nothing can
    # ever sit "under" it, so prefix scope would strand the crawl on one page.
    seed_is_file = bool(re.search(r"\.(aspx?|html?|php|jsp)$", seed_path, re.I))

    # ---- gather the evidence in one pass over the seed page's links ----
    candidates = 0   # same-host pages we could actually crawl into
    under      = 0   # ...of those, how many live under the seed's path
    docs       = 0   # PDFs / DOCX / Download buttons sitting on the seed page
    for l in links:
        href = l.get("href") or ""
        p = urlparse(href)
        if p.scheme not in ("http", "https") or p.netloc.lower() != seed_host:
            continue                                    # off-site or mailto: — ignore
        if is_document_link(href):
            docs += 1                                   # a file, not a page to crawl
            continue
        if ext_of(href) in SKIP_EXTS:
            continue                                    # images, css, fonts
        path = re.sub(r"/{2,}", "/", p.path).rstrip("/")
        if path == seed_path:
            continue                                    # link back to the seed itself
        candidates += 1
        if seed_path and path.startswith(seed_path + "/"):
            under += 1

    ratio     = (under / candidates) if candidates else 0.0
    doc_share = (docs / (docs + candidates)) if (docs + candidates) else 0.0
    crumbs    = [c for c in breadcrumb if c and c.strip()]
    ev = (f"{docs} docs, {under}/{candidates} links under '{seed_path or '/'}' "
          f"({ratio:.0%}), {len(crumbs)} breadcrumb steps")

    # ---- the rules: first match wins, strongest signal first ----
    if not segs:
        return "host", f"seed is the site root, nothing narrower to stay inside — {ev}"
    if not seed_is_file and (ratio >= PREFIX_MIN_RATIO or under >= PREFIX_MIN_COUNT):
        return "prefix", f"links cluster under the seed path — {ev}"
    if not seed_is_file and docs >= LISTING_DOC_MIN and doc_share >= LISTING_DOC_SHARE:
        return "prefix", f"seed page is mostly document links (a listing page) — {ev}"
    if len(crumbs) >= BREADCRUMB_MIN_LEN:
        return "breadcrumb", f"flat URLs but a real breadcrumb trail — {ev}"
    return "host", f"no clear signal, falling back to same-domain — {ev}"


def probe_scope(page, seed_url, seed_host, nav_timeout=60000):
    """Load the seed page once and let detect_scope() read it.

    Costs one extra page load per crawl. The main loop below stays exactly as it was.
    """
    try:
        page.goto(seed_url, wait_until="domcontentloaded", timeout=nav_timeout)
        try:
            page.wait_for_load_state("networkidle", timeout=2500)
        except Exception:
            pass
        for _ in range(3):
            page.mouse.wheel(0, 6000)
            page.wait_for_timeout(300)
        page.evaluate("window.scrollTo(0, 0)")
        try:
            page.wait_for_function(
                "() => document.querySelectorAll('a[href]').length > 15 "
                "|| (document.body && document.body.innerText.trim().length > 500)",
                timeout=10000)
        except Exception:
            pass
        # Snapshot BEFORE expanding. expand_tree() CLICKS things, and on some
        # sites (SBP) a click navigates away or tears down the DOM — leaving
        # nothing to read. crawl() guards against exactly this by snapshotting
        # first and merging both passes. The probe must do the same, or a 
        # healthy page can measure as empty.
        bc1, _c1, _s1, ln1 = extract_all(page)
        try:
            expand_tree(page)           # a collapsed menu hides links we want to count
            bc2, _c2, _s2, ln2 = extract_all(page)
        except Exception:
            bc2, ln2 = [], []
        breadcrumb = bc1 or bc2         # first non-empty trail wins
        _seen, links = set(), []        # union, so neither pass can lose a link
        for l in (ln1 + ln2):
            h = l.get("href")
            if h and h not in _seen:
                _seen.add(h)
                links.append(l)

        return detect_scope(breadcrumb, links, seed_url, seed_host)
    except Exception as e:
        # Probe failed. Prefer set default guess: a wrong 'prefix' collects too
        # little (visible, fixable); a wrong 'host' can wander a whole domain.
        fallback = "prefix" if urlparse(seed_url).path.strip("/") else "host"
        return fallback, f"probe failed ({str(e)[:120]}) — defaulting to {fallback}"

# ============================================================================
# SECTION D — crawl(): the main loop that ties everything together
#   queue of pages -> load -> render/scroll/expand -> extract -> scope check ->
#   record documents + page -> enqueue child links -> repeat until caps hit.
# ============================================================================


def crawl(seed_url, out_dir, max_pages=150, max_depth=8, scope="auto",
          headless=True, wait_ms=700, nav_timeout=60000):
    out = Path(out_dir)
    (out / "html").mkdir(parents=True, exist_ok=True)

    seed_norm = normalize_url(seed_url)
    seed_host = urlparse(seed_norm).netloc.lower()
    seed_prefix = urlparse(seed_norm).path.rstrip("/")  # "under the seed path" for prefix scope
    # If the seed sits under a 2-3 letter language segment (/en/...), lock the crawl
    # to that language so we don't load every page's /ar/ mirror just to reject it.
    _seg0 = first_seg(urlparse(seed_norm).path)
    lang_lock = _seg0 if re.fullmatch(r"[a-z]{2,3}", _seg0 or "") else None

    visited = set()
    content_hashes = {}     # content_key -> url that first recorded it
    records = []
    documents = {}          # normalized doc url -> record
    section_anchor = None   # set from the seed's breadcrumb (last item)

    emit({"event": "start", "seed": seed_norm, "scope": scope,
          "max_pages": max_pages, "max_depth": max_depth})

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        ctx = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
            locale="en-US",
        )
        page = ctx.new_page()

        # --- 3.1 auto-scope: decide before the queue starts moving ---
        if scope == "auto":
            scope, why = probe_scope(page, seed_norm, seed_host, nav_timeout)
            emit({"event": "scope", "scope": scope, "reason": why})

        queue = deque([(seed_norm, 0)])      # content / detail pages (high priority)
        page_queue = deque()                 # pagination / index pages (low priority)
        visited.add(seed_norm)

        while (queue or page_queue) and len(records) < max_pages:
            # Always drain real content pages before following pagination, so a page
            # cap is spent on actual documents, not on index pages full of links.
            url, depth = queue.popleft() if queue else page_queue.popleft()
            nav_ok = False
            last_err = ""
            for attempt in range(1, 3):  # goto can be slow on protected/heavy sites
                try:
                    # Load the DOM fast, then give late XHR a SHORT settle window.
                    # (Using wait_until="networkidle" in goto hangs the full timeout on
                    #  chatty sites like SBP whose analytics beacons never go idle.)
                    page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout)
                    try:
                        page.wait_for_load_state("networkidle", timeout=2500)
                    except Exception:
                        pass
                    nav_ok = True
                    break
                except Exception as e:
                    last_err = str(e)[:200]
                    emit({"event": "retry", "url": url, "attempt": attempt, "message": last_err})
                    page.wait_for_timeout(1500)
            if not nav_ok:
                emit({"event": "error", "url": url, "depth": depth, "message": last_err})
                continue
            try:
                # Many JS sites (e.g. SBP) render their list only on scroll (lazy load).
                # Scroll down a few times to trigger it, then return to top.
                try:
                    for _ in range(3):
                        page.mouse.wheel(0, 6000)
                        page.wait_for_timeout(300)
                    page.evaluate("window.scrollTo(0, 0)")
                except Exception:
                    pass
                # Wait for real content/links to appear (SPA routes inject body via JS).
                try:
                    page.wait_for_function(
                        "() => document.querySelectorAll('a[href]').length > 15 "
                        "|| (document.body && document.body.innerText.trim().length > 500)",
                        timeout=10000)
                except Exception:
                    pass
            except Exception as e:
                emit({"event": "error", "url": url, "depth": depth, "message": str(e)[:200]})
                continue

            # Snapshot NOW, before redirect-prone delays. Some SPAs (SBP) render the
            # content then client-side-redirect away seconds later; capturing here keeps it.
            title = (page.title() or "").strip()
            try:
                bc1, c1, st1, ln1 = extract_all(page)
            except Exception:
                bc1, c1, st1, ln1 = [], {"html": "", "text": ""}, "", []
            # Then settle and snapshot again — late-rendering content lands here.
            try:
                page.wait_for_timeout(wait_ms)
                bc2, c2, st2, ln2 = extract_all(page)
            except Exception:
                bc2, c2, st2, ln2 = [], {"html": "", "text": ""}, "", []
            # Merge: richer content wins; links are the union; first non-empty crumb/status.
            content = c1 if len(c1.get("text", "")) >= len(c2.get("text", "")) else c2
            breadcrumb = bc1 or bc2
            status = st1 or st2
            # reveal_all_links() covers the page-size menus, the known "Next" selectors
            # and adds a click-and-verify fallback plus a URL guard, so a click can no 
            # longer navigate away and leave the rest of this loop reading the wrong page.
            try:
                ln3 = reveal_all_links(page)
            except Exception as e:
                # Never swallow this silently. A bug in reveal_all_links() looks
                # exactly like "this site hides nothing behind buttons", and the
                # missing links are invisible in the output. Say so in the log.
                ln3 = []
                emit({"event": "error", "url": url, "depth": depth,
                      "message": f"reveal_all_links failed: {str(e)[:160]}"})
            links = list(_merge_links({}, ln1 + ln2 + ln3).values())
            if not title:
                title = (page.title() or "").strip()

            if section_anchor is None:
                # anchor = last meaningful breadcrumb item, else the page title
                section_anchor = (breadcrumb[-1] if breadcrumb else title).strip().lower()
                emit({"event": "anchor", "section_anchor": section_anchor})

            # --- scope decision ---
            in_scope = True
            crumb_l = [re.sub(r"\s+", " ", c).strip().lower() for c in breadcrumb]
            if scope == "breadcrumb":
                # EXACT match: the section anchor must be one of the breadcrumb steps.
                # (Substring matching wrongly pulled in parent sections, e.g. "Capital
                #  Market" leaking into "Capital Market Law".)
                anchor = re.sub(r"\s+", " ", section_anchor or "").strip()
                in_scope = bool(anchor) and anchor in crumb_l
                # the seed itself always counts
                if url == seed_norm:
                    in_scope = True
            elif scope == "prefix":
                in_scope = urlparse(url).path.startswith(seed_prefix)
            elif scope == "host":
                in_scope = True  # same-host already enforced at enqueue time

            # --- collect document links on this page regardless of scope ---
            # Includes plain .pdf/.docx AND download-manager links (SECP "Download"
            # buttons → /document/<slug>/?wpdmdl=<id>) that carry the title as link text.
            page_docs = []
            for l in links:
                href = l["href"]
                if l.get("chrome"):     # header/footer furniture, not this page's content
                    continue
                if urlparse(href).scheme in ("http", "https") and is_document_link(href):
                    dn = normalize_url(href)
                    page_docs.append(dn)
                    heads = collapse_heading_path(l.get("heading_path"))
                    # The section heading groups the links: every link under one
                    # heading gets that title, so a title repeats across rows.
                    doc_title = heads[-1][:200] if heads else best_doc_title(l, dn)
                    # Keyed on both, because several sections can link the same
                    # file (each into its own part of one big PDF) and each of
                    # those is an entry, while one file linked repeatedly under
                    # the same title stays a single entry.
                    key = (dn, doc_title)
                    if key not in documents:
                        documents[key] = {
                            "title": doc_title,
                            "doc_url": dn,
                            "type": doc_type_of(href),
                            "found_on": url,
                            "section_path": " > ".join(breadcrumb),
                        }

            if not in_scope:
                emit({"event": "skip", "url": url, "depth": depth,
                      "reason": "out-of-scope", "breadcrumb": breadcrumb})
                continue

            # --- skip aggregator / print pages (duplicate content + cause circling) ---
            if is_aggregator(url, title):
                emit({"event": "skip", "url": url, "depth": depth, "reason": "aggregator"})
                continue

            # --- skip same-content duplicates (Drupal -0/-1 twins, aliases) ---
            ckey = content_key(content["text"])
            if ckey and ckey in content_hashes:
                emit({"event": "skip", "url": url, "depth": depth,
                      "reason": "duplicate-content", "same_as": content_hashes[ckey]})
                continue
            if ckey:
                content_hashes[ckey] = url

            # --- record this page ---
            slug = slugify(urlparse(url).path or title) or f"page-{len(records)}"
            html_file = f"html/{slug}.html"
            (out / html_file).write_text(content["html"], encoding="utf-8")

            rec = {
                "section_path": " > ".join(breadcrumb),
                "title": title,
                "url": url,
                "depth": depth,
                "status": status,
                "n_pdfs": len(page_docs),
                "pdf_links": " | ".join(page_docs),
                "text_len": len(content["text"]),
                "html_file": html_file,
                "text": content["text"],
                "html": content["html"],
                "breadcrumb": breadcrumb,
            }
            records.append(rec)
            emit({"event": "visit", "url": url, "depth": depth, "title": title,
                  "section_path": rec["section_path"], "n_pdfs": len(page_docs),
                  "text_len": rec["text_len"], "recorded": len(records),
                  "queued": len(queue), "page_queued": len(page_queue)})

            # --- enqueue children ---
            if depth < max_depth:
                for l in links:
                    href = l["href"]
                    nh = normalize_url(href)
                    if nh in visited:
                        continue
                    pu = urlparse(nh)
                    if pu.scheme not in ("http", "https"):
                        continue
                    if pu.netloc.lower() != seed_host:      # same-host rule
                        continue
                    e = ext_of(nh)
                    if e in SKIP_EXTS or is_document_link(nh):  # assets ignored; docs recorded above
                        continue
                    if is_aggregator(nh, l.get("text", "")):  # never crawl entire-section/print pages
                        continue
                    if lang_lock and first_seg(pu.path) != lang_lock:  # skip other-language mirrors
                        continue
                    if DENY_PATH_PAT.search(pu.path) or pu.path in ("/", f"/{lang_lock}"):  # chrome pages
                        continue
                    # Header/footer links: follow one only if it stays under the
                    # seed's own path. Real content is sometimes reachable only from
                    # the nav, but a footer link that leaves the section opens the
                    # whole site.
                    if l.get("chrome") and not (seed_prefix
                                                and pu.path.startswith(seed_prefix)):
                        continue
                    if scope == "prefix" and not pu.path.startswith(seed_prefix):
                        continue
                    visited.add(nh)
                    # Pagination/index links go to the low-priority queue so real
                    # content pages are crawled first.
                    if l.get("nav"):
                        page_queue.append((nh, depth + 1))
                    else:
                        queue.append((nh, depth + 1))

        browser.close()

    # ---- write outputs ----
    (out / "pages.json").write_text(
        json.dumps({"seed": seed_norm, "pages": records,
                    "documents": list(documents.values())},
                   ensure_ascii=False, indent=2),
        encoding="utf-8")

    _write_excel(out / "pages.xlsx", records, list(documents.values()))

    emit({"event": "done", "pages": len(records), "documents": len(documents),
          "out_dir": str(out), "xlsx": str(out / "pages.xlsx")})
    return records


# ============================================================================
# SECTION E — write the Excel workbook + command-line entry point
# ============================================================================


def _write_excel(path, records, documents):
    import pandas as pd
    CELL_MAX = 32000
    page_rows = [{
        "section_path": r["section_path"],
        "title": r["title"],
        "url": r["url"],
        "depth": r["depth"],
        "status": r["status"],
        "n_pdfs": r["n_pdfs"],
        "pdf_links": r["pdf_links"][:CELL_MAX],
        "text_len": r["text_len"],
        "html_file": r["html_file"],
        "text_preview": (r["text"] or "")[:CELL_MAX],
    } for r in records]

    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        pd.DataFrame(page_rows).to_excel(xw, sheet_name="pages", index=False)
        if documents:
            pd.DataFrame(documents).to_excel(xw, sheet_name="documents", index=False)


def main():
    ap = argparse.ArgumentParser(description="Standalone Playwright sidebar crawler (test tool)")
    ap.add_argument("--url", required=True, help="Seed URL")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--max-pages", type=int, default=150)
    ap.add_argument("--max-depth", type=int, default=8)
    ap.add_argument("--scope", choices=["auto", "breadcrumb", "prefix", "host"],
                    default="auto", help="auto = work it out from the seed page (default)")
    ap.add_argument("--headful", action="store_true", help="Show the browser window")
    ap.add_argument("--wait-ms", type=int, default=700, help="Settle wait after each page")
    args = ap.parse_args()

    crawl(args.url, args.out, max_pages=args.max_pages, max_depth=args.max_depth,
          scope=args.scope, headless=not args.headful, wait_ms=args.wait_ms)


if __name__ == "__main__":
    main()
