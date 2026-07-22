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
You give it ONE start URL and ONE "scope" setting. It opens the page in a real
headless browser and behaves like a diligent person clicking around:

  1. Loads the page and lets JavaScript finish rendering.
  2. Expands menus/trees, reads content inside <frame>s, and scrolls to trigger
     lazy-loaded lists — so nothing stays hidden.
  3. Walks every level of the site (breadth-first: folder within folder ...),
     following links, until it runs out or hits the page/depth caps.
  4. On each page it records:
       - the BREADCRUMB trail   -> the "folder path" (how we mirror site structure)
       - the page CONTENT       -> rendered HTML + plain text
       - every DOCUMENT link    -> PDFs, DOCX, and "Download" buttons, with titles
  5. Keeps the crawl inside the section you asked for using the chosen SCOPE
     (breadcrumb / prefix / host) plus a same-host rule and hard caps.

--------------------------------------------------------------------------------
SCOPE (the only per-site choice — a setting, not code)
--------------------------------------------------------------------------------
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
  C. Page actions          - extract_all(), collect_paginated_links(), expand_tree()
  D. crawl()               - the main loop that ties it all together
  E. Excel writer + CLI

Run directly:
  venv/Scripts/python.exe sidebar_crawler_standalone/crawler.py \
      --url https://rulebook.sama.gov.sa/en/regulatory-sandbox \
      --out output/standalone_crawler/sama_sandbox --scope breadcrumb --max-pages 150
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
    """Pick a human title for a document link. The anchor text is often a generic
    'Download' button, so fall back to the row/card context (title + date) and
    finally to the URL slug."""
    t = (link.get("text") or "").strip()
    if t.lower() not in GENERIC_LINK_TEXT and len(t) > 3:
        return t[:200]
    ctx = (link.get("ctx") or "").strip()
    ctx = re.sub(r"\b(download|pdf|view|click here|read more)\b", "", ctx, flags=re.I).strip(" -|")
    if len(ctx) > 3:
        return ctx[:200]
    return title_from_slug(url) or t


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
JS_LINKS = r"""
() => Array.from(document.querySelectorAll('a[href]')).map(a => {
  const t = (a.textContent || '').trim();
  let nav = false, el = a;
  for (let i = 0; i < 4 && el; i++) {
    const c = ((el.className && el.className.toString ? el.className.toString() : '') + ' ' +
               (el.id || '')).toLowerCase();
    if (/paginat|pager|page-numbers|page-nav/.test(c)) { nav = true; break; }
    el = el.parentElement;
  }
  const rel = (a.getAttribute('rel') || '').toLowerCase();
  if (rel === 'next' || rel === 'prev') nav = true;
  if (/^(\d+|«|»|<|>|‹|›|\.\.\.|next|previous|prev|first|last)$/i.test(t)) nav = true;
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
  return { href: a.href, text: t.slice(0, 300), nav: nav, ctx: ctx };
})
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


# ============================================================================
# SECTION C — page actions (run against a loaded page, may click / read frames)
# ============================================================================


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
    links, seen = [], set()
    for fr in page.frames:                 # page.frames includes the main frame
        try:
            for l in fr.evaluate(JS_LINKS):
                h = l.get("href")
                if h and h not in seen:
                    seen.add(h)
                    links.append(l)
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
    return breadcrumb, best, status, links


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


# ============================================================================
# SECTION D — crawl(): the main loop that ties everything together
#   queue of pages -> load -> render/scroll/expand -> extract -> scope check ->
#   record documents + page -> enqueue child links -> repeat until caps hit.
# ============================================================================


def crawl(seed_url, out_dir, max_pages=150, max_depth=8, scope="breadcrumb",
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
            # Then let the tree expand and snapshot again (tree sites reveal more links).
            try:
                page.wait_for_timeout(wait_ms)
                expand_tree(page)
                bc2, c2, st2, ln2 = extract_all(page)
            except Exception:
                bc2, c2, st2, ln2 = [], {"html": "", "text": ""}, "", []
            # Merge: richer content wins; links are the union; first non-empty crumb/status.
            content = c1 if len(c1.get("text", "")) >= len(c2.get("text", "")) else c2
            breadcrumb = bc1 or bc2
            status = st1 or st2
            # Handle in-page (JS) pagination like DataTables "Show N entries" + 1/2
            # buttons (SECP): click through and gather links from every draw.
            try:
                ln3 = collect_paginated_links(page)
            except Exception:
                ln3 = []
            _seenh = set()
            links = []
            for l in (ln1 + ln2 + ln3):
                h = l.get("href")
                if h and h not in _seenh:
                    _seenh.add(h)
                    links.append(l)
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
                if urlparse(href).scheme in ("http", "https") and is_document_link(href):
                    dn = normalize_url(href)
                    page_docs.append(dn)
                    if dn not in documents:
                        documents[dn] = {
                            "title": best_doc_title(l, dn),   # real title/date, not "Download"
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
    ap.add_argument("--scope", choices=["breadcrumb", "prefix", "host"], default="breadcrumb")
    ap.add_argument("--headful", action="store_true", help="Show the browser window")
    ap.add_argument("--wait-ms", type=int, default=700, help="Settle wait after each page")
    args = ap.parse_args()

    crawl(args.url, args.out, max_pages=args.max_pages, max_depth=args.max_depth,
          scope=args.scope, headless=not args.headful, wait_ms=args.wait_ms)


if __name__ == "__main__":
    main()
