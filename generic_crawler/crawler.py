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
  venv/Scripts/python.exe generic_crawler/crawler.py \
      --url https://rulebook.sama.gov.sa/en/regulatory-sandbox \
      --out output/standalone_crawler/sama_sandbox --scope breadcrumb --max-pages 150
"""

import argparse
import json
import re
import sys
from collections import deque
from pathlib import Path
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, unquote

from playwright.sync_api import sync_playwright

# Shape-aware strategies (additive): detect tree / table layouts and dispatch.
try:
    from strategies import detect_shape, crawl_tree, crawl_table, crawl_list
    from blockcheck import blocked_reason
except ImportError:  # when imported as a package
    from .strategies import detect_shape, crawl_tree, crawl_table, crawl_list
    from .blockcheck import blocked_reason

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


def scope_prefix(path: str) -> str:
    """The path that `scope: prefix` means by "under the seed".

    Naively this was `path.rstrip("/")`, which is right only when the seed URL is
    a DIRECTORY. Point it at a page and the page's own filename lands in the
    prefix, so nothing on the site can ever start with it:

        seed    /en/RulesRegulations/Pages/rules.aspx
        prefix  /en/RulesRegulations/Pages/rules.aspx      <- a leaf
        test    /en/RulesRegulations/Agreements  -> False  <- the real content

    Every sibling is rejected, including the documents the seed exists to reach.
    Measured on ZATCA and Ministry of Commerce: 38 and 47 rows of site chrome
    (Contact Us, Careers, News, Brand Identity) and zero regulations.

    TWO SEGMENTS COME OFF, IN ORDER:

    1. A trailing FILENAME — a last segment containing a dot. `/a/b/rules.aspx`
       is the page, `/a/b` is the section it lives in.

    2. A trailing `/pages` — SharePoint keeps every page of a section in a
       `Pages/` folder, so `/en/RulesRegulations/Pages` names a storage folder,
       not a subject area. Sibling sections live at `/en/RulesRegulations/Taxes`
       and `/en/RulesRegulations/Agreements`, which are under the SECTION but
       not under `Pages`. Both KSA sites we crawl this way are SharePoint.

        /en/RulesRegulations/Pages/rules.aspx   -> /en/RulesRegulations
        /en/Regulations/pages/default.aspx      -> /en/Regulations
        /activities/laws                        -> /activities/laws  (unchanged)

    A directory seed is returned untouched, so hosts that already worked are
    unaffected. Never returns "" — an empty prefix matches every path on the
    host, silently turning `prefix` into `host`; the last real segment is kept
    instead.
    """
    p = (path or "").rstrip("/")
    segs = [s for s in p.split("/") if s]
    if segs and "." in segs[-1]:            # a file, not a directory
        segs.pop()
    if len(segs) > 1 and segs[-1].lower() == "pages":   # SharePoint page store
        segs.pop()
    return "/" + "/".join(segs) if segs else ""


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


# ---- external portals that HOST law text themselves ----
# Some regulators don't attach a PDF: they link out to a national legal portal
# where the law lives as a web page with no file extension (MISA links 24 of its
# laws to laws.boe.gov.sa/BoeLaws/Laws/LawDetails/<guid>). Nothing in the URL says
# "document", so the link would otherwise be crawled as an ordinary page — or,
# being off-host, dropped entirely.
#
# This is a hand-maintained list and it WILL need a new entry per regulator.
# Check it when onboarding a site: probe_signals.py prints the hosts it saw.
EXTERNAL_LAW_PORTALS = {
    "boe.gov.sa", "laws.boe.gov.sa",    # Bureau of Experts (Council of Ministers)
    "mc.gov.sa",                        # Ministry of Commerce
    "moj.gov.sa", "laws.moj.gov.sa",    # Ministry of Justice
    "pr.gov.sa",                        # Premium Residency
    "zatca.gov.sa",                     # Zakat, Tax and Customs Authority
}


def is_external_law_portal(url: str, seed_host: str = "") -> bool:
    """True if the URL's host is a known legal portal that serves law text itself,
    rather than something we can recognise from the path or extension.

    IT MUST NOT FIRE FOR LINKS THAT STAY ON THE SITE WE ARE CRAWLING.

    EXTERNAL_LAW_PORTALS answers "some OTHER regulator's site links OUT to
    zatca.gov.sa as a cross-reference — that link is a law, not a page to crawl".
    The word doing the work is EXTERNAL. When the seed IS zatca.gov.sa, every
    ordinary navigation link on the site matches the host and is misread as a
    terminal document.

    Measured on ZATCA 2026-08-12: the seed page alone produced n_pages=1 and
    n_documents=38, and those 38 were Contact Us, Careers, News, Magazine and
    Brand Identity. Documents are collected regardless of scope, so no scope
    setting can filter them out — the crawl also never went deeper, because
    every link it could have followed had been marked terminal.

    The standalone crawler (generic_crawler/crawler_MISA_MC_ZATCA.py) already
    carried this seed_host guard and the warning above; this shared engine had
    the copy without it. MC has the same exposure — mc.gov.sa is also listed.
    """
    host = urlparse(url).netloc.lower()
    if seed_host and (host == seed_host or seed_host.endswith("." + host)
                      or host.endswith("." + seed_host)):
        return False
    return any(host == d or host.endswith("." + d) for d in EXTERNAL_LAW_PORTALS)


def is_document_link(url: str, seed_host: str = "") -> bool:
    """True if a link points to a downloadable document — not just plain .pdf/.docx,
    but also download-manager links (WordPress Download Manager `wpdmdl=`, /document/,
    /download/ endpoints) that serve a file without a file extension in the URL,
    AND known external law portals that host the law as a page (see above).
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
    if is_external_law_portal(url, seed_host):
        return True
    return False


def doc_type_of(url: str, seed_host: str = "") -> str:
    """The file_type stored for a document link.

    `seed_host` matters here for the same reason it does in is_document_link:
    without it, EXTERNAL_LAW_PORTALS matches the site we are CURRENTLY crawling
    and every one of its own pages is stamped EXTERNAL. Measured on Ministry of
    Commerce 2026-08-12 — mc.gov.sa is in that set, so mc.gov.sa's own pages came
    back as EXTERNAL. The guard was threaded through is_document_link and missed
    here.
    """
    e = ext_of(url).lstrip(".").upper()
    if e:
        return e
    if is_external_law_portal(url, seed_host=seed_host):
        return "EXTERNAL"
    return "DOC"


GENERIC_LINK_TEXT = {"", "download", "pdf", "download pdf", "view", "view details",
                     "click here", "read more", "open", "details", "more",
                     # Action words that had been missing. "press here" reached
                     # the library as a document TITLE via the formfill side; the
                     # same words arrive here through anchor text.
                     "press here", "press", "click", "tap here", "here",
                     "download here", "download file", "download document",
                     "see more", "show more", "view more", "read", "detail",
                     "link", "attachment", "file", "document",
                     "اضغط هنا", "تحميل", "المزيد"}


def _norm_link_text(s: str) -> str:
    """Lowercased, with the invisible characters gov sites embed removed.

    GENERIC_LINK_TEXT is matched EXACTLY, so a single zero-width space defeats
    it. Ministry of Commerce stored a document titled `click here​` for
    exactly that reason — visually "click here", not equal to it.
    """
    s = (s or "").strip().lower()
    s = s.replace("​", "").replace("‌", "").replace("‎", "")
    s = s.replace("‏", "").replace("﻿", "").replace(" ", " ")
    s = re.sub(r"[\s.:،…]+", " ", s).strip()
    return s


PAGE_EXTS = {".aspx", ".asp", ".html", ".htm", ".php", ".jsp"}


def title_from_slug(url: str) -> str:
    """Human-readable name from the last URL segment.

    Handles the three things real regulator URLs throw at us:
      * percent-encoding      %20 / Arabic filenames  -> decoded
      * a file extension      ...Policy.pdf           -> dropped
      * squashed camelCase    BeneficiaryVoiceQ1      -> Beneficiary Voice Q1

    Only title-cases when the slug carries NO case information of its own, so
    "SAMA_Circular" is not mangled into "Sama Circular".
    """
    segs = [s for s in urlparse(url).path.split("/") if s]
    if not segs:
        return ""
    try:
        slug = unquote(segs[-1])
    except Exception:
        slug = segs[-1]

    m = re.search(r"\.[A-Za-z0-9]{2,5}$", slug)
    if m and m.group(0).lower() in (DOC_EXTS | PAGE_EXTS):
        slug = slug[:m.start()]

    had_case = not slug.islower() and not slug.isupper()
    slug = re.sub(r"[-_+]+", " ", slug)
    if had_case:
        # aBC -> a BC, and letter/digit boundaries: Q1 2025 stays, Voice2025 splits
        slug = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", slug)
        slug = re.sub(r"(?<=[A-Za-z])(?=\d{4}\b)", " ", slug)
    slug = re.sub(r"\s+", " ", slug).strip()
    if not had_case:
        slug = slug.title()
    return slug[:180]


def disambiguate_titles(documents: list) -> int:
    """A title shared by several DIFFERENT documents is not a title.

    SDAIA links whole groups of quarterly reports as "2025" and a set of distinct
    policies as "Policies" — 41 of its 415 documents collide this way. The URL
    slug names them properly, so re-derive only the colliding ones and leave every
    unique title untouched. Returns how many were rewritten.
    """
    from collections import Counter
    counts = Counter((d.get("title") or "").strip().lower() for d in documents)
    fixed = 0
    for d in documents:
        t = (d.get("title") or "").strip()
        if not t or counts[t.lower()] < 2:
            continue
        alt = title_from_slug(d.get("doc_url") or "")
        if alt and alt.strip().lower() != t.lower() and len(alt) > 3:
            d["title"] = alt
            fixed += 1
    return fixed


def _norm_heading(s: str) -> str:
    """Case/punctuation-insensitive form, so '&' and 'and' compare equal."""
    s = (s or "").lower().replace("&", " and ")
    s = "".join(c if (c.isalnum() or c.isspace()) else " " for c in s)
    return " ".join(s.split())


def collapse_heading_path(path) -> list:
    """Trim a raw heading trail to the headings that actually NAME a section.

    Two conventions, no thresholds:
      1. a heading ending in ':' introduces what follows, it doesn't name it
         ("Learn More about the Policies and Regulations:")
      2. a heading that restates its parent adds nothing ("Laws > Laws")
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


def doc_section_path(breadcrumb: list, group: str = "", nav_path: str = "",
                     heading_path=None) -> str:
    """The folder trail for a document: the page's breadcrumb, then optionally the
    on-page heading the link sits under (aml.gov.sa: "... > Rules and Regulations >
    Laws and Regulations"). The heading is dropped when it just repeats the last
    crumb or the page title, so we never emit "X > X".

    `group` is only passed when --group-headings is set, because a page's headings
    are a real section grouping on SOME sites only. Measured on the others: SECP
    headings are dates, CBB's is "FOLLOW US", and CMA's are the document titles
    themselves — appending those adds a junk or duplicate level. Opt in per site.

    `nav_path` is the structured trail from JS_NAV_PATH (tab panel + category
    heading). It is MORE specific than `group` — two named levels instead of one
    nearest heading — so when a page provides it, it wins and `group` is not also
    appended. It needs no opt-in flag: it is empty unless the markup matches.
    """
    # Drop a crumb that repeats one already in the trail. SDAIA's breadcrumb is
    # "SDAIA > Saudi Data and Artificial Intelligence Authority > SDAIA > About
    # SDAIA" — the same folder name twice. Only the display trail is affected;
    # breadcrumb SCOPE matching reads the raw list, not this.
    trail, _seen = [], set()
    for c in (breadcrumb or []):
        c = re.sub(r"\s+", " ", c or "").strip()
        if not c or c.lower() in _seen:
            continue
        _seen.add(c.lower())
        trail.append(c)

    def _append(parts):
        for p in parts:
            p = re.sub(r"\s+", " ", p).strip()
            if p and p.lower() not in [c.lower() for c in trail]:
                trail.append(p)
        return " > ".join(trail)

    # Precedence, most specific first:
    #   1. nav_path      — tab panel + category, two named levels (MISA)
    #   2. heading_path  — the page's own heading nesting (SDAIA)
    #   3. group         — single nearest heading, the original behaviour
    nav_parts = [p for p in (nav_path or "").split(">") if p.strip()]
    if nav_parts:
        return _append(nav_parts)

    hp = collapse_heading_path(heading_path or [])
    if hp:
        return _append(hp)

    return _append([group or ""])


def best_doc_title(link: dict, url: str) -> str:
    """Pick a human title for a document link, best source first:
      1. the anchor text            — unless it's a generic 'Download' button
      2. the anchor's title="..."   — usually the full name when the visible text
                                      is a bare year ("2025") or "Policies"
      3. the row/card context       — holds the real title + date on table rows
      4. the URL slug               — last resort
    """
    t = (link.get("text") or "").strip()
    if _norm_link_text(t) not in GENERIC_LINK_TEXT and len(t) > 3:
        return t[:200]
    ta = (link.get("title_attr") or "").strip()
    if len(ta) > 3:
        return ta[:200]
    ctx = (link.get("ctx") or "").strip()
    ctx = re.sub(r"\b(download|pdf|view|click here|read more)\b", "", ctx, flags=re.I).strip(" -|")
    if len(ctx) > 3:
        return ctx[:200]
    return title_from_slug(url) or t


# ============================================================================
# SECTION A2 — per-site profiles  (TEMPORARY: this moves to config/regulators/*.yml)
# ============================================================================
# Fixes that a site NEEDS but that would change what we already produce for the
# other regulators are opted into per host, so a default run stays byte-identical
# to before. Each key below was added because it was MEASURED to be wrong-by-
# default on some site and right on another — see the notes.
#
# Verified with a side-by-side old-vs-new run on the SAMA rulebook, SAMA
# circulars, CBB, SECP acts, SBP circulars and CMA seeds: with the profile off,
# breadcrumb and content output are unchanged on all six.
SITE_PROFILES = {
    "www.aml.gov.sa": {
        # The current crumb is a <span class="breadcrumbCurrent">, not an <a>, and
        # the Arabic home link ships in a display:none span. Anchors-only reading
        # therefore drops "Rules and Regulations" and keeps a hidden "AML", which
        # made the breadcrumb-scope anchor match every page on the site.
        "breadcrumb_current": True,
        # SharePoint wraps the whole page in <form id="aspnetForm">, so treating
        # <form> as junk deleted the entire document (0-byte HTML).
        "unwrap_forms": True,
        # No main/#content on the page; without these it falls through to <body>.
        "sharepoint_main": True,
        # The <h3>s ("Laws and Regulations" / "Rules and Instructions") are a real
        # section grouping here. On SECP the headings are dates, on CBB it is
        # "FOLLOW US", on CMA they are the document titles — junk levels there.
        "group_headings": True,
    },
    "sdaia.gov.sa": {
        # SharePoint, same as aml.gov.sa: the whole page is inside <form
        # id="aspnetForm"> and there is no <main>/#content.
        "unwrap_forms": True,
        "sharepoint_main": True,
        # Measured 2026-08-01 on RegulationsAndPolicies.aspx: all 36 documents
        # sit under real section headings ("Personal Data Protection Law and The
        # implementing Regulation", "Data classification Policy and Regulations",
        # "Freedom of Information Policy and Regulations"). Without this every
        # document lands in one flat folder — the single nearest heading is the
        # same "[Laws and Regulations]" label for all 36.
        "group_headings": True,
    },
}

DEFAULT_PROFILE = {
    "breadcrumb_current": False,
    "unwrap_forms": False,
    "sharepoint_main": False,
    "group_headings": False,
}


def profile_for(url: str) -> dict:
    """Per-host switches for the site-specific fixes; defaults = pre-existing
    behaviour for every host not listed."""
    prof = dict(DEFAULT_PROFILE)
    prof.update(SITE_PROFILES.get(urlparse(url).netloc.lower(), {}))
    return prof


# ============================================================================
# SECTION B — browser-side JavaScript snippets
# These strings are handed to the browser and run INSIDE the page (via
# page.evaluate). That is how we read what a real user sees after JS renders.
# Snippets that a profile can change take an `opts` argument (the profile dict).
# ============================================================================
# ---------------- browser-side extraction (runs as JS in the page) --------------

# Pull the breadcrumb trail as a list of visible link/label texts.
JS_BREADCRUMB = r"""
(opts) => {
  opts = opts || {};
  const isSep = t => !t || /^[>›—–\-|/·]+$/.test(t);   // separator-only or empty
  const sels = ['.breadcrumb','.bread-crumb','nav[aria-label*="readcrumb" i]',
                'ol[class*="crumb"]','[class*="rumb"]'];
  for (const s of sels) {
    for (const el of document.querySelectorAll(s)) {
      // skip the hidden PDF-clone breadcrumb (its separators are the only tagged text)
      if (el.closest('[aria-hidden="true"], #pdfDownloadLayout') ||
          /pdf/i.test(el.className || '')) continue;
      // Only crumbs a real user can SEE. Bilingual sites (aml.gov.sa) ship the
      // other language's home link in a display:none span — including it would
      // poison the section anchor and make breadcrumb scope match every page.
      // Rendered-box test, not a style test: display:none on an ANCESTOR leaves the
      // crumb's own computed display as "inline", so only asking about the node
      // itself would keep the hidden crumb. No box => the user cannot see it.
      const visible = n => !opts.breadcrumb_current ||
                           ((n.getClientRects().length > 0 ||
                             n.offsetWidth > 0 || n.offsetHeight > 0) &&
                            getComputedStyle(n).visibility !== 'hidden');
      // Breadcrumbs are links, BUT the current (last) crumb is often NOT a link —
      // it's a <span class="breadcrumbCurrent">/[aria-current]. Reading those too
      // is opt-in: it can pick a DIFFERENT breadcrumb container than anchors-only
      // did (measured on CMA: "Capital Market Authority" -> "Home") and it moves
      // the scope anchor from the parent section to the current page.
      const sel = opts.breadcrumb_current
        ? 'a, [class*="current" i], [aria-current], [class*="active" i]'
        : 'a';
      let nodes = Array.from(el.querySelectorAll(sel));
      // drop a node that merely CONTAINS another crumb node (avoid double counting)
      nodes = nodes.filter(n => !nodes.some(o => o !== n && n.contains(o)));
      let parts = nodes.filter(visible)
        .map(n => n.textContent.trim()).filter(t => !isSep(t) && t.length < 200);
      if (!parts.length)
        parts = Array.from(el.querySelectorAll('li')).filter(visible)
          .map(n => n.textContent.trim()).filter(t => !isSep(t) && t.length < 200);
      const out = [];
      for (const p of parts) if (out[out.length-1] !== p) out.push(p);
      if (out.length) return out;
    }
  }
  return [];
}
"""

# Main content: clone the best content container, then strip everything that isn't
# the actual document text. Critically this removes:
#   - HIDDEN PDF-CLONE blocks (SBP circulars render the body twice: once visible,
#     once inside a hidden #pdfDownloadLayout/#pdfContentClone used to make their PDF)
#     -> without stripping these the saved HTML is DUPLICATED.
#   - buttons like "Download PDF", the accessibility widget, banners, breadcrumb,
#     back-to-top — page chrome we don't want in the captured content.
JS_MAIN_CONTENT = r"""
(opts) => {
  opts = opts || {};
  // Extra containers are opt-in: querySelector returns the first match in DOCUMENT
  // order, not selector order, so widening this list can change which element is
  // picked on a site that already matched one.
  const sels = 'main, [role="main"], article, #content, .content, #main' +
               (opts.sharepoint_main ? ',[id*="PlaceHolderMain"], #contentBox, .main-content' : '');
  const pick = document.querySelector(sels);
  const src = pick || document.body || document.documentElement;
  if (!src) return { html: '', text: '' };   // frameset top docs have no <body>
  const clone = src.cloneNode(true);
  // ASP.NET / SharePoint wrap the ENTIRE page in <form id="aspnetForm">. Removing
  // <form> outright then deletes the whole page (we saw 0-byte HTML on aml.gov.sa).
  // So when opted in: UNWRAP content-bearing forms (keep children, drop the
  // wrapper) and only REMOVE small ones, the real search/subscribe widgets.
  if (opts.unwrap_forms) {
    clone.querySelectorAll('form').forEach(f => {
      const meaty = (f.innerText || '').trim().length > 400 ||
                    f.querySelector('h1,h2,h3,h4,table,ul,ol,article');
      if (meaty) { while (f.firstChild) f.parentNode.insertBefore(f.firstChild, f); }
      f.remove();
    });
  }
  const junk = [
    'script','style','noscript','nav','aside','header','footer','button','iframe',
    ...(opts.unwrap_forms ? [] : ['form']),
    '[aria-hidden="true"]','[hidden]','.no-print',
    '#pdfDownloadLayout','[id*="pdfDownload"]','[id*="Clone"]','[id*="clone"]',
    '#accessibility-modal','[id*="accessibility"]','.back-to-top',
    '.overlay-mega-menu','.bg-overlay','.pages-banner','.bread-crumb','.breadcrumb',
    '[style*="display:none"]','[style*="display: none"]'
  ];
  clone.querySelectorAll(junk.join(',')).forEach(n => n.remove());
  return { html: clone.innerHTML, text: (clone.innerText || '').trim() };
}
"""

# The real document title. Page <title>/banner is often generic ("Circulars"),
# so prefer a subject-like heading in the body, skipping banner/nav/breadcrumb.
JS_DOC_TITLE = r"""
() => {
  const clean = s => (s || '').replace(/\s+/g, ' ').trim();
  const banned = el => el.closest('.pages-banner,.banner,header,nav,.bread-crumb,' +
                                  '.breadcrumb,footer,[aria-hidden="true"],#pdfDownloadLayout');
  // 1) The real title is the main heading (green <h2>): first h1/h2 that is NOT the
  //    site banner and not the generic word "Circulars".  (Do NOT trust
  //    .circular-subject here — SBP tags BOTH the recipient line and the subject
  //    with that class, so it's ambiguous.)
  for (const h of document.querySelectorAll('h1, h2')) {
    if (banned(h)) continue;
    const t = clean(h.innerText);
    if (t && t.length > 4 && !/^circulars?$/i.test(t)) return t.slice(0, 250);
  }
  // 2) fallbacks
  for (const sel of ['.pdf-title', 'h1.entry-title', '.page-title h1']) {
    const e = document.querySelector(sel);
    if (e && !banned(e)) { const t = clean(e.innerText); if (t.length > 4) return t.slice(0, 250); }
  }
  return '';
}
"""

# All anchors: absolute href + visible text + whether it's a pagination/nav link.
# `nav=true` links (page numbers, next/prev, pager containers) are followed at LOW
# priority so real content/detail pages get crawled before index pages.
JS_LINKS = r"""
() => {
// Headings, in document order, resolved ONCE per call (the DOM changes between
// calls — expand_tree reveals more — so this must not be cached on window).
const heads = Array.from(document.querySelectorAll('h1,h2,h3,h4,legend,caption'))
  .map(h => ({ el: h, text: (h.innerText || '').replace(/\s+/g, ' ').trim() }))
  .filter(h => h.text && h.text.length < 120);

// Ranked headings, for the NESTED trail (heading_path) rather than the single
// nearest heading (group). h1..h6 nest; legend/caption/summary act as headings
// but sort below any real one. Headings inside nav/aside/header/footer label a
// widget, not the content, so they are excluded here (they are NOT excluded from
// `group`, whose behaviour must stay exactly as it was).
const rankOf = el => {
  const t = el.tagName;
  if (/^H[1-6]$/.test(t)) return parseInt(t[1], 10);
  if (el.getAttribute('role') === 'heading') {
    const n = parseInt(el.getAttribute('aria-level') || '0', 10);
    if (n >= 1 && n <= 6) return n;
  }
  if (t === 'LEGEND' || t === 'CAPTION' || t === 'SUMMARY') return 7;
  return 0;
};
const inWidget = el => !!el.closest(
  'nav, aside, header, footer, [role="navigation"], [role="complementary"], '
  + '[role="banner"], [role="contentinfo"]');
const rankedHeads = Array.from(document.querySelectorAll(
    'h1,h2,h3,h4,h5,h6,legend,caption,summary,[role="heading"]'))
  .map(h => ({ el: h, rank: rankOf(h),
               text: (h.innerText || h.textContent || '').replace(/\s+/g, ' ').trim() }))
  .filter(h => h.rank && h.text && h.text.length < 300 && !inWidget(h.el));
// Links are not always <a href>. Government SharePoint routinely ships a
// <button onclick="window.location.href='...'"> that navigates exactly like a
// link — CMA builds its entire article index that way, so every article was
// invisible to a walk that only reads anchors. Note the destination must come
// from the onclick: CMA's matching data-bs-target id is lowercase and 404s.
// Collected as pseudo-anchors so the rest of this function is unchanged.
const _cands = [];
for (const a of document.querySelectorAll('a[href]')) _cands.push({el: a, href: a.href});
for (const b of document.querySelectorAll('[onclick]')) {
  if (b.tagName === 'A' && b.hasAttribute('href')) continue;    // already have it
  const oc = b.getAttribute('onclick') || '';
  const m = oc.match(/location\.href\s*=\s*['"]([^'"]+)['"]/i);
  if (!m) continue;
  let abs = '';
  try { abs = new URL(m[1], location.href).href; } catch (e) { continue; }
  _cands.push({el: b, href: abs});
}
return _cands.map(({el: a, href: _href}) => {
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
  // Group heading = nearest heading BEFORE this link in document order. Listing
  // pages split their documents under on-page headings that never appear in the
  // breadcrumb (aml.gov.sa: "Laws and Regulations" / "Rules and Instructions"),
  // so without this the sub-section of the trail is lost.
  let group = '';
  for (const h of heads) {
    // compareDocumentPosition: 4 = a comes AFTER h -> h precedes the link, keep it.
    // Once a heading no longer precedes the link, every later one won't either.
    if (h.el.compareDocumentPosition(a) & 4) group = h.text; else break;
  }
  // heading_path = the NESTED trail. Walk the ranked headings that precede this
  // link, popping any of equal or shallower rank, exactly as the page nests them.
  // On SDAIA `group` returns the same "[Laws and Regulations]" for all 36
  // documents, while this returns the real section each one sits under.
  const stack = [];
  for (const h of rankedHeads) {
    if (!(h.el.compareDocumentPosition(a) & 4)) break;
    while (stack.length && stack[stack.length - 1].rank >= h.rank) stack.pop();
    stack.push(h);
  }
  const heading_path = stack.map(s => s.text);
  // Chrome = this link sits in the site header/footer, i.e. furniture repeated on
  // every page (Privacy Policy, the mega-menu, social icons) rather than this
  // page's content. Used to keep furniture OUT OF THE DOCUMENTS LIST only — a
  // chrome link is still followed as a page, because on some sites the mega-menu
  // is the only route to real content (SECP's masthead holds 86% of its links).
  // <nav>/<aside> are deliberately NOT treated as chrome: unlike header/footer
  // they are often a genuine in-page category sidebar.
  const chrome = !!a.closest('header, footer, [role="banner"], [role="contentinfo"]');
  return { href: _href, text: t.slice(0, 300), nav: nav, ctx: ctx, group: group,
           heading_path: chrome ? [] : heading_path,
           chrome: chrome,
           // title="..." often holds the full document name when the visible
           // link text is a bare year ("2025") or a generic word ("Policies").
           title_attr: (a.getAttribute('title') || '').trim().slice(0, 300) };
});
}
"""

# Structured section trail, for pages that group their links in tab panels or
# accordions rather than in a breadcrumb. Two levels: the panel's own heading
# ("Sectoral Legislations") and the group heading nearest the link ("Real estate
# sector"). Where a breadcrumb only says "Home > Activities", this is the real
# structure of the page.
#
# Measured coverage: MISA 89 links, SECP/SBP/SAMA/SDAIA 0 — it stays quiet on
# markup it does not recognise, which is why it needs no per-site flag.
JS_NAV_PATH = r"""
() => {
  const HEADING_SEL = 'h2,h3,h4,h5,h6';
  const clean = el => el ? (el.textContent || '').replace(/\s+/g, ' ').trim() : '';
  const SECTION_SELECTORS = ['.regulationContent', '[id$="Show"]',
                             '[class*="tab-panel" i]', '[class*="tabpanel" i]'];
  const closestSection = el => {
    for (const sel of SECTION_SELECTORS) {
      try { const f = el.closest(sel); if (f) return f; } catch (e) {}
    }
    return null;
  };
  const categoryLabel = (a, section) => {
    // the card/banner this link sits in usually carries the category heading
    const panel = a.closest('.showLawItems, [class*="banner" i], [class*="panel" i]');
    if (panel) { const t = clean(panel.querySelector(HEADING_SEL)); if (t) return t; }
    // mobile variant: the category is the <li> preceding the wrapper
    const mob = a.closest('[class*="MobItems" i], [class*="mob-items" i]');
    if (mob) {
      let sib = mob.previousElementSibling;
      while (sib) {
        if (sib.tagName === 'LI') { const t = clean(sib); if (t) return t; }
        sib = sib.previousElementSibling;
      }
    }
    // otherwise: the last heading inside the section that PRECEDES this link
    if (section) {
      const hs = Array.from(section.querySelectorAll(HEADING_SEL));
      const before = hs.filter(h =>
        !!(h.compareDocumentPosition(a) & Node.DOCUMENT_POSITION_FOLLOWING));
      if (before.length) return clean(before[before.length - 1]);
    }
    return '';
  };
  return Array.from(document.querySelectorAll('a[href]')).map(a => {
    const section = closestSection(a);
    const parts = [clean(section ? section.querySelector(HEADING_SEL) : null),
                   categoryLabel(a, section)].filter(Boolean);
    return { href: a.href, text: clean(a), nav_path: parts.join(' > ') };
  });
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


# ============================================================================
# SECTION C — page actions (run against a loaded page, may click / read frames)
# ============================================================================


def _merge_links(store: dict, links) -> dict:
    """Merge links into an href-keyed store. First sighting wins, EXCEPT:

      * a link seen outside the header/footer even once is never chrome, and
      * a sighting that carries a heading trail beats one that lost it.

    Keying on href alone is deliberate. The same URL usually appears in both the
    content and the footer of a page; whichever copy happened to be seen first
    would otherwise decide whether a real document survives the chrome filter.
    Clicking (pagination, accordions) can also re-render a link without its
    surrounding headings, so an earlier, richer sighting must not be overwritten.
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
        if not store[h].get("heading_path") and l.get("heading_path"):
            store[h]["heading_path"] = l["heading_path"]
        if not store[h].get("group") and l.get("group"):
            store[h]["group"] = l["group"]
    return store


def extract_all(page, opts=None):
    """Frame-aware extraction — the key to handling ALL site types.

    Old sites (e.g. SBP) use <frameset>: the top document has no <body>, and the
    real links/content live inside child <frame>s. Modern sites (e.g. SAMA) are a
    single document. This walks every frame, merges the links, keeps the richest
    content frame, and takes a breadcrumb/status from whichever frame has one — so
    the same code works for both without site-specific config.
    `opts` is the per-host profile (see SITE_PROFILES); None = default behaviour.
    Returns (breadcrumb, content{html,text}, status, links)."""
    opts = opts or DEFAULT_PROFILE
    breadcrumb, status = [], ""
    best = {"html": "", "text": ""}
    link_map = {}
    for fr in page.frames:                 # page.frames includes the main frame
        try:
            _merge_links(link_map, fr.evaluate(JS_LINKS))
        except Exception:
            pass
        try:
            c = fr.evaluate(JS_MAIN_CONTENT, opts)
            if c and len(c.get("text", "")) > len(best["text"]):
                best = c
        except Exception:
            pass
        if not breadcrumb:
            try:
                b = fr.evaluate(JS_BREADCRUMB, opts)
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


# ---------------------------------------------------------------------------
# Click-and-verify link revealing.
#
# The old approach guessed which element was the "Next" button from a hardcoded
# selector list, and expand_tree() clicked anything that looked like a toggle
# WITHOUT checking whether the click helped — or even whether it navigated away,
# which silently left the rest of the page's extraction running on the wrong
# document.
#
# This does not try to know which element is the right one. It clicks a plausible
# candidate and checks whether the page actually gained links; every click is
# budgeted, verified, and undone if it navigated. That makes it safe to run on a
# site nobody has looked at.
# ---------------------------------------------------------------------------

# Candidates are found in the page and stamped with a temporary id so Python can
# click them. Nothing here decides what "works" — the click loop does.
JS_CLICKABLES = r"""
() => {
  document.querySelectorAll('[data-crawl-id]').forEach(
    n => n.removeAttribute('data-crawl-id'));

  // NOTE: each regex MUST stay on one line. JavaScript has no /x flag, so a
  // literal broken across lines is a SyntaxError — and page.evaluate() would then
  // throw on every call, silently disabling this whole tier.
  const NEVER = /log\s*out|sign\s*out|logout|signout|delete|remove|unsubscribe|submit|register|sign\s*up|login|sign\s*in|print|download|share/i;
  const ADVANCE = /^\s*(next|more|load\s*more|show\s*(all|more)|view\s*(all|more)|see\s*more|older|newer|\d{1,4}|»|›|>>|>)\s*$/i;
  const ADVANCE_CLASS = /paginat|pager|page-numbers|page-nav|load-?more|show-?more/i;
  const EXPAND_CLASS  = /toggle|expand|collaps|accordion|tree|caret|chevron|has-children|dropdown/i;

  const out = [];
  let id = 0;
  for (const el of document.querySelectorAll(
      'a, button, [role="button"], [aria-expanded], summary, '
      + '[class*="toggle"], [class*="expand"]')) {
    const r = el.getBoundingClientRect();
    if (r.width < 2 || r.height < 2) continue;              // not on screen
    const cs = getComputedStyle(el);
    if (cs.visibility === 'hidden' || cs.display === 'none' || cs.opacity === '0') continue;

    const text = (el.innerText || el.textContent || '').replace(/\s+/g,' ').trim().slice(0,80);
    const aria = (el.getAttribute('aria-label') || '').trim().slice(0, 80);
    const cls  = ((el.className && el.className.toString) ? el.className.toString() : '')
                 + ' ' + (el.id || '');
    const label = (text + ' ' + aria).trim();
    if (NEVER.test(label) || NEVER.test(cls)) continue;

    // Only click things with NO real destination. A real href is already in the
    // crawl queue; clicking it just navigates away and wastes budget. We want
    // buttons, "#" links and javascript: links — controls that change the page.
    const href = el.getAttribute('href');
    if (el.tagName === 'A' && href && !/^\s*(#|javascript:)/i.test(href)) continue;
    if (/disabled/i.test(cls) || el.getAttribute('aria-disabled') === 'true' || el.disabled)
      continue;

    let kind = '';
    if (el.getAttribute('aria-expanded') === 'false' || EXPAND_CLASS.test(cls)) kind = 'expand';
    if (ADVANCE.test(label) || ADVANCE_CLASS.test(cls)) kind = 'advance';
    if (!kind) continue;

    el.setAttribute('data-crawl-id', String(id));
    out.push({ cid: id, kind: kind, label: label.slice(0,60), cls: cls.slice(0,80) });
    id++;
  }
  return out;
}
"""

REVEAL_CLICK_BUDGET = 25   # hard cap on clicks per page — stops runaway loops
REVEAL_SETTLE_MS    = 600  # how long to let the page redraw after a click
REVEAL_MAX_BARREN   = 1    # stop after the first click that gains nothing

# The old hardcoded guesses, kept as a cheap fast path before the discovery tier.
KNOWN_ADVANCE_SELECTORS = [
    ".paginate_button.next", "a.paginate_button.next", "li.next a", "a.next",
    "[rel='next']", ".pagination .next a", "button.next",
]


def _harvest(page, opts=None):
    """Every link the crawler can currently see, keyed by href.
    Goes through extract_all() so we see exactly what the crawl will see."""
    try:
        _bc, _c, _s, links = extract_all(page, opts)
    except Exception:
        return {}
    return _merge_links({}, links)


def _unstamp(page):
    """Remove the data-crawl-id markers so they never reach the saved HTML."""
    try:
        page.evaluate("() => document.querySelectorAll('[data-crawl-id]')"
                      ".forEach(n => n.removeAttribute('data-crawl-id'))")
    except Exception:
        pass


def _click_and_keep(page, el, seen, opts=None):
    """Click ONE element; keep the result only if the page gained links.

    Always leaves the browser on the page we started from — the guard expand_tree
    lacks. Returns True if new links appeared.
    """
    before_url = page.url
    before_n = len(seen)
    try:
        el.scroll_into_view_if_needed(timeout=800)
        el.click(timeout=1200)
        page.wait_for_timeout(REVEAL_SETTLE_MS)
    except Exception:
        return False                       # not clickable / detached — no harm

    # A click that NAVIGATED is not a reveal. Undo it, or everything after this
    # runs against the wrong document.
    if page.url != before_url:
        try:
            page.go_back(wait_until="domcontentloaded", timeout=15000)
            page.wait_for_timeout(300)
        except Exception:
            pass
        return False

    _merge_links(seen, _harvest(page, opts).values())
    return len(seen) > before_n


def reveal_all_links(page, budget=REVEAL_CLICK_BUDGET, opts=None):
    """Click things that might reveal more links; return the union of all states.

    Three tiers, cheapest first:
      0. maximise a "Show N entries" page-size menu
      1. the known Next selectors  (fast path)
      2. candidates discovered by JS_CLICKABLES  (site-agnostic fallback)

    Supersedes collect_paginated_links() + expand_tree(), which remain defined
    for probe_scope() and manual debugging.
    """
    seen = _harvest(page, opts)
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
    _merge_links(seen, _harvest(page, opts).values())
    g0 = len(seen) - baseline
    _mark = len(seen)

    # ---- tier 1: the known selectors ----
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
            gained = _click_and_keep(page, el, seen, opts)
            clicks += 1
            barren = 0 if gained else barren + 1
            if not gained:
                break
    g1 = len(seen) - _mark
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
            # candidates". Report it rather than letting the tier vanish silently.
            js_error = f"{type(e).__name__}: {str(e)[:120]}"
            break
        n_cands = max(n_cands, len(cands))
        # Expanders first: they ADD to the page. Pagination REPLACES content, so
        # expanding before advancing captures both states rather than one.
        cands.sort(key=lambda c: {"expand": 0, "advance": 1}.get(c.get("kind"), 2))

        pick = None
        for c in cands:
            # Identity is label+class, NOT cid — cid is reassigned on every scan.
            key = (c.get("kind"), c.get("label"), c.get("cls"))
            if key not in tried:
                pick = c
                tried.add(key)
                break
        if pick is None:
            break

        try:
            el = page.query_selector(f"[data-crawl-id='{pick['cid']}']")
        except Exception:
            el = None
        if el is None:
            continue                       # vanished between scan and click
        gained = _click_and_keep(page, el, seen, opts)
        clicks += 1
        barren = 0 if gained else barren + 1

    _unstamp(page)
    ev = {"event": "reveal", "links": len(seen), "gained": len(seen) - baseline,
          "t0_select": g0, "t1_known": g1, "t2_discovered": len(seen) - _mark,
          "clicks": clicks, "t1_clicks": c1, "t2_clicks": clicks - c1,
          "cands": n_cands}
    if js_error:                 # tier 2 never ran — a code bug, not a site
        ev["js_error"] = js_error
    emit(ev)
    return list(seen.values())


# ============================================================================
# AUTO-SCOPE — work the boundary out from the landing page
#
# Scope used to be a human decision typed in before the crawl. Here the crawler
# reads the seed page and derives it, so the answer is re-derived on every run
# rather than remembered from the last one.
#
# SCOPE answers "how far may I wander" (breadcrumb / prefix / host).
# SHAPE (strategies.py) answers "how do I read this layout" (tree / table / BFS).
# They are INDEPENDENT: SECP is table+prefix, SAMA is tree+breadcrumb, SDAIA is
# generic+breadcrumb. Both are decided from the same single page load below.
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
#
# THESE FIVE VALUES ARE GLOBAL. Tuning one to fix a single site can silently flip
# another site's scope — re-run the harness on ALL sites before changing any.
PREFIX_MIN_RATIO   = 0.30  # UNVALIDATED: no site fires this rule. Kept as a net for
                           # "few links, mostly children" (high ratio, low count).
PREFIX_MIN_COUNT   = 10    # gap 0 -> 33 (SBP). Well clear on both sides.
LISTING_DOC_MIN    = 5     # gap 1 (SAMA) -> 23 (SECP). Floor against tiny pages.
LISTING_DOC_SHARE  = 0.07  # gap 2% (SAMA) -> 12% (SECP). NARROW — weakest knob.
                           # Relies on the seed_is_file guard to exclude SDAIA (24%).
BREADCRUMB_MIN_LEN = 2     # DO NOT RAISE: SAMA sandbox has exactly 2 crumbs.


def detect_scope(breadcrumb, links, seed_url, seed_host):
    """Choose 'breadcrumb' | 'prefix' | 'host' from evidence on the seed page.

    Pure function over the three things extract_all() already returns. The reason
    string is logged, so a wrong guess is diagnosable at a glance.
    """
    seed_path = urlparse(seed_url).path.rstrip("/")

    # Real folder depth. A leading /en/ or /ar/ is a language marker, not a level.
    segs = [s for s in seed_path.split("/") if s]
    if segs and re.fullmatch(r"[a-z]{2,3}", segs[0]):
        segs = segs[1:]

    # A seed ending .aspx/.html/.php is a PAGE, not a folder — nothing can sit
    # "under" it, so prefix scope would strand the crawl on that one page.
    seed_is_file = bool(re.search(r"\.(aspx?|html?|php|jsp)$", seed_path, re.I))

    candidates = under = docs = 0
    for l in links:
        href = l.get("href") or ""
        p = urlparse(href)
        if p.scheme not in ("http", "https") or p.netloc.lower() != seed_host:
            continue                                    # off-site / mailto:
        # seed_host matters here too: without it every same-host link on a
        # portal host counts as a document, and this function's docs-vs-pages
        # ratio is exactly what chooses the scope.
        if is_document_link(href, seed_host):
            docs += 1                                   # a file, not a page
            continue
        if ext_of(href) in SKIP_EXTS:
            continue                                    # images, css, fonts
        path = re.sub(r"/{2,}", "/", p.path).rstrip("/")
        if path == seed_path:
            continue                                    # link back to the seed
        candidates += 1
        if seed_path and path.startswith(seed_path + "/"):
            under += 1

    ratio = (under / candidates) if candidates else 0.0
    doc_share = (docs / (docs + candidates)) if (docs + candidates) else 0.0
    crumbs = [c for c in breadcrumb if c and c.strip()]
    ev = (f"{docs} docs, {under}/{candidates} links under '{seed_path or '/'}' "
          f"({ratio:.0%}), {len(crumbs)} breadcrumb steps")

    if not segs:
        return "host", f"seed is the site root, nothing narrower to stay inside — {ev}"
    if not seed_is_file and (ratio >= PREFIX_MIN_RATIO or under >= PREFIX_MIN_COUNT):
        return "prefix", f"links cluster under the seed path — {ev}"
    if not seed_is_file and docs >= LISTING_DOC_MIN and doc_share >= LISTING_DOC_SHARE:
        return "prefix", f"seed page is mostly document links (a listing page) — {ev}"
    if len(crumbs) >= BREADCRUMB_MIN_LEN:
        return "breadcrumb", f"flat URLs but a real breadcrumb trail — {ev}"
    return "host", f"no clear signal, falling back to same-domain — {ev}"


def probe_scope(page, seed_url, seed_host, opts=None):
    """Read an ALREADY-LOADED seed page and let detect_scope() judge it.

    Snapshot BEFORE expanding: expand_tree() clicks things, and on some sites a
    click navigates away or tears the DOM down, leaving nothing to read. crawl()
    guards against this by snapshotting first and merging both passes; the probe
    must do the same or a healthy page can measure as empty.
    """
    try:
        bc1, _c1, _s1, ln1 = extract_all(page, opts)
        try:
            expand_tree(page)          # a collapsed menu hides links worth counting
            bc2, _c2, _s2, ln2 = extract_all(page, opts)
        except Exception:
            bc2, ln2 = [], []
        breadcrumb = bc1 or bc2        # first non-empty trail wins
        links = list(_merge_links({}, ln1 + ln2).values())

        # A page with NO links is a failed measurement, not evidence. SBP
        # intermittently serves an empty page; reading that as "no clear signal"
        # returns 'host', which would send the crawl across the whole domain —
        # the most damaging possible answer to get from a blank page.
        if not links:
            fallback = "prefix" if urlparse(seed_url).path.strip("/") else "host"
            return fallback, (f"seed page yielded no links (site flaky or not "
                              f"rendered) — defaulting to {fallback}, not guessing")

        return detect_scope(breadcrumb, links, seed_url, seed_host)
    except Exception as e:
        # Prefer the safer default: a wrong 'prefix' collects too little (visible,
        # fixable); a wrong 'host' can wander an entire domain.
        fallback = "prefix" if urlparse(seed_url).path.strip("/") else "host"
        return fallback, f"probe failed ({str(e)[:120]}) — defaulting to {fallback}"


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


def crawl(seed_url, out_dir, max_pages=150, max_depth=8, scope="auto",
          headless=True, wait_ms=700, nav_timeout=60000, strategy="auto",
          group_headings=False, list_details=True, max_details=None):
    out = Path(out_dir)
    (out / "html").mkdir(parents=True, exist_ok=True)

    seed_norm = normalize_url(seed_url)
    seed_host = urlparse(seed_norm).netloc.lower()
    # Per-host fixes. Hosts with no profile behave exactly as they did before.
    # (scope_prefix is defined at module level — see its docstring for why the
    #  seed's own filename must not end up in the prefix.)
    prof = profile_for(seed_norm)
    group_headings = group_headings or prof["group_headings"]
    seed_prefix = scope_prefix(urlparse(seed_norm).path)  # "under the seed path" for prefix scope
    # If the seed sits under a 2-3 letter language segment (/en/...), lock the crawl
    # to that language so we don't load every page's /ar/ mirror just to reject it.
    _seg0 = first_seg(urlparse(seed_norm).path)
    lang_lock = _seg0 if re.fullmatch(r"[a-z]{2,3}", _seg0 or "") else None

    visited = set()
    content_hashes = {}     # content_key -> url that first recorded it
    records = []
    documents = {}          # normalized doc url -> record
    chrome_documents = {}   # same, for links found only in the site header/footer
    link_titles = {}        # normalized url -> the anchor text that linked to it
    link_parents = {}       # normalized url -> the page it was linked from
    section_anchor = None   # set from the seed's breadcrumb (last item)

    # What the walk learned about itself. These were all emitted as events and
    # then forgotten — baseline.py re-parsed stdout to get them back — so nothing
    # downstream of the process could tell a clean run from a truncated one.
    note = {"blocked_pages": 0, "errors": 0, "retries": 0,
            "cap_hit": False, "seed_loaded": True, "stopped": "", "resume": {}}

    emit({"event": "start", "seed": seed_norm, "scope": scope,
          "max_pages": max_pages, "max_depth": max_depth})

    with sync_playwright() as pw:
        # --disable-dev-shm-usage: Chromium's default /dev/shm is small, and a
        # long crawl (SBP: 4,160 detail pages) exhausts it and takes the whole
        # browser down mid-run. Observed: dead after ~44 detail pages.
        browser = pw.chromium.launch(
            headless=headless,
            args=["--disable-dev-shm-usage", "--disable-gpu",
                  "--renderer-process-limit=2"])
        ctx = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
            locale="en-US",
        )
        page = ctx.new_page()

        # ---- shape-aware dispatch (additive) ---------------------------------
        # Load the seed once, detect its layout, and hand off to a specialised
        # strategy. Falls through to the existing BFS when shape == "generic".
        # ONE seed load answers BOTH questions: how far may I go (scope) and how
        # do I read this layout (shape). They are independent, and loading the
        # page twice to ask them separately would only risk them disagreeing.
        seed_loaded = False
        if strategy != "generic" or scope == "auto":
            # RETRY. Every other page in this crawler gets two attempts; the seed
            # used to get one — and the seed decides BOTH the scope and the shape
            # for the entire run. A single transient DNS blip (these sites give
            # them regularly) silently downgraded the whole crawl to
            # generic + default scope. An empty page counts as a failure too.
            for attempt in range(1, 4):
                try:
                    page.goto(seed_norm, wait_until="domcontentloaded", timeout=nav_timeout)
                    page.wait_for_timeout(wait_ms + 2500)
                    try: page.mouse.wheel(0, 2500); page.wait_for_timeout(600)
                    except Exception: pass
                    if page.evaluate("()=>document.querySelectorAll('a[href]').length") > 15:
                        seed_loaded = True
                        break
                    note["retries"] += 1
                    emit({"event": "retry", "url": seed_norm, "attempt": attempt,
                          "message": "seed rendered with no links"})
                except Exception as e:
                    note["retries"] += 1
                    emit({"event": "retry", "url": seed_norm, "attempt": attempt,
                          "message": str(e)[:160]})
                page.wait_for_timeout(2000)
            # A challenge page can satisfy the link check above, and a blocked
            # SEED means every decision below it — scope, shape, the whole walk —
            # was made against the WAF's page rather than the site's.
            reason = blocked_reason(page)
            if reason:
                note["blocked_pages"] += 1
                note["stopped"] = f"seed blocked by bot protection ({reason})"
                emit({"event": "blocked", "url": seed_norm, "reason": reason})
            if not seed_loaded:
                note["seed_loaded"] = False
                note["errors"] += 1
                # Not just a failed page: the seed decides scope AND shape for the
                # whole run, so everything below it was decided on defaults
                # (MERGE_LOG §13, robustness fix 1).
                note["stopped"] = note["stopped"] or (
                    "seed did not load after 3 attempts — scope and shape fell "
                    "back to defaults, so this walk may be of the wrong pages")
                emit({"event": "error", "url": seed_norm,
                      "message": "seed did not load after 3 attempts — scope and "
                                 "shape fall back to defaults"})

        # SHAPE BEFORE SCOPE — order matters and is not arbitrary.
        # probe_scope() calls expand_tree(), which CLICKS things, and on some
        # sites (SBP) a click tears the listing out of the DOM. Detecting shape
        # afterwards then sees a wrecked page: SBP's 26 `h4.mb-2` rows vanish and
        # it is misread as 'generic'. detect_shape() only reads (its child probe
        # opens a separate page), so it is safe to run first.
        shape = "generic"
        if strategy != "generic":
            shape = (strategy if strategy in ("tree", "table", "list")
                     else detect_shape(page, ctx))
            emit({"event": "shape", "requested": strategy, "detected": shape})

        if scope == "auto":
            if seed_loaded:
                scope, why = probe_scope(page, seed_norm, seed_host, prof)
            else:
                scope = "prefix" if seed_prefix.strip("/") else "host"
                why = f"seed did not load — defaulting to {scope}"
            emit({"event": "scope", "scope": scope, "reason": why})

        if strategy != "generic":
            if shape in ("tree", "table", "list"):
                if shape == "tree":
                    recs, docs, walked = crawl_tree(ctx, seed_norm, out, max_pages=max_pages,
                                                    max_depth=max_depth,
                                                    wait_ms=max(wait_ms, 2000))
                elif shape == "list":
                    # For this shape max_pages bounds LISTING pages, not
                    # documents: SBP's 139 listing pages hold 4,160 entries, so
                    # the default 150 covers it. list_details=False stops before
                    # the expensive detail pass (see crawl_list).
                    recs, docs, walked = crawl_list(ctx, seed_norm, out,
                                                    max_pages=max_pages,
                                                    wait_ms=wait_ms,
                                                    fetch_details=list_details,
                                                    max_details=max_details)
                else:
                    recs, docs, walked = crawl_table(ctx, seed_norm, out,
                                                     max_pages=max_pages * 50,
                                                     wait_ms=wait_ms)
                # The walker reports what it hit; the seed checks above already
                # put their own findings in `note`. Add, never overwrite.
                _merge_note(note, walked)
                # Write FIRST, close second. A dead browser can hang close(),
                # and losing a completed walk to a failed teardown is the worst
                # possible trade.
                result = _finish(out, seed_norm, recs, docs, [], shape=shape, note=note)
                _safe_close(browser)
                return result
            # shape == "generic": continue with the existing link-walk below.

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
                    note["retries"] += 1
                    emit({"event": "retry", "url": url, "attempt": attempt, "message": last_err})
                    page.wait_for_timeout(1500)
            if not nav_ok:
                note["errors"] += 1
                emit({"event": "error", "url": url, "depth": depth, "message": last_err})
                continue
            # Checked on every page, not just the seed: a WAF usually lets the
            # first few through and starts serving challenges once it has decided
            # we are a bot — which is exactly how SIMAH was tripped.
            reason = blocked_reason(page)
            if reason:
                note["blocked_pages"] += 1
                if note["blocked_pages"] <= 3:
                    emit({"event": "blocked", "url": url, "reason": reason})
                continue          # never record a challenge page as a page
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
                note["errors"] += 1
                emit({"event": "error", "url": url, "depth": depth, "message": str(e)[:200]})
                continue

            # Snapshot NOW, before redirect-prone delays. Some SPAs (SBP) render the
            # content then client-side-redirect away seconds later; capturing here keeps it.
            # Prefer the real document heading over the generic <title>/banner text.
            try:
                title = (page.evaluate(JS_DOC_TITLE) or "").strip()
            except Exception:
                title = ""
            if not title:
                title = (page.title() or "").strip()
            try:
                bc1, c1, st1, ln1 = extract_all(page, prof)
            except Exception:
                bc1, c1, st1, ln1 = [], {"html": "", "text": ""}, "", []
            # Then let the tree expand and snapshot again (tree sites reveal more links).
            try:
                page.wait_for_timeout(wait_ms)
                expand_tree(page)
                bc2, c2, st2, ln2 = extract_all(page, prof)
            except Exception:
                bc2, c2, st2, ln2 = [], {"html": "", "text": ""}, "", []
            # Merge: richer content wins; links are the union; first non-empty crumb/status.
            content = c1 if len(c1.get("text", "")) >= len(c2.get("text", "")) else c2
            breadcrumb = bc1 or bc2
            status = st1 or st2
            # Reveal links hidden behind in-page controls: "Show N entries"
            # menus, Next buttons, accordions, "Load more". Click-and-verify —
            # a click is kept only if the page actually gained links, and undone
            # if it navigated. Supersedes collect_paginated_links().
            try:
                ln3 = reveal_all_links(page, opts=prof)
            except Exception:
                ln3 = []
            # Merge the three passes (pre-expand, post-expand, paginated) with the
            # same rule extract_all uses: never let a later, poorer sighting of a
            # URL overwrite a richer earlier one.
            links = list(_merge_links({}, ln1 + ln2 + ln3).values())
            if not title:
                title = (page.title() or "").strip() or (breadcrumb[-1] if breadcrumb else "")

            # Structured section trail from tab panels / accordion containers.
            # Fires only where the markup provides it (measured: 89 links on MISA,
            # 0 on SECP/SBP/SAMA/SDAIA), so it is always on — it cannot affect a
            # site whose markup it does not match.
            nav_path_map = {}
            try:
                for item in (page.evaluate(JS_NAV_PATH) or []):
                    if item.get("nav_path"):
                        key = (normalize_url(item.get("href") or ""),
                               (item.get("text") or "").strip())
                        nav_path_map.setdefault(key, item["nav_path"])
            except Exception:
                pass

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
                if urlparse(href).scheme in ("http", "https") and is_document_link(href, seed_host):
                    dn = normalize_url(href)
                    rec_doc = {
                        "title": best_doc_title(l, dn),   # real title/date, not "Download"
                        "doc_url": dn,
                        "type": doc_type_of(href, seed_host),
                        "found_on": url,
                        "section_path": doc_section_path(
                            breadcrumb,
                            l.get("group") if group_headings else "",
                            nav_path_map.get(
                                (dn, (l.get("text") or "").strip()), ""),
                            l.get("heading_path") if group_headings else None),
                    }
                    # Site furniture (header/footer) is not this section's content:
                    # a "Privacy Policy" PDF or a site-wide guidebook banner would
                    # otherwise be filed under whatever page happened to show it.
                    # Dropped, but KEPT IN AN AUDIT LIST so nothing vanishes silently
                    # and the rule can be checked against a real crawl.
                    if l.get("chrome"):
                        chrome_documents.setdefault(dn, rec_doc)
                        continue
                    page_docs.append(dn)
                    # Keyed by (url, section_path), not url alone: regulators
                    # deliberately cross-list one document under several sections,
                    # and each listing is a separate place in the library. The DB
                    # agrees — document_exists_by_url(url, category) is
                    # category-scoped precisely for SAMA's cross-listed documents.
                    key = (dn, rec_doc["section_path"])
                    if key not in documents:
                        documents[key] = rec_doc

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
                # How we got here. The page's own <title> is often generic
                # ("Details", "Circulars"); the anchor that led to it usually
                # carries the real document name.
                "linked_from_title": link_titles.get(url, ""),
                "parent_page_url": link_parents.get(url, ""),
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
                if e in SKIP_EXTS or is_document_link(nh, seed_host):  # assets ignored; docs recorded above
                    continue
                if is_aggregator(nh, l.get("text", "")):  # never crawl entire-section/print pages
                    continue
                if lang_lock and first_seg(pu.path) != lang_lock:  # skip other-language mirrors
                    continue
                if DENY_PATH_PAT.search(pu.path) or pu.path in ("/", f"/{lang_lock}"):  # chrome pages
                    continue
                if scope == "prefix" and not pu.path.startswith(seed_prefix):
                    continue
                # Remember HOW we reached this page: the anchor text that linked to
                # it, and the page it was linked from. A detail page's own <title>
                # is often generic ("Details"), while the link that led to it
                # carries the real document name — so this is frequently the best
                # title we will ever have for it.
                link_titles.setdefault(nh, (l.get("text") or "").strip()[:300])
                link_parents.setdefault(nh, url)
                if l.get("nav"):
                    # Pagination / "Next" links: LOW priority (real rows crawled first)
                    # and they do NOT consume the depth budget, so a long pagination
                    # chain (e.g. SBP: 133 pages via repeated "Next") is never cut off
                    # by max_depth. Only max_pages bounds it.
                    visited.add(nh)
                    page_queue.append((nh, depth))
                elif depth < max_depth:
                    # Real content / detail page (a row): counts toward depth.
                    visited.add(nh)
                    queue.append((nh, depth + 1))

        # The cap stops the walk with work still queued. That is the difference
        # between "this is the site" and "this is 150 pages of the site", and
        # what is left in the queues is where a resumed run would start.
        left = len(queue) + len(page_queue)
        if len(records) >= max_pages and left:
            note["cap_hit"] = True
            note["stopped"] = (f"page cap: {len(records)} of max_pages={max_pages}, "
                               f"{left} URLs still queued")
            note["resume"] = {"pages_walked": len(records), "queued": left,
                              "next_urls": [u for u, _ in list(queue)[:5]]
                                           or [u for u, _ in list(page_queue)[:5]]}
            emit({"event": "cap", "pages": len(records), "queued": left})

        _safe_close(browser)

    # ---- write outputs ----
    # A document seen in the header/footer on one page but in real content on
    # another IS content — the content sighting wins. `documents` is keyed by
    # (url, section_path), so compare on the url part only.
    kept_urls = {k[0] for k in documents}
    dropped_chrome = [d for u, d in chrome_documents.items() if u not in kept_urls]

    return _finish(out, seed_norm, records, list(documents.values()),
                   dropped_chrome, shape="generic", note=note)


def _safe_close(browser):
    """Close the browser without letting it take the run down.

    A crashed Chromium does not just raise on close() — it can BLOCK, waiting for
    a process that is never going to answer. That hung the whole crawler after a
    successful walk, so results were computed and then never written. Give it a
    few seconds on a side thread and move on.
    """
    import threading
    done = threading.Event()

    def _close():
        try:
            browser.close()
        except Exception:
            pass
        finally:
            done.set()

    threading.Thread(target=_close, daemon=True).start()
    done.wait(10)


# THE OUTCOME, IN ONE WORD. `done` used to mean only "the walk reached _finish",
# and every consumer read that as success — main() never even set an exit code. A
# WAF page reaches _finish too, and its 1,054 characters clear the 200-char bar
# that makes a page a document.
#
#   ok          pages recorded, nothing blocked, nothing cut short
#   blocked     a page was a bot-protection wall; no count means anything
#   zero        no pages recorded — a failed extraction, not an empty site
#   incomplete  cap hit / seed never loaded / browser died / pages failed.
#               REPORTED, NOT FATAL (MERGE_LOG §13: keep the rows, log
#               INCOMPLETE). `stopped` and `resume` are what
#               resume-from-where-it-died will read; today that is thrown away.
#
# NO-DOCS stays in baseline_report.py: only it has the cross-site context to tell
# SAMA's real 3 files from a broken extraction, and it already flags it.
FATAL_STATUSES = ("blocked", "zero")


def _merge_note(note: dict, walked: dict) -> dict:
    """Fold a walker's findings into the run's note. Counters add; the first
    `stopped` wins, because the earliest thing that cut the walk short is the one
    a reader needs to act on."""
    for k in ("blocked_pages", "errors", "retries"):
        note[k] = note.get(k, 0) + (walked or {}).get(k, 0)
    note["stopped"] = note.get("stopped") or (walked or {}).get("stopped", "")
    note["resume"] = note.get("resume") or (walked or {}).get("resume") or {}
    return note


def run_status(counts: dict) -> str:
    """Classify a finished run. Pure function of the counters, so it is testable
    without a browser and cannot drift from what `done` reports.

    `errors` is counted but does not decide the status: a page that 404s is skipped
    by design, and if 1 bad page in 150 said INCOMPLETE the word would stop meaning
    anything. Only the walk being CUT SHORT flips it — cap, dead browser, or a seed
    that never arrived (which decides scope and shape for everything after it).
    """
    if counts.get("blocked_pages"):
        return "blocked"
    if not counts.get("pages"):
        return "zero"
    if (counts.get("cap_hit") or counts.get("stopped")
            or not counts.get("seed_loaded", True)):
        return "incomplete"
    return "ok"


def _finish(out, seed_norm, records, documents, chrome_dropped, shape, note=None):
    """Shared tail for every walker: normalise, hash, write, return.

    Every path through crawl() ends here, so tree/table/generic produce the same
    schema, files, return value — and now the same outcome vocabulary. `note`
    carries what the walk learned; absent, the run is judged on its counts alone.
    Returns (records, documents).
    """
    # Rewrite only titles that several different documents share (SDAIA's "2025").
    renamed = disambiguate_titles(documents)

    # content_hash: what change detection compares between runs. It is the hash of
    # the page's TEXT, not its HTML — HTML churns on every deploy (build ids,
    # cache-busting query strings) and would report every page as modified.
    for r in records:
        r.setdefault("html", "")
        r.setdefault("breadcrumb", [])
        r.setdefault("linked_from_title", "")
        r.setdefault("parent_page_url", "")
        r["content_hash"] = content_key(r.get("text") or "")
    for d in documents:
        # A document is a file we have not downloaded here, so hash what identifies
        # it: its URL plus its title. Real content hashing happens when the
        # pipeline fetches the file.
        d["content_hash"] = content_key(
            f"{d.get('doc_url','')}|{d.get('title','')}")

    n = dict(note or {})
    counts = {"pages": len(records), "documents": len(documents),
              "blocked_pages": n.get("blocked_pages", 0),
              "errors": n.get("errors", 0), "retries": n.get("retries", 0),
              "cap_hit": bool(n.get("cap_hit")),
              "seed_loaded": n.get("seed_loaded", True),
              "stopped": n.get("stopped", "")}
    status = run_status(counts)

    # `status` travels in pages.json as well as the event: the file outlives the
    # stdout stream, and whoever reads it later must be able to tell a crawl from
    # a challenge page. Additive — every existing key is untouched. `pages` and
    # `documents` stay the LISTS they have always been, so the counters go in
    # under their own names.
    outcome = {f"n_{k}" if k in ("pages", "documents") else k: v
               for k, v in counts.items()}
    (out / "pages.json").write_text(
        json.dumps({"seed": seed_norm, "shape": shape, "status": status,
                    **outcome, "resume": n.get("resume") or {},
                    "pages": records,
                    "documents": documents, "chrome_dropped": chrome_dropped},
                   ensure_ascii=False, indent=2),
        encoding="utf-8")
    _write_excel(out / "pages.xlsx", records, documents, chrome_dropped)

    emit({"event": "done", "status": status, "shape": shape, **counts,
          "resume": n.get("resume") or {},
          "chrome_dropped": len(chrome_dropped),
          "titles_disambiguated": renamed,
          "out_dir": str(out), "xlsx": str(out / "pages.xlsx")})
    return records, documents


# ============================================================================
# SECTION E — write the Excel workbook + command-line entry point
# ============================================================================


def _write_excel(path, records, documents, chrome_dropped=None):
    import pandas as pd
    CELL_MAX = 32000
    page_rows = [{
        "section_path": r["section_path"],
        "title": r["title"],
        "url": r["url"],
        "depth": r["depth"],
        "linked_from_title": r.get("linked_from_title", ""),
        "parent_page_url": r.get("parent_page_url", ""),
        "status": r["status"],
        "n_pdfs": r["n_pdfs"],
        "pdf_links": r["pdf_links"][:CELL_MAX],
        "text_len": r["text_len"],
        "content_hash": r.get("content_hash", ""),
        "html_file": r["html_file"],
        "text_preview": (r["text"] or "")[:CELL_MAX],
    } for r in records]

    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        pd.DataFrame(page_rows).to_excel(xw, sheet_name="pages", index=False)
        if documents:
            pd.DataFrame(documents).to_excel(xw, sheet_name="documents", index=False)
        # Audit sheet: what the header/footer rule removed. If a real document
        # ever shows up here, the rule is wrong — check this before trusting a run.
        if chrome_dropped:
            pd.DataFrame(chrome_dropped).to_excel(
                xw, sheet_name="chrome_dropped", index=False)


def main():
    ap = argparse.ArgumentParser(description="Standalone Playwright sidebar crawler (test tool)")
    ap.add_argument("--url", required=True, help="Seed URL")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--max-pages", type=int, default=150)
    ap.add_argument("--max-depth", type=int, default=8)
    ap.add_argument("--scope", choices=["auto", "breadcrumb", "prefix", "host"],
                default="auto",
                help="auto = work it out from the landing page; or force one by hand")
    ap.add_argument("--strategy", choices=["auto", "tree", "table", "list", "generic"],
                    default="auto",
                    help="auto = detect layout (tree/table) and dispatch; else force one")
    ap.add_argument("--group-headings", action="store_true",
                    help="Append the on-page heading a document sits under to its "
                         "section_path (aml.gov.sa 'Laws and Regulations'). Off by "
                         "default: on other sites those headings are dates or titles.")
    ap.add_argument("--headful", action="store_true", help="Show the browser window")
    ap.add_argument("--wait-ms", type=int, default=700, help="Settle wait after each page")
    ap.add_argument("--no-details", action="store_true",
                    help="list shape: walk the listing only, skip detail pages "
                         "(cheap full inventory — use for change detection)")
    ap.add_argument("--max-details", type=int, default=0,
                    help="list shape: cap how many detail pages are opened")
    args = ap.parse_args()

    crawl(args.url, args.out, max_pages=args.max_pages, max_depth=args.max_depth,
          list_details=not args.no_details,
          max_details=args.max_details or None,
          scope=args.scope, headless=not args.headful, wait_ms=args.wait_ms,
          strategy=args.strategy, group_headings=args.group_headings)

    # The status is authoritative, and `pages.json` is where it survives the
    # process. A blocked or empty crawl exits non-zero so a caller that reads
    # nothing else still cannot mistake it for a crawl; INCOMPLETE exits 0 by
    # design — the rows are worth keeping, they just are not the whole site.
    return _report_outcome(Path(args.out))


def _report_outcome(out: Path) -> int:
    """Print the outcome and turn it into an exit code. Reads pages.json rather
    than a return value so it reports the same thing a later reader would see."""
    try:
        data = json.loads((out / "pages.json").read_text(encoding="utf-8"))
    except Exception as e:
        print(f"\nNO-RESULT  -  could not read {out / 'pages.json'}: {e}",
              file=sys.stderr)
        return 1

    status = data.get("status", "ok")
    line = (f"{data.get('n_pages', 0)} pages | {data.get('n_documents', 0)} documents"
            f" | {data.get('blocked_pages', 0)} blocked"
            f" | {data.get('errors', 0)} errors | {data.get('retries', 0)} retries")
    if status == "ok":
        print(f"\nOK  -  {line}\n  {out}")
        return 0

    w = sys.stderr
    print(f"\n{'=' * 70}\n{status.upper()}  -  {data.get('seed', '')}\n{'=' * 70}", file=w)
    print(f"  {line}", file=w)
    if data.get("stopped"):
        print(f"  stopped: {data['stopped']}", file=w)
    if data.get("resume"):
        print(f"  resume:  {json.dumps(data['resume'])}", file=w)
    print(f"  {_NEXT_STEP.get(status, '')}\n  read: {out / 'pages.json'}", file=w)
    return 1 if status in FATAL_STATUSES else 0


# Kept next to the exit code so the two cannot say different things. ASCII only:
# this text lands in scheduler logs and pipes, where a Windows console encoding
# turns a stray em-dash into a UnicodeEncodeError.
_NEXT_STEP = {
    "blocked": ("A bot-protection wall answered instead of the site. Nothing here can\n"
                "  be used. Slow the pacing, or give this source a hand-written\n"
                "  crawler. Do NOT re-run in a loop - that is what trips a WAF."),
    "zero": ("No pages were recorded, which is a failed extraction, not an empty\n"
             "  site. Check the scope and the shape first - calibrate_shape.py and\n"
             "  calibrate_scope.py both exit non-zero on a wrong answer."),
    "incomplete": ("Part of the site was not walked, so these counts are a floor, not a\n"
                   "  total. Read `stopped` above before comparing this run with any\n"
                   "  other, and never treat it as coverage."),
}


if __name__ == "__main__":
    sys.exit(main())
