"""
================================================================================
 Standalone Playwright crawler — regulator sites (MISA, MC, ZATCA)
================================================================================
"""

import argparse
import hashlib
import json
import re
import sys
from collections import deque
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

from playwright.sync_api import sync_playwright

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
# SECTION A — small helpers
# ============================================================================

DOC_EXTS = {".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".zip", ".ppt", ".pptx"}
SKIP_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico", ".css", ".js",
             ".woff", ".woff2", ".ttf", ".mp4", ".mp3", ".webp"}
TRACKING_PREFIXES = ("utm_",)
TRACKING_KEYS = {"fbclid", "gclid", "gclsrc", "dclid", "msclkid", "_ga", "refresh"}
EXTERNAL_LAW_PORTALS = {
    "boe.gov.sa", "laws.boe.gov.sa",
    "mc.gov.sa",
    "moj.gov.sa", "laws.moj.gov.sa",
    "pr.gov.sa",
    "zatca.gov.sa",
}


def is_external_law_portal(url: str, seed_host: str = "") -> bool:
    """True if a URL's host is a known Saudi government legal portal that hosts
    law text itself, rather than a document extension/path pattern.

    IMPORTANT: this must NOT fire for links that stay on the site we are
    currently crawling. EXTERNAL_LAW_PORTALS is for the case where some OTHER
    regulator's site links OUT to e.g. zatca.gov.sa as a cross-reference. If
    the seed itself IS zatca.gov.sa, every normal in-site navigation link
    would otherwise match this host and get wrongly treated as a terminal
    document instead of a page to crawl into -- which silently stops the
    crawl from ever going deeper than the first level of cards."""
    host = urlparse(url).netloc.lower()
    if seed_host and (host == seed_host or seed_host.endswith("." + host)
                      or host.endswith("." + seed_host)):
        return False
    return any(host == d or host.endswith("." + d) for d in EXTERNAL_LAW_PORTALS)


AGGREGATOR_URL_PAT = re.compile(r"/(entiresection|customprint|custom-print|printpdf|print)(/|$|\?|-)", re.I)
AGGREGATOR_TITLE_PAT = re.compile(r"^\s*(entire section|custom print|print\s*/\s*save)", re.I)


def is_aggregator(url: str, title: str = "") -> bool:
    return bool(AGGREGATOR_URL_PAT.search(url)) or bool(AGGREGATOR_TITLE_PAT.search(title or ""))


DENY_PATH_PAT = re.compile(
    r"/(search|login|sign-?in|register|contact|sitemap|rss|feed|revision-updates|"
    r"terms-and-conditions|privacy|cookie)s?(/|$|\?)", re.I)


def first_seg(path: str) -> str:
    segs = [s for s in path.split("/") if s]
    return segs[0] if segs else ""


def content_key(text: str) -> str:
    norm = re.sub(r"\s+", " ", (text or "")).strip().lower()
    return hashlib.md5(norm.encode("utf-8")).hexdigest() if norm else ""


def emit(event: dict):
    sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def normalize_url(url: str) -> str:
    try:
        p = urlparse(url)
    except Exception:
        return url
    if p.scheme not in ("http", "https"):
        return url
    query = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True)
             if not (k.lower() in TRACKING_KEYS or k.lower().startswith(TRACKING_PREFIXES))]
    path = re.sub(r"/{2,}", "/", p.path)
    path = path.rstrip("/") or "/"
    return urlunparse((p.scheme, p.netloc.lower(), path, "", urlencode(query), ""))


def ext_of(url: str) -> str:
    path = urlparse(url).path.lower()
    dot = path.rfind(".")
    return path[dot:] if dot != -1 else ""


def is_document_link(url: str, seed_host: str = "") -> bool:
    if ext_of(url) in DOC_EXTS:
        return True
    p = urlparse(url)
    q = p.query.lower()
    if "wpdmdl=" in q or "download" in q:
        return True
    segs = [s for s in p.path.lower().split("/") if s]
    if any(s in ("document", "documents", "download", "downloads") for s in segs):
        return True
    if is_external_law_portal(url, seed_host=seed_host):
        return True
    return False


def doc_type_of(url: str) -> str:
    e = ext_of(url).lstrip(".").upper()
    if e:
        return e
    if is_external_law_portal(url):
        return "EXTERNAL"
    return "DOC"


GENERIC_LINK_TEXT = {"", "download", "pdf", "download pdf", "view", "view details",
                     "click here", "read more", "open", "details", "more"}


def url_path_breadcrumb(url: str) -> list:
    """Fallback breadcrumb derived from the URL's own path segments, used
    ONLY when the page has no real on-page breadcrumb widget (confirmed:
    zatca.gov.sa's category pages -- JS_BREADCRUMB finds nothing there, so
    section_path was collapsing to just the page's own title with no
    category trail before it at all). Not as precise as a real breadcrumb,
    but the URL structure itself already encodes real hierarchy for free
    (/en/RulesRegulations/Taxes/Pages/customs-bussiness/export-pages/... ->
    RulesRegulations, Taxes, customs bussiness, export pages) -- skips the
    language-code segment, the 'Pages' segment, and the final filename
    (captured separately as the page title, so including it again here
    would duplicate it)."""
    segs = [s for s in urlparse(url).path.split("/") if s]
    cleaned = []
    for s in segs[:-1]:   # exclude the last segment -- that's the page's own file
        if re.fullmatch(r"[a-z]{2,3}", s, re.I):
            continue
        if s.lower() == "pages":
            continue
        cleaned.append(re.sub(r"[-_]+", " ", s).strip())
    return cleaned


def title_from_slug(url: str) -> str:
    segs = [s for s in urlparse(url).path.split("/") if s]
    slug = segs[-1] if segs else ""
    return re.sub(r"[-_]+", " ", slug).strip().title()[:180]


def best_doc_title(link: dict, url: str) -> str:
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
# ============================================================================

JS_BREADCRUMB = r"""
() => {
  const HIDDEN_RE = /(visually-hidden|sr-only|screen-reader-text|visuallyhidden)/i;
  const SUSPICIOUS_CONCAT_RE = /[a-z][A-Z]/;

  // SharePoint's out-of-box breadcrumb webpart (class="ms-breadcrumb",
  // usually paired with an id containing "ListSiteMapPath") -- a real,
  // standard SharePoint convention (confirmed: zatca.gov.sa), not invented
  // for this one site. Structured as DEEPLY NESTED <li><ul><li><ul>...,
  // each level's <ul> sitting INSIDE the previous level's <li>, rather than
  // as flat siblings. Reading .textContent on the outer container (the
  // generic path below) recursively grabs every level's text at once with
  // no separators, producing one squished string that trips the
  // "suspicious concatenation" guard even though this IS a genuine,
  // correctly-structured breadcrumb -- it's just nested differently than a
  // typical flat list. Tried FIRST, before the generic loop, since the
  // generic loop's own sanity check would otherwise reject this pattern
  // outright. Falls through to the generic logic below on any site that
  // doesn't use this specific SharePoint webpart.
  const spBreadcrumb = document.querySelector('ul.ms-breadcrumb, [class*="ms-breadcrumb" i]');
  if (spBreadcrumb) {
    const spParts = [];
    let node = spBreadcrumb;
    while (node) {
      const li = node.querySelector(':scope > li');
      if (!li) break;
      // This level's OWN label: its direct <a> (for a linked crumb) or
      // direct <span class="ms-breadcrumbCurrentNode"> (the final,
      // unlinked crumb) -- NOT text from any nested <ul> further down,
      // which is exactly what caused the squishing above.
      const label = li.querySelector(':scope > a, :scope > span.ms-breadcrumbCurrentNode');
      const t = label ? (label.textContent || '').replace(/\s+/g, ' ').trim() : '';
      if (t) spParts.push(t);
      node = li.querySelector(':scope > ul');
    }
    if (spParts.length) return spParts;
  }

  const sels = ['nav[aria-label*="readcrumb" i] ol', 'nav[aria-label*="readcrumb" i] ul',
                'ol.breadcrumb', 'ul.breadcrumb', '.breadcrumb', '[class*="rumb"]'];
  for (const s of sels) {
    const container = document.querySelector(s);
    if (!container) continue;
    let items = Array.from(container.children).filter(c => c.tagName === 'LI');
    if (!items.length) items = Array.from(container.children);
    const parts = [];
    for (const it of items) {
      if (HIDDEN_RE.test(it.className || '')) continue;
      let textSrc = it;
      if (it.querySelector && it.querySelector('[class*="visually-hidden" i], [class*="sr-only" i]')) {
        textSrc = it.cloneNode(true);
        textSrc.querySelectorAll('[class*="visually-hidden" i], [class*="sr-only" i]').forEach(n => n.remove());
      }
      const t = (textSrc.textContent || '').replace(/\s+/g, ' ').trim();
      if (t) parts.push(t);
    }
    if (parts.some(p => SUSPICIOUS_CONCAT_RE.test(p) || p.length > 60)) continue;
    const out = [];
    for (const p of parts) if (out[out.length - 1] !== p) out.push(p);
    if (out.length) return out;
  }
  return [];
}
"""

JS_MAIN_CONTENT = r"""
() => {
  const CHROME_EXACT_TOKENS = new Set(['header', 'footer', 'navbar', 'masthead',
                                        'topnav', 'site-nav', 'global-nav', 'nav-bar']);
  const CHROME_KNOWN_COMPOUNDS = new Set([
    'site-header', 'site-footer', 'page-footer', 'main-footer',
    'global-header', 'global-footer', 'sitewide-header', 'sitewide-footer'
  ]);
  function isChromeClassOrId(str) {
    if (!str) return false;
    const tokens = str.toString().trim().toLowerCase().split(/\s+/);
    return tokens.some(t => CHROME_EXACT_TOKENS.has(t) || CHROME_KNOWN_COMPOUNDS.has(t));
  }
  function isChrome(el) {
    if (!el.matches) return false;
    const role = (el.getAttribute('role') || '').toLowerCase();
    if (['banner', 'navigation', 'contentinfo'].includes(role)) return true;
    const cls = (el.className && el.className.toString) ? el.className.toString() : '';
    const id = el.id || '';
    return isChromeClassOrId(cls) || isChromeClassOrId(id);
  }
  const phMatches = Array.from(document.querySelectorAll('[id*="PlaceHolderMain" i]'));
  let anchor = null;
  if (phMatches.length) {
    let candidate = phMatches[0];
    while (candidate && !phMatches.every(m => candidate.contains(m))) {
      candidate = candidate.parentElement;
    }
    anchor = candidate;
  }
  const pick = anchor
    || document.querySelector('main, [role="main"], article, #content, .content, #main');
  const src = pick || document.body || document.documentElement;
  if (!src) return { html: '', text: '' };
  const clone = src.cloneNode(true);
  clone.querySelectorAll('script,style,noscript,form').forEach(n => n.remove());
  clone.querySelectorAll('nav, aside, [role="banner"], [role="navigation"], [role="contentinfo"]')
    .forEach(n => n.remove());
  Array.from(clone.querySelectorAll('div, section'))
    .filter(isChrome)
    .forEach(n => n.remove());

  // Strip the "Add comments" / CAPTCHA widget -- confirmed markup
  // (SharePoint BotDetect CAPTCHA): [id*="captcha" i], .LBD_CaptchaDiv, and
  // sibling Name/Email/Comment fields under one shared container. Widened
  // to 8 hops and a broader match ("container"/"row" in addition to
  // form-group/LBD_Captcha/formTtl) -- 4 hops only reached the captcha's
  // own immediate wrapper (e.g. the Verification field's row), not the
  // sibling Name/Email/Comment fields sitting in the same outer container.
  Array.from(clone.querySelectorAll('[id*="captcha" i], [class*="captcha" i]')).forEach(cap => {
    let target = cap, anc = cap, hops = 0;
    while (anc && hops < 8) {
      const cls = (anc.className && anc.className.toString) ? anc.className.toString() : '';
      if (/form-group|LBD_Captcha|formTtl|\bcontainer\b|\brow\b/i.test(cls)) target = anc;
      anc = anc.parentElement;
      hops++;
    }
    target.remove();
  });

  // Strip breadcrumb -- JS_BREADCRUMB already extracts this into its own
  // dedicated section_path field, so leaving it inside the main content too
  // is pure duplication. Reuses the SAME selector list as JS_BREADCRUMB
  // itself, so both stay consistent rather than drifting apart over time.
  clone.querySelectorAll(
    'nav[aria-label*="readcrumb" i], ol.breadcrumb, ul.breadcrumb, '
    + '.breadcrumb, [class*="rumb"]'
  ).forEach(n => n.remove());

  // Strip leftover search-widget wrappers. The actual search <input>/
  // <button> are already gone via the form-control strip above; this just
  // removes the surrounding label/container so no orphaned "Search
  // Regulations"-style text remains once its interactive controls are gone.
  clone.querySelectorAll('[class*="search-box" i], [class*="search-bar" i], '
    + '[class*="search-wrapper" i], [id*="searchbox" i]').forEach(n => n.remove());

  // Strip the star-rating widget and share buttons -- confirmed markup:
  // BOTH sit as siblings of .lastEdit ("Last Modified ...") inside the same
  // outer .shareWrapper, each under its own distinct, specific class name.
  // Deliberately targeting THESE two classes directly, NOT .shareWrapper
  // itself -- removing the wrapper would also remove .lastEdit, which is
  // meant to stay. Template-specific (like .pagetopfunctions-wrapper), not
  // a general third-party brand -- harmless on sites that don't use it.
  clone.querySelectorAll('.shareBtns, .rating.page-rating').forEach(n => n.remove());

  // Strip ReadSpeaker's "Listen" widget -- a well-known third-party text-
  // to-speech accessibility service (used across many government/
  // enterprise sites, not just this one), matched by its own standard
  // class/id conventions. General, like the reCAPTCHA/hCaptcha exclusions
  // elsewhere -- benefits any other site using the same service.
  clone.querySelectorAll('.rsbtn, .rs_skip, [id*="readspeaker" i]').forEach(n => n.remove());

  // Strip the page-top toolbar (Listen/favorite/print icons). This
  // container class is specific to this site's template, not general --
  // but its own name ("pagetopfunctions") is self-describing enough that
  // it's safe to include as a targeted extra: it simply won't match
  // anything on a site that doesn't use this exact template.
  clone.querySelectorAll('.pagetopfunctions-wrapper, [id="addfavorite"], '
    + '[id="print"]').forEach(n => n.remove());

  // Strip Bootstrap modals wholesale -- a general pattern, not tied to any
  // specific widget. Broadened to bare '.modal' -- confirmed: zatca.gov.sa's
  // #voiceModal has class="modal voice-modal" with NEITHER role="dialog"
  // NOR the .fade class, so the original narrower selector would have
  // missed it entirely. '.modal' alone is Bootstrap's own foundational
  // class for this component, reliably present regardless of which
  // accessory classes/attributes a given modal also happens to carry.
  clone.querySelectorAll('.modal').forEach(n => n.remove());

  // Strip the top header wrapper wholesale -- confirmed markup
  // (zatca.gov.sa): .header-wrapper contains the government-verification
  // "digital stamp" badge, language switcher, contact-us link, mobile-app
  // link, accessibility-toggle widget, and search panel, plus a nested
  // <header> tag. The nested <header> tag itself was already being caught
  // by the existing class/id chrome check (class="header" matches exactly),
  // but .header-wrapper itself was NOT ("header-wrapper" doesn't exactly
  // equal the token "header"), leaving every OTHER sibling inside the
  // wrapper -- the verification badge, search panel, etc -- uncaught.
  clone.querySelectorAll('.header-wrapper').forEach(n => n.remove());

  // Strip the cookie-consent banner -- confirmed markup (zatca.gov.sa):
  // #cookiesModal / .cookies-notification. Site furniture required by
  // privacy regulation, never the actual page content.
  clone.querySelectorAll('#cookiesModal, .cookies-notification').forEach(n => n.remove());

  // Strip ZATCA's own "Comments and Suggestions" widget -- confirmed
  // markup: #ctl00_ucAddComment_createCommentDiv (a SharePoint web-part
  // id). Different UI pattern than MC's inline CAPTCHA form (this one is a
  // simple Bootstrap-modal-trigger card), but the same underlying category
  // of non-content chrome. Matched by the "ucAddComment" web-part name
  // fragment rather than the exact id, so it still catches this even if
  // the numeric/generated prefix differs on another page.
  clone.querySelectorAll('[id*="ucAddComment" i]').forEach(n => n.remove());

  // Strip the "Was this page useful?" feedback widget -- confirmed markup
  // (zatca.gov.sa): #ctl00_ucAddFeedback_userRatingDiv. A genuinely
  // SEPARATE SharePoint web-part from ucAddComment above, not a variant of
  // it -- "ucAddFeedback" doesn't contain "ucAddComment" as a substring, so
  // the existing rule had no way to catch this one. Without this, the
  // yes/no prompt text and all its reason-checkbox labels ("It was
  // helpful", "The answers were relevant", etc.) would survive even after
  // the empty <input>/<label> form controls inside it are stripped, since
  // none of that surrounding text is wrapped in anything else we target.
  clone.querySelectorAll('[id*="ucAddFeedback" i]').forEach(n => n.remove());

  // Strip the site footer div -- confirmed markup (zatca.gov.sa):
  // #ctl00_ucFooter_zatcaFooterDiv (floating button menu, external-link
  // modal placeholder). Not wrapped in an actual <footer> tag, and its id
  // doesn't exactly equal the token "footer" either, so neither existing
  // chrome check would have caught it. Matched by the "ucFooter" web-part
  // name fragment, same reasoning as the comment-widget strip above.
  clone.querySelectorAll('[id*="ucFooter" i]').forEach(n => n.remove());

  // Strip all interactive form controls -- this file is going into a
  // library that only needs the actual regulation TEXT, never search boxes,
  // comment forms, or any other interactive control. Confirmed safe: real
  // downloadable content on this site is always a plain <a href> link (the
  // regapis download links, external portal links), never a <button> or
  // <input> -- so removing these broadly doesn't risk losing real content.
  clone.querySelectorAll('input, textarea, select, button').forEach(n => n.remove());

  // Strip share/social icons -- page furniture, never the actual content.
  clone.querySelectorAll(
    'a[href*="facebook.com/sharer" i], a[href*="twitter.com/intent" i], '
    + 'a[href*="linkedin.com/cws/share" i], a[id*="share_whatsapp" i]'
  ).forEach(n => n.remove());

  // Normalize hidden-state markers, but ONLY on real accordion/tab-panel
  // content -- NOT every hidden element on the page. An earlier version of
  // this un-hid ALL display:none elements indiscriminately, which also
  // revealed things that were meant to stay hidden (confirmed: a loading
  // spinner/overlay became visible in the saved file). role="tabpanel" +
  // .ui-accordion-content is confirmed, specific markup for the actual
  // content panels (Law chapters, Regulation articles) -- a generic loading
  // indicator is very unlikely to carry this specific ARIA role, since it's
  // reserved for real tab/accordion content, not transient UI feedback.
  clone.querySelectorAll('[role="tabpanel"][style], .ui-accordion-content[style]').forEach(el => {
    if (/display\s*:\s*none/i.test(el.getAttribute('style') || '')) {
      const cleaned = el.getAttribute('style').replace(/display\s*:\s*none\s*;?/ig, '');
      if (cleaned.trim()) el.setAttribute('style', cleaned);
      else el.removeAttribute('style');
    }
  });
  clone.querySelectorAll('[role="tabpanel"][aria-hidden="true"], .ui-accordion-content[aria-hidden="true"]')
    .forEach(el => el.removeAttribute('aria-hidden'));

  // Rewrite every link's href ATTRIBUTE to its resolved absolute form.
  // innerHTML (used just below) serializes the ATTRIBUTE as originally

  // written in the source -- NOT the auto-resolved .href PROPERTY. A site
  // with relative hrefs (confirmed: mc.gov.sa's regapis download links,
  // href="/regapis?...") would otherwise save that relative path verbatim
  // into html_file, which then silently breaks the moment the saved file is
  // opened from anywhere other than the live site itself (e.g. a local
  // file has no base URL to resolve against, so the browser falls back to
  // the filesystem -- confirmed: ERR_FILE_NOT_FOUND). This is general, not
  // MC-specific: any site with relative links benefits.
  clone.querySelectorAll('a[href]').forEach(a => {
    try { a.setAttribute('href', a.href); } catch (e) {}
  });

  // Same problem, same fix, for embedded media -- confirmed: zatca.gov.sa
  // Taxes pages embed inline <img src="/en/RulesRegulations/Taxes/
  // PublishingImages/EN%20(1).svg"> content images (plain relative src, NOT
  // a sprite-sheet <use> reference), which 404'd once the saved HTML was
  // opened outside the live site for the exact same reason as the href
  // case above. SKIP_EXTS has no bearing here -- that only governs whether
  // a LINKED file gets enqueued as a page to crawl, never how an
  // already-embedded <img>'s own src attribute gets serialized.
  clone.querySelectorAll('img[src]').forEach(img => {
    try { img.setAttribute('src', img.src); } catch (e) {}
  });
  clone.querySelectorAll('img[srcset]').forEach(img => {
    try {
      const resolved = (img.getAttribute('srcset') || '').split(',').map(part => {
        const bits = part.trim().split(/\s+/);
        if (!bits[0]) return part;
        try { bits[0] = new URL(bits[0], document.baseURI).href; } catch (e) {}
        return bits.join(' ');
      }).join(', ');
      img.setAttribute('srcset', resolved);
    } catch (e) {}
  });
  clone.querySelectorAll('source[src], video[src], audio[src]').forEach(el => {
    try { el.setAttribute('src', el.src); } catch (e) {}
  });

  return { html: clone.innerHTML, text: (clone.innerText || '').trim() };
}
"""

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
  const CHROME_EXACT_TOKENS = new Set(['header', 'footer', 'masthead']);
  const CHROME_KNOWN_COMPOUNDS = new Set([
    'site-header', 'site-footer', 'page-footer', 'main-footer',
    'global-header', 'global-footer', 'sitewide-header', 'sitewide-footer'
  ]);
  function isChromeClassOrId(str) {
    if (!str) return false;
    const tokens = str.toString().trim().toLowerCase().split(/\s+/);
    return tokens.some(t => CHROME_EXACT_TOKENS.has(t) || CHROME_KNOWN_COMPOUNDS.has(t));
  }
  const chromeEl = a.closest('header, footer, [role="banner"], [role="contentinfo"]');
  let chrome = !!chromeEl;
  if (!chrome) {
    let anc = a;
    for (let i = 0; i < 6 && anc; i++) {
      const cls = (anc.className && anc.className.toString) ? anc.className.toString() : '';
      const id = anc.id || '';
      if (isChromeClassOrId(cls) || isChromeClassOrId(id)) { chrome = true; break; }
      anc = anc.parentElement;
    }
  }
  let ctx = '', row = a;
  for (let i = 0; i < 5 && row; i++) {
    const tag = row.tagName || '';
    const cn = (row.className && row.className.toString ? row.className.toString() : '');
    // P added: catches the common "[descriptive sentence], click here"
    // pattern (confirmed: zatca.gov.sa) -- a plain prose paragraph wrapping
    // a generic-text link, which previously matched neither the tag-name
    // check (TR/LI/ARTICLE only) nor the class-name check (card/item/box/
    // publication/row/result), so ctx came back empty and best_doc_title()
    // fell all the way through to a mechanically-derived URL-slug guess
    // instead of using the real, human-written surrounding sentence.
    if (/^(TR|LI|ARTICLE|P)$/.test(tag) || /card|item|box|publication|row|result/i.test(cn)) {
      ctx = (row.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 250);
      break;
    }
    row = row.parentElement;
  }
  return { href: a.href, text: t.slice(0, 300), nav: nav, ctx: ctx, chrome: chrome };
})
"""

JS_STATUS = r"""
() => {
  const el = Array.from(document.querySelectorAll('*'))
    .find(n => /status\s*:/i.test(n.textContent || '') && n.children.length < 4);
  if (!el) return '';
  const m = (el.textContent || '').match(/status\s*:\s*([A-Za-z\- ]{2,40})/i);
  return m ? m[1].trim() : '';
}
"""

JS_NAV_PATH = """
() => {
    const results = [];
    const anchors = Array.from(document.querySelectorAll('a[href]'));
    const HEADING_SEL = 'h2,h3,h4,h5,h6';

    function cleanText(el) {
        if (!el) return '';
        return (el.textContent || '').replace(/\\s+/g, ' ').trim();
    }

    const SECTION_SELECTORS = ['.regulationContent', '[id$="Show"]',
                                '[class*="tab-panel" i]', '[class*="tabpanel" i]'];

    function closestSection(el) {
        for (const sel of SECTION_SELECTORS) {
            try { const found = el.closest(sel); if (found) return found; }
            catch (e) {}
        }
        return null;
    }

    function groupLabel(sectionEl) {
        if (!sectionEl) return '';
        return cleanText(sectionEl.querySelector(HEADING_SEL));
    }

    function categoryLabel(a, sectionEl) {
        const panel = a.closest('.showLawItems, [class*="banner" i], [class*="panel" i]');
        if (panel) {
            const t = cleanText(panel.querySelector(HEADING_SEL));
            if (t) return t;
        }
        const mobWrap = a.closest('[class*="MobItems" i], [class*="mob-items" i]');
        if (mobWrap) {
            let sib = mobWrap.previousElementSibling;
            while (sib) {
                if (sib.tagName === 'LI') {
                    const t = cleanText(sib);
                    if (t) return t;
                }
                sib = sib.previousElementSibling;
            }
        }
        if (sectionEl) {
            const headings = Array.from(sectionEl.querySelectorAll(HEADING_SEL));
            const before = headings.filter(h => {
                const pos = h.compareDocumentPosition(a);
                return !!(pos & Node.DOCUMENT_POSITION_FOLLOWING);
            });
            if (before.length) return cleanText(before[before.length - 1]);
        }
        return '';
    }

    for (const a of anchors) {
        if (!a.hasAttribute('href')) continue;
        const sectionEl = closestSection(a);
        const group = groupLabel(sectionEl);
        const category = categoryLabel(a, sectionEl);
        const parts = [group, category].filter(Boolean);
        results.push({
            href: a.href,
            text: cleanText(a),
            nav_path: parts.join(' > ')
        });
    }
    return results;
}
"""

# ============================================================================
# SECTION C — page actions
# ============================================================================


def extract_all(page):
    """Frame-aware extraction. Excludes frames that can't be real page content:
    Chrome's own network-error interstitial, or a known CAPTCHA/challenge
    widget's own iframe (confirmed: zatca.gov.sa's reCAPTCHA; mc.gov.sa's
    background SSO check failure page) -- both would otherwise be able to win
    the "richest frame" comparison below against real content that hasn't
    rendered yet.
    Returns (breadcrumb, content{html,text}, status, links)."""
    UNRELATED_FRAME_URL_MARKERS = (
        "recaptcha", "gstatic.com/recaptcha", "google.com/recaptcha",
        "hcaptcha.com", "challenges.cloudflare.com",
    )
    breadcrumb, status = [], ""
    best = {"html": "", "text": ""}
    links, seen = [], set()
    for fr in page.frames:
        try:
            frame_url = (fr.url or "").lower()
        except Exception:
            frame_url = ""
        if any(marker in frame_url for marker in UNRELATED_FRAME_URL_MARKERS):
            continue
        try:
            frame_is_error = fr.evaluate(
                "() => !!document.getElementById('main-frame-error')")
        except Exception:
            frame_is_error = False
        if frame_is_error:
            continue
        try:
            for l in fr.evaluate(JS_LINKS):
                h = l.get("href")
                key = (h, (l.get("text") or "").strip())
                if h and key not in seen:
                    seen.add(key)
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
    seen = {}
    UNRELATED_FRAME_URL_MARKERS = (
        "recaptcha", "gstatic.com/recaptcha", "google.com/recaptcha",
        "hcaptcha.com", "challenges.cloudflare.com",
    )

    def harvest():
        for fr in page.frames:
            try:
                frame_url = (fr.url or "").lower()
            except Exception:
                frame_url = ""
            if any(marker in frame_url for marker in UNRELATED_FRAME_URL_MARKERS):
                continue
            try:
                frame_is_error = fr.evaluate(
                    "() => !!document.getElementById('main-frame-error')")
            except Exception:
                frame_is_error = False
            if frame_is_error:
                continue
            try:
                for l in fr.evaluate(JS_LINKS):
                    h = l.get("href")
                    key = (h, (l.get("text") or "").strip())
                    if h and key not in seen:
                        seen[key] = l
            except Exception:
                pass

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

    harvest()

    NEXT_SELECTORS = [".paginate_button.next", "a.paginate_button.next", "li.next a",
                      "a.next", "[rel='next']", ".pagination .next a", "button.next",
                      # Bootstrap-style pagers that mark "next" via aria-label
                      # rather than a distinguishing class -- confirmed:
                      # zatca.gov.sa's numbered pager gives every item
                      # (prev/numbers/next) the identical "page-link" class,
                      # so only aria-label reliably tells them apart.
                      ".pagination a[aria-label='Next']", ".pagination a[aria-label*='next' i]",
                      "nav[aria-label*='pagination' i] a[aria-label*='next' i]"]
    for _ in range(max_clicks):
        before = len(seen)
        el = None
        for s in NEXT_SELECTORS:
            cand = page.query_selector(s)
            if cand:
                cls = (cand.get_attribute("class") or "").lower()
                aria = (cand.get_attribute("aria-disabled") or "").lower()
                try:
                    parent_disabled = bool(cand.evaluate(
                        "el => !!el.closest('.disabled, [aria-disabled=\"true\"]')"))
                except Exception:
                    parent_disabled = False
                if "disabled" in cls or aria == "true" or parent_disabled:
                    continue
                el = cand
                break
        if el is None:
            # Fallback for numbered Bootstrap-style pagination with NO
            # distinct "next" control at all -- confirmed: zatca.gov.sa's
            # Taxes/Agreements listing pages use a plain <nav><ul
            # class="pagination"><li class="page-item">1/2/3/4</li>...</ul>
            # </nav> with every item (including any prev/next arrows)
            # sharing the exact same class, so no CSS selector can single
            # "next" out. Read the active page's number instead and click
            # whichever available page-link has the smallest higher number.
            el = _next_numbered_page_el(page)
        if el is None:
            break
        try:
            el.scroll_into_view_if_needed(timeout=800)
            el.click(timeout=1000)
            page.wait_for_timeout(800)
        except Exception:
            break
        harvest()
        if len(seen) == before:
            break
    return list(seen.values())


def _next_numbered_page_el(page):
    """Fallback for numbered Bootstrap-style pagination (.pagination
    .page-item > .page-link) where prev/numbers/next all share identical
    classes, so no selector reliably identifies "next" alone (confirmed:
    zatca.gov.sa). Reads the currently active page's number and returns the
    element for the smallest available number greater than it, or None if
    there isn't one (i.e. already on the last page)."""
    try:
        items = page.query_selector_all(".pagination .page-item")
    except Exception:
        return None
    if not items:
        return None
    active_num = None
    candidates = []
    for li in items:
        try:
            cls = (li.get_attribute("class") or "").lower()
            link = li.query_selector("a.page-link, .page-link")
            if not link:
                continue
            text = (link.inner_text() or "").strip()
            if not text.isdigit():
                continue
            num = int(text)
            if "active" in cls:
                active_num = num
            elif "disabled" not in cls:
                candidates.append((num, link))
        except Exception:
            continue
    if active_num is None or not candidates:
        return None
    higher = [c for c in candidates if c[0] > active_num]
    if not higher:
        return None
    higher.sort(key=lambda c: c[0])
    return higher[0][1]


def expand_tree(page, max_rounds=40):
    """Best-effort: repeatedly click collapsed expanders so child links appear.
    Generic (aria-expanded + common toggle classes). No-ops harmlessly if the
    site doesn't use them.

    700ms between rounds, not 250ms -- some sites (confirmed: mc.gov.sa) fetch
    a section's content asynchronously on click rather than just toggling CSS
    visibility on already-present content. Too short a wait risks the next
    round's re-query running before that content has actually landed."""
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
        page.wait_for_timeout(700)
    return total


# ============================================================================
# SECTION D — crawl()
# ============================================================================


def crawl(seed_url, out_dir, max_pages=150, max_depth=8, scope="breadcrumb",
          headless=True, wait_ms=700, nav_timeout=60000, prefix_root=None):
    out = Path(out_dir)
    (out / "html").mkdir(parents=True, exist_ok=True)

    seed_norm = normalize_url(seed_url)
    seed_host = urlparse(seed_norm).netloc.lower()
    if prefix_root is not None:
        seed_prefixes = [p.strip().rstrip("/") for p in prefix_root.split(",") if p.strip()]
    else:
        seed_prefixes = [urlparse(seed_norm).path.rstrip("/")]
    _seg0 = first_seg(urlparse(seed_norm).path)
    lang_lock = _seg0 if re.fullmatch(r"[a-z]{2,3}", _seg0 or "") else None

    visited = set()
    content_hashes = {}
    records = []
    documents = {}
    link_titles = {}
    link_parents = {}
    section_anchor = None
    used_slugs = set()

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

        queue = deque([(seed_norm, 0)])
        page_queue = deque()
        visited.add(seed_norm)

        while (queue or page_queue) and len(records) < max_pages:
            url, depth = queue.popleft() if queue else page_queue.popleft()
            nav_ok = False
            last_err = ""
            for attempt in range(1, 3):
                try:
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
                try:
                    for _ in range(3):
                        page.mouse.wheel(0, 6000)
                        page.wait_for_timeout(300)
                    page.evaluate("window.scrollTo(0, 0)")
                except Exception:
                    pass
                try:
                    page.wait_for_function(
                        "() => document.querySelectorAll('a[href]').length > 15 "
                        "|| (document.body && document.body.innerText.trim().length > 500)",
                        timeout=10000)
                except Exception:
                    pass
                page.wait_for_timeout(5000)
            except Exception as e:
                emit({"event": "error", "url": url, "depth": depth, "message": str(e)[:200]})
                continue

            title = (page.title() or "").strip()
            try:
                bc1, c1, st1, ln1 = extract_all(page)
            except Exception:
                bc1, c1, st1, ln1 = [], {"html": "", "text": ""}, "", []
            try:
                page.wait_for_timeout(wait_ms)
                expand_tree(page)
                bc2, c2, st2, ln2 = extract_all(page)
            except Exception:
                bc2, c2, st2, ln2 = [], {"html": "", "text": ""}, "", []
            content = c1 if len(c1.get("text", "")) >= len(c2.get("text", "")) else c2
            breadcrumb = bc1 or bc2
            if not breadcrumb:
                # No real on-page breadcrumb found (confirmed: zatca.gov.sa's
                # category pages) -- fall back to deriving one from the URL's
                # own path structure, rather than leaving section_path with
                # no category trail at all. Gated behind "if not breadcrumb"
                # -- on sites where the real breadcrumb already works, this
                # never executes at all.
                breadcrumb = url_path_breadcrumb(url)
            status = st1 or st2
            try:
                ln3 = collect_paginated_links(page)
            except Exception:
                ln3 = []

            _seenh = set()
            links = []
            for l in (ln1 + ln2 + ln3):
                h = l.get("href")
                if not h:
                    continue
                key = (h, (l.get("text") or "").strip())
                if key not in _seenh:
                    _seenh.add(key)
                    links.append(l)

            if not title:
                title = (page.title() or "").strip()
            try:
                nav_path_raw = page.evaluate(JS_NAV_PATH)
            except Exception:
                nav_path_raw = []
            nav_path_map = {}
            for item in nav_path_raw:
                full_url = urljoin(url, item["href"])
                nh = normalize_url(full_url)
                text_key = (item.get("text") or "").strip()
                if item["nav_path"]:
                    nav_path_map[(nh, text_key)] = item["nav_path"]

            if section_anchor is None:
                section_anchor = (breadcrumb[-1] if breadcrumb else title).strip().lower()
                emit({"event": "anchor", "section_anchor": section_anchor})

            in_scope = True
            crumb_l = [re.sub(r"\s+", " ", c).strip().lower() for c in breadcrumb]
            if scope == "breadcrumb":
                anchor = re.sub(r"\s+", " ", section_anchor or "").strip()
                in_scope = bool(anchor) and anchor in crumb_l
                if url == seed_norm:
                    in_scope = True
            elif scope == "prefix":
                in_scope = any(urlparse(url).path.startswith(p) for p in seed_prefixes)
            elif scope == "host":
                in_scope = True

            page_docs = []
            for l in links:
                href = l["href"]
                if l.get("chrome"):
                    continue
                if urlparse(href).scheme in ("http", "https") and is_document_link(href, seed_host=seed_host):
                    dn = normalize_url(href)
                    page_docs.append(dn)
                    text_key = (l.get("text") or "").strip()
                    nav = nav_path_map.get((dn, text_key), "")
                    sp = " > ".join(breadcrumb)
                    parts = [p for p in (sp, title, nav) if p]
                    section_path = " > ".join(parts)
                    dedup_key = (dn, section_path)
                    if dedup_key not in documents:
                        documents[dedup_key] = {
                            "title": best_doc_title(l, dn),
                            "doc_url": dn,
                            "type": doc_type_of(href),
                            "found_on": url,
                            "section_path": section_path,
                        }

            if not in_scope:
                emit({"event": "skip", "url": url, "depth": depth,
                      "reason": "out-of-scope", "breadcrumb": breadcrumb})
                continue

            if is_aggregator(url, title):
                emit({"event": "skip", "url": url, "depth": depth, "reason": "aggregator"})
                continue

            ckey = content_key(content["text"])
            if ckey and ckey in content_hashes:
                emit({"event": "skip", "url": url, "depth": depth,
                      "reason": "duplicate-content", "same_as": content_hashes[ckey]})
                continue
            if ckey:
                content_hashes[ckey] = url

            slug_base = slugify(urlparse(url).path or title) or f"page-{len(records)}"
            slug = slug_base
            if slug in used_slugs:
                suffix = hashlib.md5(url.encode()).hexdigest()[:8]
                slug = f"{slug_base}-{suffix}"
            used_slugs.add(slug)
            html_file = f"html/{slug}.html"
            (out / html_file).write_text(content["html"], encoding="utf-8")

            rec = {
                "section_path": " > ".join(breadcrumb),
                "title": title,
                "url": url,
                "depth": depth,
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
            if rec["linked_from_title"] and depth > 0 and rec["text_len"] > 1000:
                internal_key = (url, rec["section_path"])
                if internal_key not in documents:
                    documents[internal_key] = {
                        "title": rec["linked_from_title"],
                        "doc_url": url,
                        "type": "INTERNAL_PAGE",
                        "found_on": rec["parent_page_url"],
                        "section_path": rec["section_path"],
                    }
            emit({"event": "visit", "url": url, "depth": depth, "title": title,
                  "section_path": rec["section_path"], "n_pdfs": len(page_docs),
                  "text_len": rec["text_len"], "recorded": len(records),
                  "queued": len(queue), "page_queued": len(page_queue)})

            if depth < max_depth:
                for l in links:
                    href = l["href"]
                    nh = normalize_url(href)
                    if nh in visited:
                        continue
                    pu = urlparse(nh)
                    if pu.scheme not in ("http", "https"):
                        continue
                    if pu.netloc.lower() != seed_host:
                        continue
                    e = ext_of(nh)
                    if e in SKIP_EXTS or is_document_link(nh, seed_host=seed_host):
                        continue
                    if is_aggregator(nh, l.get("text", "")):
                        continue
                    if lang_lock and first_seg(pu.path) != lang_lock:
                        continue
                    if DENY_PATH_PAT.search(pu.path) or pu.path in ("/", f"/{lang_lock}"):
                        continue
                    if l.get("chrome"):
                        continue
                    if scope == "prefix" and not any(pu.path.startswith(p) for p in seed_prefixes):
                        continue
                    link_titles[nh] = (l.get("text") or "").strip()
                    link_parents[nh] = url
                    visited.add(nh)
                    if l.get("nav"):
                        page_queue.append((nh, depth + 1))
                    else:
                        queue.append((nh, depth + 1))

        browser.close()

    doc_list = list(documents.values())
    (out / "pages.json").write_text(
        json.dumps({"seed": seed_norm, "pages": records, "documents": doc_list},
                   ensure_ascii=False, indent=2),
        encoding="utf-8")
    _write_excel(out / "pages.xlsx", records, doc_list)
    emit({"event": "done", "pages": len(records), "documents": len(doc_list),
          "out_dir": str(out), "xlsx": str(out / "pages.xlsx")})
    return records


# ============================================================================
# SECTION E — Excel writer + CLI
# ============================================================================


def _write_excel(path, records, documents):
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
        "html_file": r["html_file"],
        "text_preview": (r["text"] or "")[:CELL_MAX],
    } for r in records]

    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        pd.DataFrame(page_rows).to_excel(xw, sheet_name="pages", index=False)
        if documents:
            pd.DataFrame(documents).to_excel(xw, sheet_name="documents", index=False)


def main():
    ap = argparse.ArgumentParser(description="Standalone Playwright crawler (ASPX sites)")
    ap.add_argument("--url", required=True, help="Seed URL")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--max-pages", type=int, default=150)
    ap.add_argument("--max-depth", type=int, default=8)
    ap.add_argument("--scope", choices=["breadcrumb", "prefix", "host"], default="breadcrumb")
    ap.add_argument("--prefix-root", default=None,
        help="Path prefix for scope=prefix. Comma-separated for multiple "
             "roots, e.g. /en/RulesRegulations/,/en/E-Invoicing/ -- use this "
             "when legitimate content spans more than one top-level section.")
    ap.add_argument("--headful", action="store_true", help="Show the browser window")
    ap.add_argument("--wait-ms", type=int, default=700, help="Settle wait after each page")
    args = ap.parse_args()

    crawl(args.url, args.out, max_pages=args.max_pages, max_depth=args.max_depth,
          scope=args.scope, headless=not args.headful, wait_ms=args.wait_ms,
          prefix_root=args.prefix_root)


if __name__ == "__main__":
    main()
