"""THE EXECUTOR — our code, driven by the form. No LLM anywhere in this file.

This module deliberately does not import `propose.py`, and must not grow an
import of it. The whole safety argument rests on the crawl being deterministic:
a hints file is read from disk, and the same file produces the same walk every
time. Ask a model afresh on each run and two runs can pick different rows — then
change detection reports hundreds of documents as "disappeared" and the
monitoring you built the library for is worthless.

TWO PHASES, separable on purpose (same split as generic_crawler's list strategy):

  PHASE 1  the listing only. Harvest each row's link plus the fields the form
           names — reference number, date, department. On SBP that is 139 pages
           and ~20 minutes for a COMPLETE inventory of 4,160 circulars, and it
           produces structured metadata a link-walk can never recover.

  PHASE 2  open each entry for its HTML. On SBP that is 4,160 more page loads.

Phase 1 alone answers "what is new since last time" — the listing IS the change
feed — so phase 2 only ever has to run for rows that are actually new.

OUTPUT SCHEMA
    Identical to generic_crawler's `pages.json` (records + documents), so a run
    from here is directly comparable with a run from there, and the orchestrator
    adapter cannot tell which one produced it.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright

from dynamic_crawler.formfill.schema import compile_field_regexes
from generic_crawler.crawler import (GENERIC_LINK_TEXT, disambiguate_titles,
                                     title_from_slug)

# "Press Here" is GOSI's; the rest come from the shared list.
_GENERIC_TEXT = GENERIC_LINK_TEXT | {"press here", "here", "click", "link"}

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

_DOC_EXT_RE = re.compile(r"\.(pdf|docx?|xlsx?|pptx?|csv|zip)(\?|$)", re.I)

# A WAF challenge page is HTML, returns 200, and has a plausible amount of text.
# Nothing about it looks like a failure — SIMAH's Cloudflare block was stored as
# 1,054 characters of "law", passed the fill-rate check, and would have been
# indexed into the library as the Credit Information Law. Any regulator can put a
# WAF in front of their site, so this is checked on every page.
_BLOCK_RE = re.compile(
    r"(you have been blocked|attention required|cf-error-details|just a moment"
    r"|checking your browser|access denied|error 10\d\d|ddos protection"
    r"|请稍候|captcha-bypass|verify you are (a )?human)", re.I)


def _blocked(page) -> str:
    """Return a reason string when the page is a bot-protection wall, else ''."""
    try:
        probe = page.evaluate(
            "()=>({t: document.title || '',"
            "      x: (document.body ? document.body.innerText : '').slice(0, 3000)})")
    except Exception:
        return ""
    m = _BLOCK_RE.search(f"{probe['t']}\n{probe['x']}")
    return m.group(0)[:60] if m else ""


def emit(event: dict) -> None:
    sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def content_key(text: str) -> str:
    """Same definition as generic_crawler.crawler.content_key, so hashes from the
    two engines are comparable."""
    norm = re.sub(r"\s+", " ", (text or "")).strip().lower()
    return hashlib.md5(norm.encode("utf-8")).hexdigest() if norm else ""


# --------------------------------------------------------------------------- #
# Extraction runs in the page: one evaluate per listing page, not one call per
# row. On a 30-row page that is the difference between 1 round trip and 150.
# --------------------------------------------------------------------------- #

JS_ROWS = r"""(cfg) => {
  const clean = s => (s || '').replace(/\s+/g, ' ').trim();
  // innerText returns "" for anything inside a hidden tab, because it reports
  // RENDERED text. MISA's "Basic Legislations" pane is hidden until clicked, so
  // reading innerText alone silently blanked every title and section name in it
  // — a wrong answer that looks like a legitimately empty field. textContent
  // does not care about rendering, so it is the fallback.
  const txt = el => clean(el.innerText || el.textContent);
  const out = [];
  // Rows are searched inside one panel when the form declares `panels`, so the
  // same selector yields a different row set per instrument on a tabbed page.
  let root = document;
  if (cfg.rootSel) {
    root = document.querySelector(cfg.rootSel);
    if (!root) return { rows: [], matched: 0 };
  }
  const rows = Array.from(root.querySelectorAll(cfg.rowSel));
  for (let ri = 0; ri < rows.length; ri++) {
    const row = rows[ri];
    // A row inside a panel has no URL of its own, so it is addressed by a stamp
    // and re-read from the same document in phase 2.
    let ffRow = null;
    if (cfg.stampBase !== null && cfg.stampBase !== undefined) {
      ffRow = cfg.stampBase + ri;
      row.setAttribute('data-ff-row', String(ffRow));
    }
    // The link that opens this entry.
    let a = null;
    if (cfg.linkSel) a = row.querySelector(cfg.linkSel) || (row.matches(cfg.linkSel) ? row : null);
    if (!a) a = row.matches('a[href]') ? row : row.querySelector('a[href]');

    // Where this row sits in the site's own hierarchy. Each level walks UP to
    // the nearest matching ancestor and reads a title inside it, so one listing
    // page can yield many different section paths — which is the point: the row
    // knows which sector block it lives in, the page as a whole does not.
    const section = [];
    for (const lv of (cfg.sectionLevels || [])) {
      // Two ways a page marks a group.
      //   ancestor+title — the rows sit INSIDE a block that names itself (MISA)
      //   preceding      — a heading sits BEFORE the rows as a sibling and the
      //                    rows are not wrapped at all (aml.gov.sa's two <h3>s,
      //                    and most SharePoint pages)
      if (lv.preceding) {
        let best = '';
        let hs = [];
        try { hs = Array.from(document.querySelectorAll(lv.preceding)); } catch (e) { hs = []; }
        for (const h of hs) {
          // 4 = DOCUMENT_POSITION_FOLLOWING: the row comes after this heading.
          if (h.compareDocumentPosition(row) & 4) best = txt(h);
        }
        section.push(best.slice(0, 120));
        continue;
      }
      let anc = null;
      try { anc = row.closest(lv.ancestor); } catch (e) { anc = null; }
      if (!anc) { section.push(''); continue; }
      let t = null;
      try { t = anc.querySelector(lv.title); } catch (e) { t = null; }
      section.push(t ? txt(t).slice(0, 120) : '');
    }
    // The panel is the outermost level: "…> OH benefits Regulation > SECTION VI".
    if (cfg.rootLabel) section.unshift(cfg.rootLabel);

    const fields = {};
    for (const f of cfg.cssFields) {
      let el = null;
      try { el = row.querySelector(f.selector) || (row.matches(f.selector) ? row : null); }
      catch (e) { el = null; }                 // an invalid selector yields nothing, never throws
      if (!el) { fields[f.target] = ''; continue; }
      if (f.attr === 'text')      fields[f.target] = txt(el);
      else if (f.attr === 'href') fields[f.target] = el.href || '';
      else if (f.attr === 'src')  fields[f.target] = el.src || '';
      else                        fields[f.target] = el.getAttribute(f.attr) || '';
    }
    out.push({
      row_text: txt(row).slice(0, 4000),
      href: a ? a.href : '',
      link_text: a ? txt(a).slice(0, 400) : '',
      section,
      ff_row: ffRow,
      fields
    });
  }
  return { rows: out, matched: rows.length };
}"""

# --------------------------------------------------------------------------- #
# PANELS — one URL holding several documents in fragment-addressed tabs
#
# A tab is <a href="#2">; the panel <div id="2"> is ALREADY in the DOM. Nothing
# navigates, so a link walk never follows one, and picking the longest
# main-content candidate keeps exactly one panel. GOSI's Social Insurance page is
# six separate legal instruments this way.
# --------------------------------------------------------------------------- #

JS_PANELS = r"""(sel) => {
  const clean = s => (s || '').replace(/\s+/g, ' ').trim();
  let tabs = [];
  try { tabs = Array.from(document.querySelectorAll(sel)); } catch (e) { return []; }
  const out = [];
  for (const t of tabs) {
    const a = t.matches('a[href]') ? t : t.querySelector('a[href]');
    if (!a) continue;
    const frag = a.getAttribute('href') || '';
    if (frag.length < 2 || frag[0] !== '#') continue;
    let panel = null;
    try { panel = document.getElementById(decodeURIComponent(frag.slice(1))); } catch (e) {}
    // Two tabs pointing at one panel would double every row inside it.
    if (!panel || panel.hasAttribute('data-ff-panel')) continue;
    const i = out.length;
    panel.setAttribute('data-ff-panel', String(i));
    out.push({
      index: i,
      fragment: frag,
      label: clean(a.innerText || a.textContent).slice(0, 300),
      // textContent, never innerText — see _check_panels in schema.py.
      text_len: clean(panel.textContent).length,
      links: panel.querySelectorAll('a[href]').length
    });
  }
  return out;
}"""

# Content of one stamped element, for rows that live inside the page rather than
# at a URL of their own. textContent for the same reason as JS_DETAIL's fallback:
# a collapsed accordion renders as its headings and nothing else.
JS_IN_PAGE_DETAIL = r"""(sel) => {
  const el = document.querySelector(sel);
  if (!el) return null;
  const clone = el.cloneNode(true);
  clone.querySelectorAll('script,style,noscript').forEach(n => n.remove());
  // `ctx` is the nearest heading above the link. A PDF inside a panel is usually
  // anchored by "Press Here", and the heading is the document's real name.
  const ctxOf = a => {
    let n = a;
    while (n && n !== el) {
      let s = n.previousElementSibling;
      while (s) {
        if (/^H[1-6]$/.test(s.tagName)) return (s.textContent || '').replace(/\s+/g,' ').trim();
        s = s.previousElementSibling;
      }
      n = n.parentElement;
    }
    return '';
  };
  const links = Array.from(el.querySelectorAll('a[href]'))
    .map(a => ({href: a.href,
                text: (a.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 200),
                ctx: ctxOf(a).slice(0, 200)}));
  return {html: clone.innerHTML, text: (clone.textContent || '').trim(), links};
}"""

JS_DETAIL = r"""() => {
  // querySelector with a comma list returns the first match in DOCUMENT ORDER, not
  // the first selector that matches: hrsd.gov.sa's stray <div class="content"> (7
  // chars) beat <main class="main-content"> (3,639). Take the LONGEST candidate;
  // empty ones are skipped so a page with no container still falls back to <body>.
  // Platform conventions belong here, the same way [id^="book-navigation"] encodes
  // Drupal: DeltaPlaceHolderMain is SharePoint's main-content placeholder. On
  // SIMAH it holds the law and nothing else (9,016 chars) while the next container
  // up adds the ribbon, "Sign In" and the footer (29,555).
  const SELS = ['main', '[role="main"]', 'article', '#content', '.content', '#main',
                '[id^="DeltaPlaceHolderMain"]', '.article-content'];
  let pick = null, best = 0;
  for (const sel of SELS) {
    for (const el of document.querySelectorAll(sel)) {
      const n = (el.innerText || '').trim().length;
      if (n > best) { best = n; pick = el; }
    }
  }
  // SECOND PASS, only when nothing scored: innerText reports RENDERED text, so a
  // page whose content sits in collapsed panels scores zero everywhere. SIMAH's law
  // is 17 articles in an EXCLUSIVE bootstrap accordion (data-bs-parent), so at most
  // one can be open at a time and clicking cannot defeat that — innerText sees one
  // article of seventeen. textContent does not care about rendering. Kept as a
  // fallback rather than the default so that on a normal page the visible main
  // content still wins, exactly as before.
  if (!pick) {
    for (const sel of SELS) {
      for (const el of document.querySelectorAll(sel)) {
        const n = (el.textContent || '').trim().length;
        if (n > best) { best = n; pick = el; }
      }
    }
  }
  const src = pick || document.body || document.documentElement;
  if (!src) return {html:'', text:'', links:[]};
  const clone = src.cloneNode(true);
  clone.querySelectorAll('script,style,noscript,nav,aside,header,footer').forEach(n=>n.remove());
  // <form> is UNWRAPPED, not removed. SharePoint / ASP.NET WebForms wrap the entire
  // page in <form id="aspnetForm">, so removing forms deleted SIMAH's whole law:
  // 8,182 characters of articles became 0, with a 444-character husk of markup left
  // behind and every other check still passing. Unwrapping drops the form semantics
  // and keeps the content.
  clone.querySelectorAll('form').forEach(f => {
    while (f.firstChild) f.parentNode.insertBefore(f.firstChild, f);
    f.remove();
  });
  const links = Array.from(src.querySelectorAll('a[href]'))
    .filter(a => !a.closest('header, footer, nav, [role="banner"], [role="contentinfo"]'))
    .map(a => ({href:a.href, text:(a.innerText||'').replace(/\s+/g,' ').trim().slice(0,200)}));
  // The clone is detached, so it has no layout and innerText is unreliable on it —
  // another reason textContent has to be the fallback here.
  return {html: clone.innerHTML,
          text:(clone.innerText || clone.textContent || '').trim(), links};
}"""

JS_HREFS = "() => Array.from(document.querySelectorAll('a[href]')).map(a => a.href)"

JS_CRUMBS = r"""(sel) => {
  const clean = s => (s || '').replace(/\s+/g, ' ').trim();
  let els = [];
  try { els = Array.from(document.querySelectorAll(sel)); } catch (e) { return []; }
  return els.map(e => clean(e.innerText || e.textContent)).filter(Boolean);
}"""

# --------------------------------------------------------------------------- #
# TREE — a nested menu instead of a paginated listing
#
# The menu IS the index: every node is one document page, and the nesting is the
# section path. So a tree reuses everything the list path already does — the
# nodes become the rows, phase 2 opens them exactly as it opens list entries, and
# the same verify gate applies. Only the way rows are DISCOVERED differs.
# --------------------------------------------------------------------------- #

JS_EXPAND = r"""(sel) => {
  let clicked = 0;
  for (const el of document.querySelectorAll(sel)) {
    try { el.click(); clicked++; } catch (e) {}
  }
  return clicked;
}"""

# --- click pagination -------------------------------------------------------
# A JS pager offers no URL to plan with (MHRSD: <a href="#">, and ?page=N there
# silently returns page 1). So it is walked by clicking, under the same rule as
# generic_crawler's reveal_all_links(): CLICK AND VERIFY — a click counts as a
# page turn only if the ROW SET changed. Without that, a dead control re-reads
# page 1 forever and the run reports a clean multiple of the real count.

# Fingerprint of the row set: count + each row's link. Cheap to poll, and page 2
# cannot look like page 1.
JS_ROW_SIG = r"""(a) => {
  const rows = Array.from(document.querySelectorAll(a.rowSel));
  return rows.length + '|' + rows.map(r => {
    const l = r.querySelector(a.linkSel);
    return (l && l.getAttribute('href')) || (r.innerText || '').slice(0, 40);
  }).join('~');
}"""

# Some pagers keep "next" on the last page, hidden or disabled — existing is not
# enough. el.click() rather than a pointer click: the pager sits below the fold
# and these sites float chat widgets over it.
JS_CLICK_NEXT = r"""(sel) => {
  const usable = e => {
    const r = e.getBoundingClientRect();
    if (r.width === 0 && r.height === 0) return false;
    const st = getComputedStyle(e);
    if (st.display === 'none' || st.visibility === 'hidden') return false;
    if (e.disabled || e.getAttribute('aria-disabled') === 'true') return false;
    return !/\bdisabled\b/.test(e.className || '');
  };
  const el = Array.from(document.querySelectorAll(sel)).find(usable);
  if (!el) return 'no-control';
  try { el.scrollIntoView({block: 'center'}); } catch (e) {}
  try { el.click(); } catch (e) { return 'click-failed'; }
  return 'clicked';
}"""

CLICK_SETTLE_MS = 8000    # how long a click gets to change the rows
CLICK_POLL_MS = 250

JS_TREE = r"""(cfg) => {
  const clean = s => (s || '').replace(/\s+/g, ' ').trim();
  const txt = el => clean(el.innerText || el.textContent);
  const out = [];
  for (const menu of document.querySelectorAll(cfg.menuSel)) {
    for (const node of menu.querySelectorAll(cfg.nodeSel)) {
      const a = node.querySelector(cfg.linkSel);
      if (!a || !a.href) continue;
      // The section path of a tree node is simply its ancestor nodes' labels.
      // Walk up to the menu root collecting them — "Regulatory Sandbox
      // Framework > Stage 1" falls out of the nesting, no selectors needed.
      const trail = [];
      let p = node.parentElement;
      while (p && p !== menu.parentElement) {
        if (p.matches && p.matches(cfg.nodeSel)) {
          const pa = p.querySelector(cfg.linkSel);
          if (pa) trail.unshift(txt(pa).slice(0, 120));
        }
        p = p.parentElement;
      }
      out.push({
        href: a.href,
        title: txt(a).slice(0, 300),
        trail: trail,
        depth: trail.length,
        // A node that has children is a folder as well as a page. Kept so a
        // reviewer can see the shape of what was found.
        children: node.querySelectorAll(cfg.nodeSel).length
      });
    }
  }
  return out;
}"""


# --------------------------------------------------------------------------- #
# PAGINATION — turn the form's pattern into the full page sequence
# --------------------------------------------------------------------------- #

def _pattern_regex(pattern: str, token: str) -> re.Pattern:
    """'https://x/P{offset}' -> regex matching https://x/P<digits>."""
    left, right = pattern.split(token, 1)
    return re.compile(re.escape(left) + r"(\d+)" + re.escape(right) + r"$")


def plan_pages(pagination: dict, seed_url: str, discovered_hrefs: list[str]) -> tuple[list[str], dict]:
    """Return (list of listing-page URLs, a note about how the plan was decided).

    `max_offset` in the form freezes the plan. When it is absent we discover the
    last page from the pager links on page 1 — which works, but is a number that
    can move between runs, so we report it and verify.py flags the difference.
    """
    mode = pagination.get("mode", "none")
    max_pages = int(pagination.get("max_pages", 200))
    note: dict = {"mode": mode, "frozen": False, "discovered_max": None}

    if mode == "none" or mode == "click":
        return [seed_url], note

    token = "{offset}" if mode == "url_offset" else "{page}"
    pattern = pagination["pattern"]
    step = int(pagination.get("step", 1))
    rx = _pattern_regex(pattern, token)

    declared = pagination.get("max_offset")
    if declared:
        last = int(declared)
        note["frozen"] = True
    else:
        nums = [int(m.group(1)) for h in discovered_hrefs if (m := rx.match(h))]
        last = max(nums) if nums else 0
        note["discovered_max"] = last

    urls = [seed_url]
    if last >= step:
        urls += [pattern.replace(token, str(n)) for n in range(step, last + 1, step)]
    # De-dupe in case the seed is itself the first pattern URL.
    wanted = len(dict.fromkeys(urls))
    urls = list(dict.fromkeys(urls))[:max_pages]
    note["planned_pages"] = len(urls)
    note["max_pages"] = max_pages
    # True only when the cap actually cut the plan short — a plan of 3 pages under
    # a cap of 200 is not capped, and saying so would train people to ignore the
    # warning that matters.
    note["capped_by_max_pages"] = wanted > max_pages
    note["pages_wanted"] = wanted
    return urls, note


# --------------------------------------------------------------------------- #
# THE RUN
# --------------------------------------------------------------------------- #

def _expand(page, selector: str, rounds: int = 6) -> int:
    """Click every match until the page stops growing.

    Accordions and "show more" controls hide content that is already in the DOM
    but collapsed. Reading it collapsed gives you the headings and none of the
    substance — SIMAH's page would be 17 lines saying "Article-N" and no law.
    Clicking is also what makes the SAVED HTML match what a person sees.
    """
    if not selector:
        return 0
    clicked = 0
    last = -1
    for _ in range(rounds):
        try:
            size = page.evaluate("()=>document.body.innerText.length")
            if size == last:
                break
            last = size
            clicked += page.evaluate(JS_EXPAND, selector)
        except Exception:
            break
        page.wait_for_timeout(400)
    return clicked


def _row_signature(page, row_sel: str, link_sel: str) -> str:
    try:
        return page.evaluate(JS_ROW_SIG, {"rowSel": row_sel, "linkSel": link_sel})
    except Exception:
        return ""


def _click_through(page, pagination: dict, row_sel: str, link_sel: str,
                   css_fields: dict, rx_fields: dict, rows: list, seen: set,
                   section_prefix, section_levels, wait_ms: int,
                   page_log: list) -> dict:
    """Walk a JavaScript pager, harvesting each page. Page 1 is already harvested.

    Returns {"note": ..., "warnings": [...]}. Every turn is verified against the
    row fingerprint, so the walk stops rather than looping on a dead control.
    """
    next_sel = pagination.get("next_selector") or ""
    max_pages = int(pagination.get("max_pages", 200))
    note = {"mode": "click", "pages_walked": 1, "stopped": "", "max_pages": max_pages}
    warns: list[str] = []

    empty_streak = 0
    page_no = 1
    for page_no in range(2, max_pages + 1):
        sig_before = _row_signature(page, row_sel, link_sel)
        url_before = page.url
        try:
            outcome = page.evaluate(JS_CLICK_NEXT, next_sel)
        except Exception as e:
            outcome = f"click-error: {str(e)[:60]}"
        if outcome != "clicked":
            note["stopped"] = f"page {page_no}: {outcome}"
            if page_no == 2:
                warns.append(f"pagination.next_selector {next_sel!r} matched no usable "
                             f"control on the seed page ({outcome}) — only page 1 was "
                             "walked. Check it against `formfill inspect`.")
            break

        changed, waited = False, 0
        while waited < CLICK_SETTLE_MS:
            page.wait_for_timeout(CLICK_POLL_MS)
            waited += CLICK_POLL_MS
            if _row_signature(page, row_sel, link_sel) != sig_before:
                changed = True
                break
        if not changed:
            # On the last page this is normal — the control is often still there.
            # On the FIRST turn it means the selector never worked at all.
            note["stopped"] = f"page {page_no}: rows unchanged after click"
            if page_no == 2:
                warns.append(f"pagination.next_selector {next_sel!r} was clicked but the "
                             "row set never changed — only page 1 was walked. Wrong "
                             "selector, or this pager needs mode: custom.")
            break

        page.wait_for_timeout(wait_ms)
        h = _harvest(page, row_sel, link_sel, css_fields, rx_fields,
                     page.url, rows, seen, section_prefix, section_levels)
        page_log.append({"url": f"{page.url}#page-{page_no}", "matched": h["matched"],
                         "new": h["new"],
                         "status": "ok-clicked" + ("-navigated" if page.url != url_before else "")})
        note["pages_walked"] = page_no
        emit({"event": "click_page", "page": page_no, "matched": h["matched"],
              "new": h["new"], "rows": len(rows)})

        if h["new"] == 0:
            empty_streak += 1
            if empty_streak >= 2:
                note["stopped"] = f"page {page_no}: two consecutive pages added no new rows"
                break
        else:
            empty_streak = 0
    else:
        # Ran the whole budget without the pager ending: the count is the cap.
        note["capped_by_max_pages"] = True
        note["planned_pages"] = note["pages_walked"]
        note["pages_wanted"] = f"more than {note['pages_walked']}"
        note["stopped"] = f"hit max_pages ({max_pages})"

    return {"note": note, "warnings": warns}


# SNAPSHOTS — develop a form against a page we already have.
#
# One full run of SIMAH's form is TWO loads of one URL, so volume never tripped
# Cloudflare: ITERATION did. Every selector fix and every `verify --runs 3` was
# more live traffic for no new information. Capture once, then run offline.
#
# `<base href>` is not optional. Fields read `el.href` (the RESOLVED property, see
# JS_ROWS), so without it every relative link resolves against about:blank and
# document_url comes out quietly wrong.
_BASE_RE = re.compile(r"<base\b", re.I)


def snapshot_html(html: str, base_url: str) -> str:
    """Saved page + a <base>, so relative links resolve as they did live."""
    if _BASE_RE.search(html or ""):
        return html
    tag = f'<base href="{base_url}">'
    m = re.search(r"<head[^>]*>", html or "", re.I)
    return (html[:m.end()] + tag + html[m.end():]) if m else tag + (html or "")


def _load(page, url: str, wait_ms: int, tries: int = 3, snap: str | None = None) -> bool:
    """These sites are flaky. An empty page must never be read as 'no rows' —
    that is exactly the silent failure that puts holes in the library.

    `snap` is saved HTML: serve it instead of fetching, and touch no network.
    """
    if snap is not None:
        page.set_content(snapshot_html(snap, url), wait_until="domcontentloaded")
        page.wait_for_timeout(min(wait_ms, 300))
        return True
    for _ in range(tries):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(wait_ms)
            for _ in range(2):
                page.mouse.wheel(0, 6000)
                page.wait_for_timeout(250)
            # Wait for the page to STOP GROWING rather than for a fixed time.
            # SIMAH renders its 17 articles late: at 1.7s the body is 686
            # characters and div.accordion does not exist yet, so a fixed wait
            # captured a page that had not arrived. Two identical samples means
            # it has settled; a fast page costs one extra 400ms.
            settled = -1
            for _ in range(12):
                size = page.evaluate("()=>document.body ? document.body.innerText.length : 0")
                if size == settled:
                    break
                settled = size
                page.wait_for_timeout(400)
            # "Enough links" alone is the wrong test. SIMAH's law page is a
            # single document with almost no links — it reports 1 anchor until
            # the nav finishes rendering — and a >15 threshold silently threw the
            # whole page away as a failed load. Text is the other evidence that a
            # page arrived, and a document page has plenty of it.
            state = page.evaluate(
                "()=>({a: document.querySelectorAll('a[href]').length,"
                "      t: (document.body ? document.body.innerText.length : 0)})")
            if state["a"] > 15 or state["t"] > 400:
                return True
        except Exception:
            pass
        page.wait_for_timeout(1500)
    return False


def run(hints: dict, out_dir: str | Path, headless: bool = True, wait_ms: int = 1200,
        fetch_details: bool | None = None, max_details: int | None = None,
        max_pages: int | None = None, write_excel: bool = True,
        snapshot: str | Path | None = None) -> dict:
    """Crawl `hints['seed_url']` exactly as the form says. Returns a run summary.

    `snapshot` is a saved copy of the seed page. Given one, the run makes NO
    network requests: the same form, the same extraction, replayed against the
    saved HTML. The summary records `source: snapshot` so a replay can never be
    mistaken for a crawl of the live site.
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    started = time.time()
    pagination = dict(hints.get("pagination") or {"mode": "none"})
    if max_pages:
        pagination["max_pages"] = max_pages
    row_sel = hints.get("row_selector") or ""      # absent on shape: tree
    link_sel = hints.get("detail_link_selector") or ""
    css_fields = [{"target": t, "selector": r["selector"], "attr": r.get("attr", "text")}
                  for t, r in (hints.get("fields") or {}).items() if r.get("from") == "css"]
    rx_fields = compile_field_regexes(hints)
    do_details = hints.get("fetch_details", True) if fetch_details is None else fetch_details

    sp = hints.get("section_path") or {}
    section_prefix = list(sp.get("prefix") or [])
    expand_sel = hints.get("expand_selector") or ""
    crumb_sel = sp.get("from_breadcrumb") or ""
    crumb_drop_first = int(sp.get("drop_first", 0))
    crumb_drop_last = int(sp.get("drop_last", 0))
    section_levels = [
        {"preceding": lv["preceding"]} if lv.get("preceding")
        else {"ancestor": lv["ancestor"], "title": lv["title"]}
        for lv in (sp.get("levels") or [])]

    panels_cfg = hints.get("panels") or {}
    tabs_sel = panels_cfg.get("tabs") or ""
    include_panel = bool(panels_cfg.get("include_panel"))
    panels: list[dict] = []

    seed = hints["seed_url"]
    section = (hints.get("name") or urlparse(seed).netloc).split(".")[-1].title()
    is_tree = hints.get("shape") == "tree"
    tree_max_nodes = int((hints.get("tree") or {}).get("max_nodes", 1000))

    # Read the snapshot ONCE. A missing file is a hard error, not a silent
    # fallthrough to the live site: the whole point is that this run cannot
    # generate traffic, and quietly crawling instead is the surprise that costs
    # an IP block.
    snap_html: str | None = None
    if snapshot is not None:
        snap_path = Path(snapshot)
        if not snap_path.exists():
            raise FileNotFoundError(
                f"snapshot not found: {snap_path}. Capture one with "
                f"`formfill snapshot {hints.get('name')}` — this run will not go live.")
        snap_html = snap_path.read_text(encoding="utf-8")

        # A SNAPSHOT IS ONE PAGE, so it can only replay a one-page form.
        #
        # Refused rather than half-served, because both alternatives are worse than
        # an error: fetching pages 2..N would generate exactly the live traffic a
        # snapshot exists to avoid, and skipping them would report a fraction of the
        # site as if it were the whole thing. SBP's 139 listing pages and SAMA's
        # 40-node tree need the network; SIMAH's single law page does not.
        mode = (pagination.get("mode") or "none").lower()
        if mode != "none":
            raise ValueError(
                f"{hints.get('name')}: --snapshot only works on a single-page form, "
                f"and this one paginates (mode: {mode}). One saved page cannot stand "
                f"in for a walk of many, and serving it for page 1 while fetching the "
                f"rest would put live traffic behind a flag that promises none.")
        if hints.get("shape") == "tree":
            raise ValueError(
                f"{hints.get('name')}: --snapshot only works on a single-page form, "
                f"and shape: tree discovers its nodes by visiting them — the seed's "
                f"menu shows a fraction of the tree (20 of SAMA's 40), so a replay "
                f"would silently report that fraction as the whole rulebook.")

    rows: list[dict] = []
    seen: set[str] = set()
    page_log: list[dict] = []
    warnings: list[str] = []
    plan_note: dict = {"mode": pagination.get("mode", "none")}
    blocked = 0        # pages that came back as a bot-protection wall

    emit({"event": "start", "name": hints.get("name"), "seed": seed,
          "row_selector": row_sel, "mode": pagination.get("mode")})

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        ctx = browser.new_context(user_agent=UA, locale="en-US")
        page = ctx.new_page()

        # ---------------- PHASE 1: the listing ----------------
        try:
            if not _load(page, seed, wait_ms, snap=snap_html):
                warnings.append("seed page failed to load after 3 attempts")
            blocked_reason = _blocked(page)
            if blocked_reason:
                blocked += 1
                warnings.append(
                    f"BLOCKED BY BOT PROTECTION on the seed page ({blocked_reason!r}). "
                    "Everything below is the challenge page, not the site. Slow the "
                    "crawl down, or this source needs mode: custom.")
                emit({"event": "blocked", "url": seed, "reason": blocked_reason})
            if expand_sel:
                n = _expand(page, expand_sel)
                emit({"event": "expand", "selector": expand_sel, "clicked": n})

            try:
                h1 = page.evaluate(
                    "()=>{const h=document.querySelector('h1');return h?(h.innerText||''):''}")
                section = re.sub(r"\s+", " ", h1).strip() or section
            except Exception:
                pass

            if tabs_sel:
                panels = _find_panels(page, tabs_sel)
                emit({"event": "panels", "found": len(panels),
                      "labels": [p["label"] for p in panels]})
                if not panels:
                    warnings.append(
                        f"panels.tabs {tabs_sel!r} matched no tab whose href is a "
                        "fragment pointing at an element on this page — check it "
                        "against `formfill inspect`")

            # Each panel is one document: the tab names it, the panel's text IS
            # its content. Same idea as include_page, one level in.
            if include_panel:
                for p in panels:
                    url = f"{seed}{p['fragment']}"
                    rows.append({
                        "title": p["label"],
                        "href": url,
                        "row_text": "",
                        "found_on": seed,
                        "in_page": f"[data-ff-panel=\"{p['index']}\"]",
                        "section_trail": list(section_prefix),
                        "fields": {"title": p["label"], "document_url": url},
                    })
                    seen.add(url)

            # The seed page as a document in its own right, added FIRST so it
            # leads the inventory. Phase 2 opens it like any other row, which is
            # what captures its text and HTML.
            if hints.get("include_page"):
                rows.append({
                    "title": section,
                    "href": seed,
                    "row_text": "",
                    "found_on": seed,
                    "section_trail": list(section_prefix) or [section],
                    "fields": {"title": section, "document_url": seed},
                })
                seen.add(seed)

            if is_tree:
                # One page, one menu. Everything below the listing loop is the
                # same for both shapes, which is the whole reason a tree is a
                # shape here rather than a second crawler.
                res = _walk_tree(page, hints["tree"], rx_fields, seed, rows, seen,
                                 section_prefix)
                page_log.append({"url": seed, "matched": res["matched"],
                                 "new": res["new"], "status": "ok"})
                plan_note = {"mode": "tree", "nodes_found": res["matched"]}
                emit({"event": "tree", "nodes": res["matched"], "rows": len(rows)})
                if res["new"] == 0:
                    warnings.append(
                        f"the menu {hints['tree']['menu_selector']!r} yielded no nodes — "
                        "check menu_selector / node_selector against `formfill inspect`")
                listing_urls = []
            else:
                hrefs = (page.evaluate(JS_HREFS)
                         if pagination.get("mode") in ("url_offset", "url_page") else [])
                listing_urls, plan_note = plan_pages(pagination, seed, hrefs)
                emit({"event": "plan", "pages": len(listing_urls), **plan_note})

            empty_streak = 0
            for i, lurl in enumerate(listing_urls, 1):
                if i > 1 and not _load(page, lurl, wait_ms):
                    page_log.append({"url": lurl, "matched": 0, "new": 0, "status": "load-failed"})
                    warnings.append(f"listing page failed to load: {lurl}")
                    continue
                if i > 1 and tabs_sel:
                    panels = _find_panels(page, tabs_sel)
                new = _harvest_page(page, row_sel, link_sel, css_fields, rx_fields,
                                    lurl, rows, seen, section_prefix, section_levels,
                                    panels if tabs_sel else None)
                matched = new["matched"]
                page_log.append({"url": lurl, "matched": matched, "new": new["new"], "status": "ok"})

                if matched == 0:
                    empty_streak += 1
                    # Two empty pages in a row means the pattern has run past the
                    # end (or the selector is wrong). Keep going and it invents
                    # hundreds of pointless loads.
                    if empty_streak >= 2:
                        warnings.append(f"stopped at page {i}/{len(listing_urls)}: "
                                        "two consecutive pages matched no rows")
                        emit({"event": "stop_early", "page": i, "of": len(listing_urls)})
                        break
                else:
                    empty_streak = 0

                if i == 1 or i == len(listing_urls) or i % 10 == 0:
                    emit({"event": "list_page", "page": i, "of": len(listing_urls),
                          "matched": matched, "rows": len(rows)})

            # PHASE 1 over. This is the inventory, and on its own it is what
            # change detection consumes.
            if pagination.get("mode") == "click" and not is_tree:
                res = _click_through(page, pagination, row_sel, link_sel, css_fields,
                                     rx_fields, rows, seen, section_prefix,
                                     section_levels, wait_ms, page_log)
                plan_note = res["note"]
                warnings.extend(res["warnings"])
                emit({"event": "plan", "pages": plan_note["pages_walked"], **plan_note})
        finally:
            page.close()

        # ---------------- PHASE 2: the detail pages ----------------
        records, documents = [], {}
        # NOTE: for a tree this is the SAME list object as `rows`, not a copy —
        # that is what lets the walk below discover new nodes while iterating.
        targets = rows if do_details else []
        if max_details and not is_tree:
            targets = targets[:max_details]
        elif max_details and is_tree:
            warnings.append("--max-details is ignored for shape: tree (slicing the list "
                            "would stop the walk discovering deeper nodes) — "
                            "use tree.max_nodes instead")

        if targets:
            dp = ctx.new_page()
            # Rows that live inside the seed document share one load of it. The
            # stamps from phase 1 were on a page that is now closed, so the same
            # deterministic passes are replayed here to put them back.
            in_page_ready: bool | None = None       # None = not attempted yet

            def _prepare_in_page() -> bool:
                if not _load(dp, seed, wait_ms, tries=2, snap=snap_html):
                    warnings.append("could not reload the seed page for its panels — "
                                    "panel text is missing from this run")
                    return False
                if expand_sel:
                    _expand(dp, expand_sel)
                again = _find_panels(dp, tabs_sel) if tabs_sel else []
                if row_sel and tabs_sel:
                    _harvest_page(dp, row_sel, link_sel, css_fields, rx_fields, seed,
                                  [], set(), section_prefix, section_levels, again)
                return True

            try:
                for i, r in enumerate(targets, 1):
                    # A row that already points AT a file has no detail page to
                    # open. Trying anyway loads a PDF in a browser tab, finds no
                    # HTML, and books it as a failed fetch — so the file never
                    # reaches `documents` at all. Register it and move on.
                    if r["href"] and _is_doc(r["href"]):
                        _add_document(documents, r["href"], r["title"],
                                      r.get("found_on") or seed,
                                      " > ".join(r.get("section_trail") or []) or section,
                                      declared=True)      # the form named this row
                        records.append(_record(r, section, seed))
                        continue
                    if r.get("in_page"):
                        # No page to fetch: the content is an element of the seed
                        # document, which is loaded once for every such row.
                        if in_page_ready is None:
                            in_page_ready = _prepare_in_page()
                        if not in_page_ready:
                            records.append(_record(r, section, seed))
                            continue
                        try:
                            d = dp.evaluate(JS_IN_PAGE_DETAIL, r["in_page"])
                        except Exception:
                            d = None
                        if not d:
                            warnings.append(f"panel content not found again for "
                                            f"{r['title']!r} ({r['in_page']})")
                            records.append(_record(r, section, seed))
                            continue
                    else:
                        # On a snapshot run the only page we hold is the seed, so a
                        # row pointing at it (include_page) replays; any other row
                        # would need the network and is left without detail content
                        # rather than quietly fetched.
                        row_snap = snap_html if (snap_html is not None
                                                 and r["href"] == seed) else None
                        if snap_html is not None and row_snap is None:
                            records.append(_record(r, section, seed))
                            continue
                        if not r["href"] or not _load(dp, r["href"], wait_ms, tries=2,
                                                      snap=row_snap):
                            records.append(_record(r, section, seed))
                            continue
                        # Phase 2 loads a fresh tab, so anything the form expanded in
                        # phase 1 is collapsed again here. Expand before capturing or
                        # the saved HTML is the closed version of the page.
                        if expand_sel:
                            _expand(dp, expand_sel)
                        reason = _blocked(dp)
                        if reason:
                            # Store the row, never the challenge page as its content.
                            blocked += 1
                            records.append(_record(r, section, seed))
                            if blocked <= 3:
                                warnings.append(
                                    f"BLOCKED BY BOT PROTECTION at {r['href']}: {reason!r}")
                            continue
                        try:
                            d = dp.evaluate(JS_DETAIL)
                        except Exception:
                            records.append(_record(r, section, seed))
                            continue
                    # A tree is discovered as it is walked. Drupal book menus
                    # (and most rulebook sidebars) only render the branch you
                    # are currently in, so the seed page shows 20 nodes and the
                    # rest only appear once you are standing on a child page.
                    # Re-reading the menu here turns phase 2 into a breadth-first
                    # walk: `rows` grows while the loop iterates it.
                    if is_tree and len(rows) < tree_max_nodes:
                        _walk_tree(dp, hints["tree"], rx_fields, r["href"], rows, seen,
                                   section_prefix)

                    # The page's own breadcrumb wins over anything inferred from a
                    # menu. On a tree the menu only shows the branch you are in —
                    # a node discovered three levels down inherits whatever
                    # nesting that one page happened to render, which is how
                    # "A1 Identification/Contact Details" ended up filed directly
                    # under "Regulatory Sandbox". The breadcrumb states the real
                    # path, in the site's own words.
                    if crumb_sel:
                        try:
                            crumbs = dp.evaluate(JS_CRUMBS, crumb_sel)
                        except Exception:
                            crumbs = []
                        crumbs = crumbs[crumb_drop_first:len(crumbs) - crumb_drop_last
                                        if crumb_drop_last else None]
                        if len(crumbs) >= 2:
                            # The breadcrumb says where the PAGE sits; the panel
                            # says where in the page, so it survives underneath.
                            r["section_trail"] = crumbs + (
                                [r["panel_label"]] if r.get("panel_label") else [])

                    row_path = " > ".join(r.get("section_trail") or []) or section
                    doc_links = []
                    for l in d.get("links", []):
                        h = l.get("href") or ""
                        if h.startswith("http") and _is_doc(h):
                            doc_links.append(h)
                            # Scraped from an anchor: keeps the title only if no
                            # declared row claims this file (see _add_document).
                            _add_document(documents, h,
                                          _scraped_doc_title(l, r["title"]) if r.get("in_page")
                                          else (l.get("text") or r["title"]),
                                          r["href"], row_path, declared=False)
                    records.append(_record(r, section, seed, {
                        "n_pdfs": len(doc_links),
                        "pdf_links": " | ".join(doc_links),
                        "text_len": len(d.get("text") or ""),
                        "text": d.get("text") or "",
                        "html": d.get("html") or "",
                    }))
                    if i % 25 == 0 or i == len(targets):
                        emit({"event": "detail_page", "done": i, "of": len(targets)})
            finally:
                dp.close()
        else:
            records = [_record(r, section, seed) for r in rows]
            # A row whose link is already a PDF is a document in its own right;
            # without phase 2 it would otherwise be lost.
            for r in rows:
                if r["href"] and _is_doc(r["href"]):
                    _add_document(documents, r["href"], r["title"], seed,
                                  " > ".join(r.get("section_trail") or []) or section,
                                  declared=True)

        browser.close()

    for rec in records:
        rec["content_hash"] = content_key(rec.get("text") or rec.get("row_text") or "")
    docs = list(documents.values())
    # A title shared by several different documents is not a title: GOSI files
    # four occupational-hazard policies under one "Insurance coverage document"
    # heading. Declared titles are the form's answer and are left alone.
    retitled = disambiguate_titles([d for d in docs if not d.get("title_declared")])
    for d in docs:
        d["content_hash"] = content_key(f"{d.get('doc_url','')}|{d.get('title','')}")

    summary = {
        "name": hints.get("name"),
        "seed": seed,
        "engine": "formfill",
        "hints_version": hints.get("version"),
        "started_at": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(timespec="seconds"),
        "seconds": round(time.time() - started, 1),
        "listing_pages": len(page_log),
        # How the page sequence was decided. verify.py warns when it was
        # discovered from the site instead of frozen in the form, because a
        # discovered number can move between runs.
        "plan": plan_note,
        "panels": [{"label": p["label"], "fragment": p["fragment"],
                    "text_len": p["text_len"]} for p in panels],
        "rows": len(rows),
        "records": len(records),
        "documents": len(docs),
        "titles_disambiguated": retitled,
        "phase2_ran": bool(targets),
        "fill_rates": _fill_rates(rows, hints),
        "blocked_pages": blocked,
        # Provenance. A replay produces the same rows as a crawl, which is what
        # makes it useful and also what makes it dangerous: unlabelled, a snapshot
        # run would tell change detection "unchanged" forever while the site moved
        # on. Everything downstream reads this.
        "source": "snapshot" if snap_html is not None else "live",
        "snapshot": str(snapshot) if snap_html is not None else "",
        "warnings": warnings,
        "pages": page_log,
    }

    # Write the HTML files BEFORE pages.json, so the html_file each record
    # points at is already stamped into the JSON too.
    html_written = _write_html_files(out, records) if any(r.get("html") for r in records) else 0
    summary["html_files"] = html_written

    (out / "pages.json").write_text(json.dumps(
        {"seed": seed, "shape": hints.get("shape"), "engine": "formfill",
         "source": summary["source"], "snapshot": summary["snapshot"],
         "pages": records, "documents": docs}, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "rows.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "run.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    if write_excel:
        try:
            _write_excel(out / "results.xlsx", rows, records, docs)
            summary["xlsx"] = str(out / "results.xlsx")
        except Exception as e:                    # never lose a crawl to a spreadsheet
            summary["warnings"].append(f"excel write failed: {e}")

    emit({"event": "done", **{k: summary[k] for k in
                              ("rows", "records", "documents", "html_files", "seconds")},
          "out_dir": str(out)})
    return summary


def _walk_tree(page, tree: dict, rx_fields: dict, page_url: str, rows: list, seen: set,
               section_prefix=()) -> dict:
    """Expand the menu, then read every node out of it.

    Returns the same {matched, new} the listing harvest returns, and appends the
    same row dicts — so everything downstream is unchanged.
    """
    cfg = {"menuSel": tree["menu_selector"], "nodeSel": tree["node_selector"],
           "linkSel": tree["link_selector"]}
    max_depth = int(tree.get("max_depth", 8))
    max_nodes = int(tree.get("max_nodes", 1000))

    # Most rulebook menus render collapsed: the deep nodes do not exist in the
    # DOM until something is clicked. Keep clicking until the node count stops
    # growing — that, not a fixed number of rounds, is when the tree is open.
    expand_sel = tree.get("expand_selector")
    rounds, before = 0, -1
    while expand_sel and rounds < 20:
        try:
            count = page.evaluate(
                "(s)=>document.querySelectorAll(s).length", cfg["nodeSel"])
        except Exception:
            break
        if count == before:
            break
        before = count
        try:
            page.evaluate(JS_EXPAND, expand_sel)
        except Exception:
            break
        page.wait_for_timeout(600)
        rounds += 1
    if expand_sel:
        emit({"event": "tree_expand", "rounds": rounds, "nodes": before})

    try:
        nodes = page.evaluate(JS_TREE, cfg)
    except Exception as e:
        emit({"event": "tree_error", "error": str(e)[:200]})
        return {"matched": 0, "new": 0}

    new = 0
    for n in nodes:
        if n["depth"] > max_depth or len(rows) >= max_nodes:
            continue
        href = n["href"]
        if not href.startswith("http") or href in seen:
            continue
        seen.add(href)
        fields = {"title": n["title"], "document_url": href}
        for target, rx in rx_fields.items():
            m = rx.search(n["title"])
            fields[target] = (m.group(1) if m and rx.groups else (m.group(0) if m else "")).strip()
        rows.append({
            "title": n["title"],
            "href": href,
            "row_text": n["title"],
            "found_on": page_url,
            "section_trail": [c for c in (list(section_prefix) + n["trail"]) if c],
            "fields": fields,
        })
        new += 1
    return {"matched": len(nodes), "new": new}


def _harvest(page, row_sel, link_sel, css_fields, rx_fields, page_url, rows, seen,
             section_prefix=(), section_levels=(), root_sel=None, root_label="",
             root_fragment="", stamp_base=None) -> dict:
    """`root_sel` scopes the search to one panel; `stamp_base` marks each row so
    phase 2 can find it again in the same document."""
    try:
        res = page.evaluate(JS_ROWS, {"rowSel": row_sel, "linkSel": link_sel,
                                      "cssFields": css_fields,
                                      "sectionLevels": list(section_levels),
                                      "rootSel": root_sel, "rootLabel": root_label,
                                      "stampBase": stamp_base})
    except Exception as e:
        emit({"event": "harvest_error", "url": page_url, "error": str(e)[:200]})
        return {"matched": 0, "new": 0}

    new = 0
    for raw in res.get("rows", []):
        fields = dict(raw.get("fields") or {})
        # Regex fields read the row's own visible text — the line that carries
        # the reference number, date and department the detail page often omits.
        for target, rx in rx_fields.items():
            m = rx.search(raw.get("row_text", ""))
            fields[target] = (m.group(1) if m and rx.groups else (m.group(0) if m else "")).strip()

        href = fields.get("document_url") or raw.get("href") or ""
        title = fields.get("title") or raw.get("link_text") or ""
        # Blank levels are dropped rather than left as empty crumbs: a row that
        # sits outside one of the groups should read "Laws > Financial Sector",
        # not "Laws >  > Financial Sector".
        trail = [c for c in (list(section_prefix) + list(raw.get("section") or [])) if c]
        # A row with no URL is identified by its place as well as its title: GOSI
        # panels 4 and 5 both hold a "SECTION III: PROVISIONS CONCERNING
        # VOLUNTARILY CONTRIBUTORS", and they are different documents.
        key = href or f"{page_url}::{'/'.join(trail)}::{title}"
        if not key.strip("::") or key in seen:
            continue
        seen.add(key)
        row = {
            "title": title,
            "href": href,
            "row_text": raw.get("row_text", ""),
            "found_on": page_url,
            "section_trail": trail,
            "fields": fields,
        }
        if raw.get("ff_row") is not None and not href:
            row["in_page"] = f"[data-ff-row=\"{raw['ff_row']}\"]"
            row["panel_label"] = root_label
            # It has no URL of its own; the tab that reveals it is the closest
            # thing a reader can open. Identity comes from section_trail.
            row["href"] = fields["document_url"] = f"{page_url}{root_fragment}"
        rows.append(row)
        new += 1
    return {"matched": res.get("matched", 0), "new": new}


def _harvest_page(page, row_sel, link_sel, css_fields, rx_fields, page_url, rows, seen,
                  section_prefix, section_levels, panels) -> dict:
    """One harvest per panel, or one for the whole page when the form declares no
    panels. A form may declare panels and no row_selector (include_panel alone),
    in which case there is nothing to harvest."""
    if not row_sel:
        return {"matched": 0, "new": 0}
    if panels is None:
        return _harvest(page, row_sel, link_sel, css_fields, rx_fields, page_url,
                        rows, seen, section_prefix, section_levels)
    total = {"matched": 0, "new": 0}
    for p in panels:
        r = _harvest(page, row_sel, link_sel, css_fields, rx_fields, page_url, rows,
                     seen, section_prefix, section_levels,
                     root_sel=f"[data-ff-panel=\"{p['index']}\"]", root_label=p["label"],
                     root_fragment=p["fragment"], stamp_base=p["index"] * 100_000)
        total["matched"] += r["matched"]
        total["new"] += r["new"]
    return total


def _find_panels(page, tabs_sel: str) -> list[dict]:
    """Resolve the tab strip to panels, stamping each one. Deterministic, so
    running it again on a fresh load of the same page gives the same stamps."""
    try:
        return page.evaluate(JS_PANELS, tabs_sel)
    except Exception as e:
        emit({"event": "panels_error", "error": str(e)[:200]})
        return []


def _record(r: dict, section: str, seed: str, extra: dict | None = None) -> dict:
    """One row in generic_crawler's `pages.json` schema — same keys, same meaning."""
    trail = r.get("section_trail") or []
    # " > " is generic_crawler's separator (crawler.py::doc_section_path), so a
    # path from either engine reads the same downstream.
    path = " > ".join(trail) if trail else section
    rec = {
        "section_path": path,
        "title": r["title"],
        "url": r["href"],
        "depth": 1,
        "linked_from_title": r["title"],
        "parent_page_url": r.get("found_on") or seed,
        "status": "",
        "n_pdfs": 0,
        "pdf_links": "",
        "text_len": 0,
        "html_file": "",
        "text": "",
        "html": "",
        "breadcrumb": trail or ([section] if section else []),
        "row_text": r.get("row_text", ""),
        # The form's extracted fields, kept alongside so the orchestrator adapter
        # can map them onto RegulatoryDocument without re-parsing anything.
        "fields": r.get("fields", {}),
    }
    rec.update(extra or {})
    return rec


def _fill_rates(rows: list[dict], hints: dict) -> dict:
    """What fraction of rows actually got each field. The single most useful
    number for spotting a selector that matched the wrong thing: a regex that
    fires on 3% of rows is wrong, not 'partially working'."""
    # Always include title and document_url even when the form does not declare
    # them: a tree fills those from the menu node itself, and reporting "0%" for
    # a field that is in fact 100% populated would fail the gate on a bookkeeping
    # detail rather than on anything wrong with the crawl.
    targets = list(dict.fromkeys(["title", "document_url"]
                                 + list((hints.get("fields") or {}).keys())))
    n = len(rows) or 1
    return {t: round(100.0 * sum(1 for r in rows if (r["fields"].get(t) or "").strip()) / n, 1)
            for t in targets}


def _add_document(documents: dict, url: str, title: str, found_on: str, section: str,
                  declared: bool = False) -> None:
    """One row per FILE, not one per place the file is linked from.

    generic_crawler keys documents on (url, section_path) so a document
    cross-listed under two sections shows up under both. On a tree that backfires:
    the SAMA sandbox's "Print / Save as PDF" link appears on every page of a
    section, so three actual PDFs were reported as thirty-nine documents — a
    number that reads like coverage and is really repetition.

    So the key is the URL, the first section wins (breadth-first order means that
    is the shallowest one), and the other placements are kept in `also_in` rather
    than thrown away. Deduplicated, the sandbox reports 3 — which is what
    generic_crawler's baseline says too.

    `declared` says WHERE the title came from, and it is the one thing that can
    overwrite an existing one:

        declared=True   the form named this row a document entry and the gate
                        measured how often that field fills. SIMAH's <h6> is
                        "The Implementing Regulations for Credit Information Law".
        declared=False  scraped from an anchor while scanning a page. SIMAH's
                        anchor text is "Download PDF", which is not a title — the
                        form file says so in a comment.

    First-sighting-wins gave the anchor the last word whenever a page linked its own
    declared row (`include_page`), so the library got "Download PDF". A declared
    title now replaces a scraped one; nothing else about precedence changes, and a
    declared title is never downgraded. Where a URL has only one kind of sighting —
    every other form today — this is a no-op.
    """
    d = documents.get(url)
    if d is None:
        documents[url] = {
            "title": title,
            "doc_url": url,
            "type": _ext_type(url),
            "found_on": found_on,
            "section_path": section,
            "times_linked": 1,
            "also_in": "",
            "title_declared": bool(declared),
        }
        return
    d["times_linked"] += 1
    if declared and not d.get("title_declared") and (title or "").strip():
        d["title"] = title
        d["title_declared"] = True
    if section and section != d["section_path"]:
        others = [s for s in d["also_in"].split(" | ") if s]
        if section not in others and len(others) < 20:
            others.append(section)
            d["also_in"] = " | ".join(others)


def _scraped_doc_title(link: dict, fallback: str) -> str:
    """Anchor text unless it is a call to action, then the nearest heading, then
    the URL slug — generic_crawler's `best_doc_title` order, reusing its word
    list. Only the in-page path uses this; changing the ordinary detail path
    would move titles on six approved forms nothing has re-measured.
    """
    t = (link.get("text") or "").strip().replace("\xa0", " ")
    if t.lower() not in _GENERIC_TEXT and len(t) > 3:
        return t[:200]
    ctx = (link.get("ctx") or "").strip()
    if len(ctx) > 3:
        return ctx[:200]
    return title_from_slug(link.get("href") or "") or fallback


def _is_doc(url: str) -> bool:
    return bool(_DOC_EXT_RE.search(url)) or bool(
        re.search(r"wpdmdl=|/document/|/download/", url, re.I))


def _ext_type(url: str) -> str:
    m = _DOC_EXT_RE.search(url)
    return m.group(1).upper() if m else "DOC"


def _write_html_files(out: Path, records: list[dict]) -> int:
    """Save each page's HTML next to the data and record the filename.

    The HTML is always captured — it just used to live only inside pages.json,
    where nobody looking at the spreadsheet could tell it existed. A file per
    page makes it visible and lets you open one without parsing JSON.
    """
    hdir = out / "html"
    hdir.mkdir(parents=True, exist_ok=True)
    written = 0
    for i, rec in enumerate(records, 1):
        html = rec.get("html") or ""
        if not html:
            continue
        slug = re.sub(r"[^a-z0-9]+", "-", (rec.get("title") or "page").lower()).strip("-")[:60]
        name = f"{i:04d}_{slug or 'page'}.html"
        (hdir / name).write_text(
            f"<!-- source: {rec.get('url', '')} -->\n{html}", encoding="utf-8")
        rec["html_file"] = f"html/{name}"
        written += 1
    return written


def _write_excel(path: Path, rows: list[dict], records: list[dict], documents: list[dict]) -> None:
    """Three sheets. `inventory` is the one a reviewer reads: one line per
    document, with the extracted fields, the section path, and — when phase 2
    ran — how much text was captured and which file holds the HTML.

    The raw HTML itself is deliberately NOT a column: it averages 8 KB a page,
    which blows past Excel's 32k cell limit and makes the sheet unreadable. The
    `html_file` column points at it instead.
    """
    import pandas as pd
    CELL = 32000
    TEXT_PREVIEW = 2000

    # Prefer records: after phase 2 they carry everything the rows carry PLUS
    # the page text, the PDF links and the HTML filename.
    if records:
        inventory = [{
            "section_path": rec.get("section_path", ""),
            "title": rec.get("title", ""),
            **{k: v for k, v in (rec.get("fields") or {}).items() if k != "title"},
            "url": rec.get("url", ""),
            "text_len": rec.get("text_len", 0),
            "n_pdfs": rec.get("n_pdfs", 0),
            "pdf_links": (rec.get("pdf_links") or "")[:CELL],
            "html_file": rec.get("html_file", ""),
            "text_preview": (rec.get("text") or "")[:TEXT_PREVIEW],
            "row_text": (rec.get("row_text") or "")[:CELL],
        } for rec in records]
    else:
        inventory = [{"section_path": " > ".join(r.get("section_trail") or []),
                      "title": r["title"],
                      **{k: v for k, v in r["fields"].items() if k != "title"},
                      "url": r["href"], "found_on": r["found_on"],
                      "row_text": r["row_text"][:CELL]} for r in rows]

    pages = [{k: (str(v)[:CELL] if isinstance(v, (str, list)) else v)
              for k, v in rec.items() if k not in ("html", "fields")} for rec in records]

    with pd.ExcelWriter(path, engine="openpyxl") as xl:
        pd.DataFrame(inventory or [{"note": "no rows"}]).to_excel(
            xl, sheet_name="inventory", index=False)
        pd.DataFrame(pages or [{"note": "no pages"}]).to_excel(
            xl, sheet_name="pages", index=False)
        pd.DataFrame(documents or [{"note": "no documents"}]).to_excel(
            xl, sheet_name="documents", index=False)
