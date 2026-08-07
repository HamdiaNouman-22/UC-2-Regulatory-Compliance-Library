"""RENDERED-PAGE DIGEST — what the LLM actually gets to look at.

Why this exists when `dynamic_crawler/onboarding/site_inspector.py` already
"inspects a site": that one fetches with `requests` and hands the model ~45,000
characters of raw HTML. Both halves of that are wrong for our regulators:

  * RAW is empty on the sites that matter. SBP's circular list is drawn by
    JavaScript — the HTML `requests` receives contains none of the 30 entries.
    A model reading it will confidently describe a page that isn't there.

  * 45,000 CHARACTERS buries the answer. A rendered regulator page is ~300 links
    of markup; the four facts we need (which element is a row, which link opens
    it, how pagination works, where the date lives) are a rounding error inside
    it.

So we render with Playwright and then answer those four questions ourselves,
mechanically, and send the model a ~3 KB structured digest instead of a page.

That summarising step IS most of the detection work — which is exactly the point
worth being honest about: the LLM is doing the last mile (choosing between
candidates we found and writing the regexes), not the whole job.

Everything is written to an artifacts folder so a proposal can be audited later
against the page it was derived from, instead of an ephemeral browser session
nobody can reopen.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# --------------------------------------------------------------------------- #
# The digest is computed in the page, in one pass, so we never ship markup back
# to Python just to count it.
# --------------------------------------------------------------------------- #

JS_DIGEST = r"""() => {
  const DATE_RE = /\b(\d{1,2}[-\/ ][A-Za-z]{3,9}[-\/ ]\d{2,4}|[A-Za-z]{3,9}\.? \d{1,2},? \d{4}|\d{4}-\d{2}-\d{2}|\d{1,2}\/\d{1,2}\/\d{2,4})\b/;
  const REF_RE  = /\b(?:No\.?|Circular|Act|Notification|Ref)\b[^\n]{0,40}\d/i;
  const clean = s => (s || '').replace(/\s+/g, ' ').trim();
  const CHROME = 'header, footer, nav, aside, [role="banner"], [role="contentinfo"], [role="navigation"]';

  const inChrome = el => !!el.closest(CHROME);

  // A stable, human-readable selector for an element: tag plus up to two of its
  // own classes. Anything with generated-looking classes (hashes, digits-only)
  // is skipped — those change on the next deploy and would make a hints file
  // rot silently.
  const sigOf = el => {
    const cls = Array.from(el.classList)
      .filter(c => /^[A-Za-z][A-Za-z0-9_-]{1,30}$/.test(c) && !/^(ng|css|jsx)-/.test(c))
      .slice(0, 2);
    return el.tagName.toLowerCase() + cls.map(c => '.' + c).join('');
  };

  // ---- candidate ROW containers ----------------------------------------
  // Group every element by its signature; a "row" is whatever repeats.
  const groups = new Map();
  for (const el of document.body.querySelectorAll('*')) {
    if (inChrome(el)) continue;
    const txt = clean(el.innerText);
    // The floor is only here to skip empty and whitespace-only elements. It used
    // to be 12 characters, which quietly excluded real document links — SDAIA
    // labels its PDFs "The law" (7) and "Policies" (8), so a candidate holding
    // all 36 of its files was reported as holding 28, and the wrong row selector
    // looked like the right one. Short entries are ranked, not hidden.
    if (txt.length < 3 || txt.length > 3000) continue;
    const sig = sigOf(el);
    if (!sig.includes('.') && !['tr', 'li', 'article'].includes(el.tagName.toLowerCase())) continue;
    if (!groups.has(sig)) groups.set(sig, []);
    groups.get(sig).push(el);
  }

  const rowCandidates = [];
  for (const [sig, els] of groups) {
    if (els.length < 4) continue;
    const texts = els.map(e => clean(e.innerText));
    const linkOf = e => e.matches('a[href]') ? e : e.querySelector('a[href]');
    const withLink = els.filter(e => linkOf(e)).length;
    // Rows whose link is a file. aml.gov.sa's nav menu (li.static x33) otherwise
    // outranks its 11 real documents on link count alone — but nav items never
    // point at a PDF, and document rows very often do.
    const withDoc = els.filter(e => {
      const a = linkOf(e);
      return a && (/\.(pdf|docx?|xlsx?)($|\?)/i.test(a.href) || /wpdmdl=|\/download\//i.test(a.href));
    }).length;
    const dated = texts.filter(t => DATE_RE.test(t)).length;
    const reffed = texts.filter(t => REF_RE.test(t)).length;
    const avgLen = Math.round(texts.reduce((a, t) => a + t.length, 0) / texts.length);
    // Depth of the first one, so a reviewer can tell an outer container from the
    // inner heading that repeats the same number of times.
    let depth = 0; for (let p = els[0]; p; p = p.parentElement) depth++;
    // The link inside the row — the thing that opens the entry.
    const a = els[0].querySelector('a[href]') || (els[0].matches('a[href]') ? els[0] : null);
    rowCandidates.push({
      selector: sig,
      count: els.length,
      with_link: withLink,
      with_doc_link: withDoc,
      dated: dated,
      ref_like: reffed,
      avg_text_len: avgLen,
      depth: depth,
      sample_link_selector: a ? sigOf(a) : null,
      sample_href: a ? a.href : null,
      samples: texts.slice(0, 3).map(t => t.slice(0, 240))
    });
  }
  // Best first. A document row links to a file or carries a date; a nav item
  // does neither, however many times it repeats — so raw count is the weakest
  // term in the score.
  const score = c => c.with_doc_link * 2 + c.dated + c.with_link * 0.5 + c.count * 0.25;
  rowCandidates.sort((a, b) => score(b) - score(a));

  // ---- candidate SECTION levels ----------------------------------------
  // The site's own hierarchy is encoded in the row's ANCESTORS: a row sits in a
  // sector block, which sits in a tab pane. Walk up from the best row candidate
  // and report every ancestor that (a) holds fewer rows than the page total, so
  // it genuinely groups, and (b) has a heading naming the group.
  const groupLevels = [];
  if (rowCandidates.length) {
    const all = Array.from(document.querySelectorAll(rowCandidates[0].selector));
    const total = rowCandidates[0].count;
    // Sample rows from across the list, not just the first. MISA renders a
    // desktop copy AND a mobile copy of the same laws; the first row lives in
    // the mobile block, whose ancestors do not include the sector heading. One
    // sample would have missed the level the hierarchy actually needs.
    const picks = [...new Set([0, Math.floor(all.length / 3),
                               Math.floor(all.length / 2),
                               Math.floor(all.length * 2 / 3), all.length - 1])]
      .filter(i => i >= 0 && i < all.length).map(i => all[i]);
    // First class only: a state class like "show" is usually appended, and
    // baking it into a selector would silently skip the hidden tab.
    const ancSig = el => el.tagName.toLowerCase()
      + (Array.from(el.classList).filter(c => /^[A-Za-z][A-Za-z0-9_-]{1,30}$/.test(c))[0]
         ? '.' + Array.from(el.classList).filter(c => /^[A-Za-z][A-Za-z0-9_-]{1,30}$/.test(c))[0] : '');
    for (const start of picks) {
      let node = start.parentElement, hops = 0;
      while (node && node !== document.body && hops++ < 10) {
        const inside = node.querySelectorAll(rowCandidates[0].selector).length;
        if (inside > 1 && inside < total) {
          for (const hs of ['h1', 'h2', 'h3', 'h4', 'h5', 'legend', 'caption']) {
            const t = node.querySelector(hs);
            if (t && clean(t.innerText)) {
              const sig = ancSig(node);
              if (sig.includes('.') && !groupLevels.some(g => g.ancestor === sig)) {
                groupLevels.push({
                  ancestor: sig, title: hs, rows_inside: inside,
                  sample_title: clean(t.innerText).slice(0, 60),
                  groups_on_page: document.querySelectorAll(sig).length
                });
              }
              break;
            }
          }
        }
        node = node.parentElement;
      }
    }
  }
  // Outermost first — the order a section path is written in. An ancestor that
  // contains more rows sits higher up.
  groupLevels.sort((a, b) => b.rows_inside - a.rows_inside);

  // ---- candidate TREES --------------------------------------------------
  // A tree is a menu whose items nest: <li> inside <li>. That nesting IS the
  // section path, so finding the right menu is the whole job. Site navigation
  // nests too, which is why chrome is excluded and why the sample labels are
  // reported — a reviewer can tell "Regulatory Sandbox Framework" from
  // "About us" at a glance.
  // An id beats classes: it is usually the only selector that picks ONE menu.
  // rulebook.sama.gov.sa has three separate `ul.menu` elements; only
  // `#book-navigation-1` identifies the book tree.
  const menuSig = el => (/^[A-Za-z][\w-]{1,40}$/.test(el.id || '')) ? '#' + el.id : sigOf(el);
  // Chrome by class as well as by tag — many sites use <div class="header-…">
  // rather than <header>. Scoped to named chrome only: walking up looking for
  // any suspicious class hits page-level wrappers like
  // "dialog-off-canvas-main-canvas" and rejects the entire document.
  const CHROME_CLASS = /header|footer|navbar|mega|social|breadcrumb/i;
  const inChromeish = el => inChrome(el) || CHROME_CLASS.test(el.className || '')
    || !!el.closest('[class*=header],[class*=footer],[class*=navbar],[class*=mega-menu]');

  const treeRaw = [];
  for (const cont of document.querySelectorAll('ul, ol, nav, [id]')) {
    if (inChromeish(cont)) continue;
    const items = Array.from(cont.querySelectorAll('li')).filter(li => li.querySelector('a[href]'));
    if (items.length < 5) continue;
    const nested = items.filter(li => li.parentElement && li.parentElement.closest('li'));
    if (nested.length < 1) continue;                  // flat list, not a tree
    let maxDepth = 0;
    for (const li of items) {
      let d = 0;
      for (let p = li.parentElement; p && p !== cont; p = p.parentElement) if (p.matches('li')) d++;
      if (d > maxDepth) maxDepth = d;
    }
    const sig = menuSig(cont);
    if (!sig.includes('.') && !sig.startsWith('#')
        && !['nav', 'ul', 'ol'].includes(cont.tagName.toLowerCase())) continue;
    if (treeRaw.some(t => t.menu_selector === sig)) continue;
    treeRaw.push({
      el: cont,
      menu_selector: sig,
      node_selector: sigOf(items.find(li => li.classList.length) || items[0]),
      link_selector: 'a[href]',
      nodes: items.length,
      nested_nodes: nested.length,
      max_depth: maxDepth,
      menus_on_page: document.querySelectorAll(sig).length,
      samples: items.slice(0, 3).map(li => {
        const a = li.querySelector('a[href]');
        return clean(a ? (a.innerText || a.textContent) : '').slice(0, 60);
      })
    });
  }

  // Drop wrappers. `#main` contains the book menu and therefore "has" all its
  // nodes, but naming it would sweep in every other menu on the page too. If a
  // candidate contains another candidate that is already uniquely addressable,
  // the inner one is the real menu.
  const treeCandidates = treeRaw
    .filter(a => !treeRaw.some(b => b !== a && b.menus_on_page === 1 && a.el.contains(b.el)))
    .map(({ el, ...rest }) => rest);

  // Uniquely addressable first, then the deeper / larger tree.
  treeCandidates.sort((a, b) =>
    ((b.menus_on_page === 1) - (a.menus_on_page === 1)) * 1000
    + (b.nested_nodes + b.max_depth * 10) - (a.nested_nodes + a.max_depth * 10));

  // Things that look like "expand this branch" controls inside the best menu.
  const expandCandidates = [];
  if (treeCandidates.length) {
    const menu = document.querySelector(treeCandidates[0].menu_selector);
    if (menu) {
      for (const e of menu.querySelectorAll('[aria-expanded], [class*=toggle], [class*=expand], [class*=caret], [class*=collaps]')) {
        const sig = sigOf(e);
        if (sig.includes('.') && !expandCandidates.some(x => x.selector === sig)) {
          expandCandidates.push({ selector: sig, count: menu.querySelectorAll(sig).length });
        }
      }
    }
  }

  // ---- candidate SECTION headings (the un-wrapped kind) ----------------
  // Some pages never wrap their groups: aml.gov.sa just puts <h3>Laws and
  // Regulations</h3> above one set of links and <h3>Rules and Instructions</h3>
  // above the next. There is no ancestor to walk up to, so the rule has to be
  // "the nearest heading BEFORE this row".
  const headingGroups = [];
  if (rowCandidates.length) {
    const rowsAll = Array.from(document.querySelectorAll(rowCandidates[0].selector));
    for (const hs of ['h2', 'h3', 'h4']) {
      const heads = Array.from(document.querySelectorAll(hs))
        .filter(h => !inChrome(h) && clean(h.innerText || h.textContent));
      if (heads.length < 2 || heads.length > 12) continue;
      const groups = heads.map(h => ({
        title: clean(h.innerText || h.textContent).slice(0, 50),
        rows_after: rowsAll.filter(r =>
          (h.compareDocumentPosition(r) & 4) &&
          !heads.some(o => o !== h && (h.compareDocumentPosition(o) & 4)
                                   && (o.compareDocumentPosition(r) & 4))).length
      })).filter(g => g.rows_after > 0);
      if (groups.length >= 2) { headingGroups.push({ preceding: hs, groups }); break; }
    }
  }

  // ---- candidate PAGINATION -------------------------------------------
  const pagerLinks = Array.from(document.querySelectorAll('a[href]'))
    .filter(a => {
      const t = clean(a.innerText).toLowerCase();
      const cls = (a.className || '') + ' ' + (a.parentElement ? a.parentElement.className : '');
      return /^\d{1,4}$/.test(t) || /next|last|»|›|>>/.test(t)
             || /pag(e|inat)/i.test(String(cls)) || /rel=?next/i.test(a.rel || '');
    });
  // Collapse each href to a shape: /circulars/P30 -> /circulars/P{n}
  const shapes = new Map();
  for (const a of pagerLinks) {
    const shape = a.href.replace(/\d+/g, '{n}');
    if (!shapes.has(shape)) shapes.set(shape, { pattern: shape, numbers: [], sample: a.href });
    const nums = (a.href.match(/\d+/g) || []).map(Number);
    shapes.get(shape).numbers.push(...nums);
  }
  const pagination = Array.from(shapes.values())
    .map(s => ({
      pattern: s.pattern,
      sample: s.sample,
      numbers: Array.from(new Set(s.numbers)).sort((a, b) => a - b).slice(0, 12)
    }))
    .filter(s => s.numbers.length >= 2)
    .slice(0, 6);

  const nextButtons = Array.from(document.querySelectorAll('a,button,li,span'))
    .filter(e => !inChrome(e))
    .filter(e => /^(next|next page|»|›|>)$/i.test(clean(e.innerText)) ||
                 /next/i.test(e.getAttribute('aria-label') || '') ||
                 (e.getAttribute('rel') || '') === 'next')
    .slice(0, 5)
    .map(e => ({ selector: sigOf(e), text: clean(e.innerText).slice(0, 30) }));

  // ---- tables ----------------------------------------------------------
  const tables = Array.from(document.querySelectorAll('table')).map(t => ({
    rows: t.querySelectorAll('tbody tr').length || t.querySelectorAll('tr').length,
    headers: Array.from(t.querySelectorAll('th')).slice(0, 10).map(h => clean(h.innerText).slice(0, 40))
  })).filter(t => t.rows >= 2).slice(0, 5);

  // "Show 100 entries" — a DataTables-style page-size control changes the whole
  // pagination plan, so surface it rather than let the model guess.
  const pageSize = Array.from(document.querySelectorAll('select'))
    .filter(s => /entries|per ?page|show/i.test((s.parentElement || s).innerText || ''))
    .slice(0, 2)
    .map(s => ({ selector: sigOf(s), options: Array.from(s.options).map(o => clean(o.text)).slice(0, 8) }));

  const breadcrumb = Array.from(document.querySelectorAll(
      '[class*=breadcrumb] a, [class*=Breadcrumb] a, nav[aria-label*=readcrumb] a'))
    .map(a => clean(a.innerText)).filter(Boolean).slice(0, 10);

  return {
    url: location.href,
    title: clean(document.title).slice(0, 200),
    h1: clean((document.querySelector('h1') || {}).innerText || '').slice(0, 200),
    breadcrumb,
    total_links: document.querySelectorAll('a[href]').length,
    doc_links: Array.from(document.querySelectorAll('a[href]'))
      .filter(a => /\.(pdf|docx?|xlsx?)($|\?)/i.test(a.href) || /wpdmdl=|\/download\//i.test(a.href))
      .length,
    iframes: document.querySelectorAll('iframe, frame').length,
    row_candidates: rowCandidates.slice(0, 8),
    section_levels: groupLevels,
    heading_groups: headingGroups,
    tree_candidates: treeCandidates.slice(0, 4),
    expand_candidates: expandCandidates.slice(0, 4),
    pagination_candidates: pagination,
    next_buttons: nextButtons,
    tables,
    page_size_control: pageSize
  };
}"""


def inspect(seed_url: str, artifacts_dir: str | Path, headless: bool = True,
            wait_ms: int = 3000) -> dict:
    """Render `seed_url`, compute the digest, save the artifacts, return the digest."""
    art = Path(artifacts_dir)
    art.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        ctx = browser.new_context(user_agent=UA, locale="en-US")
        page = ctx.new_page()
        try:
            page.goto(seed_url, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(wait_ms)
            # Lazy lists (SBP) only render what has been scrolled past.
            for _ in range(3):
                page.mouse.wheel(0, 6000)
                page.wait_for_timeout(400)
            digest = page.evaluate(JS_DIGEST)
            rendered = page.content()
        finally:
            browser.close()

    digest["inspected_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    digest["rendered_html_chars"] = len(rendered)
    digest["js_dependence"] = _js_dependence(seed_url, digest["total_links"])

    (art / "rendered.html").write_text(rendered[:2_000_000], encoding="utf-8")
    (art / "digest.json").write_text(
        json.dumps(digest, ensure_ascii=False, indent=2), encoding="utf-8")
    return digest


def _js_dependence(url: str, rendered_links: int) -> dict:
    """How much of the page only exists after JavaScript runs.

    This is the number that settles the "can't we just use requests?" question
    per site, instead of arguing about it. On SBP the raw HTML has a fraction of
    the links the rendered page does.
    """
    try:
        import re as _re
        import requests
        raw = requests.get(url, headers={"User-Agent": UA}, timeout=30).text
        raw_links = len(_re.findall(r"<a\s[^>]*href=", raw, _re.I))
        return {
            "raw_links": raw_links,
            "rendered_links": rendered_links,
            "verdict": ("javascript-rendered — requests-only inspection would miss the content"
                        if rendered_links > max(20, raw_links * 1.5)
                        else "static enough that raw HTML shows the content"),
        }
    except Exception as e:                        # never block a proposal on this
        return {"error": str(e)[:200]}


def digest_for_prompt(digest: dict) -> str:
    """The digest, trimmed to what the model needs to fill the form.

    Deliberately small. If this grows past a few KB, the failure mode of the old
    onboarding path is creeping back in.
    """
    keep = {
        "url": digest.get("url"),
        "title": digest.get("title"),
        "h1": digest.get("h1"),
        "breadcrumb": digest.get("breadcrumb"),
        "total_links": digest.get("total_links"),
        "doc_links": digest.get("doc_links"),
        "iframes": digest.get("iframes"),
        "row_candidates": digest.get("row_candidates", [])[:6],
        "section_levels": digest.get("section_levels", []),
        "heading_groups": digest.get("heading_groups", []),
        "tree_candidates": digest.get("tree_candidates", []),
        "expand_candidates": digest.get("expand_candidates", []),
        "pagination_candidates": digest.get("pagination_candidates", []),
        "next_buttons": digest.get("next_buttons"),
        "tables": digest.get("tables"),
        "page_size_control": digest.get("page_size_control"),
    }
    return json.dumps(keep, ensure_ascii=False, indent=2)


def print_digest(digest: dict) -> None:
    """Human summary — run `formfill inspect` before involving an LLM at all.
    On an easy site the answer is already obvious here."""
    print(f"\n{digest.get('title', '')}\n{digest.get('url', '')}")
    jd = digest.get("js_dependence") or {}
    if jd.get("verdict"):
        print(f"  {jd['raw_links']} links in raw HTML vs {jd['rendered_links']} rendered "
              f"-> {jd['verdict']}")
    print(f"  {digest.get('total_links')} links, {digest.get('doc_links')} look like documents, "
          f"{digest.get('iframes')} frames")

    print("\n  ROW CANDIDATES (which repeated element is one entry?)")
    for c in digest.get("row_candidates", [])[:5]:
        print(f"    {c['selector']:<28} x{c['count']:<5} links:{c['with_link']:<4} "
              f"docs:{c.get('with_doc_link', 0):<4} dated:{c['dated']:<4} "
              f"ref:{c['ref_like']:<4} avg {c['avg_text_len']} chars")
        if c["samples"]:
            print(f"        e.g. {c['samples'][0][:120]}")

    if digest.get("tree_candidates"):
        print("\n  TREE CANDIDATES (a nested menu — each node is its own page)")
        for t in digest["tree_candidates"]:
            print(f"    menu {t['menu_selector']:<24} node {t['node_selector']:<18} "
                  f"{t['nodes']} nodes, {t['nested_nodes']} nested, depth {t['max_depth']}"
                  + (f", x{t['menus_on_page']} such menus" if t["menus_on_page"] > 1 else ""))
            if t["samples"]:
                print(f"        e.g. {' | '.join(s for s in t['samples'] if s)[:110]}")
        for e in digest.get("expand_candidates", []):
            print(f"    expand control: {e['selector']}  (x{e['count']})")

    if digest.get("section_levels"):
        print("\n  SECTION LEVELS (the site's own hierarchy, outermost first)")
        for g in digest["section_levels"]:
            print(f"    {g['ancestor']:<28} title {g['title']:<4} "
                  f"x{g['groups_on_page']} groups, {g['rows_inside']} rows inside "
                  f"— e.g. {g['sample_title']!r}")

    for hg in digest.get("heading_groups", []):
        print(f"\n  SECTION HEADINGS (no wrapper — use preceding: {hg['preceding']})")
        for g in hg["groups"][:8]:
            print(f"    {g['title']:<45} {g['rows_after']} rows follow it")

    print("\n  PAGINATION CANDIDATES")
    for p in digest.get("pagination_candidates", []):
        print(f"    {p['pattern']}   numbers seen: {p['numbers']}")
    for b in digest.get("next_buttons", []):
        print(f"    next control: {b['selector']}  ({b['text']})")
    for t in digest.get("tables", []):
        print(f"    <table> with {t['rows']} rows  headers={t['headers']}")
    for s in digest.get("page_size_control", []):
        print(f"    page-size control {s['selector']}  options={s['options']}")
    print()
