"""
Shape-aware strategies for the generic crawler.

Regulator tabs come in a few recurring LAYOUTS ("shapes"). One link-walk does not
fit all, so the generic crawler now DETECTS the shape of the seed page and picks a
strategy:

  * "table"   -> a paginated list/table of documents  (e.g. SAMA Circulars, SECP
                 acts/notifications). We find the biggest document table, click
                 through its pagination, and read each row into a document.
  * "tree"    -> a nested rulebook/book hierarchy      (e.g. SAMA Rulebook). We walk
                 the outline recursively, build section_path from the breadcrumb,
                 and take each leaf's clean body (.node__content) + Original PDF.
  * "generic" -> anything else -> the crawler's existing BFS link-walk.

This module is ADDITIVE: it only adds detect_shape() + crawl_tree() + crawl_table();
crawl() falls back to its old behaviour when the shape is "generic" or forced off.
Records/documents are returned in the exact format _write_excel() expects.
"""
import json
import re, hashlib
from collections import deque
from urllib.parse import urlparse

# One WAF check for both engines. It lives in its own module because crawler.py
# imports THIS file and never the reverse, so a helper both use cannot live in
# either of them.
try:
    from blockcheck import blocked_reason
except ImportError:  # when imported as a package
    from .blockcheck import blocked_reason

# --------------------------------------------------------------------------- #
# Shape detection
# --------------------------------------------------------------------------- #
JS_SHAPE = r"""() => {
  const host = location.host;
  let maxRows = 0;                       // biggest "document table" (rows with a link)
  document.querySelectorAll('table').forEach(t => {
    let n = 0;
    t.querySelectorAll('tbody tr').forEach(r => { if (r.querySelector('a[href]')) n++; });
    if (n > maxRows) maxRows = n;
  });
  const body = document.querySelector('.node__content');
  const hasNodeContent = !!body;
  // Only REAL Drupal book markers. `li.menu-item` was dropped: it is the class any
  // nav menu uses, so any site with a nav bar looked like a rulebook -- MHRSD's 40
  // such links made it 'tree', and crawl_tree() then returned 0 pages, 0 documents.
  // SECP has 20 and survives only because the table rule fires first.
  // Measured 2026-08-03 (real book / generic nav links): SAMA sandbox 15/33, SAMA
  // CB law 41/35 -- both keep 'tree'; SECP 0/20, MHRSD 0/40, SBP/MISA/SDAIA 0/0 --
  // unchanged. Gated by calibrate_shape.py.
  const hasBookMenu = document.querySelectorAll('.book-block-menu a[href], .book-navigation a[href], nav[id^=book-block-menu-] a[href], [id^="book-navigation"] a[href]').length >= 5;
  const hasBreadcrumb = !!document.querySelector('.breadcrumb a, .bread-crumb a');
  const hasDataTables = document.querySelectorAll('.dt-paging-button, .dataTables_paginate, .paginate_button').length > 0;
  // same-host content links in the MAIN area (exclude chrome + article citations) --
  // these are the outline children of a category/tree page.
  const isChrome = e => e.closest('header,nav,footer,.breadcrumb,.bread-crumb,.disp_toolbar,.book-pager');
  const childUrls = []; const seen = new Set();
  document.querySelectorAll('a[href]').forEach(a => {
    const h = a.href;
    try { if (new URL(h).host !== host) return; } catch(e) { return; }
    if (h.includes('#') || /\.pdf|\.docx?|\.xlsx?/i.test(h)) return;
    if (isChrome(a) || (body && body.contains(a))) return;
    if (/\/entiresection\/|\/revisions\//.test(h)) return;
    if (seen.has(h)) return; seen.add(h);
    // Never PROBE a pagination link. SBP's first "child" is /circulars/P30 —
    // page 2 of the list — which says nothing about whether this is a tree, and
    // is what the shape probe used to make its decision on.
    if (/\/P\d+$|\/page\/\d+|[?&]page=\d+/i.test(h)) return;
    if (childUrls.length < 8) childUrls.push(h);
  });
  return {maxRows, hasNodeContent, hasBookMenu, hasBreadcrumb, hasDataTables,
          outlineLinks: seen.size, childUrls};
}"""

# Is this child page a node of a rulebook TREE?
#
# The breadcrumb test that used to live here ("2 or more crumb links") was far too
# weak: essentially every site has a "Home > Section" breadcrumb. SBP circulars —
# a flat list site — was classified as a tree on that basis alone, handed to
# crawl_tree, which found no book menu and returned 0 pages and 0 documents.
#
# Only real tree markers count now: a Drupal book body, or a sizeable book/menu
# navigation. Both SAMA tabs are detected earlier by strong signals on the seed
# itself (hasNodeContent + hasBookMenu), so tightening this costs them nothing.
# `li.menu-item` was removed here too, and had to be: fixing JS_SHAPE alone left
# MHRSD falling through to this probe, whose children carry the same 40 nav links
# and 0 book markers -- misread as a tree by a second rule. Now false for MHRSD;
# SDAIA already was; SECP/SBP decided earlier; SAMA never reaches here (no children).
JS_IS_TREE_NODE = r"""() => {
  if (document.querySelector('.node__content')) return true;
  const menu = document.querySelectorAll(
    '.book-block-menu a[href], .book-navigation a[href], '
    + 'nav[id^=book-block-menu-] a[href], [id^="book-navigation"] a[href]').length;
  return menu >= 5;
}"""

TABLE_MIN_ROWS = 8
LIST_MIN_ROWS = 8

# ---------------------------------------------------------------------------
# LIST shape — a paginated listing that is NOT built from <table>.
#
# SBP circulars is the case this exists for: 4,160 entries over 139 pages, and
# ZERO <table> elements. Rows are repeated `h4.mb-2` blocks. Because maxRows was
# 0, detect_shape could never call it a table, so it fell to the generic BFS —
# which mixes listing pages and detail pages together and burns the page cap on
# both. It reached 101 of 4,160.
#
# Behaviourally SBP is identical to SECP: walk the rows, page by page, and open
# each row's link. Only the markup differs. So this finds "rows" structurally
# rather than by tag name.
# ---------------------------------------------------------------------------
JS_LIST_ROWS = r"""() => {
  const sig = el => { const c=(el.className&&el.className.toString)?el.className.toString().trim():'';
    return el.tagName + (c ? '.'+c.split(/\s+/).slice(0,2).join('.') : ''); };
  // Site furniture is not a listing. SBP's mega-menu alone yields 124 bare <a>
  // "rows" and outnumbers the 30 real entries 4 to 1.
  const chrome = el => !!el.closest(
    'header, footer, nav, [role="banner"], [role="contentinfo"], [role="navigation"]');
  const DATE = /\b(20\d{2}|19\d{2})\b|\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)/i;

  const tally = {};
  for (const a of document.querySelectorAll('a[href]')) {
    const t = (a.innerText || '').trim();
    if (t.length < 12 || chrome(a)) continue;          // skip 'Next', 'PDF', icons
    let el = a;
    for (let i=0;i<4&&el;i++){ const s=sig(el); (tally[s]=tally[s]||[]).push({el,a}); el=el.parentElement; }
  }
  const uniq = items => new Set(items.map(x=>x.el)).size;
  const cands = Object.entries(tally).filter(([s,v]) => uniq(v) >= 8);
  if (!cands.length) return {sig:null, rows:[]};
  // A bare tag name is too generic to be a row signature; prefer a classed one.
  cands.sort((a,b) => ((b[0].includes('.')?1000:0)+uniq(b[1]))
                    - ((a[0].includes('.')?1000:0)+uniq(a[1])));

  const build = ([s, items]) => {
    const seen = new Set(), rows = [];
    for (const {el,a} of items) {
      if (seen.has(el)) continue; seen.add(el);
      // Widen from the title element to the whole row: climb while the parent
      // does NOT contain a second row. That is what picks up the reference
      // number and date lines sitting beside the title.
      let row = el;
      for (let i=0;i<3;i++){ const p=row.parentElement; if(!p) break;
        if (Array.from(p.querySelectorAll('*')).filter(x=>sig(x)===s).length > 1) break;
        row = p; }
      rows.push({ title:(a.innerText||'').trim().slice(0,300), href:a.href,
                  row_text:(row.innerText||'').replace(/\s+/g,' ').trim().slice(0,600) });
    }
    return rows;
  };

  // Prefer a candidate whose rows carry DATES — that is what separates a document
  // listing from a list of navigation links.
  let chosen = null, chosenRows = null;
  for (const c of cands.slice(0,5)) {
    const rows = build(c);
    const dated = rows.filter(r => DATE.test(r.row_text)).length;
    if (dated >= Math.max(3, rows.length * 0.5)) { chosen = c; chosenRows = rows; break; }
    if (!chosen) { chosen = c; chosenRows = rows; }
  }
  return { sig: chosen[0], rows: chosenRows,
           dated: chosenRows.filter(r=>DATE.test(r.row_text)).length };
}"""

# Pagination hrefs, in both the forms seen so far: /P30 (offset) and ?page=2.
JS_PAGER_HREFS = r"""() => [...new Set(Array.from(document.querySelectorAll('a[href]'))
  .map(a => a.href)
  .filter(h => /[?&\/]P\d+\b|[?&]page=\d+|\/page\/\d+/i.test(h)))].slice(0, 60)"""


def detect_shape(page, ctx=None):
    """Classify the seed page: 'table' | 'list' | 'tree' | 'generic'.
    When markup is ambiguous (a tree ROOT has few signals), probe one child."""
    try:
        s = page.evaluate(JS_SHAPE)
    except Exception:
        return "generic"
    # A real document table wins first: a circulars page under a rulebook has BOTH a
    # table and a book menu -- the table is the content.
    if s.get("maxRows", 0) >= TABLE_MIN_ROWS:
        return "table"
    # Strong tree signals on THIS page.
    if s.get("hasBookMenu") or (s.get("hasNodeContent") and s.get("outlineLinks", 0) >= 3):
        return "tree"
    # A table without <table>: many repeated, DATED rows outside the site chrome.
    # Checked after tree so a rulebook page listing its children is not mistaken
    # for a document listing.
    try:
        lst = page.evaluate(JS_LIST_ROWS)
        if (lst.get("rows") and len(lst["rows"]) >= LIST_MIN_ROWS
                and lst.get("dated", 0) >= max(3, len(lst["rows"]) * 0.5)):
            return "list"
    except Exception:
        pass
    # Ambiguous root (category landing): confirm by probing a child node.
    kids = s.get("childUrls") or []
    if ctx is not None and len(kids) >= 3:
        pr = ctx.new_page()
        try:
            pr.goto(kids[0], wait_until="domcontentloaded", timeout=60000); pr.wait_for_timeout(2500)
            if pr.evaluate(JS_IS_TREE_NODE):
                return "tree"
        except Exception:
            pass
        finally:
            pr.close()
    return "generic"

def _clean(s): return re.sub(r"\s+", " ", s or "").strip()
def _sha(b): return hashlib.sha1(b).hexdigest()

# --------------------------------------------------------------------------- #
# TREE strategy  (ported from the validated SAMA rulebook runner)
# --------------------------------------------------------------------------- #
JS_TREE_NODES = r"""() => {
  const clean = s => (s||'').replace(/\s+/g,' ').trim();
  const cur = location.href.replace(/\/$/,'');
  const isDoc = h => /\.pdf(\?|$)|\.docx?|\.xlsx?|wpdmdl|LegalPDF|\/Media\/|AdvancedSearchDetails/i.test(h);
  const children = [], docs = []; const seenC = new Set(), seenD = new Set();
  const skip = t => /^(up|up to|home|entire section|custom print|text only|rich text|print \/ save as pdf|versions|download original pdf)$/i.test(t);
  const addChild = a => { const h=a.href, t=clean(a.innerText);
    if(!t || t.length<2 || h.includes('#') || skip(t) || seenC.has(h)) return;
    if(/\/entiresection\/|\/revisions\//.test(h)) return;
    seenC.add(h); children.push({title:t, url:h}); };
  const addDoc = a => { const h=a.href, t=clean(a.innerText);
    if(!t || h.includes('#') || seenD.has(h)) return; seenD.add(h); docs.push({title:t, url:h}); };

  // (1) Drupal "book" navigation: find the ACTIVE node (the <li> whose link == this page)
  //     and take ONLY its DIRECT child <ul>. That is this node's children -- following
  //     just these walks the parent->child tree without wandering the whole rulebook.
  document.querySelectorAll('nav[id^=book-block-menu-], .book-block-menu, .book-navigation, nav.book').forEach(nav => {
    nav.querySelectorAll('li').forEach(li => {
      const a = li.querySelector(':scope > a[href]') || li.querySelector('a[href]');
      if (!a || a.href.replace(/\/$/,'') !== cur) return;      // active node = current page
      const ul = li.querySelector(':scope > ul');               // its direct children
      if (ul) ul.querySelectorAll(':scope > li > a[href]').forEach(a2 => {
        if (isDoc(a2.href)) addDoc(a2); else addChild(a2);
      });
    });
  });

  // (2) Content nav lists: landing pages (e.g. CBB "marketlist") whose children are
  //     regular links inside .node__content, plus any document links there.
  const body = document.querySelector('.node__content');
  if (body) body.querySelectorAll('ul > li > a[href]').forEach(a => {
    if (isDoc(a.href)) addDoc(a); else if (!a.closest('.node__content p')) addChild(a);
  });

  // (3) Any document links inside the content (PDFs / legal-doc pages).
  (body || document).querySelectorAll('a[href]').forEach(a => { if (isDoc(a.href)) addDoc(a); });

  return {children, docs};
}"""

JS_TREE_LEAF = r"""() => {
  const clean = s => (s||'').replace(/\s+/g,' ').trim();
  let bc = []; const bcEl = document.querySelector('.breadcrumb, .bread-crumb');
  if (bcEl) Array.from(bcEl.querySelectorAll('a,span,li')).map(x=>clean(x.innerText)).filter(Boolean)
            .forEach(t => { if (bc[bc.length-1] !== t) bc.push(t); });
  const notFound = /requested page could not be found/i.test(document.body.innerText||'');
  const pdf = document.querySelector('a.icopdf') || document.querySelector('.node__content a[href*=".pdf" i]');
  const title = clean((document.querySelector('h1, h2.page-title, .page-title')||{}).innerText||'');
  const src = document.querySelector('.node__content') || document.querySelector('content')
            || document.querySelector('article') || document.body;
  const cl = src.cloneNode(true);
  cl.querySelectorAll('nav,script,style,button,a.icopdf,.icopdf,header,.page-title,.info-table,.book-notification,.show-previous,.show-next,[class*=traversal i],[id*=associated_pdf i],[id*=revision_block i],[class*=revisionblock i]').forEach(e=>e.remove());
  let html = (cl.innerHTML||'').trim(); if (html.length > 400000) html = html.slice(0,400000);
  const body_text_len = (cl.textContent||'').replace(/\s+/g,' ').trim().length;
  const text = (cl.textContent||'').replace(/\s+/g,' ').trim();
  return {breadcrumb: bc, title, not_found: notFound, pdf_url: pdf?pdf.href:'',
          document_html: html, body_text_len, text};
}"""

def crawl_tree(ctx, seed_norm, out_dir, max_pages=400, max_depth=12, wait_ms=2500):
    """Recursive rulebook-tree walk. Returns (records, documents, note)."""
    note = {"blocked_pages": 0, "errors": 0, "stopped": "", "resume": {}}
    seed_host = urlparse(seed_norm).netloc.lower()
    # scope anchor = the seed's own breadcrumb leaf (keeps us inside this section)
    p0 = ctx.new_page()
    try:
        p0.goto(seed_norm, wait_until="domcontentloaded", timeout=90000); p0.wait_for_timeout(wait_ms)
        # A blocked seed makes the anchor — and therefore the scope of the whole
        # walk — the WAF's idea of a breadcrumb.
        reason = blocked_reason(p0)
        if reason:
            note["blocked_pages"] += 1
            note["stopped"] = f"seed blocked by bot protection ({reason})"
            print(json.dumps({"event": "blocked", "url": seed_norm,
                              "reason": reason}), flush=True)
        seed_leaf = p0.evaluate(JS_TREE_LEAF)
    finally:
        p0.close()
    seed_bc = seed_leaf["breadcrumb"]
    # scope anchor: breadcrumb leaf if the site HAS breadcrumbs (SAMA). If the site has
    # no breadcrumbs (CBB), leave anchor empty and rely on content-only children +
    # same-host + max_pages to bound the section.
    anchor = (seed_bc[-1].lower() if seed_bc else "")

    records, documents, seen = [], {}, {seed_norm}
    q = deque([(seed_norm, 0)])
    while q and len(records) < max_pages:
        url, depth = q.popleft()
        pg = ctx.new_page()
        try:
            pg.goto(url, wait_until="domcontentloaded", timeout=90000)
            pg.wait_for_timeout(wait_ms)
            # wait for the article content to actually render (CBB/Drupal can be slow)
            try:
                pg.wait_for_function(
                    "() => document.querySelector('.node__content, article, main') && document.body.innerText.length > 300",
                    timeout=8000)
            except Exception:
                pass
            reason = blocked_reason(pg)
            if reason:
                note["blocked_pages"] += 1
                if note["blocked_pages"] <= 3:
                    print(json.dumps({"event": "blocked", "url": url,
                                      "reason": reason}), flush=True)
                pg.close(); continue
            d = pg.evaluate(JS_TREE_LEAF); nodes = pg.evaluate(JS_TREE_NODES)
        except Exception:
            note["errors"] += 1
            pg.close(); continue
        pg.close()
        if d["not_found"]: continue
        bc = d["breadcrumb"]
        in_scope = (url == seed_norm) or (not anchor) or (anchor in [x.lower() for x in bc])
        if not in_scope:
            continue
        title = d["title"]
        section_path = " > ".join(bc + ([title] if title and (not bc or bc[-1] != title) else []))
        # This page is a content leaf if it has real prose or its own Original PDF.
        has_content = bool(d["pdf_url"]) or d["body_text_len"] > 200
        if has_content and url != seed_norm:
            # SCHEMA NOTE: "text" is PLAIN TEXT and "html" is HTML — the same as
            # the generic walk produces. This used to put document_html into
            # "text", which silently fed HTML to anything expecting prose (the
            # pipeline branches on exactly that distinction).
            # n_children tells a FOLDER from a SHORT DOCUMENT — the two look
            # identical by text length alone. "Chapter 3: Monetary Policy" holds
            # 10 characters and one child (a folder); "Article 3" holds 184
            # characters and no children (a real, if brief, article). Judging on
            # length alone would drop the article along with the folder.
            records.append({
                "section_path": section_path, "title": title, "url": url, "depth": depth,
                "linked_from_title": "", "parent_page_url": "",
                "status": "", "n_pdfs": 1 if d["pdf_url"] else 0,
                "pdf_links": d["pdf_url"], "text_len": len(d["text"]),
                "html_file": "", "text": d["text"], "html": d["document_html"],
                "breadcrumb": bc,
                "n_children": len(nodes.get("children", [])),
            })
            if d["pdf_url"]:
                documents[d["pdf_url"]] = {"title": title, "doc_url": d["pdf_url"], "type": "PDF",
                                           "found_on": url, "section_path": section_path}
        # Document links listed on this page (PDFs / legal-doc pages) -> record each,
        # with its own link text as the title (fixes CBB "resolution not found / no title").
        for dl in nodes.get("docs", []):
            if dl["url"] in documents:
                continue
            documents[dl["url"]] = {
                "title": dl["title"], "doc_url": dl["url"],
                "type": "PDF" if re.search(r"\.pdf", dl["url"], re.I) else "DOC",
                "found_on": url, "section_path": (section_path + " > " + dl["title"])[:400],
            }
        # Recurse into sub-section links (menu outline or in-body nav list).
        if depth < max_depth:
            for k in nodes.get("children", []):
                ku = k["url"]
                if ku not in seen and urlparse(ku).netloc.lower() == seed_host:
                    seen.add(ku); q.append((ku, depth + 1))
    if len(records) >= max_pages and q:
        note["stopped"] = (f"page cap: {len(records)} of max_pages={max_pages}, "
                           f"{len(q)} nodes still queued")
        note["resume"] = {"pages_walked": len(records), "queued": len(q),
                          "next_urls": [u for u, _ in list(q)[:5]]}
    return records, list(documents.values()), note

# --------------------------------------------------------------------------- #
# TABLE strategy  (ported from the validated SAMA circulars / SECP runners)
# --------------------------------------------------------------------------- #
JS_TABLE_ROWS = r"""() => {
  const clean = s => (s||'').replace(/\s+/g,' ').trim();
  const dateRe = /\d{1,2}[\/\-\s][A-Za-z0-9]+[\/\-\s]\d{2,4}|\d{4}-\d{2}-\d{2}/;
  const t = Array.from(document.querySelectorAll('table'))
      .sort((a,b) => b.querySelectorAll('tbody tr').length - a.querySelectorAll('tbody tr').length)[0];
  if (!t) return {header: [], rows: []};
  const header = Array.from(t.querySelectorAll('thead th, thead td')).map(x=>clean(x.innerText));
  const rows = Array.from(t.querySelectorAll('tbody tr')).map(r => {
    const cells = Array.from(r.querySelectorAll('td')).map(td => clean(td.innerText));
    const a = r.querySelector('a[href]');
    // best title cell = longest non-date, non-"download" cell; else the link text
    let title = '';
    cells.forEach(c => { if (c && !/^download$/i.test(c) && !dateRe.test(c) && c.length > title.length) title = c; });
    if (!title && a) title = clean(a.innerText);
    const dcell = cells.find(c => dateRe.test(c)) || '';
    return {cells, title, date: dcell, href: a ? a.href : ''};
  }).filter(x => x.title || x.href);
  return {header, rows};
}"""

def _table_show_all_and_pages(page, wait_ms):
    """Maximise DataTables length, then click Next, harvesting rows each draw."""
    try:
        page.evaluate("""() => {
          const sel = document.querySelector('select[name$=_length], .dataTables_length select');
          if (sel) { const opts = Array.from(sel.options).map(o=>o.value);
            const all = opts.find(v=>v==='-1') || opts.map(Number).filter(n=>!isNaN(n)).sort((a,b)=>b-a)[0];
            if (all !== undefined) { sel.value = String(all); sel.dispatchEvent(new Event('change',{bubbles:true})); } }
        }""")
        page.wait_for_timeout(1200)
    except Exception:
        pass
    seen = {}
    def harvest():
        data = page.evaluate(JS_TABLE_ROWS)
        for r in data["rows"]:
            seen[(r["href"], r["title"])] = r
        return data["header"]
    header = harvest()
    for _ in range(80):
        before = len(seen)
        nxt = page.query_selector(".dt-paging-button.next:not(.disabled), .paginate_button.next:not(.disabled), a.next:not(.disabled), [rel=next]:not(.disabled)")
        if not nxt:
            break
        try:
            nxt.click(); page.wait_for_timeout(max(900, wait_ms))
        except Exception:
            break
        harvest()
        if len(seen) == before:
            break
    return header, list(seen.values())

def crawl_table(ctx, seed_norm, out_dir, max_pages=5000, wait_ms=1200):
    """Paginated document-table extraction. Returns (records, documents, note)."""
    note = {"blocked_pages": 0, "errors": 0, "stopped": "", "resume": {}}
    page = ctx.new_page()
    try:
        page.goto(seed_norm, wait_until="domcontentloaded", timeout=90000); page.wait_for_timeout(max(3000, wait_ms))
        try: page.mouse.wheel(0, 2500); page.wait_for_timeout(800)
        except Exception: pass
        # A table site is ONE page: if it is a challenge page there is nothing
        # else to harvest, so stop rather than reading rows off the WAF.
        reason = blocked_reason(page)
        if reason:
            note["blocked_pages"] += 1
            note["stopped"] = f"blocked by bot protection ({reason})"
            print(json.dumps({"event": "blocked", "url": seed_norm,
                              "reason": reason}), flush=True)
            return [], [], note
        # section label from breadcrumb leaf or <h1>
        sec = page.evaluate(r"""() => {
          const bc = document.querySelectorAll('.breadcrumb a, .bread-crumb a');
          if (bc.length) return (bc[bc.length-1].innerText||'').trim();
          const h = document.querySelector('h1'); return h ? (h.innerText||'').trim() : '';
        }""") or "Documents"
        header, rows = _table_show_all_and_pages(page, wait_ms)
    finally:
        page.close()

    documents = {}
    for r in rows[:max_pages]:
        key = r["href"] or (r["title"] + r["date"])
        if key in documents:
            continue
        documents[key] = {
            "title": r["title"], "doc_url": r["href"], "type": "PDF" if re.search(r"\.pdf|wpdmdl|/document|/node/", r["href"] or "", re.I) else "LINK",
            "date": r["date"], "found_on": seed_norm, "section_path": sec,
        }
    # One synthetic page row standing for the whole table. Same key set as every
    # other walker so downstream code never has to ask which shape produced it.
    records = [{
        "section_path": sec, "title": sec, "url": seed_norm, "depth": 0,
        "linked_from_title": "", "parent_page_url": "", "status": "",
        "n_pdfs": len(documents), "pdf_links": "", "text_len": 0, "html_file": "",
        "text": f"[table shape] {len(documents)} documents across {len(header)} columns: {header}",
        "html": "", "breadcrumb": [sec] if sec else [],
    }]
    if len(rows) > max_pages:
        note["stopped"] = (f"row cap: kept {max_pages} of {len(rows)} table rows")
        note["resume"] = {"rows_kept": max_pages, "rows_found": len(rows)}
    return records, list(documents.values()), note


# --------------------------------------------------------------------------- #
# LIST strategy — paginated listing + detail pages
#
# Two phases, deliberately separable:
#
#   PHASE 1  walk the pagination only, harvesting each row's title, link and the
#            row text (which carries the reference number, date and department).
#            SBP: 139 pages, ~20 minutes, and it yields a COMPLETE inventory of
#            all 4,160 circulars.
#
#   PHASE 2  open each row's detail page for its HTML. SBP: 4,160 more page
#            loads. This is the expensive half.
#
# Keeping them separate is the point. Phase 1 alone answers "what is new since
# last time" — the listing IS the change feed — so phase 2 only has to run for
# rows that are actually new. Same saving CBB gets from its Thomson Reuters feed,
# available on any list site without the regulator's cooperation.
# --------------------------------------------------------------------------- #

JS_DETAIL_CONTENT = r"""() => {
  const pick = document.querySelector('main, [role="main"], article, #content, .content, #main');
  const src = pick || document.body || document.documentElement;
  if (!src) return {html:'', text:'', links:[]};
  const clone = src.cloneNode(true);
  clone.querySelectorAll('script,style,noscript,nav,aside,header,footer,form').forEach(n=>n.remove());
  const links = Array.from(src.querySelectorAll('a[href]'))
    .filter(a => !a.closest('header, footer, nav, [role="banner"], [role="contentinfo"]'))
    .map(a => ({href:a.href, text:(a.innerText||'').replace(/\s+/g,' ').trim().slice(0,200)}));
  return {html: clone.innerHTML, text:(clone.innerText||'').trim(), links};
}"""

_DOC_EXT_RE = re.compile(r"\.(pdf|docx?|xlsx?|pptx?|csv|zip)(\?|$)", re.I)


def _is_doc(url):
    return bool(_DOC_EXT_RE.search(url)) or bool(
        re.search(r"wpdmdl=|/document/|/download/", url, re.I))


def _ext_type(url):
    m = _DOC_EXT_RE.search(url)
    return m.group(1).upper() if m else "DOC"


def _pager_offsets(hrefs):
    """Turn the handful of pager links a page shows into the FULL page sequence.

    SBP renders only "1 2 3 ... 139", whose hrefs are /P30, /P60 and /P4140. From
    those: step = 30, last = 4140, so the real sequence is P30, P60 ... P4140 —
    139 pages. Without this we would only ever visit the three it links to.
    Returns [] when there is no offset pattern.
    """
    pat = re.compile(r"^(.*?[?&/])P(\d+)\b(.*)$", re.I)
    seen = {}
    for h in hrefs:
        m = pat.match(h)
        if m:
            seen[int(m.group(2))] = (m.group(1), m.group(3))
    if len(seen) < 2:
        return []
    nums = sorted(seen)
    prefix, suffix = seen[nums[0]]
    steps = [b - a for a, b in zip(nums, nums[1:]) if b > a]
    step = min(steps) if steps else 0
    if step <= 0 or max(nums) // step > 5000:      # runaway guard
        return []
    return [f"{prefix}P{n}{suffix}" for n in range(step, max(nums) + 1, step)]


class _ResilientPage:
    """A page that survives what a long crawl does to a browser.

    Two failure modes, both seen on SBP and both fatal to a plain page object:
      * the resolver blips and goto() raises ERR_NAME_NOT_RESOLVED
      * Chromium crashes outright ("Page crashed"), after which EVERY later call
        on that page object throws — so the crawl dies, not just the one URL

    A 4,160-page walk cannot assume neither happens. On a crash the page is
    thrown away and replaced; on a blip it just retries. An empty render counts
    as a failure too, or a flaky page gets recorded as "this listing has no rows".
    """

    def __init__(self, ctx, wait_ms):
        self.ctx = ctx
        self.wait_ms = wait_ms
        self.crashes = 0
        self.dead = False          # the whole context died; nothing more is possible
        self.blocked = 0           # loads that came back as a bot-protection wall
        self.load_failures = 0     # loads that never arrived at all
        try:
            self.page = ctx.new_page()
        except Exception:
            self.page, self.dead = None, True

    def _recreate(self):
        self.crashes += 1
        try:
            self.page.close()
        except Exception:
            pass
        try:
            self.page = self.ctx.new_page()
        except Exception:
            # A hard Chromium crash takes the CONTEXT with it, so even opening a
            # fresh page fails. Nothing here can recover that — the caller must
            # stop and report a PARTIAL crawl rather than raising, because
            # partial results are still worth keeping and a crash must never look
            # like "this site has no documents".
            self.page, self.dead = None, True

    # Chromium leaks across hundreds of navigations and eventually dies. Cheap
    # insurance: throw the page away every N loads and start a clean one.
    RECYCLE_EVERY = 40

    def load(self, url, tries=3):
        if self.dead:
            return False
        self.loads = getattr(self, "loads", 0) + 1
        if self.loads % self.RECYCLE_EVERY == 0:
            self._recreate()
            if self.dead:
                return False
        for attempt in range(tries):
            try:
                self.page.goto(url, wait_until="domcontentloaded", timeout=90000)
                self.page.wait_for_timeout(self.wait_ms)
                for _ in range(2):
                    self.page.mouse.wheel(0, 6000)
                    self.page.wait_for_timeout(250)
                if self.page.evaluate(
                        "()=>document.querySelectorAll('a[href]').length") > 15:
                    # One check here covers every page a list crawl touches —
                    # both phases, seed included. Retrying a challenge page only
                    # convinces the WAF harder, so report it and give up on it.
                    reason = blocked_reason(self.page)
                    if reason:
                        self.blocked += 1
                        if self.blocked <= 3:
                            print(json.dumps({"event": "blocked", "url": url,
                                              "reason": reason}), flush=True)
                        return False
                    return True
            except Exception as e:
                msg = str(e).lower()
                if "crash" in msg or "closed" in msg or "target" in msg:
                    self._recreate()
                    if self.dead:
                        self.load_failures += 1
                        return False
            if attempt < tries - 1:
                try:
                    self.page.wait_for_timeout(1500)
                except Exception:
                    self._recreate()
        self.load_failures += 1
        return False

    def evaluate(self, js):
        return self.page.evaluate(js)

    def close(self):
        # A page whose browser has died does not raise on close() — it BLOCKS,
        # waiting for a process that will never answer. That hung the crawler
        # after a completed walk, so the results were never written. There is
        # nothing to tidy up on a dead browser anyway.
        if self.dead or self.page is None:
            return
        try:
            self.page.close()
        except Exception:
            pass


def crawl_list(ctx, seed_norm, out_dir, max_pages=200, wait_ms=1200,
               fetch_details=True, max_details=None):
    """Walk a paginated listing; optionally open each entry.

    Returns (records, documents, note) in the same schema as every other walker.
    `note` is what the walk learned about itself — blocked pages, failed loads,
    and where it stopped — which crawler.py turns into the run's status."""

    note = {"blocked_pages": 0, "errors": 0, "stopped": "", "resume": {}}

    # ---------------- PHASE 1: the listing ----------------
    page = _ResilientPage(ctx, wait_ms)
    rows, seen_href, list_pages = [], set(), [seed_norm]
    section = "Documents"
    try:
        if page.load(seed_norm):
            try:
                section = _clean(page.evaluate(
                    "()=>{const h=document.querySelector('h1');return h?(h.innerText||''):''}")
                ) or "Documents"
            except Exception:
                pass
            try:
                list_pages += _pager_offsets(page.evaluate(JS_PAGER_HREFS))
            except Exception:
                pass
            list_pages = list(dict.fromkeys(list_pages))[:max_pages]
            total = len(list_pages)
            for i, lp in enumerate(list_pages, 1):
                if i > 1 and not page.load(lp):
                    if page.dead:
                        # INCOMPLETE was already the honest word here; it just had
                        # nowhere to go but stdout. Now it reaches the run status,
                        # with the page number a resumed run would restart from.
                        note["stopped"] = (f"browser died on listing page {i} of "
                                           f"{total} — listing INCOMPLETE")
                        note["resume"] = {"listing_page": i, "of": total,
                                          "next_url": lp}
                        print(json.dumps({"event": "error", "url": lp,
                                          "message": "browser died — listing INCOMPLETE",
                                          "pages_done": i - 1, "of": total}),
                              flush=True)
                        break
                    continue
                try:
                    res = page.evaluate(JS_LIST_ROWS)
                except Exception:
                    continue
                new = 0
                for r in (res.get("rows") or []):
                    h = r.get("href") or ""
                    if not h.startswith("http") or h in seen_href:
                        continue
                    seen_href.add(h)
                    rows.append(r)
                    new += 1
                if i == 1 or i == total or i % 10 == 0:
                    print(json.dumps({"event": "list_page", "page": i, "of": total,
                                      "rows": len(rows), "new": new},
                                     ensure_ascii=False), flush=True)
    finally:
        page.close()

    records, documents = [], {}

    def _row_record(r, extra=None):
        rec = {
            "section_path": section, "title": r["title"], "url": r["href"],
            "depth": 1, "linked_from_title": r["title"], "parent_page_url": seed_norm,
            "status": "", "n_pdfs": 0, "pdf_links": "", "text_len": 0,
            "html_file": "", "text": "", "html": "",
            "breadcrumb": [section] if section else [],
            # The listing row, verbatim. It carries the reference number, date and
            # department that the detail page often does not repeat — metadata a
            # link walk can never recover.
            "row_text": r.get("row_text", ""),
        }
        rec.update(extra or {})
        return rec

    # ---------------- PHASE 2: the detail pages ----------------
    targets = rows if fetch_details else []
    if max_details:
        targets = targets[:max_details]

    if not targets:
        records = [_row_record(r) for r in rows]
        note["blocked_pages"] += page.blocked
        note["errors"] += page.load_failures
        return records, [], note

    dp = _ResilientPage(ctx, wait_ms)
    try:
        for i, r in enumerate(targets, 1):
            if not dp.load(r["href"], tries=2):
                records.append(_row_record(r))       # keep the row even if it failed
                if dp.dead:
                    # Keep the rows with no detail content rather than raising, so
                    # a crash cannot look like "this site has no documents". Record
                    # which row to resume at.
                    note["stopped"] = (f"browser died on detail page {i} of "
                                       f"{len(targets)} — details INCOMPLETE")
                    note["resume"] = {"detail_index": i, "of": len(targets),
                                      "next_url": r["href"]}
                    print(json.dumps({"event": "error", "url": r["href"],
                                      "message": "browser died — details INCOMPLETE",
                                      "details_done": i - 1, "of": len(targets)}),
                          flush=True)
                    for rest in targets[i:]:
                        records.append(_row_record(rest))
                    break
                continue
            try:
                d = dp.evaluate(JS_DETAIL_CONTENT)
            except Exception:
                records.append(_row_record(r))
                continue
            doc_links = []
            for l in d.get("links", []):
                h = l.get("href") or ""
                if h.startswith("http") and _is_doc(h):
                    doc_links.append(h)
                    key = (h, section)
                    if key not in documents:
                        documents[key] = {
                            "title": l.get("text") or r["title"],
                            "doc_url": h,
                            "type": _ext_type(h),
                            "found_on": r["href"],
                            "section_path": section,
                        }
            records.append(_row_record(r, {
                "n_pdfs": len(doc_links),
                "pdf_links": " | ".join(doc_links),
                "text_len": len(d.get("text") or ""),
                "text": d.get("text") or "",
                "html": d.get("html") or "",
            }))
            if i % 25 == 0 or i == len(targets):
                print(json.dumps({"event": "detail_page", "done": i,
                                  "of": len(targets)}, ensure_ascii=False), flush=True)
    finally:
        dp.close()

    note["blocked_pages"] += page.blocked + dp.blocked
    note["errors"] += page.load_failures + dp.load_failures
    return records, list(documents.values()), note
