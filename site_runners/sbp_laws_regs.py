"""
SBP  ->  Laws & Regulations   (tuned runner, ADDITIVE — does not touch the generic engine)

The list page https://www.sbp.org.pk/laws-regulations renders every document as:
    <tr class="law-row" data-type="laws" data-title="..." data-date="YYYY-MM-DD">
      <a href="...">   <-- the action link
The 5 "Filter By Type" checkboxes only hide/show rows; the DOM already holds them all.
We read the rows directly (perfect type + date), then classify each link:

  * FILE  (.pdf/.xls/.xlsx/.doc.../assets/...)  -> a document, downloaded to pdfs/
  * EXTERNAL (other host, e.g. pakistancode)    -> a document, link recorded only
  * INTERNAL PAGE (/laws-regulations/<slug>)    -> a nested CONTENT page (e.g. Foreign
                                                   Exchange Manual, CPIS, Reporting Guides).
      We VISIT it, capture its HTML content, and pull its items from THREE layouts:
        - "Items" table  (title = item-name cell, link = the row's download icon)
        - document "cards" (title = card heading, link = "Download Document"/"Open Main File")
        - plain in-content links (FE-Manual chapters, etc.)
      All file types captured (CPIS has XLS forms), titles taken from the row/card
      (never the bare "Download"/icon text).

Hierarchy in `section_path`:
    Laws & Regulations > <Type> [ > <Nested page title> [ > ... ] ]

Output:  output/standalone_crawler/batch_sbp_laws_regs/
    SBP_Laws_Regulations.xlsx   sheets: documents + pages(with document_html)
    documents.csv, docs.json
    pdfs/    downloaded same-host files (any type)

Run:
    venv/Scripts/python.exe site_runners/sbp_laws_regs.py
"""
import os, sys, json, re, hashlib
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright
import pandas as pd

try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

SEED   = "https://www.sbp.org.pk/laws-regulations"
OUTDIR = os.path.join("output", "standalone_crawler", "batch_sbp_laws_regs")
ROOT   = "Laws & Regulations"
HOST   = "www.sbp.org.pk"
MAX_NEST_DEPTH = 2
FILE_EXTS = (".pdf", ".xls", ".xlsx", ".doc", ".docx", ".zip", ".ppt", ".pptx", ".csv", ".rar")

TYPE_LABEL = {
    "laws": "Laws", "regulations": "Regulations",
    "gazette": "Gazette Notifications", "gazette-notifications": "Gazette Notifications",
    "guidelines": "Guidelines",
    "licensing-guidelines": "Licensing Guidelines", "licensing": "Licensing Guidelines",
}

def log(**kw): print(json.dumps(kw, ensure_ascii=False), flush=True)

def link_kind(href):
    if not href: return "none"
    low = href.lower()
    if low.endswith(FILE_EXTS) or "/assets/document" in low: return "file"
    h = urlparse(href).netloc.lower()
    if h and h != HOST: return "external"
    if "/laws-regulations/" in href: return "page"
    return "other"

def slugify(s, n=90):
    s = re.sub(r"[^A-Za-z0-9._-]+", "-", s).strip("-"); return (s[:n] or "file").lower()

def is_recursable_title(t):
    """Avoid recursing junk anchors like 'III', '3', very short labels."""
    t = (t or "").strip()
    if len(t) < 4: return False
    if re.fullmatch(r"[IVXLCDM]+", t, re.I): return False
    if re.fullmatch(r"[\d.\s]+", t): return False
    return True

def file_ext(url):
    m = re.search(r"\.(pdf|xlsx?|docx?|pptx?|zip|csv|rar)(\?|$)", url, re.I)
    return ("." + m.group(1).lower()) if m else ""

# ---- rows of the main list (tr.law-row with data-*) --------------------------------
JS_ROWS = r"""() => {
  const rows = Array.from(document.querySelectorAll('tr.law-row, tr[data-type], [data-type][data-title]'));
  return rows.map(r => {
    const a = r.querySelector('a[href]');
    return {
      type:  (r.getAttribute('data-type')||'').trim(),
      title: (r.getAttribute('data-title')||'').trim(),
      date:  (r.getAttribute('data-date')||'').trim(),
      href:  a ? a.href : ''
    };
  }).filter(x => x.title);
}"""

# ---- nested content page: page title + cleaned HTML + items (3 layouts) -------------
JS_NESTED = r"""() => {
  const clean = (s)=> (s||'').replace(/\s+/g,' ').trim();
  // page title: breadcrumb last link, else a prominent content heading (skip the banner)
  let ptitle = '';
  const bc = document.querySelectorAll('.bread-crumb a, .breadcrumb a');
  if(bc.length) ptitle = clean(bc[bc.length-1].innerText);
  if(!ptitle){
    const h = Array.from(document.querySelectorAll('h1,h2')).map(e=>clean(e.innerText))
              .find(t=>t && !/^laws\s*&\s*regulations$/i.test(t));
    ptitle = h||'';
  }
  const items = []; const seen = new Set();
  const push = (title,href)=>{ title=clean(title); if(!href) return;
     if(seen.has(href)) return; seen.add(href); items.push({title, href}); };

  // (A) "Items" tables: first cell = item name, row link = the download icon
  document.querySelectorAll('table').forEach(t=>{
    const head = clean((t.querySelector('thead')||{}).innerText||'');
    t.querySelectorAll('tbody tr').forEach(r=>{
      const a = r.querySelector('a[href]'); if(!a) return;
      const tds = Array.from(r.querySelectorAll('td')).map(td=>clean(td.innerText));
      const name = tds.find(x=>x && !/^download$/i.test(x)) || '';
      push(name, a.href);
    });
  });

  // (B) document "cards": heading + Download Document / Open Main File button
  document.querySelectorAll('a[href]').forEach(a=>{
    const t = clean(a.innerText);
    if(!/download document|open main file|download$/i.test(t)) return;
    let card = a; for(let i=0;i<5 && card.parentElement;i++){ card=card.parentElement;
        if(clean(card.innerText).length>40) break; }
    let head = clean((card.querySelector('h1,h2,h3,h4,h5,h6')||{}).innerText||'');
    if(/^laws\s*&\s*regulations$/i.test(head) || head==='DOCUMENTS') head='';
    const fileM = clean(card.innerText).match(/(?:File|Title):\s*([^\n]+?\.(?:pdf|xlsx?|docx?|zip))/i);
    const name = head || (fileM?fileM[1]:'') || t;
    push(name, a.href);
  });

  // (C) plain in-content links (FE-Manual chapters etc.)
  const bad = /our-operations|about-sbp|help-desk|careers|museum|contact-us|publications$|archive\.sbp|\/circulars$|\/notifications$|economic-data$/i;
  document.querySelectorAll('main a[href], table a[href], .container a[href], .content a[href], article a[href]').forEach(a=>{
    const t = clean(a.innerText); const h = a.href;
    if(!t || t.length<2 || h.includes('#') || bad.test(h)) return;
    if(/^(laws\s*&\s*regulations|home|download|download document|open main file)$/i.test(t)) return;
    push(t, h);
  });

  // cleaned main content HTML — pick the LARGEST content block (not the navbar .container).
  const cands = Array.from(document.querySelectorAll('.border-box, section.section-gap-padding, main, article, .col-lg-12'))
      .filter(e => !e.closest('header,nav,footer'));
  const pick = cands.sort((a,b)=>(b.innerText||'').length-(a.innerText||'').length)[0] || document.body;
  const cl = pick.cloneNode(true);
  cl.querySelectorAll('header,nav,footer,script,style,button,.pages-banner,.bread-crumb,.breadcrumb,[aria-hidden="true"],.overlay-mega-menu,.bg-overlay,.back-to-top').forEach(e=>e.remove());
  let html = cl.innerHTML || '';
  if(html.length > 200000) html = html.slice(0,200000);
  return {ptitle, items, content_html: html};
}"""

def new_page(ctx, url):
    pg = ctx.new_page()
    try: pg.goto(url, wait_until="domcontentloaded", timeout=60000)
    except Exception as e: log(event="goto_fail", url=url, err=str(e)[:120])
    pg.wait_for_timeout(3500)
    # wait until the page-specific content (a table or the content card) actually renders
    try:
        pg.wait_for_function(
            "() => (document.querySelector('table a[href], .border-box, main') && document.body.innerText.length > 400)",
            timeout=12000)
    except Exception: pass
    try:
        pg.mouse.wheel(0, 4000); pg.wait_for_timeout(1200)
        pg.mouse.wheel(0, 4000); pg.wait_for_timeout(700)
    except Exception: pass
    return pg

def fetch_nested(ctx, url, js):
    """Load a nested page and extract; retry once if it came back empty (slow SPA)."""
    for attempt in range(2):
        pg = new_page(ctx, url)
        data = pg.evaluate(js); pg.close()
        if data["items"] or len(data["content_html"]) > 200:
            return data
        log(event="nested_empty_retry", url=url, attempt=attempt)
    return data

def download(ctx, url):
    fn = slugify(os.path.basename(urlparse(url).path) or url)
    ext = file_ext(url) or ".pdf"
    if not fn.lower().endswith(ext): fn += ext
    path = os.path.join(OUTDIR, "pdfs", fn)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        try:
            with open(path, "rb") as f: return path, hashlib.sha1(f.read()).hexdigest()
        except Exception: return path, ""
    try:
        resp = ctx.request.get(url, timeout=60000)
        if not resp.ok: log(event="dl_http", url=url, status=resp.status); return "", ""
        body = resp.body(); sha = hashlib.sha1(body).hexdigest()
        with open(path, "wb") as f: f.write(body)
        return path, sha
    except Exception as e:
        log(event="dl_fail", url=url, err=str(e)[:120]); return "", ""

def add_doc(documents, seen, **row):
    key = (row.get("doc_url",""), row.get("title",""), row.get("section_path",""))
    if key in seen: return
    seen.add(key); documents.append(row)

def main():
    os.makedirs(os.path.join(OUTDIR, "pdfs"), exist_ok=True)
    documents, pages, seen = [], [], set()

    with sync_playwright() as p:
        b = p.chromium.launch(headless=True); ctx = b.new_context(accept_downloads=True)

        # 1) main list
        pg = new_page(ctx, SEED)
        rows = pg.evaluate(JS_ROWS); pg.close()
        pages.append({"section_path": ROOT, "url": SEED, "kind": "list", "n_items": len(rows), "document_html": ""})
        log(event="main", rows=len(rows))

        nested_queue = []
        for r in rows:
            tlabel = TYPE_LABEL.get((r["type"] or "").lower(), (r["type"] or "Uncategorised").title())
            spath = f"{ROOT} > {tlabel}"
            kind = link_kind(r["href"])
            if kind == "page":
                nested_queue.append((r["title"], r["href"], f"{spath} > {r['title']}"))
                add_doc(documents, seen, type=tlabel, title=r["title"], date=r["date"],
                        link_kind="nested-page", doc_url=r["href"], section_path=spath,
                        downloaded_file="", content_hash="")
                continue
            dl, sha = ("", "")
            if kind == "file" and HOST in urlparse(r["href"]).netloc:
                dl, sha = download(ctx, r["href"])
            add_doc(documents, seen, type=tlabel, title=r["title"], date=r["date"],
                    link_kind=kind, doc_url=r["href"], section_path=spath,
                    downloaded_file=dl, content_hash=sha)
        log(event="main_done", docs=len(documents), nested=len(nested_queue))

        # 2) recurse nested content pages
        depth, frontier = 0, nested_queue
        while frontier and depth < MAX_NEST_DEPTH:
            nxt = []
            for title, url, spath in frontier:
                data = fetch_nested(ctx, url, JS_NESTED)
                tlabel = spath.split(" > ")[1] if " > " in spath else ""
                pages.append({"section_path": spath, "url": url, "kind": "content-page",
                              "n_items": len(data["items"]), "document_html": data["content_html"]})
                log(event="nested", title=title, items=len(data["items"]), html_len=len(data["content_html"]))
                for it in data["items"]:
                    kind = link_kind(it["href"])
                    if kind in ("none", "other"): continue
                    if kind == "page":
                        if not is_recursable_title(it["title"]): continue
                        nxt.append((it["title"], it["href"], f"{spath} > {it['title']}"))
                        add_doc(documents, seen, type=tlabel, title=it["title"], date="",
                                link_kind="nested-page", doc_url=it["href"], section_path=spath,
                                downloaded_file="", content_hash="")
                        continue
                    dl, sha = ("", "")
                    if kind == "file" and HOST in urlparse(it["href"]).netloc:
                        dl, sha = download(ctx, it["href"])
                    add_doc(documents, seen, type=tlabel, title=it["title"], date="",
                            link_kind=kind, doc_url=it["href"], section_path=spath,
                            downloaded_file=dl, content_hash=sha)
            frontier = nxt; depth += 1
        b.close()

    # 3) outputs (JSON+CSV always; xlsx to dedicated name, lock-safe)
    with open(os.path.join(OUTDIR, "docs.json"), "w", encoding="utf-8") as f:
        json.dump({"documents": documents, "pages": pages}, f, ensure_ascii=False, indent=2)
    df_docs, df_pages = pd.DataFrame(documents), pd.DataFrame(pages)
    df_docs.to_csv(os.path.join(OUTDIR, "documents.csv"), index=False, encoding="utf-8-sig")
    xlsx = os.path.join(OUTDIR, "SBP_Laws_Regulations.xlsx")
    for i in range(3):
        try:
            with pd.ExcelWriter(xlsx, engine="openpyxl") as xw:
                df_docs.to_excel(xw, sheet_name="documents", index=False)
                df_pages.to_excel(xw, sheet_name="pages", index=False)
            break
        except PermissionError:
            xlsx = os.path.join(OUTDIR, f"SBP_Laws_Regulations_{i+1}.xlsx")
    dl_ct = sum(1 for d in documents if d["downloaded_file"])
    by = {}
    for d in documents: by[d["section_path"]] = by.get(d["section_path"], 0) + 1
    log(event="done", documents=len(documents), pages=len(pages), files_downloaded=dl_ct,
        xlsx=xlsx, by_section=by)

if __name__ == "__main__":
    main()
