"""
SAMA  ->  Circulars   (tuned runner, ADDITIVE)

https://rulebook.sama.gov.sa/en/sama-circulars is a DataTables 2.x grid:
  columns:  Circular No. | Circular Title | Issue Date (G) | Issue Date (H) | Status | Scope
  ~69 pages x 10 rows.  Pagination is JS (.dt-paging-button.next) -- NOT ?page=,
  and the generic crawler's breadcrumb/host scopes both fail on it.

This runner clicks through the pager, harvesting the full metadata row each draw.
Each title links to a /en/node/<id> detail page (doc_url).

section_path: SAMA > Circulars

Run:
  venv/Scripts/python.exe site_runners/sama_circulars.py --max-pages 20
  (default caps at 20 pages = ~200 circulars for a sample; --max-pages 100 for all)
"""
import os, sys, json, argparse, re, hashlib
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright
import pandas as pd

try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

SEED = "https://rulebook.sama.gov.sa/en/sama-circulars"
OUTDIR = os.path.join("output", "standalone_crawler", "batch_sama_circulars")
SECTION = "SAMA > Circulars"
def log(**kw): print(json.dumps(kw, ensure_ascii=False), flush=True)
def slugify(s, n=90):
    s = re.sub(r"[^A-Za-z0-9._-]+", "-", s).strip("-"); return (s[:n] or "file").lower()

# Detail page: original-PDF link (a.icopdf), translated link, cleaned <article> HTML.
JS_DETAIL = r"""() => {
  const pdf = document.querySelector('a.icopdf');
  const tr  = Array.from(document.querySelectorAll('a[href]')).find(a=>/translated/i.test(a.innerText||''));
  const art = document.querySelector('article') || document.querySelector('main') || document.body;
  const cl = art.cloneNode(true);
  cl.querySelectorAll('nav,script,style,button,.breadcrumb,.bread-crumb,a.icopdf,.icopdf,[class*=print i],[class*=toolbar i],[class*=translated i],[class*=related i]').forEach(e=>e.remove());
  let html = (cl.innerHTML||'').trim();
  if(html.length>300000) html=html.slice(0,300000);
  return { pdf_url: pdf?pdf.href:'', translated_url: tr?tr.href:'', document_html: html };
}"""

def download(ctx, url):
    fn = slugify(os.path.basename(urlparse(url).path) or url)
    if not fn.lower().endswith(".pdf"): fn += ".pdf"
    path = os.path.join(OUTDIR, "pdfs", fn)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        try:
            with open(path, "rb") as f: return path, hashlib.sha1(f.read()).hexdigest()
        except Exception: return path, ""
    try:
        r = ctx.request.get(url, timeout=90000)
        if not r.ok: log(event="dl_http", url=url, status=r.status); return "", ""
        body = r.body(); os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f: f.write(body)
        return path, hashlib.sha1(body).hexdigest()
    except Exception as e:
        log(event="dl_fail", url=url, err=str(e)[:120]); return "", ""

JS_ROWS = r"""() => {
  const t = Array.from(document.querySelectorAll('table'))
      .sort((a,b)=>b.querySelectorAll('tbody tr').length - a.querySelectorAll('tbody tr').length)[0];
  if(!t) return [];
  return Array.from(t.querySelectorAll('tbody tr')).map(r=>{
    const c = Array.from(r.querySelectorAll('td')).map(td=>td.innerText.trim().replace(/\s+/g,' '));
    const a = r.querySelector('a[href]');
    return { circular_no: c[0]||'', title: c[1]||'', issue_date_g: c[2]||'',
             issue_date_h: c[3]||'', status: c[4]||'', scope: c[5]||'',
             doc_url: a ? a.href : '' };
  }).filter(x=>x.circular_no || x.title);
}"""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=20)
    ap.add_argument("--detail-limit", type=int, default=0,
                    help="visit this many circular detail pages to capture HTML + PDF (0 = skip)")
    ap.add_argument("--download", action="store_true", help="download the Original PDF of each detailed circular")
    a = ap.parse_args()
    os.makedirs(OUTDIR, exist_ok=True)
    seen, docs = set(), []

    with sync_playwright() as p:
        b = p.chromium.launch(headless=True); ctx = b.new_context(accept_downloads=True); pg = ctx.new_page()
        pg.goto(SEED, wait_until="domcontentloaded", timeout=90000); pg.wait_for_timeout(5000)
        try: pg.wait_for_function("() => document.querySelectorAll('table tbody tr').length > 0", timeout=20000)
        except Exception: pass

        total_pages = pg.evaluate(r"""() => { let m=1;
          document.querySelectorAll('.dt-paging-button').forEach(bt=>{const n=parseInt(bt.innerText);if(!isNaN(n))m=Math.max(m,n)});
          return m; }""")
        log(event="start", total_pages_available=total_pages, cap=a.max_pages)

        page_i = 0
        while page_i < a.max_pages:
            for r in pg.evaluate(JS_ROWS):
                key = r["doc_url"] or (r["circular_no"] + r["title"])
                if key in seen: continue
                seen.add(key); r["section_path"] = SECTION; docs.append(r)
            page_i += 1
            nxt = pg.query_selector(".dt-paging-button.next:not(.disabled)")
            if not nxt: break
            try:
                nxt.click(); pg.wait_for_timeout(1400)
            except Exception: break
            if page_i % 10 == 0: log(event="progress", pages=page_i, docs=len(docs))

        # ---- detail enrichment: open each circular, grab article HTML + Original PDF ----
        for d in docs: d.setdefault("pdf_url",""); d.setdefault("translated_url",""); d.setdefault("document_html",""); d.setdefault("downloaded_file",""); d.setdefault("content_hash","")
        if a.detail_limit:
            targets = [d for d in docs if d["doc_url"]][:a.detail_limit]
            log(event="detail_start", count=len(targets), download=a.download)
            for i, d in enumerate(targets, 1):
                pgd = ctx.new_page()
                try:
                    pgd.goto(d["doc_url"], wait_until="domcontentloaded", timeout=90000); pgd.wait_for_timeout(3000)
                    det = pgd.evaluate(JS_DETAIL)
                    d["pdf_url"] = det["pdf_url"]; d["translated_url"] = det["translated_url"]
                    d["document_html"] = det["document_html"]
                    if a.download and det["pdf_url"]:
                        d["downloaded_file"], d["content_hash"] = download(ctx, det["pdf_url"])
                except Exception as e:
                    log(event="detail_fail", url=d["doc_url"], err=str(e)[:100])
                finally:
                    pgd.close()
                if i % 10 == 0: log(event="detail_progress", done=i, of=len(targets))
        b.close()

    with open(os.path.join(OUTDIR, "docs.json"), "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)
    df = pd.DataFrame(docs)
    df.to_csv(os.path.join(OUTDIR, "documents.csv"), index=False, encoding="utf-8-sig")
    xlsx = os.path.join(OUTDIR, "SAMA_Circulars.xlsx")
    for i in range(3):
        try:
            with pd.ExcelWriter(xlsx, engine="openpyxl") as xw:
                df.to_excel(xw, sheet_name="documents", index=False)
            break
        except PermissionError:
            xlsx = os.path.join(OUTDIR, f"SAMA_Circulars_{i+1}.xlsx")
    log(event="done", documents=len(docs), pages_crawled=page_i,
        total_pages_available=total_pages, xlsx=xlsx)

if __name__ == "__main__":
    main()
