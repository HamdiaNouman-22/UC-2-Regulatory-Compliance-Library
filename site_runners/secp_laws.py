"""
SECP  ->  Laws family (Acts, Ordinances, Rules, Regulations, Notifications,
          Directives, Guidelines, Circulars)   -- tuned runner, ADDITIVE.

Why tuned: every SECP /laws/<tab>/ page is the SAME template -- a single
DataTables table `table.table-downloads` with columns Date | Title | Download.
The generic crawler also swept in unrelated "recent documents" mega-menu items
and could not tag the per-tab category. This runner reads ONLY the downloads
table, so the output is exactly the tab's documents, correctly categorised.

Handles: DataTables in-page pagination (set length to All, else click Next),
WordPress Download-Manager links (/document/<slug>?wpdmdl=...), file download.

section_path:  SECP > Laws > <Tab>

Run one tab:
  venv/Scripts/python.exe site_runners/secp_laws.py --tab Acts \
     --url https://www.secp.gov.pk/laws/acts/ --out output/standalone_crawler/batch_secp_acts
"""
import os, sys, json, re, hashlib, argparse
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright
import pandas as pd

try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

HOST = "www.secp.gov.pk"
def log(**kw): print(json.dumps(kw, ensure_ascii=False), flush=True)
def slugify(s, n=90):
    s = re.sub(r"[^A-Za-z0-9._-]+", "-", s).strip("-"); return (s[:n] or "file").lower()

# Read every row of the downloads table: date, title, download href.
JS_TABLE = r"""() => {
  const t = document.querySelector('table.table-downloads') ||
            Array.from(document.querySelectorAll('table')).sort((a,b)=>
              b.querySelectorAll('tbody tr').length - a.querySelectorAll('tbody tr').length)[0];
  if(!t) return [];
  return Array.from(t.querySelectorAll('tbody tr')).map(r=>{
    const tds = r.querySelectorAll('td');
    const a = r.querySelector('a[href]');
    const dateCell = tds[0] ? tds[0].innerText.trim() : '';
    // title = the cell that is not the date and not the bare "Download"
    let title = '';
    tds.forEach(td=>{ const x=td.innerText.trim();
      if(x && x!==dateCell && !/^download$/i.test(x) && x.length>title.length) title=x; });
    return { date: dateCell, title: title.replace(/\s+/g,' '),
             href: a ? a.href : '' };
  }).filter(x=>x.title || x.href);
}"""

def set_show_all(pg):
    """Maximise the DataTables length menu so all rows render (avoids multi-page clicks)."""
    try:
        pg.evaluate("""() => {
          const sel = document.querySelector('select[name$=_length], .dataTables_length select');
          if(!sel) return;
          const opts = Array.from(sel.options).map(o=>o.value);
          const all = opts.find(v=>v==='-1') || opts.map(Number).filter(n=>!isNaN(n)).sort((a,b)=>b-a)[0];
          if(all!==undefined){ sel.value=String(all);
            sel.dispatchEvent(new Event('change',{bubbles:true})); }
        }""")
        pg.wait_for_timeout(1500)
    except Exception: pass

def click_next_pages(pg, harvest, max_pages=60):
    """Fallback: click DataTables Next, harvesting rows each draw until no growth."""
    seen = {}
    def add(rows):
        for r in rows: seen[(r["href"], r["title"])] = r
    add(harvest())
    for _ in range(max_pages):
        before = len(seen)
        nxt = pg.query_selector(".paginate_button.next:not(.disabled):not([aria-disabled=true]), a.next:not(.disabled)")
        if not nxt: break
        try: nxt.click(); pg.wait_for_timeout(900)
        except Exception: break
        add(harvest())
        if len(seen) == before: break
    return list(seen.values())

def download(ctx, url, outdir):
    try:
        resp = ctx.request.get(url, timeout=60000)
        if not resp.ok: log(event="dl_http", status=resp.status, url=url); return "", ""
        body = resp.body(); sha = hashlib.sha1(body).hexdigest()
        ct = (resp.headers or {}).get("content-type", "")
        ext = ".pdf" if "pdf" in ct else (".zip" if "zip" in ct else (".doc" if "word" in ct or "msword" in ct else ""))
        base = slugify(os.path.basename(urlparse(url).path) or "doc")
        if ext and not base.lower().endswith(ext): base += ext
        elif not os.path.splitext(base)[1]: base += ".pdf" if "pdf" in ct else ".bin"
        path = os.path.join(outdir, "files", base)
        if os.path.exists(path) and os.path.getsize(path) > 0:
            with open(path, "rb") as f: return path, hashlib.sha1(f.read()).hexdigest()
        with open(path, "wb") as f: f.write(body)
        return path, sha
    except Exception as e:
        log(event="dl_fail", url=url, err=str(e)[:120]); return "", ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tab", required=True)
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--download", action="store_true", help="also download the files")
    a = ap.parse_args()
    os.makedirs(os.path.join(a.out, "files"), exist_ok=True)
    section = f"SECP > Laws > {a.tab}"

    with sync_playwright() as p:
        b = p.chromium.launch(headless=True); ctx = b.new_context()
        pg = ctx.new_page()
        pg.goto(a.url, wait_until="domcontentloaded", timeout=60000); pg.wait_for_timeout(4000)
        try: pg.mouse.wheel(0, 2500); pg.wait_for_timeout(1000)
        except Exception: pass
        set_show_all(pg)
        rows = click_next_pages(pg, lambda: pg.evaluate(JS_TABLE))
        log(event="scanned", tab=a.tab, rows=len(rows))

        docs = []
        for r in rows:
            dl, sha = "", ""
            if a.download and r["href"] and HOST in urlparse(r["href"]).netloc:
                dl, sha = download(ctx, r["href"], a.out)
            docs.append({"category": a.tab, "title": r["title"], "date": r["date"],
                         "doc_url": r["href"], "section_path": section,
                         "downloaded_file": dl, "content_hash": sha})
        b.close()

    with open(os.path.join(a.out, "docs.json"), "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)
    df = pd.DataFrame(docs)
    df.to_csv(os.path.join(a.out, "documents.csv"), index=False, encoding="utf-8-sig")
    xlsx = os.path.join(a.out, f"SECP_{slugify(a.tab)}.xlsx")
    for i in range(3):
        try:
            with pd.ExcelWriter(xlsx, engine="openpyxl") as xw:
                df.to_excel(xw, sheet_name="documents", index=False)
            break
        except PermissionError:
            xlsx = os.path.join(a.out, f"SECP_{slugify(a.tab)}_{i+1}.xlsx")
    log(event="done", tab=a.tab, documents=len(docs),
        downloaded=sum(1 for d in docs if d["downloaded_file"]), xlsx=xlsx)

if __name__ == "__main__":
    main()
