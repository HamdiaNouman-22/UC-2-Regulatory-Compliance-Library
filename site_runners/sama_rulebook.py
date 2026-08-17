"""
SAMA Rulebook TREE runner (tuned, ADDITIVE) -- for the sidebar_tree sections
(Laws & Implementing Regulations, Finance Sector, Banking Sector, ...).

Model (matches how a human reads the rulebook):
  category page  ->  lists every node in the section as links (Law, Chapter, Article)
  each node page ->  real breadcrumb gives its ancestor path (Law > Chapter),
                     its own title is the leaf; body has a "Download Original PDF".

So we:
  1. open the category URL, collect ALL node links from the outline (exact hrefs --
     chapter/article slugs cannot be guessed),
  2. visit each node, build section_path from the (de-duplicated) breadcrumb + title,
  3. capture the BODY html only -- the print toolbar, the "No:/Date/Status" line,
     the "Versions (N)" block and the "Download Original PDF" link are stripped,
  4. capture pdf_url (a.icopdf) and optionally download it,
  5. scope: keep only nodes whose breadcrumb sits under the requested --tab.

section_path:  SAMA Rulebook > <Tab> > <Law> [ > <Chapter> ] > <Title>

Run:
  venv/Scripts/python.exe site_runners/sama_rulebook.py \
    --tab "Laws and Implementing Regulations" \
    --url https://rulebook.sama.gov.sa/en/laws-and-implementing-regulations \
    --out output/standalone_crawler/batch_sama_laws_impl_regs --limit 40 --download
"""
import os, sys, json, re, hashlib, argparse
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright
import pandas as pd

try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
def log(**kw): print(json.dumps(kw, ensure_ascii=False), flush=True)
def slugify(s, n=90):
    s = re.sub(r"[^A-Za-z0-9._-]+", "-", s).strip("-"); return (s[:n] or "file").lower()

# All node links in the category outline (exact hrefs; skip toolbar/citations/revisions).
JS_NODES = r"""() => {
  const clean=s=>(s||'').replace(/\s+/g,' ').trim();
  const body=document.querySelector('.node__content');   // inline citations live here -> skip
  const out=[]; const seen=new Set();
  document.querySelectorAll('a[href]').forEach(a=>{
    const h=a.href, t=clean(a.innerText);
    if(!t || t.length<2 || h.includes('#') || /\.pdf/i.test(h)) return;
    if(!/rulebook\.sama\.gov\.sa\/en\/[^/]/.test(h)) return;      // must be an /en/<node>
    if(/\/entiresection\/|\/revisions\/|\/en\/?$/.test(h)) return;
    if(a.closest('.disp_toolbar,.breadcrumb,.book-pager,.book-pager__item,header,footer')) return;
    if(body && body.contains(a)) return;                         // exclude inline citations
    if(/^(up|up to|entire section|custom print|text only|rich text|print \/ save as pdf|sama rulebook|versions|download original pdf|home)$/i.test(t)) return;
    if(seen.has(h)) return; seen.add(h); out.push({title:t, url:h});
  });
  return out;
}"""

# Leaf: deduped breadcrumb ancestors, title, pdf link, BODY-only html.
JS_LEAF = r"""() => {
  const clean=s=>(s||'').replace(/\s+/g,' ').trim();
  let bc=[]; const bcEl=document.querySelector('.breadcrumb,.bread-crumb');
  if(bcEl){ Array.from(bcEl.querySelectorAll('a,span,li')).map(x=>clean(x.innerText)).filter(Boolean)
            .forEach(t=>{ if(bc[bc.length-1]!==t) bc.push(t); }); }
  const notFound = /requested page could not be found/i.test(document.body.innerText||'');
  const pdf=document.querySelector('a.icopdf');
  const title=clean((document.querySelector('h1')||{}).innerText||'');
  // The real body is DIV.node__content -- the header (title + No/Date/Status info-table),
  // the Versions block and the Download-Original-PDF block all sit OUTSIDE it.
  const body=document.querySelector('.node__content')||document.querySelector('content')||document.querySelector('article')||document.body;
  const cl=body.cloneNode(true);
  cl.querySelectorAll('nav,script,style,button,a.icopdf,.icopdf,header,.page-title,.info-table,.book-notification,.show-previous,.show-next,[class*=traversal i],[id*=associated_pdf i],[id*=revision_block i],[class*=revisionblock i]').forEach(e=>e.remove());
  let html=(cl.innerHTML||'').trim(); if(html.length>400000) html=html.slice(0,400000);
  const body_text_len=(cl.textContent||'').replace(/\s+/g,' ').trim().length;
  return {breadcrumb:bc, title, not_found:notFound, pdf_url:pdf?pdf.href:'', document_html:html, body_text_len};
}"""

def new_page(ctx, url):
    pg=ctx.new_page()
    try: pg.goto(url, wait_until="domcontentloaded", timeout=90000)
    except Exception as e: log(event="goto_fail", url=url, err=str(e)[:100])
    pg.wait_for_timeout(3000)
    return pg

def download(ctx, url, outdir):
    fn=slugify(os.path.basename(urlparse(url).path) or url)
    if not fn.lower().endswith(".pdf"): fn+=".pdf"
    path=os.path.join(outdir,"pdfs",fn)
    if os.path.exists(path) and os.path.getsize(path)>0:
        with open(path,"rb") as f: return path, hashlib.sha1(f.read()).hexdigest()
    try:
        r=ctx.request.get(url, timeout=90000)
        if not r.ok: return "", ""
        body=r.body(); open(path,"wb").write(body); return path, hashlib.sha1(body).hexdigest()
    except Exception as e:
        log(event="dl_fail", url=url, err=str(e)[:100]); return "", ""

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--tab", required=True)
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--limit", type=int, default=1500, help="max pages to VISIT; 0 = 1500")
    ap.add_argument("--max-depth", type=int, default=12)
    ap.add_argument("--download", action="store_true")
    ap.add_argument("--headful", action="store_true", help="show the browser window")
    a=ap.parse_args()
    os.makedirs(os.path.join(a.out,"pdfs"), exist_ok=True)
    visit_cap = a.limit or 1500

    from collections import deque
    docs=[]; seen={a.url}; q=deque([(a.url,0)]); visited=0

    with sync_playwright() as p:
        b=p.chromium.launch(headless=not a.headful, slow_mo=200 if a.headful else 0)
        ctx=b.new_context(accept_downloads=True)

        while q and visited < visit_cap:
            url, depth = q.popleft()
            pg=new_page(ctx, url)
            d=pg.evaluate(JS_LEAF); kids=pg.evaluate(JS_NODES); pg.close()
            visited+=1
            if d["not_found"]: continue
            bc=d["breadcrumb"]
            in_scope = (url==a.url) or (a.tab.lower() in [x.lower() for x in bc])
            has_content = bool(d["pdf_url"]) or d["body_text_len"] > 200

            # record content pages (skip the category root itself)
            if has_content and in_scope and url!=a.url:
                title=d["title"]
                section_path=" > ".join(bc + ([title] if title and (not bc or bc[-1]!=title) else []))
                dl,sha=("","")
                if a.download and d["pdf_url"]:
                    dl,sha=download(ctx, d["pdf_url"], a.out)
                docs.append({"tab":a.tab, "title":title, "section_path":section_path,
                             "url":url, "depth":depth, "pdf_url":d["pdf_url"],
                             "downloaded_file":dl, "content_hash":sha,
                             "html_len":len(d["document_html"]), "document_html":d["document_html"]})

            # recurse: enqueue every unseen in-scope outline child (citations already
            # excluded in JS_NODES). Dedup via `seen`; the section tree is finite.
            if in_scope and depth < a.max_depth:
                for k in kids:
                    if k["url"] not in seen:
                        seen.add(k["url"]); q.append((k["url"], depth+1))

            if visited % 10 == 0: log(event="progress", visited=visited, queued=len(q), kept=len(docs))
        b.close()
        log(event="walk_done", visited=visited, kept=len(docs))

    with open(os.path.join(a.out,"tree_docs.json"),"w",encoding="utf-8") as f:
        json.dump(docs,f,ensure_ascii=False,indent=2)
    df=pd.DataFrame(docs)
    df.to_csv(os.path.join(a.out,"tree_documents.csv"),index=False,encoding="utf-8-sig")
    # Excel caps a cell at 32767 chars; keep full html in json/csv, put a preview in xlsx.
    df_x=df.copy()
    if "document_html" in df_x.columns:
        df_x["document_html"]=df_x["document_html"].astype(str).str.slice(0,30000)
    xlsx=os.path.join(a.out,f"SAMA_{slugify(a.tab)}_tree.xlsx")
    for i in range(3):
        try:
            with pd.ExcelWriter(xlsx,engine="openpyxl") as xw: df_x.to_excel(xw,sheet_name="documents",index=False)
            break
        except PermissionError: xlsx=os.path.join(a.out,f"SAMA_{slugify(a.tab)}_tree_{i+1}.xlsx")
    log(event="done", visited=len(docs), kept=len(docs),
        downloaded=sum(1 for d in docs if d["downloaded_file"]), xlsx=xlsx)

if __name__=="__main__":
    main()
