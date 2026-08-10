"""
CBB glossary enricher (ADDITIVE, requests-based).

CBB rule pages mark defined terms as <glossary type="i">TERM</glossary>. The
definition is NOT in the page -- CBB's JS turns the tag into a Drupal AJAX modal
link to /glossary-tag/{TERM}. We fetch that directly:

    GET /glossary-tag/{term}?_wrapper_format=drupal_ajax   (X-Requested-With: XMLHttpRequest)
    -> Drupal AJAX JSON; the definition is the HTML-string command.

For each doc we:
  1. pull raw HTML, take the body (.node__content),
  2. find every <glossary> term,
  3. resolve each DISTINCT term's definition once (cached, gently paced),
  4. write an ENRICHED html: each term gets a hover title + a "Defined Terms"
     appendix listing term -> definition,
  5. emit a glossary.xlsx / .json (term, definition, times_used).

Run:
  venv/Scripts/python.exe site_runners/cbb_glossary.py --out output/standalone_crawler/_cbb_glossary \
     --urls https://cbben.thomsonreuters.com/rulebook/bc-10113-0 ...
"""
import os, sys, json, re, time, argparse, html as ihtml
from urllib.parse import quote, urljoin
import requests
from bs4 import BeautifulSoup
import pandas as pd

try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
BASE = "https://cbben.thomsonreuters.com"
H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
HAJAX = dict(H, **{"X-Requested-With": "XMLHttpRequest"})
def log(**k): print(json.dumps(k, ensure_ascii=False), flush=True)
def clean(s): return re.sub(r"\s+", " ", s or "").strip()

_defs = {}       # term(lower) -> definition   (cache: one fetch per distinct term)
GLOSSARY = {}    # term(lower) -> {"term": original, "definition": ...}
def get_definition(term, tries=4):
    key = term.lower().strip()
    if key in _defs:
        return _defs[key]
    url = f"{BASE}/glossary-tag/{quote(term)}?_wrapper_format=drupal_ajax"
    for a in range(tries):
        try:
            r = requests.get(url, headers=HAJAX, timeout=25)
            if r.status_code == 403:
                time.sleep(3 * (a + 1)); continue
            if r.status_code != 200:
                _defs[key] = ""; return ""
            for c in json.loads(r.text):
                d = c.get("data")
                if isinstance(d, str) and len(d) > 40:
                    _defs[key] = clean(re.sub(r"<[^>]+>", " ", d)); return _defs[key]
            _defs[key] = ""; return ""
        except Exception as e:
            log(event="def_err", term=term, err=str(e)[:100]); _defs[key] = ""; return ""
    _defs[key] = ""; return ""

def enrich_doc(url, out_dir, idx):
    r = requests.get(url, headers=H, timeout=30)
    if r.status_code != 200:
        log(event="doc_http", url=url, status=r.status_code); return None
    soup = BeautifulSoup(r.text, "html.parser")
    body = soup.select_one(".node__content") or soup.select_one(".field--name-body") or soup.select_one("main")
    if not body:
        log(event="no_body", url=url); return None
    title = clean((soup.select_one("h1") or soup.title).get_text()) if (soup.select_one("h1") or soup.title) else url

    terms = body.find_all("glossary")
    used = {}
    for g in terms:
        term = clean(g.get_text())
        if not term: continue
        d = get_definition(term); time.sleep(0.5)
        used[term.lower()] = {"term": term, "definition": d}
        GLOSSARY.setdefault(term.lower(), {"term": term, "definition": d})
        # annotate the term inline: click-popup (data-def) + hover tooltip (title)
        defn = d or "(definition not found)"
        span = soup.new_tag("span", **{"class": "glossterm", "title": defn,
                                       "data-term": term, "data-def": defn})
        span.string = term
        g.replace_with(span)

    # "Defined Terms" appendix
    if used:
        hr = soup.new_tag("hr"); body.append(hr)
        h = soup.new_tag("h3"); h.string = "Defined Terms (from CBB glossary)"; body.append(h)
        ul = soup.new_tag("ul", **{"class": "defined-terms"})
        for v in used.values():
            li = soup.new_tag("li")
            b = soup.new_tag("b"); b.string = v["term"] + ": "
            li.append(b); li.append(v["definition"] or "(definition not found)")
            ul.append(li)
        body.append(ul)

    style = ("<style>"
             "body{font-family:Georgia,serif;max-width:900px;margin:24px auto;line-height:1.5;color:#222}"
             "p{margin:.5em 0}"
             ".glossterm{color:#c0392b;border-bottom:1px dotted #c0392b;cursor:help;font-weight:600}"
             ".glossterm:hover{background:#fdf2f0}"
             "h3{margin-top:1.5em;color:#c0392b}"
             ".defined-terms li{margin:.4em 0}"
             "</style>")
    modal = (
        "<div id='glossmodal' onclick=\"if(event.target.id==='glossmodal')this.style.display='none'\" "
        "style='display:none;position:fixed;inset:0;background:rgba(0,0,0,.45);z-index:9999'>"
        "<div style='background:#fff;max-width:600px;margin:8% auto;padding:22px 24px;border-radius:8px;"
        "position:relative;box-shadow:0 8px 30px rgba(0,0,0,.3);font-family:Georgia,serif'>"
        "<button onclick=\"document.getElementById('glossmodal').style.display='none'\" "
        "style='position:absolute;top:6px;right:10px;border:0;background:none;font-size:24px;cursor:pointer;color:#888'>&times;</button>"
        "<h3 id='gm-term' style='color:#c0392b;margin:0 0 10px'></h3><div id='gm-def' style='line-height:1.55'></div>"
        "</div></div>"
        "<script>document.querySelectorAll('.glossterm').forEach(function(t){"
        "t.style.cursor='pointer';"
        "t.addEventListener('click',function(){"
        "document.getElementById('gm-term').textContent=this.getAttribute('data-term');"
        "document.getElementById('gm-def').textContent=this.getAttribute('data-def');"
        "document.getElementById('glossmodal').style.display='block';});});</script>")
    page = (f"<!doctype html><meta charset='utf-8'><title>{ihtml.escape(title)}</title>{style}"
            f"<h1>{ihtml.escape(title)}</h1><p><a href='{url}'>{url}</a></p><hr>{body}{modal}")
    fn = os.path.join(out_dir, f"{idx:02d}_{re.sub(r'[^a-z0-9]+','-',title.lower())[:60]}.html")
    with open(fn, "w", encoding="utf-8") as f: f.write(page)
    return {"title": title, "url": url, "glossary_terms": len(terms),
            "distinct_terms": len(used), "defs_found": sum(1 for v in used.values() if v["definition"]),
            "html_file": fn, "terms": list(used.values())}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--urls", nargs="+", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    docs = []
    for i, u in enumerate(a.urls, 1):
        res = enrich_doc(u, a.out, i)
        if res:
            docs.append(res)
            log(event="doc_done", title=res["title"][:50], terms=res["glossary_terms"],
                distinct=res["distinct_terms"], defs=res["defs_found"])
    # glossary sheet (all distinct terms found across docs)
    gl = sorted(GLOSSARY.values(), key=lambda v: v["term"].lower())
    json.dump({"docs": [{k: v for k, v in d.items() if k != "terms"} for d in docs],
               "glossary": gl}, open(os.path.join(a.out, "glossary.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=2)
    xlsx = os.path.join(a.out, "CBB_Glossary.xlsx")
    for i in range(3):
        try:
            pd.DataFrame(gl).to_excel(xlsx, index=False); break
        except PermissionError:
            xlsx = os.path.join(a.out, f"CBB_Glossary_{i+1}.xlsx")
    log(event="done", docs=len(docs), distinct_terms=len(gl),
        defs_found=sum(1 for v in gl if v["definition"]), xlsx=xlsx, out=a.out)

if __name__ == "__main__":
    main()
