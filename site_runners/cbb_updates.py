"""
CBB  ->  Updates / Recent Changes   (tuned runner, ADDITIVE, requests-based)

https://cbben.thomsonreuters.com/view-revision-updates is a server-rendered Drupal
"view" -- the whole list is in the static HTML (no JS render needed), so we fetch it
with `requests` (reliable; the browser render is flaky). Each change is a `.views-row`:

    .book-detail  -> <a href="/rulebook/...">TITLE</a> + <time datetime=...>DATE</time>
    .book-trail   -> the hierarchy, e.g. "Volume 6-Capital Markets >> Ad-hoc Communications"

Date filter: changed=-5 day | -10 day | -30 day  (with f_days=on).
Pagination:  &page=0..N (URL-based).  We loop pages until empty.

Output:  output/standalone_crawler/batch_cbb_updates/
    CBB_Updates.xlsx / .csv / .json  columns:
    counter, title, doc_url, changed_date, changed_iso, volume, section_path

Run:
    venv/Scripts/python.exe site_runners/cbb_updates.py --days 30
    venv/Scripts/python.exe site_runners/cbb_updates.py --days 5
"""
import os, sys, json, re, argparse, time
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import pandas as pd

try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

BASE = "https://cbben.thomsonreuters.com"
UPDATES = BASE + "/view-revision-updates"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
OUTDIR = os.path.join("output", "standalone_crawler", "batch_cbb_updates")

def log(**kw): print(json.dumps(kw, ensure_ascii=False), flush=True)
def clean(s): return re.sub(r"\s+", " ", s or "").strip()
def slugify(s, n=90):
    s = re.sub(r"[^A-Za-z0-9._-]+", "-", str(s or "")).strip("-"); return (s[:n] or "doc").lower()

CHROME_TEXTS = {"quick search", "go", "cbb rulebook: contents", "location:", "contents", "search"}

def fetch_detail(url, tries=4):
    """Fetch an update's page -> (clean_body_html, text). Retries on 403 (rate limit)."""
    for attempt in range(tries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=45)
            if r.status_code == 403:
                time.sleep(3 * (attempt + 1)); continue          # rate-limited -> back off
            soup = BeautifulSoup(r.text, "html.parser")
            node = (soup.select_one(".node__content") or soup.select_one(".field--name-body")
                    or soup.select_one(".region-content") or soup.select_one("main"))
            if not node:
                return "", ""
            # strip page chrome: search widget, nav, breadcrumb, pager, and the
            # "Quick Search / CBB Rulebook: Contents / Location:" labels the user flagged.
            for bad in node.select("nav, script, style, form, input, button, "
                                   ".book-navigation, .book-pager, .disp_toolbar, .breadcrumb, "
                                   "[id*=search i], [class*=search i], [class*=quick i]"):
                bad.decompose()
            for el in node.find_all(["h1", "h2", "h3", "h4", "h5", "div", "span", "p", "label"]):
                if el.get_text(" ", strip=True).lower().strip() in CHROME_TEXTS:
                    el.decompose()
            return str(node), clean(node.get_text(" "))
        except Exception as e:
            log(event="detail_fail", url=url, err=str(e)[:120]); return "", ""
    log(event="detail_403", url=url); return "", ""

def parse_rows(soup):
    rows = []
    for r in soup.select(".views-row"):
        det = r.select_one(".book-detail")
        if not det:
            continue
        a = det.find("a", href=True)
        t = det.find("time")
        counter = clean(r.select_one(".views-field-counter").get_text()) if r.select_one(".views-field-counter") else ""
        trail_el = r.select_one(".book-trail")
        trail = clean(trail_el.get_text()) if trail_el else ""
        trail_parts = [p.strip() for p in re.split(r">>+", trail) if p.strip()]
        rows.append({
            "counter": counter.rstrip(" -"),
            "title": clean(a.get_text()) if a else "",
            "doc_url": urljoin(BASE, a["href"]) if a else "",
            "changed_date": clean(t.get_text()) if t else "",
            "changed_iso": (t.get("datetime") if t else "") or "",
            "volume": trail_parts[0] if trail_parts else "",
            "section_path": "CBB Updates > " + " > ".join(trail_parts) if trail_parts else "CBB Updates",
        })
    return rows

def total_expected(soup):
    m = re.search(r"Showing results\s+\d+\s+to\s+\d+\s+of\s+(\d+)", soup.get_text())
    return int(m.group(1)) if m else None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, choices=[5, 10, 30], default=30)
    ap.add_argument("--per-page", type=int, default=40)
    ap.add_argument("--out", default=OUTDIR)
    ap.add_argument("--no-html", action="store_true", help="skip fetching each update's page HTML")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    html_dir = os.path.join(a.out, "html"); os.makedirs(html_dir, exist_ok=True)
    params_base = {"f_days": "on", "changed": f"-{a.days} day", "items_per_page": a.per_page}

    all_rows, seen, expected = [], set(), None
    for page in range(0, 60):
        params = dict(params_base, page=page)
        resp = requests.get(UPDATES, params=params, headers=HEADERS, timeout=45)
        soup = BeautifulSoup(resp.text, "html.parser")
        if expected is None:
            expected = total_expected(soup)
        rows = parse_rows(soup)
        fresh = [r for r in rows if (r["doc_url"], r["changed_iso"], r["section_path"]) not in seen]
        for r in fresh:
            seen.add((r["doc_url"], r["changed_iso"], r["section_path"]))
        all_rows.extend(fresh)
        log(event="page", page=page, rows=len(rows), fresh=len(fresh), total=len(all_rows), expected=expected)
        if not rows or not fresh:
            break
        if expected and len(all_rows) >= expected:
            break

    # fetch each update's page HTML and save one .html file per update
    if not a.no_html:
        for i, r in enumerate(all_rows, 1):
            r["html_file"] = ""; r["content_len"] = 0
            if not r["doc_url"]:
                continue
            body, text = fetch_detail(r["doc_url"])
            if body:
                fn = f"{i:03d}_{slugify(r['title'], 70)}.html"
                path = os.path.join(html_dir, fn)
                page = (f"<!doctype html><meta charset='utf-8'><title>{r['title']}</title>"
                        f"<h1>{r['title']}</h1>"
                        f"<p><b>Changed:</b> {r['changed_date']} &nbsp; <b>Section:</b> {r['section_path']}</p>"
                        f"<p><a href='{r['doc_url']}'>{r['doc_url']}</a></p><hr>{body}")
                with open(path, "w", encoding="utf-8") as f: f.write(page)
                r["html_file"] = os.path.join("html", fn); r["content_len"] = len(text)
            time.sleep(0.7)   # polite delay -> avoid 403 rate-limit
            if i % 10 == 0: log(event="html_progress", done=i, of=len(all_rows))
        log(event="html_done", saved=sum(1 for r in all_rows if r.get("html_file")))

    with open(os.path.join(a.out, "cbb_updates.json"), "w", encoding="utf-8") as f:
        json.dump(all_rows, f, ensure_ascii=False, indent=2)
    df = pd.DataFrame(all_rows)
    df.to_csv(os.path.join(a.out, "cbb_updates.csv"), index=False, encoding="utf-8-sig")
    xlsx = os.path.join(a.out, f"CBB_Updates_{a.days}d.xlsx")
    for i in range(3):
        try:
            df.to_excel(xlsx, index=False); break
        except PermissionError:
            xlsx = os.path.join(a.out, f"CBB_Updates_{a.days}d_{i+1}.xlsx")
    log(event="done", updates=len(all_rows), expected=expected, days=a.days, xlsx=xlsx)

if __name__ == "__main__":
    main()
