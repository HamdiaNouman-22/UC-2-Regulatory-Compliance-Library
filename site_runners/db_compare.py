"""
db_compare.py  --  test a crawl against the existing DB baseline (READ-ONLY).

Loads what the crawler brought in (a crawl output json/xlsx), pulls the matching
existing rows from the `regulations` table (SELECT only -- never writes prod), and
writes a diff Excel so you can SEE, per document:

    matched         -> crawler found it AND it's already in the DB
    only_in_crawl   -> crawler brought it in but it's NOT in the DB (new / changed / extra)
    only_in_db      -> in the DB but the crawler MISSED it (coverage gap)

plus a summary sheet with counts + coverage %.

Matching is by normalised TITLE and by URL-slug (robust to the crawl storing the
web page while the DB stores the PDF).

Run:
  venv/Scripts/python.exe site_runners/db_compare.py \
     --crawl output/standalone_crawler/batch_cbb_regs_resolutions_tree/cbb_resolutions.json \
     --regulator "Central Bank of Bahrain" --category "CBB Regulations and Resolutions" \
     --out output/standalone_crawler/_db_compare/cbb_regs_resolutions
"""
import os, sys, json, re, argparse
import pyodbc
from dotenv import load_dotenv
import pandas as pd

try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

def norm_title(s):
    return re.sub(r"[^a-z0-9]+", " ", str(s or "").lower()).strip()

def slug(url):
    u = re.sub(r"[#?].*$", "", str(url or "")).rstrip("/")
    return u.rsplit("/", 1)[-1].lower()

def load_crawl(path):
    rows = []
    if path.lower().endswith(".json"):
        data = json.load(open(path, encoding="utf-8"))
        data = data if isinstance(data, list) else data.get("documents") or data.get("pages") or []
    else:
        df = pd.read_excel(path)
        data = df.to_dict("records")
    for r in data:
        title = r.get("title") or r.get("Title") or ""
        url = r.get("url") or r.get("doc_url") or r.get("document_url") or r.get("pdf_links") or ""
        if title or url:
            rows.append({"title": str(title), "url": str(url)})
    return rows

def db_conn():
    load_dotenv()
    cs = (f"DRIVER={os.getenv('MSSQL_DRIVER')};SERVER={os.getenv('MSSQL_SERVER')};"
          f"DATABASE={os.getenv('MSSQL_DATABASE')};UID={os.getenv('MSSQL_USERNAME')};"
          f"PWD={os.getenv('MSSQL_PASSWORD')};TrustServerCertificate=yes")
    return pyodbc.connect(cs, timeout=30, readonly=True)   # READ-ONLY

def load_db(regulator, category, source_system):
    where = ["regulator = ?"]; params = [regulator]
    if category:      where.append("category = ?");      params.append(category)
    if source_system: where.append("source_system = ?"); params.append(source_system)
    sql = ("SELECT title, document_url, source_page_url, published_date, reference_no "
           "FROM regulations WHERE " + " AND ".join(where))
    con = db_conn(); cur = con.cursor(); cur.execute(sql, params)
    rows = [{"title": r[0] or "", "document_url": r[1] or "", "source_page_url": r[2] or "",
             "published_date": str(r[3] or ""), "reference_no": r[4] or ""} for r in cur.fetchall()]
    con.close()
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--crawl", required=True, help="crawl output json/xlsx")
    ap.add_argument("--regulator", required=True)
    ap.add_argument("--category", default=None)
    ap.add_argument("--source-system", default=None)
    ap.add_argument("--out", required=True, help="output dir")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    crawl = load_crawl(a.crawl)
    db = load_db(a.regulator, a.category, a.source_system)
    print(f"crawl rows: {len(crawl)} | db rows: {len(db)}")

    # index DB by normalised title and by url-slug
    db_titles = {norm_title(d["title"]): d for d in db}
    db_slugs = {}
    for d in db:
        for u in (d["document_url"], d["source_page_url"]):
            if u: db_slugs.setdefault(slug(u), d)

    matched, only_crawl = [], []
    matched_db_ids = set()
    for c in crawl:
        nt, sl = norm_title(c["title"]), slug(c["url"])
        hit = (db_titles.get(nt) if nt else None) or (db_slugs.get(sl) if sl else None)
        if hit:
            matched.append({"crawl_title": c["title"], "db_title": hit["title"],
                            "crawl_url": c["url"], "db_document_url": hit["document_url"]})
            matched_db_ids.add(id(hit))
        else:
            only_crawl.append(c)
    only_db = [d for d in db if id(d) not in matched_db_ids]

    cov = round(100 * len(matched) / len(db), 1) if db else 0.0
    summary = pd.DataFrame([
        {"metric": "crawl documents", "value": len(crawl)},
        {"metric": "db documents (baseline)", "value": len(db)},
        {"metric": "matched (in both)", "value": len(matched)},
        {"metric": "only in crawl (new/extra)", "value": len(only_crawl)},
        {"metric": "only in db (crawler missed)", "value": len(only_db)},
        {"metric": "coverage of DB by crawl %", "value": cov},
    ])
    xlsx = os.path.join(a.out, "db_compare.xlsx")
    for i in range(3):
        try:
            with pd.ExcelWriter(xlsx, engine="openpyxl") as xw:
                summary.to_excel(xw, "summary", index=False)
                pd.DataFrame(matched).to_excel(xw, "matched", index=False)
                pd.DataFrame(only_crawl).to_excel(xw, "only_in_crawl", index=False)
                pd.DataFrame(only_db).to_excel(xw, "only_in_db", index=False)
            break
        except PermissionError:
            xlsx = os.path.join(a.out, f"db_compare_{i+1}.xlsx")
    print(f"matched={len(matched)} only_in_crawl={len(only_crawl)} only_in_db={len(only_db)} coverage={cov}%")
    print("xlsx:", xlsx)

if __name__ == "__main__":
    main()
