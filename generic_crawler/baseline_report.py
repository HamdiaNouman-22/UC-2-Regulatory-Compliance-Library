"""
baseline_report.py — turn a baseline run into ONE Excel workbook you can read.

baseline.py runs the crawls and leaves a folder per site under output/_baseline/.
This script collects all of that into a single workbook so the numbers can be
checked by hand, without opening six files.

It reads only. It never crawls and never changes the crawler.

SHEETS
------
  Summary     one row per site: pages, documents, shape, cap hit, errors, flag
  Documents   every document found, across all sites (site column first)
  Pages       every page recorded, across all sites (site column first)

THE 'flag' COLUMN ON Summary -- read this first
-----------------------------------------------
  OK          real measurement, trust it
  ZERO        crawler found nothing -- extraction FAILED, the site is not empty
  CAP         stopped at the page cap; the count is the LIMIT, not the coverage
  NO-DOCS     pages were found but zero documents -- suspicious, check by hand
  TIMEOUT     did not finish
  NO-RESULT   crawler produced no result

USAGE
-----
  venv/Scripts/python.exe generic_crawler/baseline_report.py
  venv/Scripts/python.exe generic_crawler/baseline_report.py --tag after-1.2

Writes output/_baseline/baseline_report.xlsx (or _<tag>.xlsx when --tag is given).
Safe to run while a crawl is still going -- it reports whatever is on disk so far.
"""
import argparse
import json
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
OUTROOT = REPO / "output" / "_baseline"
CELL_MAX = 32000


def load_history():
    """The run log written by baseline.py, if it exists yet."""
    store = OUTROOT / "results.json"
    if not store.exists():
        return []
    try:
        return json.loads(store.read_text(encoding="utf-8"))
    except Exception:
        return []


def pick_run(history, tag):
    if not history:
        return None
    if tag:
        for run in reversed(history):
            if run.get("tag") == tag:
                return run
        return None
    return history[-1]


def read_site_sheets(site_dir: Path):
    """Return (pages_df, documents_df) for one site folder.

    pages.xlsx is written by every code path (tree, table and generic), so it is
    the one file we can always rely on. pages.json only exists for the generic
    walk, which is why it isn't used here.
    """
    xlsx = site_dir / "pages.xlsx"
    if not xlsx.exists():
        return None, None
    pages = docs = None
    try:
        book = pd.read_excel(xlsx, sheet_name=None)
    except Exception:
        return None, None
    for name, df in book.items():
        low = name.strip().lower()
        if low == "pages":
            pages = df
        elif low == "documents":       # sheet is omitted when nothing was found
            docs = df
    return pages, docs


def flag_for(rec):
    if rec.get("status") == "timeout":
        return "TIMEOUT"
    if rec.get("status") in ("no-result", None) and rec.get("pages") is None:
        return "NO-RESULT"
    pages, docs = rec.get("pages"), rec.get("documents")
    if (pages or 0) == 0 and (docs or 0) == 0:
        return "ZERO"
    if rec.get("cap_hit"):
        return "CAP"
    if (docs or 0) == 0:
        return "NO-DOCS"
    return "OK"


def main():
    ap = argparse.ArgumentParser(description="Build one Excel from a baseline run")
    ap.add_argument("--tag", default="", help="which run to report (default: latest)")
    args = ap.parse_args()

    if not OUTROOT.exists():
        print(f"Nothing to report: {OUTROOT} does not exist. Run baseline.py first.")
        return 1

    run = pick_run(load_history(), args.tag)
    if run is None and args.tag:
        print(f"No run tagged '{args.tag}' in results.json.")
        return 1

    # Map site name -> its recorded numbers (when the run log is available).
    recs = {r["site"]: r for r in (run or {}).get("results", [])}

    # Every site folder on disk, so a crawl still in progress is still reported.
    site_dirs = sorted(p for p in OUTROOT.iterdir() if p.is_dir())

    summary, all_docs, all_pages = [], [], []

    for site_dir in site_dirs:
        # folder name is the slug; recover the display name from the run log
        name = next((s for s in recs if s.lower().replace(" ", "_") == site_dir.name),
                    site_dir.name)
        rec = recs.get(name, {})
        pages_df, docs_df = read_site_sheets(site_dir)

        n_pages_file = 0 if pages_df is None else len(pages_df)
        n_docs_file = 0 if docs_df is None else len(docs_df)

        summary.append({
            "site": name,
            "flag": flag_for(rec) if rec else ("ZERO" if n_pages_file == 0 else "OK"),
            "shape": rec.get("shape", ""),
            "scope": rec.get("scope_detected") or rec.get("scope_requested", ""),
            "pages": rec.get("pages", n_pages_file),
            "documents": rec.get("documents", n_docs_file),
            "pages_in_file": n_pages_file,
            "documents_in_file": n_docs_file,
            "cap_hit": rec.get("cap_hit", ""),
            "errors": rec.get("errors", ""),
            "retries": rec.get("retries", ""),
            "seconds": rec.get("seconds", ""),
            "status": rec.get("status", "not-in-log"),
            "note": rec.get("note", ""),
            "url": rec.get("url", ""),
        })

        if docs_df is not None and len(docs_df):
            d = docs_df.copy()
            d.insert(0, "site", name)
            all_docs.append(d)
        if pages_df is not None and len(pages_df):
            p = pages_df.copy()
            p.insert(0, "site", name)
            all_pages.append(p)

    summary_df = pd.DataFrame(summary)
    docs_out = (pd.concat(all_docs, ignore_index=True) if all_docs
                else pd.DataFrame(columns=["site", "title", "doc_url", "type",
                                           "found_on", "section_path"]))
    pages_out = (pd.concat(all_pages, ignore_index=True) if all_pages
                 else pd.DataFrame(columns=["site", "section_path", "title", "url"]))

    # Excel refuses cells over ~32k characters.
    for df in (docs_out, pages_out):
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].apply(
                    lambda v: v[:CELL_MAX] if isinstance(v, str) else v)

    suffix = f"_{args.tag}" if args.tag else ""
    out = OUTROOT / f"baseline_report{suffix}.xlsx"
    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        summary_df.to_excel(xw, sheet_name="Summary", index=False)
        docs_out.to_excel(xw, sheet_name="Documents", index=False)
        pages_out.to_excel(xw, sheet_name="Pages", index=False)

    print(f"tag: {run.get('tag') if run else '(no run log yet)'}")
    print(f"sites: {len(summary_df)}   documents: {len(docs_out)}   "
          f"pages: {len(pages_out)}")
    print(summary_df[["site", "flag", "shape", "pages", "documents",
                      "status"]].to_string(index=False))
    bad = summary_df[summary_df["flag"] != "OK"]
    if len(bad):
        print("\nNEEDS A LOOK:")
        for _, r in bad.iterrows():
            print(f"  {r['site']:<16} {r['flag']:<10} {r['note']}")
    print(f"\nWrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
