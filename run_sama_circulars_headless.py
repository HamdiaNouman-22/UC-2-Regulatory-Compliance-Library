"""
Standalone, headless SAMA Circulars crawler -- run this file directly.

Not part of the DB/orchestrator pipeline (jobs/sama_job.py) and does not touch
SQL Server. It only crawls https://rulebook.sama.gov.sa/en/sama-circulars using
crawler.sama_circulars_crawler.SAMARulebookCrawler (Selenium, headless Chrome)
and writes the results to output/standalone_crawler/sama_circulars_only/, so
they can be handed off / merged in by hand.

Run:
    python run_sama_circulars_headless.py
    python run_sama_circulars_headless.py --limit 20      # quick test run
    python run_sama_circulars_headless.py --show           # debug with a visible browser
    python run_sama_circulars_headless.py --delay 3        # slower, gentler on the site
"""
import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from crawler.sama_circulars_crawler import SAMARulebookCrawler  # noqa: E402

OUTDIR = PROJECT_ROOT / "output" / "standalone_crawler" / "sama_circulars_only"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=None, help="only crawl the first N circulars (default: all)")
    ap.add_argument("--show", action="store_true", help="run with a visible browser window instead of headless")
    ap.add_argument("--delay", type=float, default=1.0, help="seconds to wait between each circular's detail-page request (default: 1.0)")
    args = ap.parse_args()

    OUTDIR.mkdir(parents=True, exist_ok=True)

    crawler = SAMARulebookCrawler(headless=not args.show, request_delay=args.delay)
    documents = crawler.fetch_documents(limit=args.limit)

    json_path = OUTDIR / "sama_circulars.json"
    crawler.save_to_json(documents, filename=str(json_path))

    try:
        import pandas as pd
        from dataclasses import asdict

        df = pd.DataFrame([asdict(doc) for doc in documents])
        df.to_csv(OUTDIR / "sama_circulars.csv", index=False, encoding="utf-8-sig")

        xlsx_path = OUTDIR / "SAMA_Circulars.xlsx"
        for i in range(3):
            try:
                with pd.ExcelWriter(xlsx_path, engine="openpyxl") as xw:
                    df.to_excel(xw, sheet_name="documents", index=False)
                break
            except PermissionError:
                xlsx_path = OUTDIR / f"SAMA_Circulars_{i + 1}.xlsx"
    except ImportError:
        print("pandas/openpyxl not installed -- skipped CSV/XLSX export, JSON only.")

    print(f"\nDone: {len(documents)} circulars written to {OUTDIR}")


if __name__ == "__main__":
    main()
