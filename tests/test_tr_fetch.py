"""
Standalone test: fetch TR changed pages and dump to Excel.
Run from project root:
    python -m tests.test_tr_fetch
"""

import sys
import os
import logging
from datetime import date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawler.cbb_monitoring_crawler import (
    _get_thomson_reuters_changes, CHANGES_URL, BASE_URL, REQUEST_DELAY, _fetch
)
from urllib.parse import urljoin
from datetime import datetime
import time

FROM_DATE = date(2026, 5, 27)
TO_DATE   = date.today()


def main():
    print(f"\nFetching TR changes from {FROM_DATE} to {TO_DATE} ...")

    # ── verbose page-by-page fetch so we can see what's happening ──
    base_params = {
        "f_date":         "on",
        "min":            FROM_DATE.strftime("%Y-%m-%d"),
        "max":            TO_DATE.strftime("%Y-%m-%d"),
        "items_per_page": "40",
        "sort_by":        "revision_timestamp_1",
    }

    from bs4 import BeautifulSoup
    import re

    def row_date(r):
        dd = r.find("div", class_="book-detail")
        if not dd:
            return None
        t = dd.find("time", attrs={"datetime": True})
        if not t:
            return None
        try:
            return datetime.fromisoformat(t["datetime"]).date()
        except Exception:
            return None

    all_pages = []
    page_num = 0
    while True:
        params = dict(base_params)
        if page_num > 0:
            params["page"] = str(page_num)
        soup = _fetch(CHANGES_URL, params=params)
        if not soup:
            break
        results_area = soup.find("div", class_="view-content")
        if not results_area:
            break
        rows = results_area.find_all("div", class_="views-row")
        if not rows:
            break

        first_date = row_date(rows[0])
        print(f"\n--- Page {page_num + 1} (first entry: {first_date}) ---")

        if first_date is not None and first_date < FROM_DATE:
            print("  ↳ First entry is before from_date — stopping.")
            break

        for row in rows:
            dd = row.find("div", class_="book-detail")
            if not dd:
                continue
            a = dd.find("a", href=True)
            if not a:
                continue
            idate = row_date(row)
            title = a.get_text(strip=True)
            print(f"  [{idate}] {title}")
            if idate is None or (FROM_DATE <= idate <= TO_DATE):
                all_pages.append({
                    "title": title,
                    "url": urljoin(BASE_URL, a["href"]),
                    "change_date": idate.isoformat() if idate else None,
                    "breadcrumb": (row.find("div", class_="book-trail") or dd).get_text(strip=True),
                })

        if len(rows) < 40:
            break
        page_num += 1
        time.sleep(REQUEST_DELAY)

    pages = all_pages
    print(f"\nTotal pages returned: {len(pages)}")

    if not pages:
        print("No results — check network/params.")
        return

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tr_changes.xlsx")
    try:
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "TR Changes"

        headers = ["#", "title", "url", "change_date", "breadcrumb"]
        ws.append(headers)
        for col in ws.iter_cols(min_row=1, max_row=1):
            for cell in col:
                cell.font = openpyxl.styles.Font(bold=True)

        for i, p in enumerate(pages, 1):
            ws.append([
                i,
                p.get("title", ""),
                p.get("url", ""),
                p.get("change_date", ""),
                p.get("breadcrumb", ""),
            ])

        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 80)

        wb.save(out_path)
        print(f"\nSaved to: {out_path}")

    except ImportError:
        import csv
        out_path = out_path.replace(".xlsx", ".csv")
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["title", "url", "change_date", "breadcrumb"])
            writer.writeheader()
            writer.writerows(pages)
        print(f"\nopenpyxl not installed — saved as CSV to: {out_path}")

    print(f"\nSample (first 5):")
    for p in pages[:5]:
        print(f"  [{p['change_date']}] {p['title']}")
        print(f"           {p['url']}")


if __name__ == "__main__":
    main()
