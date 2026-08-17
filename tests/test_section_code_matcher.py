"""
Test: Section Code Matching for CBB Monitoring Crawler
=======================================================
READ-ONLY diagnostic. Does not modify anything.

Produces a clean Excel report with:
  Sheet 1 - Summary         : counts at a glance
  Sheet 2 - New Records      : one row per new monitoring-crawl record
  Sheet 3 - Old Matches      : what older DB records were found for each new one
  Sheet 4 - No Section Code  : new records we cannot classify

Run from the project root:
    python tests/test_section_code_matcher.py
"""

import os
import re
import sys
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

import pyodbc

# ── DB connection ────────────────────────────────────────────────────────────

def get_conn():
    conn_str = (
        f"DRIVER={os.getenv('MSSQL_DRIVER')};"
        f"SERVER={os.getenv('MSSQL_SERVER')};"
        f"DATABASE={os.getenv('MSSQL_DATABASE')};"
        f"UID={os.getenv('MSSQL_USERNAME')};"
        f"PWD={os.getenv('MSSQL_PASSWORD')};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


# ── Section code helpers ─────────────────────────────────────────────────────

SECTION_CODE_RE = re.compile(
    r'^([A-Z]{1,5}-\d+[A-Z]?(?:\.\d+[A-Z]?)*)',
    re.IGNORECASE
)

def extract_section_code(title: str):
    m = SECTION_CODE_RE.match((title or "").strip())
    return m.group(1).upper() if m else None

def extract_volume(category: str):
    m = re.search(r'Volume\s+\d+', category or '', re.IGNORECASE)
    return m.group(0) if m else (category or '').strip()

def html_to_text(html: str, max_chars: int = 300) -> str:
    """Strip HTML tags and return plain-text preview."""
    if not html:
        return ""
    # Simple tag stripper — avoid heavy dependency
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text).strip()
    if len(text) > max_chars:
        text = text[:max_chars] + "..."
    return text

def relationship(new_code: str, old_code: str) -> str:
    """
    Describe how the old section code relates to the new one.
    new_code = LR-1A.1  old_code = LR-1A.1        -> SAME
    new_code = LR-1A.1  old_code = LR-1A.1.1       -> OLD is CHILD  (new is parent)
    new_code = LR-1A.1.1 old_code = LR-1A.1        -> OLD is PARENT (new is child)
    """
    if new_code == old_code:
        return "SAME"
    if old_code.startswith(new_code + "."):
        return "OLD is CHILD of new"
    if new_code.startswith(old_code + "."):
        return "OLD is PARENT of new"
    return "SIBLING / PARTIAL"


# ── DB queries ───────────────────────────────────────────────────────────────

def get_recent_cbb_records(conn, days_back=15, limit=200):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TOP (?)
            r.id,
            r.title,
            r.source_page_url,
            r.document_url,
            r.category,
            CONVERT(varchar(30), r.created_at, 120) AS created_at,
            r.content_hash,
            r.document_html
        FROM regulations r
        WHERE r.regulator = 'Central Bank of Bahrain'
          AND r.created_at >= DATEADD(day, -?, SYSDATETIMEOFFSET())
        ORDER BY r.created_at DESC
    """, limit, days_back)
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def find_older_by_section_code(conn, section_code: str, volume: str, exclude_id: int):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            r.id,
            r.title,
            r.source_page_url,
            r.document_url,
            r.category,
            CONVERT(varchar(30), r.created_at, 120) AS created_at,
            r.content_hash,
            r.document_html
        FROM regulations r
        WHERE r.regulator = 'Central Bank of Bahrain'
          AND r.id != ?
          AND r.title LIKE ?
          AND r.category LIKE ?
        ORDER BY r.created_at ASC
    """, exclude_id, section_code + '%', '%' + volume + '%')
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


# ── Main ─────────────────────────────────────────────────────────────────────

EMPTY_HASH = "d41d8cd98f00b204e9800998ecf8427e"
OUTPUT_FILE = PROJECT_ROOT / "tests" / "section_code_matches_v2.xlsx"


def run():
    print("Connecting to DB...")
    conn = get_conn()

    recent = get_recent_cbb_records(conn, days_back=15, limit=200)
    print(f"Recent CBB records found: {len(recent)}")

    new_records_rows = []   # Sheet 2 — one row per new record
    old_matches_rows = []   # Sheet 3 — one row per (new, old) pair
    no_code_rows     = []   # Sheet 4

    for rec in recent:
        title   = rec["title"] or ""
        cat     = rec["category"] or ""
        volume  = extract_volume(cat)
        code    = extract_section_code(title)
        is_empty = (rec.get("content_hash") or "") == EMPTY_HASH
        text_preview = html_to_text(rec.get("document_html") or "")

        if not code:
            no_code_rows.append({
                "ID":            rec["id"],
                "Title":         title,
                "Category":      cat,
                "Created":       rec["created_at"],
                "Source URL":    rec["source_page_url"],
                "Content":       text_preview,
            })
            continue

        older = find_older_by_section_code(conn, code, volume, exclude_id=rec["id"])
        n_old = len(older)

        # Classify what kind of matches we got
        exact = [o for o in older if extract_section_code(o["title"]) == code]
        children = [o for o in older if relationship(code, extract_section_code(o["title"] or "")) == "OLD is CHILD of new"]
        other = [o for o in older if o not in exact and o not in children]

        _DELETION_KW = re.compile(r'\b(deleted|moved\s+to\s+section|removed|transferred)\b', re.IGNORECASE)
        is_deletion_notice = is_empty and bool(_DELETION_KW.search(title))

        if is_deletion_notice:
            action = "MARK OLD RECORDS DELETED"
        elif is_empty:
            action = "FOLDER PAGE — no action (sub-sections still live)"
        elif exact:
            action = "ARCHIVE OLD -> CREATE NEW VERSION"
        else:
            action = "REVIEW — no exact match, only related records found"

        new_records_rows.append({
            "New ID":            rec["id"],
            "Section Code":      code,
            "Volume":            volume,
            "Title":             title,
            "Is Empty / Deleted": (
                "YES — CBB deleted this section" if is_deletion_notice
                else "YES — folder/index page (sub-sections still live)" if is_empty
                else "No — has content"
            ),
            "Suggested Action":  action,
            "# Old Matches":     n_old,
            "# Exact matches":   len(exact),
            "# Child matches":   len(children),
            "Created":           rec["created_at"],
            "New Source URL":    rec["source_page_url"],
            "New Content":       text_preview,
        })

        for old in older:
            old_code = extract_section_code(old["title"] or "")
            rel = relationship(code, old_code or "")
            old_preview = html_to_text(old.get("document_html") or "")
            old_empty = (old.get("content_hash") or "") == EMPTY_HASH

            old_matches_rows.append({
                "New ID":          rec["id"],
                "New Section Code": code,
                "New Title":       title,
                "New Created":     rec["created_at"],
                "New Source URL":  rec["source_page_url"],
                "New Is Empty":    "YES" if is_empty else "No",
                "New Content Preview": text_preview,
                "──────":          "──────",
                "Old ID":          old["id"],
                "Old Section Code": old_code or "(none)",
                "Old Title":       old["title"],
                "Old Created":     old["created_at"],
                "Old Source URL":  old["source_page_url"],
                "Old Is Empty":    "YES" if old_empty else "No",
                "Old Content Preview": old_preview,
                "──────2":         "──────",
                "Relationship":    rel,
                "Action":          action,
                "What this means": _explain(rel, is_empty, code),
            })

    conn.close()

    # ── Summary ──────────────────────────────────────────────────────────────
    matched_ids = {r["New ID"] for r in new_records_rows if r["# Old Matches"] > 0}
    no_match_ids = {r["New ID"] for r in new_records_rows if r["# Old Matches"] == 0}

    summary_rows = [
        {"Metric": "Total recent CBB records (last 15 days)", "Count": len(recent)},
        {"Metric": "Records with a section code",             "Count": len(new_records_rows)},
        {"Metric": "Records WITHOUT a section code",          "Count": len(no_code_rows)},
        {"Metric": "Records that found old matches in DB",    "Count": len(matched_ids)},
        {"Metric": "Records with NO old match (truly new)",   "Count": len(no_match_ids)},
        {"Metric": "Total (new -> old) pairs found",           "Count": len(old_matches_rows)},
        {"Metric": "", "Count": ""},
        {"Metric": "=== What the Relationship column means ===", "Count": ""},
        {"Metric": "SAME",                     "Count": "New and old are the same section code — direct replacement"},
        {"Metric": "OLD is CHILD of new",      "Count": "Old record was a sub-section (e.g. LR-1A.1.1) under new (LR-1A.1) — CBB collapsed children into parent"},
        {"Metric": "OLD is PARENT of new",     "Count": "Old record was a parent section — unlikely, investigate"},
        {"Metric": "SIBLING / PARTIAL",        "Count": "Partial code overlap — review manually"},
        {"Metric": "", "Count": ""},
        {"Metric": "=== What the Action column means ===", "Count": ""},
        {"Metric": "MARK OLD RECORDS DELETED",              "Count": "New record is empty (CBB deleted section). Mark all matched old records as deleted in DB."},
        {"Metric": "ARCHIVE OLD -> CREATE NEW VERSION",      "Count": "New record has content. Archive old version, point regulation to new content."},
        {"Metric": "REVIEW — no exact match",               "Count": "No exact section code match found. Inspect manually."},
    ]

    print(f"Writing Excel to: {OUTPUT_FILE}")

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:

        df_summary = pd.DataFrame(summary_rows)
        df_summary.to_excel(writer, sheet_name="1_Summary", index=False)
        _autofit(writer, "1_Summary", df_summary)

        df_new = pd.DataFrame(new_records_rows)
        df_new.to_excel(writer, sheet_name="2_New_Records", index=False)
        _autofit(writer, "2_New_Records", df_new)

        if old_matches_rows:
            df_old = pd.DataFrame(old_matches_rows)
            df_old.to_excel(writer, sheet_name="3_Old_Matches", index=False)
            _autofit(writer, "3_Old_Matches", df_old)

        if no_code_rows:
            df_nc = pd.DataFrame(no_code_rows)
            df_nc.to_excel(writer, sheet_name="4_No_Section_Code", index=False)
            _autofit(writer, "4_No_Section_Code", df_nc)

    print(f"Done. Open: {OUTPUT_FILE}")
    print()
    print("QUICK SUMMARY")
    print("=============")
    for r in summary_rows:
        if r["Metric"]:
            print(f"  {r['Metric']:50s}  {r['Count']}")


def _explain(rel: str, is_empty: bool, code: str) -> str:
    if rel == "SAME":
        return "Direct replacement. If empty -> CBB deleted this section. If has content -> CBB updated it."
    if rel == "OLD is CHILD of new":
        if is_empty:
            return (f"CBB deleted/collapsed the whole {code} section. "
                    "All these child sub-sections should be marked DELETED in the DB.")
        return (f"CBB merged sub-sections into the parent {code}. "
                "Old children are superseded; new record is the combined replacement.")
    if rel == "OLD is PARENT of new":
        return "Old was a parent; new is a child. Possibly a newly added sub-section."
    return "Partial match. Check manually."


def _autofit(writer, sheet_name: str, df: pd.DataFrame, max_width: int = 60):
    ws = writer.sheets[sheet_name]
    for col_cells in ws.columns:
        header = col_cells[0].value or ""
        is_content = any(k in header.lower() for k in ("content", "html", "means", "action", "metric"))
        w = max_width if is_content else min(max(len(str(header)) + 4, 12), 40)
        ws.column_dimensions[col_cells[0].column_letter].width = w


if __name__ == "__main__":
    run()
