"""
approve_and_store.py
After reviewing all_volumes_results.xlsx, run this script to push
the crawled docs to MSSQL.

Usage:
    python approve_and_store.py                    # interactive prompt
    python approve_and_store.py --yes              # skip prompt, store all
    python approve_and_store.py --volumes "Common Volume,Referee Volume"
    python approve_and_store.py --dry-run          # validate without inserting
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

# ── pyodbc (MSSQL driver) ─────────────────────────────────────────────────────
try:
    import pyodbc
except ImportError:
    print("Run: pip install pyodbc")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── DB Config — edit these ────────────────────────────────────────────────────
DB_SERVER   = "YOUR_SERVER"          # e.g. "localhost" or "myserver.database.windows.net"
DB_NAME     = "YOUR_DATABASE"
DB_USER     = "YOUR_USER"
DB_PASSWORD = "YOUR_PASSWORD"
DB_DRIVER   = "ODBC Driver 17 for SQL Server"  # or 18

PENDING_JSON = "pending_docs.json"

# ── DDL (run once to create the table if it doesn't exist) ───────────────────
CREATE_TABLE_SQL = """
IF NOT EXISTS (
    SELECT * FROM sys.tables WHERE name = 'rulebook_docs'
)
CREATE TABLE rulebook_docs (
    id               INT IDENTITY(1,1) PRIMARY KEY,
    volume_index     INT,
    volume_label     NVARCHAR(255),
    title            NVARCHAR(512),
    url              NVARCHAR(2048),
    doc_path         NVARCHAR(1024),
    depth            INT,
    is_folder        BIT,
    content_hash     NVARCHAR(64),
    pdf_link         NVARCHAR(2048),
    pdf_links_all    NVARCHAR(MAX),
    faq_link         NVARCHAR(2048),
    html_file        NVARCHAR(1024),
    content_preview  NVARCHAR(MAX),
    content_text     NVARCHAR(MAX),
    crawled_at       DATETIME2,
    stored_at        DATETIME2 DEFAULT GETUTCDATE()
);
"""

UPSERT_SQL = """
MERGE rulebook_docs AS target
USING (VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)) AS source (
    volume_index, volume_label, title, url, doc_path,
    depth, is_folder, content_hash, pdf_link, pdf_links_all,
    faq_link, html_file, content_preview, content_text,
    crawled_at, stored_at
)
ON target.url = source.url
WHEN MATCHED THEN
    UPDATE SET
        volume_index    = source.volume_index,
        volume_label    = source.volume_label,
        title           = source.title,
        doc_path        = source.doc_path,
        depth           = source.depth,
        is_folder       = source.is_folder,
        content_hash    = source.content_hash,
        pdf_link        = source.pdf_link,
        pdf_links_all   = source.pdf_links_all,
        faq_link        = source.faq_link,
        html_file       = source.html_file,
        content_preview = source.content_preview,
        content_text    = source.content_text,
        crawled_at      = source.crawled_at,
        stored_at       = GETUTCDATE()
WHEN NOT MATCHED THEN
    INSERT (
        volume_index, volume_label, title, url, doc_path,
        depth, is_folder, content_hash, pdf_link, pdf_links_all,
        faq_link, html_file, content_preview, content_text,
        crawled_at, stored_at
    )
    VALUES (
        source.volume_index, source.volume_label, source.title, source.url,
        source.doc_path, source.depth, source.is_folder, source.content_hash,
        source.pdf_link, source.pdf_links_all, source.faq_link,
        source.html_file, source.content_preview, source.content_text,
        source.crawled_at, GETUTCDATE()
    );
"""


# ── Helpers ──────────────────────────────────────────────────────────────────
def _get_connection() -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD};"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, timeout=30)


def _load_pending(json_path: str) -> dict:
    p = Path(json_path)
    if not p.exists():
        log.error(f"Pending file not found: {json_path}")
        log.error("Run test_rulebook_all_volumes_parallel.py first.")
        sys.exit(1)
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def _filter_volumes(docs: List[dict], volumes: Optional[List[str]]) -> List[dict]:
    if not volumes:
        return docs
    volumes_lower = {v.strip().lower() for v in volumes}
    filtered = [d for d in docs if d["volume_label"].strip().lower() in volumes_lower]
    log.info(f"Volume filter: {len(filtered)}/{len(docs)} docs selected.")
    return filtered


def _print_summary(docs: List[dict]) -> None:
    from collections import Counter
    counts = Counter(d["volume_label"] for d in docs)
    print(f"\n{'='*55}")
    print(f"  Docs pending storage: {len(docs)}")
    print(f"{'─'*55}")
    for vol, cnt in sorted(counts.items()):
        print(f"  {vol:<40} {cnt:>5} docs")
    print(f"{'='*55}\n")


def _store(docs: List[dict], crawled_at: str, dry_run: bool) -> None:
    if dry_run:
        log.info(f"DRY-RUN: would upsert {len(docs)} rows (no DB writes).")
        return

    log.info("Connecting to MSSQL ...")
    conn = _get_connection()
    cursor = conn.cursor()

    # Ensure table exists
    log.info("Ensuring table exists ...")
    cursor.execute(CREATE_TABLE_SQL)
    conn.commit()

    crawled_dt = datetime.fromisoformat(crawled_at)
    stored_at  = datetime.utcnow()

    BATCH_SIZE = 200
    inserted = updated = errors = 0

    log.info(f"Upserting {len(docs)} docs in batches of {BATCH_SIZE} ...")
    for i in range(0, len(docs), BATCH_SIZE):
        batch = docs[i : i + BATCH_SIZE]
        for doc in batch:
            try:
                cursor.execute(UPSERT_SQL, (
                    doc["volume_index"],
                    doc["volume_label"],
                    doc["title"],
                    doc["url"],
                    doc["doc_path"],
                    doc["depth"],
                    1 if doc["is_folder"] else 0,
                    doc["content_hash"],
                    doc["pdf_link"],
                    doc["pdf_links_all"],
                    doc["faq_link"],
                    doc["html_file"],
                    doc["content_preview"],
                    doc["content_text"],
                    crawled_dt,
                    stored_at,
                ))
            except pyodbc.Error as exc:
                log.warning(f"Row skipped (url={doc['url'][:60]}): {exc}")
                errors += 1
        conn.commit()
        done = min(i + BATCH_SIZE, len(docs))
        log.info(f"  Progress: {done}/{len(docs)} rows committed")

    cursor.close()
    conn.close()

    log.info(f"\nStorage complete.")
    log.info(f"   Total rows processed : {len(docs)}")
    log.info(f"   Errors / skipped     : {errors}")


# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Approve and store rulebook docs to MSSQL.")
    parser.add_argument("--yes",       action="store_true", help="Skip interactive prompt.")
    parser.add_argument("--dry-run",   action="store_true", help="Validate without inserting.")
    parser.add_argument("--volumes",   type=str, default=None,
                        help='Comma-separated volume labels to store, e.g. "Common Volume,Referee Volume"')
    parser.add_argument("--json",      type=str, default=PENDING_JSON,
                        help=f"Path to pending JSON (default: {PENDING_JSON})")
    args = parser.parse_args()

    payload    = _load_pending(args.json)
    crawled_at = payload.get("crawled_at", datetime.utcnow().isoformat())
    all_docs   = payload["docs"]

    volume_filter = [v.strip() for v in args.volumes.split(",")] if args.volumes else None
    docs = _filter_volumes(all_docs, volume_filter)

    _print_summary(docs)

    if args.dry_run:
        log.info("--dry-run flag set. Validating data only, no DB writes.")
        _store(docs, crawled_at, dry_run=True)
        return

    if not args.yes:
        answer = input(f"Store {len(docs)} docs to MSSQL [{DB_SERVER}/{DB_NAME}]? [y/N]: ").strip().lower()
        if answer != "y":
            log.info("Aborted by user.")
            sys.exit(0)

    _store(docs, crawled_at, dry_run=False)


if __name__ == "__main__":
    main()