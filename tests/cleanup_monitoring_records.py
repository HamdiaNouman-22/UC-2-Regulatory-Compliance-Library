"""
Cleanup: delete the 13 bad monitoring records (IDs 108648-108660)
created by the broken monitoring crawler run on 2026-07-02.

These records were created with wrong logic (HC-6.1 treated as deleted,
duplicate runs, doc_path root mismatch). The fixed crawler will re-create
correct records on the next scheduled run.

Run with --dry-run first (default), then --execute to apply.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

import pyodbc

MONITORING_IDS = [i for i in range(108661, 108674) if i != 108664]  # 108661-108673, keep Appendix CA-24

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


def preview(conn):
    cursor = conn.cursor()
    placeholders = ",".join("?" * len(MONITORING_IDS))
    cursor.execute(f"""
        SELECT id, title,
               CONVERT(varchar(30), created_at, 120) AS created_at,
               category, content_hash
        FROM regulations
        WHERE id IN ({placeholders})
        ORDER BY id
    """, MONITORING_IDS)
    rows = cursor.fetchall()

    print(f"\nRecords to DELETE ({len(rows)} found of {len(MONITORING_IDS)} expected):")
    print("-" * 90)
    for row in rows:
        reg_id, title, created, category, chash = row
        empty = "(EMPTY)" if chash == "d41d8cd98f00b204e9800998ecf8427e" else "(has content)"
        print(f"  [{reg_id}]  {empty}  {(title or '')[:60]}")
        print(f"           Created: {created}  |  Category: {(category or '')[:50]}")
    print("-" * 90)

    # Also check regulation_versions for these IDs
    cursor.execute(f"""
        SELECT regulation_id, COUNT(*) as cnt
        FROM regulation_versions
        WHERE regulation_id IN ({placeholders})
        GROUP BY regulation_id
    """, MONITORING_IDS)
    ver_rows = cursor.fetchall()
    if ver_rows:
        print(f"\nRelated regulation_versions rows:")
        for r in ver_rows:
            print(f"  regulation_id={r[0]}  versions={r[1]}")
    else:
        print("\nNo regulation_versions rows for these IDs.")

    return len(rows)


def execute(conn):
    cursor = conn.cursor()
    placeholders = ",".join("?" * len(MONITORING_IDS))

    # Delete child rows first (regulation_versions)
    cursor.execute(f"""
        DELETE FROM regulation_versions
        WHERE regulation_id IN ({placeholders})
    """, MONITORING_IDS)
    ver_deleted = cursor.rowcount
    print(f"  Deleted {ver_deleted} regulation_versions rows")

    # Delete the regulations themselves
    cursor.execute(f"""
        DELETE FROM regulations
        WHERE id IN ({placeholders})
    """, MONITORING_IDS)
    reg_deleted = cursor.rowcount
    print(f"  Deleted {reg_deleted} regulations rows")

    conn.commit()
    print(f"\nDone. {reg_deleted} monitoring records removed.")


if __name__ == "__main__":
    dry_run = "--execute" not in sys.argv

    print("Connecting to DB...")
    conn = get_conn()

    found = preview(conn)

    if dry_run:
        print("\n*** DRY RUN — nothing deleted ***")
        print("Re-run with --execute to apply.")
    else:
        if found == 0:
            print("\nNothing to delete.")
        else:
            print(f"\nExecuting delete of {found} records...")
            execute(conn)

    conn.close()
