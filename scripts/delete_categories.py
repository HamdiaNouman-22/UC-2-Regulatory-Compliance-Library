"""
scripts/delete_categories.py
Recursively deletes specified compliancecategory IDs and all their
descendants, along with any regulations and regulation_versions that
belong to those categories.

DRY_RUN = True  → show what would be deleted, touch nothing
DRY_RUN = False → execute (pass --execute flag on command line)
"""

import os, sys, pyodbc
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DRY_RUN = True
TAG = "[DRY-RUN]"

# Root category IDs to delete (with all descendants)
ROOT_IDS = [97530, 97532, 97534, 97543, 97545, 97547]


def get_conn():
    cs = (
        f"DRIVER={os.getenv('MSSQL_DRIVER')};"
        f"SERVER={os.getenv('MSSQL_SERVER')};"
        f"DATABASE={os.getenv('MSSQL_DATABASE')};"
        f"UID={os.getenv('MSSQL_USERNAME')};"
        f"PWD={os.getenv('MSSQL_PASSWORD')};"
        f"TrustServerCertificate=yes;"
        f"ConnectRetryCount=3;ConnectRetryInterval=5;"
    )
    return pyodbc.connect(cs, autocommit=False, timeout=30)


def get_all_descendants(cur, root_ids):
    """Return all category IDs in the subtree rooted at root_ids (inclusive)."""
    placeholders = ",".join("?" * len(root_ids))
    cur.execute(f"""
        WITH subtree AS (
            SELECT compliancecategory_id, parentid, title, 0 AS depth
            FROM compliancecategory
            WHERE compliancecategory_id IN ({placeholders})

            UNION ALL

            SELECT c.compliancecategory_id, c.parentid, c.title, s.depth + 1
            FROM compliancecategory c
            INNER JOIN subtree s ON c.parentid = s.compliancecategory_id
        )
        SELECT compliancecategory_id, parentid, title, depth
        FROM subtree
        ORDER BY depth, compliancecategory_id
    """, root_ids)
    return cur.fetchall()   # list of (id, parent, title, depth)


def run():
    c = get_conn()
    cur = c.cursor()

    print(f"\n{'='*65}")
    print(f"  Category Recursive Delete   {TAG}")
    print(f"  Root IDs: {ROOT_IDS}")
    print(f"{'='*65}\n")

    # ── Resolve full subtree ──────────────────────────────────────────
    rows = get_all_descendants(cur, ROOT_IDS)
    if not rows:
        print("No categories found for those IDs. Nothing to do.")
        c.close()
        return

    all_cat_ids = [r[0] for r in rows]
    print(f"Category tree ({len(all_cat_ids)} nodes total):")
    for cat_id, parent, title, depth in rows:
        indent = "  " * depth
        print(f"  {indent}[{cat_id}] {title[:60]}  (parent={parent})")
    print()

    # ── Regulations in those categories ──────────────────────────────
    placeholders = ",".join("?" * len(all_cat_ids))
    cur.execute(f"""
        SELECT id, title, status, CAST(created_at AS DATE) as created
        FROM regulations
        WHERE compliancecategory_id IN ({placeholders})
        ORDER BY compliancecategory_id, id
    """, all_cat_ids)
    regs = cur.fetchall()
    reg_ids = [r[0] for r in regs]

    print(f"Regulations to delete ({len(regs)}):")
    for reg_id, title, status, created in regs:
        print(f"  id={reg_id:6}  [{status:8}]  {str(created)}  {title[:55]}")
    print()

    # ── Regulation versions for those regs ───────────────────────────
    vers_count = 0
    if reg_ids:
        rp = ",".join("?" * len(reg_ids))
        cur.execute(f"""
            SELECT COUNT(*), MIN(CAST(created_at AS DATE)), MAX(CAST(created_at AS DATE))
            FROM regulation_versions
            WHERE regulation_id IN ({rp})
        """, reg_ids)
        r = cur.fetchone()
        vers_count = r[0]
        print(f"Regulation versions to delete: {vers_count}  (range {r[1]} to {r[2]})")
        print()

    # ── Summary ──────────────────────────────────────────────────────
    print(f"{'='*65}")
    print(f"  SUMMARY  {TAG}")
    print(f"  compliancecategory rows to delete : {len(all_cat_ids)}")
    print(f"  regulations to delete             : {len(regs)}")
    print(f"  regulation_versions to delete     : {vers_count}")
    print(f"{'='*65}\n")

    if DRY_RUN:
        print("DRY RUN complete. Run with --execute to apply.")
        c.close()
        return

    # ═══════════════════════════════════════════════════════════════════
    # EXECUTE
    # ═══════════════════════════════════════════════════════════════════
    print("Executing deletion...\n")

    # Delete versions first (FK dependency)
    if reg_ids:
        rp = ",".join("?" * len(reg_ids))
        cur.execute(f"DELETE FROM regulation_versions WHERE regulation_id IN ({rp})", reg_ids)
        print(f"  Deleted {cur.rowcount} regulation_versions")

        cur.execute(f"DELETE FROM regulations WHERE id IN ({rp})", reg_ids)
        print(f"  Deleted {cur.rowcount} regulations")

    # Delete categories bottom-up (leaves first to avoid FK violations)
    # Sort by depth descending so children are deleted before parents
    sorted_cats = sorted(rows, key=lambda x: x[3], reverse=True)
    deleted_cats = 0
    for cat_id, parent, title, depth in sorted_cats:
        cur.execute("DELETE FROM compliancecategory WHERE compliancecategory_id = ?", [cat_id])
        deleted_cats += cur.rowcount

    print(f"  Deleted {deleted_cats} compliancecategory rows")

    c.commit()
    print("\nDone. Changes committed.")
    c.close()


if __name__ == "__main__":
    if "--execute" in sys.argv:
        DRY_RUN = False
        TAG = "[EXECUTE]"
    run()
