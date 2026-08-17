"""
scripts/cleanup_monitoring.py
Rolls back all monitoring changes made on or after CUTOFF_DATE,
then leaves the DB in a clean state ready for a fresh monitoring run.

Set DRY_RUN = True  → prints what WOULD happen, touches nothing
Set DRY_RUN = False → executes the cleanup

Steps
-----
1. Reactivate any regulation_versions that monitoring set to 'inactive'
   (to make room for a new 'active' V2 that we are about to delete)
2. Delete all regulation_versions created >= CUTOFF_DATE
3. Revert regulations.status to 'active' for rows that monitoring set to 'deleted'
   (only for regulations that existed before the cutoff)
4. Delete regulations created >= CUTOFF_DATE (monitoring-added NEW entries)
5. Delete compliancecategory leaf nodes that became orphaned after step 4
   (no regulations, no children, compliancecategory_id > MAX_INITIAL_CAT_ID)
"""

import os, sys, pyodbc
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DRY_RUN     = True              # ← flip to False to actually run
CUTOFF_DATE = "2026-05-25"      # monitoring started here
TAG = "[DRY-RUN]" if DRY_RUN else "[EXECUTE]"


def conn():
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


def run():
    c = conn()
    cur = c.cursor()

    print(f"\n{'='*65}")
    print(f"  CBB Monitoring Cleanup   CUTOFF = {CUTOFF_DATE}   {TAG}")
    print(f"{'='*65}\n")

    # ── 0. Baseline counts ────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM regulations WHERE regulator='Central Bank of Bahrain'")
    total_regs = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM regulation_versions WHERE regulator='Central Bank of Bahrain'")
    total_vers = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM compliancecategory")
    total_cats = cur.fetchone()[0]
    print(f"Current state:")
    print(f"  regulations       : {total_regs:,}")
    print(f"  regulation_versions: {total_vers:,}")
    print(f"  compliancecategory : {total_cats:,}\n")

    # ── 1. Versions to delete ─────────────────────────────────────────
    cur.execute("""
        SELECT COUNT(*), MIN(created_at), MAX(created_at)
        FROM regulation_versions
        WHERE regulator = 'Central Bank of Bahrain'
          AND CAST(created_at AS DATE) >= ?
    """, [CUTOFF_DATE])
    r = cur.fetchone()
    vers_del_count = r[0]
    print(f"Step 1  regulation_versions to DELETE  : {vers_del_count:,}")
    print(f"        date range: {r[1]}  to  {r[2]}")

    # Break down by change_summary
    cur.execute("""
        SELECT change_summary, status, COUNT(*) as n
        FROM regulation_versions
        WHERE regulator = 'Central Bank of Bahrain'
          AND CAST(created_at AS DATE) >= ?
        GROUP BY change_summary, status
        ORDER BY n DESC
    """, [CUTOFF_DATE])
    for row in cur.fetchall():
        print(f"        [{row[1]:8}]  {row[0] or '(null)':40}  x{row[2]}")
    print()

    # ── 1b. sama_requirement_mapping FK refs that will be cleared ────
    cur.execute("""
        SELECT COUNT(*)
        FROM sama_requirement_mapping
        WHERE version_id IN (
            SELECT version_id FROM regulation_versions
            WHERE regulator = 'Central Bank of Bahrain'
              AND CAST(created_at AS DATE) >= ?
        )
    """, [CUTOFF_DATE])
    srm_count = cur.fetchone()[0]
    print(f"Step 1b sama_requirement_mapping FK refs to clear  : {srm_count:,}\n")

    # ── 2. Old versions to REACTIVATE ────────────────────────────────
    cur.execute("""
        SELECT COUNT(DISTINCT rv_old.version_id)
        FROM regulation_versions rv_old
        WHERE rv_old.regulator = 'Central Bank of Bahrain'
          AND rv_old.status    = 'inactive'
          AND CAST(rv_old.created_at AS DATE) < ?
          AND EXISTS (
              SELECT 1 FROM regulation_versions rv_new
              WHERE rv_new.regulation_id = rv_old.regulation_id
                AND CAST(rv_new.created_at AS DATE) >= ?
                AND rv_new.status IN ('active','inactive')
          )
    """, [CUTOFF_DATE, CUTOFF_DATE])
    react_count = cur.fetchone()[0]
    print(f"Step 2  old regulation_versions to REACTIVATE  : {react_count:,}")
    print(f"        (set back to active after deleting monitoring V2)\n")

    # ── 3. Regulations whose status to REVERT ────────────────────────
    cur.execute("""
        SELECT COUNT(*)
        FROM regulations
        WHERE regulator = 'Central Bank of Bahrain'
          AND status    = 'deleted'
          AND CAST(created_at AS DATE) < ?
    """, [CUTOFF_DATE])
    revert_status_count = cur.fetchone()[0]
    print(f"Step 3  regulations.status to REVERT to 'active'  : {revert_status_count:,}")
    print(f"        (pre-existing regs marked deleted by monitoring backfill)\n")

    # ── 4. Regulations to DELETE + FK ref counts ─────────────────────
    cur.execute("""
        SELECT COUNT(*)
        FROM regulations
        WHERE regulator = 'Central Bank of Bahrain'
          AND CAST(created_at AS DATE) >= ?
    """, [CUTOFF_DATE])
    regs_del_count = cur.fetchone()[0]
    print(f"Step 4  regulations to DELETE (monitoring-added NEW)  : {regs_del_count:,}")

    for tbl, col in [("sama_requirement_mapping", "regulation_id"),
                     ("processinglogs",            "regulation_id"),
                     ("compliance_analysis",        "regulation_id"),
                     ("compliance_analysis_versions","regulation_id"),
                     ("gap_analysis",               "regulation_id")]:
        cur.execute(f"""
            SELECT COUNT(*) FROM {tbl}
            WHERE {col} IN (
                SELECT id FROM regulations
                WHERE regulator='Central Bank of Bahrain'
                  AND CAST(created_at AS DATE) >= ?
            )
        """, [CUTOFF_DATE])
        n = cur.fetchone()[0]
        if n:
            print(f"        FK rows to clear in {tbl}.{col}: {n:,}")

    # Show titles
    cur.execute("""
        SELECT TOP 30 id, title, CAST(created_at AS DATE) as created, status
        FROM regulations
        WHERE regulator = 'Central Bank of Bahrain'
          AND CAST(created_at AS DATE) >= ?
        ORDER BY created_at
    """, [CUTOFF_DATE])
    for row in cur.fetchall():
        print(f"        id={row[0]:6}  [{row[3]:8}]  {str(row[2])}  {row[1][:55]}")
    print()

    # ── 5. Orphaned compliancecategory leaves ─────────────────────────
    # Categories that will have zero regulations after step 4,
    # AND zero child categories. We also restrict to IDs above
    # the max ID that existed before monitoring (approximated by
    # finding the max cat_id linked to a pre-cutoff regulation).
    cur.execute("""
        SELECT MAX(compliancecategory_id)
        FROM compliancecategory cc
        WHERE EXISTS (
            SELECT 1 FROM regulations r
            WHERE r.compliancecategory_id = cc.compliancecategory_id
              AND r.regulator = 'Central Bank of Bahrain'
              AND CAST(r.created_at AS DATE) < ?
        )
    """, [CUTOFF_DATE])
    max_initial_cat = cur.fetchone()[0] or 0

    cur.execute("""
        SELECT cc.compliancecategory_id, cc.title, cc.parentid
        FROM compliancecategory cc
        WHERE cc.compliancecategory_id > ?
          AND NOT EXISTS (
              SELECT 1 FROM regulations r
              WHERE r.compliancecategory_id = cc.compliancecategory_id
                AND r.regulator = 'Central Bank of Bahrain'
                AND CAST(r.created_at AS DATE) < ?
          )
          AND NOT EXISTS (
              SELECT 1 FROM compliancecategory child
              WHERE child.parentid = cc.compliancecategory_id
          )
        ORDER BY cc.compliancecategory_id
    """, [max_initial_cat, CUTOFF_DATE])
    orphan_cats = cur.fetchall()
    print(f"Step 5  orphaned category leaves to DELETE  : {len(orphan_cats):,}")
    print(f"        (max initial cat_id = {max_initial_cat})")
    for row in orphan_cats[:20]:
        print(f"        id={row[0]:6}  parent={str(row[2]):6}  {row[1][:55]}")
    if len(orphan_cats) > 20:
        print(f"        ... and {len(orphan_cats)-20} more")
    print()

    # ── Summary ───────────────────────────────────────────────────────
    print(f"{'='*65}")
    print(f"  SUMMARY  {TAG}")
    print(f"  regulation_versions deleted    : {vers_del_count:,}")
    print(f"  regulation_versions reactivated: {react_count:,}")
    print(f"  regulations.status reverted    : {revert_status_count:,}")
    print(f"  regulations deleted            : {regs_del_count:,}")
    print(f"  categories deleted             : {len(orphan_cats):,}")
    print(f"{'='*65}\n")

    if DRY_RUN:
        print("DRY RUN complete. Set DRY_RUN = False to execute.")
        c.close()
        return

    # ═══════════════════════════════════════════════════════════════════
    # EXECUTE
    # ═══════════════════════════════════════════════════════════════════
    print("Executing cleanup...\n")

    # Step 2 first: reactivate old versions BEFORE deleting new ones
    cur.execute("""
        UPDATE rv_old
        SET rv_old.status = 'active'
        FROM regulation_versions rv_old
        WHERE rv_old.regulator = 'Central Bank of Bahrain'
          AND rv_old.status    = 'inactive'
          AND CAST(rv_old.created_at AS DATE) < ?
          AND EXISTS (
              SELECT 1 FROM regulation_versions rv_new
              WHERE rv_new.regulation_id = rv_old.regulation_id
                AND CAST(rv_new.created_at AS DATE) >= ?
          )
    """, [CUTOFF_DATE, CUTOFF_DATE])
    print(f"  Reactivated {cur.rowcount:,} old regulation_versions")

    # Step 1a: clear FK references that point at regulation_versions before deleting them
    cur.execute("""
        UPDATE sama_requirement_mapping
        SET version_id = NULL
        WHERE version_id IN (
            SELECT version_id FROM regulation_versions
            WHERE regulator = 'Central Bank of Bahrain'
              AND CAST(created_at AS DATE) >= ?
        )
    """, [CUTOFF_DATE])
    print(f"  Cleared {cur.rowcount:,} sama_requirement_mapping.version_id FK refs")

    # compliance_analysis also has a version_id FK -> regulation_versions
    cur.execute("""
        UPDATE compliance_analysis
        SET version_id = NULL
        WHERE version_id IN (
            SELECT version_id FROM regulation_versions
            WHERE regulator = 'Central Bank of Bahrain'
              AND CAST(created_at AS DATE) >= ?
        )
    """, [CUTOFF_DATE])
    print(f"  Cleared {cur.rowcount:,} compliance_analysis.version_id FK refs")

    # Step 1b: delete monitoring versions
    cur.execute("""
        DELETE FROM regulation_versions
        WHERE regulator = 'Central Bank of Bahrain'
          AND CAST(created_at AS DATE) >= ?
    """, [CUTOFF_DATE])
    print(f"  Deleted {cur.rowcount:,} monitoring regulation_versions")

    # Step 3: revert deleted status on pre-existing regs
    cur.execute("""
        UPDATE regulations
        SET status = 'active'
        WHERE regulator = 'Central Bank of Bahrain'
          AND status    = 'deleted'
          AND CAST(created_at AS DATE) < ?
    """, [CUTOFF_DATE])
    print(f"  Reverted {cur.rowcount:,} regulations.status to 'active'")

    # Step 4a: clear all FK references to monitoring-added regulations
    # Collect the reg IDs first so we can use them in parameterised deletes
    cur.execute("""
        SELECT id FROM regulations
        WHERE regulator = 'Central Bank of Bahrain'
          AND CAST(created_at AS DATE) >= ?
    """, [CUTOFF_DATE])
    reg_id_rows = cur.fetchall()
    reg_ids_to_del = [r[0] for r in reg_id_rows]

    if reg_ids_to_del:
        rp = ",".join("?" * len(reg_ids_to_del))

        cur.execute(f"DELETE FROM sama_requirement_mapping WHERE regulation_id IN ({rp})", reg_ids_to_del)
        print(f"  Cleared {cur.rowcount:,} sama_requirement_mapping FK rows (regulation_id)")

        cur.execute(f"DELETE FROM processinglogs WHERE regulation_id IN ({rp})", reg_ids_to_del)
        print(f"  Cleared {cur.rowcount:,} processinglogs FK rows")

        # compliance_analysis_versions may reference compliance_analysis, delete child first
        cur.execute(f"DELETE FROM compliance_analysis_versions WHERE regulation_id IN ({rp})", reg_ids_to_del)
        print(f"  Cleared {cur.rowcount:,} compliance_analysis_versions FK rows")

        cur.execute(f"DELETE FROM compliance_analysis WHERE regulation_id IN ({rp})", reg_ids_to_del)
        print(f"  Cleared {cur.rowcount:,} compliance_analysis FK rows")

        cur.execute(f"DELETE FROM gap_analysis WHERE regulation_id IN ({rp})", reg_ids_to_del)
        print(f"  Cleared {cur.rowcount:,} gap_analysis FK rows")

    # Step 4b: delete monitoring-added regulations
    # (their versions and all FK refs were already cleared above)
    cur.execute("""
        DELETE FROM regulations
        WHERE regulator = 'Central Bank of Bahrain'
          AND CAST(created_at AS DATE) >= ?
    """, [CUTOFF_DATE])
    print(f"  Deleted {cur.rowcount:,} monitoring-added regulations")

    # Step 5: delete orphaned category leaves (batch to stay under 2100-param limit)
    if orphan_cats:
        orphan_ids = [row[0] for row in orphan_cats]
        deleted_cats = 0
        batch_size = 1000
        for i in range(0, len(orphan_ids), batch_size):
            batch = orphan_ids[i:i + batch_size]
            ph = ",".join("?" * len(batch))
            cur.execute(f"DELETE FROM compliancecategory WHERE compliancecategory_id IN ({ph})", batch)
            deleted_cats += cur.rowcount
        print(f"  Deleted {deleted_cats:,} orphaned category entries")

    c.commit()
    print("\nCleanup committed successfully.")

    # ── Post-cleanup counts ───────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM regulations WHERE regulator='Central Bank of Bahrain'")
    print(f"\nPost-cleanup regulations        : {cur.fetchone()[0]:,}")
    cur.execute("SELECT COUNT(*) FROM regulation_versions WHERE regulator='Central Bank of Bahrain'")
    print(f"Post-cleanup regulation_versions: {cur.fetchone()[0]:,}")
    cur.execute("SELECT COUNT(*) FROM compliancecategory")
    print(f"Post-cleanup compliancecategory : {cur.fetchone()[0]:,}")

    c.close()


if __name__ == "__main__":
    if "--execute" in sys.argv:
        # Safety: require explicit flag to avoid accidental execution
        import builtins
        builtins.DRY_RUN = False
        # Redefine at module level
        globals()["DRY_RUN"] = False
        globals()["TAG"] = "[EXECUTE]"
    run()
