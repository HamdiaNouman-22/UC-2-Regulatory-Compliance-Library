"""Remove one regulator from the library completely, so it can be re-crawled.

    python -m scripts.purge_regulator "Central Bank of Egypt (CBE)"
    python -m scripts.purge_regulator "Central Bank of Egypt (CBE)" --apply

WHAT IT REMOVES, and why each one matters if left behind

    regulations                 the documents
    regulation_versions         their content history
    compliance_analysis         + _versions, the LLM output keyed on them
    sama_requirement_mapping    matching verdicts keyed on them
    processinglogs              the per-document run log
    compliancecategory          the FOLDER SUBTREE under the regulator's node.
                                Left behind, the next crawl finds the old leaf
                                nodes and `_get_or_create_compliance_category`
                                refuses to reuse a leaf another regulation owns
                                -- except the owner is gone, so it creates a
                                SECOND node beside each orphan and the tree
                                doubles.
    run_history                 the completeness gate's baselines. Left behind,
                                the next run is measured against a count from
                                the library it just replaced: a fresh crawl of
                                621 documents compared to a stored 621 looks
                                fine, but a fresh crawl of 400 would PASS
                                against a baseline it should have failed.
    output/change_state/*.json  absence streaks keyed on identity. Left behind,
                                a document absent from the new crawl carries a
                                streak from the old library and can reach the
                                withdrawal gate a run earlier than it should.

THE REGULATOR'S OWN TREE NODE IS KEPT, and so is its country parent. Deleting it
would only mean the next crawl recreates it -- at the ROOT, if the regulator is
not in config/countries.yml -- and that is a worse outcome than an empty folder.

ORDER MATTERS. Children before parents, or the foreign keys refuse. The one that
bit the earlier dedupe cleanup: `compliancecategory` keys on
`compliancecategory_id`, not `id`.

BACKED UP FIRST. Everything removed is written to output/ as json, so a purge
made in error is recoverable by hand.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from jobs.monitor_jobs import _repo                    # noqa: E402


def _slug(name: str) -> str:
    """The regulator name as change_state spells it in a filename.

    Runs of non-alphanumerics collapse to ONE underscore, so
    "Central Bank of Egypt (CBE)" -> "central_bank_of_egypt_cbe" and not
    "central_bank_of_egypt__cbe_", which is what a naive per-character
    substitution gives and which matches no file on disk. The dry run found the
    state file only after this was fixed.
    """
    import re as _re
    return _re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("regulator", help='exact regulations.regulator value')
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    REG = a.regulator

    repo = _repo()
    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT @@SERVERNAME, DB_NAME()")
        srv, db = cur.fetchone()
        print(f"database : {srv} / {db}")
        print(f"regulator: {REG!r}\n")

        cur.execute("SELECT id, compliancecategory_id FROM regulations WHERE regulator = ?", [REG])
        rows = cur.fetchall()
        ids = [int(r[0]) for r in rows]
        cats = sorted({int(r[1]) for r in rows if r[1] is not None})
        if not ids:
            print("nothing to remove -- no rows under that exact regulator name.")
            return 0
        idlist = ",".join(map(str, ids))

        counts = {"regulations": len(ids)}
        for tbl in ("regulation_versions", "compliance_analysis",
                    "compliance_analysis_versions", "sama_requirement_mapping",
                    "processinglogs"):
            try:
                cur.execute(f"SELECT COUNT(*) FROM {tbl} WHERE regulation_id IN ({idlist})")
                counts[tbl] = cur.fetchone()[0]
            except Exception:
                counts[tbl] = None          # table absent on this database
        cur.execute("SELECT COUNT(*) FROM run_history WHERE source = ? OR source LIKE ?",
                    [REG, REG + "/%"])
        counts["run_history"] = cur.fetchone()[0]
        counts["compliancecategory (leaves)"] = len(cats)

        for k, v in counts.items():
            print(f"   {k:34} {'n/a' if v is None else v}")

        state = sorted((REPO_ROOT / "output" / "change_state").rglob(f"*{_slug(REG)}*.json"))
        for p in state:
            print(f"   {'change_state file':34} {p.name}")

        if not a.apply:
            print("\nDRY RUN -- nothing removed. Re-run with --apply.")
            return 0

        stamp = datetime.now().strftime("%Y-%m-%d_%H%M")
        backup = REPO_ROOT / "output" / f"backup_purge_{_slug(REG)}_{stamp}.json"
        dump = {"database": f"{srv}/{db}", "regulator": REG, "ids": ids, "tables": {}}
        cur.execute(f"""SELECT id, title, document_url, source_page_url, doc_path,
                               content_hash, compliancecategory_id, source_system,
                               CAST(extra_meta AS nvarchar(max))
                        FROM regulations WHERE id IN ({idlist})""")
        cols = [c[0] for c in cur.description]
        dump["tables"]["regulations"] = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.execute("SELECT run_id, source, row_count, verdict FROM run_history "
                    "WHERE source = ? OR source LIKE ?", [REG, REG + "/%"])
        dump["tables"]["run_history"] = [list(r) for r in cur.fetchall()]
        backup.write_text(json.dumps(dump, indent=2, ensure_ascii=False, default=str),
                          encoding="utf-8")
        print(f"\nbacked up -> {backup}")

        removed = {}
        # children first
        for tbl in ("compliance_analysis_versions", "compliance_analysis",
                    "sama_requirement_mapping", "processinglogs",
                    "regulation_versions"):
            try:
                cur.execute(f"DELETE FROM {tbl} WHERE regulation_id IN ({idlist})")
                removed[tbl] = cur.rowcount
            except Exception as e:
                removed[tbl] = f"skipped ({str(e)[:40]})"
        cur.execute(f"DELETE FROM regulations WHERE id IN ({idlist})")
        removed["regulations"] = cur.rowcount
        # leaf folders, now that nothing points at them. The regulator's own node
        # and its country parent are deliberately kept.
        cur.execute("SELECT compliancecategory_id FROM compliancecategory WHERE title = ?", [REG])
        own = {int(x[0]) for x in cur.fetchall()}
        drop = [c for c in cats if c not in own]
        n_cat = 0
        for cid in drop:
            cur.execute("SELECT COUNT(*) FROM regulations WHERE compliancecategory_id = ?", [cid])
            if cur.fetchone()[0]:
                continue                    # another regulator shares this folder
            cur.execute("SELECT COUNT(*) FROM compliancecategory WHERE parentid = ?", [cid])
            if cur.fetchone()[0]:
                continue                    # still has children
            cur.execute("DELETE FROM compliancecategory WHERE compliancecategory_id = ?", [cid])
            n_cat += cur.rowcount
        removed["compliancecategory"] = n_cat
        cur.execute("DELETE FROM run_history WHERE source = ? OR source LIKE ?",
                    [REG, REG + "/%"])
        removed["run_history"] = cur.rowcount
        conn.commit()

        for p in state:
            p.unlink()
            removed.setdefault("change_state files", []).append(p.name)

        print()
        for k, v in removed.items():
            print(f"   removed {k:32} {v}")

        # ---- verify ------------------------------------------------------- #
        cur.execute("SELECT COUNT(*) FROM regulations WHERE regulator = ?", [REG])
        left_regs = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM run_history WHERE source = ? OR source LIKE ?",
                    [REG, REG + "/%"])
        left_runs = cur.fetchone()[0]
        cur.execute("SELECT compliancecategory_id, parentid FROM compliancecategory WHERE title = ?", [REG])
        node = cur.fetchall()
        print()
        if left_regs or left_runs:
            print(f"VERIFICATION FAILED: {left_regs} regulation(s), {left_runs} "
                  f"run_history row(s) remain")
            return 1
        print("VERIFIED")
        print(f"   regulations remaining : {left_regs}")
        print(f"   run_history remaining : {left_runs}")
        print(f"   tree node kept        : {[tuple(n) for n in node]}  (empty, ready for the re-crawl)")
        return 0


if __name__ == "__main__":
    sys.exit(main())
