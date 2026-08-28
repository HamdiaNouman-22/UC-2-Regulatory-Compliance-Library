"""Put a COUNTRY level above the regulators in the compliancecategory tree.

    python -m scripts.add_country_tree            # dry run, writes nothing
    python -m scripts.add_country_tree --apply

WHAT IT DOES
------------
Today every regulator is a ROOT node (`parentid IS NULL`). This creates one node
per country and re-parents the regulators under it:

    Kingdom of Saudi Arabia
      Saudi Arabian Monetary Authority (SAMA)
      Capital Market Authority (CMA)
      ...
    Egypt
      Central Bank of Egypt (CBE)
      Egyptian Anti-Money Laundering ... (MLCU)

WHAT IT DELIBERATELY DOES NOT DO
--------------------------------
It does not touch `regulations` AT ALL — not `doc_path`, not
`compliancecategory_id`. `doc_path` is one of the three identity fields
(document_url, doc_path, title), so rewriting it would give every one of the
stored documents a new identity: the next crawl would read all of them as `new`
and every stored row as `disappeared`, and `disappeared` feeds the withdrawal
gate. The country belongs to the TREE, not to the document's identity.

That is why `utils/countries.py::tree_path` prepends the country when BUILDING
the tree while leaving `doc.doc_path` alone. This script and that function are
two halves of one change: run the script without the code change and the next
crawl recreates every regulator at the root, giving you two SAMA folders.

RUN IT ON A SETTLED TREE. A crawl in flight inserts folder nodes, and
re-parenting underneath one leaves half-migrated state.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from jobs.monitor_jobs import _repo                       # noqa: E402
from utils.countries import country_for, countries        # noqa: E402

BACKUP_DIR = REPO_ROOT / "output"


def snapshot(cur) -> dict:
    """Everything the verification needs to prove nothing else moved."""
    cur.execute("SELECT COUNT(*) FROM compliancecategory")
    nodes = cur.fetchone()[0]
    cur.execute("SELECT compliancecategory_id, title FROM compliancecategory "
                "WHERE parentid IS NULL ORDER BY compliancecategory_id")
    roots = [(int(i), t) for i, t in cur.fetchall()]
    # The mapping that MUST be identical afterwards: no regulation may be
    # re-pointed at a different folder by this script.
    cur.execute("SELECT id, compliancecategory_id FROM regulations")
    regs = {int(i): (int(c) if c is not None else None) for i, c in cur.fetchall()}
    return {"node_count": nodes, "roots": roots, "reg_to_folder": regs}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="actually write. Without it nothing is changed.")
    a = ap.parse_args()

    repo = _repo()
    with repo._get_conn() as conn:
        cur = conn.cursor()
        before = snapshot(cur)

        # ---- plan ------------------------------------------------------- #
        plan, unmapped = [], []
        for cid, title in before["roots"]:
            ctry = country_for(title)
            if ctry:
                plan.append((cid, title, ctry))
            else:
                unmapped.append((cid, title))

        by_country = Counter(c for _, _, c in plan)
        print(f"roots now            : {len(before['roots'])}")
        print(f"nodes now            : {before['node_count']:,}")
        print(f"regulations          : {len(before['reg_to_folder']):,}")
        print()
        for c in sorted(by_country):
            print(f"  {c:26} <- {by_country[c]} regulator(s)")
        if unmapped:
            # Loud: an unmapped regulator silently STAYS at the root, and a tree
            # that is half-migrated looks fine until someone browses it.
            print("\nNOT IN config/countries.yml — these would stay at the root:")
            for cid, t in unmapped:
                print(f"    {cid:>6}  {t}")
            print("\nRefusing to run. Add them to config/countries.yml first: a "
                  "partially migrated tree is harder to notice than none.")
            return 1

        if not a.apply:
            print("\nDRY RUN — nothing written. Re-run with --apply.")
            return 0

        # ---- backup ------------------------------------------------------ #
        stamp = datetime.now().strftime("%Y-%m-%d")
        path = BACKUP_DIR / f"backup_country_tree_{stamp}.json"
        path.write_text(json.dumps(
            {"roots_before": before["roots"],
             "node_count_before": before["node_count"],
             "reg_to_folder_before": before["reg_to_folder"]},
            indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nbacked up -> {path}")

        # ---- write ------------------------------------------------------- #
        created = {}
        for ctry in sorted(by_country):
            cur.execute("SELECT TOP 1 compliancecategory_id FROM compliancecategory "
                        "WHERE title = ? AND parentid IS NULL", [ctry])
            row = cur.fetchone()
            if row:
                created[ctry] = int(row[0])
                print(f"  country node exists: {ctry} = {created[ctry]}")
                continue
            cur.execute(
                "INSERT INTO compliancecategory (title, parentid, type) "
                "OUTPUT INSERTED.compliancecategory_id VALUES (?, NULL, 'F')",
                [ctry])
            created[ctry] = int(cur.fetchone()[0])
            print(f"  created country node: {ctry} = {created[ctry]}")

        moved = 0
        for cid, title, ctry in plan:
            cur.execute("UPDATE compliancecategory SET parentid = ? "
                        "WHERE compliancecategory_id = ?", [created[ctry], cid])
            moved += cur.rowcount
        conn.commit()
        print(f"  re-parented {moved} regulator node(s)")

        # ---- verify ------------------------------------------------------ #
        after = snapshot(cur)
        problems = []

        expected_roots = sorted(created.values())
        actual_roots = sorted(i for i, _ in after["roots"])
        if actual_roots != expected_roots:
            problems.append(
                f"roots are {actual_roots}, expected exactly the country nodes "
                f"{expected_roots}")

        if after["node_count"] != before["node_count"] + len(created):
            problems.append(
                f"node count {after['node_count']} != "
                f"{before['node_count']} + {len(created)} — something other "
                f"than the country nodes was created")

        if after["reg_to_folder"] != before["reg_to_folder"]:
            diff = [k for k in before["reg_to_folder"]
                    if before["reg_to_folder"].get(k) != after["reg_to_folder"].get(k)]
            problems.append(
                f"{len(diff)} regulation(s) changed folder — this script must "
                f"not touch `regulations` at all. e.g. ids {diff[:5]}")

        for cid, title, ctry in plan:
            cur.execute("SELECT parentid FROM compliancecategory "
                        "WHERE compliancecategory_id = ?", [cid])
            got = cur.fetchone()[0]
            if int(got) != created[ctry]:
                problems.append(f"{title!r} parent is {got}, expected "
                                f"{created[ctry]} ({ctry})")

        print()
        if problems:
            print("VERIFICATION FAILED:")
            for p in problems:
                print("   -", p)
            print(f"\nThe backup at {path} holds the previous parentid for every "
                  f"root. Nothing was deleted, so this is reversible.")
            return 1

        print("VERIFIED")
        print(f"   roots          : {len(before['roots'])} -> {len(after['roots'])}"
              f"  ({', '.join(sorted(created))})")
        print(f"   nodes          : {before['node_count']:,} -> {after['node_count']:,}"
              f"  (+{len(created)}, the country nodes only)")
        print(f"   regulations    : {len(after['reg_to_folder']):,} rows, "
              f"0 re-pointed")
        print("\nNEXT: run a monitoring pass and confirm 0 new / 0 modified / "
              "0 disappeared. Identity did not change, so anything else means "
              "utils/countries.py is not wired into the path that ran.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
