"""Rename ZATCA to its full name, in the files AND the database, together.

    python -m scripts.rename_zatca            # dry run, writes nothing
    python -m scripts.rename_zatca --apply

    "ZATCA"  ->  "Zakat, Tax and Customs Authority (ZATCA)"

WHY THIS IS A MIGRATION AND NOT A FIND-AND-REPLACE
--------------------------------------------------
`regulator` becomes `doc_path[0]`, and doc_path is one of the three identity
fields:

    DEFAULT_IDENTITY = ("document_url", "doc_path", "title")

So the crawlers and the stored rows must change IN THE SAME OPERATION. Change
only the files and the next crawl matches nothing: all 151 documents read as
`new`, all 151 stored rows read as `disappeared`, and `disappeared` feeds the
withdrawal gate. Change only the database and the same thing happens in reverse.

WHAT IT TOUCHES, and why each one matters

    regulations.regulator            151   the name itself
    regulations.doc_path[0]          151   IDENTITY. json.dumps(list), so the
                                           first element is rewritten in python,
                                           never by string replace on the column
    regulation_versions.regulator    302   kept in step so history reads back
    compliancecategory node 8186       1   the tree node under Kingdom of Saudi
                                           Arabia; renamed in place so the
                                           country migration is not undone
    run_history keys                  16   the completeness gate's baselines are
                                           keyed on the source NAME. Left behind,
                                           every ZATCA form loses its baseline
                                           and the gate silently has nothing to
                                           compare against
    config/*.yml, hints/*.yml          9   what the next crawl will emit
    output/change_state/*.json         6   absence streaks, keyed by identity

WHAT IT DOES NOT TOUCH
    output/workbooks/zatca.*.xlsx — exported under the OLD name. They are stale
    after this and must be re-exported, never promoted: promoting one would
    insert its rows again under the old regulator.

RUN IT ON A SETTLED LIBRARY. No crawl, no monitoring, no export in flight.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from jobs.monitor_jobs import _repo                    # noqa: E402

OLD = "ZATCA"
NEW = "Zakat, Tax and Customs Authority (ZATCA)"

#: Only where the value is the regulator NAME. `zatca.yml`'s filename, the
#: source_system values and the prose comments all keep saying ZATCA, correctly —
#: this renames the regulator, not the acronym.
FILE_EDITS = [
    ("config/sources/zatca.yml", f'regulator: "{OLD}"', f'regulator: "{NEW}"'),
    ("config/countries.yml", f'    - "{OLD}"', f'    - "{NEW}"'),
    ("config/change_signals.yml", f'regulator: "{OLD}"', f'regulator: "{NEW}"'),
] + [
    (f"dynamic_crawler/hints/zatca.{f}.yml",
     f'regulator: "{OLD}"', f'regulator: "{NEW}"')
    for f in ("agreements", "ie_agreements", "ie_circulars", "ie_guidelines", "taxes")
] + [
    # CHEAP_PROBE_SOURCES holds (regulator, source_system) pairs and the sweep
    # scopes on them, so a stale name here means the probe looks for a regulator
    # that no longer exists and quietly finds nothing. The OTHER mentions of
    # ZATCA in this file are prose about the acronym and must NOT change.
    ("jobs/monitor_jobs.py",
     f'("{OLD}", "Rules and Regulations")', f'("{NEW}", "Rules and Regulations")'),
]


def edit_files(apply: bool) -> list:
    out = []
    for rel, old, new in FILE_EDITS:
        p = REPO_ROOT / rel
        if not p.exists():
            out.append((rel, "MISSING FILE", 0))
            continue
        s = p.read_text(encoding="utf-8")
        n = s.count(old)
        if n and apply:
            p.write_text(s.replace(old, new), encoding="utf-8")
        out.append((rel, old, n))
    return out


def edit_state(apply: bool) -> list:
    """Absence streaks, keyed on identity — which contains doc_path."""
    out = []
    for p in sorted((REPO_ROOT / "output" / "change_state").rglob("*.json")):
        s = p.read_text(encoding="utf-8")
        if f'"{OLD}"' not in s and f"{OLD} |" not in s and f"{OLD} >" not in s:
            continue
        n = s.count(OLD)
        if apply:
            # The name appears inside JSON string values (doc_path lists, joined
            # forms). Replacing the bare token is safe here because ZATCA is not
            # a substring of any other word in these files.
            p.write_text(s.replace(f'"{OLD}"', f'"{NEW}"')
                          .replace(f"{OLD} | ", f"{NEW} | ")
                          .replace(f"{OLD} > ", f"{NEW} > "), encoding="utf-8")
        out.append((str(p.relative_to(REPO_ROOT)), n))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    repo = _repo()
    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, doc_path FROM regulations WHERE regulator = ?", [OLD])
        rows = [(int(i), d) for i, d in cur.fetchall()]
        cur.execute("SELECT COUNT(*) FROM regulation_versions WHERE regulator = ?", [OLD])
        n_versions = cur.fetchone()[0]
        cur.execute("SELECT compliancecategory_id, parentid FROM compliancecategory "
                    "WHERE title = ?", [OLD])
        nodes = cur.fetchall()
        cur.execute("SELECT run_id, source FROM run_history WHERE source = ? "
                    "OR source LIKE ?", [OLD, OLD + "/%"])
        runs = cur.fetchall()

        print(f'"{OLD}"  ->  "{NEW}"\n')
        print(f"  regulations              {len(rows)}")
        print(f"  regulation_versions      {n_versions}")
        print(f"  compliancecategory nodes {len(nodes)}  {[tuple(n) for n in nodes]}")
        print(f"  run_history keys         {len(runs)}")
        print()
        for rel, old, n in edit_files(apply=False):
            print(f"  {'file':6} {n:>3}x  {rel}")
        for rel, n in edit_state(apply=False):
            print(f"  {'state':6} {n:>3}x  {rel}")

        if not a.apply:
            print("\nDRY RUN — nothing written. Re-run with --apply.")
            return 0

        stamp = datetime.now().strftime("%Y-%m-%d")
        backup = REPO_ROOT / "output" / f"backup_zatca_rename_{stamp}.json"
        backup.write_text(json.dumps(
            {"old": OLD, "new": NEW,
             "regulations": [{"id": i, "doc_path": d} for i, d in rows],
             "version_count": n_versions,
             "nodes": [list(n) for n in nodes],
             "run_history": [list(r) for r in runs]},
            indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nbacked up -> {backup}")

        # ---- doc_path, element 0, in python -------------------------------- #
        changed = 0
        for rid, raw in rows:
            try:
                parts = json.loads(raw) if raw else []
            except Exception:
                print(f"  SKIPPED id {rid}: doc_path is not json ({str(raw)[:40]})")
                continue
            if not parts or parts[0] != OLD:
                continue
            parts[0] = NEW
            cur.execute("UPDATE regulations SET doc_path = ? WHERE id = ?",
                        [json.dumps(parts, ensure_ascii=False), rid])
            changed += 1
        cur.execute("UPDATE regulations SET regulator = ? WHERE regulator = ?", [NEW, OLD])
        n_reg = cur.rowcount
        cur.execute("UPDATE regulation_versions SET regulator = ? WHERE regulator = ?", [NEW, OLD])
        n_ver = cur.rowcount
        cur.execute("UPDATE compliancecategory SET title = ? WHERE title = ?", [NEW, OLD])
        n_node = cur.rowcount
        # run_history keys are "<source>" and "<source>/<form>"
        cur.execute("UPDATE run_history SET source = ? WHERE source = ?", [NEW, OLD])
        n_run = cur.rowcount
        cur.execute("UPDATE run_history SET source = ? + SUBSTRING(source, ?, 200) "
                    "WHERE source LIKE ?", [NEW, len(OLD) + 1, OLD + "/%"])
        n_run += cur.rowcount
        conn.commit()
        print(f"  doc_path rewritten       {changed}")
        print(f"  regulations.regulator    {n_reg}")
        print(f"  versions.regulator       {n_ver}")
        print(f"  tree node retitled       {n_node}")
        print(f"  run_history keys         {n_run}")

        for rel, old, n in edit_files(apply=True):
            if n:
                print(f"  file  {n:>3}x  {rel}")
        for rel, n in edit_state(apply=True):
            print(f"  state {n:>3}x  {rel}")

        # ---- verify -------------------------------------------------------- #
        problems = []
        cur.execute("SELECT COUNT(*) FROM regulations WHERE regulator = ?", [OLD])
        if cur.fetchone()[0]:
            problems.append("regulations still hold the old regulator")
        cur.execute("SELECT COUNT(*) FROM regulations WHERE regulator = ?", [NEW])
        got = cur.fetchone()[0]
        if got != len(rows):
            problems.append(f"{got} rows under the new name, expected {len(rows)}")
        # CHECKED IN PYTHON, NOT WITH `LIKE`. In T-SQL `[` opens a character
        # class, so the obvious pattern '["<name>"%' matches NOTHING and this
        # check reported all 151 rows broken when every one was correct. Same
        # reason `_norm_doc_path` compares doc_path in python: the column is JSON
        # our own code wrote, and SQL pattern matching is the wrong tool for it.
        cur.execute("SELECT doc_path FROM regulations WHERE regulator = ?", [NEW])
        stragglers = 0
        for (raw,) in cur.fetchall():
            try:
                parts = json.loads(raw) if raw else []
            except Exception:
                parts = []
            if not parts or parts[0] != NEW:
                stragglers += 1
        if stragglers:
            problems.append(f"{stragglers} row(s) whose doc_path does not start "
                            f"with the new name — identity would not match")
        cur.execute("SELECT COUNT(*) FROM compliancecategory WHERE title = ?", [OLD])
        if cur.fetchone()[0]:
            problems.append("a tree node still titled with the old name")
        cur.execute("SELECT parentid FROM compliancecategory WHERE title = ?", [NEW])
        parents = [p[0] for p in cur.fetchall()]
        if len(parents) != 1:
            problems.append(f"expected exactly one tree node, found {len(parents)}")

        print()
        if problems:
            print("VERIFICATION FAILED:")
            for p in problems:
                print("   -", p)
            print(f"\n{backup} holds every previous value.")
            return 1
        print("VERIFIED")
        print(f"   {got} regulations, doc_path[0] rewritten on all of them")
        print(f"   tree node kept its parent ({parents[0]}) — the country level "
              f"is intact")
        print("\nNEXT: re-export any zatca workbook before promoting it — the ones "
              "on disk hold the OLD name.\nThen run a ZATCA crawl and require "
              "0 new / 0 modified / 0 disappeared.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
