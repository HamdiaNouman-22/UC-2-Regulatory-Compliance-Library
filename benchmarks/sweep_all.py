"""Sweep every source in the library and print one table.

WHAT THIS IS FOR

`dynamic_crawler.cli.sweep` answers "has this source changed?" for ONE
(regulator, source_system) pair. The library now holds eleven, so testing
monitoring meant eleven commands and eleven JSON reports to read side by side.
This runs them all and prints the counts as a table.

WHAT A RUN MEANS

The sweep compares what a regulator serves NOW against a state file under
output/change_state/. So:

    first run   every document is `new`   -- there is no prior state to compare
                against. This is the BASELINE, not a finding.
    second run  `unchanged` for anything the regulator has not touched, and
                `modified` for anything it has. THIS is the test.

Running it once and reading "new: 8552" as a change is the mistake this note
exists to prevent.

LOAD

Every document is one HTTP request, and SAMA alone is 6,101. `--limit` caps the
documents probed PER SOURCE and defaults to 25 so a test run costs a few hundred
requests rather than nine thousand. Pass `--limit 0` for the real thing, and
expect it to take a while.

A source does not need an entry in config/change_signals.yml — without one it
uses `defaults`. An entry adds `confirm` (fetch and hash before believing a
counter), a different identity, or a cheaper signal (MHRSD has a sitemap).
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def sources_from_db() -> list[tuple[str, str, int]]:
    """Every (regulator, source_system) the library actually holds."""
    import pyodbc
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env", override=True)
    cs = (f"DRIVER={os.getenv('MSSQL_DRIVER')};"
          f"SERVER={os.getenv('MSSQL_SERVER')};"
          f"DATABASE={os.getenv('MSSQL_DATABASE')};")
    user = os.getenv("MSSQL_USERNAME")
    cs += (f"UID={user};PWD={os.getenv('MSSQL_PASSWORD')};" if user
           else "Trusted_Connection=yes;")
    cs += "TrustServerCertificate=yes;"
    with pyodbc.connect(cs, autocommit=True) as cn:
        return [(r[0], r[1], r[2]) for r in cn.execute(
            "SELECT regulator, source_system, COUNT(*) FROM regulations "
            "WHERE ISNULL(regulator,'') <> '' "
            "GROUP BY regulator, source_system ORDER BY COUNT(*) DESC")]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--limit", type=int, default=25,
                    help="documents probed per source; 0 = all (slow)")
    ap.add_argument("--only", default=None,
                    help="substring filter on the regulator name")
    args = ap.parse_args()

    rows = sources_from_db()
    if args.only:
        rows = [r for r in rows if args.only.lower() in r[0].lower()]
    if not rows:
        print("no sources found")
        return 1

    print(f"{'REGULATOR':<50} {'STORED':>6} {'SEEN':>5} "
          f"{'NEW':>5} {'MOD':>5} {'SAME':>5} {'?':>4}  BASIS")
    print("-" * 104)
    failures = []
    for regulator, source, stored in rows:
        cmd = [sys.executable, "-B", "-m", "dynamic_crawler.cli.sweep",
               "--signal", "stored-inventory", "--regulator", regulator,
               "--source", source or "", "--with-db"]
        if args.limit:
            cmd += ["--limit", str(args.limit)]
        p = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
        try:
            # The report is the last JSON object on stdout.
            rep = json.loads(p.stdout[p.stdout.index("{"):])
            c = rep.get("counts", {})
            basis = ",".join(sorted((rep.get("by_basis") or {}))) or "-"
            print(f"{regulator[:50]:<50} {stored:>6} {rep.get('observed',0):>5} "
                  f"{c.get('new',0):>5} {c.get('modified',0):>5} "
                  f"{c.get('unchanged',0):>5} {c.get('unknown',0):>4}  {basis}")
        except Exception:
            failures.append((regulator, (p.stderr or p.stdout or "").strip()[-160:]))
            print(f"{regulator[:50]:<50} {stored:>6}   FAILED")
    for regulator, err in failures:
        print(f"\n{regulator}:\n  {err}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
