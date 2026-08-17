"""Monitoring end to end, one regulator at a time, with timings.

WHAT IT DOES PER REGULATOR

    1  SWEEP     probe the stored inventory -> new / modified / unchanged
    2  TARGETS   write the modified urls to a file
    3  CRAWL     re-crawl ONLY those, through NewOrchestrator -> workbook
    4  PROMOTE   insert into MSSQL: new rows, and a new version per changed doc

Steps 3 and 4 are skipped when the sweep finds nothing, which is the point of
monitoring — an unchanged regulator costs one cheap pass and no crawl.

WHY IT PRINTS AS IT GOES

Each regulator's line is flushed the moment it finishes, and each phase is
timed, so a slow or failing source is visible while the rest are still running
rather than at the end of a batch.

WHAT THE FIRST RUN MEANS

The sweep compares against a state file under output/change_state/. On the very
first run for a source there is nothing to compare against and every document
reads `new` — a BASELINE. The second run is the real test. `--baseline` runs
step 1 only, for exactly this reason: it records the state without pulling a
crawl behind it.

LOAD

One HTTP request per document probed. SAMA alone is 6,101, so `--limit` caps
documents per source and defaults to 25. `--limit 0` is the real thing.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
STATE = REPO_ROOT / "output" / "monitor_targets"

# regulator (as stored) -> how to re-crawl it. A source with no entry can still
# be SWEPT; it just cannot be auto-re-crawled, and the run says so rather than
# quietly skipping the crawl.
# (crawler name, is_form, WORKBOOK LABEL).
#
# The label is given explicitly and is NOT derived from the crawler name. The
# form is filed as `sdaia.regs`, so name.upper() produced "SDAIA.REGS.xlsx"
# beside the real "SDAIA.xlsx" — two workbooks for one regulator in the
# directory whose whole meaning is "these rows go to the database", which is the
# exact trap run_source_standalone documents for its own labels.
CRAWLERS = {
    "Anti-Money Laundering Permanent Committee (AML)": ("aml.rules", True, "AML"),
    "Ministry of Investment (MISA)":                   ("misa.laws", True, "MISA"),
    "Saudi Data and AI Authority (SDAIA)":             ("sdaia.regs", True, "SDAIA"),
    "Ministry of Education":                           ("moe.regulations", True, "MOE"),
    "Ministry of Human Resource and Social Development (MHRSD)":
                                                      ("mhrsd.regs", True, "MHRSD"),
    "Saudi Exchange":                                  ("tadawul.rules", True, "Tadawul"),
    "Ministry of Health":                              ("moh", False, "MOH"),
    "Ministry of Commerce":                            ("mc", False, "MC"),
    "Capital Market Authority (CMA)":                  ("cma", False, "CMA"),
    "Saudi Arabian Monetary Authority (SAMA)":         ("sama", False, "SAMA"),
    # SIMAH is Cloudflare-blocked; its form runs from a snapshot and must not be
    # pulled by a monitoring loop. See config/change_signals.yml skip_hosts.
}


def sources():
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


def signal_for(regulator: str, source: str):
    """Which signal this source uses, and its settings — read from
    config/change_signals.yml, never hardcoded here.

    That file is already where a source's monitoring is described (MHRSD's
    `sitemap:`, AML's `confirm:`), so the CHOICE of signal belongs there too.
    Hardcoding it in this driver would put the same decision in two places, and
    the two would drift the moment one was edited.

    Default is `stored-inventory` — probe every url we store — which is what a
    source with no entry gets.
    """
    import yaml
    cfg = yaml.safe_load(
        (REPO_ROOT / "config" / "change_signals.yml").read_text(encoding="utf-8"))
    for src in (cfg.get("sources") or []):
        if src.get("regulator") == regulator and src.get("source_system") == source:
            return src.get("signal") or "stored-inventory", src
    return "stored-inventory", {}


def say(*a):
    print(*a, flush=True)          # flushed: this is the whole point


def run(cmd, timeout):
    """Run a child and read its output as UTF-8.

    `text=True` alone decodes with the LOCALE encoding, which on this machine is
    cp1252 — and every report here carries Arabic titles and em dashes. The child
    reconfigures its own streams to utf-8, so it writes correctly; the parent was
    the half that could not read it.

    Measured 2026-08-15: CMA's sweep ran the full 600 seconds, probed all 1,979
    documents and wrote its state file, and then the PARENT raised
    UnicodeDecodeError reading the result — reported as "SWEEP FAILED" for work
    that had entirely succeeded. ZATCA the same. `errors="replace"` so a stray
    byte can never again turn a finished sweep into a failure.
    """
    t0 = time.time()
    p = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True,
                       encoding="utf-8", errors="replace", timeout=timeout)
    return p, time.time() - t0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--limit", type=int, default=25,
                    help="documents probed per source; 0 = all")
    ap.add_argument("--only", default=None, help="substring filter on regulator")
    ap.add_argument("--baseline", action="store_true",
                    help="sweep only — record state, pull no crawl")
    ap.add_argument("--no-promote", action="store_true",
                    help="crawl the changed documents but do not write to MSSQL")
    ap.add_argument("--crawl-timeout", type=int, default=5400)
    args = ap.parse_args()

    STATE.mkdir(parents=True, exist_ok=True)
    rows = sources()
    if args.only:
        rows = [r for r in rows if args.only.lower() in r[0].lower()]

    totals = {"swept": 0, "modified": 0, "crawled": 0, "promoted": 0, "failed": 0}
    t_all = time.time()
    for regulator, source, stored in rows:
        say("\n" + "=" * 78)
        say(f"{regulator}  |  {source}  |  {stored} stored")
        say("=" * 78)

        # ---- 1. sweep ----------------------------------------------------- #
        targets_file = STATE / (
            "".join(c if c.isalnum() else "_" for c in regulator)[:60] + ".txt")
        sig, settings = signal_for(regulator, source)

        # `signal: crawl` — THE CRAWL IS THE SIGNAL.
        #
        # For CMA and Ministry of Commerce no probe can answer. MC's host
        # refuses plain HTTP clients outright; CMA's token is noise and a
        # confirm costs a full page fetch per document, so believing it costs
        # what crawling costs. Sweeping them anyway is worse than not sweeping:
        # it reports `unchanged`/`unknown` cleanly and can NEVER detect a
        # change, which reads as monitored when it is not.
        #
        # So the probe is skipped, the crawler runs, and the ORCHESTRATOR makes
        # the new/modified/unchanged decision from what it fetched — the same
        # verdict, arrived at by the only route these two sources allow.
        if sig == "crawl":
            entry = CRAWLERS.get(regulator)
            if not entry:
                say("  1 SIGNAL    crawl — but NO CRAWLER MAPPED for this source")
                totals["failed"] += 1
                continue
            name, is_form, label = entry
            say(f"  1 SIGNAL    crawl (no probe can answer for this source)")
            if args.baseline:
                say("  2 CRAWL     skipped (--baseline: a crawl is not a baseline)")
                continue
            cmd = [sys.executable, "-u", "-B",
                   "benchmarks/run_source_standalone.py", name, "--label", label]
            if is_form:
                cmd.append("--form")
            try:
                p_, dt = run(cmd, timeout=args.crawl_timeout)
            except subprocess.TimeoutExpired:
                say(f"  2 CRAWL     TIMED OUT after {args.crawl_timeout}s")
                totals["failed"] += 1
                continue
            ok = p_.returncode == 0
            say(f"  2 CRAWL     {dt:6.1f}s  {'ok' if ok else 'FAILED rc=%s' % p_.returncode}")
            if not ok:
                say("              " + (p_.stderr or "").strip()[-300:])
                totals["failed"] += 1
                continue
            totals["crawled"] += 1
            wb = None
            for line in p_.stdout.splitlines():
                if "workbook:" in line:
                    wb = line.split("workbook:")[-1].strip()
            say(f"              workbook: {wb}")
            if args.no_promote or not wb:
                say("  3 PROMOTE   skipped")
                continue
            p_, dt = run([sys.executable, "-B", "-m",
                          "dynamic_crawler.formfill.promote", wb], timeout=7200)
            try:
                rep = json.loads(p_.stdout[p_.stdout.index("{"):])
                say(f"  3 PROMOTE   {dt:6.1f}s  inserted={rep.get('inserted')} "
                    f"skipped={rep.get('skipped_already_present')} "
                    f"versions={rep.get('regulation_versions')} "
                    f"failed={rep.get('failed')}")
                totals["promoted"] += 1
            except Exception:
                say(f"  3 PROMOTE   FAILED in {dt:6.1f}s")
                say("              " + (p_.stderr or p_.stdout or "").strip()[-300:])
                totals["failed"] += 1
            continue
        cmd = [sys.executable, "-B", "-m", "dynamic_crawler.cli.sweep",
               "--signal", sig, "--regulator", regulator,
               "--source", source or "", "--with-db",
               "--targets", str(targets_file)]
        # --limit caps DOCUMENTS PROBED, which only means anything when the
        # signal probes documents one by one. A feed reads one page for the
        # whole source, so limiting it would silently truncate the window.
        if args.limit and sig == "stored-inventory":
            cmd += ["--limit", str(args.limit)]
        if sig == "sama-feed" and settings.get("feed_days"):
            cmd += ["--feed-days", str(settings["feed_days"])]
        try:
            p, dt = run(cmd, timeout=1800)
        except subprocess.TimeoutExpired:
            say(f"  1 SWEEP     TIMED OUT after 1800s"); totals["failed"] += 1; continue
        try:
            rep = json.loads(p.stdout[p.stdout.index("{"):])
        except Exception:
            say(f"  1 SWEEP     FAILED in {dt:6.1f}s")
            say("              " + (p.stderr or p.stdout or "").strip()[-300:])
            totals["failed"] += 1
            continue
        c = rep.get("counts", {})
        basis = ",".join(sorted(rep.get("by_basis") or {})) or "-"
        say(f"  1 SWEEP  [{sig}]  {dt:6.1f}s  observed={rep.get('observed',0)} "
            f"new={c.get('new',0)} modified={c.get('modified',0)} "
            f"unchanged={c.get('unchanged',0)} unknown={c.get('unknown',0)}")
        say(f"              basis: {basis}")
        totals["swept"] += 1
        totals["modified"] += c.get("modified", 0)

        # ---- 2. targets --------------------------------------------------- #
        tgts = [l for l in (targets_file.read_text(encoding="utf-8").splitlines()
                            if targets_file.exists() else []) if l.strip()]
        say(f"  2 TARGETS   {len(tgts)} url(s) -> {targets_file.name}")
        if args.baseline:
            say("  3 CRAWL     skipped (--baseline: state recorded, nothing pulled)")
            continue
        if not tgts:
            say("  3 CRAWL     skipped — nothing changed (this is the win)")
            continue

        # ---- 3. targeted crawl through the orchestrator -------------------- #
        entry = CRAWLERS.get(regulator)
        if not entry:
            say("  3 CRAWL     NO CRAWLER MAPPED — sweep only for this source")
            continue
        name, is_form, label = entry
        cmd = [sys.executable, "-u", "-B", "benchmarks/run_source_standalone.py",
               name, "--only-urls", str(targets_file), "--label", label]
        if is_form:
            cmd.append("--form")
        try:
            p, dt = run(cmd, timeout=args.crawl_timeout)
        except subprocess.TimeoutExpired:
            say(f"  3 CRAWL     TIMED OUT after {args.crawl_timeout}s")
            totals["failed"] += 1
            continue
        ok = p.returncode == 0
        say(f"  3 CRAWL     {dt:6.1f}s  {'ok' if ok else 'FAILED rc=%s' % p.returncode}")
        if not ok:
            say("              " + (p.stderr or "").strip()[-300:])
            totals["failed"] += 1
            continue
        totals["crawled"] += 1
        wb = None
        for line in p.stdout.splitlines():
            if "workbook:" in line:
                wb = line.split("workbook:")[-1].strip()
        say(f"              workbook: {wb}")

        # ---- 4. promote ---------------------------------------------------- #
        if args.no_promote or not wb:
            say("  4 PROMOTE   skipped")
            continue
        p, dt = run([sys.executable, "-B", "-m",
                     "dynamic_crawler.formfill.promote", wb], timeout=7200)
        try:
            rep = json.loads(p.stdout[p.stdout.index("{"):])
            say(f"  4 PROMOTE   {dt:6.1f}s  inserted={rep.get('inserted')} "
                f"skipped={rep.get('skipped_already_present')} "
                f"versions={rep.get('regulation_versions')} "
                f"failed={rep.get('failed')}")
            totals["promoted"] += 1
        except Exception:
            say(f"  4 PROMOTE   FAILED in {dt:6.1f}s")
            say("              " + (p.stderr or p.stdout or "").strip()[-300:])
            totals["failed"] += 1

    say("\n" + "=" * 78)
    say(f"TOTAL {time.time() - t_all:.1f}s   swept={totals['swept']} "
        f"modified={totals['modified']} crawled={totals['crawled']} "
        f"promoted={totals['promoted']} failed={totals['failed']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
