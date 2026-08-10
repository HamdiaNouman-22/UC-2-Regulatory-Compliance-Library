"""
baseline.py — a measuring instrument, not part of the crawler.

Runs generic_crawler/crawler.py against a fixed list of regulator sites with fixed
settings, and records three numbers per site: pages found, documents found, and
which walker the code chose. That table is the reference point for the merge --
see MERGE_LOG.md.

It touches nothing. It only runs the crawler as a subprocess and reads the JSON
progress lines the crawler already prints.

WHY IT EXISTS
-------------
Three of us are merging changes into one crawler. A change made for one regulator
can silently reduce coverage on another. The only way to catch that is to know what
every site produced BEFORE the change, and re-check after each one.

RULES
-----
* SETTINGS MUST NOT CHANGE between runs. Comparing a run at --max-pages 150 with
  one at --max-pages 50 is meaningless. The defaults below are the agreed values.
* A site whose pages == max_pages was CUT SHORT. Its number is a limit, not a
  measurement -- reported as cap_hit.

USAGE
-----
  venv/Scripts/python.exe generic_crawler/baseline.py
  venv/Scripts/python.exe generic_crawler/baseline.py --tag after-1.2
  venv/Scripts/python.exe generic_crawler/baseline.py --only "MISA laws,SDAIA regs"

Results append to output/_baseline/results.json, one entry per run, so runs can be
compared over time. The table is also printed to the console.
"""
import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
CRAWLER = REPO / "generic_crawler" / "crawler.py"
OUTROOT = REPO / "output" / "_baseline"

# ---- the agreed settings. Changing these invalidates every past run. ----
MAX_PAGES = 150
MAX_DEPTH = 8
SITE_TIMEOUT = 3000          # seconds per site; SBP now crawls 150 real pages

# ---- the six calibration sites, with the scope known to be correct for each ----
# (scope column taken from teammate A's calibrate_scope.py findings, so the
#  baseline reflects the best the current code can do rather than a bad guess)
SITES = [
    ("SECP acts",     "https://www.secp.gov.pk/laws/acts/",                     "prefix"),
    ("SBP circulars", "https://www.sbp.org.pk/circulars",                       "prefix"),
    ("SAMA sandbox",  "https://rulebook.sama.gov.sa/en/regulatory-sandbox",     "breadcrumb"),
    ("SAMA CB law",   "https://rulebook.sama.gov.sa/en/saudi-central-bank-law", "breadcrumb"),
    ("MISA laws",     "https://misa.gov.sa/activities/laws/",                   "prefix"),
    ("SDAIA regs",    "https://sdaia.gov.sa/en/SDAIA/about/Pages/"
                      "RegulationsAndPolicies.aspx",                            "breadcrumb"),
]


def slug(name: str) -> str:
    return name.lower().replace(" ", "_")


def run_site(name, url, scope):
    """Run one crawl, return the numbers. Never raises -- a failure is a result."""
    out_dir = OUTROOT / slug(name)
    cmd = [sys.executable, str(CRAWLER),
           "--url", url,
           "--out", str(out_dir),
           "--scope", scope,
           "--max-pages", str(MAX_PAGES),
           "--max-depth", str(MAX_DEPTH)]

    rec = {"site": name, "url": url, "scope_requested": scope,
           "pages": None, "documents": None, "shape": None,
           "scope_detected": None, "cap_hit": None,
           "errors": 0, "retries": 0, "status": "ok", "note": "", "seconds": 0}

    t0 = time.time()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              encoding="utf-8", errors="replace",
                              timeout=SITE_TIMEOUT)
        stdout, stderr = proc.stdout or "", proc.stderr or ""
    except subprocess.TimeoutExpired as e:
        stdout = (e.stdout or "") if isinstance(e.stdout, str) else ""
        stderr = ""
        rec["status"] = "timeout"
        rec["note"] = f"exceeded {SITE_TIMEOUT}s"
    rec["seconds"] = round(time.time() - t0, 1)

    # The crawler prints one JSON object per line. Read the ones we care about.
    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            ev = json.loads(line)
        except Exception:
            continue
        kind = ev.get("event")
        if kind == "done":
            rec["pages"] = ev.get("pages")
            rec["documents"] = ev.get("documents")
            # The engine now classifies its own run. Recorded alongside the flag
            # this file derives, not instead of it: `flag` is cross-site context
            # (NO-DOCS, CAP), `engine_status` is what the crawl itself saw
            # (blocked, incomplete). They answer different questions.
            rec["engine_status"] = ev.get("status", "")
            rec["blocked_pages"] = ev.get("blocked_pages", 0)
            rec["stopped"] = ev.get("stopped", "")
            if ev.get("shape"):
                rec["shape"] = ev["shape"]
        elif kind == "shape":
            rec["shape"] = ev.get("detected") or rec["shape"]
        elif kind == "scope":                      # only present after the merge
            rec["scope_detected"] = ev.get("scope")
        elif kind == "error":
            rec["errors"] += 1
        elif kind == "retry":
            rec["retries"] += 1

    if rec["pages"] is None and rec["status"] == "ok":
        rec["status"] = "no-result"
        tail = (stderr or stdout).strip().splitlines()
        rec["note"] = tail[-1][:160] if tail else "crawler produced no 'done' event"
    if rec["pages"] is not None:
        rec["cap_hit"] = rec["pages"] >= MAX_PAGES
        if rec["shape"] is None:
            rec["shape"] = "generic"

    return rec


def print_table(rows):
    hdr = (f"{'site':<15}{'shape':<9}{'scope':<12}{'pages':>6}{'docs':>6}"
           f"{'cap?':>6}{'err':>5}{'secs':>7}  status")
    print("\n" + hdr)
    print("-" * len(hdr))
    for r in rows:
        pages = "-" if r["pages"] is None else r["pages"]
        docs = "-" if r["documents"] is None else r["documents"]
        cap = "" if r["cap_hit"] is None else ("CAP" if r["cap_hit"] else "no")
        scope = r["scope_detected"] or r["scope_requested"]
        print(f"{r['site']:<15}{(r['shape'] or '-'):<9}{scope:<12}{pages:>6}{docs:>6}"
              f"{cap:>6}{r['errors']:>5}{r['seconds']:>7}  {r['status']}"
              + (f"  ({r['note']})" if r["note"] else ""))
    caps = [r["site"] for r in rows if r.get("cap_hit")]
    if caps:
        print(f"\nNOTE: hit the {MAX_PAGES}-page cap: {', '.join(caps)}")
        print("      Their counts are the CAP, not real coverage.")


def print_markdown(rows):
    """Table ready to paste into MERGE_LOG.md."""
    print("\n--- markdown (paste into MERGE_LOG.md) ---\n")
    print("| site | shape | scope | pages | documents | cap hit? | errors |")
    print("|---|---|---|---|---|---|---|")
    for r in rows:
        pages = "-" if r["pages"] is None else r["pages"]
        docs = "-" if r["documents"] is None else r["documents"]
        cap = "-" if r["cap_hit"] is None else ("**yes**" if r["cap_hit"] else "no")
        scope = r["scope_detected"] or r["scope_requested"]
        status = "" if r["status"] == "ok" else f" ({r['status']})"
        print(f"| {r['site']}{status} | {r['shape'] or '-'} | {scope} | "
              f"{pages} | {docs} | {cap} | {r['errors']} |")


def main():
    ap = argparse.ArgumentParser(description="Baseline measurement for the crawler merge")
    ap.add_argument("--tag", default="baseline",
                    help="label for this run, e.g. 'after-1.2'")
    ap.add_argument("--only", default="",
                    help="comma-separated site names to run (default: all)")
    args = ap.parse_args()

    wanted = [s.strip() for s in args.only.split(",") if s.strip()]
    sites = [s for s in SITES if not wanted or s[0] in wanted]
    if not sites:
        print(f"No sites matched --only '{args.only}'. Known: "
              f"{', '.join(s[0] for s in SITES)}")
        return 1

    OUTROOT.mkdir(parents=True, exist_ok=True)
    print(f"tag={args.tag}  sites={len(sites)}  "
          f"max_pages={MAX_PAGES} max_depth={MAX_DEPTH}")

    rows = []
    for i, (name, url, scope) in enumerate(sites, 1):
        print(f"[{i}/{len(sites)}] {name} ...", flush=True)
        r = run_site(name, url, scope)
        rows.append(r)
        print(f"        pages={r['pages']} documents={r['documents']} "
              f"shape={r['shape']} ({r['seconds']}s) {r['status']}", flush=True)

    print_table(rows)
    print_markdown(rows)

    # Append this run so past runs stay comparable.
    store = OUTROOT / "results.json"
    history = []
    if store.exists():
        try:
            history = json.loads(store.read_text(encoding="utf-8"))
        except Exception:
            history = []
    history.append({
        "tag": args.tag,
        "when": datetime.now().isoformat(timespec="seconds"),
        "max_pages": MAX_PAGES, "max_depth": MAX_DEPTH,
        "results": rows,
    })
    store.write_text(json.dumps(history, indent=2, ensure_ascii=False),
                     encoding="utf-8")
    print(f"\nSaved to {store}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
