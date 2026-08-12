"""Crawl every reachable regulator, then demonstrate monitoring — separately.

TWO PHASES, TWO SETS OF WORKBOOKS, ON PURPOSE

  phase 1  CRAWL       -> output/formfill/_orch_runs/crawl/<regulator>.xlsx
           These are the rows that go to the database via POST /approve.
           Every document classifies as `new` because each starts empty.

  phase 2  MONITORING  -> output/formfill/_orch_runs/monitoring/<regulator>.xlsx
           A COPY of the phase-1 workbook, re-run against itself. The second
           run classifies `unchanged` / `modified` / `new`, which is the change
           detection working. For viewing only.

The copy is what keeps them apart. Re-running against the phase-1 workbook
directly would append a second run's rows to the file you are about to import,
so the thing you approve would no longer be the thing you read.

SIMAH and Tadawul are excluded: Cloudflare-blocked and headless-blocked
respectively. Neither is a code problem.

    python benchmarks/run_all_regulators.py                # both phases
    python benchmarks/run_all_regulators.py --phase crawl  # just the crawl
    python benchmarks/run_all_regulators.py --only moh,cma
"""

import argparse
import json
import shutil
import sys
import time
from pathlib import Path

import requests

REPO = Path(__file__).resolve().parent.parent
import os
API = os.getenv("FORMFILL_API", "http://127.0.0.1:8101")
RUNS = REPO / "output" / "formfill" / "_orch_runs"
CRAWL_DIR = RUNS / "crawl"
MON_DIR = RUNS / "monitoring"

# One route per regulator.
#
# SAMA IS DELIBERATELY ABSENT. The lead is rebuilding that crawler from scratch,
# so the existing SAMACombinedCrawler's output is not wanted. It also mattered
# operationally: SAMA is the largest source by an order of magnitude, the API
# serializes runs behind one lock, and phase 2 cannot begin until every phase-1
# target is done — so leaving it in meant a five-hour run standing between the
# crawl and the monitoring pass, and monitoring never happening overnight.
#
# To put it back: add ("SAMA", "source", "sama").
TARGETS = [
    ("AML",        "form",   "aml.rules"),
    ("MISA",       "form",   "misa.laws"),
    ("SDAIA",      "form",   "sdaia.regs"),
    ("MOE",        "form",   "moe.regulations"),
    ("MHRSD",      "form",   "mhrsd.regs"),
    ("GOSI-SI",    "form",   "gosi.social_insurance"),
    ("GOSI-Saned", "form",   "gosi.saned"),
    ("MC",         "source", "mc"),
    ("ZATCA",      "source", "zatca"),
    ("CMA",        "source", "cma"),
    ("MOH",        "source", "moh"),
]

# The API serializes runs behind a single threading.Lock, so a request spends
# most of its life queued behind the previous regulator. SAMA Combined walks the
# whole rulebook and the pipeline OCRs long PDFs page by page, so an hour is not
# a generous ceiling here -- it is one that would fire mid-run. If the client
# gives up, the server keeps going and the NEXT request queues behind a run
# nobody is reading, which is how a whole overnight sequence goes missing.
TIMEOUT = 6 * 3600


def trigger(kind: str, name: str, workbook: str, timeout: int = TIMEOUT) -> dict:
    if kind == "form":
        url = f"{API}/trigger/{name}"
        params = {"limit": 0, "analyse": "false", "reuse_last": "false",
                  "workbook": workbook}
    else:
        url = f"{API}/trigger/source/{name}"
        params = {"limit": 0, "analyse": "false", "workbook": workbook}
    r = requests.post(url, params=params, timeout=timeout)
    try:
        return r.json()
    except Exception:
        return {"detail": f"HTTP {r.status_code}: {r.text[:200]}"}


def summarise(label: str, d: dict) -> dict:
    if "detail" in d:
        print(f"  {label:<12} FAILED  {str(d['detail'])[:110]}")
        return {"label": label, "ok": False, "error": str(d["detail"])[:300]}
    c = d.get("classified") or {}
    row = {
        "label": label, "ok": True,
        "crawled": d.get("crawled"), "processed": d.get("processed"),
        "new": c.get("new"), "modified": c.get("modified"),
        "unchanged": c.get("unchanged"), "disappeared": c.get("disappeared"),
        "seconds": d.get("seconds"),
        "gate": d.get("baseline_verdict"),
        "tables": d.get("tables"),
        "excel": str(d.get("excel", "")),
    }
    print(f"  {label:<12} crawled={row['crawled']:<6} "
          f"new={row['new']:<5} mod={row['modified']:<4} unch={row['unchanged']:<5} "
          f"{row['seconds']:>7.1f}s  gate={row['gate']}")
    return row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["crawl", "monitor", "both"], default="both")
    ap.add_argument("--only", default="", help="comma-separated labels")
    a = ap.parse_args()

    targets = TARGETS
    if a.only:
        want = {s.strip().upper() for s in a.only.split(",")}
        targets = [t for t in TARGETS if t[0].upper() in want]
        if not targets:
            raise SystemExit(f"no target matched {a.only!r}. "
                             f"Known: {[t[0] for t in TARGETS]}")

    CRAWL_DIR.mkdir(parents=True, exist_ok=True)
    MON_DIR.mkdir(parents=True, exist_ok=True)

    try:
        requests.get(API + "/", timeout=10)
    except Exception:
        raise SystemExit(f"API not reachable at {API}. Start it with:\n"
                         f"  venv\\Scripts\\python.exe -m uvicorn "
                         f"dynamic_crawler.formfill.api:app --port 8101")

    report = {"crawl": [], "monitor": []}
    report_path = RUNS / "run_all_report.json"

    def save():
        """Written after every target. An overnight run that dies at 3am should
        still leave a record of the ten regulators that did finish."""
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False),
                               encoding="utf-8")

    if a.phase in ("crawl", "both"):
        print("=" * 78)
        print("PHASE 1 — CRAWL   (these workbooks are what gets approved into the DB)")
        print("=" * 78)
        for label, kind, name in targets:
            wb = f"crawl/{label}.xlsx"
            t0 = time.time()
            try:
                d = trigger(kind, name, wb)
            except Exception as e:
                d = {"detail": f"{type(e).__name__}: {e}"}
            row = summarise(label, d)
            row["minutes"] = round((time.time() - t0) / 60, 1)
            report["crawl"].append(row)
            save()

    if a.phase in ("monitor", "both"):
        print()
        print("=" * 78)
        print("PHASE 2 — MONITORING   (copies; for viewing only, NOT for the DB)")
        print("=" * 78)
        for label, kind, name in targets:
            src = CRAWL_DIR / f"{label}.xlsx"
            if not src.exists():
                print(f"  {label:<12} skipped — no crawl workbook to compare against")
                continue
            dst = MON_DIR / f"{label}.xlsx"
            shutil.copy2(src, dst)          # never touch the phase-1 workbook
            try:
                d = trigger(kind, name, f"monitoring/{label}.xlsx")
            except Exception as e:
                d = {"detail": f"{type(e).__name__}: {e}"}
            report["monitor"].append(summarise(label, d))
            save()

    save()
    print()
    print(f"report: {report_path}")
    print(f"crawl workbooks      -> {CRAWL_DIR}")
    print(f"monitoring workbooks -> {MON_DIR}")


if __name__ == "__main__":
    main()
