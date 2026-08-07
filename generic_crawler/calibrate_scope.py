"""
calibrate_scope.py — a measuring instrument, and the gate on the five knobs.

Originally written by teammate A to calibrate detect_scope(). Kept and promoted:
it is no longer a one-off, it is the CHECK THAT MUST PASS before anyone changes
a scope threshold.

Loads each site's seed page once, prints the raw numbers detect_scope() sees, and
compares its answer against the scope we know is right for that site.

  venv/Scripts/python.exe generic_crawler/calibrate_scope.py
  venv/Scripts/python.exe generic_crawler/calibrate_scope.py --only "MISA laws"

EXIT CODE 0 only when every site matches. A wrong scope is silent in a crawl —
too narrow collects too little, too broad wanders a domain — so this has to fail
loudly rather than print a table nobody reads.

WHY THIS MATTERS
----------------
The five knobs in crawler.py (PREFIX_MIN_RATIO, PREFIX_MIN_COUNT, LISTING_DOC_MIN,
LISTING_DOC_SHARE, BREADCRUMB_MIN_LEN) are GLOBAL. Auto-scope traded a per-site
setting, which could only ever break one site, for shared thresholds that affect
every site at once. Nudging one to fix MISA can silently flip SDAIA. Some margins
are thin — LISTING_DOC_SHARE separates SAMA at 2% from SECP at 12%.

So: change a knob, run this, all sites still pass, THEN it is mergeable.

CHANGE A KNOB -> RUN THIS -> ALL SITES PASS -> ONLY THEN MERGE.
"""
import argparse
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from playwright.sync_api import sync_playwright

import crawler as dyn

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# site, seed url, the scope known to be correct
SITES = [
    ("SECP acts",     "https://www.secp.gov.pk/laws/acts/",                     "prefix"),
    ("SBP circulars", "https://www.sbp.org.pk/circulars",                       "prefix"),
    ("SAMA sandbox",  "https://rulebook.sama.gov.sa/en/regulatory-sandbox",     "breadcrumb"),
    ("SAMA CB law",   "https://rulebook.sama.gov.sa/en/saudi-central-bank-law", "breadcrumb"),
    ("MISA laws",     "https://misa.gov.sa/activities/laws/",                   "prefix"),
    ("SDAIA regs",    "https://sdaia.gov.sa/en/SDAIA/about/Pages/"
                      "RegulationsAndPolicies.aspx",                            "breadcrumb"),
]

ATTEMPTS = 3   # SBP and the Saudi sites intermittently serve an empty page


def measure(page, url):
    """Load one seed page and count exactly what detect_scope() counts."""
    seed = dyn.normalize_url(url)
    host = urlparse(seed).netloc.lower()
    prof = dyn.profile_for(seed)

    page.goto(seed, wait_until="domcontentloaded", timeout=90000)
    try:
        page.wait_for_load_state("networkidle", timeout=2500)
    except Exception:
        pass
    for _ in range(3):
        page.mouse.wheel(0, 6000)
        page.wait_for_timeout(300)
    page.evaluate("window.scrollTo(0, 0)")
    try:
        page.wait_for_function(
            "() => document.querySelectorAll('a[href]').length > 15 "
            "|| (document.body && document.body.innerText.trim().length > 500)",
            timeout=10000)
    except Exception:
        pass

    # Snapshot BEFORE expanding — expand_tree() clicks, and on some sites a click
    # navigates away or tears the DOM down. Mirrors probe_scope() exactly.
    bc1, _c1, _s1, ln1 = dyn.extract_all(page, prof)
    try:
        dyn.expand_tree(page)
        bc2, _c2, _s2, ln2 = dyn.extract_all(page, prof)
    except Exception:
        bc2, ln2 = [], []
    breadcrumb = bc1 or bc2
    links = list(dyn._merge_links({}, ln1 + ln2).values())

    # The same counting loop as detect_scope(), via the SAME helpers.
    seed_path = urlparse(seed).path.rstrip("/")
    cands = under = docs = 0
    for l in links:
        href = l.get("href") or ""
        p = urlparse(href)
        if p.scheme not in ("http", "https") or p.netloc.lower() != host:
            continue
        if dyn.is_document_link(href):
            docs += 1
            continue
        if dyn.ext_of(href) in dyn.SKIP_EXTS:
            continue
        path = re.sub(r"/{2,}", "/", p.path).rstrip("/")
        if path == seed_path:
            continue
        cands += 1
        if seed_path and path.startswith(seed_path + "/"):
            under += 1

    scope, reason = dyn.detect_scope(breadcrumb, links, seed, host)
    return {
        "docs": docs, "under": under, "cands": cands,
        "ratio": (under / cands) if cands else 0.0,
        "share": (docs / (docs + cands)) if (docs + cands) else 0.0,
        "crumbs": len([c for c in breadcrumb if c and c.strip()]),
        "path": seed_path or "/",
        "is_file": bool(re.search(r"\.(aspx?|html?|php|jsp)$", seed_path, re.I)),
        "scope": scope, "reason": reason,
        "links": len(links),
    }


def main():
    ap = argparse.ArgumentParser(description="Calibrate / gate the scope thresholds")
    ap.add_argument("--only", default="", help="comma-separated site names")
    args = ap.parse_args()
    wanted = [s.strip() for s in args.only.split(",") if s.strip()]
    sites = [s for s in SITES if not wanted or s[0] in wanted]

    rows = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
            locale="en-US")
        for name, url, expect in sites:
            print(f"... measuring {name}", flush=True)
            m = None
            for attempt in range(1, ATTEMPTS + 1):
                page = ctx.new_page()          # fresh page per try — no state bleed
                try:
                    m = measure(page, url)
                except Exception as e:
                    m = {"error": str(e)[:70]}
                finally:
                    page.close()
                # An empty page is a FAILED measurement, never a result.
                if not m.get("error") and m.get("links"):
                    break
                if attempt < ATTEMPTS:
                    print(f"      attempt {attempt}: no links, retrying", flush=True)
            rows.append((name, expect, m))
        browser.close()

    hdr = (f"{'site':<15}{'expect':<12}{'got':<12}{'docs':>5}{'under':>6}"
           f"{'cands':>6}{'ratio':>7}{'share':>7}{'crumbs':>7}  path")
    print("\n" + hdr)
    print("-" * len(hdr))
    fails = []
    for name, expect, m in rows:
        if not m or m.get("error"):
            print(f"{name:<15}{expect:<12}FAILED: {(m or {}).get('error','?')}")
            fails.append(name)
            continue
        if not m["links"]:
            print(f"{name:<15}{expect:<12}NO LINKS after {ATTEMPTS} tries "
                  f"— extraction failed, not a threshold issue")
            fails.append(name)
            continue
        ok = (m["scope"] == expect)
        if not ok:
            fails.append(name)
        print(f"{name:<15}{expect:<12}{m['scope']:<12}{m['docs']:>5}{m['under']:>6}"
              f"{m['cands']:>6}{m['ratio']:>6.0%}{m['share']:>7.0%}"
              f"{m['crumbs']:>7}  {m['path']}"
              f"{'  [SEED IS A FILE]' if m['is_file'] else ''}"
              f"{'' if ok else '   <-- MISMATCH'}")

    print()
    if fails:
        print(f"FAIL — {len(fails)} site(s) wrong: {', '.join(fails)}")
        print("Do NOT merge a threshold change until every site passes.")
        return 1
    print(f"PASS — all {len(rows)} sites resolve to the expected scope.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
