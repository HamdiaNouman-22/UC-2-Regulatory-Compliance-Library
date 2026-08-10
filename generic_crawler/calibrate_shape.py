"""
calibrate_shape.py — the gate on detect_shape().

Companion to calibrate_scope.py. That file gates SCOPE ("how far may the crawl
wander"); this one gates SHAPE ("how do I read this layout"). The two questions
are independent — SECP is table+prefix, SAMA is tree+breadcrumb — so a change to
one does not move the other, and they need separate gates.

  venv/Scripts/python.exe generic_crawler/calibrate_shape.py
  venv/Scripts/python.exe generic_crawler/calibrate_shape.py --only "MISA laws"

EXIT CODE 0 only when every calibration site is classified exactly as before.

WHY THIS EXISTS
---------------
detect_shape() picks which walker runs, and the walker decides everything
downstream. Pick wrong and the failure is SILENT: crawl_tree() on a page with no
book menu finds no nodes, returns 0 pages and 0 documents, writes its files and
reports success. That is how SBP produced nothing for weeks (MERGE_LOG.md, B1)
and how MHRSD produced nothing on first contact.

Before this file the only check on a detect_shape() change was baseline.py: six
full crawls, ~50 minutes, and it can no longer measure SBP at all (it times out
attempting the ~9-hour phase-2 detail walk). Too slow to run per edit — and a
moved document count cannot separate "I broke the classifier" from "the site
changed under me", which on SDAIA moves by ±70 documents between identical runs.

Shape is the cheap, exact thing to measure: ONE page load per site, about two
minutes for all of them, and it reads out the code being changed directly.

CHANGE detect_shape() -> RUN THIS -> ALL SITES UNCHANGED -> ONLY THEN MERGE.

THREE THINGS THIS DOES ON PURPOSE
---------------------------------
1. It imports the real detect_shape(). Re-implementing the rules here would test
   the copy instead of the code.

2. It passes a live browser CONTEXT. detect_shape(page, ctx) uses ctx for its
   last-resort child probe (rule 4, strategies.py). With ctx=None that rule never
   runs — so a gate without it would bless a half-fix. MHRSD is misclassified by
   rule 2 AND independently by rule 4, and only a live ctx catches the second.

3. An empty page is a FAILED measurement, never a result. detect_shape() returns
   "generic" when it sees nothing, so one transient DNS blip looks like a
   confident answer. Same trap detect_scope() hit by returning "host" on an empty
   page (MERGE_LOG.md, Change 13). Three attempts, using the same >15-links test
   the crawl itself applies to the seed.

The seed is loaded the way crawl() loads it, and detect_shape() is called on the
UN-CLICKED page — no expand_tree() first. That ordering is deliberate in crawl()
("SHAPE BEFORE SCOPE"), because a click can tear the listing out of the DOM and
make a list site read as generic. A gate that loaded the page differently would
be measuring something the crawl never sees.

WHAT THIS CANNOT TELL YOU
-------------------------
That a classification is RIGHT — only that it is UNCHANGED. SBP was consistently
'tree' while returning zero documents. The `expect` column is what we currently
believe, and MHRSD is standing proof that a stable wrong answer can sit in that
column indefinitely.
"""
import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from playwright.sync_api import sync_playwright

import crawler as dyn
from strategies import detect_shape, JS_SHAPE, JS_LIST_ROWS

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# site, seed url, the shape we currently believe is correct, known_bug
#
# known_bug=True means "we know this one is wrong today". It is reported loudly
# but does NOT fail the run, so the gate stays usable as a regression detector
# while a known defect is open. When such a site starts matching, the run says so
# and asks for the flag to be removed — a permanent known_bug is just a silence
# with extra steps.
SITES = [
    ("SECP acts",     "https://www.secp.gov.pk/laws/acts/",
     "table",   False),
    ("SBP circulars", "https://www.sbp.org.pk/circulars",
     "list",    False),
    ("SAMA sandbox",  "https://rulebook.sama.gov.sa/en/regulatory-sandbox",
     "tree",    False),
    ("SAMA CB law",   "https://rulebook.sama.gov.sa/en/saudi-central-bank-law",
     "tree",    False),
    ("MISA laws",     "https://misa.gov.sa/activities/laws/",
     "generic", False),
    ("SDAIA regs",    "https://sdaia.gov.sa/en/SDAIA/about/Pages/"
                      "RegulationsAndPolicies.aspx",
     "generic", False),
    # MHRSD: a card listing with no book menu and no .node__content. Was
    # classified 'tree' until 2026-08-03 because hasBookMenu counted any
    # `li.menu-item` nav menu (40 here) as proof of a Drupal book, and
    # JS_IS_TREE_NODE repeated the same test on the child probe. Both were
    # tightened; a relapse in either now fails this gate.
    ("MHRSD regs",    "https://www.hrsd.gov.sa/en/ministry/about-ministry/"
                      "policies-strategies/regulation-and-procedures",
     "generic", False),
]

ATTEMPTS = 3      # SBP and the Saudi sites intermittently serve an empty page
WAIT_MS = 700     # crawler.py's default --wait-ms, so the load matches the crawl


def load_seed(page, seed):
    """Load the seed exactly as crawl() does before it calls detect_shape().

    Returns (ok, note). ok=False means the page never rendered enough to be
    worth classifying — which is a failed measurement, not 'no signal'.
    """
    note = ""
    for attempt in range(1, ATTEMPTS + 1):
        try:
            page.goto(seed, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(WAIT_MS + 2500)
            try:
                page.mouse.wheel(0, 2500)
                page.wait_for_timeout(600)
            except Exception:
                pass
            if page.evaluate("()=>document.querySelectorAll('a[href]').length") > 15:
                return True, note
            note = f"rendered with no links (x{attempt})"
        except Exception as e:
            note = str(e)[:70]
        if attempt < ATTEMPTS:
            print(f"      attempt {attempt}: {note}, retrying", flush=True)
            page.wait_for_timeout(2000)
    return False, note or "seed did not load"


def measure(page, ctx, url):
    """Classify one seed page, and report the raw signals behind the verdict."""
    seed = dyn.normalize_url(url)
    ok, note = load_seed(page, seed)
    if not ok:
        return {"error": note}

    # The real function, with a live ctx so the child probe (rule 4) runs.
    shape = detect_shape(page, ctx)

    # The same signals detect_shape() reads, for diagnosis when a row moves.
    # Read-only, so evaluating them again cannot disturb the verdict above.
    try:
        s = page.evaluate(JS_SHAPE) or {}
    except Exception:
        s = {}
    try:
        lst = page.evaluate(JS_LIST_ROWS) or {}
        rows = len(lst.get("rows") or [])
        dated = lst.get("dated") or 0
    except Exception:
        rows = dated = 0

    # hasBookMenu is expected to split into hasRealBookMenu / hasGenericMenu when
    # the tree-detection defect is fixed. Render whichever exists so this gate
    # keeps working across that change instead of needing an edit alongside it.
    if "hasRealBookMenu" in s or "hasGenericMenu" in s:
        book = f"{'Y' if s.get('hasRealBookMenu') else 'n'}/" \
               f"{'Y' if s.get('hasGenericMenu') else 'n'}"
    elif "hasBookMenu" in s:
        book = "Y" if s.get("hasBookMenu") else "n"
    else:
        book = "-"

    return {
        "shape": shape,
        "maxRows": s.get("maxRows", 0),
        "nodeBody": "Y" if s.get("hasNodeContent") else "n",
        "book": book,
        "outline": s.get("outlineLinks", 0),
        "kids": len(s.get("childUrls") or []),
        "rows": rows,
        "dated": dated,
    }


def main():
    ap = argparse.ArgumentParser(description="Calibrate / gate detect_shape()")
    ap.add_argument("--only", default="", help="comma-separated site names")
    args = ap.parse_args()
    wanted = [s.strip() for s in args.only.split(",") if s.strip()]
    sites = [s for s in SITES if not wanted or s[0] in wanted]
    if not sites:
        print(f"No sites matched --only '{args.only}'. Known: "
              f"{', '.join(s[0] for s in SITES)}")
        return 1

    rows = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True,
                                     args=["--disable-dev-shm-usage"])
        ctx = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
            locale="en-US")
        for name, url, expect, known in sites:
            print(f"... measuring {name}", flush=True)
            page = ctx.new_page()          # fresh page per site — no state bleed
            try:
                m = measure(page, ctx, url)
            except Exception as e:
                m = {"error": str(e)[:70]}
            finally:
                page.close()
            rows.append((name, expect, known, m))
        browser.close()

    hdr = (f"{'site':<15}{'expect':<9}{'got':<9}{'maxRows':>8}{'nodeBody':>9}"
           f"{'book':>6}{'outline':>8}{'kids':>6}{'rows':>6}{'dated':>6}")
    print("\n" + hdr)
    print("-" * len(hdr))

    regressions, errors, still_broken, now_fixed = [], [], [], []
    for name, expect, known, m in rows:
        if m.get("error"):
            print(f"{name:<15}{expect:<9}FAILED — {m['error']}")
            errors.append(name)
            continue
        got = m["shape"]
        ok = (got == expect)
        if ok and known:
            tag = "   <-- NOW CORRECT, remove known_bug"
            now_fixed.append(name)
        elif ok:
            tag = ""
        elif known:
            tag = "   <-- KNOWN BUG (not a regression)"
            still_broken.append(name)
        else:
            tag = "   <-- REGRESSION"
            regressions.append(name)
        print(f"{name:<15}{expect:<9}{got:<9}{m['maxRows']:>8}{m['nodeBody']:>9}"
              f"{m['book']:>6}{m['outline']:>8}{m['kids']:>6}"
              f"{m['rows']:>6}{m['dated']:>6}{tag}")

    print()
    print("book column: Y/n = hasBookMenu; Y/n over Y/n = real book menu / "
          "generic nav menu, once those signals are split.")
    print("rows/dated are the JS_LIST_ROWS signals behind the 'list' verdict.")
    print()

    if still_broken:
        print(f"KNOWN BUG — {', '.join(still_broken)} still misclassified "
              f"(expected, not a regression).")
    if now_fixed:
        print(f"FIXED — {', '.join(now_fixed)} now classify correctly. "
              f"Clear their known_bug flag in SITES so a relapse fails the gate.")

    if errors:
        print(f"\nFAIL — {len(errors)} site(s) could not be measured: "
              f"{', '.join(errors)}")
        print("A site that did not render is not a result. Re-run before "
              "concluding anything about a code change.")
        return 1
    if regressions:
        print(f"\nFAIL — {len(regressions)} site(s) changed shape: "
              f"{', '.join(regressions)}")
        print("Do NOT merge a detect_shape() change until every site is "
              "classified as before.")
        return 1

    print(f"PASS — all {len(rows)} sites classified as expected"
          f"{' (known bugs aside)' if still_broken else ''}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
