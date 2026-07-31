"""
calibrate_scope.py — a measuring instrument, not part of the crawler.

Loads each site's seed page once, prints the raw numbers detect_scope() sees.
Read the table it prints, then set the five knobs in dynamic_crawler.py.
Delete this file (or keep it) once calibration is done — it touches nothing.

  venv/Scripts/python.exe dynamic_crawler/calibrate_scope.py
"""
import re
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright
import crawler as dyn

SITES = [
    ("SECP acts",     "https://www.secp.gov.pk/laws/acts/",                     "prefix"),
    ("SBP circulars", "https://www.sbp.org.pk/circulars",                       "prefix"),
    ("SAMA sandbox",  "https://rulebook.sama.gov.sa/en/regulatory-sandbox",     "breadcrumb"),
    ("SAMA CB law",   "https://rulebook.sama.gov.sa/en/saudi-central-bank-law", "breadcrumb"),
    ("MISA laws",     "https://misa.gov.sa/activities/laws/",                   "prefix"),
    ("SDAIA regs",    "https://sdaia.gov.sa/en/SDAIA/about/Pages/"
                      "RegulationsAndPolicies.aspx",                            "breadcrumb"),
]


def measure(page, url):
    """Load one seed page and count exactly what detect_scope() counts."""
    seed = dyn.normalize_url(url)
    host = urlparse(seed).netloc.lower()

    page.goto(seed, wait_until="domcontentloaded", timeout=60000)
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
    # Snapshot BEFORE expanding. expand_tree() CLICKS things, and on some sites
    # (SBP) a click navigates away or tears down the DOM — leaving nothing to
    # read. Mirrors probe_scope() in dynamic_crawler.py exactly.
    bc1, _c1, _s1, ln1 = dyn.extract_all(page)
    try:
        dyn.expand_tree(page)           # a collapsed menu hides links we want to count
        bc2, _c2, _s2, ln2 = dyn.extract_all(page)
    except Exception:
        bc2, ln2 = [], []
    breadcrumb = bc1 or bc2             # first non-empty trail wins
    _seen, links = set(), []            # union, so neither pass can lose a link
    for l in (ln1 + ln2):
        h = l.get("href")
        if h and h not in _seen:
            _seen.add(h)
            links.append(l)


    # Same counting loop as detect_scope(), using the SAME imported helpers.
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
    }


def main():
    hdr = (f"{'site':<15}{'expect':<12}{'got':<12}{'docs':>5}{'under':>6}"
           f"{'cands':>6}{'ratio':>7}{'share':>7}{'crumbs':>7}  path")
    rows = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
            locale="en-US")
        for name, url, expect in SITES:
            print(f"… measuring {name}", flush=True)
            page = ctx.new_page()            # fresh page per site — no state bleed
            try:
                m = measure(page, url)
            except Exception as e:
                rows.append((name, expect, f"FAILED: {str(e)[:60]}", None))
                m = None
            finally:
                page.close()
            if m is not None:
                rows.append((name, expect, m["scope"], m))
        browser.close()

    print("\n" + hdr)
    print("-" * len(hdr))
    for name, expect, got, m in rows:
        if m is None:
            print(f"{name:<15}{expect:<12}{got}")
            continue
        if m["cands"] == 0 and m["docs"] == 0:
            flag = "   <-- NO LINKS FOUND: extraction failed, not a threshold issue"
        elif expect == "?" or expect == got:
            flag = ""
        else:
            flag = "   <-- MISMATCH"
        print(f"{name:<15}{expect:<12}{got:<12}{m['docs']:>5}{m['under']:>6}"
              f"{m['cands']:>6}{m['ratio']:>6.0%}{m['share']:>7.0%}"
              f"{m['crumbs']:>7}  {m['path']}"
              f"{'  [SEED IS A FILE]' if m['is_file'] else ''}{flag}")


if __name__ == "__main__":
    main()
