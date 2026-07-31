"""
debug_page.py — open ONE url in a visible browser and report exactly what the
crawler would see. Diagnostic only: reads nothing, writes nothing, changes nothing.

  venv/Scripts/python.exe dynamic_crawler/debug_page.py https://www.sbp.org.pk/circulars
"""
import re
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

import crawler as dyn

# First non-flag argument is the URL; --expand / --paginate enable those stages.
_args = [a for a in sys.argv[1:] if not a.startswith("--")]
URL = _args[0]
SHOT_DIR = Path(__file__).resolve().parent / "debug_screenshots"
SHOT_DIR.mkdir(parents=True, exist_ok=True)

def shot_path(url: str) -> Path:
    """One file per run, named after the URL + a timestamp, so screenshots from
    different sites (and different attempts) never overwrite each other."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", url.split("://")[-1]).strip("-")[:60]
    return SHOT_DIR / f"{slug}-{int(time.time())}.png"

def snapshot(page, label, rows):
    """Record what extract_all() sees right now, so we can compare stage to stage."""
    try:
        bc, content, _s, links = dyn.extract_all(page)
        rows.append((label, str(len(links)), str(len(content.get("text", ""))),
                     " > ".join(bc[:3])))
    except Exception as e:
        rows.append((label, f"THREW {type(e).__name__}", "-", "-"))

with sync_playwright() as pw:
    # slow_mo makes each action visible so you can watch it happen
    # Flags let us change ONE launch variable at a time and see which one matters.
    HEADLESS = "--headless" in sys.argv
    SLOW_MO = 0 if "--fast" in sys.argv else 250
    VIEWPORT = ({"width": 1280, "height": 720} if "--vp720" in sys.argv
                else {"width": 1400, "height": 900})
    print(f"mode: headless={HEADLESS} slow_mo={SLOW_MO} viewport={VIEWPORT}")

    browser = pw.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO)
    ctx = browser.new_context(
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
        locale="en-US", viewport=VIEWPORT)
    page = ctx.new_page()

    print(f"\n=== requesting {URL}")
    resp = page.goto(URL, wait_until="domcontentloaded", timeout=60000)

    # The crawler THROWS THIS AWAY. A 404 page loads perfectly well as far as
    # goto() is concerned — it just has no links on it.
    print(f"HTTP status : {resp.status if resp else '(no response object)'}")
    print(f"final URL   : {page.url}")        # differs from URL == you were redirected
    print(f"title       : {page.title()!r}")

    # ---- replay the crawler's exact render sequence ----
    for _ in range(3):
        page.mouse.wheel(0, 6000)
        page.wait_for_timeout(300)
    page.evaluate("window.scrollTo(0, 0)")
    try:
        page.wait_for_function(
            "() => document.querySelectorAll('a[href]').length > 15 "
            "|| (document.body && document.body.innerText.trim().length > 500)",
            timeout=10000)
        print("render gate : PASSED")
    except Exception:
        print("render gate : TIMED OUT after 10s   <-- the crawler gives up here")

    # ---- stage-by-stage: which step, if any, destroys the page? ----
    rows = []
    snapshot(page, "after load + scroll", rows)
    if "--expand" in sys.argv:
        try:
            dyn.expand_tree(page)
        except Exception as e:
            print(f"expand_tree threw: {str(e)[:120]}")
        snapshot(page, "after expand_tree", rows)
    if "--paginate" in sys.argv:
        try:
            dyn.collect_paginated_links(page)
        except Exception as e:
            print(f"collect_paginated_links threw: {str(e)[:120]}")
        snapshot(page, "after paginate", rows)

    print(f"\n{'stage':<24}{'links':>7}{'chars':>8}  breadcrumb")
    print("-" * 62)
    prev = None
    for label, n, chars, bc in rows:
        drop = ""
        if prev is not None and n.isdigit() and prev.isdigit() and int(n) < int(prev) * 0.5:
            drop = "   <-- THIS STAGE LOST MOST LINKS"
        print(f"{label:<24}{n:>7}{chars:>8}  {bc}{drop}")
        prev = n


    # ---- what extract_all() would actually walk ----
    print(f"\nframes      : {len(page.frames)}")
    for i, fr in enumerate(page.frames):
        try:
            n     = fr.evaluate("() => document.querySelectorAll('a[href]').length")
            body  = fr.evaluate("() => !!document.body")
            chars = fr.evaluate(
                "() => document.body ? document.body.innerText.trim().length : 0")
        except Exception as e:
            n, body, chars = f"ERR:{str(e)[:30]}", "?", "?"
        print(f"  [{i}] anchors={n!s:<8} body={body!s:<6} chars={chars!s:<8} "
              f"url={fr.url[:85]}")

     # ---- Where do the 302 anchors actually go? ----
    print("\n--- what extract_all() returns ---")
    try:
        bc, content, status, links = dyn.extract_all(page)
        print(f"links returned : {len(links)}")
        print(f"breadcrumb     : {bc}")
        print(f"text chars     : {len(content.get('text', ''))}")
    except Exception as e:
        print(f"extract_all THREW: {type(e).__name__}: {str(e)[:200]}")

    # Same JS, but WITHOUT the bare except that hides the error in extract_all().
    print("\n--- raw JS_LINKS (exception deliberately NOT swallowed) ---")
    raw = []
    try:
        raw = page.main_frame.evaluate(dyn.JS_LINKS)
        print(f"JS_LINKS returned {len(raw)} entries")
    except Exception as e:
        print(f"JS_LINKS THREW: {type(e).__name__}: {str(e)[:300]}")

    # Replay detect_scope()'s filters and show which one eats the links.
    from urllib.parse import urlparse as _up
    host = _up(page.url).netloc.lower()
    seed_path = _up(page.url).path.rstrip("/")
    buckets = {"same-host page (counted!)": 0, "same-host document": 0,
               "off-host": 0, "non-http (# / javascript: / mailto:)": 0,
               "equals seed path": 0, "asset (css/img/font)": 0}
    for l in raw:
        h = l.get("href") or ""
        p = _up(h)
        if p.scheme not in ("http", "https"):
            buckets["non-http (# / javascript: / mailto:)"] += 1
        elif p.netloc.lower() != host:
            buckets["off-host"] += 1
        elif dyn.is_document_link(h):
            buckets["same-host document"] += 1
        elif dyn.ext_of(h) in dyn.SKIP_EXTS:
            buckets["asset (css/img/font)"] += 1
        elif p.path.rstrip("/") == seed_path:
            buckets["equals seed path"] += 1
        else:
            buckets["same-host page (counted!)"] += 1
    print("\n--- where the anchors land ---")
    for k, v in buckets.items():
        print(f"  {k:<40} {v}")

    print("\n--- first 15 hrefs ---")
    for l in raw[:15]:
        print(f"  {(l.get('href') or '')[:110]}")

    shot = shot_path(URL)
    page.screenshot(path=str(shot))
    print(f"\nscreenshot  : {shot}")
    if not HEADLESS:
        print("\nBrowser is open. Look at it, then press Enter here to close.")
        input()
    browser.close()

