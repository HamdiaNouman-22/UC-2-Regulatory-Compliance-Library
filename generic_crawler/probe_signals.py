"""
probe_signals.py — look at every signal on a seed page, without crawling.

Loads each site's landing page ONCE and reports, side by side, everything the
merge depends on:

  * breadcrumb          - what we use for the folder path today (and its junk)
  * B's nav_path        - folder path from section containers / tab panels
  * A's heading_path    - folder path from h1..h6 nesting
  * chrome              - which links sit in the header/footer
  * link classification - direct document / external law portal / page
  * shape signals       - the raw numbers detect_shape() reads, to diagnose
                          misclassification (this is why SBP returns nothing)

It is READ-ONLY: it never crawls, never writes into the crawler, and imports the
real helpers so it measures exactly what the crawler would see.

  venv/Scripts/python.exe generic_crawler/probe_signals.py
  venv/Scripts/python.exe generic_crawler/probe_signals.py --only "SDAIA regs"

Writes output/_baseline/probe_signals.xlsx  (Summary + Links + ShapeSignals)
"""
import argparse
import sys
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from playwright.sync_api import sync_playwright

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
REPO = HERE.parent
OUTROOT = REPO / "output" / "_baseline"

import crawler as C                     # the real helpers, so we match the crawler
from strategies import JS_SHAPE, detect_shape

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

SITES = [
    ("SECP acts",     "https://www.secp.gov.pk/laws/acts/"),
    ("SBP circulars", "https://www.sbp.org.pk/circulars"),
    ("SAMA sandbox",  "https://rulebook.sama.gov.sa/en/regulatory-sandbox"),
    ("SAMA CB law",   "https://rulebook.sama.gov.sa/en/saudi-central-bank-law"),
    ("MISA laws",     "https://misa.gov.sa/activities/laws/"),
    ("SDAIA regs",    "https://sdaia.gov.sa/en/SDAIA/about/Pages/"
                      "RegulationsAndPolicies.aspx"),
]

# B's external law-portal list (from B's crawler.py)
EXTERNAL_LAW_PORTALS = {
    "boe.gov.sa", "laws.boe.gov.sa", "mc.gov.sa", "moj.gov.sa",
    "laws.moj.gov.sa", "pr.gov.sa", "zatca.gov.sa",
}


def is_external_portal(url):
    host = urlparse(url).netloc.lower()
    return any(host == d or host.endswith("." + d) for d in EXTERNAL_LAW_PORTALS)


# ---------------- B's JS_NAV_PATH (raw string so \s reaches the browser) -------
JS_NAV_PATH = r"""
() => {
    const results = [];
    const HEADING_SEL = 'h2,h3,h4,h5,h6';
    const cleanText = el => el ? (el.textContent || '').replace(/\s+/g, ' ').trim() : '';
    const SECTION_SELECTORS = ['.regulationContent', '[id$="Show"]',
                               '[class*="tab-panel" i]', '[class*="tabpanel" i]'];
    function closestSection(el) {
        for (const sel of SECTION_SELECTORS) {
            try { const f = el.closest(sel); if (f) return f; } catch (e) {}
        }
        return null;
    }
    function categoryLabel(a, sectionEl) {
        const panel = a.closest('.showLawItems, [class*="banner" i], [class*="panel" i]');
        if (panel) { const t = cleanText(panel.querySelector(HEADING_SEL)); if (t) return t; }
        const mob = a.closest('[class*="MobItems" i], [class*="mob-items" i]');
        if (mob) {
            let sib = mob.previousElementSibling;
            while (sib) {
                if (sib.tagName === 'LI') { const t = cleanText(sib); if (t) return t; }
                sib = sib.previousElementSibling;
            }
        }
        if (sectionEl) {
            const hs = Array.from(sectionEl.querySelectorAll(HEADING_SEL));
            const before = hs.filter(h => !!(h.compareDocumentPosition(a)
                                             & Node.DOCUMENT_POSITION_FOLLOWING));
            if (before.length) return cleanText(before[before.length - 1]);
        }
        return '';
    }
    for (const a of Array.from(document.querySelectorAll('a[href]'))) {
        const sectionEl = closestSection(a);
        const parts = [cleanText(sectionEl ? sectionEl.querySelector(HEADING_SEL) : null),
                       categoryLabel(a, sectionEl)].filter(Boolean);
        results.push({ href: a.href, text: cleanText(a), nav_path: parts.join(' > ') });
    }
    return results;
}
"""

# ---------------- A's heading-stack + chrome flag ------------------------------
JS_HEADING_PATH = r"""
() => {
  const root = document.body || document.documentElement;
  if (!root) return [];
  const rankOf = (el) => {
    const tag = el.tagName;
    if (/^H[1-6]$/.test(tag)) return parseInt(tag[1], 10);
    if (el.getAttribute('role') === 'heading') {
      const n = parseInt(el.getAttribute('aria-level') || '0', 10);
      if (n >= 1 && n <= 6) return n;
    }
    if (tag === 'SUMMARY' || tag === 'CAPTION' || tag === 'LEGEND') return 7;
    return 0;
  };
  const isChrome = e => !!e.closest('header, footer, [role="banner"], [role="contentinfo"]');
  const isWidget = e => isChrome(e) ||
    !!e.closest('nav, aside, [role="navigation"], [role="complementary"]');
  const out = [], stack = [];
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT);
  let node = walker.currentNode;
  while (node) {
    const r = rankOf(node);
    if (r && !isWidget(node)) {
      const ht = (node.innerText || node.textContent || '').replace(/\s+/g, ' ').trim();
      if (ht && ht.length < 300) {
        while (stack.length && stack[stack.length - 1].rank >= r) stack.pop();
        stack.push({ rank: r, text: ht });
      }
    }
    if (node.tagName === 'A' && node.hasAttribute('href')) {
      const chrome = isChrome(node);
      out.push({ href: node.href,
                 text: (node.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 200),
                 title_attr: (node.getAttribute('title') || '').trim().slice(0, 200),
                 chrome: chrome,
                 heading_path: chrome ? [] : stack.map(s => s.text) });
    }
    node = walker.nextNode();
  }
  return out;
}
"""


def load(page, url):
    page.goto(url, wait_until="domcontentloaded", timeout=90000)
    try:
        page.wait_for_load_state("networkidle", timeout=4000)
    except Exception:
        pass
    for _ in range(4):
        page.mouse.wheel(0, 6000)
        page.wait_for_timeout(350)
    page.evaluate("window.scrollTo(0, 0)")
    try:
        page.wait_for_function(
            "() => document.querySelectorAll('a[href]').length > 15 "
            "|| (document.body && document.body.innerText.trim().length > 500)",
            timeout=10000)
    except Exception:
        pass
    page.wait_for_timeout(1200)


def probe(ctx, name, url):
    page = ctx.new_page()
    row = {"site": name, "url": url}
    links = []
    try:
        load(page, url)
        crumb = page.evaluate(C.JS_BREADCRUMB) or []
        nav = page.evaluate(JS_NAV_PATH) or []
        head = page.evaluate(JS_HEADING_PATH) or []
        shape_sig = page.evaluate(JS_SHAPE) or {}
        shape = detect_shape(page, ctx)
    except Exception as e:
        page.close()
        row["error"] = str(e)[:160]
        return row, [], {}
    page.close()

    navmap = {r["href"]: r["nav_path"] for r in nav if r.get("nav_path")}

    seen = set()
    n_doc = n_ext = n_page = n_chrome = 0
    for r in head:
        h = r["href"]
        if not h.startswith("http") or h in seen:
            continue
        seen.add(h)
        hp = C.collapse_heading_path(r["heading_path"]) if hasattr(
            C, "collapse_heading_path") else r["heading_path"]
        kind = ("document" if C.is_document_link(h)
                else "external_portal" if is_external_portal(h)
                else "asset" if C.ext_of(h) in C.SKIP_EXTS
                else "page")
        if kind == "document":
            n_doc += 1
        elif kind == "external_portal":
            n_ext += 1
        elif kind == "page":
            n_page += 1
        if r["chrome"]:
            n_chrome += 1
        links.append({
            "site": name, "kind": kind, "chrome": r["chrome"],
            "text": r["text"][:120], "title_attr": r["title_attr"][:120],
            "B_nav_path": navmap.get(h, ""),
            "A_heading_path": " > ".join(hp[:5]),
            "host": urlparse(h).netloc.lower(),
            "url": h[:300],
        })

    crumb_clean = [c for c in crumb if c and c.strip()]
    junk = [c for c in crumb_clean if c.strip() in ("/", "|", "-", "»", "\\")]

    row.update({
        "shape_detected": shape,
        "breadcrumb": " > ".join(crumb_clean),
        "crumb_steps": len(crumb_clean),
        "crumb_junk": len(junk),
        "links_unique": len(seen),
        "documents": n_doc,
        "external_portal": n_ext,
        "pages": n_page,
        "chrome_links": n_chrome,
        "B_nav_path_hits": sum(1 for l in links if l["B_nav_path"]),
        "A_head_path_hits": sum(1 for l in links if l["A_heading_path"]),
    })
    return row, links, shape_sig


def main():
    ap = argparse.ArgumentParser(description="Probe every signal on each seed page")
    ap.add_argument("--only", default="", help="comma-separated site names")
    args = ap.parse_args()
    wanted = [s.strip() for s in args.only.split(",") if s.strip()]
    sites = [s for s in SITES if not wanted or s[0] in wanted]

    OUTROOT.mkdir(parents=True, exist_ok=True)
    summary, all_links, shapes = [], [], []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
            locale="en-US")
        for name, url in sites:
            print(f"... {name}", flush=True)
            # These sites are flaky (SBP intermittently serves an empty page).
            # The crawler retries; so must the probe, or a transient failure gets
            # written down as "this site has no links".
            for attempt in (1, 2, 3):
                row, links, sig = probe(ctx, name, url)
                if row.get("links_unique"):
                    break
                print(f"      attempt {attempt}: no links, retrying", flush=True)
            summary.append(row)
            all_links.extend(links)
            sig = dict(sig or {})
            sig.pop("childUrls", None)
            sig["site"] = name
            sig["shape_detected"] = row.get("shape_detected", "")
            shapes.append(sig)
        browser.close()

    sdf = pd.DataFrame(summary)
    ldf = pd.DataFrame(all_links)
    shdf = pd.DataFrame(shapes)
    if len(shdf):
        cols = ["site", "shape_detected"] + [c for c in shdf.columns
                                             if c not in ("site", "shape_detected")]
        shdf = shdf[cols]

    # MERGE into any existing report rather than replacing it. A partial run
    # (--only, or a re-run after a transient DNS/network failure) must repair
    # those sites' rows and leave every other site's results intact.
    out = OUTROOT / "probe_signals.xlsx"
    probed = {r["site"] for r in summary}
    if out.exists():
        try:
            old = pd.read_excel(out, sheet_name=None)
        except Exception:
            old = {}
        def merge(sheet, new_df):
            prev = old.get(sheet)
            if prev is None or "site" not in prev.columns or not len(prev):
                return new_df
            kept = prev[~prev["site"].isin(probed)]
            if not len(new_df):
                return kept
            return pd.concat([kept, new_df], ignore_index=True)
        sdf, ldf, shdf = (merge("Summary", sdf), merge("Links", ldf),
                          merge("ShapeSignals", shdf))
        order = {name: i for i, (name, _) in enumerate(SITES)}
        for df in (sdf, ldf, shdf):
            if len(df) and "site" in df.columns:
                df.sort_values("site", key=lambda c: c.map(
                    lambda v: order.get(v, 99)), kind="stable", inplace=True)

    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        sdf.to_excel(xw, sheet_name="Summary", index=False)
        ldf.to_excel(xw, sheet_name="Links", index=False)
        shdf.to_excel(xw, sheet_name="ShapeSignals", index=False)

    show = ["site", "shape_detected", "crumb_steps", "crumb_junk", "links_unique",
            "documents", "external_portal", "chrome_links",
            "B_nav_path_hits", "A_head_path_hits"]
    print("\n" + sdf[[c for c in show if c in sdf.columns]].to_string(index=False))
    print("\nbreadcrumbs:")
    for _, r in sdf.iterrows():
        print(f"  {r['site']:<15} {r.get('breadcrumb','')}")
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
