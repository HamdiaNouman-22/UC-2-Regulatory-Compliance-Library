"""Make every url inside `regulations.document_html` absolute, so the stored
HTML renders correctly wherever it is served from.

    python -m scripts.absolutise_document_html            # dry run
    python -m scripts.absolutise_document_html --apply

THE PROBLEM
-----------
The crawlers store the page's own markup, and regulator pages link their files
relatively:

    <a href="/sites/default/files/2026-06/tra00856_en.pdf">

Served from a frontend, `/sites/default/...` resolves against YOUR host, not the
regulator's, so every such link and image 404s. MEASURED 2026-08-20: 243 of the
7,447 rows carrying HTML have at least one relative url.

    152  Capital Market Authority (CMA)
     57  Ministry of Human Resource and Social Development (MHRSD)
     26  Zakat, Tax and Customs Authority (ZATCA)
      7  Saudi Arabian Monetary Authority (SAMA)
      1  Saudi Credit Bureau (SIMAH)

CBE is absent from that list because `fix_lazy_images` already does this at crawl
time for that host (generic_crawler/crawler.py). This is the same fix applied
backwards, to rows crawled before it existed.

WHAT IT LEAVES ALONE, deliberately
    absolute http(s)      already work from anywhere
    protocol-relative //  already work
    #anchor               in-page, and rewriting one would break it
    mailto: tel: data: javascript:
So a url that already works from anywhere is never touched.

WHICH BASE IT RESOLVES AGAINST, in order
    1. source_page_url   the page the markup came from — always the right answer
    2. document_url      same host, one level off, still correct for /root paths
    3. the regulator's dominant host, derived from ITS OWN other rows

(3) exists because 57 CMA rows carry neither url. The host is measured, not
hardcoded: every regulator's rows are counted and the most common scheme+host
wins (CMA: cma.gov.sa, 1,893 rows). A hardcoded table would be one more thing to
keep in step with reality.

WHAT IT DOES NOT FIX
    <script>, inline styles, and the two 2.4 MB SAMA rows are separate concerns
    for the frontend to sanitise and cap. This is only about urls that point at
    the wrong host.
"""

from __future__ import annotations

import argparse
import collections
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from jobs.monitor_jobs import _repo                    # noqa: E402

#: src/href/srcset whose value is NOT already resolvable from anywhere.
ATTR = re.compile(
    r"""(?P<attr>\b(?:src|href|srcset)\s*=\s*)(?P<q>["'])(?P<url>(?!\s*(?:https?:|//|\#|mailto:|tel:|javascript:|data:))[^"']+)(?P=q)""",
    re.I)


def dominant_hosts(cur) -> dict:
    """{regulator -> "scheme://host"} measured from the rows themselves."""
    cur.execute("SELECT regulator, document_url, source_page_url FROM regulations")
    seen = collections.defaultdict(collections.Counter)
    for reg, du, spu in cur.fetchall():
        for u in (du, spu):
            u = str(u or "")
            if u.lower().startswith("http"):
                p = urlparse(u)
                if p.netloc:
                    seen[reg][f"{p.scheme}://{p.netloc}"] += 1
    return {reg: c.most_common(1)[0][0] for reg, c in seen.items() if c}


def rewrite(html: str, base: str) -> tuple:
    """(new_html, n_changed). Every relative url resolved against `base`."""
    n = 0

    def sub(m):
        nonlocal n
        url = m.group("url").strip()
        if not url:
            return m.group(0)
        try:
            absolute = urljoin(base, url)
        except Exception:
            return m.group(0)
        if not absolute.lower().startswith("http"):
            return m.group(0)
        n += 1
        return f'{m.group("attr")}{m.group("q")}{absolute}{m.group("q")}'

    return ATTR.sub(sub, html), n


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    repo = _repo()
    with repo._get_conn() as conn:
        cur = conn.cursor()
        hosts = dominant_hosts(cur)
        cur.execute("SELECT id, regulator, source_page_url, document_url, "
                    "document_html FROM regulations "
                    "WHERE document_html IS NOT NULL AND LEN(document_html) > 0")
        rows = cur.fetchall()

        plan, skipped = [], []
        per_reg = collections.Counter()
        used = collections.Counter()
        for rid, reg, spu, du, html in rows:
            html = str(html)
            if not ATTR.search(html):
                continue
            spu, du = str(spu or "").strip(), str(du or "").strip()
            if spu.lower().startswith("http"):
                base, why = spu, "source_page_url"
            elif du.lower().startswith("http"):
                base, why = du, "document_url"
            elif hosts.get(reg):
                base, why = hosts[reg] + "/", "regulator host"
            else:
                skipped.append((rid, reg))
                continue
            new_html, n = rewrite(html, base)
            if n and new_html != html:
                plan.append((rid, new_html, n))
                per_reg[reg] += 1
                used[why] += 1

        print(f"rows carrying html          : {len(rows):,}")
        print(f"rows with relative urls     : {len(plan) + len(skipped)}")
        print(f"rows this will rewrite      : {len(plan)}")
        print(f"urls rewritten in total     : {sum(n for _, _, n in plan)}")
        if skipped:
            print(f"SKIPPED (no base at all)    : {len(skipped)}  {skipped[:3]}")
        print()
        for k, v in used.most_common():
            print(f"  base from {k:16} {v} row(s)")
        print()
        for reg, v in per_reg.most_common():
            print(f"  {v:>5}  {str(reg)[:52]}")

        if not a.apply:
            if plan:
                rid, new_html, n = plan[0]
                before = next(str(r[4]) for r in rows if r[0] == rid)
                b = ATTR.search(before)
                print(f"\nexample, id {rid} ({n} url(s)):")
                print(f"  before  {b.group(0)[:96] if b else ''}")
                m = re.search(r'\b(?:src|href)\s*=\s*["\'][^"\']+', new_html)
                print(f"  after   {m.group(0)[:96] if m else ''}")
            print("\nDRY RUN — nothing written. Re-run with --apply.")
            return 0

        stamp = datetime.now().strftime("%Y-%m-%d")
        backup = REPO_ROOT / "output" / f"backup_document_html_urls_{stamp}.json"
        backup.write_text(json.dumps(
            [{"id": rid, "document_html": next(str(r[4]) for r in rows if r[0] == rid)}
             for rid, _, _ in plan], ensure_ascii=False), encoding="utf-8")
        print(f"\nbacked up {len(plan)} original value(s) -> {backup}")

        for rid, new_html, _ in plan:
            cur.execute("UPDATE regulations SET document_html = ? WHERE id = ?",
                        [new_html, rid])
        conn.commit()
        print(f"updated {len(plan)} row(s)")

        # ---- verify -------------------------------------------------------- #
        cur.execute("SELECT id, document_html FROM regulations "
                    "WHERE document_html IS NOT NULL AND LEN(document_html) > 0")
        left = [int(i) for i, h in cur.fetchall() if ATTR.search(str(h))]
        expected = {rid for rid, _ in skipped}
        unexpected = [i for i in left if i not in expected]
        print()
        if unexpected:
            print(f"VERIFICATION FAILED: {len(unexpected)} row(s) still hold a "
                  f"relative url, e.g. {unexpected[:5]}")
            print(f"{backup} holds every original value.")
            return 1
        print("VERIFIED")
        print(f"   rows still holding a relative url: {len(left)}"
              f"  (all {len(expected)} are the no-base rows, unchanged by design)")
        return 0


if __name__ == "__main__":
    sys.exit(main())
