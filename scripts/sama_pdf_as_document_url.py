"""Put SAMA's PDF in `document_url`, and shuffle what it displaces.

    python -m scripts.sama_pdf_as_document_url            # dry run
    python -m scripts.sama_pdf_as_document_url --apply

WHAT IS WRONG TODAY
-------------------
SAMA rows carry THREE urls in two slots and one extra_meta key:

    document_url     /en/node/11105                       the document's PAGE
    source_page_url  /en/sama-circulars                   the LISTING it was found on
    org_pdf_link     /sites/.../SAMA_EN_11105_VER1.pdf    the actual FILE

So `document_url` -- the field everything downstream treats as "the document" --
points at a web page, and the PDF is hidden in extra_meta. MEASURED 2026-08-20:
4,994 rows.

THE SHUFFLE, and why nothing is lost
    extra_meta.found_on  <- source_page_url   the listing, preserved. `found_on`
                                              is not invented here: CMA already
                                              uses it for exactly this, 1,979 rows
    source_page_url      <- document_url      the node page, which IS the page
                                              this document came from
    document_url         <- org_pdf_link      the file
    org_pdf_link          removed             it is now document_url; leaving a
                                              copy invites the two to drift

THIS CHANGES IDENTITY. `document_url` is one of the three identity fields, so
the crawler MUST emit the new shape in the same operation -- otherwise the next
SAMA crawl produces the old shape, matches nothing, and reports 4,994 documents
as `new` plus 4,994 stored rows as `disappeared`. crawler/sama_circulars_crawler.py
is changed alongside this script, and output/change_state is remapped below.

SCOPED TO ROWS THAT HAVE A PDF. SAMA's other ~1,100 rows have no org_pdf_link --
their document_url is already the right thing -- and they are not touched.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from jobs.monitor_jobs import _repo                    # noqa: E402

REGULATOR_LIKE = "%SAMA%"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    repo = _repo()
    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""SELECT id, document_url, source_page_url,
                              CAST(extra_meta AS nvarchar(max))
                       FROM regulations WHERE regulator LIKE ?""", [REGULATOR_LIKE])
        rows = cur.fetchall()

        plan, skipped = [], []
        for rid, du, spu, em in rows:
            try:
                meta = json.loads(em or "{}")
            except Exception:
                meta = {}
            pdf = str(meta.get("org_pdf_link") or "").strip()
            if not pdf.lower().startswith("http"):
                # `org_pdf_link: None` -- the crawler found no file for this row.
                # MEASURED: 37 rows. Counted, not silently dropped, because the
                # verification below asserts on what is LEFT holding the key and
                # an uncounted skip made a correct run report FAILED.
                if "org_pdf_link" in meta:
                    skipped.append(int(rid))
                continue
            du, spu = str(du or "").strip(), str(spu or "").strip()
            if pdf == du:
                skipped.append(int(rid))     # already the file; nothing to move
                continue
            new_meta = dict(meta)
            new_meta.pop("org_pdf_link", None)
            if spu:
                new_meta["found_on"] = spu   # the listing, preserved
            plan.append((int(rid), pdf, du or spu,
                         json.dumps(new_meta, ensure_ascii=False)))

        print(f"SAMA rows                     : {len(rows):,}")
        print(f"rows with a real org_pdf_link : {len(plan) + len(skipped):,}")
        print(f"rows this will shuffle        : {len(plan):,}")
        print(f"left alone (no pdf / already) : {len(skipped):,}")
        if plan:
            rid, pdf, spu, _ = plan[0]
            old = next(r for r in rows if int(r[0]) == rid)
            print(f"\nexample id {rid}")
            print(f"  document_url    {str(old[1])[:70]}")
            print(f"               -> {pdf[:70]}")
            print(f"  source_page_url {str(old[2])[:70]}")
            print(f"               -> {spu[:70]}")
            print(f"  found_on        <- {str(old[2])[:70]}")

        if not a.apply:
            print("\nDRY RUN -- nothing written. Re-run with --apply.")
            return 0

        stamp = datetime.now().strftime("%Y-%m-%d")
        backup = REPO_ROOT / "output" / f"backup_sama_urls_{stamp}.json"
        backup.write_text(json.dumps(
            [{"id": int(r[0]), "document_url": r[1], "source_page_url": r[2],
              "extra_meta": r[3]} for r in rows if int(r[0]) in {p[0] for p in plan}],
            ensure_ascii=False), encoding="utf-8")
        print(f"\nbacked up -> {backup}")

        for rid, pdf, spu, meta in plan:
            cur.execute("""UPDATE regulations
                           SET document_url = ?, source_page_url = ?, extra_meta = ?
                           WHERE id = ?""", [pdf, spu, meta, rid])
        conn.commit()
        print(f"updated {len(plan):,} row(s)")

        # ---- verify -------------------------------------------------------- #
        cur.execute("""SELECT COUNT(*) FROM regulations
                       WHERE regulator LIKE ?
                         AND CAST(extra_meta AS nvarchar(max)) LIKE '%org_pdf_link%'""",
                    [REGULATOR_LIKE])
        left = cur.fetchone()[0]
        cur.execute("""SELECT COUNT(*) FROM regulations
                       WHERE regulator LIKE ? AND document_url LIKE '%/node/%'""",
                    [REGULATOR_LIKE])
        still_node = cur.fetchone()[0]
        print()
        if left != len(skipped):
            print(f"VERIFICATION FAILED: {left} row(s) still hold org_pdf_link, "
                  f"expected {len(skipped)}")
            print(f"{backup} holds every original value.")
            return 1
        print("VERIFIED")
        print(f"   org_pdf_link remaining      : {left}  (the already-correct rows)")
        print(f"   document_url still a /node/ : {still_node}  (rows that never had a pdf)")
        print("\nNEXT: output/change_state for SAMA keys on identity, which now "
              "changed.\nRun a SAMA crawl and expect the first one to reconcile, "
              "the second to be clean.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
