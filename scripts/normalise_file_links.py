"""One rule for where a document's files live, applied to every regulator.

    python -m scripts.normalise_file_links            # dry run
    python -m scripts.normalise_file_links --apply

THE RULE (lead, 2026-08-24)

    exactly one file   ->  document_url,     attachment_links absent
    more than one      ->  attachment_links, document_url empty

It counts FILES, and that is the change. crawler/cma_crawler_wrapper.py and
crawler/mc_crawler_wrapper.py implement a different, also-deliberate convention
-- "the page is the document, and the PDFs hanging off it are attachments" --
which is why 141 rows carry BOTH a document_url and an attachment_links. Under
this rule a page is not a file, so those rows hold ONE file and it belongs in
document_url.

MEASURED 2026-08-24 over 9,359 rows:

    9,119  one file in document_url                          already correct
       76  attachment_links DUPLICATES document_url          drop the copy
       65  document_url is a PAGE, the attachment is the FILE
       99  one link in attachment_links, no document_url
        0  genuinely more than one file

WHAT MOVES

  A. 76 duplicates   drop extra_meta.attachment_links.
                     document_url unchanged -> IDENTITY UNCHANGED.

  B. 65 page+file    document_url    <- the file
                     source_page_url <- the old document_url (the page)
                     the old source_page_url is kept as extra_meta.found_on
                     IDENTITY CHANGES. Same shuffle SAMA had on 2026-08-24.

  C. 99 link-only    document_url <- the one link
                     drop attachment_links AND the identity_fields override.
                     cma_crawler_wrapper sets
                         ["doc_path", "extra_meta.attachment_links", "title"]
                     precisely BECAUSE document_url was empty; once it is
                     populated the default identity applies, and leaving the
                     override would key the row on a field it no longer carries.
                     IDENTITY CHANGES.

B and C change identity, so the crawlers change with them. A database in the new
shape and a crawler still emitting the old one makes every such document read as
`new` and every stored row as `disappeared` -- the failure the whole change-
signal design exists to avoid.
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


def links_of(v) -> list:
    """The http links in an attachment_links value.

    mc_crawler_wrapper joins with " | ", others with newlines, and a few rows use
    commas. Split on all three rather than trusting one -- a missed separator
    reads as a single very long url and the row looks single-file when it is not.
    """
    s = str(v or "")
    for sep in ("|", ","):
        s = s.replace(sep, "\n")
    return [x.strip() for x in s.split("\n") if x.strip().lower().startswith("http")]


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    repo = _repo()
    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, regulator, document_url, source_page_url, "
                    "CAST(extra_meta AS nvarchar(max)) FROM regulations")
        rows = cur.fetchall()

        A, B, C, many = [], [], [], []
        for rid, reg, du, spu, em in rows:
            try:
                meta = json.loads(em or "{}")
            except Exception:
                continue
            att = links_of(meta.get("attachment_links"))
            if not att:
                continue
            du = str(du or "").strip()
            spu = str(spu or "").strip()
            has_du = du.lower().startswith("http")

            if len(att) > 1 and not has_du:
                many.append(int(rid))                     # already correct
                continue

            new_meta = dict(meta)
            new_meta.pop("attachment_links", None)

            if has_du and len(att) == 1 and att[0] == du:
                A.append((int(rid), du, spu,
                          json.dumps(new_meta, ensure_ascii=False)))
            elif has_du:
                if spu:
                    new_meta.setdefault("found_on", spu)
                B.append((int(rid), att[0], du,
                          json.dumps(new_meta, ensure_ascii=False)))
            else:
                new_meta.pop("identity_fields", None)
                C.append((int(rid), att[0], spu,
                          json.dumps(new_meta, ensure_ascii=False)))

        print(f"rows                                   : {len(rows):,}")
        print(f"A  duplicate attachment, drop the copy : {len(A):>5}   identity unchanged")
        print(f"B  page in document_url, file below    : {len(B):>5}   identity CHANGES")
        print(f"C  one link, no document_url           : {len(C):>5}   identity CHANGES")
        print(f"   already >1 file, left alone         : {len(many):>5}")

        for label, group in (("A", A), ("B", B), ("C", C)):
            if not group:
                continue
            rid, du, spu, _ = group[0]
            old = next(r for r in rows if int(r[0]) == rid)
            print(f"\n  {label} example id {rid} [{str(old[1])[:26]}]")
            print(f"     document_url    {str(old[2])[:64]}")
            print(f"                  -> {du[:64]}")
            if label != "A":
                print(f"     source_page_url {str(old[3])[:64]}")
                print(f"                  -> {spu[:64]}")

        if not a.apply:
            print("\nDRY RUN -- nothing written. Re-run with --apply.")
            return 0

        stamp = datetime.now().strftime("%Y-%m-%d")
        ids = {r[0] for r in A + B + C}
        backup = REPO_ROOT / "output" / f"backup_file_links_{stamp}.json"
        backup.write_text(json.dumps(
            [{"id": int(r[0]), "document_url": r[2], "source_page_url": r[3],
              "extra_meta": r[4]} for r in rows if int(r[0]) in ids],
            ensure_ascii=False), encoding="utf-8")
        print(f"\nbacked up {len(ids)} row(s) -> {backup}")

        for rid, du, spu, meta in A + B + C:
            cur.execute("UPDATE regulations SET document_url = ?, "
                        "source_page_url = ?, extra_meta = ? WHERE id = ?",
                        [du, spu or None, meta, rid])
        conn.commit()
        print(f"updated {len(A) + len(B) + len(C)} row(s)")

        cur.execute("SELECT id, document_url, CAST(extra_meta AS nvarchar(max)) "
                    "FROM regulations WHERE CAST(extra_meta AS nvarchar(max)) "
                    "LIKE '%attachment_links%'")
        bad = []
        for rid, du, em in cur.fetchall():
            try:
                att = links_of(json.loads(em or "{}").get("attachment_links"))
            except Exception:
                continue
            has_du = str(du or "").strip().lower().startswith("http")
            if att and (has_du or len(att) < 2):
                bad.append(int(rid))
        print()
        if bad:
            print(f"VERIFICATION FAILED: {len(bad)} row(s) still break the rule, "
                  f"e.g. {bad[:5]}")
            print(f"{backup} holds every original value.")
            return 1
        print("VERIFIED  every row holds exactly one file in document_url, or "
              "several in attachment_links, and never both")
        return 0


if __name__ == "__main__":
    sys.exit(main())
