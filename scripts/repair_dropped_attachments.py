"""Restore the file urls scripts/normalise_file_links.py dropped on 2026-08-24.

    python -m scripts.repair_dropped_attachments            # dry run
    python -m scripts.repair_dropped_attachments --apply

THE BUG
-------
`normalise_file_links` sorted rows into three groups. Group B was "document_url
holds a PAGE, extra_meta holds the FILE", and it did:

    B.append((rid, att[0], du, ...))          # att[0] -- THE FIRST ONLY

That is right when there is one file. 48 of the 65 group-B rows had SEVERAL,
pipe-separated, and every url after the first was discarded: 84 urls across
Ministry of Commerce and CMA, one row losing 9 of its 10.

It was not caught because the run's own verification only asked whether each row
still broke the RULE -- one file in document_url, several in attachment_links,
never both. A row that lost four of its five files satisfies that perfectly. The
check tested the shape, not the content, and a shape check cannot see deletion.

THE REPAIR
----------
`utils/file_links.normalise` is the correct implementation of the same rule, so
this replays each backed-up row THROUGH IT rather than hand-rolling the fix a
third time. One file -> document_url; several -> attachment_links with
document_url empty and the page kept in source_page_url.

Reads output/backup_file_links_2026-08-24.json, which holds every original
value. Rows the migration handled correctly are re-derived to the same result
and updated harmlessly.
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

from jobs.monitor_jobs import _repo                        # noqa: E402
from utils.file_links import normalise, split_links        # noqa: E402

BACKUP = REPO_ROOT / "output" / "backup_file_links_2026-08-24.json"


class _Doc:
    """Just enough of a document for utils.file_links.normalise to work on."""

    def __init__(self, row):
        self.document_url = row.get("document_url") or ""
        self.source_page_url = row.get("source_page_url") or ""
        try:
            self.extra_meta = json.loads(row.get("extra_meta") or "{}")
        except Exception:
            self.extra_meta = {}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    if not BACKUP.exists():
        raise SystemExit(f"backup not found: {BACKUP}")
    backup = {int(r["id"]): r for r in json.loads(BACKUP.read_text(encoding="utf-8"))}

    repo = _repo()
    with repo._get_conn() as conn:
        cur = conn.cursor()
        ids = ",".join(str(i) for i in backup)
        cur.execute(f"SELECT id, regulator, document_url, source_page_url, "
                    f"CAST(extra_meta AS nvarchar(max)) FROM regulations "
                    f"WHERE id IN ({ids})")
        current = {int(r[0]): r for r in cur.fetchall()}

        plan, unchanged = [], 0
        restored_urls = 0
        for rid, row in backup.items():
            d = _Doc(row)
            before = len(split_links((d.extra_meta or {}).get("attachment_links"))) or (
                1 if str(d.document_url or "").lower().startswith("http") else 0)
            normalise(d)
            meta = json.dumps(d.extra_meta, ensure_ascii=False)
            now = current.get(rid)
            if now is None:
                continue
            same = (str(now[2] or "") == str(d.document_url or "")
                    and str(now[3] or "") == str(d.source_page_url or "")
                    and json.loads(now[4] or "{}") == d.extra_meta)
            if same:
                unchanged += 1
                continue
            after = len(split_links(d.extra_meta.get("attachment_links"))) or (
                1 if d.document_url else 0)
            now_files = len(split_links(json.loads(now[4] or "{}").get("attachment_links"))) or (
                1 if now[2] else 0)
            restored_urls += max(after - now_files, 0)
            plan.append((rid, d.document_url or None,
                         d.source_page_url or None, meta))

        print(f"rows in the backup            : {len(backup)}")
        print(f"already correct               : {unchanged}")
        print(f"rows to repair                : {len(plan)}")
        print(f"file urls restored            : {restored_urls}")

        if plan:
            rid, du, spu, meta = plan[0]
            att = json.loads(meta).get("attachment_links")
            now = current[rid]
            print(f"\nexample id {rid} [{str(now[1])[:26]}]")
            print(f"   db holds now  document_url={str(now[2])[:56]}")
            print(f"   repaired      document_url={str(du)[:56]}")
            print(f"                 attachment_links={str(att)[:80]}")

        if not a.apply:
            print("\nDRY RUN -- nothing written. Re-run with --apply.")
            return 0

        stamp = datetime.now().strftime("%Y-%m-%d")
        pre = REPO_ROOT / "output" / f"backup_before_attachment_repair_{stamp}.json"
        pre.write_text(json.dumps(
            [{"id": int(r[0]), "document_url": r[2], "source_page_url": r[3],
              "extra_meta": r[4]} for r in current.values()],
            ensure_ascii=False), encoding="utf-8")
        print(f"\nbacked up current state -> {pre}")

        for rid, du, spu, meta in plan:
            cur.execute("UPDATE regulations SET document_url = ?, "
                        "source_page_url = ?, extra_meta = ? WHERE id = ?",
                        [du, spu, meta, rid])
        conn.commit()
        print(f"repaired {len(plan)} row(s)")

        # ---- verify: COUNT THE FILES, not the shape --------------------- #
        cur.execute(f"SELECT id, document_url, CAST(extra_meta AS nvarchar(max)) "
                    f"FROM regulations WHERE id IN ({ids})")
        missing = []
        for rid, du, em in cur.fetchall():
            rid = int(rid)
            want = _Doc(backup[rid])
            normalise(want)
            want_n = len(split_links(want.extra_meta.get("attachment_links"))) or (
                1 if want.document_url else 0)
            got_n = len(split_links(json.loads(em or "{}").get("attachment_links"))) or (
                1 if str(du or "").strip() else 0)
            if got_n < want_n:
                missing.append((rid, want_n, got_n))
        print()
        if missing:
            print(f"VERIFICATION FAILED: {len(missing)} row(s) still hold fewer "
                  f"files than the backup, e.g. {missing[:5]}")
            return 1
        print("VERIFIED  every row now holds as many file urls as the backup did")
        return 0


if __name__ == "__main__":
    sys.exit(main())
