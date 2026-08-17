"""Crawl to a workbook, check it, then put it in the database.

Three commands, meant to be run in this order:

    python -m tools.workbook export moh                    -> output/workbooks/moh.xlsx
    python -m tools.workbook check  output/workbooks/moh.xlsx
    python -m tools.workbook promote output/workbook/moh.xlsx --apply

WHY A WORKBOOK AT ALL
    The direct-write path (`jobs.monitor_jobs`) is right for a source already
    trusted: it crawls and writes in one step, and monitoring keeps it honest.
    A NEW regulator has no baseline and nobody has ever looked at its output, so
    the first crawl is the one you most want to read before it reaches the
    library. This gives you that read.

    `export` opens no database connection at all. Nothing here can touch the
    library until you run `promote --apply`.

WHY `check` EXISTS
    "Looks fine in Excel" is not the same as "will land correctly". Every rule
    it enforces is one that has actually gone wrong here:

      * a missing content_hash classifies the document `modified` on every run
        forever, writing two version rows each time (this cost a whole day)
      * two rows with the same identity silently overwrite each other on insert,
        so the workbook says 40 documents and the library gains 39
      * a row whose identity fields are ALL blank matches every other such row
      * a non-empty `status` is a human decision being forged by a machine
      * >1 attachment with a non-empty document_url breaks the identity rule
        multi-file rows depend on

    `check` reads the file and nothing else. It never opens a connection.

EXIT CODES
    0  fine (warnings may still be printed)
    1  errors found — `promote` would produce a library you did not intend
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter, defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logger = logging.getLogger("workbook")

DEFAULT_DIR = PROJECT_ROOT / "output" / "workbooks"

#: Read by `check`. Identity is the tuple the orchestrator matches on — keep in
#: step with changesignal.DEFAULT_IDENTITY, which is where the reasoning lives.
IDENTITY = ("document_url", "doc_path", "title")
REQUIRED = ("title", "regulator", "source_system", "doc_path")


# --------------------------------------------------------------------------- #
#  export                                                                      #
# --------------------------------------------------------------------------- #

def cmd_export(a) -> int:
    """Crawl one source into a workbook. No database connection is opened."""
    from dynamic_crawler.formfill.excel_repo import ExcelRepo
    from orchestrator.orchestrator import Orchestrator
    from processor.downloader import Downloader
    from jobs.monitor_jobs import build_crawler

    out = Path(a.out) if a.out else DEFAULT_DIR / f"{a.name}.xlsx"
    out.parent.mkdir(parents=True, exist_ok=True)

    crawler, regulator = build_crawler(a.name, a.form)
    repo = ExcelRepo(out)
    orch = Orchestrator(crawler=crawler, repo=repo, downloader=Downloader(),
                        source_name=regulator, analyse=False, limit=a.limit)
    report = orch.run_for_regulator(regulator) or {}

    # ExcelRepo accumulates in memory and writes on save(); without this the
    # process exits with a correct crawl and an empty file.
    if hasattr(repo, "save"):
        repo.save()

    print(json.dumps({k: v for k, v in report.items() if k != "withdrawals"},
                     indent=2, ensure_ascii=False, default=str))
    print(f"\nworkbook: {out}")
    print(f"next    : python -m tools.workbook check \"{out}\"")
    return 0


# --------------------------------------------------------------------------- #
#  check                                                                       #
# --------------------------------------------------------------------------- #

def _key(row: dict) -> str:
    return "|".join(f"{f}={_flat(row.get(f))}" for f in IDENTITY)


def _flat(v) -> str:
    if isinstance(v, (list, tuple)):
        return " > ".join(str(x) for x in v)
    return "" if v is None else str(v).strip()


def _files_of(row: dict) -> list:
    """document_url and attachment_links read as ONE set of files.

    A multi-file row leaves document_url empty and lists everything in
    extra_meta.attachment_links; a single-file row uses document_url. Reading
    only one of the two makes half the library look empty.
    """
    files = []
    url = _flat(row.get("document_url"))
    if url:
        files.append(url)
    meta = row.get("extra_meta")
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    links = (meta or {}).get("attachment_links") if isinstance(meta, dict) else None
    if isinstance(links, str):
        files += [x.strip() for x in links.split("|") if x.strip()]
    elif isinstance(links, (list, tuple)):
        files += [str(x).strip() for x in links if str(x).strip()]
    return files


def cmd_check(a) -> int:
    from dynamic_crawler.formfill.promote import _read

    path = Path(a.workbook)
    if not path.exists():
        raise SystemExit(f"no such workbook: {path}")

    sheets = _read(path)
    rows = sheets.get("regulations") or []
    errors, warnings = [], []

    if not rows:
        print(json.dumps({"workbook": str(path), "regulations": 0,
                          "errors": ["the regulations sheet is empty -- a crawl "
                                     "that found nothing is a failed crawl, not "
                                     "an empty regulator"]}, indent=2))
        return 1

    # 1. required fields
    for i, r in enumerate(rows, 1):
        missing = [f for f in REQUIRED if not _flat(r.get(f))]
        if missing:
            errors.append(f"row {i} ({_flat(r.get('title'))[:40]!r}) is missing "
                          f"{', '.join(missing)}")

    # 2. identity collisions — the workbook says N, the library would gain fewer
    seen = defaultdict(list)
    for i, r in enumerate(rows, 1):
        seen[_key(r)].append(i)
    for key, idx in seen.items():
        if len(idx) > 1:
            errors.append(f"{len(idx)} rows share one identity (rows {idx[:6]}"
                          f"{'...' if len(idx) > 6 else ''}): {key[:120]} -- they "
                          f"overwrite each other on insert")

    # 3. an identity that is entirely blank matches every other blank one
    blank = [i for i, r in enumerate(rows, 1)
             if not any(_flat(r.get(f)) for f in IDENTITY)]
    if blank:
        errors.append(f"{len(blank)} row(s) have every identity field blank "
                      f"(rows {blank[:6]}) -- they all match each other")

    # 4. status is a human column
    stamped = [i for i, r in enumerate(rows, 1) if _flat(r.get("status"))]
    if stamped:
        errors.append(f"{len(stamped)} row(s) carry a non-empty `status` "
                      f"(rows {stamped[:6]}) -- approve/reject is a person's "
                      f"decision and nothing automated may write it")

    # 5. the multi-file rule
    for i, r in enumerate(rows, 1):
        files, url = _files_of(r), _flat(r.get("document_url"))
        if len(files) > 1 and url:
            errors.append(f"row {i} has {len(files)} files AND a document_url -- "
                          f"a multi-file row must leave document_url empty, or "
                          f"its identity depends on which file the site listed "
                          f"first")
        if not files:
            warnings.append(f"row {i} ({_flat(r.get('title'))[:40]!r}) has no "
                            f"file at all -- neither document_url nor "
                            f"attachment_links")

    # 6. THE SIDECAR CAME WITH THE WORKBOOK.
    #
    # A cell tops out at 32,767 characters, so anything longer is parked in a
    # `<workbook>.fulltext.json` beside the file and a marker is left in the
    # cell. `promote` rehydrates from it. If the workbook was emailed or copied
    # WITHOUT the sidecar there is nothing to rehydrate from, and the preview is
    # promoted as if it were the whole document -- a 92,995 character instrument
    # arriving as 32,028, its text stopping mid-article, with nothing to say so.
    #
    # This is a handoff failure, not a crawl failure: it only happens when the
    # file travels between people, which is exactly what this tool is for.
    from dynamic_crawler.formfill.excel_repo import OVERFLOW_PREFIX
    sidecar_path = path.with_suffix(".fulltext.json")
    marked = [i for i, r in enumerate(rows, 1)
              if any(OVERFLOW_PREFIX in _flat(v) for v in r.values())]
    if marked and not sidecar_path.exists():
        errors.append(
            f"{len(marked)} row(s) hold text too long for a cell (rows "
            f"{marked[:6]}) but {sidecar_path.name} is missing -- it must travel "
            f"WITH the .xlsx. Promoting now would store the preview and make the "
            f"truncation permanent")
    elif marked:
        warnings.append(f"{len(marked)} row(s) rehydrate from "
                        f"{sidecar_path.name} -- keep the two files together")

    # 7. the fingerprint -- a warning, not an error: the row still lands, it just
    #    re-versions itself on every run until its crawler is fixed.
    nofp = [i for i, r in enumerate(rows, 1) if not _flat(r.get("content_hash"))]
    if nofp:
        warnings.append(f"{len(nofp)} of {len(rows)} row(s) have no "
                        f"content_hash -- each will classify `modified` on every "
                        f"run and write a version row each time")

    report = {
        "workbook": str(path),
        "regulations": len(rows),
        "versions": len(sheets.get("regulation_versions") or []),
        "folders": len(sheets.get("compliancecategory") or []),
        "regulators": dict(Counter(_flat(r.get("regulator")) for r in rows)),
        "errors": errors,
        "warnings": warnings,
        "verdict": "REJECT" if errors else "OK",
    }
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    if errors:
        print(f"\n{len(errors)} error(s) -- fix the crawler and re-export. "
              f"`promote` would not give you the library this workbook shows.")
    else:
        print(f"\nOK. next: python -m tools.workbook promote \"{path}\" --apply")
    return 1 if errors else 0


# --------------------------------------------------------------------------- #
#  promote                                                                     #
# --------------------------------------------------------------------------- #

def cmd_promote(a) -> int:
    """Insert an approved workbook into MSSQL.

    Dry run unless --apply, and the dry run opens a READ-ONLY connection so
    `skipped_already_present` is a real number rather than always 0.

    Promoting is idempotent: it matches on identity and skips what is already
    stored, so running it twice inserts nothing the second time.
    """
    from dynamic_crawler.formfill.promote import _build_repo, promote

    path = Path(a.workbook)
    if not path.exists():
        raise SystemExit(f"no such workbook: {path}")

    # The gate is not optional. Promoting a workbook that `check` rejects is the
    # one thing this tool exists to prevent, so it runs the same checks first
    # rather than trusting that somebody remembered to.
    if not a.skip_check:
        rc = cmd_check(argparse.Namespace(workbook=str(path)))
        if rc:
            print("\nrefusing to promote a workbook that failed `check`. "
                  "Pass --skip-check only if you have read every error above "
                  "and decided each one is acceptable.")
            return 1

    repo = _build_repo()
    repo.get_folder_id("__connectivity_probe__", None)   # fail loudly on a bad login
    report = promote(path, repo, dry_run=not a.apply)
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    if not a.apply:
        print("\nDRY RUN -- nothing written. Re-run with --apply.")
    return 1 if report.get("failed") else 0


def main() -> int:
    ap = argparse.ArgumentParser(
        prog="python -m tools.workbook",
        description="Crawl to a workbook, check it, then load it into MSSQL.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    e = sub.add_parser("export", help="crawl a source into a workbook (no DB)")
    e.add_argument("name", help="a form name (zatca.taxes) or source config (moh)")
    e.add_argument("--form", action="store_true",
                   help="`name` is a hints file, not a config/sources entry")
    e.add_argument("-o", "--out", help=f"default: {DEFAULT_DIR}/<name>.xlsx")
    e.add_argument("--limit", type=int, default=None)
    e.set_defaults(func=cmd_export)

    c = sub.add_parser("check", help="validate a workbook; opens no connection")
    c.add_argument("workbook")
    c.set_defaults(func=cmd_check)

    p = sub.add_parser("promote", help="insert an approved workbook into MSSQL")
    p.add_argument("workbook")
    p.add_argument("--apply", action="store_true",
                   help="actually write; without it this is a dry run")
    p.add_argument("--skip-check", action="store_true",
                   help="promote even if `check` reports errors")
    p.set_defaults(func=cmd_promote)

    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    return a.func(a)


if __name__ == "__main__":
    raise SystemExit(main())
