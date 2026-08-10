"""CLI: ask each regulator what version it holds of the documents we store.

Read-only. One SELECT against the library, two bytes per document against the
regulator, and the only thing written anywhere is the sweep's own state file under
output/change_state/ — which is what the NEXT sweep compares against. Nothing
here touches a regulations row.

Usage:
    python -m dynamic_crawler.cli.sweep --regulator SDAIA \\
        --source "Laws and Regulations" --workbook output/formfill/run.xlsx

    python -m dynamic_crawler.cli.sweep --regulator SDAIA \\
        --source "Laws and Regulations" --with-db --limit 5 --dry-run

A first sweep of a source stores a baseline and reports every document as new.
That is not a change report; the second sweep is.
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from dynamic_crawler import changesignal as cs
from dynamic_crawler.change_state import ChangeStateStore
from dynamic_crawler.inventory_sweep import (StoredInventorySweep,
                                             WorkbookInventory, load_config,
                                             settings_for, skip_hosts)

logger = logging.getLogger(__name__)


def sweep(regulator: str, source_system: str, *, repo=None, workbook=None,
          config_path=None, state_root=None, limit=None, workers=None,
          dry_run: bool = False) -> dict:
    """Sweep one source and return the report."""
    if repo is None and workbook is None:
        raise ValueError("give either a repo or a workbook to read the "
                         "stored inventory from")
    config = load_config(config_path)
    settings = settings_for(config, regulator, source_system)

    signal = StoredInventorySweep(
        repo if repo is not None else WorkbookInventory(workbook),
        regulator, source_system,
        identity=settings.get("identity") or ("document_url", "doc_path"),
        confirm_required=bool(settings.get("confirm")),
        workers=workers or settings.get("workers"),
        timeout=float(settings.get("timeout") or 20),
        limit=limit,
        skip_hosts=skip_hosts(config))

    store = ChangeStateStore.for_source(f"{regulator}/{source_system}",
                                        root=state_root)
    first = not store.keys()
    report, buckets = cs.run_sweep(signal, store)
    report["sweep"] = signal.stats
    if first:
        report["note"] = ("first sweep for this source: every document is a "
                          "baseline, not a change")
    if not dry_run:
        report["state_file"] = str(store.save())
    else:
        report["state_written"] = False

    # The shortlist is the product, so print it rather than only counting it.
    report["shortlist"] = {
        verdict: [{"key": o.key if hasattr(o, "key") else o,
                   "title": getattr(o, "title", "")[:70],
                   "why": why}
                  for o, why in buckets[verdict]]
        for verdict in (cs.MODIFIED, cs.UNKNOWN, cs.NEW)
        if buckets[verdict]}
    return report


def main() -> int:
    # Before argparse prints anything: titles and error pages from these
    # regulators are Arabic, and a cp1252 console kills the run on the first one.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            pass

    ap = argparse.ArgumentParser(
        description="Re-read the version token of every stored document")
    ap.add_argument("--regulator", required=True,
                    help="as stored in the regulations row, e.g. SDAIA")
    ap.add_argument("--source", required=True, dest="source_system",
                    help="the source_system as stored, e.g. 'Laws and Regulations'")
    ap.add_argument("--workbook", help="read the inventory from a formfill "
                                       "workbook instead of the database")
    ap.add_argument("--with-db", action="store_true",
                    help="read the inventory from MSSQL (one SELECT)")
    ap.add_argument("--limit", type=int, help="probe only the first N documents")
    ap.add_argument("--workers", type=int, help="override the configured probes "
                                                "in flight")
    ap.add_argument("--dry-run", action="store_true",
                    help="probe and report, but do not update the state file — "
                         "so the next sweep still compares against today's "
                         "baseline")
    ap.add_argument("--config", help="default: config/change_signals.yml")
    ap.add_argument("--state-root", help="default: output/change_state")
    ap.add_argument("--json", dest="json_out", help="also write the report here")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    if not a.workbook and not a.with_db:
        raise SystemExit("pass --workbook <xlsx> or --with-db: the sweep reads "
                         "the urls it probes out of the stored inventory")
    if a.workbook and not Path(a.workbook).exists():
        raise SystemExit(f"no such workbook: {a.workbook}")

    repo = None
    if a.with_db:
        # Same connection the rest of the pipeline builds, so the driver and the
        # credentials come from one place.
        from dynamic_crawler.formfill.promote import _build_repo
        repo = _build_repo()
        # A SELECT that is allowed to raise, first: find_regulations_by_source
        # raises on failure but a bad login should not look like an empty library.
        repo.get_folder_id("__connectivity_probe__", None)

    report = sweep(a.regulator, a.source_system, repo=repo, workbook=a.workbook,
                   config_path=a.config, state_root=a.state_root, limit=a.limit,
                   workers=a.workers, dry_run=a.dry_run)
    text = json.dumps(report, indent=2, ensure_ascii=False, default=str)
    print(text)
    if a.json_out:
        Path(a.json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(a.json_out).write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    sys.exit(main())
