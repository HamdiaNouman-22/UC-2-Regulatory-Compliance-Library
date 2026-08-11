"""CLI: ask each regulator what version it holds of the documents we store.

Read-only. One SELECT against the library, two bytes per document against the
regulator, and the only thing written anywhere is the sweep's own state file under
output/change_state/ — which is what the NEXT sweep compares against. Nothing
here touches a regulations row.

Three signals, one entry point, because they answer the same question and keep
their memory in the same place:

    stored-inventory  re-read the version token of every url we already store.
                      Detect only — it cannot see a document it never stored.
    gosi              one JSON request per seed page, which returns the WHOLE
                      page, so this one can also report a document as absent.
    snapshot-articles one hash per article of a page already saved on disk.
                      No request at all, which is what makes it usable against
                      a host we are blocked from.
    sitemap           one request for a whole source, where the sitemap's
                      per-url lastmod is a real edit history. It refuses to run
                      on a sitemap that only carries its own build time.

Usage:
    python -m dynamic_crawler.cli.sweep --regulator SDAIA \\
        --source "Laws and Regulations" --workbook output/formfill/run.xlsx

    python -m dynamic_crawler.cli.sweep --regulator SDAIA \\
        --source "Laws and Regulations" --with-db --limit 5 --dry-run

    python -m dynamic_crawler.cli.sweep --signal gosi --source SocialInsurance

    python -m dynamic_crawler.cli.sweep --signal snapshot-articles \\
        --regulator SIMAH --source simah.rules

    python -m dynamic_crawler.cli.sweep --signal sitemap --regulator MHRSD \\
        --source mhrsd.regs --run-workbook output/formfill/mhrsd.regs/run/results.xlsx

A first sweep of a source stores a baseline and reports every document as new.
That is not a change report; the second sweep is.

Every report carries a `withdrawals` block: which absent documents meet the
two-consecutive-sweeps rule and which condition stopped the rest. It is a
proposal for a person — nothing here withdraws anything.
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from dynamic_crawler import changesignal as cs
from dynamic_crawler.change_state import ChangeStateStore
from dynamic_crawler.gosi_signal import SEEDS, GosiJsonSweep
from dynamic_crawler.inventory_sweep import (StoredInventorySweep,
                                             WorkbookInventory, load_config,
                                             run_workbook_urls, settings_for,
                                             skip_hosts)
from dynamic_crawler.sitemap_signal import DEFAULT_CUT, SitemapLastmodSweep
from dynamic_crawler.snapshot_articles import SnapshotArticleSweep
from dynamic_crawler import withdrawal

logger = logging.getLogger(__name__)


def _run(signal, source: str, state_root=None, dry_run: bool = False) -> dict:
    """Run one signal against its own state file and report. Common to both."""
    store = ChangeStateStore.for_source(source, root=state_root)
    first = not store.keys()
    report, buckets = cs.run_sweep(signal, store)
    report["sweep"] = getattr(signal, "stats", {})
    if first:
        report["note"] = ("first sweep for this source: every document is a "
                          "baseline, not a change")
    if not dry_run:
        report["state_file"] = str(store.save())
    else:
        report["state_written"] = False

    # The shortlist is the product, so print it rather than only counting it.
    def entry(o, why: str) -> dict:
        """The `missing` bucket holds bare identity keys, not observations.

        Not one getattr with a default: `getattr(str, "title")` finds the string
        METHOD, so this raised on the first document any sweep reported absent.
        """
        if isinstance(o, str):
            return {"key": o, "title": "", "why": why}
        return {"key": o.key, "title": str(o.title or "")[:70], "why": why}

    verdicts = (cs.MODIFIED, cs.UNKNOWN, cs.NEW, cs.MISSING)
    report["shortlist"] = {
        verdict: [entry(o, why) for o, why in buckets[verdict]]
        for verdict in verdicts if buckets.get(verdict)}
    # Absence counted is not absence acted on. This says which absences meet the
    # rule and which condition stopped the rest; nothing is withdrawn by it.
    report["withdrawals"] = withdrawal.proposals(signal, store, report, buckets)
    return report


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

    return _run(signal, f"{regulator}/{source_system}", state_root, dry_run)


def snapshot_sweep(form: str, *, regulator: str = "SIMAH", state_root=None,
                   snapshot_dir=None, allow_stale: bool = False,
                   dry_run: bool = False) -> dict:
    """Hash each article of a page already saved on disk. No request is made."""
    from dynamic_crawler.formfill.snapshot import SnapshotStore

    store = SnapshotStore(form, directory=snapshot_dir)
    signal = SnapshotArticleSweep(form, store=store, page=form,
                                  allow_stale=allow_stale)
    return _run(signal, f"{regulator}/{form}", state_root, dry_run)


def _tracked_urls(regulator: str, source_system: str, *, repo=None,
                  workbook=None, run_workbook=None) -> list:
    """The urls this source stores, from whichever inventory is available."""
    if run_workbook:
        return run_workbook_urls(run_workbook)
    inventory = repo if repo is not None else WorkbookInventory(workbook)
    rows = inventory.find_regulations_by_source(source_system,
                                                regulator=regulator)
    return [str(r.get("document_url") or "") for r in rows]


def sitemap_sweep(source_system: str, *, regulator: str, sitemap_url=None,
                  repo=None, workbook=None, run_workbook=None, config_path=None,
                  state_root=None, dry_run: bool = False) -> dict:
    """Read one sitemap and shortlist the tracked urls whose lastmod moved."""
    config = load_config(config_path)
    settings = settings_for(config, regulator, source_system)
    url = sitemap_url or settings.get("sitemap")
    if not url:
        raise SystemExit(f"no sitemap url for {regulator}/{source_system}: pass "
                         f"--sitemap or add one to config/change_signals.yml")

    signal = SitemapLastmodSweep(
        url, f"{regulator}/{source_system}",
        _tracked_urls(regulator, source_system, repo=repo, workbook=workbook,
                      run_workbook=run_workbook),
        cut_marker=settings.get("cut_marker") or DEFAULT_CUT,
        timeout=float(settings.get("timeout") or 30))
    return _run(signal, f"{regulator}/{source_system}", state_root, dry_run)


def gosi_sweep(seed: str, *, regulator: str = "GOSI", config_path=None,
               state_root=None, workers=None, probe_documents: bool = True,
               dry_run: bool = False) -> dict:
    """Sweep one GOSI seed page: one JSON request, no browser, no database."""
    config = load_config(config_path)
    settings = settings_for(config, regulator, seed)

    signal = GosiJsonSweep(seed,
                           timeout=float(settings.get("timeout") or 20),
                           probe_documents=probe_documents,
                           workers=workers or settings.get("workers"),
                           skip_hosts=skip_hosts(config))

    return _run(signal, f"{regulator}/{seed}", state_root, dry_run)


def _repo(a):
    """The read-only connection, or None when the inventory comes off disk."""
    if not a.with_db:
        return None
    # Same connection the rest of the pipeline builds, so the driver and the
    # credentials come from one place.
    from dynamic_crawler.formfill.promote import _build_repo
    repo = _build_repo()
    # A SELECT that is allowed to raise, first: find_regulations_by_source
    # raises on failure but a bad login should not look like an empty library.
    repo.get_folder_id("__connectivity_probe__", None)
    return repo


def _emit(report: dict, json_out=None) -> int:
    text = json.dumps(report, indent=2, ensure_ascii=False, default=str)
    print(text)
    if json_out:
        Path(json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(json_out).write_text(text, encoding="utf-8")
    return 0


def main() -> int:
    # Before argparse prints anything: titles and error pages from these
    # regulators are Arabic, and a cp1252 console kills the run on the first one.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            pass

    ap = argparse.ArgumentParser(
        description="Ask a regulator which version it holds of what we store")
    ap.add_argument("--signal", default="stored-inventory",
                    choices=("stored-inventory", "gosi", "snapshot-articles",
                             "sitemap"),
                    help="stored-inventory: probe every url we already store. "
                         "gosi: one JSON request per seed page. "
                         "snapshot-articles: a saved page, no request at all. "
                         "sitemap: one request, per-url lastmod")
    ap.add_argument("--regulator", default="GOSI",
                    help="as stored in the regulations row, e.g. SDAIA. "
                         "Required for --signal stored-inventory")
    ap.add_argument("--source", required=True, dest="source_system",
                    help="the source_system as stored, e.g. 'Laws and "
                         f"Regulations'. For --signal gosi: one of {SEEDS}")
    ap.add_argument("--workbook", help="read the inventory from a formfill "
                                       "workbook instead of the database")
    ap.add_argument("--with-db", action="store_true",
                    help="read the inventory from MSSQL (one SELECT)")
    ap.add_argument("--no-documents", action="store_true",
                    help="gosi: read the page only. It then reports no absence "
                         "at all, because the documents it skipped would be it")
    ap.add_argument("--allow-stale", action="store_true",
                    help="snapshot-articles: sweep a snapshot past its grace "
                         "period anyway. It reports what the page said when it "
                         "was captured, not what it says now")
    ap.add_argument("--snapshot-dir", help="default: output/snapshots")
    ap.add_argument("--run-workbook",
                    help="read the tracked urls from a formfill RUN workbook's "
                         "inventory sheet — the only inventory a form with no "
                         "promoted rows has")
    ap.add_argument("--sitemap", help="sitemap url, when the config has none")
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

    if a.signal == "snapshot-articles":
        report = snapshot_sweep(a.source_system, regulator=a.regulator,
                                state_root=a.state_root,
                                snapshot_dir=a.snapshot_dir,
                                allow_stale=a.allow_stale, dry_run=a.dry_run)
        return _emit(report, a.json_out)

    if a.signal == "gosi":
        if a.source_system not in SEEDS:
            raise SystemExit(f"--source for --signal gosi is one of {SEEDS}, "
                             f"not {a.source_system!r}")
        report = gosi_sweep(a.source_system, regulator=a.regulator,
                            config_path=a.config, state_root=a.state_root,
                            workers=a.workers,
                            probe_documents=not a.no_documents,
                            dry_run=a.dry_run)
        return _emit(report, a.json_out)

    if a.signal == "sitemap":
        if not (a.workbook or a.with_db or a.run_workbook):
            raise SystemExit("pass --run-workbook, --workbook or --with-db: the "
                             "guard measures the sitemap against the urls this "
                             "source stores, and cannot run without them")
        report = sitemap_sweep(a.source_system, regulator=a.regulator,
                               sitemap_url=a.sitemap, repo=_repo(a),
                               workbook=a.workbook, run_workbook=a.run_workbook,
                               config_path=a.config, state_root=a.state_root,
                               dry_run=a.dry_run)
        return _emit(report, a.json_out)

    if not a.workbook and not a.with_db:
        raise SystemExit("pass --workbook <xlsx> or --with-db: the sweep reads "
                         "the urls it probes out of the stored inventory")
    if a.workbook and not Path(a.workbook).exists():
        raise SystemExit(f"no such workbook: {a.workbook}")

    report = sweep(a.regulator, a.source_system,
                   repo=_repo(a), workbook=a.workbook,
                   config_path=a.config, state_root=a.state_root, limit=a.limit,
                   workers=a.workers, dry_run=a.dry_run)
    return _emit(report, a.json_out)


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    sys.exit(main())
