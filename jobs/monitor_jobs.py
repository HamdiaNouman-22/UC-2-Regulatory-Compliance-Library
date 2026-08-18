"""Scheduled monitoring — the jobs the scheduler calls, writing straight to MSSQL.

FIVE JOBS, NOT TWELVE. Regulators are grouped by what their site will actually
answer, because that — not the regulator's importance — is what decides how often
and how expensively it can be checked.

    monitor_cheap_probes   daily    MOE, SDAIA, AML, MHRSD, ZATCA — ask each
                                    stored url for its version token, crawl only
                                    what moved. MOH rides along in the same job
                                    but skips the probe: its crawl already IS a
                                    cheap, self-describing signal (see below).
    monitor_sama           daily    ~3 seconds. SAMA publishes its own
                                    "what changed" page, so one request replaces
                                    6,101 probes — and it also DISCOVERS
                                    documents we do not hold, which a probe
                                    structurally cannot.
    monitor_mc             weekly   ~16 minutes. mc.gov.sa refuses plain HTTP
                                    clients, so no probe can answer and the
                                    CRAWL is the signal.
    monitor_cma            weekly   CMA's token is the current time and a
                                    confirm costs a full page fetch, so the
                                    crawl is the signal here too.
    monitor_cbe            weekly   Twelve sources: the circulars API (one
                                    request, and it DISCOVERS) plus eleven
                                    browser crawls of the HTML sections. Weekly
                                    because of the eleven, not the one.

WHERE THE ROWS GO

Straight into MSSQL, by the lead's decision 2026-08-16 — no workbook, no
approval step in the middle.

EXCELREPO IS NOT ON THIS PATH, AND MUST NOT BE PUT BACK ON IT. The class stays
in the repo (it is a working second implementation of the same contract, and
`promote` still replays a workbook when someone deliberately produces one), but
no scheduled job may write through it. Confirmed by the lead 2026-08-16: "data
should drop directly in db no excel needed... keep the repo but dont use it in
actual orch path".

That decision has a cost worth stating once: the workbook was the only place a
person saw rows before they entered the library. What replaces it is `status`
— every row arrives empty and a human sets active/reject — so the review moved
from before the write to after it, and nothing is lost as long as something
actually reads `WHERE status = ''`. `status` is still left EMPTY: the orchestrator's
`_set_status` puts the monitoring state in extra_meta and leaves the column for a
person, so "what arrived overnight and nobody has judged" is exactly

    SELECT * FROM regulations WHERE status = ''

Nothing here writes `active`. A pipeline that approves its own output is not an
approval.

WHY THE BLOCKED SITES HAVE NO JOB AT ALL

Saudi Exchange and SIMAH are deliberately absent, and this is the important part:
they are not merely skipped, they must not be RETRIED BY A MACHINE.

    saudiexchange.sa   Akamai 403 to everything, headless browser included, so
                       it is the IP being judged and not the User-Agent. It was
                       reachable at 18:27 on 2026-08-15 and blocked within two
                       hours, after one crawl plus repeated probes from this
                       address.
    simah.com          Cloudflare 1020-class block. The note in
                       config/change_signals.yml records that it was "triggered
                       by repeated iteration, not volume".

Both blocks were caused by automated access from one address. A scheduled retry
is therefore not a way out of them — it is the thing that made them, and it would
deepen them. `skip_hosts` in config/change_signals.yml already stops a sweep
touching either host; this file additionally gives them no job, so nothing can
schedule its way past that. Each entry carries an `until` date, and that date is
when a PERSON may retest by hand — it is a review date, not an expiry. Nothing
here unblocks itself.

RUNNING THIS

The functions are registered in scheduler/scheduler.py's DIRECT_JOB_MAPPING and
timed by config/scheduler.yml. Nothing starts them from this file.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

from filelock import FileLock, Timeout as LockTimeout

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
#  overlap guard                                                               #
# --------------------------------------------------------------------------- #
#
# APScheduler's `max_instances=1` (set in scheduler.py) only stops ONE job from
# overlapping ITSELF — it does nothing for two different jobs. MHRSD's
# `Page crashed` on 2026-08-16 was memory contention between two separate
# monitor_* jobs' browser crawls running at once: the daily/weekly stagger in
# config/scheduler.yml is a schedule, not a guarantee, and CMA's crawl (up to
# 2h49m measured) can still be running when the next job's trigger fires.
#
# All four monitor_* jobs share this one lock because any of them can end up
# running a browser crawl (`_crawl_into_db` / `FormfillCrawler`), and it is
# concurrent BROWSER work, not concurrent jobs per se, that crashes a page.
#
# Decision: SKIP, not queue. These are recurring sweeps (daily or weekly) —
# the next trigger picks up whatever a skipped run would have found. Queueing
# risks pile-up instead: a slow CMA run queueing behind a stuck cheap_probes
# run, then next week's CMA queueing behind THAT, with no bound on how far
# behind it falls. A skip is visible in the log and cheap to recover from; a
# growing queue of stacked crawls is not.
_LOCK_PATH = REPO_ROOT / "output" / "monitor_jobs.lock"


def _run_exclusive(job_name: str, fn):
    """Run `fn()` only if no other monitor_* job currently holds the crawl
    lock. Returns fn()'s result, or a skip dict if the lock was busy.
    """
    _LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(_LOCK_PATH), timeout=0)
    try:
        with lock:
            return fn()
    except LockTimeout:
        msg = (f"{job_name} SKIPPED: another monitor_* job already holds "
               f"{_LOCK_PATH.name} (concurrent crawls crash a browser page, "
               f"see comment above _run_exclusive). Next scheduled run will "
               f"pick this up.")
        logger.warning(msg)
        return {"skipped": True, "reason": msg}

#: Sources whose site answers a cheap probe honestly. Measured 2026-08-15/16 —
#: every one of these returned 0 false `modified` on a second sweep once its
#: signal was configured (ZATCA and CMA needed `confirm: true`; ZATCA is here,
#: CMA is not, because confirming CMA costs a full page fetch per document).
#: MOH is NOT here — see CRAWL_AS_SIGNAL below, it used to be but the probe step
#: was pure overhead once the site's real API was found.
CHEAP_PROBE_SOURCES = [
    ("Ministry of Education", "Systems, Regulations and Policies"),
    ("Saudi Data and AI Authority (SDAIA)", "Laws and Regulations"),
    ("Anti-Money Laundering Permanent Committee (AML)", "Rules and Regulations"),
    ("Ministry of Human Resource and Social Development (MHRSD)",
     "Regulations and procedural guidelines"),
    ("ZATCA", "Rules and Regulations"),
]

#: Regulator -> (crawler name, is_form) for the sources whose crawl IS the
#: signal. Kept here rather than derived, so adding one is a deliberate act.
#:
#: MOH joined this 2026-08-17. It used to be a CHEAP_PROBE_SOURCES entry —
#: probe each stored url, crawl only if something moved — but that assumed the
#: only way to read the site was a browser walk of the "Recent"/"Archived"
#: lists (`dynamic_crawler/hints/moh.rules_recent.yml` /
#: `moh.rules_archived.yml`, still in the repo and still correct, just no
#: longer on this path). The real listing page turns out to call a SharePoint
#: REST endpoint that returns all 83 documents, with each one's own
#: last-changed timestamp, in a single ~2 second request
#: (`crawler/moh_crawler.py`). That is already cheaper than a per-url probe
#: loop AND it already tells you what changed, so the probe step was pure
#: overhead — the crawl IS the signal here, same as MC and CMA, just fast
#: enough to run daily instead of weekly.
#:
#: CBE joined 2026-08-18, and for the same reason MOH did: its circulars page is
#: a "Load more" pager that a crawl reads 4.5% of (18 of 396, reported `ok`),
#: while the page's own JavaScript calls /api/listing/circulars and returns all
#: 396 in ONE request — with a publication date, the regulator's own category,
#: and a Sitecore GUID per record. No probe can improve on that.
#:
#: CBE differs from MOH in one way that decides its cadence: its config holds
#: TWELVE sources, and eleven of them are browser crawls of the HTML sections.
#: `build_regulator_crawler` has no source filter, so a job gets all twelve or
#: none — which is why CBE is weekly like MC and CMA rather than daily like MOH.
#: If daily circulars are ever wanted, the upgrade is to split cbe.yml in two,
#: not to run eleven browser crawls every night.
CRAWL_AS_SIGNAL = {
    "Ministry of Commerce": ("mc", False),
    "Capital Market Authority (CMA)": ("cma", False),
    "Ministry of Health": ("moh", False),
    "Central Bank of Egypt (CBE)": ("cbe", False),
}


# --------------------------------------------------------------------------- #
#  plumbing                                                                    #
# --------------------------------------------------------------------------- #

def _repo():
    """An MSSQLRepository from .env. Direct writes, no workbook."""
    from dotenv import load_dotenv
    from storage.mssql_repo import MSSQLRepository
    load_dotenv(REPO_ROOT / ".env", override=True)
    return MSSQLRepository({
        "server": os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver": os.getenv("MSSQL_DRIVER", "{ODBC Driver 17 for SQL Server}"),
    })


def _run(cmd, timeout):
    """A child process, decoded as utf-8.

    `text=True` alone decodes with the LOCALE encoding — cp1252 on this machine —
    and every report here carries Arabic titles. A finished CMA sweep was once
    reported as FAILED for exactly that reason, after 600 seconds of real work.
    """
    t0 = time.time()
    p = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True,
                       encoding="utf-8", errors="replace", timeout=timeout)
    return p, time.time() - t0


def _sweep(regulator: str, source: str, targets_file: Path, timeout=1800) -> dict:
    """One source's change sweep. Returns the report, or {} if it failed."""
    cmd = [sys.executable, "-B", "-m", "dynamic_crawler.cli.sweep",
           "--signal", _signal_for(regulator, source), "--regulator", regulator,
           "--source", source, "--with-db", "--targets", str(targets_file)]
    try:
        p, dt = _run(cmd, timeout)
        rep = json.loads(p.stdout[p.stdout.index("{"):])
        rep["_seconds"] = round(dt, 1)
        return rep
    except Exception as e:                       # noqa: BLE001 — logged, not raised
        logger.error("sweep failed for %s/%s: %s", regulator, source, str(e)[:200])
        return {}


def _signal_for(regulator: str, source: str) -> str:
    """Which signal this source uses — from config, never hardcoded.

    config/change_signals.yml already describes a source's monitoring (MHRSD's
    sitemap, AML's confirm), so the CHOICE of signal belongs there too.
    """
    import yaml
    cfg = yaml.safe_load(
        (REPO_ROOT / "config" / "change_signals.yml").read_text(encoding="utf-8"))
    for src in (cfg.get("sources") or []):
        if (src.get("regulator") == regulator
                and src.get("source_system") == source):
            return src.get("signal") or "stored-inventory"
    return "stored-inventory"


def build_crawler(name: str, is_form: bool, only_urls=None):
    """(crawler, regulator) for a form name or a source-config name.

    PUBLIC because `tools/workbook.py` needs the identical crawler in order to
    export to a workbook instead of the database. A second copy of this would
    drift — the workbook and the direct-write path would quietly crawl different
    things, and the workbook you approved would not be what the database got.
    """
    if is_form:
        from dynamic_crawler.formfill.pipeline import FormfillCrawler
        from dynamic_crawler.formfill.schema import load_hints
        path = REPO_ROOT / "dynamic_crawler" / "hints" / f"{name}.yml"
        lib = (load_hints(str(path)).get("library") or {})
        regulator = lib.get("regulator") or name
        return FormfillCrawler(str(path),
                               regulator=regulator,
                               source_system=lib.get("source_system") or name,
                               require_approved=False,
                               only_urls=only_urls), regulator

    import yaml
    from crawler.generic_crawler_wrapper import build_regulator_crawler
    cfg = yaml.safe_load(
        (REPO_ROOT / "config" / "sources" / f"{name}.yml").read_text(
            encoding="utf-8")) or {}
    return build_regulator_crawler(cfg), cfg.get("regulator", name.upper())


def _crawl_into_db(name: str, is_form: bool, only_urls=None, timeout=14400) -> dict:
    """Crawl a source and write what it finds straight to MSSQL.

    The orchestrator classifies each document new / modified / unchanged against
    the stored rows, versions what changed, and builds the folder tree. `status`
    is left empty by `_set_status` — a person decides that.
    """
    from processor.downloader import Downloader
    from orchestrator.orchestrator import Orchestrator

    crawler, regulator = build_crawler(name, is_form, only_urls)
    orch = Orchestrator(crawler=crawler, repo=_repo(), downloader=Downloader(),
                        source_name=regulator)
    t0 = time.time()
    result = orch.run_for_regulator(regulator) or {}
    result["_seconds"] = round(time.time() - t0, 1)
    return result


# --------------------------------------------------------------------------- #
#  the jobs                                                                    #
# --------------------------------------------------------------------------- #

def monitor_cheap_probes() -> dict:
    """DAILY. Probe every source whose site answers honestly, crawl what moved.

    One HTTP request per stored document, about a minute for all six sources.
    A source with nothing changed costs exactly that and no crawl — which is the
    whole point of probing before crawling.
    """
    return _run_exclusive("monitor_cheap_probes", _monitor_cheap_probes_impl)


def _monitor_cheap_probes_impl() -> dict:
    out = {}
    state = REPO_ROOT / "output" / "monitor_targets"
    state.mkdir(parents=True, exist_ok=True)
    for regulator, source in CHEAP_PROBE_SOURCES:
        tf = state / ("".join(c if c.isalnum() else "_" for c in regulator)[:60]
                      + ".txt")
        rep = _sweep(regulator, source, tf)
        counts = rep.get("counts", {})
        targets = [l.strip() for l in
                   (tf.read_text(encoding="utf-8").splitlines()
                    if tf.exists() else []) if l.strip()]
        entry = {"counts": counts, "targets": len(targets),
                 "seconds": rep.get("_seconds")}
        # A crawl only when something actually moved. `new` on a detect-only
        # sweep means "first time this was swept", not a new document, so it
        # must NOT pull a crawl — that would re-read the whole source on its
        # first run.
        if targets:
            forms = _forms_for(regulator)
            if forms:
                # A regulator can have MORE THAN ONE form sharing one
                # (regulator, source_system) pair -- MOH (recent/archived) and
                # ZATCA (5 sub-forms) both do, and the sweep above probes them
                # as one source, so `targets` can mix urls from any of them.
                # Picking just the first form (the old behaviour) silently
                # dropped every target that belonged to a different form: that
                # form's own listing never contains another form's urls, so
                # `only_urls` would find nothing and the change went
                # unrecrawled with no error. Every matching form gets the same
                # target list instead; each one only opens the urls it
                # actually finds in its own listing, so this is safe even when
                # most targets belong to a sibling form.
                entry["crawl"] = {
                    f: _crawl_into_db(f, True, only_urls=targets) for f in forms
                }
            else:
                entry["crawl"] = {"skipped": "no crawler mapped"}
        out[regulator] = entry
        logger.info("%s: %s", regulator, entry)

    # MOH: no probe-then-crawl here. The crawl (crawler/moh_crawler.py, via
    # CRAWL_AS_SIGNAL) reads the site's own SharePoint API directly -- ~2
    # seconds for all 83 documents, cheaper than probing each one individually,
    # and each document carries its own change timestamp so the orchestrator's
    # new/modified/unchanged classification against the DB already does what
    # the probe step exists to do for the other five sources. Direct, every day.
    moh_rep = _crawl_into_db("moh", False)
    out["Ministry of Health"] = moh_rep
    logger.info("Ministry of Health: %s", moh_rep)

    return out


def monitor_sama() -> dict:
    """DAILY. SAMA's own revision page: one request instead of 6,101 probes.

    Also the only signal here that DISCOVERS — an entry matching nothing we hold
    is a document missing from the library, which a stored-inventory probe can
    never report because it only re-reads rows we already have.
    """
    return _run_exclusive("monitor_sama", _monitor_sama_impl)


def _monitor_sama_impl() -> dict:
    state = REPO_ROOT / "output" / "monitor_targets"
    state.mkdir(parents=True, exist_ok=True)
    tf = state / "SAMA.txt"
    rep = _sweep("Saudi Arabian Monetary Authority (SAMA)", "SAMA RULEBOOK", tf)
    out = {"counts": rep.get("counts", {}), "feed": rep.get("feed", {}),
           "seconds": rep.get("_seconds")}
    # Documents the feed named that the library does not hold.
    if (rep.get("feed") or {}).get("not_in_library"):
        p, dt = _run([sys.executable, "-B", "benchmarks/sama_feed_ingest.py"], 3600)
        out["discovery"] = {"rc": p.returncode, "seconds": round(dt, 1)}
    logger.info("SAMA: %s", out)
    return out


def monitor_mc() -> dict:
    """WEEKLY. The crawl is the signal — mc.gov.sa refuses plain HTTP clients.

    Measured 2026-08-15: requests.get is reset on every url, while a headless
    Chromium gets 200 on the same ones. So a probe can only ever answer
    `unknown` here, and re-crawling is the only way to see a change.
    """
    return _run_exclusive("monitor_mc", _monitor_mc_impl)


def _monitor_mc_impl() -> dict:
    res = _crawl_into_db("mc", False, timeout=5400)
    logger.info("Ministry of Commerce: %s", res)
    return res


def monitor_cma() -> dict:
    """WEEKLY. The crawl is the signal, and it must NOT walk the whole history.

    CMA cannot be probed (its Last-Modified is the current time) and cannot be
    confirmed at scale (a confirm is a full page fetch and the host throttles
    after ~60 of 1,979).

    THE ANNOUNCEMENTS TAB IS THE TRAP. It is 3,299 items over 550 pages, and a
    full walk measured 2h49m on 2026-08-16 and still came back with 300 of the
    1,053 announcements we already hold — reported as a clean run. A short crawl
    is worse than none here: the 753 it missed would be ruled `disappeared` and
    become withdrawal proposals.

    Announcements are ordered NEWEST FIRST, so monitoring does not need the
    history at all — only back as far as the newest one already stored. That is
    what `since_days` on the announcements tab is for
    (site_runners/cma_laws.py); it is currently None for the one-off backfill and
    MUST be a small window here. Set CMA_SINCE_DAYS to control it.
    """
    return _run_exclusive("monitor_cma", _monitor_cma_impl)


def _monitor_cma_impl() -> dict:
    days = os.getenv("CMA_SINCE_DAYS", "30")
    os.environ["CMA_SINCE_DAYS"] = days      # read by the CMA runner
    res = _crawl_into_db("cma", False, timeout=14400)
    res["announcements_window_days"] = days
    logger.info("CMA: %s", res)
    return res


def monitor_cbe() -> dict:
    """WEEKLY. Twelve sources: the circulars API, plus eleven section crawls.

    THE CIRCULARS HALF IS THE CHEAP, HONEST SIGNAL and it also DISCOVERS.
    `crawler/cbe_crawler.py` reads /api/listing/circulars in one request and gets
    all 396 with a publication date and a Sitecore GUID each, so the orchestrator's
    new/modified/unchanged classification against the DB already does everything a
    probe step would. It refuses to return a partial inventory rather than let a
    short list read downstream as documents having disappeared.

    THE OTHER ELEVEN ARE BROWSER CRAWLS, which is what makes this weekly. Measured
    2026-08-18 over the nine sections then configured: 100 pages, 152 documents.
    `Regulations Book` is the big one at 143 sitemap urls and is capped at 250.

    WHY THE PACING MATTERS HERE MORE THAN USUAL. cbe.org.eg runs bot protection —
    it already refuses `urllib` outright and answers HEAD with 403. Both hosts in
    `skip_hosts` were blocked by automated access from this address, and SIMAH's
    note records that it was "triggered by repeated iteration, not volume".
    Eleven prefix crawls is real iteration. Weekly, and never in a retry loop.

    A SHORT CRAWL IS THE DANGER, not a slow one — the CMA lesson. A section that
    hits its page cap or times out returns fewer documents than are stored, and
    absent documents are ruled `disappeared`. The orchestrator's completeness gate
    is what stands between that and a withdrawal proposal, and it is keyed per
    source, which is exactly why cbe.yml splits the sections rather than crawling
    /en/laws-regulations as one. Give this job room rather than a tight timeout.
    """
    return _run_exclusive("monitor_cbe", _monitor_cbe_impl)


def _monitor_cbe_impl() -> dict:
    # 10800s = 3 hours. Deliberately generous: CMA's job was killed at 5400s
    # after real work and produced nothing, and a killed run is worse than a slow
    # one because it looks like a source that returned nothing.
    res = _crawl_into_db("cbe", False, timeout=10800)
    logger.info("Central Bank of Egypt: %s", res)
    return res


def _forms_for(regulator: str) -> list:
    """Every hints form that crawls this regulator, sorted for determinism.

    A regulator is not always one form: MOH is split recent/archived and ZATCA
    is split five ways (taxes, agreements, and three Information Exchange
    Portal sub-forms), all sharing one (regulator, source_system) pair in
    change_signals.yml because that is what the sweep probes as one source.
    Returning only the first match (the old behaviour) meant a probe could
    detect a change and then hand it to the wrong form to re-crawl, which
    finds nothing in its own listing and drops the change silently.
    """
    from dynamic_crawler.formfill.schema import load_hints
    hints = REPO_ROOT / "dynamic_crawler" / "hints"
    forms = []
    for p in sorted(hints.glob("*.yml")):
        try:
            lib = (load_hints(str(p)).get("library") or {})
        except Exception:
            continue
        if lib.get("regulator") == regulator:
            forms.append(p.stem)
    return forms


__all__ = ["monitor_cheap_probes", "monitor_sama", "monitor_mc", "monitor_cma",
           "monitor_cbe"]
