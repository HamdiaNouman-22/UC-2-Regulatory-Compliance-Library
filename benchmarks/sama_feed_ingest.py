"""Bring in the documents SAMA's revision feed found that the library does not hold.

`--signal sama-feed` reports them; this fetches them and writes a workbook that
`promote` inserts. It is the DISCOVERY half of monitoring — a stored-inventory
probe can only ever re-read rows we already have, so without this the feed can
say "there are 4 documents you are missing" and nothing acts on it.

THE FOLDER PATH COMES FROM THE PAGE, NOT FROM THE FEED

The feed hands back a `book-trail`, and it is tempting to file by it. It is the
wrong axis. Measured 2026-08-15 on "Guide To Financial Institutions Services
Fees":

    feed book-trail : All Regulated Entities | Banks | FinTechs | Credit Bureaus
    page breadcrumb : SAMA Rulebook | All Financial Institutions |
                      Consumer Protection and Financial Conduct | <title>

The trail says WHO the document applies to; the breadcrumb says WHERE it lives.
Filing by the trail would have put these documents in folders that do not exist
in the library's tree.

    venv/Scripts/python.exe benchmarks/sama_feed_ingest.py --since 2026-01-01
    venv/Scripts/python.exe -m dynamic_crawler.formfill.promote <the workbook>
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from crawler.sama_rulebook_crawler import (SAMAFullRulebookCrawler,  # noqa: E402
                                           SAMA_REGULATOR)
from dynamic_crawler.sama_feed_signal import (canonical_node_url,  # noqa: E402
                                              default_window, fetch_entries)
from models.models import RegulatoryDocument  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("sama-ingest")

SOURCE_SYSTEM = "SAMA RULEBOOK"


def stored_urls() -> set:
    import os

    import pyodbc
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env", override=True)
    cs = (f"DRIVER={os.getenv('MSSQL_DRIVER')};"
          f"SERVER={os.getenv('MSSQL_SERVER')};"
          f"DATABASE={os.getenv('MSSQL_DATABASE')};")
    user = os.getenv("MSSQL_USERNAME")
    cs += (f"UID={user};PWD={os.getenv('MSSQL_PASSWORD')};" if user
           else "Trusted_Connection=yes;")
    cs += "TrustServerCertificate=yes;"
    with pyodbc.connect(cs, autocommit=True) as cn:
        return {str(r[0] or "").split("?")[0].rstrip("/").lower()
                for r in cn.execute(
                    "SELECT document_url FROM regulations "
                    "WHERE regulator LIKE '%SAMA%'")}


def breadcrumb(soup) -> list:
    for sel in ("nav.breadcrumb a", ".breadcrumb a", "ol.breadcrumb li"):
        els = soup.select(sel)
        if els:
            return [e.get_text(strip=True) for e in els if e.get_text(strip=True)]
    return []


def build(entry: dict, crawler) -> RegulatoryDocument | None:
    soup = crawler._fetch(entry["url"])
    if soup is None:
        logger.warning("could not open %s", entry["url"])
        return None
    leaf = crawler._extract_structured_leaf(soup, entry["title"]) or {}
    crumbs = breadcrumb(soup)
    # Drop "SAMA Rulebook" (it is source_system) and the last crumb (the title).
    middle = [c for c in crumbs[1:-1] if c] if len(crumbs) > 2 else []
    category = middle[0] if middle else "SAMA Rulebook"
    doc = RegulatoryDocument(
        regulator=SAMA_REGULATOR,
        source_system=SOURCE_SYSTEM,
        category=category,
        title=entry["title"],
        document_url=entry["url"],
        source_page_url=entry.get("slug_url") or entry["url"],
        published_date=leaf.get("date_gregorian") or entry.get("date"),
        reference_no=leaf.get("reference_no"),
        file_type="HTML",
        document_html=leaf.get("document_html") or "",
    )
    doc.doc_path = [SAMA_REGULATOR, SOURCE_SYSTEM] + middle + [entry["title"]]
    # `status` is the HUMAN approve/reject flag and stays empty. The site's own
    # "In-Force"/"Superseded" is the REGULATOR's claim about its document, so it
    # goes to extra_meta — the same split every other source here uses.
    doc.status = ""
    doc.extra_meta = {
        "sama_status": leaf.get("status") or "",
        "sama_date_hijri": leaf.get("date_hijri") or "",
        "org_pdf_link": leaf.get("org_pdf_link") or "",
        "feed_book_trail": entry.get("trail") or "",
        "discovered_by": "sama revision feed",
        "feed_date_shown": entry.get("date") or "",
    }
    return doc


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    lo, hi = default_window(30)
    ap.add_argument("--since", default=lo)
    ap.add_argument("--until", default=hi)
    ap.add_argument("--out", default=str(
        REPO_ROOT / "output" / "formfill" / "_orch_runs" / "crawl" /
        "SAMA-discoveries.xlsx"))
    a = ap.parse_args()

    entries = fetch_entries(a.since, a.until)
    logger.info("feed: %d entr(ies) for %s..%s", len(entries), a.since, a.until)
    have = stored_urls()

    crawler = SAMAFullRulebookCrawler()
    docs, seen = [], 0
    for e in entries:
        node = canonical_node_url(e["slug_url"])
        url = node or e["slug_url"]
        if url.split("?")[0].rstrip("/").lower() in have:
            seen += 1
            continue
        doc = build({"url": url, "slug_url": e["slug_url"], "title": e["title"],
                     "date": e["date_shown"], "trail": e["book_trail"]}, crawler)
        if doc:
            docs.append(doc)
            logger.info("NEW  %-56s %s", doc.title[:56], " > ".join(doc.doc_path[2:-1]))

    logger.info("already in the library: %d | to insert: %d", seen, len(docs))
    if not docs:
        logger.info("nothing to write")
        return 0

    from dynamic_crawler.formfill.excel_repo import ExcelRepo
    from dynamic_crawler.formfill.orch import NewOrchestrator

    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    class _Shim:
        """The orchestrator crawls; here the documents are already in hand."""
        regulator = SAMA_REGULATOR
        source_system = SOURCE_SYSTEM

        def fetch_documents(self, limit=None):
            return docs[:limit] if limit else docs

    repo = ExcelRepo(str(out))
    orch = NewOrchestrator(_Shim(), repo=repo, analyse=False,
                           source_name=f"{SAMA_REGULATOR}/{SOURCE_SYSTEM}")
    # run_for_regulator is the entry point: it fetches, classifies each document
    # new/modified/unchanged against the repo, and versions what changed.
    result = orch.run_for_regulator(SAMA_REGULATOR)
    logger.info("orchestrator: %s", {k: v for k, v in (result or {}).items()
                                     if k in ("new", "modified", "unchanged",
                                              "total", "stored")})
    # ExcelRepo BUFFERS: nothing reaches disk until save() is called. Without
    # this the orchestrator ran all four documents — fetch, OCR, classify — and
    # the run reported a workbook path that did not exist.
    repo.save()
    logger.info("workbook: %s", out)
    logger.info("promote with:  venv/Scripts/python.exe -m "
                "dynamic_crawler.formfill.promote %s", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
