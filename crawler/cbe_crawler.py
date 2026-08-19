"""CBECircularsCrawler — Central Bank of Egypt circulars, from the site's own listing API.

WHY THIS IS NOT A BROWSER CRAWL
-------------------------------
The circulars page renders ten rows and a "Load more" button, so a crawl walks
into a 40-page JavaScript pager. Measured on 2026-08-18, a prefix crawl of
`/en/laws-regulations` recorded **18 of 396 circulars — 4.5%**, and reported
`status: ok` while doing it. Nothing about that run looked like a failure.

The page's own JavaScript calls an endpoint that returns the whole list as JSON:

    GET /api/listing/circulars?pageNo=0&pageSize=500
    -> 396 results, 274 KB, one request, no browser

Same idea as MOHCrawler and the GOSI signal: read what the CMS already
publishes rather than re-rendering its UI.

WHAT THE API GIVES THAT A LINK WALK CANNOT
------------------------------------------
    customDate   ISO 8601 publication date. `generic_crawler_wrapper` documents
                 that it must leave published_date None because "a link walk
                 cannot reliably read issue dates". Here the publisher states it.
    categories   the regulator's own taxonomy - Credit Granting (69), Banking
                 Practices (34), Prudential Regulations (28), ...
    itemId       a Sitecore GUID, measured unique across all 396 records. A
                 stronger identity than url|title: it survives a retitle and a
                 file move.
    title        the full title, so nothing has to be derived from a slug. One
                 circular is served as `circul~1.pdf`, an 8.3 short name that
                 would otherwise produce a garbage title.

TWO THINGS THAT LOOK LIKE SUCCESS AND ARE NOT
---------------------------------------------
1. A REJECTION ARRIVES AS HTTP 200 + HTML. cbe.org.eg answers a request it
   dislikes with a 269-byte "Request Rejected" page under a 200 status. Checking
   the status code alone concludes it worked. This module judges the content
   type and the payload shape, exactly as MOHCrawler has to.

2. `urllib` IS REJECTED, `requests` IS NOT. The WAF reads the header signature,
   not the client's honesty about being a browser. Measured: identical
   User-Agent, urllib gets the rejection page, requests gets the file.

   Related: **HEAD is refused with 403 while GET returns 200.** Nothing here
   sends a HEAD; the note is for whoever writes the sweep.

WHAT THIS DELIBERATELY DOES NOT COVER
-------------------------------------
`/api/listing/circulars` and `/api/listing/news` exist; `laws`, `regulations`,
`tenders` and `auctions` all 404. So the laws, the regulations-book chapters and
the other sections still need the generic crawl - CBE is a split source, not a
replaced one. News is deliberately out of the library.

THE OTHER HALF OF CBE, for whoever reads this next. Those sections are crawled by
`generic_crawler/crawler.py` with `--subpaths` (one prefix-scoped run per section,
SECTION F there), and the Procurement PDF - which is reachable only from the site
navigation, so the header/footer rule correctly files it under `chrome_dropped` -
is declared with `--documents`:

    venv/Scripts/python.exe generic_crawler/crawler.py
        --seed https://www.cbe.org.eg/en/
        --subpaths governance,laws-regulations,aml-cft,...
        --out output/cbe_test_1 --scope prefix --max-pages 150
        --documents "About CBE :: Procurement :: https://.../procurement.pdf"

See docs/changes_cboe.md for the measured output of that command. This file and
that command do not overlap: the API owns the circulars, the crawl owns the pages.

THE API IS UNDOCUMENTED
-----------------------
It can change or vanish without notice. That is not a reason to take 4.5%
instead, but it is a reason to fail LOUDLY: a shape change must raise, never
return a short list that looks like a small month.
"""

from __future__ import annotations

import logging
from typing import List, Optional
from urllib.parse import urljoin

import requests

from models.models import RegulatoryDocument
from crawler.fingerprint import stamp_content_hashes
from dynamic_crawler.formfill.runner import _ext_type, _is_doc
from generic_crawler.crawler import content_key

logger = logging.getLogger(__name__)

BASE = "https://www.cbe.org.eg"
LISTING_PAGE = f"{BASE}/en/laws-regulations/regulations/circulars"
API = f"{BASE}/api/listing/circulars"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

#: One page of the API. 500 returned all 396 in a single call; paging is kept
#: anyway so the crawler does not silently truncate the day CBE passes the cap.
PAGE_SIZE = 200

#: Refuse to believe an implausibly empty answer. A crawl that finds nothing is
#: a failed crawl, not an empty regulator (tools/workbook.py says the same).
MIN_EXPECTED = 1


class CBECircularsCrawler:
    """Central Bank of Egypt — Circulars."""

    def __init__(
        self,
        regulator: str = "Central Bank of Egypt (CBE)",
        source_system: str = "Circulars",
        timeout: int = 45,
    ):
        # "Full Name (ACRONYM)" is the library's naming rule. Config lookups match
        # on this string and fall back to defaults SILENTLY when it does not
        # match, so a near-miss is not an error, it is a wrong answer.
        self.regulator = regulator
        self.source_system = source_system
        self.timeout = timeout
        self.last_result: dict = {}

    # ------------------------------------------------------------------ #
    #  the API                                                            #
    # ------------------------------------------------------------------ #

    def _fetch_page(self, session: requests.Session, page_no: int) -> dict:
        resp = session.get(
            API,
            params={"pageNo": page_no, "pageSize": PAGE_SIZE},
            headers={"Accept": "application/json, text/plain, */*",
                     "Referer": LISTING_PAGE},
            timeout=self.timeout,
            allow_redirects=True,
        )
        ctype = (resp.headers.get("Content-Type") or "").lower()
        if "json" not in ctype:
            snippet = resp.text[:160].replace("\n", " ")
            raise RuntimeError(
                f"CBE circulars API did not return JSON (status "
                f"{resp.status_code}, content-type {ctype!r}). Usually the WAF "
                f"refusing the call, which it does with 200 and an HTML page. "
                f"Body: {snippet}")
        payload = resp.json() or {}
        if "results" not in payload:
            raise RuntimeError(
                f"CBE circulars API returned JSON without a `results` key "
                f"(keys: {sorted(payload)[:8]}). The endpoint's shape changed; "
                f"do not treat this as an empty month.")
        return payload

    # ------------------------------------------------------------------ #
    #  the contract: docs = crawler.fetch_documents()                     #
    # ------------------------------------------------------------------ #

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        cap = limit if isinstance(limit, int) and limit > 0 else None
        warnings: List[str] = []

        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})

        # The listing page first, for cookies and an honest Referer. MOH needs
        # this to avoid a 302; CBE has not been seen to, and it costs one request
        # to not find out the hard way on a site that answers refusals with 200.
        try:
            session.get(LISTING_PAGE, timeout=self.timeout)
        except requests.RequestException as e:
            logger.warning("CBE listing page unreachable (%s) - calling the API "
                           "anyway", e)

        items, total, page_no = [], None, 0
        while True:
            payload = self._fetch_page(session, page_no)
            batch = payload.get("results") or []
            if total is None:
                total = payload.get("totalResultsCount")
            items.extend(batch)
            if not batch or total is None or len(items) >= total:
                break
            page_no += 1
            if page_no > 200:                      # runaway guard, never reached
                warnings.append("stopped paging at 200 pages")
                break

        logger.info("CBE circulars API returned %d of %s item(s)",
                    len(items), total)

        # A SHORT ANSWER IS A FINDING, NOT A RESULT. The whole reason this file
        # exists is that a run which quietly returned 18 of 396 reported success.
        if total is not None and len(items) < total:
            raise RuntimeError(
                f"CBE circulars API listed {total} circulars but only "
                f"{len(items)} were retrieved. Refusing to return a partial "
                f"inventory - a short list here reads downstream as documents "
                f"having disappeared.")
        if len(items) < MIN_EXPECTED:
            raise RuntimeError(
                "CBE circulars API returned no circulars. That is a failed "
                "read, not an empty regulator.")

        docs: List[RegulatoryDocument] = []
        seen, skipped_no_url = set(), 0

        for it in items:
            raw_url = (it.get("url") or "").strip()
            if not raw_url:
                skipped_no_url += 1
                continue
            url = urljoin(BASE, raw_url)
            if url in seen:
                continue
            seen.add(url)

            title = (it.get("title") or "").strip()
            if not title:
                title = raw_url.rsplit("/", 1)[-1].rsplit(".", 1)[0]
                title = title.replace("-", " ").replace("_", " ").strip()

            cats = [c.get("value") for c in (it.get("categories") or [])
                    if c.get("value")]
            item_id = (it.get("itemId") or "").strip()
            # "2026-08-17T00:00:00" -> "2026-08-17"
            published = (it.get("customDate") or "")[:10] or None

            docs.append(RegulatoryDocument(
                regulator=self.regulator,
                source_system=self.source_system,
                # The regulator's own taxonomy. Safe to carry: the default
                # identity is (document_url, doc_path, title), so a category CBE
                # re-files does not change which row this is.
                category=cats[0] if cats else self.source_system,
                title=title,
                document_url=url,
                # FLAT, deliberately: regulator > source_system > title, as MOH
                # and every other source. Putting the API category in the path
                # would make the folder - and therefore `disappeared` scoping -
                # move whenever CBE re-files a circular. The taxonomy is kept in
                # `category` and extra_meta, where it costs nothing.
                doc_path=[self.regulator, self.source_system, title],
                file_type=_ext_type(url) if _is_doc(url) else "HTML",
                # The publisher states this. No parsing, no guess.
                published_date=published,
                source_page_url=LISTING_PAGE,
                # Hashed from the API RECORD, not from url|title.
                #
                # itemId is the publisher's identity and never moves; the rest is
                # what CBE would have to change for the circular to be a
                # different document. This catches a retitle, a re-date and a
                # replaced file under a new name.
                #
                # It CANNOT catch a PDF replaced silently behind an unchanged
                # url - nothing in the JSON moves for that. That case belongs to
                # the sweep, which reads the file's own ETag (measured present
                # and stable on /-/media/ files). Splitting it this way keeps
                # this crawler at one request instead of 396.
                content_hash=content_key(
                    f"{item_id}|{it.get('customDate') or ''}|{url}|{title}"),
                extra_meta={
                    "crawl_source": self.source_system,
                    "cbe_item_id": item_id,
                    "cbe_item_path": it.get("itemPath"),
                    "cbe_categories": " | ".join(cats),
                    "cbe_date": it.get("date"),
                },
            ))
            if cap and len(docs) >= cap:
                break

        if skipped_no_url:
            warnings.append(f"{skipped_no_url} CBE circular(s) had no url")

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings},
            "by_source": {self.source_system: len(docs)},
        }
        logger.info("CBECircularsCrawler finished: %d document(s)", len(docs))

        # The single exit. Every hash above is already set and stamp_ never
        # overwrites one; this is the backstop for a branch added later that
        # forgets - which is exactly how the gap appeared the first time.
        return stamp_content_hashes(docs)
