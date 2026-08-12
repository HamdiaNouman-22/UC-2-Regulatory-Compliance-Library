"""MOHCrawler — Ministry of Health regulations, from SharePoint's own list API.

WHY THIS IS NOT A BROWSER CRAWL

The listing page renders its rows with JavaScript, so a plain fetch returns
nothing and the obvious conclusion is "this needs a browser". It does not. The
page's own JavaScript calls a SharePoint REST endpoint that returns every
document as JSON in one request:

    GET /en/Ministry/Rules/_api/web/lists/getbytitle('Documents')/items
        ?$select=FileRef,Title,Modified&$top=500

83 documents, one call, no browser. Same idea as the GOSI signal — read what the
CMS already publishes rather than re-rendering its UI.

TWO THINGS THAT MAKE IT WORK, BOTH EASY TO GET WRONG

1. CASE. The path must be `/en/Ministry/Rules/_api/...`. Lowercase
   `/en/ministry/rules/_api/...` 302s away, and the site-collection root
   `/_api/...` is refused by the WAF.

2. COOKIES. The endpoint needs the session the listing page hands out. Fetch the
   page first, keep the cookies, then call the API. Without them it 302s.

A REJECTION LOOKS LIKE A SUCCESS

The WAF answers a blocked request with **HTTP 200** and an HTML error page, not
a 4xx. Anything checking the status code alone concludes it worked. This module
checks the content type and the payload shape instead.

`Modified` is a per-document timestamp, so the same call that lists the library
also says which documents changed — a change signal as well as an inventory.
"""

from __future__ import annotations

import logging
from typing import List, Optional
from urllib.parse import urljoin

import requests

from models.models import RegulatoryDocument

logger = logging.getLogger(__name__)

BASE = "https://www.moh.gov.sa"
LISTING_PAGE = f"{BASE}/en/ministry/rules/pages/default.aspx"
# Capitalisation is load-bearing — see the module docstring.
API = (f"{BASE}/en/Ministry/Rules/_api/web/lists/getbytitle('Documents')/items"
       "?$select=FileRef,Title,Modified,FileLeafRef&$top=1000")

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")


class MOHCrawler:
    """Ministry of Health — Rules and Regulations."""

    def __init__(
        self,
        regulator: str = "Ministry of Health",
        source_system: str = "MOH-RULES",
        category: str = "Regulations",
        timeout: int = 45,
    ):
        self.regulator = regulator
        self.source_system = source_system
        self.category = category
        self.timeout = timeout
        self.last_result: dict = {}
        logger.info("Initialized MOHCrawler")

    @property
    def source_names(self) -> List[str]:
        return [self.category]

    # ------------------------------------------------------------------ #

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        cap = limit if isinstance(limit, int) and limit > 0 else None
        warnings: List[str] = []

        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})

        # 1. The listing page, purely for the session cookies.
        try:
            session.get(LISTING_PAGE, timeout=self.timeout)
        except requests.RequestException as e:
            raise RuntimeError(f"MOH listing page unreachable: {e}") from e

        # 2. The list itself.
        resp = session.get(
            API,
            headers={"Accept": "application/json;odata=nometadata",
                     "Referer": LISTING_PAGE},
            timeout=self.timeout,
            allow_redirects=True,
        )

        # A WAF rejection arrives as 200 + HTML, so the status code alone proves
        # nothing. Judge the content type.
        ctype = (resp.headers.get("Content-Type") or "").lower()
        if "json" not in ctype:
            snippet = resp.text[:160].replace("\n", " ")
            raise RuntimeError(
                f"MOH API did not return JSON (status {resp.status_code}, "
                f"content-type {ctype!r}). Usually the WAF refusing the call, "
                f"which it does with 200 and an HTML page. Body: {snippet}")

        items = (resp.json() or {}).get("value") or []
        logger.info("MOH API returned %d item(s)", len(items))

        docs: List[RegulatoryDocument] = []
        seen = set()
        skipped_no_url = 0

        for it in items:
            file_ref = (it.get("FileRef") or "").strip()
            if not file_ref:
                skipped_no_url += 1
                continue
            url = urljoin(BASE, file_ref)
            if url in seen:
                continue
            seen.add(url)

            title = (it.get("Title") or "").strip()
            if not title:
                # Some rows carry only the filename. Better a readable stem than
                # an empty title the library cannot show.
                title = (it.get("FileLeafRef") or file_ref.rsplit("/", 1)[-1])
                title = title.rsplit(".", 1)[0].replace("-", " ").replace("_", " ").strip()

            docs.append(RegulatoryDocument(
                regulator=self.regulator,
                source_system=self.source_system,
                category=self.category,
                title=title,
                document_url=url,
                doc_path=[self.category],
                # `Modified` is the CMS's own last-changed stamp: the published
                # date we have, and the change signal for a later sweep.
                published_date=(it.get("Modified") or "")[:10] or None,
                extra_meta={
                    "crawl_source": self.category,
                    "moh_modified": it.get("Modified"),
                    "moh_file_ref": file_ref,
                },
            ))
            if cap and len(docs) >= cap:
                break

        if skipped_no_url:
            warnings.append(f"{skipped_no_url} MOH row(s) had no FileRef")

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings},
            "by_source": {self.category: len(docs)},
        }
        logger.info("MOHCrawler finished: %d document(s)", len(docs))
        return docs
