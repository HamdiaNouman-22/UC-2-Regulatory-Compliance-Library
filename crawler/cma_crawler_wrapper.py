"""CMACrawler — the CMA site runner behind the standard crawler interface.

WHY A WRAPPER RATHER THAN A `mode: generic` SOURCE

`site_runners/cma_laws.py` is not a link walk. CMA publishes nine tabs in six
different page shapes — a chapter-structured law, single pages, card grids,
grouped cards, an iframe, sub-tabbed paginated lists with detail pages, and a
paginated FAQ — and the runner has a handler per shape. Pointing the generic
crawler at the landing page collects the easy ones and silently under-collects
the rest, which is worse than not running it: the completeness gate would take
that undercount as its baseline and never report the gap.

So the shapes stay where they are. This class only adapts the runner's output to
the interface `build_regulator_crawler(mode: custom)` expects, the same way
`crawler/sama_crawler_wrapper.py` does for SAMA.

WHAT IT DOES NOT DO

It does not re-implement, re-scope or "improve" any handler. If a tab is wrong,
it is wrong in `cma_laws.py` and should be fixed there.

Media Center announcements (3,297 over 550 pages) are excluded by default. That
tab is the one shape whose list is NOT held in the DOM, so it pages the site
hundreds of times; it is opt-in via `tabs=[...]` rather than something a routine
crawl pays for.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

from playwright.sync_api import sync_playwright

from models.models import RegulatoryDocument
from site_runners import cma_laws

logger = logging.getLogger(__name__)

#: Laws & Regulations only. Everything else CMA publishes is reachable by
#: naming it in `tabs`, but this is the set that matches the regulator scope.
DEFAULT_TABS = [
    "capital_market_law",
    "sifi",
    "forms",
    "cpe",
    "guides",
    "circulars",
    "public_consultation",
    "implementing_regulations",
    "faqs",
]

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


def _as_doc_list(tab_docs) -> List[dict]:
    """Normalise what `crawl_tab` hands back.

    The shape handlers return `list(documents.values())` — a list of dicts, each
    carrying doc_url / title / section_path / type / found_on. But two early
    error paths return the bare `documents` dict instead, and one returns `[]`.
    Assuming the dict form cost a 272-second CMA run that reported 0 documents
    and PASSED the completeness gate, because an exception per tab was caught
    and logged as "tab failed" rather than raised.
    """
    if not tab_docs:
        return []
    if isinstance(tab_docs, dict):
        return [v for v in tab_docs.values() if isinstance(v, dict)]
    return [d for d in tab_docs if isinstance(d, dict)]


class CMACrawler:
    """Runs the CMA site runner's implemented tabs and returns
    RegulatoryDocument objects."""

    def __init__(
        self,
        headless: bool = True,
        tabs: Optional[List[str]] = None,
        source_system: str = "CMA-RULES",
        regulator: str = "CMA",
        delay_ms: int = 600,
        max_articles: Optional[int] = None,
        max_chapters: Optional[int] = None,
    ):
        self.headless = headless
        self.regulator = regulator
        self.source_system = source_system
        # CMA throttles. The runner's own note: 600-1000ms finishes SOONER than
        # 0, because backing off avoids the throttle it would otherwise trip.
        self.delay_ms = delay_ms
        self.max_articles = max_articles
        self.max_chapters = max_chapters

        requested = tabs or DEFAULT_TABS
        unknown = [t for t in requested if t not in cma_laws.TABS]
        if unknown:
            raise ValueError(
                f"unknown CMA tab(s) {unknown}. Known: {sorted(cma_laws.TABS)}")
        # A tab whose shape has no handler yet would crawl nothing and report
        # success, so drop it loudly instead.
        self.tabs = []
        for t in requested:
            shape = cma_laws.TABS[t].get("shape")
            if shape in cma_laws.IMPLEMENTED:
                self.tabs.append(t)
            else:
                logger.warning("CMA tab %r has shape %r with no handler — skipped",
                               t, shape)

        self.last_result: Dict = {}
        logger.info("Initialized CMACrawler (headless=%s, tabs=%d)",
                    headless, len(self.tabs))

    # ------------------------------------------------------------------ #

    @property
    def source_names(self) -> List[str]:
        """One label per tab, so the completeness gate can size each tab
        against its own history rather than against CMA as a whole. Without
        this a tab dying entirely hides inside the total's tolerance."""
        return [cma_laws.TABS[t]["label"] for t in self.tabs]

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        """Walk every implemented tab and return the documents found.

        `limit` accepts an int (documents overall) or a dict keyed by tab, to
        match the loose convention the other crawlers use.
        """
        overall_cap = limit if isinstance(limit, int) and limit > 0 else None
        per_tab = limit if isinstance(limit, dict) else {}

        docs: List[RegulatoryDocument] = []
        seen = set()
        per_tab_counts: Dict[str, int] = {}
        warnings: List[str] = []
        failed_tabs: List[str] = []

        cma_laws.PACE["detail_ms"] = self.delay_ms
        cma_laws.PACE["page_ms"] = self.delay_ms // 2

        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=self.headless,
                args=["--disable-dev-shm-usage", "--disable-gpu"])
            ctx = browser.new_context(user_agent=USER_AGENT, locale="en-US",
                                      viewport={"width": 1600, "height": 1000})
            try:
                for tab_key in self.tabs:
                    label = cma_laws.TABS[tab_key]["label"]
                    try:
                        _records, tab_docs = cma_laws.crawl_tab(
                            ctx, tab_key,
                            per_tab.get(tab_key) or self.max_chapters,
                            per_tab.get(tab_key) or self.max_articles)
                    except Exception as e:
                        # One tab failing must not lose the other eight. It is
                        # recorded so the gate sees an incomplete run rather
                        # than a short one.
                        msg = f"CMA tab {label!r} failed: {type(e).__name__}: {e}"
                        logger.error("  %-44s FAILED  %s", label,
                                     f"{type(e).__name__}: {e}"[:70])
                        warnings.append(msg)
                        failed_tabs.append(label)
                        per_tab_counts[label] = 0
                        continue

                    before = len(docs)
                    for d in _as_doc_list(tab_docs):
                        href = (d.get("doc_url") or "").strip()
                        if not href or href in seen:
                            continue
                        seen.add(href)
                        docs.append(self._to_document(
                            d, href, d.get("section_path") or "", label))
                        if overall_cap and len(docs) >= overall_cap:
                            break
                    per_tab_counts[label] = len(docs) - before
                    logger.info("  %-44s %4d document(s)   (running total %d)",
                                label, per_tab_counts[label], len(docs))
                    if overall_cap and len(docs) >= overall_cap:
                        break
            finally:
                try:
                    browser.close()
                except Exception:
                    pass

        # The shape the completeness gate reads.
        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings},
            "by_source": per_tab_counts,
            "failed_tabs": failed_tabs,
        }

        # EVERY tab failing is a broken crawler, not a regulator with no
        # documents — but both return an empty list, and the completeness gate
        # cannot tell them apart. It would take the zero as a valid baseline.
        #
        # This is not hypothetical: a wrong assumption about crawl_tab's return
        # type made all nine tabs raise, and the run reported 0 documents and
        # gate=PASS after 272 seconds. cma_laws.py warns about exactly this:
        # "a tab that quietly returns nothing looks exactly like a tab with no
        # documents, and that is the failure mode this whole project keeps
        # tripping over."
        if failed_tabs and len(failed_tabs) == len(self.tabs):
            raise RuntimeError(
                f"every CMA tab failed ({len(failed_tabs)}/{len(self.tabs)}). "
                f"This is a crawler fault, not an empty regulator. "
                f"First error: {warnings[0] if warnings else 'unknown'}")

        logger.info("CMACrawler finished: %d document(s) across %d tab(s)%s",
                    len(docs), len(per_tab_counts),
                    f", {len(failed_tabs)} tab(s) FAILED" if failed_tabs else "")
        return docs

    # ------------------------------------------------------------------ #

    def _to_document(self, d: dict, href: str, section_path: str,
                     tab_label: str) -> RegulatoryDocument:
        """One runner document -> one RegulatoryDocument.

        `section_path` is the runner's own trail, already rooted at the tab, so
        it becomes doc_path directly. The orchestrator builds the folder tree
        from that list.
        """
        trail = [p.strip() for p in (section_path or "").split(">") if p.strip()]
        if not trail:
            trail = [tab_label]

        return RegulatoryDocument(
            regulator=self.regulator,
            source_system=self.source_system,
            category=trail[0] if trail else tab_label,
            title=(d.get("title") or "").strip() or href.rsplit("/", 1)[-1],
            document_url=href,
            doc_path=trail,
            published_date=d.get("published_date"),
            reference_no=d.get("reference_no"),
            extra_meta={
                "crawl_source": tab_label,
                "found_on": d.get("found_on", ""),
                "doc_type": d.get("type", ""),
            },
        )
