"""One place that decides what a document's `content_hash` is.

WHY THIS EXISTS
---------------
`content_hash` is the whole of change detection: the classifier compares the
stored hash with the freshly crawled one and calls the document `modified` when
they differ. What it actually does when a hash is MISSING is the part that bit
us — `if old_hash and new_hash and old_hash == new_hash` treats an absent hash
as "cannot match", which is `modified`.

The generic crawler stamps a hash on every row it produces
(`generic_crawler/crawler.py`, at the end of the walk). Hand-written crawlers
never did. On 2026-08-16 that accounted for 8,151 of 8,714 stored rows having no
fingerprint at all:

    SAMA RULEBOOK                   6,105
    CMA-RULES                       1,979
    Regulations and Laws               48
    Exchange Rules And Procedures      19

MOH was the source that happened to get re-run twice, so MOH is where it
surfaced: all 83 documents classified `modified` on every run, two version rows
written each time, and ten documents reached five versions of identical content.
The other sources are only dormant, not exempt — they are monitored by their own
signals today, and would do the same the first time they run through the
direct-write path.

WHAT TO HASH
------------
Preference order, and the reason for it:

1. The page's visible TEXT, when we have the page. HTML churns on every CMS
   deploy — build ids, cache-busting query strings, rotating widget markup —
   and hashing it reports every page as modified. This is the same choice the
   generic crawler documents at crawler.py:1901.

2. `document_url | title`, when the document is a FILE we have not downloaded.
   Weak but honest: it cannot move when a PDF is replaced behind an unchanged
   link. Where the site publishes its own last-changed stamp, hash that instead
   and say so at the call site — MOHCrawler hashes SharePoint's `Modified`,
   which does move on replacement.

Never invent a hash from something that varies per run (a fetch timestamp, a
session id, a row position). A hash that changes on its own is worse than no
hash: it produces a permanent stream of false `modified`, and every one of them
costs a version row and an LLM re-analysis.
"""

from __future__ import annotations

import logging
import re
from typing import Iterable, List

from generic_crawler.crawler import content_key

logger = logging.getLogger(__name__)

__all__ = ["text_of_html", "hash_for", "stamp_content_hashes"]

_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")
# Script and style bodies are not visible text, and they are exactly the parts a
# CMS rewrites between deploys. Stripped before the tags so their CONTENT goes
# too, not just their angle brackets.
_NOISE = re.compile(r"(?is)<(script|style|noscript)\b.*?</\1\s*>")


def text_of_html(html: str) -> str:
    """Visible text of an HTML fragment, whitespace-normalised.

    Deliberately regex-based rather than BeautifulSoup: this runs once per
    document on crawls of several thousand pages, and the result is fed to a
    hash, so tag-soup edge cases cost nothing a parser would save.
    """
    if not html:
        return ""
    return _WS.sub(" ", _TAG.sub(" ", _NOISE.sub(" ", html))).strip()


def hash_for(doc) -> str:
    """The fingerprint for one document. See the module docstring for the order."""
    html = getattr(doc, "document_html", "") or ""
    text = text_of_html(html)
    if text:
        return content_key(text)
    url = (getattr(doc, "document_url", "") or "").strip()
    title = (getattr(doc, "title", "") or "").strip()
    return content_key(f"{url}|{title}") if (url or title) else ""


def stamp_content_hashes(docs: Iterable) -> List:
    """Fill in `content_hash` on every document that lacks one, and return them.

    Call at a crawler's single public exit, the way the CMA wrapper applies
    `_scrub_urls`. Stamping at the exit rather than at each construction site is
    what keeps this correct when someone adds a fifth `RegulatoryDocument(...)`
    branch and forgets the hash — which is how the gap appeared in the first
    place.

    An EXISTING hash is never overwritten: a crawler that knows a better answer
    than "hash the text" (MOH's `Modified` stamp) has already set it, and this
    must not undo that.
    """
    out = list(docs)
    for doc in out:
        if not (getattr(doc, "content_hash", "") or ""):
            try:
                doc.content_hash = hash_for(doc)
            except Exception as e:
                # A document with no fingerprint still belongs in the library;
                # it classifies as `modified` until someone fixes its crawler,
                # which is noisy but not lossy. Losing the whole crawl over it
                # would be worse. Note this cannot retry the assignment — the
                # likely cause IS the assignment (a __slots__ or frozen record
                # type, as sama_circulars_crawler's local RegulatoryDocument
                # nearly was), so a second attempt raises the same error.
                logger.warning("no content_hash for %r: %s",
                               str(getattr(doc, "title", ""))[:60], e)
    return out
