"""SAMA's own "what changed" page, instead of probing 6,101 documents.

    https://rulebook.sama.gov.sa/en/view-revision-updates
      ?f_date=on&changed_1[min]=YYYY-MM-DD&changed_1[max]=YYYY-MM-DD&items_per_page=40

Plain GET, no auth, no browser. It filters on Drupal's `changed` timestamp, so it
answers "what did SAMA touch between these dates" directly.

WHY THIS EXISTS

A `stored-inventory` sweep of SAMA is ONE REQUEST PER DOCUMENT — 6,101 of them,
which is 94% of the whole library's monitoring traffic and takes the best part of
an hour. This is one request. Measured 2026-08-15: 22 entries for the seven and a
half months from 2026-01-01, so SAMA barely changes and the expensive sweep was
almost always confirming that nothing had happened.

items_per_page IS CAPPED AT 40, AND ASKING FOR MORE RETURNS ZERO

    10  -> 10 entries      50  -> 0
    20  -> 20 entries     100  -> 0
    40  -> 22 entries  (all of them in that window)

Measured 2026-08-15. A larger value does not error — the page answers 200 with an
empty result, so an over-large request reads as "nothing changed". That is the
trap this module exists to not fall into: PAGE_SIZE is fixed at 40 and windows
are walked with `&page=N` rather than by asking for more per page.

SLUGS HERE, NODE IDS IN THE LIBRARY

The feed links documents by slug (/en/update-account-opening-rules-...), while
the stored rows hold node urls (/en/node/11033). Matching the two directly finds
nothing — that is what invalidated the first test of this feed and left it unused
for months. The slug page is opened to read its canonical node url, and THAT is
matched against the stored inventory. It costs one request per CHANGED document,
which is the same request a re-crawl has to make anyway.

WHAT IT CANNOT DO

Deletions. A withdrawn document simply stops being listed, and no window will
ever mention it. `stored-inventory` remains SAMA's way of finding removals; run
it occasionally, not daily.
"""
from __future__ import annotations

import datetime as _dt
import logging
import re
from typing import List, Optional

import requests

from dynamic_crawler import fingerprint
from dynamic_crawler.changesignal import ChangeSignal, Observation, identity_key

logger = logging.getLogger(__name__)

FEED_URL = "https://rulebook.sama.gov.sa/en/view-revision-updates"
BASE = "https://rulebook.sama.gov.sa"

#: Fixed, not configurable. See the module docstring: above 40 the page returns
#: an EMPTY result rather than an error, so a "bigger" request silently reads as
#: "nothing changed".
PAGE_SIZE = 40

BASIS_FEED = "sama revision feed (the regulator's own changed-on date)"

#: One entry: "12 . <a href="/en/slug">Title</a> (30 June 2026)"
ENTRY_RE = re.compile(
    r'<div class="book-detail">\s*\d+\s*\.\s*<a href="([^"]+)"[^>]*>([^<]+)</a>'
    r'\s*\(([^)]+)\)')
#: The folder hierarchy, which the feed hands over for free — normally the
#: most expensive part of a rulebook crawl to reconstruct.
TRAIL_RE = re.compile(r'<div class="book-trail">([^<]*)</div>')
#: The canonical node url, read off the opened slug page.
NODE_RE = re.compile(r'/(?:en/)?node/(\d+)')


#: Transient network faults, retried. Measured 2026-08-15: a sweep that had
#: worked minutes earlier failed with `getaddrinfo failed` for a host that then
#: resolved 3/3 on retest — a DNS blip on this machine, not a change at SAMA.
#: Without a retry that reads as "SAMA monitoring is broken", and the whole point
#: of the feed is that it runs often and cheaply enough to be trusted.
_RETRIES = 3
_BACKOFF = 2.0


def _get(url: str, timeout: float, params=None) -> str:
    import time as _t
    last = None
    for attempt in range(_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=timeout,
                             headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            return r.text
        except (requests.ConnectionError, requests.Timeout) as e:
            # Only connection-level faults. An HTTP error is the regulator
            # answering, and retrying it would just ask the same question again.
            last = e
            if attempt < _RETRIES - 1:
                logger.warning("sama feed: %s (attempt %d/%d), retrying in %.0fs",
                               str(e)[:120], attempt + 1, _RETRIES, _BACKOFF)
                _t.sleep(_BACKOFF * (attempt + 1))
    raise last


def fetch_entries(since: str, until: str, *, timeout: float = 45.0,
                  max_pages: int = 25) -> List[dict]:
    """Every entry SAMA changed in [since, until], walking the pager.

    Stops on the first empty page. `max_pages` is a guard, not a limit anyone
    should hit: 40 x 25 is 1,000 changes in one window, and the measured rate is
    22 in seven months.
    """
    out, seen = [], set()
    for page in range(max_pages):
        params = {"f_date": "on", "changed_1[min]": since,
                  "changed_1[max]": until, "items_per_page": PAGE_SIZE}
        if page:
            params["page"] = page
        html = _get(FEED_URL, timeout, params)
        entries = ENTRY_RE.findall(html)
        trails = TRAIL_RE.findall(html)
        if not entries:
            break
        for i, (href, title, shown) in enumerate(entries):
            href = href.strip()
            if href in seen:
                continue
            seen.add(href)
            out.append({
                "slug_url": href if href.startswith("http") else BASE + href,
                "title": " ".join(title.split()),
                "date_shown": shown.strip(),
                # Same order as the entries on the page; absent on some rows, so
                # it is read positionally and defaulted rather than assumed.
                "book_trail": (trails[i].strip() if i < len(trails) else ""),
            })
        if len(entries) < PAGE_SIZE:
            break
    return out


def canonical_node_url(slug_url: str, *, timeout: float = 45.0) -> Optional[str]:
    """The /en/node/<id> url behind a slug, or None.

    The library stores node urls, the feed speaks slugs, and nothing matches
    until one is turned into the other. Read from the opened page rather than
    guessed — this is the step whose absence made the feed look broken.
    """
    try:
        html = _get(slug_url, timeout)
    except Exception as e:                       # noqa: BLE001 - reported, not raised
        logger.warning("sama feed: could not open %s: %s", slug_url, e)
        return None
    # A canonical link if the page offers one, else the first node reference.
    m = re.search(r'<link[^>]+rel="canonical"[^>]+href="([^"]+)"', html)
    if m and NODE_RE.search(m.group(1)):
        node = NODE_RE.search(m.group(1)).group(1)
        return f"{BASE}/en/node/{node}"
    m = re.search(r'"shortlink"[^>]+href="([^"]+)"', html) or NODE_RE.search(html)
    if m:
        node = NODE_RE.search(m.group(0) if m.re is NODE_RE else m.group(1))
        if node:
            return f"{BASE}/en/node/{node.group(1)}"
    return None


class SamaFeedSweep(ChangeSignal):
    """One request per sweep, plus one per CHANGED document.

    `tracked` is the urls the library already holds. An entry whose node url is
    in it is a MODIFIED document; one that is not is a NEW document the feed has
    discovered — which a stored-inventory probe structurally cannot do, since it
    only ever looks at rows we already have.
    """

    name = "sama-feed"

    def __init__(self, source: str, tracked, *, since: str, until: str,
                 timeout: float = 45.0, resolve_nodes: bool = True):
        self.source = source
        self.tracked = {self._norm(u) for u in (tracked or []) if u}
        self.since, self.until = since, until
        self.timeout = timeout
        self.resolve_nodes = resolve_nodes
        self.stats = {}

    @staticmethod
    def _norm(u: str) -> str:
        return str(u or "").split("?")[0].rstrip("/").lower()

    def sweep(self) -> List[Observation]:
        entries = fetch_entries(self.since, self.until, timeout=self.timeout)
        logger.info("sama feed: %d entr(ies) for %s..%s",
                    len(entries), self.since, self.until)
        obs, matched, unmatched, unresolved = [], 0, 0, 0
        for e in entries:
            node = (canonical_node_url(e["slug_url"], timeout=self.timeout)
                    if self.resolve_nodes else None)
            url = node or e["slug_url"]
            if node is None:
                unresolved += 1
            known = self._norm(url) in self.tracked
            matched += known
            unmatched += (not known)
            fields = {"document_url": url}
            obs.append(Observation(
                key=identity_key(fields), fields=fields,
                identity_fields=("document_url",),
                # The regulator's own changed-on date IS the version token: the
                # feed only lists a document when that date moved.
                token=e["date_shown"],
                basis=BASIS_FEED,
                url=url,
                title=e["title"][:120]))
        self.stats = {"entries": len(entries), "already_tracked": matched,
                      "not_in_library": unmatched,
                      "node_url_unresolved": unresolved,
                      "window": f"{self.since}..{self.until}"}
        return obs

    def confirm_required_for(self, obs: Observation) -> bool:
        # The feed lists a document BECAUSE the regulator changed it. There is
        # nothing to second-guess, unlike a counter that can move in bulk.
        return False

    def confirm(self, obs: Observation) -> Optional[str]:
        return None


def default_window(days: int = 30) -> tuple:
    """The last `days` days, as the feed wants them (YYYY-MM-DD)."""
    today = _dt.date.today()
    return (today - _dt.timedelta(days=days)).isoformat(), today.isoformat()
