"""A sitemap's per-url `lastmod`, for the one regulator whose timestamps are real.

One request covers a whole source, which makes this the cheapest signal there is
— and the easiest to adopt wrongly. Four of the five sitemaps measured carry a
`lastmod` on every url and a single distinct value across all of them: the build
time of the sitemap, which cannot shortlist anything. A fifth carries 500
genuine timestamps for 500 newsroom pages and none for the regulations. So the
adapter asks two questions before it will run at all, and both are counted over
the documents we actually track rather than over the sitemap.

The date only shortlists. A bulk CMS operation moves it — 26 of the 62 documents
tracked here still share one timestamp — so a content hash decides.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from typing import List, Optional
from urllib.parse import unquote

import requests

from dynamic_crawler import fingerprint
from dynamic_crawler.changesignal import ChangeSignal, Observation, identity_key

logger = logging.getLogger(__name__)

BASIS_LASTMOD = "sitemap lastmod (moves on any edit — confirm with a hash)"

#: Everything from this marker on is an eight-card "related regulations" view
#: holding 68-92% of the page's text, so a whole-page hash moves whenever the
#: ministry publishes anything at all. Named here rather than hard-cut so the
#: next site on the same CMS can declare its own.
DEFAULT_CUT = "block-views-block-regulations-and-procedures-related"

#: Question two: the share of the documents we track that the sitemap contains.
#: A newsroom sitemap scores near zero here and passes every other check.
MIN_OVERLAP = 0.8

#: Question one: distinct timestamps over the documents we track. Measured 0.60
#: where the signal is real and 0.004-0.009 on the three sitemaps that only
#: carry a build time, so anywhere in the middle separates them.
MIN_DISTINCT_RATIO = 0.2

_SKIP = ("script", "style", "noscript")
_WS = re.compile(r"\s+")


# --------------------------------------------------------------------------- #
#  reading the sitemap                                                         #
# --------------------------------------------------------------------------- #

def normalise(url) -> str:
    """Percent-decoded, without a trailing slash.

    The library stores these hrefs percent-encoded and the sitemap publishes the
    same paths as literal Arabic; compared raw, nothing matches anything.
    """
    return unquote(str(url or "").strip()).rstrip("/")


def fetch_sitemap(url: str, session=None, timeout: float = 30.0) -> str:
    """The sitemap as text, decoded `utf-8-sig`.

    Two of the measured sitemaps open with a BOM, which survives into the first
    tag and defeats any check that the body starts with `<`.
    """
    getter = session or requests
    r = getter.get(url, timeout=timeout,
                   headers={"User-Agent": fingerprint.USER_AGENT,
                            "Accept": "application/xml,text/xml"})
    r.raise_for_status()
    return r.content.decode("utf-8-sig", errors="replace")


def parse_sitemap(xml: str) -> List[tuple]:
    """`[(loc, lastmod)]` from a `<urlset>`.

    An index raises rather than returning nothing: read as an empty urlset it
    would fail the guards below for having no overlap, which points at the
    source instead of at the url that was asked for.
    """
    root = ET.fromstring((xml or "").strip())
    tag = root.tag.rsplit("}", 1)[-1]
    if tag == "sitemapindex":
        children = [(c.text or "").strip() for c in root.iter()
                    if c.tag.rsplit("}", 1)[-1] == "loc"]
        raise ValueError(f"this is a sitemapindex of {len(children)} sitemaps, "
                         f"not a urlset — point at one of them, e.g. "
                         f"{children[0] if children else '?'}")
    if tag != "urlset":
        raise ValueError(f"expected a <urlset>, got <{tag}>")

    out = []
    for node in root:
        loc, lastmod = "", ""
        for child in node:
            name = child.tag.rsplit("}", 1)[-1]
            if name == "loc":
                loc = (child.text or "").strip()
            elif name == "lastmod":
                lastmod = (child.text or "").strip()
        if loc:
            out.append((loc, lastmod))
    return out


# --------------------------------------------------------------------------- #
#  the two guards                                                              #
# --------------------------------------------------------------------------- #

def assess(entries, stored_urls, *, min_overlap: float = MIN_OVERLAP,
           min_distinct: float = MIN_DISTINCT_RATIO) -> dict:
    """Is this sitemap a signal for THESE documents? Both questions, one count.

    Counted over the tracked subset and never over the sitemap: one measured
    page holds 1000 urls of which 62 are ours and 228 are news, and a sitemap
    can equally have good site-wide hygiene and a frozen stamp on exactly our
    subset. The subset is the only set whose timestamps we would ever act on.
    """
    by_url = {normalise(loc): stamp for loc, stamp in entries or ()}
    tracked = [u for u in (normalise(u) for u in stored_urls or ()) if u]
    matched = [u for u in tracked if u in by_url]
    stamps = [by_url[u] for u in matched if by_url[u]]

    overlap = len(matched) / len(tracked) if tracked else 0.0
    ratio = len(set(stamps)) / len(stamps) if stamps else 0.0

    why = []
    if not tracked:
        why.append("no stored urls to measure against — the guard cannot run")
    elif overlap < min_overlap:
        why.append(f"the sitemap holds {len(matched)} of the {len(tracked)} "
                   f"documents we track ({overlap:.0%}, needs {min_overlap:.0%}) "
                   f"— it is not a sitemap of this source")
    if tracked and not stamps:
        why.append("not one tracked url carries a lastmod")
    elif ratio < min_distinct:
        why.append(f"{len(set(stamps))} distinct lastmod over {len(stamps)} "
                   f"documents ({ratio:.1%}, needs {min_distinct:.0%}) — this is "
                   f"a build time, not an edit history")

    return {"urls_in_sitemap": len(by_url), "tracked": len(tracked),
            "matched": len(matched), "overlap": round(overlap, 4),
            "with_lastmod": len(stamps), "distinct_lastmod": len(set(stamps)),
            "distinct_ratio": round(ratio, 4),
            "usable": not why, "why_not": why}


def candidate_prefix(tracked) -> str:
    """The path every stored url shares, up to the last `/`.

    Derived rather than configured so that spotting an addition needs no second
    setting per site — and so it cannot drift away from what we actually store.
    """
    urls = [u for u in tracked if u]
    if len(urls) < 2:
        return ""
    common = os.path.commonprefix(urls)
    return common[:common.rfind("/") + 1] if "/" in common else ""


# --------------------------------------------------------------------------- #
#  the confirm hash                                                            #
# --------------------------------------------------------------------------- #

class _Text(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts: List[str] = []
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in _SKIP:
            self._skip += 1

    def handle_endtag(self, tag):
        if tag in _SKIP:
            self._skip = max(0, self._skip - 1)

    def handle_data(self, data):
        if not self._skip:
            self.parts.append(data)


def page_text(html: str, cut_marker: str = DEFAULT_CUT) -> tuple:
    """`(text, was_cut)` — the page's own text, with the trailing view removed.

    Text and not markup, for the same reason a snapshot is hashed per article:
    the volatile part of the page sits right next to the stable one.
    """
    body = str(html or "")
    at = body.find(cut_marker) if cut_marker else -1
    if at >= 0:
        # Back to the start of the tag that carries the marker, so its own
        # attributes do not land in the hash.
        body = body[:body.rfind("<", 0, at)]
    parser = _Text()
    parser.feed(body)
    parser.close()
    return _WS.sub(" ", "".join(parser.parts)).strip(), at >= 0


def page_hash(html: str, cut_marker: str = DEFAULT_CUT) -> tuple:
    text, was_cut = page_text(html, cut_marker)
    return hashlib.sha256(text.encode("utf-8")).hexdigest(), was_cut


# --------------------------------------------------------------------------- #
#  the sweep                                                                   #
# --------------------------------------------------------------------------- #

class SitemapLastmodSweep(ChangeSignal):
    """One request, one observation per document we track and the sitemap holds.

    Documents we track that the sitemap does NOT hold are reported in the stats
    and nowhere else — see `covers_inventory`.
    """

    # Absence from the sitemap is not absence from the regulator, measured: one
    # tracked document is published only at the Arabic url and is missing from
    # the English sitemap while the regulation is live. Reporting absences here
    # would hand D7 a two-run miss streak on a document nobody withdrew.
    covers_inventory = False
    # The date moves on any edit, including a bulk CMS operation that moved 26
    # of 62 at once. Only the hash separates those.
    confirm_required = True

    def __init__(self, sitemap_url: str, source: str, stored_urls, *,
                 cut_marker: str = DEFAULT_CUT, timeout: float = 30.0,
                 fetch_xml=None, session=None,
                 min_overlap: float = MIN_OVERLAP,
                 min_distinct: float = MIN_DISTINCT_RATIO):
        self.sitemap_url = str(sitemap_url)
        self.source = str(source)
        self.stored_urls = [str(u) for u in (stored_urls or ())]
        self.cut_marker = cut_marker
        self.timeout = float(timeout)
        self.session = session
        self._fetch = fetch_xml or (
            lambda: fetch_sitemap(self.sitemap_url, session, self.timeout))
        self.min_overlap = float(min_overlap)
        self.min_distinct = float(min_distinct)
        self.stats: dict = {}

    @property
    def name(self) -> str:
        return f"sitemap-lastmod:{self.source}"

    def sweep(self) -> List[Observation]:
        entries = parse_sitemap(self._fetch())
        guard = assess(entries, self.stored_urls, min_overlap=self.min_overlap,
                       min_distinct=self.min_distinct)
        if not guard["usable"]:
            # Refused in the adapter rather than in a runbook: a sitemap that
            # answers the wrong question answers it every day, silently.
            raise ValueError(f"{self.source}: this sitemap is not a change "
                             f"signal for this source — " + "; ".join(guard["why_not"]))

        by_url = {normalise(loc): stamp for loc, stamp in entries}
        tracked = [normalise(u) for u in self.stored_urls if str(u).strip()]
        prefix = candidate_prefix(tracked)

        observations, absent = [], []
        for url in dict.fromkeys(tracked):
            if url not in by_url:
                # Kept out of the observations AND out of the store: this sweep
                # cannot tell a withdrawal from a url that moved.
                absent.append(url)
                continue
            stamp = by_url[url]
            fields = {"page": self.source, "document_url": url}
            observations.append(Observation(
                key=identity_key(fields), fields=fields,
                identity_fields=("page", "document_url"),
                token=stamp,
                basis=BASIS_LASTMOD if stamp else fingerprint.BASIS_NONE,
                url=url, title=url.rsplit("/", 1)[-1][:120]))

        unstored = sorted(loc for loc in by_url
                          if prefix and loc.startswith(prefix)
                          and loc not in set(tracked))
        self.stats = {
            "sitemap": self.sitemap_url,
            "guard": guard,
            "observed": len(observations),
            "not_in_sitemap": absent,
            "unstored_locs": unstored,
            "confirm_uncut": 0,
        }
        if absent:
            logger.warning("%s: %s tracked document(s) are not in the sitemap — "
                           "reported, not withdrawn", self.source, len(absent))
        return observations

    def confirm(self, obs: Observation) -> Optional[str]:
        """The regulation's own text, fetched only for what the date shortlisted."""
        r = requests.get(obs.url, timeout=self.timeout,
                         headers={"User-Agent": fingerprint.USER_AGENT})
        r.raise_for_status()
        digest, was_cut = page_hash(r.content.decode("utf-8", errors="replace"),
                                    self.cut_marker)
        if not was_cut:
            # The hash then covers the neighbouring cards too, so this document
            # will read `modified` more often than it changed. Counted, because
            # a confirm tier that quietly stopped confirming is worse.
            self.stats["confirm_uncut"] = self.stats.get("confirm_uncut", 0) + 1
            logger.warning("%s: %r not found in %s — hashing the whole page",
                           self.source, self.cut_marker, obs.url[:80])
        return digest


__all__ = ["SitemapLastmodSweep", "assess", "candidate_prefix", "fetch_sitemap",
           "normalise", "page_hash", "page_text", "parse_sitemap",
           "BASIS_LASTMOD", "DEFAULT_CUT", "MIN_OVERLAP", "MIN_DISTINCT_RATIO"]
