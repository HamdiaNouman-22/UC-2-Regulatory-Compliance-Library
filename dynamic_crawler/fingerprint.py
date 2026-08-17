"""Ask a server which version of a document it is holding.

A document's `content_hash` is derived from its URL and its link text, and the URL
is also its identity — so a regulator that replaces the file behind an unchanged
link produces an identical hash and the document reads `unchanged` forever. A
two-byte ranged GET returns validators that do move: SharePoint answers with
`ETag: "{GUID},<version>"` and increments the version on every save.

The token is stored alongside the hash rather than replacing it, so enabling this
on a source does not reclassify its whole library on the first run.
"""

from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote, urlsplit, urlunsplit

import requests

logger = logging.getLogger(__name__)

# HEAD, and any request without a browser User-Agent, is answered by some
# regulator firewalls with a 247-byte "Request Rejected" body behind a 200.
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

#: Probing is pure network wait: 21s per document against CMA, measured
#: 2026-08-10. Six in flight doubled each request's own latency for a 3.5x total
#: gain, so the returns are already flat here — four keeps most of the speedup
#: without leaning on a regulator's server. SIMAH is blocked today for repeated
#: iteration, not for volume, and that is the failure this number guards against.
DEFAULT_WORKERS = 4

#: No validator offered. Same wording as crawler/simah_wrapper.py so a reader
#: meets one vocabulary for "this hash cannot detect an edit", not two.
BASIS_NONE = "url+title (identity only — cannot detect an edit)"
BASIS_FAILED = "probe-failed"

# Publishing sites append a suffix: "{GUID},1pub". A pattern anchored on the
# digits drops those silently, and the document then reports as never changing.
_SP_ETAG = re.compile(r"^\{?([0-9A-Fa-f-]{36})\}?,(\d+)[A-Za-z]*$")


def _encoded(url: str) -> str:
    """AML's hrefs carry literal spaces, which cannot be put on the wire."""
    s = urlsplit(url)
    return urlunsplit((s.scheme, s.netloc,
                       quote(s.path, safe="/%:@&=+$,~()!*'"),
                       quote(s.query, safe="/%:@&=+$,?~"), ""))


def version_token(url: str, session=None, timeout: float = 20.0) -> tuple:
    """`(token, basis)` for one document. Never raises.

    `basis` says how the token was obtained, including the two ways of not
    getting one, because a token that is empty for an unknown reason is
    indistinguishable from a document that never changes.
    """
    if not (url or "").startswith("http"):
        return "", BASIS_NONE
    getter = session or requests
    try:
        r = getter.get(_encoded(url), timeout=timeout, allow_redirects=True,
                       headers={"User-Agent": USER_AGENT, "Accept": "*/*",
                                "Range": "bytes=0-1"})
    except Exception as e:
        logger.debug("version probe failed for %s: %s", url, e)
        return "", BASIS_FAILED
    if r.status_code >= 400:
        logger.debug("version probe %s for %s", r.status_code, url)
        return "", BASIS_FAILED

    etag = (r.headers.get("ETag") or "").strip().strip('"')
    if etag:
        # An opaque ETag still detects a change; it is reported separately so a
        # host emitting a volatile one is visible instead of flagging everything.
        return etag, "etag" if _SP_ETAG.match(etag) else "etag-opaque"

    last_modified = (r.headers.get("Last-Modified") or "").strip()
    if last_modified:
        size = (r.headers.get("Content-Range") or "").rsplit("/", 1)[-1].strip()
        return f"{last_modified}|{size}", "last-modified+length"
    return "", BASIS_NONE


def tokens_for(urls, workers=None, timeout: float = 20.0) -> dict:
    """`{url: (token, basis)}`, a few documents at a time."""
    urls = [u for u in dict.fromkeys(urls) if u]
    if not urls:
        return {}
    count = max(1, int(workers or DEFAULT_WORKERS))
    with requests.Session() as session:      # one handshake, not one per document
        if count == 1:
            return {u: version_token(u, session, timeout) for u in urls}
        with ThreadPoolExecutor(max_workers=count) as pool:
            probe = lambda u: version_token(u, session, timeout)   # noqa: E731
            return dict(zip(urls, pool.map(probe, urls)))


def summarise(tokens: dict) -> dict:
    """Counts per basis, so a source that got no usable token says so."""
    by_basis: dict = {}
    for _token, basis in tokens.values():
        by_basis[basis] = by_basis.get(basis, 0) + 1
    return {"probed": len(tokens),
            "with_token": sum(1 for token, _b in tokens.values() if token),
            "by_basis": by_basis}


def annotate(docs, workers=None, timeout: float = 20.0) -> dict:
    """Write `version_token` and `hash_basis` into each document's extra_meta."""
    tokens = tokens_for([getattr(d, "document_url", "") for d in docs],
                        workers=workers, timeout=timeout)
    for d in docs:
        token, basis = tokens.get(getattr(d, "document_url", ""), ("", BASIS_NONE))
        meta = dict(getattr(d, "extra_meta", None) or {})
        meta["version_token"] = token
        meta["hash_basis"] = basis
        d.extra_meta = meta
    return summarise(tokens)
