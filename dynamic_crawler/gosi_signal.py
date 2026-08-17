"""GOSI publishes each page as JSON, so detecting a change there needs no browser.

One unauthenticated request per seed returns the entire page — the tab strip, all
of its sections' HTML and every document link. That makes this the first signal
that sees everything the regulator publishes, and therefore the first that may
report a document as absent rather than only as changed.

Two different tokens come out of that one response, and they are not equally
strong. The page's publish date is shared by every instrument on the page, so a
republish moves all of them at once and only a content hash separates a real
amendment from a bulk save. The version counter on the PDFs the page links to is
per document, and moves only when that document is saved.
"""

from __future__ import annotations

import hashlib
import html
import logging
import re
from typing import List, Optional
from urllib.parse import urlsplit

import requests

from dynamic_crawler import fingerprint
from dynamic_crawler.changesignal import (ChangeSignal, Observation,
                                          identity_key)

logger = logging.getLogger(__name__)

API = "https://cmsapi.gosi.gov.sa/api/SharePoint/GetSiteContent"
PARENT_SITE = "SystemsAndRegulations"
PAGE = "https://www.gosi.gov.sa/en/SystemsAndRegulations/{seed}"

#: The seeds the two approved forms already crawl. `siteUrl` is GOSI's own name
#: for the page, so it is what a source is called here.
SEEDS = ("SocialInsurance", "Saned")

BASIS_PAGE_DATE = "cms-lastpublisheddate (page-wide — confirm with a hash)"
BASIS_BLOCKED = "host blocked in config"

#: Anything the page offers for download. Only PDFs have ever been seen, but a
#: sweep that silently ignored a .docx would report that page as unchanged.
_DOC_SUFFIX = (".pdf", ".doc", ".docx", ".xls", ".xlsx")
_HREF = re.compile(r"""href\s*=\s*["']([^"']+)["']""", re.I)


def endpoint(seed: str, lang: str = "en") -> str:
    return f"{API}?parentSiteUrl={PARENT_SITE}&siteUrl={seed}&lang={lang}"


def fetch(seed: str, lang: str = "en", session=None, timeout: float = 20.0) -> dict:
    """The seed's whole page, as JSON. No cookie, no Referer, no authentication."""
    getter = session or requests
    r = getter.get(endpoint(seed, lang), timeout=timeout,
                   headers={"User-Agent": fingerprint.USER_AGENT,
                            "Accept": "application/json"})
    r.raise_for_status()
    return r.json()


def _sort_key(value) -> tuple:
    """Numeric ids in numeric order, anything else after them, always the same
    order — the confirm hash is only worth anything if it is deterministic."""
    text = str(value if value is not None else "")
    return (0, int(text), "") if text.isdigit() else (1, 0, text)


def content_hash(content_list) -> str:
    """One instrument's sections, hashed.

    Sorted by section id rather than kept in the order the API returned: a CMS
    that reorders its own list has not amended anything, and an amendment moves
    the text either way.
    """
    parts = []
    for item in sorted(content_list or [], key=lambda c: _sort_key(c.get("ID"))):
        parts.append("\x1f".join([str(item.get("ID") or ""),
                                  str(item.get("Title") or ""),
                                  str(item.get("Content") or "")]))
    return hashlib.sha256("\x1e".join(parts).encode("utf-8")).hexdigest()


def document_links(systems) -> List[str]:
    """Every downloadable link in the page's own HTML, in the order published.

    The hrefs carry a triple slash (`cmsgosi.gosi.gov.sa///sites/`) which is left
    exactly as published: it is half of the document's identity here, and the
    server answers it.
    """
    out = []
    for system in systems or []:
        for section in system.get("ContentList") or []:
            for href in _HREF.findall(str(section.get("Content") or "")):
                href = html.unescape(href).strip()
                path = urlsplit(href).path.lower()
                if href.startswith("http") and path.endswith(_DOC_SUFFIX):
                    out.append(href)
    return list(dict.fromkeys(out))


class GosiJsonSweep(ChangeSignal):
    """One seed page: its instruments and the documents it links to.

    Both kinds go through ONE sweep on purpose. `missing` is computed as
    everything in the store this sweep did not see, so splitting them into two
    sweeps over one store would make each report the other's documents withdrawn.
    """

    # The response is the whole page, so an instrument that is not in it is
    # genuinely gone. Withdrawn again if the documents are not being read - see
    # __init__.
    covers_inventory = True
    # The page date is shared by every instrument on it; only the hash decides.
    confirm_required = True

    def __init__(self, seed: str, *, lang: str = "en", fetch_json=None,
                 session=None, timeout: float = 20.0,
                 probe_documents: bool = True, workers: Optional[int] = None,
                 skip_hosts=()):
        self.seed = str(seed)
        self.lang = lang
        self._fetch = fetch_json or (
            lambda: fetch(self.seed, self.lang, session, timeout))
        self.timeout = float(timeout)
        self.probe_documents = bool(probe_documents)
        self.workers = workers
        self.skip_hosts = {str(h).lower().lstrip(".") for h in (skip_hosts or ())}
        self.stats: dict = {}
        self._sections: dict = {}
        if not self.probe_documents:
            # Its documents are in the store from previous sweeps, and a sweep
            # that claims to cover the inventory while not looking at them would
            # report every one of them withdrawn.
            self.covers_inventory = False

    @property
    def name(self) -> str:
        return f"gosi-json:{self.seed}"

    def _blocked(self, url: str) -> Optional[str]:
        host = (urlsplit(url).hostname or "").lower()
        for h in self.skip_hosts:
            bare = h[4:] if h.startswith("www.") else h
            if host == h or host == bare or host.endswith("." + bare):
                return h
        return None

    # ----------------------------------------------------------------- #

    def _systems(self) -> list:
        """The tab strip, or an exception.

        Never an empty list: this sweep reports absences, so a page it could not
        read must produce no report at all rather than a report saying every
        instrument GOSI publishes has been withdrawn.
        """
        payload = self._fetch()
        if not isinstance(payload, dict):
            raise ValueError(f"{self.seed}: expected a JSON object, got "
                             f"{type(payload).__name__}")
        code = payload.get("ReturnCode")
        if code not in (0, "0", None):
            raise ValueError(f"{self.seed}: ReturnCode {code!r} "
                             f"{str(payload.get('Message') or '')[:120]!r}")
        systems = payload.get("SystemsList")
        if not isinstance(systems, list) or not systems:
            raise ValueError(f"{self.seed}: no SystemsList in the response — "
                             f"refusing to report an empty page as an absence")
        return systems

    def _instrument(self, system: dict) -> Observation:
        """One tab, with its confirm hash already computed.

        Hashed here and not only when a verdict asks for it, because the text
        arrived with the date and costs nothing: a hash taken only for documents
        already ruled modified is never stored at baseline, so the FIRST time the
        page date moves there is nothing to compare against and every instrument
        shortlists — which is the bulk republish this tier exists to absorb.
        """
        system_id = str(system.get("ID") or "").strip()
        fields = {"page": self.seed, "system_id": system_id}
        date = str(system.get("LastPublishedDate") or "").strip()
        sections = system.get("ContentList") or []
        self._sections[identity_key(fields)] = sections
        return Observation(
            key=identity_key(fields), fields=fields,
            identity_fields=("page", "system_id"),
            token=date,
            basis=BASIS_PAGE_DATE if date else fingerprint.BASIS_NONE,
            url=f"{PAGE.format(seed=self.seed)}#{system_id}",
            title=str(system.get("Title") or "").strip(),
            confirm_hash=content_hash(sections))

    def _documents(self, systems) -> List[Observation]:
        """One observation per linked document, whether or not it was probed.

        A document that was skipped or that failed is still a document this sweep
        saw. Dropping it would put it in `missing`, and `missing` is the bucket a
        withdrawal is proposed from.
        """
        links = document_links(systems)
        blocked = {u: self._blocked(u) for u in links}
        self.stats["documents_found"] = len(links)
        self.stats["documents_blocked"] = sum(1 for h in blocked.values() if h)

        probe = [u for u in links if not blocked[u]] if self.probe_documents else []
        tokens = fingerprint.tokens_for(probe, workers=self.workers,
                                        timeout=self.timeout) if probe else {}
        if tokens:
            self.stats["documents"] = fingerprint.summarise(tokens)

        out = []
        for url in links:
            if blocked[url]:
                token, basis = "", BASIS_BLOCKED
            elif not self.probe_documents:
                token, basis = "", fingerprint.BASIS_NONE
            else:
                token, basis = tokens.get(url, ("", fingerprint.BASIS_NONE))
            fields = {"page": self.seed, "document_url": url}
            out.append(Observation(
                key=identity_key(fields), fields=fields,
                identity_fields=("page", "document_url"),
                token=token, basis=basis, url=url,
                title=url.rsplit("/", 1)[-1]))
        return out

    def sweep(self) -> List[Observation]:
        systems = self._systems()
        self._sections = {}
        instruments = [self._instrument(s) for s in systems]
        documents = self._documents(systems)
        self.stats.update({
            "seed": self.seed,
            "instruments": len(instruments),
            "sections": sum(len(s.get("ContentList") or []) for s in systems),
            "page_dates": sorted({o.token for o in instruments if o.token}),
        })
        return instruments + documents

    def confirm_required_for(self, obs: Observation) -> bool:
        """Only the instruments. A document's version counter is proof on its
        own — it moves when that one file is saved, and nothing else moves it."""
        return "system_id" in obs.fields

    def confirm(self, obs: Observation) -> Optional[str]:
        """The instrument's own sections, out of the response already fetched.

        No second request: the confirm tier here costs nothing because the bytes
        it hashes arrived with the date that shortlisted them.
        """
        sections = self._sections.get(obs.key)
        return content_hash(sections) if sections is not None else None


__all__ = ["GosiJsonSweep", "SEEDS", "API", "PAGE", "endpoint", "fetch",
           "content_hash", "document_links", "BASIS_PAGE_DATE", "BASIS_BLOCKED"]
