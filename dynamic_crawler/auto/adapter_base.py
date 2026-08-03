"""The contract every auto-generated crawler adapter must implement.

The autonomous onboarding agent (dynamic_crawler/auto/onboard.py) has an LLM
WRITE a subclass of RegulatorAdapter for a regulator whose site shape the
config-driven engine doesn't already support. The generated subclass is then
run — once written — as ordinary deterministic Python, inside a sandboxed
subprocess (dynamic_crawler/auto/sandbox.py + runner.py).

Keeping the surface tiny (one `crawl` method, one injected `fetcher`) is
deliberate: it's easier for a model to implement correctly, easier to audit,
and it forces all network access through the rate-limited Fetcher.
"""

from typing import List, Optional

from models.models import RegulatoryDocument


class RegulatorAdapter:
    """Base class for a single regulator/tab crawler.

    A generated subclass MUST:
      - set the four class attributes below,
      - implement crawl(self, limit=None) returning a list of RegulatoryDocument,
      - fetch pages ONLY via self.fetcher.get(url) (returns a BeautifulSoup or None),
      - build RegulatoryDocument objects using the exact field names in
        models/models.py, always setting regulator, source_system, category,
        title, document_url, source_page_url and a doc_path that begins
        [REGULATOR, SOURCE_SYSTEM, category, ...],
      - honor `limit` (cap the number of top-level sections/pages crawled when set),
      - use dynamic_crawler.urlnorm.absolutize()/canonical() for URL handling.

    A generated subclass MUST NOT import os/sys/subprocess/socket/requests,
    open files, or call eval/exec — the sandbox blocks these anyway.
    """

    REGULATOR: str = ""
    SOURCE_SYSTEM: str = ""
    BASE_URL: str = ""
    SEED_URL: str = ""

    def __init__(self, fetcher):
        # `fetcher` is a dynamic_crawler.fetcher.Fetcher (or a CountingFetcher
        # wrapper). Use fetcher.get(url) -> BeautifulSoup | None.
        self.fetcher = fetcher

    def crawl(self, limit: Optional[int] = None) -> List[RegulatoryDocument]:
        raise NotImplementedError(
            "Generated adapter must implement crawl(self, limit=None) -> List[RegulatoryDocument]"
        )
