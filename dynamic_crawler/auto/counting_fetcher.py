"""A Fetcher wrapper that caps the total number of page fetches, so a runaway
or pathological generated adapter can't hammer a regulator site indefinitely.
"""

from dynamic_crawler.fetcher import Fetcher


class CountingFetcher:
    """Delegates to a real Fetcher but enforces a hard cap on total .get() calls.

    When the budget is hit we return None (as if the page failed to load) rather
    than raising, so the adapter stops fetching GRACEFULLY and still returns the
    documents it has already collected. This makes bounded test probes meaningful:
    a correctly-recursing adapter that hits the cap still yields its partial docs
    (enough to prove recursion), instead of crashing and losing everything.
    """

    def __init__(self, fetch_cfg: dict, max_fetches: int = 1500):
        self._fetcher = Fetcher(fetch_cfg)
        self._max_fetches = max_fetches
        self.count = 0
        self.budget_hit = False

    def get(self, url):
        if self.count >= self._max_fetches:
            self.budget_hit = True
            return None
        self.count += 1
        return self._fetcher.get(url)

    def close(self):
        self._fetcher.close()
