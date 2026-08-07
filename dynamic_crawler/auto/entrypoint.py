"""Resolve a working seed URL when the one provided returns an error.

Regulator sites move pages around (SBP's old circulars index now 404s). Rather
than fail silently, this tries a few strategies and, if it has to guess, surfaces
the guess to the human instead of proceeding quietly.
"""

import logging
from urllib.parse import urljoin, urlparse

from dynamic_crawler.fetcher import Fetcher
from dynamic_crawler.onboarding.llm_client import call_llm, extract_json_from_llm_response

logger = logging.getLogger(__name__)


def _is_ok(fetcher, url) -> bool:
    soup = fetcher.get(url)
    if soup is None:
        return False
    # crude 404 detection: SBP serves a 404 page with 200-ish content sometimes
    text = soup.get_text(" ").lower()
    if "page not found" in text or "404" in (soup.title.get_text().lower() if soup.title else ""):
        return False
    return len(text.strip()) > 200


def resolve(seed_url: str, tab_name: str, fetch_cfg: dict, model: str = None) -> dict:
    """Return {'url': <working url>, 'confident': bool, 'note': str}."""
    fetcher = Fetcher(fetch_cfg)
    try:
        if _is_ok(fetcher, seed_url):
            return {"url": seed_url, "confident": True, "note": "seed URL works as given"}

        logger.warning(f"Seed URL not usable: {seed_url} -- attempting to resolve entry point")
        parsed = urlparse(seed_url)
        home = f"{parsed.scheme}://{parsed.netloc}/"

        home_soup = fetcher.get(home)
        if home_soup is None:
            return {"url": seed_url, "confident": False,
                    "note": f"seed URL failed and homepage {home} unreachable; needs human input"}

        # Collect candidate links whose text/href hint at the target section.
        candidates = []
        kw = tab_name.lower().split()
        for a in home_soup.find_all("a", href=True):
            text = a.get_text(" ", strip=True)
            href = urljoin(home, a["href"])
            score = sum(1 for k in kw if k in text.lower() or k in href.lower())
            if score:
                candidates.append({"text": text, "url": href, "score": score})
        candidates.sort(key=lambda c: -c["score"])
        candidates = candidates[:25]

        if not candidates:
            return {"url": seed_url, "confident": False,
                    "note": f"no homepage links matched '{tab_name}'; needs human input"}

        # Ask the LLM to pick the most likely index page for this section.
        prompt = (
            f"The regulator section we want to crawl is '{tab_name}'. The original URL "
            f"{seed_url} no longer works. From these homepage links, pick the single URL "
            f"most likely to be the '{tab_name}' listing/index page. Respond as JSON: "
            f'{{"url": "<chosen url>", "confident": true/false}}.\n\nLINKS:\n'
            + "\n".join(f"- {c['text']} -> {c['url']}" for c in candidates)
        )
        kwargs = {"system_prompt": "You identify the correct index page for a website section.",
                  "user_prompt": prompt, "max_tokens": 500}
        if model:
            kwargs["model"] = model
        try:
            choice = extract_json_from_llm_response(call_llm(**kwargs))
            chosen = choice.get("url")
        except Exception as e:
            logger.warning(f"LLM entry-point selection failed: {e}")
            chosen = candidates[0]["url"]
            choice = {"confident": False}

        works = _is_ok(fetcher, chosen) if chosen else False
        return {
            "url": chosen or seed_url,
            "confident": bool(works and choice.get("confident")),
            "note": (f"resolved to {chosen} (reachable={works}); "
                     f"VERIFY this is the right page before trusting results"),
            "candidates": candidates[:10],
        }
    finally:
        fetcher.close()
