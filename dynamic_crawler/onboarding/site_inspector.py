"""Fetches a regulator tab's seed page (plus a few sample linked pages) and
saves the raw HTML locally so the onboarding agent's config proposal is
derived from inspectable, reproducible artifacts -- not an ephemeral live
browser session nobody can audit afterward.
"""

import logging
import os
import re
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _fetch_html(url: str, timeout: int = 30) -> Optional[str]:
    try:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logger.warning(f"Failed to fetch {url} for inspection: {e}")
        return None


def _clean_html_for_llm(html: str, max_chars: int = 45000) -> str:
    """Reduce a page to the structurally-informative HTML an LLM needs to write a
    crawler for it, then cap length.

    Regulator pages often carry tens of KB of <head>, inline scripts, and site
    chrome BEFORE any real content — naively truncating cuts off exactly the
    navigation grids / tables / sidebars the model must see. So we drop the
    boilerplate (head, scripts, styles, meta/link, svg/iframe) and keep the body,
    which pushes the meaningful structure (menus, cards, tables, content
    containers) inside the budget. <img>/<a> are kept — their src/href/class are
    load-bearing signals (e.g. bullet-image list markers, folder nav classes)."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "svg", "noscript", "meta", "link", "iframe", "head", "path"]):
        tag.decompose()
    # Site chrome (top mega-menu, footer) is usually tens of KB of links that
    # aren't the document navigation we care about, and it pushes the real content
    # past the length cap. Drop the outermost header/footer only.
    for sel in ("body > header", "body > footer", "header[role=banner]", "footer[role=contentinfo]"):
        for tag in soup.select(sel):
            tag.decompose()
    root = soup.body or soup
    text = str(root)
    text = re.sub(r"\n\s*\n+", "\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    if len(text) > max_chars:
        text = text[:max_chars] + "\n<!-- truncated for prompt size -->"
    return text


def _save_sample(sample_dir: str, name: str, url: str, html: str) -> None:
    os.makedirs(sample_dir, exist_ok=True)
    path = os.path.join(sample_dir, f"{name}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"<!-- source: {url} -->\n{html}")
    logger.info(f"Saved inspection sample: {path}")


def inspect_site(seed_url: str, sample_dir: str, sample_urls: Optional[List[str]] = None) -> Dict[str, dict]:
    """Fetch the seed page and a handful of sample pages, save raw HTML locally,
    and return cleaned HTML snippets ready to include in an LLM prompt.
    """
    samples: Dict[str, dict] = {}

    seed_html = _fetch_html(seed_url)
    if seed_html is None:
        raise RuntimeError(f"Could not fetch seed URL for inspection: {seed_url}")
    _save_sample(sample_dir, "seed", seed_url, seed_html)
    samples["seed"] = {"url": seed_url, "cleaned_html": _clean_html_for_llm(seed_html)}

    urls_to_sample = list(sample_urls or [])
    if not urls_to_sample:
        # Auto-discover a couple of sample links off the seed page's card grid
        # (best-effort guess at "div.views-row"-style landing pages; if that
        # pattern isn't present, falls back to the first few in-domain links).
        soup = BeautifulSoup(seed_html, "html.parser")
        seen = set()
        candidates = soup.select("div.views-row a[href]") or soup.find_all("a", href=True)
        for a in candidates:
            href = a.get("href")
            if not href or not href.startswith("/"):
                continue
            full = urljoin(seed_url, href)
            if full in seen or full == seed_url:
                continue
            seen.add(full)
            urls_to_sample.append(full)
            if len(urls_to_sample) >= 3:
                break

    for i, url in enumerate(urls_to_sample, 1):
        html = _fetch_html(url)
        if html is None:
            continue
        _save_sample(sample_dir, f"sample_{i}", url, html)
        samples[f"sample_{i}"] = {"url": url, "cleaned_html": _clean_html_for_llm(html)}

    return samples
