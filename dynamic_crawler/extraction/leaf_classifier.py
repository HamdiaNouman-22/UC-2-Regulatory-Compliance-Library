"""Config-driven leaf-page classification, generalizing the structured / listing /
plain-content cascade in crawler/sama_finance_sector_crawler.py::_process.
"""

from typing import List, Tuple

from bs4 import BeautifulSoup

from dynamic_crawler.extraction.field_extractor import select_first
from dynamic_crawler.urlnorm import absolutize


def is_structured_leaf(soup: BeautifulSoup, structured_indicator_selector: str) -> bool:
    return select_first(soup, structured_indicator_selector) is not None


def extract_body_links(soup: BeautifulSoup, base_url: str, body_links_cfg: dict) -> List[Tuple[str, str]]:
    """Links found inside the page's own content container (not nav/sidebar/breadcrumb)."""
    container = select_first(soup, body_links_cfg["container_selector"])
    if not container:
        return []
    prefixes = body_links_cfg.get("href_prefix_filters", ["/en/"])
    links = []
    for a in container.find_all("a", href=True):
        href = a["href"]
        if not any(href.startswith(p) for p in prefixes):
            continue
        title = a.get_text(strip=True)
        if title:
            links.append((title, absolutize(base_url, href)))
    return links
