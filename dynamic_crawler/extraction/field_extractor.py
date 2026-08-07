"""Config-driven field extraction, generalizing the per-field parsing logic in
crawler/sama_finance_sector_crawler.py::_extract_structured_leaf /
_extract_page_title into a small set of reusable operations driven by
extraction.fields.* config entries.

Supported "source" operations:
  - regex:         search a container's text for a regex pattern, capture group 1
  - css_text:       .get_text(strip=True) of a selected element, optional prefix strip
  - css_attr:       an attribute (e.g. href) of a selected element, optional absolutize
  - html_container: a cleaned HTML fragment of a selected element (strip child
                     selectors, optionally absolutify links)

Every "selector" value may contain "|"-separated alternatives tried in priority
order (first match wins) -- this mirrors patterns like the existing crawler's
`soup.find(id=...) or soup.find(class_=...)` fallback chains.
"""

import re
from typing import Optional

from bs4 import BeautifulSoup

from dynamic_crawler.urlnorm import absolutize, absolutify_links


def select_first(container, selector_spec: str):
    """Try each "|"-separated selector in order, return the first element found."""
    if container is None:
        return None
    for sel in selector_spec.split("|"):
        el = container.select_one(sel.strip())
        if el:
            return el
    return None


def _apply_post_process(value: str, post_process) -> str:
    if not value or not post_process:
        return value
    for step in post_process:
        if "sub" in step:
            value = re.sub(step["sub"], step.get("repl", ""), value).strip()
    return value


def extract_field(soup: BeautifulSoup, base_url: str, field_cfg: dict) -> Optional[str]:
    source = field_cfg["source"]

    if source == "regex":
        container = select_first(soup, field_cfg["container_selector"]) if field_cfg.get("container_selector") else soup
        if container is None:
            return None
        text = container.get_text()
        m = re.search(field_cfg["pattern"], text)
        if not m:
            return None
        value = m.group(1).strip()
        return _apply_post_process(value, field_cfg.get("post_process")) or None

    if source == "css_text":
        el = select_first(soup, field_cfg["selector"])
        if not el:
            return None
        value = el.get_text(strip=True)
        strip_prefix = field_cfg.get("strip_prefix")
        if strip_prefix:
            value = value.replace(strip_prefix, "").strip()
        return value or None

    if source == "css_attr":
        el = select_first(soup, field_cfg["selector"])
        if not el:
            return None
        value = el.get(field_cfg["attr"], "")
        if not value:
            return None
        if field_cfg.get("absolutize"):
            value = absolutize(base_url, value)
        return value

    if source == "html_container":
        el = select_first(soup, field_cfg["selector"])
        if not el:
            return None
        content_copy = BeautifulSoup(str(el), "html.parser")
        for strip_sel in field_cfg.get("strip_selectors", []):
            for tag in content_copy.select(strip_sel):
                tag.decompose()
        html = str(content_copy)
        if field_cfg.get("absolutize_links"):
            html = absolutify_links(html, base_url)
        return html

    raise ValueError(f"Unknown extraction source: {source!r}")


def extract_page_title(soup: BeautifulSoup, fallback: str, strip_suffix_regex: Optional[str] = None) -> str:
    """The referring link's anchor text is often generic ("click here", "view PDF").
    The target page's own <title> tag usually has the real document name."""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
        if strip_suffix_regex:
            title = re.sub(strip_suffix_regex, "", title).strip()
        if title and title.lower() not in ("redirecting", ""):
            return title
    return fallback


def extract_year(date_str: Optional[str]) -> Optional[str]:
    if not date_str:
        return None
    match = re.search(r"\b(19|20)\d{2}\b", date_str)
    return match.group(0) if match else None
