"""Config-driven generalization of the sidebar-tree discovery logic in
crawler/sama_finance_sector_crawler.py (_li_is_folder / _parse_ul / _expand_node /
_collect_top_categories). Any Drupal-Book-style rulebook (landing grid of top
categories -> per-category <nav id="book-block-menu-{id}"> sidebar) can reuse
this by pointing config.discovery.strategy at "sidebar_tree".
"""

import re
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from bs4 import Tag

from dynamic_crawler.urlnorm import absolutize, canonical


@dataclass
class Node:
    title: str
    url: str
    children: List["Node"] = field(default_factory=list)
    is_folder_hint: bool = False


def derive_nav_id(seed_url: str, sidebar_cfg: dict) -> str:
    pattern = sidebar_cfg["category_id_regex"]
    m = re.search(pattern, seed_url)
    if not m:
        raise ValueError(
            f"Could not derive sidebar nav id from seed_url={seed_url!r} using "
            f"discovery.sidebar.category_id_regex={pattern!r}"
        )
    return sidebar_cfg["nav_id_template"].format(category_id=m.group(1))


def li_is_folder(li: Tag, folder_classes: List[str]) -> bool:
    classes = li.get("class", [])
    return any(c in classes for c in folder_classes)


def parse_ul(ul: Optional[Tag], base_url: str, folder_classes: List[str]) -> List[Node]:
    if not ul:
        return []
    nodes = []
    for li in ul.find_all("li", recursive=False):
        a = li.find("a", href=True)
        if not a:
            continue
        title = a.get_text(strip=True)
        url = absolutize(base_url, a["href"])
        is_folder = li_is_folder(li, folder_classes)
        child_ul = li.find("ul", recursive=False)
        children = parse_ul(child_ul, base_url, folder_classes) if (child_ul and is_folder) else []
        nodes.append(Node(title=title, url=url, children=children, is_folder_hint=is_folder))
    return nodes


def collect_top_categories(fetcher, base_url: str, seed_url: str, top_level_cfg: dict) -> List[Node]:
    """Parse the tab's landing page card grid (e.g. Drupal views-row cards)."""
    soup = fetcher.get(seed_url)
    if not soup:
        return []

    href_prefix = top_level_cfg.get("href_prefix_filter", "/en/")
    link_selector = top_level_cfg.get("link_selector", "a[href]")

    nodes = []
    seen = set()
    for row in soup.select(top_level_cfg["selector"]):
        a = row.select_one(link_selector)
        if not a or not a.get("href"):
            continue
        href = a["href"]
        if href_prefix and not href.startswith(href_prefix):
            continue
        title = a.get_text(strip=True)
        if not title:
            continue
        url = absolutize(base_url, href)
        key = canonical(url)
        if key in seen:
            continue
        seen.add(key)
        nodes.append(Node(title=title, url=url, is_folder_hint=True))
    return nodes


def expand_node(fetcher, base_url: str, nav_id: str, url: str, folder_classes: List[str]) -> Tuple[List[Node], Optional[object]]:
    """Fetch url and read its sidebar <li> children under nav_id."""
    soup = fetcher.get(url)
    if not soup:
        return [], None

    target = canonical(url)
    nav = soup.find("nav", id=nav_id)
    if not nav:
        return [], soup

    for li in nav.find_all("li"):
        a = li.find("a", href=True)
        if not a:
            continue
        if canonical(absolutize(base_url, a["href"])) == target:
            child_ul = li.find("ul", recursive=False)
            return parse_ul(child_ul, base_url, folder_classes), soup

    return [], soup
