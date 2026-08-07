"""Shared URL helpers used by both the crawl engine and the diff harness,
so document identity (what counts as "the same URL") is computed identically
on both sides of a comparison."""

from urllib.parse import urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup


def absolutize(base_url: str, href: str) -> str:
    if not href:
        return href
    return urljoin(base_url, href)


def canonical(url: str) -> str:
    """Strip query/fragment and trailing slash so the same page isn't visited/joined twice."""
    parts = urlsplit(url)
    path = parts.path.rstrip("/")
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def absolutify_links(html: str, base_url: str) -> str:
    """Rewrite all relative href/src values in an HTML fragment to absolute URLs."""
    if not html:
        return html
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(True):
        for attr in ("href", "src"):
            val = tag.get(attr)
            if val and not val.startswith(("http", "mailto:", "tel:", "#", "javascript:")):
                tag[attr] = urljoin(base_url, val)
    return str(soup)
