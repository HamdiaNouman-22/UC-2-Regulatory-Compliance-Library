"""
cma_announcements_crawler.py
==============================
CMA (Capital Market Authority - Saudi Arabia) Media Center > Announcements crawler.

Source: https://cma.gov.sa/en/MediaCenter/NEWS/Pages/default.aspx

How this works
---------------
The page LOOKS like it has 548 client-side pages with Year/Month dropdown
filters, but that pager (pager.js) is cosmetic: it just groups whatever
items are already in the DOM into batches of 6 and hides/shows them with
CSS (`hideRowItem`). Only ~30 real items are ever in the DOM per HTTP
request - client_side_pages := ceil(itemsInDom / rowsPerPage).

The REAL pagination is classic SharePoint list-view continuation, exposed
by the "next" arrow (`.nxtbtn`), whose onclick calls:

    RefreshPageTo(event, "/en/.../default.aspx?Paged=TRUE&p_SortBehavior=0
                           &p_ArticleStartDate=<...>&p_ID=<lastItemId>
                           &PageFirstRow=<N>&View={<viewGuid>}")

So we walk that continuation link page by page (~30 items/request) until
no `.nxtbtn` is present. This is both simpler and far fewer requests than
trying to replay the Year/Month dropdown's `FilterField/FilterValue` query
string across every (year, month) combination.

The SharePoint REST/owssvr.dll endpoints that would return this data as
one JSON/XML blob are blocked by CMA's WAF ("_api", "_vti_bin") - direct
requests to them return an "Access Error" HTML page, confirmed by hand.

Per item we:
  1. Read title / detail URL / date straight off the listing card.
  2. Follow "Read More" to the detail page and extract:
       - title                 h1.page-title
       - published_date (raw)  span#page-date  (format DD/MM/YYYY,
                                pre-JS-formatting - reliable to parse)
       - content_html/content_text   div.ms-rtestate-field inside <article>

Output fields mirror the RegulatoryDocument convention used by the other
crawlers in this repo (see crawler/crawler.py), with year/month added so
documents can be grouped the same way the site's own folder tree /
Year-Month dropdown groups them.
"""

import hashlib
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from html import unescape
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

BASE_URL      = "https://cma.gov.sa"
LIST_URL      = f"{BASE_URL}/en/MediaCenter/NEWS/Pages/default.aspx"
REQUEST_DELAY = 1.0
MAX_RETRIES   = 3

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
})

_NEXT_BTN_RE = re.compile(r'RefreshPageTo\(event,\s*"([^"]+)"')


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class CMADocument:
    regulator: str
    source_system: str
    category: str
    title: str
    document_url: str
    published_date: Optional[str]        # ISO "YYYY-MM-DD"
    year: Optional[str]
    month: Optional[str]                 # e.g. "July"
    reference_no: Optional[str]          # e.g. "CMA_N_4088"
    doc_path: List[str]
    last_modified_date: Optional[str] = None   # ISO datetime, Saudi Arabia local time
    document_html: Optional[str] = None
    content_text: Optional[str] = None
    content_hash: Optional[str] = None
    extra_meta: Dict[str, Any] = field(default_factory=dict)


def _hash(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


# ── HTTP ──────────────────────────────────────────────────────────────────────

def _fetch(url: str) -> Optional[BeautifulSoup]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            log.debug(f"  OK [{resp.status_code}] {url}")
            return BeautifulSoup(resp.text, "lxml")
        except requests.RequestException as e:
            log.warning(f"  Attempt {attempt}/{MAX_RETRIES} {url}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
    log.error(f"  All retries exhausted: {url}")
    return None


def _abs(href: str) -> str:
    return urljoin(BASE_URL, href)


# ── Listing page parsing ───────────────────────────────────────────────────────

def _parse_listing_items(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """Extract one entry per announcement card from a listing page."""
    items = []
    for card in soup.find_all("div", class_="page"):
        item_id = card.get("data-id", "")
        h3 = card.find("h3")
        a  = card.find("a", href=True, attrs={"title": "Read More"}) or card.find("a", href=True)
        date_span = card.select_one(".info-wrapper .date")

        if not a or not h3:
            continue

        items.append({
            "id":            item_id,
            "title":         h3.get_text(strip=True),
            "detail_url":    _abs(a["href"]),
            "list_date_text": date_span.get_text(strip=True) if date_span else "",
        })
    return items


def _get_next_page_url(soup: BeautifulSoup) -> Optional[str]:
    """Follow the `.nxtbtn` continuation link (real SharePoint paging)."""
    nxt = soup.select_one("a.nxtbtn")
    if not nxt:
        return None
    onclick = nxt.get("onclick", "")
    m = _NEXT_BTN_RE.search(onclick)
    if not m:
        return None
    return _abs(unescape(m.group(1)))


# ── Detail page parsing ────────────────────────────────────────────────────────

_BLOCK_TAGS = ["p", "div", "li", "table", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "ul", "ol"]


def _extract_content_text(content_div) -> str:
    """
    Build plain text from the article body.

    The CMS frequently wraps individual fragments (a single drop-cap
    letter, digits of a Hijri date, a parenthesised number) in their own
    adjacent <span> tags with NO whitespace between them in the source,
    e.g. `</span><span>123</span><span>) days...`. A naive
    `get_text(separator=" ")` inserts a space at *every* tag boundary,
    which corrupts these into "T he", "( 123 )", "144 8 /01/16", etc.

    Fix: extract each leaf block-level element (p/li/td/...) with NO
    separator (so touching spans concatenate exactly as they would
    render in a browser), then join those blocks with newlines - the
    only place a real separator is actually needed.
    """
    blocks = content_div.find_all(_BLOCK_TAGS)
    leaf_blocks = [b for b in blocks if not b.find(_BLOCK_TAGS)] if blocks else []

    if not leaf_blocks:
        text = content_div.get_text(separator="", strip=True)
        return _clean_content_text(text)

    parts = []
    for block in leaf_blocks:
        txt = block.get_text(separator="", strip=True)
        txt = re.sub(r"[ \t\r\n]+", " ", txt)
        if txt:
            parts.append(_clean_content_text(txt))
    return "\n".join(parts)


def _clean_content_text(text: str) -> str:
    """Strip zero-width-space artifacts left over from the CMS markup."""
    return text.replace("​", "").strip()


def _parse_detail_page(soup: BeautifulSoup) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "title": None, "published_date": None, "last_modified_date": None,
        "document_html": None, "content_text": None,
    }

    h1 = soup.find("h1", class_="page-title")
    if h1:
        result["title"] = h1.get_text(strip=True)

    date_span = soup.find("span", id="page-date")
    if date_span:
        raw = date_span.get_text(strip=True)
        try:
            result["published_date"] = datetime.strptime(raw, "%d/%m/%Y").date().isoformat()
        except ValueError:
            log.warning(f"  Could not parse published date: {raw!r}")

    modified_span = soup.find("span", id="ctl00_LastModifiedControl_spnLasModified")
    if modified_span:
        raw = modified_span.get_text(strip=True)
        try:
            result["last_modified_date"] = datetime.strptime(raw, "%d/%m/%Y - %I:%M %p").isoformat()
        except ValueError:
            log.warning(f"  Could not parse last modified date: {raw!r}")

    article = soup.find("article")
    content_div = article.find("div", class_="ms-rtestate-field") if article else None
    if content_div:
        for tag in content_div.find_all(["script", "style"]):
            tag.decompose()
        result["document_html"] = str(content_div)
        result["content_text"] = _extract_content_text(content_div)

    return result


def _reference_no(detail_url: str) -> Optional[str]:
    m = re.search(r"/([^/]+)\.aspx$", detail_url)
    return m.group(1) if m else None


# ── Build one document ─────────────────────────────────────────────────────────

def _build_document(item: Dict[str, str], request_delay: float) -> Optional[CMADocument]:
    time.sleep(request_delay)
    soup = _fetch(item["detail_url"])
    if not soup:
        return None

    detail = _parse_detail_page(soup)
    title  = detail["title"] or item["title"]

    year = month = None
    if detail["published_date"]:
        d = datetime.fromisoformat(detail["published_date"])
        year, month = str(d.year), d.strftime("%B")

    return CMADocument(
        regulator      = "Capital Market Authority",
        source_system  = "CMA-MediaCenter",
        category       = "Announcements",
        title          = title,
        document_url   = item["detail_url"],
        published_date = detail["published_date"],
        year           = year,
        month          = month,
        reference_no   = _reference_no(item["detail_url"]),
        doc_path       = ["CMA", "Media Center", "Announcements", year or "Unknown", month or "Unknown"],
        last_modified_date = detail["last_modified_date"],
        document_html  = detail["document_html"],
        content_text   = detail["content_text"],
        content_hash   = _hash(detail["content_text"]) if detail["content_text"] else None,
        extra_meta     = {
            "list_item_id":     item["id"],
            "list_date_text":   item["list_date_text"],
            "last_modified_tz": "Asia/Riyadh",
        },
    )


# ── Public API ──────────────────────────────────────────────────────────────────

def crawl_announcements(
    max_items: Optional[int] = None,
    max_pages: Optional[int] = None,
    request_delay: float = REQUEST_DELAY,
) -> List[CMADocument]:
    """
    Crawl CMA Announcements, newest first, following the real SharePoint
    continuation pagination (not the cosmetic 548-page client pager).

    Args:
        max_items: stop after collecting this many documents (None = all).
        max_pages: stop after this many listing-page fetches (None = all).
        request_delay: seconds to sleep between HTTP requests.
    """
    log.info("=== CMA Announcements Crawler ===")
    results: List[CMADocument] = []
    next_url = LIST_URL
    page_no = 0

    while next_url:
        page_no += 1
        if max_pages and page_no > max_pages:
            log.info(f"Reached max_pages={max_pages}, stopping")
            break

        log.info(f"[Listing page {page_no}] {next_url}")
        soup = _fetch(next_url)
        if not soup:
            break

        items = _parse_listing_items(soup)
        log.info(f"  Found {len(items)} items on this page")

        for item in items:
            if max_items and len(results) >= max_items:
                break
            doc = _build_document(item, request_delay)
            if doc:
                results.append(doc)
                log.info(f"  [{len(results)}] {doc.published_date} - {doc.title[:60]}")

        if max_items and len(results) >= max_items:
            log.info(f"Reached max_items={max_items}, stopping")
            break

        next_url = _get_next_page_url(soup)
        time.sleep(request_delay)

    log.info(f"=== Done: {len(results)} announcements collected ===")
    return results


def save_to_json(documents: List[CMADocument], filename: str = "cma_announcements_sample.json") -> None:
    import json
    from dataclasses import asdict

    data = [asdict(doc) for doc in documents]
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log.info(f"Saved {len(documents)} documents to {filename}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Quick smoke test: 2 listing pages worth (~60 items capped to 12 docs)
    docs = crawl_announcements(max_items=12, max_pages=2)

    print("\n" + "=" * 80)
    print(f"Extracted {len(docs)} documents")
    print("=" * 80)

    if docs:
        d = docs[0]
        print(f"\nExample Document:")
        print(f"  Title: {d.title}")
        print(f"  URL: {d.document_url}")
        print(f"  Reference No: {d.reference_no}")
        print(f"  Published: {d.published_date}  (Year={d.year}, Month={d.month})")
        print(f"  Doc Path: {d.doc_path}")
        print(f"  Content length: {len(d.content_text) if d.content_text else 0} chars")

    save_to_json(docs)
