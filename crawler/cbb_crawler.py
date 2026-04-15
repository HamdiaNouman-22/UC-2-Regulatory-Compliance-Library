"""
CBB Crawler v2 — adds two new sections on top of the existing pipeline:

NEW: MODE 5 — COMPLIANCE  (https://www.cbb.gov.bh/compliance/)
  The page has TWO main anchor sections:
    #aml   → Anti-Money Laundering & Combating the Financing of Terrorism
    #eofi  → Exchange of Financial Information

  Within each section, accordion panels (ult_exp_section divs) expand inline.
  NO child pages are crawled — all content lives on the single compliance page.

  DOCUMENT HIERARCHY:
    [REGULATOR, "Compliance", "AML", <accordion_title>]
    [REGULATOR, "Compliance", "EOFI", <accordion_title>]

  Accordion routing:
    - Compliance Directorate Publications → _parse_publications_accordion
    - AML/CFT Legislation                 → _parse_legislation_accordion (EN+AR per decree)
    - Guidance Papers                     → _parse_guidance_papers_accordion (Letter+GP per entry)
    - Common Reporting Standard (CRS)     → _parse_crs_accordion (overview + directives)
    - Foreign Account Tax Compliance (FATCA) → _parse_fatca_accordion
    - Base Erosion and Profit Shifting (BEPS) → _parse_beps_accordion
    - AML/CFT Links / Industry Assoc.    → _parse_links_accordion
    - AML/CFT Mutual Evaluation          → _parse_mutual_evaluation_accordion
    - Everything else                    → _parse_text_accordion

EXISTING (unchanged from v1):
  MODE 1 — CBB Regulations and Resolutions
  MODE 2 — Book pages (AML Law, Corporate Governance, Rulebook Volumes)
  MODE 3 — Laws & Regulations (cbb.gov.bh accordion)
  MODE 4 — CBB Capital Market Regulations (mixed internal/external)
"""

import hashlib
import requests
from bs4 import BeautifulSoup, Tag
import re
import time
import logging
from datetime import datetime
from urllib.parse import urljoin
from typing import List, Optional, Dict, Any

try:
    from crawler.crawler import BaseCrawler
    from models.models import RegulatoryDocument
except ImportError:
    class BaseCrawler:
        pass

    from dataclasses import dataclass, field

    @dataclass
    class RegulatoryDocument:
        regulator: str = ""
        source_system: str = ""
        category: str = ""
        title: str = ""
        document_url: str = ""
        urdu_url: Any = None
        published_date: Any = None
        reference_no: Any = None
        department: Any = None
        year: Any = None
        source_page_url: str = ""
        file_type: Any = None
        document_html: str = ""
        extra_meta: dict = field(default_factory=dict)
        doc_path: list = field(default_factory=list)


log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
BASE_URL                       = "https://cbben.thomsonreuters.com"
CBB_RULEBOOK_INDEX             = "https://cbben.thomsonreuters.com/cbb-rulebook"
CAPITAL_MARKET_REGULATIONS_URL = "https://cbben.thomsonreuters.com/cbb-capital-market-regulations"
CAPITAL_MARKET_CATEGORY        = "CBB Capital Market Regulations"
LAWS_REGULATIONS_URL           = "https://www.cbb.gov.bh/laws-regulations/"
COMPLIANCE_URL                 = "https://www.cbb.gov.bh/compliance/"
COMPLIANCE_BASE                = "https://www.cbb.gov.bh"

# Compliance-specific constants (used by parsers and tests)
CATEGORY      = "Compliance"
SOURCE_SYSTEM = "CBB-Compliance"

REQUEST_DELAY = 1.5
MAX_RETRIES   = 3
REGULATOR     = "Central Bank of Bahrain"

STATIC_BASE_URLS = {
    "CBB Regulations and Resolutions": (
        "https://cbben.thomsonreuters.com/rulebook/cbb-regulations-and-resolutions",
        "resolutions",
    ),
    "Bahrain Anti Money Laundering Law 2001": (
        "https://cbben.thomsonreuters.com/rulebook/bahrain-anti-money-laundering-law-2001",
        "book",
    ),
    "The Corporate Governance Code of the Kingdom of Bahrain": (
        "https://cbben.thomsonreuters.com/rulebook/corporate-governance-code-kingdom-bahrain",
        "book",
    ),
}

STATIC_BOOK_NAV_IDS = {
    "Bahrain Anti Money Laundering Law 2001":                  "book-block-menu-1000001",
    "The Corporate Governance Code of the Kingdom of Bahrain": "book-block-menu-2300001",
}

RULEBOOK_VOLUME_NAV_IDS: Dict[str, str] = {
    "Common Volume":                                  "book-block-menu-2304719",
    "Volume 1 | Conventional Banks":                 "book-block-menu-100001",
    "Volume 2 | Islamic Banks":                      "book-block-menu-200001",
    "Volume 3 | Insurance":                          "book-block-menu-300001",
    "Volume 4 | Investment Business":                "book-block-menu-400001",
    "Volume 5 | Specialised Licensees":              "book-block-menu-500001",
    "Volume 6 | Capital Markets":                    "book-block-menu-600001",
    "Volume 7 | Collective Investment Undertakings": "book-block-menu-700001",
}

# Month name → number mapping for compliance date parsing
MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

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


# ═══════════════════════════════════════════════════════════════════════════════
#  OPTION B — FACTORY FUNCTION
#  Single point of document creation for the ENTIRE crawler.
#  Every RegulatoryDocument must be built through create_cbb_document() so that
#  content_hash is guaranteed to be present in extra_meta for monitoring.
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_content_hash(extra_meta: Dict) -> str:
    """
    Compute an MD5 hash from content_text inside extra_meta.

    Hash input priority:
      1. extra_meta["content_text"]  — preferred: human-readable extracted text
      2. extra_meta["document_url"]  — fallback for pure-PDF docs with no body text
      3. empty string                — last resort (hash will be constant; acceptable
                                       because monitoring will still store the doc)

    Returns a 32-character hex string.
    """
    content = (
        extra_meta.get("content_text")
        or extra_meta.get("document_url", "")
        or ""
    )
    return hashlib.md5(content.encode("utf-8")).hexdigest()


def create_cbb_document(**kwargs) -> RegulatoryDocument:
    """
    Factory wrapper around RegulatoryDocument.

    Usage — replace every bare `RegulatoryDocument(...)` call with
    `create_cbb_document(...)` using identical keyword arguments.

    What it adds automatically:
      • extra_meta["content_hash"]  — MD5 of content_text (or fallback)

    The caller is still responsible for populating extra_meta["content_text"]
    before calling this function; the hash is derived from whatever is there.
    """
    # Ensure extra_meta is always a dict (never None)
    if "extra_meta" not in kwargs or kwargs["extra_meta"] is None:
        kwargs["extra_meta"] = {}

    doc = RegulatoryDocument(**kwargs)

    # Inject hash — done AFTER construction so we hash the final extra_meta
    doc.extra_meta["content_hash"] = _compute_content_hash(doc.extra_meta)

    return doc


# ─── Shared HTTP helper ────────────────────────────────────────────────────────
def _fetch(url: str) -> Optional[BeautifulSoup]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            log.info(f"✓ Fetched [{resp.status_code}] {url}")
            return BeautifulSoup(resp.content, "lxml")
        except requests.RequestException as e:
            log.warning(f"Attempt {attempt}/{MAX_RETRIES} failed for {url}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
    log.error(f"✗ All retries exhausted for {url}")
    return None


# ─── Shared URL helpers ────────────────────────────────────────────────────────
def _make_absolute(block: BeautifulSoup, base: str = BASE_URL) -> None:
    """Rewrite relative hrefs/srcs to absolute URLs in-place (Modes 1-4)."""
    for tag in block.find_all(href=True):
        if tag["href"].startswith("/"):
            tag["href"] = urljoin(base, tag["href"])
    for tag in block.find_all(src=True):
        if tag["src"].startswith("/"):
            tag["src"] = urljoin(base, tag["src"])


def _abs(href: str) -> str:
    """Make a compliance URL absolute (Mode 5)."""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return urljoin(COMPLIANCE_BASE, href)


def _is_pdf(url: str) -> bool:
    return url.lower().endswith(".pdf")


# ═══════════════════════════════════════════════════════════════════════════════
#  MODES 1-4
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_content(soup: BeautifulSoup) -> tuple:
    content_block = soup.find("div", class_="field--name-body")
    content_html  = ""
    content_text  = ""
    download_links: List[Dict] = []
    english_pdf = None
    arabic_pdf  = None

    if content_block:
        _make_absolute(content_block)
        content_html = str(content_block)
        content_text = content_block.get_text(separator=" ", strip=True)

        for a in content_block.find_all("a", href=True):
            full_url    = a["href"]
            parent_text = a.parent.get_text().lower() if a.parent else ""
            lang        = "arabic" if "arabic" in parent_text else "english"
            download_links.append({
                "text": a.parent.get_text(strip=True),
                "url":  full_url,
                "type": "pdf",
                "language": lang,
            })
            if lang == "english" and not english_pdf:
                english_pdf = full_url
            if lang == "arabic" and not arabic_pdf:
                arabic_pdf = full_url

    return content_html, content_text, download_links, english_pdf, arabic_pdf


def _extract_book_child_links(soup: BeautifulSoup) -> List[Dict]:
    children = []
    seen = set()
    for nav_div in soup.find_all("div", id=re.compile(r"book-navigation")):
        for a in nav_div.find_all("a", href=True):
            text     = a.get_text(strip=True)
            full_url = urljoin(BASE_URL, a["href"])
            if full_url not in seen:
                seen.add(full_url)
                children.append({"text": text, "url": full_url})
    return children


def _discover_rulebook_volumes() -> List[Dict]:
    soup = _fetch(CBB_RULEBOOK_INDEX)
    if not soup:
        log.error("Could not fetch /cbb-rulebook index page")
        return []

    body = soup.find("div", class_="field--name-body")
    if not body:
        log.error("field--name-body not found on /cbb-rulebook")
        return []

    volumes: List[Dict] = []
    current_heading: Optional[str] = None
    current_links:   List[Dict]    = []

    def _flush():
        nonlocal current_heading, current_links
        if current_heading and current_links:
            nav_id = RULEBOOK_VOLUME_NAV_IDS.get(current_heading, "")
            volumes.append({"category": current_heading, "nav_id": nav_id, "links": list(current_links)})
        current_heading = None
        current_links   = []

    for tag in body.descendants:
        if tag.name == "strong" and "h3" in (tag.get("class") or []):
            _flush()
            current_heading = tag.get_text(strip=True)
            current_links   = []
        elif tag.name == "a" and current_heading and tag.get("href"):
            href     = tag["href"]
            full_url = urljoin(BASE_URL, href) if href.startswith("/") else href
            current_links.append({"text": tag.get_text(strip=True), "url": full_url})

    _flush()
    return volumes


# ── Mode 1 — Resolutions ──────────────────────────────────────────────────────

def _get_resolution_links(list_url: str) -> List[Dict]:
    soup = _fetch(list_url)
    if not soup:
        return []

    links = []
    seen  = set()
    nav   = soup.find("nav", {"id": "book-block-menu-2200001"})
    root  = nav if nav else soup

    for a in root.find_all("a", href=True):
        text = a.get_text(strip=True)
        if not re.search(r"Resolution\s+No\.|Regulation\s+No\.", text, re.IGNORECASE):
            continue
        full_url = urljoin(BASE_URL, a["href"])
        if full_url in seen:
            continue
        seen.add(full_url)
        links.append({"text": text, "url": full_url})

    return links


def _scrape_resolution(url: str, category: str) -> Optional[RegulatoryDocument]:
    soup = _fetch(url)
    if not soup:
        return None

    title_tag = soup.find("h2", class_="page-title") or soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else ""
    if not title:
        return None

    resolution_number = ""
    year_int          = None
    published_date    = None
    m = re.search(
        r"(?:Resolution|Regulation)\s+[Nn]o\.\s*\(?(\d+)\)?\s+(?:of|for)\s+(?:the\s+year\s+)?(\d{4})",
        title,
    )
    if m:
        resolution_number = m.group(1)
        year_int          = int(m.group(2))
        try:
            published_date = datetime(year_int, 1, 1).date().isoformat()
        except ValueError:
            pass

    content_html, content_text, download_links, english_pdf, arabic_pdf = _extract_content(soup)
    primary_pdf = english_pdf or arabic_pdf

    prev_resolution = next_resolution = None
    pager = soup.find("div", class_="book-pager")
    if pager:
        for a in pager.find_all("a", href=True):
            rel  = a.get("rel", [])
            href = urljoin(BASE_URL, a["href"])
            text = a.get_text(strip=True)
            if "prev" in rel:
                prev_resolution = {"text": text, "url": href}
            elif "next" in rel:
                next_resolution = {"text": text, "url": href}

    # ── FACTORY: replaces bare RegulatoryDocument(...) ──────────────────────
    return create_cbb_document(
        regulator=REGULATOR, source_system="CBB-Rulebook", category=category,
        title=title, document_url=primary_pdf or url, urdu_url=None,
        published_date=published_date, reference_no=resolution_number,
        department=None, year=year_int, source_page_url=url,
        file_type="PDF" if primary_pdf else None, document_html=content_html,
        extra_meta={
            "resolution_number": resolution_number, "download_links": download_links,
            "org_pdf_link": english_pdf, "arabic_pdf_link": arabic_pdf,
            "content_text": content_text, "prev_resolution": prev_resolution,
            "next_resolution": next_resolution,
        },
        doc_path=[REGULATOR, category, title],
    )


# ── Mode 2 — Book (recursive) ─────────────────────────────────────────────────

def _get_top_level_book_links(list_url: str, nav_id: str) -> List[Dict]:
    soup = _fetch(list_url)
    if not soup:
        return []

    nav = soup.find("nav", {"id": nav_id}) if nav_id else None
    if not nav:
        nav = soup

    links = []
    seen  = set()
    root_ul = nav.find("ul", recursive=False)
    if not root_ul:
        return links

    article_ul = None
    for li in root_ul.find_all("li", recursive=False):
        sub_ul = li.find("ul", recursive=False)
        if sub_ul:
            article_ul = sub_ul
            break

    if article_ul is None:
        article_ul = root_ul

    for li in article_ul.find_all("li", recursive=False):
        a = li.find("a", href=True, recursive=False)
        if not a:
            continue
        text     = a.get_text(strip=True)
        full_url = urljoin(BASE_URL, a["href"])
        if full_url not in seen:
            seen.add(full_url)
            links.append({"text": text, "url": full_url})

    return links


def _scrape_book_page_recursive(
    url: str,
    category: str,
    path_so_far: List[str],
    visited: set,
    delay: float,
) -> List[RegulatoryDocument]:
    if url in visited:
        return []
    visited.add(url)

    time.sleep(delay)
    soup = _fetch(url)
    if not soup:
        return []

    title_tag = soup.find("h2", class_="page-title") or soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else (path_so_far[-1] if path_so_far else "")
    if not title:
        return []

    current_path = path_so_far + [title]
    content_html, content_text, download_links, english_pdf, arabic_pdf = _extract_content(soup)
    primary_pdf = english_pdf or arabic_pdf
    child_links = _extract_book_child_links(soup)

    if not content_html and child_links:
        book_nav_div = soup.find("div", id=re.compile(r"book-navigation"))
        if book_nav_div:
            _make_absolute(book_nav_div)
            content_html = str(book_nav_div)
            content_text = book_nav_div.get_text(separator=" ", strip=True)

    prev_page = next_page = None
    pager = soup.find("div", class_="book-pager")
    if pager:
        for a in pager.find_all("a", href=True):
            rel  = a.get("rel", [])
            href = urljoin(BASE_URL, a["href"])
            text = a.get_text(strip=True)
            if "prev" in rel:
                prev_page = {"text": text, "url": href}
            elif "next" in rel:
                next_page = {"text": text, "url": href}

    # ── FACTORY ─────────────────────────────────────────────────────────────
    doc = create_cbb_document(
        regulator=REGULATOR, source_system="CBB-Rulebook", category=category,
        title=title, document_url=primary_pdf or url, urdu_url=None,
        published_date=None, reference_no=None, department=None, year=None,
        source_page_url=url, file_type="PDF" if primary_pdf else None,
        document_html=content_html,
        extra_meta={
            "download_links": download_links, "org_pdf_link": english_pdf,
            "arabic_pdf_link": arabic_pdf, "content_text": content_text,
            "book_path": current_path, "depth": len(current_path),
            "has_children": bool(child_links), "prev_page": prev_page, "next_page": next_page,
        },
        doc_path=[REGULATOR, "CBB Rulebook", category] + current_path,
    )

    results = [doc]
    for child in child_links:
        child_docs = _scrape_book_page_recursive(
            url=child["url"], category=category, path_so_far=current_path,
            visited=visited, delay=delay,
        )
        results.extend(child_docs)

    return results


# ── Mode 3 — Laws & Regulations ───────────────────────────────────────────────

LAWS_REGULATIONS_BASE = "https://www.cbb.gov.bh"
LAWS_CATEGORY         = "Laws & Regulations"


def _resolve_laws_href(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return urljoin(LAWS_REGULATIONS_BASE, href)


def _scrape_laws_and_regulations(url: str = LAWS_REGULATIONS_URL) -> List[RegulatoryDocument]:
    soup = _fetch(url)
    if not soup:
        return []

    documents: List[RegulatoryDocument] = []
    accordion_headers = soup.find_all(
        "div",
        id=re.compile(r"^uvc-exp-wrap-\d+$"),
        class_=re.compile(r"ult_exp_section"),
    )

    for header_div in accordion_headers:
        title_el = header_div.find("div", class_="ult_expheader")
        title = title_el.get_text(strip=True) if title_el else ""
        if not title:
            continue

        content_div = header_div.find_next_sibling("div", class_="ult_exp_content")
        if not content_div:
            continue

        all_links = content_div.find_all("a", href=True)
        english_pdf: Optional[str] = None
        arabic_pdf:  Optional[str] = None
        all_download_links: List[Dict] = []

        for a in all_links:
            link_text = a.get_text(strip=True).lower()
            href      = _resolve_laws_href(a["href"])

            if "english" in link_text:
                lang = "english"
                if not english_pdf:
                    english_pdf = href
            elif "arabic" in link_text:
                lang = "arabic"
                if not arabic_pdf:
                    arabic_pdf = href
            else:
                parent_text = (a.parent.get_text() if a.parent else "").lower()
                lang = "arabic" if "arabic" in parent_text else "english"
                if lang == "english" and not english_pdf:
                    english_pdf = href
                elif lang == "arabic" and not arabic_pdf:
                    arabic_pdf = href

            all_download_links.append({"text": a.get_text(strip=True), "url": href, "type": "pdf", "language": lang})

        primary_pdf = english_pdf or arabic_pdf
        _make_absolute(content_div, base=LAWS_REGULATIONS_BASE)
        content_text = content_div.get_text(separator=" ", strip=True)

        # ── FACTORY ─────────────────────────────────────────────────────────
        doc = create_cbb_document(
            regulator=REGULATOR, source_system="CBB-Laws-Regulations",
            category=LAWS_CATEGORY, title=title,
            document_url=primary_pdf or url, urdu_url=None,
            published_date=None, reference_no=None, department=None, year=None,
            source_page_url=url, file_type="PDF" if primary_pdf else None,
            document_html=str(content_div),
            extra_meta={
                "org_pdf_link": english_pdf, "arabic_pdf_link": arabic_pdf,
                "download_links": all_download_links,
                "content_text": content_text,
                "has_english_pdf": english_pdf is not None,
                "has_arabic_pdf": arabic_pdf is not None,
            },
            doc_path=[REGULATOR, LAWS_CATEGORY, title],
        )
        documents.append(doc)

    return documents


# ── Mode 4 — Capital Market Regulations ──────────────────────────────────────

_EXTERNAL_PDF_DOMAINS = ("legalaffairs.gov.bh", "www.legalaffairs.gov.bh")


def _is_external_pdf_link(href: str) -> bool:
    return any(domain in href for domain in _EXTERNAL_PDF_DOMAINS)


def _parse_capital_market_list(url: str) -> tuple:
    soup = _fetch(url)
    if not soup:
        return [], [], ""

    body = soup.find("div", class_="field--name-body")
    if not body:
        return [], [], ""

    _make_absolute(body)
    page_html = str(body)

    internal_links: List[Dict] = []
    external_links: List[Dict] = []
    seen = set()

    market_list = body.find("ul", class_="marketlist")
    search_root = market_list if market_list else body

    for a in search_root.find_all("a", href=True):
        href      = a["href"]
        full_url  = urljoin(BASE_URL, href) if href.startswith("/") else href
        link_text = a.get_text(strip=True)

        if not link_text or full_url in seen:
            continue
        seen.add(full_url)

        entry = {"text": link_text, "url": full_url}
        if _is_external_pdf_link(full_url):
            external_links.append(entry)
        else:
            internal_links.append(entry)

    return internal_links, external_links, page_html


def _scrape_external_pdf_doc(link_text: str, link_url: str, source_page_url: str) -> RegulatoryDocument:
    resolution_number = ""
    year_int          = None
    published_date    = None

    m = re.search(r"Resolution\s+[Nn]o\.?\s*\(?(\d+)\)?\s+of\s+(\d{4})", link_text)
    if m:
        resolution_number = m.group(1)
        year_int          = int(m.group(2))
        try:
            published_date = datetime(year_int, 1, 1).date().isoformat()
        except ValueError:
            pass

    # ── FACTORY ─────────────────────────────────────────────────────────────
    # content_text = link_text so the hash is stable and unique per directive
    return create_cbb_document(
        regulator=REGULATOR, source_system="CBB-Capital-Market-Regulations",
        category=CAPITAL_MARKET_CATEGORY, title=link_text,
        document_url=link_url, urdu_url=None, published_date=published_date,
        reference_no=resolution_number or None, department=None, year=year_int,
        source_page_url=source_page_url, file_type="PDF", document_html="",
        extra_meta={
            "org_pdf_link": None, "arabic_pdf_link": link_url,
            "download_links": [{"text": link_text, "url": link_url, "type": "pdf", "language": "arabic"}],
            "content_text": link_text,           # ← used as hash input (link text is unique)
            "is_external_pdf": True,
            "has_english_pdf": False, "has_arabic_pdf": True,
        },
        doc_path=[REGULATOR, CAPITAL_MARKET_CATEGORY, link_text],
    )


def _scrape_capital_market_regulations(
    url: str = CAPITAL_MARKET_REGULATIONS_URL,
    request_delay: float = REQUEST_DELAY,
) -> List[RegulatoryDocument]:
    log.info(f"=== Crawling [capital_market]: {CAPITAL_MARKET_CATEGORY} ===")
    internal_links, external_links, page_html = _parse_capital_market_list(url)

    documents: List[RegulatoryDocument] = []
    visited   = set()

    for i, link in enumerate(internal_links, 1):
        log.info(f"[internal {i}/{len(internal_links)}] Recursing into: {link['text'][:60]}")
        time.sleep(request_delay)
        docs = _scrape_book_page_recursive(
            url=link["url"], category=CAPITAL_MARKET_CATEGORY,
            path_so_far=[], visited=visited, delay=request_delay,
        )
        log.info(f"  → {len(docs)} documents from '{link['text'][:40]}'")
        documents.extend(docs)

    for i, link in enumerate(external_links, 1):
        log.info(f"[external {i}/{len(external_links)}] Building doc for: {link['text'][:60]}")
        doc = _scrape_external_pdf_doc(link["text"], link["url"], url)
        documents.append(doc)

    log.info(f"Capital Market Regulations total: {len(documents)} documents")
    return documents


# ═══════════════════════════════════════════════════════════════════════════════
#  MODE 5 — COMPLIANCE (corrected v2)
#  https://www.cbb.gov.bh/compliance/
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_date(text: str) -> Optional[str]:
    """
    Parse a date string like '30th January 2017' → '2017-01-30'.
    Returns ISO date string or None.
    """
    m = re.search(r'(\d{1,2})(?:st|nd|rd|th)?\s+(\w+)\s+(\d{4})', text, re.IGNORECASE)
    if m:
        day        = int(m.group(1))
        month_name = m.group(2).lower()
        year       = int(m.group(3))
        month      = MONTH_MAP.get(month_name)
        if month:
            try:
                return datetime(year, month, day).date().isoformat()
            except ValueError:
                pass
    # fallback: year only
    m2 = re.search(r'\b(\d{4})\b', text)
    if m2:
        return m2.group(1)
    return None


def _make_doc(
    section: str,
    accordion_title: str,
    title: str,
    document_url: str,
    document_html: str = "",
    extra_meta: Optional[Dict] = None,
    published_date: Optional[str] = None,
    reference_no: Optional[str] = None,
    file_type: Optional[str] = None,
) -> RegulatoryDocument:
    """
    Compliance-specific document factory.

    Thin wrapper around create_cbb_document() that fills in the standard
    compliance fields (regulator, source_system, category, source_page_url,
    year derived from published_date) so individual parsers stay concise.

    The content_hash is injected by create_cbb_document() automatically —
    parsers just need to populate extra_meta["content_text"] as usual.
    """
    return create_cbb_document(
        regulator=REGULATOR,
        source_system=SOURCE_SYSTEM,
        category=CATEGORY,
        title=title,
        document_url=document_url,
        urdu_url=None,
        published_date=published_date,
        reference_no=reference_no,
        department=None,
        year=(
            int(published_date[:4])
            if published_date and len(published_date) >= 4 and published_date[:4].isdigit()
            else None
        ),
        source_page_url=COMPLIANCE_URL,
        file_type=file_type,
        document_html=document_html,
        extra_meta=extra_meta or {},
        doc_path=[REGULATOR, CATEGORY, section, accordion_title],
    )


# ─── Accordion content parsers ────────────────────────────────────────────────
# All parsers call _make_doc() → create_cbb_document() → hash injected automatically.

def _parse_text_accordion(content_div: Tag, section: str, accordion_title: str) -> List[RegulatoryDocument]:
    """Pure-text accordions (Compliance Directorate, Committee, Black List, etc.)"""
    html = str(content_div)
    text = content_div.get_text(separator=" ", strip=True)
    doc = _make_doc(
        section=section, accordion_title=accordion_title, title=accordion_title,
        document_url=COMPLIANCE_URL, document_html=html,
        extra_meta={"content_text": text, "is_text_only": True},
        file_type=None,
    )
    doc.doc_path = [REGULATOR, CATEGORY, section, accordion_title]
    return [doc]


def _parse_publications_accordion(content_div: Tag, section: str, accordion_title: str) -> List[RegulatoryDocument]:
    """Compliance Directorate Publications: one doc per link found."""
    docs = []
    for a in content_div.find_all("a", href=True):
        href        = _abs(a["href"])
        link_text   = a.get_text(strip=True)
        parent_text = a.parent.get_text(strip=True) if a.parent else link_text
        doc = _make_doc(
            section=section, accordion_title=accordion_title,
            title=link_text or parent_text, document_url=href,
            document_html=str(a.parent) if a.parent else "",
            extra_meta={
                "content_text": parent_text,
                "download_links": [{"text": link_text, "url": href, "type": "pdf", "language": "english"}],
            },
            file_type="PDF" if _is_pdf(href) else None,
        )
        doc.doc_path = [REGULATOR, CATEGORY, section, accordion_title, link_text]
        docs.append(doc)
    return docs


def _parse_legislation_accordion(content_div: Tag, section: str, accordion_title: str) -> List[RegulatoryDocument]:
    """
    AML/CFT Legislation: numbered decree entries, each with English + Arabic PDFs.
    Pattern: <p>1.Decree Law No. 4 of 2001...<br/><a>English</a>/<a>Arabic</a></p>
    """
    docs = []
    for p in content_div.find_all("p"):
        p_text = p.get_text(separator=" ", strip=True)
        if p_text.startswith("The Kingdom of Bahrain") or not p_text:
            continue

        decree_match = re.match(r'^(\d+)\.\s*(.+?)(?:\s+English|\s+Arabic|$)', p_text, re.DOTALL)
        if not decree_match:
            continue

        entry_num = decree_match.group(1)
        links = p.find_all("a", href=True)
        if not links:
            continue

        full_p_text = p.get_text(separator="\n", strip=True)
        lines = [l.strip() for l in full_p_text.split("\n") if l.strip()]
        title_parts = []
        for line in lines:
            if line in ("English", "Arabic", "English / Arabic", "/"):
                break
            title_parts.append(line)
        title = " ".join(title_parts).strip() or f"Decree Entry {entry_num}"

        english_url = None
        arabic_url  = None
        download_links = []
        for a in links:
            href   = _abs(a["href"])
            a_text = a.get_text(strip=True).lower()
            lang   = "arabic" if a_text == "arabic" else "english"
            if lang == "english" and not english_url:
                english_url = href
            elif lang == "arabic" and not arabic_url:
                arabic_url = href
            download_links.append({"text": a.get_text(strip=True), "url": href, "type": "pdf", "language": lang})

        primary_url = english_url or arabic_url or COMPLIANCE_URL
        doc = _make_doc(
            section=section, accordion_title=accordion_title, title=title,
            document_url=primary_url, document_html=str(p),
            extra_meta={
                "content_text": full_p_text, "english_url": english_url,
                "arabic_url": arabic_url, "download_links": download_links,
                "entry_number": entry_num,
            },
            file_type="PDF" if primary_url != COMPLIANCE_URL else None,
        )
        doc.doc_path = [REGULATOR, CATEGORY, section, accordion_title, title]
        docs.append(doc)

    return docs


def _parse_guidance_papers_accordion(content_div: Tag, section: str, accordion_title: str) -> List[RegulatoryDocument]:
    """
    Guidance Papers: numbered entries, each with Letter + Guidance Paper PDFs.
    Pattern: <p>1. Topic...<br/><a>Letter</a>/<a>Guidance Paper</a></p>
    """
    docs = []
    for p in content_div.find_all("p"):
        p_text = p.get_text(separator="\n", strip=True)
        if not p_text:
            continue

        num_match = re.match(r'^(\d+)\.', p_text)
        if not num_match:
            continue

        entry_num = num_match.group(1)
        links = p.find_all("a", href=True)
        if not links:
            continue

        lines = [l.strip() for l in p_text.split("\n") if l.strip()]
        title_parts = []
        for line in lines:
            if line.lower() in ("letter", "guidance paper", "letter / guidance paper", "/"):
                break
            title_parts.append(line)
        title = " ".join(title_parts).strip() or f"Guidance Paper Entry {entry_num}"

        letter_url         = None
        guidance_paper_url = None
        download_links     = []
        for a in links:
            href   = _abs(a["href"])
            a_text = a.get_text(strip=True).lower()
            if "letter" in a_text:
                if not letter_url:
                    letter_url = href
                download_links.append({"text": a.get_text(strip=True), "url": href, "type": "pdf", "language": "english", "role": "letter"})
            elif "guidance" in a_text:
                if not guidance_paper_url:
                    guidance_paper_url = href
                download_links.append({"text": a.get_text(strip=True), "url": href, "type": "pdf", "language": "english", "role": "guidance_paper"})
            else:
                download_links.append({"text": a.get_text(strip=True), "url": href, "type": "pdf", "language": "english"})

        primary_url = guidance_paper_url or letter_url or COMPLIANCE_URL
        doc = _make_doc(
            section=section, accordion_title=accordion_title, title=title,
            document_url=primary_url, document_html=str(p),
            extra_meta={
                "content_text": p_text, "letter_url": letter_url,
                "guidance_paper_url": guidance_paper_url,
                "download_links": download_links, "entry_number": entry_num,
            },
            file_type="PDF" if primary_url != COMPLIANCE_URL else None,
        )
        doc.doc_path = [REGULATOR, CATEGORY, section, accordion_title, title]
        docs.append(doc)

    return docs


def _parse_directives_section(
    content_div: Tag, section: str, accordion_title: str, sub_section_title: str
) -> List[RegulatoryDocument]:
    """
    Numbered directive entries (CRS / FATCA / BEPS).
    Pattern: <p>1. <a href="...">OG/53/2017 – Title</a><br/>30th January 2017</p>
    """
    docs = []
    for p in content_div.find_all("p"):
        p_text = p.get_text(separator="\n", strip=True)
        if not p_text:
            continue

        num_match = re.match(r'^(\d+)\.\s*', p_text)
        if not num_match:
            continue

        entry_num = num_match.group(1)
        links = p.find_all("a", href=True)
        if not links:
            continue

        a         = links[0]
        href      = _abs(a["href"])
        link_text = a.get_text(strip=True)

        ref_match    = re.match(r'^([A-Z][A-Z0-9/_\-\.]+)', link_text)
        reference_no = ref_match.group(1) if ref_match else None

        lines    = [l.strip() for l in p_text.split("\n") if l.strip()]
        date_str = None
        for line in lines:
            if re.search(r'\d{4}', line) and any(mn in line.lower() for mn in MONTH_MAP):
                date_str = line
                break
        published_date = _parse_date(date_str) if date_str else None

        download_links = []
        for extra_a in links:
            extra_href = _abs(extra_a["href"])
            download_links.append({
                "text": extra_a.get_text(strip=True),
                "url": extra_href,
                "type": "pdf" if _is_pdf(extra_href) else "link",
                "language": "english",
            })

        doc = _make_doc(
            section=section, accordion_title=accordion_title, title=link_text,
            document_url=href, document_html=str(p),
            extra_meta={
                "content_text": p_text, "download_links": download_links,
                "entry_number": entry_num, "sub_section": sub_section_title,
                "date_text": date_str,
            },
            published_date=published_date, reference_no=reference_no,
            file_type="PDF" if _is_pdf(href) else None,
        )
        doc.doc_path = [REGULATOR, CATEGORY, section, accordion_title, sub_section_title, link_text]
        docs.append(doc)

    return docs


def _parse_links_accordion(content_div: Tag, section: str, accordion_title: str) -> List[RegulatoryDocument]:
    """AML/CFT Links and Industry Associations: store as single reference doc."""
    html = str(content_div)
    text = content_div.get_text(separator=" ", strip=True)
    external_links = [
        {"text": a.get_text(strip=True), "url": a["href"]}
        for a in content_div.find_all("a", href=True)
    ]
    doc = _make_doc(
        section=section, accordion_title=accordion_title, title=accordion_title,
        document_url=COMPLIANCE_URL, document_html=html,
        extra_meta={"content_text": text, "external_links": external_links, "is_links_collection": True},
        file_type=None,
    )
    doc.doc_path = [REGULATOR, CATEGORY, section, accordion_title]
    return [doc]


def _parse_mutual_evaluation_accordion(content_div: Tag, section: str, accordion_title: str) -> List[RegulatoryDocument]:
    """AML/CFT Mutual Evaluation: text + optional external PDF."""
    html        = str(content_div)
    text        = content_div.get_text(separator=" ", strip=True)
    links       = []
    primary_url = COMPLIANCE_URL
    file_type   = None

    for a in content_div.find_all("a", href=True):
        href      = a["href"]
        link_text = a.get_text(strip=True)
        links.append({"text": link_text, "url": href, "type": "pdf" if _is_pdf(href) else "link"})
        if _is_pdf(href) and primary_url == COMPLIANCE_URL:
            primary_url = href
            file_type   = "PDF"

    doc = _make_doc(
        section=section, accordion_title=accordion_title, title=accordion_title,
        document_url=primary_url, document_html=html,
        extra_meta={"content_text": text, "download_links": links},
        file_type=file_type,
    )
    doc.doc_path = [REGULATOR, CATEGORY, section, accordion_title]
    return [doc]


def _parse_crs_accordion(content_div: Tag, section: str, accordion_title: str) -> List[RegulatoryDocument]:
    """CRS accordion: intro overview doc + numbered CRS Directives."""
    docs         = []
    wrapper_divs = content_div.find_all("div", class_="wpb_wrapper")
    intro_html   = ""
    intro_text   = ""

    for wpb_div in wrapper_divs:
        h2 = wpb_div.find("h2")
        if not h2:
            inner_text = wpb_div.get_text(separator=" ", strip=True)
            if inner_text:
                intro_html += str(wpb_div)
                intro_text += " " + inner_text

    if intro_text.strip():
        intro_doc = _make_doc(
            section=section, accordion_title=accordion_title,
            title=f"{accordion_title} – Overview",
            document_url=COMPLIANCE_URL, document_html=intro_html,
            extra_meta={"content_text": intro_text.strip(), "is_overview": True},
            file_type=None,
        )
        intro_doc.doc_path = [REGULATOR, CATEGORY, section, accordion_title, f"{accordion_title} – Overview"]
        docs.append(intro_doc)

    found_header = False
    for wpb_div in wrapper_divs:
        h2 = wpb_div.find("h2")
        if h2 and "directives" in h2.get_text(strip=True).lower():
            found_header = True
            continue
        if found_header:
            docs.extend(_parse_directives_section(wpb_div, section, accordion_title, "CRS Directives"))
            found_header = False

    return docs


def _parse_fatca_accordion(content_div: Tag, section: str, accordion_title: str) -> List[RegulatoryDocument]:
    """FATCA accordion: intro overview doc + numbered FATCA Directives."""
    docs         = []
    wrapper_divs = content_div.find_all("div", class_="wpb_wrapper")
    intro_html   = ""
    intro_text   = ""
    found_header = False

    for wpb_div in wrapper_divs:
        h2 = wpb_div.find("h2")
        if h2 and "directives" in h2.get_text(strip=True).lower():
            found_header = True
            if intro_text.strip():
                intro_doc = _make_doc(
                    section=section, accordion_title=accordion_title,
                    title=f"{accordion_title} – Overview",
                    document_url=COMPLIANCE_URL, document_html=intro_html,
                    extra_meta={"content_text": intro_text.strip(), "is_overview": True},
                    file_type=None,
                )
                intro_doc.doc_path = [REGULATOR, CATEGORY, section, accordion_title, f"{accordion_title} – Overview"]
                docs.append(intro_doc)
            continue

        if found_header:
            docs.extend(_parse_directives_section(wpb_div, section, accordion_title, "FATCA Directives"))
            found_header = False
        else:
            inner_text = wpb_div.get_text(separator=" ", strip=True)
            if inner_text:
                intro_html += str(wpb_div)
                intro_text += " " + inner_text

    return docs


def _parse_beps_accordion(content_div: Tag, section: str, accordion_title: str) -> List[RegulatoryDocument]:
    """BEPS accordion: intro overview doc + numbered BEPS Directives."""
    docs         = []
    wrapper_divs = content_div.find_all("div", class_="wpb_wrapper")
    intro_html   = ""
    intro_text   = ""
    found_header = False

    for wpb_div in wrapper_divs:
        h2 = wpb_div.find("h2")
        if h2 and "directives" in h2.get_text(strip=True).lower():
            found_header = True
            if intro_text.strip():
                intro_doc = _make_doc(
                    section=section, accordion_title=accordion_title,
                    title=f"{accordion_title} – Overview",
                    document_url=COMPLIANCE_URL, document_html=intro_html,
                    extra_meta={"content_text": intro_text.strip(), "is_overview": True},
                    file_type=None,
                )
                intro_doc.doc_path = [REGULATOR, CATEGORY, section, accordion_title, f"{accordion_title} – Overview"]
                docs.append(intro_doc)
            continue

        if found_header:
            docs.extend(_parse_directives_section(wpb_div, section, accordion_title, "BEPS Directives"))
            found_header = False
        else:
            inner_text = wpb_div.get_text(separator=" ", strip=True)
            if inner_text:
                intro_html += str(wpb_div)
                intro_text += " " + inner_text

    return docs


# ─── Accordion router ─────────────────────────────────────────────────────────

def _route_accordion(accordion_title: str, content_div: Tag, section: str) -> List[RegulatoryDocument]:
    """Dispatch to the correct parser based on accordion title."""
    t = accordion_title.lower()

    if "compliance directorate publications" in t:
        return _parse_publications_accordion(content_div, section, accordion_title)
    elif "legislation" in t:
        return _parse_legislation_accordion(content_div, section, accordion_title)
    elif "guidance papers" in t:
        return _parse_guidance_papers_accordion(content_div, section, accordion_title)
    elif "common reporting standard" in t or "crs" in t:
        return _parse_crs_accordion(content_div, section, accordion_title)
    elif "foreign account tax compliance" in t or "fatca" in t:
        return _parse_fatca_accordion(content_div, section, accordion_title)
    elif "base erosion" in t or "beps" in t:
        return _parse_beps_accordion(content_div, section, accordion_title)
    elif "links" in t or "associations" in t:
        return _parse_links_accordion(content_div, section, accordion_title)
    elif "mutual evaluation" in t:
        return _parse_mutual_evaluation_accordion(content_div, section, accordion_title)
    else:
        return _parse_text_accordion(content_div, section, accordion_title)


# ─── Section parser ───────────────────────────────────────────────────────────

def _parse_section(section_div: Tag, section_name: str) -> List[RegulatoryDocument]:
    """Parse one top-level section (AML or EOFI) and all its accordions."""
    docs = []

    intro_h2 = section_div.find("h2")
    if intro_h2:
        intro_parts = []
        for sib in intro_h2.find_next_siblings():
            if sib.get("class") and "ult_exp_section_layer" in " ".join(sib.get("class", [])):
                break
            if hasattr(sib, "get_text"):
                t = sib.get_text(strip=True)
                if t:
                    intro_parts.append(str(sib))
        if intro_parts:
            intro_html = "\n".join(intro_parts)
            intro_text = BeautifulSoup(intro_html, "lxml").get_text(separator=" ", strip=True)
            if intro_text:
                intro_doc = _make_doc(
                    section=section_name, accordion_title="Overview",
                    title=f"{section_name} Overview",
                    document_url=COMPLIANCE_URL, document_html=intro_html,
                    extra_meta={"content_text": intro_text, "is_section_intro": True},
                )
                intro_doc.doc_path = [REGULATOR, CATEGORY, section_name, "Overview"]
                docs.append(intro_doc)
                log.info(f"  ✓ Stored section intro: {section_name} Overview")

    accordion_layers = section_div.find_all("div", class_="ult_exp_section_layer")
    log.info(f"  Found {len(accordion_layers)} accordion layers in section: {section_name}")

    for layer in accordion_layers:
        header_div = layer.find("div", class_="ult_expheader")
        if not header_div:
            header_div = layer.find("div", id=re.compile(r"uvc-exp-wrap"))
            accordion_title = header_div.get("data-title", "").strip() if header_div else ""
        else:
            accordion_title = header_div.get_text(strip=True)

        if not accordion_title:
            continue

        content_div = layer.find("div", class_="ult_exp_content")
        if not content_div:
            log.warning(f"  No content div for accordion: {accordion_title}")
            empty_doc = _make_doc(
                section=section_name, accordion_title=accordion_title,
                title=accordion_title, document_url=COMPLIANCE_URL,
                extra_meta={"is_folder": True, "empty": True},
            )
            empty_doc.doc_path = [REGULATOR, CATEGORY, section_name, accordion_title]
            docs.append(empty_doc)
            continue

        log.info(f"    Processing accordion: {accordion_title}")
        accordion_docs = _route_accordion(accordion_title, content_div, section_name)
        docs.extend(accordion_docs)
        log.info(f"    → {len(accordion_docs)} document(s) from '{accordion_title}'")

    return docs


# ─── Main compliance entry point ──────────────────────────────────────────────

def scrape_compliance(url: str = COMPLIANCE_URL) -> List[RegulatoryDocument]:
    """
    Crawl https://www.cbb.gov.bh/compliance/ and return all RegulatoryDocuments.
    Finds div#aml and div#eofi on the single page — no additional HTTP requests.
    """
    log.info(f"=== Crawling Compliance page: {url} ===")

    soup = _fetch(url)
    if not soup:
        log.error("Failed to fetch Compliance page.")
        return []

    all_docs: List[RegulatoryDocument] = []

    aml_div = soup.find("div", id="aml")
    if aml_div:
        log.info("Processing AML section...")
        aml_docs = _parse_section(aml_div, "AML")
        all_docs.extend(aml_docs)
        log.info(f"  AML section: {len(aml_docs)} documents")
    else:
        log.error("Could not find #aml section on Compliance page!")

    eofi_div = soup.find("div", id="eofi")
    if eofi_div:
        log.info("Processing EOFI section...")
        eofi_docs = _parse_section(eofi_div, "EOFI")
        all_docs.extend(eofi_docs)
        log.info(f"  EOFI section: {len(eofi_docs)} documents")
    else:
        log.error("Could not find #eofi section on Compliance page!")

    log.info(f"=== Compliance crawl complete: {len(all_docs)} total documents ===")
    return all_docs


# ═══════════════════════════════════════════════════════════════════════════════
#  CBBCrawler class
# ═══════════════════════════════════════════════════════════════════════════════

class CBBCrawler(BaseCrawler):

    def __init__(self, request_delay: float = REQUEST_DELAY):
        self.request_delay = request_delay

    def _crawl_resolutions(self, list_url: str, category: str) -> List[RegulatoryDocument]:
        documents = []
        links = _get_resolution_links(list_url)
        for i, link in enumerate(links, 1):
            log.info(f"[{i}/{len(links)}] {link['text'][:80]}")
            time.sleep(self.request_delay)
            doc = _scrape_resolution(link["url"], category)
            if doc:
                documents.append(doc)
        return documents

    def _crawl_book(self, list_url: str, category: str, nav_id: str = "") -> List[RegulatoryDocument]:
        top_links = _get_top_level_book_links(list_url, nav_id)
        if not top_links:
            return []

        visited   = set()
        documents = []
        for i, link in enumerate(top_links, 1):
            log.info(f"[{i}/{len(top_links)}] {link['text'][:60]}")
            docs = _scrape_book_page_recursive(
                url=link["url"], category=category, path_so_far=[],
                visited=visited, delay=self.request_delay,
            )
            documents.extend(docs)
        return documents

    def _crawl_laws_and_regulations(self) -> List[RegulatoryDocument]:
        log.info("=== Crawling [accordion]: Laws & Regulations ===")
        return _scrape_laws_and_regulations()

    def _crawl_capital_market_regulations(self) -> List[RegulatoryDocument]:
        return _scrape_capital_market_regulations(
            url=CAPITAL_MARKET_REGULATIONS_URL,
            request_delay=self.request_delay,
        )

    def _crawl_compliance(self) -> List[RegulatoryDocument]:
        return scrape_compliance(url=COMPLIANCE_URL)

    def get_documents(self) -> List[RegulatoryDocument]:
        all_docs: List[RegulatoryDocument] = []

        # Mode 1-2: static sections
        for category, (url, mode) in STATIC_BASE_URLS.items():
            log.info(f"=== Crawling [{mode}]: {category} ===")
            if mode == "resolutions":
                docs = self._crawl_resolutions(url, category)
            elif mode == "book":
                nav_id = STATIC_BOOK_NAV_IDS.get(category, "")
                docs   = self._crawl_book(url, category, nav_id)
            else:
                docs = []
            all_docs.extend(docs)

        # Mode 2 (dynamic): rulebook volumes
        log.info("=== Discovering rulebook volumes dynamically ===")
        volumes = _discover_rulebook_volumes()
        for vol in volumes:
            category = vol["category"]
            nav_id   = vol["nav_id"]
            if not vol["links"]:
                continue
            entry_url = vol["links"][0]["url"]
            docs = self._crawl_book(entry_url, category, nav_id)
            all_docs.extend(docs)

        # Mode 3
        all_docs.extend(self._crawl_laws_and_regulations())

        # Mode 4
        all_docs.extend(self._crawl_capital_market_regulations())

        # Mode 5 (compliance — corrected v2)
        all_docs.extend(self._crawl_compliance())

        return all_docs

    def fetch_documents(self, timeout=None) -> List[RegulatoryDocument]:
        return self.get_documents()