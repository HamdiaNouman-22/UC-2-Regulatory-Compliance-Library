"""
sama_rulebook_crawler.py
========================
Generic recursive crawler for the entire SAMA Rulebook.

Starts from https://rulebook.sama.gov.sa/en, auto-discovers every sector box
on the homepage, then recurses through each sector's category tree exactly the
way sama_finance_sector_crawler.py does for Finance Sector — no fixed URLs.

Usage:
    python crawler/sama_rulebook_crawler.py                        # all sectors
    python crawler/sama_rulebook_crawler.py --sector "Banking Sector"
    python crawler/sama_rulebook_crawler.py --sector "Banking Sector" --limit 3
    python crawler/sama_rulebook_crawler.py --selenium             # Selenium backend
"""

import json
import logging
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlsplit, urlunsplit

# Allow running as `python crawler/sama_rulebook_crawler.py` (not just -m)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
from bs4 import BeautifulSoup, Tag

from models.models import RegulatoryDocument
from crawler.fingerprint import stamp_content_hashes

# The stored regulator name. Full name then acronym is the house style, and
# this string is ALSO the first crumb of doc_path, so it is the root folder of
# SAMA's tree. Changed 2026-08-15: it was the bare acronym "SAMA", while the
# library already held the full name — a crawl would have created a SECOND
# regulator beside the 6,101 rows already stored.
SAMA_REGULATOR = "Saudi Arabian Monetary Authority (SAMA)"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ── constants ──────────────────────────────────────────────────────────────────

BASE_URL      = "https://rulebook.sama.gov.sa"
RULEBOOK_HOME = f"{BASE_URL}/en"
REQUEST_DELAY = 1.2
MAX_RETRIES   = 3
USER_AGENT    = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Sectors handled by dedicated crawlers — skip during generic crawl
SKIP_SECTORS = {"SAMA Circulars"}

# Hardcoded sector seed URLs, bypassing homepage auto-discovery (_discover_sectors)
# -- the rulebook homepage (/en) has proven unreliable to fetch (DNS/connectivity
# issues observed repeatedly), while each sector's own landing page is stable.
KNOWN_SECTORS = [
    ("Laws and Implementing Regulations", f"{BASE_URL}/en/book-category/1361"),
    ("All Financial Institutions",        f"{BASE_URL}/en/book-category/1362"),
    ("Banking Sector",                    f"{BASE_URL}/en/book-category/1363"),
    ("Finance Sector",                    f"{BASE_URL}/en/book-category/1365"),
    ("Payment Systems",                   f"{BASE_URL}/en/book-category/1367"),
    ("Money Exchange Sector",             f"{BASE_URL}/en/book-category/1366"),
    ("Credit Bureaus",                    f"{BASE_URL}/en/book-category/5902"),
    ("Regulatory Sandbox",                f"{BASE_URL}/en/book-category/1368"),
]

OUTPUT_DIR = Path("output") / "sama_rulebook"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

_GENERIC_LINK_TEXT = re.compile(
    r"^(click here|here|view|view here|download|read more|see here|see more|link|more)\.?$",
    re.IGNORECASE,
)


# ── helpers ────────────────────────────────────────────────────────────────────

def _abs(href: str) -> str:
    return urljoin(BASE_URL, href) if href else href


def _absolutify_links(html: str) -> str:
    """Rewrite all relative href/src values to absolute SAMA URLs."""
    if not html:
        return html
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(True):
        for attr in ("href", "src"):
            val = tag.get(attr)
            if val and not val.startswith(("http", "mailto:", "tel:", "#", "javascript:")):
                tag[attr] = urljoin(BASE_URL, val)
    return str(soup)


def _canonical(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), "", ""))


def _safe_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>| ]+', "_", name).strip("_")


# ── node dataclass ─────────────────────────────────────────────────────────────

@dataclass
class _Node:
    title: str
    url: str
    children: List["_Node"] = field(default_factory=list)
    is_folder_hint: bool = False


# ── crawler ────────────────────────────────────────────────────────────────────

class SAMAFullRulebookCrawler:
    """
    Recursive crawler for the full SAMA Rulebook.

    Discovers sectors dynamically from the homepage — no hardcoded URLs.
    Each sector gets its own output JSON; a combined file is also written.
    """

    def __init__(self, use_selenium: bool = False, headless: bool = True):
        self.use_selenium = use_selenium
        self.headless     = headless
        self.driver       = None
        self.session      = requests.Session()
        self.session.headers.update({
            "User-Agent":      USER_AGENT,
            "Accept":          "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })
        # Set per-sector before recursion begins
        self._sector_name: str = ""
        self._sector_url: str  = ""
        self._book_nav_id: str = ""
        logger.info(f"SAMAFullRulebookCrawler init (backend={'selenium' if use_selenium else 'requests'})")

    # ── Selenium lifecycle ─────────────────────────────────────────────────────

    def _init_driver(self):
        from selenium import webdriver
        opts = webdriver.ChromeOptions()
        if self.headless:
            opts.add_argument("--headless")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument(f"user-agent={USER_AGENT}")
        self.driver = webdriver.Chrome(options=opts)
        self.driver.implicitly_wait(10)

    def _close_driver(self):
        if self.driver:
            self.driver.quit()
            self.driver = None

    # ── fetch ──────────────────────────────────────────────────────────────────

    def _fetch(self, url: str) -> Optional[BeautifulSoup]:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                if self.use_selenium:
                    self.driver.get(url)
                    time.sleep(2)
                    html = self.driver.page_source
                else:
                    resp = self.session.get(url, timeout=30, allow_redirects=True)
                    resp.raise_for_status()
                    html = resp.text
                return BeautifulSoup(html, "html.parser")
            except Exception as e:
                logger.warning(f"Attempt {attempt}/{MAX_RETRIES} failed for {url}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(2 * attempt)
        logger.error(f"Failed to fetch after {MAX_RETRIES} attempts: {url}")
        return None

    # ── sector / category discovery ────────────────────────────────────────────

    def _discover_sectors(self) -> List[_Node]:
        """Auto-discover all sector boxes from the SAMA Rulebook homepage."""
        logger.info(f"Discovering sectors from {RULEBOOK_HOME}")
        soup = self._fetch(RULEBOOK_HOME)
        if not soup:
            return []
        nodes, seen = [], set()
        for row in soup.find_all("div", class_="views-row"):
            a = row.find("a", href=True)
            if not a:
                continue
            href = a["href"]
            url  = urljoin(BASE_URL + "/", href.lstrip("/"))
            key  = _canonical(url)
            if key in seen:
                continue
            seen.add(key)
            title = a.get_text(strip=True)
            if title in SKIP_SECTORS:
                logger.info(f"  Skipping '{title}' (handled by dedicated crawler)")
                continue
            nodes.append(_Node(title=title, url=url, is_folder_hint=True))
            logger.info(f"  Sector: {title}  ->  {url}")
        logger.info(f"Found {len(nodes)} sectors to crawl")
        return nodes

    def _discover_categories(self, sector_url: str) -> List[_Node]:
        """Auto-discover sub-category boxes from a sector landing page."""
        time.sleep(REQUEST_DELAY)
        soup = self._fetch(sector_url)
        if not soup:
            return []
        nodes, seen = [], set()
        for row in soup.find_all("div", class_="views-row"):
            a = row.find("a", href=True)
            if not a:
                continue
            href = a["href"]
            url  = urljoin(BASE_URL + "/", href.lstrip("/"))
            key  = _canonical(url)
            if key in seen:
                continue
            seen.add(key)
            title = a.get_text(strip=True)
            if title:
                nodes.append(_Node(title=title, url=url, is_folder_hint=True))
        logger.info(f"  Found {len(nodes)} categories in {self._sector_name}")
        return nodes

    def _book_nav_id_for(self, sector_url: str) -> str:
        """
        Derive book-block-menu-XXXX from the sector URL.
        /en/book-category/1365 -> book-block-menu-1365
        Falls back to detecting any book-block-menu nav on the first category page.
        """
        m = re.search(r"/book-category/(\d+)", sector_url)
        if m:
            return f"book-block-menu-{m.group(1)}"
        return ""  # will be auto-detected in _expand_node

    # ── sidebar parsing ────────────────────────────────────────────────────────

    @staticmethod
    def _li_is_folder(li: Tag) -> bool:
        classes = li.get("class", [])
        return "menu-item--collapsed" in classes or "menu-item--expanded" in classes

    def _parse_ul(self, ul: Optional[Tag]) -> List[_Node]:
        if not ul:
            return []
        nodes = []
        for li in ul.find_all("li", recursive=False):
            a = li.find("a", href=True)
            if not a:
                continue
            title     = a.get_text(strip=True)
            url       = _abs(a["href"])
            is_folder = self._li_is_folder(li)
            child_ul  = li.find("ul", recursive=False)
            children  = self._parse_ul(child_ul) if (child_ul and is_folder) else []
            nodes.append(_Node(title=title, url=url, children=children, is_folder_hint=is_folder))
        return nodes

    def _find_book_nav(self, soup: BeautifulSoup) -> Optional[Tag]:
        """Find the book sidebar nav, auto-detecting the ID if needed."""
        if self._book_nav_id:
            nav = soup.find("nav", id=self._book_nav_id)
            if nav:
                return nav
        # Auto-detect: any book-block-menu nav on the page
        nav = soup.find("nav", id=lambda i: i and i.startswith("book-block-menu-"))
        if nav and not self._book_nav_id:
            self._book_nav_id = nav.get("id", "")
            logger.info(f"  Auto-detected book nav ID: {self._book_nav_id}")
        return nav

    def _expand_node(self, url: str) -> Tuple[List[_Node], Optional[BeautifulSoup]]:
        """Fetch url and read its sidebar children."""
        time.sleep(REQUEST_DELAY)
        soup = self._fetch(url)
        if not soup:
            return [], None
        target = _canonical(url)
        nav    = self._find_book_nav(soup)
        if not nav:
            return [], soup
        for li in nav.find_all("li"):
            a = li.find("a", href=True)
            if not a:
                continue
            if _canonical(_abs(a["href"])) == target:
                child_ul = li.find("ul", recursive=False)
                return self._parse_ul(child_ul), soup
        return [], soup

    # ── leaf content extraction ────────────────────────────────────────────────

    def _extract_structured_leaf(self, soup: BeautifulSoup, title: str) -> Optional[Dict]:
        info_table = soup.find("table", class_="info-table")
        if not info_table:
            return None
        result = {
            "reference_no": None, "date_gregorian": None,
            "date_hijri": None,   "status": None,
            "org_pdf_link": None, "document_html": None,
        }
        text = info_table.get_text()
        m = re.search(r"No:\s*([^\s]+)", text)
        if m:
            result["reference_no"] = m.group(1).strip()
        m = re.search(r"Date\(g\):\s*([^\s|]+)", text)
        if m:
            result["date_gregorian"] = m.group(1).strip()
        m = re.search(r"Date\(h\):\s*([^\s]+)", text)
        if m:
            result["date_hijri"] = re.sub(r"Status:.*$", "", m.group(1).strip()).strip()
        span = info_table.find("span", class_="document_status")
        if span:
            result["status"] = span.get_text(strip=True).replace("Status:", "").strip()
        pdf = soup.select_one("a.icopdf[href*='.pdf']")
        if pdf:
            result["org_pdf_link"] = _abs(pdf.get("href", ""))
        content_div = soup.find("div", id="viewall-entire-section") or soup.find("div", class_="node__content")
        if content_div:
            copy_ = BeautifulSoup(str(content_div), "html.parser")
            tbl   = copy_.find("table", class_="info-table")
            if tbl:
                tbl.decompose()
            for tag in copy_.find_all(["script", "style", "nav", "header", "footer"]):
                tag.decompose()
            result["document_html"] = _absolutify_links(str(copy_))
        return result

    @staticmethod
    def _extract_year(date_str: Optional[str]) -> Optional[str]:
        if not date_str:
            return None
        m = re.search(r"\b(19|20)\d{2}\b", date_str)
        return m.group(0) if m else None

    @staticmethod
    def _extract_page_title(soup: BeautifulSoup, fallback: str) -> str:
        if soup.title and soup.title.string:
            title = re.sub(r"\s*\|\s*SAMA Rulebook\s*$", "", soup.title.string.strip()).strip()
            if title and title.lower() not in ("redirecting", ""):
                return title
        return fallback

    @staticmethod
    def _in_notification_box(a: Tag) -> bool:
        """True if `a` sits inside a disclaimer/cross-reference note box
        (div.en-notifications, div.book-notification) rather than the real
        document list — those are inline references within a sentence, not
        distinct documents to crawl."""
        for parent in a.parents:
            classes = parent.get("class") or []
            if any("notification" in c for c in classes):
                return True
        return False

    def _extract_hub_content(self, soup: BeautifulSoup) -> Optional[str]:
        """Capture a listing/hub page's own full body content -- intro text,
        disclaimer/cross-reference notes, and the visible link list itself --
        exactly as rendered on the live page, not just an isolated snippet."""
        content_div = soup.find("div", class_="node__content")
        if not content_div:
            return None
        copy_ = BeautifulSoup(str(content_div), "html.parser")
        for tag in copy_.find_all(["script", "style", "nav", "header", "footer"]):
            tag.decompose()
        text = copy_.get_text(strip=True)
        if not text:
            return None
        return _absolutify_links(str(copy_))

    def _body_links(self, soup: BeautifulSoup) -> List[Tuple[str, str]]:
        content_div = soup.find("div", class_="node__content")
        if not content_div:
            return []
        links = []
        for a in content_div.find_all("a", href=True):
            href = a["href"]
            if not href.startswith("/en/") and not href.startswith(BASE_URL):
                continue
            if self._in_notification_box(a):
                continue
            title = a.get_text(strip=True)
            if title:
                links.append((title, _abs(href)))
        return links

    # ── recursive driver ───────────────────────────────────────────────────────

    def _process(
        self,
        node: _Node,
        path: List[str],
        depth: int,
        visited: Set[str],
        results: List[RegulatoryDocument],
        from_listing: bool = False,
    ) -> None:
        key = _canonical(node.url)

        # Always dedupe by URL, even for cross-reference (from_listing) nodes —
        # paragraphs/footnotes in legal text frequently cross-reference each other
        # in cycles (A -> B -> A), and without this check those cycles recurse
        # until Python's call stack hits RecursionError.
        if key in visited:
            return
        visited.add(key)

        cur_path = path + [node.title]
        indent   = "  " * depth
        logger.info(f"{indent}{'[xref] ' if from_listing else ''}Visiting: {node.title}")

        children  = node.children
        page_soup = None

        if not from_listing and node.is_folder_hint and not children:
            children, page_soup = self._expand_node(node.url)

        if not from_listing and children:
            logger.info(f"{indent}-> folder with {len(children)} children")
            if page_soup is None:
                time.sleep(REQUEST_DELAY)
                page_soup = self._fetch(node.url)
            if page_soup:
                real_title = self._extract_page_title(page_soup, node.title)
                if real_title != node.title:
                    cur_path = cur_path[:-1] + [real_title]
                hub_html = self._extract_hub_content(page_soup)
                if hub_html:
                    hub_doc = RegulatoryDocument(
                        regulator       = SAMA_REGULATOR,
                        source_system   = "SAMA RULEBOOK",
                        category        = self._sector_name,
                        title           = real_title,
                        document_url    = node.url,
                        source_page_url = self._sector_url,
                        file_type       = "HTML",
                        document_html   = hub_html,
                    )
                    hub_doc.doc_path = [SAMA_REGULATOR, "SAMA RULEBOOK", self._sector_name] + cur_path
                    results.append(hub_doc)
                    logger.info(f"{indent}-> hub doc captured (folder page content)")
            for child in children:
                self._process(child, cur_path, depth + 1, visited, results)
            return

        if page_soup is None:
            time.sleep(REQUEST_DELAY)
            page_soup = self._fetch(node.url)
        if not page_soup:
            return

        real_title = self._extract_page_title(page_soup, node.title)
        if real_title != node.title:
            logger.info(f"{indent}  retitled: '{node.title}' -> '{real_title}'")
            cur_path = cur_path[:-1] + [real_title]

        structured = self._extract_structured_leaf(page_soup, real_title)
        if structured:
            year      = self._extract_year(structured["date_gregorian"])
            extra_meta: Dict = {}
            if structured["org_pdf_link"]:
                extra_meta["org_pdf_link"] = structured["org_pdf_link"]
            if structured["status"]:
                extra_meta["status"] = structured["status"]
            if structured["date_hijri"]:
                extra_meta["issue_date_hijri"] = structured["date_hijri"]
            doc = RegulatoryDocument(
                regulator     = SAMA_REGULATOR,
                source_system = "SAMA RULEBOOK",
                category      = self._sector_name,
                title         = real_title,
                document_url  = node.url,
                published_date= structured["date_gregorian"],
                reference_no  = structured["reference_no"],
                year          = year,
                source_page_url = self._sector_url,
                file_type     = "PDF" if structured["org_pdf_link"] else None,
                extra_meta    = extra_meta,
                document_html = structured["document_html"],
            )
            doc.doc_path = [SAMA_REGULATOR, "SAMA RULEBOOK", self._sector_name] + cur_path
            results.append(doc)
            logger.info(f"{indent}-> structured doc captured")
            return

        body_links = self._body_links(page_soup)
        if body_links:
            logger.info(f"{indent}-> listing page ({len(body_links)} links)")
            hub_html = self._extract_hub_content(page_soup)
            if hub_html:
                hub_doc = RegulatoryDocument(
                    regulator       = SAMA_REGULATOR,
                    source_system   = "SAMA RULEBOOK",
                    category        = self._sector_name,
                    title           = real_title,
                    document_url    = node.url,
                    source_page_url = self._sector_url,
                    file_type       = "HTML",
                    document_html   = hub_html,
                )
                hub_doc.doc_path = [SAMA_REGULATOR, "SAMA RULEBOOK", self._sector_name] + cur_path
                results.append(hub_doc)
                logger.info(f"{indent}-> hub doc captured (full page)")
            for link_title, link_url in body_links:
                child       = _Node(title=link_title, url=link_url)
                parent_path = path if _GENERIC_LINK_TEXT.match(link_title.strip()) else cur_path
                self._process(child, parent_path, depth + 1, visited, results, from_listing=True)
            return

        content_div  = page_soup.find("div", class_="node__content")
        text_content = content_div.get_text(strip=True) if content_div else ""
        if text_content:
            doc = RegulatoryDocument(
                regulator     = SAMA_REGULATOR,
                source_system = "SAMA RULEBOOK",
                category      = self._sector_name,
                title         = real_title,
                document_url  = node.url,
                source_page_url = self._sector_url,
                file_type     = "HTML",
                document_html = _absolutify_links(str(content_div)),
            )
            doc.doc_path = [SAMA_REGULATOR, "SAMA RULEBOOK", self._sector_name] + cur_path
            results.append(doc)
            logger.info(f"{indent}-> plain content doc captured")

    # ── public API ─────────────────────────────────────────────────────────────

    def crawl_sector(
        self,
        sector: _Node,
        limit_categories: Optional[int] = None,
    ) -> List[RegulatoryDocument]:
        """Crawl a single sector, return its documents."""
        self._sector_name  = sector.title
        self._sector_url   = sector.url
        self._book_nav_id  = self._book_nav_id_for(sector.url)
        logger.info(f"\n{'='*70}")
        logger.info(f"SECTOR: {sector.title}")
        logger.info(f"  URL       : {sector.url}")
        logger.info(f"  Nav ID    : {self._book_nav_id or '(auto-detect)'}")
        logger.info(f"{'='*70}")

        categories = self._discover_categories(sector.url)
        if limit_categories:
            categories = categories[:limit_categories]

        results: List[RegulatoryDocument] = []
        visited: Set[str] = set()

        for i, cat in enumerate(categories, 1):
            logger.info(f"\n  [{i}/{len(categories)}] {cat.title}")
            self._process(cat, [], 0, visited, results)

        logger.info(f"\nSector '{sector.title}': {len(results)} docs from {len(visited)} pages")
        # Every one of the four RegulatoryDocument branches above omitted
        # content_hash, so all 6,105 stored SAMA rows had no fingerprint and
        # would classify `modified` on every run. Stamped here, at the one exit
        # `fetch_all` also goes through, so a fifth branch cannot reintroduce
        # the gap. See crawler/fingerprint.py.
        return stamp_content_hashes(results)

    def fetch_all(
        self,
        only_sectors: Optional[List[str]] = None,
        limit_categories: Optional[int] = None,
    ) -> Dict[str, List[RegulatoryDocument]]:
        """
        Crawl all sectors discovered from the homepage.
        Returns {sector_name: [RegulatoryDocument, ...]}.
        Each sector is saved to output/sama_rulebook/<sector>.json as it completes.
        """
        if self.use_selenium:
            self._init_driver()

        all_results: Dict[str, List[RegulatoryDocument]] = {}
        try:
            sectors = self._discover_sectors()
            if only_sectors:
                sectors = [s for s in sectors if s.title in only_sectors]
                logger.info(f"Filtered to {len(sectors)} sector(s): {only_sectors}")

            for i, sector in enumerate(sectors, 1):
                logger.info(f"\n[Sector {i}/{len(sectors)}] {sector.title}")
                docs = self.crawl_sector(sector, limit_categories=limit_categories)
                all_results[sector.title] = docs
                self._save_sector(sector.title, docs)

        finally:
            if self.use_selenium:
                self._close_driver()

        # Combined file
        combined = [asdict(d) for docs in all_results.values() for d in docs]
        combined_path = OUTPUT_DIR / "sama_rulebook_all.json"
        combined_path.write_text(
            json.dumps(combined, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        total = sum(len(v) for v in all_results.values())
        logger.info(f"\nAll sectors done: {total} total docs -> {combined_path}")
        return all_results

    def fetch_documents(self, limit: Optional[int] = None) -> List[RegulatoryDocument]:
        """
        Orchestrator-facing entry point: crawl every known SAMA Rulebook sector
        (everything except SAMA Circulars, which has its own dedicated crawler)
        and return the combined flat document list. Uses KNOWN_SECTORS rather
        than homepage auto-discovery -- see KNOWN_SECTORS comment for why.
        """
        if self.use_selenium:
            self._init_driver()

        all_docs: List[RegulatoryDocument] = []
        try:
            for i, (sector_name, sector_url) in enumerate(KNOWN_SECTORS, 1):
                logger.info(f"\n[Sector {i}/{len(KNOWN_SECTORS)}] {sector_name}")
                sector_node = _Node(title=sector_name, url=sector_url, is_folder_hint=True)
                docs = self.crawl_sector(sector_node, limit_categories=limit)
                self._save_sector(sector_name, docs)
                all_docs.extend(docs)
        finally:
            if self.use_selenium:
                self._close_driver()

        logger.info(f"\nfetch_documents complete: {len(all_docs)} total documents")
        return all_docs

    def _save_sector(self, sector_name: str, docs: List[RegulatoryDocument]):
        fname = OUTPUT_DIR / f"{_safe_filename(sector_name)}.json"
        data  = [asdict(d) for d in docs]
        fname.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info(f"  Saved {len(docs)} docs -> {fname}")


# ── entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    use_selenium = "--selenium" in sys.argv
    gen_excel    = "--excel"    in sys.argv

    sector_name = None
    seed_url    = None
    limit       = None

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        a = args[i]
        if a.startswith("--sector="):
            sector_name = a.split("=", 1)[1]
        elif a == "--sector" and i + 1 < len(args):
            sector_name = args[i + 1]; i += 1
        elif a.startswith("--seed-url="):
            seed_url = a.split("=", 1)[1]
        elif a == "--seed-url" and i + 1 < len(args):
            seed_url = args[i + 1]; i += 1
        elif a.startswith("--limit="):
            limit = int(a.split("=", 1)[1])
        i += 1

    crawler = SAMAFullRulebookCrawler(use_selenium=use_selenium)

    # If seed-url is provided we skip homepage discovery entirely
    if seed_url and sector_name:
        sector_node = _Node(title=sector_name, url=seed_url, is_folder_hint=True)
        if crawler.use_selenium:
            crawler._init_driver()
        try:
            docs = crawler.crawl_sector(sector_node, limit_categories=limit)
        finally:
            if crawler.use_selenium:
                crawler._close_driver()
        crawler._save_sector(sector_name, docs)
        results = {sector_name: docs}
    else:
        only_sectors = [sector_name] if sector_name else None
        results = crawler.fetch_all(only_sectors=only_sectors, limit_categories=limit)

    print("\n" + "=" * 70)
    print("SAMA RULEBOOK CRAWL COMPLETE")
    print("=" * 70)
    total = 0
    for sector, docs in results.items():
        structured = sum(1 for d in docs if d.reference_no)
        print(f"  {sector:50s} {len(docs):4d} docs  ({structured} structured)")
        total += len(docs)
    print(f"\n  TOTAL: {total} documents")
    print(f"  Output: {OUTPUT_DIR.resolve()}")

    if gen_excel:
        from dataclasses import asdict as _asdict
        from sama_finance_sector_to_excel import build_excel, EXCEL_DIR
        EXCEL_DIR.mkdir(parents=True, exist_ok=True)
        print("\nGenerating Excel reports...")
        for sector_name, docs in results.items():
            safe = re.sub(r'[\\/*?:"<>| ]', "_", sector_name)
            xlsx_path = EXCEL_DIR / f"SAMA_{safe}.xlsx"
            raw_docs  = [_asdict(d) for d in docs]
            build_excel(raw_docs, xlsx_path, sector_label=sector_name)
            print(f"  -> {xlsx_path}")
        print("Done.")
