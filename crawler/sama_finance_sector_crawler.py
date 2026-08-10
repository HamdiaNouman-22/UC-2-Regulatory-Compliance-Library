"""
SAMA Finance Sector Crawler (pilot)
====================================
Recursively walks the SAMA Rulebook's Finance Sector category tree instead
of relying on fixed URLs, so it picks up everything underneath it
(Licensing Provisions, AML/CFT, Cyber Risk Control, Finance Sector
Circulars, etc.) and not just the flat "Laws and Implementing Regulations"
list the existing sama_laws_and_regs_crawler.py covers.

Site shape (confirmed by inspecting the live pages):
  - https://rulebook.sama.gov.sa/en/book-category/1365 (Finance Sector
    landing page) is a plain grid of category links, no sidebar.
  - Each category sub-page carries a Drupal Book sidebar
    (<nav id="book-navigation-1365">) using the same menu-item--collapsed /
    menu-item--expanded classes as the CBB Rulebook sidebar already parsed
    in cbb_test_crawlers/cbb_rulebook_crawler.py. Only the active branch is
    expanded in the HTML, so children are discovered lazily by fetching a
    node's own page when the sidebar marks it as a folder.
  - A visited node's own div.node__content is one of three shapes:
      1. Structured document  -> table.info-table present (No / Date(g) /
         Date(h) / Status), usually a PDF link. Same shape already parsed
         by sama_laws_and_regs_crawler.py::_extract_law_detail.
      2. Listing/index page   -> no info-table, but the body is a list of
         links to other documents (e.g. "Laws and Regulations" links out
         to 6 laws; "Finance Sector Circulars" links out to 30+ circulars).
         Each linked page is fetched and classified by this same rule.
      3. Plain content page   -> no info-table, no links, just prose
         (e.g. "Types of Licenses"). Captured as-is.

Two fetch backends share the same parsing code so their document_html
output can be compared directly:
    crawler = SAMAFinanceSectorCrawler(use_selenium=False)   # requests
    crawler = SAMAFinanceSectorCrawler(use_selenium=True)    # headless Chrome
"""

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup, Tag

from models.models import RegulatoryDocument

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

_GENERIC_LINK_TEXT = re.compile(
    r'^(click here|here|view|view here|download|read more|see here|see more|link|more)\.?$',
    re.IGNORECASE,
)

BASE_URL = "https://rulebook.sama.gov.sa"
SEED_URL = f"{BASE_URL}/en/book-category/1365"
BOOK_NAV_ID = "book-block-menu-1365"
REQUEST_DELAY = 1.2
MAX_RETRIES = 3
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _abs(href: str) -> str:
    if not href:
        return href
    return urljoin(BASE_URL, href)


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
    """Strip query/fragment and trailing slash so the same page isn't visited twice."""
    parts = urlsplit(url)
    path = parts.path.rstrip("/")
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


@dataclass
class _Node:
    title: str
    url: str
    children: List["_Node"] = field(default_factory=list)
    is_folder_hint: bool = False


class SAMAFinanceSectorCrawler:
    """Recursive crawler for any SAMA Rulebook sector tree.

    Defaults to Finance Sector for backwards compatibility.
    Pass sector_name + seed_url to crawl any other sector.
    """

    @classmethod
    def discover_sectors(cls, session=None) -> List[Tuple[str, str]]:
        """Fetch the SAMA Rulebook homepage and return all (sector_name, seed_url) pairs."""
        import requests as _req
        s = session or _req.Session()
        s.headers.update({"User-Agent": USER_AGENT})
        try:
            resp = s.get(f"{BASE_URL}/en/rulebook", timeout=20)
            soup = BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            logger.error(f"Could not fetch SAMA Rulebook homepage: {e}")
            return []
        seen, sectors = set(), []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/book-category/" not in href:
                continue
            url = urljoin(BASE_URL, href)
            if url in seen:
                continue
            seen.add(url)
            name = a.get_text(strip=True)
            if name:
                sectors.append((name, url))
        logger.info(f"Discovered {len(sectors)} sectors from SAMA Rulebook homepage")
        return sectors

    def __init__(
        self,
        use_selenium: bool = False,
        headless: bool = True,
        sector_name: str = "Finance Sector",
        seed_url: str = SEED_URL,
    ):
        self.use_selenium = use_selenium
        self.headless = headless
        self.sector_name = sector_name
        self.seed_url = seed_url
        # Derive book nav ID from the category number in the seed URL
        m = re.search(r'book-category/(\d+)', seed_url)
        self.book_nav_id = f"book-block-menu-{m.group(1)}" if m else BOOK_NAV_ID
        self.driver = None
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })
        logger.info(
            f"Initialized SAMAFinanceSectorCrawler sector='{sector_name}' "
            f"nav='{self.book_nav_id}' backend="
            f"{'selenium' if use_selenium else 'requests'}"
        )

    # ---- driver lifecycle (selenium backend only) ----

    def _init_driver(self):
        from selenium import webdriver
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument(f'user-agent={USER_AGENT}')
        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(10)
        logger.info("Chrome WebDriver initialized")

    def _close_driver(self):
        if self.driver:
            self.driver.quit()
            self.driver = None

    # ---- fetch (shared by both backends) ----

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

    # ---- sidebar parsing (same shape as cbb_rulebook_crawler.py) ----

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
            title = a.get_text(strip=True)
            url = _abs(a["href"])
            is_folder = self._li_is_folder(li)
            child_ul = li.find("ul", recursive=False)
            children = self._parse_ul(child_ul) if (child_ul and is_folder) else []
            nodes.append(_Node(title=title, url=url, children=children, is_folder_hint=is_folder))
        return nodes

    def _expand_node(self, url: str) -> Tuple[List[_Node], Optional[BeautifulSoup]]:
        """Fetch url and read its sidebar <li> children under BOOK_NAV_ID."""
        time.sleep(REQUEST_DELAY)
        soup = self._fetch(url)
        if not soup:
            return [], None

        target = _canonical(url)
        nav = soup.find("nav", id=self.book_nav_id)
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

    # ---- leaf content extraction ----

    def _extract_structured_leaf(self, soup: BeautifulSoup, title: str) -> Optional[Dict]:
        """Mirror sama_laws_and_regs_crawler.py::_extract_law_detail for a page with table.info-table."""
        info_table = soup.find('table', class_='info-table')
        if not info_table:
            return None

        result = {
            'reference_no': None,
            'date_gregorian': None,
            'date_hijri': None,
            'status': None,
            'org_pdf_link': None,
            'document_html': None,
        }

        text = info_table.get_text()
        no_match = re.search(r'No:\s*([^\s]+)', text)
        if no_match:
            result['reference_no'] = no_match.group(1).strip()
        date_g_match = re.search(r'Date\(g\):\s*([^\s|]+)', text)
        if date_g_match:
            result['date_gregorian'] = date_g_match.group(1).strip()
        date_h_match = re.search(r'Date\(h\):\s*([^\s]+)', text)
        if date_h_match:
            hijri_date = re.sub(r'Status:.*$', '', date_h_match.group(1).strip()).strip()
            result['date_hijri'] = hijri_date
        status_span = info_table.find('span', class_='document_status')
        if status_span:
            result['status'] = status_span.get_text(strip=True).replace('Status:', '').strip()

        pdf_link = soup.select_one('a.icopdf[href*=".pdf"]')
        if pdf_link:
            result['org_pdf_link'] = _abs(pdf_link.get('href', ''))

        content_div = soup.find('div', id='viewall-entire-section') or soup.find('div', class_='node__content')
        if content_div:
            content_copy = BeautifulSoup(str(content_div), 'html.parser')
            info_table_copy = content_copy.find('table', class_='info-table')
            if info_table_copy:
                info_table_copy.decompose()
            for tag in content_copy.find_all(['script', 'style', 'nav', 'header', 'footer']):
                tag.decompose()
            result['document_html'] = _absolutify_links(str(content_copy))

        return result

    @staticmethod
    def _extract_year(date_str: Optional[str]) -> Optional[str]:
        if not date_str:
            return None
        match = re.search(r'\b(19|20)\d{2}\b', date_str)
        return match.group(0) if match else None

    @staticmethod
    def _extract_page_title(soup: BeautifulSoup, fallback: str) -> str:
        """The referring link's anchor text is often generic ("click here", "view PDF").
        The target page's own <title> tag always has the real document name."""
        if soup.title and soup.title.string:
            title = re.sub(r'\s*\|\s*SAMA Rulebook\s*$', '', soup.title.string.strip()).strip()
            if title and title.lower() not in ("redirecting", ""):
                return title
        return fallback

    def _body_links(self, soup: BeautifulSoup) -> List[Tuple[str, str]]:
        """Links found inside the page's own node__content (i.e. content, not nav/sidebar/breadcrumb)."""
        content_div = soup.find('div', class_='node__content')
        if not content_div:
            return []
        links = []
        for a in content_div.find_all('a', href=True):
            href = a['href']
            if not href.startswith('/en/') and not href.startswith(BASE_URL):
                continue
            title = a.get_text(strip=True)
            if title:
                links.append((title, _abs(href)))
        return links

    # ---- recursive driver ----

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

        if from_listing:
            # Cross-reference capture: a listing page linked to this URL even though the
            # sidebar tree may have already placed it elsewhere.  Capture it at this listing
            # path too (replicating SAMA's display), but don't expand the sidebar — just
            # fetch the leaf data.
            pass
        else:
            if key in visited:
                return
            visited.add(key)

        cur_path = path + [node.title]
        indent = "  " * depth
        logger.info(f"{indent}{'[xref] ' if from_listing else ''}Visiting: {node.title} ({node.url})")

        children = node.children
        page_soup = None

        # Sidebar expansion only for the primary tree traversal, not cross-ref captures
        if not from_listing and node.is_folder_hint and not children:
            children, page_soup = self._expand_node(node.url)

        if not from_listing and children:
            logger.info(f"{indent}-> folder with {len(children)} children")
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
            logger.info(f"{indent}  (retitled '{node.title}' -> '{real_title}' from page <title>)")
            cur_path = cur_path[:-1] + [real_title]

        structured = self._extract_structured_leaf(page_soup, real_title)
        if structured:
            year = self._extract_year(structured['date_gregorian'])
            extra_meta = {}
            if structured['org_pdf_link']:
                extra_meta['org_pdf_link'] = structured['org_pdf_link']
            if structured['status']:
                extra_meta['status'] = structured['status']
            if structured['date_hijri']:
                extra_meta['issue_date_hijri'] = structured['date_hijri']

            doc = RegulatoryDocument(
                regulator="SAMA",
                source_system="SAMA RULEBOOK",
                category=self.sector_name,
                title=real_title,
                document_url=node.url,
                published_date=structured['date_gregorian'],
                reference_no=structured['reference_no'],
                year=year,
                source_page_url=self.seed_url,
                file_type="PDF" if structured['org_pdf_link'] else None,
                extra_meta=extra_meta,
                document_html=structured['document_html'],
            )
            doc.doc_path = ["SAMA", "SAMA RULEBOOK", self.sector_name] + cur_path
            results.append(doc)
            logger.info(f"{indent}-> structured document captured")
            return

        body_links = self._body_links(page_soup)
        if body_links:
            logger.info(f"{indent}-> listing page with {len(body_links)} linked documents")
            for link_title, link_url in body_links:
                child = _Node(title=link_title, url=link_url)
                # A generic "click here"-style link is a passthrough — don't add this
                # page's title as a path level (it would duplicate the target's title).
                parent_path = path if _GENERIC_LINK_TEXT.match(link_title.strip()) else cur_path
                # Use from_listing=True so documents already captured via the sidebar tree
                # are still captured here too, replicating SAMA's cross-reference display.
                self._process(child, parent_path, depth + 1, visited, results, from_listing=True)
            return

        content_div = page_soup.find('div', class_='node__content')
        text_content = content_div.get_text(strip=True) if content_div else ""
        if text_content:
            doc = RegulatoryDocument(
                regulator="SAMA",
                source_system="SAMA RULEBOOK",
                category=self.sector_name,
                title=real_title,
                document_url=node.url,
                source_page_url=self.seed_url,
                file_type="HTML",
                document_html=_absolutify_links(str(content_div)),
            )
            doc.doc_path = ["SAMA", "SAMA RULEBOOK", self.sector_name] + cur_path
            results.append(doc)
            logger.info(f"{indent}-> plain content document captured")

    # ---- top-level category discovery ----

    def _collect_top_categories(self) -> List[_Node]:
        """Parse the Finance Sector landing page's box grid (Drupal views-row cards)."""
        soup = self._fetch(self.seed_url)
        if not soup:
            return []

        nodes = []
        seen = set()
        for row in soup.find_all('div', class_='views-row'):
            a = row.find('a', href=True)
            if not a:
                continue
            href = a['href']
            if not href.startswith('/en/'):
                continue
            title = a.get_text(strip=True)
            if not title:
                continue
            url = _abs(href)
            key = _canonical(url)
            if key in seen:
                continue
            seen.add(key)
            nodes.append(_Node(title=title, url=url, is_folder_hint=True))

        logger.info(f"Found {len(nodes)} top-level categories under '{self.sector_name}'")
        return nodes

    # ---- public API ----

    def fetch_documents(self, limit: Optional[int] = None) -> List[RegulatoryDocument]:
        results: List[RegulatoryDocument] = []
        visited: Set[str] = set()

        if self.use_selenium:
            self._init_driver()

        try:
            categories = self._collect_top_categories()
            if limit:
                categories = categories[:limit]
                logger.info(f"Limited to first {limit} categories")

            for i, cat in enumerate(categories, 1):
                logger.info(f"\n[{i}/{len(categories)}] === {cat.title} ===")
                self._process(cat, [], 0, visited, results)

        finally:
            if self.use_selenium:
                self._close_driver()

        logger.info(f"\nDone: {len(results)} documents captured from {len(visited)} pages visited")
        return results

    def save_to_json(self, documents: List[RegulatoryDocument], filename: str = "sama_finance_sector_test.json"):
        import json
        from dataclasses import asdict

        data = [asdict(doc) for doc in documents]
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(documents)} documents to {filename}")


if __name__ == "__main__":
    import sys, re as _re

    use_selenium = "--selenium" in sys.argv
    limit = None
    single_sector = None
    for arg in sys.argv[1:]:
        if arg.startswith("--limit="):
            limit = int(arg.split("=", 1)[1])
        if arg.startswith("--sector="):
            single_sector = arg.split("=", 1)[1]

    # Discover all sectors from the SAMA Rulebook homepage
    print("Discovering SAMA Rulebook sectors...")
    sectors = SAMAFinanceSectorCrawler.discover_sectors()

    if not sectors:
        # Fallback to Finance Sector only if discovery fails
        print("WARNING: sector discovery failed, falling back to Finance Sector only")
        sectors = [("Finance Sector", SEED_URL)]

    if single_sector:
        sectors = [(n, u) for n, u in sectors if single_sector.lower() in n.lower()]
        if not sectors:
            print(f"No sector matched '--sector={single_sector}'. Available:")
            for n, u in SAMAFinanceSectorCrawler.discover_sectors():
                print(f"  {n}")
            sys.exit(1)

    print(f"\nWill crawl {len(sectors)} sector(s):")
    for name, url in sectors:
        print(f"  {name:50s}  {url}")
    print()

    all_results = {}
    for sector_name, seed_url in sectors:
        safe = _re.sub(r'[\\/*?:"<>| ]', "_", sector_name)
        out_file = f"sama_{safe}_requests.json"

        print(f"\n{'='*70}")
        print(f"CRAWLING: {sector_name}")
        print(f"{'='*70}")

        crawler = SAMAFinanceSectorCrawler(
            use_selenium=use_selenium,
            sector_name=sector_name,
            seed_url=seed_url,
        )
        documents = crawler.fetch_documents(limit=limit)
        crawler.save_to_json(documents, out_file)
        all_results[sector_name] = len(documents)
        print(f"  -> {len(documents)} documents saved to {out_file}")

    print("\n" + "=" * 70)
    print("ALL SECTORS COMPLETE")
    print("=" * 70)
    for name, count in all_results.items():
        print(f"  {count:5d}  {name}")
