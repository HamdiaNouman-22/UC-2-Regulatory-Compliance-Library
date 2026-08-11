import logging
import time
import re
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from bs4 import BeautifulSoup
from typing import List, Optional, Dict
from dataclasses import dataclass, field
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

_SAMA_BASE = "https://rulebook.sama.gov.sa"
_USER_AGENT = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
               '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')


def _parse_sama_date(date_str: Optional[str]):
    """'D/M/YYYY' or 'DD/MM/YYYY' -> date. SAMA's site pads days/months
    inconsistently (e.g. '21/5/2026' vs '21/05/2026' for the same crawl), so
    comparisons must go through this rather than raw string equality."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str.strip(), "%d/%m/%Y").date()
    except ValueError:
        return None


def _norm_title(title: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (title or "")).strip().casefold()


def _absolutify_links(html: str, base_url: str = _SAMA_BASE) -> str:
    """Rewrite all relative href/src values to absolute URLs so they work when rendered outside the origin."""
    if not html:
        return html
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(True):
        for attr in ("href", "src"):
            val = tag.get(attr)
            if val and not val.startswith(("http", "mailto:", "tel:", "#", "javascript:")):
                tag[attr] = urljoin(base_url, val)
    return str(soup)


@dataclass
class RegulatoryDocument:
    """Standard regulatory document model for SECP/SBP."""

    # ---- Identity ----
    regulator: str
    source_system: str
    category: str

    # ---- Title / URLs ----
    title: str
    document_url: str
    urdu_url: Optional[str] = None

    # ---- Metadata ----
    published_date: Optional[str] = None
    reference_no: Optional[str] = None
    fingerprint: Optional[str] = None

    # ---- Folder / compliance category ----
    compliancecategory_id: Optional[int] = None
    doc_path: Optional[list] = None

    # ---- SBP Context / optional ----
    department: Optional[str] = None
    year: Optional[str] = None

    # ---- Source Page ----
    source_page_url: Optional[str] = None

    file_type: Optional[str] = None
    extra_meta: Dict = field(default_factory=dict)

    # ---- HTML content ----
    document_html: Optional[str] = None

    # ---- DB assigned ID ----
    id: Optional[int] = None


class SAMARulebookCrawler:
    """Crawler for SAMA Rulebook Circulars.

    Playwright-based (not Selenium): the site's own Chrome + a Selenium-Manager
    -downloaded chromedriver.exe both hit an enterprise WDAC/Smart App Control
    block on locked-down Windows machines (chromedriver isn't signed to the
    required "Enterprise" level, and that re-triggers every time Chrome
    auto-updates and forces a fresh driver download). Playwright bundles its
    own browser binary and does not hit that same block.
    """

    BASE_URL = "https://rulebook.sama.gov.sa/en/sama-circulars"

    def __init__(self, headless: bool = True, request_delay: float = 1.0):
        self.headless = headless
        self.request_delay = request_delay
        self._playwright = None
        self._browser = None
        self._context = None
        self.page = None
        logger.info(f"Initializing SAMARulebookCrawler (headless={headless}, request_delay={request_delay}s)")

    def _init_driver(self):
        """Launch Playwright Chromium."""
        logger.info("Launching Playwright Chromium...")
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=self.headless)
        self._context = self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=_USER_AGENT,
        )
        self.page = self._context.new_page()
        logger.info("Playwright Chromium launched")

    def _close_driver(self):
        """Close the browser.

        Runs from fetch_documents' finally block. If the browser already
        crashed mid-crawl, closing it can itself raise -- letting that
        escape here would blow up the finally block and lose every document
        already collected before it could be returned/saved, so it's
        swallowed and just logged.
        """
        for closer, label in (
            (self._context, "context"),
            (self._browser, "browser"),
            (self._playwright, "playwright"),
        ):
            if not closer:
                continue
            try:
                closer.close() if label != "playwright" else closer.stop()
            except Exception as e:
                logger.warning(f"Failed to close {label} (browser likely already crashed): {e}")
        self.page = self._context = self._browser = self._playwright = None

    def _select_show_all(self) -> bool:
        """Select 'All' from the DataTables entries dropdown"""
        try:
            logger.info("Attempting to select 'Show All' option...")

            # Wait for table
            self.page.wait_for_selector('table.circulars', timeout=20000)
            time.sleep(0.5)  # let DataTables finish initializing its JS over the table

            # Find select element
            select_selectors = [
                'select[name="DataTables_Table_0_length"]',
                'div.dt-length select',
                'select.form-select',
                'div.dataTables_length select',
                'select[name$="_length"]'
            ]

            select_locator = None
            for selector in select_selectors:
                try:
                    loc = self.page.locator(selector)
                    if loc.count() > 0 and loc.first.is_visible():
                        select_locator = loc.first
                        logger.info(f"Found select element: {selector}")
                        break
                except Exception:
                    continue

            if not select_locator:
                logger.error("Could not find select element")
                return False

            # Scroll and select
            select_locator.scroll_into_view_if_needed()
            time.sleep(0.3)

            try:
                select_locator.select_option('-1')
                logger.info("Selected 'All' via value=-1")
            except Exception:
                # JavaScript fallback
                self.page.evaluate("""
                    () => {
                        var select = document.querySelector('select[name*="length"]');
                        if (select) {
                            select.value = '-1';
                            select.dispatchEvent(new Event('change', { bubbles: true }));
                        }
                    }
                """)
                logger.info("Selected 'All' via JavaScript")

            # Poll instead of a blind sleep -- DataTables usually finishes
            # rendering ~685 rows well under the old fixed 5s wait.
            try:
                self.page.wait_for_function(
                    "() => document.querySelectorAll('table.circulars tbody tr').length > 10",
                    timeout=12000,
                )
            except PWTimeoutError:
                pass

            row_count = self.page.locator('table.circulars tbody tr').count()
            logger.info(f"Rows visible: {row_count}")

            return row_count > 10

        except Exception as e:
            logger.error(f"Error in _select_show_all: {e}")
            return False

    def _extract_year_from_date(self, date_str: str) -> Optional[str]:
        """Extract year from date string"""
        if not date_str:
            return None
        try:
            # Try to extract 4-digit year
            year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
            if year_match:
                return year_match.group(0)

            # Try DD/MM/YYYY format
            parts = date_str.split('/')
            if len(parts) == 3 and len(parts[2]) == 4:
                return parts[2]
        except Exception:
            pass
        return None

    def _extract_table_rows(self) -> List[dict]:
        """Extract all rows from the circulars table"""
        rows_data = []

        try:
            logger.info("Extracting table rows...")
            page_source = self.page.content()
            soup = BeautifulSoup(page_source, 'html.parser')

            table = soup.find('table', class_='circulars')
            if not table:
                logger.error("Table not found")
                return rows_data

            tbody = table.find('tbody') or table
            rows = tbody.find_all('tr')
            logger.info(f"Found {len(rows)} rows")

            for idx, row in enumerate(rows, 1):
                try:
                    cells = row.find_all('td')
                    if len(cells) < 6:
                        continue

                    # Extract circular number and link
                    circular_no_cell = cells[0]
                    circular_no_link = circular_no_cell.find('a')
                    if circular_no_link:
                        circular_no = circular_no_link.get_text(strip=True)
                        detail_url = circular_no_link.get('href', '')
                        if detail_url and not detail_url.startswith('http'):
                            detail_url = f"https://rulebook.sama.gov.sa{detail_url}"
                    else:
                        circular_no = circular_no_cell.get_text(strip=True)
                        detail_url = None

                    # Extract title
                    title_cell = cells[1]
                    title_link = title_cell.find('a')
                    if title_link:
                        title = title_link.get_text(strip=True)
                        if not detail_url:
                            detail_url = title_link.get('href', '')
                            if detail_url and not detail_url.startswith('http'):
                                detail_url = f"https://rulebook.sama.gov.sa{detail_url}"
                    else:
                        title = title_cell.get_text(strip=True)

                    if not detail_url:
                        logger.warning(f"Row {idx}: No detail URL, skipping")
                        continue

                    # Extract other fields
                    issue_date_g = cells[2].get_text(strip=True)
                    issue_date_h = cells[3].get_text(strip=True)
                    status = cells[4].get_text(strip=True)
                    scope = cells[5].get_text(separator='\n', strip=True)

                    row_data = {
                        'circular_no': circular_no,
                        'title': title,
                        'issue_date_gregorian': issue_date_g,
                        'issue_date_hijri': issue_date_h,
                        'status': status,
                        'scope_of_application': scope,
                        'detail_url': detail_url,
                        'row_index': idx
                    }

                    rows_data.append(row_data)
                    logger.debug(f"Row {idx}: {circular_no}")

                except Exception as e:
                    logger.error(f"Error extracting row {idx}: {e}")
                    continue

            logger.info(f"Extracted {len(rows_data)} rows")

        except Exception as e:
            logger.error(f"Error in _extract_table_rows: {e}")

        return rows_data

    def _extract_detail_page(self, detail_url: str) -> dict:
        """Extract PDF link and document HTML from detail page"""
        result = {
            'org_pdf_link': None,
            'document_html': None
        }

        try:
            logger.info(f"Visiting detail page: {detail_url}")
            self.page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(1.5)

            # Look for PDF download link using BeautifulSoup (more reliable)
            page_source = self.page.content()
            soup = BeautifulSoup(page_source, 'html.parser')

            # Strategy 1: Look for the specific PDF block
            pdf_block = soup.select_one('#block-associatedpdfblock--2 a.submenu.icopdf[href*=".pdf"]')

            if pdf_block:
                pdf_url = pdf_block.get('href', '')
                if pdf_url:
                    if not pdf_url.startswith('http'):
                        pdf_url = f"https://rulebook.sama.gov.sa{pdf_url}"
                    result['org_pdf_link'] = pdf_url
                    logger.info(f"Found PDF link: {pdf_url}")
            else:
                # Strategy 2: Look for any icopdf class link
                pdf_link = soup.select_one('a.icopdf[href*=".pdf"]')
                if pdf_link:
                    pdf_url = pdf_link.get('href', '')
                    if pdf_url:
                        if not pdf_url.startswith('http'):
                            pdf_url = f"https://rulebook.sama.gov.sa{pdf_url}"
                        result['org_pdf_link'] = pdf_url
                        logger.info(f"Found PDF link: {pdf_url}")
                else:
                    logger.warning("PDF download link not found on this circular page")

            # Extract document content
            try:
                # Look specifically in the main content area
                content_div = soup.select_one('div.node__content')

                if content_div:
                    # Remove unwanted elements
                    for tag in content_div.find_all(['script', 'style', 'nav', 'header', 'footer']):
                        tag.decompose()

                    # Remove info table and notification divs
                    for tag in content_div.find_all(['table'], class_='info-table'):
                        tag.decompose()
                    for tag in content_div.find_all(['div'], class_='book-notification'):
                        tag.decompose()

                    result['document_html'] = _absolutify_links(str(content_div))
                    logger.info(f"Extracted document HTML ({len(result['document_html'])} chars)")
                else:
                    logger.warning("Could not find main content area")

            except Exception as e:
                logger.error(f"Error extracting document HTML: {e}")

        except Exception as e:
            logger.error(f"Error in _extract_detail_page: {e}")

        return result

    def fetch_documents(self, limit: Optional[int] = None,
                         known_documents: Optional[List[Dict[str, str]]] = None) -> List[RegulatoryDocument]:
        """Main method to fetch all SAMA circulars.

        known_documents: optional list of {"title": ..., "published_date": ...}
        already stored (e.g. from the DB). The circulars table lists newest
        first, so this is used two ways:
          1. Early stop: once a row's issue date is older than the latest
             known date, everything after it is assumed already stored and
             the rest of the table is not visited at all.
          2. Same-day dedup: rows whose date matches a known date are only
             skipped (no detail-page visit) if their title also matches one
             already known for that date -- two circulars can share a date.
        Dates are parsed rather than string-compared, since SAMA's site pads
        the day inconsistently (e.g. '21/5/2026' vs '21/05/2026').
        """
        documents = []
        skipped_unchanged = 0
        stopped_early_at = None

        known_by_date: Dict = {}
        latest_known_date = None
        if known_documents:
            for k in known_documents:
                d = _parse_sama_date(k.get("published_date"))
                if d is None:
                    continue
                known_by_date.setdefault(d, set()).add(_norm_title(k.get("title")))
                if latest_known_date is None or d > latest_known_date:
                    latest_known_date = d

        try:
            logger.info("=" * 80)
            logger.info("STARTING SAMA RULEBOOK CRAWLER")
            logger.info("=" * 80)

            self._init_driver()

            logger.info(f"Navigating to {self.BASE_URL}")
            self.page.goto(self.BASE_URL, wait_until="domcontentloaded", timeout=60000)
            # No blind sleep here -- goto() already blocks for DOMContentLoaded,
            # and _select_show_all() below waits explicitly for the table.

            # Select "Show All"
            show_all_success = self._select_show_all()

            if not show_all_success:
                logger.warning("Failed to select 'Show All', continuing anyway...")

            # Extract table rows
            rows_data = self._extract_table_rows()

            if not rows_data:
                logger.error("No rows extracted from table")
                return documents

            logger.info(f"Processing {len(rows_data)} circulars...")

            # Apply limit if specified
            if limit:
                rows_data = rows_data[:limit]
                logger.info(f"Limited to first {limit} documents")

            # Process each row
            for i, row in enumerate(rows_data, 1):
                try:
                    if known_documents is not None:
                        row_date = _parse_sama_date(row['issue_date_gregorian'])

                        if row_date and latest_known_date and row_date < latest_known_date:
                            logger.info(
                                f"Row {i}: issue date {row_date} is older than the latest known "
                                f"circular ({latest_known_date}) -- stopping, rest of the table is "
                                f"assumed already stored ({len(rows_data) - i + 1} rows not visited)."
                            )
                            stopped_early_at = i
                            break

                        if row_date and row_date in known_by_date:
                            if _norm_title(row['title']) in known_by_date[row_date]:
                                skipped_unchanged += 1
                                continue
                            # Same date, different title -- a distinct circular, fetch it.

                    logger.info(f"\n[{i}/{len(rows_data)}] Processing: {row['circular_no']} - {row['title'][:50]}...")

                    # Extract detail page data
                    detail_data = self._extract_detail_page(row['detail_url'])

                    # Parse dates
                    published_date = row['issue_date_gregorian']
                    year = self._extract_year_from_date(published_date)

                    # Create RegulatoryDocument
                    doc = RegulatoryDocument(
                        regulator="SAMA",
                        source_system="SAMA RULEBOOK",
                        category="SAMA Circulars",
                        title=row['title'],
                        document_url=row['detail_url'],
                        urdu_url=None,
                        published_date=published_date,
                        reference_no=row['circular_no'],
                        department=None,
                        year=year,
                        source_page_url=self.BASE_URL,
                        file_type="PDF" if detail_data['org_pdf_link'] else None,
                        extra_meta={
                            "org_pdf_link": detail_data['org_pdf_link'],
                            "scope_of_application": row['scope_of_application'],
                            "status": row['status'],
                            "issue_date_hijri": row['issue_date_hijri']
                        },
                        document_html=detail_data['document_html'],
                    )
                    doc.doc_path= [doc.regulator, doc.source_system, doc.category, doc.title]
                    documents.append(doc)
                    logger.info(f"Document {i} processed successfully")

                    # Delay between requests (politeness / avoid rate limiting)
                    time.sleep(self.request_delay)

                except Exception as e:
                    logger.error(f"Error processing row {i}: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    continue

            logger.info("\n" + "=" * 80)
            logger.info(
                f"CRAWLING COMPLETE: {len(documents)} documents extracted"
                + (f", {skipped_unchanged} skipped (already up to date)" if known_documents is not None else "")
                + (f", stopped early at row {stopped_early_at}/{len(rows_data)}" if stopped_early_at else "")
            )
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Critical error in fetch_documents: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Re-raise rather than returning an empty list: callers save
            # documents to disk keyed on this return value, and a crash
            # before any row was even read must not look identical to a
            # legitimate "found 0 new circulars" result -- that would
            # silently overwrite good prior output with nothing.
            raise

        finally:
            self._close_driver()

        return documents

    def save_to_json(self, documents: List[RegulatoryDocument], filename: str = "sama_circulars.json"):
        """Save documents to JSON file"""
        import json
        from dataclasses import asdict

        data = [asdict(doc) for doc in documents]

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved {len(documents)} documents to {filename}")


# Example usage
if __name__ == "__main__":
    # Create crawler (set headless=False to see browser)
    Circularcrawler = SAMARulebookCrawler(headless=False)

    # Fetch documents (limit to 5 for testing)
    documents = Circularcrawler.fetch_documents(limit=12)

    # Print summary
    print("\n" + "=" * 80)
    print(f"Extracted {len(documents)} documents")
    print("=" * 80)

    # Show first document as example
    if documents:
        doc = documents[0]
        print(f"\nExample Document:")
        print(f"  Title: {doc.title}")
        print(f"  Reference No: {doc.reference_no}")
        print(f"  Published: {doc.published_date}")
        print(f"  Year: {doc.year}")
        print(f"  Status: {doc.extra_meta.get('status')}")
        print(f"  PDF Link: {doc.extra_meta.get('org_pdf_link')}")
        print(f"  Document HTML Length: {len(doc.document_html) if doc.document_html else 0} chars")
        print(f"  Doc Path: {doc.doc_path}")

    # Save to JSON
    Circularcrawler.save_to_json(documents)
