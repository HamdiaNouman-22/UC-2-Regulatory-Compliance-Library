import logging
import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
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
    """Crawler for SAMA Rulebook Circulars"""

    BASE_URL = "https://rulebook.sama.gov.sa/en/sama-circulars"

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.driver = None
        logger.info(f"Initializing SAMARulebookCrawler (headless={headless})")

    def _init_driver(self):
        """Initialize Chrome WebDriver"""
        logger.info("Initializing Chrome WebDriver...")
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument(
            'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(10)
        logger.info("✓ Chrome WebDriver initialized")

    def _close_driver(self):
        """Close WebDriver"""
        if self.driver:
            logger.info("Closing WebDriver...")
            self.driver.quit()
            self.driver = None

    def _select_show_all(self) -> bool:
        """Select 'All' from the DataTables entries dropdown"""
        try:
            logger.info("Attempting to select 'Show All' option...")

            # Wait for table
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'table.circulars'))
            )
            time.sleep(3)

            # Find select element
            select_selectors = [
                'select[name="DataTables_Table_0_length"]',
                'div.dt-length select',
                'select.form-select',
                'div.dataTables_length select',
                'select[name$="_length"]'
            ]

            select_element = None
            for selector in select_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements and elements[0].is_displayed():
                        select_element = elements[0]
                        logger.info(f"✓ Found select element: {selector}")
                        break
                except:
                    continue

            if not select_element:
                logger.error("Could not find select element")
                return False

            # Scroll and select
            self.driver.execute_script("arguments[0].scrollIntoView(true);", select_element)
            time.sleep(1)

            try:
                select = Select(select_element)
                select.select_by_value('-1')
                logger.info("✓ Selected 'All' via value=-1")
            except:
                # JavaScript fallback
                self.driver.execute_script("""
                    var select = document.querySelector('select[name*="length"]');
                    if (select) {
                        select.value = '-1';
                        select.dispatchEvent(new Event('change', { bubbles: true }));
                    }
                """)
                logger.info("✓ Selected 'All' via JavaScript")

            time.sleep(5)

            row_count = len(self.driver.find_elements(By.CSS_SELECTOR, 'table.circulars tbody tr'))
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
        except:
            pass
        return None

    def _extract_table_rows(self) -> List[dict]:
        """Extract all rows from the circulars table"""
        rows_data = []

        try:
            logger.info("Extracting table rows...")
            page_source = self.driver.page_source
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
                    logger.debug(f"✓ Row {idx}: {circular_no}")

                except Exception as e:
                    logger.error(f"Error extracting row {idx}: {e}")
                    continue

            logger.info(f"✓ Extracted {len(rows_data)} rows")

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
            self.driver.get(detail_url)
            time.sleep(3)

            # Look for PDF download link using BeautifulSoup (more reliable)
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')

            # Strategy 1: Look for the specific PDF block
            pdf_block = soup.select_one('#block-associatedpdfblock--2 a.submenu.icopdf[href*=".pdf"]')

            if pdf_block:
                pdf_url = pdf_block.get('href', '')
                if pdf_url:
                    if not pdf_url.startswith('http'):
                        pdf_url = f"https://rulebook.sama.gov.sa{pdf_url}"
                    result['org_pdf_link'] = pdf_url
                    logger.info(f"✓ Found PDF link: {pdf_url}")
            else:
                # Strategy 2: Look for any icopdf class link
                pdf_link = soup.select_one('a.icopdf[href*=".pdf"]')
                if pdf_link:
                    pdf_url = pdf_link.get('href', '')
                    if pdf_url:
                        if not pdf_url.startswith('http'):
                            pdf_url = f"https://rulebook.sama.gov.sa{pdf_url}"
                        result['org_pdf_link'] = pdf_url
                        logger.info(f"✓ Found PDF link: {pdf_url}")
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

                    result['document_html'] = str(content_div)
                    logger.info(f"✓ Extracted document HTML ({len(result['document_html'])} chars)")
                else:
                    logger.warning("Could not find main content area")

            except Exception as e:
                logger.error(f"Error extracting document HTML: {e}")

        except Exception as e:
            logger.error(f"Error in _extract_detail_page: {e}")

        return result
    def fetch_documents(self, limit: Optional[int] = None) -> List[RegulatoryDocument]:
        """Main method to fetch all SAMA circulars"""
        documents = []

        try:
            logger.info("=" * 80)
            logger.info("STARTING SAMA RULEBOOK CRAWLER")
            logger.info("=" * 80)

            self._init_driver()

            logger.info(f"Navigating to {self.BASE_URL}")
            self.driver.get(self.BASE_URL)
            time.sleep(5)

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
                    logger.info(f"✓ Document {i} processed successfully")

                    # Small delay between requests
                    time.sleep(1)

                except Exception as e:
                    logger.error(f"✗ Error processing row {i}: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    continue

            logger.info("\n" + "=" * 80)
            logger.info(f"CRAWLING COMPLETE: {len(documents)} documents extracted")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Critical error in fetch_documents: {e}")
            import traceback
            logger.error(traceback.format_exc())

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

        logger.info(f"✓ Saved {len(documents)} documents to {filename}")


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