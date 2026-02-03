import logging
import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
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


class SAMALawsCrawler:
    """Crawler for SAMA Rulebook Laws and Implementing Regulations"""

    BASE_URL = "https://rulebook.sama.gov.sa/en/book-category/1361"

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.driver = None
        logger.info(f"Initializing SAMALawsCrawler (headless={headless})")

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

    def _extract_law_tabs(self) -> List[dict]:
        """Extract all law tabs/links from the main page"""
        tabs_data = []

        try:
            logger.info("Extracting law tabs from main page...")

            # Wait for the page to load
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'a'))
            )
            time.sleep(3)

            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')

            # Find all law links - they appear in boxes on the main page
            # Look for links that contain "Law" or "Regulation" in their text
            law_links = soup.find_all('a', href=True)

            seen_urls = set()  # To avoid duplicates

            for link in law_links:
                href = link.get('href', '')
                title = link.get_text(strip=True)

                # Filter out empty titles
                if not title or len(title) < 3:
                    continue

                # Skip common navigation links
                skip_keywords = ['home', 'search', 'view updates', 'terms and conditions',
                                 'sama rulebook', 'entire section', 'custom print', 'print',
                                 'save as pdf', 'chapter', 'article']
                if any(keyword in title.lower() for keyword in skip_keywords):
                    continue

                # Ensure it's a law/regulation link (contains "Law" or "Regulation")
                if 'law' in title.lower() or 'regulation' in title.lower() or 'rules' in title.lower():
                    # Build full URL
                    if not href.startswith('http'):
                        href = f"https://rulebook.sama.gov.sa{href}"

                    # Avoid duplicates
                    if href in seen_urls:
                        continue

                    seen_urls.add(href)
                    tabs_data.append({
                        'title': title,
                        'url': href
                    })

            logger.info(f"✓ Found {len(tabs_data)} law tabs")

        except Exception as e:
            logger.error(f"Error extracting law tabs: {e}")

        return tabs_data

    def _clean_html_content(self, html_content: str) -> str:
        """Clean and format HTML content, converting tables to readable format"""
        soup = BeautifulSoup(html_content, 'html.parser')

        # Process all tables
        for table in soup.find_all('table'):
            # Check if this is a structural table (used for layout) or a data table
            # Layout tables typically have colspan attributes and minimal actual data
            is_layout_table = False

            # Get all rows
            rows = table.find_all('tr')

            # If table has very few columns and lots of colspans, it's likely layout
            has_colspan = any(td.get('colspan') for row in rows for td in row.find_all(['td', 'th']))

            if has_colspan and len(rows) <= 20:
                # This is likely a layout table - convert to clean text
                is_layout_table = True

                # Extract text content preserving structure
                new_content = []

                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    for cell in cells:
                        cell_text = cell.get_text(strip=True)
                        if cell_text:
                            # Check if it's a numbered item (like "1.", "a.", etc.)
                            if re.match(r'^[0-9]+\.|^[a-z]\)', cell_text):
                                new_content.append(f"<p><strong>{cell_text}</strong></p>")
                            else:
                                new_content.append(f"<p>{cell_text}</p>")

                # Replace table with clean paragraphs
                new_div = soup.new_tag('div', **{'class': 'cleaned-content'})
                new_div.append(BeautifulSoup('\n'.join(new_content), 'html.parser'))
                table.replace_with(new_div)

            else:
                # This is a real data table - keep it but clean it up
                # Remove inline styles
                for tag in table.find_all(True):
                    if tag.has_attr('style'):
                        del tag['style']
                    if tag.has_attr('cellpadding'):
                        del tag['cellpadding']
                    if tag.has_attr('cellspacing'):
                        del tag['cellspacing']
                    if tag.has_attr('border'):
                        del tag['border']

                # Add clean CSS classes
                table['class'] = 'data-table'

        # Clean up nested lists
        for ul in soup.find_all('ul'):
            # Remove inline styles
            if ul.has_attr('style'):
                del ul['style']

        # Clean up excessive nesting
        for tag in soup.find_all(['div', 'span']):
            # If a div/span only contains one child of the same type, unwrap it
            children = list(tag.children)
            if len(children) == 1 and children[0].name == tag.name:
                tag.unwrap()

        return str(soup)

    def _extract_law_detail(self, law_url: str, law_title: str) -> dict:
        """Extract details from a specific law page"""
        result = {
            'reference_no': None,
            'date_gregorian': None,
            'date_hijri': None,
            'status': None,
            'org_pdf_link': None,
            'document_html': None
        }

        try:
            logger.info(f"Visiting law page: {law_url}")
            self.driver.get(law_url)
            time.sleep(3)

            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')

            # Extract metadata from the info-table (this is correct)
            info_table = soup.find('table', class_='info-table')
            if info_table:
                text = info_table.get_text()

                # Extract No
                no_match = re.search(r'No:\s*([^\s]+)', text)
                if no_match:
                    result['reference_no'] = no_match.group(1).strip()
                    logger.info(f"Found No: {result['reference_no']}")

                # Extract Date(g)
                date_g_match = re.search(r'Date\(g\):\s*([^\s|]+)', text)
                if date_g_match:
                    result['date_gregorian'] = date_g_match.group(1).strip()
                    logger.info(f"Found Date(g): {result['date_gregorian']}")

                # Extract Date(h)
                date_h_match = re.search(r'Date\(h\):\s*([^\s]+)', text)
                if date_h_match:
                    # OLD - was capturing "Status:" attached to date
                    result['date_hijri'] = date_h_match.group(1).strip()

                    # NEW - removes "Status:" if attached
                    hijri_date = date_h_match.group(1).strip()
                    hijri_date = re.sub(r'Status:.*$', '', hijri_date).strip()
                    result['date_hijri'] = hijri_date

                # Extract Status
                status_span = info_table.find('span', class_='document_status')
                if status_span:
                    status_text = status_span.get_text(strip=True)
                    result['status'] = status_text.replace('Status:', '').strip()
                    logger.info(f"Found Status: {result['status']}")

            # Extract PDF download link
            try:
                # Wait until the PDF link is present in the page
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located(
                        (By.XPATH,
                         "//a[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download original pdf')]")
                    )
                )

                # Extract it directly via Selenium
                pdf_link = self.driver.find_element(
                    By.XPATH,
                    "//a[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download original pdf')]"
                )
                pdf_url = pdf_link.get_attribute('href')
                if pdf_url and not pdf_url.startswith('http'):
                    pdf_url = f"https://rulebook.sama.gov.sa{pdf_url}"
                result['org_pdf_link'] = pdf_url
                logger.info(f"✓ Found PDF link: {pdf_url}")

                if pdf_link:
                    pdf_url = pdf_link.get('href', '')
                    if pdf_url and not pdf_url.startswith('http'):
                        pdf_url = f"https://rulebook.sama.gov.sa{pdf_url}"
                    result['org_pdf_link'] = pdf_url
                    logger.info(f"✓ Found PDF link: {pdf_url}")

            except Exception as e:
                logger.warning(f"PDF download link not found: {e}")

            # **KEY FIX: Extract document HTML from the correct location**
            try:
                # The actual content is in div with id="book-navigation-1"
                # Inside a div with id="viewall-entire-section"
                content_div = soup.find('div', id='viewall-entire-section')

                if not content_div:
                    # Fallback: try to find it by class
                    content_div = soup.find('div', class_='node__content')

                if content_div:
                    # Clone the content
                    content_copy = BeautifulSoup(str(content_div), 'html.parser')

                    # Remove the metadata table (info-table) - it's already extracted
                    info_table_in_content = content_copy.find('table', class_='info-table')
                    if info_table_in_content:
                        info_table_in_content.decompose()

                    # Remove the title h2 (it's redundant)
                    title_h2 = content_copy.find('h2', class_='page-title', string=lambda x: x and law_title in str(x))
                    if title_h2:
                        title_h2.decompose()

                    # Remove scripts, styles, navigation
                    for tag in content_copy.find_all(['script', 'style', 'nav', 'header', 'footer']):
                        tag.decompose()

                    # **Convert layout tables to clean HTML**
                    result['document_html'] = self._convert_tables_to_clean_html(str(content_copy))
                    logger.info(f"✓ Extracted document HTML ({len(result['document_html'])} chars)")
                else:
                    logger.warning("Could not find content div")

            except Exception as e:
                logger.error(f"Error extracting document HTML: {e}")
                import traceback
                logger.error(traceback.format_exc())

        except Exception as e:
            logger.error(f"Error in _extract_law_detail: {e}")
            import traceback
            logger.error(traceback.format_exc())

        return result

    def _convert_tables_to_clean_html(self, html_content: str) -> str:
        """Convert layout tables to clean paragraph-based HTML"""
        soup = BeautifulSoup(html_content, 'html.parser')

        # Process all tables
        for table in soup.find_all('table'):
            rows = table.find_all('tr')

            if not rows:
                continue

            # Convert table to clean structure
            new_elements = []

            for row in rows:
                cells = row.find_all(['td', 'th'])

                if not cells:
                    continue

                # Check if this is a simple numbered/lettered row
                if len(cells) == 2:
                    first_cell_text = cells[0].get_text(strip=True)

                    # Check if first cell is a number or letter marker
                    if re.match(r'^[0-9]+\.$|^[a-z]\)$|^[a-z]\.$', first_cell_text):
                        # Build content from second cell
                        content_parts = []
                        for element in cells[1].descendants:
                            if element.name == 'a':
                                href = element.get('href', '')
                                if href and not href.startswith('http'):
                                    href = f"https://rulebook.sama.gov.sa{href}"
                                content_parts.append(f'<a href="{href}">{element.get_text(strip=True)}</a>')
                            elif element.name == 'br':
                                continue  # Skip br tags
                            elif isinstance(element, str):
                                text = element.strip()
                                if text and text != '\xa0':
                                    # Skip if this text is already part of a link we processed
                                    if element.parent.name != 'a':
                                        content_parts.append(text)

                        content_html = ' '.join(content_parts).strip()
                        if content_html:
                            p = soup.new_tag('p')
                            strong = soup.new_tag('strong')
                            strong.string = first_cell_text
                            p.append(strong)
                            p.append(' ')

                            # Parse the content to handle links properly
                            content_soup = BeautifulSoup(content_html, 'html.parser')
                            for child in content_soup.children:
                                p.append(child)

                            new_elements.append(p)
                        continue

                # Check if this is a colspan row (introductory text)
                if len(cells) == 1 or (len(cells) > 1 and cells[0].get('colspan')):
                    cell = cells[0]

                    content_parts = []
                    for element in cell.descendants:
                        if element.name == 'a':
                            href = element.get('href', '')
                            if href and not href.startswith('http'):
                                href = f"https://rulebook.sama.gov.sa{href}"
                            content_parts.append(f'<a href="{href}">{element.get_text(strip=True)}</a>')
                        elif element.name == 'br':
                            continue
                        elif isinstance(element, str):
                            text = element.strip()
                            if text and text != '\xa0':
                                if element.parent.name != 'a':
                                    content_parts.append(text)

                    content_html = ' '.join(content_parts).strip()
                    if content_html:
                        p = soup.new_tag('p')
                        content_soup = BeautifulSoup(content_html, 'html.parser')
                        for child in content_soup.children:
                            p.append(child)
                        new_elements.append(p)
                    continue

                # Check for 3-cell rows (nested structure like in Article 8, 10, etc.)
                if len(cells) == 3:
                    first = cells[0].get_text(strip=True)
                    second = cells[1].get_text(strip=True)

                    # Skip empty first cell
                    if not first or first == '\xa0':
                        # It's a sub-item (a, b, c, etc.)
                        if re.match(r'^[a-z]\)$|^[a-z]\.$', second):
                            content_parts = []
                            for element in cells[2].descendants:
                                if element.name == 'a':
                                    href = element.get('href', '')
                                    if href and not href.startswith('http'):
                                        href = f"https://rulebook.sama.gov.sa{href}"
                                    content_parts.append(f'<a href="{href}">{element.get_text(strip=True)}</a>')
                                elif element.name == 'br':
                                    continue
                                elif isinstance(element, str):
                                    text = element.strip()
                                    if text and text != '\xa0':
                                        if element.parent.name != 'a':
                                            content_parts.append(text)

                            content_html = ' '.join(content_parts).strip()
                            if content_html:
                                p = soup.new_tag('p')
                                p['style'] = 'margin-left: 20px;'
                                strong = soup.new_tag('strong')
                                strong.string = second
                                p.append(strong)
                                p.append(' ')

                                content_soup = BeautifulSoup(content_html, 'html.parser')
                                for child in content_soup.children:
                                    p.append(child)

                                new_elements.append(p)
                        continue

                    # It's a main item with colspan=2 for content
                    if re.match(r'^[0-9]+\.$', first):
                        content_parts = []
                        for element in cells[1].descendants:
                            if element.name == 'a':
                                href = element.get('href', '')
                                if href and not href.startswith('http'):
                                    href = f"https://rulebook.sama.gov.sa{href}"
                                content_parts.append(f'<a href="{href}">{element.get_text(strip=True)}</a>')
                            elif element.name == 'br':
                                continue
                            elif isinstance(element, str):
                                text = element.strip()
                                if text and text != '\xa0':
                                    if element.parent.name != 'a':
                                        content_parts.append(text)

                        content_html = ' '.join(content_parts).strip()
                        if content_html:
                            p = soup.new_tag('p')
                            strong = soup.new_tag('strong')
                            strong.string = first
                            p.append(strong)
                            p.append(' ')

                            content_soup = BeautifulSoup(content_html, 'html.parser')
                            for child in content_soup.children:
                                p.append(child)

                            new_elements.append(p)

            # Replace table with clean divs
            if new_elements:
                new_div = soup.new_tag('div')
                new_div['class'] = 'article-content'

                for elem in new_elements:
                    new_div.append(elem)

                table.replace_with(new_div)

        # Clean up list styling
        for ul in soup.find_all('ul'):
            if ul.has_attr('style'):
                del ul['style']

        # Get final HTML
        html_str = str(soup)

        # Clean up excessive spacing
        html_str = re.sub(r'\s+', ' ', html_str)  # Normalize all whitespace to single space
        html_str = re.sub(r'>\s+<', '><', html_str)  # Remove space between tags

        return html_str

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

    def fetch_documents(self, limit: Optional[int] = None, include_appendix3: bool = True) -> List[RegulatoryDocument]:
        """
        Main method to fetch all regulatory documents.

        Fetches:
        1. SAMA laws and regulations
        2. SBP FE Manual Appendix III notifications (if include_appendix3=True)

        Args:
            limit: Optional limit on number of SAMA laws to fetch (for testing)
            include_appendix3: Whether to include SBP Appendix III documents (default: True)

        Returns:
            List of RegulatoryDocument objects from both sources
        """
        documents = []

        try:
            logger.info("=" * 80)
            logger.info("STARTING REGULATORY DOCUMENTS CRAWLER")
            logger.info("=" * 80)

            self._init_driver()

            # ========== PART 1: SAMA LAWS ==========
            logger.info("\n### CRAWLING SAMA LAWS ###\n")

            logger.info(f"Navigating to {self.BASE_URL}")
            self.driver.get(self.BASE_URL)
            time.sleep(5)

            law_tabs = self._extract_law_tabs()

            if not law_tabs:
                logger.error("No law tabs found")
            else:
                logger.info(f"Processing {len(law_tabs)} laws...")

                if limit:
                    law_tabs = law_tabs[:limit]
                    logger.info(f"Limited to first {limit} laws")

                for i, tab in enumerate(law_tabs, 1):
                    try:
                        logger.info(f"\n[{i}/{len(law_tabs)}] Processing: {tab['title']}")

                        detail_data = self._extract_law_detail(tab['url'], tab['title'])

                        year = self._extract_year_from_date(detail_data['date_gregorian'])

                        extra_meta = {}
                        if detail_data['org_pdf_link']:
                            extra_meta['org_pdf_link'] = detail_data['org_pdf_link']
                        if detail_data['status']:
                            extra_meta['status'] = detail_data['status']
                        if detail_data['date_hijri']:
                            extra_meta['issue_date_hijri'] = detail_data['date_hijri']

                        doc = RegulatoryDocument(
                            regulator="SAMA",
                            source_system="SAMA RULEBOOK",
                            category="Laws and Implementing Regulations",
                            title=tab['title'],
                            document_url=tab['url'],
                            urdu_url=None,
                            published_date=detail_data['date_gregorian'],
                            reference_no=detail_data['reference_no'],
                            department=None,
                            year=year,
                            source_page_url=self.BASE_URL,
                            file_type="PDF" if detail_data['org_pdf_link'] else None,
                            extra_meta=extra_meta,
                            document_html=detail_data['document_html'],
                        )
                        doc.doc_path = [doc.regulator, doc.source_system, doc.category, doc.title]

                        documents.append(doc)
                        logger.info(f"✓ Law {i} processed successfully")

                        time.sleep(1)

                    except Exception as e:
                        logger.error(f"✗ Error processing law {i}: {e}")
                        import traceback
                        logger.error(traceback.format_exc())
                        continue

                logger.info(f"\nSAMA LAWS COMPLETE: {len(documents)} laws extracted")

            # ========== PART 2: SBP APPENDIX III ==========
            if include_appendix3:
                logger.info("\n### CRAWLING SBP APPENDIX III ###\n")

                appendix3_docs = self._fetch_appendix3_internal()
                documents.extend(appendix3_docs)

                logger.info(f"SBP APPENDIX III COMPLETE: {len(appendix3_docs)} notifications extracted")

            # ========== FINAL SUMMARY ==========
            logger.info("\n" + "=" * 80)
            logger.info(f"CRAWLING COMPLETE: {len(documents)} total documents extracted")
            logger.info(f"  - SAMA Laws: {len([d for d in documents if d.regulator == 'SAMA'])}")
            logger.info(f"  - SBP Appendix III: {len([d for d in documents if d.regulator == 'SBP'])}")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Critical error in fetch_documents: {e}")
            import traceback
            logger.error(traceback.format_exc())

        finally:
            self._close_driver()

        return documents

    def save_to_json(self, documents: List[RegulatoryDocument], filename: str = "sama_laws.json"):
        """Save documents to JSON file"""
        import json
        from dataclasses import asdict

        data = [asdict(doc) for doc in documents]

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"✓ Saved {len(documents)} documents to {filename}")

    def _extract_notification_number(self, text: str) -> Optional[str]:
        """Extract notification number from SBP Appendix III text"""
        patterns = [
            r'NOTIFICATION\s+NO\.?\s*([A-Z]+\.?\s*[A-Z]+\.?\s*\d+/\d+-?[A-Z]*)',
            r'NO\.?\s*([A-Z]+\.?\s*[A-Z]+\.?\s*\d+/\d+-?[A-Z]*)',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None

    def _extract_sbp_date(self, text: str) -> Optional[str]:
        """Extract date from SBP Appendix III text"""
        patterns = [
            r'DATED\s+THE\s+(\d+(?:ST|ND|RD|TH)?\s+[A-Z]+,?\s+\d{4})',
            r'DATED\s+(\d+(?:ST|ND|RD|TH)?\s+[A-Z]+,?\s+\d{4})',
            r'DATED\s+THE\s+([A-Z]+\s+\d+,?\s+\d{4})',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None

    def _extract_appendix3_sections(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract all Appendix III sections from single page"""
        sections = []

        # All 36 notifications are on one page with anchor links #1 to #36
        for i in range(1, 37):
            anchor = soup.find('a', {'name': str(i)})

            if not anchor:
                logger.warning(f"Anchor #{i} not found")
                continue

            # Get header row (contains notification number and date)
            parent_tr = anchor.find_parent('tr')
            if not parent_tr:
                continue

            header_text = parent_tr.get_text()
            notification_no = self._extract_notification_number(header_text)
            date = self._extract_sbp_date(header_text)

            # Get content row (next sibling after header)
            content_tr = parent_tr.find_next_sibling('tr')
            content_html = ""
            if content_tr:
                content_td = content_tr.find('td')
                if content_td:
                    content_html = str(content_td)

            sections.append({
                'section_number': i,
                'notification_no': notification_no,
                'date': date,
                'content_html': content_html,
                'anchor_name': str(i)
            })

            logger.info(f"✓ Found Appendix III-{i}: {notification_no or 'No ref'}")

        return sections

    # ADD THIS MAIN FETCH METHOD:

    def fetch_appendix3_documents(self, limit: Optional[int] = None) -> List[RegulatoryDocument]:
        """
        Fetch all 36 notifications from SBP FE Manual Appendix III.

        Unlike SAMA laws (separate pages), all Appendix III notifications
        are on ONE page with anchor links (#1, #2, etc.)

        Args:
            limit: Optional limit on number of documents to fetch (for testing)

        Returns:
            List of RegulatoryDocument objects
        """
        documents = []
        base_url = "http://sbp.org.pk/fe_manual/appendix%20files/appendix%203/appendix3.htm"

        try:
            logger.info("=" * 80)
            logger.info("FETCHING SBP APPENDIX III NOTIFICATIONS")
            logger.info("=" * 80)

            # Navigate to the single page containing all notifications
            logger.info(f"Loading: {base_url}")
            self.driver.get(base_url)
            time.sleep(3)

            # Parse the page
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            sections = self._extract_appendix3_sections(soup)

            if not sections:
                logger.error("No sections found")
                return documents

            logger.info(f"Found {len(sections)} sections")

            # Apply limit if specified
            if limit:
                sections = sections[:limit]
                logger.info(f"Limited to first {limit} sections")

            # Process each section
            for section in sections:
                try:
                    section_num = section['section_number']
                    logger.info(f"\n[{section_num}/36] Processing Appendix III-{section_num}")

                    # Extract year
                    year = self._extract_year_from_date(section['date'])

                    # Clean HTML content (reuse your existing method)
                    cleaned_html = ""
                    if section['content_html']:
                        cleaned_html = self._clean_html_content(section['content_html'])

                    # Build title
                    title_parts = [f"APPENDIX III-{section_num}"]
                    if section['notification_no']:
                        title_parts.append(section['notification_no'])
                    if section['date']:
                        title_parts.append(f"dated {section['date']}")
                    title = " - ".join(title_parts)

                    # Create document (same structure as SAMA documents)
                    doc = RegulatoryDocument(
                        regulator="SBP",
                        source_system="SBP FE Manual",
                        category="Appendix III - Foreign Exchange Notifications",
                        title=title,
                        document_url=f"{base_url}#{section['anchor_name']}",
                        published_date=section['date'],
                        reference_no=section['notification_no'],
                        year=year,
                        source_page_url=base_url,
                        file_type="HTML",
                        extra_meta={
                            'section_number': section_num,
                            'appendix': 'III'
                        },
                        document_html=cleaned_html,
                    )

                    # Set folder path
                    doc.doc_path = [
                        "SBP",
                        "SBP FE Manual",
                        "Appendix III - Foreign Exchange Notifications",
                        f"APPENDIX III-{section_num}"
                    ]

                    documents.append(doc)
                    logger.info(f"✓ Section {section_num} processed")

                except Exception as e:
                    logger.error(f"✗ Error processing section {section.get('section_number')}: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    continue

            logger.info("\n" + "=" * 80)
            logger.info(f"APPENDIX III COMPLETE: {len(documents)}/36 notifications")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Critical error in fetch_appendix3_documents: {e}")
            import traceback
            logger.error(traceback.format_exc())

        return documents


# Example usage
if __name__ == "__main__":
    # Create crawler (set headless=False to see browser)
    crawler = SAMALawsCrawler(headless=False)

    # Fetch documents (limit to 3 for testing)
    documents = crawler.fetch_documents(limit=3)

    # Print summary
    print("\n" + "=" * 80)
    print(f"Extracted {len(documents)} laws")
    print("=" * 80)

    # Show first document as example
    if documents:
        doc = documents[0]
        print(f"\nExample Document:")
        print(f"  Title: {doc.title}")
        print(f"  Document URL (page): {doc.document_url}")
        print(f"  Reference No: {doc.reference_no}")
        print(f"  Published: {doc.published_date}")
        print(f"  Year: {doc.year}")
        print(f"  Extra Meta: {doc.extra_meta}")
        print(f"  Document HTML Length: {len(doc.document_html) if doc.document_html else 0} chars")
        print(f"  Doc Path: {doc.doc_path}")

    # Save to JSON
    crawler.save_to_json(documents)
