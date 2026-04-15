"""
CBB Monitoring Crawler - FINAL FIXED VERSION
=============================================
Matches the updated v2 full crawler structure:
- Compliance now uses #aml/#eofi sections
- Proper hash calculation and deduplication

Usage:
    from cbb_monitoring_crawler import CBBMonitoringCrawler, monitor_cbb_changes

    # Class-based usage
    crawler = CBBMonitoringCrawler(repo)
    docs = crawler.fetch_documents()

    # Function-based usage (for main.py pipeline)
    monitor_cbb_changes()
"""

import requests
from bs4 import BeautifulSoup
import re
import time
import logging
from datetime import datetime, date, timedelta
from urllib.parse import urljoin
from typing import List, Optional, Dict
import hashlib
import os

from models.models import RegulatoryDocument

log = logging.getLogger(__name__)

# ─── URLs ─────────────────────────────────────────────────────────────────────
BASE_URL           = "https://cbben.thomsonreuters.com"
CHANGES_URL        = "https://cbben.thomsonreuters.com/view-revision-updates"
LAWS_REGULATIONS_URL = "https://www.cbb.gov.bh/laws-regulations/"
COMPLIANCE_URL       = "https://www.cbb.gov.bh/compliance/"
CBB_GOV_BASE         = "https://www.cbb.gov.bh"

REQUEST_DELAY = 1.0
MAX_RETRIES   = 3
REGULATOR     = "Central Bank of Bahrain"


# ─── HTTP Session ─────────────────────────────────────────────────────────────
def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


SESSION = _make_session()


def _fetch(url: str, params: dict = None) -> Optional[BeautifulSoup]:
    """Fetch URL with retries"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SESSION.get(url, params=params, timeout=30)
            resp.raise_for_status()
            log.info(f"✓ [{resp.status_code}] {url}")
            return BeautifulSoup(resp.content, "lxml")
        except requests.RequestException as e:
            log.warning(f"Attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
    log.error(f"✗ All retries exhausted: {url}")
    return None


def _make_absolute(block: BeautifulSoup, base: str = BASE_URL) -> None:
    """Convert relative URLs to absolute"""
    for tag in block.find_all(href=True):
        if tag["href"].startswith("/"):
            tag["href"] = urljoin(base, tag["href"])
    for tag in block.find_all(src=True):
        if tag["src"].startswith("/"):
            tag["src"] = urljoin(base, tag["src"])


# ═══════════════════════════════════════════════════════════════════════════════
#  PART A: THOMSON REUTERS "VIEW UPDATES" MONITORING
# ═══════════════════════════════════════════════════════════════════════════════

def _get_thomson_reuters_changes(from_date: date, to_date: date) -> List[Dict]:
    """
    Fetch changed URLs from Thomson Reuters "View Updates" page.

    Returns: [{"title": ..., "url": ..., "change_date": ...}, ...]
    """
    changed_pages = []
    seen = set()

    date1_str = from_date.strftime("%d/%m/%Y")
    date2_str = to_date.strftime("%d/%m/%Y")

    params = {
        "f_days": "on",
        "changed": "between",
        "min": date1_str,
        "max": date2_str,
        "items_per_page": "40",
        "sort_by": "revision_timestamp_1",
    }

    page_url = CHANGES_URL
    page_num = 0

    while page_url:
        log.info(f"Fetching TR changes page {page_num}: {page_url}")
        soup = _fetch(page_url, params=params if page_num == 0 else None)
        if not soup:
            break

        results_area = soup.find("div", class_="view-content")
        if not results_area:
            log.warning("No view-content div found")
            break

        for row in results_area.find_all("div", class_="views-row"):
            detail_div = row.find("div", class_="book-detail")
            if not detail_div:
                continue

            a = detail_div.find("a", href=True)
            if not a:
                continue

            href = a.get("href", "")
            full_url = urljoin(BASE_URL, href)

            if full_url in seen:
                continue
            seen.add(full_url)

            title = a.get_text(strip=True)

            detail_text = detail_div.get_text()
            change_date = None
            dm = re.search(r"\((\d{1,2}\s+\w+\s+\d{4})\)", detail_text)
            if dm:
                for fmt in ("%d %B %Y", "%d %b %Y"):
                    try:
                        change_date = datetime.strptime(dm.group(1), fmt).date().isoformat()
                        break
                    except ValueError:
                        pass

            trail_div = row.find("div", class_="book-trail")
            breadcrumb = trail_div.get_text(strip=True) if trail_div else ""

            changed_pages.append({
                "title": title,
                "url": full_url,
                "change_date": change_date,
                "breadcrumb": breadcrumb,
            })
            log.info(f"  TR Change: {title[:60]}  ({change_date})")

        pager = soup.find("nav", class_="pager")
        if pager:
            next_link = pager.find("a", string=re.compile(r"next|›", re.IGNORECASE))
            if next_link and next_link.get("href"):
                page_url = urljoin(BASE_URL, next_link["href"])
                params = None
                page_num += 1
                time.sleep(REQUEST_DELAY)
                continue

        break

    log.info(f"Total TR changed pages found: {len(changed_pages)}")
    return changed_pages


# ═══════════════════════════════════════════════════════════════════════════════
#  PART B: CBB.GOV.BH CONTENT MONITORING (hash comparison)
# ═══════════════════════════════════════════════════════════════════════════════

def _get_laws_and_regulations_hashes() -> List[Dict]:
    """
    Re-crawl Laws & Regulations accordion page and return current content hashes.

    Returns: [{"title": ..., "url": ..., "content_hash": ...}, ...]
    """
    soup = _fetch(LAWS_REGULATIONS_URL)
    if not soup:
        return []

    items = []
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

        content_text = content_div.get_text(separator=" ", strip=True)
        content_hash = hashlib.md5(content_text.encode("utf-8")).hexdigest()

        items.append({
            "title": title,
            "url": LAWS_REGULATIONS_URL,
            "content_hash": content_hash,
            "content_text": content_text,
            "source": "laws_regulations",
        })

    log.info(f"Laws & Regulations: {len(items)} sections hashed")
    return items


def _get_compliance_hashes() -> List[Dict]:
    """
    Re-crawl Compliance page (#aml/#eofi sections) and return current content hashes.

    CRITICAL: This MUST match the structure used by the full crawler's scrape_compliance()

    Returns: [{"title": ..., "url": ..., "content_hash": ..., "doc_path": ...}, ...]
    """
    soup = _fetch(COMPLIANCE_URL)
    if not soup:
        return []

    items = []

    # Process #aml section
    aml_div = soup.find("div", id="aml")
    if aml_div:
        accordion_layers = aml_div.find_all("div", class_="ult_exp_section_layer")
        log.info(f"  Found {len(accordion_layers)} accordion layers in AML section")

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
                continue

            content_text = content_div.get_text(separator=" ", strip=True)
            content_hash = hashlib.md5(content_text.encode("utf-8")).hexdigest()

            items.append({
                "title": accordion_title,  # Use raw title for lookup
                "url": COMPLIANCE_URL,
                "content_hash": content_hash,
                "content_text": content_text,
                "source": "compliance",
                "section": "AML",
                "doc_path": [REGULATOR, "Compliance", "AML", accordion_title],
            })

    # Process #eofi section
    eofi_div = soup.find("div", id="eofi")
    if eofi_div:
        accordion_layers = eofi_div.find_all("div", class_="ult_exp_section_layer")
        log.info(f"  Found {len(accordion_layers)} accordion layers in EOFI section")

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
                continue

            content_text = content_div.get_text(separator=" ", strip=True)
            content_hash = hashlib.md5(content_text.encode("utf-8")).hexdigest()

            items.append({
                "title": accordion_title,
                "url": COMPLIANCE_URL,
                "content_hash": content_hash,
                "content_text": content_text,
                "source": "compliance",
                "section": "EOFI",
                "doc_path": [REGULATOR, "Compliance", "EOFI", accordion_title],
            })

    log.info(f"Compliance: {len(items)} accordion sections hashed")
    return items


# ═══════════════════════════════════════════════════════════════════════════════
#  CONTENT EXTRACTION (reused from full crawler)
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_content(soup: BeautifulSoup) -> Dict:
    """Extract content from Thomson Reuters page"""
    content_block = soup.find("div", class_="field--name-body")
    document_html = None
    content_text = ""
    download_links = []
    english_pdf = None
    arabic_pdf = None

    if content_block:
        _make_absolute(content_block)
        document_html = str(content_block)
        content_text = content_block.get_text(separator=" ", strip=True)

        for a in content_block.find_all("a", href=True):
            full_url = a["href"]
            parent_text = a.parent.get_text().lower() if a.parent else ""
            lang = "arabic" if "arabic" in parent_text else "english"
            download_links.append({
                "text": a.parent.get_text(strip=True),
                "url": full_url,
                "language": lang,
            })
            if lang == "english" and not english_pdf:
                english_pdf = full_url
            if lang == "arabic" and not arabic_pdf:
                arabic_pdf = full_url

    return {
        "document_html": document_html,
        "content_text": content_text,
        "download_links": download_links,
        "english_pdf": english_pdf,
        "arabic_pdf": arabic_pdf,
    }


def _extract_breadcrumb(soup: BeautifulSoup) -> List[str]:
    """Extract breadcrumb navigation"""
    crumb_nav = soup.find("nav", class_="breadcrumb")
    if not crumb_nav:
        return []
    return [
        a.get_text(strip=True)
        for a in crumb_nav.find_all("a")
        if a.get_text(strip=True)
    ]


def _detect_book_category(soup: BeautifulSoup) -> str:
    """Identify which book/volume from active sidebar nav"""
    for nav in soup.find_all("nav", id=re.compile(r"^book-block-menu-")):
        if nav.find(class_=re.compile(r"menu-item--active-trail")):
            first_a = nav.find("a", href=True)
            if first_a:
                return first_a.get_text(strip=True)
    return "CBB Rulebook"


# ═══════════════════════════════════════════════════════════════════════════════
#  SCRAPING CHANGED PAGES
# ═══════════════════════════════════════════════════════════════════════════════

def _scrape_changed_tr_page(
    url: str,
    category: str,
    monitoring_status: str,
    existing_regulation_id: Optional[int] = None,
) -> Optional[RegulatoryDocument]:
    """Scrape a Thomson Reuters page that has changed"""
    time.sleep(REQUEST_DELAY)
    soup = _fetch(url)
    if not soup:
        return None

    title_tag = soup.find("h2", class_="page-title") or soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else url.split("/")[-1]

    raw_crumb = _extract_breadcrumb(soup)
    breadcrumb = raw_crumb[1:] if raw_crumb else []
    doc_path = [REGULATOR, category] + breadcrumb + [title]

    content = _extract_content(soup)
    primary_pdf = content["english_pdf"] or content["arabic_pdf"]

    text = soup.get_text()
    updated_date = None
    m = re.search(r"Updated\s+Date:\s*(\d{1,2}\s+\w+\s+\d{4})", text, re.IGNORECASE)
    if m:
        for fmt in ("%d %b %Y", "%d %B %Y"):
            try:
                updated_date = datetime.strptime(m.group(1).strip(), fmt).date().isoformat()
                break
            except ValueError:
                pass

    content_hash = hashlib.md5(
        (content["content_text"] or "").encode("utf-8")
    ).hexdigest()

    return RegulatoryDocument(
        regulator=REGULATOR,
        source_system="CBB-Rulebook",
        category=category,
        title=title,
        document_url=primary_pdf or url,
        urdu_url=None,
        published_date=updated_date,
        reference_no=None,
        department=None,
        year=None,
        source_page_url=url,
        file_type="PDF" if primary_pdf else None,
        document_html=content["document_html"],
        extra_meta={
            "download_links": content["download_links"],
            "org_pdf_link": content["english_pdf"],
            "arabic_pdf_link": content["arabic_pdf"],
            "content_text": content["content_text"],
            "breadcrumb": breadcrumb,
            "depth": len(breadcrumb),
            "content_hash": content_hash,
            "monitoring_status": monitoring_status,
            "existing_regulation_id": existing_regulation_id,
        },
        doc_path=doc_path,
    )


def _create_cbb_gov_bh_doc(
    item: Dict,
    monitoring_status: str,
    existing_regulation_id: Optional[int] = None,
) -> RegulatoryDocument:
    """Create RegulatoryDocument for CBB.gov.bh content change"""
    title = item["title"]
    source = item["source"]

    category = "Laws & Regulations" if source == "laws_regulations" else "Compliance"

    # Use provided doc_path or construct default
    doc_path = item.get("doc_path", [REGULATOR, category, title])

    return RegulatoryDocument(
        regulator=REGULATOR,
        source_system=f"CBB-{source.replace('_', '-').title()}",
        category=category,
        title=title,
        document_url=item["url"],
        urdu_url=None,
        published_date=date.today().isoformat(),
        reference_no=None,
        department=None,
        year=None,
        source_page_url=item["url"],
        file_type=None,
        document_html=item.get("content_html", ""),
        extra_meta={
            "content_text": item["content_text"],
            "content_hash": item["content_hash"],
            "monitoring_status": monitoring_status,
            "existing_regulation_id": existing_regulation_id,
            "source_section": source,
            "section": item.get("section", ""),  # AML or EOFI
        },
        doc_path=doc_path,
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN MONITORING CRAWLER
# ═══════════════════════════════════════════════════════════════════════════════

class CBBMonitoringCrawler:
    """
    Dual-mode monitoring crawler for CBB:

    1. Thomson Reuters content: Uses /view-revision-updates API
    2. CBB.gov.bh content: Re-crawls and compares hashes
    """

    def __init__(self, repo, request_delay: float = REQUEST_DELAY):
        self.repo = repo
        self.request_delay = request_delay

    def _get_last_crawl_date(self) -> date:
        """Get last CBB crawl date from DB, fallback to 30 days ago"""
        try:
            last_date = self.repo.get_last_cbb_crawl_date()
            if last_date:
                if isinstance(last_date, datetime):
                    return (last_date - timedelta(days=1)).date()
                return last_date - timedelta(days=1)
        except Exception as e:
            log.warning(f"Could not get last crawl date: {e}")

        fallback = date.today() - timedelta(days=30)
        log.info(f"Using fallback last crawl date: {fallback}")
        return fallback

    def fetch_documents(self, timeout=None) -> List[RegulatoryDocument]:
        """
        Main entry point - monitors both TR and CBB.gov.bh content.

        Returns: List of RegulatoryDocument objects with monitoring_status set.
        """
        from_date = self._get_last_crawl_date()
        to_date = date.today()

        log.info(f"=== CBB MONITORING: {from_date} → {to_date} ===")

        all_docs: List[RegulatoryDocument] = []

        # ── PART A: Thomson Reuters changes ───────────────────────────────
        log.info("=== Monitoring Thomson Reuters content ===")
        tr_changes = _get_thomson_reuters_changes(from_date, to_date)

        for item in tr_changes:
            url = item["url"]
            title = item["title"]

            existing_id = self.repo.get_regulation_id_by_source_url(url)

            if existing_id:
                stored_hash = self.repo.get_cbb_content_hash(existing_id)
                monitoring_status = "modified"
                log.info(f"  TR Modified (ID={existing_id}): {title[:60]}")
            else:
                stored_hash = None
                monitoring_status = "new"
                log.info(f"  TR New: {title[:60]}")

            time.sleep(self.request_delay)
            soup = _fetch(url)
            if not soup:
                continue

            category = _detect_book_category(soup)

            doc = _scrape_changed_tr_page(
                url=url,
                category=category,
                monitoring_status=monitoring_status,
                existing_regulation_id=existing_id,
            )

            if not doc:
                continue

            # Skip if content unchanged
            if monitoring_status == "modified" and stored_hash:
                new_hash = doc.extra_meta.get("content_hash")
                if new_hash == stored_hash:
                    log.info(f"  Content unchanged (hash match): {title[:60]}")
                    continue

            all_docs.append(doc)

        log.info(f"TR content: {len(all_docs)} changes detected")

        # ── PART B: CBB.gov.bh hash comparison ────────────────────────────
        log.info("=== Monitoring CBB.gov.bh content ===")

        # Laws & Regulations
        laws_items = _get_laws_and_regulations_hashes()
        for item in laws_items:
            title = item["title"]
            url = item["url"]
            new_hash = item["content_hash"]

            # Check by doc_path (more reliable than title)
            existing_id = self.repo.get_regulation_id_by_doc_path(
                [REGULATOR, "Laws & Regulations", title]
            )

            if existing_id:
                stored_hash = self.repo.get_cbb_content_hash(existing_id)
                if stored_hash == new_hash:
                    continue  # No change

                monitoring_status = "modified"
                log.info(f"  Laws Modified: {title[:60]}")
            else:
                monitoring_status = "new"
                log.info(f"  Laws New: {title[:60]}")

            doc = _create_cbb_gov_bh_doc(
                item,
                monitoring_status=monitoring_status,
                existing_regulation_id=existing_id,
            )
            all_docs.append(doc)

        # Compliance (AML + EOFI)
        compliance_items = _get_compliance_hashes()
        for item in compliance_items:
            title = item["title"]
            section = item.get("section", "")
            doc_path = item["doc_path"]
            new_hash = item["content_hash"]

            # Use doc_path for lookup
            existing_id = self.repo.get_regulation_id_by_doc_path(doc_path)

            if existing_id:
                stored_hash = self.repo.get_cbb_content_hash(existing_id)
                if stored_hash == new_hash:
                    continue

                monitoring_status = "modified"
                log.info(f"  Compliance/{section} Modified: {title[:60]}")
            else:
                monitoring_status = "new"
                log.info(f"  Compliance/{section} New: {title[:60]}")

            doc = _create_cbb_gov_bh_doc(
                item,
                monitoring_status=monitoring_status,
                existing_regulation_id=existing_id,
            )
            all_docs.append(doc)

        log.info(f"=== TOTAL CHANGES: {len(all_docs)} documents ===")
        return all_docs


# ═══════════════════════════════════════════════════════════════════════════════
#  WRAPPER FUNCTION FOR MAIN.PY PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

def monitor_cbb_changes():
    """
    Main entry point for CBB monitoring pipeline.

    This function:
    1. Creates the monitoring crawler
    2. Fetches changed documents (new or modified)
    3. Processes them through the orchestrator
    4. Generates analysis and matching

    Called by:
    - POST /trigger/CBB endpoint
    - Scheduled jobs in scheduler.py
    - Manual runs

    Returns:
        dict: Summary of changes detected and processed
    """
    log.info("=" * 80)
    log.info("CBB MONITORING CRAWLER STARTED")
    log.info("=" * 80)

    # Import here to avoid circular dependencies
    from storage.mssql_repo import MSSQLRepository
    from orchestrator.orchestrator import Orchestrator
    from processor.downloader import Downloader
    from processor.html_fallback_engine import HTMLFallbackEngine

    # Initialize repository
    repo = MSSQLRepository({
        "server": os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver": os.getenv("MSSQL_DRIVER", "{ODBC Driver 17 for SQL Server}"),
    })

    # Initialize monitoring crawler
    monitoring_crawler = CBBMonitoringCrawler(repo)

    # Fetch changed documents
    log.info("Fetching changed CBB documents...")
    try:
        changed_docs = monitoring_crawler.fetch_documents()

        if not changed_docs:
            log.info("✓ No changes detected. CBB content is up to date.")
            return {
                "status": "success",
                "changes_detected": 0,
                "new_processed": 0,
                "modified_processed": 0,
                "message": "No CBB content changes detected"
            }

        log.info(f"Found {len(changed_docs)} changed documents")

        # Separate new vs modified
        new_docs = [d for d in changed_docs if d.extra_meta.get("monitoring_status") == "new"]
        modified_docs = [d for d in changed_docs if d.extra_meta.get("monitoring_status") == "modified"]

        log.info(f"  - New: {len(new_docs)}")
        log.info(f"  - Modified: {len(modified_docs)}")

        # Process through orchestrator
        orchestrator = Orchestrator(
            crawler=monitoring_crawler,
            repo=repo,
            downloader=Downloader(),
            ocr_engine=HTMLFallbackEngine()
        )

        processed_new = []
        processed_modified = []
        errors = []

        # Process NEW documents
        for doc in new_docs:
            try:
                log.info(f"Processing NEW: {doc.title[:60]}")
                orchestrator._process_cbb_doc(doc)
                processed_new.append({
                    "title": doc.title,
                    "url": doc.source_page_url
                })
            except Exception as e:
                log.error(f"Failed to process new doc {doc.title[:60]}: {e}", exc_info=True)
                errors.append({
                    "title": doc.title,
                    "error": str(e),
                    "type": "new"
                })

        # Process MODIFIED documents
        for doc in modified_docs:
            try:
                log.info(f"Processing MODIFIED: {doc.title[:60]}")
                orchestrator._process_cbb_doc(doc)
                processed_modified.append({
                    "title": doc.title,
                    "url": doc.source_page_url,
                    "regulation_id": doc.extra_meta.get("existing_regulation_id")
                })

            except Exception as e:
                log.error(f"Failed to process modified doc {doc.title[:60]}: {e}", exc_info=True)
                errors.append({
                    "title": doc.title,
                    "error": str(e),
                    "type": "modified"
                })

        log.info("=" * 80)
        log.info("CBB MONITORING CRAWLER COMPLETED")
        log.info(f"  New documents processed: {len(processed_new)}")
        log.info(f"  Modified documents processed: {len(processed_modified)}")
        log.info(f"  Errors: {len(errors)}")
        log.info("=" * 80)

        return {
            "status": "success" if not errors or (processed_new or processed_modified) else "partial_failure",
            "changes_detected": len(changed_docs),
            "new_processed": len(processed_new),
            "modified_processed": len(processed_modified),
            "total_errors": len(errors),
            "new_documents": processed_new,
            "modified_documents": processed_modified,
            "errors": errors,
        }

    except Exception as e:
        log.error(f"CBB monitoring failed: {e}", exc_info=True)
        return {
            "status": "failed",
            "error": str(e),
            "changes_detected": 0,
            "new_processed": 0,
            "modified_processed": 0,
        }


# Export both class and function
__all__ = ["CBBMonitoringCrawler", "monitor_cbb_changes"]