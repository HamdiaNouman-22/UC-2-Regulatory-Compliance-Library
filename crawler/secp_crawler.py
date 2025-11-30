# secp_crawler.py

from typing import List
from playwright.sync_api import sync_playwright, Page
from crawler.crawler import BaseCrawler
from models import RegulatoryDocument
import time


class SECPCrawler(BaseCrawler):

    BASE_URLS = {
        "Rules": "https://www.secp.gov.pk/laws/rules/",
        "Regulations": "https://www.secp.gov.pk/laws/regulations/",
        "Notifications": "https://www.secp.gov.pk/laws/notifications/"
    }

    def __init__(self, headless: bool = True, retries: int = 3, backoff: float = 1.5):
        self.headless = headless
        self.retries = retries
        self.backoff = backoff

    def _safe_goto(self, page: Page, url: str, label: str):
        """
        Retry mechanism for SECP table pages.
        """
        for attempt in range(1, self.retries + 1):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=150000)
                return
            except Exception as e:
                print(f"[SECP] ERROR loading {label} (Attempt {attempt}/{self.retries}): {e}")
                time.sleep(self.backoff * attempt)

        raise RuntimeError(f"[SECP] FAILED to load page after {self.retries} attempts → {url}")


    def _crawl_section(self, page: Page, base_url: str, category: str) -> List[RegulatoryDocument]:
        documents: List[RegulatoryDocument] = []

        self._safe_goto(page, base_url, category)

        try:
            page.wait_for_selector("table tbody", timeout=30000)
        except:
            print(f"[SECP] WARNING: No table found for {category}")
            return documents

        try:
            dropdown = page.locator("select")
            dropdown.select_option("-1")
            page.wait_for_timeout(1500)
        except Exception:
            pass

        rows = page.locator("table tbody tr")
        total = rows.count()
        print(f"[SECP] {category}: Found {total} rows")

        for i in range(total):
            try:
                row = rows.nth(i)

                title = row.locator("td:nth-child(2)").inner_text().strip()
                if not title:
                    continue

                download_a = row.locator("a:has-text('Download')")
                if download_a.count() == 0:
                    continue

                href = download_a.first.get_attribute("href")
                if not href:
                    continue

                if href.startswith("/"):
                    href = "https://www.secp.gov.pk" + href

                doc = RegulatoryDocument(
                    regulator="SECP",
                    source_system="SECP-LAWS",
                    category=category,
                    title=title,
                    document_url=href,
                    urdu_url=None,
                    published_date=None,
                    reference_no=None,
                    department=None,
                    year=None,
                    source_page_url=base_url,
                    file_type=None,
                    extra_meta={
                        "download_url": href,
                        "table_row": i,
                    }
                )

                documents.append(doc)

            except Exception as e:
                print(f"[SECP] ERROR parsing row {i}: {e}")

        return documents

    def get_documents(self) -> List[RegulatoryDocument]:
        """
        Crawl all SECP categories:
        - Rules
        - Regulations
        - Notifications

        Returns standard RegulatoryDocument objects.
        """
        all_docs: List[RegulatoryDocument] = []

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=self.headless)
            context = browser.new_context()

            for category, url in self.BASE_URLS.items():
                print(f"[SECP] Crawling {category} → {url}")

                page = context.new_page()
                docs = self._crawl_section(page, url, category)

                print(f"[SECP] Completed {category}: {len(docs)} docs")

                all_docs.extend(docs)
                page.close()

            browser.close()

        print(f"[SECP] TOTAL SECP documents collected: {len(all_docs)}")
        return all_docs
