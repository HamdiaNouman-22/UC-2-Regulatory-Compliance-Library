"""Config-driven HTTP fetcher: requests or Selenium backend, with the same
retry/backoff/delay behavior as the existing crawler/sama_finance_sector_crawler.py
(MAX_RETRIES=3, REQUEST_DELAY=1.2s, exponential-ish backoff), just parameterized
from config instead of hardcoded module constants.
"""

import logging
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class Fetcher:
    def __init__(self, fetch_cfg: dict):
        self.backend = fetch_cfg.get("backend", "requests")
        self.timeout = fetch_cfg.get("timeout_seconds", 30)
        self.max_retries = fetch_cfg.get("max_retries", 3)
        self.retry_backoff = fetch_cfg.get("retry_backoff_seconds", 2)
        self.request_delay = fetch_cfg.get("request_delay_seconds", 1.2)
        self.user_agent = fetch_cfg.get("user_agent") or DEFAULT_USER_AGENT
        self.headless = fetch_cfg.get("headless", True)

        self.driver = None
        self._made_first_request = False

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def _init_driver(self):
        from selenium import webdriver
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument(f"user-agent={self.user_agent}")
        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(10)
        logger.info("Chrome WebDriver initialized")

    def close(self):
        if self.driver:
            self.driver.quit()
            self.driver = None

    def get(self, url: str) -> Optional[BeautifulSoup]:
        if self._made_first_request:
            time.sleep(self.request_delay)
        self._made_first_request = True

        for attempt in range(1, self.max_retries + 1):
            try:
                if self.backend == "selenium":
                    if self.driver is None:
                        self._init_driver()
                    self.driver.get(url)
                    time.sleep(2)
                    html = self.driver.page_source
                else:
                    resp = self.session.get(url, timeout=self.timeout, allow_redirects=True)
                    resp.raise_for_status()
                    html = resp.text
                return BeautifulSoup(html, "html.parser")
            except Exception as e:
                logger.warning(f"Attempt {attempt}/{self.max_retries} failed for {url}: {e}")
                if attempt < self.max_retries:
                    time.sleep(self.retry_backoff * attempt)

        logger.error(f"Failed to fetch after {self.max_retries} attempts: {url}")
        return None
