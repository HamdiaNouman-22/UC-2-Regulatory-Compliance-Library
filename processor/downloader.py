import os
import re
import hashlib
from pathlib import Path
import time
import requests
from urllib.parse import urlparse, parse_qs
from playwright.sync_api import sync_playwright
import unicodedata


class Downloader:
    # Extensions that should be downloaded directly
    DIRECT_DOWNLOAD_EXTENSIONS = {
        "pdf", "doc", "docx", "xls", "xlsx", "csv", "zip", "rtf", "txt"
    }

    def __init__(self, download_dir="downloads", headless=True, retries=3, backoff=1.5):
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)

        self.headless = headless
        self.retries = retries
        self.backoff = backoff

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        })

    # FILENAME SANITIZER
    def _sanitize_filename(self, name: str) -> str:
        if not name:
            return "document"

        name = unicodedata.normalize("NFKD", name)
        name = name.replace("\r", " ").replace("\n", " ").replace("\t", " ")
        name = re.sub(r'[<>:"/\\|?*]', "_", name)
        name = re.sub(r"\s+", " ", name).strip()

        if not name:
            return "document"

        return name[:200]    # Avoid very long filenames


    # FILE HASH
    def _compute_hash(self, file_path: Path) -> str:
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()


    def download(self, document) -> tuple[str, str]:
        """Accepts dict or RegulatoryDocument"""

        title = getattr(document, "title", None) or document.get("title")
        title = self._sanitize_filename(title)

        # Determine main working URL
        url = (
            getattr(document, "document_url", None)
            or getattr(document, "english_url", None)
            or getattr(document, "circular_url", None)
            or getattr(document, "urdu_url", None)
            or document.get("document_url")
            or document.get("english_url")
            or document.get("circular_url")
            or document.get("urdu_url")
        )

        if not url or url.lower().startswith("javascript"):
            raise ValueError(f"[Downloader] Invalid or unsupported URL: {url}")

        filename_safe = title
        ext = self._extract_extension(url)


        # NEW: SPECIAL SECP RULE — detect binary download URLs
        if "wpdmdl" in url or "download" in url.lower():
            print("[Downloader] Detected SECP direct-download URL → binary download")
            return self._download_binary(url, filename_safe, ext or "pdf")

        # NORMAL binary downloads
        if ext in self.DIRECT_DOWNLOAD_EXTENSIONS:
            return self._download_binary(url, filename_safe, ext)

        return self._html_to_pdf(url, filename_safe)


    def _extract_extension(self, url: str) -> str:
        path = urlparse(url).path
        if "." in path:
            return path.split(".")[-1].lower()
        return ""


    def _download_binary(self, url: str, filename: str, ext: str):
        file_path = self.download_dir / f"{filename}.{ext}"

        for attempt in range(1, self.retries + 1):
            try:
                resp = self.session.get(url, stream=True, timeout=60)
                resp.raise_for_status()

                with open(file_path, "wb") as f:
                    for chunk in resp.iter_content(1024 * 1024):
                        f.write(chunk)

                file_hash = self._compute_hash(file_path)
                print(f"[Downloader] Saved binary: {file_path}")
                return str(file_path), file_hash

            except Exception as e:
                print(f"[Downloader] Binary download failed ({attempt}/{self.retries}): {e}")
                time.sleep(self.backoff * attempt)

        raise RuntimeError(f"[Downloader] FAILED to download file after retries → {url}")


    def _html_to_pdf(self, url: str, filename: str):
        file_path = self.download_dir / f"{filename}.pdf"

        for attempt in range(1, self.retries + 1):
            try:
                with sync_playwright() as pw:
                    browser = pw.chromium.launch(headless=self.headless)
                    context = browser.new_context()
                    page = context.new_page()

                    page.goto(url, wait_until="networkidle", timeout=200000)

                    # Detect SBP iframe
                    iframe = next((f for f in page.frames if f != page.main_frame), None)

                    if iframe:
                        print("[Downloader] Rendering iframe → PDF")
                        iframe.wait_for_load_state("networkidle")
                        iframe.pdf(path=str(file_path), format="A4", print_background=True)
                    else:
                        print("[Downloader] Rendering main page → PDF")
                        page.pdf(path=str(file_path), format="A4", print_background=True)

                    browser.close()

                file_hash = self._compute_hash(file_path)
                return str(file_path), file_hash

            except Exception as e:
                print(f"[Downloader] HTML→PDF failed ({attempt}/{self.retries}): {e}")
                time.sleep(self.backoff * attempt)

        raise RuntimeError(f"[Downloader] FAILED to render PDF after retries → {url}")
