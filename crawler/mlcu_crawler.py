"""MLCUCrawler — one section of Egypt's AML/CFT unit, read with `requests`.

Server-rendered links, so no browser: a headless Chromium is REFUSED by this
host's F5 WAF while a plain desktop-UA GET works. The WAF also refuses with
HTTP 200 and an HTML body, so `_reject_if_blocked` reads the body, not the
status code.

One instance = one section. Five sections means five `sources:` entries in
config/sources/mlcu.yml, never one crawler walking all five — see that file.

The crawler also downloads each PDF and stores its text on the document. That is
not an optimisation: the orchestrator writes the version row BEFORE it extracts
anything (`orch.py:869`), so text it extracts itself never reaches a workbook.
The text is also what the fingerprint is made of, with `document_url|title` left
as the floor for documents that extract to nothing.
"""

from __future__ import annotations

import hashlib
import logging
import os
import pathlib
import re
import tempfile
import unicodedata
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from models.models import RegulatoryDocument
from dynamic_crawler.formfill.runner import _ext_type, _is_doc
from crawler.fingerprint import stamp_content_hashes
from generic_crawler.crawler import content_key

logger = logging.getLogger(__name__)

BASE = "https://mlcu.org.eg"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# Same count as `a.AutoDownload` on all five leaves, measured 2026-08-17.
DOC_SELECTOR = "ul.news-linksLargIcon li a"

# F5 BIG-IP ASM's refusal page. It arrives as 200 + text/html, so nothing above
# the body distinguishes it from a real page.
_BLOCK_MARKERS = ("the requested url was rejected",
                  "request rejected",
                  "your support id is")

# Ordinary Arabic. Presentation forms are not listed because NFKC has already
# folded them by the time this runs — see `_text_for`.
_ARABIC = re.compile(r"[؀-ۿ]")

# openpyxl's ILLEGAL_CHARACTERS_RE, which it raises on rather than escapes. A PDF
# extractor emits these freely — form feeds between pages, control bytes from a
# bad encoding — and `workbook export` then dies inside save(), after the whole
# crawl has already run. Tab, newline and carriage return are legal and kept.
_ILLEGAL_XLSX = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


class MLCUCrawler:
    """One MLCU framework section. `section_url` and `section_title` come from
    the YAML so a site retitle cannot silently reshape `doc_path`."""

    def __init__(
        self,
        regulator: str,
        source_system: str,
        section_id: str,
        section_url: str,
        section_title: str,
        category: Optional[str] = None,
        timeout: int = 45,
        snapshot_dir: Optional[str] = None,
        pdf_cache_dir: Optional[str] = None,
    ):
        self.regulator = regulator
        self.source_system = source_system
        self.section_id = str(section_id)
        self.section_url = section_url
        self.section_title = section_title
        self.category = category or section_title
        self.timeout = timeout
        # Replay a cached page instead of fetching. Keeps a re-test off a host
        # that has already refused us once; see the access rule in MLCU_TASKS.md.
        self.snapshot_dir = snapshot_dir
        # Read-through cache for the PDFs. Set it on the live run and every later
        # re-parse costs zero requests — the access rule applied to the documents
        # the way `snapshot_dir` applies it to the section pages.
        #
        # Read from the environment when the caller does not pass one, because
        # `tools.workbook export` builds this class from the yml and there is no
        # other way in. It stays OUT of config/sources/mlcu.yml deliberately: a
        # cache in production is SIMAH's rule 5 — a stale document reporting
        # `unchanged` while the law was amended.
        self.pdf_cache_dir = pdf_cache_dir or os.environ.get("MLCU_PDF_CACHE_DIR") or None
        self.last_result: dict = {}
        self._session: Optional[requests.Session] = None
        # Stamped onto every document so a replay is never mistaken for a crawl.
        self._origin = "live"
        self._captured = ""

    def _http(self) -> requests.Session:
        """One keep-alive session for the section page and its 24 PDFs."""
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({"User-Agent": USER_AGENT})
        return self._session

    @property
    def source_names(self) -> List[str]:
        return [self.section_title]

    # ------------------------------------------------------------------ #

    def _reject_if_blocked(self, body: str, status: int) -> None:
        """The WAF answers 200 with an HTML error page, so the status code says
        success either way. The body is the only honest signal."""
        head = (body or "")[:4000].lower()
        for marker in _BLOCK_MARKERS:
            if marker in head:
                raise RuntimeError(
                    f"MLCU refused {self.section_url} (status {status}, WAF "
                    f"marker {marker!r}). Do NOT retry in a loop and do NOT "
                    f"reach for a browser — a headless Chromium is refused "
                    f"here. See the access rule in MLCU_TASKS.md.")

    def _load(self) -> str:
        if self.snapshot_dir:
            path = pathlib.Path(self.snapshot_dir) / f"{self.section_id}.html"
            self._origin = "snapshot"
            self._captured = datetime.fromtimestamp(
                path.stat().st_mtime, timezone.utc).isoformat(timespec="seconds")
            logger.info("MLCU %s: replaying snapshot %s (captured %s)",
                        self.section_title, path, self._captured)
            return path.read_text(encoding="utf-8", errors="replace")

        session = self._http()
        try:
            resp = session.get(self.section_url, timeout=self.timeout)
        except requests.RequestException as e:
            raise RuntimeError(f"MLCU section unreachable: {self.section_url}: {e}") from e

        ctype = (resp.headers.get("Content-Type") or "").lower()
        if "html" not in ctype:
            raise RuntimeError(
                f"MLCU returned {ctype!r} for {self.section_url}, expected HTML")
        self._reject_if_blocked(resp.text, resp.status_code)
        return resp.text

    @staticmethod
    def _title_of(anchor) -> str:
        """Title without the `<p class="fa …">` icon element.

        The icon carries no text today, so stripping it is a no-op — it is here
        because it stops being a no-op silently (the MOE trap).
        """
        node = BeautifulSoup(str(anchor), "html.parser")
        for icon in node.select("p.fa, .fa"):
            icon.decompose()
        heading = node.select_one("h3")
        return (heading or node).get_text(" ", strip=True)

    # ------------------------------------------------------------------ #
    #  DOCUMENT TEXT                                                      #
    # ------------------------------------------------------------------ #

    def _pdf_path(self, url: str) -> Tuple[Optional[pathlib.Path], str]:
        """Local path for one PDF, downloading it once if `pdf_cache_dir` is set.

        A snapshot replay never reaches the network: with no cached file there is
        simply no text, which is visible in the row rather than silently filled.
        """
        cached = None
        if self.pdf_cache_dir:
            # md5 of the url, not its basename: these are percent-encoded Arabic
            # filenames and two sections reuse the same stem at the site root.
            name = hashlib.md5(url.encode("utf-8")).hexdigest()[:16] + ".pdf"
            cached = pathlib.Path(self.pdf_cache_dir) / name
            if cached.exists():
                return cached, "pdf-cache"

        if self.snapshot_dir:
            # Replay means replay. A cache miss is a document with no text, never
            # a download — otherwise setting a cache dir would silently turn an
            # offline re-parse into 24 live requests.
            return None, "snapshot-no-pdf"

        try:
            resp = self._http().get(url, timeout=self.timeout, stream=True)
            resp.raise_for_status()
            body = resp.content
        except requests.RequestException as e:
            logger.warning("MLCU pdf fetch failed %s: %s", url[:80], e)
            return None, "fetch-failed"

        ctype = (resp.headers.get("Content-Type") or "").lower()
        if "html" in ctype:
            # The WAF answers 200 with an HTML body, so a refusal arrives here
            # looking like a successful download of a very small PDF.
            self._reject_if_blocked(body[:4000].decode("utf-8", "replace"), resp.status_code)
            logger.warning("MLCU pdf %s came back as %s, not a file", url[:80], ctype)
            return None, "not-a-file"

        if cached is not None:
            cached.parent.mkdir(parents=True, exist_ok=True)
            cached.write_bytes(body)
            return cached, "live"

        tmp = pathlib.Path(tempfile.mkstemp(suffix=".pdf")[1])
        tmp.write_bytes(body)
        return tmp, "live"

    def _text_for(self, url: str) -> Tuple[str, str, dict]:
        """Extracted text for one PDF, where the bytes came from, and how much of
        the document the extractor could actually read.

        Uses the orchestrator's own extractor so the text stored here is the same
        text it would have produced (`orchestrator.py:273`), not a second opinion.
        """
        if not _is_doc(url):
            return "", "not-a-document", {}
        path, origin = self._pdf_path(url)
        if path is None:
            return "", origin, {}
        try:
            # Heavy import (cv2, pdfplumber, pytesseract) — kept off module load
            # so importing this crawler stays cheap for anything that only parses.
            from processor.Text_Extractor import OCRProcessor
            text, meta = OCRProcessor.extract_text_from_pdf_smart(pdf_path=str(path))
            ocr_ok = OCRProcessor.is_ocr_available()
        except Exception as e:
            logger.warning("MLCU text extraction failed %s: %s", url[:80], e)
            return "", "extract-failed", {}

        meta = meta or {}
        total = int(meta.get("total_pages") or 0)
        good = int(meta.get("good_pages") or 0)
        ocred = int(meta.get("ocr_pages") or 0)

        # A page routed to OCR contributes NOTHING when the engine is absent, but
        # `extract_text_from_pdf_smart` still counts it in `good_pages` and logs
        # it as "OK (0 chars)" — the one thing the extractor does not report. So
        # subtract those to get the number of pages actually read.
        usable = good - (0 if ocr_ok else ocred)
        info = {"pages": total, "ocr_pages": ocred, "ocr_available": ocr_ok,
                "pages_read": max(usable, 0)}

        # NFKC first. Four of the six 1065 documents extract as Arabic
        # PRESENTATION FORMS (U+FB50-FEFF) rather than ordinary Arabic
        # (U+0600-06FF) — the letters are right and in logical order, but they are
        # a different string, so they match neither their own title nor an Arabic
        # search nor anything downstream. NFKC maps them back.
        #
        # Then sanitise, so the text that is hashed is the same text that reaches
        # the workbook. Hashing before either step would fingerprint words no
        # reader ever sees.
        text = unicodedata.normalize("NFKC", text or "")
        text = _ILLEGAL_XLSX.sub(" ", text).strip()

        # DISCARD ON "NO ARABIC", not on the page arithmetic above. MLCU publishes
        # only in Arabic, so text with no Arabic character in it is not the
        # document, whatever the page counts say. The counts alone are not enough:
        # the consumer-finance controls have a font with no ToUnicode map, so
        # every glyph decodes to (cid:NNN) — `_is_text_broken` catches four of its
        # six pages and routes them to an OCR engine that is not installed, but
        # ONE page's mojibake passes that check and lands in `good_pages`. The
        # arithmetic therefore reads "1 page of 6 was read" and keeps 1,290
        # characters of residue.
        #
        # Zero is the whole threshold. There is nothing to tune, and it cannot
        # misfire on a healthy document here — the seventeen readable ones run
        # 60-76% Arabic.
        if text and not _ARABIC.search(text):
            logger.warning("MLCU %s: discarding %d chars with no Arabic in them "
                           "— %d/%d pages needed OCR and OCR is %s",
                           url.rsplit("/", 1)[-1][:50], len(text), ocred, total,
                           "available" if ocr_ok else "NOT installed")
            return "", "unreadable-text-layer", info

        if usable <= 0:
            # Nothing survived at all: a scanned document with no OCR engine.
            return "", ("no-pages-read" if origin in ("live", "pdf-cache") else origin), info

        return text, origin, info

    def _attach_text(self, docs: List[RegulatoryDocument]) -> List[RegulatoryDocument]:
        """Store each document's text and fingerprint it from those words.

        The orchestrator extracts the same text later, but writes the version row
        BEFORE doing so (`orch.py:869`), so a workbook only ever carries text the
        crawler put here.
        """
        for doc in docs:
            text, origin, info = self._text_for(doc.document_url)
            doc.extra_meta["content_text"] = text
            doc.extra_meta["text_origin"] = origin
            doc.extra_meta["text_chars"] = len(text)
            # WHY a text cell is empty, in the row rather than in a log nobody
            # will still have. Seven documents here need OCR — six are scanned
            # images, one has an unmappable text layer — and without this the
            # reader cannot tell that from a crawler that simply missed them.
            if info:
                doc.extra_meta["text_pages"] = info["pages"]
                doc.extra_meta["text_pages_read"] = info["pages_read"]
                doc.extra_meta["text_ocr_pages"] = info["ocr_pages"]
                doc.extra_meta["text_ocr_available"] = info["ocr_available"]

            # Hash the words, not the link. `content_key("")` is "" — falsy — so
            # `stamp_content_hashes` fills in the `document_url|title` fallback for
            # the scanned-image documents that extract to nothing (six of the
            # twenty-four today, no OCR engine on this machine). That fallback is
            # not a nicety: one hash of "" shared by all six would report
            # `unchanged` straight through a replacement.
            doc.content_hash = content_key(text)
            doc.extra_meta["content_hash_basis"] = (
                "text" if doc.content_hash else "document_url|title")
        return docs

    # ------------------------------------------------------------------ #

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        cap = limit if isinstance(limit, int) and limit > 0 else None
        warnings: List[str] = []

        html = self._load()
        self._reject_if_blocked(html, 200)
        soup = BeautifulSoup(html, "html.parser")

        # A retitled section would otherwise change doc_path for every document
        # in it, which reads as the whole section disappearing and reappearing.
        h1 = soup.select_one("h1")
        if h1 and h1.get_text(strip=True) != self.section_title:
            warnings.append(
                f"section title moved: yml has {self.section_title!r}, page says "
                f"{h1.get_text(strip=True)!r} — reconcile before trusting doc_path")
            logger.warning(warnings[-1])

        anchors = soup.select(DOC_SELECTOR)
        if not anchors:
            # Never return an empty section quietly: absent documents are ruled
            # `disappeared` downstream and become withdrawal proposals against
            # law still in force.
            raise RuntimeError(
                f"MLCU {self.section_title!r}: selector {DOC_SELECTOR!r} matched "
                f"nothing at {self.section_url}. That is a broken crawler or a "
                f"WAF page, not a section with no documents.")

        docs: List[RegulatoryDocument] = []
        seen = set()
        skipped_no_href = 0

        for a in anchors:
            href = (a.get("href") or "").strip()
            if not href:
                skipped_no_href += 1
                continue
            url = urljoin(BASE, href)
            if url in seen:
                continue
            seen.add(url)

            title = self._title_of(a)
            if not title:
                # Better a readable filename stem than a row the library cannot
                # show. Arabic filenames survive unquoting intact.
                from urllib.parse import unquote
                title = unquote(url.rsplit("/", 1)[-1]).rsplit(".", 1)[0].strip()

            docs.append(RegulatoryDocument(
                regulator=self.regulator,
                source_system=self.source_system,
                category=self.category,
                title=title,
                document_url=url,
                doc_path=[self.regulator, self.source_system,
                          self.section_title, title],
                file_type=_ext_type(url) if _is_doc(url) else "HTML",
                source_page_url=self.section_url,
                extra_meta={
                    "crawl_source": self.section_title,
                    "mlcu_section_id": self.section_id,
                    "mlcu_section_url": self.section_url,
                    # Provenance, so a replayed run cannot be read as a crawl.
                    # `snapshot_dir` is an ordinary init_kwargs key: without this
                    # stamp anyone could put it in the yml and the exported
                    # workbook would look exactly like live traffic.
                    "source": self._origin,
                    "snapshot_captured": self._captured,
                },
            ))
            if cap and len(docs) >= cap:
                break

        if skipped_no_href:
            warnings.append(f"{skipped_no_href} MLCU anchor(s) had no href")

        # Single exit: text first, then fingerprints, so nothing is hashed before
        # its words have arrived. `stamp_content_hashes` fills only what
        # `_attach_text` left blank.
        docs = stamp_content_hashes(self._attach_text(docs))

        by_basis: dict = {}
        for d in docs:
            key = d.extra_meta.get("content_hash_basis", "?")
            by_basis[key] = by_basis.get(key, 0) + 1
        needs_ocr = [d for d in docs
                     if d.extra_meta.get("text_ocr_pages")
                     and not d.extra_meta.get("text_ocr_available")]
        if needs_ocr:
            warnings.append(
                f"{len(needs_ocr)} document(s) need OCR and tesseract is not "
                f"installed; their text is empty by measurement, not by omission")

        if by_basis.get("document_url|title"):
            # Not noise: these are the documents whose change detection is running
            # on the weak fingerprint, and the count is what the delivery message
            # has to state.
            warnings.append(
                f"{by_basis['document_url|title']} document(s) extracted no text "
                f"and fell back to the document_url|title fingerprint")

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings},
            "by_source": {self.section_title: len(docs)},
            "hash_basis": by_basis,
            "source": self._origin,
        }
        logger.info("MLCUCrawler %s: %d document(s), %s, hash basis %s",
                    self.section_title, len(docs), self._origin, by_basis)
        return docs
