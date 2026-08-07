"""
GenericSiteCrawler — plugs the generic crawler into the existing pipeline.

The orchestrator asks one thing of a crawler (orchestrator.py, run_for_regulator):

    docs = self.crawler.fetch_documents()      # -> List[RegulatoryDocument]

The hand-written crawlers (SAMACombinedCrawler, SBPCrawler, SECPCrawler) already
answer that. The generic crawler did not: it wrote Excel/JSON files. This class is
the translator, so a generic crawl becomes indistinguishable from a hand-written
one as far as the pipeline is concerned.

    crawler = GenericSiteCrawler(
        seed_url      = "https://misa.gov.sa/activities/laws/",
        regulator     = "MISA",
        source_system = "MISA-LAWS",
        category      = "Laws and Regulations",
    )
    Orchestrator(crawler=crawler, repo=..., ...).run_for_regulator("MISA")

WHY IT RUNS IN A SUBPROCESS BY DEFAULT
--------------------------------------
The engine uses Playwright's SYNC api, which refuses to start inside a running
asyncio loop. The pipeline already installs a twisted/asyncio reactor for Scrapy
(scheduler.py, jobs/sbp_job.py call crochet.setup()), so an in-process crawl would
fail there. scheduler.py already runs SECP and SAMA as subprocesses for the same
reason. Pass in_process=True only from a plain script or a test.

WHAT MAPS TO WHAT
-----------------
    crawl row                     RegulatoryDocument
    ----------------------------  ---------------------------------------
    documents[].doc_url           document_url
    documents[].title             title
    documents[].type              file_type      (PDF / DOCX / EXTERNAL ...)
    documents[].found_on          source_page_url
    documents[].section_path      doc_path  (split on " > ", regulator first)
    documents[].content_hash      content_hash
    pages[].text                  extra_meta["content_text"]
    pages[].html                  document_html

`published_date` is left None: a link walk cannot reliably read issue dates. The
orchestrator handles that — filter_new_documents falls back to deduping on
(document_url, category) when published_date is missing.
"""

import json
import logging
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Optional

from models.models import RegulatoryDocument

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
ENGINE = REPO_ROOT / "generic_crawler" / "crawler.py"

# Matches MIN_TEXT_LEN in orchestrator.py. A page with at least this much prose
# is a document no matter what else we know about it.
MIN_PAGE_TEXT = 200

# A LEAF page (no children in the site tree) is a real document even when it is
# short: SAMA's "Article 3" is 184 characters of actual law. Only judge such a
# page by length once we know it has no children — otherwise a folder like
# "Chapter 3: Monetary Policy" (10 characters, one child) looks the same.
MIN_LEAF_TEXT = 50


# ---- reading the listing row -------------------------------------------------
# A listing row carries what the detail page usually does not repeat:
#     "... BPRD Circular Letter No. 15 of 2026  July 06 2026 | BPRD | Circular Letters"
# published_date and reference_no are the two fields the pipeline actually uses,
# so parse those generically and keep the raw row for anything else.

_MONTH = (r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
          r"Jul(?:y)?|Aug(?:ust)?|Sep(?:t|tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?")
_DATE_PATTERNS = [
    re.compile(rf"\b({_MONTH})\s+(\d{{1,2}}),?\s+(\d{{4}})\b", re.I),   # July 06 2026
    re.compile(rf"\b(\d{{1,2}})\s+({_MONTH}),?\s+(\d{{4}})\b", re.I),   # 06 July 2026
    re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b"),                          # 2026-07-06
    re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b"),                      # 06/07/2026
]
_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun",
     "jul", "aug", "sep", "oct", "nov", "dec"], start=1)}

# "BPRD Circular Letter No. 15 of 2026", "DMMD Circular No.03 of 2026"
_REF_RE = re.compile(
    r"\b([A-Z][A-Za-z&.\-]*(?:\s+[A-Za-z&.\-]+){0,4}\s+No\.?\s*\d+"
    r"(?:\s+of\s+\d{4})?)", re.I)


def _parse_row_date(row_text: str) -> Optional[str]:
    """First recognisable date in the row, as ISO. None when unsure — a wrong
    date is worse than no date, since the pipeline dedupes on it."""
    for pat in _DATE_PATTERNS:
        m = pat.search(row_text or "")
        if not m:
            continue
        try:
            a, b, c = m.group(1), m.group(2), m.group(3)
            if a.lower()[:3] in _MONTHS:                       # July 06 2026
                return f"{int(c):04d}-{_MONTHS[a.lower()[:3]]:02d}-{int(b):02d}"
            if b.lower()[:3] in _MONTHS:                       # 06 July 2026
                return f"{int(c):04d}-{_MONTHS[b.lower()[:3]]:02d}-{int(a):02d}"
            if len(a) == 4:                                    # 2026-07-06
                return f"{int(a):04d}-{int(b):02d}-{int(c):02d}"
            return f"{int(c):04d}-{int(b):02d}-{int(a):02d}"    # 06/07/2026 (d/m/y)
        except Exception:
            continue
    return None


def _parse_row_ref(row_text: str, title: str = "") -> Optional[str]:
    """The regulator's own reference number, if the row states one."""
    body = (row_text or "")
    if title and body.startswith(title):
        body = body[len(title):]        # the title itself is not the reference
    m = _REF_RE.search(body)
    return m.group(1).strip()[:120] if m else None


def _split_section_path(section_path: str) -> List[str]:
    return [p.strip() for p in (section_path or "").split(">") if p.strip()]


def _dedupe_keep_order(parts: List[str]) -> List[str]:
    out = []
    for p in parts:
        if not out or out[-1].strip().lower() != p.strip().lower():
            out.append(p)
    return out


class GenericSiteCrawler:
    """One seed URL -> a list of RegulatoryDocument."""

    def __init__(
        self,
        seed_url: str,
        regulator: str,
        source_system: str,
        category: Optional[str] = None,
        scope: str = "auto",
        max_pages: int = 150,
        max_depth: int = 8,
        out_dir: Optional[str] = None,
        include_pages: str = "auto",      # "auto" | "always" | "never"
        in_process: bool = False,
        timeout: int = 3600,
    ):
        self.seed_url = seed_url
        self.regulator = regulator
        self.source_system = source_system
        self.category = category
        self.scope = scope
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.out_dir = out_dir
        self.include_pages = include_pages
        self.in_process = in_process
        self.timeout = timeout
        self.last_result = {}             # raw crawl output, for diagnostics

    # ------------------------------------------------------------------ #
    #  running the engine                                                  #
    # ------------------------------------------------------------------ #

    def _run_crawl(self) -> dict:
        out = Path(self.out_dir) if self.out_dir else Path(
            tempfile.mkdtemp(prefix="generic_crawl_"))

        if self.in_process:
            sys.path.insert(0, str(REPO_ROOT))
            from generic_crawler.crawler import crawl
            crawl(self.seed_url, str(out), max_pages=self.max_pages,
                  max_depth=self.max_depth, scope=self.scope)
        else:
            cmd = [sys.executable, str(ENGINE),
                   "--url", self.seed_url, "--out", str(out),
                   "--scope", self.scope,
                   "--max-pages", str(self.max_pages),
                   "--max-depth", str(self.max_depth)]
            proc = subprocess.run(cmd, capture_output=True, text=True,
                                  encoding="utf-8", errors="replace",
                                  timeout=self.timeout)
            if proc.returncode != 0:
                tail = (proc.stderr or proc.stdout or "").strip().splitlines()
                raise RuntimeError(
                    f"generic crawler failed for {self.seed_url}: "
                    f"{tail[-1] if tail else 'no output'}")

        pages_json = out / "pages.json"
        if not pages_json.exists():
            raise RuntimeError(f"generic crawler produced no pages.json in {out}")
        return json.loads(pages_json.read_text(encoding="utf-8"))

    # ------------------------------------------------------------------ #
    #  mapping                                                             #
    # ------------------------------------------------------------------ #

    def _doc_path(self, section_path: str) -> List[str]:
        """Folder trail for the library.

        ALWAYS starts with the regulator. _get_or_create_compliance_category()
        builds the folder tree from this list, so two regulators that both use a
        top-level folder called "Circulars" would otherwise merge into one node
        and tangle their documents together.
        """
        parts = [self.regulator, self.source_system] + _split_section_path(section_path)
        return _dedupe_keep_order([p for p in parts if p])

    def _category_for(self, section_path: str) -> str:
        if self.category:
            return self.category
        parts = _split_section_path(section_path)
        return parts[-1] if parts else self.source_system

    def _doc_from_document_row(self, d: dict, shape: str) -> Optional[RegulatoryDocument]:
        url = (d.get("doc_url") or "").strip()
        if not url:
            return None
        section_path = d.get("section_path") or ""
        return RegulatoryDocument(
            regulator=self.regulator,
            source_system=self.source_system,
            category=self._category_for(section_path),
            title=(d.get("title") or "").strip() or url.rsplit("/", 1)[-1],
            document_url=url,
            published_date=None,          # a link walk cannot read issue dates
            source_page_url=d.get("found_on") or self.seed_url,
            file_type=d.get("type") or None,
            doc_path=self._doc_path(section_path),
            content_hash=d.get("content_hash"),
            extra_meta={
                "crawler": "generic",
                "shape": shape,
                "seed_url": self.seed_url,
                "section_path": section_path,
                "record_kind": "document",
            },
        )

    def _doc_from_page_row(self, r: dict, shape: str) -> Optional[RegulatoryDocument]:
        """A content page IS the document on tree sites (SAMA rulebook, CBB):
        there is no attached PDF, the regulation is the page text."""
        url = (r.get("url") or "").strip()
        text = r.get("text") or ""
        if not url:
            return None
        section_path = r.get("section_path") or ""
        title = ((r.get("title") or "").strip()
                 or (r.get("linked_from_title") or "").strip()
                 or url.rsplit("/", 1)[-1])
        # The page's own "Original PDF". The orchestrator looks for exactly this
        # key (extract_text_content_unified, tier 3) and will download + OCR it
        # when the page text is too short to analyse — which is the case for
        # every short article on a rulebook site.
        pdf = (r.get("pdf_links") or "").split(" | ")[0].strip()
        extra_pdf = {"org_pdf_link": pdf} if pdf.startswith("http") else {}
        # The listing row is the only place the reference number and issue date
        # appear on most list sites — the detail page rarely repeats them.
        row_text = r.get("row_text") or ""
        return RegulatoryDocument(
            regulator=self.regulator,
            source_system=self.source_system,
            category=self._category_for(section_path),
            title=title,
            document_url=url,
            published_date=_parse_row_date(row_text),
            reference_no=_parse_row_ref(row_text, title),
            source_page_url=url,
            file_type="HTML",
            doc_path=self._doc_path(section_path),
            document_html=r.get("html") or None,
            content_hash=r.get("content_hash"),
            extra_meta={
                "crawler": "generic",
                "shape": shape,
                "seed_url": self.seed_url,
                "section_path": section_path,
                "record_kind": "page",
                # Tier 1b of the orchestrator's extraction: use this text directly
                # instead of re-fetching the page.
                "content_text": text,
                "depth": r.get("depth", 0),
                "parent_page_url": r.get("parent_page_url", ""),
                "row_text": row_text,
                **extra_pdf,
            },
        )

    def _want_pages(self, pages: List[dict]) -> bool:
        if self.include_pages == "always":
            return True
        if self.include_pages == "never":
            return False
        # "auto": include pages only when they carry real prose. On a table site
        # the single synthetic page row has no text and must not become a document.
        return any((p.get("text_len") or 0) >= MIN_PAGE_TEXT for p in pages)

    @staticmethod
    def _page_is_document(r: dict) -> bool:
        """Is this page a document, or just a folder in the site's tree?

        Length alone cannot tell them apart — see MIN_LEAF_TEXT. When the walker
        reported how many children a page has, trust that: a leaf is content, a
        page with children is a folder whose content lives underneath it.
        """
        text_len = r.get("text_len") or 0
        if text_len >= MIN_PAGE_TEXT:
            return True
        n_children = r.get("n_children")
        if n_children is not None:
            return n_children == 0 and text_len >= MIN_LEAF_TEXT
        return False

    # ------------------------------------------------------------------ #
    #  the pipeline entry point                                            #
    # ------------------------------------------------------------------ #

    def fetch_documents(self, limit: Optional[int] = None) -> List[RegulatoryDocument]:
        result = self._run_crawl()
        self.last_result = result
        shape = result.get("shape", "generic")
        pages = result.get("pages", []) or []
        documents = result.get("documents", []) or []

        out: List[RegulatoryDocument] = []
        # Dedupe on (url, folder path), NOT url alone. Regulators cross-list one
        # document under several sections and each listing is its own place in the
        # library — the crawler now keys documents the same way, and the DB agrees
        # (document_exists_by_url is category-scoped). Deduping on url here would
        # silently collapse those back into one.
        seen = set()

        def _add(doc):
            if not doc:
                return
            key = (doc.document_url, " > ".join(doc.doc_path or []))
            if key not in seen:
                seen.add(key)
                out.append(doc)

        for d in documents:
            _add(self._doc_from_document_row(d, shape))

        if self._want_pages(pages):
            for r in pages:
                if not self._page_is_document(r):
                    continue                      # folder/index page, not a document
                _add(self._doc_from_page_row(r, shape))

        logger.info(
            "GenericSiteCrawler[%s/%s] shape=%s -> %d documents "
            "(%d file links, %d content pages) from %s",
            self.regulator, self.source_system, shape, len(out),
            len(documents), len(out) - len(documents), self.seed_url,
        )
        return out[:limit] if limit else out


class CompositeCrawler:
    """Runs several sources for one regulator and joins the results.

    Same shape as SAMACombinedCrawler, which already concatenates three different
    crawlers into one list — generalised so the sources can be a mix of generic
    crawlers and hand-written ones. Any object with fetch_documents() works.

    A failing source is logged and skipped; it must not take the others down.
    """

    def __init__(self, crawlers: List[object]):
        self.crawlers = crawlers

    def fetch_documents(self, limit: Optional[int] = None) -> List[RegulatoryDocument]:
        docs: List[RegulatoryDocument] = []
        for c in self.crawlers:
            name = getattr(c, "seed_url", None) or type(c).__name__
            try:
                got = c.fetch_documents() or []
                docs.extend(got)
                logger.info("  source ok: %s -> %d documents", name, len(got))
            except Exception as e:
                logger.error("  source FAILED: %s -> %s", name, e, exc_info=True)
        return docs[:limit] if limit else docs


def build_source(cfg: dict):
    """Turn ONE source entry from a regulator's YAML into a crawler object.

        mode: generic  -> GenericSiteCrawler (shared code, no per-site python)
        mode: custom   -> import and instantiate the named class

    Both come back with the same fetch_documents(), which is the whole point:
    whether a source is generic or hand-written stops mattering above this line.
    """
    mode = (cfg.get("mode") or "generic").lower()

    if mode == "custom":
        path = cfg.get("crawler_class")
        if not path or "." not in path:
            raise ValueError(f"source '{cfg.get('name')}': mode=custom needs "
                             f"crawler_class like 'crawler.x_wrapper.XCrawler'")
        module_name, _, cls_name = path.rpartition(".")
        import importlib
        cls = getattr(importlib.import_module(module_name), cls_name)
        return cls(**(cfg.get("init_kwargs") or {}))

    if mode != "generic":
        raise ValueError(f"source '{cfg.get('name')}': unknown mode '{mode}'")

    missing = [k for k in ("seed_url", "regulator", "source_system")
               if not cfg.get(k)]
    if missing:
        raise ValueError(f"source '{cfg.get('name')}': missing {missing}")

    return GenericSiteCrawler(
        seed_url=cfg["seed_url"],
        regulator=cfg["regulator"],
        source_system=cfg["source_system"],
        category=cfg.get("category"),
        scope=cfg.get("scope", "auto"),
        max_pages=int(cfg.get("max_pages", 150)),
        max_depth=int(cfg.get("max_depth", 8)),
        out_dir=cfg.get("out_dir"),
        include_pages=cfg.get("include_pages", "auto"),
    )


def build_regulator_crawler(config: dict):
    """Build the crawler for a whole regulator from its loaded YAML.

    A regulator is a LIST of sources, each independently generic or custom, so
    one regulator can mix both — e.g. SAMA's rulebook sectors on the generic
    engine while its circulars keep a tuned runner.
    """
    regulator = config.get("regulator")
    if not regulator:
        raise ValueError("config has no 'regulator'")
    sources = config.get("sources") or []
    if not sources:
        raise ValueError(f"{regulator}: config lists no sources")

    built = []
    for src in sources:
        merged = dict(src)
        merged.setdefault("regulator", regulator)
        built.append(build_source(merged))
    return CompositeCrawler(built)


__all__ = ["GenericSiteCrawler", "CompositeCrawler",
           "build_source", "build_regulator_crawler"]
