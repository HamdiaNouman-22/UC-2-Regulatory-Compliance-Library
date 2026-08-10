"""WHAT TEXT GOES TO THE LLM — the gate, and the HTML-vs-PDF decision.

Replaces `extract_text_content_unified`'s "first tier that has enough text wins"
with two rules the lead specified:

  THE GATE — proceed only when there is something to analyse:
      document_html has real text,  OR
      document_url is a file (.pdf/.doc/…),  OR
      document_url is a page we can fetch
    Otherwise skip: insert the document, log it, analyse nothing. A regulator
    that publishes only an external link (MISA's 24 laws.boe.gov.sa entries) is
    stored and left alone rather than silently half-processed.

  THE INPUT — when both an HTML rendering and a file exist:
      they say the same thing  ->  send the HTML only
      they differ              ->  SEND BOTH
    Both, deliberately. A PDF is often the authoritative text while the page is a
    summary, and the reverse happens too — SAMA pages run 379 characters against
    a full PDF. Dropping either risks dropping a requirement, and a few hundred
    wasted tokens is the cheaper mistake.

  "SAY THE SAME THING" is not a hash. OCR never matches HTML byte for byte, so
  this compares 5-word shingles and asks how much of the shorter text appears in
  the longer one. Cheap, no LLM, tolerant of whitespace and OCR noise.

This module does no I/O. The caller passes in the two fetchers, so it is testable
without a network or a database — see tests/test_formfill_textinput.py.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, List, Optional

# Matches orchestrator.py's MIN_TEXT_LEN. Kept as a parameter so the two cannot
# drift silently: the caller passes its own value in.
DEFAULT_MIN_TEXT_LEN = 200

# How much of the shorter text must appear in the longer one before we call them
# the same document. 0.8 tolerates OCR noise, headers and footers; 0.95 would
# treat almost every PDF as different and send both every time.
SAME_CONTENT_THRESHOLD = 0.8

SHINGLE_SIZE = 5

_FILE_EXT = re.compile(r"\.(pdf|docx?|xlsx?|pptx?|rtf|txt)(\?|#|$)", re.I)
_DOWNLOAD_HINT = re.compile(r"wpdmdl=|/download/|/document/|attachment", re.I)

# The separator the model sees when both sources are sent. Explicit, because the
# model must know it is reading one regulation twice and not two regulations.
BOTH_HEADER_HTML = "=== SOURCE 1 OF 2 — text of the published web page ==="
BOTH_HEADER_FILE = "=== SOURCE 2 OF 2 — text of the attached document ({name}) ==="


def is_file_url(url: str) -> bool:
    """Does this URL point at a document rather than a web page?"""
    u = (url or "").strip()
    return bool(u) and bool(_FILE_EXT.search(u) or _DOWNLOAD_HINT.search(u))


def _words(text: str) -> List[str]:
    return re.findall(r"[a-z0-9؀-ۿ]+", (text or "").lower())


def shingles(text: str, n: int = SHINGLE_SIZE) -> set:
    """Overlapping n-word groups. Word sets alone would call any two documents
    about the same subject identical; ordered groups will not."""
    w = _words(text)
    if len(w) < n:
        return {" ".join(w)} if w else set()
    return {" ".join(w[i:i + n]) for i in range(len(w) - n + 1)}


def containment(a: str, b: str) -> float:
    """How much of the SHORTER text appears in the longer one, 0.0–1.0.

    Containment, not Jaccard: a 5-page PDF that fully includes a 1-paragraph page
    summary should score high. Jaccard would score it low simply because the PDF
    is bigger, and we would send both when one would do.
    """
    sa, sb = shingles(a), shingles(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / min(len(sa), len(sb))


def same_content(a: str, b: str, threshold: float = SAME_CONTENT_THRESHOLD) -> bool:
    return containment(a, b) >= threshold


@dataclass
class Decision:
    """What to analyse, and why — the `why` is logged so a skipped document can
    always be explained without re-running anything."""
    skip: bool
    reason: str
    text: Optional[str] = None
    content_type: str = "html"
    sources: List[str] = field(default_factory=list)
    overlap: Optional[float] = None

    def __str__(self) -> str:
        if self.skip:
            return f"SKIP — {self.reason}"
        return (f"ANALYSE {'+'.join(self.sources)} ({len(self.text or ''):,} chars) "
                f"— {self.reason}")


def decide(
    *,
    document_html: str = "",
    content_text: str = "",
    document_url: str = "",
    attachment_url: str = "",
    fetch_file_text: Optional[Callable[[str], Optional[str]]] = None,
    fetch_page_text: Optional[Callable[[str], Optional[str]]] = None,
    min_text_len: int = DEFAULT_MIN_TEXT_LEN,
    threshold: float = SAME_CONTENT_THRESHOLD,
) -> Decision:
    """Apply the gate, then choose the input.

    `content_text` is the crawler's already-extracted page text and is preferred
    over `document_html` — it is the same content with the markup already gone.
    `attachment_url` is extra_meta["org_pdf_link"] when the page has a file
    hanging off it; when `document_url` is itself a file, that is used instead.
    """
    html_text = (content_text or "").strip() or (document_html or "").strip()
    doc_is_file = is_file_url(document_url)
    file_url = document_url if doc_is_file else (attachment_url or "").strip()
    page_url = "" if doc_is_file else (document_url or "").strip()

    # ---- THE GATE ----------------------------------------------------------
    if len(html_text) < min_text_len and not file_url and not page_url:
        return Decision(True, "no html text, no file, no page to fetch")

    # ---- gather what we can -----------------------------------------------
    file_text = ""
    if file_url and fetch_file_text:
        file_text = (fetch_file_text(file_url) or "").strip()

    if len(html_text) < min_text_len and page_url and fetch_page_text:
        # The document_url is a web page and nothing was captured at crawl time,
        # so read it now. This is what keeps SBP's circulars and MISA's external
        # law-portal links alive; without it the gate drops them.
        html_text = (fetch_page_text(page_url) or "").strip()

    html_ok = len(html_text) >= min_text_len
    file_ok = len(file_text) >= min_text_len

    # ---- THE INPUT --------------------------------------------------------
    if html_ok and file_ok:
        ov = containment(html_text, file_text)
        if ov >= threshold:
            # The file is the same regulation rendered as a document. Send the
            # HTML: it is already text, so it needs no OCR trust.
            return Decision(False, f"file duplicates the page (overlap {ov:.2f}) — html only",
                            html_text, "html", ["html"], ov)
        name = file_url.rsplit("/", 1)[-1][:80] or "attached file"
        combined = "\n\n".join([
            BOTH_HEADER_HTML, html_text,
            BOTH_HEADER_FILE.format(name=name), file_text,
        ])
        # "pdf_text" so the analyser's normaliser leaves it alone: the HTML half
        # is already plain text by this point, and running the HTML cleaner over
        # the combined string would mangle the PDF half.
        return Decision(False, f"page and file differ (overlap {ov:.2f}) — sending both",
                        combined, "pdf_text", ["html", "file"], ov)

    if html_ok:
        return Decision(False, "html only", html_text, "html", ["html"])
    if file_ok:
        return Decision(False, "file only", file_text, "pdf_text", ["file"])

    got = f"html {len(html_text)} chars, file {len(file_text)} chars"
    return Decision(True, f"nothing reached {min_text_len} chars ({got})")


def decide_for_document(doc, *, fetch_file_text=None, fetch_page_text=None,
                        min_text_len: int = DEFAULT_MIN_TEXT_LEN) -> Decision:
    """Convenience wrapper for a RegulatoryDocument."""
    meta = getattr(doc, "extra_meta", None) or {}
    return decide(
        document_html=getattr(doc, "document_html", "") or "",
        content_text=meta.get("content_text") or "",
        document_url=getattr(doc, "document_url", "") or "",
        attachment_url=meta.get("org_pdf_link") or "",
        fetch_file_text=fetch_file_text,
        fetch_page_text=fetch_page_text,
        min_text_len=min_text_len,
    )


__all__ = ["Decision", "decide", "decide_for_document", "is_file_url",
           "containment", "same_content", "shingles"]
