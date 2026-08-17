"""The gate and the HTML-vs-PDF decision, pinned with fixed inputs.

No network, no database — the fetchers are passed in, so every branch is
exercised deterministically. These are the rules the lead specified; if someone
later "optimises" them into first-tier-wins, these fail.

    venv/Scripts/python.exe -m pytest tests/test_formfill_textinput.py -q
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dynamic_crawler.formfill.textinput import (  # noqa: E402
    containment, decide, is_file_url, same_content,
)

LAW = ("The following words and phrases wherever mentioned in this Law shall "
       "have the meanings assigned to them unless the context requires otherwise. "
       "Licensed companies shall provide credit information to the Central Bank "
       "within thirty days of a written request. A licensed company may not "
       "disclose credit information except as permitted by this Law. ") * 4

# Same regulation, OCR'd out of a PDF: headers, page furniture, a little noise.
LAW_AS_PDF = ("PAGE 1\n" + LAW.replace("thirty", "thirty ") +
              "\nPage 2 of 4    Official Gazette    ")

DIFFERENT = ("Applications for the Regulatory Sandbox are assessed against the "
             "eligibility criteria in the Framework Document. Stage one covers "
             "application form completion and initial evaluation of the proposed "
             "innovation and its business plan. ") * 4


# ---------------------------------------------------------------- is_file_url

def test_file_urls_recognised():
    for u in ["https://x.gov/a.pdf", "https://x.gov/a.PDF?v=2", "https://x.gov/b.docx",
              "https://x.gov/c.xls", "https://x.gov/dl?wpdmdl=123",
              "https://x.gov/download/9", "https://x.gov/document/9"]:
        assert is_file_url(u), u


def test_pages_are_not_files():
    for u in ["https://www.sbp.org.pk/circulars/dmmd-circular-letter-no-10-of-2026",
              "https://rulebook.sama.gov.sa/en/node/11105",
              "https://laws.boe.gov.sa/BoeLaws/Laws/LawDetails/abc/1", ""]:
        assert not is_file_url(u), u


# ------------------------------------------------------------------ the gate

def test_gate_skips_when_there_is_nothing():
    d = decide(document_html="", document_url="")
    assert d.skip and "no html text" in d.reason


def test_gate_skips_an_external_page_when_it_cannot_be_fetched():
    """MISA's laws.boe.gov.sa links: a page, but no fetcher was supplied."""
    d = decide(document_url="https://laws.boe.gov.sa/BoeLaws/Laws/LawDetails/abc/1")
    assert d.skip


def test_gate_lets_a_page_through_when_it_can_be_fetched():
    """SBP: document_url is an HTML page and nothing was captured at crawl time."""
    d = decide(document_url="https://www.sbp.org.pk/circulars/x-of-2026",
               fetch_page_text=lambda u: LAW)
    assert not d.skip
    assert d.sources == ["html"]


def test_gate_lets_a_file_through():
    d = decide(document_url="https://x.gov/a.pdf", fetch_file_text=lambda u: LAW)
    assert not d.skip and d.sources == ["file"] and d.content_type == "pdf_text"


def test_gate_skips_when_everything_is_too_short():
    d = decide(document_html="tiny", document_url="https://x.gov/a.pdf",
               fetch_file_text=lambda u: "also tiny")
    assert d.skip and "reached 200 chars" in d.reason


# ------------------------------------------------------- the input decision

def test_identical_content_sends_html_only():
    d = decide(content_text=LAW, document_url="https://x.gov/law.pdf",
               attachment_url="https://x.gov/law.pdf",
               fetch_file_text=lambda u: LAW_AS_PDF)
    assert not d.skip
    assert d.sources == ["html"], d.reason
    assert d.overlap >= 0.8
    assert "duplicates" in d.reason


def test_different_content_sends_both():
    """The lead's call: when they differ, send both — losing a requirement is
    worse than spending a few hundred tokens."""
    d = decide(content_text=DIFFERENT, document_url="https://x.gov/law.pdf",
               attachment_url="https://x.gov/law.pdf",
               fetch_file_text=lambda u: LAW_AS_PDF)
    assert not d.skip
    assert d.sources == ["html", "file"], d.reason
    assert "SOURCE 1 OF 2" in d.text and "SOURCE 2 OF 2" in d.text
    # both bodies survive
    assert "Regulatory Sandbox" in d.text and "Official Gazette" in d.text


def test_a_short_page_against_a_full_pdf_sends_both():
    """SAMA rulebook stubs: 379 chars of page text against a whole PDF. The lead
    chose both rather than preferring the longer one."""
    stub = ("Guidance Notes on Completing the SAMA Regulatory Sandbox Application "
            "Form. Refer to the attached document for the full guidance. ") * 3
    d = decide(content_text=stub, attachment_url="https://x.gov/SAMA_EN_8320.pdf",
               document_url="https://rulebook.sama.gov.sa/en/guidance-notes",
               fetch_file_text=lambda u: DIFFERENT)
    assert d.sources == ["html", "file"]


def test_content_text_is_preferred_over_raw_html():
    d = decide(content_text=LAW, document_html="<div>" + DIFFERENT + "</div>")
    assert d.text == LAW.strip()


def test_combined_output_is_typed_pdf_text():
    """Not 'html' — the analyser's HTML cleaner would mangle the PDF half."""
    d = decide(content_text=DIFFERENT, attachment_url="https://x.gov/a.pdf",
               document_url="https://x.gov/page", fetch_file_text=lambda u: LAW)
    assert d.content_type == "pdf_text"


# ------------------------------------------------------------- the similarity

def test_containment_is_high_for_ocr_of_the_same_text():
    assert containment(LAW, LAW_AS_PDF) >= 0.8


def test_containment_is_low_for_different_documents():
    assert containment(LAW, DIFFERENT) < 0.3


def test_a_summary_inside_a_longer_document_counts_as_the_same():
    """Containment, not Jaccard — a page summary fully quoted inside a big PDF
    must not be reported as different just because the PDF is longer."""
    summary = " ".join(LAW.split()[:60])
    assert same_content(summary, LAW + DIFFERENT)


def test_empty_text_is_not_similar_to_anything():
    assert containment("", LAW) == 0.0
    assert not same_content("", "")
