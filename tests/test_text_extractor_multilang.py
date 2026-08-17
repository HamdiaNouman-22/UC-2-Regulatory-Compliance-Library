"""The PDF extraction fixes, pinned. No network, no tesseract needed.

Three things these lock down:

1. A page must not be discarded for being in a language the code had not heard
   of. `_is_bad_page` used to require Arabic or English specifically; the library
   spans KSA, Egypt and Bahrain today and is meant to grow.
2. `_is_pdf_scanned` must sample ACROSS the document. Judging from the first
   three pages sent an 81-page regulation — whose body extracts perfectly — to
   OCR on every page.
3. When OCR cannot run, that must be reported as OCR being unavailable, not as a
   poor scan.

    venv/Scripts/python.exe -m pytest tests/test_text_extractor_multilang.py -q
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from processor.Text_Extractor import OCRProcessor  # noqa: E402

fitz = pytest.importorskip("fitz")

# Real prose in several scripts, long enough to clear the length floors.
ARABIC = ("يجب على الشركات المرخصة تقديم المعلومات الائتمانية إلى البنك المركزي "
          "خلال ثلاثين يوما من تاريخ الطلب الكتابي وفقا لأحكام هذا النظام. ") * 6
ENGLISH = ("Licensed companies shall provide credit information to the Central "
           "Bank within thirty days of a written request under this Law. ") * 6
FRENCH = ("Les établissements agréés doivent communiquer les informations de "
          "crédit à la Banque centrale dans un délai de trente jours. ") * 6
TURKISH = ("Yetkili kuruluşlar, yazılı talep tarihinden itibaren otuz gün "
           "içinde kredi bilgilerini Merkez Bankasına iletmek zorundadır. ") * 6
URDU = ("لائسنس یافتہ کمپنیاں تحریری درخواست کی تاریخ سے تیس دن کے اندر "
        "کریڈٹ معلومات مرکزی بینک کو فراہم کریں گی۔ ") * 6
RUSSIAN = ("Лицензированные организации обязаны предоставлять кредитную "
           "информацию в Центральный банк в течение тридцати дней. ") * 6


# ------------------------------------------------- language-agnostic quality

@pytest.mark.parametrize("name,text", [
    ("arabic", ARABIC), ("english", ENGLISH), ("french", FRENCH),
    ("turkish", TURKISH), ("urdu", URDU), ("russian", RUSSIAN),
])
def test_real_prose_is_never_discarded_whatever_the_script(name, text):
    assert not OCRProcessor._is_bad_page(text), f"{name} prose was thrown away"


def test_genuine_garbage_is_still_discarded():
    assert OCRProcessor._is_bad_page("")
    assert OCRProcessor._is_bad_page("short")
    # a page of nothing but figures — a table of numbers, not prose
    assert OCRProcessor._is_bad_page(("1234567890 " * 60))
    # punctuation and symbols with no letters at all
    assert OCRProcessor._is_bad_page(("=-=-=-  ..... ///// " * 40))


def test_arabic_is_not_penalised_for_short_words():
    """The old rule counted only words longer than three characters and needed
    30 of them. Arabic words are short, so legitimate pages failed."""
    short_words = " ".join(["يجب", "على", "بنك", "مال", "سنة"] * 40)
    assert not OCRProcessor._is_bad_page(short_words)


def test_broken_font_mapping_is_detected():
    assert OCRProcessor._is_text_broken("�" * 40 + ENGLISH)


def test_good_text_is_not_called_broken():
    for text in (ARABIC, ENGLISH, RUSSIAN, URDU):
        assert not OCRProcessor._is_text_broken(text)


# ------------------------------------------------------- scanned-PDF sampling

def _pdf(page_texts) -> str:
    doc = fitz.open()
    for t in page_texts:
        pg = doc.new_page()
        if t:
            pg.insert_textbox(fitz.Rect(40, 40, 550, 780), t, fontsize=9)
    p = Path(fitz.__file__).parent / "_t.pdf"
    import tempfile
    p = Path(tempfile.gettempdir()) / "_extract_test.pdf"
    doc.save(str(p)); doc.close()
    return str(p)


def test_cover_pages_no_longer_trigger_full_ocr():
    """THE 81-PAGE BUG. Three near-empty front pages then a body that extracts
    fine. Judged on the first three pages this was 'scanned'."""
    pages = ["", "Contents", "  "] + [ENGLISH] * 12
    path = _pdf(pages)
    with fitz.open(path) as doc:
        assert not OCRProcessor._is_pdf_scanned(doc), \
            "a document with 12 readable pages was called a scan"


def test_a_real_scan_is_still_detected():
    """Every page empty of text — an image-only PDF."""
    path = _pdf(["", "", "", "", "", "", ""])
    with fitz.open(path) as doc:
        assert OCRProcessor._is_pdf_scanned(doc)


def test_mostly_empty_document_is_treated_as_a_scan():
    pages = [ENGLISH] + [""] * 9
    path = _pdf(pages)
    with fitz.open(path) as doc:
        assert OCRProcessor._is_pdf_scanned(doc)


def test_arabic_body_is_not_mistaken_for_a_scan():
    path = _pdf([ARABIC] * 6)
    with fitz.open(path) as doc:
        # Arabic may not round-trip through the default font; only assert when
        # the text layer actually came back, otherwise this proves nothing.
        if len(doc[0].get_text("text").strip()) >= 100:
            assert not OCRProcessor._is_pdf_scanned(doc)


# --------------------------------------------------------- language selection

def test_ocr_langs_drops_what_is_not_installed(monkeypatch):
    monkeypatch.setattr(OCRProcessor, "_lang_cache", ["eng", "fra"])
    monkeypatch.setattr("processor.Text_Extractor.OCR_LANGS", "ara+eng+fra")
    assert OCRProcessor.ocr_langs() == "eng+fra"


def test_availability_does_not_require_arabic(monkeypatch):
    """An English-only box used to report OCR as entirely unavailable because the
    check was `'ara' in langs`."""
    monkeypatch.setattr(OCRProcessor, "_lang_cache", ["eng"])
    monkeypatch.setattr("processor.Text_Extractor.OCR_LANGS", "eng")
    assert OCRProcessor.is_ocr_available()


def test_no_tesseract_means_unavailable(monkeypatch):
    monkeypatch.setattr(OCRProcessor, "_lang_cache", [])
    assert not OCRProcessor.is_ocr_available()


def test_ocr_page_reports_the_real_reason(monkeypatch, caplog):
    """Not 'low quality' — the engine is missing."""
    monkeypatch.setattr(OCRProcessor, "_lang_cache", [])
    with caplog.at_level("ERROR"):
        out = OCRProcessor._ocr_single_page("nonexistent.pdf", 1)
    assert out == ""
    assert "OCR unavailable" in caplog.text
