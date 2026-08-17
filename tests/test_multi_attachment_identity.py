"""A multi-attachment row has NO document_url, so identity must come from elsewhere.

Decided 2026-08-12: for a card that carries several PDFs, the files live in
`extra_meta["attachment_links"]` and `document_url` is left EMPTY — no single file
names the row. The previous design put the FIRST file in `document_url`, which made
the row's identity depend on the order the site happened to list its files.

The consequence these tests exist for: the default identity
`(document_url, doc_path)` COLLAPSES when document_url is empty. Every card in one
folder would share `("", doc_path)`, so change detection could not tell SDAIA's 29
cards apart — they would collapse into one row, or duplicate, depending on which
side of the comparison ran first.

So such a row declares its own identity: `doc_path` + `extra_meta.attachment_links`.
"""

import tempfile
from pathlib import Path

import pytest

from dynamic_crawler.changesignal import fields_of, identity_key, resolve_field
from dynamic_crawler.formfill.excel_repo import ExcelRepo
from models.models import RegulatoryDocument

IDENTITY = ["doc_path", "extra_meta.attachment_links"]
FOLDER = ["SDAIA", "Laws and Regulations", "Data classification"]


def card(title, pdfs):
    d = RegulatoryDocument(
        regulator="SDAIA", source_system="Laws and Regulations",
        category="Laws", title=title, document_url="", doc_path=list(FOLDER))
    d.extra_meta = {
        "attachment_links": " | ".join(pdfs),
        "identity_fields": IDENTITY,
        "n_files": len(pdfs),
    }
    return d


PDPL = card("Personal Data Protection Law",
            ["https://sdaia.gov.sa/pdpl.pdf", "https://sdaia.gov.sa/pdpl-ir.pdf"])
AIAF = card("AI Adoption Framework",
            ["https://sdaia.gov.sa/ai-en.pdf", "https://sdaia.gov.sa/ai-ar.pdf"])


def test_document_urls_field_is_gone():
    """The list field was removed, not merely stopped being written."""
    import dataclasses
    names = {f.name for f in dataclasses.fields(RegulatoryDocument)}
    assert "document_urls" not in names
    assert not hasattr(RegulatoryDocument, "__post_init__")


def test_single_file_row_is_unaffected():
    d = RegulatoryDocument("SDAIA", "Laws and Regulations", "Laws",
                           "One File", "https://sdaia.gov.sa/only.pdf")
    assert d.document_url == "https://sdaia.gov.sa/only.pdf"


def test_default_identity_would_collide():
    """The reason the declared identity exists. If this ever stops colliding the
    rest of these tests are no longer proving anything."""
    a = identity_key(fields_of(PDPL, ["document_url", "doc_path"]))
    b = identity_key(fields_of(AIAF, ["document_url", "doc_path"]))
    assert a == b, "expected the default identity to collapse on empty document_url"


def test_declared_identity_separates_cards_in_one_folder():
    a = identity_key(fields_of(PDPL, IDENTITY))
    b = identity_key(fields_of(AIAF, IDENTITY))
    assert a != b


def test_resolve_field_reads_into_extra_meta():
    assert resolve_field(PDPL, "extra_meta.attachment_links").startswith("https://")
    # Also from a stored row, where extra_meta is a JSON STRING rather than a dict.
    row = {"extra_meta": '{"attachment_links": "https://x/a.pdf"}'}
    assert resolve_field(row, "extra_meta.attachment_links") == "https://x/a.pdf"
    # Absent keys and absent containers are empty, never an exception.
    assert resolve_field({"extra_meta": {}}, "extra_meta.nope") is None
    assert resolve_field({}, "extra_meta.nope") is None


def test_excel_lookup_finds_the_right_card():
    out = Path(tempfile.mkdtemp()) / "SDAIA.xlsx"
    repo = ExcelRepo(out)
    repo._insert_regulation(PDPL)
    repo._insert_regulation(AIAF)

    assert repo.find_by_identity_fields(
        fields_of(PDPL, IDENTITY))["title"] == PDPL.title
    assert repo.find_by_identity_fields(
        fields_of(AIAF, IDENTITY))["title"] == AIAF.title
    # A card we never stored must not match a sibling just because the folder does.
    ghost = card("Ghost", ["https://sdaia.gov.sa/absent.pdf"])
    assert repo.find_by_identity_fields(fields_of(ghost, IDENTITY)) is None


def test_losing_a_pdf_changes_identity():
    """Documented trade, asserted so nobody is surprised by it later: a card that
    gains or loses a file reads as one `new` plus one `disappeared`, not
    `modified`. That is the price of not letting file order name the row."""
    fewer = card(PDPL.title, ["https://sdaia.gov.sa/pdpl.pdf"])
    assert identity_key(fields_of(PDPL, IDENTITY)) != identity_key(
        fields_of(fewer, IDENTITY))
