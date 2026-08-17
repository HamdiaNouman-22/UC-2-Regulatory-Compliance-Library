from dataclasses import dataclass, field
from typing import Optional, Dict, List


@dataclass
class RegulatoryDocument:
    """
    Standard regulatory document model for SECP/SBP.

    ONE ROW IS ONE INSTRUMENT, WHICH MAY CARRY SEVERAL FILES

    Some regulators publish an instrument as a card holding several PDFs — SDAIA's
    "Personal Data Protection Law and The implementing Regulation" attaches three.
    The existing manual library models that as ONE entry with several attachments,
    and this system has to match it.

    THE FILES LIVE IN extra_meta, AND document_url IS LEFT EMPTY.

        document_url                 ""                      (nothing to name)
        extra_meta.attachment_links  "<pdf> | <pdf> | <pdf>"

    A single-file source is unchanged: `document_url` is that file.

    An earlier revision of this class carried a `document_urls: List[str]` field
    with `document_url` mirroring the first entry. It was removed 2026-08-12 —
    naming a row by whichever file happened to be listed first made the row's
    identity depend on the site's ordering.

    IDENTITY WHEN document_url IS EMPTY

    Identity defaults to (document_url, doc_path), which collapses for these rows:
    every card in one folder would share ("", doc_path). So a multi-attachment row
    DECLARES its own identity in `extra_meta["identity_fields"]`:

        ["doc_path", "extra_meta.attachment_links", "title"]

    the folder plus the set of files it carries. `_identity_for` in
    formfill/orch.py honours that per document, so single- and multi-file sources
    coexist in one run.

    Known trade: if a card gains or loses a PDF its identity changes, so it reads
    as one `new` plus one `disappeared` rather than `modified`. That is the cost of
    not letting file order name the row.
    """

    # ---- Identity ----
    regulator: str
    source_system: str
    category: str

    # ---- Title / URLs ----
    title: str
    # Defaulted, and legitimately EMPTY for a multi-attachment row — the files are
    # in extra_meta["attachment_links"]. Still the 5th positional argument, so
    # every existing positional call is unaffected.
    document_url: str = ""
    urdu_url: Optional[str] = None

    # ---- Metadata ----
    published_date: Optional[str] = None
    reference_no: Optional[str] = None
    fingerprint: Optional[str] = None

    # ---- Folder / compliance category ----
    compliancecategory_id: Optional[int] = None  # links to COMPLIANCECATEGORY table
    doc_path: Optional[list] = None             # hierarchy for folder creation

    # ---- SBP Context / optional ----
    department: Optional[str] = None
    year: Optional[str] = None

    # ---- Source Page ----
    source_page_url: Optional[str] = None

    file_type: Optional[str] = None
    extra_meta: Dict = field(default_factory=dict)

    # ---- HTML content ----
    document_html: Optional[str] = None
    type: Optional[str] = None 

    # ---- DB assigned ID ----
    id: Optional[int] = None

 # ---- Content hash (for CBB change detection) ----
    content_hash: Optional[str] = None

