"""Which `extra_meta` keys leave the API, and which never do.

AN ALLOWLIST, NOT A BLOCKLIST -- that is the whole point.

apis/pipeline_api.py used to do this instead:

    reg_dict["extra_meta"].pop("org_pdf_html", None)
    reg_dict["extra_meta"].pop("org_pdf_text", None)

which fails OPEN. Every new key a crawler invents is published the day it is
added, because nobody remembers to hide it. MEASURED 2026-08-24: `extra_meta`
holds 58 distinct keys across 9,359 rows, and only about a third describe the
DOCUMENT. The rest describe how we fetched, classified and analysed it:

    monitoring_status      9,359 rows   what the LAST RUN decided, not a fact
                                        about the document -- it reads on screen
                                        like a document state and is not one
    crawl_source           8,857        which configured source produced the row
    found_on               6,952        the listing page, provenance not content
    content_text           2,334        a copy of document_html, ~1,407 chars each
    existing_regulation_id   562        a working value from the matching step
    identity_fields          567        which fields identify the row
    text_ocr_* / analysis_* / snapshot_*  how the text was read and hashed

An allowlist fails CLOSED: a key nobody has decided about is simply absent, and
adding a regulator cannot leak its internals by accident.

TO PUBLISH A NEW KEY, add it here. That is a deliberate act, which is the point.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

#: The regulator's OWN fields -- what the source publishes about the document.
#: Grouped for readability; the API does not care about the grouping.
PUBLIC_META_KEYS = frozenset({
    # -- publication and lifecycle -------------------------------------------
    "status",              # the REGULATOR's status ("In-Force", "Superseded").
                           # NOT regulations.status, which is the review state.
                           # Label it "Regulator status" or the two get confused.
    "issue_date_hijri",    # "4/6/1444" -- Hijri, and ambiguous d/m vs m/d.
    "cbe_date",            # "08 Jan 2020"
    "authored_on",         # "14-Sha'ban-1438-10-May-2017" -- dual calendar
    "release_date",
    "execution_date",
    "last_update",         # "Last Update: 11 Mar 2026 04:57 PM Saudi ..."
    "moh_modified",        # ISO timestamp, MOH's own change stamp

    # -- classification and scope --------------------------------------------
    "scope_of_application",   # multi-valued, newline separated
    "cbe_categories",         # the regulator's own category
    "sector",
    "beneficiaries",          # multi-valued, " | " separated
    "doc_type",               # "Announcement", "FAQ"
    "law",                    # the parent law a part belongs to
    "resolution_number",

    # -- relationships and files ---------------------------------------------
    "superseded_by",       # a regulation id -- worth rendering as a link
    "attachment_links",    # the files, when a document has several. See
                           # utils/file_links.py for the one-file/many rule.
    "file_titles",         # their human names, parallel to attachment_links
})

#: Kept out of the payload even though they are large and might look useful.
#: Named only so the reason is written down somewhere.
#:   content_text     a copy of document_html; serve that column instead
#:   org_pdf_html     the original markup, megabytes on some rows
#:   related_regulations  MHRSD, averaging 26,076 chars over 62 rows
NEVER_PUBLISH = frozenset({"content_text", "org_pdf_html", "org_pdf_text",
                           "related_regulations", "monitoring_status"})


def public_extra_meta(meta: Any) -> Optional[Dict[str, Any]]:
    """`meta` with only the approved keys, or None if nothing survives.

    Accepts the dict the repo hands back, or the raw json string the column
    holds, so callers do not each have to remember which they have.
    """
    if meta is None:
        return None
    if isinstance(meta, str):
        import json
        try:
            meta = json.loads(meta)
        except Exception:
            return None
    if not isinstance(meta, dict):
        return None
    out = {k: v for k, v in meta.items()
           if k in PUBLIC_META_KEYS and v not in (None, "", [], {})}
    return out or None


def public_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """One regulation dict with its extra_meta filtered. Mutates and returns."""
    if isinstance(row, dict) and "extra_meta" in row:
        row["extra_meta"] = public_extra_meta(row.get("extra_meta"))
    return row


__all__ = ["PUBLIC_META_KEYS", "NEVER_PUBLISH", "public_extra_meta", "public_row"]
