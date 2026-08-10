"""
test_rulebook_live.py — Common Volume only
"""

import logging
import os
import re
import sys
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).parent))
try:
    from cbb_test_crawlers.cbb_rulebook_crawler import (
        crawl_rulebook_sidebar, RulebookDoc, SIDEBAR_SEED,
    )
except ImportError:
    from cbb_test_crawlers.cbb_rulebook_crawler import (
        crawl_rulebook_sidebar, RulebookDoc, SIDEBAR_SEED,
    )

try:
    import xlsxwriter
except ImportError:
    print("Run: pip install xlsxwriter")
    sys.exit(1)

from storage.mssql_repo import MSSQLRepository
from models.models import RegulatoryDocument

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
MAX_VOLUMES   = None                        # 1 = Common Volume only
REQUEST_DELAY = 1.2
OUTPUT_XLSX   = "common_volume_results.xlsx"
HTML_DIR      = Path("html_output_common")

REGULATOR     = "Central Bank of Bahrain"
SOURCE_SYSTEM = "CBB Rulebook"

COLS = [
    "Row", "Title", "URL", "Doc Path", "Depth", "Type",
    "Content Hash", "PDF Link (primary)", "PDF Links (all)",
    "FAQ Link", "HTML File", "Content Preview (300 chars)",
]
COL_WIDTHS = [5, 42, 60, 75, 7, 8, 34, 60, 80, 55, 65, 60]


def _wrap_html(fragment: str, title: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"><title>{title}</title>
  <style>
    body  {{font-family:Arial,sans-serif;font-size:13px;color:#333;
            max-width:960px;margin:0 auto;padding:20px;line-height:1.6}}
    h1    {{color:#8b0000;border-bottom:2px solid #8b0000;padding-bottom:8px}}
    h2    {{color:#8b0000}}
    table {{border-collapse:collapse;width:100%;margin:12px 0;font-size:12px}}
    td,th {{border:1px solid #c0c0c0;padding:6px 10px;vertical-align:top}}
    th    {{background:#d3d3d3;font-weight:bold}}
    .FSAmnd{{background:#dce9f7;border:1px solid #a8c4e0;color:#1a4a7a;
              font-size:11px;padding:4px 8px;display:inline-block;border-radius:3px}}
    a     {{color:#0563c1}}
  </style>
</head>
<body>
  <h1>{title}</h1>
  {fragment}
</body>
</html>"""


def _slug(text: str, idx: int) -> str:
    return f"{idx:04d}_{re.sub(r'[^\\w\\-]', '_', text)[:60]}"


# ── DB storage ────────────────────────────────────────────────────────────────

def _store_doc(
    doc: RulebookDoc,
    repo: MSSQLRepository,
    volume_name: str,
    html_file: str,
    folder_id_map: dict,        # tuple(doc_path) → compliancecategory_id
) -> None:
    """
    Folder → compliancecategory  (type='F')
    Leaf   → regulations         (type='R') + regulation_versions snapshot
    """
    all_pdfs     = doc.extra_meta.get("pdf_links", [])
    all_pdfs_str = "\n".join(f"{p['name']}: {p['url']}" for p in all_pdfs)

    extra_meta = {
        **doc.extra_meta,
        "volume":        volume_name,
        "html_file":     html_file,
        "pdf_links_all": all_pdfs_str,
    }

    parent_path_key = tuple(doc.doc_path[:-1]) if len(doc.doc_path) > 1 else ()
    parent_id: Optional[int] = folder_id_map.get(parent_path_key)
    path_key = tuple(doc.doc_path)

    if doc.is_folder:
        existing_id = repo.get_folder_id(doc.title, parent_id)
        if existing_id:
            folder_id_map[path_key] = existing_id
            return
        folder_id = repo.insert_folder(
            title     = doc.title,
            parent_id = parent_id,
            type      = "F"
        )
        folder_id_map[path_key] = folder_id

    else:
        if repo.document_exists_by_source_url(doc.url):
            log.debug(f"  Already in DB, skipping: {doc.url}")
            return

        reg_doc = RegulatoryDocument(
            regulator             = REGULATOR,
            source_system         = SOURCE_SYSTEM,
            category              = volume_name,
            title                 = doc.title,
            document_url          = doc.url,
            source_page_url       = doc.url,
            doc_path              = doc.doc_path,
            published_date        = None,
            reference_no          = None,
            department            = None,
            year                  = None,
            extra_meta            = extra_meta,
            compliancecategory_id = parent_id,
            document_html         = doc.document_html,
            type                  = "R"
        )
        reg_id = repo._insert_regulation(reg_doc)

        if doc.content_hash:
            repo.update_cbb_content_hash(reg_id, doc.content_hash)

        repo.insert_cbb_version(
            regulation_id = reg_id,
            content_html  = doc.document_html or "",
            content_text  = doc.content_text  or "",
            content_hash  = doc.content_hash  or "",
            updated_date  = None,
            change_summary= "Initial crawl",
        )


def _store_all(docs: List[RulebookDoc], repo: MSSQLRepository, html_files: dict) -> None:
    """Walk all docs in order and store each one. Parents always come before children."""
    folder_id_map: dict = {}
    volume_name = docs[0].title if docs else "Common Volume"
    ok = err = 0

    for doc in docs:
        html_file = html_files.get(doc.url, "")
        try:
            _store_doc(doc, repo, volume_name, html_file, folder_id_map)
            ok += 1
        except Exception as e:
            log.error(f"  DB failed [{doc.title[:50]}]: {e}")
            err += 1

    log.info(f"DB done — {ok} ok / {err} errors")


# ── Excel ─────────────────────────────────────────────────────────────────────

def _build_excel(docs: List[RulebookDoc], html_dir: Path, out_path: Path) -> dict:
    """Unchanged from original — also returns {url: html_file} for DB use."""
    wb = xlsxwriter.Workbook(
        str(out_path),
        options={"constant_memory": True, "strings_to_urls": False}
    )

    hdr = wb.add_format({
        "bold": True, "font_name": "Arial", "font_size": 10,
        "bg_color": "#1F4E79", "font_color": "#FFFFFF",
        "align": "center", "valign": "top", "border": 1,
    })
    fld = wb.add_format({
        "font_name": "Arial", "font_size": 10, "bg_color": "#D9E1F2",
        "align": "left", "valign": "top", "text_wrap": True,
    })
    lef = wb.add_format({
        "font_name": "Arial", "font_size": 10, "bg_color": "#FFFFFF",
        "align": "left", "valign": "top", "text_wrap": True,
    })
    sh  = wb.add_format({"font_name": "Arial", "font_size": 10})
    shb = wb.add_format({"font_name": "Arial", "font_size": 10, "bold": True})
    shh = wb.add_format({
        "bold": True, "font_name": "Arial", "font_size": 10,
        "bg_color": "#1F4E79", "font_color": "#FFFFFF",
    })

    # ── Summary sheet ─────────────────────────────────────────────────────────
    ws_s = wb.add_worksheet("Summary")
    ws_s.set_column(0, 0, 35)
    ws_s.set_column(1, 1, 20)
    ws_s.write(0, 0, "Metric", shh)
    ws_s.write(0, 1, "Value",  shh)

    total    = len(docs)
    folders  = sum(1 for d in docs if d.is_folder)
    leaves   = total - folders
    with_pdf = sum(1 for d in docs if d.extra_meta.get("pdf_link"))
    with_faq = sum(1 for d in docs if d.extra_meta.get("faq_link"))

    for r, (m, v) in enumerate([
        ("Volume",        "Common Volume"),
        ("Total docs",    f"{total:,}"),
        ("Folders",       f"{folders:,}"),
        ("Leaf pages",    f"{leaves:,}"),
        ("With PDF link", f"{with_pdf:,}"),
        ("With FAQ link", f"{with_faq:,}"),
    ], 1):
        ws_s.write(r, 0, m, shb if r == 1 else sh)
        ws_s.write(r, 1, v, shb if r == 1 else sh)

    # ── Common Volume sheet ───────────────────────────────────────────────────
    ws = wb.add_worksheet("Common Volume")
    ws.freeze_panes(1, 0)
    for ci, (col, w) in enumerate(zip(COLS, COL_WIDTHS)):
        ws.set_column(ci, ci, w)
        ws.write(0, ci, col, hdr)

    html_files: dict = {}   # url → html_file path

    for ri, doc in enumerate(docs, 1):
        fmt = fld if doc.is_folder else lef

        html_file = ""
        if not doc.is_folder and doc.document_html:
            fname = _slug(doc.title, ri) + ".html"
            fpath = html_dir / fname
            fpath.write_text(_wrap_html(doc.document_html, doc.title), encoding="utf-8")
            html_file = str(fpath.resolve())

        html_files[doc.url] = html_file

        all_pdfs = doc.extra_meta.get("pdf_links", [])
        all_pdfs_str = "\n".join(f"{p['name']}: {p['url']}" for p in all_pdfs)

        row = [
            ri,
            doc.title,
            doc.url,
            " > ".join(doc.doc_path),
            doc.depth,
            "Folder" if doc.is_folder else "Leaf",
            doc.content_hash,
            doc.extra_meta.get("pdf_link") or "",
            all_pdfs_str,
            doc.extra_meta.get("faq_link") or "",
            html_file,
            (doc.content_text or "")[:300],
        ]
        for ci, val in enumerate(row):
            if isinstance(val, (int, float)):
                ws.write_number(ri, ci, val, fmt)
            else:
                ws.write_string(ri, ci, str(val or "")[:32767], fmt)

    wb.close()
    size_mb = out_path.stat().st_size / 1_048_576
    log.info(f"Excel saved -> {out_path.resolve()}  ({size_mb:.1f} MB)")
    return html_files


def main():
    HTML_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=== Crawling: Common Volume only ===\n")
    docs = crawl_rulebook_sidebar(
        seed_url=SIDEBAR_SEED,
        request_delay=REQUEST_DELAY,
        max_volumes=MAX_VOLUMES,
    )

    log.info(f"\nTotal: {len(docs)} documents")

    # Print tree
    print(f"\n{'='*65}")
    print("COMMON VOLUME TREE")
    print("="*65)
    for doc in docs:
        indent = "  " * doc.depth
        kind   = "[FOLDER]" if doc.is_folder else "[LEAF  ]"
        print(f"{indent}{kind} {doc.title[:62]}")
        if doc.extra_meta.get("pdf_link"):
            print(f"{indent}         PDF -> {doc.extra_meta['pdf_link'][:62]}")
    print("="*65)

    log.info("\nBuilding Excel...")
    html_files = _build_excel(docs, HTML_DIR, Path(OUTPUT_XLSX))
    log.info(f"HTML  -> {HTML_DIR.resolve()}/")

    log.info("\nStoring to DB...")
    repo = MSSQLRepository({
        "driver":   os.getenv("MSSQL_DRIVER"),
        "server":   os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "trusted_connection": "yes",
    })
    _store_all(docs, repo, html_files)


if __name__ == "__main__":
    main()