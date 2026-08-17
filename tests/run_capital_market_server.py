# save as tests/run_capital_market_server.py
import logging
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

from crawler.cbb_crawler import CBBCrawlerV2
from run_cbb_parallel import _resolve_folder_chain, _get_repo

def main():
    repo = _get_repo()

    log.info("=== Crawling CBB Capital Market Regulations (Mode 4) ===")
    crawler = CBBCrawlerV2()
    docs = crawler.fetch_documents(mode="4")
    log.info(f"Found {len(docs)} documents")

    if not docs:
        log.info("No documents found. Exiting.")
        return

    # Save to Excel
    try:
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Capital Market Docs"
        ws.append(["#", "Title", "Doc Path", "URL", "HTML Length", "Document HTML"])
        for i, doc in enumerate(docs, 1):
            ws.append([
                i,
                doc.title,
                " > ".join(doc.doc_path),
                doc.document_url,
                len(doc.document_html or ""),
                doc.document_html or "",
            ])
        excel_path = Path("tests/capital_market_preview.xlsx")
        wb.save(excel_path)
        log.info(f"Saved preview to {excel_path}")
    except Exception as e:
        log.warning(f"Could not save Excel: {e}")

    # Show preview
    print("\n" + "="*60)
    print(f"PREVIEW — {len(docs)} documents found:")
    print("="*60)
    for i, doc in enumerate(docs, 1):
        print(f"  [{i:>3}] {doc.title[:70]}")
        print(f"        path: {' > '.join(doc.doc_path)}")
        print(f"        html: {len(doc.document_html or '')} chars")
        print()

    # Ask permission
    confirm = input(f"\nStore these {len(docs)} documents to SERVER DB? [y/N] ").strip().lower()
    if confirm != "y":
        print("Aborted. Nothing stored.")
        return

    stored = skipped = errors = 0
    for doc in docs:
        try:
            if repo.get_regulation_id_by_doc_path(doc.doc_path):
                skipped += 1
                continue
            _, leaf_id = _resolve_folder_chain(repo, doc.doc_path)
            doc.compliancecategory_id = leaf_id
            doc.type = "R"
            repo._insert_regulation(doc)
            stored += 1
        except Exception as e:
            log.error(f"ERR {doc.title}: {e}")
            errors += 1

    log.info(f"Done — Stored: {stored} | Skipped: {skipped} | Errors: {errors}")

if __name__ == "__main__":
    main()