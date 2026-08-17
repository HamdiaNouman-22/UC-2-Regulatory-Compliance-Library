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

    log.info("=== Crawling CBB Regulations and Resolutions (Mode 1) ===")
    crawler = CBBCrawlerV2()
    docs = crawler.fetch_documents(mode="1")
    log.info(f"Found {len(docs)} documents")

    stored = 0
    skipped = 0
    errors = 0

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