# save as run_aml_corpgov_server.py
import logging
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

from cbb_test_crawlers.Aml_crawler_v2 import crawl_rulebook
from storage.mssql_repo import MSSQLRepository
from run_cbb_parallel import store_aml_corpgov_docs, _get_repo

def main():
    repo = _get_repo()
    
    log.info("=== Crawling AML ===")
    docs = crawl_rulebook("aml")
    log.info(f"AML: {len(docs)} docs found")
    stats = store_aml_corpgov_docs(repo, "aml", docs)
    log.info(f"AML stored: {stats}")

    log.info("=== Crawling CorpGov ===")
    docs = crawl_rulebook("corpgov")
    log.info(f"CorpGov: {len(docs)} docs found")
    stats = store_aml_corpgov_docs(repo, "corpgov", docs)
    log.info(f"CorpGov stored: {stats}")

if __name__ == "__main__":
    main()