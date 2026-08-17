"""
run_cbb_sequential.py
=====================
Sequential crawl for CBB Rulebook + AML + CorpGov.

Why sequential: Thomson Reuters blocks concurrent requests with timeouts.
Sequential with 1.2s delay between requests is the only reliable approach.

Checkpoint resumes from where it left off if stopped.
Run overnight — expected ~20 hours for all 8 volumes.

Usage:
    python run_cbb_sequential.py
    python -m tests.run_cbb_sequential
"""

import logging
import os
import sys
import time
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Print hierarchy from crawler
logging.getLogger("cbb_test_crawlers.cbb_rulebook_crawler").setLevel(logging.INFO)
logging.getLogger("cbb_rulebook_crawler").setLevel(logging.INFO)

try:
    from cbb_test_crawlers.cbb_rulebook_crawler import (
        SIDEBAR_SEED, _collect_volumes, _process,
    )
    from cbb_test_crawlers.Aml_crawler_v2 import crawl_rulebook
except ImportError:
    from cbb_rulebook_crawler import (
        SIDEBAR_SEED, _collect_volumes, _process,
    )
    from Aml_crawler_v2 import crawl_rulebook

from storage.mssql_repo import MSSQLRepository
from models.models import RegulatoryDocument

# ── Config ────────────────────────────────────────────────────────────────────
RULEBOOK_DELAY  = 1.2
REGULATOR       = "Central Bank of Bahrain"
CHECKPOINT_FILE = Path("cbb_crawl_checkpoint.json")

SOURCE_SYSTEM_MAP = {
    "aml":     "CBB-AML-LAW",
    "corpgov": "CBB-CORPGOV",
}


# ── DB Connection ─────────────────────────────────────────────────────────────
def _get_repo() -> MSSQLRepository:
    return MSSQLRepository({
        "driver":   os.getenv("MSSQL_DRIVER"),
        "server":   os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
    })


# ── Checkpoint ────────────────────────────────────────────────────────────────
def _load_checkpoint() -> dict:
    if CHECKPOINT_FILE.exists():
        try:
            return json.loads(CHECKPOINT_FILE.read_text())
        except Exception:
            pass
    return {}


def _save_checkpoint(data: dict):
    CHECKPOINT_FILE.write_text(json.dumps(data, indent=2))


# ── Category helpers ──────────────────────────────────────────────────────────
def _get_or_create_category(repo, title, parent_id, cat_type) -> int:
    fid = repo.get_folder_id(title, parent_id)
    if not fid:
        fid = repo.insert_folder(title, parent_id, cat_type=cat_type)
    return fid


def _resolve_folder_chain(repo, doc_path) -> tuple:
    if not doc_path:
        return None, None
    parent_id = None
    for folder_title in doc_path[:-1]:
        parent_id = _get_or_create_category(repo, folder_title, parent_id, "F")
    leaf_id = _get_or_create_category(repo, doc_path[-1], parent_id, "R")
    return parent_id, leaf_id


# ── Storage: Rulebook ─────────────────────────────────────────────────────────
def store_rulebook_docs(repo, docs, vol_name="") -> dict:
    stats = {"folders": 0, "leaves": 0, "skipped": 0, "errors": 0}
    total = len(docs)

    for i, doc in enumerate(docs, 1):
        if i % 200 == 0:
            log.info(
                f"  [{vol_name[:35]}] {i:,}/{total:,} "
                f"stored:{stats['leaves']:,} skip:{stats['skipped']:,} err:{stats['errors']}"
            )
        try:
            if doc.is_folder:
                parent_id = None
                for t in doc.doc_path:
                    parent_id = _get_or_create_category(repo, t, parent_id, "F")
                stats["folders"] += 1
                continue

            if repo.get_regulation_id_by_doc_path(doc.doc_path):
                stats["skipped"] += 1
                continue

            _, leaf_id = _resolve_folder_chain(repo, doc.doc_path)

            reg = RegulatoryDocument(
                regulator       = REGULATOR,
                source_system   = "CBB-Rulebook",
                category        = doc.doc_path[1] if len(doc.doc_path) > 1 else "CBB Rulebook",
                title           = doc.title,
                document_url    = doc.url,
                source_page_url = doc.url,
                document_html   = doc.document_html,
                doc_path        = doc.doc_path,
                extra_meta      = {
                    "pdf_link":     doc.extra_meta.get("pdf_link"),
                    "pdf_links":    doc.extra_meta.get("pdf_links", []),
                    "faq_link":     doc.extra_meta.get("faq_link"),
                    "content_text": doc.content_text,
                    "content_hash": doc.content_hash,
                },
            )
            reg.compliancecategory_id = leaf_id
            reg.type = "R"

            reg_id = repo._insert_regulation(reg)
            repo.insert_regulation_version(
                regulation_id=reg_id, regulator=REGULATOR,
                content_html=doc.document_html or "",
                content_text=doc.content_text or "",
                content_hash=doc.content_hash,
                updated_date=None, change_summary="Initial crawl", status="active",
            )
            stats["leaves"] += 1

        except Exception as e:
            log.error(f"  ERR storing '{doc.title}': {e}")
            stats["errors"] += 1

    return stats


# ── Storage: AML / CorpGov ────────────────────────────────────────────────────
def store_aml_corpgov_docs(repo, source_key, docs) -> dict:
    source_system = SOURCE_SYSTEM_MAP.get(source_key, f"CBB-{source_key.upper()}")
    stats = {"folders": 0, "leaves": 0, "skipped": 0, "errors": 0}

    for doc in docs:
        try:
            doc_path = doc.path

            if doc.row_type == "F":
                parent_id = None
                for t in doc_path:
                    parent_id = _get_or_create_category(repo, t, parent_id, "F")
                stats["folders"] += 1
                continue

            if repo.get_regulation_id_by_doc_path(doc_path):
                stats["skipped"] += 1
                continue

            _, leaf_id = _resolve_folder_chain(repo, doc_path)

            reg = RegulatoryDocument(
                regulator       = REGULATOR,
                source_system   = source_system,
                category        = doc.category,
                title           = doc.title,
                document_url    = doc.url,
                source_page_url = doc.url,
                document_html   = doc.content_html,
                doc_path        = doc_path,
                extra_meta      = {
                    "content_text": doc.content_text,
                    "content_hash": doc.content_hash,
                    "source_key":   doc.source_key,
                },
            )
            reg.compliancecategory_id = leaf_id
            reg.type = "R"

            reg_id = repo._insert_regulation(reg)
            repo.insert_regulation_version(
                regulation_id=reg_id, regulator=REGULATOR,
                content_html=doc.content_html or "",
                content_text=doc.content_text or "",
                content_hash=doc.content_hash,
                updated_date=None, change_summary="Initial crawl", status="active",
            )
            stats["leaves"] += 1

        except Exception as e:
            log.error(f"  ERR storing '{doc.title}': {e}")
            stats["errors"] += 1

    return stats


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    t_start    = time.time()
    checkpoint = _load_checkpoint()

    log.info("=" * 60)
    log.info("=== CBB Sequential Crawl ===")
    log.info(f"  RULEBOOK_DELAY : {RULEBOOK_DELAY}s between requests")
    log.info(f"  CHECKPOINT     : {CHECKPOINT_FILE}")
    log.info("=" * 60)

    repo = _get_repo()

    # ── Discover volumes ──────────────────────────────────────────────────────
    log.info("Discovering sidebar volumes...")
    volumes = _collect_volumes(SIDEBAR_SEED)
    log.info(f"Found {len(volumes)} volumes\n")

    if not volumes:
        log.error("No volumes found — check network")
        return

    total_sources = len(volumes) + 2  # +AML +CorpGov
    done_count    = sum(1 for v in checkpoint.values() if v)

    # ── Crawl + store each volume one at a time ───────────────────────────────
    for idx, vol_node in enumerate(volumes, 1):
        vol_key = f"vol_{idx}"

        if checkpoint.get(vol_key):
            log.info(f"SKIP [{idx}/{len(volumes)}] {vol_node.text} (already done)")
            continue

        log.info(f"\n{'='*60}")
        log.info(f"CRAWLING [{idx}/{len(volumes)}] {vol_node.text}")
        log.info(f"{'='*60}")

        t0      = time.time()
        results = []
        visited = set()

        _process(
            node=vol_node, path=["CBB Rulebook"], depth=0,
            visited=visited, results=results, request_delay=RULEBOOK_DELAY,
        )

        elapsed = time.time() - t0
        leaves  = sum(1 for d in results if not d.is_folder)
        folders = sum(1 for d in results if d.is_folder)
        log.info(
            f"\nCRAWL DONE [{vol_node.text}]: "
            f"{len(results):,} docs ({folders} folders, {leaves} leaves) "
            f"in {elapsed/60:.1f}min"
        )

        # Store immediately
        log.info(f"STORING {len(results):,} docs...")
        stats = store_rulebook_docs(repo, results, vol_name=vol_node.text)
        log.info(
            f"STORED: leaves:{stats['leaves']:,} folders:{stats['folders']:,} "
            f"skipped:{stats['skipped']:,} errors:{stats['errors']}"
        )

        checkpoint[vol_key] = True
        _save_checkpoint(checkpoint)
        done_count += 1

        elapsed_total = time.time() - t_start
        log.info(
            f"Progress: {done_count}/{total_sources} done | "
            f"elapsed: {elapsed_total/3600:.1f}h"
        )

    # ── AML ───────────────────────────────────────────────────────────────────
    if checkpoint.get("aml"):
        log.info("\nSKIP AML Law (already done)")
    else:
        log.info("\n" + "="*60)
        log.info("CRAWLING AML Law")
        log.info("="*60)
        t0   = time.time()
        docs = crawl_rulebook("aml")
        log.info(f"AML crawl done: {len(docs):,} docs in {time.time()-t0:.0f}s")

        stats = store_aml_corpgov_docs(repo, "aml", docs)
        log.info(
            f"AML STORED: leaves:{stats['leaves']:,} folders:{stats['folders']:,} "
            f"skipped:{stats['skipped']:,} errors:{stats['errors']}"
        )
        checkpoint["aml"] = True
        _save_checkpoint(checkpoint)
        done_count += 1

    # ── Corporate Governance ──────────────────────────────────────────────────
    if checkpoint.get("corpgov"):
        log.info("\nSKIP Corporate Governance (already done)")
    else:
        log.info("\n" + "="*60)
        log.info("CRAWLING Corporate Governance")
        log.info("="*60)
        t0   = time.time()
        docs = crawl_rulebook("corpgov")
        log.info(f"CorpGov crawl done: {len(docs):,} docs in {time.time()-t0:.0f}s")

        stats = store_aml_corpgov_docs(repo, "corpgov", docs)
        log.info(
            f"CorpGov STORED: leaves:{stats['leaves']:,} folders:{stats['folders']:,} "
            f"skipped:{stats['skipped']:,} errors:{stats['errors']}"
        )
        checkpoint["corpgov"] = True
        _save_checkpoint(checkpoint)
        done_count += 1

    # ── Summary ───────────────────────────────────────────────────────────────
    total_time = time.time() - t_start
    log.info(f"\n{'='*60}")
    log.info(f"=== ALL COMPLETE ===")
    log.info(f"Total time: {total_time/3600:.1f}h ({total_time/60:.0f}min)")
    log.info(f"{'='*60}")


if __name__ == "__main__":
    main()