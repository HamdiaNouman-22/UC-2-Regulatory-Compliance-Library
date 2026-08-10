"""
push_vol7_draft.py
==================
Pushes Vol 7 under "CBB Rulebook DRAFT" category for frontend preview.
Once satisfied, delete draft and replace existing Vol 7.

Usage:
    python tests/push_vol7_draft.py
"""
import json
import sys
import os
import logging
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env", override=True)

from storage.mssql_repo import MSSQLRepository
from models.models import RegulatoryDocument

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

REGULATOR  = "Central Bank of Bahrain"
SOURCE_SYS = "CBB-Rulebook"
DRAFT_ROOT = "CBB Rulebook"


def _build_repo():
    return MSSQLRepository({
        "server":   os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver":   os.getenv("MSSQL_DRIVER", "{ODBC Driver 17 for SQL Server}"),
    })


def _get_or_create(repo, title, parent_id, cat_type="F"):
    fid = repo.get_folder_id(title, parent_id)
    if not fid:
        fid = repo.insert_folder(title, parent_id, cat_type=cat_type)
    return fid


def _resolve_chain(repo, doc_path, cache, is_leaf=False):
    """
    Walk doc_path creating/fetching categories.
    - All intermediate segments → type F
    - Last segment → type R only if is_leaf=True, otherwise F
    """
    parent_id = None
    for i, title in enumerate(doc_path):
        key = tuple(doc_path[:i+1])
        if key in cache:
            parent_id = cache[key]
            continue
        # Only the last segment of a leaf regulation gets type R
        is_last = (i == len(doc_path) - 1)
        cat_type = "R" if (is_last and is_leaf) else "F"
        folder_id = _get_or_create(repo, title, parent_id, cat_type)
        cache[key] = folder_id
        parent_id = folder_id
    return parent_id


def main():
    cache_path = ROOT / "output" / "cache" / "vol7_pending.json"
    if not cache_path.exists():
        log.error(f"Cache not found: {cache_path}")
        sys.exit(1)

    payload = json.loads(cache_path.read_text(encoding="cp1252"))
    docs = payload["docs"]
    log.info(f"Loaded {len(docs)} docs from cache")

    # Replace "CBB Rulebook" with DRAFT_ROOT in all doc_paths
    for doc in docs:
        if doc["doc_path"] and doc["doc_path"][0] == "CBB Rulebook":
            doc["doc_path"][0] = DRAFT_ROOT

    leaves  = [d for d in docs if not d["is_folder"]]
    folders = [d for d in docs if d["is_folder"]]
    log.info(f"Folders: {len(folders)} | Leaves: {len(leaves)}")
    log.info(f"Will store under root: '{DRAFT_ROOT}'")

    # Preview first 5 paths
    log.info("Sample doc_paths:")
    for d in leaves[:3]:
        log.info(f"  LEAF: {d['doc_path']}")
    for d in folders[:3]:
        log.info(f"  FOLDER: {d['doc_path']}")

    confirm = input(f"\nPush {len(leaves)} regulations to SERVER DB under '{DRAFT_ROOT}'? [y/N] ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        return

    repo = _build_repo()
    cache = {}
    inserted = skipped = errors = 0

    # Process folders first to build the category tree
    log.info("Creating folder structure...")
    for doc in docs:
        if not doc["is_folder"]:
            continue
        try:
            _resolve_chain(repo, doc["doc_path"], cache, is_leaf=False)
        except Exception as e:
            log.error(f"ERR folder '{doc['title']}': {e}")
            errors += 1

    log.info(f"Folder structure created. Now inserting regulations...")

    # Then process leaves
    for doc in docs:
        if doc["is_folder"]:
            continue
        try:
            # Check duplicate
            if repo.get_regulation_id_by_doc_path(doc["doc_path"]):
                skipped += 1
                continue

            # is_leaf=True so last segment gets type R
            cat_id = _resolve_chain(repo, doc["doc_path"], cache, is_leaf=True)

            reg = RegulatoryDocument(
                regulator             = REGULATOR,
                source_system         = SOURCE_SYS,
                category              = doc["doc_path"][1] if len(doc["doc_path"]) > 1 else SOURCE_SYS,
                title                 = doc["title"],
                document_url          = doc.get("url", ""),
                source_page_url       = doc.get("url", ""),
                document_html         = doc.get("document_html", ""),
                doc_path              = doc["doc_path"],
                compliancecategory_id = cat_id,
                content_hash          = doc.get("content_hash", ""),
                extra_meta            = doc.get("extra_meta", {}),
            )
            reg.type = "R"
            repo._insert_regulation(reg)
            inserted += 1

        except Exception as e:
            log.error(f"ERR '{doc['title']}': {e}")
            errors += 1

    log.info(f"\nDone — Inserted: {inserted} | Skipped: {skipped} | Errors: {errors}")
    log.info(f"Check frontend under '{DRAFT_ROOT}'")


if __name__ == "__main__":
    main()
