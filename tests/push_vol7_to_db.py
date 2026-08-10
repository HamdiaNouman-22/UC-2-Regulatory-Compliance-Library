# save as tests/crawl_vol7.py
import sys
import logging
import json
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

from crawl_to_excel import crawl_volume, save_to_excel, save_to_cache, _collect_volumes

def main():
    log.info("Discovering volumes...")
    volumes = _collect_volumes("https://cbben.thomsonreuters.com/rulebook/common-volume")
    
    vol7 = next((v for v in volumes if "Collective" in v.text), None)
    if not vol7:
        log.error("Vol 7 not found. Available volumes:")
        for v in volumes:
            log.info(f"  - {v.text}")
        return

    log.info(f"Found: {vol7.text}")
    log.info("Starting crawl...")

    outcome = crawl_volume(vol7)
    docs = outcome["results"]
    leaves = [d for d in docs if not d.is_folder]
    folders = [d for d in docs if d.is_folder]

    log.info(f"Crawl complete: {len(docs)} total ({len(folders)} folders, {len(leaves)} leaves)")

    # Save Excel for review
    excel_path = save_to_excel(outcome["volume"], docs)
    log.info(f"Excel saved: {excel_path}")

    # Save cache as vol7_pending.json
    cache_dir = Path("output/cache")
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / "vol7_pending.json"

    records = []
    for doc in docs:
        from crawl_to_excel import _clean_html, _html_to_text
        raw_html = getattr(doc, "document_html", "") or ""
        clean_html = _clean_html(raw_html)
        raw_text = getattr(doc, "content_text", "") or ""
        clean_text = raw_text if raw_text else _html_to_text(clean_html)
        extra = getattr(doc, "extra_meta", {}) or {}
        records.append({
            "is_folder":     doc.is_folder,
            "title":         doc.title or "",
            "url":           doc.url or "",
            "doc_path":      doc.doc_path,
            "document_html": clean_html,
            "content_text":  clean_text,
            "content_hash":  getattr(doc, "content_hash", "") or "",
            "pdf_link":      extra.get("pdf_link") or "",
            "pdf_links":     extra.get("pdf_links") or [],
            "faq_link":      extra.get("faq_link") or "",
            "extra_meta":    extra,
        })

    payload = {
        "volume":     outcome["volume"],
        "crawled_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "docs":       records,
    }
    cache_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    log.info(f"Cache saved: {cache_path}")

    log.info("")
    log.info("=" * 60)
    log.info(f"Review Excel: {excel_path.resolve()}")
    log.info(f"If correct, run: python tests/push_vol7_to_db.py")
    log.info("=" * 60)

if __name__ == "__main__":
    main()