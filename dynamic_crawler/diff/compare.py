"""CLI: diff the new config-driven engine's output against a baseline -- either
the cached crawler/../sama_finance_sector_requests.json artifact, or a fresh
live run of the existing crawler.sama_finance_sector_crawler.SAMAFinanceSectorCrawler
(read-only against the site, no DB involved). This is the accuracy proof for
Phase 1: the new engine must reproduce what the trusted existing crawler finds.

Usage:
    python -m dynamic_crawler.diff.compare \\
        --new output/dynamic_crawler/sama_finance_sector_new.json \\
        --baseline-file sama_finance_sector_requests.json \\
        --config config/regulators/sama.finance_sector.yml \\
        --report output/dynamic_crawler/diff_report.json

    python -m dynamic_crawler.diff.compare \\
        --new output/dynamic_crawler/sama_finance_sector_new.json \\
        --fresh-baseline \\
        --config config/regulators/sama.finance_sector.yml \\
        --report output/dynamic_crawler/diff_report_fresh.json
"""

import argparse
import hashlib
import json
import logging
import re
from dataclasses import asdict
from typing import Dict

from bs4 import BeautifulSoup

from dynamic_crawler.config_loader import load_config
from dynamic_crawler.urlnorm import canonical

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

COMPARE_FIELDS = ["title", "published_date", "reference_no", "category", "file_type", "doc_path", "extra_meta"]


def _normalized_html_hash(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    text = re.sub(r"\s+", " ", soup.get_text()).strip()
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _load_docs_from_json(path: str) -> Dict[str, dict]:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    by_url = {}
    for doc in raw:
        url = doc.get("document_url")
        if not url:
            continue
        by_url[canonical(url)] = doc
    return by_url


def _load_docs_from_fresh_crawl(sector_name: str, seed_url: str, use_selenium: bool) -> Dict[str, dict]:
    from crawler.sama_finance_sector_crawler import SAMAFinanceSectorCrawler
    crawler = SAMAFinanceSectorCrawler(use_selenium=use_selenium, sector_name=sector_name, seed_url=seed_url)
    documents = crawler.fetch_documents()
    by_url = {}
    for doc in documents:
        by_url[canonical(doc.document_url)] = asdict(doc)
    return by_url


def compare(new_docs: Dict[str, dict], baseline_docs: Dict[str, dict], cfg: dict) -> dict:
    new_urls = set(new_docs)
    baseline_urls = set(baseline_docs)

    missing_in_new = sorted(baseline_urls - new_urls)
    extra_in_new = sorted(new_urls - baseline_urls)
    common = new_urls & baseline_urls

    field_mismatches = []
    for url in sorted(common):
        new_doc = new_docs[url]
        base_doc = baseline_docs[url]
        diffs = {}
        for field_name in COMPARE_FIELDS:
            nv = new_doc.get(field_name) or ({} if field_name == "extra_meta" else None)
            bv = base_doc.get(field_name) or ({} if field_name == "extra_meta" else None)
            if nv != bv:
                diffs[field_name] = {"new": nv, "baseline": bv}

        new_html_hash = _normalized_html_hash(new_doc.get("document_html"))
        base_html_hash = _normalized_html_hash(base_doc.get("document_html"))
        if new_html_hash != base_html_hash:
            diffs["document_html_hash"] = {"new": new_html_hash, "baseline": base_html_hash}

        if diffs:
            field_mismatches.append({
                "document_url": url,
                "title": new_doc.get("title") or base_doc.get("title"),
                "diffs": diffs,
            })

    validation_cfg = cfg["validation"]
    new_count = len(new_docs)
    count_in_range = validation_cfg["expected_doc_count_min"] <= new_count <= validation_cfg["expected_doc_count_max"]

    report = {
        "new_document_count": new_count,
        "baseline_document_count": len(baseline_docs),
        "count_in_expected_range": count_in_range,
        "missing_in_new": [{"document_url": u, "title": baseline_docs[u].get("title")} for u in missing_in_new],
        "extra_in_new": [{"document_url": u, "title": new_docs[u].get("title")} for u in extra_in_new],
        "field_mismatches": field_mismatches,
        "clean": not missing_in_new and not extra_in_new and not field_mismatches and count_in_range,
    }
    return report


def main():
    parser = argparse.ArgumentParser(description="Diff dynamic_crawler engine output against the existing SAMA crawler's output.")
    parser.add_argument("--new", required=True, help="Path to the new engine's output JSON")
    parser.add_argument("--config", required=True, help="Path to the config used for the new run (for expected doc-count range)")
    parser.add_argument("--baseline-file", help="Path to a cached baseline JSON (e.g. sama_finance_sector_requests.json)")
    parser.add_argument("--fresh-baseline", action="store_true", help="Run the existing SAMAFinanceSectorCrawler live instead of using a cached file")
    parser.add_argument("--selenium", action="store_true", help="Use selenium backend for --fresh-baseline")
    parser.add_argument("--report", required=True, help="Output path for the diff report JSON")
    args = parser.parse_args()

    if not args.baseline_file and not args.fresh_baseline:
        parser.error("Must pass either --baseline-file or --fresh-baseline")

    cfg = load_config(args.config)
    new_docs = _load_docs_from_json(args.new)

    if args.fresh_baseline:
        logger.info("Running existing SAMAFinanceSectorCrawler live for a fresh baseline...")
        baseline_docs = _load_docs_from_fresh_crawl(cfg["tab_name"], cfg["seed_url"], use_selenium=args.selenium)
    else:
        baseline_docs = _load_docs_from_json(args.baseline_file)

    report = compare(new_docs, baseline_docs, cfg)

    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    logger.info(
        f"Diff report: new={report['new_document_count']} baseline={report['baseline_document_count']} "
        f"missing={len(report['missing_in_new'])} extra={len(report['extra_in_new'])} "
        f"field_mismatches={len(report['field_mismatches'])} clean={report['clean']}"
    )
    logger.info(f"Full report written to {args.report}")


if __name__ == "__main__":
    main()
