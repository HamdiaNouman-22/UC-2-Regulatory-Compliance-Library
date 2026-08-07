"""Self-verification of a test crawl against the live regulator site.

This is the accuracy heart of the autonomous loop. "Did the adapter run without
crashing" is not enough — we must confirm it extracted the RIGHT things. So for
a sample of the produced documents we independently re-fetch their source pages
and assert the extracted field values actually appear there, and that the
document links resolve.
"""

import logging
import random
import re
from dataclasses import asdict
from typing import List

import requests

from dynamic_crawler.fetcher import Fetcher
from models.models import RegulatoryDocument

logger = logging.getLogger(__name__)

PASS_FIELD_HIT_RATE = 0.90
DEFAULT_SAMPLE_K = 6

# Many regulator servers (e.g. *.gov.sa) drop connections from clients that send
# no User-Agent. The cross-check must look like a normal browser or it produces
# false negatives on perfectly valid document links.
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip().lower()


def _date_variants(date_str: str):
    """Yield loose variants of a date so format differences don't cause false misses."""
    d = date_str.strip()
    variants = {d, d.lower(), _normalize(d)}
    # split date tokens (digits/month names) so "01-Jan-2024" still matches "1 January 2024" partially
    tokens = re.findall(r"[A-Za-z]{3,}|\d{1,4}", d)
    variants.update(t.lower() for t in tokens if len(t) >= 3)
    return {v for v in variants if v}


def crosscheck(
    documents: List[RegulatoryDocument],
    fetch_cfg: dict,
    sample_k: int = DEFAULT_SAMPLE_K,
    check_document_urls: bool = True,
) -> dict:
    """Return a structured report; report['pass'] is the acceptance flag."""
    if not documents:
        return {"pass": False, "reason": "no documents produced", "samples": [], "field_hit_rate": 0.0}

    sample = random.sample(documents, min(sample_k, len(documents)))
    fetcher = Fetcher(fetch_cfg)

    total_assertions = 0
    passed_assertions = 0
    url_checks = []
    sample_reports = []

    try:
        for doc in sample:
            entry = {"title": doc.title, "document_url": doc.document_url,
                     "source_page_url": doc.source_page_url, "field_results": {}}

            # 1. Field-presence checks against the source page's text.
            page_text = ""
            if doc.source_page_url:
                soup = fetcher.get(doc.source_page_url)
                if soup is not None:
                    page_text = _normalize(soup.get_text(" "))

            for field in ("title", "reference_no", "published_date"):
                val = getattr(doc, field, None)
                if not val:
                    continue
                total_assertions += 1
                if field == "published_date":
                    hit = any(v in page_text for v in _date_variants(val)) if page_text else False
                else:
                    hit = _normalize(val) in page_text if page_text else False
                entry["field_results"][field] = hit
                if hit:
                    passed_assertions += 1

            # 2. Document URL resolves and content-type roughly matches file_type.
            if check_document_urls and doc.document_url:
                ok, detail = _check_url(doc.document_url, doc.file_type)
                url_checks.append(ok)
                entry["document_url_ok"] = ok
                entry["document_url_detail"] = detail

            sample_reports.append(entry)
    finally:
        fetcher.close()

    field_hit_rate = (passed_assertions / total_assertions) if total_assertions else 0.0
    all_urls_ok = all(url_checks) if url_checks else True

    passed = field_hit_rate >= PASS_FIELD_HIT_RATE and all_urls_ok

    report = {
        "pass": passed,
        "field_hit_rate": round(field_hit_rate, 3),
        "field_assertions": total_assertions,
        "field_passed": passed_assertions,
        "document_urls_checked": len(url_checks),
        "document_urls_ok": sum(1 for x in url_checks if x),
        "all_document_urls_ok": all_urls_ok,
        "sample_size": len(sample),
        "samples": sample_reports,
    }
    if not passed:
        report["reason"] = _failure_reason(field_hit_rate, all_urls_ok, sample_reports)
    return report


def _check_url(url: str, file_type):
    headers = {"User-Agent": _UA}
    try:
        try:
            resp = requests.head(url, timeout=20, allow_redirects=True, headers=headers)
            if resp.status_code >= 400 or resp.status_code == 405:
                raise requests.RequestException("retry with GET")
        except requests.RequestException:
            # many servers reject/close HEAD; fall back to a streamed GET
            resp = requests.get(url, timeout=25, allow_redirects=True, stream=True, headers=headers)
        ok = resp.status_code < 400
        ctype = resp.headers.get("Content-Type", "")
        if ok and file_type:
            ft = file_type.lower()
            if ft == "pdf" and "pdf" not in ctype.lower() and not url.lower().endswith(".pdf"):
                return False, f"expected PDF, content-type={ctype}"
        return ok, f"status={resp.status_code}, content-type={ctype}"
    except requests.RequestException as e:
        return False, f"request error: {e}"


def _failure_reason(field_hit_rate, all_urls_ok, sample_reports) -> str:
    parts = []
    if field_hit_rate < PASS_FIELD_HIT_RATE:
        missed = {}
        for s in sample_reports:
            for f, hit in s["field_results"].items():
                if not hit:
                    missed.setdefault(f, 0)
                    missed[f] += 1
        parts.append(
            f"field hit-rate {field_hit_rate:.0%} < {PASS_FIELD_HIT_RATE:.0%}; "
            f"fields that did NOT appear on their source pages: {missed} "
            f"(likely the wrong element is being extracted for these fields)"
        )
    if not all_urls_ok:
        bad = [s["document_url"] for s in sample_reports if s.get("document_url_ok") is False]
        parts.append(f"document_url(s) did not resolve: {bad[:5]}")
    return " | ".join(parts)
