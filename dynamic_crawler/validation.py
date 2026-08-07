"""Accuracy checks run on every crawl: required-field completeness and a
doc-count anomaly check against the config's expected range. This never raises
or silently drops data -- it produces a report so a broken crawl (e.g. a
selector that stopped matching) is flagged instead of quietly written as if
it were a legitimate "nothing new today" result.
"""

import logging
from dataclasses import asdict
from typing import List

from models.models import RegulatoryDocument

logger = logging.getLogger(__name__)


def validate_documents(documents: List[RegulatoryDocument], cfg: dict) -> dict:
    validation_cfg = cfg["validation"]
    required_fields = validation_cfg["required_fields"]
    min_count = validation_cfg["expected_doc_count_min"]
    max_count = validation_cfg["expected_doc_count_max"]

    field_issues = []
    for i, doc in enumerate(documents):
        doc_dict = asdict(doc)
        missing = [f for f in required_fields if not doc_dict.get(f)]
        if missing:
            field_issues.append({
                "index": i,
                "document_url": doc.document_url,
                "missing_fields": missing,
            })

    count = len(documents)
    count_in_range = min_count <= count <= max_count

    report = {
        "document_count": count,
        "expected_min": min_count,
        "expected_max": max_count,
        "count_in_expected_range": count_in_range,
        "documents_with_missing_required_fields": field_issues,
        "ok": count_in_range and not field_issues,
    }

    if not report["ok"]:
        logger.warning(f"Validation flagged this crawl: doc_count={count} (expected {min_count}-{max_count}), "
                        f"{len(field_issues)} document(s) missing required fields")
    else:
        logger.info(f"Validation passed: {count} documents, all required fields present")

    return report
