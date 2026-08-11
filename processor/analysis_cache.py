"""
Content-hash cache for staged analysis.

Why this exists
---------------
Two runs of the same document do not produce the same analysis. Measured on
regulation 103296: three runs of the identical text gave 27, 38 and 44
obligations. Temperature 0, a fixed seed and a pinned provider do not fix it --
see docs/determinism.md. The only way to make a stored analysis stable is to
produce it once and reuse it.

The existing guard in `/trigger/staged-analysis` is text-blind, and wrong in
both directions:

  * no force + analysis exists  -> skipped even when the document HAS changed,
                                   so a stale analysis is kept indefinitely
  * force=true                  -> re-runs even when the text is byte-identical,
                                   producing a different answer for no reason

Hashing the normalized text fixes both: skip when the input is unchanged,
re-analyse automatically when it is not.

Storage
-------
`regulations.extra_meta` JSON, so there is no schema migration:

    extra_meta["analysis_input_hash"]  sha256 of the normalized text
    extra_meta["analysis_model"]       model that produced it
    extra_meta["analysis_hashed_at"]   ISO timestamp

The hash deliberately covers the *normalized* text -- what actually reaches the
LLM -- not the raw HTML/PDF. Cosmetic source changes that normalize away should
not trigger a re-analysis.

Known limitation
----------------
The key does not include a prompt version, so editing a prompt will NOT
invalidate anything. Until that is added, use ?force=true after a prompt change.
See docs/analyzer_code_review.md §C2.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

HASH_KEY = "analysis_input_hash"
MODEL_KEY = "analysis_model"
STAMP_KEY = "analysis_hashed_at"


def compute_input_hash(clean_text: str, model: str) -> str:
    """Fingerprint of exactly what the pipeline will send to the LLM."""
    h = hashlib.sha256()
    h.update((model or "").encode("utf-8"))
    h.update(b"\x00")
    h.update((clean_text or "").encode("utf-8"))
    return h.hexdigest()


def as_dict(extra_meta: Any) -> Dict:
    """extra_meta arrives as a dict or a JSON string depending on the path."""
    if isinstance(extra_meta, dict):
        return dict(extra_meta)
    if isinstance(extra_meta, str) and extra_meta.strip():
        try:
            parsed = json.loads(extra_meta)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def stored_hash(extra_meta: Any) -> Optional[str]:
    return as_dict(extra_meta).get(HASH_KEY)


def decide(
    extra_meta: Any,
    clean_text: str,
    model: str,
    has_existing_rows: bool,
    force: bool,
) -> Tuple[bool, str, str]:
    """Should the analysis be re-run?

    Returns (should_run, input_hash, reason). `reason` is meant to be surfaced
    in the API response so the caller can tell a cache hit from a real run.
    """
    input_hash = compute_input_hash(clean_text, model)
    previous = stored_hash(extra_meta)

    if force:
        return True, input_hash, "force=true requested"
    if not has_existing_rows:
        return True, input_hash, "no existing analysis"
    if not previous:
        # Analysed before this cache existed. Trust the stored analysis and
        # record the hash now, rather than re-running everything once.
        return False, input_hash, "existing analysis predates the cache; hash recorded"
    if previous != input_hash:
        return True, input_hash, "document text changed since last analysis"
    return False, input_hash, "input unchanged; reused stored analysis"


def record(repo, regulation_id: int, extra_meta: Any, input_hash: str, model: str) -> None:
    """Persist the hash on the regulation. Never raises -- a cache write
    failing must not fail an otherwise successful analysis."""
    try:
        meta = as_dict(extra_meta)
        meta[HASH_KEY] = input_hash
        meta[MODEL_KEY] = model
        meta[STAMP_KEY] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        repo.update_regulation(regulation_id, extra_meta=json.dumps(meta, ensure_ascii=False))
        logger.info(f"Analysis hash recorded for regulation {regulation_id}: {input_hash[:12]}")
    except Exception as e:
        logger.error(f"Could not record analysis hash for regulation {regulation_id}: {e}")
