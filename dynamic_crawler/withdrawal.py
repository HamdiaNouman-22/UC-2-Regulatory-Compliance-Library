"""When an absent document may be proposed for withdrawal, and when it may not.

A sweep counts how many times in a row it did not see a document. Turning that
count into "withdrawn" is a separate decision, and it is the only one in this
tier that can empty a compliance library — so it refuses by default and names
the condition that was not met.

Nothing here writes to a regulations row. The product is a proposal for a person.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from dynamic_crawler import changesignal as cs

logger = logging.getLogger(__name__)

#: Absent from this many consecutive sweeps before anything is proposed.
MIN_MISSES = 2
#: ...and the streak must SPAN this long. Two sweeps a second apart are two
#: sweeps; a regulator halfway through republishing its site is not a withdrawal.
MIN_SPAN_HOURS = 20.0
#: The tolerance the crawl's completeness gate uses, for the same reason: SDAIA
#: swung by 70 documents between runs of identical code.
COUNT_TOLERANCE_PCT = 5.0

PROPOSED = "withdrawal-proposed"
WATCHING = "watching"
NOT_JUDGED = "not-judged"


def _at(stamp) -> Optional[datetime]:
    try:
        return datetime.strptime(str(stamp), "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc)
    except Exception:
        return None


def span_hours(record: dict) -> Optional[float]:
    """How long this streak has run, or None when it was never stamped."""
    first, last = _at((record or {}).get("first_missed")), \
        _at((record or {}).get("last_missed"))
    return None if not (first and last) else max(
        0.0, (last - first).total_seconds() / 3600.0)


def count_drop(observed: int, prior: Optional[int]) -> Optional[str]:
    """The sweep-side completeness gate: a run that LOST documents is not a run
    in which documents were withdrawn.

    The allowance is one document OR the percentage, whichever is larger. A flat
    percentage is only meaningful on a source of some size: GOSI observes 12
    things and SIMAH 17, where one document is 8.3% and 5.9%, so a flat 5% blocks
    every real single withdrawal on them and this layer never proposes anything.
    """
    if not prior or observed >= prior:
        return None
    lost = prior - observed
    allowed = max(1, int(prior * COUNT_TOLERANCE_PCT / 100.0))
    if lost <= allowed:
        return None
    return (f"observed {observed} where the last sweep observed {prior} — {lost} "
            f"fewer, over the {allowed} a source this size allows "
            f"({COUNT_TOLERANCE_PCT}% or one document, whichever is larger)")


def gate(signal, report: dict) -> tuple:
    """(may_propose, [reasons]) for one sweep, before any single document."""
    reasons = []
    observed = int(report.get("observed") or 0)
    if not getattr(signal, "covers_inventory", False):
        reasons.append("this signal reads only documents already stored, so an "
                       "absence is not something it can observe")
    if report.get("collisions"):
        reasons.append(f"{len(report['collisions'])} identity collision(s): which "
                       f"document is which is in doubt for this source")
    if not observed:
        reasons.append("the sweep observed nothing")
    drop = count_drop(observed, report.get("observed_last"))
    if drop:
        reasons.append(drop)
    return (not reasons), reasons


def decide(record: dict, signal_name: str, may_propose: bool, blocked_by,
           *, min_misses: int = MIN_MISSES,
           min_span_hours: float = MIN_SPAN_HOURS) -> tuple:
    """(verdict, why) for one absent identity.

    Attribution comes first: a signal may not judge an absence it was never in a
    position to observe. Two signals can share one state file, because the store
    is per source and not per signal.
    """
    record = record or {}
    owner = str(record.get("signal") or "")
    misses = int(record.get("misses") or 0)

    if not owner:
        return NOT_JUDGED, ("no sweep recorded which signal last saw this "
                            "document, so its absence cannot be attributed — the "
                            "next sweep that sees it stamps it")
    if owner != signal_name:
        return NOT_JUDGED, (f"recorded by {owner}, not {signal_name}: a signal "
                            f"may not judge another's absences")
    if not may_propose:
        return WATCHING, "; ".join(blocked_by)
    if misses < min_misses:
        return WATCHING, f"absent from {misses} sweep(s), {min_misses} required"
    span = span_hours(record)
    if span is None:
        return WATCHING, f"absent from {misses} sweep(s) over no measured span"
    if span < min_span_hours:
        return WATCHING, (f"absent from {misses} sweep(s) but only over "
                          f"{span:.1f}h, {min_span_hours:.0f}h required")
    return PROPOSED, f"absent from {misses} consecutive sweeps over {span:.1f}h"


def proposals(signal, store, report: dict, buckets: dict) -> dict:
    """What a person is being asked to confirm, and what was refused instead."""
    may, blocked = gate(signal, report)
    name = str(report.get("signal") or getattr(signal, "name", "") or "")
    out = {"rule": (f"absent from {MIN_MISSES} consecutive sweeps spanning "
                    f"{MIN_SPAN_HOURS:.0f}h, then a person confirms"),
           "may_propose": may, "blocked_by": blocked,
           PROPOSED: [], WATCHING: [], NOT_JUDGED: []}

    for key, _reason in buckets.get(cs.MISSING) or []:
        record = store.get(key) or {}
        verdict, why = decide(record, name, may, blocked)
        out[verdict].append({
            "key": key,
            "title": str(record.get("title") or "")[:70],
            "url": record.get("url", ""),
            "misses": int(record.get("misses") or 0),
            "first_seen": record.get("first_seen", ""),
            "last_seen": record.get("last_seen", ""),
            "first_missed": record.get("first_missed", ""),
            "why": why})

    out["counts"] = {k: len(out[k]) for k in (PROPOSED, WATCHING, NOT_JUDGED)}
    out["confirmed"] = False
    out["next_step"] = (
        "nothing is withdrawn by this report. Open each proposed document at its "
        "stored url first: a url that 404s is a library problem, not a withdrawal. "
        "The status write needs a senior developer's approval.")
    if out[PROPOSED]:
        logger.warning("%s: %d document(s) meet the withdrawal rule and are "
                       "waiting on a person", name, len(out[PROPOSED]))
    return out


__all__ = ["proposals", "gate", "decide", "span_hours", "count_drop",
           "PROPOSED", "WATCHING", "NOT_JUDGED", "MIN_MISSES", "MIN_SPAN_HOURS",
           "COUNT_TOLERANCE_PCT"]
