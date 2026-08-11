"""Whether a crawl may propose withdrawing the documents it did not see.

A sweep remembers, per document, how many runs in a row it went unseen. The
crawl path has `run_history` and nothing per document, which is the only reason
its `disappeared` bucket has always been reported and never judged. This holds
that memory and decides whose absences may be judged; `withdrawal.decide` then
applies the same rule the sweeps use, unchanged.

Nothing here reads the network or a database, and nothing writes a regulations
row. The product is a proposal for a person.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

from dynamic_crawler import changesignal as cs
from dynamic_crawler import withdrawal as wd
from dynamic_crawler.change_state import DEFAULT_ROOT, ChangeStateStore

logger = logging.getLogger(__name__)

#: A directory of the crawl's own, never a file a sweep also writes. `run_sweep`
#: counts an absence for every key in the file it opens and `missed()` never asks
#: who owns the record, so a daily sweep would build the crawl's miss streak —
#: and its 20-hour span — on the crawl's behalf.
CRAWL_ROOT = DEFAULT_ROOT / "crawl"
#: Stamped on every record. The file is already per source, so the name only has
#: to say which writer produced it.
SIGNAL = "crawl"
#: Problems that stop every source rather than one of them.
RUN_WIDE = "run"
SEEN = "in this run's listing"


def store_for(source: str, root=None) -> ChangeStateStore:
    return ChangeStateStore.for_source(source,
                                       root=Path(root) if root else CRAWL_ROOT)


def source_verdicts(problems, labels) -> Dict[str, List[str]]:
    """Which gate problems stop which source: label -> the reasons blocking it.

    A problem naming one source stops that source alone; a bot-protection page or
    a cap is run-wide and stops all of them. The run's own PASS/QUARANTINED
    verdict is untouched — this decides only whose absences may be judged.
    """
    own: Dict[str, List[str]] = {str(label): [] for label in (labels or [])}
    run_wide: List[str] = []
    for problem in problems or []:
        head = str(problem).split(":", 1)[0].strip()
        (own[head] if head in own else run_wide).append(str(problem))
    return {RUN_WIDE: run_wide,
            **{label: run_wide + rest for label, rest in own.items()}}


def source_system_labels(crawler) -> Dict[str, str]:
    """`source_system` -> the one source label that writes it.

    `source_systems` is a de-duplicated union and not a list parallel to
    `source_names`: one yaml source can be three crawlers. A source_system that
    two sources both write is left out, so the rows carrying it go unjudged
    rather than being charged to a guess.
    """
    owners: Dict[str, set] = {}
    names = list(getattr(crawler, "source_names", None) or [])
    for i, child in enumerate(getattr(crawler, "crawlers", None) or []):
        label = str(names[i]) if i < len(names) else ""
        for system in (getattr(child, "source_systems", None)
                       or [getattr(child, "source_system", None)]):
            if system and label:
                owners.setdefault(str(system), set()).add(label)
    return {system: sorted(labels)[0]
            for system, labels in owners.items() if len(labels) == 1}


def owner_of(row, labels, systems) -> Optional[str]:
    """The source label whose gate verdict governs one stored row.

    A single-source run — which is every formfill run — has one answer for every
    row. Only a composite has to be told apart, and it cannot always be.
    """
    labels = [str(label) for label in (labels or [])]
    if len(labels) <= 1:
        return labels[0] if labels else None
    return systems.get(str((row or {}).get("source_system") or "").strip())


def note_seen(store, docs, identity) -> int:
    """Clear the streak of every document this run's listing did hold.

    Positive evidence counts whatever the gate said — a document present in a
    blocked run is present. An absence is the other way round: only a source
    that passed the gate advances a streak.
    """
    written = 0
    for doc in docs or []:
        obs = cs.observation_for(doc, identity)
        if any(obs.fields.values()):
            store.record(obs, SEEN, SIGNAL)
            written += 1
    return written


def blocked_for(label, verdicts, *, observed, prior, targeted="",
                collisions=()) -> List[str]:
    """Why this source's absences may not be proposed. Empty means they may.

    The count question uses `withdrawal.count_drop`'s allowance of one document
    or 5%, not the gate's flat 5%: one document is 8.3% of GOSI's 12 and 5.9% of
    SIMAH's 17, so a flat percentage makes this layer inert on the sources it
    was built for. The gate's own tolerance is unchanged.
    """
    reasons = list(verdicts.get(label, verdicts.get(RUN_WIDE) or []))
    if targeted:
        reasons.append(str(targeted))
    if not observed:
        reasons.append("this source produced no documents in this run")
    if collisions:
        reasons.append(f"{len(collisions)} identity collision(s): which document "
                       f"is which is in doubt for this source")
    drop = wd.count_drop(int(observed or 0), prior)
    if drop:
        reasons.append(drop)
    return reasons


def _keyed(store, rows, identity, labels, systems) -> tuple:
    """(judgeable, unattributed, collisions per label) for the absent rows."""
    judgeable, unattributed, collisions = [], [], {}
    counted: Dict[str, str] = {}
    for row in rows or []:
        obs = cs.observation_for(row, identity)
        label = owner_of(row, labels, systems)
        if not any(obs.fields.values()):
            unattributed.append((obs, "", "the stored row carries no value for "
                                 f"its identity {list(obs.identity_fields)}"))
            continue
        if label is None:
            unattributed.append((obs, "", "which source this row belongs to "
                                 "cannot be told from source_system "
                                 f"{str(row.get('source_system') or '')!r}"))
            continue
        clash = store.collision(obs.key, obs.fields) or (
            f"identity key {obs.key[:120]!r} is carried by two absent rows"
            if obs.key in counted else "")
        if clash:
            collisions.setdefault(label, []).append(clash)
        counted[obs.key] = label
        judgeable.append((obs, label))
    return judgeable, unattributed, collisions


def judge(store, rows, *, identity, labels, counts, priors, problems,
          systems=None, targeted="", record: bool = True) -> dict:
    """The `withdrawals` block for one crawl: what a person is asked to confirm.

    Per source, because a broken sibling must not hold back a healthy source's
    decisions — and a row that cannot be charged to a source is not judged at
    all rather than charged to the run.
    """
    verdicts = source_verdicts(problems, labels)
    judgeable, unattributed, collisions = _keyed(
        store, rows, identity, labels, systems or {})

    blocked: Dict[str, List[str]] = {}
    for label in {label for _obs, label in judgeable}:
        blocked[label] = blocked_for(
            label, verdicts, observed=(counts or {}).get(label) or 0,
            prior=(priors or {}).get(label), targeted=targeted,
            collisions=collisions.get(label) or ())

    out = {"rule": (f"absent from {wd.MIN_MISSES} consecutive trustworthy runs "
                    f"spanning {wd.MIN_SPAN_HOURS:.0f}h, then a person confirms"),
           "signal": SIGNAL, "state_file": str(store.path),
           "by_source": {label: {"may_propose": not reasons,
                                 "blocked_by": reasons,
                                 "observed": (counts or {}).get(label) or 0,
                                 "observed_last": (priors or {}).get(label)}
                         for label, reasons in blocked.items()},
           wd.PROPOSED: [], wd.WATCHING: [], wd.NOT_JUDGED: []}

    for obs, label in judgeable:
        reasons = blocked.get(label) or []
        may = not reasons
        # Only a source that may judge advances a streak. A blocked run that
        # counted its absences would hand the next good run a streak it never
        # earned, which is the whole point of the 20-hour span.
        if may and record:
            store.missed(obs.key)
        verdict, why = wd.decide(store.get(obs.key) or {}, SIGNAL, may, reasons)
        out[verdict].append(_entry(obs, store, label, why))

    for obs, label, why in unattributed:
        out[wd.NOT_JUDGED].append(_entry(obs, store, label, why))

    out["counts"] = {k: len(out[k]) for k in
                     (wd.PROPOSED, wd.WATCHING, wd.NOT_JUDGED)}
    out["confirmed"] = False
    out["next_step"] = (
        "nothing is withdrawn by this report. Open each proposed document at its "
        "stored url first: a url that 404s is a library problem, not a withdrawal. "
        "The status write needs a senior developer's approval.")
    if out[wd.PROPOSED]:
        logger.warning("%s: %d document(s) meet the withdrawal rule and are "
                       "waiting on a person", store.source, len(out[wd.PROPOSED]))
    return out


def _entry(obs, store, label: str, why: str) -> dict:
    record = store.get(obs.key) or {}
    return {"key": obs.key, "source": label,
            "title": str(obs.title or record.get("title") or "")[:70],
            "url": obs.url or record.get("url", ""),
            "misses": int(record.get("misses") or 0),
            "first_seen": record.get("first_seen", ""),
            "last_seen": record.get("last_seen", ""),
            "first_missed": record.get("first_missed", ""),
            "why": why}


__all__ = ["judge", "note_seen", "store_for", "source_verdicts",
           "source_system_labels", "owner_of", "blocked_for",
           "CRAWL_ROOT", "SIGNAL", "RUN_WIDE"]
