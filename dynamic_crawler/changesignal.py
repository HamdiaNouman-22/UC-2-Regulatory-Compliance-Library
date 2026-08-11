"""One shape for "did this document change?", for every regulator.

Every signal measured across the ten regulators is the same two steps with a
different field in the middle: read something the server maintains cheaply, then
shortlist what moved. This module is the part that does not vary — an
observation, a verdict, and what a run remembers. The adapters that produce
observations are separate.

It sits outside the formfill engine because for most regulators the sweep is an
HTTP client and a parse: no browser, no form, no shape.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import List, Optional

from dynamic_crawler import fingerprint

logger = logging.getLogger(__name__)

NEW = "new"
MODIFIED = "modified"
UNCHANGED = "unchanged"
UNKNOWN = "unknown"          # seen, but the signal could not be read
MISSING = "missing"          # in the store, not in a sweep that covers everything


# --------------------------------------------------------------------------- #
#  identity — read off the document, never off the regulator                   #
# --------------------------------------------------------------------------- #

def clean_fields(fields) -> tuple:
    """One string, a list or a tuple, all to a tuple of field names."""
    if isinstance(fields, str):
        fields = [fields]
    return tuple(str(f).strip() for f in (fields or ()) if str(f).strip())


def _meta_of(obj) -> dict:
    """extra_meta, whether it arrives as a dict or as the JSON text a column
    holds."""
    meta = (obj.get("extra_meta") if isinstance(obj, dict)
            else getattr(obj, "extra_meta", None))
    if isinstance(meta, str) and meta.strip():
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    return meta if isinstance(meta, dict) else {}


def declared_fields(obj, default=()) -> tuple:
    """The identity fields for THIS document or row.

    A regulator's config is a LIST of sources, so the choice belongs to the
    source that produced the document — a grid keyed on a circular number and a
    link walk with no number cannot share one. The crawler stamps the source's
    own choice on each document; the regulator-wide value is only the fallback.
    """
    return clean_fields(_meta_of(obj).get("identity_fields")) or clean_fields(default)


def fields_of(obj, fields) -> dict:
    """The identity of one document or stored row, as {field: value}."""
    out = {}
    for name in fields:
        value = (obj.get(name) if isinstance(obj, dict)
                 else getattr(obj, name, None))
        if isinstance(value, (list, tuple)):
            value = " > ".join(str(v) for v in value)
        out[name] = str(value).strip() if value is not None else ""
    return out


def identity_key(fields: dict) -> str:
    """`field=value|field=value`, in the order the source declared the fields.

    `field=` and not the values alone: one run can carry two sources whose
    identities are different fields entirely.
    """
    return "|".join(f"{k}={v}" for k, v in fields.items())


# --------------------------------------------------------------------------- #
#  what a sweep produces                                                       #
# --------------------------------------------------------------------------- #

@dataclass
class Observation:
    """One document as this sweep saw it."""
    key: str
    fields: dict = field(default_factory=dict)
    identity_fields: tuple = ()
    token: str = ""
    basis: str = fingerprint.BASIS_NONE
    url: str = ""
    confirm_hash: str = ""
    title: str = ""


def observation_for(obj, default_fields=(), token=None, basis=None) -> Observation:
    """An Observation for a crawled document or a stored regulation row.

    Both shapes go through the same function so a document keys identically
    whichever side it is read from — that is what makes a sweep over the stored
    inventory comparable with a sweep over a live listing.
    """
    names = declared_fields(obj, default_fields)
    values = fields_of(obj, names)
    meta = _meta_of(obj)
    get = obj.get if isinstance(obj, dict) else lambda k, d=None: getattr(obj, k, d)
    return Observation(
        key=identity_key(values), fields=values, identity_fields=names,
        token=str(meta.get("version_token") or "") if token is None else str(token),
        basis=(str(meta.get("hash_basis") or fingerprint.BASIS_NONE)
               if basis is None else basis),
        url=str(get("document_url", "") or ""),
        title=str(get("title", "") or ""))


# --------------------------------------------------------------------------- #
#  the verdict                                                                 #
# --------------------------------------------------------------------------- #

def _short(token: str, keep: int = 28) -> str:
    """Enough of a token to see what moved.

    The version sits at the END of `{GUID},<version>`, so a plain head
    truncation prints two identical strings and hides the only part that changed.
    """
    return token if len(token) <= keep else f"{token[:8]}...{token[-12:]}"


def verdict_for(obs: Observation, stored: Optional[dict],
                confirm_required: bool = False) -> tuple:
    """(verdict, reason). One observation against what the last sweep stored.

    `unknown` exists so that a probe which did not run is never reported as a
    document which did not change — those two are indistinguishable in the
    output otherwise, and the second is the one people act on.
    """
    if stored is None:
        return NEW, "not in the change store"

    old = str(stored.get("token") or "")
    new = str(obs.token or "")

    if obs.basis == fingerprint.BASIS_FAILED:
        return UNKNOWN, "the probe did not run"
    if old and not new:
        return UNKNOWN, f"a token was stored and this run read none ({obs.basis})"
    if not old:
        # First token for a document already known: a baseline, not a change.
        # Enabling a signal must not reclassify a whole library.
        return UNCHANGED, "no stored token to compare against — baseline recorded"
    if old == new:
        return UNCHANGED, "token unchanged"

    if not confirm_required:
        return MODIFIED, f"token moved {_short(old)} -> {_short(new)}"

    # This source re-uploads its library in bulk, so a moved counter is not by
    # itself evidence that anything was edited.
    if not obs.confirm_hash:
        return UNKNOWN, "token moved and no confirming content hash was read"
    old_hash = str(stored.get("confirm_hash") or "")
    if not old_hash:
        return MODIFIED, "token moved; no stored content hash to compare against"
    if obs.confirm_hash == old_hash:
        return UNCHANGED, "token moved, content did not — a bulk republish"
    return MODIFIED, "token and content both moved"


# --------------------------------------------------------------------------- #
#  the interface, and the run                                                  #
# --------------------------------------------------------------------------- #

class ChangeSignal:
    """Tier one is `sweep`, tier two is `confirm`.

    `covers_inventory` is the discover/detect split. A sweep of a live listing
    sees everything the regulator publishes, so it may report a document as
    absent. A sweep over our own stored urls only ever looks at what it already
    knew — absence is not something it can observe, and saying "0 missing" there
    would be a zero that means "not measured".

    `confirm_required` is per source, not a property of the platform: one
    regulator's counters carry real per-document history, another's move all at
    once on a bulk re-upload.
    """

    name = "change-signal"
    confirm_required = False
    covers_inventory = False

    def sweep(self) -> List[Observation]:
        raise NotImplementedError

    def confirm_required_for(self, obs: Observation) -> bool:
        """Per observation, because one source can carry two kinds of token.

        GOSI's page date is shared by every instrument on the page, so only a
        content hash separates an amendment from a republish; the version counter
        on the documents that page links to is per file and is proof on its own.
        """
        return self.confirm_required

    def confirm(self, obs: Observation) -> Optional[str]:
        """A content hash for one shortlisted observation, or None when the
        token is proof on its own."""
        return None


def _confirm(signal: ChangeSignal, obs: Observation) -> str:
    try:
        return str(signal.confirm(obs) or "")
    except Exception as e:                  # an unreadable confirm is `unknown`
        logger.warning("confirm failed for %s: %s", obs.key[:80], e)
        return ""


def run_sweep(signal: ChangeSignal, store, record: bool = True) -> tuple:
    """Sweep, classify against the store, record, and report.

    Nothing here touches the library. The shortlist IS the product: what a
    caller does with `modified` — re-crawl, re-analyse — stays outside, and that
    is what keeps a sweep runnable with no database.
    """
    observations = list(signal.sweep() or [])
    buckets = {NEW: [], MODIFIED: [], UNCHANGED: [], UNKNOWN: [], MISSING: []}
    by_basis, collisions, seen, confirmed = {}, [], set(), 0

    for obs in observations:
        by_basis[obs.basis] = by_basis.get(obs.basis, 0) + 1
        clash = store.collision(obs.key, obs.fields)
        if clash:
            collisions.append(clash)      # never merge two documents' history
            continue

        seen.add(obs.key)
        stored = store.get(obs.key)
        verdict, reason = verdict_for(obs, stored)
        if verdict == MODIFIED and signal.confirm_required_for(obs):
            obs.confirm_hash = _confirm(signal, obs)      # the shortlist only
            confirmed += 1
            verdict, reason = verdict_for(obs, stored, confirm_required=True)

        buckets[verdict].append((obs, reason))
        if record:
            store.record(obs, verdict)

    absent = sorted(store.keys() - seen) if signal.covers_inventory else []
    for key in absent:
        streak = (store.missed(key) if record
                  else int((store.get(key) or {}).get("misses") or 0) + 1)
        buckets[MISSING].append((key, f"absent from {streak} consecutive sweep(s)"))

    report = {
        "signal": signal.name,
        "source": store.source,
        "observed": len(observations),
        "counts": {k: len(v) for k, v in buckets.items()},
        "by_basis": by_basis,
        "without_token": sum(1 for o in observations if not o.token),
        "confirm_required": signal.confirm_required,
        "confirmed": confirmed,
        "collisions": collisions,
        "state_file": str(store.path),
    }
    if not signal.covers_inventory:
        report["counts"].pop(MISSING)
        report["missing"] = ("not measured — this sweep reads only documents "
                             "already stored, so it cannot see an absence")
    return report, buckets


__all__ = ["NEW", "MODIFIED", "UNCHANGED", "UNKNOWN", "MISSING",
           "Observation", "ChangeSignal", "observation_for", "verdict_for",
           "run_sweep", "clean_fields", "declared_fields", "fields_of",
           "identity_key"]
