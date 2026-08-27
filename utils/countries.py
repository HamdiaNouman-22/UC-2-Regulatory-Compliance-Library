"""The country a regulator files under, for the folder tree only.

READ config/countries.yml FIRST — it carries the reasoning, and the one rule
that matters: the country is prepended when BUILDING THE TREE and is never put
into `doc_path`, because `doc_path` is an identity field and changing it would
reclassify the entire library in a single run.

Three call sites build folders and all three come here, for the same reason
`changesignal.find_existing` is the single implementation of "is this the same
document?": two copies of a rule drift, and the drift is invisible until the
tree has duplicates in it.

    orchestrator/orchestrator.py     the direct-write crawl path
    dynamic_crawler/formfill/orch.py the formfill/monitoring path
    dynamic_crawler/formfill/promote.py  replaying a workbook
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG = REPO_ROOT / "config" / "countries.yml"

_CACHE: Optional[Dict[str, str]] = None
_WARNED: set = set()


def _load() -> Dict[str, str]:
    """{regulator name -> country}, inverted from the country->regulators file.

    Cached: this is read once per document otherwise, and the file does not
    change while a run is in flight.
    """
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    mapping: Dict[str, str] = {}
    try:
        raw = yaml.safe_load(CONFIG.read_text(encoding="utf-8")) or {}
    except FileNotFoundError:
        logger.warning("config/countries.yml missing — every regulator will "
                       "stay at the tree root")
        _CACHE = {}
        return _CACHE
    for country, regulators in (raw.get("countries") or {}).items():
        for reg in (regulators or []):
            name = str(reg).strip()
            if not name:
                continue
            if name in mapping and mapping[name] != country:
                # Two countries claiming one regulator is a config error, not a
                # thing to resolve silently — the folder would land in whichever
                # happened to be parsed last.
                raise ValueError(
                    f"config/countries.yml lists {name!r} under both "
                    f"{mapping[name]!r} and {country!r}")
            mapping[name] = str(country).strip()
    _CACHE = mapping
    return _CACHE


def country_for(regulator: str) -> Optional[str]:
    """The country this regulator files under, or None if it is not listed.

    None means "leave it where it is". A regulator missing from the config keeps
    its old place at the tree root rather than being guessed at — a wrong
    country is harder to notice than a missing one, because the folder still
    exists and still holds the documents.
    """
    name = str(regulator or "").strip()
    hit = _load().get(name)
    if not hit and name and name not in _WARNED:
        # Once per regulator per process. Silence here is what lets a regulator
        # sit at the tree root for months: the documents are all fine, so nothing
        # else complains. `tools/workbook check` says the same thing louder, at
        # the moment a person is actually looking.
        _WARNED.add(name)
        logger.warning("%r is not in config/countries.yml — its folders will "
                       "stay at the tree root. Add it under a country, matching "
                       "this name exactly.", name)
    return hit


def tree_path(doc_path: List[str], regulator: str = "") -> List[str]:
    """`doc_path` with its country prepended — the hierarchy for the TREE.

    Pass the result to `_get_or_create_compliance_category`. Do NOT assign it
    back to `doc.doc_path`: that column is an identity field and must keep
    starting at the regulator.

    The regulator is taken from `doc_path[0]` when not given, which is what
    every crawler already puts there (`generic_crawler_wrapper` documents that
    doc_path "ALWAYS starts with the regulator").
    """
    path = [p for p in (doc_path or []) if str(p).strip()]
    if not path:
        return path
    country = country_for(regulator or path[0])
    if not country:
        return path
    if path[0] == country:            # already prefixed; do not double it
        return path
    return [country] + path


def countries() -> List[str]:
    """Every country named in the config, for the migration's own assertions."""
    return sorted(set(_load().values()))


def regulators() -> Dict[str, str]:
    """The full {regulator -> country} mapping."""
    return dict(_load())


__all__ = ["country_for", "tree_path", "countries", "regulators", "CONFIG"]
