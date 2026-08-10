"""Ask the server what version it holds of every document we already store.

The read needs exactly one input, a document url, and the library already holds
every url it has ever stored. So this is not one adapter per site: it is a single
sweep that walks the stored inventory for a source and re-reads two bytes per
document. Written per site it would have been the same code four times and still
left the two JavaScript-rendered regulators uncovered, because their pages expose
no document url to anything but a browser — those urls can only come from us.

It answers "did a document we already store change?". It cannot answer "are there
documents we have never seen?" — that half still needs the crawl.
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlsplit

import requests
import yaml

from dynamic_crawler import fingerprint
from dynamic_crawler.changesignal import ChangeSignal, Observation, observation_for

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = Path("config") / "change_signals.yml"

#: A confirm hash of part of a document is a hash that lies — an edit at the end
#: of a PDF would not move it. Past this size no hash is returned and the document
#: is reported `unknown` instead.
MAX_CONFIRM_BYTES = 25 * 1024 * 1024


# --------------------------------------------------------------------------- #
#  configuration                                                               #
# --------------------------------------------------------------------------- #

def load_config(path=None) -> dict:
    path = Path(path) if path else DEFAULT_CONFIG
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def settings_for(config: dict, regulator: str, source_system: str) -> dict:
    """The settings for one source, over the defaults.

    Matched on BOTH halves: `source_system` is not unique across regulators, so
    matching on it alone would hand one regulator another's confirm tier.
    """
    out = dict(config.get("defaults") or {})
    for entry in config.get("sources") or []:
        if (str(entry.get("regulator") or "") == regulator
                and str(entry.get("source_system") or "") == source_system):
            out.update({k: v for k, v in entry.items()
                        if k not in ("regulator", "source_system")})
            break
    return out


def skip_hosts(config: dict) -> List[str]:
    """Hosts no sweep may probe.

    `until` is when the entry may be reviewed, not when it expires: a block that
    lifted itself on a date would send the next run at a host we are banned from
    with nobody having decided that.
    """
    out = []
    for entry in config.get("skip_hosts") or []:
        host = str((entry or {}).get("host") or "").strip().lower()
        if host:
            out.append(host)
    return out


# --------------------------------------------------------------------------- #
#  the sweep                                                                   #
# --------------------------------------------------------------------------- #

class StoredInventorySweep(ChangeSignal):
    # It reads only urls we already store, so an absence is not something it can
    # observe. This stays False whatever the source looks like.
    covers_inventory = False

    def __init__(self, repo, regulator: str, source_system: str, *,
                 identity=("document_url", "doc_path"),
                 confirm_required: bool = False,
                 workers: Optional[int] = None,
                 timeout: float = 20.0,
                 limit: Optional[int] = None,
                 skip_hosts=()):
        self.repo = repo
        self.regulator = regulator
        self.source_system = source_system
        self.identity = tuple(identity)
        self.confirm_required = bool(confirm_required)
        self.workers = workers
        self.timeout = float(timeout)
        self.limit = limit
        self.skip_hosts = {str(h).lower().lstrip(".") for h in (skip_hosts or ())}
        self.stats: dict = {}

    @property
    def name(self) -> str:
        return f"stored-inventory:{self.regulator}/{self.source_system}"

    def _blocked(self, url: str) -> Optional[str]:
        host = (urlsplit(url).hostname or "").lower()
        for h in self.skip_hosts:
            bare = h[4:] if h.startswith("www.") else h
            if host == h or host == bare or host.endswith("." + bare):
                return h
        return None

    def sweep(self) -> List[Observation]:
        rows = self.repo.find_regulations_by_source(self.source_system,
                                                    regulator=self.regulator)
        if self.limit:
            rows = rows[:self.limit]

        probe, skipped, no_url = [], {}, 0
        for row in rows:
            url = str(row.get("document_url") or "")
            blocked = self._blocked(url) if url else None
            if blocked:
                # Dropped rather than probed, and counted rather than forgotten:
                # the reason we may not go here is in the config, with a date.
                skipped[blocked] = skipped.get(blocked, 0) + 1
                continue
            if not url.startswith("http"):
                # Kept: it is still a document, and one we cannot detect an edit
                # on. The count is what makes that visible.
                no_url += 1
            probe.append((row, url))

        tokens = fingerprint.tokens_for([u for _r, u in probe if u.startswith("http")],
                                        workers=self.workers, timeout=self.timeout)
        observations = []
        for row, url in probe:
            token, basis = tokens.get(url, ("", fingerprint.BASIS_NONE))
            observations.append(observation_for(row, self.identity,
                                                token=token, basis=basis))

        self.stats = {"stored_rows": len(rows),
                      "no_probeable_url": no_url,
                      "skipped_hosts": skipped,
                      **fingerprint.summarise(tokens)}
        if skipped:
            logger.warning("skipped %s — blocked in config/change_signals.yml",
                           ", ".join(f"{n} row(s) on {h}"
                                     for h, n in skipped.items()))
        return observations

    def confirm(self, obs: Observation) -> Optional[str]:
        """The document's own bytes, for a source whose counters move in bulk."""
        if not self.confirm_required or not obs.url.startswith("http"):
            return None
        r = requests.get(obs.url, timeout=self.timeout * 3, stream=True,
                         headers={"User-Agent": fingerprint.USER_AGENT})
        r.raise_for_status()
        digest, read = hashlib.sha256(), 0
        for chunk in r.iter_content(65536):
            read += len(chunk)
            if read > MAX_CONFIRM_BYTES:
                raise ValueError(f"{obs.url[:80]} is over {MAX_CONFIRM_BYTES} "
                                 f"bytes — refusing to confirm on a partial read")
            digest.update(chunk)
        return digest.hexdigest()


# --------------------------------------------------------------------------- #
#  a workbook as the inventory                                                 #
# --------------------------------------------------------------------------- #

class WorkbookInventory:
    """The `regulations` sheet of a formfill workbook, as much of a repo as a
    sweep needs.

    It is what lets this run at all while there is no route to the database: the
    workbook holds the same rows under the same column names.
    """

    def __init__(self, path):
        # The workbook reader already exists, with its own quirks about how a
        # doc_path was flattened. One reader, not two.
        from dynamic_crawler.formfill.promote import _as_list, _read
        import json

        self.path = Path(path)
        self.rows = []
        for row in _read(self.path).get("regulations") or []:
            row = dict(row)
            row["doc_path"] = _as_list(row.get("doc_path"))
            raw = row.get("extra_meta")
            if isinstance(raw, str) and raw.strip().startswith("{"):
                try:
                    row["extra_meta"] = json.loads(raw)
                except Exception:
                    row["extra_meta"] = {}
            elif not isinstance(raw, dict):
                row["extra_meta"] = {}
            self.rows.append(row)

    def find_regulations_by_source(self, source_system: str,
                                   regulator: Optional[str] = None) -> list:
        return [dict(r) for r in self.rows
                if str(r.get("source_system") or "") == source_system
                and (not regulator or str(r.get("regulator") or "") == regulator)
                and str(r.get("status") or "") != "withdrawn"]


__all__ = ["StoredInventorySweep", "WorkbookInventory", "MAX_CONFIRM_BYTES",
           "DEFAULT_CONFIG", "load_config", "settings_for", "skip_hosts"]
