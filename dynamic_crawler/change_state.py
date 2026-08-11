"""What a change sweep remembers between runs.

One JSON file per source, keyed on the identity the SOURCE declared for each
document. It is deliberately not a table in the library, for three reasons in
order of weight: a sweep has to be runnable with no route to the database; it
records two things no column holds — how many sweeps in a row a document has
been absent, and why a probe produced no token; and a sweep that wrote to
`regulations` would be a write against production.

Nothing here decides anything. It counts, and the decision is made elsewhere.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

DEFAULT_ROOT = Path("output") / "change_state"
FORMAT = 1
#: How many sweeps' observed counts to keep. The withdrawal gate needs the
#: previous one; the rest are there to read when a gate refusal is disputed.
RUNS_KEPT = 10


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def slug(source: str) -> str:
    return (re.sub(r"[^A-Za-z0-9]+", "_", source or "").strip("_").lower()
            or "unknown")[:80]


class ChangeStateStore:
    def __init__(self, path, source: str = ""):
        self.path = Path(path)
        self.source = source
        self.records: Dict[str, dict] = {}
        self.runs: list = []

    @classmethod
    def for_source(cls, source: str, root=None) -> "ChangeStateStore":
        root = Path(root) if root else DEFAULT_ROOT
        return cls(root / f"{slug(source)}.json", source=source).load()

    def load(self) -> "ChangeStateStore":
        if not self.path.exists():
            return self
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception as e:
            # An unreadable state file must not be read as an empty one: every
            # document would come back `new` and every miss streak would reset.
            raise ValueError(f"{self.path} is not readable change state ({e}) — "
                             f"move it aside deliberately to start over")
        self.records = dict(data.get("records") or {})
        self.runs = list(data.get("runs") or [])
        self.source = self.source or str(data.get("source") or "")
        return self

    def keys(self) -> set:
        return set(self.records)

    def get(self, key: str) -> Optional[dict]:
        record = self.records.get(key)
        return dict(record) if record else None

    def collision(self, key: str, fields: dict) -> Optional[str]:
        """A key that landed on a record holding different field values.

        Identity keys are `field=value` pairs joined by `|`, so a value carrying
        a separator could in principle address another document's history.
        Storing the fields makes that visible instead of merging the two.
        """
        record = self.records.get(key)
        if record and record.get("fields") and dict(record["fields"]) != dict(fields):
            return (f"identity key {key[:120]!r} already holds "
                    f"{record['fields']}, not {fields}")
        return None

    def record(self, obs, verdict: str, signal: str = "") -> None:
        """Store what this sweep saw of one document."""
        prior = self.records.get(obs.key) or {}
        self.records[obs.key] = {
            "fields": dict(obs.fields),
            "identity_fields": list(obs.identity_fields),
            # Which signal saw it. Two signals can share one state file, because
            # this store is per source, and neither may judge the other's
            # absences.
            "signal": signal or prior.get("signal", ""),
            "title": obs.title or prior.get("title", ""),
            "url": obs.url or prior.get("url", ""),
            # A probe that failed must not erase the token it failed to re-read:
            # the stored one is still the last thing the server actually said.
            "token": obs.token or prior.get("token", ""),
            "basis": obs.basis,
            "confirm_hash": obs.confirm_hash or prior.get("confirm_hash", ""),
            "first_seen": prior.get("first_seen") or _now(),
            "last_seen": _now(),
            "last_verdict": verdict,
            # Seen but unreadable is not absent, so any verdict clears the streak.
            "misses": 0,
        }

    def missed(self, key: str) -> int:
        """One more sweep in which this identity was not seen; the streak.

        Both ends are stamped: a rule that counted sweeps alone would be
        satisfied by running the CLI twice in one second. A withdrawal needs two
        consecutive trustworthy runs and then a person, and neither of those
        lives here — this only counts.
        """
        record = self.records.setdefault(key, {"first_seen": _now(), "misses": 0})
        record["misses"] = int(record.get("misses") or 0) + 1
        record.setdefault("first_missed", _now())
        record["last_missed"] = _now()
        return record["misses"]

    def note_run(self, signal: str, observed: int) -> None:
        """This sweep's observed count, for the withdrawal gate.

        The crawl's gate compares against run_history; a sweep has to work with
        no route to the database, so its baseline lives in its own state file.
        """
        self.runs = (self.runs or [])[-(RUNS_KEPT - 1):] + [
            {"at": _now(), "signal": str(signal), "observed": int(observed)}]

    def last_observed(self, signal: str = "") -> Optional[int]:
        """What the previous sweep saw, or None when there was not one."""
        for run in reversed(self.runs or []):
            if not signal or run.get("signal") == signal:
                return int(run.get("observed") or 0)
        return None

    def save(self) -> Path:
        """Written to a temporary file and moved into place: a half-written
        state file would load as an empty one."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({"format": FORMAT, "source": self.source,
                                   "updated_at": _now(), "runs": self.runs,
                                   "records": self.records},
                                  ensure_ascii=False, indent=1, default=str),
                       encoding="utf-8")
        os.replace(tmp, self.path)
        return self.path


__all__ = ["ChangeStateStore", "slug", "DEFAULT_ROOT"]
