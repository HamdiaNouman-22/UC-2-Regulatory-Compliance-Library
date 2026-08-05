"""SimahCrawler — `mode: custom` for a site behind a Cloudflare firewall rule.

WHAT MAKES THIS CUSTOM IS THE FETCH POLICY, NOT THE EXTRACTION.

SIMAH's page is not hard to read: `dynamic_crawler/hints/simah.rules.yml` already
describes it in twelve fields (one law as 17 collapsible articles, plus its
Implementing Regulations as an attached PDF). What no form can express is *how
often we are allowed to ask*. So this class owns the policy and delegates
everything else:

    extraction   the form, unchanged             (no selectors live in this file)
    mapping      FormfillCrawler -> RegulatoryDocument (no mapping lives here)
    policy       this file

THE POLICY, AND WHY EACH RULE EXISTS

  1. Serve the snapshot when it is fresh. A full form run is two loads of one URL,
     so the traffic was never the problem — ITERATION was. Replay costs nothing.
  2. At most ONE live navigation per run, and only when the snapshot is due for
     refresh and the backoff clock allows it (SnapshotStore.may_attempt).
  3. NEVER retry a block. Retrying is what turns a temporary rule into a lasting
     one. A blocked attempt records itself and pushes the next one further out:
     6h, 24h, 72h, 7d, 14d.
  4. A blocked refresh falls back to the last good snapshot rather than failing the
     regulator — but only while that snapshot is inside its grace period.
  5. Past the grace period with every refresh blocked, RAISE. A snapshot replayed
     forever would tell change detection "unchanged" while the law was amended,
     which is a silent false negative in the one system whose job is noticing
     change. Better a loud failure than a confident lie.

Every document carries `extra_meta["source"] = "snapshot" | "live"` and the capture
date, so nothing downstream can mistake a replay for a crawl.

    - name: "Rules and Regulations"
      mode: custom
      crawler_class: "crawler.simah_wrapper.SimahCrawler"
      init_kwargs:
        regulator: "SIMAH"
        source_system: "SIMAH-RULES"
"""

from __future__ import annotations

import json
import logging
import tempfile
from pathlib import Path
from typing import Optional

from dynamic_crawler.formfill.pipeline import FormfillCrawler
from dynamic_crawler.formfill.snapshot import (DEFAULT_GRACE_DAYS,
                                               DEFAULT_MAX_AGE_DAYS,
                                               SnapshotStore, capture)

logger = logging.getLogger(__name__)

DEFAULT_HINTS = "dynamic_crawler/hints/simah.rules.yml"


class SimahCrawler(FormfillCrawler):
    """One approved form + a snapshot policy -> List[RegulatoryDocument]."""

    def __init__(self, regulator: str = "SIMAH",
                 source_system: str = "SIMAH-RULES",
                 hints_path: str | Path = DEFAULT_HINTS,
                 category: Optional[str] = None,
                 out_dir: Optional[str] = None,
                 require_approved: bool = True,
                 snapshot_dir: Optional[str] = None,
                 max_age_days: int = DEFAULT_MAX_AGE_DAYS,
                 grace_days: int = DEFAULT_GRACE_DAYS,
                 allow_live: bool = True,
                 headed: bool = True):
        super().__init__(hints_path=hints_path, regulator=regulator,
                         source_system=source_system, category=category,
                         out_dir=out_dir, require_approved=require_approved,
                         in_process=False)
        self.store = SnapshotStore(self.hints["name"], snapshot_dir,
                                   max_age_days=max_age_days,
                                   grace_days=grace_days)
        # allow_live=False makes this crawler provably incapable of generating
        # traffic — the setting to use while the block is active and someone else
        # is chasing an allowlist.
        self.allow_live = allow_live
        self.headed = headed
        self.last_capture: dict = {}

    # ------------------------------------------------------------------ #
    #  the policy                                                          #
    # ------------------------------------------------------------------ #

    def _refresh_if_due(self) -> dict:
        """Decide whether to spend a live request, and spend at most one."""
        state = self.store.state()
        if state == "fresh":
            return {"result": "not-due", "state": state}
        if not self.allow_live:
            return {"result": "not-allowed", "state": state}

        allowed, why = self.store.may_attempt()
        if not allowed:
            logger.warning("SIMAH: not attempting a refresh — %s", why)
            return {"result": "refused", "state": state, "reason": why}

        logger.warning("SIMAH: snapshot is %s, spending ONE live attempt", state)
        res = capture(self.hints, self.store, headed=self.headed)
        self.last_capture = res
        if res["result"] == "ok":
            logger.warning("SIMAH: refreshed (%s bytes, changed=%s)",
                           res.get("bytes"), res.get("changed"))
        elif res["result"] == "blocked":
            logger.error("SIMAH: BLOCKED (%s). Next attempt after %s. Falling back "
                         "to the stored snapshot.", res.get("reason"),
                         res.get("next_attempt_after"))
        return res

    def _run_crawl(self) -> dict:
        refresh = self._refresh_if_due()
        state = self.store.state()

        if state == "missing":
            raise RuntimeError(
                f"SIMAH has no snapshot at {self.store.html_path} and the live page "
                f"is not available ({refresh.get('result')}: "
                f"{refresh.get('reason', '')}). Capture one from a network the site "
                f"accepts:\n  python -m dynamic_crawler.formfill snapshot "
                f"{self.hints_path}")

        if state == "stale":
            # The loud failure that keeps a stale replay from being published as
            # current. See rule 5.
            m = self.store.manifest()
            raise RuntimeError(
                f"SIMAH's snapshot is STALE: captured {m.get('captured_at')} "
                f"({self.store.age_days():.0f} days ago), past the {self.store.grace_days}-day "
                f"grace period, and every refresh since has been blocked "
                f"({m.get('consecutive_blocks')} in a row). Refusing to publish it as "
                f"current — the documents would read 'unchanged' while the law may have "
                f"moved on. Fix access (allowlist, or a network the site accepts), or "
                f"take these instruments from SAMA's rulebook instead.")

        # Replay the form against whatever the store now holds. No network here at
        # all: the only request this class ever makes is in _refresh_if_due.
        out = Path(self.out_dir) if self.out_dir else Path(
            tempfile.mkdtemp(prefix="simah_"))
        from dynamic_crawler.formfill import runner
        runner.run(self.hints, out, fetch_details=self.fetch_details,
                   snapshot=self.store.html_path)

        pages_json = out / "pages.json"
        if not pages_json.exists():
            raise RuntimeError(f"formfill produced no pages.json in {out}")
        result = json.loads(pages_json.read_text(encoding="utf-8"))
        # Stamp provenance onto the result so _doc_from_* can copy it onto every
        # document; a reviewer must be able to see this came from a saved page.
        m = self.store.manifest()
        result["snapshot_state"] = state
        result["captured_at"] = m.get("captured_at", "")
        result["snapshot_sha256"] = m.get("sha256", "")
        return result

    # ------------------------------------------------------------------ #
    #  provenance on every document                                        #
    # ------------------------------------------------------------------ #

    def _stamp(self, doc):
        if doc is None:
            return None
        r = self.last_result or {}
        doc.extra_meta["source"] = "snapshot"
        doc.extra_meta["snapshot_state"] = r.get("snapshot_state", "")
        doc.extra_meta["captured_at"] = r.get("captured_at", "")
        doc.extra_meta["snapshot_sha256"] = r.get("snapshot_sha256", "")
        return doc

    def _doc_from_page_row(self, r: dict, shape: str):
        return self._stamp(super()._doc_from_page_row(r, shape))

    def _doc_from_document_row(self, d: dict, shape: str):
        return self._stamp(super()._doc_from_document_row(d, shape))


__all__ = ["SimahCrawler"]
