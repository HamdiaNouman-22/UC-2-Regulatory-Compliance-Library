"""FormfillCrawler — makes a form answer `fetch_documents() -> List[RegulatoryDocument]`.

The orchestrator asks exactly one thing of any crawler (orchestrator.py,
run_for_regulator):

    docs = self.crawler.fetch_documents()      # -> List[RegulatoryDocument]

`crawler/generic_crawler_wrapper.py::GenericSiteCrawler` already answers that for
the generic engine, and it does the whole pages.json -> RegulatoryDocument
mapping. Since formfill emits pages.json in the SAME schema on purpose, the only
thing that differs is which engine produced the file.

So this subclasses it and overrides one method. No mapping logic is copied: if
the pipeline's idea of a document changes, both engines change together.

WHAT A FORM ADDS OVER THE GENERIC WALK
--------------------------------------
The generic wrapper leaves `published_date` as None ("a link walk cannot reliably
read issue dates") and regex-guesses `reference_no` from the row text. A form
DECLARES those fields, and the verify gate measures how often they actually fill
— on SBP: published_date 99.6%, department 90.9%, reference_no 89.1%. This class
prefers the form's extracted values and only falls back to the generic guess.

    from dynamic_crawler.formfill.pipeline import FormfillCrawler
    docs = FormfillCrawler("dynamic_crawler/hints/sbp.circulars.yml",
                           regulator="SBP", source_system="SBP-CIRCULARS").fetch_documents()

Refusing to run an unapproved form is the default. The gate only means something
if the pipeline honours it.
"""

from __future__ import annotations

import logging
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Optional

try:
    from crawler.generic_crawler_wrapper import GenericSiteCrawler, _parse_row_date
except ImportError as _e:                      # pragma: no cover - environment issue
    # This module is the ONLY part of formfill that needs the generic wrapper —
    # deliberately, since it reuses that file's pages.json -> RegulatoryDocument
    # mapping instead of copying it. If the wrapper has not landed in your branch
    # yet, everything else (inspect / propose / run / verify / approve) still
    # works; only the pipeline adapter is unavailable.
    raise ImportError(
        "dynamic_crawler.formfill.pipeline needs crawler/generic_crawler_wrapper.py, "
        "which is not present in this checkout. Pull the branch that adds it. "
        "The formfill CLI does not depend on it and works without.") from _e

from dynamic_crawler.formfill.schema import approval_state, load_hints

logger = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[2]


class FormfillCrawler(GenericSiteCrawler):
    """One approved hints file -> a list of RegulatoryDocument."""

    def __init__(self, hints_path: str | Path, regulator: str, source_system: str,
                 category: Optional[str] = None, out_dir: Optional[str] = None,
                 require_approved: bool = True, fetch_details: Optional[bool] = None,
                 in_process: bool = False, timeout: int = 14400):
        self.hints_path = str(hints_path)
        self.hints = load_hints(self.hints_path)
        ok, why = approval_state(self.hints)
        if require_approved and not ok:
            raise RuntimeError(
                f"{self.hints_path}: {why}\n"
                "The pipeline will not run a form that has not passed the gate. "
                "Run `formfill verify` then `formfill approve`, or pass "
                "require_approved=False for a deliberate one-off.")
        if not ok:
            logger.warning("running UNAPPROVED form %s (%s)", self.hints_path, why)

        super().__init__(
            seed_url=self.hints["seed_url"],
            regulator=regulator,
            source_system=source_system,
            category=category,
            scope=self.hints.get("scope", "auto"),
            out_dir=out_dir,
            in_process=in_process,
            timeout=timeout,
        )
        self.fetch_details = fetch_details

    def _run_crawl(self) -> dict:
        """Run the form instead of the generic engine. Everything downstream —
        the mapping, the folder trail, the dedupe — is inherited unchanged."""
        import json

        out = Path(self.out_dir) if self.out_dir else Path(
            tempfile.mkdtemp(prefix="formfill_"))

        if self.in_process:
            from dynamic_crawler.formfill import runner
            runner.run(self.hints, out, fetch_details=self.fetch_details)
        else:
            # Same reason as the parent class: Playwright's sync API refuses to
            # start inside the asyncio/twisted reactor the scheduler installs.
            cmd = [sys.executable, "-m", "dynamic_crawler.formfill", "run",
                   self.hints_path, "--out", str(out)]
            if self.fetch_details is False:
                cmd.append("--no-details")
            proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO_ROOT),
                                  encoding="utf-8", errors="replace", timeout=self.timeout)
            if proc.returncode != 0:
                tail = (proc.stderr or proc.stdout or "").strip().splitlines()
                raise RuntimeError(f"formfill failed for {self.hints_path}: "
                                   f"{tail[-1] if tail else 'no output'}")

        pages_json = out / "pages.json"
        if not pages_json.exists():
            raise RuntimeError(f"formfill produced no pages.json in {out}")
        return json.loads(pages_json.read_text(encoding="utf-8"))

    # ---- a declared row IS a document ---------------------------------------
    #
    # The parent has to GUESS whether a page is a document or just a folder in
    # the site tree, because a link walk cannot know: it uses "has it got 200+
    # characters of prose?" as the test.
    #
    # That guess is wrong for a form, and expensively so. SBP's 4,160-circular
    # inventory is phase 1 only — every row has a title, a URL, a date and a
    # reference number, but no page text yet, and no attached PDF. Under the
    # parent's rule all 4,160 were discarded and fetch_documents() returned ZERO.
    # The most valuable output we have — the complete inventory that drives
    # change detection — vanished at the pipeline boundary.
    #
    # A form does not have to guess. A human declared "this selector is one
    # document entry" and the gate verified it fills. So: a row with a URL is a
    # document.

    def _want_pages(self, pages: List[dict]) -> bool:
        return True

    @staticmethod
    def _page_is_document(r: dict) -> bool:
        return bool((r.get("url") or "").strip())

    # ---- the fields a form knows and a link walk does not -------------------

    def _doc_from_page_row(self, r: dict, shape: str):
        """Let the form's declared fields win over the parent's regex guesses.

        The parent parses `published_date` and `reference_no` out of the raw row
        text, which is the best it can do without a form. When the form named
        those fields explicitly, that value is the better one — it was written
        against this site and its fill rate was measured before approval.
        """
        doc = super()._doc_from_page_row(r, shape)
        if doc is None:
            return None
        fields = r.get("fields") or {}
        for target in ("reference_no", "department", "year", "category", "urdu_url"):
            value = (fields.get(target) or "").strip()
            if value:
                setattr(doc, target, value)

        # Dates go through the pipeline's own parser rather than straight in. A
        # form extracts what the page says — "July 31 2026" — but the pipeline
        # dedupes on published_date, so a mix of site formats and ISO would stop
        # two records of the same document matching. If it will not parse, keep
        # whatever the parent worked out rather than overwriting good with bad.
        raw_date = (fields.get("published_date") or "").strip()
        if raw_date:
            doc.published_date = _parse_row_date(raw_date) or doc.published_date or raw_date
        doc.extra_meta["crawler"] = "formfill"
        doc.extra_meta["hints"] = self.hints_path
        doc.extra_meta["form_approved_by"] = (self.hints.get("meta") or {}).get("approved_by")
        return doc

    def _doc_from_document_row(self, d: dict, shape: str):
        doc = super()._doc_from_document_row(d, shape)
        if doc is None:
            return None
        doc.extra_meta["crawler"] = "formfill"
        doc.extra_meta["hints"] = self.hints_path
        # A file linked from many pages is still ONE document; keep the evidence
        # of where else it appeared rather than dropping it.
        if d.get("times_linked", 1) > 1:
            doc.extra_meta["times_linked"] = d["times_linked"]
            doc.extra_meta["also_in"] = d.get("also_in", "")
        return doc


def build_formfill_source(cfg: dict):
    """A `mode: formfill` entry in config/sources/<regulator>.yml.

        - name: "Circulars"
          mode: formfill
          hints: dynamic_crawler/hints/sbp.circulars.yml
          source_system: "SBP-CIRCULARS"
          category: "Circulars"
    """
    missing = [k for k in ("hints", "regulator", "source_system") if not cfg.get(k)]
    if missing:
        raise ValueError(f"source '{cfg.get('name')}': missing {missing}")
    return FormfillCrawler(
        hints_path=cfg["hints"],
        regulator=cfg["regulator"],
        source_system=cfg["source_system"],
        category=cfg.get("category"),
        out_dir=cfg.get("out_dir"),
        require_approved=bool(cfg.get("require_approved", True)),
        fetch_details=cfg.get("fetch_details"),
    )


__all__ = ["FormfillCrawler", "build_formfill_source"]
