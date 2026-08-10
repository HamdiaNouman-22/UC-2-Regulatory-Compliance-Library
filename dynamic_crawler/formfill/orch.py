"""NewOrchestrator — the orchestrator changes we agreed, as a subclass.

`orchestrator.py` and `storage/mssql_repo.py` still have uncommitted changes from
another session, so nothing here edits them. Everything is an override, which
also makes the diff reviewable: what follows IS the list of changes.

WHAT IS DIFFERENT FROM THE PARENT

1. ONE DOOR. `run_for_regulator` handles every regulator including CBB. There is
   no `run_for_cbb`, and no `if regulator_upper == "CBB"` fork.

2. classify_documents() REPLACES filter_new_documents(). Four outcomes instead of
   two — new / modified / unchanged / disappeared — decided by ONE configured
   identity key instead of three hardcoded per-regulator branches. It also sets
   the two keys the crawler cannot know, because they need a database lookup:
       extra_meta["monitoring_status"]      = "new" | "modified"
       extra_meta["existing_regulation_id"]
   Doing it here keeps crawlers DB-free, which they must be: formfill runs as a
   subprocess.

3. VERSIONING FOR EVERY REGULATOR. `_process_versioned_doc` is the parent's CBB
   path with the CBB check removed and its raw `UPDATE regulation_versions` SQL
   replaced by `repo.mark_all_versions_inactive()`.

4. THE COMPLETENESS GATE. A run may not mark anything disappeared unless the run
   itself is trustworthy: no bot-protection pages, no early stop, not capped, and
   the count within tolerance of the last good run. SDAIA returned 415/363/439 on
   three runs of identical code — a run that "loses" 52 documents is not a run
   where 52 were withdrawn.

5. THE TEXT DECISION. `extract_text_content_unified` is replaced by
   formfill/textinput.py: a gate (is there anything to analyse at all?) and then
   the HTML-vs-file choice — same content, send the HTML; different, SEND BOTH.

6. NO STRING BRANCHES. "regulatory returns" is not special-cased by name; a
   document is analysed when there is text to analyse and skipped when there is
   not, which the parent already does for short text.
"""

from __future__ import annotations

import hashlib
import logging
import threading
from datetime import date
from typing import Dict, List, Optional

from orchestrator.orchestrator import MIN_TEXT_LEN, Orchestrator

from dynamic_crawler.formfill.textinput import decide_for_document

logger = logging.getLogger(__name__)

# Percent spread against the last good run before a run is distrusted.
COUNT_TOLERANCE_PCT = 5.0


class NewOrchestrator(Orchestrator):
    def __init__(self, crawler, repo, downloader=None, *,
                 source_name: str = "unknown",
                 identity: tuple = ("document_url", "doc_path"),
                 version_key: Optional[str] = "reference_no",
                 analyse: bool = False,
                 limit: Optional[int] = None,
                 **kw):
        # The analyzers are constructed by the parent's __init__ and are only
        # touched when analyse=True, so an analysis-free run costs nothing.
        super().__init__(crawler=crawler, repo=repo, downloader=downloader, **kw)
        self.source_name = source_name
        self.identity = identity
        self.version_key = version_key
        self.analyse = analyse
        self.limit = limit
        self.report: Dict = {}
        # The folder walk is a get-then-insert across several repo calls, so a
        # lock inside the repo cannot make it safe. Two documents sharing a
        # parent folder would each find it missing and each create it, giving
        # one folder two ids and splitting the tree.
        self._folder_lock = threading.RLock()

    # ------------------------------------------------------------------ #
    #  IDENTITY + CLASSIFICATION                                          #
    # ------------------------------------------------------------------ #

    def _identity_of(self, doc) -> tuple:
        url = (getattr(doc, "document_url", "") or "").strip()
        path = " > ".join(getattr(doc, "doc_path", None) or [])
        return (url, path)

    @staticmethod
    def _set_status(doc, monitoring_status: str) -> None:
        """Put the monitoring state in `status`, where the schema expects it.

        `regulations.status` is a real column — `_insert_regulation` reads
        `getattr(document, "status", "active")` — so the monitoring state belongs
        there rather than buried in extra_meta.

        THREE things wanted that one column and they are not the same thing:

          our lifecycle          active / inactive, used by the archive logic
          the REGULATOR's status "In-Force" / "Superseded", straight off SAMA's
                                 own table column
          the monitoring state   new / modified / unchanged

        The regulator's claim about its own document is not our record's state, so
        it moves to extra_meta["regulator_status"] and `status` becomes ours
        alone. Anything already in `status` from a form field is preserved there
        rather than being silently overwritten.
        """
        meta = doc.extra_meta = dict(getattr(doc, "extra_meta", None) or {})
        site_status = (getattr(doc, "status", "") or "").strip()
        if site_status and site_status.lower() not in (
                "new", "modified", "unchanged", "active", "inactive", "withdrawn"):
            meta.setdefault("regulator_status", site_status)
        meta["monitoring_status"] = monitoring_status      # kept for the CBB path
        doc.status = monitoring_status

    def classify_documents(self, docs: List) -> Dict[str, List]:
        """new / modified / unchanged / disappeared.

        `modified` is decided on content_hash: same identity, different hash. When
        the hash matches we do nothing at all — that is the cheap common case and
        the reason a nightly run is minutes rather than hours.
        """
        buckets = {"new": [], "modified": [], "unchanged": [], "disappeared": []}
        seen_ids = set()

        for doc in docs:
            url, path = self._identity_of(doc)
            existing = self.repo.find_by_identity(url, path)

            # Tiebreak: a regulator that republishes at a NEW url would otherwise
            # look like one new document plus one disappearance. Same reference
            # number means it is the same document at a new address.
            if existing is None and self.version_key:
                ref = getattr(doc, self.version_key, None)
                if ref:
                    existing = self.repo.find_by_reference(ref)

            if existing is None:
                doc.extra_meta = dict(getattr(doc, "extra_meta", None) or {})
                self._set_status(doc, "new")
                buckets["new"].append(doc)
                continue

            seen_ids.add(existing.get("id"))
            old_hash = (existing.get("content_hash") or "").strip()
            new_hash = (getattr(doc, "content_hash", "") or "").strip()
            doc.extra_meta = dict(getattr(doc, "extra_meta", None) or {})
            doc.extra_meta["existing_regulation_id"] = existing.get("id")

            if old_hash and new_hash and old_hash == new_hash:
                self._set_status(doc, "unchanged")
                buckets["unchanged"].append(doc)
            else:
                self._set_status(doc, "modified")
                buckets["modified"].append(doc)

        # Anything in the store for this source that this run did not see.
        for r in self.repo.t["regulations"] if hasattr(self.repo, "t") else []:
            if r.get("source_system") == getattr(self.crawler, "source_system", None) \
                    and r.get("id") not in seen_ids:
                buckets["disappeared"].append(r)

        return buckets

    # ------------------------------------------------------------------ #
    #  THE FOLDER TREE — folders are "F", the document's own node is "R"   #
    # ------------------------------------------------------------------ #

    def _get_or_create_compliance_category(self, hierarchy: list) -> int:
        """Same walk as the parent, but it types the nodes.

        `compliancecategory.type` is what the frontend uses to tell a folder from
        a regulation. The parent calls `insert_folder(title, parent_id)` and never
        passes the third argument, so every node took the default "F" — including
        the leaf, which since the doc_path change IS the document. Every document
        therefore rendered as an empty folder.

        The convention already exists in the repo (tests/push_vol7_draft.py):

            cat_type = "R" if (is_last and is_leaf) else "F"

        Intermediate nodes are folders; the last segment is the regulation.
        """
        with self._folder_lock:
            return self._walk_folders(hierarchy)

    def _walk_folders(self, hierarchy: list) -> int:
        parent_id = None
        last_index = len(hierarchy) - 1

        for i, title in enumerate(hierarchy):
            folder_id = self.repo.get_folder_id(title, parent_id)

            if folder_id is None and parent_id is not None:
                folder_id = self.repo.find_folder_in_subtree(title, parent_id)

            # Leaf rule, unchanged from the parent: never hand one document's node
            # to another. A same-named sibling is created instead.
            if folder_id is not None and i == last_index:
                if self.repo.regulation_exists_for_category(folder_id):
                    folder_id = None

            if folder_id is None:
                folder_id = self.repo.insert_folder(
                    title, parent_id, cat_type=("R" if i == last_index else "F"))
            parent_id = folder_id

        return parent_id

    # ------------------------------------------------------------------ #
    #  THE COMPLETENESS GATE                                             #
    # ------------------------------------------------------------------ #

    def _inventory_hash(self, docs: List) -> str:
        keys = sorted("|".join(self._identity_of(d)) for d in docs)
        return hashlib.md5("\n".join(keys).encode("utf-8")).hexdigest()[:12]

    def check_run_trustworthy(self, docs: List) -> tuple:
        """(trustworthy, [reasons]). Only a trustworthy run may act on
        'disappeared'; an untrustworthy one still ingests new and modified."""
        problems = []
        crawl = getattr(self.crawler, "last_result", None) or {}
        run = crawl.get("run") or {}

        blocked = run.get("blocked_pages", 0)
        if blocked:
            problems.append(f"{blocked} page(s) came back as a bot-protection challenge")
        for w in run.get("warnings", []) or []:
            if "capped" in w.lower() or "stopped at page" in w.lower():
                problems.append(w[:120])

        last = self.repo.last_good_run(self.source_name) if hasattr(
            self.repo, "last_good_run") else None
        if last and last.get("row_count"):
            prev, now = last["row_count"], len(docs)
            spread = abs(now - prev) / max(prev, 1) * 100
            if spread > COUNT_TOLERANCE_PCT:
                problems.append(
                    f"count moved {prev} -> {now} ({spread:.1f}%), over the "
                    f"{COUNT_TOLERANCE_PCT}% tolerance")
        return (not problems), problems

    # ------------------------------------------------------------------ #
    #  THE TEXT DECISION                                                  #
    # ------------------------------------------------------------------ #

    def extract_text_content_unified(self, doc, regulation_id: Optional[int] = None):
        """The gate, then HTML-vs-file. Replaces first-tier-wins."""
        dec = decide_for_document(
            doc,
            fetch_file_text=self._safe_pdf_text,
            fetch_page_text=self._safe_page_text,
            min_text_len=MIN_TEXT_LEN,
        )
        self._last_decision = dec
        logger.info("  %s", dec)
        if dec.skip:
            return None, None
        return dec.text, dec.content_type

    def _safe_pdf_text(self, url: str) -> Optional[str]:
        try:
            return self._download_and_extract_pdf(url)
        except Exception as e:                     # never lose a document to a fetch
            logger.warning("  pdf fetch failed for %s: %s", url[:70], e)
            return None

    def _safe_page_text(self, url: str) -> Optional[str]:
        try:
            import requests
            from bs4 import BeautifulSoup
            r = requests.get(url, timeout=45, headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(r.text, "html.parser")
            for t in soup(["script", "style", "noscript", "header", "footer", "nav"]):
                t.decompose()
            return soup.get_text(" ", strip=True)
        except Exception as e:
            logger.warning("  page fetch failed for %s: %s", url[:70], e)
            return None

    # ------------------------------------------------------------------ #
    #  ONE PROCESSING PATH FOR EVERY REGULATOR                            #
    # ------------------------------------------------------------------ #

    def _process_single_doc(self, idx, doc, regulator_name):
        try:
            if isinstance(getattr(doc, "doc_path", None), list):
                doc.compliancecategory_id = self._get_or_create_compliance_category(doc.doc_path)
            else:
                doc.compliancecategory_id = None
        except Exception as e:
            logger.error("folder tree failed: %s", e)
            doc.compliancecategory_id = None

        # No `if regulator == CBB` and no `if category == "regulatory returns"`.
        # Everything takes the versioned path; whether it gets analysed is decided
        # by whether there is text, not by its name.
        self._process_versioned_doc(doc)

    def _process_versioned_doc(self, doc):
        meta = getattr(doc, "extra_meta", None) or {}
        status = meta.get("monitoring_status", "new")
        existing_id = meta.get("existing_regulation_id")
        new_hash = getattr(doc, "content_hash", "") or ""

        if status == "modified" and existing_id:
            # Archive what is there, then snapshot the new content. Same steps as
            # the parent's CBB path, minus the hand-written SQL.
            self.repo.mark_all_versions_inactive(existing_id)
            old = self.repo.get_regulation_by_id(existing_id) or {}
            self.repo.insert_regulation_version(
                regulation_id=existing_id,
                content_text=(old.get("extra_meta") or {}).get("content_text", "")
                             if isinstance(old.get("extra_meta"), dict) else "",
                content_html=old.get("document_html") or "",
                content_hash=old.get("content_hash") or "",
                updated_date=date.today(), status="inactive",
                change_summary=f"archived {date.today().isoformat()}")
            version_id = self.repo.insert_regulation_version(
                regulation_id=existing_id,
                content_text=meta.get("content_text", ""),
                content_html=getattr(doc, "document_html", "") or "",
                content_hash=new_hash, updated_date=date.today(), status="active",
                change_summary="content changed")
            self.repo.archive_current_analysis(existing_id, version_id)
            self.repo.update_regulation(existing_id, content_hash=new_hash)
            regulation_id = existing_id
            self._log_step(regulation_id, "version", "SUCCESS",
                           f"new version {version_id} (was {old.get('content_hash','')[:8]})")
        else:
            regulation_id = self.repo._insert_regulation(doc)
            doc.id = regulation_id
            version_id = self.repo.insert_regulation_version(
                regulation_id=regulation_id,
                content_text=meta.get("content_text", ""),
                content_html=getattr(doc, "document_html", "") or "",
                content_hash=new_hash, updated_date=date.today(), status="active",
                change_summary="first version")
            self._log_step(regulation_id, "insert", "SUCCESS", "inserted")

        text, content_type = self.extract_text_content_unified(doc, regulation_id)
        dec = getattr(self, "_last_decision", None)
        self._log_step(regulation_id, "text_decision",
                       "SKIPPED" if not text else "SUCCESS", str(dec))
        if not text:
            return
        if not self.analyse:
            self._log_step(regulation_id, "llm_analysis", "SKIPPED",
                           f"analyse=False; would have sent {len(text):,} chars "
                           f"as {content_type}")
            return
        self._run_llm_analysis(regulation_id=regulation_id, doc=doc,
                               text_content=text, content_type=content_type,
                               version_id=version_id)

    def _log_step(self, regulation_id, step, status, message):
        try:
            self.repo._log_processing(regulation_id, step, status, message)
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    #  THE RUN                                                            #
    # ------------------------------------------------------------------ #

    def run_for_regulator(self, regulator_name: str) -> Dict:
        docs = self.crawler.fetch_documents()
        logger.warning("crawler returned %d documents", len(docs))

        trustworthy, problems = self.check_run_trustworthy(docs)
        inv = self._inventory_hash(docs)
        buckets = self.classify_documents(docs)

        last = self.repo.last_good_run(self.source_name) if hasattr(
            self.repo, "last_good_run") else None
        if last and last.get("inventory_hash") == inv:
            logger.warning("inventory hash unchanged (%s) — nothing to do", inv)

        todo = buckets["new"] + buckets["modified"]
        if self.limit:
            todo = todo[:self.limit]

        # The parent's thread pool, not a loop of our own.
        #
        # `_process_docs` already does what this needs: DOC_MAX_WORKERS documents
        # in flight (default 4), LLM calls separately capped by
        # LLM_MAX_CONCURRENCY inside StagedLLMAnalyzer so more workers cannot
        # stampede OpenRouter, and one failed document never aborting the batch.
        # Looping serially here quietly gave all of that up — with analyse=true
        # at roughly four minutes a document, a 40-document run took 2.7 hours
        # instead of 40 minutes.
        #
        # Set DOC_MAX_WORKERS=1 to get the serial behaviour back.
        if todo:
            self._process_docs(todo, regulator_name)

        verdict = "PASS" if trustworthy else "QUARANTINED"
        if hasattr(self.repo, "record_run"):
            self.repo.record_run(self.source_name, len(docs), inv, verdict,
                                 "; ".join(problems)[:400])

        self.report = {
            "regulator": regulator_name,
            "source": self.source_name,
            "crawled": len(docs),
            "classified": {k: len(v) for k, v in buckets.items()},
            "processed": len(todo),
            "limit": self.limit,
            "analyse": self.analyse,
            "inventory_hash": inv,
            "run_trustworthy": trustworthy,
            "gate_problems": problems,
            "disappeared_actioned": bool(trustworthy) and bool(buckets["disappeared"]),
            "tables": self.repo.counts() if hasattr(self.repo, "counts") else {},
        }
        if not trustworthy and buckets["disappeared"]:
            self.report["note"] = (
                f"{len(buckets['disappeared'])} document(s) were not seen this run, "
                "but the run is not trustworthy so nothing was marked withdrawn")
        return self.report


__all__ = ["NewOrchestrator"]
