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
import json
import logging
import threading
import time
from contextlib import contextmanager
from datetime import date
from typing import Dict, List, Optional

from orchestrator.orchestrator import MIN_TEXT_LEN, Orchestrator

from dynamic_crawler import crawl_absence
from dynamic_crawler.changesignal import clean_fields, fields_of, identity_key
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
                 change_root=None,
                 **kw):
        # The analyzers are constructed by the parent's __init__ and are only
        # touched when analyse=True, so an analysis-free run costs nothing.
        super().__init__(crawler=crawler, repo=repo, downloader=downloader, **kw)
        self.source_name = source_name
        self.identity = self._clean_identity(identity)
        self.version_key = version_key or None
        self.analyse = analyse
        self.limit = limit
        # Where the per-document miss streaks live. Its own directory, not one a
        # change sweep also writes — see crawl_absence.CRAWL_ROOT.
        self.change_root = change_root
        self.report: Dict = {}
        # The folder walk is a get-then-insert across several repo calls, so a
        # lock inside the repo cannot make it safe. Two documents sharing a
        # parent folder would each find it missing and each create it, giving
        # one folder two ids and splitting the tree.
        self._folder_lock = threading.RLock()
        # (regulation_id, stored_meta, doc) for rows that are unchanged but have
        # no version token stored yet.
        self._token_backfill: List = []

    # ------------------------------------------------------------------ #
    #  IDENTITY + CLASSIFICATION                                          #
    # ------------------------------------------------------------------ #

    DEFAULT_IDENTITY = ("document_url", "doc_path")

    @staticmethod
    def _clean_identity(identity) -> tuple:
        """One string, a list or a tuple, all to a tuple of field names.

        A source YAML writes `identity: [reference_no]` or `identity: page`, so
        the value arrives in whatever shape yaml produced. The change sweep has
        to key a document exactly as this does or the two cannot be compared, so
        the shaping lives in the shared module and not here.
        """
        return clean_fields(identity) or NewOrchestrator.DEFAULT_IDENTITY

    def _identity_for(self, doc) -> tuple:
        """The identity fields for THIS document.

        One run can mix sources, so the fields come from the source that produced
        the document when it declared any, and from the run default otherwise.
        """
        declared = (getattr(doc, "extra_meta", None) or {}).get("identity_fields")
        return self._clean_identity(declared) if declared else self.identity

    def _identity_fields_of(self, doc) -> dict:
        """The configured identity of one document, as {field: value}."""
        return fields_of(doc, self._identity_for(doc))

    def _check_identities(self, docs: List) -> None:
        """Refuse a run in which a configured identity is empty on any document.

        Every field blank means every such document carries the SAME identity, so
        they match each other and the second overwrites the first. Fatal rather
        than skip-and-continue: a skipped document is also a document this run did
        not see, which would put it in `disappeared` and withdraw it because its
        key went missing.
        """
        bad = [d for d in docs if not any(self._identity_fields_of(d).values())]
        if bad:
            raise ValueError(
                f"{len(bad)} of {len(docs)} documents have an empty identity "
                f"{list(self._identity_for(bad[0]))} — e.g. "
                f"{[str(getattr(d, 'title', '?'))[:60] for d in bad[:3]]}")

    def _version_key_for(self, doc) -> Optional[str]:
        """The field the new-url tiebreak compares, per source.

        Read with `in` because a source may set it to null to switch the tiebreak
        off — `find_by_reference` searches the whole store, across sources, so a
        reference number that is only unique within one source must not drive it.
        """
        meta = getattr(doc, "extra_meta", None) or {}
        if "version_key" in meta:
            return meta["version_key"] or None
        return self.version_key

    def _identity_of(self, doc) -> tuple:
        """The identity as an ordered tuple — what logs and dedupe keys want."""
        return tuple(self._identity_fields_of(doc).values())

    def _find_existing(self, doc) -> Optional[dict]:
        """The stored row matching this document's configured identity.

        The default identity keeps using `find_by_identity`, which is the tested
        path every existing source runs on. Anything else needs the generic
        lookup, and a repo that does not offer one cannot honour the config —
        say so rather than silently classifying everything as new.
        """
        fields = self._identity_fields_of(doc)
        if tuple(fields) == self.DEFAULT_IDENTITY:
            return self.repo.find_by_identity(fields["document_url"],
                                              fields["doc_path"])
        finder = getattr(self.repo, "find_by_identity_fields", None)
        if not callable(finder):
            raise NotImplementedError(
                f"{type(self.repo).__name__} cannot look up on "
                f"identity={list(fields)}; it only supports "
                f"{list(self.DEFAULT_IDENTITY)}")
        return finder(fields)

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
        """new / modified / unchanged / disappeared / not_reread.

        `modified` is decided on content_hash: same identity, different hash. When
        the hash matches we do nothing at all — that is the cheap common case and
        the reason a nightly run is minutes rather than hours.

        `not_reread` is only ever filled by a targeted run: the documents it
        chose not to open, neither compared nor counted as absent. Stored rows
        held back from `disappeared` for the same reason are counted in
        `_not_reread_stored` rather than mixed into a bucket of documents.
        """
        buckets = {"new": [], "modified": [], "unchanged": [], "disappeared": [],
                   "not_reread": []}
        seen_ids = set()
        # Pages a targeted run walked past without opening. Their stored
        # attachments are not produced by such a run either, so both halves are
        # kept out of `disappeared` below.
        not_reread_pages = set()
        self._not_reread_stored = 0
        self._token_backfill = []
        self._check_identities(docs)

        for doc in docs:
            existing = self._find_existing(doc)

            # A row this run did not open has no content to compare, and its
            # hash would be of the LISTING. Comparing it reads as an edit and
            # B2's refresh then writes the empty page over the stored one.
            if (getattr(doc, "extra_meta", None) or {}).get("detail_skipped"):
                for u in (getattr(doc, "document_url", ""),
                          getattr(doc, "source_page_url", "")):
                    if u:
                        not_reread_pages.add(str(u).strip().rstrip("/"))
                if existing is not None:
                    seen_ids.add(existing.get("id"))
                buckets["not_reread"].append(doc)
                continue

            # Tiebreak: a regulator that republishes at a NEW url would otherwise
            # look like one new document plus one disappearance. Same reference
            # number means it is the same document at a new address.
            version_key = self._version_key_for(doc)
            if existing is None and version_key:
                ref = getattr(doc, version_key, None)
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

            # A hash built from the URL and link text cannot move when the file
            # behind an unchanged link is replaced. The server's version token
            # can, so either one moving means modified.
            old_meta = existing.get("extra_meta")
            old_token = str((old_meta or {}).get("version_token") or "") \
                if isinstance(old_meta, dict) else ""
            new_token = str(doc.extra_meta.get("version_token") or "")

            if old_hash and new_hash and old_hash == new_hash:
                if old_token and new_token and old_token != new_token:
                    self._set_status(doc, "modified")
                    buckets["modified"].append(doc)
                    continue
                # First sight of a token for a document already stored: record it
                # without reprocessing, so enabling the probe costs one metadata
                # write per document instead of a full reclassification.
                if new_token and not old_token:
                    self._token_backfill.append((existing.get("id"),
                                                 dict(old_meta or {}), doc))
                self._set_status(doc, "unchanged")
                buckets["unchanged"].append(doc)
            else:
                self._set_status(doc, "modified")
                buckets["modified"].append(doc)

        # Anything in the store for this source that this run did not see —
        # except what it deliberately did not look at. A document hanging off a
        # page a targeted run skipped is absent from the run because nothing
        # opened that page, which is not the same as gone from the site.
        for r in self._stored_for_source(docs):
            if r.get("id") in seen_ids:
                continue
            if not_reread_pages and any(
                    str(r.get(k) or "").strip().rstrip("/") in not_reread_pages
                    for k in ("source_page_url", "document_url")):
                self._not_reread_stored += 1
                continue
            buckets["disappeared"].append(r)

        return buckets

    def _apply_token_backfill(self) -> int:
        """Store first-seen version tokens on rows nothing else will write.

        An unchanged document is not otherwise touched, so without this the token
        would be re-read and discarded on every run and never become a baseline
        to compare against.
        """
        written = 0
        for regulation_id, stored_meta, doc in self._token_backfill:
            meta = dict(stored_meta or {})
            new_meta = getattr(doc, "extra_meta", None) or {}
            meta["version_token"] = new_meta.get("version_token", "")
            meta["hash_basis"] = new_meta.get("hash_basis", "")
            try:
                self.repo.update_regulation(
                    regulation_id,
                    extra_meta=json.dumps(meta, ensure_ascii=False, default=str))
                written += 1
            except Exception as e:
                logger.warning("could not store version token for %s: %s",
                               regulation_id, e)
        return written

    @staticmethod
    def _regulator_of(docs: Optional[List]) -> Optional[str]:
        """The one regulator this run's documents belong to, if it is one.

        Taken from the documents rather than from the run's name because these
        are the strings that get written to the row — a display name that differs
        by a word would scope the lookup to nothing.
        """
        names = {str(getattr(d, "regulator", "") or "").strip()
                 for d in (docs or [])} - {""}
        return names.pop() if len(names) == 1 else None

    def _stored_for_source(self, docs: Optional[List] = None) -> List[dict]:
        """What the library already holds for the sources this run covers.

        This used to read `self.repo.t`, an ExcelRepo-only table behind a
        hasattr. On MSSQL the guard was False, so `disappeared` was always empty
        and the completeness gate had nothing to gate.

        It then read `crawler.source_system`, which a composite of several sources
        does not have — so the lookup went out as None, both repos answered [] for
        a falsy source, and `disappeared` was silently empty again for every
        regulator built from a source config. Ask for every source it covers.

        Scoped by regulator when the documents agree on one, because
        `source_system` is not unique across regulators: two publish under "Rules
        and Regulations" and two under "Laws and Regulations". Unscoped, this
        bucket can hold a sibling regulator's library and offer it up as
        disappeared.
        """
        sources = [s for s in (getattr(self.crawler, "source_systems", None)
                               or [getattr(self.crawler, "source_system", None)])
                   if s]
        if not sources:
            logger.warning("%s exposes no source_system — `disappeared` will be "
                           "empty and the completeness gate is inert",
                           type(self.crawler).__name__)
            return []

        regulator = self._regulator_of(docs)
        finder = getattr(self.repo, "find_regulations_by_source", None)
        if not callable(finder):
            if hasattr(self.repo, "t"):
                return [r for r in self.repo.t["regulations"]
                        if r.get("source_system") in sources
                        and (not regulator or r.get("regulator") == regulator)]
            logger.warning("%s cannot list stored regulations — `disappeared` "
                           "will be empty and the completeness gate is inert",
                           type(self.repo).__name__)
            return []

        def rows_for(scope: Optional[str]) -> List[dict]:
            found, seen = [], set()
            for source in sources:
                for r in (finder(source, regulator=scope) if scope else finder(source)):
                    if r.get("id") not in seen:
                        seen.add(r.get("id"))
                        found.append(r)
            return found

        rows = rows_for(regulator)
        if regulator and not rows:
            # An empty bucket is the safe answer — nothing can be withdrawn from
            # it — but it must not be a silent one. Say whether the source really
            # holds nothing or the regulator string does not match the column.
            unscoped = rows_for(None)
            if unscoped:
                logger.warning(
                    "%d stored row(s) under %s, none of them under regulator %r — "
                    "`disappeared` is empty because the run's regulator name does "
                    "not match the stored one",
                    len(unscoped), sources, regulator)
        return rows

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
        # `field=value`, not the values alone: one run can carry two sources whose
        # identities are different fields entirely.
        keys = sorted(identity_key(self._identity_fields_of(d)) for d in docs)
        return hashlib.md5("\n".join(keys).encode("utf-8")).hexdigest()[:12]

    def _docs_by_source(self, docs: List) -> Dict[str, List]:
        """Documents grouped by the source that produced them.

        Every source the crawler was built with gets a key even when it produced
        nothing — a source that returned zero documents is the case this exists to
        make visible, and it is invisible in a group-by over the documents.
        """
        groups: Dict[str, List] = {
            name: [] for name in (getattr(self.crawler, "source_names", None) or [])}
        for d in docs:
            label = ((getattr(d, "extra_meta", None) or {}).get("crawl_source")
                     or self.source_name)
            groups.setdefault(label, []).append(d)
        return groups

    def _history_key(self, label: str) -> str:
        """run_history is per source. `run_history.source` is NVARCHAR(200) and
        record_run logs its own failures, so an overflow would cost the gate its
        baseline quietly — truncate here instead."""
        key = label if label == self.source_name else f"{self.source_name}/{label}"
        return key[:200]

    def _last_good(self, key: str) -> Optional[dict]:
        return (self.repo.last_good_run(key)
                if hasattr(self.repo, "last_good_run") else None)

    def _count_problem(self, label: str, prev: int, now: int) -> Optional[str]:
        spread = abs(now - prev) / max(prev, 1) * 100
        if spread <= COUNT_TOLERANCE_PCT:
            return None
        return (f"{label}: count moved {prev} -> {now} ({spread:.1f}%), over the "
                f"{COUNT_TOLERANCE_PCT}% tolerance")

    def check_run_trustworthy(self, docs: List) -> tuple:
        """(trustworthy, [reasons]). Only a trustworthy run may act on
        'disappeared'; an untrustworthy one still ingests new and modified.

        The count problems are also kept on their own: the withdrawal decision
        allows one document where this flat 5% allows none, and it needs to tell
        the two kinds of problem apart without parsing the message.
        """
        problems = []
        self._count_problems: List[str] = []
        # Sources this run had no baseline for, so their count was never checked.
        self._unchecked: List[str] = []
        crawl = getattr(self.crawler, "last_result", None) or {}
        run = crawl.get("run") or {}

        blocked = run.get("blocked_pages", 0)
        if blocked:
            problems.append(f"{blocked} page(s) came back as a bot-protection challenge")
        for w in run.get("warnings", []) or []:
            if "capped" in w.lower() or "stopped at page" in w.lower():
                problems.append(w[:120])

        last = self._last_good(self.source_name)
        if last and last.get("row_count"):
            problem = self._count_problem("total", last["row_count"], len(docs))
            if problem:
                problems.append(problem)
                self._count_problems.append(problem)

        # Per source as well as in total. A composite logs a failed source and
        # carries on, so a small source dying entirely hides inside a 5% tolerance
        # measured against the regulator's whole inventory.
        groups = self._docs_by_source(docs)
        if len(groups) > 1:
            for label, group in groups.items():
                prev = (self._last_good(self._history_key(label))
                        or {}).get("row_count")
                if not prev:
                    self._unchecked.append(label)
                    continue
                problem = self._count_problem(label, prev, len(group))
                if problem:
                    problems.append(problem)
                    self._count_problems.append(problem)
        return (not problems), problems

    def _source_gate(self, groups: Dict[str, List],
                     problems: List[str]) -> Dict[str, List[str]]:
        """Which gate problems stop which source, for the per-source history rows.

        The same attribution the withdrawal decision uses, with one addition: a
        `total` count problem stops only the sources that had no baseline of
        their own to be checked against. A source checked individually and found
        within tolerance is already answered; a source with no history was never
        checked, and letting a short run set its first baseline is what
        `last_good_run` exists to prevent.
        """
        verdicts = crawl_absence.source_verdicts(problems, list(groups))
        totals = [p for p in getattr(self, "_count_problems", [])
                  if p.startswith("total:")]
        unchecked = getattr(self, "_unchecked", [])
        return {label: [p for p in (verdicts.get(label) or [])
                        if p not in totals or label in unchecked]
                for label in groups}

    def _withdrawals(self, buckets: Dict[str, List], groups: Dict[str, List],
                     problems: List[str]) -> dict:
        """The withdrawal decision for the documents this run did not see.

        Call this BEFORE `record_run`, or the count baseline is this run's own row
        and the check can never fire. The count problems are dropped from the
        reasons because this layer re-asks that question with its own allowance.
        """
        store = crawl_absence.store_for(self.source_name,
                                        root=getattr(self, "change_root", None))
        crawl_absence.note_seen(store, buckets["new"] + buckets["modified"]
                                + buckets["unchanged"] + buckets["not_reread"],
                                self.identity)
        skipped = len(buckets["not_reread"]) + self._not_reread_stored
        block = crawl_absence.judge(
            store, buckets["disappeared"],
            identity=self.identity,
            labels=list(groups),
            counts={label: len(group) for label, group in groups.items()},
            priors={label: (self._last_good(self._history_key(label)) or {})
                    .get("row_count") for label in groups},
            problems=[p for p in problems
                      if p not in getattr(self, "_count_problems", [])],
            systems=crawl_absence.source_system_labels(self.crawler),
            # A run that walked past pages without opening them is not entitled
            # to call anything absent, the same rule as a sweep's --no-documents.
            targeted=(f"this run walked past {skipped} page(s) or row(s) without "
                      f"opening them; only a full crawl may propose"
                      if skipped else ""))
        store.save()
        return block

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
            # Keep this id. The analysis being archived describes the OLD
            # content, so it must be stamped with the old version, not the one
            # replacing it. Parent does this at orchestrator/orchestrator.py:803.
            old_version_id = self.repo.insert_regulation_version(
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
            self.repo.archive_current_analysis(existing_id, old_version_id)
            self.repo.update_regulation(existing_id, **self._modified_row_fields(doc, new_hash))
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

        # Timed: this is the step that downloads and OCRs, so it is one of the two
        # places a slow run actually spends its time.
        with self._timed(regulation_id, "text_decision") as t:
            text, content_type = self.extract_text_content_unified(doc, regulation_id)
            dec = getattr(self, "_last_decision", None)
            t["status"] = "SKIPPED" if not text else "SUCCESS"
            t["message"] = str(dec)

        if not text:
            return
        if not self.analyse:
            self._log_step(regulation_id, "llm_analysis", "SKIPPED",
                           f"analyse=False; would have sent {len(text):,} chars "
                           f"as {content_type}")
            return

        # The other one: roughly four minutes a document.
        with self._timed(regulation_id, "llm_analysis_total") as t:
            self._run_llm_analysis(regulation_id=regulation_id, doc=doc,
                                   text_content=text, content_type=content_type,
                                   version_id=version_id)
            t["message"] = f"{len(text):,} chars as {content_type}"

    @staticmethod
    def _modified_row_fields(doc, new_hash: str) -> dict:
        """What a modify refreshes on the `regulations` row.

        Updating content_hash alone left the new hash next to the old html, so
        the hash no longer described its own row. Empty values are dropped — a
        crawl that returns no title must not blank the stored one.
        """
        fields = {"content_hash": new_hash}
        for column in ("title", "document_html", "published_date",
                       "reference_no", "category"):
            value = getattr(doc, column, None)
            if value not in (None, "", []):
                fields[column] = value
        meta = getattr(doc, "extra_meta", None)
        if isinstance(meta, dict) and meta:
            fields["extra_meta"] = json.dumps(meta, ensure_ascii=False, default=str)
        return fields

    def _log_step(self, regulation_id, step, status, message, duration_ms=None):
        try:
            self.repo._log_processing(regulation_id, step, status, message,
                                      duration_ms=duration_ms)
        except Exception:
            pass

    @contextmanager
    def _timed(self, regulation_id, step):
        """Time a step and log it whether it succeeds or raises.

        Nothing in the pipeline recorded step durations, so where a run spends
        its time was unanswerable with data. The expensive steps are the text
        decision (download + OCR) and the analysis (~4 minutes a document), and
        those are the two this wraps.

        The caller's own message is set via the yielded dict, so a step can still
        say WHAT it did as well as how long it took.
        """
        box = {"status": "SUCCESS", "message": ""}
        t0 = time.perf_counter()
        try:
            yield box
        except Exception as e:
            self._log_step(regulation_id, step, "FAILED", str(e)[:400],
                           duration_ms=(time.perf_counter() - t0) * 1000)
            raise
        else:
            self._log_step(regulation_id, step, box["status"], box["message"],
                           duration_ms=(time.perf_counter() - t0) * 1000)

    # ------------------------------------------------------------------ #
    #  THE RUN                                                            #
    # ------------------------------------------------------------------ #

    def run_for_regulator(self, regulator_name: str) -> Dict:
        docs = self.crawler.fetch_documents()
        logger.warning("crawler returned %d documents", len(docs))

        trustworthy, problems = self.check_run_trustworthy(docs)
        inv = self._inventory_hash(docs)
        buckets = self.classify_documents(docs)
        tokens_stored = self._apply_token_backfill()

        last = self._last_good(self.source_name)
        if (last and last.get("inventory_hash") == inv
                and not any(buckets[k] for k in ("new", "modified", "disappeared"))):
            # This logged "nothing to do" and then did everything anyway.
            # Exits only when the buckets agree with the hash — an unchanged
            # inventory with pending work means a previous run died mid-way.
            logger.warning("inventory hash unchanged (%s) — nothing to do", inv)
            self.report = {
                "regulator": regulator_name,
                "source": self.source_name,
                "crawled": len(docs),
                "classified": {k: len(v) for k, v in buckets.items()},
                "processed": 0,
                "limit": self.limit,
                "analyse": self.analyse,
                "skipped": "inventory hash unchanged since last good run",
                "inventory_hash": inv,
                "run_trustworthy": trustworthy,
                "gate_problems": problems,
                "disappeared_actioned": False,
                # Nothing is absent on this path, but the streak memory is still
                # written: a run that recorded nothing leaves every document
                # unattributed, and an unattributed absence can never be judged.
                "withdrawals": self._withdrawals(
                    buckets, self._docs_by_source(docs), problems),
                "version_tokens_stored": tokens_stored,
                "tables": self.repo.counts() if hasattr(self.repo, "counts") else {},
            }
            return self.report

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
        groups = self._docs_by_source(docs)
        withdrawals = self._withdrawals(buckets, groups, problems)
        gate = self._source_gate(groups, problems)
        if hasattr(self.repo, "record_run"):
            self.repo.record_run(self.source_name, len(docs), inv, verdict,
                                 "; ".join(problems)[:400])
            # One row per source too, each with the verdict its OWN problems
            # earn. Stamping the run's verdict here froze a healthy source's
            # baseline for as long as a sibling was broken, and `last_good_run`
            # reads PASS only — so it then failed its own count check against a
            # baseline several runs old.
            if len(groups) > 1:
                for label, group in groups.items():
                    own = gate[label]
                    self.repo.record_run(self._history_key(label), len(group),
                                         self._inventory_hash(group),
                                         "PASS" if not own else "QUARANTINED",
                                         "; ".join(own)[:400])

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
            # Still False, and it is not the same claim as `withdrawals`: that
            # block is a proposal for a person, and no code here writes a status.
            "disappeared_actioned": False,
            "withdrawals": withdrawals,
            "version_tokens_stored": tokens_stored,
            "tables": self.repo.counts() if hasattr(self.repo, "counts") else {},
        }
        if len(groups) > 1:
            self.report["by_source"] = {k: len(v) for k, v in groups.items()}
            self.report["gate_by_source"] = {
                label: {"verdict": "PASS" if not own else "QUARANTINED",
                        "problems": own}
                for label, own in gate.items()}
        if buckets["not_reread"] or self._not_reread_stored:
            # A targeted run. Said out loud, because its `unchanged` count is
            # not the same claim a full crawl's is: most of this source was
            # never looked at.
            self.report["targeted_run"] = {
                "documents_not_reread": len(buckets["not_reread"]),
                "stored_rows_not_reread": self._not_reread_stored,
            }
        if buckets["disappeared"]:
            self.report["note"] = (
                f"{len(buckets['disappeared'])} document(s) were not seen this run. "
                + f"See `withdrawals`: {withdrawals['counts']}. Nothing is "
                  f"withdrawn by this run — the block is a proposal, and the "
                  f"status write needs a senior developer's approval.")
        return self.report


__all__ = ["NewOrchestrator"]
