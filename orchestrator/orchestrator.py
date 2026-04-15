import os
import logging
import gc
from processor.downloader import Downloader
from storage.mssql_repo import MSSQLRepository
from processor.html_fallback_engine import HTMLFallbackEngine
from typing import List, Optional, Tuple
from processor.LlmAnalyzer import LLMAnalyzer
from processor.requirement_matcher import RequirementMatcher
from processor.Text_Extractor import OCRProcessor
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from processor.staged_LLM_Analyzer import StagedLLMAnalyzer
import json
from datetime import date

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("orchestrator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

MIN_TEXT_LEN = 200


class Orchestrator:
    """
    Central pipeline controller.

    VERSIONING STRATEGY (unified — all regulators use compliance_analysis):
    ────────────────────────────────────────────────────────────────────────
    All analysis goes into compliance_analysis (is_current=1, schema_version='v2').

    For CBB specifically:
      - version_id is set on every compliance_analysis row (links to
        regulation_versions.version_id).
      - When a CBB document is modified, old compliance_analysis rows are
        moved to compliance_analysis_versions (status='inactive') and
        deleted from compliance_analysis BEFORE new analysis is written.
      - regulation_versions holds the content snapshots (HTML/text/hash).

    For SAMA / SBP / SECP:
      - version_id is NULL on compliance_analysis rows.
      - No archiving, no regulation_versions rows.
    ────────────────────────────────────────────────────────────────────────
    """

    def __init__(self, crawler, repo: MSSQLRepository, downloader: Downloader,
                 ocr_engine: HTMLFallbackEngine = None, llm_analyzer: LLMAnalyzer = None):
        self.crawler = crawler
        self.repo = repo
        self.downloader = downloader
        self.ocr_engine = ocr_engine
        self.llm_analyzer = LLMAnalyzer()
        self.staged_analyzer = StagedLLMAnalyzer()
        self.requirement_matcher = RequirementMatcher()

    # ================================================================== #
    #  HELPERS                                                             #
    # ================================================================== #

    def create_robust_session(self):
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def log(self, regulation_id, step, status, message, doc_url=None):
        try:
            self.repo._log_processing(
                regulation_id=regulation_id,
                step=step,
                status=status,
                message=message,
                document_url=doc_url
            )
        except Exception as e:
            logger.error(f"Failed to write processing log: {e}")

    # ================================================================== #
    #  UNIFIED CONTENT EXTRACTION — 3-TIER STRATEGY                        #
    # ================================================================== #

    def extract_text_content_unified(
            self,
            doc,
            regulation_id: Optional[int] = None
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Unified 3-tier content extraction.
        Returns (text_content, content_type) or (None, None).

        FOR CBB: If regulation_id is provided, fetch from regulation_versions first.
        """
        extra_meta = getattr(doc, "extra_meta", {}) or {}

        # ── CBB VERSIONED CONTENT: Fetch from regulation_versions ──
        if regulation_id:
            try:
                # Check if this is a CBB regulation
                reg_data = self.repo.get_regulation_by_id(regulation_id)
                if reg_data and reg_data.get("regulator") == "Central Bank of Bahrain":
                    logger.info(f"  → CBB regulation detected, fetching from regulation_versions...")

                    # Get the ACTIVE version
                    version_data = self.repo.get_active_regulation_version(regulation_id)

                    if version_data:
                        content_text = (version_data.get("content_text") or "").strip()
                        content_html = (version_data.get("content_html") or "").strip()

                        if len(content_text) >= MIN_TEXT_LEN:
                            logger.info(f"  ✓ CBB VERSION: content_text ({len(content_text):,} chars)")
                            return content_text, "html"

                        if len(content_html) >= MIN_TEXT_LEN:
                            logger.info(f"  ✓ CBB VERSION: content_html ({len(content_html):,} chars)")
                            return content_html, "html"

                        logger.warning(f"  ⚠ CBB version exists but text too short")
            except Exception as e:
                logger.warning(f"  ⚠ Failed to fetch CBB version content: {e}")

        # Tier 1a: SAMA pre-OCR'd PDF text
        org_pdf_text = (extra_meta.get("org_pdf_text") or "").strip()
        if len(org_pdf_text) >= MIN_TEXT_LEN:
            logger.info(f"  ✓ TIER 1a: org_pdf_text ({len(org_pdf_text):,} chars)")
            return org_pdf_text, "pdf_text"

        # Tier 1b: CBB / pre-extracted HTML content_text (from extra_meta)
        content_text = (extra_meta.get("content_text") or "").strip()
        if len(content_text) >= MIN_TEXT_LEN:
            logger.info(f"  ✓ TIER 1b: content_text ({len(content_text):,} chars)")
            return content_text, "html"

        # Tier 2: Stored document HTML
        document_html = (getattr(doc, "document_html", None) or "").strip()
        if len(document_html) >= MIN_TEXT_LEN:
            logger.info(f"  ✓ TIER 2: document_html ({len(document_html):,} chars)")
            return document_html, "html"
        # Tier 3: Download & OCR
        logger.info("  → TIER 3: no pre-extracted text, trying downloads...")

        org_pdf_link = extra_meta.get("org_pdf_link")
        if org_pdf_link:
            text = self._download_and_extract_pdf(org_pdf_link, regulation_id)
            if text and len(text) >= MIN_TEXT_LEN:
                return text, "pdf_text"

        document_url = getattr(doc, "document_url", None) or ""
        if document_url.lower().endswith(".pdf"):
            text = self._download_and_extract_pdf(document_url, regulation_id)
            if text and len(text) >= MIN_TEXT_LEN:
                return text, "pdf_text"

        arabic_pdf_link = extra_meta.get("arabic_pdf_link")
        if arabic_pdf_link:
            text = self._download_and_extract_pdf(arabic_pdf_link, regulation_id)
            if text and len(text) >= MIN_TEXT_LEN:
                return text, "pdf_text"

        urdu_url = extra_meta.get("urdu_url")
        if urdu_url:
            text = self._download_and_extract_pdf(urdu_url, regulation_id)
            if text and len(text) >= MIN_TEXT_LEN:
                return text, "pdf_text"

        if document_url and not document_url.lower().endswith(".pdf"):
            logger.info("  → Tier 3e: fetching HTML from document_url...")
            try:
                resp = requests.get(
                    document_url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=30
                )
                resp.raise_for_status()
                html = resp.text
                if html and len(html) >= MIN_TEXT_LEN:
                    logger.info(f"  ✓ Tier 3e: HTML ({len(html):,} chars)")
                    return html, "html"
            except Exception as e:
                logger.warning(f"  ⚠ Tier 3e HTML fetch failed: {e}")

        logger.warning("  ✗ All extraction tiers exhausted")
        return None, None

    def _download_and_extract_pdf(
        self,
        pdf_url: str,
        regulation_id: Optional[int] = None
    ) -> Optional[str]:
        import tempfile
        tmp_path = None
        try:
            logger.info(f"    ⬇ PDF: {pdf_url[:80]}")
            resp = requests.get(
                pdf_url,
                headers={"User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )},
                timeout=60,
                stream=True
            )
            resp.raise_for_status()

            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                for chunk in resp.iter_content(chunk_size=8192):
                    tmp.write(chunk)
                tmp_path = tmp.name

            text_content, metadata = OCRProcessor.extract_text_from_pdf_smart(pdf_path=tmp_path)

            if text_content:
                logger.info(
                    f"    ✓ Extracted {len(text_content):,} chars "
                    f"(method={metadata.get('method', '?')})"
                )
                if regulation_id:
                    self.log(regulation_id, "pdf_extraction", "SUCCESS",
                             f"{len(text_content):,} chars, {metadata.get('method', '?')}")
                return text_content
            else:
                logger.warning("    ⚠ Empty text from PDF")
                return None

        except Exception as e:
            logger.warning(f"    ⚠ PDF download/extract failed: {e}")
            if regulation_id:
                self.log(regulation_id, "pdf_extraction", "ERROR", str(e))
            return None
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    # ================================================================== #
    #  REQUIREMENT MATCHING                                                #
    # ================================================================== #

    def _run_requirement_matching(
        self,
        regulation_id: int,
        analysis_result: dict,
        version_id: Optional[int] = None
    ):
        """
        Cross-reference extracted requirements against existing internal records.
        version_id is passed for CBB (links mappings to a content version),
        None for SAMA/SBP/SECP.
        """
        try:
            self.log(regulation_id, "requirement_matching", "STARTED",
                     f"Starting matching (version_id={version_id})")

            extracted_requirements = analysis_result.get("requirements", [])
            if not extracted_requirements:
                logger.warning(f"No requirements to match for regulation {regulation_id}")
                self.log(regulation_id, "requirement_matching", "SKIPPED",
                         "No extracted requirements")
                return

            existing_requirements  = self.repo.get_all_compliance_requirements()
            existing_controls      = self.repo.get_all_demo_controls()
            existing_kpis          = self.repo.get_all_demo_kpis()
            linked_controls_by_req = self.repo.get_linked_controls_by_requirement()
            linked_kpis_by_req     = self.repo.get_linked_kpis_by_requirement()

            match_results = self.requirement_matcher.match_requirements(
                regulation_id=regulation_id,
                extracted_requirements=extracted_requirements,
                existing_controls=existing_controls,
                existing_kpis=existing_kpis,
                existing_requirements=existing_requirements,
                linked_controls_by_req=linked_controls_by_req,
                linked_kpis_by_req=linked_kpis_by_req
            )

            requirement_mappings   = match_results["requirement_mappings"]
            control_links          = match_results["control_links"]
            kpi_links              = match_results["kpi_links"]
            new_controls_to_insert = match_results["new_controls_to_insert"]
            new_kpis_to_insert     = match_results["new_kpis_to_insert"]

            if requirement_mappings:
                self.repo.store_requirement_mappings(requirement_mappings, version_id=version_id)

            partially_matched_ids = [
                m["matched_requirement_id"]
                for m in requirement_mappings
                if m["match_status"] == "partially_matched"
                and m["matched_requirement_id"] is not None
            ]
            if partially_matched_ids:
                self.repo.flag_partially_matched_requirements(partially_matched_ids)

            new_req_mappings = [m for m in requirement_mappings if m["match_status"] == "new"]
            for i, mapping in enumerate(new_req_mappings):
                try:
                    req_text = mapping["extracted_requirement_text"]
                    title    = req_text[:100].strip() + ("..." if len(req_text) > 100 else "")
                    new_req_id = self.repo.insert_new_suggested_requirement({
                        "title":       title,
                        "description": req_text,
                        "ref_key":     f"AUTO-{regulation_id}-{i}",
                        "ref_no":      f"REG-{regulation_id}"
                    })
                    for ctrl in new_controls_to_insert:
                        if ctrl.get("_req_id") is None:
                            ctrl["_req_id"] = new_req_id
                    for kpi in new_kpis_to_insert:
                        if kpi.get("_req_id") is None:
                            kpi["_req_id"] = new_req_id
                except Exception as e:
                    logger.error(f"Failed to insert new suggested requirement: {e}")

            if control_links:
                self.repo.store_control_links(control_links)
            if kpi_links:
                self.repo.store_kpi_links(kpi_links)

            for ctrl in new_controls_to_insert:
                try:
                    new_ctrl_id = self.repo.insert_new_suggested_control({
                        "title": ctrl["title"], "description": ctrl["description"],
                        "control_key": ctrl["control_key"]
                    })
                    req_id = ctrl.get("_req_id")
                    if req_id:
                        self.repo.store_control_links([{
                            "compliancerequirement_id": req_id,
                            "control_id": new_ctrl_id,
                            "match_status": "new",
                            "match_explanation": ctrl.get("_explanation", ""),
                            "regulation_id": regulation_id
                        }])
                except Exception as e:
                    logger.error(f"Failed to insert new suggested control: {e}")

            for kpi in new_kpis_to_insert:
                try:
                    new_kpi_id = self.repo.insert_new_suggested_kpi({
                        "title": kpi["title"], "description": kpi["description"],
                        "kisetup_key": kpi["kisetup_key"], "formula": kpi.get("formula", "")
                    })
                    req_id = kpi.get("_req_id")
                    if req_id:
                        self.repo.store_kpi_links([{
                            "compliancerequirement_id": req_id,
                            "kisetup_id": new_kpi_id,
                            "match_status": "new",
                            "match_explanation": kpi.get("_explanation", ""),
                            "regulation_id": regulation_id
                        }])
                except Exception as e:
                    logger.error(f"Failed to insert new suggested KPI: {e}")

            fully   = sum(1 for m in requirement_mappings if m["match_status"] == "fully_matched")
            partial = sum(1 for m in requirement_mappings if m["match_status"] == "partially_matched")
            new_r   = sum(1 for m in requirement_mappings if m["match_status"] == "new")

            self.log(
                regulation_id, "requirement_matching", "SUCCESS",
                f"Reqs: {fully} fully / {partial} partial / {new_r} new | "
                f"Ctrl links: {len(control_links)} | KPI links: {len(kpi_links)} | "
                f"New controls: {len(new_controls_to_insert)} | New KPIs: {len(new_kpis_to_insert)}"
            )

        except Exception as e:
            logger.error(f"Requirement matching failed for regulation {regulation_id}: {e}")
            self.log(regulation_id, "requirement_matching", "ERROR", str(e))

    # ================================================================== #
    #  UNIFIED LLM ANALYSIS — ALL REGULATORS                               #
    #                                                                       #
    #  Single analysis method used by CBB, SAMA, SBP, SECP.               #
    #  The only difference is whether version_id is passed in.             #
    # ================================================================== #

    def _run_llm_analysis(
        self,
        regulation_id: int,
        doc,
        text_content: str,
        content_type: str,
        version_id: Optional[int] = None,
    ) -> bool:
        """
        Run the 4-stage LLM pipeline and store results in compliance_analysis.

        Works for ALL regulators:
          - version_id=None  → SAMA / SBP / SECP (no content versioning)
          - version_id=<int> → CBB (links analysis row to a regulation_versions snapshot)

        Returns True on success, False on failure.
        """
        try:
            self.log(regulation_id, "llm_analysis", "STARTED",
                     f"4-stage LLM (version_id={version_id}, "
                     f"content_type={content_type}, text_len={len(text_content):,})")

            clean_text = self.llm_analyzer.normalize_input_text(
                text_content, content_type=content_type
            )

            if len(clean_text) < MIN_TEXT_LEN:
                raise ValueError(
                    f"Text too short after normalization ({len(clean_text)} chars)"
                )

            rows = self.staged_analyzer.analyze(
                text=clean_text,
                regulation_id=regulation_id,
                document_title=getattr(doc, "title", "Untitled"),
            )

            if not rows:
                raise ValueError("4-stage analysis returned no requirements")

            # All regulators write to the same table.
            # version_id=None for SAMA/SBP, version_id=<int> for CBB.
            self.repo.store_analysis(rows, version_id=version_id)

            self.log(regulation_id, "llm_analysis", "SUCCESS",
                     f"4-stage analysis: {len(rows)} rows stored "
                     f"(version_id={version_id})")

            # Build requirement list for matching
            extracted_for_matcher = []
            for r in rows:
                s2 = r.get("stage2_json") or {}
                if isinstance(s2, str):
                    try:
                        s2 = json.loads(s2)
                    except Exception:
                        s2 = {}
                for ob in s2.get("normalized_obligations", []):
                    extracted_for_matcher.append({
                        "requirement_text": ob["obligation_text"],
                        "department": "",
                        "risk_level": ob.get("criticality", "Medium"),
                        "controls": [],
                        "kpis": [],
                        "_obligation_id": ob["obligation_id"],
                        "_requirement_id": r.get("requirement_id"),
                    })

            combined_for_matcher = {"requirements": extracted_for_matcher}
            # version_id flows through to sama_requirement_mapping
            self._run_requirement_matching(
                regulation_id, combined_for_matcher, version_id=version_id
            )
            return True

        except Exception as e:
            logger.error(f"LLM analysis failed for regulation {regulation_id}: {e}")
            self.log(regulation_id, "llm_analysis", "ERROR", str(e))
            return False

    # Keep the old names as aliases so any code that imported them still works
    def _run_llm_analysis_unified(self, regulation_id, doc, text_content,
                                   content_type) -> bool:
        return self._run_llm_analysis(
            regulation_id, doc, text_content, content_type, version_id=None
        )

    def _run_llm_analysis_versioned(self, regulation_id, doc, text_content,
                                     content_type, version_id) -> bool:
        return self._run_llm_analysis(
            regulation_id, doc, text_content, content_type, version_id=version_id
        )

    # ================================================================== #
    #  DOCUMENT FILTERING                                                   #
    # ================================================================== #

    def run_for_regulator(self, regulator_name: str):
        logger.warning(f"=== RUNNING REGULATOR: {regulator_name} ===")
        docs = self.crawler.fetch_documents()
        logger.warning(f"Scraped {len(docs)} documents from crawler")

        new_docs, existing_docs = self.filter_new_documents(docs)
        logger.warning(f"{len(new_docs)} new / {len(existing_docs)} existing")

        if not new_docs:
            logger.warning("No new documents to process. Exiting...")
            return

        for idx, doc in enumerate(new_docs, start=1):
            logger.info(f"Processing {idx}/{len(new_docs)}: {doc.title}")
            self._process_single_doc(idx, doc, regulator_name)
            gc.collect()

        logger.warning(f"Finished processing all {len(new_docs)} documents.")

    def run_for_cbb(self, mode: str = "auto"):
        from crawler.cbb_crawler import CBBCrawler
        from crawler.cbb_monitoring_crawler import CBBMonitoringCrawler

        logger.warning(f"=== CBB PIPELINE: mode={mode} ===")

        if mode == "auto":
            last_date = self.repo.get_last_cbb_crawl_date()
            mode = "monitoring" if last_date else "full"
            logger.warning(f"  Auto-detected mode: {mode} (last crawl: {last_date})")

        crawler = CBBCrawler(request_delay=1.5) if mode == "full" \
            else CBBMonitoringCrawler(repo=self.repo, request_delay=1.0)

        docs = crawler.fetch_documents()
        logger.warning(f"  Fetched {len(docs)} documents")

        new_docs, existing_docs = self.filter_new_documents(docs)
        modified_docs = [
            d for d in existing_docs
            if d.extra_meta.get("monitoring_status") == "modified"
        ]
        docs_to_process = new_docs + modified_docs

        logger.warning(
            f"  {len(new_docs)} new / {len(modified_docs)} modified / "
            f"{len(existing_docs) - len(modified_docs)} unchanged"
        )

        if not docs_to_process:
            logger.warning("  No documents to process.")
            return

        for idx, doc in enumerate(docs_to_process, start=1):
            logger.info(f"  [{idx}/{len(docs_to_process)}] {doc.title[:60]}")
            self._process_single_doc(idx, doc, "CBB")
            gc.collect()

        logger.warning(f"  CBB pipeline complete. Processed {len(docs_to_process)} documents.")

    def filter_new_documents(self, all_documents: List):
        new_docs, existing_docs = [], []

        for doc in all_documents:
            if getattr(doc, "regulator", "") == "Central Bank of Bahrain":
                source_url = getattr(doc, "source_page_url", None)
                if not source_url:
                    logger.warning(f"CBB doc has no source_page_url, skipping: {doc.title}")
                    continue
                exists = self.repo.document_exists_by_source_url(source_url)
                (existing_docs if exists else new_docs).append(doc)
                continue

            if doc.published_date:
                exists = self.check_exists_in_db(
                    doc.title, doc.published_date, getattr(doc, "doc_path", None)
                )
                (existing_docs if exists else new_docs).append(doc)
                continue

            if getattr(doc, "category", "").lower() == "regulatory returns":
                exists = self.check_exists_in_db(
                    doc.title, None, getattr(doc, "doc_path", None)
                )
                (existing_docs if exists else new_docs).append(doc)
                continue

            if getattr(doc, "source_system", "").upper() == "DPC-CIRCULAR":
                exists = self.check_exists_in_db(
                    doc.title, None, getattr(doc, "doc_path", None)
                )
                (existing_docs if exists else new_docs).append(doc)
                continue

            logger.warning(f"Skipping {doc.title} (missing published_date)")

        return new_docs, existing_docs

    def _get_or_create_compliance_category(self, hierarchy: list) -> int:
        parent_id = None
        for title in hierarchy:
            folder_id = self.repo.get_folder_id(title, parent_id)
            parent_id = folder_id if folder_id else self.repo.insert_folder(title, parent_id)
        return parent_id

    def check_exists_in_db(self, title, published_date, doc_path) -> bool:
        try:
            return self.repo.document_exists(title, published_date, doc_path)
        except Exception as e:
            logger.error(f"Failed to check document existence: {e}")
            return False

    # ================================================================== #
    #  SINGLE DOC PROCESSING — MAIN ENTRY                                  #
    # ================================================================== #

    def _process_single_doc(self, idx, doc, regulator_name):
        logger.info(f"[{idx}] Starting: {doc.title}")
        regulator_upper = regulator_name.upper()

        try:
            if hasattr(doc, "doc_path") and isinstance(doc.doc_path, list):
                doc.compliancecategory_id = self._get_or_create_compliance_category(doc.doc_path)
            else:
                doc.compliancecategory_id = None
        except Exception as e:
            logger.error(f"Failed to assign compliance category: {e}")
            doc.compliancecategory_id = None

        # Regulatory Returns: insert only, no LLM
        if getattr(doc, "category", "").lower() == "regulatory returns":
            try:
                import hashlib
                doc.title_hash = (
                    hashlib.md5((doc.title or "").encode("utf-8")).hexdigest()
                    if doc.title else None
                )
                regulation_id = self.repo._insert_regulation(doc)
                doc.id = regulation_id
                self.log(regulation_id, "insert", "SUCCESS",
                         "Regulatory Return inserted (no document)")
            except Exception as e:
                logger.error(f"Failed to insert Regulatory Return: {e}")
                self.log(None, "insert", "ERROR", str(e),
                         doc_url=getattr(doc, "document_url", None))
            return

        # CBB: versioned path
        if regulator_upper == "CBB":
            self._process_cbb_doc(doc)
            return

        # SAMA / SBP / SECP: simple insert → extract → analyze
        try:
            regulation_id = self.repo._insert_regulation(doc)
            doc.id = regulation_id
            self.log(regulation_id, "insert", "SUCCESS",
                     f"{regulator_name} document inserted")
        except Exception as e:
            logger.error(f"Failed to insert {regulator_name} document: {e}")
            self.log(None, "insert", "ERROR", str(e))
            return

        self._extract_and_analyze(doc, regulation_id)

    # ================================================================== #
    #  CBB-SPECIFIC: VERSIONED INSERT / UPDATE                             #
    # ================================================================== #

    def _process_cbb_doc(self, doc):
        """
        CBB versioning logic:

        NEW document:
          1. Insert regulation record
          2. Create regulation_versions snapshot (version_id=N)
          3. Store content hash on regulations row
          4. Run analysis → store in compliance_analysis with version_id=N

        MODIFIED document:
          1. Fetch old content from regulations
          2. Archive old content → regulation_versions (version_id=A)
          3. Archive old analysis → compliance_analysis_versions (status=inactive)
             AND delete from compliance_analysis
          4. Create new regulation_versions snapshot (version_id=B)
          5. Update regulations row with new content + hash
          6. Run analysis → store in compliance_analysis with version_id=B
        """
        extra_meta        = getattr(doc, "extra_meta", {}) or {}
        monitoring_status = extra_meta.get("monitoring_status", "new")
        existing_reg_id   = extra_meta.get("existing_regulation_id")
        content_hash      = extra_meta.get("content_hash", "")
        content_text      = extra_meta.get("content_text", "")
        document_html     = getattr(doc, "document_html", None)
        regulation_id     = None
        current_version_id = None

        # ── MODIFIED ─────────────────────────────────────────────────────
        if monitoring_status == "modified" and existing_reg_id:
            try:
                logger.info(
                    f"  Processing MODIFIED CBB document (reg_id={existing_reg_id})"
                )

                # Step 1: fetch old content
                existing = self.repo.get_regulation_by_id(existing_reg_id)
                if existing:
                    old_html = existing.get("document_html") or ""
                    old_meta = existing.get("extra_meta") or {}
                    old_text = old_meta.get("content_text") or ""
                    old_hash = existing.get("content_hash") or ""
                else:
                    logger.warning(
                        f"Could not fetch existing regulation {existing_reg_id}"
                    )
                    old_html = old_text = old_hash = ""

                # Step 2: CRITICAL - Mark ALL existing active versions as inactive
                with self.repo._get_conn() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        UPDATE regulation_versions 
                        SET status = 'inactive' 
                        WHERE regulation_id = ? 
                        AND status = 'active'
                        """,
                        (existing_reg_id,)
                    )
                    rows_updated = cursor.rowcount
                    conn.commit()
                    logger.info(f"  ✓ Marked {rows_updated} existing version(s) as inactive")

                # Step 3: archive old CONTENT → regulation_versions (as inactive)
                old_version_id = self.repo.insert_regulation_version(
                    regulation_id=existing_reg_id,
                    regulator="Central Bank of Bahrain",
                    content_html=old_html,
                    content_text=old_text,
                    content_hash=old_hash,
                    updated_date=date.today(),
                    change_summary=(
                        f"Previous version archived on {date.today().isoformat()}"
                    ),
                    status='inactive',  # ← Explicitly set as inactive
                )
                logger.info(f"  ✓ Archived old content as version {old_version_id}")

                # Step 4: archive old ANALYSIS → compliance_analysis_versions
                #         AND clear compliance_analysis
                archived = self.repo.archive_current_analysis(
                    existing_reg_id, old_version_id
                )
                logger.info(
                    f"  ✓ Archived {archived} analysis rows "
                    f"(version={old_version_id}, status=inactive)"
                )

                # Step 5: create NEW content version (active by default)
                current_version_id = self.repo.insert_regulation_version(
                    regulation_id=existing_reg_id,
                    regulator="Central Bank of Bahrain",
                    content_html=document_html,
                    content_text=content_text,
                    content_hash=content_hash,
                    updated_date=doc.published_date,
                    change_summary=f"Updated content on {date.today().isoformat()}",
                )
                logger.info(f"  ✓ Created new version {current_version_id}")

                # Step 6: update regulations row
                self.repo.update_cbb_content_hash(existing_reg_id, content_hash)
                self.repo.update_regulation(
                    existing_reg_id,
                    document_html=document_html,
                    published_date=doc.published_date,
                )

                self.log(
                    existing_reg_id, "cbb_version", "SUCCESS",
                    f"Versions: {old_version_id} (archived) → "
                    f"{current_version_id} (active)"
                )
                regulation_id = existing_reg_id

            except Exception as e:
                logger.error(f"Failed to version CBB doc {doc.title}: {e}")
                self.log(existing_reg_id, "cbb_version", "ERROR", str(e))
                return

        # ── NEW ──────────────────────────────────────────────────────────
        else:
            try:
                logger.info("  Processing NEW CBB document")

                regulation_id = self.repo._insert_regulation(doc)
                doc.id = regulation_id

                self.repo.update_cbb_content_hash(regulation_id, content_hash)

                current_version_id = self.repo.insert_regulation_version(
                    regulation_id=regulation_id,
                    regulator="Central Bank of Bahrain",
                    content_html=document_html,
                    content_text=content_text,
                    content_hash=content_hash,
                    updated_date=doc.published_date,
                    change_summary="Initial crawl",
                )
                logger.info(
                    f"  ✓ Created initial version {current_version_id} "
                    f"for regulation {regulation_id}"
                )
                self.log(
                    regulation_id, "insert", "SUCCESS",
                    f"CBB page inserted with initial version {current_version_id}"
                )

            except Exception as e:
                logger.error(f"Failed to insert CBB doc {doc.title}: {e}")
                self.log(None, "insert", "ERROR", str(e))
                return

        # Skip LLM for shallow/folder pages
        depth = extra_meta.get("depth", 0)
        if depth < 2:
            logger.info(
                f"  Skipping LLM for CBB page (depth={depth}) — folder/index page"
            )
            self.log(regulation_id, "llm_analysis", "SKIPPED",
                     f"depth={depth}, folder/index page")
            return

        # Run analysis with version_id
        logger.info(
            f"  → Running analysis for regulation {regulation_id}, "
            f"version {current_version_id}"
        )
        self._extract_and_analyze(doc, regulation_id, version_id=current_version_id)

    # ================================================================== #
    #  EXTRACT + ANALYZE — UNIFIED FOR ALL REGULATORS                      #
    # ================================================================== #

    def _extract_and_analyze(
        self,
        doc,
        regulation_id: int,
        version_id: Optional[int] = None,
    ):
        """
        Run content extraction then 4-stage LLM analysis.

        Works for ALL regulators. version_id is:
          - None        → SAMA / SBP / SECP (no content versioning)
          - <int>       → CBB (links compliance_analysis row to regulation_versions)
        """
        logger.info(
            f"  → Extraction (regulation_id={regulation_id}, version_id={version_id})"
        )

        text_content, content_type = self.extract_text_content_unified(
            doc, regulation_id=regulation_id
        )

        if not text_content or len(text_content) < MIN_TEXT_LEN:
            msg = f"Insufficient text: {len(text_content or '')} chars"
            logger.error(f"  ✗ {msg}")
            self.log(regulation_id, "validation", "ERROR", msg)
            return

        success = self._run_llm_analysis(
            regulation_id=regulation_id,
            doc=doc,
            text_content=text_content,
            content_type=content_type,
            version_id=version_id,
        )

        if success:
            logger.info(
                f"  ✓ Analysis complete "
                f"(regulation_id={regulation_id}, version_id={version_id})"
            )
        else:
            logger.warning(
                f"  ⚠ Analysis failed "
                f"(regulation_id={regulation_id}, version_id={version_id})"
            )

    # Keep old method names as aliases — they delegate to the unified method
    def _extract_and_analyze_versioned(self, doc, regulation_id: int, version_id: int):
        self._extract_and_analyze(doc, regulation_id, version_id=version_id)