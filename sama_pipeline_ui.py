"""
SAMA Circulars -- Crawl & Pipeline Preview (Streamlit)

Two phases, in order:
  1. Crawl: opens a VISIBLE Chrome window (headful) and crawls
     https://rulebook.sama.gov.sa/en/sama-circulars, comparing against the
     regulations DB the same way run_sama_circulars_headless.py does --
     only new circulars / ones whose issue date changed come back.
  2. Pipeline preview: on request, runs each of those circulars through the
     same extraction -> LLM analysis -> requirement matching logic the
     production orchestrator uses, WITHOUT writing anything to the DB --
     existing requirements/controls/KPIs are only read (for matching), never
     inserted or updated, and no regulation/analysis rows are created. Results
     are shown live in the UI and exported to an Excel workbook instead.

Phase 2 still calls the LLM API per circular (has a real cost/time), it just
doesn't persist anything to the database.

Run:
    streamlit run sama_pipeline_ui.py
"""
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from crawler.sama_circulars_crawler import SAMARulebookCrawler  # noqa: E402
from storage.mssql_repo import MSSQLRepository  # noqa: E402
from processor.downloader import Downloader  # noqa: E402
from orchestrator.orchestrator import Orchestrator, MIN_TEXT_LEN  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

load_dotenv()

REGULATOR = "SAMA"
CATEGORY = "SAMA Circulars"
OUTDIR = PROJECT_ROOT / "output" / "standalone_crawler" / "sama_circulars_only"
STATUS_ICON = {"SUCCESS": "✅", "ERROR": "❌", "PENDING": "⏳"}

st.set_page_config(page_title="SAMA Circulars Pipeline", layout="wide")
st.title("SAMA Circulars -- Crawl & Pipeline Preview")

if "crawled_docs" not in st.session_state:
    st.session_state.crawled_docs = None


def get_repo() -> MSSQLRepository:
    conn_params = {
        "server": os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver": os.getenv("MSSQL_DRIVER", "{ODBC Driver 17 for SQL Server}"),
    }
    return MSSQLRepository(conn_params)


def get_known_circulars(repo: MSSQLRepository) -> dict:
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT reference_no, published_date FROM regulations "
                "WHERE regulator = ? AND category = ? AND reference_no IS NOT NULL",
                [REGULATOR, CATEGORY],
            )
            return {row[0]: row[1] for row in cursor.fetchall()}
    except Exception as e:
        st.warning(f"Could not read DB baseline ({e}). Treating every circular as new.")
        return {}


def run_pipeline_dry_run(doc, repo: MSSQLRepository, orchestrator: Orchestrator):
    """Extract + LLM-analyze + match one circular WITHOUT writing to the DB.

    Returns (summary_row, requirement_rows, match_rows) for the Excel export.
    Existing requirements/controls/KPIs are read (needed to classify matches
    as fully/partially/new) but nothing is inserted or updated.
    """
    row = {
        "Circular No": doc.reference_no,
        "Title": doc.title,
        "Issue Date": doc.published_date,
        "Extraction": "PENDING",
        "Extracted Chars": 0,
        "LLM Analysis": "PENDING",
        "Requirements Extracted": 0,
        "Matching": "PENDING",
        "Fully Matched": 0,
        "Partially Matched": 0,
        "New Requirements": 0,
        "Error": "",
    }
    requirement_rows, match_rows = [], []

    try:
        text_content, content_type = orchestrator.extract_text_content_unified(doc, regulation_id=None)
        if not text_content or len(text_content) < MIN_TEXT_LEN:
            row["Extraction"] = "ERROR"
            row["Error"] = f"Insufficient text ({len(text_content or '')} chars)"
            return row, requirement_rows, match_rows
        row["Extraction"] = "SUCCESS"
        row["Extracted Chars"] = len(text_content)
    except Exception as e:
        row["Extraction"] = "ERROR"
        row["Error"] = str(e)
        return row, requirement_rows, match_rows

    try:
        clean_text = orchestrator.llm_analyzer.normalize_input_text(text_content, content_type=content_type)
        analysis_rows = orchestrator.staged_analyzer.analyze(
            text=clean_text, regulation_id=0, document_title=doc.title
        )
        if not analysis_rows:
            row["LLM Analysis"] = "ERROR"
            row["Error"] = "4-stage analysis returned no requirements"
            return row, requirement_rows, match_rows
        row["LLM Analysis"] = "SUCCESS"
        row["Requirements Extracted"] = len(analysis_rows)
        for r in analysis_rows:
            requirement_rows.append({
                "Circular No": doc.reference_no,
                "Requirement ID": r.get("requirement_id"),
                "Requirement Title": r.get("requirement_title"),
                "Execution Category": r.get("execution_category"),
                "Criticality": r.get("criticality"),
                "Obligation Type": r.get("obligation_type"),
            })
    except Exception as e:
        row["LLM Analysis"] = "ERROR"
        row["Error"] = str(e)
        return row, requirement_rows, match_rows

    try:
        extracted_for_matcher = []
        for r in analysis_rows:
            s2 = r.get("stage2_json") or {}
            if isinstance(s2, str):
                s2 = json.loads(s2)
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

        match_results = orchestrator.requirement_matcher.match_requirements(
            regulation_id=0,
            extracted_requirements=extracted_for_matcher,
            existing_requirements=repo.get_all_compliance_requirements(),
            existing_controls=repo.get_all_demo_controls(),
            existing_kpis=repo.get_all_demo_kpis(),
            linked_controls_by_req=repo.get_linked_controls_by_requirement(),
            linked_kpis_by_req=repo.get_linked_kpis_by_requirement(),
        )
        mappings = match_results["requirement_mappings"]
        row["Matching"] = "SUCCESS"
        row["Fully Matched"] = sum(1 for m in mappings if m["match_status"] == "fully_matched")
        row["Partially Matched"] = sum(1 for m in mappings if m["match_status"] == "partially_matched")
        row["New Requirements"] = sum(1 for m in mappings if m["match_status"] == "new")
        for m in mappings:
            match_rows.append({
                "Circular No": doc.reference_no,
                "Extracted Requirement": m["extracted_requirement_text"],
                "Match Status": m["match_status"],
                "Matched Requirement ID": m.get("matched_requirement_id"),
                "Explanation": m.get("match_explanation"),
            })
    except Exception as e:
        row["Matching"] = "ERROR"
        row["Error"] = str(e)

    return row, requirement_rows, match_rows


# ---- Sidebar: crawl settings ----
st.sidebar.header("Crawl settings")
delay = st.sidebar.slider("Delay between requests (s)", 0.5, 5.0, 1.0, 0.5)
limit = st.sidebar.number_input("Limit (0 = all)", min_value=0, value=0, step=1)

st.sidebar.divider()
run_pipeline_after = st.sidebar.checkbox(
    "Also run a pipeline preview (extraction + LLM analysis + requirement matching) on new/changed circulars",
    value=False,
    help="Calls the LLM API per circular (real cost/time). No DB writes -- results go to Excel only.",
)
confirmed = True
if run_pipeline_after:
    confirmed = st.sidebar.checkbox("I understand this calls the LLM API for each circular", value=False)

if st.sidebar.button("Start crawl", type="primary"):
    repo = get_repo()
    known = get_known_circulars(repo)
    st.info(f"DB baseline: {len(known)} known SAMA circulars.")
    st.warning("A visible Chrome window will open for the crawl -- watch it there. This page updates once it's done.")

    crawler = SAMARulebookCrawler(headless=False, request_delay=delay)
    with st.spinner("Crawling rulebook.sama.gov.sa (see the Chrome window)..."):
        docs = crawler.fetch_documents(limit=(limit or None), known_documents=known)

    st.session_state.crawled_docs = docs
    st.success(f"Crawl complete: {len(docs)} new/changed circulars found.")

docs = st.session_state.crawled_docs

if docs is None:
    st.info("Click **Start crawl** in the sidebar to begin.")
else:
    st.subheader(f"New / changed circulars ({len(docs)})")

    if not docs:
        st.info("Nothing new -- DB is already up to date.")
    else:
        st.dataframe(
            [{
                "Circular No": d.reference_no,
                "Title": d.title,
                "Issue Date": d.published_date,
                "Status": (d.extra_meta or {}).get("status"),
            } for d in docs],
            use_container_width=True,
        )

        if run_pipeline_after and not confirmed:
            st.sidebar.error("Check the confirmation box to enable the pipeline preview.")

        if run_pipeline_after and confirmed and st.button("Run pipeline preview (no DB writes)"):
            repo = get_repo()
            orchestrator = Orchestrator(crawler=None, repo=repo, downloader=Downloader())
            progress = st.progress(0.0)
            summary_rows, all_requirement_rows, all_match_rows = [], [], []

            for i, doc in enumerate(docs, start=1):
                st.markdown(f"**[{i}/{len(docs)}] {doc.title[:90]}**")
                step_area = st.empty()

                row, req_rows, match_rows = run_pipeline_dry_run(doc, repo, orchestrator)
                summary_rows.append(row)
                all_requirement_rows.extend(req_rows)
                all_match_rows.extend(match_rows)

                with step_area.container():
                    st.write(f"{STATUS_ICON.get(row['Extraction'], '•')} Extraction -- "
                             f"{row['Extraction']} ({row['Extracted Chars']} chars)")
                    st.write(f"{STATUS_ICON.get(row['LLM Analysis'], '•')} LLM Analysis -- "
                             f"{row['LLM Analysis']} ({row['Requirements Extracted']} requirements)")
                    st.write(f"{STATUS_ICON.get(row['Matching'], '•')} Requirement Matching -- "
                             f"{row['Matching']} ({row['Fully Matched']} fully / "
                             f"{row['Partially Matched']} partial / {row['New Requirements']} new)")
                    if row["Error"]:
                        st.error(row["Error"])

                progress.progress(i / len(docs))

            OUTDIR.mkdir(parents=True, exist_ok=True)
            xlsx_path = OUTDIR / f"pipeline_preview_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
            for attempt in range(3):
                try:
                    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as xw:
                        pd.DataFrame(summary_rows).to_excel(xw, sheet_name="summary", index=False)
                        pd.DataFrame(all_requirement_rows).to_excel(xw, sheet_name="requirements", index=False)
                        pd.DataFrame(all_match_rows).to_excel(xw, sheet_name="requirement_matches", index=False)
                    break
                except PermissionError:
                    xlsx_path = OUTDIR / f"pipeline_preview_{datetime.now():%Y%m%d_%H%M%S}_{attempt + 1}.xlsx"

            st.success(f"Pipeline preview complete -- no DB writes made. Saved to {xlsx_path}")
            with open(xlsx_path, "rb") as f:
                st.download_button("Download Excel", f, file_name=xlsx_path.name)
