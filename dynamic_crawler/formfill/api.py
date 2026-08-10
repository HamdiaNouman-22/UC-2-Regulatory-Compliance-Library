"""A separate API for the new orchestrator, writing to Excel instead of MSSQL.

Deliberately NOT added to `apis/pipeline_api.py`: that one is wired to production
MSSQL and to the existing per-regulator pipelines. This is its own app on its own
port so a run here cannot touch the database.

START IT

    venv/Scripts/python.exe -m uvicorn dynamic_crawler.formfill.api:app --port 8100

TRIGGER IT

    Browser:  http://127.0.0.1:8100/docs          (Swagger — click Execute)
              http://127.0.0.1:8100/forms         (what is available)

    curl:     curl -X POST "http://127.0.0.1:8100/trigger/sama.circulars?limit=5"

WHAT A RUN DOES

    crawl (or reuse the last crawl)  ->  RegulatoryDocument
      -> classify: new / modified / unchanged / disappeared
      -> completeness gate
      -> folder tree + regulations + regulation_versions rows
      -> the text decision (gate, then html vs file, both when they differ)
      -> optional LLM analysis
      -> one .xlsx with a sheet per table

    Nothing is written to MSSQL. Run it twice against the same workbook and the
    second run reports `unchanged` for everything — that is the change detection
    working.

TWO SAFETY DEFAULTS

    reuse_last = true   Uses the crawl output already on disk. Set false to
                        re-crawl (SAMA circulars takes ~45 minutes).
    analyse    = false  No LLM calls, no spend. Set true and each document costs
                        roughly $0.007 and ~4 minutes.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import yaml
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("formfill.api")

REPO_ROOT = Path(__file__).resolve().parents[2]
HINTS_DIR = REPO_ROOT / "dynamic_crawler" / "hints"
OUT_DIR = REPO_ROOT / "output" / "formfill" / "_orch_runs"

app = FastAPI(
    title="Formfill orchestrator (Excel-backed)",
    description=__doc__,
    version="1.0",
)

_runs: Dict[str, dict] = {}
_lock = threading.Lock()


def _forms() -> Dict[str, dict]:
    out = {}
    for f in sorted(HINTS_DIR.glob("*.yml")):
        h = yaml.safe_load(f.read_text(encoding="utf-8")) or {}
        lib = h.get("library") or {}
        meta = h.get("meta") or {}
        out[h.get("name", f.stem)] = {
            "path": str(f.relative_to(REPO_ROOT)),
            "regulator": lib.get("regulator"),
            "source_system": lib.get("source_system"),
            "shape": h.get("shape"),
            "approved": bool(meta.get("approved")),
            "fetch_details": h.get("fetch_details", True),
        }
    return out


@app.get("/", tags=["info"])
def index():
    return {
        "app": "formfill orchestrator, Excel-backed",
        "docs": "/docs",
        "forms": "/forms",
        "trigger": "POST /trigger/{form}?limit=5&analyse=false&reuse_last=true",
        "runs": "/runs",
        "database": "NONE — output is an .xlsx per run",
    }


@app.get("/forms", tags=["info"])
def list_forms():
    return _forms()


@app.get("/runs", tags=["info"])
def list_runs():
    return _runs


@app.get("/runs/{run_id}/excel", tags=["info"])
def download_excel(run_id: str):
    r = _runs.get(run_id)
    if not r or not r.get("excel"):
        raise HTTPException(404, "no such run, or it produced no workbook")
    return FileResponse(r["excel"], filename=Path(r["excel"]).name)


@app.post("/trigger/{form}", tags=["run"])
def trigger(form: str,
            limit: Optional[int] = 5,
            analyse: bool = False,
            reuse_last: bool = True,
            workbook: Optional[str] = None):
    """Run the new orchestrator for one form.

    - **limit** — documents to process. `0` or omit for all. Start small.
    - **analyse** — run the 4-stage LLM analysis. Costs ~$0.007 and ~4 min each.
    - **reuse_last** — use the crawl already on disk instead of re-crawling.
    - **workbook** — append to an existing workbook to see change detection on a
      second run. Omit for a fresh one.
    """
    forms = _forms()
    if form not in forms:
        raise HTTPException(400, f"unknown form. available: {sorted(forms)}")
    info = forms[form]
    if not info["regulator"] or not info["source_system"]:
        raise HTTPException(400, f"{form} has no `library:` block — cannot name "
                                 "the regulator or source_system")

    from dynamic_crawler.formfill.excel_repo import ExcelRepo
    from dynamic_crawler.formfill.orch import NewOrchestrator
    from dynamic_crawler.formfill.pipeline import FormfillCrawler

    started = datetime.now()
    run_id = f"{form}-{started:%Y%m%d-%H%M%S}"
    out_xlsx = OUT_DIR / (workbook or f"{run_id}.xlsx")

    with _lock:
        crawl_dir = REPO_ROOT / "output" / "formfill" / form
        reuse_from = None
        if reuse_last:
            # newest pages.json under this form's output folder
            cands = sorted(crawl_dir.glob("*/pages.json"),
                           key=lambda p: p.stat().st_mtime, reverse=True)
            if not cands:
                raise HTTPException(400, f"reuse_last=true but no crawl found under "
                                         f"{crawl_dir}. Run the crawler first or pass "
                                         f"reuse_last=false.")
            reuse_from = cands[0]

        repo = ExcelRepo(out_xlsx)
        if workbook and out_xlsx.exists():
            _load_workbook_into(repo, out_xlsx)

        crawler = FormfillCrawler(
            str(REPO_ROOT / info["path"]),
            regulator=info["regulator"],
            source_system=info["source_system"],
            require_approved=False,          # a preview run may use a stale form
            out_dir=str(crawl_dir / "api_run") if not reuse_last else None,
        )
        if reuse_from:
            crawler._run_crawl = lambda p=reuse_from: json.loads(
                p.read_text(encoding="utf-8"))
            logger.info("reusing crawl %s", reuse_from)

        orch = NewOrchestrator(
            crawler=crawler, repo=repo, downloader=None,
            source_name=form, analyse=analyse,
            limit=(limit or None),
        )

        try:
            report = orch.run_for_regulator(info["regulator"])
        except Exception as e:
            logger.exception("run failed")
            raise HTTPException(500, f"run failed: {e}")

        excel = repo.save()

    report.update({
        "run_id": run_id,
        "form": form,
        "crawl_reused": str(reuse_from) if reuse_from else None,
        "seconds": round((datetime.now() - started).total_seconds(), 1),
        "excel": str(excel),
        "excel_download": f"/runs/{run_id}/excel",
    })
    _runs[run_id] = report
    return report


def _load_workbook_into(repo, path: Path) -> None:
    """Read an existing workbook back so a second run can see the first one.
    Without this, every run starts empty and everything is always 'new'."""
    import pandas as pd
    for sheet in list(repo.t):
        try:
            df = pd.read_excel(path, sheet_name=sheet)
        except Exception:
            continue
        rows = [r for r in df.where(df.notna(), None).to_dict("records")
                if any(v not in (None, "") for v in r.values())]
        repo.t[sheet] = rows
        idcol = {"regulations": "id", "compliancecategory": "compliancecategory_id",
                 "regulation_versions": "version_id",
                 "compliance_analysis": "analysis_id",
                 "requirement_mappings": "mapping_id",
                 "processing_log": "log_id", "run_history": "run_id"}[sheet]
        nums = [int(r[idcol]) for r in rows if str(r.get(idcol, "")).isdigit()]
        repo._next[sheet] = max(nums) if nums else 0
    logger.info("loaded existing workbook: %s", repo.counts())
