from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from datetime import datetime
import logging
from scheduler.celery_app import celery_app, update_schedule, load_schedules_from_db

from scheduler.scheduler import run_sbp_pipeline, run_secp_pipeline, run_sama_pipeline

logger = logging.getLogger(__name__)
app = FastAPI(title="Regulatory Pipeline API", version="2.0.0")

# Mapping regulator names to functions
REGULATOR_PIPELINES = {
    "SBP": run_sbp_pipeline,
    "SECP": run_secp_pipeline,
    "SAMA": run_sama_pipeline
}

# Load schedules from DB on startup
load_schedules_from_db()


# ---------- Background pipeline runner ----------
def run_regulator_pipeline(regulator: str):
    try:
        logger.info(f"Starting {regulator} pipeline")
        pipeline_func = REGULATOR_PIPELINES.get(regulator)
        if pipeline_func:
            pipeline_func()
        logger.info(f"{regulator} pipeline completed")
    except Exception as e:
        logger.error(f"Error in {regulator} pipeline: {e}", exc_info=True)


# ---------- API Models ----------
class ScheduleUpdate(BaseModel):
    regulator: str
    hour: int
    minute: int


# ---------- API Endpoints ----------
@app.post("/update-schedule")
def update_pipeline_schedule(payload: ScheduleUpdate):
    if payload.regulator not in REGULATOR_PIPELINES:
        raise HTTPException(status_code=400, detail="Unknown regulator")

    update_schedule(payload.regulator, payload.hour, payload.minute)
    return {
        "status": "success",
        "regulator": payload.regulator,
        "hour": payload.hour,
        "minute": payload.minute,
        "message": f"{payload.regulator} schedule updated successfully"
    }


@app.post("/trigger/{regulator}")
def trigger_regulator_pipeline(regulator: str, background_tasks: BackgroundTasks):
    if regulator not in REGULATOR_PIPELINES:
        raise HTTPException(status_code=400, detail="Unknown regulator")
    background_tasks.add_task(run_regulator_pipeline, regulator)
    return {"status": "triggered", "regulator": regulator, "triggered_at": datetime.utcnow().isoformat()}


@app.post("/trigger/full")
def trigger_full_pipeline(background_tasks: BackgroundTasks):
    for regulator in REGULATOR_PIPELINES:
        background_tasks.add_task(run_regulator_pipeline, regulator)
    return {"status": "triggered", "pipeline": "full", "triggered_at": datetime.utcnow().isoformat()}


@app.get("/status")
def get_status():
    return {
        "status": "operational",
        "available_regulators": list(REGULATOR_PIPELINES.keys()),
        "timestamp": datetime.utcnow().isoformat()
    }
