import sys
import asyncio

# Force Windows to use SelectorEventLoop (required for Playwright / asyncio subprocesses)
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from twisted.internet import asyncioreactor

asyncioreactor.install(asyncio.new_event_loop())

# Setup Crochet to allow Scrapy to run in sync code
from crochet import setup

setup()

import time
import logging
import os
import yaml
import requests
import subprocess
import sys
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

from orchestrator.orchestrator import Orchestrator
from crawler.sbp_crawler_wrapper import SBPCrawler
from crawler.secp_crawler import SECPCrawler
from processor.downloader import Downloader
from processor.html_fallback_engine import HTMLFallbackEngine
from storage.mssql_repo import MSSQLRepository
import scrapy_runtime

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scheduler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

TIMEZONE = os.getenv("TIMEZONE", "Asia/Karachi")


def build_orchestrator(crawler):
    """Build orchestrator with database connection"""
    repo = MSSQLRepository({
        "server": os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver": os.getenv("MSSQL_DRIVER")
    })

    return Orchestrator(
        crawler=crawler,
        repo=repo,
        downloader=Downloader(),
        ocr_engine=HTMLFallbackEngine()
    )


# ==============================================================
# OPTION 1: DIRECT EXECUTION (Run pipeline directly in scheduler)
# ==============================================================

def run_sbp_pipeline():
    """Run SBP pipeline directly"""
    logger.info("=" * 60)
    logger.info("Starting SBP pipeline (DIRECT)")
    logger.info("=" * 60)
    try:
        orchestrator = build_orchestrator(SBPCrawler())
        orchestrator.run_for_regulator("SBP")
        logger.info("SBP pipeline completed successfully")
    except Exception as e:
        logger.error(f"SBP pipeline failed: {e}", exc_info=True)


def run_secp_pipeline():
    """Run SECP pipeline in isolated process"""
    logger.info("=" * 60)
    logger.info("Starting SECP pipeline (ISOLATED PROCESS)")
    logger.info("=" * 60)
    try:
        script_path = os.path.join(os.path.dirname(__file__), "..", "jobs", "secp_job.py")
        script_path = os.path.abspath(script_path)
        project_root = os.path.dirname(os.path.dirname(script_path))
        env = os.environ.copy()
        env["PYTHONPATH"] = project_root
        subprocess.run([sys.executable, script_path], check=True, env=env)
        logger.info("SECP pipeline completed successfully")
    except Exception as e:
        logger.error(f"SECP pipeline failed: {e}", exc_info=True)


def run_sama_pipeline():
    """Run SAMA pipeline in isolated process"""
    logger.info("=" * 60)
    logger.info("Starting SAMA pipeline (ISOLATED PROCESS)")
    logger.info("=" * 60)
    try:
        script_path = os.path.join(os.path.dirname(__file__), "..", "jobs", "sama_job.py")
        script_path = os.path.abspath(script_path)
        project_root = os.path.dirname(os.path.dirname(script_path))
        env = os.environ.copy()
        env["PYTHONPATH"] = project_root
        subprocess.run([sys.executable, script_path], check=True, env=env)
        logger.info("SAMA pipeline completed successfully")
    except Exception as e:
        logger.error(f"SAMA pipeline failed: {e}", exc_info=True)


# ==============================================================
# OPTION 2: API-BASED EXECUTION (Trigger via API)
# ==============================================================

def trigger_via_api(regulator: str):
    """
    Trigger a specific regulator pipeline via API.

    Args:
        regulator: Name of regulator (SBP, SECP, SAMA)
    """
    api_base_url = os.getenv("PIPELINE_API_URL", "http://localhost:8000")
    api_url = f"{api_base_url}/trigger/{regulator}"

    logger.info(f"Triggering {regulator} pipeline via API: {api_url}")

    try:
        response = requests.post(api_url, timeout=10)

        if response.status_code == 200:
            data = response.json()
            logger.info(f"{regulator} pipeline triggered successfully")
            logger.info(f"Response: {data}")
        else:
            logger.error(
                f"Failed to trigger {regulator} pipeline. "
                f"Status: {response.status_code}, Response: {response.text}"
            )
            raise RuntimeError(f"API returned status {response.status_code}")

    except requests.exceptions.ConnectionError:
        logger.error(f"Cannot connect to API at {api_url}. Is the API server running?")
        raise
    except Exception as e:
        logger.error(f"Error triggering {regulator} pipeline: {e}", exc_info=True)
        raise


def trigger_sbp_via_api():
    """Trigger SBP via API"""
    trigger_via_api("SBP")


def trigger_secp_via_api():
    """Trigger SECP via API"""
    trigger_via_api("SECP")


def trigger_sama_via_api():
    """Trigger SAMA via API"""
    trigger_via_api("SAMA")


def trigger_full_pipeline_via_api():
    """Trigger full pipeline (all regulators) via API"""
    api_base_url = os.getenv("PIPELINE_API_URL", "http://localhost:8000")
    api_url = f"{api_base_url}/trigger/full"

    logger.info(f"Triggering FULL pipeline via API: {api_url}")

    try:
        response = requests.post(api_url, timeout=10)

        if response.status_code == 200:
            data = response.json()
            logger.info("Full pipeline triggered successfully")
            logger.info(f"Response: {data}")
        else:
            logger.error(
                f"Failed to trigger full pipeline. "
                f"Status: {response.status_code}, Response: {response.text}"
            )
            raise RuntimeError(f"API returned status {response.status_code}")

    except Exception as e:
        logger.error(f"Error triggering full pipeline: {e}", exc_info=True)
        raise


# ==============================================================
# CONFIGURATION LOADER
# ==============================================================

def load_scheduler_config():
    """Load scheduler configuration from YAML"""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config", "scheduler.yml")

    logger.info(f"Loading scheduler config from: {config_path}")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# ==============================================================
# JOB MAPPING
# ==============================================================

# Map job names to functions
# Choose either DIRECT or API execution mode

# OPTION 1: Direct execution (runs in same process)
DIRECT_JOB_MAPPING = {
    "sbp_pipeline": run_sbp_pipeline,
    "secp_pipeline": run_secp_pipeline,
    "sama_pipeline": run_sama_pipeline,
}

# OPTION 2: API-based execution (calls API server)
API_JOB_MAPPING = {
    "sbp_pipeline": trigger_sbp_via_api,
    "secp_pipeline": trigger_secp_via_api,
    "sama_pipeline": trigger_sama_via_api,
    "full_pipeline": trigger_full_pipeline_via_api,
}

# Choose which mode to use (set via environment variable or hardcode)
EXECUTION_MODE = os.getenv("EXECUTION_MODE", "API")  # Options: "DIRECT" or "API"

if EXECUTION_MODE == "API":
    JOB_MAPPING = API_JOB_MAPPING
    logger.info("Scheduler configured for API-based execution")
else:
    JOB_MAPPING = DIRECT_JOB_MAPPING
    logger.info("Scheduler configured for direct execution")

# ==============================================================
# MAIN SCHEDULER
# ==============================================================

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("REGULATORY PIPELINE SCHEDULER STARTING")
    logger.info(f"Execution Mode: {EXECUTION_MODE}")
    logger.info(f"Timezone: {TIMEZONE}")
    logger.info("=" * 60)

    # Create scheduler
    scheduler = BackgroundScheduler(timezone=TIMEZONE)

    # Load configuration
    config = load_scheduler_config()
    jobs = config.get("jobs", {})

    # Add jobs to scheduler
    jobs_added = 0
    for job_name, job_cfg in jobs.items():
        if not job_cfg.get("enabled", False):
            logger.info(f"Skipping disabled job: {job_name}")
            continue

        # Get the job function
        job_func = JOB_MAPPING.get(job_name)
        if not job_func:
            logger.warning(f"No function mapped for job: {job_name}")
            continue

        trigger = job_cfg.get("trigger")
        schedule = job_cfg.get("schedule", {})

        # Add job to scheduler
        scheduler.add_job(
            job_func,
            trigger=trigger,
            id=f"{job_name}_job",
            name=job_name.upper(),
            max_instances=1,
            replace_existing=True,
            misfire_grace_time=6 * 60 * 60,  # 6 hours
            coalesce=False,
            **schedule
        )

        jobs_added += 1
        logger.info(f"✓ Loaded job: {job_name.upper()}")
        logger.info(f"  Trigger: {trigger}")
        logger.info(f"  Schedule: {schedule}")

    if jobs_added == 0:
        logger.warning("No jobs were added to the scheduler!")
        sys.exit(1)

    # Start scheduler
    scheduler.start()
    logger.info("=" * 60)
    logger.info(f"Scheduler started with {jobs_added} job(s)")
    logger.info("=" * 60)

    # Print scheduled jobs
    logger.info("\nScheduled Jobs:")
    for job in scheduler.get_jobs():
        next_run = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S") if job.next_run_time else "N/A"
        logger.info(f"  - {job.name}: Next run at {next_run}")

    logger.info("\nScheduler is running. Press Ctrl+C to stop.")

    # Keep running
    try:
        while True:
            time.sleep(10)
    except (KeyboardInterrupt, SystemExit):
        logger.info("\nShutting down scheduler...")
        scheduler.shutdown()
        logger.info("Scheduler stopped")

