"""
Trigger the CBB monitoring crawler right now (same logic as the scheduler job).

Optional args:
  --from YYYY-MM-DD   override start date (default: auto-detected from DB)
  --to   YYYY-MM-DD   override end date   (default: today)

Example backfill:
  python tests/run_monitoring_now.py --from 2026-05-28 --to 2026-07-06
"""
import os
import sys
import logging
import argparse
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("run_monitoring_now")

from storage.mssql_repo import MSSQLRepository
from orchestrator.orchestrator import Orchestrator
from processor.downloader import Downloader
from processor.html_fallback_engine import HTMLFallbackEngine

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="from_date", default=None,
                        help="Start date YYYY-MM-DD (default: auto from DB)")
    parser.add_argument("--to",   dest="to_date",   default=None,
                        help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--skip-analysis", dest="skip_analysis", action="store_true",
                        help="Store regulations and versions only, skip LLM analysis")
    args = parser.parse_args()

    log.warning("=== CBB MONITORING — MANUAL TRIGGER ===")
    if args.from_date:
        log.warning(f"  Date override: {args.from_date} to {args.to_date or 'today'}")

    repo = MSSQLRepository({
        "server":   os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver":   os.getenv("MSSQL_DRIVER"),
    })

    orch = Orchestrator(
        crawler=None,
        repo=repo,
        downloader=Downloader(),
        ocr_engine=HTMLFallbackEngine(),
    )
    orch.run_for_cbb(
        mode="monitoring",
        from_date=args.from_date,
        to_date=args.to_date,
        skip_analysis=args.skip_analysis,
    )

    log.warning("=== DONE ===")

if __name__ == "__main__":
    main()
