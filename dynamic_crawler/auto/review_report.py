"""Bundle the human-review artifacts for an autonomous onboarding run.

Produces, under output/dynamic_crawler/<regulator>/<model>/:
  - docs.json               (already written by onboard.py)
  - onboarding_report.json  (already written by onboard.py)
  - report.xlsx             hierarchy Excel (reuses sama_finance_sector_to_excel.build_excel)
  - a printed summary a reviewer can act on

Optionally scores docs.json against a baseline via dynamic_crawler/diff/compare.py
when one is available (an existing crawler's output).
"""

import argparse
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]


def build(work_dir: str, sector_label: str = None) -> None:
    d = Path(work_dir)
    docs_path = d / "docs.json"
    report_path = d / "onboarding_report.json"
    if not docs_path.exists():
        raise SystemExit(f"No docs.json in {work_dir}")

    documents = json.loads(docs_path.read_text(encoding="utf-8"))
    report = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else {}

    # Hierarchy Excel — reuse the existing SAMA report builder (schema-compatible,
    # it reads doc_path/extra_meta/etc. straight off RegulatoryDocument dicts).
    try:
        import sys
        if str(REPO_ROOT) not in sys.path:
            sys.path.insert(0, str(REPO_ROOT))
        from sama_finance_sector_to_excel import build_excel
        label = sector_label or (documents[0].get("category") if documents else "Documents")
        xlsx = d / "report.xlsx"
        build_excel(documents, xlsx, sector_label=label)
        logger.info(f"Hierarchy Excel: {xlsx}")
    except Exception as e:
        logger.warning(f"Could not build hierarchy Excel: {e}")

    # Reviewer summary.
    cc = report.get("crosscheck", {})
    val = report.get("validation", {})
    print("\n" + "=" * 68)
    print(f"ONBOARDING REVIEW — {d}")
    print("=" * 68)
    print(f"  documents captured : {report.get('doc_count', len(documents))}")
    print(f"  pages fetched      : {report.get('fetch_count', '?')}")
    print(f"  iterations used    : {report.get('iterations_used', '?')}")
    print(f"  seed URL           : {report.get('seed_url', '?')}")
    print(f"  cross-check        : pass={cc.get('pass')} field_hit_rate={cc.get('field_hit_rate')} "
          f"urls_ok={cc.get('all_document_urls_ok')}")
    if not cc.get("pass") and cc.get("reason"):
        print(f"     reason: {cc.get('reason')}")
    print(f"  validation         : ok={val.get('ok')} count_in_range={val.get('count_in_expected_range')}")
    print(f"  adapter            : {report.get('adapter_path', '?')}")
    print("\n  -> Review report.xlsx, then either approve, or give feedback via:")
    print("     python -m dynamic_crawler.auto.onboard --refine <REGULATOR> "
          "--feedback \"...\" [--sample-url URL]")
    print("=" * 68 + "\n")


def main():
    p = argparse.ArgumentParser(description="Build human-review artifacts for an onboarding run")
    p.add_argument("work_dir", help="output/dynamic_crawler/<regulator>/<model> dir")
    p.add_argument("--label", help="sector label for the Excel sheet")
    args = p.parse_args()
    build(args.work_dir, sector_label=args.label)


if __name__ == "__main__":
    main()
