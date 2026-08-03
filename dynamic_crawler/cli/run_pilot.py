"""CLI: run the config-driven engine against an approved config, write the
crawled documents + a validation report to disk. Read-only against the
regulator's site, writes only local JSON -- no DB, no live pipeline involved.

Usage:
    python -m dynamic_crawler.cli.run_pilot \\
        --config config/regulators/sama.finance_sector.yml \\
        --out output/dynamic_crawler/sama_finance_sector_new.json
"""

import argparse
import json
import logging
from dataclasses import asdict

from dynamic_crawler.config_loader import ConfigError, is_approved, load_config
from dynamic_crawler.engine import GenericSidebarTreeEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run(config_path: str, out_path: str, force: bool = False, limit: int = None) -> dict:
    cfg = load_config(config_path)

    if not is_approved(cfg) and not force:
        raise ConfigError(
            f"Config {config_path} is not marked metadata.approved: true. "
            f"Review it first, or pass --force to run it anyway for testing."
        )

    if cfg["discovery"]["strategy"] != "sidebar_tree":
        raise ConfigError(
            f"run_pilot.py currently only supports discovery.strategy=sidebar_tree, "
            f"got {cfg['discovery']['strategy']!r}"
        )

    engine = GenericSidebarTreeEngine(cfg)
    documents, report = engine.run(limit=limit)

    data = [asdict(doc) for doc in documents]
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved {len(documents)} documents to {out_path}")

    report_path = out_path.rsplit(".", 1)[0] + ".validation.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    logger.info(f"Validation report: {report_path} -- ok={report['ok']}")

    return report


def main():
    parser = argparse.ArgumentParser(description="Run the config-driven dynamic_crawler engine against an approved config.")
    parser.add_argument("--config", required=True, help="Path to the regulator/tab YAML config")
    parser.add_argument("--out", required=True, help="Output path for the crawled documents JSON")
    parser.add_argument("--force", action="store_true", help="Run even if metadata.approved is not true")
    parser.add_argument("--limit", type=int, default=None, help="Limit to first N top-level categories (for quick testing)")
    args = parser.parse_args()
    run(args.config, args.out, force=args.force, limit=args.limit)


if __name__ == "__main__":
    main()
