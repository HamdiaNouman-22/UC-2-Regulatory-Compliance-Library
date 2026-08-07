"""Loads and validates a dynamic_crawler regulator/tab config (config/regulators/*.yml).

The config is the "recipe card" — everything a crawl needs to know about one
regulator tab (seed URL, how to fetch, how to discover documents, how to pull
fields out of a page). See config/regulators/sama.finance_sector.yml for a
worked example.
"""

from pathlib import Path
from typing import Any, Dict

import yaml

REQUIRED_TOP_KEYS = [
    "regulator", "tab_name", "source_system", "base_url", "seed_url",
    "fetch", "discovery", "extraction", "validation",
]

SUPPORTED_DISCOVERY_STRATEGIES = ("sidebar_tree", "table_grid")

# Strategies declared in the schema but not yet implemented by the engine.
# Kept explicit so a config author gets a clear error instead of a silent no-op.
NOT_YET_IMPLEMENTED_STRATEGIES = ("table_grid",)


class ConfigError(ValueError):
    pass


def load_config(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"Config file not found: {path}")
    with open(p, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        raise ConfigError(f"Config file did not parse to a mapping: {path}")
    validate_config(cfg)
    return cfg


def validate_config(cfg: Dict[str, Any]) -> None:
    missing = [k for k in REQUIRED_TOP_KEYS if k not in cfg]
    if missing:
        raise ConfigError(f"Config missing required top-level keys: {missing}")

    discovery = cfg["discovery"]
    strategy = discovery.get("strategy")
    if strategy not in SUPPORTED_DISCOVERY_STRATEGIES:
        raise ConfigError(
            f"Unknown discovery.strategy {strategy!r}; must be one of {SUPPORTED_DISCOVERY_STRATEGIES}"
        )
    if strategy in NOT_YET_IMPLEMENTED_STRATEGIES:
        raise ConfigError(
            f"discovery.strategy={strategy!r} is reserved for a future phase and has no engine "
            f"implementation yet (Phase 1 pilot covers sidebar_tree only)."
        )
    if strategy == "sidebar_tree":
        for key in ("top_level", "sidebar", "body_links"):
            if key not in discovery:
                raise ConfigError(f"discovery.{key} is required for discovery.strategy=sidebar_tree")
        for key in ("nav_id_template", "category_id_regex", "folder_li_classes"):
            if key not in discovery["sidebar"]:
                raise ConfigError(f"discovery.sidebar.{key} is required")

    extraction = cfg["extraction"]
    if "structured_indicator_selector" not in extraction:
        raise ConfigError("extraction.structured_indicator_selector is required")
    if "fields" not in extraction or not isinstance(extraction["fields"], dict):
        raise ConfigError("extraction.fields must be a mapping of field_name -> extraction rule")
    for field_name, field_cfg in extraction["fields"].items():
        if "source" not in field_cfg:
            raise ConfigError(f"extraction.fields.{field_name} is missing 'source'")

    validation = cfg["validation"]
    for key in ("required_fields", "expected_doc_count_min", "expected_doc_count_max"):
        if key not in validation:
            raise ConfigError(f"validation.{key} is required")


def is_approved(cfg: Dict[str, Any]) -> bool:
    return bool(cfg.get("metadata", {}).get("approved", False))
