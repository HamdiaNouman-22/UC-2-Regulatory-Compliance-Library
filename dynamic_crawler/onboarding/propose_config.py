"""CLI: propose (or refine) a dynamic_crawler config by having an LLM inspect a
regulator tab's real HTML. Never writes directly to an approved config filename
-- always writes "*.proposed.yml" for a human to review, hand-correct via
plain-English feedback (--refine), and only then approve.

Usage:
    python -m dynamic_crawler.onboarding.propose_config \\
        --seed-url https://rulebook.sama.gov.sa/en/book-category/1365 \\
        --tab-name "Finance Sector" \\
        --out config/regulators/sama.finance_sector.proposed.yml

    python -m dynamic_crawler.onboarding.propose_config \\
        --refine config/regulators/sama.finance_sector.proposed.yml \\
        --feedback "the date it grabbed is wrong, use the issue date box instead" \\
        --out config/regulators/sama.finance_sector.proposed.yml
"""

import argparse
import json
import logging
import os

import yaml

from dynamic_crawler.onboarding.llm_client import call_llm, extract_json_from_llm_response
from dynamic_crawler.onboarding.site_inspector import inspect_site

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCHEMA_GUIDE = """
You are proposing a crawl CONFIG (not crawling anything yourself) for a regulator
website tab, in this exact JSON schema (it gets saved as YAML):

{
  "regulator": "<short regulator code, e.g. SAMA>",
  "tab_name": "<human name of the section/tab, e.g. Finance Sector>",
  "source_system": "<source system label, e.g. SAMA RULEBOOK>",
  "base_url": "<scheme://host of the site>",
  "seed_url": "<the tab's landing page URL>",
  "fetch": {
    "backend": "requests" | "selenium",
    "timeout_seconds": 30,
    "max_retries": 3,
    "retry_backoff_seconds": 2,
    "request_delay_seconds": 1.2,
    "headless": true
  },
  "discovery": {
    "strategy": "sidebar_tree",
    "top_level": {"selector": "<CSS selector for landing-page category cards>", "link_selector": "a[href]", "href_prefix_filter": "/en/"},
    "sidebar": {"nav_id_template": "<nav id pattern with {category_id} placeholder>", "category_id_regex": "<regex to pull the category id out of seed_url>", "folder_li_classes": ["<css class(es) marking a folder li>"]},
    "body_links": {"container_selector": "<CSS selector for the content area holding in-body links>", "href_prefix_filters": ["/en/", "<base_url>"], "generic_link_text_pattern": "^(click here|here|view|download|read more|more)\\\\.?$"}
  },
  "extraction": {
    "structured_indicator_selector": "<CSS selector present only on a 'structured document' leaf page>",
    "page_title": {"strip_suffix_regex": "<regex to strip a site-name suffix off <title>, if any>"},
    "plain_content_container_selector": "<CSS selector for the main content area on a plain content page>",
    "fields": {
      "<field_name>": {
        "source": "regex" | "css_text" | "css_attr" | "html_container",
        "...": "operation-specific keys -- regex: pattern + container_selector; css_text: selector + strip_prefix; css_attr: selector + attr + absolutize; html_container: selector + strip_selectors + absolutize_links",
        "target": "<RegulatoryDocument field name, or extra_meta.<key> for metadata fields>"
      }
    }
  },
  "validation": {
    "required_fields": ["regulator", "source_system", "category", "title", "document_url", "source_page_url"],
    "expected_doc_count_min": <int>,
    "expected_doc_count_max": <int>
  },
  "metadata": {"approved": false, "approved_by": null, "approved_date": null, "notes": "proposed by onboarding agent, needs human review"}
}

Base every selector/regex/pattern on the ACTUAL HTML you are given below -- do not
guess generic values that aren't visible in the samples. Output ONLY the JSON
object, no markdown fences, no commentary.
"""


def _render_samples(samples: dict) -> str:
    parts = []
    for name, sample in samples.items():
        parts.append(f"\n--- {name} ({sample['url']}) ---\n{sample['cleaned_html']}\n")
    return "\n".join(parts)


def _build_initial_prompt(seed_url: str, tab_name: str, samples: dict) -> str:
    return "\n".join([
        SCHEMA_GUIDE,
        f"\nseed_url: {seed_url}\ntab_name: {tab_name}\n\nHTML SAMPLES:\n",
        _render_samples(samples),
    ])


def _build_refine_prompt(previous_config: dict, feedback: str, samples: dict) -> str:
    parts = [
        SCHEMA_GUIDE,
        "\nHere is the PREVIOUSLY PROPOSED config:\n",
        json.dumps(previous_config, indent=2),
        f"\n\nHuman feedback on what's wrong with it:\n{feedback}\n",
        "\nRevise the config to address this feedback. Keep everything else that "
        "wasn't flagged as wrong. Output ONLY the revised JSON object.\n",
    ]
    if samples:
        parts.append("\nHTML SAMPLES:\n" + _render_samples(samples))
    return "\n".join(parts)


def _write_yaml(config: dict, out_path: str) -> None:
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f, sort_keys=False, allow_unicode=True)


def propose(seed_url: str, tab_name: str, out_path: str, sample_dir: str) -> None:
    samples = inspect_site(seed_url, sample_dir)
    prompt = _build_initial_prompt(seed_url, tab_name, samples)
    response = call_llm(
        system_prompt=(
            "You are a web-scraping engineer proposing precise CSS selectors and regex "
            "patterns from real HTML. Never invent selectors you haven't seen in the "
            "provided HTML."
        ),
        user_prompt=prompt,
    )
    config = extract_json_from_llm_response(response)
    _write_yaml(config, out_path)
    logger.info(f"Proposed config written to {out_path} -- review before approving.")


def refine(proposed_path: str, feedback: str, out_path: str, sample_dir: str, extra_sample_url: str = None) -> None:
    with open(proposed_path, "r", encoding="utf-8") as f:
        previous_config = yaml.safe_load(f)

    samples = {}
    if extra_sample_url:
        samples = inspect_site(previous_config.get("seed_url", ""), sample_dir, sample_urls=[extra_sample_url])

    prompt = _build_refine_prompt(previous_config, feedback, samples)
    response = call_llm(
        system_prompt=(
            "You are a web-scraping engineer revising a crawl config based on human "
            "feedback. Never invent selectors you haven't seen in the provided HTML or "
            "the previous config."
        ),
        user_prompt=prompt,
    )
    config = extract_json_from_llm_response(response)
    _write_yaml(config, out_path)
    logger.info(f"Refined config written to {out_path} -- review before approving.")


def main():
    parser = argparse.ArgumentParser(
        description="Propose or refine a dynamic_crawler config via LLM inspection of a regulator site."
    )
    parser.add_argument("--seed-url", help="Landing page URL of the tab to inspect (initial proposal mode)")
    parser.add_argument("--tab-name", help="Human name of the tab, e.g. 'Finance Sector' (initial proposal mode)")
    parser.add_argument("--out", required=True, help="Output path for the proposed YAML config")
    parser.add_argument("--sample-dir", default="dynamic_crawler/onboarding/samples", help="Where to save inspected raw HTML for audit")
    parser.add_argument("--refine", help="Path to a previously proposed config to revise")
    parser.add_argument("--feedback", help="Plain-English feedback describing what's wrong with --refine's config")
    parser.add_argument("--sample-url", help="Optional extra page URL to inspect when refining")
    args = parser.parse_args()

    if args.refine:
        if not args.feedback:
            parser.error("--refine requires --feedback")
        refine(args.refine, args.feedback, args.out, args.sample_dir, extra_sample_url=args.sample_url)
    else:
        if not args.seed_url or not args.tab_name:
            parser.error("initial proposal mode requires --seed-url and --tab-name")
        propose(args.seed_url, args.tab_name, args.out, args.sample_dir)


if __name__ == "__main__":
    main()
