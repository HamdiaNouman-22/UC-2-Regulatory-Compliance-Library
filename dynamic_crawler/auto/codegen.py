"""Turns inspected site HTML (+ any failure feedback) into a generated
RegulatorAdapter Python module, via the codebase's OpenRouter LLM client.

Three prompt paths:
  - build_initial_prompt      : first attempt from site samples
  - build_auto_refine_prompt  : retry after the agent's own cross-check/run failed
  - build_feedback_prompt     : retry after a human reviewer's plain-English note
All three ask for a single fenced ```python block; extract_python() pulls it out.
"""

import logging
import os
import re

from dynamic_crawler.onboarding.llm_client import call_llm

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "You are a senior web-scraping engineer. You write correct, defensive Python "
    "that extracts documents from a regulator website, based on the REAL HTML you "
    "are shown. Never invent selectors you haven't seen in the provided HTML. "
    "Output ONLY a single ```python code block, no prose."
)

ADAPTER_SPEC = '''
Write a Python module defining ONE subclass of RegulatorAdapter that crawls this
regulator section. The module MUST look like:

```python
import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from dynamic_crawler.auto.adapter_base import RegulatorAdapter
from dynamic_crawler.urlnorm import absolutize, canonical
from models.models import RegulatoryDocument


class GeneratedAdapter(RegulatorAdapter):
    REGULATOR = "{regulator}"
    SOURCE_SYSTEM = "{source_system}"
    BASE_URL = "{base_url}"
    SEED_URL = "{seed_url}"

    def crawl(self, limit=None):
        docs = []
        soup = self.fetcher.get(self.SEED_URL)   # BeautifulSoup or None
        # ... walk the site using ONLY self.fetcher.get(url) for network access ...
        # For each document found, append a RegulatoryDocument:
        #   doc = RegulatoryDocument(
        #       regulator=self.REGULATOR, source_system=self.SOURCE_SYSTEM,
        #       category=<section/category str>, title=<str>,
        #       document_url=<absolute url>, source_page_url=<page it was found on>,
        #       published_date=<str or None>, reference_no=<str or None>,
        #       year=<str or None>, file_type=<"PDF"/"HTML"/... or None>,
        #       extra_meta={{}},
        #   )
        #   doc.doc_path = [self.REGULATOR, self.SOURCE_SYSTEM, category, <hierarchy...>, title]
        #   docs.append(doc)
        return docs
```

HARD RULES:
- Fetch ONLY through self.fetcher.get(url). It returns a BeautifulSoup object (or
  None on failure) — always check for None. Do NOT import requests/urllib.request.
- Do NOT import os, sys, subprocess, socket, shutil; do NOT open files; do NOT use
  eval/exec. (These are blocked by the sandbox and will fail.)
- Make relative links absolute with absolutize(self.BASE_URL, href).
- Deduplicate pages you visit using canonical(url) in a set, to avoid loops.
- Base every selector/regex on the ACTUAL HTML shown below. Prefer robust
  selectors; guard every .find()/.select_one() against None.
- Do NOT write a CSS selector that STARTS with a combinator such as ">" (e.g.
  select('> li.menu-item')). soupsieve raises SelectorSyntaxError for a leading
  combinator. For direct children use element.find_all('li', recursive=False), or a
  full selector like element.select('ul.menu > li').
- Return a list of RegulatoryDocument. The class name must be GeneratedAdapter.

{shape_guidance}
'''


def _render_samples(samples: dict, cap: int = 45000, only=None) -> str:
    parts = []
    for name, s in samples.items():
        if only is not None and name not in only:
            continue
        html = s["cleaned_html"]
        if len(html) > cap:
            html = html[:cap] + "\n<!-- truncated -->"
        parts.append(f"\n--- {name} ({s['url']}) ---\n{html}\n")
    return "\n".join(parts)


def build_initial_prompt(regulator, source_system, base_url, seed_url, tab_name, samples,
                         shape_guidance="") -> str:
    spec = ADAPTER_SPEC.format(
        regulator=regulator, source_system=source_system, base_url=base_url, seed_url=seed_url,
        shape_guidance=shape_guidance,
    )
    return "\n".join([
        spec,
        f"\nRegulator: {regulator}    Section/tab: {tab_name}    Seed URL: {seed_url}\n",
        "HTML SAMPLES FROM THE LIVE SITE:\n",
        _render_samples(samples),
    ])


def build_auto_refine_prompt(previous_code, failure_summary, samples, shape_guidance="") -> str:
    return "\n".join([
        "The following generated adapter FAILED its automated test/cross-check. "
        "Fix it. Keep the same class name (GeneratedAdapter) and the RegulatorAdapter contract. "
        "Follow the shape-specific instructions below exactly.",
        ("\n=== HOW TO CRAWL THIS PAGE (shape rules) ===\n" + shape_guidance) if shape_guidance else "",
        "\n=== PREVIOUS ADAPTER ===\n```python\n" + previous_code + "\n```",
        "\n=== WHAT WENT WRONG (fix these specifically) ===\n" + failure_summary,
        # On refine the model already saw the full samples in the first turn; resend a
        # TRIMMED subset (seed + one detail page) so the refine prompt stays well under
        # per-request token caps and costs less.
        "\n=== HTML SAMPLES (trimmed) ===\n" + _render_samples(
            samples, cap=12000, only=("seed", "detail_sample", "sample_1")),
        "\nOutput ONLY the corrected ```python code block.",
    ])


def build_feedback_prompt(previous_code, human_feedback, samples) -> str:
    return "\n".join([
        "A human reviewer inspected this adapter's output and gave feedback. "
        "Revise the adapter to address it. Keep the class name (GeneratedAdapter) "
        "and the RegulatorAdapter contract. Change only what the feedback requires.",
        "\n=== CURRENT ADAPTER ===\n```python\n" + previous_code + "\n```",
        "\n=== REVIEWER FEEDBACK ===\n" + human_feedback,
        ("\n=== RELEVANT HTML SAMPLES ===\n" + _render_samples(samples)) if samples else "",
        "\nOutput ONLY the revised ```python code block.",
    ])


_CODE_BLOCK_RE = re.compile(r"```(?:python)?\s*\n(.*?)```", re.DOTALL | re.IGNORECASE)


def extract_python(text: str) -> str:
    """Pull the Python source out of the LLM response."""
    m = _CODE_BLOCK_RE.search(text)
    code = m.group(1) if m else text
    code = code.strip()
    if not code:
        raise ValueError("LLM returned no code")
    if "class GeneratedAdapter" not in code:
        raise ValueError("Generated code does not define class GeneratedAdapter")
    return code + "\n"


def generate(prompt: str, model: str = None, debug_path=None) -> str:
    # 4500 is plenty for a full adapter (~200 lines) and keeps cost/credit use lower
    # than the old 8000 ceiling (which tripped tight per-key limits).
    max_tokens = int(os.getenv("ONBOARD_MAX_TOKENS", "4500"))
    kwargs = {"system_prompt": SYSTEM_PROMPT, "user_prompt": prompt, "max_tokens": max_tokens}
    if model:
        kwargs["model"] = model
    response = call_llm(**kwargs)
    # Save the exact prompt + raw model output so a reviewer can SEE what the model
    # produced and why (its prose/reasoning around the code) when debugging a crawl.
    if debug_path is not None:
        try:
            from pathlib import Path
            p = Path(debug_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.with_suffix(".prompt.txt").write_text(prompt, encoding="utf-8")
            p.with_suffix(".response.txt").write_text(response, encoding="utf-8")
        except Exception as e:
            logger.warning(f"Could not save LLM debug output: {e}")
    return extract_python(response)
