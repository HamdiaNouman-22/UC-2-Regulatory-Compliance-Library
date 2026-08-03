"""FILL THE FORM — the only place an LLM is involved.

Inputs:
  * the rendered-page digest from inspect.py (candidates we found mechanically)
  * a plain-English instruction from the person onboarding the site

That second input is the half of the idea that makes this tractable. Inferring
INTENT from HTML alone is the hard, unreliable problem — "should I follow these
links or are they navigation?" has no answer in the markup. A human types:

    "go through each row, click the title, grab that page, come back,
     and keep going through the pagination"

...and the model's remaining job is only to translate that into selectors it can
see in the digest. Small job, checkable output.

Nothing here runs a crawl, and runner.py never imports this module. A proposal is
made once, reviewed, verified, committed — then it is data on disk like any other
config.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from dynamic_crawler.formfill import inspect as inspect_mod
from dynamic_crawler.formfill.schema import (
    CSS_ATTRS,
    EXAMPLE_HINTS,
    FIELD_TARGETS,
    HINTS_VERSION,
    PAGER_MODES,
    SHAPES,
    load_hints,
    save_hints,
    summarise,
    validate_hints,
)
from dynamic_crawler.onboarding.llm_client import (        # reuse the repo's one LLM integration
    call_llm,
    extract_json_from_llm_response,
)

SYSTEM_PROMPT = (
    "You fill in a small fixed form that configures an existing web crawler. "
    "You never write code, and you never invent selectors — every selector you "
    "output must appear verbatim in the page digest you are given. If the digest "
    "does not contain what a field needs, omit that field rather than guessing. "
    "Output only a JSON object."
)

TASK = f"""\
Fill in the crawl form below for this page.

RULES
1. `row_selector` MUST be one of the `selector` values in `row_candidates`.
   Pick the one that represents ONE document entry — usually the candidate with a
   high `with_link` and `dated` count, not the outermost container that happens to
   repeat.
2. `pagination.pattern` MUST be built from one of `pagination_candidates`. Replace
   the varying number with {{offset}} (mode url_offset, when the numbers step by the
   page size, e.g. 30, 60, 90) or {{page}} (mode url_page, when they step by 1).
   `step` is the observed gap between consecutive numbers. `max_offset` is the
   largest number seen. If there are no pagination candidates, use mode: none.
3. Regex fields are applied to the ROW's visible text — write them against the
   `samples` strings shown in the digest, and use exactly one capture group.
4. Allowed field targets: {', '.join(FIELD_TARGETS)}. `title` and `document_url`
   are required. Allowed css attrs: {', '.join(CSS_ATTRS)}.
5. Allowed shapes: {', '.join(SHAPES)}. Allowed pagination modes: {', '.join(PAGER_MODES)}.
6. If the digest has `tree_candidates` and no useful `row_candidates` (the
   repeated elements are menu links with no dates and no file links), this is a
   TREE. Then: set `shape: tree`, omit `row_selector`, `pagination` and `fields`,
   and fill a `tree` block by copying `menu_selector`, `node_selector` and
   `link_selector` verbatim from the FIRST tree candidate — it is the one that
   uniquely identifies a menu. Add `expand_selector` only if `expand_candidates`
   shows a real click-to-open control. Set `fetch_details: true`: on a tree the
   page text is the document.
7. `section_path` is OPTIONAL and rebuilds the site's own hierarchy for each row.
   Use ONLY entries from `section_levels` in the digest, in the order given
   (outermost first), copying `ancestor` and `title` verbatim. Put fixed
   crumbs the page does not repeat (usually the page's own h1) in `prefix`.
   Omit the whole block when the digest lists no section_levels.
8. Output JSON with these top-level keys: version, name, seed_url, shape, scope,
   section_path, fetch_details, notes — plus, for a list/table,
   row_selector + detail_link_selector + pagination + fields, or, for a tree, a
   `tree` block. version must be {HINTS_VERSION}.

`notes` is one sentence: what you were unsure about. A reviewer reads it first.

FORM (YAML shown for readability — you output the equivalent JSON):
{EXAMPLE_HINTS}
"""


def _prompt(name: str, seed_url: str, instruction: str, digest: dict) -> str:
    return "\n".join([
        TASK,
        f"\nname: {name}",
        f"seed_url: {seed_url}",
        "\nWHAT THE PERSON ONBOARDING THIS SITE SAYS TO DO:",
        instruction.strip(),
        "\nPAGE DIGEST (rendered with a real browser):",
        inspect_mod.digest_for_prompt(digest),
    ])


def _ask(prompt: str, model: str | None, system: str = SYSTEM_PROMPT) -> dict:
    kwargs = {"system_prompt": system, "user_prompt": prompt, "temperature": 0.0}
    if model:
        kwargs["model"] = model
    return extract_json_from_llm_response(call_llm(**kwargs))


def _stamp(h: dict, model: str | None, seed_url: str, name: str, instruction: str) -> dict:
    h.setdefault("version", HINTS_VERSION)
    h["name"] = name
    h["seed_url"] = seed_url
    h["meta"] = {
        "proposed_by": model or "default (see llm_client.DEFAULT_MODEL)",
        "proposed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "instruction": instruction.strip(),
        # A proposal is never trusted on arrival. verify.py --approve is the only
        # thing that flips this, and only after repeated runs agree.
        "approved": False,
        "approved_by": None,
        "approved_at": None,
        "verify": None,
    }
    return h


def propose(name: str, seed_url: str, instruction: str, out_path: str | Path,
            artifacts_dir: str | Path, model: str | None = None,
            headless: bool = True) -> tuple[dict, list[str]]:
    """Inspect -> ask -> validate -> (one repair attempt) -> write.

    Returns (hints, remaining_errors). A proposal that still has errors is written
    anyway, with the errors in `meta.validation_errors`, because a wrong form a
    human can correct in 30 seconds is more useful than an exception.
    """
    digest = inspect_mod.inspect(seed_url, artifacts_dir, headless=headless)
    prompt = _prompt(name, seed_url, instruction, digest)

    hints = _stamp(_ask(prompt, model), model, seed_url, name, instruction)
    errs = validate_hints(hints)

    if errs:
        # One repair round. The model is told exactly what failed, which is a far
        # easier task than the original — and if it still fails, we stop and let a
        # human fix a dozen fields by hand.
        repair = "\n".join([
            prompt,
            "\nYour previous answer was rejected by the form validator:",
            json.dumps(hints, ensure_ascii=False, indent=2),
            "\nERRORS:\n  - " + "\n  - ".join(errs),
            "\nOutput a corrected JSON object. Fix only what the errors name.",
        ])
        retry = _stamp(_ask(repair, model), model, seed_url, name, instruction)
        retry_errs = validate_hints(retry)
        if len(retry_errs) < len(errs):
            hints, errs = retry, retry_errs

    if errs:
        hints["meta"]["validation_errors"] = errs

    save_hints(hints, out_path)
    return hints, errs


def refine(hints_path: str | Path, feedback: str, out_path: str | Path,
           artifacts_dir: str | Path, model: str | None = None,
           reinspect: bool = False, headless: bool = True) -> tuple[dict, list[str]]:
    """Correct a proposal with plain-English feedback.

    'the date column is wrong, it should be the second one' — the reviewer does
    not need to know CSS. Re-inspects only when asked, so a correction round
    doesn't reload the site unnecessarily.
    """
    previous = load_hints(hints_path, require_valid=False)
    seed_url = previous.get("seed_url", "")
    name = previous.get("name", Path(hints_path).stem)
    instruction = (previous.get("meta") or {}).get("instruction", "")

    digest_path = Path(artifacts_dir) / "digest.json"
    if reinspect or not digest_path.exists():
        digest = inspect_mod.inspect(seed_url, artifacts_dir, headless=headless)
    else:
        digest = json.loads(digest_path.read_text(encoding="utf-8"))

    prompt = "\n".join([
        _prompt(name, seed_url, instruction, digest),
        "\nPREVIOUSLY PROPOSED FORM:",
        json.dumps({k: v for k, v in previous.items() if k != "meta"},
                   ensure_ascii=False, indent=2),
        f"\nHUMAN FEEDBACK ON WHAT IS WRONG WITH IT:\n{feedback.strip()}",
        "\nOutput the corrected JSON form. Keep everything the feedback did not flag.",
    ])

    hints = _stamp(_ask(prompt, model), model, seed_url, name, instruction)
    hints["meta"]["refined_from"] = str(hints_path)
    hints["meta"]["feedback"] = feedback.strip()
    errs = validate_hints(hints)
    if errs:
        hints["meta"]["validation_errors"] = errs
    save_hints(hints, out_path)
    return hints, errs


def print_result(hints: dict, errs: list[str], out_path: str | Path) -> None:
    print(summarise(hints))
    if (hints.get("notes")):
        print(f"\nmodel's note: {hints['notes']}")
    if errs:
        print("\nSTILL INVALID — fix by hand or run `formfill refine`:")
        for e in errs:
            print(f"  - {e}")
    print(f"\nwritten to {out_path}")
    print("This is a GUESS. Nothing has been crawled yet. Next:")
    print(f"  python -m dynamic_crawler.formfill verify {out_path}")
