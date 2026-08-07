"""Autonomous onboarding orchestrator.

Given a regulator + seed URL, this:
  inspect -> generate adapter (LLM) -> sandboxed test run -> cross-check vs live
  site -> refine automatically on failure (bounded) -> full sandboxed run ->
  write results + reports for human review.

It also supports a human-feedback refine mode (--refine) where a reviewer's
plain-English note drives one more generate/test/cross-check cycle.

Nothing here is auto-trusted: results are written to output/ for a human to
review and approve. The production DB is never touched.

CLI:
  python -m dynamic_crawler.auto.onboard --seed-url URL --regulator SAMA \
      --tab-name "Finance Sector" --source-system "SAMA RULEBOOK" [--model M] [--limit N]

  python -m dynamic_crawler.auto.onboard --refine SAMA \
      --feedback "published_date is wrong; use the info-box date" [--sample-url URL] [--model M]
"""

import argparse
import json
import logging
import os
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse

from dynamic_crawler.auto import codegen, crosscheck, entrypoint, sandbox, shapes
from dynamic_crawler.onboarding.site_inspector import inspect_site
from dynamic_crawler.validation import validate_documents

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
GENERATED_DIR = REPO_ROOT / "dynamic_crawler" / "auto" / "generated"
OUTPUT_ROOT = REPO_ROOT / "output" / "dynamic_crawler"

MAX_ITERS = int(os.getenv("ONBOARD_MAX_ITERS", "6"))
TEST_LIMIT = 2
# Completeness guard: a recursive crawl of TEST_LIMIT whole top-level categories
# should yield well more than a couple of documents. If it doesn't, the adapter is
# almost certainly not recursing into nested sub-pages — reject and push the model
# to go deeper rather than accepting an under-crawl. Configurable / soft.
MIN_TEST_DOCS = int(os.getenv("ONBOARD_MIN_TEST_DOCS", "8"))
DEFAULT_FETCH_CFG = {
    "backend": "requests", "request_delay_seconds": 1.0,
    "max_retries": 3, "retry_backoff_seconds": 2, "timeout_seconds": 30,
}


def _slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_")


@dataclass
class OnboardingState:
    regulator: str
    tab_name: str
    source_system: str
    seed_url: str
    base_url: str
    model: Optional[str] = None
    samples: dict = field(default_factory=dict)
    adapter_code: str = ""
    iteration: int = 0
    accepted: bool = False
    history: list = field(default_factory=list)
    shape: object = None   # dynamic_crawler.auto.shapes.Shape (chosen at classify time)


def _work_dir(regulator: str, model: Optional[str], tab: Optional[str] = None) -> Path:
    # Namespace by regulator/TAB/model so different tabs of the same regulator
    # (e.g. Circulars vs Regulatory Sandbox) never overwrite each other.
    sub = _slug(model) if model else "default"
    d = OUTPUT_ROOT / _slug(regulator)
    if tab:
        d = d / _slug(tab)
    d = d / sub
    d.mkdir(parents=True, exist_ok=True)
    return d


def _adapter_dir(regulator: str, model: Optional[str], tab: Optional[str] = None) -> Path:
    sub = _slug(model) if model else "default"
    d = GENERATED_DIR / _slug(regulator)
    if tab:
        d = d / _slug(tab)
    d = d / sub
    d.mkdir(parents=True, exist_ok=True)
    return d


def _sample_dir(regulator: str, tab: Optional[str] = None) -> Path:
    d = GENERATED_DIR / _slug(regulator)
    if tab:
        d = d / _slug(tab)
    d = d / "_samples"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _read_raw_seed(regulator: str, tab: Optional[str] = None) -> str:
    """The full, un-truncated seed HTML saved by the inspector (shape detection
    needs all the table rows, which the cleaned/truncated prompt HTML omits)."""
    try:
        return (_sample_dir(regulator, tab) / "seed.html").read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def _validation_cfg(doc_count_hint=(1, 100000)) -> dict:
    lo, hi = doc_count_hint
    return {"validation": {
        "required_fields": ["regulator", "source_system", "category", "title", "document_url", "source_page_url"],
        "expected_doc_count_min": lo, "expected_doc_count_max": hi,
    }}


def _save_adapter(state: OnboardingState) -> Path:
    d = _adapter_dir(state.regulator, state.model, state.tab_name)
    path = d / "adapter.py"
    path.write_text(state.adapter_code, encoding="utf-8")
    # version each iteration for auditability
    (d / f"adapter.v{state.iteration}.py").write_text(state.adapter_code, encoding="utf-8")
    return path


def _run_and_check(state: OnboardingState, adapter_path: Path, limit, backend, is_test=False) -> dict:
    """Sandboxed run + validation + cross-check. Returns a combined report.

    is_test=True enables the completeness guard (only meaningful for the small,
    limited test crawl, where an under-count clearly signals shallow recursion).
    """
    # Test runs must be FAST to iterate on: cap the fetch budget and shorten the
    # delay so a recursive limit=2 probe finishes in ~a minute (enough pages to
    # prove recursion + meet the completeness floor), not the many minutes a full
    # polite crawl would take. Full runs stay generous and polite.
    if is_test:
        max_fetches, req_delay, timeout_s = 80, 0.4, 300
    else:
        # Bound the full run so it always COMPLETES inside the timeout and returns
        # its documents (the CountingFetcher stops gracefully at the budget). Budget x
        # delay must stay well under timeout: 1400 x 0.6s = 840s of delays + fetch time.
        max_fetches, req_delay, timeout_s = 1400, 0.6, 5400
    res = sandbox.run(str(adapter_path), limit=limit, backend=backend,
                      request_delay=req_delay, max_fetches=max_fetches, timeout_seconds=timeout_s)
    if not res.ok:
        return {"stage": "run", "ok": False,
                "failure_summary": f"Adapter crashed or timed out.\n{res.error}\n{(res.traceback or '')[-2500:]}"}

    val = validate_documents(res.documents, _validation_cfg())
    cc = crosscheck.crosscheck(res.documents, {**DEFAULT_FETCH_CFG, "backend": backend})

    # Completeness is graded per SHAPE: a flat table is judged against its row count
    # (test run only proves extraction), a tree against recursion depth. This is what
    # stops the old tree-only guard from wrongly failing a correct flat-table crawl.
    doc_count = len(res.documents)
    if state.shape is not None:
        coverage_ok, coverage_msg = state.shape.verify(doc_count, state.shape.evidence, is_test)
    else:  # fallback to the legacy tree guard if no shape was classified
        coverage_ok = not (is_test and doc_count < MIN_TEST_DOCS)
        coverage_msg = None if coverage_ok else f"COMPLETENESS FAIL: only {doc_count} documents."

    ok = bool(res.documents) and cc["pass"] and val["ok"] and coverage_ok
    report = {"stage": "check", "ok": ok, "doc_count": doc_count,
              "fetch_count": res.fetch_count, "validation": val, "crosscheck": cc,
              "coverage_ok": coverage_ok,
              "shape": state.shape.name if state.shape else None,
              "shape_evidence": state.shape.evidence if state.shape else {}}
    report["documents"] = res.documents
    if not ok:
        fs = []
        if not res.documents:
            fs.append("Adapter produced ZERO documents.")
        if not val["ok"]:
            fs.append(f"Validation issues: count={val['document_count']} "
                      f"(min {val['expected_min']}), missing-field docs="
                      f"{len(val['documents_with_missing_required_fields'])}.")
        if not cc["pass"]:
            fs.append(f"Cross-check FAILED: {cc.get('reason', '')}")
        if not coverage_ok and coverage_msg:
            fs.append(coverage_msg)
        report["failure_summary"] = "\n".join(fs)
    return report


def _persist_results(state: OnboardingState, report: dict, tag: str):
    d = _work_dir(state.regulator, state.model, state.tab_name)
    docs = report.get("documents", [])
    (d / "docs.json").write_text(
        json.dumps([asdict(x) for x in docs], ensure_ascii=False, indent=2), encoding="utf-8")
    slim = {k: v for k, v in report.items() if k != "documents"}
    slim["tag"] = tag
    slim["adapter_path"] = str(_adapter_dir(state.regulator, state.model, state.tab_name) / "adapter.py")
    slim["seed_url"] = state.seed_url
    slim["iterations_used"] = state.iteration
    (d / "onboarding_report.json").write_text(
        json.dumps(slim, ensure_ascii=False, indent=2), encoding="utf-8")
    return d


# ---- initial autonomous onboarding ----

def onboard(regulator, tab_name, source_system, seed_url, model=None, backend="requests",
            full_run=True) -> OnboardingState:
    base_url = f"{urlparse(seed_url).scheme}://{urlparse(seed_url).netloc}"
    state = OnboardingState(regulator=regulator, tab_name=tab_name, source_system=source_system,
                            seed_url=seed_url, base_url=base_url, model=model)

    # 1. Resolve a working entry point (handles moved/404 URLs).
    ep = entrypoint.resolve(seed_url, tab_name, {**DEFAULT_FETCH_CFG, "backend": backend}, model=model)
    state.seed_url = ep["url"]
    if not ep["confident"]:
        logger.warning(f"Entry-point note: {ep['note']}")

    # 2. Inspect the live site.
    logger.info(f"Inspecting {state.seed_url} ...")
    state.samples = inspect_site(state.seed_url, str(_sample_dir(regulator, tab_name)))

    # 2b. Classify the page SHAPE from the raw (un-truncated) seed HTML, so we give
    # the model the right instructions and grade the crawl the right way.
    raw_seed = _read_raw_seed(regulator, tab_name)
    state.shape = shapes.classify(raw_seed)
    logger.info(f"Using shape='{state.shape.name}' (limit means: {state.shape.limit_meaning}; "
                f"evidence={state.shape.evidence})")

    # For a flat table, also give the model ONE real document DETAIL page as a sample,
    # so it can see how the "Download Original PDF" link is structured and wire it up.
    if state.shape.name == "flat_table":
        detail_url = shapes.first_row_link(raw_seed, base_url)
        if detail_url:
            from dynamic_crawler.onboarding import site_inspector as _si
            dhtml = _si._fetch_html(detail_url)
            if dhtml:
                state.samples["detail_sample"] = {
                    "url": detail_url, "cleaned_html": _si._clean_html_for_llm(dhtml)}
                logger.info(f"Added detail-page sample for flat_table: {detail_url}")

    prompt = codegen.build_initial_prompt(regulator, source_system, base_url, state.seed_url,
                                          tab_name, state.samples, shape_guidance=state.shape.guidance)

    # 3-5. Generate -> test -> cross-check -> auto-refine loop.
    for it in range(1, MAX_ITERS + 1):
        state.iteration = it
        logger.info(f"[iter {it}/{MAX_ITERS}] generating adapter (model={model or 'default'}) ...")
        dbg = _work_dir(regulator, model, tab_name) / "llm_debug" / f"iter{it}"
        try:
            state.adapter_code = codegen.generate(prompt, model=model, debug_path=dbg)
        except Exception as e:
            msg = str(e)
            # Billing/auth errors won't fix themselves on retry — stop immediately with
            # a clear message instead of burning all iterations on the same failure.
            if any(s in msg for s in ("402", "Payment Required", "insufficient", "401",
                                      "invalid api key", "Unauthorized")):
                logger.error(f"LLM call rejected (billing/auth): {msg}")
                raise RuntimeError(
                    "LLM provider rejected the request (out of credits or bad API key). "
                    "Top up / raise the limit on your OpenRouter key, or switch to the cheaper "
                    "DeepSeek model, then re-run. Nothing was crawled.")
            logger.error(f"Code generation failed: {e}")
            prompt = codegen.build_auto_refine_prompt(state.adapter_code or "# (no code)",
                                                      f"Your previous output was invalid: {e}",
                                                      state.samples, shape_guidance=state.shape.guidance)
            continue

        adapter_path = _save_adapter(state)
        logger.info(f"[iter {it}] test run (limit={state.shape.test_limit}) ...")
        report = _run_and_check(state, adapter_path, limit=state.shape.test_limit,
                                backend=backend, is_test=True)
        state.history.append({"iteration": it, "ok": report["ok"],
                              "summary": report.get("failure_summary", "passed")})

        if report["ok"]:
            logger.info(f"[iter {it}] test + cross-check PASSED.")
            state.accepted = True
            break

        logger.warning(f"[iter {it}] failed:\n{report['failure_summary']}")
        prompt = codegen.build_auto_refine_prompt(state.adapter_code, report["failure_summary"],
                                                  state.samples, shape_guidance=state.shape.guidance)

    if not state.accepted:
        logger.error(f"Exhausted {MAX_ITERS} iterations without passing. "
                     f"Last adapter + reports saved for human inspection.")
        _persist_results(state, {"stage": "exhausted", "ok": False,
                                 "history": state.history, "documents": []}, tag="failed")
        return state

    # 6. Full run (optional) + persist for human review.
    if full_run:
        logger.info("Running FULL crawl with the accepted adapter ...")
        adapter_path = _adapter_dir(regulator, model, tab_name) / "adapter.py"
        full = _run_and_check(state, adapter_path, limit=None, backend=backend)
        # Download the actual PDF documents locally (trusted post-processing; the
        # sandboxed adapter cannot write files). Annotates docs with local_pdf paths.
        try:
            from dynamic_crawler.auto import pdf_fetch
            pdf_summary = pdf_fetch.download_pdfs(full.get("documents", []),
                                                  _work_dir(regulator, model, tab_name))
            full["pdf_download"] = pdf_summary
        except Exception as e:
            logger.warning(f"PDF download step failed: {e}")
        d = _persist_results(state, full, tag="full")
        logger.info(f"Full run: {full.get('doc_count')} docs. Review artifacts in {d}")
    else:
        # Quick mode: SAVE the documents the passing test run actually reached, so
        # the reviewer can browse them + build the Excel. (Previously this wrote an
        # empty docs.json, making a successful quick run look like it found nothing.)
        # Note: this is a bounded sample (limited categories), not the full crawl.
        # Also download the sample's PDFs so a quick run shows the full flow fast.
        try:
            from dynamic_crawler.auto import pdf_fetch
            report["pdf_download"] = pdf_fetch.download_pdfs(report.get("documents", []),
                                                             _work_dir(regulator, model))
        except Exception as e:
            logger.warning(f"PDF download step failed: {e}")
        report["stage"] = "test-only"
        d = _persist_results(state, report, tag="test-only")
        logger.info(f"Quick test run: {report.get('doc_count')} docs saved (partial sample). "
                    f"Run again with Quick mode OFF for the complete crawl. Review in {d}")

    return state


# ---- human-feedback refine mode ----

def refine_with_feedback(regulator, feedback, model=None, sample_url=None, backend="requests",
                         source_system=None, tab_name=None):
    adir = _adapter_dir(regulator, model, tab_name)
    apath = adir / "adapter.py"
    if not apath.exists():
        raise SystemExit(f"No existing adapter at {apath}. Run initial onboarding first "
                         f"(same regulator AND tab).")
    prev_code = apath.read_text(encoding="utf-8")

    # Recover seed/base + previously-detected shape from the saved report if available.
    rep_path = _work_dir(regulator, model, tab_name) / "onboarding_report.json"
    prev_report = json.loads(rep_path.read_text(encoding="utf-8")) if rep_path.exists() else {}
    seed_url = prev_report.get("seed_url")

    samples = {}
    if sample_url:
        samples = inspect_site(sample_url, str(_sample_dir(regulator, tab_name)), sample_urls=[sample_url])

    state = OnboardingState(regulator=regulator, tab_name=tab_name or regulator,
                            source_system=source_system or "", seed_url=seed_url or "",
                            base_url="", model=model, adapter_code=prev_code)
    # Reuse the shape chosen at onboarding so the full-run completeness check (e.g. a
    # flat table's row-count target) still applies while addressing the feedback.
    state.shape = shapes.get_shape(prev_report.get("shape") or "sidebar_tree",
                                   prev_report.get("shape_evidence") or {})

    prompt = codegen.build_feedback_prompt(prev_code, feedback, samples)
    logger.info(f"Regenerating adapter from reviewer feedback (model={model or 'default'}) ...")
    _fbver = _next_version(adir)
    dbg = _work_dir(regulator, model, tab_name) / "llm_debug" / f"feedback_v{_fbver}"
    state.adapter_code = codegen.generate(prompt, model=model, debug_path=dbg)
    state.iteration = _fbver
    adapter_path = _save_adapter(state)

    report = _run_and_check(state, adapter_path, limit=None, backend=backend)
    try:
        from dynamic_crawler.auto import pdf_fetch
        report["pdf_download"] = pdf_fetch.download_pdfs(
            report.get("documents", []), _work_dir(regulator, model, tab_name))
    except Exception as e:
        logger.warning(f"PDF download step failed: {e}")
    # record the feedback string for auditability
    d = _persist_results(state, report, tag="feedback-refine")
    (d / f"feedback.v{state.iteration}.txt").write_text(feedback, encoding="utf-8")
    logger.info(f"[feedback] doc_count={report.get('doc_count')} ok={report.get('ok')} -- review {d}")
    return state


def _next_version(adir: Path) -> int:
    versions = [int(m.group(1)) for p in adir.glob("adapter.v*.py")
                if (m := re.search(r"adapter\.v(\d+)\.py", p.name))]
    return (max(versions) + 1) if versions else 1


def main():
    p = argparse.ArgumentParser(description="Autonomous crawler onboarding agent")
    p.add_argument("--seed-url")
    p.add_argument("--regulator")
    p.add_argument("--tab-name")
    p.add_argument("--source-system", default="")
    p.add_argument("--model", default=os.getenv("ONBOARD_MODEL"))
    p.add_argument("--backend", default="requests", choices=["requests", "selenium"])
    p.add_argument("--no-full-run", action="store_true", help="stop after test+cross-check passes")
    p.add_argument("--refine", metavar="REGULATOR", help="human-feedback refine mode for an existing adapter")
    p.add_argument("--feedback", help="plain-English reviewer feedback (with --refine)")
    p.add_argument("--sample-url", help="optional page URL the feedback refers to (with --refine)")
    args = p.parse_args()

    if args.refine:
        if not args.feedback:
            p.error("--refine requires --feedback")
        refine_with_feedback(args.refine, args.feedback, model=args.model,
                             sample_url=args.sample_url, backend=args.backend,
                             source_system=args.source_system, tab_name=args.tab_name)
        return

    if not (args.seed_url and args.regulator and args.tab_name):
        p.error("initial onboarding requires --seed-url, --regulator, --tab-name")
    src = args.source_system or f"{args.regulator} {args.tab_name}"
    onboard(args.regulator, args.tab_name, src, args.seed_url,
            model=args.model, backend=args.backend, full_run=not args.no_full_run)


if __name__ == "__main__":
    main()
