"""CLI for the form-filling onboarding path.

    python -m dynamic_crawler.formfill inspect  <url>                  no LLM, no crawl
    python -m dynamic_crawler.formfill propose  --name … --url … --how "…"
    python -m dynamic_crawler.formfill show     <hints.yml>
    python -m dynamic_crawler.formfill refine   <hints.yml> --feedback "…"
    python -m dynamic_crawler.formfill verify   <hints.yml> [--runs 3]
    python -m dynamic_crawler.formfill approve  <hints.yml> --by "name"
    python -m dynamic_crawler.formfill run      <hints.yml>

The order is the workflow. `inspect` first — on an easy site the row selector is
obvious from the digest and you can write the form by hand in a minute, no model
involved. Reach for `propose` when it isn't obvious.

Exit codes: verify exits 1 on FAIL, so it can gate a pipeline the same way
`generic_crawler/calibrate_scope.py` does.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

HINTS_DIR = Path("dynamic_crawler/hints")
OUT_DIR = Path("output/formfill")


def _paths(name: str) -> dict:
    return {
        "hints": HINTS_DIR / f"{name}.yml",
        "inspect": OUT_DIR / name / "inspect",
        "run": OUT_DIR / name / "run",
        "verify": OUT_DIR / name / "verify",
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m dynamic_crawler.formfill",
                                 description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--headed", action="store_true", help="show the browser window")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("inspect", help="render a page and print its structural digest")
    p.add_argument("url")
    p.add_argument("--name", default="scratch", help="where to save artifacts under output/formfill/")

    p = sub.add_parser("propose", help="have an LLM fill the form from the digest")
    p.add_argument("--name", required=True, help="e.g. sbp.circulars")
    p.add_argument("--url", required=True)
    p.add_argument("--how", required=True,
                   help="plain English: how should this site be crawled? e.g. "
                        "'go through each row, click the title, grab that page, come back'")
    p.add_argument("--model", help="OpenRouter model id (default: llm_client.DEFAULT_MODEL)")
    p.add_argument("--out", help="hints path (default dynamic_crawler/hints/<name>.yml)")

    p = sub.add_parser("refine", help="correct a form with plain-English feedback")
    p.add_argument("hints")
    p.add_argument("--feedback", required=True)
    p.add_argument("--reinspect", action="store_true", help="reload the page first")
    p.add_argument("--model")
    p.add_argument("--out", help="default: overwrite the same file")

    p = sub.add_parser("show", help="print a form for review")
    p.add_argument("hints")

    p = sub.add_parser("run", help="crawl using a form")
    p.add_argument("hints")
    p.add_argument("--no-details", action="store_true", help="phase 1 only (the inventory)")
    p.add_argument("--max-details", type=int)
    p.add_argument("--max-pages", type=int)
    p.add_argument("--out")

    p = sub.add_parser("verify", help="run it N times and judge the spread")
    p.add_argument("hints")
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--tolerance", type=float, default=2.0, help="allowed %% spread across runs")
    p.add_argument("--max-pages", type=int, help="cap listing pages — for a quick check")
    p.add_argument("--sample", type=int, default=10, help="rows to include in the report")
    p.add_argument("--out")

    p = sub.add_parser("approve", help="stamp a verified form as approved")
    p.add_argument("hints")
    p.add_argument("--by", required=True, help="who is approving")
    p.add_argument("--verify-json", help="default: output/formfill/<name>/verify/verify.json")
    p.add_argument("--force", action="store_true", help="approve despite a FAIL, and record it")

    a = ap.parse_args(argv)
    headless = not a.headed

    # Imports are per-command so `show` doesn't need playwright and `inspect`
    # doesn't need an API key.
    if a.cmd == "inspect":
        from dynamic_crawler.formfill import inspect as I
        d = I.inspect(a.url, _paths(a.name)["inspect"], headless=headless)
        I.print_digest(d)
        print(f"  artifacts: {_paths(a.name)['inspect']}")
        return 0

    if a.cmd == "propose":
        from dynamic_crawler.formfill import propose as P
        out = Path(a.out) if a.out else _paths(a.name)["hints"]
        hints, errs = P.propose(a.name, a.url, a.how, out, _paths(a.name)["inspect"],
                                model=a.model, headless=headless)
        P.print_result(hints, errs, out)
        return 0

    if a.cmd == "refine":
        from dynamic_crawler.formfill import propose as P
        from dynamic_crawler.formfill.schema import load_hints
        name = load_hints(a.hints, require_valid=False).get("name", Path(a.hints).stem)
        out = Path(a.out) if a.out else Path(a.hints)
        hints, errs = P.refine(a.hints, a.feedback, out, _paths(name)["inspect"],
                               model=a.model, reinspect=a.reinspect, headless=headless)
        P.print_result(hints, errs, out)
        return 0

    if a.cmd == "show":
        from dynamic_crawler.formfill.schema import (approval_state, load_hints,
                                                      summarise, validate_hints)
        h = load_hints(a.hints, require_valid=False)
        print(summarise(h))
        ok, why = approval_state(h)
        if why != "approved":
            # Printed even when usable: "approved before form hashing existed"
            # is a state a reviewer should see, not one to hide behind a tick.
            print(f"  -> {why}")
        errs = validate_hints(h)
        if errs:
            print("\nINVALID:")
            for e in errs:
                print(f"  - {e}")
            return 1
        return 0

    if a.cmd == "run":
        from dynamic_crawler.formfill import runner
        from dynamic_crawler.formfill.schema import approval_state, load_hints
        h = load_hints(a.hints)
        ok, why = approval_state(h)
        if not ok:
            print(f"note: {why}\n", file=sys.stderr)
        out = Path(a.out) if a.out else _paths(h["name"])["run"]
        runner.run(h, out, headless=headless,
                   fetch_details=False if a.no_details else None,
                   max_details=a.max_details, max_pages=a.max_pages)
        return 0

    if a.cmd == "verify":
        from dynamic_crawler.formfill import verify as V
        from dynamic_crawler.formfill.schema import load_hints
        h = load_hints(a.hints)
        out = Path(a.out) if a.out else _paths(h["name"])["verify"]
        rep = V.verify(a.hints, out, runs=a.runs, tolerance=a.tolerance,
                       headless=headless, max_pages=a.max_pages, sample=a.sample)
        return 1 if rep["verdict"] == "FAIL" else 0

    if a.cmd == "approve":
        from dynamic_crawler.formfill import verify as V
        from dynamic_crawler.formfill.schema import load_hints
        h = load_hints(a.hints)
        vj = Path(a.verify_json) if a.verify_json else _paths(h["name"])["verify"] / "verify.json"
        if not vj.exists():
            print(f"no verify report at {vj} — run verify first", file=sys.stderr)
            return 1
        V.approve(a.hints, vj, a.by, force=a.force)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
