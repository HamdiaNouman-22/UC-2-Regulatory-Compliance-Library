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
SNAP_DIR = Path("output/snapshots")


def _resolve_snapshot(value: str | None, name: str) -> Path | None:
    """`--snapshot` with no value means "the stored one for this form"; with a value
    it is a path. Absent means crawl live, as always."""
    if value is None:
        return None
    if value == "auto":
        from dynamic_crawler.formfill.snapshot import SnapshotStore
        store = SnapshotStore(name, SNAP_DIR)
        if not store.exists():
            raise SystemExit(
                f"no snapshot for {name} at {store.html_path}. Capture one first:\n"
                f"  python -m dynamic_crawler.formfill snapshot "
                f"dynamic_crawler/hints/{name}.yml")
        print(f"replaying {store.describe()}", file=sys.stderr)
        return store.html_path
    return Path(value)


def _read_urls(path: str | None) -> list | None:
    """The urls to re-read, or None for an ordinary full crawl.

    An EMPTY file is not the same as no file: the sweep found nothing modified,
    so the right answer is to open no detail page, not to open all of them.
    """
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"no such url list: {path}")
    return [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines()
            if ln.strip() and not ln.startswith("#")]


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

    p = sub.add_parser("snapshot", help="capture the live page ONCE, for offline work")
    p.add_argument("hints")
    p.add_argument("--headless", action="store_true",
                   help="capture without a visible window (default is headed: a real "
                        "window is what a bot-fingerprint rule is least likely to block)")
    p.add_argument("--force", action="store_true",
                   help="attempt even inside the backoff window — only if the network "
                        "or the site's rules actually changed")
    p.add_argument("--status", action="store_true",
                   help="report the stored snapshot and when the next attempt is due; "
                        "makes no request")
    p.add_argument("--dir", help=f"snapshot directory (default {SNAP_DIR})")

    p = sub.add_parser("run", help="crawl using a form")
    p.add_argument("hints")
    p.add_argument("--no-details", action="store_true", help="phase 1 only (the inventory)")
    p.add_argument("--max-details", type=int)
    p.add_argument("--only-urls",
                   help="a file of urls, one per line — usually `sweep --targets`. "
                        "Phase 1 still walks the whole listing; only the detail "
                        "pages of these urls are opened")
    p.add_argument("--max-pages", type=int)
    p.add_argument("--snapshot", nargs="?", const="auto",
                   help="replay the saved page instead of crawling — no network at "
                        "all. Bare --snapshot uses the stored one for this form.")
    p.add_argument("--out")

    p = sub.add_parser("verify", help="run it N times and judge the spread")
    p.add_argument("hints")
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--tolerance", type=float, default=2.0, help="allowed %% spread across runs")
    p.add_argument("--max-pages", type=int, help="cap listing pages — for a quick check")
    p.add_argument("--sample", type=int, default=10, help="rows to include in the report")
    p.add_argument("--snapshot", nargs="?", const="auto",
                   help="verify against the saved page — no network. Proves the form "
                        "reads THAT page deterministically; proves nothing about the "
                        "site's own stability, so it cannot approve a form.")
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

    if a.cmd == "snapshot":
        from dynamic_crawler.formfill.schema import load_hints
        from dynamic_crawler.formfill.snapshot import SnapshotStore, capture
        h = load_hints(a.hints)
        store = SnapshotStore(h["name"], a.dir or SNAP_DIR)

        if a.status:
            print(store.describe())
            allowed, why = store.may_attempt()
            print(f"  live attempt allowed now: {allowed} — {why}")
            return 0

        # The one command in this repo that touches a blocked site.
        print(f"ONE live request to {h['seed_url']}", file=sys.stderr)
        res = capture(h, store, headed=not a.headless, force=a.force)
        result = res["result"]
        if result == "ok":
            print(f"\nCAPTURED  {res['bytes']} bytes | {res['text_len']} chars of text"
                  f" | expanded {res.get('expanded', 0)} section(s)")
            print(f"  {res['path']}")
            if res.get("changed"):
                print("  CONTENT CHANGED since the last capture — the page was amended.")
            print("  Everything from here can run offline: "
                  f"`formfill run {a.hints} --snapshot`")
            return 0
        if result == "refused":
            print(f"\nREFUSED — no request was made.\n  {res['reason']}", file=sys.stderr)
            return 1
        if result == "blocked":
            print(f"\nBLOCKED — {res['reason']!r}. Nothing was saved: a challenge page "
                  f"must never become the stored document.", file=sys.stderr)
            print(f"  blocked {res['consecutive_blocks']}x in a row; next attempt "
                  f"after {res['next_attempt_after']}", file=sys.stderr)
            print("  Do not retry by hand. If nothing about the network changed, the "
                  "remaining routes are an allowlist request to SIMAH or SAMA's copy "
                  "of the same instruments.", file=sys.stderr)
            return 1
        print(f"\nFAILED to load (not a block) — {res['reason']}", file=sys.stderr)
        return 1

    if a.cmd == "run":
        from dynamic_crawler.formfill import runner
        from dynamic_crawler.formfill.schema import approval_state, load_hints
        h = load_hints(a.hints)
        ok, why = approval_state(h)
        if not ok:
            print(f"note: {why}\n", file=sys.stderr)
        out = Path(a.out) if a.out else _paths(h["name"])["run"]
        snap = _resolve_snapshot(a.snapshot, h["name"])
        runner.run(h, out, headless=headless,
                   fetch_details=False if a.no_details else None,
                   max_details=a.max_details, max_pages=a.max_pages,
                   snapshot=snap, only_urls=_read_urls(a.only_urls))
        return 0

    if a.cmd == "verify":
        from dynamic_crawler.formfill import verify as V
        from dynamic_crawler.formfill.schema import load_hints
        h = load_hints(a.hints)
        out = Path(a.out) if a.out else _paths(h["name"])["verify"]
        rep = V.verify(a.hints, out, runs=a.runs, tolerance=a.tolerance,
                       headless=headless, max_pages=a.max_pages, sample=a.sample,
                       snapshot=_resolve_snapshot(a.snapshot, h["name"]))
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
