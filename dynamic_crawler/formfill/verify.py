"""THE GATE — the step that decides whether a proposal is allowed to exist.

The obvious version of this ("run it once, show the person a count and five rows,
they click approve") does not survive contact with our own measurements. SDAIA
returned **415 -> 363 -> 439 documents across three runs of identical code**. A
reviewer looking at one count cannot tell a wrong form from a flaky site, and
approving on that basis is the same blind trust as before, wearing a UI.

So the gate runs the form N TIMES (phase 1 only — cheap) and judges the spread,
not a single number:

    FAIL   any run found 0 rows
    FAIL   run-to-run variance above tolerance (default 2%)
    FAIL   title or document_url filled on under 98% of rows
    WARN   an optional field filled on under 60% of rows
    WARN   the page count was discovered rather than frozen in the form
    WARN   the crawl stopped early or a listing page failed to load

WHAT THIS CANNOT TELL YOU — and it says so in the report, because the honest
limit matters more than the green tick:

    Consistency is not correctness. Three runs agreeing on 40 documents when the
    site has 4,160 is three consistent, wrong runs. A human reading a sample can
    confirm the rows ARE documents; nobody can confirm from a sample that ALL the
    documents are there. Coverage is settled by `db_compare.py` against a
    regulator already in the database, or by a count published by the site
    itself — never by eyeballing.
"""

from __future__ import annotations

import json
import statistics
from datetime import datetime, timezone
from pathlib import Path

from dynamic_crawler.formfill import runner
from dynamic_crawler.formfill.schema import body_hash, load_hints, stamp_meta, summarise

DEFAULT_RUNS = 3
DEFAULT_TOLERANCE = 2.0        # percent spread across runs
REQUIRED_FILL = 98.0           # title / document_url
OPTIONAL_FILL_WARN = 60.0


def verify(hints_path: str | Path, out_dir: str | Path, runs: int = DEFAULT_RUNS,
           tolerance: float = DEFAULT_TOLERANCE, headless: bool = True,
           max_pages: int | None = None, sample: int = 10,
           snapshot: str | Path | None = None) -> dict:
    hints = load_hints(hints_path)                 # structurally valid or it raises
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # On a list, phase 1 is the thing being verified and phase 2 is skipped: it
    # is minutes rather than hours, and phase 2 cannot be right if phase 1 found
    # the wrong rows anyway.
    #
    # On a TREE, phase 2 *is* the discovery — the menu only reveals deeper nodes
    # once you are standing on a child page — so skipping it would verify 20
    # nodes of a 40-node tree and call it stable. Trees therefore verify in full,
    # and take correspondingly longer.
    # Same reasoning applies to include_page: the seed page's own text and HTML
    # are captured in phase 2, so skipping it would verify a form that never
    # produces the document the form exists to produce.
    needs_phase2 = hints.get("shape") == "tree" or bool(hints.get("include_page"))

    results, url_sets = [], []
    for i in range(1, runs + 1):
        print(f"\n--- verify run {i}/{runs} ---", flush=True)
        summary = runner.run(hints, out / f"run_{i}", headless=headless,
                             fetch_details=None if needs_phase2 else False,
                             max_pages=max_pages, write_excel=(i == 1),
                             snapshot=snapshot)
        results.append(summary)
        rows = json.loads((out / f"run_{i}" / "rows.json").read_text(encoding="utf-8"))
        url_sets.append({r["href"] or f"::{r['title']}" for r in rows})

    counts = [r["rows"] for r in results]
    stable = set.intersection(*url_sets) if url_sets else set()
    union = set.union(*url_sets) if url_sets else set()
    unstable = union - stable
    spread = (100.0 * (max(counts) - min(counts)) / max(counts)) if max(counts) else 100.0

    fill = results[0]["fill_rates"]
    required = [t for t in ("title", "document_url") if fill.get(t, 0) < REQUIRED_FILL]
    weak = [t for t, v in fill.items()
            if t not in ("title", "document_url") and v < OPTIONAL_FILL_WARN]

    failures, warnings = [], []
    if min(counts) == 0:
        failures.append(f"a run found 0 rows (counts: {counts}) — the row selector "
                        f"{hints['row_selector']!r} matched nothing")
    # Only report the spread when there is a spread to report: with a count of 0
    # it is arithmetic noise on top of the real failure above, and with a single
    # run there is nothing to compare.
    elif runs < 2:
        warnings.append("a single run cannot measure stability — use --runs 3 before "
                        "approving (SDAIA returned 415/363/439 on three runs of "
                        "identical code)")
    elif spread > tolerance:
        failures.append(f"run-to-run spread {spread:.1f}% exceeds the {tolerance}% tolerance "
                        f"(counts: {counts}) — {len(unstable)} entries appeared in some runs "
                        "but not others, which would read as documents appearing and "
                        "disappearing in change detection")
    blocked_total = sum(r.get("blocked_pages", 0) for r in results)
    if blocked_total:
        # Never a warning. A blocked run stored a WAF challenge page as
        # document content; approving it would index the block page.
        failures.append(f"{blocked_total} page(s) came back as a bot-protection "
                        "challenge, not the site. Nothing from this run can be trusted.")
    for t in required:
        failures.append(f"{t} filled on only {fill.get(t, 0)}% of rows (needs {REQUIRED_FILL}%)")

    for t in weak:
        warnings.append(f"{t} filled on only {fill[t]}% of rows — check the pattern against "
                        "the inventory sheet before relying on it")
    for r in results:
        for w in r["warnings"]:
            if w not in warnings:
                warnings.append(w)
    plans = [r.get("plan") or {} for r in results]
    if any(p.get("discovered_max") is not None and not p.get("frozen") for p in plans):
        found = sorted({p.get("discovered_max") for p in plans})
        warnings.append(f"the last page was discovered from the site rather than frozen as "
                        f"pagination.max_offset (saw {found}) — set it in the form so the "
                        "page plan cannot move between runs")
    capped = [p for p in plans if p.get("capped_by_max_pages")]
    if capped:
        p = capped[0]
        # A FAILURE, not a warning. A capped walk measures the cap, not the site —
        # approving on one would put exactly the kind of hole in the library this
        # whole gate exists to prevent. Quick smoke checks are supposed to exit
        # non-zero: they are checks, not approvals.
        failures.append(f"only {p.get('planned_pages')} of {p.get('pages_wanted')} listing "
                        f"pages were walked (max_pages={p.get('max_pages')}) — a capped run "
                        "cannot approve a form. Re-run without --max-pages, and raise "
                        "pagination.max_pages in the form if that is what cut it short")

    if snapshot:
        # Never a failure — a snapshot verify is a legitimate and useful check that
        # the form reads the page deterministically. But it measures the FORM, not
        # the SITE: N runs against one saved file cannot see the run-to-run variance
        # this gate exists to catch (SDAIA: 415/363/439 on identical code). So it
        # warns, and `approve` refuses to stamp it.
        warnings.append(
            f"verified against a SNAPSHOT ({snapshot}), not the live site — this "
            "proves the form reads that saved page consistently and nothing about "
            "the site's stability. It cannot approve a form.")

    verdict = "FAIL" if failures else ("WARN" if warnings else "PASS")

    report = {
        "name": hints.get("name"),
        "seed": hints.get("seed_url"),
        # What the runs were made against. `approve` reads this.
        "source": "snapshot" if snapshot else "live",
        "snapshot": str(snapshot) if snapshot else "",
        "verified_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "runs": runs,
        "counts": counts,
        "mean": round(statistics.fmean(counts), 1) if counts else 0,
        "spread_pct": round(spread, 2),
        "tolerance_pct": tolerance,
        "stable_entries": len(stable),
        "unstable_entries": len(unstable),
        "fill_rates": fill,
        "seconds_per_run": [r["seconds"] for r in results],
        "listing_pages": results[0]["listing_pages"],
        "verdict": verdict,
        "failures": failures,
        "warnings": warnings,
    }

    (out / "verify.json").write_text(json.dumps(report, ensure_ascii=False, indent=2),
                                     encoding="utf-8")
    (out / "verify_report.md").write_text(
        _markdown(report, hints, out, sample, sorted(unstable)[:15]), encoding="utf-8")
    _print(report, out)
    return report


def _markdown(rep: dict, hints: dict, out: Path, sample: int, unstable: list[str]) -> str:
    rows = []
    try:
        rows = json.loads((out / "run_1" / "rows.json").read_text(encoding="utf-8"))[:sample]
    except Exception:
        pass

    lines = [
        f"# Verify — {rep['name']}",
        "",
        f"**{rep['verdict']}** · {rep['runs']} runs · counts {rep['counts']} · "
        f"spread {rep['spread_pct']}% (tolerance {rep['tolerance_pct']}%)",
        "",
        f"- seed: {rep['seed']}",
        f"- listing pages walked: {rep['listing_pages']}",
        f"- entries found in every run: {rep['stable_entries']}",
        f"- entries found in some runs only: {rep['unstable_entries']}",
        f"- seconds per run: {rep['seconds_per_run']}",
        "",
        "## The form",
        "```",
        summarise(hints),
        "```",
        "",
        "## Field fill rates",
        "",
        "| field | % of rows |",
        "|---|---|",
    ]
    lines += [f"| {k} | {v}% |" for k, v in rep["fill_rates"].items()]

    if rep["failures"]:
        lines += ["", "## Failures", ""] + [f"- {f}" for f in rep["failures"]]
    if rep["warnings"]:
        lines += ["", "## Warnings", ""] + [f"- {w}" for w in rep["warnings"]]
    if unstable:
        lines += ["", "## Entries that did not appear in every run", ""] + \
                 [f"- {u}" for u in unstable]

    lines += ["", f"## Sample of {len(rows)} entries — read these", "",
              "Confirm they are real documents with sensible titles, dates and "
              "reference numbers. This is the part a person can genuinely check.", ""]
    for r in rows:
        f = r.get("fields", {})
        bits = " · ".join(f"{k}={v}" for k, v in f.items()
                          if k not in ("title", "document_url") and v)
        lines.append(f"- **{r['title'][:120]}**" + (f"  \n  {bits}" if bits else "")
                     + f"  \n  {r['href']}")

    lines += [
        "",
        "## What this does not prove",
        "",
        "Consistency is not coverage. Three runs agreeing on a number says the form "
        "is *stable*; it says nothing about whether the site has ten times more "
        "documents than we found. Nobody can confirm completeness from a sample. "
        "Settle it with `db_compare.py` against a regulator already in the "
        "database, or against a total the site publishes itself.",
        "",
    ]
    return "\n".join(lines)


def _print(rep: dict, out: Path) -> None:
    print(f"\n{'=' * 62}\n{rep['verdict']}  —  {rep['name']}\n{'=' * 62}")
    print(f"  counts {rep['counts']}   spread {rep['spread_pct']}%   "
          f"stable {rep['stable_entries']} / unstable {rep['unstable_entries']}")
    print("  fill:  " + "  ".join(f"{k} {v}%" for k, v in rep["fill_rates"].items()))
    for f in rep["failures"]:
        print(f"  FAIL   {f}")
    for w in rep["warnings"]:
        print(f"  warn   {w}")
    print(f"\n  report: {out / 'verify_report.md'}")
    print(f"  excel:  {out / 'run_1' / 'results.xlsx'}   <- read the 'inventory' sheet\n")


def approve(hints_path: str | Path, verify_json: str | Path, approved_by: str,
            force: bool = False) -> dict:
    """Stamp a form as approved. Only a PASS qualifies, unless a human explicitly
    overrides — and the override is recorded in the file, not hidden."""
    hints = load_hints(hints_path)
    rep = json.loads(Path(verify_json).read_text(encoding="utf-8"))

    if rep["verdict"] == "FAIL" and not force:
        raise SystemExit(f"refusing to approve: last verify was FAIL\n  - "
                         + "\n  - ".join(rep["failures"])
                         + "\nFix the form (formfill refine) and verify again, or pass "
                           "--force to record a deliberate override.")

    # A snapshot verify cannot approve. Three runs against one saved file agree by
    # construction — approving on that would make the gate theatre for exactly the
    # sites (blocked ones) where we are least able to check anything.
    if rep.get("source") == "snapshot" and not force:
        raise SystemExit(
            f"refusing to approve: the last verify ran against a snapshot "
            f"({rep.get('snapshot')}), not the live site. Runs against one saved page "
            "agree by construction, so this says nothing about stability. Verify "
            "live when the site is reachable, or pass --force to record a deliberate "
            "override (it is stamped into the file).")

    meta = dict(hints.get("meta") or {})
    meta["approved"] = True
    meta["approved_by"] = approved_by
    meta["approved_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    meta["form_hash"] = body_hash(hints)
    meta["verify"] = {
        "verdict": rep["verdict"], "counts": rep["counts"],
        "spread_pct": rep["spread_pct"], "fill_rates": rep["fill_rates"],
        "verified_at": rep["verified_at"],
        # Which of the two overrides was used, if either — a reader of the form
        # must be able to see that this approval rests on something weaker.
        "source": rep.get("source", "live"),
        "forced": bool(force and (rep["verdict"] == "FAIL"
                                  or rep.get("source") == "snapshot")),
    }
    # Patches the meta block in place — a full rewrite would delete the comments
    # that explain the form.
    stamp_meta(hints_path, meta)
    hints["meta"] = meta
    print(f"approved by {approved_by} — {hints_path}")
    print("Commit this file. The runner reads it as-is on every run; nothing is "
          "regenerated at crawl time.")
    return hints
