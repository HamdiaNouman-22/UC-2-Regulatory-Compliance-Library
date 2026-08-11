"""
RequirementMatcher benchmark harness.

Mirrors `_run_upload_requirement_matching` in apis/pipeline_api.py exactly, but
reads the obligations from a saved analyzer run instead of the database, so the
same input can be replayed before and after a change.

Like analyzer_bench.py, the matcher itself is NOT modified -- instrumentation is
a shim around `requests.post` inside the matcher module. Nothing is written to
the database.

Usage
-----
  python benchmarks/matcher_bench.py --from-run optimized --label matcher_baseline
  python benchmarks/matcher_bench.py --from-run optimized --label matcher_batched
  python benchmarks/matcher_bench.py --compare matcher_baseline matcher_batched

Output in benchmarks/runs/<label>/ :
  metrics.json     timing, per-call tokens, totals
  mappings.json    the matching decisions -- this is the quality comparison
  summary.md       human-readable
  calls/           raw prompt + completion for every call
"""

import argparse
import collections
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
os.chdir(REPO)

from dotenv import load_dotenv
load_dotenv(REPO / ".env", override=True)

RUNS_DIR = Path(__file__).resolve().parent / "runs"
PRICE_PROMPT_PER_M = float(os.getenv("BENCH_PRICE_PROMPT_PER_M", "0.247"))
PRICE_COMPLETION_PER_M = float(os.getenv("BENCH_PRICE_COMPLETION_PER_M", "0.384"))


class CallRecorder:
    """Wraps requests.post inside the matcher module."""

    def __init__(self):
        self.calls = []
        self._orig = None

    def install(self):
        # The HTTP call may live in the matcher itself (original) or in the
        # shared client (after the refactor). Patch both so the harness keeps
        # measuring either way.
        import processor.requirement_matcher as matcher_mod
        try:
            import processor.llm_client as client_mod
        except ImportError:
            client_mod = None

        self._patched = []
        rec = self

        def make(orig):
            def instrumented(url, *args, **kwargs):
                payload = kwargs.get("json") or {}
                prompt = ""
                for m in payload.get("messages") or []:
                    if m.get("role") == "user":
                        prompt = m.get("content", "")
                t0 = time.perf_counter()
                resp = orig(url, *args, **kwargs)
                rec.calls.append(rec._record(resp, prompt, time.perf_counter() - t0, payload))
                return resp
            return instrumented

        # Both modules do `import requests`, so mod.requests is the SAME module
        # object in each. Patching per-importer would wrap the wrapper and count
        # every call twice -- dedupe on the requests module itself.
        seen = set()
        for mod in (matcher_mod, client_mod):
            if mod is None or not hasattr(mod, "requests"):
                continue
            rq = mod.requests
            if id(rq) in seen:
                continue
            seen.add(id(rq))
            orig = rq.post
            self._patched.append((mod, orig))
            rq.post = make(orig)

    def _record(self, resp, prompt, elapsed, payload):
        r = {
            "index": len(self.calls) + 1,
            "seconds": round(elapsed, 2),
            "http_status": resp.status_code,
            "max_tokens_requested": payload.get("max_tokens"),
            "prompt_chars": len(prompt),
            "prompt": prompt,
            "completion": "",
            "prompt_tokens": None,
            "completion_tokens": None,
            "finish_reason": None,
        }
        try:
            b = resp.json()
            u = b.get("usage") or {}
            r["prompt_tokens"] = u.get("prompt_tokens")
            r["completion_tokens"] = u.get("completion_tokens")
            ch = (b.get("choices") or [{}])[0]
            r["finish_reason"] = ch.get("finish_reason")
            r["completion"] = (ch.get("message") or {}).get("content") or ""
            r["provider"] = b.get("provider")
        except Exception as e:
            r["instrumentation_error"] = str(e)
        return r

    def uninstall(self):
        for mod, orig in getattr(self, "_patched", []):
            mod.requests.post = orig
        self._patched = []


def build_inputs(from_run: str):
    """Same assembly as _run_upload_requirement_matching (pipeline_api.py:449)."""
    rows = json.loads((RUNS_DIR / from_run / "rows.json").read_text(encoding="utf-8"))

    extracted = []
    for r in rows:
        s2 = r.get("stage2_json") or {}
        if isinstance(s2, str):
            try:
                s2 = json.loads(s2)
            except Exception:
                s2 = {}
        for ob in s2.get("normalized_obligations", []):
            extracted.append({
                "requirement_text": ob["obligation_text"],
                "department":       "",
                "risk_level":       ob.get("criticality", "Medium"),
                "controls":         [],
                "kpis":             [],
                "_obligation_id":   ob["obligation_id"],
                "_requirement_id":  r.get("requirement_id"),
            })

    from storage.mssql_repo import MSSQLRepository
    repo = MSSQLRepository({
        "server":   os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver":   os.getenv("MSSQL_DRIVER"),
    })
    return extracted, {
        "existing_requirements":  repo.get_all_compliance_requirements(),
        "existing_controls":      repo.get_all_demo_controls(),
        "existing_kpis":          repo.get_all_demo_kpis(),
        "linked_controls_by_req": repo.get_linked_controls_by_requirement(),
        "linked_kpis_by_req":     repo.get_linked_kpis_by_requirement(),
    }


def run(from_run: str, label: str, regulation_id: int = 103296):
    out = RUNS_DIR / label
    (out / "calls").mkdir(parents=True, exist_ok=True)

    print(f"Loading obligations from run '{from_run}' ...")
    extracted, catalogue = build_inputs(from_run)
    print(f"  obligations to match : {len(extracted)}")
    print(f"  existing requirements: {len(catalogue['existing_requirements'])}")
    print(f"  existing controls    : {len(catalogue['existing_controls'])}")
    print(f"  existing KPIs        : {len(catalogue['existing_kpis'])}")

    from processor.requirement_matcher import RequirementMatcher

    rec = CallRecorder()
    rec.install()
    deterministic = os.getenv("MATCHER_DETERMINISTIC", "0") not in ("0", "false", "False")
    print(f"  deterministic mode   : {deterministic}")
    matcher = RequirementMatcher(deterministic=deterministic)

    print(f"Running match_requirements() [label={label}] ...")
    t0 = time.perf_counter()
    error, results = None, {}
    try:
        results = matcher.match_requirements(
            regulation_id=regulation_id,
            extracted_requirements=extracted,
            **catalogue,
        )
    except Exception as e:
        error = f"{type(e).__name__}: {e}"
        print(f"  !! raised: {error}")
    finally:
        wall = time.perf_counter() - t0
        rec.uninstall()

    print(f"  wall clock: {wall:.1f}s across {len(rec.calls)} LLM call(s)")

    for c in rec.calls:
        stem = f"{c['index']:03d}"
        (out / "calls" / f"{stem}_prompt.txt").write_text(c["prompt"], encoding="utf-8")
        (out / "calls" / f"{stem}_completion.txt").write_text(c["completion"], encoding="utf-8")

    pt = sum(c["prompt_tokens"] or 0 for c in rec.calls)
    ct = sum(c["completion_tokens"] or 0 for c in rec.calls)
    mappings = results.get("requirement_mappings", [])

    metrics = {
        "label": label,
        "run_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "from_run": from_run,
        "regulation_id": regulation_id,
        "error": error,
        "inputs": {
            "obligations": len(extracted),
            "existing_requirements": len(catalogue["existing_requirements"]),
            "existing_controls": len(catalogue["existing_controls"]),
            "existing_kpis": len(catalogue["existing_kpis"]),
        },
        "totals": {
            "wall_seconds": round(wall, 2),
            "llm_calls": len(rec.calls),
            "prompt_tokens": pt,
            "completion_tokens": ct,
            "total_tokens": pt + ct,
            "cost_estimated_usd": round(pt * PRICE_PROMPT_PER_M / 1e6
                                        + ct * PRICE_COMPLETION_PER_M / 1e6, 6),
            "truncated_calls": sum(1 for c in rec.calls if c["finish_reason"] == "length"),
        },
        "output": describe(results, len(extracted)),
        "calls": [{k: v for k, v in c.items() if k not in ("prompt", "completion")}
                  for c in rec.calls],
    }

    (out / "metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2),
                                      encoding="utf-8")
    (out / "mappings.json").write_text(json.dumps(mappings, ensure_ascii=False, indent=2),
                                       encoding="utf-8")
    (out / "summary.md").write_text(render(metrics), encoding="utf-8")
    print("\n" + render(metrics))
    print(f"Written to {out}")


def describe(results, n_expected):
    m = results.get("requirement_mappings", [])
    statuses = collections.Counter(x.get("match_status") for x in m)
    matched_ids = collections.Counter(
        x.get("matched_requirement_id") for x in m if x.get("matched_requirement_id"))
    return {
        "mappings_returned": len(m),
        "obligations_in": n_expected,
        "missing_mappings": n_expected - len(m),
        "match_status": dict(statuses),
        "distinct_matched_requirements": len(matched_ids),
        "control_links": len(results.get("control_links", [])),
        "kpi_links": len(results.get("kpi_links", [])),
        "new_controls": len(results.get("new_controls_to_insert", [])),
        "new_kpis": len(results.get("new_kpis_to_insert", [])),
        "empty_explanations": sum(1 for x in m if not (x.get("match_explanation") or "").strip()),
    }


def render(m):
    t, o, i = m["totals"], m["output"], m["inputs"]
    lines = [
        f"# Matcher run: {m['label']}", "",
        f"- Obligations in       : {i['obligations']}",
        f"- Existing requirements: {i['existing_requirements']}",
        f"- Run at               : {m['run_at_utc']}",
    ]
    if m.get("error"):
        lines.append(f"- **ERROR**: {m['error']}")
    lines += [
        "", "## Cost and latency", "",
        f"| Metric | Value |", "|---|---:|",
        f"| Wall clock (s) | {t['wall_seconds']:,.1f} |",
        f"| LLM calls | {t['llm_calls']:,} |",
        f"| Prompt tokens | {t['prompt_tokens']:,} |",
        f"| Completion tokens | {t['completion_tokens']:,} |",
        f"| Total tokens | {t['total_tokens']:,} |",
        f"| Cost USD (est) | {t['cost_estimated_usd']:.4f} |",
        f"| Truncated calls | {t['truncated_calls']} |",
        "", "## Output (quality baseline)", "",
        f"- Mappings returned  : {o['mappings_returned']} of {o['obligations_in']} obligations",
        f"- Missing mappings   : {o['missing_mappings']}",
        f"- Match status       : {json.dumps(o['match_status'], ensure_ascii=False)}",
        f"- Distinct matched   : {o['distinct_matched_requirements']}",
        f"- Empty explanations : {o['empty_explanations']}",
        f"- Control links      : {o['control_links']}   New controls: {o['new_controls']}",
        f"- KPI links          : {o['kpi_links']}   New KPIs: {o['new_kpis']}",
    ]
    return "\n".join(lines) + "\n"


def compare(a, b):
    ma = json.loads((RUNS_DIR / a / "metrics.json").read_text(encoding="utf-8"))
    mb = json.loads((RUNS_DIR / b / "metrics.json").read_text(encoding="utf-8"))

    def d(x, y):
        if not x:
            return "-"
        return f"{(y - x) / x * 100:+.1f}%"

    lines = [f"# Matcher comparison: {a} -> {b}", ""]

    # A run that crashed produces zero calls and zero tokens, which renders as
    # "-100%" on every metric and reads like a spectacular win. Say so loudly.
    for label, m in ((a, ma), (b, mb)):
        if m.get("error"):
            lines += [f"> **{label} FAILED — these numbers are meaningless.**",
                      f"> `{m['error']}`", ""]
        elif m["totals"]["llm_calls"] == 0:
            lines += [f"> **{label} made zero LLM calls — nothing ran.**", ""]

    lines += ["## Cost and latency", "",
             f"| Metric | {a} | {b} | Change |", "|---|---:|---:|---:|"]
    for k, n in [("wall_seconds", "Wall clock (s)"), ("llm_calls", "LLM calls"),
                 ("prompt_tokens", "Prompt tokens"), ("completion_tokens", "Completion tokens"),
                 ("total_tokens", "Total tokens"), ("cost_estimated_usd", "Cost USD"),
                 ("truncated_calls", "Truncated")]:
        va, vb = ma["totals"].get(k), mb["totals"].get(k)
        lines.append(f"| {n} | {va:,} | {vb:,} | {d(va, vb)} |" if isinstance(va, int)
                     else f"| {n} | {va} | {vb} | {d(va, vb)} |")

    lines += ["", "## Matching decisions -- these must stay equivalent", "",
              f"| Metric | {a} | {b} |", "|---|---|---|"]
    for k, n in [("mappings_returned", "Mappings returned"), ("missing_mappings", "Missing"),
                 ("match_status", "Match status"),
                 ("distinct_matched_requirements", "Distinct matched"),
                 ("empty_explanations", "Empty explanations")]:
        lines.append(f"| {n} | {ma['output'].get(k)} | {mb['output'].get(k)} |")

    # per-obligation agreement
    la = {x["extracted_requirement_text"]: (x.get("match_status"), x.get("matched_requirement_id"))
          for x in json.loads((RUNS_DIR / a / "mappings.json").read_text(encoding="utf-8"))}
    lb = {x["extracted_requirement_text"]: (x.get("match_status"), x.get("matched_requirement_id"))
          for x in json.loads((RUNS_DIR / b / "mappings.json").read_text(encoding="utf-8"))}
    shared = set(la) & set(lb)
    agree = sum(1 for k in shared if la[k] == lb[k])
    lines += ["", "## Per-obligation agreement", "",
              f"- Obligations present in both runs: {len(shared)}",
              f"- Identical verdict (status + matched id): {agree}"
              + (f" ({agree/len(shared)*100:.0f}%)" if shared else ""),
              f"- Differing verdicts: {len(shared) - agree}"]
    if shared and agree < len(shared):
        lines += ["", "Differences:", ""]
        for k in list(shared):
            if la[k] != lb[k]:
                lines.append(f"- `{k[:70]}...`  {a}={la[k]}  {b}={lb[k]}")

    text = "\n".join(lines) + "\n"
    dest = RUNS_DIR / f"compare_{a}_vs_{b}.md"
    dest.write_text(text, encoding="utf-8")
    print(text)
    print(f"Written to {dest}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--from-run", default="optimized",
                   help="analyzer run label to take obligations from")
    p.add_argument("--label")
    p.add_argument("--regulation-id", type=int, default=103296)
    p.add_argument("--compare", nargs=2, metavar=("A", "B"))
    a = p.parse_args()
    if a.compare:
        compare(*a.compare)
        return
    if not a.label:
        p.error("--label is required (or use --compare A B)")
    run(a.from_run, a.label, a.regulation_id)


if __name__ == "__main__":
    main()
