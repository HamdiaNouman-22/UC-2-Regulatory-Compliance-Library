"""
Staged LLM Analyzer benchmark harness.

Runs one regulation through StagedLLMAnalyzer.analyze() and records, per stage:
wall time, prompt/completion tokens, finish_reason, and real OpenRouter cost.

The analyzer itself is NOT modified or reimplemented -- instrumentation is a shim
around `requests.post` inside the analyzer module, so the code path measured is
byte-identical to production. Nothing is written to the database.

Usage
-----
  # before making optimizations
  python benchmarks/analyzer_bench.py --regulation-id 103296 --label baseline

  # after making optimizations
  python benchmarks/analyzer_bench.py --regulation-id 103296 --label optimized

  # side-by-side
  python benchmarks/analyzer_bench.py --compare baseline optimized

Output lands in benchmarks/runs/<label>/ :
  metrics.json     per-stage timing/tokens/cost + totals
  rows.json        exactly what analyze() returned
  stage4.md        the executive report, extracted for eyeballing
  calls/NN_*.txt   raw prompt and raw completion for every LLM call
  summary.md       human-readable run summary
"""

import argparse
import json
import os
import statistics
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


# ---------------------------------------------------------------------------
#  Instrumentation
# ---------------------------------------------------------------------------

class CallRecorder:
    """Wraps requests.post inside the analyzer module and records every call."""

    def __init__(self):
        self.calls = []
        self._orig_post = None

    def install(self):
        import processor.staged_LLM_Analyzer as mod

        self._orig_post = mod.requests.post
        recorder = self

        def instrumented_post(url, *args, **kwargs):
            payload = kwargs.get("json") or {}
            messages = payload.get("messages") or []
            prompt = ""
            for m in messages:
                if m.get("role") == "user":
                    prompt = m.get("content", "")

            t0 = time.perf_counter()
            resp = recorder._orig_post(url, *args, **kwargs)
            elapsed = time.perf_counter() - t0

            record = {
                "index": len(recorder.calls) + 1,
                "seconds": round(elapsed, 2),
                "http_status": resp.status_code,
                "model": payload.get("model"),
                "temperature": payload.get("temperature"),
                "max_tokens_requested": payload.get("max_tokens"),
                "prompt_chars": len(prompt),
                "prompt": prompt,
                "completion": "",
                "prompt_tokens": None,
                "completion_tokens": None,
                "total_tokens": None,
                "finish_reason": None,
                "generation_id": None,
                "cost_usd": None,
            }

            try:
                body = resp.json()
                usage = body.get("usage") or {}
                record["prompt_tokens"] = usage.get("prompt_tokens")
                record["completion_tokens"] = usage.get("completion_tokens")
                record["total_tokens"] = usage.get("total_tokens")
                record["generation_id"] = body.get("id")
                choices = body.get("choices") or []
                if choices:
                    record["finish_reason"] = choices[0].get("finish_reason")
                    record["completion"] = (choices[0].get("message") or {}).get("content") or ""
                # some providers report cached prefix tokens
                details = usage.get("prompt_tokens_details") or {}
                if details:
                    record["cached_prompt_tokens"] = details.get("cached_tokens")
            except Exception as e:  # never let instrumentation break the run
                record["instrumentation_error"] = str(e)

            recorder.calls.append(record)
            return resp

        mod.requests.post = instrumented_post

    def uninstall(self):
        if self._orig_post is not None:
            import processor.staged_LLM_Analyzer as mod
            mod.requests.post = self._orig_post

    def fetch_costs(self):
        """Ask OpenRouter what each generation actually cost. Best-effort."""
        import requests

        key = os.getenv("OPENROUTER_API_KEY")
        if not key:
            return
        for call in self.calls:
            gen_id = call.get("generation_id")
            if not gen_id:
                continue
            for attempt in range(4):
                try:
                    r = requests.get(
                        "https://openrouter.ai/api/v1/generation",
                        params={"id": gen_id},
                        headers={"Authorization": f"Bearer {key}"},
                        timeout=20,
                    )
                    if r.status_code == 404:  # not yet indexed
                        time.sleep(2.0 * (attempt + 1))
                        continue
                    r.raise_for_status()
                    data = (r.json() or {}).get("data") or {}
                    call["cost_usd"] = data.get("total_cost")
                    call["native_prompt_tokens"] = data.get("native_tokens_prompt")
                    call["native_completion_tokens"] = data.get("native_tokens_completion")
                    call["provider"] = data.get("provider_name")
                    break
                except Exception:
                    time.sleep(1.5 * (attempt + 1))


# ---------------------------------------------------------------------------
#  Document loading (mirrors apis/pipeline_api.py)
# ---------------------------------------------------------------------------

def load_document(regulation_id: int):
    from storage.mssql_repo import MSSQLRepository
    from processor.LlmAnalyzer import LLMAnalyzer

    repo = MSSQLRepository({
        "server":   os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver":   os.getenv("MSSQL_DRIVER"),
    })

    regulation = repo.get_regulation_by_id(regulation_id)
    if not regulation:
        raise SystemExit(f"Regulation {regulation_id} not found")

    extra_meta = regulation.get("extra_meta") or {}
    if isinstance(extra_meta, str):
        try:
            extra_meta = json.loads(extra_meta)
        except Exception:
            extra_meta = {}

    text_content, content_type = None, "html"
    org_pdf_text = extra_meta.get("org_pdf_text")
    if org_pdf_text and len(org_pdf_text) > 200:
        text_content, content_type = org_pdf_text, "pdf_text"
    if not text_content:
        doc_html = regulation.get("document_html")
        if doc_html and len(doc_html) > 200:
            text_content, content_type = doc_html, "html"
    if not text_content:
        raise SystemExit(f"No extractable text for regulation {regulation_id}")

    clean_text = LLMAnalyzer().normalize_input_text(text_content, content_type=content_type)
    if len(clean_text) < 200:
        raise SystemExit(f"Text too short after normalization ({len(clean_text)} chars)")

    raw_date = regulation.get("published_date")
    return {
        "regulation_id":    regulation_id,
        "title":            regulation.get("title") or "Untitled",
        "regulator":        regulation.get("regulator") or "",
        "reference_no":     regulation.get("reference_no") or "",
        "published_date":   str(raw_date)[:10] if raw_date else "",
        "category":         regulation.get("category") or "",
        "content_type":     content_type,
        "clean_text":       clean_text,
    }


# ---------------------------------------------------------------------------
#  Output shape metrics -- what the pipeline actually produced
# ---------------------------------------------------------------------------

def describe_output(rows):
    obligations, controls, crits, execs, types = [], [], [], [], []
    clarity, manual_review = [], 0

    for row in rows:
        analysis = json.loads(row.get("analysis_json") or "{}")
        for ob in analysis.get("obligations", []):
            obligations.append(ob.get("obligation_text", ""))
            if ob.get("criticality"):
                crits.append(ob["criticality"])
            if ob.get("execution_category"):
                execs.append(ob["execution_category"])
            if ob.get("obligation_type"):
                types.append(ob["obligation_type"])
            if isinstance(ob.get("clarity_score"), int):
                clarity.append(ob["clarity_score"])
            if ob.get("needs_manual_review"):
                manual_review += 1
        controls.extend(analysis.get("controls", []))

    def tally(seq):
        out = {}
        for v in seq:
            out[v] = out.get(v, 0) + 1
        return dict(sorted(out.items(), key=lambda kv: -kv[1]))

    stage4 = next((r.get("stage4_md") for r in rows if r.get("stage4_md")), "") or ""

    return {
        "requirements":            len(rows),
        "obligations":             len(obligations),
        "controls":                len(controls),
        "obligations_per_req":     round(len(obligations) / len(rows), 2) if rows else 0,
        "criticality":             tally(crits),
        "execution_category":      tally(execs),
        "obligation_type":         tally(types),
        "mean_clarity_score":      round(statistics.mean(clarity), 2) if clarity else None,
        "needs_manual_review":     manual_review,
        "empty_obligation_texts":  sum(1 for t in obligations if not t.strip()),
        "stage4_chars":            len(stage4),
        "stage4_has_all_sections": all(
            h in stage4 for h in ("## 1.", "## 2.", "## 3.", "## 4.", "## 5.", "## 6.")
        ),
        "stage4_table_rows":       stage4.count("\n|") if stage4 else 0,
    }


# ---------------------------------------------------------------------------
#  Run
# ---------------------------------------------------------------------------

STAGE_NAMES = {1: "stage1_extract", 2: "stage2_normalize", 3: "stage3_controls", 4: "stage4_report"}

# Blended DeepSeek v3.2 rates, USD per million tokens. Derived from the measured
# baseline run and used only as a fallback when OpenRouter's /generation endpoint
# has not indexed a call yet -- otherwise totals silently under-report.
PRICE_PROMPT_PER_M = float(os.getenv("BENCH_PRICE_PROMPT_PER_M", "0.247"))
PRICE_COMPLETION_PER_M = float(os.getenv("BENCH_PRICE_COMPLETION_PER_M", "0.384"))


def estimate_cost(prompt_tokens, completion_tokens):
    if prompt_tokens is None and completion_tokens is None:
        return None
    return round((prompt_tokens or 0) * PRICE_PROMPT_PER_M / 1e6
                 + (completion_tokens or 0) * PRICE_COMPLETION_PER_M / 1e6, 6)


def label_for_prompt(prompt: str, index: int) -> str:
    """Infer the stage from the prompt itself.

    Stage 3 now shards and stage 4 runs concurrently, so call order no longer
    maps onto stage number -- labelling by index mislabels every call after the
    second.
    """
    p = prompt or ""
    if "Extract structured requirements" in p:
        return "stage1_extract"
    if "refining previously extracted" in p:
        return "stage2_normalize"
    if "internal controls architect" in p:
        return "stage3_controls"
    if "regulatory impact document" in p:
        return "stage4_report"
    return STAGE_NAMES.get(index, f"call_{index}")


def load_text_file(path: str, title: str, regulation_id: int):
    """Replay a previously captured input_clean_text.txt.

    Keeps a comparison valid even if the source row is edited, re-imported or
    re-numbered in the database -- which has happened at least once here.
    """
    text = Path(path).read_text(encoding="utf-8")
    return {
        "regulation_id": regulation_id,
        "title":         title,
        "regulator":     "SAMA",
        "reference_no":  "",
        "published_date": "",
        "category":      "(replayed from file)",
        "content_type":  "pre-normalized",
        "clean_text":    text,
    }


def run(regulation_id: int, label: str, text_file: str = None, title: str = None):
    out_dir = RUNS_DIR / label
    (out_dir / "calls").mkdir(parents=True, exist_ok=True)

    if text_file:
        print(f"Replaying text from {text_file} ...")
        doc = load_text_file(text_file, title or "Replayed document", regulation_id)
    else:
        print(f"Loading regulation {regulation_id} ...")
        doc = load_document(regulation_id)
    print(f"  title      : {doc['title'][:70]}")
    print(f"  clean_text : {len(doc['clean_text'])} chars ({doc['content_type']})")

    from processor.staged_LLM_Analyzer import StagedLLMAnalyzer

    recorder = CallRecorder()
    recorder.install()
    analyzer = StagedLLMAnalyzer()

    print(f"Running analyze() [label={label}] ...")
    t0 = time.perf_counter()
    error = None
    rows = []
    try:
        rows = analyzer.analyze(
            text=doc["clean_text"],
            regulation_id=doc["regulation_id"],
            document_title=doc["title"],
            regulator=doc["regulator"],
            reference=doc["reference_no"],
            publication_date=doc["published_date"],
        )
    except Exception as e:
        error = f"{type(e).__name__}: {e}"
        print(f"  !! analyze() raised: {error}")
    finally:
        total_wall = time.perf_counter() - t0
        recorder.uninstall()

    print(f"  wall clock : {total_wall:.1f}s across {len(recorder.calls)} LLM call(s)")
    print("Fetching real cost from OpenRouter ...")
    recorder.fetch_costs()

    # ---- per-call breakdown -------------------------------------------------
    stages = []
    seen_counts = {}
    for call in recorder.calls:
        base = label_for_prompt(call["prompt"], call["index"])
        # Stage 3 shards: suffix them so each is individually visible.
        seen_counts[base] = seen_counts.get(base, 0) + 1
        name = base if seen_counts[base] == 1 else f"{base}#{seen_counts[base]}"
        stages.append({
            "call_index":           call["index"],
            "stage":                name,
            "cost_estimated_usd":   estimate_cost(call["prompt_tokens"], call["completion_tokens"]),
            "seconds":              call["seconds"],
            "prompt_tokens":        call["prompt_tokens"],
            "completion_tokens":    call["completion_tokens"],
            "total_tokens":         call["total_tokens"],
            "cached_prompt_tokens": call.get("cached_prompt_tokens"),
            "finish_reason":        call["finish_reason"],
            "truncated":            call["finish_reason"] == "length",
            "max_tokens_requested": call["max_tokens_requested"],
            "temperature":          call["temperature"],
            "cost_usd":             call["cost_usd"],
            "provider":             call.get("provider"),
        })
        stem = f"{call['index']:02d}_{name}"
        (out_dir / "calls" / f"{stem}_prompt.txt").write_text(call["prompt"], encoding="utf-8")
        (out_dir / "calls" / f"{stem}_completion.txt").write_text(call["completion"], encoding="utf-8")

    def total(field):
        vals = [s[field] for s in stages if isinstance(s[field], (int, float))]
        return round(sum(vals), 6) if vals else None

    # Group shards back into logical stages so before/after stays comparable
    # even though the optimized pipeline makes more, smaller calls.
    by_stage = {}
    for s in stages:
        key = s["stage"].split("#")[0]
        g = by_stage.setdefault(key, {"stage": key, "calls": 0, "seconds": 0.0,
                                      "prompt_tokens": 0, "completion_tokens": 0,
                                      "total_tokens": 0, "cost_estimated_usd": 0.0,
                                      "truncated": 0})
        g["calls"] += 1
        g["seconds"] += s["seconds"]
        for f in ("prompt_tokens", "completion_tokens", "total_tokens"):
            g[f] += s[f] or 0
        g["cost_estimated_usd"] += s["cost_estimated_usd"] or 0.0
        g["truncated"] += 1 if s["truncated"] else 0
    for g in by_stage.values():
        g["seconds"] = round(g["seconds"], 2)
        g["cost_estimated_usd"] = round(g["cost_estimated_usd"], 6)

    metrics = {
        "label":          label,
        "run_at_utc":     datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "regulation_id":  doc["regulation_id"],
        "title":          doc["title"],
        "regulator":      doc["regulator"],
        "category":       doc["category"],
        "reference_no":   doc["reference_no"],
        "input_chars":    len(doc["clean_text"]),
        "model":          analyzer.model,
        "error":          error,
        "totals": {
            "wall_seconds":      round(total_wall, 2),
            "llm_calls":         len(stages),
            "prompt_tokens":     total("prompt_tokens"),
            "completion_tokens": total("completion_tokens"),
            "total_tokens":      total("total_tokens"),
            "cost_usd":          total("cost_usd"),
            "cost_estimated_usd": round(sum(s["cost_estimated_usd"] or 0 for s in stages), 6),
            "truncated_calls":   sum(1 for s in stages if s["truncated"]),
        },
        "by_stage": list(by_stage.values()),
        "stages": stages,
        "output": describe_output(rows) if rows else None,
    }

    (out_dir / "metrics.json").write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "rows.json").write_text(
        json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    stage4 = next((r.get("stage4_md") for r in rows if r.get("stage4_md")), "") or ""
    (out_dir / "stage4.md").write_text(stage4, encoding="utf-8")
    (out_dir / "input_clean_text.txt").write_text(doc["clean_text"], encoding="utf-8")
    (out_dir / "summary.md").write_text(render_summary(metrics), encoding="utf-8")

    print(f"\n{render_summary(metrics)}")
    print(f"Written to {out_dir}")
    return metrics


# ---------------------------------------------------------------------------
#  Reporting
# ---------------------------------------------------------------------------

def _fmt(v, suffix="", dash="-"):
    if v is None:
        return dash
    if isinstance(v, float):
        return f"{v:,.4f}{suffix}" if suffix == "" else f"{v:,.2f}{suffix}"
    return f"{v:,}{suffix}"


def render_summary(m):
    t = m["totals"]
    lines = [
        f"# Analyzer run: {m['label']}",
        "",
        f"- Regulation : {m['regulation_id']} - {m['title']}",
        f"- Regulator  : {m['regulator']} / {m['category']}",
        f"- Model      : {m['model']}",
        f"- Input      : {m['input_chars']:,} chars",
        f"- Run at     : {m['run_at_utc']}",
    ]
    if m.get("error"):
        lines.append(f"- **ERROR**  : {m['error']}")
    lines += [
        "",
        "## Cost and latency by stage",
        "",
        "Seconds are summed across calls; concurrent stages therefore total to more",
        "than the wall clock. Cost is the token-based estimate so nothing is missing.",
        "",
        "| Stage | Calls | Sec (sum) | Prompt tok | Completion tok | Total tok | Cost USD | Truncated |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for g in m.get("by_stage", []):
        lines.append(
            f"| {g['stage']} | {g['calls']} | {g['seconds']:.1f} | {_fmt(g['prompt_tokens'])} | "
            f"{_fmt(g['completion_tokens'])} | {_fmt(g['total_tokens'])} | "
            f"{_fmt(g['cost_estimated_usd'])} | {g['truncated'] or '-'} |"
        )
    lines.append(
        f"| **TOTAL** | **{t['llm_calls']}** | | **{_fmt(t['prompt_tokens'])}** | "
        f"**{_fmt(t['completion_tokens'])}** | **{_fmt(t['total_tokens'])}** | "
        f"**{_fmt(t.get('cost_estimated_usd'))}** | **{t['truncated_calls'] or '-'}** |"
    )
    lines += ["", f"Wall clock: **{t['wall_seconds']:.1f}s**"]
    if t.get("cost_usd"):
        lines.append(f"OpenRouter-reported cost for the calls it had indexed: ${t['cost_usd']:.4f}")

    lines += ["", "<details><summary>Individual calls</summary>", "",
              "| Call | Stage | Sec | Prompt | Completion | finish_reason | max_tokens |",
              "|---:|---|---:|---:|---:|---|---:|"]
    for s in m["stages"]:
        flag = " **TRUNCATED**" if s["truncated"] else ""
        lines.append(
            f"| {s['call_index']} | {s['stage']} | {s['seconds']:.1f} | "
            f"{_fmt(s['prompt_tokens'])} | {_fmt(s['completion_tokens'])} | "
            f"{s['finish_reason']}{flag} | {_fmt(s['max_tokens_requested'])} |")
    lines += ["", "</details>"]

    o = m.get("output")
    if o:
        lines += [
            "",
            "## Output shape (quality baseline)",
            "",
            f"- Requirements        : {o['requirements']}",
            f"- Obligations         : {o['obligations']} ({o['obligations_per_req']} per requirement)",
            f"- Controls designed   : {o['controls']}",
            f"- Mean clarity score  : {o['mean_clarity_score']}",
            f"- needs_manual_review : {o['needs_manual_review']}",
            f"- Empty obligation text: {o['empty_obligation_texts']}",
            f"- Criticality         : {json.dumps(o['criticality'], ensure_ascii=False)}",
            f"- Execution category  : {json.dumps(o['execution_category'], ensure_ascii=False)}",
            f"- Obligation type     : {json.dumps(o['obligation_type'], ensure_ascii=False)}",
            f"- Stage 4 report      : {o['stage4_chars']:,} chars, "
            f"{o['stage4_table_rows']} table rows, all sections present: {o['stage4_has_all_sections']}",
        ]
    else:
        lines += ["", "## Output shape", "", "No rows produced."]

    return "\n".join(lines) + "\n"


def compare(label_a: str, label_b: str):
    ma = json.loads((RUNS_DIR / label_a / "metrics.json").read_text(encoding="utf-8"))
    mb = json.loads((RUNS_DIR / label_b / "metrics.json").read_text(encoding="utf-8"))
    ta, tb = ma["totals"], mb["totals"]

    def delta(a, b):
        if a in (None, 0) or b is None:
            return "-"
        return f"{(b - a) / a * 100:+.1f}%"

    lines = [
        f"# Comparison: {label_a} -> {label_b}",
        "",
        f"Regulation {ma['regulation_id']} - {ma['title']}",
        "",
        "## Cost and latency",
        "",
        f"| Metric | {label_a} | {label_b} | Change |",
        "|---|---:|---:|---:|",
    ]
    for key, name in [
        ("wall_seconds", "Wall clock (s)"),
        ("llm_calls", "LLM calls"),
        ("prompt_tokens", "Prompt tokens"),
        ("completion_tokens", "Completion tokens"),
        ("total_tokens", "Total tokens"),
        ("cost_estimated_usd", "Cost (USD, token-based)"),
        ("truncated_calls", "Truncated calls"),
    ]:
        lines.append(f"| {name} | {_fmt(ta.get(key))} | {_fmt(tb.get(key))} | {delta(ta.get(key), tb.get(key))} |")

    oa, ob = ma.get("output") or {}, mb.get("output") or {}
    lines += [
        "",
        "## Output shape -- these should stay materially the same",
        "",
        f"| Metric | {label_a} | {label_b} | Change |",
        "|---|---:|---:|---:|",
    ]
    for key, name in [
        ("requirements", "Requirements"),
        ("obligations", "Obligations"),
        ("controls", "Controls"),
        ("mean_clarity_score", "Mean clarity"),
        ("needs_manual_review", "needs_manual_review"),
        ("stage4_table_rows", "Stage 4 table rows"),
    ]:
        lines.append(f"| {name} | {_fmt(oa.get(key))} | {_fmt(ob.get(key))} | {delta(oa.get(key), ob.get(key))} |")

    for key, name in [("criticality", "Criticality"), ("execution_category", "Execution category")]:
        lines += [
            "",
            f"**{name}**",
            "",
            f"- {label_a}: {json.dumps(oa.get(key, {}), ensure_ascii=False)}",
            f"- {label_b}: {json.dumps(ob.get(key, {}), ensure_ascii=False)}",
        ]

    text = "\n".join(lines) + "\n"
    dest = RUNS_DIR / f"compare_{label_a}_vs_{label_b}.md"
    dest.write_text(text, encoding="utf-8")
    print(text)
    print(f"Written to {dest}")


def recompute(label: str):
    """Re-derive stage labels and cost estimates for a completed run.

    Makes no LLM calls -- it re-reads the saved prompts from disk. Needed for
    runs captured before stage labelling became content-based.
    """
    out_dir = RUNS_DIR / label
    m = json.loads((out_dir / "metrics.json").read_text(encoding="utf-8"))

    prompts = sorted((out_dir / "calls").glob("*_prompt.txt"),
                     key=lambda p: int(p.name.split("_")[0]))
    if len(prompts) != len(m["stages"]):
        raise SystemExit(f"{len(prompts)} prompt files vs {len(m['stages'])} recorded calls")

    seen = {}
    renames = []
    for stage, ppath in zip(m["stages"], prompts):
        base = label_for_prompt(ppath.read_text(encoding="utf-8"), stage["call_index"])
        seen[base] = seen.get(base, 0) + 1
        stage["stage"] = base if seen[base] == 1 else f"{base}#{seen[base]}"
        stage["cost_estimated_usd"] = estimate_cost(
            stage["prompt_tokens"], stage["completion_tokens"])
        idx = ppath.name.split("_")[0]
        for kind in ("prompt", "completion"):
            src = out_dir / "calls" / f"{idx}_{ppath.name.split('_', 1)[1].replace('_prompt.txt', '')}_{kind}.txt"
            dst = out_dir / "calls" / f"{idx}_{stage['stage'].replace('#', '_')}_{kind}.txt"
            if src.exists() and src != dst:
                renames.append((src, dst))

    by_stage = {}
    for s in m["stages"]:
        key = s["stage"].split("#")[0]
        g = by_stage.setdefault(key, {"stage": key, "calls": 0, "seconds": 0.0,
                                      "prompt_tokens": 0, "completion_tokens": 0,
                                      "total_tokens": 0, "cost_estimated_usd": 0.0,
                                      "truncated": 0})
        g["calls"] += 1
        g["seconds"] += s["seconds"]
        for f in ("prompt_tokens", "completion_tokens", "total_tokens"):
            g[f] += s[f] or 0
        g["cost_estimated_usd"] += s["cost_estimated_usd"] or 0.0
        g["truncated"] += 1 if s["truncated"] else 0
    for g in by_stage.values():
        g["seconds"] = round(g["seconds"], 2)
        g["cost_estimated_usd"] = round(g["cost_estimated_usd"], 6)

    m["by_stage"] = list(by_stage.values())
    m["totals"]["cost_estimated_usd"] = round(
        sum(s["cost_estimated_usd"] or 0 for s in m["stages"]), 6)

    for src, dst in renames:
        src.rename(dst)
    (out_dir / "metrics.json").write_text(
        json.dumps(m, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "summary.md").write_text(render_summary(m), encoding="utf-8")
    print(render_summary(m))
    print(f"Recomputed {label} ({len(renames)} call files renamed)")


def main():
    p = argparse.ArgumentParser(description="Benchmark the staged LLM analyzer")
    p.add_argument("--regulation-id", type=int, help="regulation id to analyze")
    p.add_argument("--label", type=str, help="name for this run, e.g. baseline")
    p.add_argument("--compare", nargs=2, metavar=("A", "B"), help="compare two existing runs")
    p.add_argument("--recompute", type=str, metavar="LABEL",
                   help="re-derive labels and cost for a finished run; makes no LLM calls")
    p.add_argument("--text-file", type=str,
                   help="replay a saved input_clean_text.txt instead of reading the DB")
    p.add_argument("--title", type=str, help="document title when using --text-file")
    args = p.parse_args()

    if args.recompute:
        recompute(args.recompute)
        return
    if args.compare:
        compare(*args.compare)
        return
    if not args.label or (not args.regulation_id and not args.text_file):
        p.error("need --label plus --regulation-id or --text-file (or use --compare A B)")
    run(args.regulation_id or 0, args.label, args.text_file, args.title)


if __name__ == "__main__":
    main()
