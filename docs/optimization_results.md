# Optimization results — before vs after

Same document, same model, measured twice: regulation **103296 (SAMA Banking Control Law)**,
`deepseek/deepseek-v3.2`, 19,274 chars of normalized input.

- Before: [baseline_run_103296.md](baseline_run_103296.md) — `benchmarks/runs/baseline/`
- After: `benchmarks/runs/optimized/`
- Diff: `benchmarks/runs/compare_baseline_vs_optimized.md`

Reproduce:

```
python benchmarks/analyzer_bench.py --regulation-id 103296 --label optimized
python benchmarks/analyzer_bench.py --compare baseline optimized
```

## Headline

| Metric | Before | After | Change |
|---|---:|---:|---:|
| **Wall clock** | 614.5s | **253.3s** | **−58.8%** |
| Prompt tokens | 15,133 | 11,144 | −26.4% |
| Completion tokens | 19,627 | 10,872 | −44.6% |
| **Total tokens** | **34,760** | **22,016** | **−36.7%** |
| **Cost / document** | ~$0.0113 | **~$0.0069** | **−38.6%** |
| Truncated calls | 1 | **0** | fixed |
| **Controls produced** | **0** | **22** | fixed |

**10m 15s → 4m 13s per document, at 61% of the cost, and it now produces the controls it was
silently dropping.**

LLM calls went 4 → 9 because stage 3 shards per requirement group. More calls, less total work:
each shard is small, they run concurrently, and none can truncate.

## Per stage

| Stage | Calls | Sec (sum) | Prompt | Completion | Notes |
|---|---:|---:|---:|---:|---|
| 1 extract | 1 | 95.0 | 4,448 | 2,902 | unchanged reasoning; compact JSON only |
| 2 normalize | 1 | 104.1 | 2,609 | 3,207 | was 3,024 / **7,177** |
| 3 controls | 6 | 172.6 | 3,632 | 4,347 | was 1 call, 5,731 / 8,000 → **0 output** |
| 4 report | 1 | 12.6 | 455 | 221 | was 1,931 / 1,582 |

Stage 2's completion dropped **55%** (7,177 → 3,207) purely by not re-transcribing text and not
emitting indentation. Stage 4's prompt dropped **76%** because it now receives a statistical digest
instead of three truncated JSON blobs.

Sum of stage seconds (384s) exceeds wall clock (253s) — that gap is the concurrency: stage 4 and the
six stage 3 shards overlap. Call order in `metrics.json` confirms it, stage 4 completed as call 3,
before any stage 3 shard.

## Quality held

The constraint was to keep the staged output quality. Every stage is still its own focused call.

| Metric | Before | After |
|---|---:|---:|
| Requirements | 7 | 7 |
| Obligations | 38 | 39 |
| Obligations per requirement | 5.43 | 5.57 |
| Mean clarity score | 4.68 | 4.87 |
| `needs_manual_review` | 2 | 1 |
| Empty obligation texts | 0 | 0 |
| Criticality | High 31, Med 7 | High 34, Med 5 |
| Execution category | Ongoing 20, Gov 8, OneTime 6, Report 4 | Ongoing 22, Gov 10, Report 4, OneTime 3 |

Same requirement count, one more obligation, slightly higher clarity, fewer items needing manual
review. The small distribution shifts are ordinary run-to-run variance at temperature 0.1 — the
shape is the same.

**Taxonomy violations are gone.** The baseline emitted `obligation_type: "Corrective"` on 4 of 38
obligations and `evidence_expected: "License"`, neither of which the prompt permits. The optimized
run reports only permitted values (Preventive 19, Governance 14, Reporting 6) because out-of-taxonomy
values are now coerced in Python and the affected obligation is flagged `needs_manual_review`.

## The two defects are fixed

**Stage 3 no longer silently returns nothing.** It produced all 22 controls. Three things had to
change together: an explicit `max_tokens`, sharding per requirement group, and an adaptive retry that
halves a shard if it still truncates. Truncation now raises `TruncatedResponseError` instead of being
parsed into `{"requirements": []}` and discarded.

**Stage 4 can no longer fabricate.** Sections 3, 4 and 5 are rendered in Python from the assembled
rows; the model only writes the three prose sections. Verified on the optimized run:

```
controls in rows.json : 22
rows in report table  : 22
report titles NOT backed by data: NONE - fully grounded
```

And when stage 3 genuinely produces nothing, the report now says so explicitly rather than inventing
a table:

> _No controls were produced despite N ongoing-control obligation(s). This indicates a Stage 3
> failure and requires review._

The report also got materially more complete — 71 table rows vs 29 — because the old version fed the
model JSON truncated at 3,000 chars, so it never saw most of the data.

## What changed in the code

`processor/staged_LLM_Analyzer.py` — the four reasoning stages are untouched in substance. Each is
still its own call with its own focused prompt.

| Change | Plan ref |
|---|---|
| Compact/minified JSON demanded in every prompt | §1c |
| Stage 2 emits classification deltas with short keys, rehydrated in Python | §1a |
| Stage 3 receives only `{id, text}`, returns controls keyed by id with short field names | §1b |
| Exact-duplicate obligations removed by string equality in Python | §3a |
| Stage 4 tables rendered from rows; LLM writes only sections 1, 2, 6 | §3b |
| Enum validation with coercion + `needs_manual_review` flagging | §3c |
| Stage 3 explicit `max_tokens`, sharded per requirement, adaptive split on truncation | §2b, §4 |
| Stage 4 runs concurrently with stage 3 | §2c |
| Retry with backoff on 408/409/425/429/5xx; `finish_reason == length` raises | §4 |
| `response_format: {"type": "json_object"}` on all JSON stages | §4 |
| Module-level semaphore bounds concurrent calls (`LLM_MAX_CONCURRENCY`, default 8) | §2 |

`orchestrator/orchestrator.py` — both document loops now go through `_process_docs()`, which runs a
`ThreadPoolExecutor` (§2a). A failing document is logged and skipped instead of aborting the batch.
Set `DOC_MAX_WORKERS=1` to restore serial behaviour; default is 4.

### Storage shape is unchanged — deliberately

`normalized_obligations` is read in about ten places in `apis/pipeline_api.py`, so the lean model
output is rehydrated into the **exact** historical structure before storage. `stage1_json`,
`stage2_json`, `stage3_json` and `analysis_json` keep their field names and nesting, row keys are
identical, and `schema_version` stays `v2`. No consumer, export path or UI needs changing, and the
`schema_version` bump to `v3` that the plan anticipated is not required.

## Tests

`tests/test_staged_analyzer_offline.py` — 37 checks over every non-LLM path, no API calls or network
needed:

```
python tests/test_staged_analyzer_offline.py
```

Covers exact dedup, delta rehydration, split-obligation id resolution and source-reference
inheritance, enum coercion for all four taxonomies, clarity clamping, unknown-id rejection, control
field expansion, stage 4 grounding (asserts fabrication is impossible when stage 3 is empty), and
byte-level row/JSON shape compatibility with the old schema.

## Still outstanding

Not implemented, from the plan:

- **Content-hash cache** (§4) — skip all stages when re-analysing byte-identical text. Matters for
  the `monitoring_status == "modified"` reprocessing path.
- **Binding-language regex gate** (§4) — skip stage 1 when no `must|shall|يجب` appears.
- **Prompt-prefix reordering for cache hits** (§5) — stage 1 still interpolates `regulator` /
  `reference` into the middle of the prompt, breaking the shared prefix.
- **Model tiering** (§5) and **stage 2 sharding** (§2d) — both need a side-by-side quality check.
- **`apis/pipeline_api.py` batch endpoints** — only the orchestrator loops were parallelised. The
  API's own batch paths still run serially.

## Expected batch effect

Per-document latency is now 253s. With `DOC_MAX_WORKERS=4` and the semaphore capping concurrent LLM
calls at 8, a 20-document batch should land around **20–25 minutes**, against ~3.4 hours before.

This is a projection from the single-document measurement, **not measured.** Worth confirming on a
real batch before relying on it — the semaphore may become the binding constraint since stage 3
already opens up to 4 concurrent calls per document.
