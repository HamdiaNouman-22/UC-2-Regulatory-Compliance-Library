# Baseline run — Banking Control Law (regulation 103296)

Measured **2026-08-03** against the current `processor/staged_LLM_Analyzer.py`, before any
optimization work. This is the reference the "after" run gets compared against.

Reproduce with:

```
python benchmarks/analyzer_bench.py --regulation-id 103296 --label baseline
```

Artifacts in `benchmarks/runs/baseline/` — `metrics.json`, `rows.json`, `stage4.md`,
`summary.md`, and the raw prompt + completion for all four calls under `calls/`.

The harness instruments `requests.post` *inside* the analyzer module, so the measured code path is
identical to production. Nothing was written to the database.

## Document

| | |
|---|---|
| Regulation | 103296 — Banking Control Law |
| Regulator | SAMA / Laws and Implementing Regulations |
| Model | `deepseek/deepseek-v3.2` |
| Input after normalization | 19,274 chars |

## Measured cost and latency

| Stage | Sec | Prompt tok | Completion tok | Total | Cost USD | finish_reason |
|---|---:|---:|---:|---:|---:|---|
| 1 extract | 50.8 | 4,447 | 2,868 | 7,315 | 0.0022 | stop |
| 2 normalize | 229.0 | 3,024 | 7,177 | 10,201 | 0.0035 | stop |
| 3 controls | 278.6 | 5,731 | 8,000 | 13,731 | 0.0038 | **length — TRUNCATED** |
| 4 report | 55.5 | 1,931 | 1,582 | 3,513 | n/a | stop |
| **Total** | **614.5** | **15,133** | **19,627** | **34,760** | **~0.011** | |

*(Stage 4's cost lookup hadn't been indexed by OpenRouter when queried; total is the sum of the
three known values plus an estimate for stage 4.)*

**10 minutes 15 seconds and ~$0.011 for one document.**

### Wall time is almost purely a function of completion tokens

Observed generation throughput is ~30 tokens/sec, consistently across stages:

- stage 2: 7,177 tok / 229.0s = 31 tok/s
- stage 3: 8,000 tok / 278.6s = 29 tok/s

19,627 completion tokens ÷ 30 = 654s, against 614.5s measured. **Prompt size barely matters for
latency; output size is essentially the whole story.** Every token we stop the model from writing
is ~33ms saved.

## Finding 1 — Stage 3 produced nothing, at 45% of the runtime

Stage 3 hit `finish_reason: length` at exactly its 8,000-token cap. The JSON was cut mid-object,
`_parse_json` failed, and the exception handler returned `{"requirements": []}`.

```
JSON parse failed: Unterminated string starting at: line 517 column 46 (char 46015)
```

Result: **0 controls.** All 7 rows have `stage3_json = {}`. The document had 20 `Ongoing_Control`
obligations that should each have produced one.

The cost of that: **278.6 seconds — 45% of total wall clock — and 13,731 tokens, for zero output.**
This is the bug predicted at `staged_LLM_Analyzer.py:91-94`; it is now confirmed on a real document.

## Finding 2 — Stage 4 fabricated a control inventory

This is the serious one.

Stage 4 received exactly this as its Stage 3 input (verifiable in
`benchmarks/runs/baseline/calls/04_stage4_report_prompt.txt`):

```
Stage 3:
{"requirements": []}
```

It nonetheless produced a fully-populated Control Engineering Summary — 10 controls with named
owners, execution types, frequencies and residual-risk ratings:

```
| Banking License Verification | Legal & Compliance | Preventive | Automated Check | Continuous | Low |
| Capital & Deposit Ratio Monitor | Finance / Risk | Preventive | Automated Calculation & Alert | Daily | Medium |
| Single Borrower Exposure Limit | Credit Risk | Preventive | Automated Limit Check | Transactional | Medium |
...
```

**None of these exist in the pipeline's data.** The database has zero controls for this regulation;
the executive report presents ten. The values don't even respect the Stage 3 taxonomy — `Execution`
is specified as `Manual | Automated | Hybrid` but the report emits "Automated Check", "Manual
Procedure", "Automated Calculation & Alert".

This happens despite the system prompt saying "Never hallucinate" and the Stage 4 prompt saying
"Use only information from the inputs. Do not invent regulatory content."

### How widespread is it in production

Queried against `compliance_analysis` where `schema_version = 'v2'` (1,000 regulations):

| | Count |
|---|---:|
| Regulations analysed | 1,000 |
| — have `Ongoing_Control` obligations, so expect controls | 696 |
| — of those, produced **zero** controls (stage 3 genuinely failed) | **22 (3%)** |
| — legitimately zero (no ongoing obligations at all) | 304 |

So **stage 3 truncation affects ~3% of production regulations, not a third** — the larger
zero-control population is legitimate.

The fabrication, however, is not rare. Sampling 200 regulations that have zero controls in their
data but a section 5 in their report:

| | Count |
|---|---:|
| Section 5 contains **fabricated** control rows | **147 (74%)** |
| Section 5 correctly empty or states none | 53 |

Extrapolating over the 321 zero-control regulations that carry a section 5, roughly **235
regulations in production are shipping an executive report with an invented control inventory.**

Regulation 73 is the clearest illustration — with no regulatory content to work from, it invented
controls about the document itself: "PDF Format and Metadata Validation", "Document Structure and ID
Integrity Check".

**This is a data-integrity issue independent of cost, and §3b of the optimization plan fixes it as a
side effect** — rendering section 5 from the actual rows makes fabrication structurally impossible.

## Finding 3 — ~26% of every output is pretty-print whitespace

The model emits 4-space-indented JSON. Measured against the minified equivalent:

| Stage | Raw chars | Minified | Whitespace | % |
|---|---:|---:|---:|---:|
| 1 | 14,006 | 10,565 | 3,441 | 24.6% |
| 2 | 35,884 | 25,618 | 10,266 | 28.6% |
| 3 | 46,122 | 34,342 | 11,780 | 25.5% |

Roughly **5,000 completion tokens per document are indentation** — about **160 seconds of the
614-second runtime spent generating spaces and newlines**, at ~$0.0015. Nothing downstream cares:
`_parse_json` calls `json.loads`, which is whitespace-indifferent.

For stage 3 specifically this is not merely wasteful — it is plausibly *causal*. It truncated at
8,000 tokens with ~25% of what it had written being whitespace.

This was not in the original optimization plan. It is the cheapest win available and carries no
quality risk whatsoever.

## Finding 4 — Stage 2's output is only ~28% reasoning

Breaking down stage 2's 35,884 output chars:

| Component | Chars | % | Reasoning? |
|---|---:|---:|---|
| Pretty-print whitespace | 10,266 | 29% | no |
| JSON key names | 6,764 | 19% | no |
| `obligation_text` copied from stage 1 | 7,667 | 21% | no |
| `obligation_id` + `source_reference` copied | 1,083 | 3% | no |
| `test_method` (new, one sentence each) | 5,829 | 16% | **yes** |
| Other classification fields | ~4,275 | 12% | **yes** |

**~72% of stage 2's output is not the model thinking** — it is formatting, key names, and
transcription of its own earlier output.

Correction to the pre-measurement estimate in the optimization plan: copied text alone is 24%, not
the ~55% guessed. But total addressable waste is *higher* than estimated, because whitespace and key
names weren't counted at all.

## Finding 5 — Taxonomy violations pass through unvalidated

Stage 2's prompt permits exactly five `obligation_type` values
(`Preventive | Detective | Governance | Reporting | Documentation`). The output contained a sixth:

- `obligation_type = "Corrective"` on 4 of 38 obligations
- `evidence_expected = "License"`, outside the permitted nine

Nothing in the pipeline validates enum membership, so these land in the database and flow into
`_dominant()` and the reporting layer. A dict lookup after parsing would catch it for free.

## Output shape — the quality baseline to preserve

These are the numbers the post-optimization run must reproduce.

| Metric | Baseline |
|---|---|
| Requirements | 7 |
| Obligations | 38 (5.43 per requirement) |
| Controls | **0** (should be ~20 — see Finding 1) |
| Mean clarity score | 4.68 |
| `needs_manual_review` | 2 |
| Empty obligation texts | 0 |
| Criticality | High 31, Medium 7 |
| Execution category | Ongoing_Control 20, Governance_Approval 8, One_Time_Implementation 6, One_Off_Reporting 4 |
| Obligation type | Preventive 18, Governance 10, Reporting 6, Corrective 4 |
| Stage 4 report | 7,342 chars, 29 table rows |

Note the comparison target for controls is **not** the baseline's 0 — it is ~20. Stage 3 is expected
to *improve* here, not hold steady.

## What this changes in the optimization plan

1. **Stage 3's `max_tokens` moves from a predicted risk to a confirmed, reproducible failure.** It
   should be fixed first.
2. **Compact JSON output is a new item** and belongs near the top — ~26% of output tokens and
   ~160s/doc, at zero quality risk.
3. **Templating stage 4 (§3b) is no longer just a cost saving.** It is the fix for a live
   data-integrity problem affecting an estimated ~235 production regulations.
4. **Enum validation is a new item** — cheap, and currently nothing enforces the taxonomies the
   prompts define.
5. **The token-reduction target is ~42%**, revised from ~48% on measured data. The *latency*
   reduction is larger than estimated, because wall time tracks completion tokens almost exactly and
   the whitespace saving lands entirely on completion.

## Revised projection

| | Baseline (measured) | After token work | + concurrency |
|---|---:|---:|---:|
| Prompt tokens | 15,133 | ~9,700 | ~9,700 |
| Completion tokens | 19,627 | ~10,600 | ~10,600 |
| **Total tokens** | **34,760** | **~20,300 (−42%)** | ~20,300 |
| Wall clock, 1 doc | 614.5s | ~355s | ~250s |
| Wall clock, 20 docs | ~3.4 hrs | ~2.0 hrs | **~21 min** (4 workers) |
| Controls produced | 0 | ~20 | ~20 |

Cost per document falls from ~$0.011 to ~$0.006, *and* the document actually gets its controls.
