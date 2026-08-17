# Staged LLM Analyzer — Cost & Latency Optimization Plan

Target file: `processor/staged_LLM_Analyzer.py`

> **STATUS: IMPLEMENTED AND MEASURED.** Steps 1-6 of §9 are done. Results:
> **614.5s → 253.3s (−59%), 34,760 → 22,016 tokens (−37%), cost −39%, controls 0 → 22.**
> Output quality held (7 requirements both runs, clarity 4.68 → 4.87). See
> [optimization_results.md](optimization_results.md) for the full before/after and the list of
> items still outstanding. The `schema_version` bump to v3 anticipated below turned out to be
> unnecessary — lean model output is rehydrated into the existing shape in Python, so storage is
> unchanged.

> **Now grounded in a measured baseline.** All figures below come from an instrumented run of
> regulation 103296 (SAMA Banking Control Law) — see [baseline_run_103296.md](baseline_run_103296.md).
> Headline: **614.5s and 34,760 tokens for one document**, of which stage 3 spent 278.6s and 13,731
> tokens producing **nothing** (truncated), and stage 4 then **fabricated a 10-row control table**
> out of empty input. Two items below were added as a direct result of measurement: compact JSON
> output (§1c) and enum validation (§3c).

## 0. The constraint this plan is built around

**The 4-stage split exists because a single combined prompt gave poor results. That decision stands
and is not up for renegotiation here.** Any proposal to merge stages is out of scope — recorded in
§6 so it doesn't get re-raised.

The working principle for everything below:

> Keep every reasoning boundary exactly where it is. Each stage stays its own call, with its own
> focused instruction, doing its own independent pass. Cut only the things that are not reasoning.

Concretely, the pipeline currently spends time and credits on four things that have nothing to do
with the quality the staging bought you:

| Waste | What it is | Fix category |
|---|---|---|
| **Transcription** | The model re-typing text it already produced in an earlier stage | Delta contracts (§1) |
| **Waiting** | Serial calls that have no dependency on each other | Concurrency (§2) |
| **Non-reasoning work** | Exact string dedup and table rendering handed to an LLM | Move to Python (§3) |
| **Re-doing / discarding** | No cache, no retry, truncated responses thrown away | Reliability (§4) |

None of these four touch what the model reasons about. Stage 1 still extracts, unaware of
classification. Stage 2 still classifies as a fresh focused pass. Stage 3 still designs controls.
Stage 4 still writes the narrative. The decomposition is untouched.

## 1. Transcription — stop paying the model to copy itself

`obligation_text` is currently generated **three times**: Stage 1 writes it, Stage 2 copies it out
verbatim alongside its 9 new fields, Stage 3 copies it a third time. Output tokens are the slow and
expensive half of every call, and roughly half of Stage 2's output is transcription.

### 1a. Stage 2 returns classification deltas keyed by id

The model receives exactly the same input and performs exactly the same reasoning — it just stops
retyping what it was given.

```json
{"id": "REQ-001-OB-001", "type": "Preventive", "crit": "High",
 "ev": ["Policy"], "test": "...", "clarity": 5, "review": false,
 "exec": "Ongoing_Control"}
```

Merge against Stage 1 in `_assemble_rows`. Short keys are deliberate — repeating
`obligation_id` / `execution_category` / `evidence_expected` across 24 obligations is a few hundred
wasted output tokens by itself.

Rule 2 of the Stage 2 prompt (splitting multi-action obligations) is the one case that genuinely
needs new text. Give it an explicit escape hatch so the model only writes text when it has actually
decided to split:

```json
{"id": "REQ-001-OB-003", "split_into": [{"text": "..."}, {"text": "..."}]}
```

**Measured effect: ~24% off Stage 2's output** (8,750 of 35,884 chars are copied `obligation_text`,
`obligation_id` and `source_reference`). The pre-measurement estimate of ~55% was wrong — copying is
a smaller share than guessed, because `test_method` is a full sentence per obligation and dominates
the genuinely-new content. Reasoning is unchanged.

### 1c. Ask for compact JSON — *added after measurement, cheapest win available*

The model emits 4-space-indented JSON. **~26% of every stage's output is whitespace** — 3,441 chars
in stage 1, 10,266 in stage 2, 11,780 in stage 3. That is roughly **5,000 completion tokens and 160
seconds per document spent generating spaces and newlines.**

Nothing downstream cares: `_parse_json` calls `json.loads`, which is whitespace-indifferent.

Instruct compact output (`no whitespace or indentation between tokens`) and/or set
`response_format: {"type": "json_object"}`. For stage 3 this is plausibly *causal* rather than merely
wasteful — it truncated at 8,000 tokens with a quarter of what it wrote being indentation.

Zero quality risk. Do this one first.

Combined with short keys (19% of stage 2's output is repeated key names like `"execution_category":`),
**~72% of stage 2's current output is not the model reasoning** — see Finding 4 in the baseline.

### 1b. Stage 3 gets a minimal input and returns controls keyed by id

Stage 3 currently receives the whole requirement tree including all the Stage 2 metadata it never
uses, and echoes back `obligation_text` and `execution_category` in its response. Send only
`[{"id": ..., "text": ...}]` for the ongoing obligations; return `{"id": ..., "control": {...}}`.

Also delete the `control: null` passthrough instruction — you already filter to ongoing obligations
before calling (`staged_LLM_Analyzer.py:83-89`), so that rule is dead weight in every prompt.

**~70% off Stage 3's input, ~15% off its output.**

## 2. Waiting — the biggest wall-clock win, and it costs nothing

Four calls run strictly in sequence, and documents run strictly in sequence on top of that
(`orchestrator.py:496`, `orchestrator.py:543`). These are I/O-bound HTTP calls sitting idle.

### 2a. Document-level pool

`ThreadPoolExecutor(max_workers=4)` over both orchestrator loops and the batch endpoints in
`pipeline_api.py`. Keep the per-document `gc.collect()`. Changes nothing about any individual call —
this is pure scheduling.

### 2b. Shard Stage 3 by requirement

Controls are designed per obligation with no cross-requirement context; the prompt itself says "one
control per Ongoing_Control obligation." Fan out one call per requirement group, concurrently.

This is also the **structural fix for the `max_tokens` bug in §4** — each shard produces a small,
safely bounded response instead of one giant generation that can overrun the limit.

### 2c. Take Stage 4 off the critical path

Once its tables are rendered in Python (§3b), Stage 4's prose sections need only Stage 1 + Stage 2
data. So it runs **concurrently with** Stage 3 rather than after it.

Critical path goes from `S1 → S2 → S3 → S4` to `S1 → S2 → max(S3 shards, S4)`.

### 2d. Optional: shard Stage 2 by requirement group

Same idea as 2b. Viable **only after** §3a moves exact-duplicate removal into Python, since that is
the one Stage 2 task that needs to see all groups at once.

Flagging honestly: this is the one concurrency change with a non-zero quality risk. A model
classifying group 3 in isolation can calibrate `criticality` slightly differently than one seeing
all six groups together. If you want it, shard into 2-3 chunks rather than one-per-group, and
spot-check criticality distribution against current output. Skip it if the §2a-2c gains are enough —
they probably are.

### Rate limiting

4 documents × 3 shards = 12 concurrent OpenRouter requests. Use one **global semaphore** shared by
both pool levels rather than nested unbounded executors, or you'll trade latency wins for 429s.

## 3. Non-reasoning work — take it off the LLM

Two Stage 2/4 tasks are not judgement calls. Doing them in Python is cheaper, faster, **and more
reliable than an LLM.**

### 3a. Exact-duplicate removal

Stage 2 rule 1 is "Remove exact duplicates." Exact string equality is a `set` operation, not a
reasoning task — an LLM does it *less* reliably than three lines of Python. Normalize whitespace and
casing, dedupe deterministically before the Stage 2 call, and drop rule 1 from the prompt.

Leave semantic near-duplicate detection where it is — that *is* reasoning, and the Stage 1 prompt
already handles it under its deduplication rules.

### 3b. Stage 4 sections 3, 4 and 5

These are markdown tables — deterministic projections of data you already hold in `_assemble_rows`.
**You already do exactly this elsewhere:** `migrate_old_analysis.py:200-234` builds the obligation
inventory table in pure Python with no LLM involved. Reuse that approach.

Then one small LLM call for sections 1, 2 and 6 (Executive Summary, Requirement Overview,
Architectural Implications) — the parts that are genuinely writing — fed a compact digest: document
title, requirement titles, counts by execution category and criticality, control owners.

**Measured input is 1,931 tokens and output 1,582; both drop to roughly 600.**

**This is now the highest-priority item in the document, and not for cost reasons.** The baseline
proved stage 4 fabricates: handed `Stage 3:\n{"requirements": []}`, it invented a 10-row control
inventory with named owners, frequencies and residual-risk ratings. An estimated **~235 production
regulations currently ship an executive report containing a control table with no backing data**
(147 of 200 sampled zero-control regulations). Rendering section 5 from the actual rows makes that
structurally impossible.

### 3c. Validate the enums — *added after measurement*

Stage 2's prompt permits five `obligation_type` values. The baseline output contained a sixth,
`"Corrective"`, on 4 of 38 obligations, plus `evidence_expected: "License"` outside the permitted
nine. Nothing validates enum membership, so these reach the database and flow into `_dominant()`.

A dict lookup after parsing catches it for free. Coerce to the nearest permitted value or set
`needs_manual_review`.

## 4. Reliability — stop paying for work that gets discarded

These are correctness bugs. They cost money because you buy responses and throw them away.

- **`staged_LLM_Analyzer.py:91-94` — Stage 3 has no `max_tokens` override**, so it uses the 8000
  default. **CONFIRMED ON THE BASELINE RUN, not a hypothetical:** regulation 103296 hit
  `finish_reason: length` at exactly 8,000 tokens, `_parse_json` failed with
  `Unterminated string starting at ... char 46015`, and **all 20 expected controls were silently
  dropped** — 278.6 seconds (45% of total runtime) and 13,731 tokens bought zero output. In
  production this affects ~3% of regulations (22 of the 696 that have `Ongoing_Control`
  obligations). §2b fixes it structurally; also set the value explicitly.
- **`staged_LLM_Analyzer.py:310-316` — Stage 4 truncates its inputs** to 3000 / 3500 / 3000 chars.
  On anything past ~10 obligations the model never sees the tail of the data, so the inventory and
  control tables are quietly incomplete. §3b eliminates this entirely.
- **`_parse_json` swallows failures** and returns `{"requirements": []}`
  (`staged_LLM_Analyzer.py:402-404`). A Stage 2 parse failure means `_assemble_rows` silently falls
  back to unclassified Stage 1 obligations, and the row lands in the DB looking normal. Raise, or at
  minimum set `needs_manual_review` on the row.
- **No retry in `_call_llm`.** One 429 or 5xx on Stage 3 loses the document *and* the Stage 1 and
  Stage 2 calls you already paid for. Three attempts with exponential backoff.
- **No `finish_reason` check.** Truncation currently looks identical to a malformed response.
- **`response_format: {"type": "json_object"}`** on Stages 1-3 removes the fence-stripping and
  trailing-comma repair in `_parse_json`, and removes a whole class of paid-but-unparseable
  responses.
- **Content-hash cache.** Key on `sha256(clean_text) + model + prompt_version`. The orchestrator
  reprocesses documents flagged `monitoring_status == "modified"` (`orchestrator.py:528-531`) — if
  the extracted text is byte-identical, all four calls are skippable.
- **Binding-language gate.** If normalized text contains no `must|shall|required to|obligated to` /
  `يجب|يتعين|يلتزم`, Stage 1 returns nothing anyway. A regex check skips the call for free.

## 5. Housekeeping (small, free, quality-neutral)

- **Prompt-prefix caching.** DeepSeek does automatic prefix caching, so identical leading text bills
  cheaply. Your rule blocks are already static — but Stage 1 interpolates `regulator` / `reference` /
  `publication_date` into the JSON schema *in the middle* of the prompt
  (`staged_LLM_Analyzer.py:151-153`), breaking the shared prefix. Move all variable values to the
  tail, after the static rules. Verify it actually engages by checking the `usage` block OpenRouter
  returns before counting on the saving.
- **Consolidate the repeated language block.** The three `Do NOT translate` lines appear in all four
  prompts and the system prompt already says "Always respond in the same language as the source
  document." Keep it in one place.
- **Model tiering.** Stage 2 is classification against a fixed taxonomy and Stage 4 is short-form
  writing — both easier than Stage 1 extraction and Stage 3 control design. Worth testing a
  cheaper/faster model for those two *after* §1 and §3 land, and only with a side-by-side check.

## 6. Explicitly rejected

**Merging Stage 1 and Stage 2 into a single call.** This was tried before the staged rewrite and
produced materially worse extraction. Not revisiting. Recorded here so it isn't proposed again in a
future pass.

## 7. Expected effect — against the measured baseline

| | Baseline (measured) | After §1 + §3 | + §2 concurrency |
|---|---:|---:|---:|
| Prompt tokens / doc | 15,133 | ~9,700 | ~9,700 |
| Completion tokens / doc | 19,627 | ~10,600 | ~10,600 |
| **Total / doc** | **34,760** | **~20,300 (−42%)** | ~20,300 |
| Wall clock, 1 doc | 614.5s | ~355s | ~250s |
| **Wall clock, 20 docs** | **~3.4 hrs** | ~2.0 hrs | **~21 min** (4 workers) |
| Cost / doc | ~$0.011 | ~$0.006 | ~$0.006 |
| **Controls produced** | **0** | **~20** | **~20** |

Note the last row: this is not purely a cost exercise. The baseline document currently produces zero
controls; after the fix it produces the ~20 it should.

**Why latency tracks output so closely.** Measured generation throughput is ~30 tokens/sec,
consistent across stages (stage 2: 7,177 tok / 229.0s = 31/s; stage 3: 8,000 / 278.6s = 29/s).
19,627 completion tokens ÷ 30 ≈ 654s against 614.5s measured. **Prompt size barely affects latency —
output size is essentially the whole story**, so every token the model doesn't write is ~33ms saved.
That makes §1c (compact JSON) worth ~160s/doc on its own.

Batch figures assume rate limits don't bind; see §2 rate limiting.

## 8. Quality risk, honestly tiered

**Provably neutral** — the model sees the same input and reasons identically, or the work moves to
deterministic code:

- §1a, §1b delta contracts — same input, same reasoning, less typing
- §3a Python exact dedup — string equality, strictly more reliable than an LLM
- §3b Python table rendering — deterministic projection, strictly more correct than today
- §2a document pool, §2b Stage 3 sharding, §2c Stage 4 off critical path — scheduling only
- All of §4 — strictly fewer discarded and silently-degraded results

**Small risk, spot-check before adopting:**

- §2d Stage 2 sharding — possible criticality calibration drift
- §5 model tiering for Stages 2 and 4

**Rejected:** §6.

Everything in the first tier can ship without an output comparison. Only the second tier needs one.

## 9. Suggested sequencing

Re-ordered after the baseline run — the two confirmed defects come first.

1. **§4 Stage 3 `max_tokens` + retry + `finish_reason` check.** A confirmed, reproducible total loss
   of controls. One-line minimum fix.
2. **§3b templated Stage 4.** Stops ~235 production regulations shipping fabricated control tables.
   Data integrity, not cost.
3. **§1c compact JSON.** ~26% of all output tokens, ~160s/doc, zero risk, trivial to implement.
4. **§2a document-level parallelism.** Biggest batch improvement, one afternoon, no quality surface.
5. **§1a + §1b delta contracts, §3a + §3c Python offloads.** Bump `schema_version` to `v3` — the
   shape stored in `stage2_json` / `stage3_json` changes, and `export_to_prod.py` reads those columns.
6. **§2b + §2c** Stage 3 sharding and Stage 4 off the critical path.
7. **§5 housekeeping**, then optionally §2d and model tiering with a side-by-side check.

Steps 1-6 are all quality-neutral or quality-improving.

After each step, re-run the harness and diff:

```
python benchmarks/analyzer_bench.py --regulation-id 103296 --label optimized
python benchmarks/analyzer_bench.py --compare baseline optimized
```

Requirement/obligation counts, criticality and execution-category distributions, and mean clarity
should hold steady. Controls should go from 0 to ~20.

> **Team note:** step 3 changes the stored stage JSON shape, so it needs to land as one coordinated
> change with the `schema_version` bump rather than three people touching it separately.
