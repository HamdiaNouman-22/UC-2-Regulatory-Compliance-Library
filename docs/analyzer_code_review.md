# Code review — `processor/staged_LLM_Analyzer.py`

Reviewed against general software engineering practice and LLM-application practice.
Most of the current file was written during the optimization work, so most findings below are
self-inflicted rather than pre-existing.

Verdict: the pipeline logic is sound and the recent fixes hold up, but there are **three real
correctness bugs**, and the file has outgrown being one class.

---

## A. Correctness bugs — **all three FIXED 2026-08-11**

A1 and A2 now have regression tests in `tests/test_staged_analyzer_offline.py` (39 checks, all
passing). A3 emits a warning; it is not yet self-healing.

### A1. A Stage 2 parse failure silently empties the document (verified)

`_run_stage2` → `_parse_json(raw).get("o") or []`. On a malformed response this is `[]`, and
`_rehydrate_stage2` then emits requirements with `normalized_obligations: []`.

`_assemble_rows:717` was written to fall back to Stage 1 obligations:

```python
obligations = s2_req.get("normalized_obligations", req.get("obligations", []))
```

**The fallback can never fire.** `.get(key, default)` only returns the default when the key is
*absent*, and the key is always present — just empty. Verified:

```
stage2 on parse failure: {"requirements":[{"requirement_id":"REQ-001", ..., "normalized_obligations": []}]}
obligations that reach the DB row: 0   (stage 1 had 2)
```

A row is still written, with zero obligations, and nothing flags it. This is the same class of
silent-degradation bug that made the original Stage 3 failure invisible.

**Fix:** make the fallback truthiness-based, and flag the row.

```python
obligations = s2_req.get("normalized_obligations") or req.get("obligations", [])
```

Better still, have `_run_stage2` raise when it gets zero deltas for a non-empty input, so the failure
surfaces rather than degrading.

### A2. Exact-dedup is global, not per-group, and can delete a whole requirement (verified)

`_dedupe_exact` keeps one `seen` set across **all** requirement groups. The Stage 1 prompt only asks
for dedup *within* a group. Two topic groups can legitimately contain the same sentence.

```
groups: REQ-001 "Reporting", REQ-002 "Licensing", both containing
        "The bank must notify SAMA."
after dedup: removed 1 -> groups left: ['REQ-001']
```

REQ-002 lost its only obligation and was then dropped entirely by the empty-group filter. A whole
requirement disappears from the analysis.

**Fix:** scope `seen` per requirement group (move it inside the loop). Cross-group semantic overlap is
a judgement call and already belongs to the Stage 1 prompt.

### A3. Stage 3 partial failures are invisible

Two silent paths:

- `_parse_json` returns `{}` → `.get("c") or []` → every obligation in that shard gets
  `control: None`. The shard still returns successfully, so nothing logs.
- `_stage3_shard` returns `None` on a generic exception, dropping the group's controls with only a
  log line.

`_render_stage4` only warns when the control list is **completely** empty, so a run that produces 14
of 22 controls looks entirely healthy.

**Fix:** count expected vs produced controls in `_run_stage3` and log a warning on any shortfall;
consider flagging affected obligations `needs_manual_review`.

---

## A4. `gap_analyzer._split_text` does not actually chunk (found 2026-08-11)

`analyze_gaps` routes to `_analyze_in_chunks` when the document exceeds
`max_chunk_size` (12,000 chars). `_split_text` then splits on `"\n\n"` — but text that has been
through `normalize_input_text` contains no blank lines, so `paragraphs` is a single element and the
"chunker" returns **one chunk containing the whole document**.

Observed on a 19,274-char document: routed to the chunked path, produced exactly **1 LLM call**.

The loop only starts a new chunk *between* paragraphs; a single paragraph larger than the limit is
never split. So the size guard silently does nothing, and the protection it was written to provide —
staying inside the context and token budget — is absent.

**Fix:** after splitting on blank lines, hard-split any paragraph still longer than
`max_chunk_size`. Worth pairing with a log line stating the actual chunk count and sizes.

## B. Robustness gaps

### B1. Stage 2 has no truncation recovery, but produces the most output

Stage 3 shards and adaptively splits on `TruncatedResponseError`. Stage 2 — historically the largest
generation at 7,177 completion tokens — has neither. A truncation there propagates out of `analyze()`
and loses the document, including the Stage 1 call already paid for.

Stage 2 is shardable by requirement group in exactly the way Stage 3 is (see §2d of the optimization
plan, with the noted criticality-calibration caveat).

### B2. No per-document call budget

`_stage3_shard` recurses to `depth < 3`, halving each time — up to 8 leaf calls per requirement group.
With 8 groups that is 64 possible Stage 3 calls for one document. Nothing caps total calls or spend
per `analyze()`.

**Fix:** a call counter on the instance with a configurable ceiling, raising when exceeded.

### B3. `load_dotenv(override=True)` at import time

`override=True` means a stale `.env` on disk **beats real environment variables** injected by the
container, CI or systemd. This is a deployment footgun: the same image behaves differently depending
on whether a leftover `.env` is present. Library modules also should not have import-time side
effects.

**Fix:** `load_dotenv()` without override, and ideally only at application entry points.

### B4. Config is read at module import, so half of it cannot be overridden per instance

`provider`, `quantization` and `deterministic` are constructor parameters, but `_LLM_SEED` and
`_LLM_ALLOW_FALLBACKS` are read directly from module globals inside `_call_llm`. Inconsistent, and it
makes those two untestable without monkeypatching module state.

**Fix:** one frozen `LLMConfig` dataclass built once, passed in, defaulted from env.

### B5. A mistyped provider name 404s the entire pipeline with no useful message

Observed in practice: `LLM_PROVIDER=AtlasCLoud` (capital L) produced
`404 No endpoints found` on every call. Provider names are case-sensitive and `allow_fallbacks=False`
turns any typo into a total outage.

**Fix:** validate `LLM_PROVIDER` against `GET /models/{model}/endpoints` once at construction, and
fail with a message naming the valid options. Ten lines, and it also catches the `quantization`
conflict that makes `DeepInfra` + `fp8` unsatisfiable.

---

## C. LLM-application practice

### C1. Use structured outputs, not `json_object` plus manual coercion — biggest available upgrade

The endpoint metadata shows most providers advertise `structured_outputs: true`, meaning a real JSON
Schema can be enforced server-side. Currently the code asks for `{"type": "json_object"}` (free-form
JSON) and then repairs the result by hand: fence stripping, trailing-comma regex, `_coerce` across
four taxonomies, clarity clamping, list-type checks.

Supplying `json_schema` with the enums declared would make most of `_coerce` and `_parse_json`
unnecessary and remove a whole class of shape errors at the source.

### C2. No prompt versioning

Prompts are inline f-strings with no version identifier. Nothing recorded alongside a stored row says
which prompt produced it, so you cannot tell whether two analyses differ because the document changed
or because someone edited a prompt.

This is also a hard prerequisite for the content-hash cache — the cache key must include a prompt
version, or a prompt edit will never invalidate anything.

**Fix:** `PROMPT_VERSION = "2026-08-03"` (or a hash of the four templates) persisted with each row.

### C3. No cost or token observability in production

Token counts and cost only exist inside the benchmark harness, which obtains them by monkeypatching
`requests.post`. Production runs record nothing, so a prompt change that doubles spend is invisible
until the invoice.

**Fix:** read `body["usage"]` in `_call_llm` and log/emit per stage. The harness proves the data is
already in every response.

### C4. Logs are not correlated, which will hurt now that runs are parallel

With `DOC_MAX_WORKERS=4` and Stage 3 sharding, log lines from several documents interleave. Most
messages carry no `regulation_id` — `_stage3_shard` logs `requirement_id` only, which is not unique
across documents.

**Fix:** a `logging.LoggerAdapter` bound with `regulation_id` per `analyze()` call.

### C5. Silent taxonomy substitution can understate risk

`_coerce` replaces an unrecognised value with a default. For `criticality` the default is `"Medium"` —
so a garbled `"Critical"` becomes `"Medium"` in a compliance database. `needs_manual_review` is set,
which mitigates it, but the safe default for a risk field is the *conservative* one.

**Fix:** default `criticality` to `"High"`, and preserve the original value in a
`raw_<field>` key so reviewers can see what the model actually said.

### C6. No output evaluation harness

`tests/test_staged_analyzer_offline.py` covers the deterministic paths well, but there is no
regression eval on model output — no golden set, no tolerance bands. Given the measured run-to-run
variance (three runs of one document produced 27, 38 and 44 obligations), assertions on exact counts
would be flaky, but band assertions ("35 ± 8 obligations, zero taxonomy violations, every ongoing
obligation has a control") would catch real regressions.

### C7. Single language label for the whole document

`detect_language(text)` yields one language for the entire document. Mixed Arabic/English regulatory
documents are common in this domain and will get one label applied to every stage.

---

## D. Structure and maintainability

### D1. One class doing six jobs

880 lines covering HTTP transport, retry policy, prompt templates, JSON repair, taxonomy validation,
markdown rendering, row assembly and concurrency orchestration. Natural seams:

| Proposed unit | Responsibility |
|---|---|
| `OpenRouterClient` | transport, retry, semaphore, determinism payload |
| `prompts.py` | the four templates + `PROMPT_VERSION` |
| `taxonomy.py` | vocabularies, `_coerce`, aliases |
| `report.py` | `_render_stage4`, `_stage4_digest`, markdown helpers |
| `StagedLLMAnalyzer` | stage orchestration only |

This also makes the client independently testable and reusable by `gap_analyzer.py`,
`requirement_matcher.py` and `LlmAnalyzer.py`, which each carry their own `_call_llm`.

### D2. Dead code and unused parameters

- `_clean_stage4` (771) is no longer called — Stage 4 returns JSON now.
- `_render_stage4(rows, ...)` never uses `rows`; it reads `s2` and `s3` directly.
- `_assemble_rows(..., s4_md, ...)` is always called with `""` and then overwritten by the caller.
- `Any` imported but unused; `import os` appears twice (lines 1 and 13).

### D3. `temperature` arguments are now dead

Deterministic mode is on by default and forces `temperature: 0`, so the `temperature=0.25` at the
Stage 3 call site does nothing. A reader will reasonably assume it applies.

**Fix:** drop the per-call temperatures, or name them `temperature_when_nondeterministic`.

### D4. Magic numbers

`16000`, `12000`, `2500`, `depth < 3`, `160`-char truncation, `timeout=180`, `2 ** attempt`. These are
tuning decisions and should be named constants.

### D5. The class docstring documents a diff, not the class

"What changed is everything that was not reasoning…" describes the refactor rather than current
behaviour, and will read as noise in six months. That belongs in
[optimization_results.md](optimization_results.md); the docstring should describe what the class does.

### D6. The stale determinism comment

The header comment (lines 30-41) still says pinning "removes the largest source of run-to-run drift".
Subsequent measurement disproved that — see [determinism.md](determinism.md). It should be corrected
so nobody trusts it.

---

## E. What is good and should stay

- The delta-plus-rehydrate design is genuinely well-factored: the model emits minimal output while the
  persisted shape stays byte-compatible with every existing consumer. That avoided a `schema_version`
  bump and any downstream change.
- Rendering the Stage 4 tables from real data makes an entire class of hallucination structurally
  impossible, rather than asking the model not to do it.
- Adaptive shard-splitting on truncation is the right pattern — it degrades gracefully instead of
  losing the stage.
- `TruncatedResponseError` as a distinct exception, re-raised past the retry loop, is correct: a
  truncation should not be retried identically.
- The offline test suite runs with no network and covers the tricky merge and coercion logic.

---

## Suggested order

1. **A1, A2** — silent data loss, small diffs.
2. **A3, B5** — silent partial failure and the typo-outage footgun.
3. **C2, C3** — prompt version and usage logging; both prerequisites for the content-hash cache.
4. **C1** — structured outputs; deletes more code than it adds.
5. **D2, D3, D6** — dead code and misleading comments.
6. **D1** — the module split, once the above have settled.
