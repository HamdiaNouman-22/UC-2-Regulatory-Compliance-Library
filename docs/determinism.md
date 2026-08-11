# Why the same document gives different analysis each run

Observed: re-analysing an unchanged regulation produces different obligations, different groupings,
sometimes a different obligation count. This documents why, what has been changed, and what remains
irreducible.

## Short answer, after measuring

**Output length is the driver, not the provider.** Long generations diverge; short ones do not. There
is no provider setting that fixes this, and the only reliable answer is to not re-run at all (see
"The actual guarantee" below).

Measured directly, same provider (AtlasCloud), `temperature: 0`, `top_p: 1`, seed set,
`allow_fallbacks: false`, identical prompt each time —
`python benchmarks/determinism_by_length.py --runs 3`:

```
SHORT generation (~700 chars out)
  call 1:    882 chars  1c12dea6
  call 2:    882 chars  1c12dea6
  call 3:    882 chars  1c12dea6
  -> 1 distinct output from 3 calls        STABLE

LONG generation (real stage 1, ~14,000 chars out)
  call 1: 13,241 chars  c4e00f61
  call 2: 11,746 chars  a0e15201
  call 3: 11,044 chars  54f86f1c
  -> 3 distinct outputs from 3 calls       VARIES
```

Nothing differs between those two blocks except how much text the model was asked to generate. The
short case is perfectly reproducible; the long case produced three different documents, varying 20%
in length (11,044 to 13,241 chars). That length spread is not a cosmetic token difference — it is
structurally different extraction, which is how two runs end up with 38 and 44 obligations.

An earlier 10-call sample at short length returned 10/10 identical on both AtlasCloud and Alibaba,
confirming the short-length stability is not luck.

Each generated token is an opportunity for two near-tied candidates to swap places under tiny
floating-point differences. Across 700 characters that is vanishingly rare. Across 14,000 characters
of JSON it is close to certain — and because generation is autoregressive, one flip rewrites
everything after it.

### The matcher varies too — for a different reason

`RequirementMatcher` produces ~150-token replies, so the length effect above should not apply. It
still disagrees with itself, and determinism settings make no difference:

| Config | Two runs of the same 39 obligations agree |
|---|---|
| temperature 0.1, free routing (original) | 95% (2 differ) |
| temperature 0, seed, pinned provider, fp8 | **92% (3 differ)** |

Turning determinism on made it marginally *worse*, i.e. both figures are the same noise band.

The cause is visible in the disagreements — they are genuine ties, not sampling jitter:

```
"Every bank shall furnish SAMA by the end of the following month..."    run1=partially_matched(26)  run2=new
"Every bank shall also furnish SAMA within six months of the close..."  run1=new                    run2=partially_matched(26)
```

Two similar reporting obligations compete for the same existing requirement 26, and the prompt gives
no rule for which one wins. Roughly 2-3 of 39 obligations sit on such a fence.

**Implication:** the matcher does not need temperature tuning, it needs either (a) the result cached
so it is only decided once, or (b) a `confidence` field so borderline verdicts are flagged for human
review instead of being silently resolved one way or the other. `deterministic=False` remains the
default because enabling it changes verdicts and buys nothing.

### Two earlier conclusions in this document were wrong

Recorded so nobody re-runs the same dead ends:

1. **"Provider switching between calls is the dominant cause."** Pinning the provider with
   temperature 0 and a fixed seed did *not* fix it — two runs of identical text gave 38 and 44
   obligations. Pinning is still worth keeping (it holds quantization constant and removes a
   variable) but it is not the fix.

2. **"AtlasCloud is non-reproducible; switch to Novita."** Both halves were wrong. AtlasCloud
   returned 10/10 identical output on a larger sample; the original verdict came from a 3-call
   sample that was too small to classify anything. And **Novita does not support `response_format`**,
   so pinning it makes every JSON stage fail with HTTP 404 — see the provider capability table below.

**Practical takeaway:** leave `LLM_PROVIDER` on a provider that supports `response_format`, keep
temperature 0, and treat the content-hash cache as the actual solution.

### Provider capabilities — check before pinning

Not all providers support the parameters the analyzer sends. Pinning one that does not, with
`allow_fallbacks: false`, produces `404 No endpoints found`.

| Provider | Quant | `response_format` | seed | Safe to pin? |
|---|---|---|---|---|
| AtlasCloud | fp8 | yes | yes | **yes** (current default) |
| Alibaba | fp8 | yes | yes | yes |
| Baidu | fp8 | yes | yes | yes |
| SiliconFlow | fp8 | yes | no | yes |
| DeepInfra | **fp4** | yes | yes | works, but lowest precision |
| **Novita** | fp8 | **no** | yes | **no — causes 404** |
| **GMICloud** | fp8 | **no** | yes | **no — causes 404** |
| DigitalOcean, SambaNova | unknown | no | no | no |

Regenerate this table with `python benchmarks/provider_determinism.py`.

Note also that `LLM_QUANTIZATION` adds a second filter that can independently empty the endpoint list
— pinning `DeepInfra` (fp4) together with `fp8` returns 404. When a single provider is pinned the
quantization filter is redundant.

## The five contributing causes

Listed in order of how much they mattered. Cause 1 is the root; cause 5 explains the magnitude.

## Cause 1 — OpenRouter was routing calls to different companies' hardware

This is the big one, and it is measurable in the captured runs.

`deepseek/deepseek-v3.2` is not one endpoint. OpenRouter serves it from **14 different providers**,
at **different quantizations**:

| Provider | Quantization |
|---|---|
| StreamLake, SiliconFlow, AtlasCloud, Novita, Baidu, GMICloud, Alibaba | fp8 |
| **DeepInfra** | **fp4** |
| DigitalOcean, Venice, Friendli, Google, Phala, SambaNova | unknown |

OpenRouter picks one per request based on price, latency and load. Nothing pins it.

In the recorded baseline run, a **single document's analysis was produced by two different
providers**:

```
stage1_extract    provider=AtlasCloud
stage2_normalize  provider=AtlasCloud
stage3_controls   provider=StreamLake     <-- different company, different hardware
```

Different provider means different GPUs, different inference stack, different kernel
implementations, and potentially different weight quantization. fp4 versus fp8 is a substantial
difference in numerical precision — the same prompt can genuinely produce different text. "Same
model" is not the same engine.

**Fixed:** the provider and quantization are now pinned, with fallbacks off by default.

## Cause 2 — Sampling temperature was above zero

Stages ran at `temperature` 0.1, 0.1, 0.25 and 0.3. Any temperature above zero means the next token
is *sampled* from a probability distribution rather than the most likely one being taken. That is
randomness by design, and with no seed it differs every call.

Temperature 0.1 is low but not zero — and because of Cause 5 below, even rare token flips propagate.

**Fixed:** deterministic mode uses `temperature: 0` and `top_p: 1` on every stage.

## Cause 3 — No seed was sent

Even at low temperature, a seed lets the provider reproduce the same sampling sequence. None was
being sent.

**Fixed:** a fixed `seed` is now sent (`LLM_SEED`, default `20250101`). Note this is *best-effort* —
providers are not obliged to honour it, and most treat it as a hint.

## Cause 4 — Batch non-determinism, and why one flipped token ruins the whole document

This one cannot be fully solved and is worth understanding so nobody chases it.

Even with one provider, temperature 0 and a fixed seed, GPU inference batches your request together
with other users' requests. Floating-point addition is not associative — `(a+b)+c` can differ from
`a+(b+c)` in the last bits. The composition of the batch therefore shifts the computed logits very
slightly. When two candidate tokens are nearly tied, that tiny shift flips which one wins.

DeepSeek v3.2 is a Mixture-of-Experts model, which makes this worse: expert routing decisions can
also vary with batch composition, and a different expert produces genuinely different output rather
than a near-identical one.

**Crucially, the effect is not small, because generation is autoregressive.** Each token is
conditioned on every token before it. So a single flipped token does not cause a single-word
difference — it changes the context for everything that follows, and the two outputs diverge
completely from that point on.

This is measurable in the two pinned runs. Identical prompt (verified by hash), temperature 0, same
provider, same seed. Stage 1's output was byte-identical up to **character 810**, then:

```
det1: ..."obligation_text":"No person shall be a member of the Board of more than one bank."
det2: ..."obligation_text":"The license for a National Bank shall stipulate that it shall be a
                            Saudi Joint Stock Compa...
```

From one differing token onwards the documents share only **44% similarity**, ending at 27 vs 44
extracted obligations. That is the whole variance you have been seeing, from a single flipped token
800 characters in.

The measured provider table above shows this is *provider-specific*, not inherent: four of five
tested providers returned byte-identical output three times running. Whatever AtlasCloud does —
speculative decoding, non-deterministic MoE expert routing, batch-variant kernels — the others do
not do it.

## Cause 5 — The staged architecture amplifies small differences

This is why the variance looks much bigger than "a token here and there".

The stages are chained: stage 1's output *is* stage 2's input, and stage 2's output *is* stage 3's
input. So a single different decision early on cascades:

```
stage 1 splits one sentence into 2 obligations instead of 1
   -> stage 2 receives a different obligation list       (different input, not just different output)
      -> different ids, different classifications
         -> stage 3 receives a different set of ongoing obligations
            -> a different number of controls, with different titles
```

One changed grouping decision at stage 1 rewrites everything downstream. The pipeline has no
stabilising feedback — nothing pulls a diverged run back on track.

This is inherent to staging and is *not* a bug; it is the cost of the decomposition that gave the
quality in the first place. It does mean stage 1 stability matters more than anything else, which is
why the fixes above are applied to every stage but matter most at stage 1.

## What changed in the code

`processor/staged_LLM_Analyzer.py`, deterministic mode — **on by default**:

| Setting | Value | Env var |
|---|---|---|
| Temperature | `0` on all stages | `LLM_DETERMINISTIC=0` to restore per-stage temps |
| `top_p` | `1` | — |
| `seed` | `20250101` | `LLM_SEED` |
| Provider | pinned to `AtlasCloud` | `LLM_PROVIDER` |
| Quantization | pinned to `fp8` | `LLM_QUANTIZATION` |
| Fallbacks | disabled | `LLM_ALLOW_FALLBACKS=1` to allow |

`allow_fallbacks: false` is a deliberate trade-off. If the pinned provider is unavailable the call
fails and retries, rather than silently returning output from a different engine. For a compliance
library, a loud failure beats a quiet inconsistency. Set `LLM_ALLOW_FALLBACKS=1` if availability
matters more than reproducibility for a given run.

Constructor overrides are available too:

```python
StagedLLMAnalyzer(deterministic=True, provider="AtlasCloud", quantization="fp8")
```

## Step by step — making the fix yourself

Everything below is already wired up in code. The only change needed is which provider is pinned.

### Step 1 — Confirm the problem for yourself (optional, ~1 minute, ~$0.002)

```
python benchmarks/provider_determinism.py
```

Three identical calls per provider at temperature 0. You should see `AtlasCloud VARIES` and the
others `IDENTICAL`. If AtlasCloud comes back identical this time, run it again — it is intermittent,
which is exactly what makes it hard to notice in normal use.

### Step 2 — Leave the provider on AtlasCloud

**Do not set `LLM_PROVIDER=Novita`** — Novita cannot do JSON mode and every stage will fail with 404.

AtlasCloud is the code default and supports everything the analyzer sends, so the simplest correct
action is to leave `LLM_PROVIDER` unset entirely. Alibaba, Baidu and SiliconFlow are equally valid if
you want to move off it.

Note the env var name has **no leading underscore**. In `staged_LLM_Analyzer.py` the line reads:

```python
_LLM_PROVIDER = os.getenv("LLM_PROVIDER", "AtlasCloud")
#     ^ python variable          ^ the name .env must use
```

Writing `_LLM_PROVIDER=...` in `.env` silently does nothing and you fall back to the default.
Likewise, no spaces around `=`.

### Step 3 — Check the other determinism settings are on

Still in `.env`, these are the defaults if unset — set them explicitly if you want them pinned:

```
LLM_DETERMINISTIC=1        # temperature 0, top_p 1, seed, pinned provider
LLM_QUANTIZATION=fp8       # do not silently get DeepInfra's fp4
LLM_ALLOW_FALLBACKS=0      # fail loudly rather than drift to another provider
LLM_SEED=20250101
```

### Step 4 — Prove it worked

Run the same text twice and compare:

```
python benchmarks/analyzer_bench.py --text-file benchmarks/runs/baseline/input_clean_text.txt --title "Banking Control Law" --regulation-id 103296 --label novita1
python benchmarks/analyzer_bench.py --text-file benchmarks/runs/baseline/input_clean_text.txt --title "Banking Control Law" --regulation-id 103296 --label novita2
python benchmarks/analyzer_bench.py --compare novita1 novita2
```

`--text-file` replays the exact captured input, so this stays valid even though regulation 103296 no
longer exists in the database (see note below).

**Success looks like:** identical obligation counts, identical control counts, identical criticality
and execution-category distributions. For a byte-level check:

```
python -c "import hashlib;a=open('benchmarks/runs/novita1/calls/01_stage1_extract_completion.txt',encoding='utf-8').read();b=open('benchmarks/runs/novita2/calls/01_stage1_extract_completion.txt',encoding='utf-8').read();print('stage1 identical:',hashlib.sha256(a.encode()).hexdigest()==hashlib.sha256(b.encode()).hexdigest())"
```

For reference, the same check on the AtlasCloud runs (`det1` vs `det2`) reports `False`, with 38 vs
44 obligations.

### Step 5 — Decide on quality before rolling out

Determinism is not the only thing that matters. Novita is a different inference stack from
AtlasCloud, so output *character* may differ even though it is now stable. Compare a Novita run
against `benchmarks/runs/optimized/` and check the obligations still read correctly — particularly on
an Arabic document, where quantization and tokenizer handling matter more.

If Novita looks worse, GMICloud and SiliconFlow are both reproducible fp8 alternatives; just change
`LLM_PROVIDER`.

### A caveat worth keeping in mind

The provider test used three calls on one short prompt. That is enough to identify AtlasCloud as
unreliable, but it is not proof that Novita is deterministic under all conditions — long prompts,
heavy load, or a provider-side deployment change could reintroduce drift. Step 4 is worth re-running
periodically, and the cache below is what makes it not matter.

## The actual guarantee: don't re-run at all

Pinning and temperature 0 make runs *highly similar*. They cannot make them *identical*, because of
Cause 4.

For genuine reproducibility — the kind an auditor needs, where the analysis attached to a regulation
today is the same one attached to it next year — the answer is not to make the LLM deterministic. It
is to **only analyse each document once**:

1. Hash the normalized text: `sha256(clean_text) + model + provider + prompt_version`.
2. Before analysing, look for a stored analysis with that hash.
3. If found, reuse it. Do not call the LLM at all.
4. Re-analyse only when the hash changes (the document genuinely changed) or someone explicitly
   forces it.

This is item §4 "content-hash cache" in [staged_analyzer_optimization.md](staged_analyzer_optimization.md),
still outstanding. It makes repeat analysis both free and perfectly reproducible, and it removes the
question entirely for the `monitoring_status == "modified"` reprocessing path, which currently
re-analyses documents whose text may not have changed at all.

**Recommendation: treat the settings above as damage limitation and the content-hash cache as the
actual fix.**

## How much variance is left

Measured by running the same document twice with deterministic mode on — see
`benchmarks/runs/det1` and `benchmarks/runs/det2`, and the comparison at
`benchmarks/runs/compare_det1_vs_det2.md`.

Reproduce:

```
python benchmarks/analyzer_bench.py --regulation-id 103296 --label det1
python benchmarks/analyzer_bench.py --regulation-id 103296 --label det2
python benchmarks/analyzer_bench.py --compare det1 det2
```

## A note on judging the earlier before/after comparison

The baseline-vs-optimized comparison in [optimization_results.md](optimization_results.md) mixes two
effects: the deliberate changes, and this run-to-run variance. The differences reported there
(38 → 39 obligations, criticality 31/7 → 34/5) are within the variance band and should not be read
as caused by the optimization. The things that are genuinely attributable to the changes are the
structural ones: 0 → 22 controls, truncation eliminated, taxonomy violations eliminated, tables
rendered from real data.
