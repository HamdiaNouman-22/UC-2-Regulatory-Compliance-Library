# Monitoring — how we detect updates, per regulator

**Written:** 2026-08-02
**Revised:** 2026-08-10 — both decisions are made and built. §2, §4 and §6 now
describe what the code does rather than what to decide.
**Status:** design + the two decisions that must be made before anyone codes.
**Audience:** the three of us. Read `dynamic_crawler/HANDOFF.md` first.

---

## 1. The good news: most of this already exists

`orchestrator.py::_extract_and_analyze_versioned` already implements versioning:
fetch the old content, compare hashes, create version B, store a change summary,
re-run analysis against the new version. `storage/mssql_repo.py` already has
`get_cbb_content_hash` / `update_cbb_content_hash`.

It only ever runs for CBB — **not because it is CBB-specific logic, but because
only the CBB monitoring crawler sets the three keys that trigger it**:

```python
doc.extra_meta["monitoring_status"]        # "new" | "modified"
doc.extra_meta["existing_regulation_id"]   # which row to version
doc.content_hash                           # what changed
```

So the work is **not** "build monitoring". It is:

1. decide the identity key (§2),
2. set those three keys for every regulator instead of one (§3),
3. add the safety gate that stops a bad crawl deleting the library (§4).

The two repo methods are named `*_cbb_*` but their SQL is regulator-agnostic
(`SELECT content_hash FROM regulations WHERE id = ?`). Rename, do not rewrite.

---

## 2. DECISION 1 — the identity key — DECIDED, and it is per SOURCE

**Ruled and built 2026-08-10: one answer per SOURCE, not per regulator**, with the
regulator's value as the default when a source names none. `(document_url,
doc_path)` is still that default, and no config overrides it today, so this changed
no behaviour on any existing run.

**Why it moved.** A regulator's config is a LIST of sources. SAMA's holds a
DataTables grid whose documents have a `Circular No.` and two rulebook walks whose
articles have none — one key for the file has no right answer. `[reference_no]`
gives all 28 articles the identity `("",)` and they overwrite each other; the
default leaves a re-issued circular at a new url reading as one new document plus
one disappearance. Per source, each gets the key that fits it. The same is true of
`version_key`, which drives a lookup across the *whole* store, so a reference
number unique only within one source must not be allowed to drive it.

Each source stamps its own choice onto the documents it produced
(`crawler/generic_crawler_wrapper.py` — the composite's fetch loop is the only
place that knows which source a document came from), and it is read back per
document in `formfill/orch.py::_identity_for`. The change store reads the same
stamp, so the sweep and the ingest path cannot disagree about what "the same
document" means.

**Not yet possible on the formfill path:** a hint file cannot declare an identity
(`formfill/api.py`, and it is not in `formfill/schema.py`'s `KNOWN_KEYS`), so
form-backed regulators are stuck on the default however their site is shaped.

What the code does *today*, inconsistently (`filter_new_documents`):

| regulator | identity used |
|---|---|
| CBB | `source_page_url` |
| anything with a `published_date` | `(title, published_date, doc_path)` |
| "regulatory returns" | `(title, doc_path)` |

### Recommendation: `(document_url, doc_path)`, with `reference_no` as a tiebreak

Why not title+date:

- **Titles get rewritten by us.** `disambiguate_titles()` renames documents that
  share a title (SDAIA's several "2025" entries). An identity key we mutate is
  not an identity key.
- **Dates are missing on many sites.** MISA, AML, SDAIA and the SAMA sandbox
  publish no issue date at all on their listings. That is four of our six.

Why URL, with the caveats stated honestly:

- It was 100% present and 100% distinct on every site measured — 4,160 / 4,160
  on SBP, 36 / 36 on SDAIA, 89 / 89 on MISA.
- Paired with `doc_path` because regulators cross-list one file under several
  sections and each placement is its own row in the library. `document_exists_by_url`
  is already category-scoped, so the DB agrees.
- **Caveat A — a regulator that republishes at the same URL** shows up as
  *modified*, which is correct: the content hash catches it.
- **Caveat B — a regulator that puts each revision at a NEW url** shows up as
  one *new* + one *disappeared*, which is wrong. `reference_no` is the fix:
  same reference, different URL = a new version, not a new document. SBP fills
  `reference_no` on 95% of rows, so it works there. Where it is absent, accept
  new+disappeared and let a human reconcile.

**Write the decision into `config/sources/<regulator>.yml`** rather than into
code, so it is visible per source:

```yaml
  - name: "Circulars"
    mode: formfill
    hints: dynamic_crawler/hints/sbp.circulars.yml
    identity: [document_url, doc_path]      # default
    version_key: reference_no               # optional: same ref + new url = new version
```

---

## 3. How change detection runs, per shape

### List sites (SBP, SDAIA, AML, MISA) — the listing IS the feed

Phase 1 alone gives a complete inventory with dates and reference numbers and
costs a fraction of a full crawl. So:

```
1. run phase 1 only            (SBP: 139 pages, ~32 min. MISA/AML/SDAIA: seconds)
2. compare the inventory to the DB on the identity key
3. new      -> run phase 2 for THOSE ROWS ONLY, then insert
   modified -> phase 2, then the existing versioning path
   unchanged-> do nothing (the overwhelming majority)
   gone     -> see §4 before you believe it
```

The saving is the whole point: SBP's phase 2 is 4,160 page loads. Running it only
for new rows turns a multi-hour nightly job into a 32-minute one.

`formfill run --no-details` is exactly step 1, and `formfill run --only-urls
<file>` is step 3 — phase 1 still walks the whole listing, so the inventory stays
complete and only the named rows are opened. `sweep --targets <file>` writes that
list from the documents a sweep ruled `modified`.

The rows it walks past are recorded as `detail_skipped`, and the orchestrator
puts them in a `not_reread` bucket: not compared, not written, and not counted
absent. A row dropped from a targeted run instead of recorded is a row §4's gate
reports as disappeared.

### Tree sites (SAMA sandbox, rulebooks) — no cheap listing exists

A tree has no listing page to diff; you have to walk it. It is cheap anyway
(40 pages, ~2 minutes), so walk the whole thing and compare `content_hash` per
page. `content_hash` is the md5 of the page's normalised **text**, not its HTML —
HTML churns on every deploy and would report everything as modified.

### Sites that publish a feed — ask before you crawl

**This is the question to put to A and B this week:** does your regulator publish
a `sitemap.xml` with `<lastmod>`, an RSS feed, or a "recently updated" page? CBB's
Thomson Reuters feed is a ~10× saving over crawling, and any of those three gives
the same benefit. Ten minutes of checking can remove a nightly crawl entirely.

---

## 4. DECISION 2 — the completeness gate, and "disappeared"

**"Disappeared" is the dangerous outcome.** New and modified are additive; if we
get them wrong we add noise. If we get *disappeared* wrong we remove real
regulations from the compliance library.

And we know it goes wrong. **SDAIA returned 415 → 363 → 439 documents across
three runs of identical code.** A run that lost 52 documents was not a run where
52 documents were withdrawn.

### The rule

> **A run may not mark anything disappeared unless the run itself is trustworthy.**

Trustworthy means all of:

| check | where it comes from |
|---|---|
| 0 blocked pages | `run.json.blocked_pages` — the WAF guard, already built |
| no early-stop, no failed listing pages | `run.json.warnings` — already emitted |
| the walk was not cut short by a cap | `run.json.plan.capped_by_max_pages` — already emitted |
| count within tolerance of the last good run | **needs building** — store it |

Otherwise: **quarantine the run.** Ingest nothing, alert, keep yesterday's data.
A missed day is recoverable. A wrongly emptied library is not.

For the count tolerance, start at **±5%** and tighten per regulator once you have
a few weeks of history. SDAIA's own spread was ±9%, so ±5% would have quarantined
those runs — correctly.

Even when a run is trustworthy, I would not delete. Mark
`status = 'withdrawn'` with the date, and let a person confirm. Regulators do
withdraw documents, but rarely, and it is worth a human glance.

**Built 2026-08-10** — `dynamic_crawler/withdrawal.py` for the change sweeps,
`dynamic_crawler/crawl_absence.py` for the crawl.
Every sweep report carries a `withdrawals` block: `withdrawal-proposed` needs the
document absent from two consecutive sweeps **spanning 20 hours**, attributed to
the signal that is judging it; anything else is `watching` or `not-judged` and
says which condition stopped it. Four things learned building it:

- **A signal may only judge absences it recorded.** The state file is per source,
  not per signal, so two signals can share one — and each would otherwise report
  the other's documents as withdrawal candidates.
- **Counting sweeps is not measuring time.** Nothing stops two runs of the CLI a
  second apart, and a regulator halfway through republishing is not a withdrawal.
- **The count allowance is one document OR 5%, whichever is larger.** Most sources
  here hold 12–17 documents, where one document is 6–8%: a flat 5% blocks every
  real single withdrawal and the whole layer never proposes anything.
- **A signal that cannot see the whole inventory proposes nothing at all**, which
  is what keeps the sweep over stored urls and the sitemap out of this entirely.

`mark_regulation_withdrawn` exists on both repos and **is called by nothing** —
it sets the status and inserts a marker version rather than deleting, so the row
leaves the gate and every sweep while staying readable.

**The crawl path reaches the same three verdicts through `crawl_absence.py`**, and
`withdrawal.decide` is shared unchanged. Three things are its own:

- **Its streak memory is a directory of its own**, `output/change_state/crawl/`.
  A sweep counts an absence for every key in the file it opens and `missed()`
  never asks who owns the record, so a shared file would let a daily sweep build
  the crawl's streak — and its 20-hour span — on the crawl's behalf.
- **The verdict is per source, not per run.** A gate problem naming one source
  stops that source; a bot-protection page or a cap stops all of them. A stored
  row that cannot be charged to a source — a composite writing one
  `source_system` from two sources — is `not-judged` rather than charged to a
  guess.
- **It re-asks the count question with the one-document allowance.** The gate's
  flat 5% quarantines a source of 17 that lost one document, which would have
  shipped this inert on every source under 20. The gate's own tolerance is
  untouched; there are simply two count rules, each named where it is used.

A run that walked past pages without opening them proposes nothing, the same rule
as a sweep's `--no-documents`. Being seen clears a streak whatever the gate said;
only a source that passed it advances one.

### Store three numbers per run per source

That is all the history you need:

```
run_at | source | row_count | inventory_hash   (md5 of the sorted identity keys)
```

`inventory_hash` unchanged = nothing at all changed, skip everything else. It is
the cheapest possible early exit and it will be the common case.

**Built 2026-08-10, with three corrections worth knowing:**

- **One row per source AND one for the regulator's total.** The tolerance was
  measured against the sum, and a composite logs a failed source and carries on —
  so a small source dying entirely hid inside the 5%. The gate now checks both.
  A single-source run still writes exactly one row.
- **Each per-source row carries the verdict its OWN problems earn**, by the same
  attribution the withdrawal decision uses. Stamping the run's verdict on them
  froze a healthy source's baseline for as long as a sibling was broken — and
  `last_good_run` returns PASS only, so that source then failed its own count
  check against a baseline several runs old, and the withdrawal layer read the
  stale number as its prior. A `total` count problem is the one exception: it
  stops only the sources that had no baseline of their own to be checked
  against, because a source already found within tolerance is answered and a
  source with no history must not have its first baseline set by a short run.
  The run's own verdict is unchanged, and `gate_by_source` in the report says
  which source was stopped by what.
- **The recorded verdict answers "may this count be the baseline?", which is not
  "may this run act on absences?".** Sharing one answer deadlocked the gate: a run
  distrusted for a count was also a run that refused to remember what it saw, so
  the same step change was re-detected for ever and no source that grew ever got
  a new baseline. **A count that ROSE is remembered; a count that FELL is not.** A
  prior that is too high only makes the withdrawal gate stricter, while a prior
  that is too low is what opens it on the documents a truncated crawl lost. The
  report carries both answers — `run_trustworthy` and `baseline_verdict` — and a
  rise does not excuse a problem of any other kind. A source with no baseline at
  all takes its first one only from a run with nothing against it: raising a
  prior is not the same as inventing one.

**What this leaves, and it must be built with the withdrawal write:** a count that
FELL for a real reason still waits for a person, by design — and confirming those
withdrawals is not enough on its own. `mark_regulation_withdrawn` removes the rows
from the library, but the crawl's count stays below a baseline nobody lowered, so
the gate goes on quarantining the source for the shrink it just approved. **Whatever
wires up the status write has to bring the baseline down in the same step.** Nothing
does today, because nothing withdraws.
- **The identity keys are `field=value` pairs, not bare values**, because one run
  can carry two sources keyed on different fields entirely.
- **`source` is `NVARCHAR(200)` and the writer logs its own failures**, so an
  over-long key costs the gate its baseline and says nothing. Truncate at the call
  site.

**Looking up what a source already stored needs BOTH the regulator and the
source_system.** They are not the same thing and `source_system` is not unique:
AML and SIMAH both publish under `Rules and Regulations`, SDAIA and MISA both under
`Laws and Regulations`. Scoped on the string alone, one regulator's run reads
another's library and offers it up as disappeared.

---

## 5. Per-regulator plan

| regulator | shape | monitoring run | cost | cadence |
|---|---|---|---|---|
| **SBP** | list | phase 1 only, 139 pages | ~32 min | daily |
| **MISA** | list | full (1 page) | ~5 s | daily |
| **AML** | list | full (1 page) | ~8 s | daily |
| **SDAIA** | list | full (1 page) | ~15 s | daily |
| **SAMA sandbox** | tree | full walk, 40 pages | ~2 min | weekly — rulebooks move slowly |
| **SAMA circulars** | list | phase 1 | — | daily |
| **SIMAH** | blocked | — | — | **manual until the WAF is solved** |
| **CBB** | feed | Thomson Reuters | already built | as now |

Everything except SBP is seconds. Do not over-engineer the scheduling: the whole
KSA set outside SBP is under a minute of crawling.

---

## 6. What to build, in order

1. **Decide the identity key** (§2). One line in each source YAML. *Lead.*
2. **Generalise `filter_new_documents`** into `classify_documents()` returning
   new / modified / unchanged / disappeared, using the configured identity
   instead of the three hardcoded regulator branches.
3. **Set the three `extra_meta` keys** in `GenericSiteCrawler` and
   `FormfillCrawler` so the versioning path that already exists fires for every
   regulator, not only CBB.
4. **Store `run_at | source | row_count | inventory_hash`** and add the
   completeness gate (§4). This is the one that protects the library.
5. **Rename `*_cbb_content_hash` → `*_content_hash`** in `mssql_repo.py`. No
   logic change.
6. **Ask A and B about feeds** (§3). May remove work rather than add it.
7. **Wire the cadence into `scheduler.py`.**

Items 1–4 are the real work. 5 and 7 are an hour each.

---

## 7. The trap to keep repeating

A crawl that returns fewer documents looks exactly like a regulator that
withdrew documents. Nothing in the data distinguishes them — not the count, not
the hashes, not the consistency across runs.

Only the *provenance* of the run tells you which it was: was it blocked, was it
capped, did pages fail, was the count within tolerance. That is why §4 is a gate
on the run and not a check on the documents.

If you take one thing from this document: **never let an untrusted run mark
anything disappeared.**
