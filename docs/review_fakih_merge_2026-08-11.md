# Review: `feature/crawler-dev-fakih` merge — 2026-08-11

**18 commits, 42 files, +9,261 / −115.** All from 2026-08-10 and 2026-08-11 (the previous
merge `bb4470c` absorbed everything before that). Author: mohammadfakih-stack.

Verified `git merge-tree` beforehand: **merges cleanly, zero conflicts.** Nothing in it touches
`processor/` or `apis/pipeline_api.py`, so none of the analyzer work collides.

Their own handover note is `docs/change_detection_summary.md` — read that first, then this.

---

## The one-paragraph version

Two separate pieces of work. First, **four pre-existing production bugs are fixed** — these change
what your database contains and what the API returns, and they are the part that needs your
attention. Second, a **new change-detection layer** is added that answers "what actually changed?"
before a crawl runs, so you re-crawl a shortlist instead of everything. The second part ships
switched off. The first part is live the moment this merges.

---

# PART 1 — The bug fixes (live immediately, changes production data)

These are the ones to understand. Everything else is additive; these alter existing behaviour.

## 1.1 Archived analyses were never retired — duplicate live requirement sets

**File:** `storage/mssql_repo.py`, `archive_current_analysis()`

**Before.** The method ran a single `INSERT ... SELECT` copying current rows into
`compliance_analysis_versions`. It copied. It never retired the originals — they kept
`is_current = 1`.

Every reader filters on exactly that flag (`get_compliance_analysis`, and `apis/pipeline_api.py`).
So after a document was updated:

- the regulation returned **both** its old and its new requirement set as live
- the next update archived both again
- the archive table grew quadratically

It also copied `is_current` through verbatim, so archived rows landed flagged current.

**Why nobody caught it.** `ExcelRepo.archive_current_analysis` — the preview path — retires
correctly. Every dry run looked clean while production doubled.

**After.** Two statements, in a required order, in one transaction:

1. copy into `compliance_analysis_versions` with `is_current = 0, status = 'inactive'` hardcoded
2. then `UPDATE compliance_analysis SET is_current = 0, status = 'inactive'`

Order matters — the copy is scoped by `is_current = 1`, so retiring first would archive nothing.
Split into `_archive_analysis_stmts()` so the order and scope are testable without a database.

Rows are flagged, not deleted, so an incorrect archive is recoverable. Re-running is safe: a second
call finds nothing current, archives 0, retires 0.

> **This is the change your users will notice.** A regulation currently showing two live requirement
> sets will show one. It will look like requirements disappeared. Warn people before this reaches
> production.

## 1.2 Requirement mappings appended instead of replacing

**File:** `storage/mssql_repo.py`, `store_requirement_mappings()`

**Before.** A plain `INSERT` loop with no cleanup. Re-analysing a regulation appended a **second
full set** of mappings. For CBB, `version_id` at least told the sets apart; for every other
regulator `version_id` is `NULL`, so old and new were indistinguishable.

**After.** Delete-then-insert, scoped to `(regulation_id, version_id)`, in one transaction. Scoping
matters: for CBB each content version keeps its own mapping set, so clearing by regulation alone
would destroy real history. Logs how many prior rows were replaced.

## 1.3 Suggested requirements duplicated on every re-analysis

**Files:** `storage/mssql_repo.py` + `orchestrator/orchestrator.py`

**Before.** Two problems compounding:

```python
for i, mapping in enumerate(new_req_mappings):
    ...
    "ref_key": f"AUTO-{regulation_id}-{i}",
```

- `insert_new_suggested_requirement` never checked whether the row already existed, so every
  re-analysis inserted a fresh set of `AUTO-…` rows into `COMPLIANCE_REQUIREMENT`
- the key was a **loop index over an LLM-generated list**

**This is the same problem we measured today.** Their commit note says it directly:

> *"`i` is a position in a list the LLM produced. Re-analyse the same regulation and the model may
> emit a different number of new requirements in a different order, so `AUTO-42-0` could name
> different regulatory text on every run."*

That is exactly the variance in `docs/determinism.md` — 27, 38 and 44 obligations across three runs
of one document. They hit it from the database side; we hit it from the model side.

**After.** The key is derived from the text:

```python
digest = hashlib.md5(req_text.strip().encode("utf-8")).hexdigest()[:8]
"ref_key": f"AUTO-{regulation_id}-{digest}",
```

plus a new `find_requirement_by_ref_key()` that returns the existing id instead of inserting again
(lowest id wins, so it stays stable even where duplicates already exist).

Note the dependency: reuse-on-matching-key is **only safe because the key is content-derived**. If
anyone reverts the key to an index, the dedup becomes actively wrong — it would attach one
requirement's key to another's text.

## 1.4 Archived versions were being stored empty

**File:** `storage/mssql_repo.py`, new `_with_extra_meta()`

**Before.** `extra_meta` is JSON text in the column and a dict everywhere else. Both identity
lookups (`find_by_identity`, and the new `find_by_identity_fields`) omitted the column entirely. The
archive step then read the old row's `content_text` as `""` — so **every archived version was stored
empty** on SQL Server.

**After.** Both lookups select `CAST(extra_meta AS NVARCHAR(MAX))` and parse it to a dict, tolerating
malformed JSON.

## 1.5 The completeness gate had never run on one crawl path

Mentioned in their summary and implemented across `formfill/orch.py` + `crawl_absence.py`. The crawl
path had `run_history` but nothing per-document, which is why its `disappeared` bucket was always
*reported* and never *judged*.

---

# PART 2 — The new change-detection layer (ships switched off)

## The problem it solves

`content_hash` is derived from a document's **URL and link text** — and the URL is also its identity.
So a regulator that replaces the PDF behind an unchanged link produces an identical hash, and the
document reads `unchanged` **forever**.

> Directly relevant to what we found today: all 12,487 rows currently have **no** `content_hash` at
> all, so `classify_documents` marks everything "modified" on every crawl. This work is the machinery
> that fixes both ends of that.

## The approach

Ask the server what version it holds, instead of guessing from what we can see.

A two-byte ranged `GET` returns validators that actually move — SharePoint answers
`ETag: "{GUID},<version>"` and increments on every save. Six of the ten regulators run SharePoint,
which is why this is one generic sweep rather than ten site-specific ones. They measured all ten
first.

## The modules

| File | Lines | What it does |
|---|---:|---|
| `changesignal.py` | 300 | The common shape: an observation, a verdict, what a run remembers |
| `change_state.py` | 156 | One JSON file per source, keyed on the source's own identity |
| `fingerprint.py` | 122 | The two-byte ranged GET that reads ETags/validators |
| `inventory_sweep.py` | 234 | Walks stored documents for a source, re-probes each |
| `gosi_signal.py` | 259 | GOSI publishes pages as JSON — one request sees everything |
| `sitemap_signal.py` | 320 | MHRSD's per-url `lastmod` |
| `snapshot_articles.py` | 190 | Per-**article** hash of a saved page, no network at all |
| `crawl_absence.py` | 226 | Per-document absence memory for the crawl path |
| `withdrawal.py` | 158 | Turns an absence streak into a *proposal* |
| `cli/sweep.py` | 333 | `python -m dynamic_crawler.cli.sweep --signal ...` |

Two design choices worth noting, both explained in the code:

**State is JSON files, not a database table.** Deliberate: a sweep must run with no route to the
database, and it records two things no column holds — how many sweeps in a row a document has been
absent, and *why* a probe produced no token.

**Sitemaps are treated with suspicion.** Four of five sitemaps measured carry a `lastmod` on every
url with a single distinct value — the sitemap's own build time, which can shortlist nothing. The
module refuses those rather than trusting them.

## The safety rail

**Nothing in this code can remove a regulation from the library**, and there is a test enforcing it.

A missing document is a *proposal*, never an action. It requires two runs at least 20 hours apart,
a passing health check, and then it tells a person. `mark_regulation_withdrawn` exists in the repo
layer but the docstring says plainly: *"Nothing calls this yet."* It sets `status = 'withdrawn'`
plus a marker version — never a `DELETE`.

---

# PART 3 — Configuration

**New `config/change_signals.yml`.** Per-source settings for identity fields, whether to confirm a
change by fetching bytes, worker count and timeout. Every value carries the measurement that
justifies it as a comment.

**`skip_hosts`** — hosts no sweep may touch regardless of what URL a stored row holds. Currently
`simah.com` (Cloudflare 1020-class block). The `until` date is when the block may be *reviewed*, not
when it expires; nothing unblocks itself.

**SIMAH source disabled**, with a written reason rather than an empty `sources:` list that would
parse to nothing.

**The version probe is OFF everywhere**, pending your review:

> *"PENDING CODE REVIEW — then set `version_probe: true`. The first probed run lays down one metadata
> baseline per stored document, so it is a write: it needs sign-off before it runs against
> production."*

So merging this does **not** start probing anything.

---

# PART 4 — Tests

~4,700 lines across 11 new test files: `test_change_signal`, `test_crawl_absence`,
`test_fingerprint`, `test_gosi_signal`, `test_identity`, `test_inventory_sweep`,
`test_sitemap_signal`, `test_snapshot_articles`, `test_stage_a`, `test_stage_b`,
`test_targeted_recrawl`, `test_withdrawal`.

They report 314 tests, none needing network or a database. `test_stage_a.py` specifically covers the
order and scope of the two archive statements — the two things that were wrong in 1.1.

Run them:

```
python -m pytest tests/ -q
```

---

# PART 5 — What you need to decide or verify

Ordered by how much it matters.

**1. Every database change is unverified.** They had no network route to `10.11.12.76:1437` — no VPN
adapter, port unreachable. So all of Part 1, the highest-impact work in this merge, has been tested
only against mocks. **You have that access; they don't.** The end-to-end checks they were blocked on
are things only you can run.

**2. Warn people about the duplicate fix.** Regulations showing two live requirement sets will show
one. Correct, but indistinguishable from data loss to anyone who doesn't know it's coming.

**3. Run the audit before and after.** `scripts/audit_duplicate_analysis.sql` (145 lines, new) is
there to quantify the existing duplication. Worth capturing the numbers before the fix lands so you
can show what changed.

**4. Decide on the version probe.** It stays off until someone sets `version_probe: true`. First run
writes one metadata row per stored document — that's ~12,487 writes. Their ask is explicitly for
sign-off.

**5. The malformed filename.** Confirmed present in the repo root:
`s -ExecutionPolicy RemoteSigned) ; (& c:Users…Activate.ps1)`. A shell command saved as a filename.
Breaks `git clone` and `git worktree add` on Windows. One `git rm` fixes it; they left it alone
because it rewrites history for everyone. **Your call as lead.**

**6. Check it against today's work.** Nothing collides at file level, but two things interact:

- their change detection and our `processor/analysis_cache.py` both answer "has this changed?" at
  different layers — theirs on the crawl, ours on the analysis. Worth a deliberate decision about
  which is authoritative rather than letting both run blind.
- their `ref_key` fix depends on stable text; our cache depends on stable *input*. Together they
  address the same underlying instability from opposite ends.

---

# PART 6 — Verification commands

After merging:

```powershell
# today's analyzer work still intact
python tests/test_staged_analyzer_offline.py          # expect ALL PASSED (39)

# their suite
python -m pytest tests/ -q                            # expect ~314 passing

# the API still imports after +367 lines of repo changes
python -c "import sys; sys.path.insert(0,'.'); import apis.pipeline_api"

# the matcher still agrees with its baseline
python benchmarks/matcher_bench.py --from-run optimized --label matcher_postmerge
python benchmarks/matcher_bench.py --compare matcher_client2 matcher_postmerge
```

The last one should land in the 92–97% band established in `docs/determinism.md` — that's the noise
floor, not a regression.

---

# Overall read

The engineering quality is high. Every non-obvious decision carries a comment explaining the
measurement behind it, the bug fixes each state what was wrong and why it hid, and the destructive
capability is gated behind a proposal-to-a-human with a test enforcing it.

The risk is not the new code — that ships inert. **The risk is Part 1**, which changes production
data on merge and could not be tested against a real database by its author. That is the part to
verify yourself before this goes anywhere near production.
