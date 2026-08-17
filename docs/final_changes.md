# Plan — end-to-end monitoring / crawl / versioning test, plus merge follow-ups

**Part A** is the test you asked for. **Part B** is the action list from the
`feature/crawler-dev-fakih` review. Both to be written to `docs/final_changes.md` on approval.

---

# PART A — Test monitoring + crawling + versioning in one sequence

## Context

You want one API trigger that exercises monitoring → crawl → extraction → versioning →
(matching) → storage, with **analysis off**, results landing in an Excel workbook shaped like the
regulatory-document columns, and an approve step that puts *those same rows* into MSSQL without
re-crawling.

**Almost all of this already exists** in `dynamic_crawler/formfill/`, wired to `NewOrchestrator`:

| Need | Already built |
|---|---|
| Trigger a run | `POST /trigger/{form}` and `POST /trigger/source/{regulator}` |
| Analysis off | `analyse: bool = False` — already the default |
| Versioning for every regulator | `NewOrchestrator._process_versioned_doc` |
| Results to Excel, no DB writes | `ExcelRepo(out_xlsx)` — a drop-in for `MSSQLRepository` |
| Change detection on a second run | `?workbook=<existing>.xlsx` |
| Download the workbook | `GET /runs/{run_id}/excel` |
| Approve → MSSQL, no re-crawl | `POST /approve/{run_id}`, backed by `promote.py` |

`promote.py` states the guarantee you asked for outright: *"Re-running against MSSQL would be less
code, and wrong. It would re-crawl… re-analyse… Approval has to mean 'these rows', not 'whatever
this regulator looks like now'."* It matches on `(document_url, doc_path)` and skips anything
already present, so promoting twice inserts nothing the second time.

**Decision taken:** classification compares against **the workbook**, not MSSQL. So the
delete-and-recheck test deletes rows **from the workbook**, and MSSQL is only ever written by
`/approve`.

## Regulators to start with

The teammate's tested set, as available hint forms:

```
sdaia.regs        gosi.social_insurance     moe.regulations
aml.rules         gosi.saned                moh.rules_recent
misa.laws         mhrsd.regs                moh.rules_archived
```

Skip for now: `simah.rules` (Cloudflare-blocked, source disabled), `tadawul.rules` (fully blocked —
needs a browser crawl), `moh.*` if the forms are still `approved=false`. MC and ZATCA have no hint
file, so they cannot run through `/trigger/{form}` yet.

## The test sequence

Start the API:

```
venv\Scripts\python.exe -m dynamic_crawler.formfill.api
```

**Step 1 — baseline, everything, no analysis.** One regulator first; `sdaia.regs` is the cleanest
(36/36 documents returned a version token when measured).

```
POST /trigger/sdaia.regs?limit=0&analyse=false&reuse_last=false
```

`limit=0` takes everything. `reuse_last=false` forces a real crawl rather than replaying a crawl on
disk. Note the returned `run_id` and workbook name.

Expect: every document classified `new`, one `regulation_versions` row each ("first version"),
nothing written to MSSQL.

**Step 2 — inspect the workbook.**

```
GET /runs/{run_id}/excel
```

Check the sheets carry the regulatory-document columns and that the version rows are present.

**Step 3 — delete rows from the workbook** to simulate documents that are new or changed. Delete a
handful of document rows and save.

**Step 4 — re-run against the same workbook.**

```
POST /trigger/sdaia.regs?limit=0&analyse=false&reuse_last=true&workbook=<same>.xlsx
```

This is the monitoring test. Expect the deleted ones to come back `new`, the rest `unchanged`, and
no duplicate version rows for unchanged documents.

**Step 5 — test modification and versioning.** Edit a stored `content_hash` in the workbook so it no
longer matches, then re-run. Expect that document classified `modified`, its old analysis archived
and retired, and a **second** `regulation_versions` row created.

**Step 6 — approve into MSSQL.** Dry run first:

```
POST /approve/{run_id}?dry_run=true&confirm=false
POST /approve/{run_id}?dry_run=false&confirm=true
```

Expect the dry run to report what it would insert; the real run to insert exactly those rows. Run it
a second time and expect `skipped_already_present` to account for everything — that is the
idempotency guarantee.

**Step 7 — repeat for the rest**, one regulator at a time.

## What is genuinely missing (not needed for this test)

**The sweep is not wired into the API.** `grep` finds no reference to the change-detection sweep in
`dynamic_crawler/formfill/api.py`. Today the classification runs *inside a crawl* — so this tests
"crawl and classify", not "probe cheaply, then crawl only the shortlist".

Wiring `python -m dynamic_crawler.cli.sweep` and `--only-urls` into an endpoint is the step that
turns this from a cheaper crawl into real monitoring. Defer until Part A passes.

## TEST RESULTS — run 2026-08-11 against `aml.rules`

Executed against the live API (`uvicorn dynamic_crawler.formfill.api:app --port 8100`). **No database
writes.**

| Step | Result |
|---|---|
| 1. Baseline `?limit=5&analyse=false&reuse_last=false` | crawled 11, **all `new`**, 5 processed, 5 version rows, **0 analysis rows**, gate `PASS`, 37.7s |
| 2. Re-run same workbook | **`unchanged: 5`**, `new: 6` (the 6 not yet processed) — change detection correct |
| 3. Deleted a row + corrupted one `content_hash`, re-ran | **`modified: 1`, `new: 2`, `unchanged: 8`** — both detections correct |
| 4. Versioning check | regulation 1 now holds **3 versions**: `1 inactive "first version"` → `13 inactive "archived 2026-08-11"` (the corrupted hash, archived as prior) → `14 active "content changed"` (real re-fetched content). Correct. |
| 5. Approve dry run | would insert **11 regulations + 24 folders**, `failed: 0` |
| 6. Approve real | **NOT RUN — blocked by B11 below** |

Monitoring, classification and versioning all work as designed. `analyse=false` held throughout —
`compliance_analysis` stayed at 0.

## B11. `promote.py` silently drops every version row — BLOCKS APPROVE

**Found during the test above.** `dynamic_crawler/formfill/promote.py:188` calls:

```python
repo.insert_regulation_version(
    rid,
    content_text=..., content_html=..., content_hash=...)
```

But the method signature is:

```
insert_regulation_version(self, regulation_id, regulator, content_html, content_text,
                          content_hash, updated_date, change_summary, status='active')
```

`regulator`, `updated_date` and `change_summary` have **no defaults**, so every call raises
`TypeError: missing a required argument: 'regulator'`. The call sits inside
`except Exception: logger.error(...)`, so it fails silently and the API returns
`regulation_versions: 0` looking healthy.

**Effect:** approving a workbook inserts the regulations but **loses the entire version history** —
the exact thing the Excel-then-approve flow exists to preserve.

**Fix:** pass the missing arguments from the workbook row —

```python
repo.insert_regulation_version(
    rid,
    regulator=v.get("regulator") or reg_row.get("regulator") or "",
    content_html=v.get("content_html") or "",
    content_text=v.get("content_text") or "",
    content_hash=v.get("content_hash") or "",
    updated_date=v.get("updated_date") or date.today(),
    change_summary=v.get("change_summary") or "promoted from workbook",
    status=v.get("status") or "active")
```

`regulator` is not on the `regulation_versions` sheet, so take it from the matching `regulations`
row via `reg_map`. Also worth narrowing the `except Exception` so a signature error is not
indistinguishable from a database error.

**Do not approve any workbook until this is fixed** — you would get regulations with no version
history and no indication anything was missing.

## Verification

Success for Part A is:

- step 1 produces a workbook with every document as `new`, and MSSQL unchanged
- step 4 correctly reports the deleted rows as `new` and everything else `unchanged`
- step 5 produces a second version row and archives the prior analysis
- step 6 dry run matches the real run, and the second real run inserts nothing

Confirm MSSQL is untouched until step 6:

```sql
SELECT COUNT(*) FROM regulations WHERE source_system = '<the source>';
```

---

# PART B — Action items from the merge review

Full detail in `docs/review_fakih_merge_2026-08-11.md`. Database state observed 2026-08-11 (this DB
was reset mid-session — re-confirm against the one you actually crawl against):

```
compliance_analysis:           live=19  retired=0  total=668   ← 649 rows are is_current = NULL
compliance_analysis_versions:  0 rows                          ← archiving has never run
regulations:                   12,487   100% with no content_hash
SAMA document_url format:      /en/node/10911  (node ids, NOT slugs)
```

## B1. Verify archiving works before changing anything else

`archive_current_analysis()` was rewritten in this merge and has **never run against a real
database** — the author had no route to `10.11.12.76:1437`. Verify on one regulation, non-production:
count live rows → re-analyse via the crawler path → confirm one live set remains, the previous set is
in `compliance_analysis_versions`, `get_analysis_version_detail` reads it back, and a second run
archives 0 / retires 0. **Blocks B2 and B4.**

## B2. Cap retained generations in `compliance_analysis`

Archived rows are copied to the versions table **and** kept in place with `is_current = 0`, and
nothing ever removes them. Keep current + one previous generation; delete older retired rows in the
same transaction as the archive:

```sql
DELETE FROM compliance_analysis
WHERE regulation_id = ? AND is_current = 0
  AND created_at < (SELECT MAX(created_at) FROM compliance_analysis
                    WHERE regulation_id = ? AND is_current = 0)
```

Order on `version_id` where populated. **Do not ship before B1 passes** — the safety copy does not
exist yet, so purging first is data loss.

## B3. Make the API path archive instead of hard-deleting

`apis/pipeline_api.py:2711` (`/trigger/staged-analysis?force=true`) runs
`DELETE FROM compliance_analysis WHERE regulation_id = ? AND is_current = 1` — no archive, no
history. Two more unscoped deletes at `:4185` and `:4677`. Replace with
`repo.archive_current_analysis(...)`. Same endpoint as the content-hash cache, so handle together.

## B4. Investigate the 649 `is_current = NULL` rows

Invisible to every reader (`NULL = 1` is never true), and the new archive code scopes both statements
on `is_current = 1`, so they are neither archived nor retired. Any B2 purge assumes rows are 0 or 1.
Settle before B2.

## B5. Expect `AUTO-…` duplicates to keep growing slowly

The new `ref_key` is `md5(requirement_text)[:8]` — honest identity, not stable identity. The model
rewords obligations between runs (`det1` vs `det2` diverged at char 810), so reworded text yields a
new key and a new row. The fix is not re-analysing unchanged documents (the cache + B3), **not**
tightening the key. Never revert to a positional key — that is the corruption case the merge removed.

## B6. The monitoring layer is not live

Blockers: `version_probe: false` everywhere pending sign-off (so in-place replacements stay
undetectable); nothing calls `mark_regulation_withdrawn`; the configured regulators (SDAIA, AML, MOE,
GOSI ×2, MHRSD) are **absent from the current DB**, which holds SBP/SECP/SAMA; SAMA, CMA, Tadawul and
MOCI were **never measured**; GOSI and MHRSD forms declare no `regulator`/`source_system`; nothing
DB-touching was tested.

Order: confirm which DB is real → measure your four → reconcile GOSI/MHRSD naming → enable
`version_probe` on one source non-production → verify B1 → decide on `content_hash` → wire withdrawal
proposals to a person.

## B7. SAMA has a native revision feed — verify and exploit it

```
https://rulebook.sama.gov.sa/en/view-revision-updates
  ?f_date=on&changed_1[min]=YYYY-MM-DD&changed_1[max]=YYYY-MM-DD&items_per_page=40
```

Plain GET, no auth. Each entry gives title, slug URL, date and `book-trail` (hierarchy). Filters on
Drupal's `changed` timestamp; displays effective date. **If it lists newly-published documents as
well as revisions, SAMA gets updates AND discovery from one request** — no ETag probe, no tree walk,
no `version_probe` sign-off.

Still to verify: (a) does it include genuinely new documents — the first test was invalidated because
the DB stores **node-id** URLs (`/en/node/10911`) while the feed returns **slugs**, so URL matching
could never hit; re-test matching on normalised title; (b) whether `items_per_page` exceeds 40;
(c) whether SAMA Circulars and IT Governance appear in the same feed.

**It cannot detect deletions** — a removed document simply stops appearing. Deletion still needs a
probe or a listing walk.

**Integration note:** the feed speaks slugs, the library speaks node ids. Resolve the canonical node
URL when opening each changed document (you open it anyway), and join on that rather than on title.

Check every other regulator for an equivalent page **before** building probe adapters — a native
"what changed since" endpoint beats ETag probing on every axis.

## B8. Amendments published as separate instruments have no recorded relationship

`regulations` has no `supersedes` / `amends` / `replaces` column. A genuine amending circular with its
own reference number is stored as an unrelated new regulation, and the analyzer processes it
standalone. Decide between: accept it; record `amends: [regulation_id]` in `extra_meta` (no schema
change); or a `regulation_relationships(from_id, to_id, kind)` table.

## B9. Do NOT widen the identity tuple

A proposal to use `(title, doc_path, reference_no, document_url, published_date)` would make matching
worse: identity fields are **ANDed**, so more fields means more missed matches, and an edited title
produces one false `new` plus one false `disappeared` feeding the withdrawal gate. Blank fields are
also silently dropped, so documents with and without a reference number would follow different rules.
Keep `identity = (document_url, doc_path)` with `version_key = reference_no` as the new-url tiebreak.

## B10. Nobody currently has both kinds of access

The crawler developer cannot reach the database; this machine cannot resolve regulator hostnames
(`getaddrinfo failed` for `rulebook.sama.gov.sa`). So no single person can validate monitoring end to
end. Granting one of the two the missing access is probably a faster unblock than any code item here.

---

# PART C — Crawl-output corrections (2026-08-12)

Raised from reading the eleven crawl workbooks. Two global, then per regulator.

## C1. `extra_meta` no longer carries crawl bookkeeping — DONE

`extra_meta` is what a person reads about the DOCUMENT. It had become a log of
how the row was fetched. Checked every key for a reader before removing any:

| key | readers | action |
|---|---|---|
| `content_text` | 19 | **kept** — load-bearing (`orchestrator.py:183` Tier 1b, `formfill/orch.py:685` versioning) |
| `record_kind` | 1 | **kept** — `jobs/run_regulator.py:69` |
| `crawler`, `hints`, `form_approved_by` | 0 | removed |
| `parent_page_url`, `shape`, `seed_url`, `depth`, `row_text` | 0 | removed |

`hints` was the worst of them: it wrote an absolute developer path
(`d:\...\dynamic_crawler\hints\x.yml`) onto every stored row, and
`form_approved_by` was still the placeholder `"your name"`.

Files: `crawler/generic_crawler_wrapper.py`, `dynamic_crawler/formfill/pipeline.py`.

## C2. `status` is left EMPTY for a human — DONE

`status` is the review decision (active / reject) and it governs whether a row is
promoted to the main system. Nothing automated may write it, or the pipeline
approves its own output.

`_set_status` was writing the MONITORING state (`new` / `modified` / `unchanged`)
into it. That is a fourth meaning for a column whose docstring already lists
three. The monitoring state now lives only in `extra_meta["monitoring_status"]`
and `status` comes out empty.

One trap in the same change: `storage/mssql_repo.py` read the column as
`getattr(document, "status", "active") or "active"`, and `""` is falsy — so the
empty status was coerced straight back to `"active"`, marking every fresh row as
though a person had approved it. Now only a MISSING attribute defaults to
`active`, which preserves behaviour for every non-formfill crawler.

Files: `dynamic_crawler/formfill/orch.py`, `storage/mssql_repo.py`.

## C3. `doc_path` — one root cause behind four complaints — DONE

`doc_path` renders as `regulator | source_system | ...sections... | title`, so
`source_system` is a DISPLAY crumb as well as a machine key. The configs I wrote
used codes, and the codes were showing up in the library:

| | before | after |
|---|---|---|
| MC | `Ministry of Commerce \| MC-REGULATIONS \| …` | `… \| Regulations and Laws \| …` |
| ZATCA | `ZATCA \| ZATCA-RULES \| …` | `… \| Rules and Regulations \| …` |
| MHRSD | `MHRSD \| mhrsd.regs \| …` | `… \| Regulations and procedural guidelines \| …` |
| MOH | `Regulations` (no prefix at all) | `Ministry of Health \| Rules and Regulations \| <title>` |

MOH was separate: `moh_crawler.py` set `doc_path=[self.category]`, so the trail
had no regulator above it.

**`source_system` is an identity key, so changing it is not cosmetic.**
`config/change_signals.yml` looked MHRSD up under `"mhrsd.regs"`. Its own comment
had predicted this — *"reconcile it with whatever a library: block later stores,
or the sweep and the ingest path will keep separate memories of the same 63
documents"* — and it is now reconciled. The form is still FILED as `mhrsd.regs`;
only the stored `source_system` moved.

MISA: category `"Laws and Regulations"` → `"Laws"`, matching the site's own tab.

## C4. ZATCA and MC crawled site chrome, not regulations — FIXED

ZATCA returned 38 rows: Contact Us, Zatca Mobile Apps, Careers, News, Magazine,
Brand Identity. Zero regulations. MC was the same shape.

Root cause is in the shared engine, not either config:

```python
seed_prefix = urlparse(seed_norm).path.rstrip("/")   # generic_crawler/crawler.py
```

`scope: prefix` means "under the seed". That is right only when the seed is a
DIRECTORY. Both these seeds point at a PAGE, so the page's own filename ended up
in the prefix and no sibling could ever match it:

```
seed    /en/RulesRegulations/Pages/rules.aspx
prefix  /en/RulesRegulations/Pages/rules.aspx        <- a leaf
test    /en/RulesRegulations/Agreements  -> False    <- the real content
```

MISA, the third prefix-scoped source, has a directory seed (`/activities/laws`)
and crawled correctly. That is the tell — the failure is specific to file-leaf
seeds, which is what every SharePoint regulator gives us.

`scope_prefix()` now strips two segments: a trailing filename, then a trailing
`/pages` (SharePoint stores every page of a section there, so
`/en/RulesRegulations/Pages` names a storage folder while the real sibling
sections are `/Taxes` and `/Agreements`, under the section but not under
`Pages`). A directory seed is returned untouched. It never returns `""`, which
would match every path and silently turn `prefix` into `host`.

Verified: all four chrome URLs now out of scope, all three real sections in.
Regression test: `tests/test_scope_prefix.py` (10 passed).

**Both regulators still need a re-crawl to confirm the fix on the live sites.**

## C5. Still open

- **MC** — attachments and the deeper path hierarchy, after the C4 re-crawl
- **MHRSD** — HTML capture is wrong: the inner section should go to `extra_meta`,
  the rest stored as proper HTML, and the link also written to `document_url`
- **MOE** — rows present in the workbook that are not on the website
- **GOSI** — incorrect throughout; the partner's `panels` approach is the reference
- `'float' object has no attribute 'strip'` still blocks MOH / GOSI re-runs
- Re-approve the edited forms (`formfill verify` then `approve`)
- Re-run crawl + monitoring for everything corrected above

## C6. ZATCA — the real cause was NOT the scope bug

C4 is a genuine bug and the fix stands, but it was not what produced the 38 junk
rows. The reproduction shows why:

```
n_pages 1   n_documents 38   status ok
visit … "n_pdfs": 38, "text_len": 22, "queued": 0, "page_queued": 0
```

They were never pages. They were **documents**, and documents are collected
*regardless of scope* — so no scope setting could ever have filtered them.

`EXTERNAL_LAW_PORTALS` lists `zatca.gov.sa` and `mc.gov.sa`. The set answers "some
OTHER regulator links OUT to zatca.gov.sa; treat that link as a law, not a page".
The word doing the work is EXTERNAL. When the seed IS zatca.gov.sa, every
ordinary navigation link matches the host and is marked a terminal document —
which also stops the crawl descending, because every link it might have followed
was already classified as a leaf.

`generic_crawler/crawler_MISA_MC_ZATCA.py` **already had the fix** — an
`is_external_law_portal(url, seed_host)` guard with a docstring describing this
exact failure. The shared engine had the copy without it. Ported, and threaded
through `is_document_link`, `doc_type_of` and `probe_scope` (whose docs-vs-pages
ratio chooses the scope, so it was skewed too).

After: the same crawl returns real content — agreement PDFs, Bureau of Experts
law links, ministerial resolutions — instead of Contact Us and Careers.

**Still shallow.** `n_pages` stays 1: the landing page is JS-rendered and reads
back `text_len=22`, queueing nothing. `--wait-ms` existed in the engine but the
wrapper never passed it; now wired (`wait_ms:` in the source YAML, set for ZATCA
and MC). It did not lift the page count, so ZATCA's yield is currently its ~35
linked documents. Open.

## C7. GOSI was closer than it looked — the fault was a TITLE

Not "incorrect at all": 6 Social Insurance instruments and 2 Saned instruments,
matching the `panels` design. What was visibly wrong was one row stored under the
title **`Press Here`**.

`_GENERIC_LINK_TEXT` listed "click here" but not "press here", so the anchor text
became the document's name. ZATCA produced a `More` the same way. Widened to
cover press/tap/see more/details and the Arabic equivalents, and given a
filename fallback so the last resort is specific to the document:

```
'Press Here' + no row title  ->  'OH benefits Regulation'   (from the file)
'More'       + no row title  ->  'TFA'
'Social Insurance Law'       ->  unchanged
```

## C8. MOE's extra rows — a tab click that had not landed

The 20 extra rows are one document recorded under two categories:

```
Scholarship Procedures | … | Scholarship Program   <- true
Scholarship Procedures | … | Special Education     <- false
```

`_walk_tabs` clicked a tab, waited a fixed `wait_ms`, and harvested whatever was
on screen. When the site had not re-filtered yet it harvested the PREVIOUS tab's
rows under the NEW tab's label. The tab strip needs 14s to render; the per-tab
settle was 1500ms.

Pagination has guarded this for a long time — it stops on "rows unchanged after
click", because a disabled Next re-harvests page 1 forever. Tabs never got the
same guard. Now the row fingerprint is compared across clicks: unchanged means
wait longer and retry, and if still unchanged the tab is skipped and reported as
`unchanged-after-click` rather than harvested.

Tabs that genuinely share a document still record both placements — what is
rejected is a row set byte-identical to the previous tab's, which on a 16-tab
site is a failed click, not a coincidence. `tabs.wait_ms` also raised to 5000.

## C9. SAMA — same empty-run hole as CMA, closed before running

`SAMACombinedCrawler` wraps each of its three sub-crawls in `try/except`, logs a
traceback and continues, so one failing cannot take the other two down. The cost:
ALL THREE failing returns an empty list with no exception — indistinguishable
from a regulator that publishes nothing, which is how CMA reached `0 documents /
gate=PASS`. SAMA holds 651 documents, so zero is never true. It now raises, and
exposes `source_names` + `last_result` so the gate sizes each section separately.

Re-added to `benchmarks/run_all_regulators.py`, **last** in the list: the API
serialises runs behind one lock, and a five-hour SAMA anywhere but the end blocks
every short regulator behind it.

`benchmarks/run_source_standalone.py` (new) runs one source config OUTSIDE the
API for that reason — same crawler, orchestrator, repo and workbook shape, its
own process. It also moves an existing workbook aside instead of appending: a
corrective re-crawl must REPLACE, and MC/ZATCA had ended up holding 28+28 and
38+38 rows, old wrong rows sitting beside new ones in the file that gets approved.

## C10. The GOSI/MOH blocker: NaN is truthy — FIXED

`run failed: 'float' object has no attribute 'strip'` blocked every GOSI and MOH
re-run. The cause is one Python detail:

```python
old_hash = (existing.get("content_hash") or "").strip()
```

pandas represents an EMPTY Excel cell as `float("nan")`, and **NaN is truthy**.
So `or ""` never fires and `.strip()` is called on a float. It only bites on the
SECOND pass, when a workbook written earlier is read back — which is exactly the
change-detection path: the crawl works, the comparison against stored rows dies.

All six `.strip()` sites in `orch.py` now go through a NaN-safe `_text()`.
GOSI-SI and GOSI-Saned went from FAILED to 43s and 22s.

## C11. A corrective re-crawl was not replacing anything — FIXED

The first re-run after the fixes reported:

```
AML    crawled=11   new=0  unch=11
MISA   crawled=89   new=0  unch=89
SDAIA  crawled=29   new=0  unch=29
MOE    crawled=136  new=0  unch=136
```

Every document `unchanged`, so nothing was rewritten and the workbooks still held
the OLD rows with the old `doc_path` and `status` — the fixes were in the code
and absent from the output.

Phase 1 passes `workbook=`, which loads the existing file and compares against
it. Right for phase 2, wrong for a corrective crawl, which must REPLACE. Both
runners now move the previous workbook aside (kept, not deleted); `--append`
restores the old behaviour for change-detection testing.

Note the knock-on: a fresh run has real work to do. MOE took 55s when everything
classified `unchanged` and 2878s when all 136 documents were actually extracted.
The fast number was the crawl doing nothing.

## C12. Titles: a zero-width space, and a naming clash

`GENERIC_LINK_TEXT` is matched EXACTLY, and Ministry of Commerce stored a
document titled `click here\u200b` — visually "click here", not equal to it. Link
text is now normalised (invisible characters stripped) before the lookup, in the
generic engine as well as the formfill runner.

Separately, `run_source_standalone.py` named workbooks after `cfg["regulator"]`
while the harness uses the label, so `Ministry of Commerce.xlsx` sat beside
`MC.xlsx` — two workbooks for one regulator, one stale, in the directory whose
whole purpose is "these rows go to the database". Now both name by config stem.

## C13. doc_path repeated itself — FIXED

```
Ministry of Commerce | Regulations and Laws | Internet | Ministry of Commerce | Regulations & Laws
```

All 115 MC rows shared that trail. `_dedupe_keep_order` only removes ADJACENT
repeats, and SharePoint's site-collection crumb "Internet" sat between the two
copies of the regulator. `_clean_trail` now drops CMS crumbs and any crumb
already said earlier, comparing "Regulations & Laws" and "Regulations and Laws"
as the same folder. Result: `Ministry of Commerce | Regulations and Laws`.

## C14. Where each regulator ended up (2026-08-12)

| Regulator | rows | status | doc_path |
|---|---|---|---|
| CMA | 220 | empty | `Capital Market Authority (CMA) \| Laws & Regulations \| …` |
| MOE | 136 | empty | 0 duplicate URLs (was 20 phantoms) |
| MISA | 89 | empty | `MISA \| Laws and Regulations \| …` |
| MOH | 83 | empty | `Ministry of Health \| Rules and Regulations \| <title>` |
| MC | 72 | empty | `Ministry of Commerce \| Regulations and Laws` |
| MHRSD | 46 | empty | `MHRSD \| Regulations and procedural guidelines \| …`, document_url is the PDF |
| ZATCA | 31 | empty | `ZATCA \| Rules and Regulations` |
| SDAIA | 29 | empty | unchanged |
| AML | 11 | empty | unchanged |
| GOSI-SI / Saned | 7 / 2 | empty | `Press Here` gone |
| SAMA | running | — | 685 circulars done, rulebook walk in progress |

## C15. Still open — READ BEFORE APPROVING

- **MHRSD collects only page 1.** `formfill verify` warns
  `pagination.next_selector 'a[title="Go to next page"]' matched no usable
  control on the seed page (no-control) — only page 1 was walked`. 46 rows
  against the 63 documents `change_signals.yml` tracks. The `combined` change
  accounts for some of the drop (one row per instrument, not per attachment) but
  not all of it. **Under-collecting.**
- **MC and ZATCA pull Arabic documents into an /en/ crawl.** `lang_lock` applies
  to pages, not to collected documents. Deliberately NOT auto-dropped — some
  instruments are Arabic-only and silently discarding them would lose real
  content. A decision for a person.
- **MC has no hierarchy**: all 72 rows share one `doc_path`. The junk crumbs are
  gone but the nesting asked for (`laws and regulations / public consultations`)
  is not built.
- **MC titles**: `click here\u200b` now falls through to the URL slug, giving `2`
  for `/BoeLaws/Laws/Folders/2`. Honest, still not a title.
- **ZATCA page walk is shallow** (`n_pages` 1) — see C6.
- MOH/MC/ZATCA counts vary run to run (MC 115 then 72, ZATCA 35 then 31); worth
  a stability check before trusting either as a baseline.

## C16. GOSI doc_path — DONE

Now exactly as the library asked:

```
General Organization for Social Insurance (GOSI) | Laws and Regulations | Social Insurance Law | <title>
General Organization for Social Insurance (GOSI) | Laws and Regulations | Saned               | <title>
```

Dropped the site's own nav crumbs (`from_breadcrumb` gave "Home > Retirement
Laws") and the prefix that repeated the regulator.

**Consequence worth knowing:** the branch moved to the THIRD crumb, so BOTH GOSI
sources now store `source_system: "Laws and Regulations"`. `change_signals.yml`
names a source by the (regulator, source_system) PAIR, so its two GOSI entries
became identical and are merged into one. Identity is unaffected — that is
(document_url, doc_path), and doc_path still separates them. What a sweep loses
is probing one branch without the other; for two pages on one host that costs
nothing.

## C17. Excel was silently truncating documents — FIXED

The "html only reaches Article 20/26" report is not a crawler fault. It is
`excel_repo.py`:

```python
if isinstance(v, str) and len(v) > 32000:
    return v[:32000] + f"  ...[truncated, {len(v):,} chars]"
```

Excel's hard limit is 32,767 characters per cell. The crawler captured the whole
instrument — the suffix states the true size, 92,995 and 34,370 characters — and
32,000 of it reached the workbook. Every long document in every regulator was
affected; GOSI is only where it became visible.

**The serious part was promotion.** `promote.py` reads the workbook cell, so
approving into MSSQL would have written the PREVIEW and made the truncation
permanent.

Oversized values now go to a `<workbook>.fulltext.json` sidecar with a marker
left in the cell; the cell keeps its readable 32k preview. `promote.py`
rehydrates via `resolve_overflow()` and logs an ERROR if the sidecar is missing
or unreadable, rather than promoting truncated content quietly.

Verified end to end: a 99,211-character document stores as a 32,112-character
cell and comes back out of `promote._read()` at 99,211. Confirmed on the live
run — GOSI-SI restores to 92,995 and Saned to 34,370, the exact figures the
truncation suffix had been reporting.

**A stale server hid this once.** The first re-run still wrote 32,028 with an
empty sidecar because uvicorn had been started before the edit. The API caches
imported modules; it must be restarted after changing `excel_repo.py`,
`schema.py` or `runner.py`.

## C18. GOSI per-panel HTML — STILL BROKEN

Five of six Social Insurance instruments, and one of two Saned instruments, store
NO html. Only the first panel of each page has any.

What is established:

* Phase 1 finds all six panels, with text lengths
  `81937, 4278, 82123, 76509, 104951, 15891`. Panel #5 alone reports MORE text
  than the whole page (82,064), so these fragment ids resolve to overlapping,
  NESTED subtrees, not six disjoint instruments.
* Phase 2 re-loads the seed and re-stamps (`_prepare_in_page`), and only index 0
  is found again. The run warns explicitly, once per missing panel:
  `panel content not found again for 'Implementing Regulations' ([data-ff-panel="1"])`
* The single row that does carry html holds 92,995 characters: the WHOLE page,
  all six instruments, not one panel's worth.
* Raising `wait_ms` to 8000 did NOT fix it, so unlike the MOE tab bug this is not
  a settle-time problem.

The likely cause is the nesting: `_find_panels` skips an element that already
carries `data-ff-panel`, so if the ids resolve to enclosing containers, stamping
the first swallows the rest. Confirming that needs the live DOM, not the stored
artifacts.

Until it is fixed, GOSI's per-instrument TEXT is not usable for analysis: one row
has everything and the others have nothing. The INVENTORY — 8 rows, titles, urls,
doc_path — is correct.

## C19. document_urls REVERSED — files move to extra_meta, identity is declared

Decision reversed 2026-08-12, after the `document_urls` design had shipped.

**Before (removed):** `RegulatoryDocument.document_urls: List[str]`, with
`document_url` mirroring the FIRST file and `__post_init__` keeping the two in
step.

**Now:**

```
document_url                 ""                       <- deliberately EMPTY
extra_meta.attachment_links  "<pdf> | <pdf> | <pdf>"
extra_meta.identity_fields   ["doc_path", "extra_meta.attachment_links"]
```

A single-file source is untouched — `document_url` is that file.

**Why the reversal:** naming a row by whichever file the site happened to list
first makes the row's identity depend on the site's ordering.

### The part that needed building, not just deleting

`document_url` is half of the default identity. Leaving it empty COLLAPSES that
identity — every card in one folder gets `("", doc_path)`:

```
default identity, both SDAIA cards in one folder:
   A  document_url=|doc_path=SDAIA > Laws and Regulations > Data classification
   B  document_url=|doc_path=SDAIA > Laws and Regulations > Data classification   <- COLLIDES
```

So a multi-attachment row DECLARES its identity as `doc_path` +
`extra_meta.attachment_links` — the folder plus the set of files it carries. Three
pieces made that possible:

1. `changesignal.resolve_field()` — resolves a dotted `extra_meta.<key>` name,
   from a live document (dict) or a stored row (JSON string). One level deep, and
   only into extra_meta: identity must be comparable by BOTH repos, and extra_meta
   is the one column both already parse back into a dict.
2. `ExcelRepo.find_by_identity_fields` — resolves dotted names against the parsed
   extra_meta.
3. `MSSQLRepository.find_by_identity_fields` — accepts `extra_meta.<key>` and
   compares it in PYTHON after the SQL narrows, exactly as `doc_path` is already
   handled, so no new column is required. The row loop now also checks the meta
   fields; without that, a doc_path match would return a SIBLING card, since every
   multi-attachment row in a folder shares its doc_path.

`orchestrator.py` Tier 0 now reads `extra_meta["attachment_links"]` instead of the
removed field. That path is not optional for these rows: with `document_url`
empty, they would otherwise reach the analyzer with no text at all.

### Accepted costs (raised before building, accepted)

- A card that gains or loses a PDF changes identity, so it reads as one `new` plus
  one `disappeared` rather than `modified`. That is the price of not letting file
  order name the row.
- One requirement set still spans all files in a row, so an obligation cannot be
  attributed to the law versus its implementing regulation.

Regression tests: `tests/test_multi_attachment_identity.py` (7 tests). One of them
asserts that the DEFAULT identity still collides — if that ever stops being true,
the others have stopped proving anything.
