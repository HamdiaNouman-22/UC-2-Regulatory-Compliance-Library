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
