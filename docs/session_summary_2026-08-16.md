# Session summary — 2026-08-16

Handoff document. Written to be read cold. Continues
`docs/session_summary_2026-08-14.md`, whose §9 and §10 cover the MSSQL load and
the monitoring work this builds on.

---

## 0. State of the library right now

```
regulations             8,714      12 regulators
regulation_versions     9,398
compliancecategory      8,354
compliance_analysis         3      (one document analysed, as a test)
COMPLIANCE_REQUIREMENT     14      (created by that test's matching run)

database: uc2-db on localhost,1433  (NOT plain "localhost" — see §5)
rows awaiting a human decision: 8,714  (WHERE status = '')
```

---

## 1. The decisions taken today

These are the lead's, and everything else follows from them.

**Scheduled monitoring writes STRAIGHT TO MSSQL.** No workbook, no approval step
in the middle. `status` is still left EMPTY on every row — the review moved from
*before* the write to *after* it, and "what arrived overnight and nobody has
judged" is `SELECT * FROM regulations WHERE status = ''`.

**ExcelRepo stays in the repo but off the orchestrator path.** It is a working
second implementation of the same contract and `promote` still replays a
workbook when someone deliberately produces one — but no scheduled job may write
through it.

**`title` is part of the identity**, in both sets:

    default          (document_url, doc_path, title)
    multi-attachment (doc_path, extra_meta.attachment_links, title)

The measurement said it adds nothing (identity was already unique, and 96% of
rows already carry the title as doc_path's last crumb) and costs a failure mode
(an edited title = one false `new` + one false `disappeared`). Taken anyway, as a
deliberate choice. **If titles are edited in bulk again, expect phantom pairs and
check the withdrawal gate before acting on them.**

**Blocked hosts get no scheduled job at all** — not merely skipped. See §4.

---

## 2. The orchestrator merge

There were never two orchestrators. `NewOrchestrator` subclasses `Orchestrator`,
overrides 5 methods and makes ONE `super()` call. The inheritance IS the merge.

What was actually wrong: callers could still reach the BASE, and four did —
`jobs/run_regulator.py`, `jobs/sama_job.py`, `jobs/sbp_job.py` and
`crawler/cbb_monitoring_crawler.py`. Those runs had **no change classification,
no version rows and no folder tree**, and looked like they were working.

So `orchestrator/orchestrator.py` now defines `BaseOrchestrator`, and the NAME
`Orchestrator` resolves to the merged class through a lazy module `__getattr__`
(lazy because `orch.py` imports that module — a top-level import would be a
cycle). Every consumer verified to hold the merged class, including
`apis/pipeline_api.py`.

> **Behaviour change to expect:** SBP, SECP, SAMA and CBB now classify, version
> and build folder trees on their next scheduled run, where they did not before.

`BaseOrchestrator` is still importable by name — asking for it is now a
deliberate statement that you want the pre-merge behaviour.

---

## 3. Identity — one implementation, three fallbacks

`changesignal.find_existing` is now the ONLY answer to "is this the same
document?". `promote` and the orchestrator both call it; they used to have
separate copies, and every promote bug found on 2026-08-15 was a rule the
orchestrator had and promote did not.

The lookup order:

1. exact match on the configured identity
2. **same folder + shared FILE** — `document_url` and `attachment_links` read as
   ONE thing, compared by overlap. Handles a document that gains or loses an
   attachment, which otherwise crosses between the two spellings and never
   matches itself.
3. **same folder + same TITLE** — a document that changed url.

Fallback 3 was added from a real case: MHRSD serves one instrument at TWO urls
(an English filename and an Arabic slug) and **both answer 200**. An earlier
crawl stored the first, a later listing linked the second, and it came out as one
false `new` + one false `disappeared`. `version_key: reference_no` exists for
exactly this and could not help — neither row has a reference number.

Fallbacks can only find matches that would otherwise be lost, so they cannot
orphan anything. That is what makes them safe to apply after the exact lookups.

---

## 4. Scheduling — prepared, nothing enabled

`jobs/monitor_jobs.py` + `config/scheduler.yml`, registered in
`scheduler/scheduler.py`'s `DIRECT_JOB_MAPPING`. **All four jobs are
`enabled: false`** — turning them on is a server decision.

```
monitor_cheap_probes   daily 03:00     MOE, MOH, SDAIA, AML, MHRSD, ZATCA  ~75s
monitor_sama           daily 03:15     the revision feed                   ~3-5s
monitor_mc             Sunday 04:00    crawl-as-signal                     ~16m
monitor_cma            Sunday 05:00    crawl-as-signal, recent window      long
```

Grouped by **what each site will answer**, not by regulator. Measured timings for
the daily sweep: SAMA 4.5s, MHRSD 5.1s, SDAIA 7.2s, MOH 10.8s, MOE 11.5s,
ZATCA 24.0s.

**Saudi Exchange and SIMAH have NO JOB, deliberately.** Both blocks were caused
by automated access from this one address — saudiexchange.sa went from crawling
cleanly at 18:27 to Akamai 403 within two hours on 2026-08-15. A scheduled retry
is not a way out of a block; it is what made it. `skip_hosts` stops a sweep
touching the hosts; giving them no job means nothing can schedule its way past
that either. The `until` dates are REVIEW dates for a person (2026-08-22 and
2026-09-04), not expiries.

---

## 5. Bugs found by testing the direct-write path

Every row in the library had arrived through `promote` reading a workbook. The
orchestrator writing straight to MSSQL — what a 3am job does — **had never run
once**. It was broken in three ways:

1. **Every version insert failed.** `MSSQLRepository.insert_regulation_version()`
   required `regulator`; `ExcelRepo`'s had a default and a `**kw` that swallowed
   extras. So the orchestrator's versioning worked perfectly against a workbook
   and failed on every document against the database. Signatures now aligned in
   both directions, `**kw` on each.
   > This also corrects a claim in the earlier summary: the two repos share
   > method NAMES, not signatures. 37 names match; the contract did not.
2. **Folders duplicated every run.** The leaf rule ("never hand one document's
   node to another") could not tell someone else's leaf from **the leaf of the
   document being re-processed**. 11 duplicates per AML run, for ever. It now
   checks whether the occupant IS this document.
3. **A duplicate regulation** created while those two were failing.

**Not a bug, though it looked like one:** the first direct run of any source
reclassifies its whole set as `modified` and writes one extra version per
document. That is a ONE-TIME reconciliation between the promote-written content
hash and the direct path's, and it settles — MISA went `89 modified` then
`unchanged 89, versions +0`. Do not read the first run's numbers as churn.

---

## 6. Coverage — the biggest finding

**ZATCA was 34 documents of 151.** Its Rules & Regulations landing page is three
cards and only the first was ever crawled:

```
Zakat, Tax and Customs Regulations     34   was in the library
Tax and Customs Agreements             98   NEW — added today
Information Exchange Portal            19   NEW — added today (three sub-forms)
```

78% of what ZATCA publishes was not in the library, and **nothing we measure
would have shown it**: every check we have tests DEPTH (are the documents we hold
current?) and none tests BREADTH (is there a section nobody pointed a crawler
at?). New forms: `zatca.agreements`, `zatca.ie_agreements`, `zatca.ie_guidelines`,
`zatca.ie_circulars`.

**The same question is unasked for the other eleven regulators.** MISA already
hints at it — its 89 documents span six hosts.

---

## 7. URL stability — the same disease three times

A url is part of the IDENTITY of a multi-attachment row, so a parameter that
changes per request makes the row look new on every crawl.

```
Ministry of Commerce   dt=<DDMMYYYYHHMMSS>   duplicated all 16 attachment rows
CMA                    csrt=<token>          plus a literal undefined=undefined
```

`runner.stable_url()` is now the ONE rule, used by both. It strips
`dt, csrt, _, ts, nonce, sid, session, token, undefined` and **never** touches
`attId` or `lawId`, which name the document. Verified on MC that the endpoint
returns the identical 261,632-byte PDF with and without `dt`.

Applied at CMA's single exit (`_scrub_urls`) so no code path can bypass it.

---

## 8. SAMA reads its own revision page

`--signal sama-feed` (`dynamic_crawler/sama_feed_signal.py`), beside the other
four signals. **One request instead of 6,101 probes: 2.7–5.4 seconds.**

```
22 entries for 2026-01-01..2026-08-15   SAMA changes ~22 times in 7 months
18 already tracked | 4 NOT in the library | 0 unresolved node urls
```

Those 4 were fetched and inserted (`benchmarks/sama_feed_ingest.py`) — the first
time monitoring maintained the library end to end.

Two traps recorded: `items_per_page` caps at **40** and a larger value returns an
EMPTY page rather than an error; and the feed **cannot see deletions**, so
`stored-inventory` stays SAMA's occasional way of finding removals.

CMA was checked for an equivalent and has none — most candidate paths return an
identical 1,221-byte soft-404, `/sitemap.xml` has zero overlap with its documents
and `/en/updates` is a newsletter signup form.

---

## 9. WHAT IS LEFT

### Blocking — would misbehave if scheduled today

1. **`compliance_analysis.version_id` is NULL.** The analysis is not anchored to
   the document version it read, so `archive_current_analysis(regulation_id,
   version_id)` has nothing to stamp. Needs tracing where the analysis row is
   written.
2. **Job overlap guard.** MHRSD's `Page crashed` was memory contention from
   concurrent crawls. The schedule staggers jobs, but CMA's long run can bleed
   into the next day's probes. Needs a lock (or APScheduler `max_instances`) and
   a decision: skip or queue when a job is still running at its next trigger.
3. **Full-path testing is incomplete.** AML, MISA, MHRSD and MOE have run through
   direct-write. **ZATCA ×4 and MOH have not.** Each first run also does the
   one-time reconciliation of §5, so do it deliberately.

### Should do

4. **Stray empty `uc2-db` on the DEFAULT instance.** Two databases with the same
   name on one machine — bare `localhost` reaches the empty one, `localhost,1433`
   the real one. A `DROP DATABASE` nobody has authorised yet.
5. **Retest the blocked hosts by hand** — Saudi Exchange after 2026-08-22, SIMAH
   after 2026-09-04. Never by a scheduled job.
6. **MHRSD id=321** carries `extra_meta.superseded_by = 8752` and `status = ''`.
   The same instrument at a url the site no longer lists. A person decides.

### Judgement calls, not engineering

7. **Which documents are worth analysing.** Regulation 3913 produced 14
   obligations of which **8 were print colours** ("500 Riyals must be RGB
   230,195,157"). The analyzer turns every imperative into an obligation. Across
   8,714 documents that is real LLM spend and a requirements library filling with
   noise. Run `id=3` (Anti-Money Laundering Law — now analysable, see below) and
   compare: if its obligations are mostly genuine, the fix is SELECTION, not the
   analyzer.
8. **Breadth check on the other eleven regulators** — see §6. Likely the largest
   remaining gap in the library.

### Known and recorded, no action needed

- **MISA reports ~21 `unknown` for ever.** Its documents span six hosts;
  laws.boe.gov.sa times out at TCP level (a browser cannot help) and mc.gov.sa
  refuses plain HTTP clients. 16 + 5 = exactly the unknown count.
- **Ministry of Commerce and CMA cannot be probed at all.** Their monitoring is
  the crawl. Both carry `signal: crawl` in `config/change_signals.yml`.
- **CMA needs 2h49m for a full crawl** and its announcements tab MUST run as a
  recent window — a full walk returned 300 of the 1,053 announcements we hold and
  would rule the other 753 `disappeared`. `CMA_SINCE_DAYS` now drives that.

---

## 10. Analysis — what runs today

`POST /trigger/staged-analysis/{id}` works, and **PDF-only regulations now work
too**: a fifth fallback downloads `document_url` when nothing in the library has
text, reusing the orchestrator's `_download_and_extract_pdf` (with OCR). Verified
on the AML Law (id=3): 35,871 characters extracted.

That unblocked ~300 documents — AML 11, MOH 83, ZATCA Agreements 98, MISA's 65
PDFs, SDAIA 29, Tadawul 19 — which previously returned
`No extractable text`.

Start Swagger with:

```
venv\Scripts\python.exe -m uvicorn apis.pipeline_api:app --reload --port 8000
# http://127.0.0.1:8000/docs
```

`/trigger/full-analysis/{id}` = staged analysis THEN requirement matching.
Matching failure is non-fatal and returns `matching.error` while the outer
`success` stays true — read `matching.success`, not the outer flag.

> `?force=true` on staged-analysis HARD-DELETES the current analysis rather than
> archiving it (merge review B3, still unfixed). Harmless on an empty table; do
> not use it once there are analyses worth keeping.

---

## 11. Two mistakes worth not repeating

**A machine wrote the human column.** While resolving the MHRSD duplicate I set
`status='reject'` automatically. `status` is the human approve/reject decision and
nothing automated may write it — the pipeline would be approving its own output.
Reverted to `''` within the minute, with the finding recorded in
`extra_meta.superseded_by` instead. The machine states the evidence; a person
decides.

**One run is never a result.** MISA's first direct run was reported here as
nightly version churn on the strength of a single observation. The second run
showed `unchanged 89, versions +0`. The same discipline caught CMA's 1,134 and
ZATCA's 12 false positives — always run a signal twice before believing it.
