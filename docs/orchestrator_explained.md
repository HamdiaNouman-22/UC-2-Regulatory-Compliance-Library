# How the Orchestrator Works (Right Now)

Plain-language walkthrough of [orchestrator/orchestrator.py](../orchestrator/orchestrator.py)
and the newer [dynamic_crawler/formfill/orch.py](../dynamic_crawler/formfill/orch.py).

---

## 1. What it is, in one sentence

The Orchestrator is the **middle manager** of the pipeline. It doesn't crawl
websites, doesn't talk to the LLM directly, and doesn't write SQL. It just
decides, for each document: *is this new? do I have readable text? should I
analyse it? where do the results go?* — then calls the specialist for each step.

```
Crawler  →  Orchestrator  →  Repo (MSSQL)
              │
              ├─ Downloader / OCR      (get the text)
              ├─ StagedLLMAnalyzer     (4-stage analysis)
              └─ RequirementMatcher    (link to internal controls/KPIs)
```

It is constructed with those helpers injected
([orchestrator.py:54-62](../orchestrator/orchestrator.py#L54-L62)), so the same
orchestrator works for any regulator — you just hand it a different crawler.

---

## 2. Who starts it

Nobody runs the orchestrator by itself. It gets built and kicked off by:

| Caller | Entry point used |
|---|---|
| [jobs/sama_job.py](../jobs/sama_job.py), [sbp_job.py](../jobs/sbp_job.py), [secp_job.py](../jobs/secp_job.py) | `run_for_regulator("SAMA")` etc. |
| [jobs/run_regulator.py](../jobs/run_regulator.py) | `run_for_regulator(<name>)` |
| [scheduler/scheduler.py](../scheduler/scheduler.py) | `run_for_regulator("SBP")`, `run_for_cbb(mode="monitoring", ...)` |
| [dynamic_crawler/formfill/api.py](../dynamic_crawler/formfill/api.py) | `NewOrchestrator.run_for_regulator(...)` |

So there are **two doors** today: a normal one and a CBB-only one.

---

## 3. The main flow (SAMA / SBP / SECP)

`run_for_regulator()` — [orchestrator.py:485](../orchestrator/orchestrator.py#L485)

**Step 1 — Ask the crawler for everything**
`self.crawler.fetch_documents()`. The orchestrator doesn't care how the crawler
got them.

**Step 2 — Split into new vs. already-seen**
`filter_new_documents()` ([line 583](../orchestrator/orchestrator.py#L583)).
This is where things get messy today — there are **five different ways** a
document can be identified, tried in order:

1. CBB → match on `source_page_url`
2. has a `published_date` → match on (title, date, doc_path)
3. category is "regulatory returns" → match on (title, doc_path)
4. `source_system == "DPC-CIRCULAR"` → match on (title, doc_path)
5. fallback → match on (document_url, category)

If none of those apply, the document is **dropped with a warning**. Existing
documents are simply ignored (no change detection) for everyone except CBB.

**Step 3 — Process the new ones**
`_process_docs()` ([line 501](../orchestrator/orchestrator.py#L501)) runs
documents in a thread pool, `DOC_MAX_WORKERS` (default **4**) at a time. Set it
to `1` for the old serial behaviour. LLM concurrency is separately capped inside
`StagedLLMAnalyzer` by `LLM_MAX_CONCURRENCY`, so raising the worker count can't
flood OpenRouter. One document blowing up is logged and skipped — it never kills
the batch.

**Step 4 — Per document** (`_process_single_doc`, [line 669](../orchestrator/orchestrator.py#L669))

1. Build the folder tree from `doc_path` → `compliancecategory_id`.
2. If category is "regulatory returns" → **insert only, no LLM**. Stop.
3. If regulator is CBB → hand off to the versioned path (section 4).
4. Otherwise: insert the regulation row, then extract + analyse.

---

## 4. The CBB path (versioning)

CBB is the only regulator that keeps history.
`run_for_cbb()` → `_process_cbb_doc()` ([line 722](../orchestrator/orchestrator.py#L722)).

`run_for_cbb(mode="auto")` looks at the last crawl date and picks a crawler:
`CBBCrawlerV2` for a **full** crawl, `CBBMonitoringCrawler` for **monitoring**.
Monitoring also picks up **modified** documents, not just new ones.

**New CBB document:**
1. Insert regulation row
2. Save content hash
3. Create `regulation_versions` snapshot (status = active)
4. Analyse → rows in `compliance_analysis` tagged with that `version_id`

**Modified CBB document:**
1. Mark all existing versions `inactive` (raw SQL, [line 771](../orchestrator/orchestrator.py#L771))
2. Snapshot the **old** content as an inactive version
3. Move old analysis rows → `compliance_analysis_versions`, delete them from `compliance_analysis`
4. Snapshot the **new** content as the active version
5. Update the regulations row
6. Analyse fresh → new rows pointing at the new `version_id`

Two escape hatches: pages with `depth < 2` are treated as folder/index pages and
skipped, and `--skip-analysis` skips the LLM entirely.

For SAMA / SBP / SECP, `version_id` is simply `None` — same tables, no history.

---

## 5. Getting the text (3-tier extraction)

`extract_text_content_unified()` ([line 92](../orchestrator/orchestrator.py#L92)).
**First tier that yields 200+ characters wins** — everything below it is never
tried.

| Tier | Source |
|---|---|
| CBB-only | active row in `regulation_versions` |
| 1a | `extra_meta["org_pdf_text"]` (SAMA pre-OCR'd) |
| 1b | `extra_meta["content_text"]` (pre-extracted HTML) |
| 2 | `doc.document_html` |
| 3 | download + OCR: `org_pdf_link` → `document_url`.pdf → `arabic_pdf_link` → `urdu_url` → fetch the HTML page |

Tier 3 downloads to a temp file, runs `OCRProcessor.extract_text_from_pdf_smart`,
and always deletes the temp file. Everything under 200 chars is a `validation`
error in the log and the document is not analysed.

---

## 6. Analysis and matching

`_run_llm_analysis()` ([line 388](../orchestrator/orchestrator.py#L388)) —
one method for every regulator:

1. Normalise the text (HTML vs. PDF-text aware).
2. `StagedLLMAnalyzer.analyze()` → the 4-stage pipeline.
3. `repo.store_analysis(rows, version_id=...)` → `compliance_analysis`.
4. Pull `normalized_obligations` out of `stage2_json` and pass them to matching.

`_run_requirement_matching()` ([line 249](../orchestrator/orchestrator.py#L249))
then compares each obligation against existing requirements, controls and KPIs:

- **fully matched** → store the mapping
- **partially matched** → store it, and flag the existing requirement for review
- **new** → insert a suggested requirement (`AUTO-<reg_id>-<n>`), plus any
  suggested controls/KPIs, and link them

The whole method is wrapped in try/except — a matching failure is logged but
never fails the analysis.

---

## 7. Logging

Every step writes a row via `self.log(...)` → `repo._log_processing`, with
`step` / `status` / `message` (e.g. `llm_analysis / SUCCESS / 12 rows stored`).
That's the audit trail for a run. File logs go to `orchestrator.log` at DEBUG.

---

## 8. `NewOrchestrator` — the newer version

[dynamic_crawler/formfill/orch.py](../dynamic_crawler/formfill/orch.py) is a
**subclass** that overrides the messy parts without editing the parent (which
has uncommitted changes from other sessions). It is used by the formfill /
dynamic-crawler path only, not by the nightly jobs.

What it changes:

1. **One door.** `run_for_regulator` handles CBB too — no `run_for_cbb`, no
   `if regulator == "CBB"` branch.
2. **`classify_documents()` replaces `filter_new_documents()`.** Four buckets —
   new / modified / unchanged / **disappeared** — decided by *one* configurable
   identity key `(document_url, doc_path)`, with a fallback tiebreak on
   `reference_no` so a document republished at a new URL isn't counted as one new
   plus one disappearance. "Modified" is decided on `content_hash`; unchanged
   documents cost nothing, which is why a nightly run takes minutes.
3. **Versioning for everyone**, not just CBB.
4. **A completeness gate** (`check_run_trustworthy`). A run may only act on
   "disappeared" if it looks trustworthy: no bot-protection pages, no early
   stop, no cap hit, and the document count within **5%** of the last good run.
   SDAIA returned 415 / 363 / 439 on three identical runs — a run that loses 52
   documents isn't a run where 52 were withdrawn. Untrustworthy runs are marked
   `QUARANTINED` but still ingest new and modified documents.
5. **A real text decision** instead of first-tier-wins: `textinput.py` gates on
   "is there anything to analyse at all?", then chooses HTML vs. file — and when
   they differ, **sends both**.
6. **No string branches.** "regulatory returns" isn't special-cased by name; a
   document is analysed when it has text and skipped when it doesn't.
7. **Correct folder types.** Intermediate nodes are `"F"` (folder), the leaf is
   `"R"` (regulation). The parent left everything as `"F"`, so every document
   rendered in the frontend as an empty folder.

It also returns a **report dict** (crawled count, bucket sizes, inventory hash,
gate verdict, table counts) instead of just logging.

---

## 9. The short version

- Old orchestrator: **crawl → is it new? → get text → analyse → match → store.**
  Special cases for CBB and "regulatory returns" are hardcoded, existing
  documents are ignored, and the first text source that clears 200 chars wins.
- `NewOrchestrator`: same spine, but **one path for all regulators**, change
  detection by hash, versioning for everyone, a trust gate before declaring
  anything withdrawn, and a smarter text choice.

## 10. Known rough edges

- **Two entry points** (`run_for_regulator` / `run_for_cbb`) that behave
  differently.
- **Five identity rules** in `filter_new_documents`, and documents matching none
  are silently dropped.
- **No change detection** for SAMA / SBP / SECP — modified documents are just
  "existing" and skipped.
- **Raw SQL** inside `_process_cbb_doc` ([line 771](../orchestrator/orchestrator.py#L771))
  instead of a repo method.
- **Dead code**: `self.llm_analyzer` is overwritten with a fresh `LLMAnalyzer()`
  in `__init__`, so the injected one and `ocr_engine` are never used.
- `NewOrchestrator` fixes most of this, but only the formfill path uses it — the
  nightly jobs still run the parent.
