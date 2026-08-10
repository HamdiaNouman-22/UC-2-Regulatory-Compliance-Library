# How Updates Are Handled in the Orchestrator

What happens when a regulator **changes a document we already have**.
Companion to [new_orchestrator_explained.md](new_orchestrator_explained.md).

---

## 1. The short answer

There are two update stories, and they are not equally finished.

| | Old `Orchestrator` | `NewOrchestrator` |
|---|---|---|
| Who gets update detection | **CBB only** | every regulator |
| How a change is spotted | `content_hash` on the `regulations` row | `content_hash`, same idea |
| Where history goes | `regulation_versions` + `compliance_analysis_versions` | same tables |
| Runs in production | yes (CBB nightly) | **no** — Excel preview only |

For SAMA / SBP / SECP under the old orchestrator there is **no update handling at
all**: `filter_new_documents` puts a known document in `existing_docs` and the
pipeline never looks at it again. A changed SAMA circular is invisible.

---

## 2. Detecting the change

`classify_documents()` ([orch.py:112](../dynamic_crawler/formfill/orch.py#L112))

Two lookups, then one comparison:

```python
existing = self.repo.find_by_identity(url, path)          # (document_url, doc_path)
if existing is None and self.version_key:
    existing = self.repo.find_by_reference(ref)           # tiebreak on reference_no
```

The tiebreak matters for updates specifically: a regulator that **republishes at a
new URL** would otherwise register as *one new document plus one disappearance*,
and the old analysis would sit there forever as the live version. Same
`reference_no` = same document at a new address = an **update**, not a birth.

Then:

```python
if old_hash and new_hash and old_hash == new_hash:  → unchanged   (do nothing)
else                                                → modified    (full update path)
```

Note the `and` guards: if **either** hash is missing/empty, the document is
classified `modified` and reprocessed. That's fail-safe rather than fail-quiet —
a missing hash costs an unnecessary re-analysis, not a missed update.

`extra_meta["existing_regulation_id"]` is stamped on the doc here, because
`_process_versioned_doc` needs the row id and the crawler cannot know it.

---

## 3. Applying the update — six steps

`_process_versioned_doc()`, the `status == "modified"` branch
([orch.py:298-321](../dynamic_crawler/formfill/orch.py#L298-L321))

```
1. mark_all_versions_inactive(existing_id)     no two rows claim "active"
2. old = get_regulation_by_id(existing_id)     read what we currently hold
3. insert_regulation_version(old content,  status="inactive")   ← the archive
4. insert_regulation_version(new content,  status="active")     ← version_id
5. archive_current_analysis(existing_id, version_id)            ← the old analysis
6. update_regulation(existing_id, content_hash=new_hash)
   → then text decision → _run_llm_analysis(version_id=<new>)
```

The shape is right: **snapshot before you overwrite, archive the analysis before
you replace it, and tag the new analysis with the version it came from.** Step 1
exists so `get_active_regulation_version()` — which the extractor uses to find the
text — can never return two rows.

Same idea in the old CBB path (`_process_cbb_doc`,
[orchestrator.py:751](../orchestrator/orchestrator.py#L751)), except step 1 there
is hand-written `UPDATE regulation_versions` SQL through `_get_conn()`. The new
code calls `repo.mark_all_versions_inactive()` instead, which is why versioning
now works for any repo backend.

---

## 4. Six real problems with that path

I checked each of these against the repo implementations. They are current.

### 4.1 On MSSQL, old analysis is never removed — requirements double

This is the one I'd fix first.

`archive_current_analysis()`
([mssql_repo.py:760](../storage/mssql_repo.py#L760)) is a single
`INSERT INTO compliance_analysis_versions … SELECT … FROM compliance_analysis`.
It **copies**. There is no `DELETE FROM compliance_analysis` and no
`is_current = 0` update — not in that method, not anywhere in
[storage/](../storage/).

But the design says otherwise. The parent's own class docstring
([orchestrator.py:42-45](../orchestrator/orchestrator.py#L42-L45)) states:

> *"old `compliance_analysis` rows are moved to `compliance_analysis_versions`
> (status='inactive') **and deleted from `compliance_analysis`** BEFORE new
> analysis is written"*

The delete was never written. So on a modified CBB document:

- old rows stay in `compliance_analysis` with `is_current = 1`
- `store_analysis` inserts the new rows alongside them
- the regulation now shows **both** requirement sets as live
- the *next* update archives all of them again — the copy in
  `compliance_analysis_versions` grows quadratically

`ExcelRepo.archive_current_analysis`
([excel_repo.py:229](../dynamic_crawler/formfill/excel_repo.py#L229)) flips
`status = "inactive"` **in place**, which is correct. So the Excel preview run
looks clean and production doubles. That asymmetry is why this hasn't been caught.

**Fix:** add the deactivation to the MSSQL method, inside the same transaction as
the archive insert.

### 4.2 The archived analysis is tagged with the wrong version

Step 3 creates the old-content version but throws the id away:

```python
self.repo.insert_regulation_version(...)          # ← return value discarded
version_id = self.repo.insert_regulation_version(...)   # the NEW active version
self.repo.archive_current_analysis(existing_id, version_id)   # ← new id!
```

So analysis that describes the *old* content gets stamped with the version that
**replaced** it. The parent gets this right — it keeps `old_version_id` and passes
that ([orchestrator.py:803](../orchestrator/orchestrator.py#L803)). Anyone later
joining `compliance_analysis_versions` to `regulation_versions` to answer "what did
this regulation require in March?" gets the wrong snapshot.

### 4.3 The `regulations` row keeps the old content

Step 6 updates **only** `content_hash`:

```python
self.repo.update_regulation(existing_id, content_hash=new_hash)
```

The parent's CBB path also refreshed the content itself:

```python
self.repo.update_regulation(existing_id,
                            document_html=document_html,
                            published_date=doc.published_date)
```

So after a `NewOrchestrator` update the `regulations` row has the **new hash next
to the old HTML** — internally inconsistent, and the hash no longer describes the
row it lives on. `title`, `published_date` and `extra_meta` aren't refreshed
either, so a retitled or re-dated document keeps its old metadata.

### 4.4 Requirement mappings accumulate

`store_requirement_mappings` ([mssql_repo.py:1025](../storage/mssql_repo.py#L1025))
is a plain `INSERT` loop with no cleanup of prior rows. Re-analysing a regulation
appends a second full set of mappings to `sama_requirement_mapping`. For CBB
`version_id` separates them; for everyone else `version_id` is `NULL`, so old and
new mappings are indistinguishable.

Worse, the `new`-status branch re-inserts suggested requirements with a
**deterministic** ref key ([orchestrator.py:313](../orchestrator/orchestrator.py#L313)):

```python
"ref_key": f"AUTO-{regulation_id}-{i}"
```

Same regulation re-analysed → same `AUTO-…` keys inserted again. Nothing checks
for the existing row, so duplicate suggested requirements pile up in
`COMPLIANCE_REQUIREMENT` on every update.

### 4.5 For page attachments, `content_hash` can't detect a content change

[pipeline.py:190](../dynamic_crawler/formfill/pipeline.py#L190):

```python
d.content_hash = content_key(f"{f['href']}|{label}")
```

`content_key` is just normalise-and-MD5 of whatever string you hand it
([runner.py:76](../dynamic_crawler/formfill/runner.py#L76)) — so for an attachment
this hashes **the URL and the link text**, not the document.

If a regulator silently swaps the PDF at `…/circular-42.pdf` for a revised one and
leaves the link text alone, the hash is byte-identical and the document classifies
`unchanged` — forever. Since `(document_url, doc_path)` is *also* the identity key,
the hash adds nothing beyond identity for these records. Silent-replacement is a
normal regulator behaviour, so this is a genuine blind spot, not a corner case.

**Fix direction:** hash the fetched bytes (or an `ETag`/`Last-Modified`/
`Content-Length` probe) rather than the link.

### 4.6 It cannot run against MSSQL yet

`find_by_identity` is called **unguarded** at
[orch.py:124](../dynamic_crawler/formfill/orch.py#L124) and exists only on
`ExcelRepo`. Until it and `find_by_reference` land in `mssql_repo`, none of the
above executes in production — the update path is Excel-only.

---

## 5. What is *not* an update

Two deliberate non-actions, both correct:

- **`unchanged`** — same hash means no fetch, no OCR, no LLM, no write. This is the
  cheap path for the overwhelming majority and the reason a nightly run is minutes.
- **`disappeared`** — a document we hold that this run didn't see is **not** treated
  as withdrawn unless [the completeness gate](new_orchestrator_explained.md#4-the-completeness-gate--the-best-idea-in-the-file)
  passes. SDAIA returned 415 / 363 / 439 on three identical runs; a run that loses
  52 documents is not a run where 52 were withdrawn. Failing the gate marks the run
  `QUARANTINED` — new and modified still get ingested, and the report states plainly
  that nothing was retired.

---

## 6. Suggested order of fixes

1. **Delete/deactivate old rows in `MSSQLRepository.archive_current_analysis`** (§4.1) —
   silent data corruption, affects CBB in production today.
2. **Port `find_by_identity` + `find_by_reference` to `mssql_repo`** (§4.6) — unblocks
   everything else.
3. **Keep the old version id and pass it to `archive_current_analysis`** (§4.2) — a
   two-line change.
4. **Refresh content/metadata in step 6, not just the hash** (§4.3).
5. **Make mapping storage idempotent** — clear prior rows for the regulation, or
   upsert on `ref_key` (§4.4).
6. **Hash attachment bytes instead of the link** (§4.5) — the largest change, and the
   difference between detecting real revisions and only detecting relinks.
