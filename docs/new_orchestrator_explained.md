# `NewOrchestrator` — How It Works (Plain Words)

About [dynamic_crawler/formfill/orch.py](../dynamic_crawler/formfill/orch.py).
For the old one, see [orchestrator_explained.md](orchestrator_explained.md).

---

## 1. What it is

`NewOrchestrator` is a **subclass** of the old `Orchestrator`. It doesn't replace
the file — it inherits everything and overrides only the parts that were wrong.
That was deliberate: `orchestrator.py` and `mssql_repo.py` had uncommitted
changes from other sessions, so nothing here edits them. The upside is that
**the list of overrides *is* the list of changes** — you can read the diff by
reading the class.

```python
class NewOrchestrator(Orchestrator):
```

It's wired up in [formfill/api.py](../dynamic_crawler/formfill/api.py#L177) with
a `FormfillCrawler` and an `ExcelRepo`, and returns a **report dict** instead of
just writing log lines.

```python
NewOrchestrator(crawler=crawler, repo=repo, downloader=None,
                source_name=form, analyse=analyse, limit=limit)
report = orch.run_for_regulator("SBP")
```

---

## 2. The seven changes, in plain words

| # | Old behaviour | New behaviour |
|---|---|---|
| 1 | Two doors: `run_for_regulator` + `run_for_cbb`, plus an `if regulator == "CBB"` fork | **One door.** `run_for_regulator` handles everything |
| 2 | `filter_new_documents` → 2 buckets, 5 hardcoded identity rules | `classify_documents` → **4 buckets**, one identity rule |
| 3 | Versioning for CBB only | **Versioning for everyone** |
| 4 | A missing document = a withdrawn document | **A trust gate** decides whether to believe the run |
| 5 | First text source over 200 chars wins | **A gate, then HTML-vs-file** — and send both when they differ |
| 6 | `if category == "regulatory returns"` — skip LLM by name | **No string branches** — analysed if it has text |
| 7 | Every folder tree node typed `"F"` | Folders `"F"`, the document's own node `"R"` |

---

## 3. Change detection — the part that matters most

`classify_documents()` ([orch.py:112](../dynamic_crawler/formfill/orch.py#L112))

The old code asked one question — *have I seen this?* — and answered it five
different ways depending on the regulator. The new code asks two questions and
answers each one way:

**Question 1: do I already have this document?**
Identity is `(document_url, doc_path)` → `repo.find_by_identity(url, path)`.

If that misses, one tiebreak: look up `reference_no` via `find_by_reference()`.
This exists because a regulator that **republishes a document at a new URL**
would otherwise look like *one new document plus one disappearance*. Same
reference number = same document, new address.

**Question 2: has it changed?**
Compare `content_hash`. Same hash → `unchanged`, and we do **nothing at all** —
no fetch, no OCR, no LLM. That cheap path is the whole reason a nightly run takes
minutes instead of hours. Different hash → `modified`.

**Four buckets out:**

```
new          → insert + version + (maybe) analyse
modified     → archive old, snapshot new, re-analyse
unchanged    → nothing (the cheap common case)
disappeared  → in the DB for this source, but this run didn't see it
```

It also sets the two things a crawler **cannot** know because they need a
database lookup:

```python
extra_meta["monitoring_status"]     = "new" | "modified" | "unchanged"
extra_meta["existing_regulation_id"]
```

Doing it here (not in the crawler) keeps crawlers DB-free — which they *must* be,
because formfill runs as a subprocess.

### The `status` column, sorted out

`_set_status()` ([orch.py:85](../dynamic_crawler/formfill/orch.py#L85)) fixes a
three-way fight over one column. Three different things wanted `regulations.status`:

- **our lifecycle** — active / inactive, used by the archive logic
- **the regulator's own claim** — "In-Force" / "Superseded", straight off SAMA's table
- **the monitoring state** — new / modified / unchanged

The regulator's opinion about its own document is not our record's state, so it
moves to `extra_meta["regulator_status"]` and `status` becomes ours alone.

---

## 4. The completeness gate — the best idea in the file

`check_run_trustworthy()` ([orch.py:210](../dynamic_crawler/formfill/orch.py#L210))

**The problem it solves:** SDAIA returned **415, then 363, then 439** documents on
three runs of *identical code*. A run that "loses" 52 documents is not a run where
52 documents were withdrawn. Without this gate, a flaky night silently marks
dozens of live regulations as gone.

So a run may only act on `disappeared` if it earns the right. Four checks:

1. **No bot-protection pages** — `run["blocked_pages"] == 0`
2. **No early stop** — no warning containing `"stopped at page"`
3. **Not capped** — no warning containing `"capped"`
4. **Count within tolerance** — within **5%** (`COUNT_TOLERANCE_PCT`) of the last
   good run's `row_count`

Fail any one → verdict `QUARANTINED`. Crucially, a quarantined run **still ingests
new and modified documents** — it just isn't allowed to declare anything withdrawn.
The report says so explicitly rather than staying quiet:

> *"N document(s) were not seen this run, but the run is not trustworthy so
> nothing was marked withdrawn"*

Alongside it, `_inventory_hash()` fingerprints the full document list (sorted
identities, MD5, first 12 chars). Identical hash to the last good run = the site
genuinely didn't change.

Every run writes its outcome via `repo.record_run(source, count, hash, verdict, note)`
into `run_history`, which is what makes the *next* run's tolerance check possible.

---

## 5. The text decision

`extract_text_content_unified()` is overridden to delegate to
[formfill/textinput.py](../dynamic_crawler/formfill/textinput.py) — a pure module
with no I/O (the caller injects the two fetchers, so it's testable without a
network or a DB).

**Step 1 — the gate.** Is there anything to analyse at all?

```
document_html has real text   OR
document_url is a file (.pdf/.docx/…)   OR
document_url is a page we can fetch
```

None of those → **skip**: store the document, log why, analyse nothing. A
regulator that publishes only an external link (MISA's 24 `laws.boe.gov.sa`
entries) is stored and left alone rather than silently half-processed.

**Step 2 — HTML vs. file.** When both a page rendering and a file exist:

- **They say the same thing → send the HTML only.** It's already text, so it
  needs no OCR trust.
- **They differ → SEND BOTH.** Deliberately. A PDF is often the authoritative
  text while the page is a summary — and the reverse happens too: SAMA pages run
  **379 characters** against a full PDF. Dropping either risks dropping a
  requirement, and a few hundred wasted tokens is the cheaper mistake.

When both are sent, the model gets explicit headers so it knows it's reading one
regulation twice, not two regulations:

```
=== SOURCE 1 OF 2 — text of the published web page ===
=== SOURCE 2 OF 2 — text of the attached document (x.pdf) ===
```

**"Say the same thing" is not a hash.** OCR never matches HTML byte for byte. It
compares **5-word shingles** and measures **containment** — how much of the
*shorter* text appears in the longer one — with a threshold of **0.8**.

- *Shingles, not word sets*: word overlap alone would call any two documents on
  the same subject identical; ordered 5-word groups won't.
- *Containment, not Jaccard*: a 5-page PDF that fully contains a 1-paragraph page
  summary should score **high**. Jaccard scores it low just because the PDF is
  bigger, so we'd wastefully send both.
- *0.8, not 0.95*: tolerates OCR noise, headers and footers. At 0.95 almost every
  PDF looks different and you send both every time.

Every outcome comes back as a `Decision` with a human-readable `reason` that gets
logged — so a skipped document can always be explained without re-running anything:

```
ANALYSE html+file (48,201 chars) — page and file differ (overlap 0.31) — sending both
SKIP — no html text, no file, no page to fetch
```

---

## 6. The folder tree fix

`_get_or_create_compliance_category()` ([orch.py:165](../dynamic_crawler/formfill/orch.py#L165))

Same tree walk as the parent, but it **types the nodes**. `compliancecategory.type`
is what the frontend uses to tell a folder from a regulation. The parent called
`insert_folder(title, parent_id)` and never passed the third argument, so every
node took the default `"F"` — including the leaf, which since the doc_path change
*is* the document. Result: **every document rendered in the frontend as an empty
folder.**

```python
cat_type = "R" if i == last_index else "F"
```

The leaf rule is kept from the parent: never hand one document's node to another —
create a same-named sibling instead.

---

## 7. One processing path

`_process_versioned_doc()` ([orch.py:292](../dynamic_crawler/formfill/orch.py#L292))
is the parent's CBB path with the CBB check removed and the hand-written
`UPDATE regulation_versions` SQL replaced by `repo.mark_all_versions_inactive()`.

**New document:**
1. `_insert_regulation(doc)`
2. `insert_regulation_version(..., status="active", change_summary="first version")`
3. Text decision → log it
4. Analyse (only if `analyse=True`)

**Modified document:**
1. `mark_all_versions_inactive(existing_id)`
2. Snapshot the **old** content as an inactive version
3. Snapshot the **new** content as the active version
4. `archive_current_analysis(...)` — old analysis rows out of `compliance_analysis`
5. `update_regulation(existing_id, content_hash=new_hash)`
6. Text decision → analyse

Note `analyse` defaults to **False**. An analysis-free run still does the full
crawl, classification, versioning and text decision, and logs what it *would*
have sent:

```
llm_analysis / SKIPPED / analyse=False; would have sent 48,201 chars as pdf_text
```

That's what makes a preview run cheap — analysis is ~$0.007 and ~4 minutes per
document.

---

## 8. What comes out — the report

```python
{
  "regulator": "SBP", "source": "sbp.circulars",
  "crawled": 4160,
  "classified": {"new": 12, "modified": 3, "unchanged": 4145, "disappeared": 0},
  "processed": 15,
  "limit": None, "analyse": False,
  "inventory_hash": "a3f9c21b8e04",
  "run_trustworthy": True,
  "gate_problems": [],
  "disappeared_actioned": False,
  "tables": {"regulations": 4160, "regulation_versions": 4163, ...}
}
```

That's the whole run in one object — which is what `api.py` returns over HTTP and
what makes a run reviewable without reading `orchestrator.log`.

---

## 9. In one paragraph

Ask the crawler for everything. Decide whether the *run itself* is trustworthy
before believing anything it implies. Sort documents into new / modified /
unchanged / disappeared using one identity key and one content hash — and do
literally nothing for the unchanged majority. For the rest: build a properly typed
folder tree, snapshot the content as a version, decide what text is worth sending
(both sources when they disagree), analyse if asked, and hand back a report saying
exactly what happened and what was deliberately not done.

---

## 10. Honest limitations — read this before promoting it

These are real, current, and worth knowing:

1. **It only runs against `ExcelRepo` today.** `find_by_identity` and
   `find_by_reference` exist **only** in
   [formfill/excel_repo.py](../dynamic_crawler/formfill/excel_repo.py#L130) — not
   in [storage/mssql_repo.py](../storage/mssql_repo.py). `find_by_identity` is
   called **unguarded** at [orch.py:124](../dynamic_crawler/formfill/orch.py#L124),
   so pointing this at MSSQL crashes on the first document. Those two methods must
   land in `mssql_repo` before it can run in production.

2. **`disappeared` is always empty outside Excel.** The bucket is filled by
   iterating `self.repo.t["regulations"]` ([orch.py:154](../dynamic_crawler/formfill/orch.py#L154)) —
   an ExcelRepo-only in-memory table, behind a `hasattr(self.repo, "t")` guard.
   Against MSSQL the guard is False and nothing is ever detected as disappeared,
   so the completeness gate currently has nothing to gate. `last_good_run`,
   `record_run` and `counts` are likewise ExcelRepo-only (those *are* hasattr-guarded,
   so they degrade quietly rather than crashing — which also means the 5% tolerance
   check silently never fires).

3. **It lost the parent's concurrency.** `run_for_regulator` is a plain `for` loop
   ([orch.py:375](../dynamic_crawler/formfill/orch.py#L375)); it doesn't call
   `_process_docs`, so the parent's `DOC_MAX_WORKERS` thread pool (default 4) is
   unused. Fine for a preview run, slow for 4,160 SBP circulars with `analyse=True`.

4. **The `identity` constructor argument is ignored.** `__init__` accepts
   `identity=("document_url", "doc_path")`, but `_identity_of`
   ([orch.py:79](../dynamic_crawler/formfill/orch.py#L79)) hardcodes exactly those
   two fields. The "one *configured* identity key" isn't configurable yet.

5. **The inventory-hash shortcut doesn't shortcut.** An unchanged inventory hash
   logs `"nothing to do"` ([orch.py:369](../dynamic_crawler/formfill/orch.py#L369))
   and then processes anyway. Harmless (the hash-based `unchanged` bucket catches
   it per document) but misleading to read.

6. **Archived analysis is tagged with the wrong version.** The parent archives old
   analysis against the **old** version id; the new code discards the
   archived-content version id and calls
   `archive_current_analysis(existing_id, version_id)` with the **new active**
   version id ([orch.py:317](../dynamic_crawler/formfill/orch.py#L317)). Old
   analysis rows end up pointing at the version that replaced them.

7. **The modified path doesn't refresh the regulations row.** It updates only
   `content_hash`. The parent's CBB path also set `document_html` and
   `published_date`, so the `regulations` row keeps the *old* HTML while the new
   version snapshot has the new HTML.

8. **`content_hash` for page attachments isn't a content hash.**
   [pipeline.py:190](../dynamic_crawler/formfill/pipeline.py#L190) sets it to
   `content_key(f"{href}|{label}")` — a hash of URL + label. If a regulator
   silently replaces a PDF at the same URL with the same link text, the hash is
   identical and the document classifies as `unchanged` forever.

9. **Nothing in production uses it.** Only the formfill API path builds a
   `NewOrchestrator`. [jobs/](../jobs/) and [scheduler/](../scheduler/) still run
   the parent class.
