# Running the new pipeline — one API, two engines, no database

This is the thing to run when you want to see what a regulator would put into the
library **without writing to MSSQL**. Every run produces one `.xlsx`. When a
workbook looks right, that is the signal to promote it — see §7 for what is still
missing there.

---

## 1. What this is

There is exactly one seam between crawling and the pipeline:

```python
docs = self.crawler.fetch_documents()      # -> List[RegulatoryDocument]
```

Everything after it — folder tree, change detection, versioning, text extraction,
LLM analysis — is regulator-agnostic. So an engine only has to answer that one
call, and there are two that do:

| engine | config lives in | what it is |
|---|---|---|
| **dynamic (formfill)** | `dynamic_crawler/hints/*.yml` | a twelve-field form describing the page; the LLM helps write the form, our code does the crawling |
| **generic** | `config/sources/*.yml` | zero-config link walker; a regulator here is a LIST of sources, each `generic` or `custom` |

`config/sources/*.yml` can mix both — SAMA keeps its hand-written circulars
crawler and could run generic sources beside it, in one file, with no python
change. The orchestrator cannot tell them apart.

Both go through **the same** `NewOrchestrator` and the same `ExcelRepo`. That is
the point: whatever differs between engines is behind the seam, so classification,
the completeness gate and versioning behave identically either way.

---

## 2. Start it

```bash
venv/Scripts/python.exe -m uvicorn dynamic_crawler.formfill.api:app --port 8100
```

Then open **http://127.0.0.1:8100/docs** and click Execute — Swagger is the
easiest way to drive this.

> Use `venv/Scripts/python.exe`, not `python`. The system interpreter has no
> `selenium`, and a few crawlers import it at module load. That is the whole
> cause of `ModuleNotFoundError: No module named 'selenium'`.

It runs on its own port, as its own app, deliberately **not** wired into
`apis/pipeline_api.py` — that one is connected to production MSSQL. A run here
cannot reach the database.

---

## 3. What to trigger

| | endpoint |
|---|---|
| see what is available | `GET /forms` · `GET /sources` |
| **dynamic** approach | `POST /trigger/{form}` |
| **generic** approach | `POST /trigger/source/{regulator}` |
| past runs | `GET /runs` · `GET /runs/{run_id}/excel` |

```bash
# dynamic — a formfill form
curl -X POST "http://127.0.0.1:8100/trigger/sama.circulars?limit=5"

# generic — a config/sources regulator
curl -X POST "http://127.0.0.1:8100/trigger/source/MISA?limit=5"
```

### Available today

**Dynamic forms** (`GET /forms` for the live list, with each form's `approved`
flag):

```
aml.rules           gosi.saned        gosi.social_insurance   mhrsd.regs
misa.laws           moe.regulations   moh.rules_archived      moh.rules_recent
sama.circulars      sama.sandbox      sbp.circulars           sdaia.regs
simah.rules         tadawul.rules
```

**Generic regulators** (`GET /sources`): `MISA` (generic), `SAMA` (custom).

`approved=false` means the form has not passed the verify gate. The API runs it
anyway — a preview is exactly when you want to look at an unapproved form — but
do not read a clean workbook as a green light.

### Parameters that matter

| | |
|---|---|
| `limit` | documents to PROCESS. `5` is a good first run. `0` or omit for all. The crawl is unaffected — you always see the full `crawled` count. |
| `analyse` | run the 4-stage LLM analysis. **Default false.** Roughly $0.007 and ~4 minutes per document. |
| `reuse_last` | *(dynamic only)* use the crawl already on disk instead of re-crawling. Default true. SAMA circulars takes ~45 minutes to re-crawl. |
| `workbook` | append to an existing workbook. This is how you test change detection — see §6. |

There is no `reuse_last` on the generic endpoint: that engine runs as a
subprocess and owns its own output directory, so there is no crawl-on-disk to
point at. Every call crawls; use `limit` to keep it short.

---

## 4. Reading the result

A run returns a report. The fields that tell you whether to trust it:

```json
{
  "crawled": 94,
  "classified": {"new": 94, "modified": 0, "unchanged": 0, "disappeared": 0},
  "processed": 8,
  "run_trustworthy": true,
  "gate_problems": [],
  "inventory_hash": "e31b62be2113",
  "excel": "output/formfill/_orch_runs/MISA-20260810-102538.xlsx"
}
```

**`classified`** — the four outcomes, decided on one identity
`(document_url, doc_path)` with `reference_no` as the tiebreak for a document
republished at a new URL. `modified` means same identity, different
`content_hash`. Only `new + modified` are processed; `unchanged` costs nothing,
which is what makes a nightly run minutes rather than hours.

**`run_trustworthy` / `gate_problems`** — the completeness gate. A run may not
mark anything `disappeared` unless the run itself is sound: no bot-protection
pages, no early stop, not capped, and the count within tolerance of the last good
run. This exists because SDAIA returned **415 / 363 / 439 documents on three runs
of identical code** — a run that "loses" 52 documents is not a run where 52 were
withdrawn.

**`inventory_hash`** — a fingerprint of what the crawl found. Identical hash
across two runs means the crawl is deterministic. Worth checking before you
believe anything about `modified`.

### The workbook

One sheet per real table:

| sheet | what to look at |
|---|---|
| `regulations` | one row per document. `status` holds the monitoring state (new/modified/unchanged). |
| `compliancecategory` | the folder tree — `type` is `F` for folders, `R` for the leaf, which IS the document |
| `regulation_versions` | content snapshots, now for every regulator, not just CBB |
| `compliance_analysis` | LLM output — empty unless `analyse=true` |
| `requirement_mappings` | matched obligations — **always empty here**, see below |
| `processing_log` | one row per step per document; where to look when something was skipped |
| `run_history` | `row_count` + `inventory_hash` per run — what the completeness gate compares against |

`requirement_mappings` is empty by design: the matching corpus reads
(`get_all_compliance_requirements`, controls, KPIs) return nothing, because there
is no internal register in a spreadsheet. Matching will correctly find nothing.
**Anything that depends on the real corpus has to be checked against the real
database — this repo cannot tell you about it.**

---

## 5. Testing a regulator end to end

The order that finds problems fastest:

**1. Does it crawl at all?** Small limit, no analysis.

```bash
curl -X POST "http://127.0.0.1:8100/trigger/source/MISA?limit=3"
```

Check `crawled` against what the site claims. A regulator that states its own
total — "Showing 1-10 of 75 items" — is giving you a free completeness proof;
use it.

**2. Is the folder tree right?** Open the workbook, look at
`compliancecategory`. The path should be where a person would have found the
document on the site. Check the leaf row is `type=R`, not `F` — a document
rendering as an empty folder is the classic symptom.

**3. Is there text to analyse?** `processing_log` says why anything was skipped.
`SKIP — nothing reached 200 chars` means neither the HTML nor the file had
content; that is a crawl problem, not an analysis one.

**4. Does change detection work?** §6.

**5. Only then, analysis.** `analyse=true` with a small `limit`. It costs money.

---

## 6. Testing change detection

This is the part most worth testing, and the part that was broken.

```bash
# run twice into the SAME workbook
curl -X POST ".../trigger/source/MISA?limit=8&workbook=misa-ct.xlsx"
curl -X POST ".../trigger/source/MISA?limit=8&workbook=misa-ct.xlsx"
```

Run 1 → `{"new": 94, "unchanged": 0}`
Run 2 → `{"new": 86, "unchanged": 8}`

The 8 that were *processed* in run 1 come back `unchanged`. The rest stay `new`
because `limit=8` meant they were never inserted. Omit `limit` and the second run
should report everything `unchanged`.

**If run 2 reports everything `new` again, change detection is broken** — do not
explain it away. That was the state until recently: `doc_path` has three
representations (MSSQL stores JSON, ExcelRepo stores pipes, the classifier
compares arrows), the string compare matched none of them, and a second run
inserted every document again — 3 documents became 6 rows, with the workbook
still looking perfectly fine.

---

## 7. What is NOT here

**No approve → database step.** Nothing promotes a reviewed workbook into MSSQL.
That is the missing half of the workflow.

**`MSSQLRepository` cannot satisfy this orchestrator yet.** It is missing five
methods that `ExcelRepo` implements:

```
find_by_identity      required, unguarded — a real DB run crashes without it
find_by_reference     required, unguarded
last_good_run         guarded, but the completeness gate has no baseline without it
record_run            guarded
counts                guarded
```

This is the actual reason the pipeline has never run end-to-end against the
database, and it has to be built before the approve step can exist.

**`db_compare.py` has never been run** (`site_runners/db_compare.py`). It is the
only thing that would prove *coverage* — that the new engines find what the old
per-regulator crawlers already put in the database. Everything measured so far
proves consistency, not correctness.

---

## 8. Command-line equivalent

Same pipeline without the API, useful in a scheduler:

```bash
# crawl and report only — no database, no LLM, no connection opened at all
venv/Scripts/python.exe jobs/run_regulator.py MISA --dry-run
venv/Scripts/python.exe jobs/run_regulator.py MISA --dry-run --to-excel out.xlsx
```

`jobs/run_regulator.py` without `--dry-run` targets **MSSQL**, not Excel, and is
therefore subject to §7. Use the API for preview runs.

---

## 9. Tuning

| env var | default | |
|---|---|---|
| `DOC_MAX_WORKERS` | 4 | documents processed concurrently. Set `1` for serial. |
| `LLM_MAX_CONCURRENCY` | — | total in-flight LLM requests, inside `StagedLLMAnalyzer`. Bounds the pool above, so raising workers cannot stampede OpenRouter. |

The orchestrator previously looped serially and ignored both. With
`analyse=true` at ~4 minutes a document, that was 2.7 hours for a 40-document run
instead of 40 minutes.

Enabling the pool needed two locks first, and they are worth knowing about if you
touch `ExcelRepo`: `_id()` is a read-modify-write and was handing out duplicate
primary keys, and the folder walk is a get-then-insert spanning several repo
calls, so two documents sharing a parent folder each created it. Both failed
silently with the workbook still looking correct.
