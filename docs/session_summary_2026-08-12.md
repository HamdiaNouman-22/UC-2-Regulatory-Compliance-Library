# Session summary — crawler & library work, 2026-08-12

Everything after the LLM-analysis optimisation. That earlier work is written up
separately in `docs/staged_analyzer_optimization.md`, `docs/optimization_results.md`
and `docs/determinism.md`; this file starts where the crawler work begins.

Detail for each item is in `docs/final_changes.md` (sections A, B, C1–C18).

---

## 1. What we set out to do

Four threads, in order:

1. **Review and merge** the teammate's `feature/crawler-dev-fakih` branch safely,
   with a rollback point (`079d246`).
2. **Test monitoring + crawling + versioning in one sequence**, analysis OFF,
   results to Excel that can later be approved into MSSQL without re-crawling —
   monitoring results kept in a SEPARATE set of workbooks.
3. **Model one instrument with several attachments**, matching the existing
   manual library (`document_url` → `document_urls`).
4. **Correct the crawl output** for every regulator, then re-crawl.

Thread 4 is where most of this session went, and it turned up more engine bugs
than per-regulator config mistakes.

---

## 2. Structural changes

### 2.1 `document_urls` — a row is one instrument with several files

`RegulatoryDocument` gained `document_urls: List[str]`, declared LAST so the
positional signature is unchanged, with `__post_init__` keeping the two fields in
step: give it a list and `document_url` becomes the first entry; give it a single
URL and the list is built from it. Nothing that reads `document_url` had to
change.

The formfill pipeline gained `attachment_is_document: "combined"` along
`true`/`false`. `combined` keeps ONE row per instrument, puts every attached file
in `document_urls`, and hashes the joined list so the identity is stable.

Verified on SDAIA (29 rows / 36 files, reproducible) and now used by MHRSD.

### 2.2 `status` belongs to a human — nothing automated may write it

`status` is the review decision (active / reject) and it governs whether a row is
promoted into the main system. `_set_status` had been writing the MONITORING state
(`new` / `modified` / `unchanged`) into it — a fourth meaning for a column whose
docstring already listed three.

The monitoring state now lives only in `extra_meta["monitoring_status"]`, and
`status` comes out EMPTY.

One trap in the same change: `storage/mssql_repo.py` read the column as
`getattr(document, "status", "active") or "active"`, and `""` is falsy — so an
empty status was coerced straight back to `"active"`, marking every fresh row as
though a person had approved it. Now only a MISSING attribute defaults to
`active`, which preserves behaviour for every non-formfill crawler.

Confirmed empty across all 11 workbooks.

### 2.3 `extra_meta` is about the DOCUMENT, not about the crawl

Every key was checked for a reader before removal:

| key | readers | action |
|---|---|---|
| `content_text` | 19 | kept — load-bearing (`orchestrator.py:183`, `formfill/orch.py:685`) |
| `record_kind` | 1 | kept — `jobs/run_regulator.py:69` |
| `crawler`, `hints`, `form_approved_by` | 0 | removed |
| `parent_page_url`, `shape`, `seed_url`, `depth`, `row_text` | 0 | removed |

`hints` was the worst: it wrote an absolute developer path onto every stored row.
`form_approved_by` was still the placeholder `"your name"`.

### 2.4 A corrective re-crawl must REPLACE, not accumulate

Both runners passed `workbook=`, which loads the existing file and compares
against it. Right for monitoring, wrong for a corrective crawl — the first re-run
after the fixes reported:

```
AML    crawled=11   new=0  unch=11
MISA   crawled=89   new=0  unch=89
SDAIA  crawled=29   new=0  unch=29
MOE    crawled=136  new=0  unch=136
```

Everything `unchanged`, so nothing was rewritten: the fixes were in the code and
absent from the output, and MC/ZATCA workbooks had accumulated 28+28 and 38+38
rows — old wrong rows beside new ones, in the file that gets approved.

Both runners now move the previous workbook aside (kept, not deleted).
`--append` restores the old behaviour for change-detection testing.

**Knock-on worth knowing:** a fresh run has real work to do. MOE took 55s when
everything classified `unchanged` and 2878s when all 136 documents were actually
extracted. The fast number was the crawl doing nothing.

---

## 3. Engine bugs found (not per-regulator config)

These were the substance of the session. Each was measured, not guessed.

### 3.1 `EXTERNAL_LAW_PORTALS` listed the regulators we crawl

**Symptom:** ZATCA returned 38 rows of site chrome — Contact Us, Careers, News,
Brand Identity — and zero regulations.

**Cause:** the set contains `zatca.gov.sa` and `mc.gov.sa`. It answers "another
site links OUT to zatca; treat that link as a law, not a page". The word doing the
work is EXTERNAL. When the seed IS zatca.gov.sa, every ordinary nav link matched
the host and was marked a terminal document — which also stopped the crawl
descending, since every link it might have followed was already a leaf.

`generic_crawler/crawler_MISA_MC_ZATCA.py` **already had the fix** — an
`is_external_law_portal(url, seed_host)` guard with a docstring describing this
exact failure. The shared engine had the copy without it. Ported, and threaded
through `is_document_link`, `doc_type_of` and `probe_scope`.

**Correction to something said earlier in the session:** I first attributed the
junk rows to a scope bug. The reproduction disproved it — `n_pages 1,
n_documents 38`. They were never pages, and documents are collected regardless of
scope, so no scope setting could have filtered them.

### 3.2 `scope: prefix` kept the seed's filename

Genuine bug, separate from 3.1. `seed_prefix = path.rstrip("/")` is right only
when the seed is a DIRECTORY:

```
seed    /en/RulesRegulations/Pages/rules.aspx
prefix  /en/RulesRegulations/Pages/rules.aspx      <- a leaf
test    /en/RulesRegulations/Agreements  -> False  <- the real content
```

`scope_prefix()` now strips a trailing filename, then a trailing `/pages`
(SharePoint's page store — the real sibling sections are `/Taxes` and
`/Agreements`, under the section but not under `Pages`). Never returns `""`, which
would match every path and silently turn `prefix` into `host`.

MISA, the only prefix source with a directory seed, was the one that crawled
correctly — that is the tell. 10 regression tests in `tests/test_scope_prefix.py`.

### 3.3 Excel was silently truncating documents — and promotion would have kept it

**Symptom:** GOSI HTML "stops at Article 20 / 26".

**Cause:** `excel_repo.py` cut any cell over 32,000 chars, because Excel's hard
limit is 32,767. The crawler had captured the whole instrument — the truncation
suffix even states the true size, 92,995 and 34,370 characters.

**The serious part:** `promote.py` reads the workbook cell, so approving into
MSSQL would have written the PREVIEW and made the truncation permanent. This
affected every long document in every regulator; GOSI is only where it became
visible.

Oversized values now go to a `<workbook>.fulltext.json` sidecar with a marker in
the cell; the cell keeps its readable 32k preview. `promote.py` rehydrates via
`resolve_overflow()` and logs an ERROR if the sidecar is missing rather than
promoting truncated content quietly.

Verified end to end: 99,211 chars in → 32,112-char cell → 99,211 back out. On the
live run GOSI-SI restores to 92,995 and Saned to 34,370.

### 3.4 NaN is truthy

`run failed: 'float' object has no attribute 'strip'` blocked every GOSI and MOH
re-run for days.

```python
old_hash = (existing.get("content_hash") or "").strip()
```

pandas represents an empty Excel cell as `float("nan")`, and **NaN is truthy** —
so `or ""` never fires and `.strip()` is called on a float. It only bites on the
SECOND pass, when a workbook is read back, which is exactly the change-detection
path: the crawl works, the comparison against stored rows dies.

All six `.strip()` sites in `orch.py` now go through a NaN-safe `_text()`. GOSI
went from FAILED to 43s and 22s.

### 3.5 A tab click that had not landed

**Symptom:** MOE had 20 rows not on the website.

**Cause:** the same document filed under two categories — `Scholarship Program`
(true) and `Special Education` (false). `_walk_tabs` clicked a tab, waited a fixed
`wait_ms`, and harvested whatever was on screen; when the site had not re-filtered
yet it harvested the PREVIOUS tab's rows under the NEW tab's label. The tab strip
needs 14s to render; the per-tab settle was 1500ms.

Pagination had guarded this for a long time — it stops on "rows unchanged after
click", because a disabled Next re-harvests page 1 forever. Tabs never got the
same guard. Now the row fingerprint is compared across clicks: unchanged means
wait longer and retry, and if still unchanged the tab is skipped and reported as
`unchanged-after-click`.

Tabs that genuinely share a document still record both placements. Result: 136
documents, **0 duplicate URLs**, matching the form's own verified count.

### 3.6 An empty run passing the completeness gate — twice

CMA reported **0 documents with `gate=PASS`** after 272s, then again at 00:43.

The first cause (a wrong assumption about `crawl_tab`'s return type) had been
fixed with an "all tabs failed" guard. But that guard counts tabs that RAISED, and
that is not how these handlers fail: `load()` retries three times, returns False,
and each handler prints `{"event": "error"}` and returns empty. Nine silent zeros,
no exception, gate passes.

Zero documents across all tabs is now a raised fault, as is "no tab had an
implemented handler" — a third path to a clean zero.

**SAMA had the identical hole**: three sub-crawls each swallowing their own
exception, so all three failing returns an empty list with no error. Closed
BEFORE running it, not after. It also now exposes `source_names` and
`last_result` so the gate sizes each section separately.

### 3.7 Titles that were action words

`Press Here` was stored as a document TITLE. `_GENERIC_LINK_TEXT` listed "click
here" but not "press here"; ZATCA produced a `More` the same way. Widened to
cover press/tap/see more/details and the Arabic equivalents, with a filename
fallback:

```
'Press Here' + no row title  ->  'OH benefits Regulation'   (from the file)
'More'       + no row title  ->  'TFA'
```

The generic engine has its own `best_doc_title`, which matched
`GENERIC_LINK_TEXT` EXACTLY — and Ministry of Commerce's text was
`click here​`, a zero-width space. Visually identical, not equal. Link text
is now normalised before the lookup.

### 3.8 `doc_path` repeated itself

```
Ministry of Commerce | Regulations and Laws | Internet | Ministry of Commerce | Regulations & Laws
```

All 115 MC rows shared that trail. `_dedupe_keep_order` only removes ADJACENT
repeats, and SharePoint's site-collection crumb "Internet" sat between the two
copies of the regulator. `_clean_trail` now drops CMS crumbs and any crumb already
said earlier, treating "Regulations & Laws" and "Regulations and Laws" as the same
folder.

### 3.9 `source_system` was doing double duty

`doc_path` renders as `regulator | source_system | …sections… | title`, so
`source_system` is a display crumb as well as a machine key — and the configs used
codes, which showed up in the library: `MC-REGULATIONS`, `ZATCA-RULES`,
`mhrsd.regs`, and MOH with no prefix at all.

Changing it is not cosmetic — it is an identity key. `config/change_signals.yml`
was looking MHRSD up under `"mhrsd.regs"`; its own comment had predicted the clash
and it is now reconciled. Same for both GOSI entries.

---

## 4. Per-regulator corrections

| Regulator | Complaint | Outcome |
|---|---|---|
| MISA | category should be "Laws" | fixed |
| Ministry of Commerce | doc_path + attachments | doc_path fixed; 28 → 72 rows |
| MHRSD | doc_path; HTML wrong; link in `document_url` | doc_path fixed; `combined` set, `document_url` is now the PDF |
| MOE | rows not on the website | 20 phantoms → 0 duplicates |
| ZATCA | "completely wrong, even crawled wrong" | real documents instead of site chrome |
| MOH | doc_path shows only "Regulations" | `Ministry of Health \| Rules and Regulations \| <title>` |
| GOSI | "not correct at all" | doc_path to spec; `Press Here` gone; per-instrument HTML still broken |
| CMA | workbook empty | 0 → 220 |
| SAMA | not crawled | re-added to the sweep, running |

### GOSI doc_path, as specified

```
General Organization for Social Insurance (GOSI) | Laws and Regulations | Social Insurance Law | <title>
General Organization for Social Insurance (GOSI) | Laws and Regulations | Saned                | <title>
```

**Consequence:** the branch moved to the third crumb, so both GOSI sources now
store `source_system: "Laws and Regulations"`. `change_signals.yml` names a source
by the (regulator, source_system) PAIR, so its two GOSI entries became identical
and are merged. Identity is unaffected — that is `(document_url, doc_path)`, which
still differs. A sweep loses the ability to probe one branch without the other;
for two pages on one host that costs nothing.

---

## 5. New tooling

| File | Why |
|---|---|
| `benchmarks/run_source_standalone.py` | Runs one source config OUTSIDE the API. The API holds a single lock for a whole run, so SAMA (hours) would block every short regulator behind it. Same crawler, orchestrator, repo and workbook shape. |
| `benchmarks/export_fulltext.py` | Writes the full document text as browser-openable `.html` files plus an `index.md` manifest, reading through `resolve_overflow()`. Rows with no HTML are listed as `NO HTML` rather than skipped. |
| `tests/test_scope_prefix.py` | 10 tests locking down §3.2, including the exact chrome URLs from the bad ZATCA run. |

Also: `SAMACombinedCrawler` re-added to `benchmarks/run_all_regulators.py`, LAST
in the list, for the same lock reason.

---

## 6. Where the crawl ended up

All 11 workbooks: `status` empty, `extra_meta` slimmed.

| Regulator | rows | note |
|---|---|---|
| CMA | 220 | was 0 |
| MOE | 136 | 0 duplicate URLs |
| MISA | 89 | |
| MOH | 83 | doc_path to spec |
| MC | 72 | was 28 |
| MHRSD | 46 | `document_url` is the PDF; **under-collecting** |
| ZATCA | 31 | real documents |
| SDAIA | 29 | 36 files via `document_urls` |
| AML | 11 | |
| GOSI-SI / Saned | 7 / 2 | inventory correct, text broken |
| SAMA | running | 685 circulars done, rulebook walk in progress |

---

## 7. Open items

**One unsolved bug**

- **GOSI per-panel HTML.** 5 of 6 Social Insurance and 1 of 2 Saned instruments
  store NO html; the first row holds all 92,995 chars of the whole page. Phase 1
  finds all six panels with text lengths `81937, 4278, 82123, 76509, 104951,
  15891` — panel #5 alone reports MORE text than the whole page (82,064), so the
  fragment ids resolve to overlapping, NESTED subtrees. Phase 2 re-stamps and only
  index 0 is found again. Raising `wait_ms` to 8000 changed nothing, so unlike
  §3.5 this is not a settle-time problem. Likely `_find_panels` stamping the
  outermost container and skipping the rest as already-stamped; confirming needs
  the live DOM. **Inventory is correct and approvable; per-instrument text is not
  usable for analysis.**

**Needing a decision, not code**

- **MHRSD collects only page 1.** 46 rows against the 63 `change_signals.yml`
  tracks; `formfill verify` warns its pagination selector matches no usable
  control. The `combined` change explains part of the drop, not all of it.
- **MC and ZATCA pull Arabic documents** into an `/en/` crawl. `lang_lock` applies
  to pages, not to collected documents. Deliberately NOT auto-dropped — some
  instruments are Arabic-only and discarding them would lose real content.
- **MC has no hierarchy**: all 72 rows share one `doc_path`. The junk crumbs are
  gone but the nesting sketched (`laws and regulations / public consultations`) is
  not built.
- **ZATCA's page walk is shallow** (`n_pages` 1) — the landing page is JS-rendered
  and reads back `text_len=22`, queueing nothing. `wait_ms` is now wired through
  the source YAML but did not lift the page count.
- **Counts move between runs** (MC 115 → 72, ZATCA 35 → 31) on identical code.
  Worth a stability check before treating either as a baseline.

**Carried over, not addressed this session**

The merge-review action list (`docs/final_changes.md` Part B) still stands —
notably B1 (verify archiving against a real database), B2 (cap retained analysis
generations), B3 (make the API path archive instead of hard-deleting) and B4 (the
649 `is_current = NULL` rows). The database was unreachable for most of this
session (`10.11.12.76:1437`), so none of it could be tested.

---

## 8. Two operational notes that cost time

- **The API caches imported modules.** `python -m uvicorn
  dynamic_crawler.formfill.api:app --port 8101` must be restarted after changing
  `excel_repo.py`, `schema.py` or `runner.py`. A stale server made the truncation
  fix look like it had failed — the re-run still wrote 32,028 chars with an empty
  sidecar.
- **`python -m dynamic_crawler.formfill.api` does not start a server.** There is
  no `__main__` block, so it imports and exits. Use the uvicorn form above.
  `/runs` is in-memory and empties on restart; the workbooks on disk are the
  durable record.
