# Handoff — onboarding Bahrain Bourse, and the state of monitoring

**Written:** 2026-08-20
**For:** a fresh session with no context from the CBE/MLCU work
**Read first:** [ONBOARDING.md](ONBOARDING.md) — that is the procedure, 8 steps, and it
is current. This file is the **delta**: what was learned after it was written, plus what
is specific to Bahrain Bourse.

---

## 0. Read this before you touch the site

**Bahrain Bourse is a stock exchange, and the last exchange this project crawled is now
permanently blocked.**

`config/change_signals.yml` records it under `skip_hosts`:

> saudiexchange.sa — Akamai 403 "Access Denied" to everything, and a headless Chromium
> is refused exactly as a plain GET is, so it is the IP being judged, not the
> User-Agent. MEASURED 2026-08-15: the same seed page crawled cleanly at 18:27 the same
> day. **The block appeared within two hours**, after a crawl plus repeated probes from
> one address — the same pattern the simah.com note records as *"triggered by repeated
> iteration, not volume"*.

Two hours of ordinary automated access cost this project a regulator permanently. The
19 rows in the library are all it will ever have.

So, for Bahrain Bourse, in this order:

1. **Fetch ONE page by hand first.** Confirm the response is real content, not a
   challenge page or a 403.
2. **Read `robots.txt` and honour it.** Check for a `Crawl-delay`.
3. **Do not loop.** No repeated probing while you develop. Fetch once, save the HTML to
   `output/snapshots/`, and iterate against the saved file.
4. **Do not schedule anything** until a crawl has succeeded by hand twice.

If it blocks, add it to `skip_hosts` with the measurement, and stop. Do not retry your
way out of a block — retrying is what causes them. `until:` is a review date for a
person, not an expiry.

---

## 1. Bahrain context that already exists

**CBB (Central Bank of Bahrain) is already in the library.** It is a *different
institution* from Bahrain Bourse — central bank vs exchange — so this is not a
duplicate, but expect overlapping subject matter and possibly cross-published documents.

**Do not copy CBB's structure.** CBB was onboarded through an older path: it has
`cbb_*.py` files at the repo root, a bespoke `run_cbb_monitoring` in
`scheduler/scheduler.py`, and **no `config/sources/cbb.yml`**. New regulators go through
`config/sources/<name>.yml`, the way `cbe.yml`, `mlcu.yml` and `zatca.yml` do. CBB is
history, not a template.

Existing source configs to read as models — in increasing order of complexity:

```
config/sources/mlcu.yml    small, single-mode, has an identity override
config/sources/zatca.yml
config/sources/cbe.yml     SPLIT SOURCE: one custom crawler + eleven generic crawls
```

`cbe.yml` is the one to read closely. It is heavily commented with the reasoning behind
every choice, and an exchange is likely to need the same split treatment.

---

## 2. The single highest-value thing to check first

**Before writing any crawler, look for the site's own JSON listing endpoint.**

This is the biggest lesson from CBE. Its circulars page renders ten rows behind a "Load
more" button. A prefix crawl of it recorded **18 of 396 circulars — 4.5% — and reported
`status: ok` while doing it.** Nothing about that run looked like a failure.

The page's own JavaScript was calling:

```
GET /api/listing/circulars?pageNo=0&pageSize=500
-> 396 results, 274 KB, one request, no browser
```

The API also returned a publication date, the regulator's own category, and a stable
Sitecore GUID per record — none of which a link walk can read.

**How to find it:** open the listing page in a browser, DevTools → Network → XHR/Fetch,
click "Load more" or page 2, and read what the page asks for.

An exchange site publishing disclosures, circulars or announcements is very likely to
have one. Look before you crawl.

---

## 3. Traps found since ONBOARDING.md was written

### 3a. Check the site has a content container before trusting extraction

`generic_crawler/crawler.py`'s `JS_MAIN_CONTENT` picks the content region with:

```
main, [role="main"], article, #content, .content, #main
```

**CBE had none of them**, so extraction silently fell through to `<body>` on 94 of 94
pages. It does not error. You get the whole page — navigation, banner, footer — stored
as `document_html`, and you only notice when someone reads a row.

Check early:

```javascript
document.querySelector('main, [role="main"], article, #content, .content, #main')
```

If that returns `null`, you need a site profile. Three profile keys exist for this, all
default-off, in `SITE_PROFILES` (see the `www.cbe.org.eg` entry for a worked example):

| key | use |
|---|---|
| `drop_selectors` | extra chrome selectors, for sites whose furniture is `<section class="…">` rather than `<header>`/`<nav>`/`<footer>` |
| `stop_at_headings` | heading texts that END the document; the heading and everything after it in document order is removed |
| `fix_lazy_images` | promote `data-src`/`<source srcset>` onto `<img src>`, absolutise, drop images pointing at nothing |

**Watch for near-miss class names.** CBE's breadcrumb survived every filter because its
class is `breadcrumbs` — *plural* — and the junk list holds `.breadcrumb`. A CSS class
selector does not match a longer class token.

**If you add or change a profile key**, run the offline regression first:

```powershell
venv\Scripts\python.exe generic_crawler\regression_check.py
```

It replays seven frozen regulator pages with no network. Any change outside your host is
a bug. Note `sama_circulars` currently reports `content_text_len` 2,571 vs a baseline of
2,831 — **pre-existing and unrelated**; the original code produces the same number.

### 3b. Export is flaky — run it twice and diff

`tools/workbook.py export` dropped documents on two consecutive CBE runs, differently
each time:

```
baseline    622
run 1       610    (Governance 5 instead of 17)
run 2       620    (two other pages missing, Governance correct)
```

The missing pages were fully crawlable — a standalone crawl found all of them. It is
transient page-load failure during a long multi-source walk, roughly 0.3–2%.

**And the completeness gate cannot catch it.** `export` writes a *fresh* workbook, so
`run_history` is empty and there is no prior count to compare against. Every source
reported `PASS` on the run that found 5 documents where 17 exist.

So: **run the export twice and diff the two workbooks yourself.** Do not trust
`baseline_verdict`. There is an open improvement here — make `export` read the workbook
already at its output path as its baseline — which nobody has done yet.

### 3c. `doc_path`'s last segment becomes the folder-tree label

`compliancecategory.title` is taken from the **last segment of `doc_path`**, never from
the document's title. The design gives each regulation its own leaf node
(`orchestrator/orchestrator.py:944` refuses to let two regulations share one).

Consequence: if your `doc_path` ends at a section name, the tree shows N identical
sibling labels. CBE has five nodes all called "Mobile Wallets", one per document.

Custom crawlers can avoid it by putting the title last, as `crawler/cbe_crawler.py`
does:

```python
doc_path=[self.regulator, self.source_system, title]
```

Generic crawls cannot — their path comes from the page's section trail.

**Do not "fix" this by changing `doc_path` on an existing source.** `doc_path` is part
of the identity tuple `(document_url, doc_path, title)`; changing it makes every stored
document read as one new document plus one disappearance. Fix it in the UI instead:
render `regulations.title` for `type='R'` nodes and `compliancecategory.title` for
`type='F'`.

### 3d. Give every document a `content_hash`

Non-negotiable. Call `crawler/fingerprint.py::stamp_content_hashes(docs)` at your
crawler's single public exit. It never overwrites a hash already set, so a crawler that
computes its own is safe. Missing hashes previously caused full re-OCR of unchanged
documents.

---

## 4. Monitoring — read this before promising anything

**Monitoring is not live for ANY regulator right now.** Not CBE, not MLCU, not the four
KSA ones. This is not a Bahrain problem; it is the current state of the system.

Two separate reasons, both silent:

**Reason 1 — every monitor job is `enabled: false`** in `config/scheduler.yml`. That is
deliberate: a new regulator goes to a workbook for human review first, and this path
writes straight to MSSQL, so enabling it before the review makes the first scheduled run
the ingest.

**Reason 2 — there are two job lists, and the scheduler is reading the wrong one.**

```python
# scheduler/scheduler.py:285
EXECUTION_MODE = os.getenv("EXECUTION_MODE", "API")   # defaults to API
if EXECUTION_MODE == "API":
    JOB_MAPPING = API_JOB_MAPPING     # <- this is what actually runs
else:
    JOB_MAPPING = DIRECT_JOB_MAPPING  # <- where every monitor job lives
```

`API_JOB_MAPPING` holds five entries — `sbp_pipeline`, `secp_pipeline`, `sama_pipeline`,
`cbb_monitoring`, `full_pipeline`. **No monitor job is in it**, and there is no API
endpoint for any of them (searched: the only monitoring endpoint in `apis/` is
`POST /trigger/CBB/monitoring`).

So if you register a monitor job and flip `enabled: true` today, the dispatcher logs
`WARNING: No function mapped for job: <name>` and moves on. No crash, no error surfaced.

**Unresolved. Do not paper over it.** The options are: set `EXECUTION_MODE=DIRECT` (also
switches SBP/SECP/SAMA/CBB from API calls to in-process — a real change to things that
currently work), or write API endpoints for the monitor jobs, or run monitors by hand.
For a brand-new regulator, by hand is right anyway:

```powershell
venv\Scripts\python.exe -c "import sys; sys.path.insert(0,'.'); from jobs.monitor_jobs import monitor_<name>; print(monitor_<name>())"
```

### What you still must do for Bahrain Bourse

Even though nothing is scheduled, do the wiring so it is one flag away:

1. Write `monitor_bahrain_bourse()` in `jobs/monitor_jobs.py`, wrapped in
   `_run_exclusive` like its neighbours.
2. Add it to `DIRECT_JOB_MAPPING` in `scheduler/scheduler.py` **and** its import.
3. Add a slot in `config/scheduler.yml` with **`enabled: false`**.
4. Pick a free time. All monitor jobs share one `_run_exclusive` lock and an overlap is
   a **silent SKIP**, not a failure — a colliding slot starves a job rather than erroring.
   Currently taken on Sunday: `monitor_mc` 04:00, `monitor_cma` 05:00, `monitor_mlcu`
   06:00, `monitor_cbe` 07:00. **08:00 is free.**
5. Add an entry to `config/change_signals.yml` only if it needs something the defaults do
   not give it. The defaults are
   `identity: [document_url, doc_path, title]`, `confirm: false`, `workers: 4`,
   `timeout: 20`.

### Choosing the change signal

In preference order — cheapest and most honest first:

1. **The publisher's own change stamp**, if real. MOH's SharePoint `Modified` field is.
2. **A JSON listing endpoint** — the crawl *is* the signal and costs one request.
3. **ETag / Last-Modified** on the stored files, via the stored-inventory sweep.
4. **Re-crawl the section.**

**Verify any stamp before trusting it.** Two recorded failures, opposite directions:

- CMA's `Last-Modified` returned the *current time* → 1,134 false changes. Loud, so
  someone fixed it.
- CBE's on-page `Last Updated` reads `23 Mar 2023` on unrelated pages alike — one
  build-time constant, three years stale. It would report **zero** changes forever.
  **A signal that never fires is indistinguishable from a site that never changes.**

The second is far more dangerous than the first.

### Identity overrides

If document URLs embed a date or a version — likely for an exchange publishing dated
disclosures — the default identity will read a re-issue as *one new document plus one
disappearance*, which feeds the withdrawal gate.

CBE hit exactly this and solved it in config, not code:

```yaml
identity: [extra_meta.cbe_item_id]
```

The mechanism works end to end and is verified: the source declares it, the wrapper
stamps `identity_fields` onto each document, and `dynamic_crawler/changesignal.py`'s
`identity_for` reads it back — the orchestrator and `promote` both resolve through that
one function so they cannot drift.

**Caveat:** identity only does work on a *second* run. Every row in a first workbook is
new, so `check` passing does **not** exercise your override. It gets its first real test
on the first re-crawl.

---

## 5. The workflow, condensed

```powershell
# 1. crawl to a workbook — opens NO database connection
venv\Scripts\python.exe -m tools.workbook export <name>

# 2. run it AGAIN and diff the two (see 3b — the gate cannot catch a short run)

# 3. validate
venv\Scripts\python.exe -m tools.workbook check output\workbooks\<name>.xlsx

# 4. dry run — reads the DB, writes nothing
venv\Scripts\python.exe -m tools.workbook promote output\workbooks\<name>.xlsx

# 5. only after a person has read the workbook
venv\Scripts\python.exe -m tools.workbook promote output\workbooks\<name>.xlsx --apply
```

Run crawler CLIs **from PowerShell, not Git Bash** — Git Bash rewrites `/en/...` style
arguments into Windows paths.

Keep the `.fulltext.json` sidecar with its workbook. Excel caps a cell at 32,767
characters and oversized values live in the sidecar; the workbook is incomplete without
it.

**Two runs, always.** One run is never a result — run 1 reconciles, run 2 proves
stability.

---

## 6. Known-open items, so you do not rediscover them

| item | status |
|---|---|
| `EXECUTION_MODE` / two job mappings | **unresolved**, blocks all scheduled monitoring |
| Completeness gate blind on the workbook path | **unresolved**, a short export looks identical to a good one |
| Export drops 0.3–2% of documents per run | **unresolved**, workaround is run-twice-and-diff |
| `normalize_input_text` `pdf_text` branch flattens all whitespace | **known, not fixed** — same defect fixed for HTML on 2026-08-19; changing it alters analysis input for ~8,700 existing PDF documents, so it is a decision |
| Folder tree shows repeated labels | **by design**, fix in UI not data — see 3c |
| MLCU Arabic ~10% letter transposition in headers | PDF extraction-order issue, body text clean |

Recent change records worth reading:
`docs/cbe_html_extraction_fix_2026-08-19.md`, `docs/fingerprint_fix_2026-08-16.md`.
