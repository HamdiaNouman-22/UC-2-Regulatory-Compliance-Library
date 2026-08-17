# Session summary — 2026-08-13 / 14

Handoff document. Written to be read cold by someone who was not in the session.
Continues `docs/session_summary_2026-08-12.md`.

---

## 1. What this session was for

Three things, in the order they were asked for:

1. **Global corrections** to what every regulator stores — trim `extra_meta`, leave
   `status` empty.
2. **Per-regulator fixes** (MISA, MC, MHRSD, MOE, ZATCA, MOH, GOSI), then a full
   re-crawl of everything into approvable Excel workbooks.
3. **Bring SIMAH in** from the partner's snapshot, and **model Ministry of
   Commerce as three entries per law**.

---

## 2. Global decisions — these apply to every regulator

### `status` is a HUMAN field

`status` is left **empty** by every crawler. A person sets it to `active` or
`reject`, and that decision is what drives whether a row moves into the main
system database. Nothing in the crawl path may write it.

One place still coerced it back to `active`: `storage/mssql_repo.py` used
`status or "active"`, which rewrites `""` because the empty string is falsy. Now:

```python
doc_status = getattr(document, "status", None)
doc_status = "active" if doc_status is None else doc_status
```

`None` means "this code path never set it"; `""` means "a human has not decided
yet" and is preserved.

### `extra_meta` carries facts about the document, not about the run

`crawler/generic_crawler_wrapper.py` was storing `crawler`, `shape`, `seed_url`
and similar run bookkeeping in every row. Removed, on both the page-row and
document-row paths. `extra_meta` now holds only what a reader would want about
the instrument itself.

### Identity for a multi-attachment instrument

Reverted the `document_urls` column added earlier — it did not exist in the
schema and would not survive promotion. The model now is:

- one row per instrument
- every file in `extra_meta["attachment_links"]`, pipe-separated
- `document_url` **left empty** when there is more than one file
- identity = `doc_path` + `attachment_links`, declared through
  `extra_meta["identity_fields"]`

With exactly **one** attachment, that file goes in `document_url` and the row
keeps the ordinary `(document_url, doc_path)` identity — there is one url that
names the row, and leaving the column blank would hide it from any reader.

> An empty `document_url` on a multi-attachment row is **correct**, not a bug.
> This was misread once during the session as "3 duplicate URLs" on MHRSD.

---

## 3. The class of bug that cost the most time

**Escape sequences written into source through a shell heredoc arrive as real
control characters.** They are invisible in an editor, in `grep`, and in
`inspect.getsource`. Five instances were found:

| Where | Written | Arrived as | Effect |
|---|---|---|---|
| `runner._is_doc` | `download\b` | `download<BACKSPACE>` | never matched a download endpoint |
| `runner.JS_DETAIL` | `\t and \n` in a `//` comment | a real newline | ended the comment, left `,` as code → **all 34 ZATCA rows lost their HTML, twice in one hour** |
| `runner.JS_NEXT_STATE` | `\bdisabled\b` | backspaces | pagination end never detected |
| `site_runners/cma_laws.py` | `CARD_DATE` regex | control chars | **CMA dates had never once parsed** |

A snippet that fails to parse does **not** fail loudly: `page.evaluate` raises,
the caller reads it as "this page had no detail", and the run finishes with a
PASS gate and empty content.

**Guard added:** `tests/test_js_snippets_parse.py` — 45 tests. Every `JS_*`
constant is checked for stray control characters, for raw newlines inside `//`
comments, and is handed to Chromium to confirm it actually parses. Run it after
any edit to `runner.py`.

---

## 4. Per-regulator state

| Regulator | State | Notes |
|---|---|---|
| **ZATCA** | rebuilt as a form, 34 cards | HTML fixed; Glossary + row-loss fixed 08-14 — see §5 |
| **MOE** | 136 rows, 0 duplicates | lost "General" section once to my own tab guard; fixed |
| **MHRSD** | 62 rows | full ministry name now stored; metadata sidebar lifted to `extra_meta`; **on only 19/62 — unexplained** |
| **GOSI** | per-panel HTML working | in-page stamps were destroyed by fragment navigation; runner now re-prepares them |
| **MISA** | category → "Laws" | matches the site's own tab |
| **SIMAH** | brought in end-to-end, offline | Cloudflare-blocked host; works from `output/snapshots/simah.rules.html` |
| **SAMA** | crawled | **237 truncated cells with no sidecar — outstanding** |
| **MC** | form built, 20 laws | **blocked by a site-side JS error** — see §6 |
| **SDAIA / CMA** | crawled | row-count changes vs. previous runs **unexplained** |

### SIMAH — one entry, not two

The Credit Information Law and its Implementing Regulations are stored as **one
merged entry** (user's decision). The host is Cloudflare-blocked (1020-class,
not a challenge), so the work is done from the saved snapshot;
`config/change_signals.yml` blocks the host until 2026-09-04.

### MHRSD — what was actually asked for

Three things, all now done: the metadata sidebar goes to `extra_meta` rather
than into the document HTML; the rest comes through as proper HTML; and the
**full ministry name** ("Ministry of Human Resource and Social Development", not
the acronym) is the first crumb of `doc_path`. `config/change_signals.yml` moved
with it — `(regulator, source_system)` is how a sweep names a source, and if the
two drift the sweep and the ingest path keep separate memories of the same 63
documents.

---

## 5. ZATCA — fixed 2026-08-14

Two defects, one visible and one hidden behind it.

**HTML was empty on all 34 rows.** Cause was the `JS_DETAIL` heredoc newline
(§3). After the fix: **24 of 26 rows carry HTML, median 11,341 characters**, no
leading whitespace (was 133 characters of template indentation).

**Then rows dropped 34 → 26.** `run.json` showed `rows 34` — the crawl was
complete and the **pipeline** was discarding 8. The 8 were exactly those pages
whose only attachment was the **footer Glossary PDF**:

> Bonded Zones · Business (Customs) · Comprehensive Guide to the Implementing
> Rules of Income Tax Law · Customs Brokers · Excise Tax · Income Tax ·
> Individuals (Customs) · Previous Regulations, Rules and Decisions

The chain:

1. `ZATCA-Glossary.pdf` appears twice on every page — once in the site footer
   (`div#footerContent.footer-main`) and once in the megamenu
   (`div.dropdown-menu`). Neither is a semantic `<footer>`/`<nav>`, so the
   runner's tag-based filter never saw them. It attached itself to **32 of 34**
   regulations.
2. On the 8 pages with no real file of their own, the Glossary was the *only*
   attachment. Under `combined`, one file means `document_url = that file` — so
   **8 different regulations were given one identical url**.
3. The page-row claiming guard (added for SIMAH) then dropped 7 as duplicates,
   and the 8th had already been claimed by a two-file row.

**Fixes, at all three levels:**

- `runner.JS_DETAIL` — the link harvest now honours `content.strip`. A form that
  declares a block "not the content" no longer collects PDFs from inside it.
  `strip` previously applied only to the HTML clone, never to link collection.
- `dynamic_crawler/hints/zatca.taxes.yml` — `#footerContent`, `.footer-main` and
  `.dropdown-menu` added to `strip`.
- `pipeline.py` — the claiming guard now only suppresses a row that carries **no
  HTML of its own**. The SIMAH row it was written for was an empty shell; a row
  with its own page is a different instrument that merely shares a file.

Verified live before re-crawling: Bonded Zones 0 doc-links (was 1, the
Glossary), Excise Goods Tax Law 1 (was 2, keeping the real law PDF).

**Still open on ZATCA:** sections 2 and 3 (Tax and Customs Agreements,
Information Exchange Portal) are not yet crawled.

---

## 6. Ministry of Commerce — blocked, and what was wrong about my diagnosis

The user wants **three entries per law**: the law, its regulations, and its
attachments, as three separate rows.

The form is built (20 laws). Detail pages fail with a **site-side JavaScript
error**. I concluded at one point that the code was pointing at the wrong site;
that was **wrong** — I had decoded only the `siteURL` parameter and ignored
`attId`, and all 144 `attId` values are distinct. The user was right that the
site is reachable.

**Next step:** ask the partner whether detail pages load for her now. She runs
the same code on the same network and it worked a day earlier, which points at
the site, not at us.

---

## 7. Tooling added this session

- **`benchmarks/run_source_standalone.py`** — runs one form without the API
  lock. `--form`, `--reuse-last`, `--label`. Archives the previous workbook to
  `_superseded/`, and **aborts if the workbook is open in Excel** rather than
  failing at the last write.
- **`benchmarks/export_fulltext.py`** — writes captured text as browser-openable
  HTML, for content past Excel's 32,767-character cell limit.
- **Overflow sidecar** — text beyond the cell limit is written to
  `<workbook>.fulltext.json` and restored by `resolve_overflow()` on promote, so
  approval never persists a truncated document.
- **Parallel crawling** — regulators can now be run concurrently.

---

## 8. `--reuse-last` — read this before using it

`--reuse-last` replays a crawl already on disk. It applies **only** the fixes
that happen *after* the browser: claiming, identity, column mapping.

It does **not** re-apply `content.extract`, `strip` selectors, `section_trail`,
or anything that changes captured HTML — those are browser-side. I misjudged
this twice in one session. **Any change to `runner.py` or to a form's `content:`
block needs a real crawl.**

---

## 9. Loading the library into MSSQL (uc2-db)

The first real promotes ran on 2026-08-14, into a local `uc2-db`. Four defects
had to be fixed before a single row landed, and each one would have hit every
regulator.

### Which server

This machine runs **two** SQL Server instances, and they hold different data:

| `MSSQL_SERVER` | `@@SERVERNAME` | uc2-db |
|---|---|---|
| `localhost` | `HamdiaNouman` | empty |
| `localhost,1433` | `HamdiaNouman\MSSQLSERVER01` | the 17 real tables |

Bare `localhost` connects over shared memory to the DEFAULT instance; port 1433
belongs to MSSQLSERVER01. `.env` must say `localhost,1433`. `localhost\SQLEXPRESS`
does not exist on this machine at all.

The shared server at `10.11.12.76,1437` is **not reachable from here** (TCP and
ping both fail) — the split the merge review recorded as B10.

### The four fixes

1. **Windows auth was impossible.** `_get_conn` interpolated `UID=`/`PWD=`
   unconditionally, so with no `MSSQL_USERNAME` it sent the literal
   `UID=None;PWD=None`. It now sends `Trusted_Connection=yes` when no username
   is configured.
2. **Every row failed on `year`.** A column that is empty on every row is read as
   `float64`, and float64 CANNOT hold `None` — pandas coerces
   `df.where(df.notna(), None)` straight back to `NaN`, which pyodbc binds as a
   SQL float. The server's message names *float*, not the column, so it reads
   like a schema mismatch: `Parameter 10 (""): The supplied value is not a valid
   instance of data type float`. Fixed with `_nan_to_none` in `promote.py`.
3. **`regulation_versions` inserted zero.** `promote` called
   `insert_regulation_version` with 3 of its 8 arguments; `regulator`,
   `updated_date` and `change_summary` have no defaults. It logged one error per
   row and still reported the run as successful.
4. **`status` was being set to `active`.** Fix 2 turned the empty status cell
   into `None`, and `_insert_regulation` reads `None` as "this code path never
   set it" and substitutes `active` — the exact flag that decides whether a row
   moves to the main system database. `_Doc` now pins an empty status to `""`.

### Regulator naming

House style is **full name, then the acronym**: `Anti-Money Laundering Permanent
Committee (AML)`, `Ministry of Investment (MISA)`, `Saudi Data and AI Authority
(SDAIA)`, `Saudi Credit Bureau (SIMAH)`, `Ministry of Human Resource and Social
Development (MHRSD)`. `Ministry of Commerce`, `Ministry of Education`,
`Ministry of Health` and `Saudi Exchange` stay as they are.

That string lives in **three places that must agree**, or the folder tree and the
regulator column describe different libraries:

    regulations.regulator
    regulations.doc_path        <- the FIRST CRUMB
    compliancecategory.title    <- the root row, the one with no parent

and in a fourth for the sweep: `config/change_signals.yml`, whose
`(regulator, source_system)` pair names the source. Changing the form without
moving that entry leaves the sweep and the ingest path with separate memories of
the same documents.

Prefer changing `library.regulator` in the form and RE-CRAWLING over editing a
workbook. Where a re-crawl is impossible (SIMAH's host is blocked) there is a
three-place rename script; it is the exception, not the method.

### compliancecategory ids

The `1, 2, 3` in a workbook are **workbook-local** and never reach the database.
`promote.resolve_folder` walks the tree parent-first, reuses an existing folder
via `get_folder_id` / `find_folder_in_subtree`, and rewrites each regulation's
`compliancecategory_id` to the real row. Verified after every load with:

```sql
SELECT COUNT(*) FROM regulations r
  JOIN compliancecategory c ON c.compliancecategory_id = r.compliancecategory_id
 WHERE c.type = 'F';        -- must be 0: every regulation points at a leaf
```

### Re-crawl before promoting anything crawled before 2026-08-13 22:02

That is when `file_type` was fixed. Older workbooks mistyped their documents, and
promoting one writes the error into the library:

    SDAIA    HTML 23 / PDF 6   ->  PDF 29
    Tadawul  HTML 19           ->  PDF 19
    MISA     HTML 89           ->  PDF 65 / HTML 24
    MOH      'None' x 83       ->  (re-crawled)

### Empty document_html is not always a bug

- `fetch_details: false` (SDAIA, MOE, MOH, Tadawul, MISA) — inventory by design:
  title, url, hierarchy, no content.
- PDF instruments (AML) — the document is the file; there is no page.
- `fetch_details: true` **and** no html — a real defect. MHRSD was in this state.

### Running it

```bash
venv/Scripts/python.exe -m dynamic_crawler.formfill.promote --dry-run --with-db <workbook>
venv/Scripts/python.exe -m dynamic_crawler.formfill.promote <workbook>
```

`--with-db` makes `skipped_already_present` a real number instead of always 0.
Promote is idempotent — it matches on `(document_url, doc_path)` and a second run
inserts nothing.

`compliance_analysis` and `requirement_mappings` stay at 0 because these crawls
ran with analysis off. That is expected, not a gap.

---

## 10. Monitoring, made real (2026-08-15)

Monitoring existed but had never run end to end. Two defects stopped it, both
silent.

### The chain was broken in two places

**`--targets` was ignored on the signal every regulator uses.** `stored-inventory`
called `_emit(report, a.json_out)` and dropped `a.targets_out`; the other three
signals all forwarded it. So the sweep computed the changed urls, listed them in
its report, and never wrote the file. Exit code 0, no warning. Anything driving a
re-crawl from that file read "nothing changed". Measured on SDAIA: 2 documents
modified, 2 targets in the report, 0 written, no crawl.

**`--only-urls` never reached the orchestrator.** It existed on `formfill run`
and stopped at the runner, so a targeted crawl produced no versioned rows and
stored nothing. Now carried through `FormfillCrawler(only_urls=...)` to
`run_source_standalone.py --only-urls`.

### The loop

`benchmarks/monitor_all.py` — one regulator at a time, four timed phases, output
flushed as each finishes:

    1 SWEEP     probe / read a feed  ->  new / modified / unchanged
    2 TARGETS   write the changed urls
    3 CRAWL     re-crawl ONLY those, through NewOrchestrator -> workbook
    4 PROMOTE   insert: new rows, and a new version per changed document

Steps 3 and 4 are skipped when nothing changed — which is the point.

    venv/Scripts/python.exe benchmarks/monitor_all.py --baseline --limit 0
    venv/Scripts/python.exe benchmarks/monitor_all.py --limit 0
    venv/Scripts/python.exe benchmarks/monitor_all.py --only SDAIA

Run it UNPIPED. It flushes per regulator; piping through `tail` buffers it all to
the end.

### SAMA reads its own revision page now

`dynamic_crawler/sama_feed_signal.py`, wired as `--signal sama-feed` beside the
other four. Measured 2026-08-15 over 2026-01-01..2026-08-15:

    entries 22 | already_tracked 18 | not_in_library 4 | node_url_unresolved 0

**5.4 seconds against ~40 minutes.** The 4 unmatched entries are discoveries a
stored-inventory probe cannot produce. `node_url_unresolved: 0` is the number
that matters: the feed speaks slugs, the library stores `/en/node/<id>`, and
opening each changed document to read its canonical node url is the step whose
absence invalidated the first test of this feed months ago.

Two things measured that the brief left open: **`items_per_page` caps at 40**,
and a larger value returns an EMPTY page rather than an error — so asking for
more reads as "nothing changed". And the feed **cannot see deletions**, so
`stored-inventory` stays SAMA's way of finding removals: monthly, not daily.

CMA was checked for an equivalent and has none. Most candidate paths return an
identical 1,221-byte soft-404 (answering 200, so "it responded" means nothing).
`/sitemap.xml` holds 110 navigation urls with **zero overlap** against the 1,909
stored CMA document urls; `/en/updates` is a newsletter signup form.

### Which signal a source uses is CONFIG, not code

`config/change_signals.yml` gained a `signal:` key, read by `monitor_all.py` via
`signal_for()`. That file already described a source's monitoring (MHRSD's
`sitemap:`, AML's `confirm:`), so the choice of signal belongs there too —
hardcoding it in the driver would put one decision in two places.

### Blocked hosts

`saudiexchange.sa` added to `skip_hosts` until 2026-08-22. Akamai 403 to
everything, and **a headless Chromium is refused exactly as a plain GET is** — it
is the IP being judged, not the User-Agent. It is NEW: the same seed page crawled
cleanly at 18:27 the same day, which is where the 19 stored rows came from, and
the block appeared within two hours of a crawl plus repeated probes from one
address. The sweep now reports `probed: 0, skipped_hosts: {saudiexchange.sa: 19}`.

> Editing that block nearly deleted the simah.com entry: the edit anchored on
> `- host: simah.com`, absorbing its `until`/`why` into the new entry as
> duplicate keys. YAML keeps the last, so the file parsed to ONE entry with the
> new host and SIMAH's date, and SIMAH would have been probed straight into its
> Cloudflare block. Caught only because the check printed the PARSED result
> rather than the file text.

### Baseline state, 2026-08-15

All twelve baselined. SAMA 5.4s (feed) against CMA 600s (1,979 probes) — CMA is
now the slowest source in the library by a wide margin and has no feed to move to.

CMA and ZATCA both reported `SWEEP FAILED` and had **entirely succeeded** —
1,979 and 34 records written. `subprocess.run(text=True)` decodes with the LOCALE
encoding (cp1252 here) and every report carries Arabic titles and em dashes. The
child wrote utf-8 correctly; the parent could not read it. CMA's 600 seconds of
real work was reported as a failure. Fixed with an explicit
`encoding="utf-8", errors="replace"`.

### Promote's own bugs, found by loading the library

Three, all of which the orchestrator already handled correctly — which is the
argument for consolidating the two implementations of the identity rules:

1. **rows with an empty `document_url` re-inserted on every promote.** The
   existence check was skipped entirely when the url was blank, and blank is not
   an edge case — it is the multi-attachment convention. MHRSD duplicated its 3
   such rows; MC has 16. Now uses the row's declared `identity_fields` via
   `find_by_identity_fields`.
2. **versions stacked on repeat promotes** — MHRSD reached 121 versions for 62
   regulations, every one `active`. Now skips a snapshot whose `content_hash` is
   already stored. The run that did this REPORTED `skipped_already_present: 62`.
3. **no version superseded its predecessor** — `mark_all_versions_inactive`
   existed and was never called, so a modified document ended with two `active`
   versions and nothing said which was current.

`regulation_versions.regulator` was `nvarchar(50)` while `regulations.regulator`
is `nvarchar(255)`; MHRSD's name is 57 characters, so all 62 of its versions
failed while its regulations inserted. Column widened to match.

---

## 11. Open items, in priority order

1. **ZATCA** — confirm the re-crawl returns 34 rows with HTML intact, then
   re-verify and re-approve the form (it currently reads *APPROVED BUT SINCE
   EDITED*, because the hints file changed after approval).
2. **ZATCA sections 2 and 3** — not yet crawled.
3. **MC** — blocked; ask the partner whether detail pages load for her.
4. **SAMA** — 237 truncated cells with no sidecar.
5. **MHRSD** — metadata present on only 19 of 62 rows.
6. **SDAIA / CMA** — row-count changes vs. previous runs unexplained; CMA has a
   two-spellings issue.
