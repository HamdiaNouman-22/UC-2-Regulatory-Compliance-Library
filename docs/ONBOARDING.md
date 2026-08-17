# Onboarding a regulator, and how the flow works now

Written 2026-08-16, after the fixes in
[fingerprint_fix_2026-08-16.md](fingerprint_fix_2026-08-16.md). Read that one for
*why* several of the rules below exist; this one is *what to do*.

---

## 1. The flow in one picture

```
   a website
       |
       |  CRAWLER            config/sources/<name>.yml   (a site with many pages)
       |                     dynamic_crawler/hints/<name>.yml   (one page, a form)
       v
   RegulatoryDocument objects        <- every one carries a content_hash
       |
       |  ORCHESTRATOR       dynamic_crawler/formfill/orch.py
       |                     classify: new / modified / unchanged / disappeared
       |                     version what changed, build the folder tree
       v
   +---------------------+          +--------------------------------+
   |  WORKBOOK (new)     |          |  DIRECT TO MSSQL (trusted)     |
   |  tools/workbook.py  |          |  jobs/monitor_jobs.py          |
   |  export -> check    |          |  scheduled, no human in the    |
   |        -> promote   |          |  middle                        |
   +---------------------+          +--------------------------------+
                    \                        /
                     v                      v
                        regulations  (status = '')
                                |
                                |  A PERSON sets active / reject
                                v
                         the live library
```

Two ways in, one destination. Which one you use depends only on whether the
source has earned trust yet.

**New regulator -> workbook.** Nobody has ever read this crawler's output. Look
at it before it reaches the library.

**Established regulator -> direct.** It has a baseline, monitoring watches it,
and `status = ''` means the review still happens — just after the write instead
of before it.

`status` is **never** written by any code on either path. It is the human
decision, and the queue of things nobody has judged is exactly:

```sql
SELECT * FROM regulations WHERE status = ''
```

---

## 1b. Build your workbook and send it

Run these three commands, then send the file. You need **no database access and
no `.env`** — none of this opens a connection.

**1. Crawl into a workbook**

```
python -m tools.workbook export <name> [--form]
```

Writes `output/workbooks/<name>.xlsx`.

**2. Check it**

```
python -m tools.workbook check output/workbooks/<name>.xlsx
```

It must print `"verdict": "OK"`. If it reports errors, they are crawler bugs —
fix the crawler and export again. Do not edit the workbook by hand to make an
error go away: the next crawl reproduces it, and the library ends up disagreeing
with the site.

Open the workbook and read it too. `check` catches shape, not sense — only a
person can see that a title is a cookie banner or a folder is upside down.

**3. Export a second time**

```
python -m tools.workbook export <name> [--form]
```

Everything should come back `unchanged`, with zero new version rows. A first run
looks identical whether it is right or wrong, so this is the only step that shows
your crawler is stable. If the second run still says `modified`, stop — usually a
`content_hash` that is not the same twice (see Step 2).

**4. Send it**

> Send the `.xlsx` **and** the `.fulltext.json` if one exists. Both are in
> `output/workbooks/` under the same name.

Excel cells stop at 32,767 characters, so anything longer is parked in that
sidecar with only a preview left in the cell. Without it the long documents get
stored truncated — a 92,995 character instrument arriving as 32,028, its text
stopping mid-article, with nothing to say so. `check` refuses a workbook whose
sidecar is missing, so run it and you cannot get this wrong. Most crawls produce
no sidecar at all; if there is no `.fulltext.json`, nothing overflowed and the
`.xlsx` alone is right.

In your message, say **which regulator, how many documents, and what the second
export reported.**

That is the whole job. The workbook is loaded into the database on the other
side, and every row arrives unapproved until a person judges it.

---

## 2. Onboarding a new regulator

### Step 1 — decide which kind of crawler

| the site is... | build | example |
|---|---|---|
| one page, a table or list, maybe paginated | a **form** in `dynamic_crawler/hints/<name>.yml` | `zatca.taxes.yml` |
| many pages to walk, a tree, several sections | a **source config** in `config/sources/<name>.yml` | `moh.yml` |
| behind a login, a JS app, or actively hostile | a **wrapper** in `crawler/` | `crawler/simah_wrapper.py` |

Most regulators are a form, and several forms if they publish in sections — ZATCA
has five. **Prefer several small forms over one large one.** Each gets its own
baseline and its own change signal, so one section breaking is visible instead of
being absorbed into a bigger number.

### Step 2 — write it, and give every document a fingerprint

**This is the step that has gone wrong most often.** `content_hash` is how change
detection works. A document without one is classified `modified` on *every* run,
forever, writing a version row each time.

Formfill and the generic crawler already set it. A hand-written crawler must:

```python
from crawler.fingerprint import stamp_content_hashes

def fetch_documents(self):
    ...
    return stamp_content_hashes(docs)      # at the SINGLE exit, not per branch
```

Stamp at the one exit every document leaves through. Stamping per construction
site is how the gap appeared in the first place — someone adds a fifth
`RegulatoryDocument(...)` branch and forgets.

What to hash, in order of preference:

1. the page's **visible text** — never its HTML, which churns on every CMS deploy
2. a **publisher's own change stamp** if one exists (MOH uses SharePoint's
   `Modified`; it moves when a PDF is replaced, which `url|title` cannot)
3. `document_url | title` — weak but honest, the fallback

Never hash anything that varies per run: a timestamp, a session id, a row
position. A hash that changes on its own is worse than no hash.

### Step 3 — name things the way the library already names them

* **Regulator**: `Full Name (ACRONYM)` — `Anti-Money Laundering Permanent
  Committee (AML)`. Config lookups match on this string and fall back to defaults
  silently when it does not match, so a near-miss is not an error, it is a wrong
  answer.
* **doc_path**: `[regulator, source_system, ...folders, title]`. The last crumb
  is the document itself.
* The **folder a document sits in is what scopes `disappeared`**, so put each
  form's documents under their own folder. Two forms sharing a folder will
  propose each other's documents as withdrawn.

### Step 4 — export to a workbook and read it

```
python -m tools.workbook export <name> [--form]
```

Opens no database connection. Writes `output/workbooks/<name>.xlsx`.

### Step 5 — check it

```
python -m tools.workbook check output/workbooks/<name>.xlsx
```

Reads the file and nothing else. Errors mean `promote` would give you a library
you did not intend:

| it says | it means |
|---|---|
| rows share one identity | they overwrite each other on insert — the workbook says 40, the library gains 39 |
| every identity field blank | they all match each other |
| missing title / regulator / source_system / doc_path | the row cannot be placed |
| non-empty `status` | a machine is forging a human decision |
| N files AND a document_url | multi-file rows must leave `document_url` empty, or identity depends on which file the site listed first |
| sidecar missing | the `.fulltext.json` did not travel with the `.xlsx`; long text would be promoted truncated |
| *warning:* no content_hash | it will land, then re-version itself on every run |

Open the workbook too. `check` catches shape, not sense — only you can see that a
title is a cookie banner or a folder is upside down.

### Step 6 — promote

Run by whoever holds the database credentials. If that is not you, your job ends
at Step 5 — send the workbook on (see §1b) and skip to Step 7.

```
python -m tools.workbook promote output/workbooks/<name>.xlsx            # dry run
python -m tools.workbook promote output/workbooks/<name>.xlsx --apply    # write
```

`promote` re-runs `check` first and refuses a workbook that fails it. The dry run
opens a **read-only** connection, so `skipped_already_present` is a real number —
it says how much of this workbook the library already holds. Promoting is
idempotent: run it twice and the second inserts nothing.

### Step 7 — run it twice

**One run is never a result.** Run 1 of anything new reports everything as `new`
or `modified`, before and after a bug. Only run 2 tells you whether the write
stuck:

```
python -m tools.workbook export <name>     # again, into the same workbook path
```

Expect `unchanged` for everything and **zero** new version rows. Anything else is
a real finding — most likely a fingerprint that is not stable between runs.

### Step 8 — give it a change signal

Add an entry to `config/change_signals.yml`:

```yaml
- regulator: "Full Name (ACRONYM)"
  source_system: "..."
  signal: stored-inventory        # or sitemap / snapshot-articles / sama-feed / crawl
  confirm: false                  # true if the site's version token lies
```

Then add it to the right list in `jobs/monitor_jobs.py` — `CHEAP_PROBE_SOURCES`
if its site answers a probe honestly, `CRAWL_AS_SIGNAL` if the crawl *is* the
signal.

Measure before choosing `confirm`. CMA's `Last-Modified` is the current time, so
it reported 1,134 false changes until `confirm: true` made it hash the visible
text instead.

---

## 3. How monitoring works

The point is **not to crawl**. Ask a cheap question first, crawl only what moved.

```
   SWEEP  (one request per document, ~a minute for six sources)
     |
     |  did the version token move?
     |
     +-- no  -> done. This is the normal case and it costs almost nothing.
     |
     +-- yes -> CRAWL just those urls -> classify -> version -> MSSQL
```

**The signals**, and when each is right:

| signal | when | example |
|---|---|---|
| `stored-inventory` | the site returns an honest version token per document | ZATCA, AML, MOE |
| `sitemap` | it publishes a sitemap with real lastmod dates | |
| `snapshot-articles` | one page holds many articles | |
| `sama-feed` | the regulator publishes its own "what changed" page | SAMA |
| `crawl` | no cheap question is possible; the crawl *is* the signal | MC, CMA |

`sama-feed` is the best case and worth looking for on any new regulator **before**
building a probe: one request replaces 6,101, and it also **discovers documents
we do not hold**, which a probe structurally cannot.

### The two safety gates

**The completeness gate.** Compares this run's count against the last good run's.
A big drop distrusts the run, and an untrustworthy run may not withdraw anything.
Keyed **per form** (`ZATCA/zatca.ie_guidelines`) — before 2026-08-16 it was per
regulator, so ZATCA's five forms overwrote each other's baseline and every run
was quarantined.

**The withdrawal rule.** A document absent from **two consecutive trustworthy
runs spanning 20 hours** is *proposed* for withdrawal. Proposed, not withdrawn:

> nothing is withdrawn by this report. Open each proposed document at its stored
> url first: a url that 404s is a library problem, not a withdrawal.

Nothing automated ever writes `status`.

### Blocked sites are not retried by machines

`saudiexchange.sa` (Akamai) and `simah.com` (Cloudflare) are blocked, and **both
blocks were caused by automated access from this address**. They are in
`skip_hosts` with an `until` date, and that date is when a **person** may retest
by hand. It is a review date, not an expiry. A scheduled retry is not a way out
of a block — it is what makes one.

---

## 4. Which files you touch

**Adding a regulator**

| file | why |
|---|---|
| `dynamic_crawler/hints/<name>.yml` | a form |
| `config/sources/<name>.yml` | a multi-page site |
| `crawler/<name>_wrapper.py` | only if fetch policy needs code |
| `config/change_signals.yml` | its monitoring signal |
| `jobs/monitor_jobs.py` | add to `CHEAP_PROBE_SOURCES` or `CRAWL_AS_SIGNAL` |
| `config/scheduler.yml` | when it runs |

**The machinery — read, rarely edited**

| file | what it owns |
|---|---|
| `tools/workbook.py` | export / check / promote |
| `dynamic_crawler/formfill/orch.py` | classify, version, folder tree, both gates |
| `dynamic_crawler/changesignal.py` | identity — the single definition |
| `crawler/fingerprint.py` | what a `content_hash` is |
| `dynamic_crawler/formfill/promote.py` | workbook -> MSSQL |
| `dynamic_crawler/formfill/excel_repo.py` | the workbook, same contract as MSSQL |
| `storage/mssql_repo.py` | the database |
| `jobs/monitor_jobs.py` | the four scheduled jobs |
| `dynamic_crawler/inventory_sweep.py` | the cheap probe |

**Two definitions that must never be copied**

`changesignal.DEFAULT_IDENTITY` and `crawler/fingerprint.py`. Both have already
been duplicated once and both copies drifted. On 2026-08-16 the identity tuple
existed in two places, gained `title` in only one, and the sweep and the
orchestrator built different keys for a whole day — so no withdrawal could ever
accumulate, silently.

If you need a default in a second place, take `None` and fall through to the one
definition. **A default written twice is a default that disagrees with itself.**

---

## 5. Checklist

```
[ ] crawler written (form, source config, or wrapper)
[ ] every document has a content_hash, stamped at the single exit
[ ] regulator named "Full Name (ACRONYM)"
[ ] each form's documents in their own folder
[ ] export -> workbook
[ ] check  -> "verdict": "OK"
[ ] opened the workbook and read it
[ ] exported a SECOND time: everything unchanged, zero new version rows
[ ] sent the .xlsx AND its .fulltext.json (if one exists)
[ ] entry in config/change_signals.yml
[ ] listed in jobs/monitor_jobs.py
[ ] ran the signal twice: second run reports zero false changes
[ ] someone knows to work the `status = ''` queue
```

The two steps people skip are the second export and the second sweep. They are
the only ones that can tell you the fix worked, because **run 1 looks identical
whether it is right or wrong.**
