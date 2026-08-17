# The full pipeline, and how the generic crawler plugs into it

A walkthrough of what actually runs, component by component, in the order things
happen. Each section ends with **"connecting the generic crawler"** — what
changes in that component, and what does not.

Written as we go. Components covered so far: **scheduler**.

---

## 1. The scheduler — what starts everything

`scheduler/scheduler.py` · config in `config/scheduler.yml`

Nothing in this system runs itself. A long-lived process sits there watching the
clock, and at the configured time it kicks off one regulator's pipeline. That
process is the scheduler.

```bash
venv/Scripts/python.exe scheduler/scheduler.py
```

It uses APScheduler's `BackgroundScheduler`, then sleeps in a loop forever. Stop
it with Ctrl+C. If this process is not running, **nothing is scheduled** — the
API still works, but only when a human clicks it.

### What it reads

`config/scheduler.yml`. One block per job:

```yaml
jobs:
  sbp_pipeline:
    enabled: true
    trigger: cron
    schedule:
      hour: 2
      minute: 0
```

`enabled: false` skips the job entirely. Times are in `TIMEZONE` from `.env`,
default `Asia/Karachi`.

### What is scheduled today

| job | time | enabled |
|---|---|---|
| `sbp_pipeline` | 02:00 | yes |
| `secp_pipeline` | 03:00 | yes |
| `sama_pipeline` | **10:46** | yes |
| `cbb_monitoring` | 05:00 | yes |
| `full_pipeline` | 02:00 | no |

> The SAMA entry is commented "Run every day at 4:00 AM" but is set to `hour: 10,
> minute: 46`. That is a leftover from a test run, not a decision. Worth fixing
> before anyone trusts the comment.

Each job is registered with:

- `max_instances=1` — a run that overruns will not have a second copy started on
  top of it
- `misfire_grace_time=6h` — if the machine was asleep at 2am, the job still runs
  when it wakes, as long as it is within six hours
- `coalesce=False` — several missed runs fire separately rather than collapsing
  into one

### Two ways it can run a job

This is the part worth understanding, because the two modes behave very
differently. `EXECUTION_MODE` in `.env` picks one. **Default is `API`.**

```
EXECUTION_MODE=API      scheduler --HTTP--> pipeline_api --> orchestrator
EXECUTION_MODE=DIRECT   scheduler ------------------------> orchestrator
```

**API mode** — the scheduler just makes an HTTP call to `pipeline_api`, e.g.
`POST /trigger/SBP`. The work happens inside the API process. The scheduler is
only a clock; it does not import crawlers or touch the database. If the API is
down, the job fails immediately and loudly.

**DIRECT mode** — the scheduler imports and runs the pipeline itself. No API
needed. And here the four jobs are not even consistent with each other:

| job | how DIRECT mode runs it |
|---|---|
| `sbp_pipeline` | builds `Orchestrator(SBPCrawler())` **in this process** |
| `secp_pipeline` | spawns a **subprocess** — `jobs/secp_job.py` |
| `sama_pipeline` | spawns a **subprocess** — `jobs/sama_job.py` |
| `cbb_monitoring` | builds the orchestrator with `crawler=None` and calls `run_for_cbb(...)` |

Two of them run in-process and two shell out. The subprocess ones exist because
those crawlers use Selenium/Playwright and do not survive being run inside a
long-lived process. That difference is not documented anywhere in the code — it
is just how they were each written.

`full_pipeline` only exists in API mode, because it means "one call that
triggers every regulator", which is an API concept.

---

## 1b. Where the schedules actually live (deployment)

Short answer: **nowhere permanent, and in two different places at once.**

There are **two independent schedulers** in this codebase. They do not know about
each other.

### Scheduler A — `scheduler/scheduler.py`

```python
scheduler = BackgroundScheduler(timezone=TIMEZONE)
```

No jobstore argument, so APScheduler uses its default **in-memory** store. That
means:

- the jobs exist only inside that one running process
- **stop the process and every schedule is gone**
- start it again and it rebuilds them by re-reading `config/scheduler.yml`

So `scheduler.yml` is the real source of truth, and the schedule is only "live"
for as long as that process is alive. Nothing is registered with the operating
system — there is no cron entry, no Windows Task Scheduler task, no systemd
timer. **If nobody keeps this process running, nothing ever runs.**

### Scheduler B — inside `apis/pipeline_api.py`

The API starts its own scheduler when it boots:

```python
@app.on_event("startup")
def start_scheduler():
    Thread(target=scheduler_loop, daemon=True).start()
```

`scheduler_loop` polls a **database table** every 30 seconds:

```sql
SELECT TOP 1 id, regulator FROM pipeline_schedule
WHERE scheduled_time <= ? AND status = 'PENDING'
```

and `POST /schedule?regulator=SBP&hour=2&minute=0` MERGEs a row into it. So these
schedules live in **SQL Server**, in `pipeline_schedule`, and they do survive a
restart.

### Three things to know before deploying

**1. Both can fire.** If the standalone scheduler is running with `scheduler.yml`
saying SBP at 02:00, and a row in `pipeline_schedule` also says SBP, you get two
runs. Nothing coordinates them. Pick one and turn the other off.

**2. Scheduler B runs each schedule ONCE, not daily.** The loop sets
`status='RUNNING'`, then `status='DONE'`. Nothing ever sets it back to
`'PENDING'` except another `POST /schedule`. So a row scheduled for 02:00 fires
on the next tick after 02:00, and then never again. It is a one-shot queue that
looks like a daily schedule.

**3. `POST /update-schedule` does nothing.** It validates the regulator name and
returns `{"status": "success", ...}` without writing anything anywhere. A caller
gets a success response and no schedule changes.

### Two ways to fix it when you deploy

**Option A — keep the program alive.**
Set the server up so it automatically restarts `scheduler.py` whenever it stops:
a Windows Service, `Restart=always` on Linux, or a Docker container set to
restart. The schedules stay in `scheduler.yml`.

**Option B — let the server do the scheduling.**
Delete the scheduler program. Instead create a Windows Scheduled Task (or cron
job) that says "every day at 2am, call `POST /trigger/SBP`". The operating system
remembers it, it survives reboots, and you can see it in the task list.

**Option B is simpler** — fewer moving parts, and when something did not run
there is one obvious place to look.

### Which of this is server work, and which is code

The scheduling itself is **server setup, done once at deploy time**. Neither
option needs a code change.

What *does* need code, either way:

| | |
|---|---|
| two schedulers can both fire | someone has to pick one and switch the other off |
| `pipeline_schedule` fires once, not daily | a real bug — only matters if you keep Scheduler B |
| `POST /update-schedule` does nothing | fix it or delete it; right now it lies to its caller |
| nothing points at the generic crawler | see below |

Pick A or B first, because it decides how much of that is worth doing. Under
Option B, `scheduler.py` and Scheduler B both become dead code and the first
three items disappear with them.

---

### Connecting the generic crawler

**What does not change:** the scheduler is a clock. It does not know what a
crawler is. It calls a function, or it calls a URL.

**What changes:** what those functions point at. Today:

```python
DIRECT_JOB_MAPPING = {
    "sbp_pipeline":   run_sbp_pipeline,     # hardcoded SBPCrawler()
    "secp_pipeline":  run_secp_pipeline,    # hardcoded jobs/secp_job.py
    "sama_pipeline":  run_sama_pipeline,    # hardcoded jobs/sama_job.py
    "cbb_monitoring": run_cbb_monitoring,   # hardcoded run_for_cbb
}
```

Every entry names one regulator and one crawler, in python. Adding a regulator
means writing a new function and a new job file.

With the generic crawler there is already one runner that reads which crawler to
build from a config file — `jobs/run_regulator.py`, driven by
`config/sources/<regulator>.yml`. So the mapping collapses to one line per
regulator, and the python stops changing:

```python
def run_source(regulator):
    def job():
        subprocess.run([sys.executable, "jobs/run_regulator.py", regulator],
                       check=True, env=env)
    return job

DIRECT_JOB_MAPPING = {
    "misa_pipeline": run_source("MISA"),
    "sama_pipeline": run_source("SAMA"),
    ...
}
```

Subprocess for the same reason SECP and SAMA already are: the generic engine
drives Playwright, and Playwright's sync API cannot run inside a long-lived
process that already has an event loop.

**Adding a regulator then means: one YAML file in `config/sources/`, one block
in `scheduler.yml`. No python.** That is the whole point of the change at this
layer.

**What is NOT wired yet:** nothing schedules the new orchestrator. `run_regulator.py`
exists and works, but no job in `scheduler.yml` points at it, so today every
generic-crawler run is started by hand.

---

## 2. The orchestrator — what happens to one document

`orchestrator/orchestrator.py` (the one running today)
`dynamic_crawler/formfill/orch.py` (`NewOrchestrator`, the replacement)

The scheduler decides *when*. The orchestrator decides *what happens*. It is the
only thing that talks to the crawler, the downloader, the LLM and the database,
and it is where a document turns into rows in the library.

### The one line that matters

```python
docs = self.crawler.fetch_documents()      # -> List[RegulatoryDocument]
```

That is the entire contract between crawling and the pipeline. Anything that can
answer that call can be plugged in — the old hand-written crawlers, the generic
link walker, a formfill form. Everything after this line is the same for every
regulator.

### The old flow, step by step

```
run_for_regulator("SBP")
  |
  1. docs = crawler.fetch_documents()          get everything the site has
  2. new, existing = filter_new_documents()    which have we not seen?
  3. _process_docs(new)                        4 at a time, on a thread pool
       |
       for each document:
       4. _insert_regulation()                 a row in `regulations`
       5. _extract_and_analyze()
            5a. extract_text_content_unified() find the text (see below)
            5b. _run_llm_analysis()            4 LLM stages -> compliance_analysis
            5c. _run_requirement_matching()    match obligations -> requirement_mappings
```

**Step 2 throws away `existing`.** Documents already in the database are dropped
and never looked at again. That is the single biggest limitation of the old
flow: **a regulator can edit a document and the pipeline will never notice.**

### Finding the text (step 5a)

A document is useless to the LLM until it is plain text, and where that text
comes from varies. `extract_text_content_unified` tries in order and stops at the
first thing over 200 characters:

| tier | source |
|---|---|
| CBB only | the active row in `regulation_versions` |
| 1a | `extra_meta["org_pdf_text"]` — text the crawler already extracted |
| 1b | `extra_meta["content_text"]` — HTML text the crawler already extracted |
| 2 | `doc.document_html` — HTML the crawler stored |
| 3 | download and OCR: `org_pdf_link`, then the document URL if it ends `.pdf`, then `arabic_pdf_link`, then `urdu_url`, then finally fetch the page HTML |

Tiers 1 and 2 are free — the crawler already did the work. Tier 3 costs a
download and possibly OCR. **A good crawler keeps you in tier 1**, which is why
the generic crawler stores page text and `org_pdf_link` as it goes.

If every tier comes up short, the document is logged and skipped. It still exists
as a row in `regulations`; it just has no analysis.

### Where CBB is different

`_process_single_doc` has one fork in it:

```python
if regulator_upper == "CBB":
    self._process_cbb_doc(doc)
    return
```

CBB is the only regulator that gets **versioning**: when a document changes, the
old content is snapshotted into `regulation_versions`, the old analysis is
archived, and new analysis is written against the new version. Every other
regulator just gets an insert.

`_process_cbb_doc` is not actually CBB-specific in its logic — it reads
everything it needs from `extra_meta`:

```python
monitoring_status = extra_meta.get("monitoring_status", "new")   # new | modified
existing_reg_id   = extra_meta.get("existing_regulation_id")
content_hash      = extra_meta.get("content_hash", "")
```

So the versioning machinery is already general. It is only the `if` above that
keeps it for one regulator.

### What NewOrchestrator changes

`dynamic_crawler/formfill/orch.py` is a **subclass**, not a rewrite. Everything
not listed below is inherited unchanged.

**1. One door.** `run_for_regulator` handles every regulator including CBB. The
`if regulator == "CBB"` fork is gone, and versioning applies to everyone.

**2. Four outcomes instead of two.** `classify_documents()` replaces
`filter_new_documents()`:

| | old | new |
|---|---|---|
| never seen | `new` → processed | `new` → processed |
| seen, content changed | `existing` → **dropped** | `modified` → **processed and versioned** |
| seen, content identical | `existing` → dropped | `unchanged` → skipped, costs nothing |
| in the library, not on the site | *no concept* | `disappeared` |

And it uses **one** identity — `(document_url, doc_path)`, with `reference_no` as
a tiebreak when a regulator republishes at a new URL — instead of the old
version's five different rules depending on the regulator.

It also fills in the two keys the crawler cannot know, because they need a
database lookup:

```python
extra_meta["monitoring_status"]      = "new" | "modified"
extra_meta["existing_regulation_id"] = <id>
```

That is deliberate: crawlers stay database-free, because they run as subprocesses.

**3. The completeness gate.** Before anything is marked `disappeared`, the run
itself has to be trustworthy: no bot-protection pages, no early stop, not capped,
and the document count within tolerance of the last good run.

This exists because SDAIA returned **415, then 363, then 439 documents on three
runs of identical code.** A run that "loses" 52 documents is not a run where 52
documents were withdrawn. Without this gate, monitoring would report deletions
every time a site was slow.

**4. The old filter silently dropped documents.** `filter_new_documents` ends
with:

```python
logger.warning(f"Skipping {doc.title} (missing published_date)")
```

A document with no published date and no URL match is discarded — not stored, not
reported. `classify_documents` has no such branch. **Expect the new pipeline to
find more documents than the old one**, and look at the difference rather than
assuming either side is right.

### Connecting the generic crawler

Nothing here needs to change. The orchestrator asks for
`fetch_documents()` and the generic crawler answers it —
`crawler/generic_crawler_wrapper.py` does the `pages.json` →
`RegulatoryDocument` mapping, and `build_regulator_crawler()` composes a
regulator's sources into one crawler.

What you actually swap is which orchestrator class gets built:

```python
# today
Orchestrator(crawler=crawler, repo=repo, downloader=Downloader(), ocr_engine=...)

# with monitoring
NewOrchestrator(crawler=crawler, repo=repo, downloader=Downloader(),
                source_name="SAMA", analyse=True)
```

Same crawler either way. The difference is whether you get two outcomes or four.

**What is NOT wired yet:** `apis/pipeline_api.py` still builds the old
`Orchestrator`, and `/trigger/CBB/monitoring` still calls `run_for_cbb`. So the
"one door" is real in the class and not yet real in the API.
