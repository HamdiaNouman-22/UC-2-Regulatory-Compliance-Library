# Handoff — `dynamic_crawler/formfill`

**Written:** 2026-08-02
**For:** whoever picks this up next. Assumes you have read
`generic_crawler/CRAWLING_OVERVIEW.md` and nothing else.
**Deep detail:** `dynamic_crawler/FORMFILL.md`. This document is the summary,
the decisions, and the honest list of what is unfinished.

---

## 1. What this is, in one paragraph

An LLM fills in a **small fixed form** (about twelve fields) describing how one
regulator's page is laid out — which element is a document row, which link opens
it, where the date and reference number sit, how pagination works. **Our code
does the crawling**, driven by that form. The form is proposed once, checked by a
person, verified by running it three times, approved with a name and a date, and
then committed to git and read unchanged on every run.

~2,800 lines in `dynamic_crawler/formfill/`. Touches nothing in
`generic_crawler/`.

---

## 2. Read this before you conclude we replaced the generic crawler

**We did not.** `generic_crawler` is still the first thing to try on any new
site, and for most sites it should stay the answer: paste a URL, pick a scope,
done — no form, no config, no per-site anything.

What actually happened is that we added a **second, optional layer** for the
cases where the generic walk under-delivers. If you take one thing from this
document, take the tool-choice table in §8.

The honest one-line version: *the generic crawler answers "what documents are
here?" very cheaply; a form answers "what documents are here, with their dates
and reference numbers and exact place in the site's hierarchy, reproducibly, and
signed off by a person" — and costs about ten minutes per site.*

---

## 3. Why we built it — the four things that pushed us

### 3.1 The LLM-writes-the-crawler attempt had already failed

The evidence is still in this repo:

```
dynamic_crawler/auto/generated/SAMA/anthropic_claude_sonnet_4_5/adapter.v1.py … v7.py   ← 7 attempts
dynamic_crawler/auto/generated/SAMA/deepseek_deepseek_v3_2/adapter.v1.py      … v4.py   ← 4 attempts
```

> **Note:** `dynamic_crawler/auto/generated/` is git-ignored by design — that
> folder's own `.gitignore` quarantines untrusted LLM output. The v1…v7 files
> exist locally on the machine that ran them, not in a fresh clone.

Seven attempts at one regulator. The problem was never that the model could not
write Python. **Nobody could tell whether the Python it wrote was right.** So the
change is not "use an LLM" — we tried that — it is *shrink what the LLM produces
until a person can check it in thirty seconds.*

| | LLM writes the crawler | **LLM fills the form** |
|---|---|---|
| Size to review | ~300 lines | ~12 fields |
| Checkable in 30 seconds? | no | yes |
| A wrong answer does what? | executes | matches nothing; the gate catches it |
| Needs a sandbox? | yes | no — it is data, not code |

### 3.2 Structured metadata a link walk cannot recover

`crawler/generic_crawler_wrapper.py` says it plainly in its own docstring:
*"`published_date` is left None: a link walk cannot reliably read issue dates."*

A listing row carries the fields the detail page never repeats. Measured across
all 4,160 SBP circulars through the pipeline adapter:

```
published_date  99%      reference_no  95%      department  90%
```

Those are the fields compliance work actually needs.

### 3.3 There was no gate

`generic_crawler` has `calibrate_scope.py` and `baseline.py`, but nothing that
says *"this source is verified and a named person approved it."* A form has:
three runs compared for stability, fill-rate thresholds, a recorded human
approval, and a hash so the approval goes stale if anyone edits the form
afterwards.

### 3.4 SBP was capped, not measured

The generic BFS returned 101 SBP circulars because it hit a page cap. The real
number is **4,160**. A form walks the listing properly — 139 pages, 32 minutes —
and that inventory *is* the change feed: phase 1 alone tells you what is new, so
the expensive phase 2 only runs for genuinely new rows.

---

## 4. How it works — the five files

| file | role |
|---|---|
| `formfill/schema.py` | The form, and every check that needs no network. Pure data. |
| `formfill/inspect.py` | Renders the page with Playwright, emits a ~3 KB structural digest. |
| `formfill/propose.py` | **The only file that talks to an LLM.** |
| `formfill/runner.py` | The crawl, driven by the form. Imports no LLM code, ever. |
| `formfill/verify.py` | Runs it N times, judges the spread, gates approval. |
| `formfill/pipeline.py` | `FormfillCrawler` → `List[RegulatoryDocument]` for the orchestrator. |

Two design rules that must not be broken:

1. **`runner.py` must never import `propose.py`.** A model asked afresh each run
   can change its mind; change detection would then report hundreds of documents
   as "disappeared". The form is frozen data on disk.
2. **A new form field must be nameable without naming a regulator.** `preceding
   heading` yes; `skip MISA's mobile duplicates` no — that one was solved by
   scoping the row selector *in the form*. If a fix can go in the form, it must.

### The workflow

```bash
venv/Scripts/python.exe -m dynamic_crawler.formfill inspect "<url>" --name x.y   # no LLM, no crawl
venv/Scripts/python.exe -m dynamic_crawler.formfill propose --name x.y --url "<url>" --how "..."
venv/Scripts/python.exe -m dynamic_crawler.formfill show    dynamic_crawler/hints/x.y.yml
venv/Scripts/python.exe -m dynamic_crawler.formfill refine  dynamic_crawler/hints/x.y.yml --feedback "..."
venv/Scripts/python.exe -m dynamic_crawler.formfill verify  dynamic_crawler/hints/x.y.yml --runs 3
venv/Scripts/python.exe -m dynamic_crawler.formfill approve dynamic_crawler/hints/x.y.yml --by "name"
venv/Scripts/python.exe -m dynamic_crawler.formfill run     dynamic_crawler/hints/x.y.yml
```

**Always `inspect` first.** On an easy site the digest makes the answer obvious
and you write the form by hand in a minute — no model involved. Reach for
`propose` when the digest is ambiguous. Five of the six forms below were
hand-written from the digest.

---

## 5. Where each site stands

| form | rows | verdict | approved | note |
|---|---|---|---|---|
| `sbp.circulars` | **4,160** | — | ✗ | Full phase-1 inventory done, 139 pages, 32.5 min. Needs 3× full verify (~1.5 h) to approve. |
| `misa.laws` | 89 | PASS | ✓ (pre-hash) | Full 3-level section paths. |
| `aml.rules` | 11 | PASS | ✓ (pre-hash) | SharePoint; 2 heading groups. |
| `sama.sandbox` | 40 | PASS | ✓ | **URL set matches `generic_crawler`'s baseline exactly, 40/40.** |
| `sdaia.regs` | 36 | PASS | ✓ | One row per PDF; 6 cards carry several files. |
| `simah.rules` | 2 | — | ✗ | **BLOCKED by Cloudflare.** See §6. |
| `mhrsd.regs` | **63** | PASS | ✗ | First `mode: click` pager. 4 pages (18/18/18/9), matches a manual count. `published_date` 100%. Awaiting a reviewer's approval. |
| `gosi.social_insurance` | **6** | PASS | ✗ | First `panels`. Live 3× verify **6/6/6, 0% spread**, phase 2 included: 6 instruments (366,269 chars) + 6 PDFs. Awaiting a reviewer's approval. |
| `gosi.saned` | **2** | PASS | ✗ | Live 3× verify **2/2/2, 0% spread**, 41,874 chars, no attached PDFs. Awaiting a reviewer's approval. |

"pre-hash" means approved before form hashing existed; it clears next time each
is verified.

---

## 6. Open items, in the order I would do them

1. **SIMAH is blocked by Cloudflare — and "slower pacing" was the wrong theory.**
   Evidence (2026-08-04): the stored block page is a **1020-class firewall block**
   ("Sorry, you have been blocked"), not a challenge, so there is nothing to solve
   by rendering harder. And a full run of `simah.rules` is **two loads of one URL**
   — volume never tripped it, **iteration did** (every selector fix, every
   `verify --runs 3`). The blocked IP was residential (TELUS, BC), so it is not a
   hosting-ASN ban either.

   What now exists (§12): snapshots, so all development is offline, and
   `crawler/simah_wrapper.py` — `mode: custom` owning the *fetch policy* only.
   **What is untested: whether a headed browser profile gets through.** That is one
   request, via `formfill snapshot`. Do not test it any other way.

   If that attempt is blocked too, no local change fixes it. The routes are an
   allowlist request to SIMAH (their block page asks for exactly that), or SAMA's
   rulebook, which publishes both instruments in *better* shape — the Law as 17
   articles and the Implementing Regulations as **55 articles** with a reference
   number and Hijri date, rather than one opaque PDF. SAMA is the issuing regulator;
   SIMAH is the licensed bureau republishing them. See `config/sources/simah.yml`.
2. **Approve SBP.** Needs three full 139-page verify runs, ~1.5 hours. Until
   then the pipeline will refuse to run it.
3. **Re-verify MISA and AML** so their approvals carry a form hash.
4. **SBP phase 2** has never been run: 4,160 detail pages, several hours. The
   inventory alone is what change detection needs, so this is only required if
   you want each circular's HTML.
5. **`pagination.mode: click` now walks a next-button pager** (done 2026-08-03 for
   MHRSD: 63 rows / 4 pages, 63/63/63 on verify; each turn verified against a
   row-set fingerprint, so a dead control stops the walk). **Still open:** a
   page-size control ("Show N entries") — that, not a next button, is what SECP
   needs, and with it the first real test of the `table` shape.
6. **GOSI is verified live and needs only approval** (2026-08-05). Both forms
   PASS 3 live runs with phase 2 included — `gosi.social_insurance` 6/6/6 (6
   instruments, 366,269 chars, 6 PDFs) and `gosi.saned` 2/2/2 (41,874 chars) —
   and both live totals match the earlier snapshot figures to the character, which
   is two independent page loads agreeing. Run `approve --by "<name>"` after
   eyeballing the page against the inventory sheet (§7.1 step 4).

   **The five sibling law pages are OUT OF SCOPE** — `/Civil`, `/Military`,
   `/BenefitExchange`, `/InsuranceProtection`, `/Books`. This UC covers the Social
   Insurance Law and the SANED Law only; an earlier draft of this list told the
   next person to crawl the family, which would have pulled in five instruments
   nobody asked for. Do not inspect them without a scope change.
   See `UC-2-Scratch/GOSI_FINDINGS.md`.
7. **Wire `mode: formfill` into `config/sources/*.yml`** — `pipeline.py` has
   `build_formfill_source()` ready; `build_source()` in
   `crawler/generic_crawler_wrapper.py` needs one branch added. I did not touch
   that file because another session was editing it.
8. **Audit MISA's 89 for multi-file rows**, the way SDAIA turned out to have
   them. AML is checked (11 rows, 11 files). MISA is not.

---

## 7. Adding a new regulator — the flow

**Step 0 always: `inspect`.** No LLM, no crawl, no API key, ~20 seconds. What it
prints decides everything that follows, and it is the cheapest way to find out
you are about to build the wrong thing.

```bash
venv/Scripts/python.exe -m dynamic_crawler.formfill inspect "<seed url>" --name <reg>.<section>
```

Then read the digest and pick a branch:

| the digest shows | you are in | do this |
|---|---|---|
| row candidates with links + dates, or a pagination pattern | **a listing** | write the form (§7.1) |
| a tree candidate with nested nodes, no dated rows | **a tree** | write the form with `shape: tree` |
| nothing useful, but the page is clearly the document | **a single-page law** | `include_page: true` |
| nothing useful, and content is missing entirely | **login / WAF / API** | stop — `mode: custom` (§7.3) |

### 7.1 The normal case — the vocabulary already covers it (~10 minutes)

1. **Write the form by hand** from the digest. Five of our six forms were
   hand-written; it is faster than prompting. Use `propose --how "..."` only when
   the digest is genuinely ambiguous.
2. **`show`** it. Twelve fields. If it does not read like the site, fix it now.
3. **`verify --runs 3`.** Exits 1 on FAIL. Read `verify_report.md`.
4. **Open the real page next to the inventory sheet.** Do not skip this. It is
   the only step that catches "stable, 100% filled, and the wrong question" —
   SDAIA passed 29/29/29 while dropping 7 documents.
5. **`approve --by "<your name>"`**, then commit the `.yml`.
6. **Add the source** to `config/sources/<regulator>.yml`:
   ```yaml
   - name: "Circulars"
     mode: formfill
     hints: dynamic_crawler/hints/sbp.circulars.yml
     source_system: "SBP-CIRCULARS"
   ```
7. **`jobs/run_regulator.py <REG> --dry-run`** to see the `RegulatoryDocument`
   objects without writing anything.

### 7.2 The form cannot express what the site needs

You are adding a **word to the vocabulary**. Before you do, apply the rule in §4:
*a new word must be nameable without naming the regulator.* Then ask whether a
second site would use it. If the answer is no, it belongs in `mode: custom`, not
in the form — otherwise the form grows to eighty fields, nobody can review one in
thirty seconds, and the entire argument for this approach is gone.

Adding a word is: one entry in `schema.py`, the behaviour in `runner.py`, and a
hint in `inspect.py` so the next person can see the option exists. Half a day.
Everything already built came in this way — section paths, preceding headings,
the tree shape, `include_page`, `expand_selector`.

### 7.3 The site needs code

Login, CAPTCHA, a WAF, POST-based search, a private API. Write a crawler class
and register it — the pipeline cannot tell the difference:

```yaml
  - name: "Whatever"
    mode: custom
    crawler_class: crawler.x_wrapper.XCrawler
```

Deciding this early is a feature, not a defeat. Twenty seconds of `inspect`
told us SIMAH was behind Cloudflare and SAMA's sandbox was a tree, before
either cost us a day.

### 7.4 Rough costs

| case | cost |
|---|---|
| Existing vocabulary | ~10 minutes, plus verify time |
| Verify a big site | 3 × one full crawl (SBP: ~1.5 hours) |
| New word in the vocabulary | half a day, then free for every later site |
| `mode: custom` | as long as a hand-written crawler takes |

## 8. Which tool for which site

| | use it when |
|---|---|
| **`generic_crawler`** | **First choice.** Any new site, no config at all. Still the right answer for most. |
| **`formfill`** | The listing carries reference numbers / issue dates / departments; a paginated listing needs walking precisely; auto-detection picks the wrong menu on a tree; or you want the crawl **gated and signed off**. |
| **Hand-written crawler** (`mode: custom`) | Login, CAPTCHA, WAF, POST search forms, private APIs. Nothing forces everything through a form. |

Rough cost: a form is ~10 minutes for a site the vocabulary already covers, half
a day if it needs a new word, and the wrong tool entirely for anything in row 3.

---

## 9. Things that cost me hours — do not re-learn these

- **`innerText` returns `""` inside a hidden tab.** MISA's "Basic Legislations"
  pane silently blanked every title in it. Use `innerText || textContent`.
- **A pinned element id can stop a tree walk dead.** `#book-navigation-1` exists
  on SAMA's seed page and not on deeper ones. Use `[id^="book-navigation"]`.
- **Menu nesting is a bad source for section paths; the breadcrumb is a good
  one.** A book menu only renders the branch you are standing in, so deep nodes
  inherit almost nothing. `section_path.from_breadcrumb` fixed six-level paths
  that had bottomed out at three.
- **Keying documents on (url, section) double-counts.** SDAIA's "Save as PDF"
  link sits on every page of a section: three files became thirty-nine
  documents. Key on the URL; keep the other placements in `also_in`.
- **First-sighting-wins gave anchor text the last word on titles.** With
  `include_page`, a page links its own declared row, so the anchor lands first and
  SIMAH's PDF became "Download PDF" — the exact thing its form file warns about.
  `_add_document(declared=True)` now lets a form-declared title replace a scraped
  one, and never the reverse. A no-op for any URL with only one kind of sighting,
  which is every other form today.
- **"Enough links" is a bad page-loaded test.** SIMAH's law page has one anchor
  until the nav renders; a >15 threshold threw the whole page away as a failed
  load. Check text length too.
- **A WAF challenge page returns 200 and looks like content.** SIMAH's Cloudflare
  block was stored as 1,054 characters of "law" and passed every check.
- **Deleting `<form>` deletes a SharePoint page.** ASP.NET WebForms wraps the whole
  document in `<form id="aspnetForm">`, so `JS_DETAIL`'s cleanup step removed
  SIMAH's entire law — 8,182 characters to 0, with 444 characters of markup left
  and every other check still green. Forms are unwrapped now. `aml.rules` is
  SharePoint too and only dodged this because its rows link straight to PDFs, so
  phase 2 never ran.
- **An exclusive accordion cannot be opened all at once.** SIMAH's 17 articles
  share `data-bs-parent`, so bootstrap closes each panel as the next opens:
  `expand_selector` can never have more than one open, and rendered text sees one
  article of seventeen. `textContent` is the only fix; clicking harder is not.
- **All three of those would have ruined a perfectly successful live crawl**, and
  none were findable until a snapshot let the form run repeatedly offline. That is
  the argument for §12 in one line.
- **Do not fetch a PDF as a detail page.** It loads, has no HTML, and books as a
  failed fetch — so the file never reaches `documents` at all.
- **A new shape-ish word must be added to `verify`'s phase-2 list too.** `verify`
  runs phase 1 only, and excepts `shape: tree` and `include_page` because their
  documents are *made* in phase 2. `panels` was not added, so GOSI passed
  **6/6/6, 0% spread, fill 100%, `documents: 0`** — a green gate over a form that
  had captured nothing. Ask of every new word: does phase 1 alone prove anything?

---

## 10. The limitation to keep saying out loud

**The gate proves stability, not coverage.**

SDAIA's first form scored **29 / 29 / 29, 0% spread, 100% fill — PASS.** It was
wrong: six cards attach more than one PDF, the real total is 36, and **7
documents were being silently dropped behind a green tick.** Nothing about the
result looked broken, and no amount of re-running would ever have flagged it. A
person looking at the actual page caught it.

Three runs agreeing means the form is *deterministic*. It says nothing about
whether the number is *right*. Coverage is settled by `db_compare.py` against a
regulator already in the database, or by a total the site publishes itself —
never by eyeballing a sample, and never by a passing verify.

Every `verify_report.md` says this at the bottom on purpose. Please leave it
there.

---

## 12. Snapshots — for sites that ration what we may ask (2026-08-04)

Built for SIMAH, useful for any blocked or rate-limited site. **One function
touches the live site** (`snapshot.capture`); everything else replays a saved page.

```bash
formfill snapshot <hints.yml>            # ONE live request, headed, no retry
formfill snapshot <hints.yml> --status   # what we hold, when the next try is due
formfill run    <hints.yml> --snapshot   # replay: zero requests
formfill verify <hints.yml> --snapshot   # replay N times: zero requests
```

The clock, in `SnapshotStore`, is the part that matters — nobody decides by hand
when to poke a blocked site, because a human under deadline always decides *now*:

| | |
|---|---|
| blocked attempt | backs off 6h → 24h → 72h → 7d → 14d, and **never retries** |
| load failure (timeout/DNS) | recorded, but does **not** earn the block backoff — the site did not refuse us |
| successful capture | resets the backoff, and **diffs the content hash** — a change is the monitoring signal |
| `fresh` / `aging` / `stale` | within `max_age_days` / past it and due for refresh / past `grace_days` with every refresh blocked |
| `stale` | the crawler **raises**. A replay served forever would tell change detection "unchanged" while the law was amended |

**A snapshot is ONE page, so it replays a one-page form and nothing else.** `run`
and `verify` refuse `--snapshot` on a paginated form or a tree, because both
alternatives are worse than an error: fetching pages 2..N would generate exactly
the traffic the flag promises to avoid, and skipping them would report a fraction
of the site as the whole of it. SBP's 139 listing pages and SAMA's 40-node tree
need the network; SIMAH's single law page does not.

Two more deliberate limits:

- **A challenge page is never saved.** That is how the block became 1,054
  characters of "law" in the first place.
- **A snapshot verify cannot approve a form.** N runs against one saved file agree
  by construction; it proves the form reads *that page*, not that the site is
  stable. `approve` refuses it, and a `--force` override is stamped into the form.

`<base href>` is injected into every replay. Fields read `el.href` — the resolved
property — so without it SIMAH's relative PDF link resolves against `about:blank`
and `document_url` is quietly wrong.
