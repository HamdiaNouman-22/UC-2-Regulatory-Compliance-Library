# formfill — the LLM fills a form, our code does the crawling

**Audience:** the three of us working on KSA regulators, plus whoever picks this
up later. No prior context needed beyond `generic_crawler/CRAWLING_OVERVIEW.md`.

**Status:** built and tested end to end on four live sites — SBP (4,160
circulars), MISA (89 laws), AML (11 documents) and the SAMA Regulatory Sandbox
(40 pages, matching `generic_crawler` exactly). Covers `list`, `table` and
`tree`. Not yet wired into the orchestrator. Lives entirely in
`dynamic_crawler/formfill/` and touches nothing in `generic_crawler/`.

---

## 1. The problem this solves

Onboarding a new regulator currently needs a developer: either a hand-written
crawler, or someone who can read a page's HTML and pick selectors. We wanted to
move that work from "a dev writes code" to "someone checks a result."

The obvious way to do that is to ask an LLM to write the crawler. **We already
tried it, and the evidence is still in this repo:**

```
dynamic_crawler/auto/generated/SAMA/anthropic_claude_sonnet_4_5/adapter.v1.py … v7.py    ← 7 attempts
dynamic_crawler/auto/generated/SAMA/deepseek_deepseek_v3_2/adapter.v1.py      … v4.py    ← 4 attempts
```

> **Note:** `dynamic_crawler/auto/generated/` is git-ignored by design — that
> folder's own `.gitignore` quarantines untrusted LLM output. The v1…v7 files
> exist locally on the machine that ran them, not in a fresh clone.

Seven attempts at one regulator. The problem was never that the model couldn't
write Python. It was that **nobody could tell whether the Python it wrote was
right**, and the failure mode of a wrong crawler is silent: it returns 40
documents instead of 4,160 and looks perfectly healthy.

## 2. What changed

The LLM's job is now to fill in a **small fixed form** — about twelve fields —
that an engine we wrote already knows how to execute.

| | LLM writes the crawler | **LLM fills the form** |
|---|---|---|
| Size to review | ~300 lines of Python | ~12 fields |
| Reviewable in 30 seconds? | no | yes |
| A wrong answer does what? | executes | matches nothing, and the gate catches it |
| Needs a sandbox? | yes | no — it's data, not code |
| Safe to store and re-run? | risky | natural |

And the half of the idea that makes it tractable: **the person onboarding the
site says how to crawl it, in plain English.** Inferring *intent* from HTML is
the hard, unreliable part — "should I follow these links or are they
navigation?" has no answer in the markup. A human types:

> "go through each entry, click the title, grab that page, come back, and keep
> going through the pagination"

and the model only has to translate that into selectors it can see.

## 3. The five pieces

| file | what it does |
|---|---|
| `formfill/schema.py` | The form itself, plus every check that needs no network. Pure data. |
| `formfill/inspect.py` | Renders the page with Playwright and produces a ~3 KB structural digest. |
| `formfill/propose.py` | **The only file that talks to an LLM.** |
| `formfill/runner.py` | The crawl, driven by the form. Imports no LLM code, ever. |
| `formfill/verify.py` | Runs it N times, judges the spread, gates approval. |

### Why `inspect.py` exists when `onboarding/site_inspector.py` already "inspects"

That older one fetches with `requests` and hands the model ~45,000 characters of
raw HTML. Both halves are wrong: raw HTML is empty on JavaScript-drawn sites, and
45,000 characters buries the four facts that matter. So we render the page and
answer those four questions **mechanically** — which element repeats, which link
opens it, how pagination works, where the date sits — then send the model a small
digest of candidates.

Be honest about what that means: **that summarising step is most of the detection
work.** The LLM does the last mile, not the whole job. Which is also why an easy
site needs no LLM at all (see §5).

### Why `runner.py` must never import `propose.py`

A form is proposed once, reviewed, verified, committed — then read from disk on
every run. If we asked a model afresh each run, two runs could pick different
rows, and change detection would report hundreds of documents as *disappeared*.
Determinism is not a nicety here; it is what makes monitoring possible.

## 4. The workflow

```bash
# 1. Look at the page. No LLM, no crawl, no API key.
venv/Scripts/python.exe -m dynamic_crawler.formfill inspect "https://www.sbp.org.pk/circulars/" --name sbp.circulars

# 2. Only if the digest is ambiguous — have a model fill the form.
venv/Scripts/python.exe -m dynamic_crawler.formfill propose \
    --name sbp.circulars --url "https://www.sbp.org.pk/circulars/" \
    --how "go through each entry, click the title, grab that page, come back, and keep going through the pagination"

# 3. Read it. Twelve fields.
venv/Scripts/python.exe -m dynamic_crawler.formfill show dynamic_crawler/hints/sbp.circulars.yml

# 4. Wrong? Say so in English.
venv/Scripts/python.exe -m dynamic_crawler.formfill refine dynamic_crawler/hints/sbp.circulars.yml \
    --feedback "the date it picked is the publication date column, use the issue date instead"

# 5. THE GATE. Runs it 3 times and judges the spread. Exits 1 on FAIL.
venv/Scripts/python.exe -m dynamic_crawler.formfill verify dynamic_crawler/hints/sbp.circulars.yml --runs 3

# 6. Approve — only a human does this, and it is recorded in the file.
venv/Scripts/python.exe -m dynamic_crawler.formfill approve dynamic_crawler/hints/sbp.circulars.yml --by "your name"

# 7. Run it.
venv/Scripts/python.exe -m dynamic_crawler.formfill run dynamic_crawler/hints/sbp.circulars.yml
```

Add `--headed` to any of these to watch the browser.

Forms live in `dynamic_crawler/hints/*.yml`. Output goes to
`output/formfill/<name>/`.

## 5. What a form looks like

This is the whole thing — the SBP one, written **by hand in a minute** straight
off the inspect digest, no model involved:

```yaml
version: 1
name: sbp.circulars
seed_url: "https://www.sbp.org.pk/circulars/"
shape: list
scope: prefix

row_selector: "div.publication-box-new"
detail_link_selector: "a[href]"

pagination:
  mode: url_offset
  pattern: "https://www.sbp.org.pk/circulars//P{offset}"
  step: 30
  max_offset: 4140        # frozen — a discovered value can move between runs
  max_pages: 200

fields:
  title:          {from: css,   selector: "h4.mb-2", attr: text}
  document_url:   {from: css,   selector: "a[href]", attr: href}
  reference_no:   {from: regex, pattern: '([A-Z]{2,6}[- ]?Circular(?: Letter)? No\.?\s*\d+ of \d{4})'}
  published_date: {from: regex, pattern: '([A-Z][a-z]{2,8} \d{1,2},? \d{4})'}
  department:     {from: regex, pattern: '\|\s*([A-Z]{2,8})\s*\|'}

fetch_details: true
```

Because the digest had already printed:

```
div.publication-box-new   x30  links:30  dated:30  ref:30
pagination  .../P{n}      numbers seen: [30, 60, 4140]
```

**Reach for `propose` when the digest is ambiguous, not by default.**

Only two extraction operations exist — `css` (a selector inside the row) and
`regex` (a pattern against the row's visible text). That is deliberate: fewer
operations means less for a model to be creative with and more chance a reviewer
spots a mistake.

## 6. Section paths — putting each document where it lives

One of the two guiding goals for crawling is replicating the regulator's own
structure. A flat `Laws` for every MISA document fails that; what we want is:

```
Laws > Sectoral Legislations > Real estate sector
Laws > Basic Legislations   > Commercial Legislation
```

A listing page encodes that around the row, and pages do it in two different
ways — so the form has two kinds of level:

```yaml
section_path:
  prefix: ["Laws"]                                     # fixed crumbs (the page's own h1)
  levels:
    - {ancestor: "div.regulationContent", title: "h4"} # rows sit INSIDE a block that names itself
    - {ancestor: "div.showLawItems",      title: "h4"}
```

```yaml
section_path:
  prefix: ["Rules and Regulations"]
  levels:
    - {preceding: "h3"}      # a heading sits BEFORE unwrapped rows (SharePoint, aml.gov.sa)
```

```yaml
section_path:
  from_breadcrumb: ".breadcrumb a"     # the page states its own path — this WINS
  prefix: ["SAMA Rulebook"]            # fallback for pages with no breadcrumb
```

`ancestor` walks **up** from the row to the nearest matching block and reads a
title inside it. `preceding` walks **back** in document order to the nearest
matching heading. `from_breadcrumb` reads the trail the page itself publishes,
on the detail page, and overrides both. `formfill inspect` prints candidates for
all three, so neither you nor the model has to guess which kind of page you are
on.

**Prefer `from_breadcrumb` whenever the site has one.** Inferred structure is a
reconstruction; a breadcrumb is the site telling you the answer. On the SAMA
sandbox, menu-derived paths bottomed out at three levels and filed
`A1 Identification/Contact Details` directly under `Regulatory Sandbox`; the
breadcrumb gives all six levels:

```
SAMA Rulebook > Regulatory Sandbox > Guidance Notes on Completing the Sama
Regulatory Sandbox Application Form > Stage 1: Application Form Completion -
Initial Stage of Evaluation > A. About Your Business > A1 Identification/Contact Details
```

The separator is `" > "`, matching `generic_crawler`'s
`crawler.py::doc_section_path`, so a path from either engine reads the same
downstream.

## 7. Trees — the third shape

Tree sites are everywhere (every rulebook is one), so the form covers them too.
A tree is the same idea with the **menu standing in for the listing**: the nodes
are the rows, and the nesting is the section path.

```yaml
shape: tree
tree:
  menu_selector: "#book-navigation-1"    # WHICH menu — see the warning below
  node_selector: "li.menu-item"          # one node
  link_selector: "a[href]"               # the link inside it
  expand_selector: "…"                   # optional: click-to-open branches
  max_depth: 8
  max_nodes: 400
section_path:
  prefix: ["SAMA Rulebook", "Regulatory Sandbox"]
```

That is the whole tree form. Everything else — field extraction,
de-duplication, the output schema, the verify gate — is shared with lists.
`title` and `document_url` come from the node itself, so `fields` can be empty.

**Naming the right menu is the whole job.** The SAMA rulebook page has **13**
separate `ul.menu` elements: the book tree, the sidebar list of other books, and
the site navigation. Only `#book-navigation-1` is the one you want, and only an
id distinguishes it. `formfill inspect` now ranks tree candidates by whether the
selector is *uniquely addressable*, and prints the first few node labels so you
can tell "Introduction | Guidance Notes | …" from "Careers | Contact Us |
Sitemap" at a glance.

### The menu is re-read on every page

Drupal book menus — and most rulebook sidebars — render only the branch you are
currently standing in. The sandbox seed page shows **20** nodes; the other 20
exist only once you are on a child page. So the walk re-reads the menu on every
page it visits and the row list grows as it goes: breadth-first, not a fixed
list.

One consequence for the gate: on a list, `verify` skips phase 2 because phase 1
is what is being checked. **On a tree it cannot** — phase 2 *is* the discovery,
so skipping it would verify 20 nodes of a 40-node tree and call it stable. Trees
therefore verify in full and take proportionally longer.

### What this adds over `generic_crawler`'s `crawl_tree`

`crawl_tree` already walks trees with no config at all, and for most tree sites
it remains the first thing to try. A tree form is worth it when:

- **auto-detection picks the wrong menu** — a form pins the right one by id;
- **you want the crawl gated** — `crawl_tree` has no verify/approve step, and a
  form gets the 3-run stability check, the fill-rate check and a recorded human
  approval for free;
- **you want the section path fixed** by hand rather than inferred.

## 8. Two phases, and why they are separate

```
PHASE 1   the listing only     SBP: 139 pages, ~20 min  →  complete inventory of ~4,160 circulars
PHASE 2   open each entry      SBP: 4,160 more page loads
```

Phase 1 harvests each row's link **plus the reference number, date and department
from the row's own text** — structured metadata a link-walk can never recover.

Phase 1 alone answers "what is new since last time." **The listing is the change
feed.** So phase 2 only ever has to run for rows the inventory shows are new —
the same saving CBB gets from its Thomson Reuters feed, available on any list
site without the regulator's cooperation.

`verify` always runs phase 1 only. Phase 2 cannot be right if phase 1 found the
wrong rows.

## 9. The gate — and why "show a sample and click approve" isn't enough

The obvious dry-run gate is: run it once, show the reviewer a count and five
rows, they approve. **That does not survive our own measurements.** SDAIA
returned **415 → 363 → 439 documents across three runs of identical code.** A
reviewer looking at one count cannot tell a wrong form from a flaky site.

So `verify` runs the form N times and judges the spread:

| | rule |
|---|---|
| **FAIL** | any run found 0 rows |
| **FAIL** | run-to-run spread above tolerance (default 2%) |
| **FAIL** | `title` or `document_url` filled on under 98% of rows |
| **FAIL** | the walk was cut short by `max_pages` — a capped run measures the cap, not the site |
| **WARN** | an optional field filled on under 60% of rows |
| **WARN** | the last page was discovered from the site instead of frozen in the form |
| **WARN** | only one run — a single run cannot measure stability |

It writes `verify_report.md` (readable) and `verify.json` (machine-checkable),
exits 1 on FAIL like `generic_crawler/calibrate_scope.py`, and `approve` refuses
to stamp a FAIL unless someone passes `--force`, which is recorded in the file.

### A worked example of the gate's blind spot

SDAIA's first form used the card as the row and took the first link inside it:
**29 / 29 / 29, 0% spread, title and document_url 100%. PASS.** Nothing about
that result is inconsistent, incomplete-looking or suspicious.

It was wrong. Six of the 29 cards attach more than one PDF — "The law", "The
implementing Regulation", "Regulation on Personal Data Transfer Outside the
Kingdom" are three separate instruments on one card. The real total is **36**, so
**7 documents were being silently dropped** behind a green tick.

A human spotted it by looking at the actual page. No amount of re-running would
have: the crawl was perfectly stable at the wrong answer. That is what
"consistency is not coverage" means in practice, and it is the argument for
`db_compare.py` and for someone eyeballing the site during onboarding.

The fix needed no new vocabulary — the row became the file
(`div.file-link-row`, 36) and the card became a section level
(`{ancestor: "div.card", title: "h5.card-title"}`), so each PDF is its own
document filed under its instrument.

**Why not a list of URLs per row**, which is the tempting fix: the library's unit
is one document = one file. `RegulatoryDocument.document_url` is a single string,
each file needs its own `content_hash` for change detection, and each gets its
own DB row — so the adapter would have to explode the list back into one row per
file anyway. Making the row the file gets there directly.

### What the gate cannot tell you

**Consistency is not coverage.** Three runs agreeing on 40 documents when the
site has 4,160 is three consistent, wrong runs. A person reading a sample can
confirm the rows *are* documents with sensible titles and dates — that part is
genuinely human-checkable. Nobody can confirm from a sample that *all* the
documents are there.

Coverage is settled by `db_compare.py` against a regulator already in the
database, or against a total the site publishes itself. Never by eyeballing. The
verify report says this at the bottom of every run, on purpose.

## 10. What was actually tested (2026-08-02)

**SBP circulars** — `div.publication-box-new`, offset pagination step 30 /
max 4140 → 139 listing pages.

- **Full phase-1 inventory: 4,160 circulars, 4,160 distinct URLs, 139 pages,
  32.5 minutes, no warnings.** 1990 → 2026.
- Fill rates over all 4,160: title 100%, document_url 100%, published_date
  99.6%, department 90.9%, reference_no 89.1%.
- Departments: BPRD 1,319 · EPD 797 · DMMMD 606 · SHSFD 393 · ACFID 137.
- Small-scale check first: 2 runs × 3 pages, 90 / 90 rows, 0% spread.
- Not yet approved — approval needs 3 × full verify, ~1.5 hours.
- Data-quality notes for later: 15 rows carry no parseable date, and one date
  parses as year `0998`. `reference_no` misses ~11%, mostly forms like
  "F.E. Circular" that the pattern's `[A-Z]{2,6}` does not allow.

**MISA laws** — `div.showLawItems a.showLawDocItem`, no pagination.

- 3 full runs: **89 / 89 / 89, 0% spread. PASS.** Approved.
- Full section paths on all 89: `Laws > Sectoral Legislations > Financial
  Sector` (13), `… > Real estate sector` (6), `Laws > Basic Legislations >
  Commercial Legislation` (16), and 13 more.
- The page renders 168 anchors (a desktop copy and a hidden mobile copy of the
  same list). The row selector is scoped to the desktop copies, because only
  those sit inside a sector block; de-duplication on `document_url` then gives
  89 unique laws.
- Coverage caveat: `generic_crawler` measured 94 documents for MISA by walking
  the whole `/activities/laws/` prefix. Different question, both legitimate —
  and exactly the thing verify cannot settle.

**AML — Anti-Money Laundering Permanent Committee** (`li.dfwp-item`, SharePoint).

- 3 full runs: **11 / 11 / 11, 0% spread. PASS.** Approved.
- `Rules and Regulations > Laws and Regulations` (4) and
  `Rules and Regulations > Rules and Instructions` (7). Every row links straight
  to a PDF, so `fetch_details` is off and phase 1 alone captures the documents.

**SAMA Regulatory Sandbox** — the first `shape: tree`
(`#book-navigation-1` / `li.menu-item`).

- 3 full runs: **40 / 40 / 40, 0% spread. PASS.** Approved.
- Seed page shows **20** nodes; the breadth-first walk finished with **40**.
- **The URL set matches `generic_crawler`'s baseline exactly: 40 / 40, zero
  differences either way.** Two independently written engines agreeing on the
  same 40 pages is the strongest correctness evidence available short of
  `db_compare.py`, and it is worth more than either number alone.
- 7 documents vs the baseline's 3; all 40 pages captured text, averaging 1,431
  characters (baseline 1,274).
- Section paths like `SAMA Rulebook > Regulatory Sandbox > Regulatory Sandbox
  Framework`.
- ~2.7 minutes per run.

The document count looks low because on the SAMA rulebook **the rules are
written on the pages themselves** — the page text *is* the document. Contrast
MISA, where the page is only a list of links and all 89 documents are PDFs.

**The LLM path** — proposed a form for SBP from the digest plus the plain-English
instruction. It chose the **same** row selector and the **same** pagination as
the hand-written form, and scored **identical fill rates** on verify. Its `notes`
field said which choice it had been unsure about. One data point, not a
guarantee — but the failure mode it does have is now a form you can read, not
code you must trust.

**The failure path** — a deliberately broken `row_selector` produced FAIL on
three counts, exit code 1, and `approve` refused to stamp it.

**The feedback loop, on MISA's section paths** — worth reading as a realistic
example, because it took two rounds and the first one was wrong.

1. Feedback: *"every law just gets 'Laws'. I want Laws > Sectoral Legislations >
   Real estate sector."* The model added three levels, including two layout
   containers that are not sections, and flagged its own doubt in `notes`.
   Running it gave 55 laws as `Laws > Sectoral Legislations` and 34 as `Laws` —
   no sector name anywhere.
2. Feedback with the evidence from that run, plus the observation that the page
   renders each law twice: the model scoped `row_selector` to the desktop copies
   and used the two real levels. All 89 laws then got the full path.

Two things this shows. The loop works — but it needed a **run** between the
rounds, which is why the gate is the load-bearing part, not the proposal. And the
useful feedback was *"here is what came out and here is what I expected"*, not
*"it's wrong"*.

**Three more found by reviewing the sandbox output, all fixed:**

- **Deep tree nodes lost their ancestry.** Paths were built from the menu's
  nesting on whatever page a node was discovered on — but a book menu only
  renders the branch you are standing in, so nodes found deep inherited almost
  nothing. Fixed by reading the page's breadcrumb (see §6). Menu nesting is now
  the fallback, not the source.
- **A pinned menu id stops discovery silently.** `#book-navigation-1` exists on
  the seed page and not on deeper ones, so the walk could quietly stop.
  `[id^="book-navigation"]` matches all of them.
- **3 PDFs were reported as 39 documents.** Documents were keyed on
  (url, section_path), inherited from `generic_crawler`, where it exists so a
  genuinely cross-listed document appears under both sections. On a tree it
  backfires: the "Print / Save as PDF" link sits on every page of a section, so
  one file was counted 26 times. Documents are now keyed on the URL, with the
  extra placements kept in `also_in` and a `times_linked` count — and the
  sandbox reports 3 again, matching the baseline.

**The HTML was captured but invisible.** Every page's HTML was in `pages.json`
all along, but no `.html` files were written and the Excel dropped the column
(8 KB a page blows past Excel's 32k cell limit). Runs now write `html/NNNN_<slug>.html`
per page and the inventory sheet carries `text_len`, `n_pdfs`, `html_file` and a
2,000-character `text_preview`.

**Approval could go stale without anyone noticing.** I edited the sandbox form
*after* it was approved and nothing complained. Approval now records a hash of
the form body, and `show` / `run` report "APPROVED BUT SINCE EDITED" when the
file no longer matches what was verified. Without that the gate is decorative.

**Two bugs the MISA work exposed, both now fixed, both worth knowing about:**

- **`innerText` returns "" inside a hidden tab.** MISA's "Basic Legislations"
  pane is hidden until clicked, so every title and section name in it came back
  blank — a wrong answer that looked like a legitimately empty field. The row JS
  now falls back to `textContent`, which does not care about rendering. Any
  tabbed or accordion site would have hit this.
- **The digest ranked a nav menu above the documents.** On aml.gov.sa,
  SharePoint's `li.static` menu (33 links) outscored the 11 real documents on
  link count alone. Candidate scoring now weights *rows whose link is a file*
  most heavily, because nav items never point at a PDF and document rows usually
  do.

**Correction to something we believed:** SBP's circular listing is **not**
JavaScript-drawn. The digest measured 304 links in the raw HTML vs 302 rendered.
The `js_dependence` figure in every digest settles that question per site instead
of us arguing about it.

## 11. Limits, honestly

- **`pagination.mode: click` walks a next-button pager** (added 2026-08-03 for
  MHRSD: 63 rows over 4 pages, 63/63/63 on verify). Each click is verified against
  a fingerprint of the row set, so a dead control stops the walk instead of
  re-reading page 1. **A page-size control ("Show N entries") is still not
  handled** — that, not a next button, is what SECP needs.
- **`panels` covers tab strips whose tabs are fragments** (added 2026-08-05 for
  GOSI: six legal instruments on one url, 1 captured before). Panels are read
  with `textContent` and never clicked — on GOSI the *active* panel reports 279
  characters of innerText against 82,064, because its accordions are collapsed.
  **Still open:** a row inside a panel that holds more than one file yields only
  the first, so the section-level GOSI form gets 3 of 6 PDFs — SDAIA's bug in §9,
  and the same fix applies (make the row the file). Verified live 2026-08-05:
  6/6/6 and 2/2/2, 0% spread, and both text totals matched the snapshot figures
  to the character. `verify` had to be taught that a `panels` form needs phase 2
  — see `HANDOFF.md` §9.
- **Trees are walked, but only via the menu.** A page reachable by no menu link
  cannot be found by walking one — that needs a sitemap or the site's search.
  `crawl_tree` has the same limit.
- **`table` shape is declared but untested.** Nothing in KSA has needed it yet;
  it currently behaves as a list whose rows are `<tr>`.
- **Not wired into the orchestrator yet.** `runner.py` emits `pages.json` in
  exactly `generic_crawler`'s schema (same record and document keys), so the
  adapter work is small, but it is not done.
- **The digest can only offer candidates it can see.** Sites behind logins,
  CAPTCHAs, or private data APIs are unchanged by any of this.
- **`--force` exists.** If it starts getting used routinely, the gate has become
  theatre and we should find out why.

## 12. Output — files, and `RegulatoryDocument`

A run writes four things:

```
output/formfill/<name>/run/
├── pages.json      records + documents — generic_crawler's EXACT schema
├── rows.json       the phase-1 inventory
├── results.xlsx    inventory / pages / documents sheets
└── html/           one .html per page
```

`pages.json` matching `generic_crawler` is not a coincidence — it means
`crawler/generic_crawler_wrapper.py`, which already maps that file to
`List[RegulatoryDocument]`, works for formfill too. So
`formfill/pipeline.py::FormfillCrawler` **subclasses** it and overrides how the
crawl is run, not how it is mapped. No mapping logic is duplicated; if the
pipeline's idea of a document changes, both engines change together.

```python
from dynamic_crawler.formfill.pipeline import FormfillCrawler
docs = FormfillCrawler("dynamic_crawler/hints/sbp.circulars.yml",
                       regulator="SBP", source_system="SBP-CIRCULARS").fetch_documents()
```

or in `config/sources/<regulator>.yml`:

```yaml
  - name: "Circulars"
    mode: formfill
    hints: dynamic_crawler/hints/sbp.circulars.yml
    source_system: "SBP-CIRCULARS"
```

**It refuses to run an unapproved form** unless you pass
`require_approved=False`. A gate the pipeline ignores is not a gate.

### Three differences from the generic mapping, all deliberate

**1. A declared row IS a document.** The parent has to guess — it asks "does this
page have 200+ characters of prose?", because a link walk cannot know a folder
from a document. That guess is wrong for a form and expensively so: SBP's
phase-1 inventory has a title, URL, date and reference number per row but no page
text yet and no attached PDF, so **all 4,160 were discarded and
`fetch_documents()` returned zero.** The complete inventory that drives change
detection vanished at the pipeline boundary. A form does not guess: a human
declared the selector and the gate verified it fills, so a row with a URL is a
document. 4,160 out.

**2. The form's fields win over the parent's regex guesses.** The generic
wrapper's own docstring says `published_date` is left `None` because "a link walk
cannot reliably read issue dates". Measured over all 4,160 SBP circulars through
the adapter: **published_date 99%, reference_no 95%, department 90%.** (reference_no
scores higher here than the crawl's own 89% because the parent's regex fallback
catches some the form's pattern misses — the two layer.)

**3. Dates go through the pipeline's parser, not straight in.** A form extracts
what the page says — `"July 31 2026"`. The pipeline dedupes on `published_date`,
so mixing site formats with ISO would stop two records of the same document
matching. 4,145 of 4,160 come out as ISO; the rest keep the raw string rather
than being silently dropped.

## 13. How this relates to the other approaches

| | when to use |
|---|---|
| `generic_crawler` (paste a URL, pick a scope) | **First choice.** Most sites, no per-site config at all. |
| `formfill` (this) | When the generic walk misses structured fields the listing carries — reference numbers, issue dates, departments; when a paginated listing needs walking precisely; when auto-detection picks the wrong menu on a tree; or when you want the crawl **gated** — 3-run stability, fill rates, recorded human approval, none of which `generic_crawler` has. |
| Hand-written crawler | Sites neither can reach. |
| `dynamic_crawler/auto/` (LLM writes code) | **Superseded by this.** Kept only as evidence. |

`formfill` is a thin optional layer on top of the generic approach, not a return
to Approach 2's config-per-site. Most regulators should still need nothing but a
URL and a scope.
