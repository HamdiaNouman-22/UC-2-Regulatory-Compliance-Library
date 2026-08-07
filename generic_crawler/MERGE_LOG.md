# Generic Crawler — Merge Plan & Change Log

**Audience:** the three of us (lead, teammate A, teammate B). Read this before
touching `generic_crawler/crawler.py`.

**Status:** MERGE COMPLETE. All of A's and B's pieces are in, measured, and
regression-tested. See §12 for the final numbers.

---

## 1. Why this document exists

Three of us worked on `generic_crawler/crawler.py` at the same time, from the same
starting version, without sharing changes. We now have **three different versions**
of the same file:

| Who | What they added | Where it lives |
|---|---|---|
| **Lead** | shape detection (`strategies.py`) → tree/table walkers; SBP content fixes; `site_runners/`; `db_compare.py` | in the repo |
| **A** | auto-scope (`detect_scope`/`probe_scope`), click-and-verify link revealing (`reveal_all_links`), `heading_path`, `chrome` flag, `calibrate_scope.py` | on A's machine |
| **B** | external law portals, `JS_NAV_PATH`, `chrome` flag, link identity = (url, text), documents keyed by (url, section_path), `linked_from_title`/`parent_page_url`, `INTERNAL_PAGE` | on B's machine |

We cannot simply take one person's file — each version is missing the other two
people's work.

### Two things worth knowing before we start

**A and B built the same feature twice.** Both added a `chrome` flag to skip
header/footer links. The code is *identical*, down to the comment explaining why
`<nav>` is excluded. That is pure duplicated effort, and it is the reason this
document exists.

**Both A's and B's copies are missing the lead's SBP fixes.** The repo version
strips SBP's hidden PDF-clone block (`#pdfDownloadLayout`) and picks the real
heading as the page title (`JS_DOC_TITLE`). Neither A's nor B's copy has these, so
merging by "take their file" would silently reintroduce duplicated SBP content.

---

## 2. The rule we're following

> **Merge one piece at a time, and check the numbers after each piece.**

If we paste eight changes in at once and a site's document count drops by 40,
nobody knows which change caused it. One at a time, and we know immediately.

This only works if we know what the numbers were *before* we started. That is what
Step 0 (below) produces.

---

## 3. Merge order — safest first

Ordered so that by the time we touch the risky parts, everything else is settled.

### Round 1 — safe (these only ADD information, they cannot remove a document)

| # | Piece | From | What it does |
|---|---|---|---|
| 1.1 | `linked_from_title`, `parent_page_url` | B | Remembers the link text that led to a page, and which page it came from |
| 1.2 | External law portals | B | Treats `boe.gov.sa`, `moj.gov.sa`, `mc.gov.sa`, `zatca.gov.sa`, `pr.gov.sa` links as documents even with no `.pdf` |
| 1.3 | `heading_path` + `collapse_heading_path` | A | Folder trail from the page's heading nesting, for sites with no breadcrumb |
| 1.4 | `JS_NAV_PATH` | B | Folder trail from section containers / tab panels — catches layouts a heading walk misses |

*Note on 1.3 and 1.4: these are NOT duplicates. A's walks heading levels (works on
normal pages), B's finds section containers (works on accordion/tab layouts). We
want both, with an order of preference.*

### Round 2 — medium (these change WHAT gets recorded; counts will move)

| # | Piece | From | What it does | Watch for |
|---|---|---|---|---|
| 2.1 | `chrome` flag | A/B (identical) | Skip header/footer links | Document counts should DROP slightly — that's correct, it's removing footer junk |
| 2.2 | Documents keyed by (url, section_path) | B | Same PDF in two sections = two rows | Counts should RISE on cross-listed sites |
| 2.3 | `INTERNAL_PAGE` documents | B | A content page with a title and >1000 chars is also recorded as a document | Counts should RISE on tree sites |

### Round 3 — risky (these change WHERE the crawler goes)

| # | Piece | From | What it does |
|---|---|---|---|
| 3.1 | `reveal_all_links` | A | Click-and-verify revealing; replaces `collect_paginated_links` + `expand_tree` |
| 3.2 | Auto-scope | A | `detect_scope`/`probe_scope` work the scope out from the landing page |

Both must stay **switchable** — `--scope` must still accept a manual value, so that
if one site misbehaves we can turn the feature off for that site instead of undoing
the merge.

### Conflicts already decided

| Question | Decision | Reason |
|---|---|---|
| Is the same URL with different link text one link or two? | **A's way** — one link, merge the details | Same URL in content AND footer must not be wrongly marked chrome |
| Is the same URL in two sections one document or two? | **B's way** — two documents | The DB agrees: `document_exists_by_url(url, category)` is category-scoped for cross-listed SAMA docs |
| Do "Next" links use up the depth budget? | **No** — keep the original behaviour | B's version made pagination consume depth. With `max_depth=8`, SBP would stop after ~8 pagination pages instead of 133 |
| `chrome` flag — whose? | Either | The code is identical |
| SBP fixes (`#pdfDownloadLayout`, `JS_DOC_TITLE`) | **Keep the lead's** | Neither A nor B has them |

---

## 4. Step 0 — the baseline (running now)

### What we're measuring

For each site, three numbers from the **current, unmodified** repo code:

- **pages** — how many web pages the crawler recorded
- **documents** — how many document links it found
- **shape** — which walker the code chose (tree / table / generic)

Plus a `cap_hit` flag: if pages == max_pages, the crawl was cut short by the page
limit and the number is a *limit*, not a *measurement*.

### The sites

The six from A's `calibrate_scope.py`, using the scope A determined is correct for
each — so the baseline reflects the best the current code can do, not a bad guess.

| Site | Seed URL | Scope |
|---|---|---|
| SECP acts | `https://www.secp.gov.pk/laws/acts/` | prefix |
| SBP circulars | `https://www.sbp.org.pk/circulars` | prefix |
| SAMA sandbox | `https://rulebook.sama.gov.sa/en/regulatory-sandbox` | breadcrumb |
| SAMA CB law | `https://rulebook.sama.gov.sa/en/saudi-central-bank-law` | breadcrumb |
| MISA laws | `https://misa.gov.sa/activities/laws/` | prefix |
| SDAIA regs | `https://sdaia.gov.sa/en/SDAIA/about/Pages/RegulationsAndPolicies.aspx` | breadcrumb |

### Settings — these must never change between runs

```
--max-pages 150   --max-depth 8
```

If we compare a later run at a different cap, the comparison is meaningless.

### How to re-run it

```
venv/Scripts/python.exe generic_crawler/baseline.py            # run the crawls
venv/Scripts/python.exe generic_crawler/baseline_report.py     # build the Excel
```

`baseline.py` writes `output/_baseline/results.json` and a folder per site.
Use `--tag <name>` to label a run (e.g. `--tag after-1.2`).

`baseline_report.py` collects all of it into **one workbook** —
`output/_baseline/baseline_report.xlsx` — with three sheets:

| Sheet | What's in it |
|---|---|
| **Summary** | one row per site: pages, documents, shape, cap hit, errors, and a `flag` |
| **Documents** | every document found across all sites, `site` column first |
| **Pages** | every page recorded across all sites, `site` column first |

Read the `flag` column on Summary first:

| flag | Meaning |
|---|---|
| `OK` | real measurement, trust it |
| `ZERO` | found nothing — **extraction failed**, the site is not empty |
| `CAP` | stopped at the page cap; the count is the LIMIT, not the coverage |
| `NO-DOCS` | pages found but zero documents — suspicious, check by hand |
| `TIMEOUT` / `NO-RESULT` | did not finish |

### Baseline results

Run `baseline-before-merge`, current repo code, `--max-pages 150 --max-depth 8`.
Nothing hit the cap, so every number is a real measurement.

| site | shape | scope | pages | documents | cap hit? | errors | seconds |
|---|---|---|---|---|---|---|---|
| SECP acts | table | prefix | 1 | 36 | no | 0 | 20 |
| SBP circulars | tree | prefix | **0** | **0** | no | 0 | 86 |
| SAMA sandbox | tree | breadcrumb | 40 | 3 | no | 0 | 147 |
| SAMA CB law | tree | breadcrumb | 36 | 1 | no | 0 | 175 |
| MISA laws | generic | prefix | 2 | 72 | no | 0 | 52 |
| SDAIA regs | generic | breadcrumb | 10 | **415** | no | 0 | 676 |

**These are the numbers no merged change may reduce.** Total run time ~19 minutes.

### What the baseline exposed

Four problems, found before merging anything. Two of them the merge already fixes;
two need their own work.

#### B1 — SBP circulars returns nothing (shape misdetected)

`detect_shape()` classified SBP circulars as `tree`. It is a list site. `crawl_tree()`
then walked it looking for a Drupal book menu, found none, and returned 0 pages and
0 documents.

This is a bug in **our own** `strategies.py`, not in A's or B's work, and neither
of their changes fixes it — A's auto-scope decides *scope*, not *shape*.

Consequence for the merge: **SBP's baseline is 0, so any later change will look
like an improvement against it.** Fix the measurement before trusting SBP
comparisons.

#### B2 — Tree pages are often too short for the pipeline to use

The orchestrator refuses to analyse anything under 200 characters
(`MIN_TEXT_LEN`, `orchestrator.py`). Pages below that threshold today:

| site | pages under 200 chars |
|---|---|
| SAMA CB law | **16 of 36** |
| SAMA sandbox | 6 of 40 |
| SECP acts | 1 of 1 (see B3) |
| MISA laws | 0 of 2 |
| SDAIA regs | 0 of 10 |

SAMA CB law's median page is 260 characters and its shortest is 10
("Chapter 3: Monetary Policy"). The tree walker is finding the *structure*
correctly — the `section_path` values are right — but not the *content*.

So nearly half of SAMA CB law would be inserted into the library and then
skipped at the analysis step. Needs investigating before the pipeline swap.

#### B3 — Table sites record no page text, by design

SECP acts produces exactly 1 page with 0 characters. `crawl_table()` writes a
single synthetic page row for the whole table; the real output is the 36
documents. That is intended, but it means for table sites **all** content must
come from downloading the PDFs (the orchestrator's tier 3, download + OCR),
which is the slow path. Worth knowing, not a bug.

#### B4 — MISA is picking up a junk page and a weak folder trail

MISA's 2 pages are "Updated Investment Law" (20,629 chars, real) and "Sign up
for our newsletter" (2,441 chars, junk). Both get `section_path = "Home > Activities"`.

This is exactly what the merge fixes: the `chrome` flag (2.1) drops the newsletter
block, and `heading_path` / `nav_path` (1.3, 1.4) replace the useless
"Home > Activities" trail with the real section names.

#### Also worth noting

SDAIA's 415 documents are **real** — 415 unique URLs, 407 of them PDFs, with
genuine regulatory titles. But 386 unique titles across 415 rows means some
generic link text is leaking in ("2025", "Policies" repeated 4–8 times). B's
`title_attr` fallback in `best_doc_title` should improve those.

---

## 4b. MISA evidence — measured on the live page, before merging

Probed `https://misa.gov.sa/activities/laws/` directly and ran B's `JS_NAV_PATH`
and A's heading-stack against it, side by side. Read-only, nothing changed.

### What the current code produces

| | value |
|---|---|
| documents recorded | **72** |
| document hosts | misa.gov.sa 71, zatca.gov.sa 1 — **boe.gov.sa 0** |
| section_path on every single document | `Home > Activities` |

### What is actually on that page

| link kind | count |
|---|---|
| direct PDFs (`misa.gov.sa/app/uploads/...`) | 136 anchors → 71 unique, all captured today |
| **external law portal links (`laws.boe.gov.sa/BoeLaws/Laws/LawDetails/...`)** | **50 — all missed today** |
| other (misa internal 64, investsaudi.sa 8, social 2) | 76 |

### Folder path: B's `nav_path` vs A's `heading_path`

Same links, both signals, real output:

| Document | B's `nav_path` | A's `heading_path` |
|---|---|---|
| Law of Real Estate Registration | `Sectoral Legislations > Real estate sector` | `Laws > Laws > Real estate sector` |
| Anti-Money Laundering Law | `Sectoral Legislations > Financial Sector` | `Laws > Laws > Financial Sector` |
| Anti-Cyber Crime Law | `Sectoral Legislations > Technology sector` | `Laws > Laws > Technology sector` |

**B's wins on MISA.** A's repeats the parent (`Laws > Laws`, which
`collapse_heading_path` would trim to `Laws`) and loses the tab group name.
B's captures both the group ("Sectoral Legislations") and the category.

Combined with the page breadcrumb, B's code builds
`section_path = breadcrumb + " > " + nav_path`, giving:

```
Home > Activities > Laws > Sectoral Legislations > Real estate sector
```

which is the structure we want, instead of today's flat `Home > Activities`.

### Expected effect of merging B's MISA pieces

| | before | after |
|---|---|---|
| documents | 72 | **~122** (+50 boe.gov.sa law pages) |
| distinct section_paths | 1 | one per sector |
| header/footer junk ("Investor Guide" ×2, newsletter block) | included | dropped by `chrome` |

### Two extra findings

**F-1 — A's and B's `JS_BREADCRUMB` is the OLD, buggy one. Ours already fixes it.**
Their copies select `'a, span, li'` and filter only `>` and `›`, so MISA's trail
comes back as `['Home', '/', 'Activities', '/', 'Laws']` — the `/` separators
become folder levels. Our repo version has an `isSep` test covering
`> › — – - | / ·` and prefers `<a>` text, giving the correct `Home > Activities`.

Verified: the same page, probed with their JS gives 5 crumb steps with junk;
probed with ours gives 2 clean steps.

**This is a second concrete reason our file must be the merge base** — alongside
the SBP `#pdfDownloadLayout` fix and `JS_DOC_TITLE`. Taking A's or B's file
wholesale would reintroduce all three.

**F-2 — no missing external portal domains for MISA.** Checked every other host
on the page: `investsaudi.sa` (8), `vision2030.gov.sa` (1), `raqmi.dga.gov.sa` (1),
social (2). None host law text. B's five-domain list is sufficient here — but it
is a hand-maintained list, so it needs review per regulator.

---

## 4c. Full signal probe — all six sites

Measured with `generic_crawler/probe_signals.py` (read-only, one page load per
site, imports the real crawler helpers). Output:
`output/_baseline/probe_signals.xlsx` — sheets **Summary**, **Links**,
**ShapeSignals**.

| site | shape | crumb steps | unique links | documents | external portal | chrome links | B nav_path | A heading_path |
|---|---|---|---|---|---|---|---|---|
| SECP acts | table | 2 | 196 | 23 | 0 | 167 | 0 | 27 |
| SBP circulars | tree | 2 | 177 | **0** | 0 | 144 | 0 | 33 |
| SAMA sandbox | tree | 2 | 132 | 2 | 0 | 88 | 0 | **0** |
| SAMA CB law | tree | 3 | 133 | 3 | 0 | 88 | 0 | 35 |
| MISA laws | generic | 2 | 131 | 69 | **24** | 41 | **89** | 89 |
| SDAIA regs | generic | 4 | 204 | 36 | 0 | 125 | 0 | 49 |

Numbers reproduced identically across two separate runs.

### DECISION 1 — folder path precedence: B's `nav_path` first, then A's `heading_path`

B's selectors (`.regulationContent`, `.showLawItems`, `MobItems`, tab panels) are
MISA-shaped and fire **only** on MISA. A's heading walk fires on five of six sites.

So B's is more precise where it applies and **cannot affect any other site**.
There is no trade-off: prefer `nav_path`, fall back to `heading_path`, fall back
to breadcrumb alone.

### DECISION 2 — the `chrome` flag must be merged as DATA, not as behaviour

Both A and B drop header/footer links from the documents list. Measured effect on
the seed pages:

| site | documents flagged chrome | verdict |
|---|---|---|
| **SAMA sandbox** | **2 of 2** | ⚠️ would drop to **zero documents** |
| **SAMA CB law** | **2 of 3** | ⚠️ loses two of three |
| **SECP acts** | **3 of 23** | ⚠️ real SROs and orders, not junk |
| MISA laws | 4 of 93 | ✅ correct — Privacy Policy, Terms & Conditions, Safe Usage Policy, Investor Guide |
| SDAIA regs | 0 of 36 | no effect |

SECP's three chrome-flagged documents are an SRO amendment, a January 2026 order,
and a stock market report — genuine content sitting in a header widget.

**Therefore: record `chrome` as a column, do not act on it yet.** Measure over a
full crawl, then decide. A's `_merge_links` rule ("a link seen outside the
header/footer even once is never chrome") may rescue SAMA's documents when they
also appear on a content page — but that is a hypothesis to test, not to assume.

### Other observations

**SDAIA's breadcrumb repeats itself:**
`SDAIA > Saudi Data and Artificial Intelligence Authority > SDAIA > About SDAIA`.
A's `collapse_heading_path` already removes a step that restates its parent —
the same rule applied to breadcrumbs would clean this up.

**SBP has 0 documents on its seed** because SBP circulars are HTML pages, not
PDFs. Combined with `detect_shape` wrongly returning `tree` for a list site,
that is why the baseline crawl produced nothing. Parked for now.

**SBP and the Saudi sites are flaky.** One probe run returned an empty SBP page;
another failed DNS (`ERR_NAME_NOT_RESOLVED`) on both `misa.gov.sa` and
`sdaia.gov.sa`, then succeeded minutes later. `probe_signals.py` now retries three
times, and merges results into the existing workbook so a partial re-run repairs
those rows instead of wiping the others. **Never record a single failed run as a
site's measurement.**

---

## 5. Change log

Every merged piece gets an entry here: what changed, why, and the before/after
numbers. Nothing gets merged without an entry.

---

### CHANGE 1 — `chrome` flag: keep site furniture out of the documents list

**From:** A and B (both wrote identical code for this).
**Files:** `generic_crawler/crawler.py` only.

#### What changed

1. **`JS_LINKS`** now returns `chrome` per link — true when the link sits inside
   `header, footer, [role="banner"], [role="contentinfo"]`.
2. **Document collection** skips chrome links, but keeps them in a separate
   `chrome_documents` audit list instead of discarding them.
3. **Page crawling does NOT skip chrome links** (see below — this is deliberate,
   and is where we differ from B).
4. **Outputs** gained a `chrome_dropped` list in `pages.json` and a
   `chrome_dropped` sheet in `pages.xlsx`. The `done` event reports the count.
5. A document seen in the header on one page but in real content on another is
   **kept** — the content sighting wins (`dropped_chrome` subtracts anything that
   also reached the real documents list).

#### Why we did NOT skip chrome links when crawling

B's version also skips chrome links when choosing which pages to visit. Measured:
SECP's `header#masthead.site-header` contains **321 of its 375 links (86%)** —
it is the mega-menu. Skipping those for crawling would blind the crawl to
anything only reachable through the menu. A keeps following them; A is right.

So: **chrome affects the documents list, never the crawl frontier.**

#### The evidence behind the rule

Checked which element actually matches, per site
(`output/_baseline/probe_signals.xlsx`):

| site | container | links inside | share |
|---|---|---|---|
| SECP | `header#masthead.site-header` | 321 | 86% |
| SAMA | `header#header.bg-gradient-custom` | 159 | 67% |
| MISA | `header.bg-primary-700` | 60 | 23% |
| SDAIA | `header` | 116 | 57% |

An earlier read of this data suggested the rule would delete real documents on
SAMA and SECP. **That was wrong, for two reasons:**

1. SAMA's header "Guidebook" link looked section-specific, but the *identical*
   two documents appear in the header of the Central Bank Law page as well —
   so they are site-wide furniture, not sandbox content.
2. More importantly, **SAMA and SECP never take this code path.** SAMA is crawled
   by `crawl_tree` and SECP by `crawl_table`; neither uses `JS_LINKS`. Their
   baseline documents are all real content
   (`rulebook.sama.gov.sa/.../en_net_file_store/...`, and 36 genuine SECP acts) —
   none of the header documents appear in either output.

So the blast radius of this change is **the generic BFS path only**: today, MISA
and SDAIA.

#### Result

| site | path | documents before | after | dropped as chrome |
|---|---|---|---|---|
| MISA laws | generic | 72 | **68** | 4 |
| SDAIA regs | generic | 415 | *(regression run)* | |
| SECP acts | table | 36 | *(unaffected — different path)* | |
| SAMA sandbox | tree | 3 | *(unaffected — different path)* | |
| SAMA CB law | tree | 1 | *(unaffected — different path)* | |
| SBP circulars | tree | 0 | *(unaffected)* | |

The four MISA documents removed, all genuine site furniture:

```
Investor Guide       .../Investor-Guide_13-02_compressed.pdf
Privacy Policy       .../سياسة-الخصوصية.pdf
Terms & Conditions   .../Terms_and_Conditions_08_01.pdf
Safe Usage Policy    .../سياسة-الاستخدام-الآمن.pdf
```

#### How to check this yourself

Open `pages.xlsx` → **`chrome_dropped`** sheet. Everything listed there was
removed from the library. **If a real regulatory document ever appears on that
sheet, the rule is wrong** — that sheet exists precisely so the rule stays
falsifiable instead of silently deleting things.

---

### CHANGE 2 — external law portals: capture laws hosted on another site

**From:** B.
**File:** `generic_crawler/crawler.py` only.

#### The problem

MISA does not attach a PDF for every law. For 24 of them it links out to the
national legal portal, where the law lives as a **web page with no file
extension**:

```
https://laws.boe.gov.sa/BoeLaws/Laws/LawDetails/25fed59a-.../1
```

Nothing in that URL says "document" — no `.pdf`, no `/download/`, no `wpdmdl=`.
So `is_document_link()` returned False, and because the link is also off-host it
was never crawled either. **Those 24 laws were invisible to us.**

#### What changed

1. New `EXTERNAL_LAW_PORTALS` set + `is_external_law_portal()` — five Saudi legal
   portals (`boe.gov.sa`, `mc.gov.sa`, `moj.gov.sa`, `pr.gov.sa`, `zatca.gov.sa`).
2. `is_document_link()` returns True for those hosts.
3. `doc_type_of()` returns `EXTERNAL` for them — **including when the URL ends in
   `.aspx`** (`mc.gov.sa` serves `.aspx`; without this, five laws came back typed
   `ASPX`, which would tell the pipeline to treat a law as a web asset).

A real file extension still wins, so `.pdf` on a portal host is still `PDF`.

#### Result — MISA

| | documents |
|---|---|
| baseline | 72 |
| after Change 1 (chrome) | 68 |
| **after Change 2** | **92** |

All 24 added are genuine laws, none junk:

```
laws.boe.gov.sa  16   Income Tax Law, Labor Law, Anti-Cyber Crime Law,
                      Enforcement Law, Civil Aviation Law, Mining Investment Law, ...
mc.gov.sa         5   Commercial Papers Law, Commercial Pledge Law,
                      Law of Commercial Agencies, ...
laws.moj.gov.sa   2   Law of the Board of Grievances, Notarization Law
pr.gov.sa         1   Privileged Residency Permit Law
```

Other sites are unaffected — none of them link to these hosts.

#### ⚠ This is per-regulator knowledge inside the shared engine

A hardcoded domain list is the kind of thing that quietly turns a generic crawler
into a site-specific one. It is data rather than a code branch, which is why it is
acceptable for now, but:

* **it will need a new entry for each new regulator**, and
* nobody will remember to check it.

When onboarding a site, run `probe_signals.py` — it prints every host seen on the
page, so a missing portal shows up as an unexpected host with many links. For MISA
the remaining hosts were `investsaudi.sa` (8), `vision2030.gov.sa` (1),
`raqmi.dga.gov.sa` (1) and social links; none host law text, so the list is
complete for this site.

Longer term this belongs in per-source config, not in the engine.

#### Regression (Change 1 verified before Change 2 was measured)

| site | path | baseline | after chrome | verdict |
|---|---|---|---|---|
| SECP acts | table | 36 | 36 | unchanged ✅ |
| SBP circulars | tree | 0 | 0 | unchanged ✅ |
| SAMA sandbox | tree | 3 | 3 | unchanged ✅ |
| SAMA CB law | tree | 1 | 1 | unchanged ✅ |
| SDAIA regs | generic | 415 | 415 | unchanged ✅ |
| MISA laws | generic | 72 | **68** | intended −4 |

Only the site the change was meant to affect moved. Confirms the analysis that
tree and table paths don't use `JS_LINKS`.

---

### CHANGE 3 — remember how we reached each page

**From:** B. **File:** `crawler.py`.

Two new fields on every page record, and two new Excel columns:

| field | meaning |
|---|---|
| `linked_from_title` | the anchor text of the link that led to this page |
| `parent_page_url` | the page that link was on |

**Why it matters.** A detail page's own `<title>` is frequently generic
("Details", "Circulars"), while the link that led to it carries the real
document name. This is often the best title we will ever have for that page —
and it is what B's `INTERNAL_PAGE` idea (a later piece) depends on to turn a
content page into a document with a usable name.

Purely additive: two columns appear, no existing value changes.

---

### CHANGE 4 — `title_attr` fallback in `best_doc_title`

**From:** B. **File:** `crawler.py`.

`JS_LINKS` now returns the anchor's `title="..."`, and `best_doc_title()` uses it
as the second choice:

```
1. anchor text        (unless it's a generic "Download" button)
2. title="..."        <- NEW
3. row/card context
4. URL slug
```

#### ⚠ Honest result: this is a no-op on all six current sites

| site | document links carrying a usable `title` attribute |
|---|---|
| MISA laws | 0 of 93 |
| SECP acts | 0 of 23 |
| SAMA CB law | 1 of 3 |
| SAMA sandbox | 0 of 2 |
| SDAIA regs | **36 of 36 — but the attribute simply repeats the link text** |

SDAIA's markup is `<a title="Policies">Policies</a>`, so the fallback yields
nothing new. Kept because it is free and correct, and may help a regulator we
have not onboarded yet — but it must **not** be credited with fixing titles.

---

### OPEN ISSUE — duplicate document titles (needs a different fix)

Found while testing Change 4, **not** solved by it.

**41 of SDAIA's 415 documents share a title with another document**
(386 unique titles across 415 rows):

| title | count | what the documents actually are |
|---|---|---|
| `2025` | 8 | BeneficiaryVoiceQ1 2025, Q2 2025, Q3 2025, … |
| `Policies` | 4 | DataClassificationPolicy, DataSharingPolicy, FreedomOfInformationPolicy |
| `2026`, `2024`, `2023`, `2022` | 4 each | quarterly reports |

A title shared by several different documents is not a title. The URL slug holds
the real name in every one of these cases.

**Proposed fix (new work — not from A or B, so logged separately):**

1. Improve `title_from_slug()` — URL-decode, drop the file extension, split
   `camelCase` (today it yields `Beneficiaryvoiceq1 2025.Pdf`).
2. After the crawl, for any title used by more than one document, re-derive those
   titles from the slug.

This is generic — no per-site word list — and measurable: it would affect 41 SDAIA
rows and, on current evidence, nothing on the other five sites.

**Implemented** — see Change 5.

---

### CHANGE 5 — better slug titles + disambiguate colliding titles

**New work** (not from A or B). **File:** `crawler.py`.

`title_from_slug()` rewritten to handle what regulator URLs actually contain:

| input | before | after |
|---|---|---|
| `BeneficiaryVoiceQ1%202025.pdf` | `Beneficiaryvoiceq1 2025.Pdf` | `Beneficiary Voice Q1 2025` |
| `DataClassificationPolicy.pdf` | `Dataclassificationpolicy.Pdf` | `Data Classification Policy` |
| `SAMA_Circular_2024.pdf` | `Sama Circular 2024` | `SAMA Circular 2024` |

Three fixes: percent-decoding, dropping the file extension, and splitting
camelCase. It only title-cases when the slug carries no case of its own, so
acronyms survive.

New `disambiguate_titles()` runs once at the end of every crawl: any title shared
by **more than one** document is re-derived from that document's slug. Unique
titles are never touched. Reported as `titles_disambiguated` in the `done` event.

Measured: SECP 2 rewritten (its two bare "Download" rows), SDAIA ~41 expected.

---

### CHANGE 6 — every walker returns the same thing, in the same shape

**New work** (needed for the pipeline). **Files:** `crawler.py`, `strategies.py`.

#### The blocking defect

The three walkers disagreed about their own output:

| | generic | tree | table |
|---|---|---|---|
| `text` | plain text | **HTML** ❌ | placeholder |
| `html` | HTML | *missing* | *missing* |
| `breadcrumb` | present | *missing* | *missing* |
| `crawl()` returns | records | **`None`** ❌ | **`None`** ❌ |

`crawl_tree` put `document_html` into the field named `text`. Nothing noticed
because `_write_excel` never reads it — but the pipeline branches on exactly that
distinction (`content_text` vs `document_html`, `orchestrator.py` tiers 1b/2), so
a tree site would have fed raw HTML to the LLM as if it were prose.

And on tree/table paths `crawl()` wrote its files then `return`ed nothing, so no
caller could ever use the results in-process.

#### What changed

* `crawl_tree` / `crawl_table` now emit the full key set: `text` (plain),
  `html`, `breadcrumb`, `linked_from_title`, `parent_page_url`.
* New `_finish()` — the single tail every path goes through. It normalises the
  records, disambiguates titles, adds `content_hash`, writes `pages.json` +
  `pages.xlsx`, emits `done`, and **returns `(records, documents)`**.
* `pages.json` now records which `shape` produced it.

Verified: page keys are identical across table / tree / generic, and tree `text`
now starts with prose while `html` starts with `<div ...`.

---

### CHANGE 7 — `content_hash` on every page and document

**New work** (needed for change detection). **File:** `crawler.py`.

* **pages** — MD5 of the page's normalised **text**. Deliberately not the HTML:
  HTML churns on every deploy (build ids, cache-busting query strings) and would
  report every page as modified on every run.
* **documents** — MD5 of `doc_url|title`. We do not download the file here, so
  this identifies the link; real content hashing happens when the pipeline
  fetches it.

This is what `new / modified / unchanged` comparison will read. The value was
already being computed for in-run duplicate detection (`content_key`) and thrown
away.

---

### CHANGE 8 — `JS_NAV_PATH`: real folder paths for tab/accordion pages

**From:** B. **File:** `crawler.py`.

Evaluated once per page, mapped by `(normalised url, link text)`, and passed to
`doc_section_path()`. Where it fires it **wins over** the `group` heading, being
two named levels rather than one nearest heading.

**No opt-in flag** — unlike `group_headings`, this cannot misfire: measured
coverage is MISA 89 links, SECP/SBP/SAMA/SDAIA 0. It stays silent on markup it
does not recognise.

#### Result — MISA folder structure

Before: **1** folder path for all 94 documents (`Home > Activities`).
After: **17** distinct paths.

| documents | path |
|---|---|
| 16 | `MISA > MISA-LAWS > Home > Activities > Basic Legislations > Commercial Legislation` |
| 13 | `… > Sectoral Legislations > Financial Sector` |
| 12 | `… > Basic Legislations > Judicial Legislation` |
| 7 | `… > Sectoral Legislations > Health and Food Security Sector` |
| 6 | `… > Sectoral Legislations > Real estate sector` |
| 6 | `… > Sectoral Legislations > Energy and Industry Sector` |
| 5 | `… > Sectoral Legislations > Transport and Logistics Sector` |
| 5 | `… > Home > Activities` (no nav_path matched — acceptable remainder) |
| … | 9 more sector folders |

---

## 6. Plugging into the pipeline — `GenericSiteCrawler`

**New file:** `crawler/generic_crawler_wrapper.py`.

The orchestrator asks a crawler for exactly one thing
(`orchestrator.py`, `run_for_regulator`):

```python
docs = self.crawler.fetch_documents()      # -> List[RegulatoryDocument]
```

`SAMACombinedCrawler` and the other hand-written crawlers already answer that.
The generic crawler did not — it wrote files. `GenericSiteCrawler` is the
translator.

```python
crawler = GenericSiteCrawler(
    seed_url      = "https://misa.gov.sa/activities/laws/",
    regulator     = "MISA",
    source_system = "MISA-LAWS",
    category      = "Laws and Regulations",
)
Orchestrator(crawler=crawler, repo=repo, ...).run_for_regulator("MISA")
```

**Nothing in the orchestrator changes.**

### Field mapping

| crawl row | RegulatoryDocument |
|---|---|
| `documents[].doc_url` | `document_url` |
| `documents[].title` | `title` |
| `documents[].type` | `file_type` (PDF / DOCX / EXTERNAL / HTML) |
| `documents[].found_on` | `source_page_url` |
| `documents[].section_path` | `doc_path` — split on `>`, **regulator first** |
| `documents[].content_hash` | `content_hash` |
| `pages[].text` | `extra_meta["content_text"]` (pipeline tier 1b) |
| `pages[].html` | `document_html` (tier 2) |

`doc_path` always begins with the regulator name. The folder tree is built from
this list (`_get_or_create_compliance_category`), so two regulators both using a
top-level "Circulars" folder would otherwise merge into one node and tangle their
documents.

`published_date` is left `None` — a link walk cannot read issue dates reliably.
The orchestrator already copes: `filter_new_documents` falls back to deduping on
`(document_url, category)` when the date is missing.

### Content pages become documents

On tree sites the regulation **is** the page — there is no attached PDF. So a
page with at least 200 characters of text (matching `MIN_TEXT_LEN` in the
orchestrator, below which analysis is refused anyway) is emitted as a document
with `file_type="HTML"`. Controlled by `include_pages="auto" | "always" | "never"`.

### It runs in a subprocess by default

The engine uses Playwright's **sync** API, which refuses to start inside a
running asyncio loop — and the pipeline installs a twisted/asyncio reactor for
Scrapy (`crochet.setup()` in `scheduler.py` and `jobs/sbp_job.py`). `scheduler.py`
already runs SECP and SAMA as subprocesses for this reason. Pass
`in_process=True` only from a plain script or a test.

### `CompositeCrawler`

Runs several sources for one regulator and joins the results — the generalised
form of what `SAMACombinedCrawler` already does by hand. Sources can be any mix
of `GenericSiteCrawler` and hand-written crawlers; anything with
`fetch_documents()` works. A failing source is logged and skipped so it cannot
take the others down.

### Verified end to end

| site | shape | RegulatoryDocument produced |
|---|---|---|
| MISA laws | generic | **94** — 68 PDF, 24 EXTERNAL, 2 HTML pages, 17 folder paths |
| SAMA CB law | tree | **28** — 1 file link + 27 content pages, 27 carrying `org_pdf_link` |
| SAMA sandbox (12-page cap) | tree | **14** — 3 file links + 11 content pages |

---

## 7. Bug fix — "thin tree pages" (a folder and a short article are not the same thing)

### What the problem actually was

16 of SAMA Central Bank Law's 36 pages hold under 200 characters, so the adapter's
`MIN_TEXT_LEN` rule dropped them. Investigating the live pages showed the
extraction was **not** broken — the pages really are short, for two different
reasons that length alone cannot distinguish:

| page | text | children | what it is |
|---|---|---|---|
| Chapter 3: Monetary Policy | 10 chars (`"Article 17"`) | 1 | a **folder** — the content is the article below it |
| Chapter 2: Management | 190 chars | **12** | a **folder** |
| Article 3 | 184 chars | 0 | a **real article**, merely brief |
| Article 27 | 102 chars | 0 | a **real article** |
| Article 4 | 3,173 chars | 0 | a normal article |

Verified directly against the site: `.node__content` on Chapter 3 genuinely
contains 10 characters, and Article 3's 184 characters are the law itself
("The objectives of the Bank are as follows: 1. Maintaining monetary stability…").

**Lowering the threshold would not work.** Chapter 2 is a folder with 190
characters — below any threshold that would rescue Article 3 (184).

### The fix

1. **`strategies.py`** — `crawl_tree` records `n_children` per page.
2. **`generic_crawler_wrapper.py`** — `_page_is_document()`:
   * `text_len >= 200` → document (unchanged)
   * otherwise, **when the walker reported `n_children`**: a **leaf**
     (`n_children == 0`) with at least 50 characters is a document; a page with
     children is a folder
   * when `n_children` is unknown (the generic walk), fall back to the 200 rule

3. Page documents now carry **`extra_meta["org_pdf_link"]`** from the page's
   Original PDF. The orchestrator looks for exactly that key
   (`extract_text_content_unified`, tier 3) and will download + OCR it when the
   page text is too short to analyse — which is precisely the case for these
   short articles.

### Result — SAMA Central Bank Law

| | documents from 36 pages |
|---|---|
| before | 20 |
| **after** | **28** |

Included (leaves, short but real): Article 27 (102), Article 17 (142),
Article 15 (163), Article 26 (174), Article 3 (184).
Excluded (all folders): Chapter 2 (12 children), Chapter 1 (7), Chapter 6 (6),
"A- Board of Directors" (5), Chapter 5 (3), "B- The Governor…" (3).

27 of the 28 carry `org_pdf_link`, so a short page still gets analysed from its
PDF rather than being skipped.

---

## 8. Bug fix — SBP returned nothing (shape misdetection)

### Diagnosis

`detect_shape()` decides in three steps. Measured signals on each seed page:

| site | maxRows | hasNodeContent | hasBookMenu | decided by |
|---|---|---|---|---|
| SECP acts | 20 | 0 | 1 | rule 1 (table) — strong |
| SAMA sandbox | 1 | **1** | **1** | rule 2 (tree) — strong |
| SAMA CB law | 1 | **1** | **1** | rule 2 (tree) — strong |
| MISA / SDAIA | 0 | 0 | 0 | no rule → generic |
| **SBP circulars** | 0 | **0** | **0** | **rule 3 — the child probe** |

SBP has no tree markers whatsoever, so it fell through to the last resort: pick a
child link, open it, and ask `JS_IS_TREE_NODE`. Two faults compounded:

1. **The child it probed was `/circulars/P30` — page 2 of the list.** A pagination
   link says nothing about the site's shape.
2. **`JS_IS_TREE_NODE` accepted "2 or more breadcrumb links" as proof of a tree.**
   Essentially every site has a `Home > Section` breadcrumb.

So SBP was handed to `crawl_tree`, which looked for a Drupal book menu, found
none, and returned 0 pages and 0 documents.

### Fix (`strategies.py`)

* `JS_IS_TREE_NODE` — the breadcrumb clause is gone. Only real tree markers now
  count: a `.node__content` body, or ≥5 links in a book/menu navigation.
* `JS_SHAPE` — pagination URLs (`/P30`, `/page/2`, `?page=3`) are excluded from
  the `childUrls` probe list.

Both SAMA tabs are classified by rule 2 on the seed page itself, so tightening
the last-resort probe cannot affect them — confirmed by re-running detection:

```
SBP circulars   -> generic     (was: tree)
SAMA sandbox    -> tree        (unchanged)
SAMA CB law     -> tree        (unchanged)
SECP acts       -> table       (unchanged)
```

### Result — SBP circulars

| | |
|---|---|
| before | **0 pages, 0 documents** |
| after (40-page cap) | 40 pages, 38 file links → **78 RegulatoryDocument** |

with real trails, e.g.
`SBP > SBP-CIRCULARS > Home > Circulars > DMMD Circular Letter No. 08`,
and attachments typed correctly (PDF / DOCX / XLSX).

SBP circulars are HTML pages rather than PDFs, which is why the crawl-level
`documents` count stays low — the adapter promotes the pages themselves.

---

## 9. ⚠ SDAIA's document count is not stable between runs

Comparing two runs of the **same code** on SDAIA:

| run | documents |
|---|---|
| baseline | 415 |
| after-merge-round1 | **363** |

Investigated, because a 52-document drop looks exactly like a regression:

* `chrome_dropped` = **0** — not the chrome rule
* the **same 10 pages** were visited in both runs, in the same order
* all 58 lost documents come from **one section**:
  `/en/MediaCenter/KnowledgeCenter/ResearchLibrary/*.pdf`
  ("Bias In AI Systems", "AI In Municipal", "Generative AI In Entertainment" …)

One widget's worth of links, missing from one run of an otherwise identical
crawl. That is **site-side rendering variability**, not a code change.

### Why this matters far beyond the merge

This is the single biggest risk to the monitoring plan. If change detection
compared these two runs it would report **58 documents as "disappeared"** and, if
wired to deletion, remove them from the library. Nothing was deleted at SDAIA.

**Conclusions to carry forward:**

1. A "disappeared" verdict can never be trusted from a single crawl. Require a
   document to be missing from **two or more consecutive** complete crawls.
2. The completeness gate needs to cover more than the page cap: a run that finds
   materially fewer documents than the last one is *suspect*, not authoritative.
3. Run-to-run variance must be measured per site before monitoring goes live —
   `baseline.py` already produces exactly the data needed.

---

## 10. Changes 9–13 — the rest of the merge

### CHANGE 9 — A's `heading_path`, behind the existing `group_headings` flag

**Measured before merging**, on every site, against the lead's `group`:

| site | verdict |
|---|---|
| SECP acts | **identical** on 20 of 23 documents |
| SAMA CB law | neither fires meaningfully |
| MISA laws | A adds a level, but B's `nav_path` already beats both |
| **SDAIA regs** | **A wins decisively** |

SDAIA is the case that justifies it:

| | value |
|---|---|
| lead's `group` | `[Laws and Regulations]` — **the same label for all 36 documents** |
| A's `heading_path` | `Personal Data Protection Law and The implementing Regulation`, `Data classification Policy and Regulations`, `Freedom of Information Policy and Regulations`, … |

**Merged as the implementation behind the existing `group_headings` flag**, not as
a new one — it is strictly better where the flag is on and irrelevant where it is
off. The lead's `group` computation is untouched, so `aml.gov.sa` is unaffected.

Precedence in `doc_section_path()`: `nav_path` (B) → `heading_path` (A) → `group`.

Also added: `collapse_heading_path()` (A's, verbatim behaviour) and the same
"drop a step that repeats one already in the trail" rule applied to the
**breadcrumb**, because SDAIA's is
`SDAIA > Saudi Data and Artificial Intelligence Authority > SDAIA > About SDAIA`.
Only the display trail changes — breadcrumb SCOPE matching reads the raw list.

Enabled `group_headings` for `sdaia.gov.sa` in `SITE_PROFILES`, with the
measurement recorded in the profile comment.

**Result: SDAIA 1 folder path → 29.**

### CHANGE 10 — A's `_merge_links` (link identity)

Links are keyed by **href** across every pass, with two merge rules:

* a link seen outside the header/footer even once is **never chrome**
* a sighting carrying a heading trail beats one that lost it

Both matter: the same URL usually appears in content *and* footer, and clicking
(pagination, accordions) can re-render a link without its surrounding headings.
B's `(href, text)` keying was rejected for links — see the conflicts table — but
adopted for documents, which is Change 11.

### CHANGE 11 — B's `(url, section_path)` document keying

Regulators deliberately cross-list one document under several sections, and each
listing is its own place in the library. The DB agrees: `document_exists_by_url`
is category-scoped precisely for SAMA's cross-listed documents.

SDAIA on a 3-page crawl: **32 → 36 documents.**

This also exposed a bug in the new adapter, which was deduping on url alone and
would have collapsed the cross-listings straight back into one. It now dedupes on
`(document_url, doc_path)`.

### CHANGE 12 — A's `reveal_all_links` (click-and-verify)

Replaces `collect_paginated_links()` in the crawl loop. Three tiers, cheapest
first: maximise a "Show N entries" menu → the known Next selectors → candidates
discovered by `JS_CLICKABLES`.

The idea that makes it safe on an unseen site: **it does not try to know which
element is the right one.** It clicks a plausible candidate and checks whether the
page actually gained links. Every click is budgeted (25/page), verified, and
**undone if it navigated** — the guard `expand_tree()` lacks, and without which
the rest of a page's extraction runs against the wrong document.

`collect_paginated_links()` and `expand_tree()` remain defined, for `probe_scope()`
and manual debugging.

Verified on SECP's generic path: `t0_select` gained 16 links, 3 click candidates
found, document count unchanged at 36.

### CHANGE 13 — A's auto-scope, and one seed load for both decisions

`detect_scope()` + `probe_scope()` + the five calibrated thresholds. `--scope`
now defaults to `auto` and still accepts a forced value.

**One page load answers both questions.** Scope ("how far may I wander") and
shape ("how do I read this layout") are independent — SECP is table+prefix, SAMA
is tree+breadcrumb, SDAIA is generic+breadcrumb — and both are now derived from
the same single seed load, so they cannot disagree about what the page contained.

#### One fix on top of A's version

A's `detect_scope()` returns `host` when it sees no signal. But a page that
returned **no links at all** is a failed measurement, not an absence of signal —
and `host` is the most damaging possible answer, sending the crawl across an
entire domain. SBP intermittently serves an empty page and hit exactly this.

`probe_scope()` now checks for zero links first and falls back to `prefix`
(A's own stated principle: a wrong `prefix` collects too little and is visible;
a wrong `host` wanders).

#### Verified — all six sites

```
site           expect      got          docs under cands  ratio  share crumbs
SECP acts      prefix      prefix         23     0   164    0%    12%      2
SBP circulars  prefix      prefix          0    33   168   20%     0%      2
SAMA sandbox   breadcrumb  breadcrumb      0     0    43    0%     0%      2
SAMA CB law    breadcrumb  breadcrumb      1     0    43    0%     2%      3
MISA laws      prefix      prefix         68     0    27    0%    72%      2
SDAIA regs     breadcrumb  breadcrumb     36     0    17    0%    68%      4  [SEED IS A FILE]
PASS — all 6 sites resolve to the expected scope.
```

Note SDAIA: 36 docs at 68% share would fire the "listing page" rule and return
`prefix` — the `seed_is_file` guard is what correctly keeps it on `breadcrumb`.
Exactly as A documented.

### `calibrate_scope.py` is now a GATE, not a one-off

A's file said "delete this once calibration is done". Kept and promoted instead:

* retries three times (an empty page is a failed measurement, never a result)
* uses `profile_for()` so it measures what the crawl actually does
* **exits non-zero on any mismatch**

> **Change a knob → run this → all sites pass → only then merge.**

The five thresholds are **global**. Auto-scope traded a per-site setting that
could only break one site for shared thresholds that affect all of them, and some
margins are thin — `LISTING_DOC_SHARE` separates SAMA at 2% from SECP at 12%.
This gate is what makes that trade safe.

---

## 11. Merge complete — what came from where

| # | Change | From | Effect |
|---|---|---|---|
| 1 | `chrome` flag (documents only, audit sheet) | A + B | MISA −4 junk |
| 2 | External law portals | B | MISA **+24 laws** |
| 3 | `linked_from_title` / `parent_page_url` | B | new columns |
| 4 | `title_attr` fallback | B | no-op today, kept |
| 5 | Slug titles + disambiguation | new | SDAIA ~41 titles fixed |
| 6 | One record schema, `crawl()` returns on all paths | new | **fixed HTML-in-`text` on tree sites** |
| 7 | `content_hash` | new | enables change detection |
| 8 | `JS_NAV_PATH` | B | MISA **1 → 17 folder paths** |
| 9 | `heading_path` | A | SDAIA **1 → 29 folder paths** |
| 10 | `_merge_links` | A | chrome/heading survive re-renders |
| 11 | `(url, section_path)` document keying | B | cross-listings preserved |
| 12 | `reveal_all_links` | A | click-and-verify, navigation guard |
| 13 | Auto-scope | A | no per-site scope decision |
| — | SAMA thin-page leaf rule + `org_pdf_link` | new | SAMA CB law **20 → 28** |
| — | SBP shape misdetection | new | SBP **0 → 78** |

### Rejected, with reasons

| Piece | Why not |
|---|---|
| B: link identity `(href, text)` | A's href-keyed merge is correct at link level; B's rule was right at document level and adopted there instead |
| B: nav links consume depth | Would cap SBP pagination at 8 pages instead of 133 |
| B: `INTERNAL_PAGE` | Superseded — the adapter promotes content pages, using the leaf rule which is more precise |
| A/B: `JS_BREADCRUMB` | Theirs is the older version and drops only `>`/`›`; ours also filters `/`, which MISA needs |
| A/B: `JS_MAIN_CONTENT` | Theirs lacks the SBP `#pdfDownloadLayout` strip, which would duplicate every SBP circular |
| A/B: skip chrome links when crawling | SECP's masthead holds 321 of 375 links — would blind the crawl |

---

## 12. Final regression — `merge-complete` vs `baseline-before-merge`

Same six sites, same settings (`--max-pages 150 --max-depth 8`).

| site | shape | pages before → after | **documents before → after** | verdict |
|---|---|---|---|---|
| SECP acts | table | 1 → 1 | **36 → 36** | unchanged ✅ |
| SBP circulars | tree → **generic** | 0 → **150** (CAP) | **0 → 101** | **fixed** ✅ |
| SAMA sandbox | tree | 40 → 40 | 3 → 3 | unchanged ✅ |
| SAMA CB law | tree | 36 → 36 | 1 → 1 | unchanged ✅ |
| MISA laws | generic | 2 → 2 | **72 → 92** | **+20** ✅ |
| SDAIA regs | generic | 10 → 10 | 415 → **439** | +24 (see variance note) |

**No site lost documents.** Totals: 527 → 672 across the six.

### Reading these numbers properly

**SBP hit the 150-page cap.** 101 documents is what fits in 150 pages, not SBP's
real size — its pagination runs to 133 list pages plus the circulars themselves.
The cap must be raised for SBP before its number means anything. Recorded as
`CAP` in the report so it cannot be mistaken for coverage.

**SAMA's crawl-level counts are unchanged on purpose.** The gain there is at the
adapter: 36 pages now yield **28** documents instead of 20 (the leaf rule), and
SAMA sandbox yields 14. The crawler's `documents` list only counts attached
files, which is why 3 and 1 stay put.

**SDAIA at 439 vs 415 baseline vs 363 mid-merge.** Three runs of near-identical
code spanning 363–439. The +24 is partly Change 11 (cross-listed documents now
get a row per section) but the spread is mostly the site variance documented in
§9. **Do not read SDAIA's number as a measurement of anything but the range.**

### Artefacts

| file | what |
|---|---|
| `output/_baseline/baseline_report.xlsx` | Summary + all 672 documents + all 239 pages |
| `output/_baseline/probe_signals.xlsx` | every signal on each seed page |
| `output/_baseline/results.json` | every run, comparable over time |

### Gates that must pass before any future change

```
venv/Scripts/python.exe generic_crawler/calibrate_scope.py    # exits non-zero on a wrong scope
venv/Scripts/python.exe generic_crawler/baseline.py --tag <name>
venv/Scripts/python.exe generic_crawler/baseline_report.py
```

A regression now takes ~50 minutes, almost all of it SBP and SDAIA.

---

## 13. The `list` shape — SBP done properly

### Why the earlier SBP "fix" was not enough

Making SBP `generic` stopped it returning zero, but the generic BFS is the wrong
tool: it mixes listing pages and detail pages in one queue and spends the page
cap on both. It reached **101 of 4,160** circulars.

The real numbers:

```
4,160 circulars ÷ 30 per page = 139 listing pages
139 listing pages + 4,160 detail pages = 4,299 page loads  (~9 hours)
```

### Why it was never classified as a table

```
tables: 0     tbody tr: 0     rows: h4.mb-2 × 30 per page
pager:  /circulars/P30, /P60 … /P4140
```

**SBP has no `<table>` at all.** `JS_SHAPE` counts `tbody tr`, gets zero, so
`maxRows` can never reach `TABLE_MIN_ROWS`. Behaviourally SBP is identical to
SECP — walk rows, page by page, open each row — only the markup differs.

### What was added

**`JS_LIST_ROWS`** finds rows *structurally* rather than by tag: group links by
their container's tag+class signature, keep signatures occurring 8+ times, then
widen from the title element outward while the parent holds no second row (that
is what picks up the reference number and date beside the title).

Two rules were needed to pick the right group:

| rule | why |
|---|---|
| exclude header/footer/nav | SBP's mega-menu alone gave **124 bare `<a>` "rows"**, outnumbering the 30 real entries 4:1 |
| prefer rows carrying a **date** | that is what separates a document listing from a list of navigation links |

**`_pager_offsets()`** turns the handful of links a pager shows into the whole
sequence. SBP renders only "1 2 3 … 139" with hrefs `/P30`, `/P60`, `/P4140`;
from those it infers step 30, last 4140 → **139 pages**, exactly matching the
site. Without it we would only ever visit the three pages it links to.

**`crawl_list()`** in two deliberately separable phases:

* **Phase 1** — listing only. 139 pages, ~20 min, complete inventory of all
  4,160 entries with title, link, reference number and date. `--no-details`.
* **Phase 2** — open each row's detail page for its HTML, then move to the next
  row. `--max-details N` to bound it.

**This split is the point.** Phase 1 alone answers "what is new since last time",
so phase 2 only has to run for rows that are actually new — the same saving CBB
gets from its Thomson Reuters feed, on any list site, without the regulator's
cooperation. It also gives change detection a *reliable full inventory*, which a
capped BFS could never provide.

### Metadata the generic crawler could never get

The listing row is kept verbatim on each record, and the adapter parses it:

```
"… BPRD Circular Letter No. 15 of 2026  July 06 2026 | BPRD | Circular Letters"
        -> reference_no    = "BPRD Circular Letter No. 15 of 2026"
        -> published_date  = "2026-07-06"
```

Measured on a 12-entry run: **12/12 dates and 12/12 reference numbers parsed**,
each with its detail HTML (25–30k chars). `published_date` had been `None` for
every generic-crawled document until now.

### Four robustness fixes forced by this work

1. **The seed load had no retry** — and the seed decides both scope *and* shape
   for the whole run. One DNS blip silently downgraded a crawl to
   generic + default scope. Now 3 attempts, and an empty render counts as a
   failure.
2. **Shape must be detected BEFORE scope.** `probe_scope()` calls `expand_tree()`,
   which clicks — and on SBP a click tears the listing out of the DOM. Detecting
   shape afterwards saw a wrecked page and read `generic`. `detect_shape()` only
   reads, so it now runs first.
3. **Chromium dies on long crawls.** It died after 15–57 detail pages. Added
   `--disable-dev-shm-usage`, page recycling every 40 loads, and — when the
   crash takes the whole browser context — a clean stop that **keeps the rows
   already collected and logs `INCOMPLETE`**, instead of raising. A crash must
   never look like "this site has no documents".
4. **A dead browser BLOCKS on close(), it does not raise.** That hung the
   crawler after a completed walk, so results were computed and never written.
   Close is now bounded and skipped entirely when the browser is known dead.

### Shape detection is unchanged everywhere else

```
SECP acts       expect table     got table
SBP circulars   expect list      got list
SAMA sandbox    expect tree      got tree
SAMA CB law     expect tree      got tree
MISA laws       expect generic   got generic
SDAIA regs      expect generic   got generic
ALL AS EXPECTED
```

The order in `detect_shape` protects this: table wins first (SECP has a real
`<table>`), then tree (both SAMA tabs have `hasNodeContent` + `hasBookMenu`), and
only then list — so a rulebook page listing its children can never be mistaken
for a document listing.

### Still open on SBP

* A full phase-2 run is ~9 hours. It should be driven by phase-1 diffs, not run
  wholesale — that wiring is not built yet.
* Chromium still dies periodically. The crawl now survives it and reports, but a
  complete 4,160-page pass will need resume-from-where-it-died.
