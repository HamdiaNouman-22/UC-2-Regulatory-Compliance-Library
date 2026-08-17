# CMA — Laws & Regulations runner

`site_runners/cma_laws.py` covers all nine tabs of the CMA Laws & Regulations
section. This is a runner, not a generic-crawler seed and not a formfill form —
§"Why a runner" says why, and that reasoning is the template for deciding the
same question about the next regulator.

```bash
venv/Scripts/python.exe site_runners/cma_laws.py --tab capital_market_law
venv/Scripts/python.exe site_runners/cma_laws.py --all          # every tab
venv/Scripts/python.exe site_runners/cma_laws.py --tab faqs --max-articles 5
venv/Scripts/python.exe site_runners/pages_to_excel.py output/site_runners/cma_guides
```

Output goes to `output/site_runners/cma_<tab>/pages.json`, in exactly the schema
`generic_crawler` and `formfill` emit — same `pages` / `documents` keys, same
record fields. One pipeline adapter reads all three engines.

---

## 1. What each tab is, and what it produced

Full `--all` regression, 2026-08-06. **634 records, 220 documents, 0 empty
records, 0 coverage gaps, 0 errors.**

| tab | shape | records | documents | secs | notes |
|---|---|---|---|---|---|
| Capital Market Law | `law_chapters` | 67 | 1 | 261 | contiguous 1..67 |
| Implementing Regulations | `subtabs_paginated_detail` | 36 | 36 | 242 | 5 categories + 1 uncategorised |
| Guides | `cards` | 13 | 13 | 10 | source list independently confirms 13 |
| Circulars | `cards_grouped` | 6 | 6 | 14 | all "AML & CFT" |
| Public Consultation | `tabs_cards_detail` | 91 | 152 | 439 | 0 active, 91 expired |
| FAQs | `faq_paginated` | 418 | 0 | 19 | 35/35 pages, no gaps |
| Forms | `single_page` | 1 | 10 | 7 | one page, ten attachments |
| SIFI | `single_page` | 1 | 1 | 7 | |
| CPE Policy | `single_page` | 1 | 1 | 7 | |

A full `--all` takes about 17 minutes; Public Consultation and Capital Market Law
are two thirds of it.

Six shapes for nine tabs. That ratio is the argument for one runner over nine
forms — and also the warning, because each shape is a separate thing that can
break on its own.

---

## 2. Why a runner and not one of the two engines

**Articles are not links.** A Capital Market Law chapter page looks like an
accordion, but the panes are genuinely empty — 0 characters of `textContent`,
not merely hidden. The real destination is inside an `onclick`:

```html
<button onclick="window.location.href='/en/.../CH1/Pages/CH1Article2.aspx'"
        data-bs-target="#flush-collapse/en/.../ch1/pages/ch1article2.aspx">
```

Both engines discover links through `a[href]`, so every article is invisible to
them. Worse, the id in `data-bs-target` is lowercased and 404s — only the
`onclick` URL works.

(`generic_crawler` now recognises that `onclick` pattern for ordinary link
walking. That fix alone does not give us the rest of this file.)

---

## 3. The four things that would have silently under-reported

Each of these produced a plausible, healthy-looking, wrong answer first. They
are written up because the *class* of mistake will recur on the next site, not
because CMA is unusual.

### 3.1 Public Consultation is an iframe

`/en/RulesRegulations/Consulting/Pages/default.aspx` renders an empty shell. Its
only real content is an iframe pointing at:

```
https://cma.gov.sa/RulesRegulations/Consulting/Pages/ENPublicConsultion.aspx
```

Note there is **no `/en/` segment** — adding one returns "Access Error". Crawl
the tab URL and you get 0 documents from a page that returns HTTP 200 and looks
completely normal. The runner crawls the inner URL directly (`CONSULT_INNER`).

Its own tab strip is mislabelled at the markup level: "Active Consultation"
targets `#first-tab-pane`, whose CSS class is `expiresectionContainer`. Trusting
the class name would file every active consultation as expired. The runner reads
the **label → pane id** mapping off the page instead.

### 3.2 Every card is rendered twice

Circulars reported 12 cards for 6 circulars. The extra 6 are the SharePoint
list-view web part (`table#onetidDoclibViewTbl0`) that feeds the grid, rendering
the same `div.card-wrapper` markup a second time.

Filtering on **visibility** would be the obvious fix and it is wrong: Guides
legitimately hides page 2, Implementing Regulations hides pages 2–3. So the
source table is excluded by **location** — a card inside a `<table>` is the data
source, not the list.

That table then turned out to be useful: it prints its own row count
(`Count= 13`), which is the list length straight from SharePoint. `check_total()`
compares it against the cards actually read. For Guides it independently
confirms 13. This is ground truth almost no regulator gives us, and it is the
only check here that can catch a card the page's own JS failed to draw.

### 3.3 The category dropdown and the sub-tabs are decoration

Circulars has a category dropdown; Implementing Regulations has six sub-tabs.
The obvious implementation is to select each option / click each tab and record
what appears — 6 to 30 round trips, racing the site's own filter JS, and giving
a wrong answer if one click silently fails.

None of it is necessary. The card's **parent** carries the metadata:

| tab | data attributes on the card's parent |
|---|---|
| Guides | `data-id` `data-page` |
| Circulars | `data-id` `data-page` `data-categoryid` `data-categoryname` |
| Implementing Regulations | `data-id` `data-page` `data-title` `data-year` `data-month` `data-category` |

Reading an attribute cannot half-work. The runner still reads the dropdown
options and the sub-tab labels — not to drive the crawl, but to **compare**
against what the cards claim:

```json
{"event":"subtabs","from_tabs":["Glossary of Defined Terms","Guidelines",
 "Instructions & Procedures","Regulations","Rules"],
 "from_cards":["Glossary of Defined Terms","Guidelines",
 "Instructions & Procedures","Regulations","Rules"],
 "only_in_tabs":[],"only_on_cards":[]}
```

Exact match. For Circulars the same check reports a real finding:

```json
{"event":"categories","from_cards":["AML & CFT"],
 "from_dropdown":["AML & CFT","Licenses"],"only_in_dropdown":["Licenses"]}
```

**"Licenses" is a category with zero published circulars.** The SharePoint source
table holds 6 rows, all AML & CFT. Not a gap in our crawl — but the run says so
out loud rather than leaving it to be discovered later.

### 3.4 The FAQ filter does nothing, and pretending otherwise invented data

The FAQ page has a left-hand filter, one checkbox per regulation, which would
have given each of the 418 questions a proper folder.

It does not work. Measured 2026-08-06: the checkboxes carry no `onclick`, no
`onchange`, `tabindex="-1"`, there is no Apply or Search button, and ticking one
changes **nothing** — still 418 items, same classes, same ids, same pager.

The first implementation inferred the grouping from what stayed visible after
each tick. That read the *pagination* hide class, not a filter, so it "labelled"
12 questions and produced 288 contradictions — 30 regulation names competing for
the same 12 page-one slots. Wrong folder names are worse than no folder names.

So FAQs are flat under the tab, and `check_faq_filter()` ticks one box each run
and reports if the site ever wires them up:

```json
{"event":"faq_grouping","grouped":false,"filter_is_inert":true,
 "why":"filter checkboxes do not alter the DOM"}
```

### 3.5 One announcement page, several consultations

The first version let the detail page's `<h1>` become the record title. It looked
like an improvement — the news page has a fuller headline than the card.

It merges documents. `CMA_N_2739` announces **two** consultations, the Investment
Funds amendments and the Real Estate amendments, and `CMA_N_2253` is shared by
four. Taking the page headline gave all of them the same title, and the
orchestrator dedupes on title + published_date, so they would have collapsed into
one document each. Unique titles went 85 → **90** of 91 once the card title was
kept; the page headline now travels as `page_title` instead.

The one remaining repeated title is genuine and instructive: two separate
consultations on the amended Rules for Qualified Foreign Financial Institutions,
one in 2016 and one in 2017, with different URLs and different text. Title alone
is not an identity key for this tab — title + date is.

---

## 4. Completeness checks

A tab that quietly returns nothing looks exactly like a tab with no documents.
That is the failure mode this project keeps tripping over, so every shape has
something that fails loudly.

| check | where | what it catches |
|---|---|---|
| unimplemented shape → `SystemExit` | `crawl_tab` | a tab silently skipped |
| 0 cards → `SystemExit` | `crawl_cards`, `crawl_regs` | the grid failed to render |
| stated pager total vs highest `data-page` | `check_total` | a list still rendering when we read it |
| SharePoint `Count= N` vs cards read | `check_total` | a card the page's JS never drew |
| sub-tab labels vs `data-category` | `crawl_regs` | a whole category missed |
| dropdown options vs `data-categoryname` | `crawl_cards` | same, for Circulars |
| FAQ page contiguity 1..35 | `crawl_faqs` | a page of questions missing |
| article contiguity 1..67 | Capital Market Law | a missed article |
| displayed-but-unlisted article recovery | `crawl_tab` | see §5 |
| filter-is-inert re-check | `check_faq_filter` | the site fixing its own filter |
| fill rates for every date field | `crawl_regs`, `crawl_consult` | a field that quietly stopped extracting |

These print as JSON events during the run. `coverage_gap` is the one to grep for.

**Empty vs short.** The run reports these separately, on purpose. *Empty* means
no text **and** no file — the record carries nothing and something failed.
*Short* is usually fine: a card that is a title plus a PDF has its content in the
file, and an FAQ answer of "Yes, you can after getting a license" is 80
characters and complete. An earlier version flagged all 15 short FAQ answers as
"thin", which is how a warning stops being read.

### Reviewing a run

```bash
venv/Scripts/python.exe site_runners/pages_to_excel.py output/site_runners/cma_* \
    --out output/site_runners/CMA_all.xlsx
```

Merges every tab into one workbook — **Summary** (one row per tab: records,
documents, empty records, date fill rates, unique URLs), **Documents**, **Pages**.
Nine separate files give you no way to notice that one came back empty; the
Summary sheet is the whole crawl on one screen. It reads `formfill` and
`generic_crawler` output too.

---

## 5. The two articles that were nearly lost

A Capital Market Law chapter page always *displays* one article, and normally
that one is also the open accordion item. Not always: Chapter 8 lists only
"Article Fifty" while displaying "Article Forty Nine", which has no accordion
entry at all. Chapter 2 does the same with "Article Four".

Iterating the accordion therefore dropped two whole articles, and the run looked
perfectly healthy — 65 articles, no errors. It was caught only by checking the
numbers were contiguous. When nothing is open, the runner now takes the
displayed article from the `<h1>` ("Chapter Eight … - Article Forty Nine") and
logs a `recovered_article` event.

Result: **67 articles, 67 unique URLs, contiguous 1..67, no gaps, no duplicates.**

---

## 6. Dates

Same rule as Tadawul and MOH, and it is not cosmetic:

- **`published_date`** = the issue date, and **part of document identity** —
  `filter_new_documents` in the orchestrator dedupes on title + published_date.
- **`last_updated_date`** (in `extra_meta`) = a revision or file-modified stamp.

Feeding a revision date into `published_date` makes an amended regulation look
like a brand-new document every time the regulator touches the page. That is a
monitoring bug that manifests as a flood of false "new document" alerts.

| tab | published_date | last_updated_date | other |
|---|---|---|---|
| Implementing Regulations | "Publishing date:" on the Details page | "Last modified date:" | — |
| SIFI / Forms / CPE | — | "Last modified date:" | — |
| Public Consultation | — | detail page's last-modified | `expiry_date` from "Expire in …" |

Fill rate on Implementing Regulations is **34/36**. The two without a date are
CMA's own gap — their cards print `----` where the date goes, and one of them
has an empty `code=` in its Details link. Both still captured their PDF. A
card-date fallback is in place for the case that actually matters: a Details
page that fails to load looks identical to one with no date on it.

---

## 7. Section paths

The hierarchy is as agreed; every **name** is the site's own wording, collapsed
whitespace and nothing else.

```
Capital Market Authority (CMA) > Laws & Regulations > Capital Market Law
    > Chapter One Definitions > Article One
Capital Market Authority (CMA) > Laws & Regulations > Implementing Regulations
    > Instructions & Procedures > Instructions of Simplified Investment Funds
Capital Market Authority (CMA) > Laws & Regulations > Circulars
    > AML & CFT > BRA Guideline
Capital Market Authority (CMA) > Laws & Regulations > Public Consultation
    > Expired Consultation > <consultation title>
Capital Market Authority (CMA) > Laws & Regulations > FAQs for Implementing Regulations
    > <the question>
```

An earlier draft normalised "Chapter One Definitions" to "Chapter 1-Definitions".
Reversed: the regulator's wording is the thing we are mirroring, and any
rewriting rule is one more thing to maintain and to disagree with the source
about.

---

## 8. Known limits

- **FAQs are not grouped by regulation** — §3.4. Unblocked the day CMA wires the
  filter up; `check_faq_filter()` will say so.
- **Active Consultation is currently empty.** The pane is present and holds 0
  cards. The runner clicks the tab before reading it and warns, so an empty tab
  and an unrendered one are distinguishable in the log — but only a run made
  while an active consultation exists will actually exercise that path.
- **Detail pages are metadata, not prose.** A Regulation Details page is title +
  date + download link, roughly 400 characters. The content is in the PDF, which
  is captured as a document. Records look thin because they are; the thin-record
  warning deliberately ignores any record that carries a file.
- **No incremental mode.** Every run is a full crawl. Implementing Regulations
  takes ~8 minutes (36 detail pages) and Public Consultation ~10 (91). Fine
  nightly, wasteful hourly.
- **Not yet wired into the orchestrator.** Output is on disk in the shared
  schema; `crawler/generic_crawler_wrapper.py` is where a CMA source would be
  registered.
