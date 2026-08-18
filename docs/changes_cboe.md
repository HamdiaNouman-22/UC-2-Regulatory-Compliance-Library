# CBE onboarding — every change we made

**Driven by:** onboarding `cbe.org.eg` (Central Bank of Egypt) as a new regulator.

Five changes, in the order we did them:

| # | change | files |
|---|---|---|
| 1 | [Prefix-scoped runs over a list of sub-sites](#1-prefix-scoped-runs-over-a-list-of-sub-sites) | `generic_crawler/run_site.py` *(new)* |
| 2 | [Images render when you open the saved HTML](#2-images-render-when-you-open-the-saved-html) | `generic_crawler/crawler.py` |
| 3 | [Document links in the saved HTML actually reach the site](#3-document-links-in-the-saved-html-actually-reach-the-site) | `generic_crawler/crawler.py` |
| 4 | [Procurement PDF declared from the CLI](#4-procurement-pdf-declared-from-the-cli) | `generic_crawler/run_site.py` |
| 5 | [Monitoring](#5-monitoring) | `crawler/cbe_crawler.py` *(new)*, `config/sources/cbe.yml` *(new)*, `config/change_signals.yml` |

`crawl()` was never touched in signature or return value — it is imported by the
live pipeline at `crawler/generic_crawler_wrapper.py:253`, so its contract had to
stay intact. Nothing was deleted anywhere.

---

## 1. Prefix-scoped runs over a list of sub-sites

**New file:** `generic_crawler/run_site.py`. One seed, a dynamic list of
sub-paths, each crawled as its own prefix-scoped run, sequentially.

```
venv/Scripts/python.exe generic_crawler/run_site.py --seed https://www.cbe.org.eg/en --subpaths governance,laws-regulations,aml-cft --out output/cbe_test --scope prefix --max-pages 150
```

`--subpaths-file <path>` for long lists (one per line, `#` comments). An entry
starting with `http` is used verbatim; anything else is joined onto the seed.
Nothing is hardcoded. Scope defaults to `prefix` and applies to every section.

**Why a new file rather than a flag on `crawl()`:** the pipeline imports `crawl()`,
so changing it puts every regulator at risk. A runner that only calls it is
additive. No scope logic needed changing either — `scope_prefix()` already derives
the prefix from whatever URL it is handed.

### Five rules built in, and the evidence for each

1. **One output directory per section, never merged.** Each keeps its own
   `pages.json` and its own `status`. Merging is the ZATCA bug — five forms shared
   one baseline, overwrote each other, and every run was quarantined.
2. **Preflight the whole list before crawling anything**, so a typo costs a second
   instead of an hour. A mistyped section otherwise crawls to `zero`, which reads
   as "failed extraction, check scope and shape" and sends you debugging the
   crawler instead of the string you typed.
3. **Sequential, never parallel.** Playwright's sync API drives one browser, and
   pacing is most of what keeps a crawl off a WAF.
4. **`blocked` aborts the whole run.** Continuing to hit other paths on a host
   whose bot wall just answered is what turns a soft block permanent —
   `saudiexchange.sa` and `simah.com` were both blocked that way. `zero` and
   `incomplete` do **not** abort: they are that section's problem, not the host's.
5. **The roll-up reuses `run_status`'s vocabulary**, worst-wins over
   `ok < incomplete < zero < blocked`, read back from each section's `pages.json`
   rather than from `crawl()`'s return value. A second definition of "did this run
   work" would disagree with the first.

### Output

```
output/cbe_test/
  governance/         pages.json  pages.xlsx  html/
  laws-regulations/   pages.json  pages.xlsx  html/
  aml-cft/            pages.json  pages.xlsx  html/
  summary.json
  summary.xlsx        sheets: subsites | documents | duplicates
```

Duplicate `doc_url`s across sections are **reported, not deduplicated** — a file
under two sections almost always means the prefixes overlap, and silently
collapsing the rows hides that while leaving the overlap to cause cross-section
withdrawal proposals downstream.

### What the first real run exposed

`cbe.org.eg` returns **HTTP 200 for any path**, so the preflight passes a
misspelled section: `/en/definitely-not-a-real-section` renders a soft-404,
records its one page, and `run_status` calls it `ok` — correctly, since one page
*is* a successful crawl of one page. Confirmed not a side effect of two crawls in
one process: that URL gives the same 1 page / 0 documents standalone.

The answer is `thin_note()` — a **note**, not a status:

```
ok   laws-regulations   17 pages   55 docs
ok   nonsense-typo       1 pages    0 docs   [thin - check this sub-path exists;
                                              some sites answer 200 for any path]
```

A sixth status word would put a second definition of a working run next to
`run_status`, which is what rule 5 exists to prevent.

**Still open:** the preflight uses `urllib`, whose header signature CBE's WAF
rejects with a 269-byte "Request Rejected" page served as **HTTP 200** — so on
this site it approves everything. `requests` with the same User-Agent gets the
real page. Moving it over fixes the check.

---

## 2. Images render when you open the saved HTML

## 3. Document links in the saved HTML actually reach the site

**Both are one bug, one fix, in `generic_crawler/crawler.py`.**

`JS_MAIN_CONTENT` returns `clone.innerHTML`, and `innerHTML` serializes attributes
**as authored** — a relative `src` or `href` stays relative. That string was
written to `html/<slug>.html` as a bare fragment with no origin, no `<base>` and
no charset. A browser opening it from `D:\` resolved everything against
`file:///D:/`:

* every image 404'd → **the broken-image icons you reported**
* every link became `file:///D:/en/laws-regulations` → **a path that does not exist**

What identified it: the broken images still rendered their `alt` text ("Background
Image", "Laws - Banking Laws"). The `<img>` tags were captured fine; only the url
was wrong.

`documents.xlsx` was never affected — `JS_LINKS` reads `a.href`, the IDL property
the DOM has already resolved. The fault was confined to the serialized HTML.

### The four edits

| where | what |
|---|---|
| `crawler.py:71-72` | `urljoin` + `html.escape` added to imports (both stdlib, no new dependency) |
| `crawler.py:207-317` | new: `absolutize_html`, `html_document`, `write_page_html`, `_abs_one`, `_abs_srcset` |
| `crawler.py:1836` | the in-walk write goes through `write_page_html` instead of raw `write_text` |
| `crawler.py:2028-2058` | `_finish()` absolutizes `r["html"]` and writes every page's file |

2,069 → 2,212 lines. Nothing removed; only `_finish` modified.

**Attributes rewritten**, longest-prefix first so `data-src` matches before `src`:
`data-lazy-src`, `data-original`, `data-src`, `data-bg`, `srcset`, `poster`,
`href`, `src`. **Skipped** because there is nothing to resolve: `#`, `data:`,
`javascript:`, `mailto:`, `tel:`, `blob:`, `about:`.

**Each saved file is now a real document:**

```html
<!doctype html>
<html><head>
<meta charset="utf-8">
<base href="https://www.cbe.org.eg/en/laws-regulations/laws/banking-laws">
<title>Banking Laws</title>
</head><body> … fragment … </body></html>
```

`<meta charset>` fixes an adjacent bug nobody had hit: the file was written UTF-8
with nothing declaring it, so Arabic rendered as mojibake from disk. `<base>` is
belt-and-braces over the rewrite, and it is the house style —
`regression_check.py`'s `freeze()` already does exactly this for frozen pages,
commented *"without it every link becomes file:/// and the document-link counts
collapse to zero."* Same bug, same remedy, never applied to the crawler's own
output.

### Why in `_finish()` and not at the three `innerHTML` sites

Three walkers serialize innerHTML — `crawler.py` `JS_MAIN_CONTENT`,
`strategies.py:270`, `strategies.py:518` — and a fourth will be written by someone
who never reads this. `_finish()` is the one exit every walker passes through,
which is why `content_hash` is stamped there. Per-serializer fixing is the
per-branch stamping mistake `crawler/fingerprint.py` exists to document.

It matters beyond the local file: `pages[].html` becomes `document_html` in MSSQL
via the wrapper, so this fixes the **stored** library HTML too.

Side effect: the tree/table/list walkers set `html_file=""` and wrote no HTML files
at all. They get them now, so the column means the same thing whichever walker ran.

### Result

17 pages from `/en/laws-regulations`. The only non-`http` values left in the saved
HTML are `data:` placeholders and `javascript:;`, both correctly skipped:

```html
<img src="https://www.cbe.org.eg/-/media/project/cbe/page-content/gallery/
          laws-and-regulation/laws---banking-laws.jpg?h=1418&w=1890&hash=6E48…"
     alt="Laws - Banking Laws">
```

### What it deliberately does not do

* **No local asset download** — images load from the live site, so viewing needs a
  connection. Ruled out explicitly.
* **No CSS** — `JS_MAIN_CONTENT` strips `<style>`/`<link>` as chrome, so pages are
  text and images, unstyled. Unchanged behaviour.
* **One image is unrecoverable.** CBE's hero is
  `src="data:image/jpeg;base64,/some_lqip_in_base_64=="` — a literal placeholder
  whose real url is injected by the site's JavaScript, which we strip. Two of the
  three broken images you reported are fixed; that one needs the site's scripts.

---

## 4. Procurement PDF declared from the CLI

**In `generic_crawler/run_site.py`.** Opt-in: with no `--documents` flag, nothing
about a run changes.

```
--documents "About CBE :: Procurement :: https://www.cbe.org.eg/-/media/project/cbe/page-content/rich-text/about-cbe/procurement.pdf"
```

Three forms, url always last: `<url>` | `<title> :: <url>` |
`<section path> :: <title> :: <url>`. Missing titles come from
`title_from_slug()`, the type from `doc_type_of()` — both already in `crawler.py`,
neither re-implemented. `--documents-file` for a list, `--documents-section` for a
default folder.

### Why it had to be declared rather than crawled

The PDF was **already being found** and correctly filed under `chrome_dropped`:

```
Procurement                       .../about-cbe/procurement.pdf   found_on: /en/laws-regulations
Board of Directors Achievements   .../about-cbe/bodachievements_2012.pdf
```

It is a nav-menu link, so it appears on every page. Un-dropping it would have:

1. recorded it **once per page** — 17 rows from that one run, and
2. carried the `section_path` of wherever it was found — `Home > Laws and
   Regulations`, the wrong folder. Documents are keyed on `(url, section_path)`,
   so with three sections crawled the same PDF lands under three folders: three
   documents proposing each other as withdrawn.

Making that safe needs dedup plus a folder override — which is this feature,
reached the long way.

### Fingerprinted on the ETag, not `url|title`

A declared document has no page text, and `url|title` cannot move when the
publisher replaces the PDF behind an unchanged link — it would add a document
change detection can never notice changing. So the server is asked, in the order
`crawler/fingerprint.py` prefers:

| basis | measured on the CBE PDF |
|---|---|
| `ETag` | `ffc4891297f348f3be3d044356700fdb` |
| `Last-Modified` | `Thu, 16 Jun 2022 07:14:19 GMT` |
| `url\|title` | the fallback, recorded **as** a fallback |

`hash_basis` travels with the row and prints in the summary, because a stamp that
quietly degraded to `url|title` looks identical to one that did not. An
unreachable url degrades and is labelled; it never raises.

`requests` not `urllib`, for the WAF reason above. **HEAD is 403 on CBE, GET is
200**, so the helper falls through HEAD → GET.

### Result

```
Declared documents (1)
  Procurement  [etag]
    About CBE  <-  .../about-cbe/procurement.pdf

OK  -  1 section(s), 14 documents
  ok   aml-cft   5 pages   13 docs
  1 declared document(s), not crawled
```

Lands in `summary.xlsx` as `subsite=(declared)`, `section_path=About CBE`,
`type=PDF`, `hash_basis=etag`. Re-stamping returns the same hash. The 13 crawled
documents and the chrome rule are untouched.

**Left out on your instruction:** `Board of Directors Achievements`, the second
real document in `chrome_dropped`. A decision, not an oversight.

---

## 5. Monitoring

### The finding that reframed it

`generic_crawler/crawler.py` has **no monitoring logic at all** — it produces
`content_hash`, the *input* to change detection, and nothing else. Comparison
lives in `change_signals.yml`, `inventory_sweep.py`, `orch.py` and
`monitor_jobs.py`.

And for CBE circulars, crawling is the wrong tool. The circulars page shows ten
rows behind a "Load more" button, so a crawl walks into a 40-page JS pager:

> **A prefix crawl of `/en/laws-regulations` got 18 of 396 circulars — 4.5% — and
> reported `status: ok`.**

The page's own JavaScript calls an endpoint, found next to
`<span id="totalResults">396</span>` in the markup:

```
GET /api/listing/circulars?pageNo=0&pageSize=500
→ 200, 274 KB, ONE request, 396 results, 2005-03-20 … 2026-08-17
```

### New: `crawler/cbe_crawler.py` + `config/sources/cbe.yml`

`CBECircularsCrawler` reads that API. What it gets that a link walk cannot:

| field | value |
|---|---|
| `customDate` | ISO 8601 publication date — the wrapper documents that it must leave `published_date` None because "a link walk cannot reliably read issue dates" |
| `categories` | the regulator's own taxonomy: Credit Granting (69), Foreign Currency Activities (33), Banking Practices (29) … |
| `itemId` | a Sitecore GUID, **396 unique across 396 rows** |
| `title` | the real title — one circular is served as `circul~1.pdf`, an 8.3 short name that would produce a garbage slug title |

**Decisions:**

* **`doc_path` flat** — `[regulator, source_system, title]`, as MOH. Putting the
  API category in the path would move the folder, and so `disappeared` scoping,
  whenever CBE re-files a circular. The taxonomy lives in `category` and
  `extra_meta`, which is free because the default identity is
  `(document_url, doc_path, title)`.
* **`content_hash` = `itemId|customDate|url|title`.** Catches a retitle, a re-date,
  a renamed file. Cannot catch a PDF swapped silently behind an unchanged url —
  nothing in the JSON moves for that, and that case is the sweep's job via ETag.
  Splitting it keeps the crawler at one request instead of 396.
* **A short answer raises.** The whole reason the file exists is that a run
  quietly returning 18 of 396 reported success. It also judges the **content
  type**, because CBE answers a refused request with HTTP 200 and an HTML page.
* **`stamp_content_hashes` at the single exit** as a backstop — it never
  overwrites, so the better hash survives.

```
CBE circulars API returned 396 of 396 item(s)
documents: 396  |  unique urls 396/396  |  unique hashes 396/396
no content_hash: 0  |  no published_date: 0
```

Run twice: **identical** — 0 `modified`, 0 new or disappeared. Built through the
real factory too: `build_regulator_crawler(cbe.yml)` returns a `CompositeCrawler`
and yields the same 396.

### `config/change_signals.yml` — three layers, all measured

1. **Circulars — the crawl IS the signal, and the crawl is one request.** A probe
   step would be pure overhead, the same reason MOH left `CHEAP_PROBE_SOURCES` on
   2026-08-17.
2. **Files — `/-/media/` PDFs answer a probe honestly.** ETag + Last-Modified
   present and stable, so `confirm: false`. Covers the laws and regulations-book
   PDFs the API does not list. Note for the sweep: HEAD is 403, GET is 200 —
   `inventory_sweep` already uses `requests.get(stream=True)`, so no change is
   needed, but do not "optimise" it to a HEAD.
3. **HTML pages — nothing cheap exists, and the obvious field is a trap.** No
   `Last-Modified`, no `ETag` on any of five pages tested. Every page *does* carry

   ```html
   <span class="newsupdateddata"> Last Updated: 23 Mar 2023 </span>
   <p><span>This page was last updated 23 Mar 2023</span></p>
   ```

   and it reads **23 Mar 2023 on all of them** — `/en/laws-regulations`,
   `/en/governance`, `/en/aml-cft`, `/en/laws-regulations/laws/banking-laws` —
   a build-time constant three years stale (`robots.txt` carries
   `Last-Modified: 25 Mar 2023`, the same deployment).

   **This is the CMA lesson inverted.** CMA's `Last-Modified` returned the current
   time and reported 1,134 false changes — loud, and someone fixed it. This would
   report zero changes forever, silently. A signal that never fires is
   indistinguishable from a site that never changes. These pages are `crawl`,
   compared on `content_hash` of visible text.

### Not wired into `jobs/monitor_jobs.py`, on purpose

ONBOARDING's rule: *"New regulator → workbook. Nobody has ever read this crawler's
output."* CBE has no baseline and nobody has reviewed a row, so it does not belong
on the direct-to-MSSQL path yet. `CRAWL_AS_SIGNAL` is where it goes **after** a
workbook is read and promoted.

### Also out of scope by decision

`/api/listing/news` returns 781 items. News stays out of the library.
`/api/listing/laws`, `regulations`, `tenders` and `auctions` all 404 — so CBE is a
**split source**: the API for circulars, the generic crawl for everything else.

---

## Verification across the whole changeset

```
venv/Scripts/python.exe generic_crawler/regression_check.py
→ All 7 regulators unchanged.   exit 0, no --save-baseline needed
```

Two risks specifically checked on the `crawler.py` change:

* **No re-versioning storm.** `content_hash` hashes **text**, never html, so
  rewriting attributes cannot move a hash. Nothing re-classifies as `modified`.
* **No baseline regeneration.** `regression_check.fingerprint()` records shape,
  breadcrumb, section_anchor, `content_text_len`, `n_links`, `n_doc_links`,
  `doc_section_paths` — the html string is not among them.

---

## Still open

* **Preflight uses `urllib`**, which CBE's WAF rejects with a 200-status page — so
  it approves every path, including a typo. Move it to `requests`.
* **CBE is not in `regression_check.py`'s `SEEDS`.** That file's own comment: *"a
  site with no entry is a site nobody will notice you broke."* Adding it is
  instructed; the `calibrate_*` lists are discretionary and CBE adds no new
  archetype (`generic` shape is already covered by MISA, SDAIA and MHRSD).
* **Identity for CBE circulars is still the default.** `MONITORING.md` §2 says
  identity is chosen per source, defaulting to `(document_url, doc_path)`, and
  that **no config overrides it today** — so `itemId` would be the first use of a
  built-but-unused mechanism. The same doc names the exact failure it would fix:
  *"the default leaves a re-issued circular at a new url reading as one new
  document plus one disappearance"*, and CBE's circular urls embed the date. Cheap
  to change now, a migration after promotion.
* **`workbook export cbe` needs `OPENROUTER_API_KEY`** (`Orchestrator.__init__`
  builds an `LLMAnalyzer`). Environment prerequisite, unrelated to the crawler.
* **`crawler.py`'s header is stale** — it still calls itself "a TEST TOOL … fully
  SEPARATE from the live pipeline", but it is imported by
  `generic_crawler_wrapper.py`, `fingerprint.py` and `moh_crawler.py`.
* **`cbb_monitoring_crawler.py:76`** `_make_absolute()` does the same job as the
  new `absolutize_html()`, in bs4 instead of regex.

---

# How to run it for CBE

Two commands. They cover different halves of the site and do not overlap: the
runner walks the HTML sections, the API crawler takes the circulars.

## 1. The sections, plus the Procurement PDF

One line:

```
venv/Scripts/python.exe generic_crawler/run_site.py --seed https://www.cbe.org.eg/en/ --subpaths governance,laws-regulations,aml-cft,financial-stability,monetary-policy,payment-systems-and-services,cybersecurity,financial-technology,sustainability/principles-and-regulatory-framework --out output/cbe_test_1 --scope prefix --max-pages 150 --documents "About CBE :: Procurement :: https://www.cbe.org.eg/-/media/project/cbe/page-content/rich-text/about-cbe/procurement.pdf"
```

Notes on the arguments:

* **Nested sub-paths work** - `sustainability/principles-and-regulatory-framework`
  is one entry, and its output directory is slugified to
  `sustainability-principles-and-regulatory-framework`.
* **No trailing slash needed** on `--seed`; `https://www.cbe.org.eg/en` and
  `.../en/` both resolve identically.
* **`--max-pages` is PER SECTION**, not for the run. Nine sections at 150 is a
  ceiling of 1,350 pages; the real total here is 100, so nothing is capped.
* **`--scope prefix`** is already the default. Passing it is harmless and explicit.

## What to expect

While it runs, in this order: the declared document is parsed and stamped first
(so a malformed `--documents` entry costs a second, not an hour), then the
preflight, then one block per section.

```
Declared documents (1)
  Procurement  [etag]
    About CBE  <-  .../about-cbe/procurement.pdf

Preflight 9 section(s)
  ok    https://www.cbe.org.eg/en/governance  (200 GET)
  ...

[1/9] https://www.cbe.org.eg/en/governance  ->  output\cbe_test_1\governance
  ok: 12 pages, 5 documents
```

Then the roll-up. **This is the actual output of that command:**

```
======================================================================
OK  -  9 section(s), 153 documents
  ok  governance                                     12 pages    5 docs
  ok  laws-regulations                               17 pages   55 docs
  ok  aml-cft                                         5 pages   13 docs
  ok  financial-stability                            19 pages   27 docs
  ok  monetary-policy                                10 pages   13 docs
  ok  payment-systems-and-services                   27 pages   37 docs
  ok  cybersecurity                                   4 pages    0 docs
  ok  financial-technology                            5 pages    2 docs
  ok  sustainability/principles-and-regulatory-...    1 pages    0 docs   [thin - ...]

  2 document(s) appear under more than one section - see the duplicates sheet.
  1 declared document(s), not crawled
```

100 pages, 152 crawled documents + 1 declared = **153**. Exit code **0**.

### On disk

```
output/cbe_test_1/
  governance/                                        pages.json  pages.xlsx  html/
  laws-regulations/                                  pages.json  pages.xlsx  html/
  aml-cft/                                           ...
  financial-stability/
  monetary-policy/
  payment-systems-and-services/
  cybersecurity/
  financial-technology/
  sustainability-principles-and-regulatory-framework/
  summary.json
  summary.xlsx     <- sheets: subsites | documents | duplicates
```

Read `summary.json` first, then a section's own `pages.json` if a number looks
wrong. Each section keeps its own `status`, which is the point of one directory
per section.

## How to read the two things this run flags

Neither is an error. Both are the report doing its job and handing you a judgement.

**The `[thin]` note on `sustainability/principles-and-regulatory-framework`.**
1 page, 0 documents. The note says *"check this sub-path exists; some sites answer
200 for any path"* - and on checking, **it does exist**: the recorded page is
titled "Principles and Regulatory Framework". It is a real leaf page with no child
pages and no attachments. So the note fired correctly and the answer is "fine, it
is just thin". This is exactly why it is a note and not a status: the crawler
cannot tell a real thin page from a soft-404, and on CBE neither can the preflight,
so a person decides.

Contrast `cybersecurity`: 4 pages, 0 documents, **no** note - more than one page
means it is plainly a real section that happens to attach no files.

**The 2 duplicate documents.**

```
cbe-law-no,-d-,-194-of-2020.pdf   laws-regulations, payment-systems-and-services
anti-terrorism-law.pdf            aml-cft, laws-regulations
```

Both are genuine cross-listings - CBE Law 194/2020 really is relevant to payment
systems, and the anti-terrorism law really does belong under both AML/CFT and
Laws. So here the answer is "leave them": they are two places in the library, and
the DB agrees, since `document_exists_by_url(url, category)` is category-scoped
for precisely this. The report exists because the *other* cause of a duplicate is
overlapping prefixes, which double-counts and makes sections propose each other's
documents as withdrawn. Read the pair and decide which you are looking at.

## 2. The circulars

Separate command, because they come from the API rather than a crawl - 396
documents in one request instead of the 18 a crawl of `/en/laws-regulations`
reaches:

```
venv/Scripts/python.exe -m tools.workbook export cbe
```

Needs `OPENROUTER_API_KEY` in `.env`. Without it, this proves the crawler with no
key and no database:

```
venv/Scripts/python.exe -c "from crawler.cbe_crawler import CBECircularsCrawler; d=CBECircularsCrawler().fetch_documents(); print(len(d), '|', d[0].published_date, '|', d[0].title)"
```

Expect `396 | 2026-08-17 | Circular dated 17 August 2026 regarding promoting
Financial Literacy for Individuals and MSMEs`. Anything short of 396 raises rather
than returning quietly.

## Then run both a second time

**One run is never a result.** Run 1 reports everything as new whether the crawler
is right or wrong.

* Command 1 again, same `--out`: expect the same 9 statuses and the same counts.
  A document count that moves between two identical runs is the finding.
* Command 2 again: expect `check` to report everything `unchanged` with **zero**
  new version rows. Verified already at the crawler level - two consecutive
  `fetch_documents()` calls produce byte-identical hashes for all 396.

## Verify the HTML fix while you are there

Open any file under `output/cbe_test_1/<section>/html/`. Images should render and
links should go to `www.cbe.org.eg`, not `file:///D:/...`. Arabic should read
correctly rather than as mojibake.

Pages will be **unstyled** - that is by design, `<style>` and `<link>` are stripped
as page chrome - and viewing needs an internet connection, because images load
from the live site rather than being downloaded.
