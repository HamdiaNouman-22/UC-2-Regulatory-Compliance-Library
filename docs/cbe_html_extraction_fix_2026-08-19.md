# CBE HTML capture — chrome, tables, images

**Date:** 2026-08-19
**Files changed:** `generic_crawler/crawler.py`, `processor/LlmAnalyzer.py`,
`generic_crawler/regression_baseline.json`
**Trigger:** the CBE workbook's stored HTML opened with an accessibility widget and
closed with a fax number; tables read as prose; images did not render.

---

## What was actually wrong

Three separate defects, only one of which is about CBE.

### 1. The whole page was captured, not the document  *(CBE only)*

`JS_MAIN_CONTENT` picks a content container with
`main, [role="main"], article, #content, .content, #main`. **CBE has none of them.**
So it fell through to `<body>` on **94 of 94** HTML pages — the root element of every
stored `document_html` is `div.site-container.main`.

That alone would be survivable if the junk list could then strip the furniture. It
could not: CBE marks every block on the page as `<section class="cbe-component">`,
not `<header>` / `<nav>` / `<footer>`.

One detail worth keeping: the breadcrumb survived because **CBE's container class is
`breadcrumbs` — plural**, and the junk list holds `.breadcrumb` and `.bread-crumb`. A
CSS class selector does not match a longer class token, so the trail sailed straight
through on all 93 pages that have one.

Measured across the 94 pages in `output/workbooks/cbe.xlsx`:

| chrome block | pages carrying it |
|---|---|
| `.Image-Container` (hero background) | 94 |
| `.breadcrumbs` (the trail) | 93 |
| `.cbe-accessibility-toolbar` ("A A contrast") | 92 |
| `.pagenote` ("This page was last updated …") | 86 |
| `.most-searched` ("Most Searched: Inflation Targets") | 81 |
| "Get in Touch" contact block | 13 |

### 2. Tables were flattened into prose  *(every regulator)*

`LLMAnalyzer.normalize_input_text` ran `re.sub(r'\s+', ' ', text)` on the HTML branch.
That collapses **newlines as well as spaces**, so the entire document arrived at the
model as a single line. CBE's licensed-bank table reached the analyzer as:

```
Licensed Banks # Bank Code Bank Name 1 AAIB Arab African International Bank 2 BDC
Banque Du Caire 3 BM Banque Misr 4 BOA Bank of Alexandria ...
```

Nothing downstream can recover which value sat in which column.

**Second, larger consequence.** `split_text_into_chunks` splits on `'\n\n'`. With every
newline already removed, it always received exactly one paragraph — so paragraph-aware
chunking has never actually run on an HTML document. Measured on the seven frozen
regression pages, the old normalizer produced **1 paragraph on all seven**.

### 3. Images

Content images were already stored with usable urls. The broken ones are lazy-load
placeholders: CBE ships `src="data:image/jpeg;base64,/some_lqip_in_base_64=="` — a
literal placeholder string, not a decodable image — and puts the real url in a sibling
`<source srcset>`. 97 such stubs, **94 of them inside the hero banner** (chrome).

Separately, `get_text()` ignores attributes, so an image's `alt` text was discarded
entirely.

---

## What changed

### `generic_crawler/crawler.py`

Three new profile keys, **all default-off**, so every host not named is byte-identical
to before:

| key | meaning |
|---|---|
| `drop_selectors` | extra chrome selectors removed from the content clone, for sites whose furniture is `<section class="…">` rather than `<header>`/`<nav>`/`<footer>` |
| `stop_at_headings` | heading texts that mark the END of the document; the heading and **every node after it in document order** are removed |
| `fix_lazy_images` | promote `data-src` / `<source srcset>` onto `<img src>`, resolve relative urls, drop images left pointing at nothing |

and a `www.cbe.org.eg` profile using them.

**Deliberately not dropped:** the banner's `<h1 class="other-hero-title">` (the page's
real title, present on all 94) and `<span class="newsupdateddata">`
("Last Updated: 23 Mar 2023", also 94/94). Dropping the whole
`[data-comp-name="breadcrumbs"]` section would have been one selector instead of three
and would have thrown both away.

"Get in Touch" is matched on **heading text, not a selector**, because only 11 of the
13 pages that have it tag it `id="contacts"`.

### `processor/LlmAnalyzer.py`

- `_tables_to_text` — every `<table>` becomes one line per row, cells joined by `' | '`.
  Innermost table first, so an outer table reads the inner one's rendered text instead
  of re-flattening it.
- `_images_to_text` — keeps `alt` as `[image: …]`; drops the tag either way.
- Block boundaries (`p`, `li`, `h1`–`h6`, `tr`, …) are marked before extraction.
- `re.sub(r'\s+', ' ')` → `re.sub(r'[^\S\n]+', ' ')`: collapse runs of spaces, keep
  line structure.

---

## Verification

**Live browser run** against the real POS page, old profile vs new
(this is the actual `JS_MAIN_CONTENT` code path, not a simulation):

```
OLD  html 17,296 chars   toolbar PRESENT  breadcrumb PRESENT  most-searched PRESENT
                         Get in Touch PRESENT  last-updated PRESENT  lqip stub PRESENT
                         3 imgs, 1 a base64 placeholder, 2 root-relative

NEW  html  7,355 chars   toolbar gone     breadcrumb gone     most-searched gone
                         Get in Touch gone  last-updated gone  lqip stub gone
                         <table> PRESENT
                         2 imgs, both absolute https://www.cbe.org.eg/-/media/...
```

**Simulated over all 94 stored CBE pages:** 18,242 chars of chrome removed, median 93%
of the text kept, **0 pages reduced below 60 characters** — nothing was over-stripped.

**Text the analyzer now receives for the POS page** (was one 1,191-char line, roughly a
sixth of it chrome):

```
POS

Last Updated: 23 Mar 2023

Introduction

A point-of-sale machine is an electronic payment accepting device that allows ...
...
Licensed Banks

# | Bank Code | Bank Name
1 | AAIB | Arab African International Bank
2 | BDC | Banque Du Caire
...
9 | ABC | ABC Bank

Statistics
```

**Regression, seven frozen regulator pages** (`generic_crawler/regression_check.py`,
offline replay): all seven changed on the `profile` key only — no breadcrumb, html
length, text length or link count moved. The new keys were then written into
`regression_baseline.json`; re-running reports `ok` for six.

Normalizer comparison on the same seven pages:

| page | old chars | new chars | old paragraphs | new paragraphs |
|---|---|---|---|---|
| aml_rules | 5,350 | 5,522 | 1 | 116 |
| cbb | 1,708 | 1,784 | 1 | 63 |
| cma_regs | 7,757 | 8,078 | 1 | 286 |
| sama_circulars | 3,293 | 3,471 | 1 | 32 |
| sama_rulebook | 528 | 547 | 1 | 20 |
| sbp_circulars | 341 | 359 | 1 | 19 |
| secp_acts | 2,361 | 2,487 | 1 | 43 |

Character counts rise 3–5% (the restored line breaks); no content is lost.

`tests/test_js_snippets_parse.py`, `test_generic_crawler_outcome.py`,
`test_scope_prefix.py`: **85 passed**.

---

## Known and NOT fixed

**`output/workbooks/cbe.xlsx` is stale.** It was exported before this change, so its 94
HTML rows still hold the chrome. Re-export before promoting.

The 396 API-sourced circulars are unaffected — they are PDFs and carry no
`document_html`.

**`sama_circulars` replays at `content_text_len` 2,571 against a baseline of 2,831.**
Pre-existing and unrelated: the **original** `crawler.py` produces the same 2,571. The
frozen page re-runs its own scripts on replay (the module's docstring warns about
exactly this), so the number depends on what loads from the live host. Left unaccepted
rather than papered over.

**The `pdf_text` branch has the identical whitespace defect.** `normalize_input_text`
CASE 1 still runs `re.sub(r'\s+', ' ', content)`, which makes the `\n{3,}` substitution
on the next line dead code and flattens every PDF to one line too. Not changed here:
it would alter the analysis input for ~8,700 existing PDF-sourced documents, which is a
decision, not a cleanup.

**Six `<img src="">` on `/en/governance/risk-management-information-security`** are
empty in CBE's own page source. `fix_lazy_images` drops them; they are not recoverable.

**Images that carry the information cannot be read by any text pipeline.** CBE's POS
"Statistics" section is a bar chart with `alt=""`, so that heading is now followed by
nothing. Reading it needs OCR of the image, which is a separate decision.

**Re-analysis.** Any CBE HTML document already analysed was analysed against the old
text. Re-crawling changes `content_hash`, which will mark them `modified` and re-run
analysis — expected, and the point of the fix.
