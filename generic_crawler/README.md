# Generic Crawler — standalone (the present "dynamic" crawler)

A **generic** web crawler for regulator sites. Give it **one start URL** and **one
scope** setting; it discovers the documents on that site — no per-site code.

> New here? Read **`CRAWLING_OVERVIEW.md`** first — it explains what this UC is trying
> to do, the history of approaches, and why this generic crawler is our current choice.

It is **self-contained** and does **not** touch the live pipeline, configs, or DB — it
only writes into `output/standalone_crawler/`.

---

## What it does 
Opens the URL in a real browser → expands menus / reads frames / scrolls to load lists
→ walks every level → on each page records the **breadcrumb (folder path)**, the
**page content (HTML + text)**, and every **document link (PDF, DOCX, "Download"
buttons)** with its **title** — then writes it all to Excel + JSON.

---

## Run it (recommended: the Streamlit UI)
Use the **venv** python (it has Playwright + Streamlit installed):
```
venv/Scripts/python.exe -m streamlit run generic_crawler/app.py
```
Paste a start URL, choose a scope, set the page cap, press **Start**. Watch it crawl
live, inspect the **Pages** and **Documents** tabs, download the Excel.

## Run it (command line)
```
venv/Scripts/python.exe generic_crawler/crawler.py \
  --url https://www.secp.gov.pk/laws/acts/ \
  --out output/standalone_crawler/secp_acts \
  --scope prefix --max-pages 100 --max-depth 4
```

---

## Choosing the scope (the only per-site decision)
| Scope | Use when | Example |
|---|---|---|
| `breadcrumb` | menu/tree site that shows a breadcrumb trail | SAMA rulebook, CMA |
| `prefix` | documents live under one URL folder | SBP `/circulars`, SECP `/laws/acts` |
| `host` | whole domain (always pair with a page cap) | broad sweeps |

Rule of thumb: **menu/tree → `breadcrumb`; list-of-documents → `prefix`.**

---

## What you get (the output schema)

Everything lands under `--out`:

### `pages.xlsx` → sheet **pages** (one row per web page visited)
| Column | Meaning |
|---|---|
| `section_path` | the breadcrumb / folder trail — **this is how we replicate the site's structure** |
| `title` | page title |
| `url` | page URL |
| `depth` | how many levels deep from the start URL |
| `status` | a status chip if the page shows one (e.g. "In-Force") |
| `n_pdfs` | how many document links were on this page |
| `pdf_links` | those document links |
| `text_len` | length of the extracted text |
| `html_file` | path to the saved full HTML for this page |
| `text_preview` | first part of the readable text |

### `pages.xlsx` → sheet **documents** (one row per document found) — *usually the important one*
| Column | Meaning |
|---|---|
| `title` | the document's title (from the row/card next to the link, not the button label) |
| `doc_url` | link to the actual file/download |
| `type` | PDF / DOCX / DOC … |
| `found_on` | the page we found it on |
| `section_path` | the folder trail it sits under |

### Also written
- `pages.json` — the complete records, including full HTML + text for every page.
- `html/<slug>.html` — the full readable HTML of each page, one file each.

### Read `status` before you read any count

`pages.json` and the `done` event both start with the run's outcome, and the exit
code follows it. A crawl that reached the end is not the same as a crawl of the site.

| status | means | exit |
|---|---|---|
| `ok` | pages recorded, nothing blocked, nothing cut short | 0 |
| `blocked` | a bot-protection wall answered instead of the site — no count from this run means anything, and the challenge page is **not** recorded | 1 |
| `zero` | no pages recorded. A failed extraction, not an empty site — check shape and scope | 1 |
| `incomplete` | pages recorded, but the walk was cut short (page cap, dead browser, seed never loaded). **Not** an error: the rows are worth keeping, they are just not coverage. `stopped` says why and `resume` says where | 0 |

> **How this maps to the library:** `section_path` → the library's `category`/structure;
> `doc_url` → `document_url`; `title`, `type`, `found_on` → the matching library fields.
> See `CRAWLING_OVERVIEW.md` §2 for the full target schema.

---

## Which tab has my documents?
For **list sites** (SBP circulars, SECP acts) the real files are in the **Documents**
sheet/tab — the Pages sheet will mostly show the listing pages. For **tree sites**
(SAMA) the Pages sheet is the structure and the Documents sheet holds the attached PDFs.

---

## Site types it handles automatically (no config)
- Modern menu/tree sites (SAMA) — expands the sidebar, walks every level.
- Old `<frameset>` / SharePoint sites (CMA) — reads content from inside frames.
- Lazy-loading JS apps (SBP) — scrolls to trigger rendering; snapshots before any
  client-side redirect can steal the content.
- URL pagination (`/circulars/P60`) **and** in-page JS pagination (DataTables
  "Show N entries" + page 1/2, SECP).
- "Download" buttons / WordPress Download Manager links that aren't plain `.pdf`.

A page that errors is skipped, not fatal — the run continues.

---

## Known limits (be honest with stakeholders)
- **Scope is manual** — you pick breadcrumb/prefix/host per site (2-second decision).
- **Structured fields** (exact reference number, hijri date) aren't parsed as precisely
  as the old hand-tuned configs — titles/dates/links are captured, fine detail is a
  later refinement.
- **Slow servers** (e.g. SBP) make crawls slow — that's their server, not the code.
- **Login / CAPTCHA / private-API** sites need a small custom nudge.
- **Orphan pages** (linked from nowhere) can't be found by link-walking — those need a
  sitemap or the site's search index.

---

## One-time setup (already done in this repo's venv)
```
venv/Scripts/python.exe -m pip install streamlit xlsxwriter
venv/Scripts/python.exe -m playwright install chromium chromium-headless-shell
```

## Files in this folder
- `crawler.py` — the crawler engine (heavily commented; read top-to-bottom).
- `app.py` — the Streamlit test UI.
- `CRAWLING_OVERVIEW.md` — the onboarding narrative (start here).
- `README.md` — this file (how to run + schema).
