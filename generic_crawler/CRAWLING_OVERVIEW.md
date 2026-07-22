# Regulatory Compliance Library — Crawling Overview

**Audience:** anyone new to this use case (UC). No prior context needed. This file
explains *only the crawling part* of the UC — what it is, why we do it, what we
tried, and what we do now.

---

## 1. What are we actually trying to do?

We are building a **Regulatory Compliance Library** — a searchable knowledge base of
the rules that financial regulators publish (laws, circulars, notifications,
guidelines, frameworks, etc.).

Regulators (SAMA in Saudi Arabia, SBP & SECP in Pakistan, CBB in Bahrain, CMA, …)
publish all of this on their **public websites**. Nobody hands us a neat database of
their documents — the documents live scattered across web pages, PDFs, and menus.

**Crawling** is the first step of the whole UC: a program visits a regulator's
website, walks through it like a person would, and pulls out:

1. **Every document** the regulator publishes (the PDF/Word file *and* the page text).
2. **Everything attached to each document** — its title, date, reference number,
   category, which section it lives under, the link, etc. (the "metadata").
3. **The structure** — which document sits under which section/folder, so the library
   mirrors how the regulator actually organises its material.

Everything downstream (search, AI answers, compliance mapping) depends on this step
being complete and accurate. **If the crawler misses documents, the whole library has
holes.** That is why we care so much about coverage.

### Our two guiding goals for crawling
> 1. **Replicate the regulator's structure as closely as possible** (the section /
>    folder hierarchy — e.g. `Regulatory Sandbox › Guidance Notes › Stage 1`).
> 2. **Get *all* documents and everything linked with them** (files + metadata),
>    with nothing silently dropped.

---

## 2. What we want out of it (the target schema)

Every document we discover should end up as one row with these fields (this is the
schema the live library already uses — see `output/dynamic_crawler/.../docs.json`):

| Field | Meaning |
|---|---|
| `regulator` | e.g. SAMA, SBP, SECP |
| `source_system` | e.g. "SAMA RULEBOOK" |
| `category` | the section it belongs to (from the site's structure) |
| `title` | the document's title |
| `document_url` | link to the actual file/page |
| `urdu_url` | link to the other-language copy, if any |
| `published_date` | issue date |
| `reference_no` | the regulator's circular/act number |
| `department` / `year` | extra metadata when available |
| `source_page_url` | the page we found it on |
| `file_type` | PDF / DOCX / HTML … |
| `document_html` | the page's readable content |
| `content_hash` / `fingerprint` | used to detect duplicates and changes |
| `extra_meta` | anything else useful (status, hijri date, original-PDF link…) |

The **structure** goal shows up as the `category` / section-path field: we record the
breadcrumb/folder trail so we know *where* each document sat on the site.

> The test crawler in this folder produces a **simplified version** of this schema
> (see `README.md`), designed to prove we can reach and capture everything. Mapping
> those columns onto the full library schema above is the next step after crawling.

---

## 3. The journey — what we tried, in order

Crawling regulator sites is hard because **every website is built differently** and
there is no standard for where documents live. Our approach evolved:

### Approach 1 — One hand-written crawler per regulator  *(folder: `crawler/`)*
We wrote a dedicated Python script for each regulator/section
(`sama_rulebook_crawler.py`, `secp_crawler.py`, `cbb_crawler.py`, …). Each one knew
that *specific* site's layout — which menu to open, which HTML tags hold the title,
etc.

- ✅ **Very accurate** for the site it was built for.
- ❌ **Doesn't scale.** ~5,000 lines of code for a handful of sites. Every new
  regulator = a new script written by a developer. When a site changes its design,
  the script breaks and needs fixing by hand.

### Detour — Firecrawl (a hosted crawling service)
We tried an off-the-shelf service (Firecrawl) to avoid writing crawlers.

- ❌ Its automatic crawl **couldn't see JavaScript menus** (it came back nearly empty
  on the SAMA rulebook, whose menu is built by JavaScript).
- ❌ The plan was **rate-limited** (~3 requests/minute), far too slow for full sites.
- ✅ Its single-page "scrape" worked, but that still left the hard part — *discovering*
  every page — unsolved.

So Firecrawl alone wasn't enough.

### Approach 2 — Config-driven engine + LLM onboarding  *(folder: `dynamic_crawler/`)*
Instead of a full script per site, we built **one generic engine** driven by a small
**config file per site** (e.g. `config/regulators/sama.finance_sector.yml`). The
config declares "the menu is this selector, the title is that tag, the date matches
this pattern." An LLM "onboarding" step could even *propose* that config by inspecting
the site (`dynamic_crawler/auto/`, the `generated/.../adapter.py` files).

- ✅ Much less code than a full script per site; a diff harness proved the engine could
  reproduce the old per-regulator crawler's output exactly.
- ⚠️ Still needs a **config (a set of CSS selectors) per site**, so a new regulator
  still needs setup, and the LLM's proposed selectors aren't always right.
- ⚠️ More moving parts (configs, adapters, validation) — harder for a newcomer to grasp.

### Approach 3 — Generic Playwright crawler  *(this folder: `generic_crawler/`)* — **PRESENT**
A single crawler that needs **no per-site code and no per-site selectors** — just a
**start URL** and one **scope** setting. It drives a real browser (Playwright) and uses
general rules that work across site types: expand menus, walk every level, read frames,
scroll to load lazy lists, click JS pagination, and recognise "Download" buttons.

- ✅ A **new regulator usually works immediately** — you paste a URL and pick a scope.
- ✅ It has learned **reusable skills** (see §6) rather than site-specific hacks.
- ⚠️ It captures links + page text + metadata well, but **structured fields**
  (exact reference number, hijri date, …) are less precise than a hand-tuned config.
- ⚠️ A few genuinely hostile sites still need a small nudge.

---

## 4. Why the present approach is our choice

| | Per-regulator script | Config + LLM engine | **Generic crawler (now)** |
|---|---|---|---|
| New site effort | Write ~500 lines | Write/generate a config | **Paste URL + pick scope** |
| Breaks when site redesigns | Yes, badly | Often (selectors move) | **Rarely (uses general rules)** |
| Handles unknown site types | No | Somewhat | **Yes (frames, SPA, tables…)** |
| Structured-field precision | Highest | High | Medium (good enough to start) |
| Easy for a newcomer to run | No | No | **Yes (a Streamlit button)** |

For the goal — *cover many regulators quickly and get all their documents* — the
generic crawler wins on **coverage and speed of onboarding**. The older approaches
remain useful when a specific site needs pixel-perfect structured extraction; we can
still fall back to a tailored config for those.

---

## 5. How accurate is the present crawler?

Honest, qualitative picture (from testing on SAMA, SBP, CMA, SECP):

- **Document discovery & capture: strong.** With the right scope it reaches nested
  pages many levels deep and captures the documents + their links + page text. It
  found the full SAMA Regulatory Sandbox tree, all SECP acts across paginated tables,
  and SBP circulars with their PDFs.
- **Structure replication: good.** It records the breadcrumb/section path for each
  page, so the folder hierarchy is preserved where the site exposes it.
- **Structured metadata: partial.** Title, date-in-context, category, and links come
  through; very specific fields (exact reference number formats, hijri dates) are not
  yet parsed as cleanly as the hand-tuned configs did.
- **Not magic.** Sites behind logins, CAPTCHAs, or private data APIs still need help.
  And a page it never *links to* can't be found by link-walking alone (that needs a
  sitemap/search index).

**Bottom line:** it is accurate enough to **discover and pull the documents** across
many regulators quickly — which is exactly what the library needs first. Fine-grained
field extraction is an improvement we layer on afterward.

---

## 6. The reusable "skills" (why it handles many sites)

Each tricky site taught the crawler a *general* skill, not a one-off hack:

- **Menu/tree expansion** — opens collapsed sidebars and walks every level.
- **Breadcrumb scoping** — stays inside the section you asked for, even when URLs are
  flat and uninformative (SAMA).
- **Frame reading** — old SharePoint sites hide content in `<frame>`s (CMA).
- **Scroll-to-load** — lazy lists that only render on scroll (SBP).
- **Redirect-proof capture** — some pages render, then redirect away; we snapshot first.
- **Two kinds of pagination** — URL pages (`/circulars/P60`) *and* in-page JavaScript
  tables ("Show N entries", page 1/2 — SECP).
- **Download-button detection** — "Download" buttons / WordPress Download Manager links
  that aren't plain `.pdf` (SECP acts), captured with their real titles.

New regulators reuse these automatically.

---

## 7. The one thing you still choose per site: **scope**

Scope tells the crawler how far to roam from the start URL. It's a setting, not code.

| Scope | Use when | Example |
|---|---|---|
| `breadcrumb` | site has a breadcrumb trail; menu/tree site | SAMA rulebook, CMA |
| `prefix` | documents live under one URL folder | SBP `/circulars`, SECP `/laws/acts` |
| `host` | you want the whole domain (use a page cap) | broad sweeps |

Rule of thumb: **menu/tree site → `breadcrumb`; list-of-documents site → `prefix`.**

---

## 8. How to run it

See **`README.md`** in this folder for exact commands. In short:

```
venv/Scripts/python.exe -m streamlit run generic_crawler/app.py
```
Paste a start URL, pick a scope, press **Start**, watch it crawl, download the Excel.

Read the code in **`crawler.py`** — it is heavily commented and organised top-to-bottom
in the order the crawl actually happens.

---

## 9. Where this fits in the bigger UC

```
   [ CRAWLING ]  ← you are here
        │  discovers documents + metadata + structure
        ▼
   Clean & map to the library schema (§2)
        ▼
   Store (database + files)
        ▼
   Search / AI answers / compliance mapping
```

Crawling is the foundation. Everything else assumes the documents are already
discovered and captured — which is this component's job.
