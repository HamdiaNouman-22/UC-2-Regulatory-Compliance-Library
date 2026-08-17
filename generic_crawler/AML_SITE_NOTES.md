# aml.gov.sa — what the generic crawler needed, and why it is opt-in

Test target: `https://www.aml.gov.sa/en-us/Pages/RulesandRegulations.aspx`
(Anti-Money Laundering Permanent Committee, Rules and Regulations).

Goal was to check whether the generic crawler could capture, with no per-site
crawler code: the **document title**, the **PDF link**, and the **correct
section path** — `Anti-Money Laundering Committee > Rules and Regulations`
followed by the on-page group (`Laws and Regulations` / `Rules and Instructions`).

**Result: it works, but only after three fixes.** Out of the box it failed all
three requirements. All three fixes are switched on *per host* so no other
regulator changes — see [Why it is opt-in](#why-it-is-opt-in).

---

## Run it

```
venv/Scripts/python.exe generic_crawler/crawler.py ^
  --url "https://www.aml.gov.sa/en-us/Pages/RulesandRegulations.aspx" ^
  --out output/standalone_crawler/aml_final ^
  --scope prefix --max-pages 5 --max-depth 2
```

No flag is needed — the host is in `SITE_PROFILES`, so the fixes apply
automatically. Takes ~30 seconds.

Output: 1 page, **11 PDFs**, 4 under Laws and Regulations, 7 under Rules and
Instructions. All 11 verified to return HTTP 200, `application/pdf`, real `%PDF`
magic bytes, 247 KB – 1.99 MB.

> Use `--scope prefix`, not `breadcrumb`. Both give the same 11 documents (they
> all sit on the seed page), but `breadcrumb` also *visits* every nav-menu page
> to reject it, at ~26 s each — a 12-minute run for the same result. `--max-pages`
> counts **recorded** pages, so with 1 in-scope page the cap never trips and the
> queue drains fully.

---

## The three bugs

### 1. Every saved HTML file was 0 bytes

`JS_MAIN_CONTENT` treated `<form>` as page chrome and removed it. This site is
SharePoint, which wraps the **entire page** in `<form id="aspnetForm">` — 88,637
characters of it. Stripping the form deleted the whole document.

Measured before: `html len: 51, text len: 0`. After: **3,606 chars of text**.

Fix: unwrap content-bearing forms (promote children, drop the wrapper); still
remove small ones, which are the genuine search/subscribe widgets.

### 2. `--scope breadcrumb` silently behaved like `--scope host`

The breadcrumb reader took only `<a>` elements. On this page:

```html
<span style="display:none"><a href="/ar-sa/...">AML</a></span>   <!-- hidden Arabic home -->
<span><a href="/en-us/Pages/home.aspx">Anti-Money Laundering Committee</a></span>
<span class="breadcrumbCurrent">Rules and Regulations</span>      <!-- not a link -->
```

So it produced `['AML', 'Anti-Money Laundering Committee']` — missing the page
you are actually on, and including a crumb nobody can see.

The crawler takes the **last** crumb as the section anchor, so the anchor became
`anti-money laundering committee`, which is true on *every* page of the site.
Scope stopped constraining anything and the crawl wandered into News, About,
Media Center and the video library. Anchor is now `rules and regulations`.

Fix: read `a` plus current/`aria-current`/active nodes, filtered by a
**rendered-box** test.

> Note for whoever touches this next: checking
> `getComputedStyle(node).display !== 'none'` does **not** work. The `display:none`
> is on the *parent* span, so the anchor's own computed display is still `inline`
> and the hidden crumb survives. Use `getClientRects().length > 0`.

### 3. The on-page group headings were never captured

`section_path` came from the breadcrumb alone, so the `<h3>`s that visibly split
this page — `Laws and Regulations` and `Rules and Instructions` — did not appear
anywhere in the output. Each link now records the nearest heading preceding it in
document order, appended by `doc_section_path()` and skipped when it merely
repeats a crumb.

---

## Why it is opt-in

The crawler is shared by every regulator, so each fix was measured old-vs-new on
the other seeds before being kept. **One of them broke CMA**, and the heading
feature was wrong on three sites:

| Regulator | detected shape | effect of the fixes if applied globally |
|---|---|---|
| SAMA rulebook | generic | no change |
| SAMA circulars | **table** | never reaches this code (`strategies.py`) |
| CBB | **tree** | never reaches this code (`strategies.py`) |
| SECP acts | **table** | never reaches this code (`strategies.py`) |
| SBP circulars | generic | no change |
| **CMA regs** | generic | **breadcrumb changed, content 6,462 → 12,192 chars** |

CMA's trail changed from `Capital Market Authority > Laws & Regulations` to
`Home > Laws & Regulations > Implementing Regulations`, moving its scope anchor
from the parent section to the current page — i.e. changing which pages CMA
crawls at all.

The heading grouping was worse. What it would have appended as a section level:

- **SECP** — `25/05/2026`, `27/07/2026` (the headings are dates)
- **CBB** — `FOLLOW US`
- **CMA** — the document's own title, e.g. `Corporate Governance Regulations`

Only on AML are the headings a real grouping.

So all four switches live in `SITE_PROFILES` at the top of `crawler.py`, keyed by
host, defaulting to off:

```python
SITE_PROFILES = {
    "www.aml.gov.sa": {
        "breadcrumb_current": True,   # current crumb is a <span>, hidden Arabic crumb
        "unwrap_forms":       True,   # SharePoint aspnetForm wraps the page
        "sharepoint_main":    True,   # no main/#content on the page
        "group_headings":     True,   # <h3>s are a real section grouping here
    },
}
```

`--group-headings` also exists as a CLI flag for trying it on another site.

**Re-verified after gating:** all six other regulators report identical
breadcrumb and content to before, CMA included. AML is the only site that changes.

### One change was reverted

Document harvesting had been moved to *after* the scope check, so out-of-scope
pages contributed no PDFs. That is arguably more correct — a PDF found on a News
page otherwise gets News's breadcrumb as its `section_path` — but it altered
behaviour for **every** regulator and could silently drop documents on sites that
cannot be fully re-tested. Reverted to the original order. AML is unaffected
because it uses `--scope prefix`, where out-of-scope pages are never enqueued.

---

## Guarding it: `regression_check.py`

The CMA breakage was invisible from the AML work — nothing about testing AML
would have surfaced it. So the check is now a repo tool:

```
# before committing any crawler change
venv/Scripts/python.exe generic_crawler/regression_check.py

# after a DELIBERATE change to what we extract, accept the new output
venv/Scripts/python.exe generic_crawler/regression_check.py --save-baseline
```

Exit 0 = all seven regulators unchanged; exit 1 = something moved, with a
per-field `was:`/`now:` diff.

**It replays frozen copies of the seed pages, not the live sites.** A regression
check has to answer "did *my change* alter the output?" — hitting live sites
cannot, because a regulator publishing a new circular moves the numbers and the
check cries wolf. `--save-baseline` saves each seed's rendered DOM into
`regression_pages/` (~1.1 MB, committed on purpose so the check runs offline for
everyone) and a `<base href>` is injected so relative links still resolve.

Guarded per seed: detected shape, breadcrumb, **section anchor**, main-content
text length, link and document-link counts, the resulting `doc_section_path`s,
and the active profile. The section anchor matters most — it decides which pages
breadcrumb scope keeps, so a change there silently redraws the crawl boundary.

Two things learned building it, both already handled:

- The baseline is recorded from a **replay**, not from the live page. Reopening a
  saved DOM re-runs its scripts, so the replay is never byte-identical to the
  live load (measured: a link count off by one, text lengths off by a few
  hundred characters). Baselining live numbers made the first check fail on four
  sites for no real reason.
- Verified it actually catches things: flipping `unwrap_forms` to `True` by
  default — the exact CMA-breaking change — was caught on all six non-AML seeds
  with exit 1.

Add a seed to `SEEDS` when you onboard a regulator. A site with no entry is a
site nobody will notice you broke.

## Limits

- Verified against each regulator's **seed page**, which proves the extraction
  logic is unchanged — not a full multi-page crawl of each site.
- `strategies.py` (tree/table) was not touched and imports nothing from
  `crawler.py`, so SAMA/CBB/SECP are structurally out of reach of these changes.
- The crawler records document **links**; it does not download the files. PDF
  fetchability was checked with a separate throwaway script.
- `SITE_PROFILES` is marked TEMPORARY — it is a host-keyed dict of booleans,
  shaped to port directly into the planned per-regulator YAML config.
