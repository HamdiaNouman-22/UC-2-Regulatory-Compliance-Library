# SAMA crawler — rebuild brief

Self-contained handover. Everything needed to pick this up cold.

---

## 1. What SAMA is, in scope terms

Three entries on the regulator scope sheet, all on the same host:

| # | Source | Sections in scope | URL |
|---|---|---|---|
| 13 | **SAMA Rulebook** | Laws & Implementing Regulations, Banking Sector, Finance Sector, Payments, Insurance, Consumer Protection, Credit Information, AML/CTF, Money Exchangers, Open Banking Policy, Cyber Risk Control | `https://rulebook.sama.gov.sa/en` |
| 14 | **SAMA Circulars** | Circulars | `https://rulebook.sama.gov.sa/en/sama-circulars` |
| 15 | **SAMA IT Governance Framework** | Governance Framework | `https://rulebook.sama.gov.sa/en/information-technology-governance-framework` |

It is the largest source in the library by an order of magnitude.

## 2. Current state

- **The library holds 651 SAMA documents** (`regulations` where `regulator='SAMA'`,
  `source_system='SAMA RULEBOOK'`).
- An existing crawler works: `crawler/sama_crawler_wrapper.py::SAMACombinedCrawler`, wired in
  `config/sources/sama.yml`. It covers circulars + full rulebook + an unrelated SBP Appendix III
  fetch that has always been bundled with it.
- **The lead is rebuilding it from scratch**, so the existing crawler's output is not wanted. It has
  been removed from the overnight sweep (`benchmarks/run_all_regulators.py`) but nothing is deleted.
- Ten other regulators were crawled successfully — 572 documents — so the surrounding pipeline is
  known-good. SAMA is the gap.

## 3. The problem

**3a. Stored URLs are node IDs, and the site's own feed speaks slugs.**

```
stored in DB : https://rulebook.sama.gov.sa/en/node/10911
feed returns : /en/update-countercyclical-capital-buffer-ccyb-rate
```

Drupal serves the same document at both. Any join on URL between the two fails. This already caused
a false result once: a test reported "22 of 22 documents are new" when several were in fact already
stored under their node-ID URL.

**3b. Change detection does not work for SAMA today.**

`content_hash` is derived from URL + link text. Both stay the same when SAMA replaces a PDF behind an
unchanged link, so an amended document reads `unchanged` forever. Separately, **all 12,487 rows in
`regulations` currently have no `content_hash` at all**, so even that comparison has no baseline.

**3c. SAMA has never been measured for a version signal.**

A teammate built a change-detection layer and measured ten regulators for ETag/version tokens
(`config/change_signals.yml`). **SAMA, CMA, Tadawul and MOCI were not in that set.** Nobody has
checked whether `rulebook.sama.gov.sa` returns a usable `ETag` or `Last-Modified`.

**3d. A tree walk is expensive.** The rulebook is a deep folder hierarchy across eleven sections.
Crawling it to find what changed costs hours; the pipeline OCRs long PDFs page by page, and measured
throughput elsewhere was roughly 30 seconds per document.

## 4. The discovery that should shape the design

**SAMA publishes a native revision feed.** Plain GET, no auth, no browser, no JavaScript:

```
https://rulebook.sama.gov.sa/en/view-revision-updates
  ?f_date=on
  &changed_1[min]=YYYY-MM-DD
  &changed_1[max]=YYYY-MM-DD
  &items_per_page=40
```

It filters on Drupal's `changed` timestamp — exactly "what did SAMA touch between these dates".

Each entry returns:

- **title**
- **slug URL**
- **date shown** (the document's effective date)
- **`book-trail`** — the folder hierarchy, e.g. `Banks | Money changers`, or
  `All Regulated Entities | Banks | FinTechs | Credit Bureaus | Digital Banks`

That last field matters: hierarchy is normally the expensive part of a rulebook crawl to
reconstruct, and the feed hands it over for free.

Confirmed working: 22 entries for 2026-01-01 → 2026-08-11.

Parse with:

```python
re.findall(
    r'<div class="book-detail">\s*\d+\s*\.\s*<a href="([^"]+)"[^>]*>([^<]+)</a>\s*\(([^)]+)\)',
    html)
# plus:  re.findall(r'<div class="book-trail">([^<]*)</div>', html)
```

## 5. What we want to build

A SAMA crawler designed around the feed rather than around a tree walk.

```
Pass A — feed (daily, one request)
    GET view-revision-updates?changed_1[min]=<last run>&changed_1[max]=<today>
      -> shortlist: title, slug, date, book-trail
      -> for each, open the document (needed anyway for content)
      -> read its CANONICAL NODE URL from the response
      -> match on that node URL against regulations.document_url
           matched   -> modified  -> new version + re-analyse
           unmatched -> new       -> insert
      Covers UPDATED and (pending verification) NEW.

Pass B — inventory walk (weekly)
    Walk the rulebook tree, or probe stored URLs.
    Covers DELETED — the feed cannot report a removal, because a withdrawn
    document simply stops appearing.
```

### Design constraints it must satisfy

- **Fits the existing orchestrator.** Expose a class with `fetch_documents(limit=None) ->
  List[RegulatoryDocument]`, wired via `config/sources/sama.yml` with `mode: custom`. Working
  templates: `crawler/moh_crawler.py` (simple) and `crawler/cma_crawler_wrapper.py` (multi-section).
- **Excel first, database second.** A run writes an `.xlsx` and touches no database;
  `POST /approve/{run_id}` replays exactly those rows into MSSQL. Never re-crawl on approve.
- **No LLM analysis during crawling** — `analyse=false`.
- **Identity stays `(document_url, doc_path)`** with `reference_no` as the new-URL tiebreak. Do not
  widen the identity tuple; fields are ANDed, so more fields means more missed matches and an edited
  title produces a false `new` plus a false `disappeared`.
- **Expose `source_names`** (one per section) so the completeness gate can size each section against
  its own history — otherwise a whole section dying hides inside the total's tolerance.

## 6. Open questions — verify before building

1. **Does the feed list genuinely NEW documents, or only revisions to existing ones?** This is the
   single most important unknown. It decides whether Pass B is needed weekly or rarely.
   **Test:** pull the feed for a wide date range, normalise titles (lowercase, strip punctuation,
   collapse whitespace, `html.unescape`), and compare against `SELECT title FROM regulations WHERE
   regulator='SAMA'`. **Do not compare on URL** — see 3a.
2. **Does `items_per_page` exceed 40?** The dropdown offers 40. Try 200.
3. **Do Circulars and the IT Governance Framework appear in the same feed**, or do they need their
   own?
4. **Does `rulebook.sama.gov.sa` return an ETag or `Last-Modified`?** The fallback if the feed does
   not cover discovery. Ranged `GET` for two bytes; look for a version counter that differs
   per document.
5. **How far back does the feed go?** Determines whether it can seed an initial full inventory or
   only incremental change.

## 7. Files that matter

| Path | Why |
|---|---|
| `config/sources/sama.yml` | current wiring — `SAMACombinedCrawler`; replace with the new class |
| `crawler/sama_crawler_wrapper.py` | existing crawler, still working, to be superseded |
| `crawler/moh_crawler.py` | **best template** — a custom crawler reading a JSON endpoint, ~150 lines |
| `crawler/cma_crawler_wrapper.py` | template for a multi-section custom crawler |
| `crawler/generic_crawler_wrapper.py` | `build_regulator_crawler` — the `mode: custom` contract |
| `models/models.py` | `RegulatoryDocument` — the shape `fetch_documents` must return |
| `dynamic_crawler/formfill/api.py` | `POST /trigger/source/{regulator}`, `POST /approve/{run_id}` |
| `benchmarks/run_all_regulators.py` | the sweep; add `("SAMA", "source", "sama")` to re-include |
| `docs/determinism.md` | why re-analysis is avoided, and the measured variance |
| `config/change_signals.yml` | the ten regulators that WERE measured; SAMA is absent |

## 8. Environment notes

- Formfill API: `venv\Scripts\python.exe -m uvicorn dynamic_crawler.formfill.api:app --port 8100`
  (add `--reload` during development — the server caches `schema.py` and hint loading, and a stale
  server rejecting a valid config is easy to lose an hour to).
- The API **serialises runs behind one lock**. Only one crawl at a time.
- **The database was unreachable as of 2026-08-12** (`TCP Provider: The wait operation timed out`
  to `10.11.12.76:1437`) — likely VPN. It was reachable earlier the same session.
- Note the access split on this project: the crawler developer cannot reach the database; the lead's
  machine has intermittently failed to resolve regulator hostnames. Nobody has reliably had both.

## 9. Suggested order

1. Answer question 6.1 — does the feed surface new documents. Everything downstream depends on it.
2. If yes: build the feed-driven crawler, Pass B rare.
3. If no: build the feed for updates, plus a tree walk for discovery, and measure ETags (6.4) as a
   possible cheaper Pass B.
4. Wire as `mode: custom` in `config/sources/sama.yml`, run through
   `POST /trigger/source/sama?limit=0&analyse=false`, read the workbook, then approve.
