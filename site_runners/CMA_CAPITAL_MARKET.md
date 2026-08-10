# CMA — Media Center and Capital Market

Extends `site_runners/cma_laws.py` beyond Laws & Regulations. Paths now carry the
site's own top-level menu as the second level, so a document sits where a person
would have found it:

```
Capital Market Authority (CMA) > Laws & Regulations > …
Capital Market Authority (CMA) > Media Center      > Announcements > …
Capital Market Authority (CMA) > Capital Market    > Prospectuses  > …
```

```bash
venv/Scripts/python.exe site_runners/cma_laws.py --section "Capital Market"
venv/Scripts/python.exe site_runners/cma_laws.py --tab announcements
venv/Scripts/python.exe site_runners/cma_laws.py --tab announcements --max-chapters 5   # 5 pages
```

---

## 1. The one structural difference from Laws & Regulations

Every Laws & Regulations tab ships its whole list in the DOM and paginates by
toggling a CSS class. One read is the complete list.

**These pages do not.** Announcements holds 30 card nodes for 3,297
announcements, and clicking a page **rewrites those same nodes in place** — the
DOM never grows, and no network request fires that a normal listener sees. So:

- reading "all cards in the DOM" gives you five pages and looks complete;
- reading only the **visible** cards and clicking through is the only correct
  approach — the opposite of the rule that was right for Circulars.

That is why `cards_paged` exists as a separate shape from `cards`. The two look
almost identical on screen and need opposite code.

Two details that cost time:

- **The next arrow is the second-to-last `<li>`**, not the last — the last is
  the "Total 550 Pages" label. Worse, the arrow and the numbered "2" share the
  class `page-item2`, so selecting by class silently clicks page 2 and stays
  there forever. The selector is `li:nth-last-child(2) > a`.
- **Page size cannot be raised.** There is a hidden `input.pageRowLimit` set to
  6. Setting it to 60 or 500, firing `change`, and passing `?rows=` on the URL
  all leave the pager at 550 pages. 550 clicks it is.

### The guard that matters

A click that silently fails re-reads page 1 forever and reports a full-looking
crawl of six items. So the walk stops on **two consecutive pages that add
nothing new**, and separately compares pages walked against the site's stated
total and rows read against the SharePoint list count. Both mismatches print
`coverage_gap`.

---

## 2. What is implemented

| tab | section | shape | size | detail page |
|---|---|---|---|---|
| Announcements | Media Center | `cards_paged` | 3,297 items / 550 pages | yes — "Read More" |
| Prospectuses | Capital Market | `cards_paged` | 266 items / 23 pages | no — card links to PDF |
| Shareholder Circulars | Capital Market | `cards_paged` | 22 items / 2 pages | no — card links to PDF |
| Facilitating Opening Investment Accounts | Capital Market | `single_page` | 2,280 chars, 3 PDFs | — |
| FinTech Lab | Capital Market | `single_page` | 2,959 chars, 3 PDFs | — |
| Foreign Investors | Capital Market | `single_page` | 2,365 chars, 2 PDFs | — |

**Announcements is expensive.** 550 page clicks to enumerate (~20 min), then
3,297 detail pages at ~2.5s each — roughly **2.5 hours** for a full first crawl.
Use `--max-chapters N` to bound the page walk while testing. This is the tab that
most needs the incremental mode the project does not have yet: announcements are
append-only and newest-first, so a nightly run only needs to walk until it hits a
known id.

**Sub-tabs on these lists are real filters** (unlike the Circulars dropdown and
the FAQ checkboxes). Announcements: All 550 pages, General 245, QFI 1. Because
"All" is a superset, the crawl walks All and files everything flat under the tab;
the sub-tab totals are logged so the categories are visible if you later want
them as folders. Getting per-item categories would mean walking General
(245 pages) and Nomu as well, roughly doubling the cost for a folder name.

---

## 3. The five registers — what they are and how to store them

You asked how to crawl these. My answer is that four of the five should **not**
be crawled as documents at all, and the reason is the same for each.

### They are registers, not documents

| page | rows | shape on screen | the document column |
|---|---|---|---|
| Financial Market Institutions | 237 + 6 CRAs | cards, 40 pages | — (email / website) |
| Special Purpose Entities | 890 | HTML table, 149 pages | **Articles of Association** |
| Registered Accounting Offices | ~100 offices | HTML table, 3 pages, nested | **Transparency Reports** |
| Investment Funds | 382 | HTML table, 8 pages | **Rules** (terms & conditions) |
| Real Estate Contributions | 1 | HTML table, 1 page | **Issuance Brochure** |

A row reading `Sukuk Morabha 2409 | Effective | Debt-Based Recourse | Sukuk
Capital | Al Safa Telecom` contains **no requirement**. Pushing ~1,600 such rows
through the 3-tier extraction and the 4-stage LLM costs real money per run and
returns nothing a compliance library can act on.

There is a second, worse problem. Registers change constantly — a licence lapses,
a "last update" stamp moves. Treated as documents, every status flip becomes a
"document changed" alert and the monitoring signal drowns. Treated as a table,
you diff rows and report *"3 licences lapsed, 1 new SPE registered"* — which is
the thing a compliance team actually wants.

### Proposed storage

Two outputs per register page, not one:

1. **`register.json` / a Register sheet** — one row per entity, every column
   preserved, plus `crawled_at`. Marked `record_type: "register_row"` so the
   orchestrator skips LLM extraction. Monitoring diffs on the entity's own key
   (licence number, fund id, entity name) and reports added / removed / changed.
2. **`documents`** — the file each row links to, in the normal schema, with the
   entity as the folder:

```
CMA > Capital Market > Special Purpose Entities > Sukuk Morabha 2409
      > Articles of Association.pdf
CMA > Capital Market > Investment Funds > Public Funds > Dinar US Equity Fund
      > Fund Rules.pdf
CMA > Capital Market > Registered Accounting Offices > Baker Tilly …
      > Transparency Report.pdf
```

Those PDFs **are** regulatory documents and go through the normal pipeline. The
register row is the metadata that files them.

### Per page

**Financial Market Institutions** — `/en/Market/AuthorisedPersons/Pages/default.aspx`
Two tabs: Licensed Financial Market Institutions (237, 40 pages) and Credit
Rating Agencies (6). Cards, not a table: `Last update : 27/07/2026 / Wadaie
Capital / Notes : Has not commenced the business yet / MI Arranging…`. The
underlying list has Title, Email, URL, Licenses (title, code, tooltip), Notes,
LastUpdate. Store as a register keyed on the institution name, with the licence
list as a repeated field — *which activities a firm is licensed for* is the
compliance-relevant part, and it is the field most likely to change.

The `?view=1AFDA661-…` in the URL you sent is a SharePoint **view GUID**. It
selects a saved column layout, not a different data set. Crawl the plain URL and
click the tabs; do not hard-code the GUID, because a view can be edited or
deleted server-side and the crawl would 404 for a reason nobody could see.

**Special Purpose Entities** — `/en/Market/SPEs/Pages/default.aspx`
890 rows over 149 pages, two tabs (Issue Debt / Issue Investment — the second has
its own 28-page pager). Columns: Entity Name, License Validity, License Purpose,
Trustee, Sponsor, Articles of Association. Register keyed on entity name; the
Articles of Association link is a document. 149 pages ≈ 6 minutes, cheap.

**Registered Accounting Offices** — `/en/Market/rafs/Pages/default.aspx`
This one is **two related tables, not one**. The outer row is an office
(Accounting Office Name, Unified National Number, Registration Number,
Registration Date, Transparency Reports, Website); nested inside each is a table
of its accountants (Accountant Name, License Number, Registration Number,
Registration Date). Flattening them loses which accountant belongs to which
office. Store as two registers — `offices` and `accountants`, joined on the
office's registration number — and take Transparency Reports as documents. Only
3 pages.

**Investment Funds** — `/en/Market/imf/Pages/default.aspx`
382 funds over 8 pages, two tabs (Public / Private) and a "Select Manager of
Fund" dropdown listing the fund managers. Columns: Fund Name, Fund ID, Date,
Classification, Rules. Two things to know before writing it: the rows arrive by
AJAX and render `Loading…` first, so the crawl must wait on real content rather
than on row count; and the manager dropdown is worth using as the section-path
level, because "funds managed by AL RAJHI CAPITAL" is how these are actually
looked up. Register keyed on **Fund ID** — the only stable key here, since fund
names get amended. The Rules link is the document.

**Real Estate Contributions** — `/en/Market/RealestateContributions/Pages/default.aspx`
One row today (Raz Al-Salamah Contribution / Sukuk Capital Company / Issuance
Brochure), two tabs (Public Offering / Private Offering). Trivial to crawl and
trivial to get wrong: a single-row table is indistinguishable from a table that
failed to render, so this one needs the stated-total check more than any of the
others. The Issuance Brochure is the document.

### Built — results

Approved and implemented as the `register` shape. Measured 2026-08-06:

| register | entities | documents | note |
|---|---|---|---|
| SPEs that Issue Debt Instruments | 879 | 136 | Articles of Association |
| SPEs that Issue Investment Units | 158 | 0 | |
| Licensed Financial Market Institutions | 237 | 0 | matches the list count exactly |
| Credit Rating Agencies | 6 | 0 | |
| Registered Accounting Offices | 32 (+52 accountants) | 0 | nested table preserved |
| Real Estate Contributions — Public Offering | 1 | 1 | Issuance Brochure |
| Real Estate Contributions — Private Offering | 1 | 0 | |
| **Investment Funds** | **BLOCKED** | | see below |

**1,314 entity rows, 137 documents.** They ride in the same `pages.json` under a
`registers` key — one adapter reads everything — and land in the workbook as
**Registers** and **Register_nested** sheets alongside Documents.

`pages_to_excel.py` picks them up automatically:

```bash
venv/Scripts/python.exe site_runners/pages_to_excel.py output/site_runners/cma_spes \
    output/site_runners/cma_cm_institutions output/site_runners/cma_accounting_firms \
    output/site_runners/cma_real_estate_contributions --out output/site_runners/CMA_registers.xlsx
```

### The row key

Monitoring diffs on `key`, chosen in this order: Fund ID, Unified National
Number, Registration Number, Licence Number, ID — then the first column as a
last resort. Deliberately **not** position or name: fund names get amended and
rows get re-sorted, and a bad key turns "one fund renamed" into "382 rows
changed". Rows that end up without a key are counted and warned about, because
those are the ones monitoring cannot track.

### Investment Funds is blocked, and says so

It reads **3 of 382**. Two reasons, both real:

1. Public Funds is **grouped by fund manager** — one table per manager, which is
   what `p_GroupCol1=Hasseef Investment Company` in its cursor means. Reading
   only the first table in the pane returned one fund; reading all of them still
   returns one, because the rest render a literal `Loading…` row and only fill
   in when a user expands that manager. Scrolling does not trigger it.
2. Private Funds' cursor URL fails to load on the first hop.

Rather than write a 3-row register, the run **aborts**:

```
Investment Funds: read 3 of 382 entities. Refusing to write a register that is
less than half the list — a partial register is indistinguishable from a
shrinking one, and monitoring would report the missing rows as deletions.
```

That last clause is the real danger. A silently partial register is worse than
no register: the first diff would report 379 funds deleted. Finishing it means
driving the manager dropdown — one pass per fund manager — which is a
straightforward next step now the rest of the shape exists.

### Two traps worth remembering

- **Nested tables need `:scope >`.** Each accounting office row contains a table
  of its accountants, and a plain `querySelectorAll('th')` pulls the child
  table's headers into the parent's column list — which is exactly what the
  first probe of that page showed, ten columns where there are six.
- **Tabs do not always use `data-bs-target`.** Real Estate Contributions leaves
  it empty and links the pane another way, so matching on it alone gave both
  panes the same label and filed Public and Private Offering into one folder.
  The lookup now tries `data-bs-target`, `href` and `aria-controls`, then falls
  back to tab order.

---

## 4. Known limits

- **Announcements has no incremental mode**, and it is the tab that needs one.
  2.5 hours per full run, append-only source.
- **Announcement categories are not folders** — All / General / QFI / Nomu are
  logged, not applied. See §2.
- **Prospectuses and Shareholder Circulars have no detail page**, as you said —
  the card is the record and the PDF is the document. Their card text is short by
  design, so they show under `short`, never `empty`.
- **The five registers are not crawled.** See §3.
