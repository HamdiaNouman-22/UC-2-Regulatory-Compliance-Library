# The re-versioning loop — what it was, what is fixed, what is left

**2026-08-16.** Started from a background batch that reported `FAILED` on all 370
documents across six sources. The crash turned out to be cosmetic. Chasing it
uncovered a real defect that had been silently inflating the version table.

---

## 1. The crash (fixed)

`'NoneType' object is not subscriptable`, 370 times, at
[orch.py:763](../dynamic_crawler/formfill/orch.py#L763):

```python
f"new version {version_id} (was {old.get('content_hash','')[:8]})"
```

`dict.get` returns its default only when the key is **absent**. A row read from
SQL always has the key, carrying `None` for a NULL column — so this returned
`None` and the slice raised.

It fires *after* every database write, so no data was lost. Verified rather than
assumed: `regulations` held at 8,714 (no duplicates), versions went 9,398 →
10,020, `content_hash` populated.

Three of the same bug fixed in [mssql_repo.py](../storage/mssql_repo.py) —
`requirement.get("title","")[:500]` and friends. Those read **LLM output**, where
a JSON `null` is entirely ordinary, so they would have crashed the analysis write.

## 2. The real defect (fixed)

**Most crawlers never set `content_hash` at all.** 8,151 of 8,714 stored rows had
no fingerprint:

| source_system | rows without a fingerprint |
|---|---|
| SAMA RULEBOOK | 6,105 |
| CMA-RULES | 1,979 |
| Regulations and Laws | 48 |
| Exchange Rules And Procedures | 19 |

The classifier reads a missing hash as "cannot match", which is `modified`:

```python
if old_hash and new_hash and old_hash == new_hash:   # unchanged
else:                                                # modified
```

and the modify path then wrote the empty hash **back over the stored one**:

```python
fields = {"content_hash": new_hash}          # unconditional
for column in ("title", "document_html", ...):
    if value not in (None, "", []):          # every other column protected
```

`content_hash` was the sole column exempt from the rule the comment right above
it states — *"a crawl that returns no title must not blank the stored one."*

That is what turned one missing field into a permanent loop: every run
re-modified every document and wrote two version rows each. Ten MOH documents
reached **five versions of identical content**.

MOH was not special. It was the source that happened to get run twice.

### Fixes

- [`crawler/fingerprint.py`](../crawler/fingerprint.py) — new. One place that
  decides what a fingerprint is: page **text** when we have the page (HTML churns
  on every CMS deploy), else `document_url | title`. Stamped at each crawler's
  single exit, so a new `RegulatoryDocument(...)` branch cannot reintroduce the
  gap — which is exactly how it appeared.
- [`moh_crawler.py`](../crawler/moh_crawler.py) — hashes `FileRef + Modified`.
  SharePoint's own change stamp is **better** than `url|title`, which cannot move
  when a PDF is replaced behind an unchanged link.
- [`orch.py`](../dynamic_crawler/formfill/orch.py) `_modified_row_fields` — an
  empty hash can no longer overwrite a stored one.
- Stamped at the exit of: `sama_rulebook_crawler` (`crawl_sector`, which
  `fetch_documents` routes through), `sama_laws_and_regs_crawler` (both exits),
  `sama_circulars_crawler`, `sama_finance_sector_crawler`, `cma_crawler_wrapper`
  (after `_scrub_urls`, so the hash covers the cleaned url).
- `sama_circulars_crawler` and `sama_laws_and_regs_crawler` define their **own**
  `RegulatoryDocument` dataclasses. `content_hash` added as a real *field* —
  `asdict()` copies fields only, so an attribute stamped on afterwards would have
  been dropped between the crawl and the database.

### Verified

Two runs, the discipline that caught three false alarms this week:

```
RUN 1   modified 83, unchanged  0   +166 version rows   1331.8s
RUN 2   modified  0, unchanged 83     +0 version rows      4.2s
```

Run 2 settled completely and short-circuited on `inventory hash unchanged since
last good run`. MOH now costs 4 seconds a run instead of 22 minutes.

All ten touched modules import clean.

---

## 2b. The identity change was only half-applied (fixed)

Found while running the tests. `title` joined the identity tuple on the lead's
instruction, but only in one of the two places that define it:

```python
DEFAULT_IDENTITY = ("document_url", "doc_path", "title")   # updated
identity: tuple = ("document_url", "doc_path"),            # constructor, NOT updated
```

A literal default in the signature is a second copy that `_clean_identity` can
never fall through to, so it silently outranked the constant. Every orchestrator
built without an explicit identity kept keying on two fields while the sweep and
`promote` keyed on three:

```
orchestrator : document_url=…|doc_path=A > B
sweep        : document_url=…|doc_path=A > B|title=a document
AGREE        : False
```

Absence streaks recorded by one side were invisible to the other, so **no
withdrawal could ever accumulate**. `test_the_sweep_and_the_orchestrator_build_the_same_key`
had been failing and saying exactly this. Constructor default is now `None`.

Verified live: `zatca.ie_guidelines` twice with three fields active — `unchanged 4`,
`+0 version rows`, `regulations` still 8,714. The `inventory_hash` moved once
(`b0b588d13e5e` → `6c2cbe2d95fb`) and then held, which is the one-time
reconciliation the pinned test documents.

## 2c. `disappeared` was scoped by source, not by form (fixed)

Each of ZATCA's five forms compared itself against all 151 ZATCA rows and
declared its four siblings withdrawn — 604 false disappearances across the five.
`source_system` is not fine-grained enough, and this is the THIRD time that has
bitten (see `_stored_for_source`'s docstring for the first two).

`_within_crawled_folders` now keeps only stored rows sitting in a folder the run
actually walked. Deliberately not a fixed doc_path depth: the three Information
Exchange forms separate at crumb 4, but for the other two crumb 4 is already the
document's own title. Parent-of-leaf is right for both because it is relative to
each document.

```
form                                    crawl  scope  disappeared
Tax and Customs Agreements                 98     98            0
Zakat, Tax and Customs Regulations         34     34            0
Information Exchange / Circulars           11     11            0
Information Exchange / Agreements and Laws  4      4            0
Information Exchange / Guidelines           4      4            0
                                                   total false: 0  (was 604)
```

Fails safe in every direction: an empty, limited or folder-skipping run narrows
the set and therefore proposes FEWER withdrawals.

## 2d. Duplicate version rows removed

554 pairs of version rows with identical content, one per document per broken
run. Kept the NEWEST of each pair — it carries `status='active'` and the
`compliance_analysis` references, so nothing had to be migrated (0 active rows
and 0 analysis references among the deletions).

The survivor's `change_summary` said **"content changed"**, which is the bug's
false story: the content never changed, the fingerprint was missing. 477
survivors had that repaired to `first version`, and their `updated_date` restored
to the earlier row's date — so the honest first-seen date survives after all.

```
regulation_versions  10,502 -> 9,948
remaining duplicates 0
backup               output/dedupe_backup_2026-08-16.json
```

Correction to an earlier claim in this document: "ten MOH documents reached five
versions of identical content" was wrong. The provable duplicates are PAIRS. The
9-row documents had 7 rows with a NULL hash, which means unknown content, not
identical content.

## 2e. The completeness gate was keyed per regulator, not per form (fixed)

The gate compares this run's document count against the last good run's. It
remembered ONE count per regulator, but a regulator publishes through several
forms of very different sizes. All five ZATCA forms wrote to the same
`run_history` key, so each run overwrote the previous form's baseline and the
next form was measured against a page it has nothing to do with:

```
agreements     98 documents  -> writes baseline 98
ie_guidelines   4 documents  -> "count moved 98 -> 4 (95.9%)"  QUARANTINED
```

Nothing was wrong, and it never settled: whichever form ran last set the baseline
the next one failed against. A check that cries wolf on every run is worse than
no check, because people stop reading it.

`_run_key` now qualifies the key with the form (`ZATCA/zatca.ie_guidelines`),
taken from the crawler's `hints_path`.

**Only FORM runs are re-keyed.** A crawler without a `hints_path` — a source
config, a hand-written wrapper — keeps the bare source name, so its stored
baseline still matches and it pays no reconciliation. The one-time cost lands on
formfill sources only, not on all twelve regulators:

```
ZATCA/zatca.ie_circulars     rows=11    <- own baseline
ZATCA/zatca.ie_guidelines    rows=4     <- own baseline
ZATCA                        rows=98    <- old shared key, now orphaned history
Ministry of Health           rows=83    <- untouched
Ministry of Education        rows=136   <- untouched
```

Verified on two forms, twice each:

```
ie_guidelines  run 1  trustworthy=True  PASS  gate_problems=[]
               run 2  skipped: inventory hash unchanged
ie_circulars   run 1  trustworthy=True  PASS  gate_problems=[]
               run 2  skipped: inventory hash unchanged
```

A side effect worth having: the early exit now fires for forms. It reads the
baseline from the same key the run writes, so an unchanged form costs one
inventory check instead of a full crawl — previously it could never match,
because it was reading a sibling's hash.

Covered by `test_each_form_keeps_its_own_baseline` and
`test_a_crawler_without_a_form_keeps_the_bare_source_name`.

## 3. Still to do

**a. Remaining crawlers without a fingerprint.** SECP and Saudi Exchange are not
yet stamped. `_modified_row_fields` now stops them destroying stored hashes, but
they still report everything `modified` on every direct-write run.

**b. Duplicate version rows already in the table.** Ten MOH documents sit at five
versions of identical content; the same pattern exists wherever a source ran more
than once. Needs a dedupe on `(regulation_id, content_hash)` keeping the earliest
of each run of identical hashes. **Not started — this writes to the database and
should be reviewed before it runs.**

**c. ZATCA `disappeared` is scoped wrong.** Each of the five ZATCA forms compares
against *all* ZATCA rows, so every form declares its four siblings withdrawn:

```
taxes          34 + 117 = 151      ie_agreements   4 + 147 = 151
agreements     98 +  53 = 151      ie_guidelines   4 + 147 = 151
ie_circulars   11 + 140 = 151
```

151 is the whole ZATCA corpus. All five share `source_system = "Rules and
Regulations"` and `regulator = "ZATCA"`, and `_stored_for_source` scopes on
exactly that pair. Harmless **today** only because nothing calls
`mark_regulation_withdrawn` — it is a trap for whoever enables withdrawal.

No form identifier is stored on the row, and `category` does not map to forms
(zatca.taxes alone spans 14 categories). The fix needs one — probably
`extra_meta["source_form"]`, stamped at ingest, with rows lacking the stamp
**excluded** from `disappeared` rather than included, so old rows fail safe.

> **All three of the above were DONE on 2026-08-16.** See §2b–2d. The scoping fix
> needed no stored form identifier after all — the folder a document sits in is
> the form, and the run knows its own folders. Items below are what remains.

**e. `JS_DETAIL` throws on some page shapes.**
`tests/test_formfill_detail_extraction.py` — 7 failures, all
`Page.evaluate: TypeError: object null is not iterable`. Pre-existing, from the
change making the link harvest honour `strip`. Live ZATCA and MOH runs go through
formfill fine, so this is a fixture shape rather than a broken crawl — but a null
guard in the snippet is owed.

**f. One regulation has two active versions.** id=832, "Circular Re." (SAMA). Two
rows inserted at the identical timestamp on 2026-08-14, both NULL hash, both
"first version". Pre-existing, untouched by the dedupe, which only acts on
non-empty hashes. One row should go.

**g. Fingerprint coverage is code-complete but not data-complete.** Every source
that has re-crawled since the fix is at 100%: AML, MOH, SDAIA, MISA, MOE, ZATCA.
The other 8,154 rows keep their NULL hash until their crawler runs again — SAMA
6,105, CMA 1,979, MC 48, Saudi Exchange 19. Nothing is wrong with them; they have
simply not been re-crawled.

**h. Carried over from the previous session,** untouched here:
`compliance_analysis.version_id` is NULL; the job overlap guard; the stray empty
`uc2-db` on the default instance; blocked hosts to retest (Saudi Exchange after
2026-08-22, SIMAH after 2026-09-04).

---

## The rule this session earned

A missing hash was treated as a changed hash. Absent and different are not the
same thing, and any code that conflates them will eventually report everything as
changed — which tells you exactly as much as reporting nothing.

Corollary, and the reason this was found at all: **one run is never a result.**
Run 1 said `modified 83` both before and after the fix. Only run 2 could tell the
difference.

Two more the day added:

**A default written twice is a default that disagrees with itself.** The
constructor's literal identity tuple silently outranked the constant it was
supposed to mirror, and the two halves of the system drifted apart for a day
without a single error. Defaults belong in one place; every other site takes
`None` and falls through.

**A red test is evidence before it is a chore.** Seven were failing when this
started. Five were stale and needed updating to a decision already taken — but
`test_the_sweep_and_the_orchestrator_build_the_same_key` was reporting a live
bug in withdrawal tracking, by name, and had been for a day. Read the failure
before assuming the test is out of date. The suite also runs under
`pytest-randomly`: the same three files gave 6 failures and then 35 on
consecutive runs, so use `-p no:randomly` when you need a number you can compare.
