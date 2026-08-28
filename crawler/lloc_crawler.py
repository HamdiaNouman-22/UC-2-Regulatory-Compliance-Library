"""LLOCLatestCrawler — Bahrain Legislation and Legal Opinion Commission, "Latest Legislation".

WHY THIS IS NOT A BROWSER CRAWL
-------------------------------
There is no url for page 2 — but that alone would not settle it, because in
principle a browser can click LLOC's pager. What settles it is that IN PRACTICE
IT CANNOT: `/Prog/main1.js`, which defines the pager, is 404'd to the browser,
so `fetchResult` is undefined, the click does nothing, and the page still renders
its first ten records looking perfectly healthy. See "THE PAGER NEVER REACHES THE
BROWSER" below.

MEASURED 2026-08-21 with

    venv/Scripts/python.exe generic_crawler/crawler.py
        --seed https://www.lloc.gov.bh/Legislation/Latest
        --out output/lloc_test --scope prefix --max-pages 150

    -> 1 page, 58 characters of text, 12 documents, `status: ok`

against 144 records the site itself declares. Four separate reasons, and only the
first is a bug we could fix in the generic crawler:

1. `JS_MAIN_CONTENT` picked the WRONG container. `document.querySelector` returns
   the first match in DOCUMENT order, not selector order, and lloc.gov.bh has
   nine matches for `main, [role="main"], article, #content, .content, #main`:

       DIV.content      depth 5      59 chars   <- picked: the phone/email bar
       DIV.bodycontent  depth 3    1714 chars   <- role="main", the real one

   Fixable with a per-host `content_selector`, exactly as RERA and SIO needed.

2. PREFIX SCOPE DROPS THE FULL TEXT. Every record links a scanned PDF at
   `/PDF/<id>.pdf` and an HTML transcription at `/Legislation/HTM/<id>`. The
   latter is not under the seed path `/Legislation/Latest`, so it is correctly
   judged out of scope and discarded.

3. THERE IS NO PAGE 2. The pager is not links:

       <select id="pages" onchange="fetchResult('/Legislation/Latest','&quot;&quot;',this.value,10,'<217-char token>')">
       <span class="paging" onclick="fetchResult('/Legislation/Latest','&quot;&quot;',2,10,'…')"> التالى </span>

   `fetchResult` (in /Prog/main1.js) POSTs back to the SAME url with an
   antiforgery token and replaces `#ajaxresult`. All 15 pages are
   `/Legislation/Latest`. There is nothing to enqueue and "Next" has no href.

   `collect_paginated_links` makes this worse rather than better: it maximises a
   `<select>` on the assumption its options are page SIZES, and these are page
   NUMBERS — it would select "15" and land on the last page.

4. TITLES WOULD BE UNUSABLE ANYWAY. Every document anchor in the section says one
   of exactly two things — `انظر كصورة` ("view as image") or `انظر كنص` ("view as
   text") — so `best_doc_title` falls through to the url slug. The captured sheet
   read `O2726`, `D4126`, `D3726`. The real title is a SIBLING `div.ArTitle`,
   never inside the anchor.

THE ENDPOINT, MEASURED
----------------------
    POST https://www.lloc.gov.bh/Legislation/Latest
    Content-Type: application/json; charset=utf-8
    RequestVerificationToken: <read from the page's own onchange attribute>
    body: {PostParam: "", PageNum: 2, PageSize: 10}

Note the body is NOT valid JSON — unquoted keys, exactly as main1.js builds it by
string concatenation. It is sent byte-for-byte as the site sends it, because that
is what the server was measured to accept.

    PageNum 1, 2, 3      -> 10 records each, different records
    PageNum 15           -> 4 records          (14*10 + 4 = 144, matches)
    PageNum 16           -> 0 records          clean stop condition
    PageSize 20/50/144/500 -> STILL 10 RECORDS. The server ignores it.

So `PageSize` cannot be used to collapse the run into one call the way CBE's
`pageSize=500` can. 15 requests is the floor, not a choice.

THE SITE THROTTLES, AND IT THROTTLES WITH 404
---------------------------------------------
THIS IS THE MOST IMPORTANT THING IN THIS FILE.

After a burst of requests lloc.gov.bh starts answering with an IIS

    404 - File or directory not found.      (1,245 bytes)

and then, if pushed further, drops connections outright
(`RemoteDisconnected`). It recovers on its own — a single GET a minute later
returned the full 48 KB page with its pager intact — so this is a THROTTLE, not
a block, and it must not be treated as one.

Two traps in that, both of which caught a first pass of this work:

  * A 404 PARSES AS AN EMPTY PAGE. A loop that stops on "no records found"
    stopped at 20 of 144 records and looked like a clean, complete run. That is
    the CBE failure mode again — `status: ok` over 4.5% coverage. So here a
    non-200 is NEVER the end of the list: only HTTP 200 with zero record blocks
    ends the walk, and a 404 is retried with backoff and a fresh token.

  * THE SAME 1,245-BYTE 404 IS SERVED FOR ASSETS TOO. A fetch of
    /Prog/main1.js during a throttled window returned 1,245 characters and the
    obvious conclusion — "main1.js does not contain fetchResult" — was wrong.
    The real file is 35,113 characters. Measure twice on this host.

`REQUEST_DELAY` exists for that reason and should not be tuned down. 15 requests
at 2.5s is under a minute for the whole section, and this is a small national
commission's server, not a CDN.

THE PAGER NEVER REACHES THE BROWSER
-----------------------------------
This is the argument against solving LLOC inside generic_crawler/crawler.py, and
it is not an aesthetic one: crawler.py is a BROWSER crawler, and in a browser
this section stops at record 10.

Rendering one page of this listing pulls the document plus eight scripts.
MEASURED on a Playwright load with the crawler's own User-Agent:

    404  1245 bytes  /Prog/jquery-3.7.1.min.js
    404  1245 bytes  /Prog/wow.min.js
    404  1245 bytes  /Prog/Chart1.js
    404  1245 bytes  /Prog/main1.js        <- fetchResult is never defined
    200                /Prog/jquery-ui.min.js, slick, alertify, cookie1

    pageerror: "jQuery is not defined"  x3
    window.fetchResult -> undefined
    clicking the site's own "التالى" (Next) span -> 0 new links

and re-fetching main1.js from inside that same page a moment later returned
200 / 33,266 chars / contains `function fetchResult`. Nothing is broken; the host
simply refused four of the nine.

THAT IS THE FAILURE MODE THAT MATTERS. The page still renders its first ten
records perfectly. The pager is dead, the click gains nothing, and a crawl
records ten of 144 documents and reports success — which is precisely what the
run at the top of this docstring did (`"clicks": 1, "gained": 0`).

AND IT IS NOT ABOUT REQUEST COUNT. The obvious fix — block the decoration and
keep only jQuery and main1.js — was tried, taking the load down to THREE
requests. Both scripts 404'd again:

    served: {'Latest': 200, 'jquery-3.7.1.min.js': 404, 'main1.js': 404}
    typeof jQuery -> undefined, typeof fetchResult -> undefined
    clicking Next 1x -> 0 new links, total reached: 10 of 144

The document itself came back 200 in the same load. So it is `/Prog/*.js` that is
being refused, not the address being cut off — and a `requests` GET of the very
same main1.js url returned 200 / 35,113 chars earlier in the same session.

CONTRAST, measured MINUTES APART on the same host in the same throttled state:

    browser + click     10 of 144, `status: ok`, no warning, no way to tell
    this module          the listing GET 404'd on attempt 1, the retry recovered
                         it, and the run reported honestly:

        WARNING LLOC listing page unusable (attempt 1/4): status 404,
                1245 chars, token found: False
        declared by site 144   documents 20   (a --limit 20 run)

Both paths meet the same throttle. One returns 7% of the section and calls it
success; the other says what happened. That difference is the reason this is a
module and not a profile entry.

This module is ONE GET (for the token) plus fifteen POSTs = sixteen requests for
all 144, and it cannot fail silently: `_fetch_page` raises on any non-200 and
`fetch_documents` refuses a count that does not match the site's own declared
total.

IDENTITY
--------
The url carries the publisher's own instrument id: `/PDF/O2726.pdf` is Royal
Order 27 of 2026, `/PDF/D4126.pdf` Decree 41 of 2026. It is derived from the
instrument's type, number and year, so unlike CBE's date-bearing paths it does
NOT move when a document is re-issued. The default identity
(document_url, doc_path, title) is therefore sound and this source does not
override it — but `lloc_legislation_id` is carried in extra_meta so a future
migration has the key ready without a re-crawl.

WHAT THIS DELIBERATELY DOES NOT COVER
-------------------------------------
"Latest Legislation" is a ROLLING WINDOW of 144 records, not the corpus. The same
left-hand menu offers, all measured present on 2026-08-21:

    /Legislation/Search            a FORM — returns nothing until submitted
    /Legislation/Category          legislation by classification
    /Legislation/Women             women-specific legislation
    /Legislation/WomenAppointments women's appointments register
    /Legislation/English           "Results (2784 records found)"
    /LegalOpinion/Search           the Commission's legal opinions

`/Legislation/English` is the same POST shape and is the best-structured section
on the site — its `div.EnTitle` contains a real anchor whose text IS the title,
its dates and gazette numbers are already in English, and its documents are
`.docx` under `/FullEn/`. If an English-facing library ever wants the corpus
rather than the recent window, that is the section to add, and this class takes
`section_path` / `source_system` so it can be reused for it without a new file.
"""

from __future__ import annotations

import html as _html
import logging
import re
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote, urljoin

import requests
from bs4 import BeautifulSoup

from crawler.fingerprint import stamp_content_hashes
from dynamic_crawler.formfill.runner import _ext_type, _is_doc
from generic_crawler.crawler import content_key
from models.models import RegulatoryDocument

logger = logging.getLogger(__name__)

BASE = "https://www.lloc.gov.bh"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

#: The server IGNORES this — every value from 10 to 500 returned ten records.
#: Sent anyway because it is what the page sends, and a server that starts
#: honouring it would only make the run shorter.
PAGE_SIZE = 10

#: Seconds between requests. See the throttle note in the module docstring: this
#: host answers a burst with 404 and then drops connections. Do not tune down.
REQUEST_DELAY = 2.5

#: A throttled 404 is transient, so it is retried — with a FRESH token, because
#: the listing GET fails in the same window and a stale token would fail again.
#: HOW LONG TO KEEP TRYING, in seconds of waiting, per request.
#:
#: MEASURED 2026-08-25, and this is why the old fixed budget was not enough: the
#: previous policy was a fixed 4 attempts with 6/12/18s waits, so it gave up
#: after 6+12+18 = 36 SECONDS. Recovery from this host's throttle was observed
#: repeatedly at 90-180s: a 45s wait still returned the 1,245-byte 404, and one
#: page needed three 90s waits before answering. So a run failed while a human
#: reloading the same url a minute later succeeded — which is the whole reason
#: this constant exists.
#:
#: PATIENCE, NOT PERSISTENCE. The waits escalate (6, 12, 24, 48, then 60s) rather
#: than hammering: if the host is counting requests, retrying fast is what keeps
#: the throttle alive. Nothing here retries without sleeping first.
#:
#: NOT INFINITE, deliberately. A site that is genuinely down must fail the run,
#: not hang the job forever. Raise it per source with `retry_budget` in
#: init_kwargs when a section proves slower to recover.
RETRY_BUDGET = 420.0

#: FAILURES THAT WAITING CANNOT FIX. All three are RequestException subclasses,
#: so the retry loops caught them and spent the whole budget on them: a
#: `--section` argument mangled into `C:/Program Files/Git/en/legislation/...`
#: (Git Bash rewrites a leading-slash argument into a Windows path) was retried
#: ten times over 390 SECONDS before failing, and the message blamed the site's
#: throttle for what was a malformed url.
#:
#: A transient failure is a dropped connection, a timeout, or the 404. A url the
#: adapter cannot even dispatch is a bug in the caller, and it must say so at
#: once.
PERMANENT_ERRORS = (requests.exceptions.MissingSchema,
                    requests.exceptions.InvalidSchema,
                    requests.exceptions.InvalidURL)

#: Bodies at or under this size are the throttle's 1,245-byte IIS 404, not a
#: page. Used to tell "the host refused" from "the page has no pager".
THROTTLE_BODY = 3000

#: A WHOLE-RUN deadline for LLOCCategoryCrawler, in seconds. `RETRY_BUDGET` is
#: per request; ~30 classifications multiply it, so this bounds the job. Reaching
#: it is reported loudly and the unread classifications are excluded from
#: `source_systems`, so a short run can never withdraw their documents.
RUN_BUDGET = 1800.0

#: The escalation, then a flat hold. Read by `_wait_schedule`.
RETRY_WAITS = (6.0, 12.0, 24.0, 48.0)
RETRY_WAIT_MAX = 60.0

#: Runaway guard. 144 records at ten per page is 15; this allows the section to
#: grow tenfold before the guard is what stops the walk.
MAX_PAGES = 150

#: Refuse to believe an implausibly empty answer, as tools/workbook.py and
#: cbe_crawler.py both do. A read that finds nothing is a failed read.
MIN_EXPECTED = 1

# The pager's own onchange attribute is where the antiforgery token is published.
# Deliberately loose about the earlier arguments: their spelling is html-escaped
# in the source (`'&quot;&quot;'`) and has no bearing on the token.
_TOKEN_RE = re.compile(r"fetchResult\([^)]*'([A-Za-z0-9_:\-]{60,})'\s*\)")

# `<div class="results">نتائج (144 سجلات موجودة)</div>`, and the English section's
# `Results (2784 records found)`. The count is the publisher stating how many
# records exist, which is what makes a short read detectable.
_TOTAL_RE = re.compile(r'class="results"[^>]*>([^<]*)<')
_DIGITS_RE = re.compile(r"[0-9٠-٩][0-9٠-٩,٬]*")

# A trailing file size, e.g. `( 75.55 KB )` or `28.98 KB`. `.ArTitle` does not
# carry one, but `.EnTitle` does and this class is meant to serve that section
# too — a byte count is never part of a law's name.
_SIZE_TAIL = re.compile(
    r"[\s\-–—|,;(\[]*\d+(?:[.,]\d+)?\s*(?:[KMGT]i?B|bytes?)\s*[)\]]*\s*$",
    re.I)

#: Arabic month names as LLOC writes them (`6-أغسطس-2026`), so `published_date`
#: is the publisher's date rather than a crawl timestamp. Keyed on a
#: normalised form: the alef family (أ إ آ ا) is written inconsistently across
#: Bahraini government sites, and "April" appears as both أبريل and إبريل.
_AR_MONTHS = {
    "يناير": 1, "فبراير": 2, "مارس": 3, "ابريل": 4, "مايو": 5, "يونيو": 6,
    "يوليو": 7, "اغسطس": 8, "سبتمبر": 9, "اكتوبر": 10, "نوفمبر": 11,
    "ديسمبر": 12,
    # Variants seen on Bahraini government sites.
    "افريل": 4, "غشت": 8, "شتنبر": 9, "دجنبر": 12, "كانون الثاني": 1,
    "تموز": 7, "اب": 8, "ايلول": 9,
}
_EN_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun",
     "jul", "aug", "sep", "oct", "nov", "dec"], 1)}


def _norm_ar(s: str) -> str:
    """Fold the alef/ya/ta-marbuta variants so a month name matches whichever
    spelling the page used. Only for LOOKUP — never for stored text."""
    s = re.sub(r"[أإآٱ]", "ا", s or "")   # أإآٱ -> ا
    s = re.sub(r"[ى]", "ي", s)                          # ى -> ي
    s = re.sub(r"[ة]", "ه", s)                          # ة -> ه
    s = re.sub(r"[ً-ْـ]", "", s)                   # harakat, tatweel
    return re.sub(r"\s+", " ", s).strip()


def _ascii_digits(s: str) -> str:
    """Arabic-Indic digits (٠-٩) to ASCII. The pages measured used ASCII, but the
    same CMS renders both and a count that silently fails to parse would turn the
    short-read guard off."""
    return (s or "").translate({0x660 + i: 0x30 + i for i in range(10)})


def parse_date(raw: str) -> Optional[str]:
    """`6-أغسطس-2026` or `17-Jul-2025` -> `2026-08-06` / `2025-07-17`.

    Returns None rather than guessing. published_date is read downstream as fact;
    a wrong date is worse than an absent one.
    """
    txt = _ascii_digits((raw or "").strip())
    if not txt:
        return None
    parts = [p.strip() for p in re.split(r"[-/–]", txt) if p.strip()]
    if len(parts) != 3:
        return None
    day, mon, year = parts
    if not (day.isdigit() and year.isdigit()):
        return None
    key = _norm_ar(mon)
    num = _AR_MONTHS.get(key) or _EN_MONTHS.get(mon[:3].lower())
    if not num:
        return None
    try:
        return f"{int(year):04d}-{num:02d}-{int(day):02d}"
    except ValueError:
        return None


def clean_title(raw: str) -> str:
    txt = _html.unescape(raw or "")
    txt = re.sub(r"\s+", " ", txt).strip()
    prev = None
    while txt and txt != prev:              # `Title 28.98 KB` -> `Title`
        prev = txt
        txt = _SIZE_TAIL.sub("", txt).strip()
    return txt


def legislation_id(url: str) -> str:
    """`/PDF/O2726.pdf` -> `O2726`; `/Legislation/HTM/O2726` -> `O2726`.

    The publisher's instrument id: type letter, number, two-digit year. Stable
    across a re-issue, unlike CBE's date-bearing media paths.
    """
    tail = (url or "").rstrip("/").rsplit("/", 1)[-1]
    return re.sub(r"\.(pdf|docx?|htm|html)$", "", tail, flags=re.I)


def _wait_schedule(budget: float):
    """Yield sleep lengths until `budget` seconds of waiting are spent.

    One policy for all three places that talk to this host — the listing GET,
    the AJAX POST and the classification index — so a retry tuning change cannot
    apply to two of them and miss the third.

    Yields 6, 12, 24, 48, then 60s each, and stops as soon as the next wait would
    exceed the budget, so the total time spent waiting is bounded by it.
    """
    spent = 0.0
    i = 0
    while True:
        wait = RETRY_WAITS[i] if i < len(RETRY_WAITS) else RETRY_WAIT_MAX
        if spent + wait > budget:
            return
        spent += wait
        i += 1
        yield wait


class LLOCLatestCrawler:
    """Bahrain LLOC — one AJAX-paged legislation listing section.

    Defaults to "Latest Legislation". `listing_path` makes the class reusable for
    the other sections of the same site (see the docstring), which all publish
    through the same `fetchResult` endpoint.
    """

    def __init__(
        self,
        regulator: str = "Legislation and Legal Opinion Commission (LLOC)",
        source_system: str = "Latest Legislation",
        listing_path: str = "/Legislation/Latest",
        timeout: int = 60,
        request_delay: float = REQUEST_DELAY,
        retry_budget: float = RETRY_BUDGET,
    ):
        # "Full Name (ACRONYM)" is the library's naming rule. Config lookups match
        # on this string and fall back to defaults SILENTLY on a near-miss, so a
        # typo here is a wrong answer rather than an error.
        self.regulator = regulator
        self.source_system = source_system
        self.listing_path = listing_path
        self.listing_url = urljoin(BASE, listing_path)
        self.timeout = timeout
        self.request_delay = request_delay
        # Seconds of WAITING this crawler will spend on one request before it
        # gives up. See RETRY_BUDGET: the old 36s was less than this host's
        # observed 90-180s recovery, so runs failed where a manual reload worked.
        self.retry_budget = retry_budget
        self.last_result: dict = {}

    # ------------------------------------------------------------------ #
    #  the endpoint                                                       #
    # ------------------------------------------------------------------ #

    def _open(self) -> Tuple[requests.Session, Optional[str], Optional[int], str]:
        """GET the listing page: session cookie, antiforgery token, declared
        record count, and the page body itself.

        Retried under a throttle: that answer is the same 1,245-byte IIS 404 as
        everything else, and a run that gave up here would report an empty
        regulator.

        THE TOKEN CAN BE LEGITIMATELY ABSENT, AND THAT IS NOT A THROTTLE.
        MEASURED 2026-08-25: the token is published in the pager's own onchange
        attribute, and the site renders NO PAGER when everything fits on one
        page — /en/legislation/category/A (a section that does not exist) returns
        HTTP 200 with 22,971 bytes, no `select#pages`, no `div.legislation`, no
        `.results` header and no token.

        A classification holding ten records or fewer looks the same minus the
        emptiness: real records in the initial HTML, no pager, no token. Treating
        "no token" as a throttle therefore burned the whole retry budget and then
        failed a section that had in fact answered correctly the first time — so
        this returns `token=None` plus the body, and the caller reads the records
        straight off the page it already has. Only a non-200, a short body or a
        dropped connection is a throttle.
        """
        last = ""
        waits = _wait_schedule(self.retry_budget)
        attempt = waited = 0
        while True:
            attempt += 1
            session = requests.Session()
            session.headers.update({"User-Agent": USER_AGENT})
            try:
                resp = session.get(self.listing_url, timeout=self.timeout)
            except PERMANENT_ERRORS as e:
                raise RuntimeError(
                    f"LLOC cannot request {self.listing_url!r}: "
                    f"{type(e).__name__}: {e}. Waiting cannot fix a url the http "
                    f"adapter will not dispatch, so this fails now rather than "
                    f"spending the retry budget. A leading-slash `--section` "
                    f"argument run through Git Bash is the usual cause: MSYS "
                    f"rewrites it to a Windows path. Use PowerShell, or set "
                    f"MSYS_NO_PATHCONV=1.") from e
            except requests.RequestException as e:
                last = f"{type(e).__name__}: {e}"
                logger.warning("LLOC listing GET failed (attempt %d): %s",
                               attempt, last)
            else:
                body = resp.text or ""
                match = _TOKEN_RE.search(_html.unescape(body))
                if resp.status_code == 200 and len(body) > THROTTLE_BODY:
                    if attempt > 1:
                        logger.info("LLOC listing page answered on attempt %d "
                                    "after %.0fs of waiting", attempt, waited)
                    if match:
                        return (session, match.group(1),
                                self._declared_total(body), body)
                    # A real page with no pager: one page of results at most.
                    logger.info("LLOC %s has no pager — reading the single page "
                                "it rendered", self.listing_path)
                    return session, None, self._declared_total(body), body
                last = f"status {resp.status_code}, {len(body)} chars"
                logger.warning("LLOC listing page unusable (attempt %d): %s",
                               attempt, last)
            wait = next(waits, None)
            if wait is None:
                break
            logger.info("  waiting %.0fs before retrying (%.0fs of %.0fs budget "
                        "spent)", wait, waited, self.retry_budget)
            time.sleep(wait)
            waited += wait

        raise RuntimeError(
            f"Could not read an antiforgery token from {self.listing_url} after "
            f"{attempt} attempts and {waited:.0f}s of waiting ({last}). Without "
            f"it the AJAX endpoint "
            f"cannot be called at all, and this site answers a burst of requests "
            f"with a 1,245-byte IIS 404 before recovering on its own — so treat "
            f"this as 'come back later', never as an empty section.")

    def _declared_total(self, body: str) -> Optional[int]:
        """The count the site prints above its own list, e.g. `نتائج (144 سجلات
        موجودة)`. This is what makes a short read detectable, so a failure to
        parse it is logged rather than passed over."""
        match = _TOTAL_RE.search(body or "")
        if not match:
            logger.warning("LLOC listing page has no `.results` header — the "
                           "short-read guard is OFF for this run")
            return None
        nums = _DIGITS_RE.findall(match.group(1))
        if not nums:
            logger.warning("LLOC `.results` header carried no number (%r) — the "
                           "short-read guard is OFF for this run",
                           match.group(1)[:80])
            return None
        return int(_ascii_digits(nums[0]).replace(",", "").replace("٬", ""))

    def _fetch_page(self, session: requests.Session, token: str,
                    page_no: int) -> Tuple[str, requests.Session, str]:
        """One page of the listing, as an HTML fragment.

        Returns the fragment plus the session/token to use next, because a
        throttle recovery re-opens both.

        NON-200 IS NEVER THE END OF THE LIST. That distinction is the whole point
        of this method: the throttled 404 parses as "no records", and a walk that
        accepted it stopped at 20 of 144 records looking perfectly healthy.
        """
        body = ("{PostParam: \"\", PageNum: %d, PageSize: %d}"
                % (page_no, PAGE_SIZE)).encode("utf-8")
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "RequestVerificationToken": token,
            "X-Requested-With": "XMLHttpRequest",
            "Referer": self.listing_url,
        }
        last = ""
        waits = _wait_schedule(self.retry_budget)
        attempt = waited = 0
        while True:
            attempt += 1
            try:
                resp = session.post(self.listing_url, data=body,
                                    headers=headers, timeout=self.timeout)
            except PERMANENT_ERRORS as e:
                raise RuntimeError(
                    f"LLOC cannot POST to {self.listing_url!r}: "
                    f"{type(e).__name__}: {e}. Not a throttle — see "
                    f"PERMANENT_ERRORS.") from e
            except requests.RequestException as e:
                # The escalated form of the throttle: the connection is dropped
                # rather than answered.
                last = f"{type(e).__name__}: {e}"
            else:
                if resp.status_code == 200:
                    if attempt > 1:
                        logger.info("LLOC page %d answered on attempt %d after "
                                    "%.0fs of waiting", page_no, attempt, waited)
                    return resp.text, session, token
                last = f"HTTP {resp.status_code} ({len(resp.text or '')} chars)"

            wait = next(waits, None)
            if wait is None:
                logger.warning("LLOC page %d refused on attempt %d: %s — retry "
                               "budget spent", page_no, attempt, last)
                break
            logger.warning("LLOC page %d refused (attempt %d): %s — backing off "
                           "%.0fs and taking a fresh token (%.0fs of %.0fs "
                           "budget spent)", page_no, attempt, last, wait,
                           waited, self.retry_budget)
            time.sleep(wait)
            waited += wait
            # A FRESH SESSION AND TOKEN EVERY TIME, not just a re-POST. Under the
            # throttle the token can be stale by the time the host answers again,
            # and _open has its own budget for getting a new one.
            session, token, _, _body = self._open()
            headers["RequestVerificationToken"] = token

        raise RuntimeError(
            f"LLOC page {page_no} of {self.listing_path} could not be read after "
            f"{attempt} attempts and {waited:.0f}s of waiting ({last}). This "
            f"host answers a burst with a "
            f"1,245-byte IIS 404 and then drops connections, recovering on its "
            f"own — so this is a rate problem, not a missing page. Raising "
            f"rather than returning a partial inventory, which downstream would "
            f"read as documents having disappeared.")

    # ------------------------------------------------------------------ #
    #  parsing                                                           #
    # ------------------------------------------------------------------ #

    @staticmethod
    def parse_records(fragment: str) -> List[Dict]:
        """One dict per `div.legislation` block in an endpoint response.

        The Arabic and English sections differ in exactly one way that matters
        here: Arabic puts the title in a bare `div.ArTitle` and the file links in
        a sibling `div.options`, while English puts an anchor INSIDE
        `div.EnTitle`. Reading the title as text and the links from the whole
        block covers both.
        """
        soup = BeautifulSoup(fragment or "", "html.parser")
        out: List[Dict] = []
        for block in soup.select("div.legislation"):
            title_el = block.select_one(".ArTitle, .EnTitle")
            date_el = block.select_one(".dt .hvalue")
            gaz_el = block.select_one(".og .hvalue")

            docs, pages = [], []
            for a in block.select("a[href]"):
                href = (a.get("href") or "").strip()
                if not href or href.startswith(("#", "javascript:")):
                    continue
                url = urljoin(BASE, href)
                (docs if _is_doc(url) else pages).append(url)

            out.append({
                "title": clean_title(title_el.get_text(" ", strip=True)
                                     if title_el else ""),
                "date_raw": (date_el.get_text(strip=True) if date_el else ""),
                "gazette": (gaz_el.get_text(strip=True) if gaz_el else ""),
                "doc_urls": docs,
                "page_urls": pages,
            })
        return out

    # ------------------------------------------------------------------ #
    #  the contract: docs = crawler.fetch_documents()                     #
    # ------------------------------------------------------------------ #

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        cap = limit if isinstance(limit, int) and limit > 0 else None
        warnings: List[str] = []

        session, token, declared, first_body = self._open()
        logger.info("LLOC %s: site declares %s record(s)",
                    self.listing_path, declared if declared is not None else "?")

        records: List[Dict] = []

        # NO PAGER MEANS NO AJAX CALL. `_open` returns token=None when the page
        # rendered without a `select#pages`, which is how this site says
        # "everything fits on one page" — and the records are already in the body
        # it just handed back. Posting to the endpoint without a token would be
        # rejected, and retrying that rejection is how a perfectly good
        # single-page classification used to fail after burning its whole retry
        # budget.
        if token is None:
            records = self.parse_records(first_body)
            logger.info("LLOC %s: %d record(s) read from the page itself "
                        "(no pager)", self.listing_path, len(records))
            page_no = 2
        else:
            page_no = 1
        while token is not None and page_no <= MAX_PAGES:
            fragment, session, token = self._fetch_page(session, token, page_no)
            batch = self.parse_records(fragment)
            if not batch:
                # HTTP 200 AND zero blocks. This is the only end-of-list
                # condition; _fetch_page has already raised on anything else.
                break
            records.extend(batch)
            page_no += 1
            if cap and len(records) >= cap:
                break
            time.sleep(self.request_delay)
        else:
            # `while/else` fires when the loop ends without a break — including
            # the no-pager case, where the loop never ran at all. Only a walk that
            # actually paged can have hit the guard.
            if token is not None:
                warnings.append(f"stopped at the {MAX_PAGES}-page guard")
                logger.warning("LLOC %s hit the %d-page guard — the section grew "
                               "past what this crawler expects",
                               self.listing_path, MAX_PAGES)

        logger.info("LLOC %s: read %d record(s) over %d page(s)",
                    self.listing_path, len(records), page_no - 1)

        # A SHORT ANSWER IS A FINDING, NOT A RESULT. Skipped when the caller
        # asked for a `limit`, which is a deliberate partial read.
        if cap is None and declared is not None and len(records) != declared:
            raise RuntimeError(
                f"LLOC {self.listing_path} declares {declared} record(s) but "
                f"{len(records)} were read. Refusing to return a partial "
                f"inventory: downstream, {declared - len(records)} missing rows "
                f"read as documents having disappeared. The usual cause is the "
                f"site's throttle answering with a 404 that parses as an empty "
                f"page — retry later rather than lowering this check.")
        if cap is None and len(records) < MIN_EXPECTED:
            raise RuntimeError(
                f"LLOC {self.listing_path} returned no records. That is a "
                f"failed read, not an empty section.")

        docs: List[RegulatoryDocument] = []
        seen = set()
        no_doc = no_date = 0

        for rec in records:
            title = rec["title"]
            doc_urls = rec["doc_urls"]
            published = parse_date(rec["date_raw"])
            gazette = rec["gazette"] or None
            if not published and rec["date_raw"]:
                no_date += 1

            # `/Legislation/HTM/<id>` — the transcription. Carried as metadata,
            # not fetched: the listing already gives title, date and gazette
            # number, so there is nothing a per-record request would add to the
            # row. It is 144 extra requests on a host that throttles.
            html_view = next((u for u in rec["page_urls"]
                              if "/legislation/htm/" in u.lower()), None)

            # THE PDF IS THE CANONICAL FILE, not whichever url the site printed
            # first. MEASURED 2026-08-25 over the 1,641 rows of a full run:
            #
            #   1,204 records offer ONE file, and 1,202 of those are the PDF
            #     437 records offer TWO — always (.docx under /FullEn/, .pdf) —
            #           and doc_urls[0] was the DOCX on every one of them
            #
            # So taking doc_urls[0] filed 437 rows under a different format from
            # the other 1,204, and made `document_url` — an identity field —
            # depend on the site's listing order. Preferring the PDF makes the
            # column mean one thing across the whole source and removes the
            # ordering dependency the workbook check exists to catch.
            pdfs = [u for u in doc_urls if u.lower().endswith(".pdf")]
            primary = (pdfs[0] if pdfs else
                       (doc_urls[0] if doc_urls else (html_view or "")))
            if not doc_urls:
                no_doc += 1
            leg_id = legislation_id(primary)

            if not primary or not title:
                warnings.append(
                    f"skipped a record with no {'url' if not primary else 'title'}"
                    f" (title={title[:60]!r}, gazette={gazette})")
                continue

            key = (primary, title)
            if key in seen:
                continue
            seen.add(key)

            docs.append(RegulatoryDocument(
                regulator=self.regulator,
                source_system=self.source_system,
                category=self.source_system,
                title=title,
                document_url=primary,
                # FLAT: regulator > source_system > title, as CBE and MOH. The
                # site offers no taxonomy for this section — "Latest" is ordered
                # by date, not filed — so there is no folder level to add that
                # would not be invented.
                doc_path=[self.regulator, self.source_system, title],
                file_type=_ext_type(primary) if _is_doc(primary) else "HTML",
                # The publisher states both of these per record. No parsing of
                # the title, no guessing from a url.
                published_date=published,
                reference_no=gazette,
                source_page_url=self.listing_url,
                # Hashed from the RECORD, not from url|title: the instrument id,
                # the gazette issue it was published in, its date and its title.
                # Catches a retitle, a re-date and a re-gazetting.
                #
                # It CANNOT catch a scanned PDF replaced silently behind an
                # unchanged url — nothing in the listing moves for that. That
                # case belongs to the sweep, and config/change_signals.yml
                # records what the files answer.
                content_hash=content_key(
                    f"{leg_id}|{gazette or ''}|{published or rec['date_raw']}"
                    f"|{primary}|{title}"),
                extra_meta={
                    "crawl_source": self.source_system,
                    # Ready for an identity migration without a re-crawl. NOT
                    # declared as `identity:` in the yml: unlike CBE's
                    # date-bearing media paths, this url does not move when a
                    # document is re-issued, so the default identity holds.
                    "lloc_legislation_id": leg_id,
                    "lloc_gazette_no": gazette or "",
                    "lloc_date_raw": rec["date_raw"],
                    "lloc_html_view": html_view or "",
                    # THE OTHER FORMAT, AND WHY IT IS NOT `attachment_links`.
                    #
                    # `attachment_links` is the workbook's multi-file convention,
                    # and tools/workbook.py reads it together with document_url as
                    # one set of files. This crawler used to write EVERY file
                    # there, primary included, so a one-file record read as two
                    # and `check` rejected all 1,635 rows it had ids for:
                    #
                    #   "row N has 2 files AND a document_url -- a multi-file row
                    #    must leave document_url empty, or its identity depends on
                    #    which file the site listed first"
                    #
                    # The rule is right and the crawler was wrong. A record here
                    # is ONE instrument, published in two formats — not a document
                    # with attachments — so the PDF is `document_url` and the
                    # other format is recorded under its own key. Identity keeps
                    # the url that carries the instrument id, and no row claims to
                    # be multi-file.
                    #
                    # `_files_of` also does not de-duplicate, so mirroring the
                    # primary here counted it twice. Recording only the OTHER
                    # files avoids relying on that either way.
                    "lloc_other_formats": " | ".join(
                        u for u in doc_urls if u != primary),
                },
            ))
            if cap and len(docs) >= cap:
                break

        if no_doc:
            warnings.append(f"{no_doc} record(s) had no downloadable file; the "
                            f"HTML transcription was used as the url")
        if no_date:
            warnings.append(f"{no_date} record(s) had a date this crawler could "
                            f"not parse — see _AR_MONTHS")

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings,
                    "declared_total": declared, "pages_read": page_no - 1},
            "by_source": {self.source_system: len(docs)},
        }
        logger.info("LLOCLatestCrawler finished: %d document(s)%s",
                    len(docs), f" ({len(warnings)} warning(s))" if warnings else "")

        # The single exit. Every hash above is already set and stamp_ never
        # overwrites one; this is the backstop for a branch added later that
        # forgets to.
        return stamp_content_hashes(docs)


# ============================================================================
# Standalone smoke test. The real entry point is `python -m tools.workbook
# export lloc`, which goes through config/sources/lloc.yml and the composite;
# this exists so the endpoint can be exercised without the workbook machinery,
# the way the SAMA crawlers do.
#
#   venv/Scripts/python.exe -m crawler.lloc_crawler
#   venv/Scripts/python.exe -m crawler.lloc_crawler --limit 20
#   venv/Scripts/python.exe -m crawler.lloc_crawler --section /Legislation/English
#
# NOTE `--limit` DISABLES THE SHORT-READ GUARD, deliberately: a capped read is a
# partial read on purpose. Never judge coverage from a --limit run.
# ============================================================================
# THE CLASSIFICATION SECTION — /en/legislation/category
# ============================================================================
# WHY THIS IS A WRAPPER AND NOT A NEW CRAWLER
#
# Every classification page publishes through the SAME `fetchResult` endpoint as
# /Legislation/Latest. Measured 2026-08-25 on
# /en/legislation/category/Criminal Legislation:
#
#     Results (17 records found)      10 record blocks rendered
#     <select id="pages">  Page 1, Page 2
#       onchange fetchResult('/En/Legislation/Category/Criminal%20Legislation',
#                            '""', this.value, 10, '<210-char token>')
#
# So LLOCLatestCrawler's paging, token handling, short-read guard and record
# parsing all apply unchanged. This class adds the one thing the section needs
# and Latest does not: read the list of classifications first.
#
# WHY THE URL IS LOWERCASE, AND WHY THAT MATTERS BEYOND THIS FILE
#
# The site's menu links /en/Legislation/Category; the page's own child links are
# /en/legislation/category/<Name>. Both serve HTTP 200 — ASP.NET routing is
# case-insensitive. The GENERIC crawler's scope test is not: `scope: prefix`
# seeded at the menu's capitalised url computes the prefix
# '/en/Legislation/Category' and then rejects every lowercase child, so the crawl
# walks one page, queues nothing, and reports `status: ok`. That is why this
# section could not be a generic prefix crawl even if the pager did not exist,
# and why the index path below is written in the case the SITE uses.
#
# ONE source_system PER CLASSIFICATION, FROM ONE CONFIG ENTRY. Each
# classification becomes its own folder and its own change signal:
# `disappeared` is scoped by source_system (formfill/orch.py::
# _stored_for_source), so one classification failing cannot withdraw another's
# documents. `source_systems` is the property CompositeCrawler reads to learn
# all of them.
#
# COST: ~30 classifications, 1-2 pages each, `request_delay` between every
# request. Budget two to three minutes. Do not lower the delay — this host
# answers a burst with the 1,245-byte IIS 404 that parses as an empty page.

#: The index that lists the classifications. Lowercase, as the site links it.
CATEGORY_INDEX = "/en/legislation/category"

_CATEGORY_HREF = re.compile(r"^/en/legislation/category/(.+)$", re.I)


class LLOCCategoryCrawler:
    """Bahrain LLOC — legislation by classification, every classification.

    Reads the classification index, then runs LLOCLatestCrawler once per
    classification with that classification as its `source_system`.
    """

    def __init__(
        self,
        regulator: str = "Legislation and Legal Opinion Commission (LLOC)",
        timeout: int = 60,
        request_delay: float = REQUEST_DELAY,
        retry_budget: float = RETRY_BUDGET,
        run_budget: float = RUN_BUDGET,
        only: Optional[List[str]] = None,
    ):
        self.regulator = regulator
        self.timeout = timeout
        self.request_delay = request_delay
        # Seconds of WAITING this crawler will spend on one request before it
        # gives up. See RETRY_BUDGET: the old 36s was less than this host's
        # observed 90-180s recovery, so runs failed where a manual reload worked.
        self.retry_budget = retry_budget
        # A shortlist for a cheap verification run: `only: ["Criminal
        # Legislation"]` reads one classification instead of thirty. Like
        # `--limit`, a run that uses it is a partial read and not coverage.
        # WHY A RUN-LEVEL DEADLINE EXISTS AS WELL AS `retry_budget`.
        #
        # retry_budget is PER REQUEST. This class makes roughly 30 classifications
        # x 1-2 pages of them, so in the worst case — a host throttling
        # throughout — the per-request budgets multiply into hours of a job that
        # is only waiting. That is not a crawl, it is a hang with logging.
        #
        # When this deadline passes, the classifications not yet read are recorded
        # in `warnings` and left OUT of `source_systems`, exactly as a failed one
        # is. That is the property that matters: a run cut short cannot mark the
        # documents of an unread classification `disappeared`, because it never
        # claims to have covered it.
        self.run_budget = run_budget
        self.only = [str(x) for x in (only or [])] or None
        self._systems: List[str] = []
        self.last_result: dict = {}

    @property
    def source_systems(self) -> List[str]:
        """Every classification this run actually wrote under. Read by
        CompositeCrawler, and by the completeness gate, which needs all of them.

        EMPTY UNTIL fetch_documents() HAS RUN, and that is safe only because of
        the order the orchestrator works in: run_for_regulator() fetches first
        and classifies second, and `_stored_for_source` — the only consumer that
        matters — runs during classification. Anything that reads this BEFORE the
        fetch gets an empty list, which reads downstream as "exposes no
        source_system" and turns the completeness gate off silently. Populating
        it eagerly would mean a network call from a property, so the ordering is
        documented rather than removed.
        """
        return list(self._systems)

    # ------------------------------------------------------------------ #
    #  the classification index                                           #
    # ------------------------------------------------------------------ #

    def _classifications(self) -> List[Tuple[str, str]]:
        """[(name, listing_path)], in the order the site lists them.

        Retried like `_open`: under the throttle this GET returns the same
        1,245-byte 404 as everything else, and a run that believed it would
        report a regulator with no classifications at all.
        """
        url = urljoin(BASE, CATEGORY_INDEX)
        last = ""
        waits = _wait_schedule(self.retry_budget)
        attempt = waited = 0
        while True:
            attempt += 1
            session = requests.Session()
            session.headers.update({"User-Agent": USER_AGENT})
            try:
                resp = session.get(url, timeout=self.timeout)
            except PERMANENT_ERRORS as e:
                raise RuntimeError(
                    f"LLOC cannot request the classification index {url!r}: "
                    f"{type(e).__name__}: {e}. Not a throttle — see "
                    f"PERMANENT_ERRORS.") from e
            except requests.RequestException as e:
                last = f"{type(e).__name__}: {e}"
            else:
                body = resp.text or ""
                if resp.status_code == 200 and len(body) > 3000:
                    found = self._parse_index(body)
                    if found:
                        if attempt > 1:
                            logger.info("LLOC classification index answered on "
                                        "attempt %d after %.0fs of waiting",
                                        attempt, waited)
                        return found
                    last = f"status 200, {len(body)} chars, no classification links"
                else:
                    last = f"status {resp.status_code}, {len(body)} chars"
            logger.warning("LLOC classification index unusable (attempt %d): %s",
                           attempt, last)
            wait = next(waits, None)
            if wait is None:
                break
            logger.info("  waiting %.0fs before retrying (%.0fs of %.0fs budget "
                        "spent)", wait, waited, self.retry_budget)
            time.sleep(wait)
            waited += wait

        raise RuntimeError(
            f"Could not read the classification list from {url} after "
            f"{attempt} attempts and {waited:.0f}s of waiting ({last}). This "
            f"host answers a burst with a "
            f"1,245-byte IIS 404 that parses as an empty page, so treat this as "
            f"'come back later', never as a section with no classifications.")

    @staticmethod
    def _parse_index(body: str) -> List[Tuple[str, str]]:
        """Classification links from the index's own content block.

        Scoped to `.bodycontent` deliberately. The same page's mega-menu links
        the /en/page/... items (About Us, Privacy Policy, FAQs) and the other
        Legislation sections; only the content block lists the classifications.
        """
        soup = BeautifulSoup(body, "html.parser")
        root = soup.select_one("div.bodycontent") or soup
        out, seen = [], set()
        for a in root.select("a[href]"):
            href = unquote((a.get("href") or "").strip())
            match = _CATEGORY_HREF.match(href)
            if not match:
                continue
            name = (a.get_text(" ", strip=True) or match.group(1)).strip()
            if not name or name.lower() in seen:
                continue
            seen.add(name.lower())
            out.append((name, href))
        return out

    # ------------------------------------------------------------------ #
    #  the contract: docs = crawler.fetch_documents()                     #
    # ------------------------------------------------------------------ #

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        cap = limit if isinstance(limit, int) and limit > 0 else None
        classifications = self._classifications()
        if self.only:
            wanted = {x.lower() for x in self.only}
            classifications = [c for c in classifications if c[0].lower() in wanted]
            if not classifications:
                raise RuntimeError(
                    f"none of only={self.only!r} matched the classifications the "
                    f"site lists. Names match case-insensitively and must "
                    f"otherwise be exact.")
        logger.info("LLOC classifications: %d to read%s", len(classifications),
                    " (shortlisted)" if self.only else "")

        docs: List[RegulatoryDocument] = []
        warnings: List[str] = []
        by_source: dict = {}
        self._systems = []

        started = time.monotonic()
        for i, (name, path) in enumerate(classifications, 1):
            elapsed = time.monotonic() - started
            if self.run_budget and elapsed > self.run_budget:
                skipped = [n for n, _ in classifications[i - 1:]]
                warnings.append(
                    f"run budget of {self.run_budget:.0f}s spent after "
                    f"{i - 1} of {len(classifications)} classification(s); "
                    f"not read: {', '.join(skipped)}")
                logger.error("LLOC run budget spent after %.0fs — %d "
                             "classification(s) NOT read: %s", elapsed,
                             len(skipped), ", ".join(skipped))
                break
            if i > 1:
                time.sleep(self.request_delay)
            sub = LLOCLatestCrawler(
                regulator=self.regulator,
                # The classification IS the source system, so it is also the
                # folder (doc_path is [regulator, source_system, title]) and the
                # `disappeared` scope. This is the taxonomy the "Latest" section
                # explicitly does not have.
                source_system=name,
                listing_path=path,
                timeout=self.timeout,
                request_delay=self.request_delay,
            )
            logger.info("[%d/%d] %s  <-  %s", i, len(classifications), name, path)
            # ONE CLASSIFICATION FAILING MUST NOT TAKE THE OTHERS DOWN — and must
            # not pass silently either. The name goes into `warnings` and NOT into
            # `source_systems`, so the run cannot mark that classification's
            # stored documents `disappeared` on the strength of a read that never
            # happened.
            try:
                got = sub.fetch_documents() or []
            except Exception as e:
                warnings.append(f"{name}: {e}")
                logger.error("  classification FAILED: %s -> %s", name, e)
                continue
            self._systems.append(name)
            by_source[name] = len(got)
            docs.extend(got)
            if cap and len(docs) >= cap:
                docs = docs[:cap]
                warnings.append(f"stopped at limit={cap}")
                break

        if not docs:
            raise RuntimeError(
                "LLOC classifications returned no documents at all. That is a "
                "failed read, not an empty section — see the warnings logged "
                "above for which classifications refused.")

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings},
            "by_source": by_source,
        }
        logger.info("LLOCCategoryCrawler finished: %d document(s) across %d "
                    "classification(s); %d failed",
                    len(docs), len(self._systems), len(warnings))
        return docs


# ============================================================================

if __name__ == "__main__":
    import argparse
    import json
    import sys

    ap = argparse.ArgumentParser(
        description="Read one LLOC legislation listing section via its own "
                    "fetchResult endpoint.")
    ap.add_argument("--section", default="/Legislation/Latest",
                    help="listing path (default: /Legislation/Latest). The other "
                         "sections publish through the same endpoint — see the "
                         "module docstring.")
    ap.add_argument("--source-system", default=None,
                    help="source_system to stamp; defaults from --section")
    ap.add_argument("--limit", type=int, default=None,
                    help="stop after N records. DISABLES the short-read guard.")
    ap.add_argument("--delay", type=float, default=REQUEST_DELAY,
                    help=f"seconds between requests (default {REQUEST_DELAY}). "
                         f"This host answers a burst with a 404; do not lower it.")
    ap.add_argument("--json", dest="json_out", default=None,
                    help="write every record to this file as JSON")
    ap.add_argument("-q", "--quiet", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(levelname)s %(message)s")

    # Windows consoles default to cp1252, and every title in this section is
    # Arabic. Without this the run dies in `print`, after the crawl succeeded.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    default_name = (args.section.rstrip("/").rsplit("/", 1)[-1] or "Legislation")
    crawler = LLOCLatestCrawler(
        source_system=args.source_system or f"{default_name} Legislation",
        listing_path=args.section,
        request_delay=args.delay,
    )

    try:
        documents = crawler.fetch_documents(limit=args.limit)
    except RuntimeError as exc:
        print(f"\nFAILED: {exc}\n")
        raise SystemExit(2)

    run = crawler.last_result.get("run", {})
    print("\n" + "=" * 78)
    print(f"{crawler.regulator}  |  {crawler.source_system}")
    print(f"  section          {crawler.listing_url}")
    print(f"  declared by site {run.get('declared_total')}")
    print(f"  pages read       {run.get('pages_read')}")
    print(f"  documents        {len(documents)}")
    print(f"  with a date      {sum(1 for d in documents if d.published_date)}")
    print(f"  with a gazette   {sum(1 for d in documents if d.reference_no)}")
    print(f"  unique urls      {len({d.document_url for d in documents})}")
    print(f"  unique hashes    {len({d.content_hash for d in documents})}")
    print(f"  file types       "
          f"{sorted({(d.file_type or '?') for d in documents})}")
    for w in run.get("warnings") or []:
        print(f"  WARNING          {w}")
    print("=" * 78)

    for d in documents[:5]:
        print(f"\n  {d.title}")
        print(f"    date {d.published_date}   gazette {d.reference_no}   "
              f"id {d.extra_meta.get('lloc_legislation_id')}")
        print(f"    file {d.document_url}")
        print(f"    text {d.extra_meta.get('lloc_html_view') or '-'}")
        print(f"    path {' > '.join(d.doc_path or [])}")
    if len(documents) > 5:
        print(f"\n  ... and {len(documents) - 5} more")

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as fh:
            json.dump([{
                "title": d.title, "document_url": d.document_url,
                "published_date": d.published_date, "reference_no": d.reference_no,
                "file_type": d.file_type, "doc_path": d.doc_path,
                "content_hash": d.content_hash, "extra_meta": d.extra_meta,
            } for d in documents], fh, ensure_ascii=False, indent=2)
        print(f"\nwrote {args.json_out}")
