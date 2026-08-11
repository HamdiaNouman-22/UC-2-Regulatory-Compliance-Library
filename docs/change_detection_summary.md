# Crawler change-detection work — done and pushed

My work on the regulatory crawler pipeline is finished. Here's what's in it.

## What I built

A **change-detection layer** that runs before the crawler and answers "what actually
changed?" cheaply:

1. **Ask the server, don't guess.** Instead of hashing the URL and link text, we now
   read a version token the server maintains (SharePoint ETags, a CMS date, a sitemap
   timestamp). Two bytes per document.
2. **Four signals.** `python -m dynamic_crawler.cli.sweep --signal ...` — one JSON
   request for a whole GOSI page, one request for MHRSD's entire sitemap, a probe of
   documents we already store, and one that reads a saved page with no request at all
   (for the site we're blocked from).
3. **A second opinion before we believe it.** Some regulators re-upload their whole
   library at once, so a moved timestamp isn't proof. For those, we fetch the
   document's own text and only a changed hash counts.
4. **Re-crawl only the shortlist.** The sweep writes the changed URLs to a file;
   `formfill run --only-urls <file>` opens exactly those pages and nothing else.
5. **Missing documents are a proposal, never an action.** If a document vanishes, the
   system requires two runs at least 20 hours apart, a passing health check, and then
   it tells a person. **Nothing in this code can remove a regulation from the
   library** — there's a test that enforces it.

Everything above runs with **no database connection** and writes only to its own JSON
files.

## Where I changed the original plan, and why

- **Identity is per source, not per regulator.** One regulator config is a list of
  sources with different shapes — SAMA has a grid with circular numbers and a rulebook
  with none. One key for the file had no right answer.
- **One generic sweep instead of ten site-specific ones.** I measured all ten
  regulators first: six run the same SharePoint platform, so most of the per-site work
  wasn't needed.
- **The documented "update by reference key" approach was unsafe.** That key was a
  *position* in an AI-generated list, so re-running an analysis could attach one
  requirement's key to another's text. It now hashes the text instead.
- **We flag documents as not-current rather than deleting them.** Same effect for
  readers, fully recoverable.

I also fixed several pre-existing bugs found along the way — archived document versions
were being saved empty on SQL Server, duplicate requirement sets were accumulating, and
a completeness check had never actually run on one of our two crawl paths.

## Before this deploys — two things to expect

- A regulation currently showing **two live requirement sets will show one**. That's
  the fix working, but it looks like requirements disappeared.
- The version probe is **switched off everywhere** pending code review, because its
  first run writes one metadata row per document.

## What's blocked

**There is no network route from my machine to the database** (`10.11.12.76:1437` — no
VPN adapter, port unreachable). Driver, credentials and code are all ready and waiting.

Until someone gives me network access **and** a read-only login, these can't be
finished:

- the end-to-end coverage check against real data
- the duplicate-analysis audit queries
- proving that a targeted re-crawl feeds the analysis step correctly
- any deployment

Everything that doesn't touch the database is done, tested (314 tests, no network
needed) and pushed.

A file in the repository root has a malformed 100-character filename. It causes
`git clone` and `git worktree add` to fail on Windows. A single `git rm` resolves it. I
have not removed it, because it changes the repository history for everyone on the
team.
