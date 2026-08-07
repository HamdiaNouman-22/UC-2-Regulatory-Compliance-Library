"""Site-shape awareness for the autonomous onboarding agent.

Regulator tabs come in a few recurring LAYOUTS ("shapes"). Forcing one prompt +
one completeness rule across all of them is what made the agent unreliable: a
tree-recursion instruction and a "you got too few docs, recurse deeper" guard
actively sabotage a perfectly correct crawler for a FLAT paginated table.

So we:
  1. classify the seed page into a known shape,
  2. give the code generator shape-specific instructions (incl. what `limit` means),
  3. grade the crawl with a shape-appropriate completeness check.

Shapes implemented (the ones we've actually seen):
  - flat_table   : one list/table of documents (e.g. SAMA Circulars). All rows are
                   usually in the static HTML; completeness = did we get ~as many
                   docs as there are rows in the table. `limit` = at most N rows.
  - sidebar_tree : a nested category tree (e.g. SAMA Rulebook). `limit` = at most
                   N top-level categories; completeness = did we recurse deep enough.

Fallback is sidebar_tree (the prior default behaviour), so unknown pages keep the
old tree treatment rather than silently mis-handling.
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Callable, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# A table with at least this many body rows is treated as a real document list
# (not a small layout/toolbar table).
FLAT_TABLE_MIN_ROWS = 8
# A correct flat-table crawl should return at least this fraction of the rows.
FLAT_TABLE_COVERAGE = 0.85
# Minimum docs a sidebar-tree test crawl (a couple of whole categories) must yield,
# or it's almost certainly not recursing. (Mirrors the previous MIN_TEST_DOCS.)
TREE_MIN_TEST_DOCS = 8


@dataclass
class Shape:
    name: str
    # test-run limit VALUE and what it MEANS (for the prompt).
    test_limit: int
    limit_meaning: str
    guidance: str                       # prompt fragment describing how to crawl this shape
    verify: Callable                    # verify(doc_count, evidence, is_test) -> (ok, failure_summary|None)
    evidence: dict = field(default_factory=dict)   # populated at classify time (e.g. total_count)


# ----------------------------------------------------------------------------
# Detection helpers (run on the RAW, un-truncated seed HTML)
# ----------------------------------------------------------------------------

def _document_table_rows(soup: BeautifulSoup) -> int:
    """Largest count of *document-list* rows across all tables on the page.

    A document-list row is one with >= 2 cells AND at least one link (the link to
    the document). This deliberately ignores prose laid out in tables (e.g. a law's
    article text), which has no per-row links, and site-chrome/toolbar tables — so a
    tree-shaped law page is not mistaken for a flat list just because it contains
    tables. The winning count is also the completeness target (rows == documents)."""
    best = 0
    for table in soup.find_all("table"):
        tbody = table.find("tbody") or table
        doc_rows = 0
        for r in tbody.find_all("tr"):
            cells = r.find_all("td")
            if len(cells) >= 2 and r.find("a", href=True):
                doc_rows += 1
        best = max(best, doc_rows)
    return best


def _document_rows(table) -> list:
    """Rows of a table that look like document rows (>= 2 cells and a link)."""
    tbody = table.find("tbody") or table
    return [r for r in tbody.find_all("tr")
            if len(r.find_all("td")) >= 2 and r.find("a", href=True)]


def first_row_link(raw_html: str, base_url: str) -> Optional[str]:
    """Absolute URL of the first document-row's link IN THE MAIN DATA TABLE, so
    onboarding can fetch one real DETAIL page as an extra sample (lets the model see
    how the download-PDF link is structured). Picks the table with the most document
    rows, so toolbar/layout tables are ignored."""
    soup = BeautifulSoup(raw_html or "", "html.parser")
    best_rows = []
    for table in soup.find_all("table"):
        rows = _document_rows(table)
        if len(rows) > len(best_rows):
            best_rows = rows
    if best_rows:
        return urljoin(base_url, best_rows[0].find("a", href=True)["href"])
    return None


def _tree_signal(soup: BeautifulSoup) -> int:
    """Heuristic strength that this is a nested category tree."""
    score = 0
    # Drupal 'book' style collapsible sidebar menu (SAMA rulebook).
    if soup.select("nav[id*=book-block], nav[class*=book-block]"):
        score += 3
    if soup.select("li[class*=collapsed], li[class*=expanded], li[class*=has-children]"):
        score += 2
    # Card grid of category landing tiles.
    if len(soup.select("div.views-row")) >= 3:
        score += 2
    # A generic nested navigation menu with several levels.
    nested = soup.select("nav ul li ul li, aside ul li ul li")
    if len(nested) >= 5:
        score += 1
    return score


# ----------------------------------------------------------------------------
# Per-shape completeness verifiers
# ----------------------------------------------------------------------------

def _verify_flat_table(doc_count, evidence, is_test):
    total = evidence.get("total_count") or 0
    if is_test:
        # On the small test run we only prove EXTRACTION works; correctness of the
        # extracted values is judged separately by the cross-check. Any docs is enough.
        if doc_count < 1:
            return False, ("Adapter produced ZERO documents from a flat table. Read the rows of "
                           "the main <table> (they are present in the static HTML) and emit one "
                           "RegulatoryDocument per row.")
        return True, None
    # Full run: compare against the number of rows the table actually has.
    if total and doc_count < FLAT_TABLE_COVERAGE * total:
        return False, (
            f"COMPLETENESS FAIL: the main table has {total} rows but the crawl returned only "
            f"{doc_count} documents. Emit ONE document per row of the table and read EVERY row "
            f"(all rows are in the static HTML — do not stop early, do not apply a small limit). "
            f"If rows are split across pages, follow the pagination too.")
    return True, None


def _verify_sidebar_tree(doc_count, evidence, is_test):
    if is_test:
        if doc_count < TREE_MIN_TEST_DOCS:
            return False, (
                f"COMPLETENESS FAIL: only {doc_count} document(s) from whole top-level categories. "
                f"A recursive crawl should yield many more. You are NOT recursing into the nested "
                f"sub-pages/sidebar folders — descend into every child page until you reach the "
                f"individual document (leaf) pages, then extract those.")
        return True, None
    return True, None  # full run has no upper anchor for a tree


# ----------------------------------------------------------------------------
# Shape guidance (prompt fragments)
# ----------------------------------------------------------------------------

_FLAT_TABLE_GUIDANCE = """
**** THIS PAGE IS A FLAT TABLE / LIST — DO NOT TREAT IT AS A TREE ****
The seed page holds ONE list of documents in an HTML <table>. Every document is a
ROW in that table; all rows are already present in the static HTML you are shown.
- Find the main data table (the one with many rows) and iterate its <tbody> <tr> rows.
- Emit exactly ONE RegulatoryDocument per row. Pull title / reference number / dates /
  status / category straight from the row's <td> cells.
- Do NOT recurse into a "tree". There are no folders here.

FOR EACH ROW, OPEN ITS DOCUMENT DETAIL PAGE (this is REQUIRED, not optional):
- Take the row's link (the title/reference link) and fetch that detail page with
  self.fetcher.get(detail_url). Set source_page_url = detail_url.
- Capture the detail page's readable content HTML into document_html. CRITICAL: a
  page usually has SEVERAL candidate content boxes and the FIRST "field--name-body"
  is often an EMPTY analytics/script wrapper. Do NOT use find('div', class_='field--name-body').
  Pick the ARTICLE/content element with the MOST text (NOT <main>, which also holds
  the toolbar/breadcrumb chrome), then STRIP the surrounding noise so document_html
  is ONLY the title + metadata + body — no toolbar, breadcrumb, "Download Original
  PDF" link, "Translated Document" tooltip, "Related Content", or revision widgets.
  Use EXACTLY this pattern:
      candidates = detail_soup.select('article, div.node__content, div.field--name-body')
      content_el = max(candidates, key=lambda c: len(c.get_text(strip=True)), default=None)
      if content_el:
          for junk in content_el.select('script, style, nav, [class*=breadcrumb], '
                  '[class*=toolbar], .disp_toolbar, .related-content, .translated_document, '
                  '.parent_transalated_doc, .book-notification, .revision, [id*=revision], '
                  '.hide-previous, .hide-next, a.icopdf'):
              junk.decompose()
      document_html = str(content_el) if content_el and content_el.get_text(strip=True) else ""
  Then pass it to the TOP-LEVEL RegulatoryDocument(document_html=document_html, ...)
  parameter. DO NOT put document_html inside extra_meta — it is its own field.
- On the detail page, find the ORIGINAL FILE download link and use it as the document:
    * It is typically an <a> whose visible text is like "Download Original PDF" (on
      SAMA it has class "icopdf"), or any <a> whose href ends in ".pdf".
    * Set document_url = that file link (absolutized with absolutize(self.BASE_URL, href))
      and file_type = "PDF".
    * If the detail page ALSO has a "Translated Document" / translated PDF link, put it
      in urdu_url (that field is the general "translated/secondary document" slot).
    * If there is NO downloadable file, fall back to document_url = detail_url and
      file_type = "HTML".
- Base the download-link selector on the DETAIL-PAGE HTML sample shown below (look for
  the sample whose url is a document page, not the list page).

- If the list is split across multiple pages (pagination links / a page query param),
  follow every page so you collect the whole list.
- `limit`, when not None, means: process AT MOST that many rows (for a quick sample).
  When limit is None, read the ENTIRE table — every single row — and open every detail page.
"""

_SIDEBAR_TREE_GUIDANCE = """
**** COMPLETENESS IS CRITICAL — THIS PAGE IS A NESTED TREE ****
This section is a TREE, not a flat list. A single section typically contains HUNDREDS
of documents nested several levels deep (category -> sub-category -> ... -> document).
Traverse the WHOLE tree RECURSIVELY:
- Do NOT stop at the landing page. Its links are usually category headers; the real
  documents live many levels below.
- The left-hand sidebar (<nav> "book outline" with <ul class="menu"> / <li class="menu-item">)
  usually contains the ENTIRE nested tree on EVERY page. The most reliable crawl is to
  parse that WHOLE nested tree ONCE and fetch every link in it.
- CRITICAL — RECURSE THROUGH EVERY NESTED <ul> AT ALL DEPTHS. A <li> can contain a
  child <ul> of sub-items, which can contain further child <ul>s (e.g. Guidance Notes
  -> Stage 1 -> A. About Your Business -> A1...). If you read ONLY the top-level
  `ul.menu > li` you will MISS everything deeper (a common, serious bug). Walk each
  <li>'s nested <ul> recursively.
- Also follow in-body links on listing/index pages down to the individual documents.
- Use a `visited` set of canonical(url) to avoid loops, but otherwise be EXHAUSTIVE.
- `limit`, when not None, means: crawl AT MOST that many TOP-LEVEL categories (for a
  quick sample). When None, crawl every category exhaustively.

FOR EACH PAGE YOU CAPTURE AS A DOCUMENT, do ALL of this:
1. HIERARCHY — PREFER the page's BREADCRUMB; FALL BACK to your traversal path when
   the site has no breadcrumb (many regulators don't). Building it purely from
   traversal can duplicate the leaf title, so de-dupe. Carry the folder titles you
   descended through as `path`, and use EXACTLY:
       crumbs = [a.get_text(strip=True) for a in soup.select('[class*=breadcrumb] a')]
       crumbs = [c for c in crumbs if c]                     # ignore empty/home-icon links
       if len(crumbs) > 1:
           hier = crumbs[1:]                                 # breadcrumb: drop site-root crumb
       else:
           hier = list(path)                                 # fallback: folders you descended
           if not hier or hier[-1] != title:                # avoid duplicating the leaf title
               hier = hier + [title]
       doc.doc_path = [self.REGULATOR, self.SOURCE_SYSTEM] + hier
   NEVER append the title again if it is already the last entry of hier.
2. document_html — capture ONLY the article's title + metadata + body into the
   TOP-LEVEL document_html field. Pick the ARTICLE/content element with the MOST text
   (NOT <main>, which also holds toolbar/breadcrumb chrome), then STRIP the noise so
   there is NO toolbar, breadcrumb, "Download Original PDF" link, "Translated
   Document" tooltip, "Related Content", or revision widget. Use EXACTLY:
       candidates = soup.select('article, div.node__content, div.field--name-body')
       content_el = max(candidates, key=lambda c: len(c.get_text(strip=True)), default=None)
       if content_el:
           for junk in content_el.select('script, style, nav, [class*=breadcrumb], '
                   '[class*=toolbar], .disp_toolbar, .related-content, .translated_document, '
                   '.parent_transalated_doc, .book-notification, .revision, [id*=revision], '
                   '.hide-previous, .hide-next, a.icopdf'):
               junk.decompose()
       document_html = str(content_el) if content_el and content_el.get_text(strip=True) else ""
   Pass it as RegulatoryDocument(document_html=document_html, ...); NOT in extra_meta.
3. ORIGINAL PDF — if the page has an <a class="icopdf"> (visible text "Download
   Original PDF"), set document_url to its href (absolutized) and file_type="PDF".
   Do NOT grab generic ".pdf" links from the top mega-menu/header/footer — ONLY the
   icopdf / "Download Original PDF" link inside the content. If none, set
   document_url = the page URL and file_type = "HTML".

PREFERRED skeleton — parse the FULL nested sidebar tree, then fetch each page once
(adapt selectors to the real HTML). `_flatten_menu` recurses into nested <ul>s at
ALL depths, which is what prevents the "stopped at Stage 1/Stage 2" under-crawl:
```python
def crawl(self, limit=None):
    docs, visited = [], set()
    seed = self.fetcher.get(self.SEED_URL)
    root_ul = seed.select_one('nav ul.menu')          # the book-outline sidebar
    entries = self._flatten_menu(root_ul, []) if root_ul else []
    if limit:                                          # quick sample: first N TOP-LEVEL items only
        tops = [e for e in entries if len(e[2]) == 0][:limit]
        keep = set(id(t) for t in tops)
        entries = [e for e in entries if len(e[2]) == 0 and id(e) in keep
                   or len(e[2]) > 0 and e[2][0] in [t[0] for t in tops]]
    for title, url, ancestors in entries:
        key = canonical(url)
        if key in visited: continue
        visited.add(key)
        soup = self.fetcher.get(url)
        if soup is None: continue
        doc = self._extract_document(soup, url, ancestors + [title])  # breadcrumb-or-path hierarchy + html + pdf
        if doc: docs.append(doc)
    return docs

def _flatten_menu(self, ul, ancestors):
    "Recurse EVERY nested <ul> at ALL depths -> list of (title, abs_url, ancestor_titles)."
    out = []
    for li in ul.find_all('li', recursive=False):     # direct children of THIS ul
        a = li.find('a', href=True)
        if not a: continue
        title = a.get_text(strip=True)
        url = absolutize(self.BASE_URL, a['href'])
        out.append((title, url, list(ancestors)))
        sub = li.find('ul', recursive=False)          # this item's OWN nested children
        if sub:
            out += self._flatten_menu(sub, ancestors + [title])
    return out
```
"""


def _make_flat_table(evidence) -> Shape:
    return Shape(
        name="flat_table",
        test_limit=15,   # 15 rows: fast, but a big enough sample for a real cross-check
        limit_meaning="at most N rows of the table",
        guidance=_FLAT_TABLE_GUIDANCE,
        verify=_verify_flat_table,
        evidence=evidence,
    )


def _make_sidebar_tree(evidence) -> Shape:
    return Shape(
        name="sidebar_tree",
        test_limit=2,    # 2 whole top-level categories
        limit_meaning="at most N top-level categories",
        guidance=_SIDEBAR_TREE_GUIDANCE,
        verify=_verify_sidebar_tree,
        evidence=evidence,
    )


# ----------------------------------------------------------------------------
# Classification
# ----------------------------------------------------------------------------

def classify(raw_seed_html: str) -> Shape:
    """Pick the best-fitting shape from the RAW (un-truncated) seed HTML.

    Heuristic and deterministic: a big data table wins as flat_table (carrying its
    row count as ground-truth for completeness); strong nested-menu signals win as
    sidebar_tree; otherwise default to sidebar_tree (prior behaviour).
    """
    soup = BeautifulSoup(raw_seed_html or "", "html.parser")
    doc_rows = _document_table_rows(soup)
    tree = _tree_signal(soup)

    logger.info(f"Shape signals: document_table_rows={doc_rows} tree_signal={tree}")

    # The decisive signal is whether the MAIN CONTENT is a table of document rows
    # (rows with links). Side navigation/chrome is present on every page of a site
    # and does NOT make a listing page a "tree", so a real document table wins.
    if doc_rows >= FLAT_TABLE_MIN_ROWS:
        logger.info(f"Detected shape: flat_table (total_count={doc_rows})")
        return _make_flat_table({"total_count": doc_rows})

    if tree >= 2:
        logger.info("Detected shape: sidebar_tree")
        return _make_sidebar_tree({"tree_signal": tree})

    logger.info("Detected shape: sidebar_tree (fallback / no strong table or tree signal)")
    return _make_sidebar_tree({"tree_signal": tree})


def get_shape(name: str, evidence: Optional[dict] = None) -> Shape:
    """Rebuild a Shape by name (used by refine mode, where we persist the name)."""
    evidence = evidence or {}
    if name == "flat_table":
        return _make_flat_table(evidence)
    return _make_sidebar_tree(evidence)
