"""Config-driven equivalent of crawler/sama_finance_sector_crawler.py's
SAMAFinanceSectorCrawler, generalized so any Drupal-Book-shaped (sidebar-tree)
regulator tab can be crawled from a config file instead of a hand-written
Python module. Composes fetcher + discovery + extraction, walks the site tree,
and returns List[RegulatoryDocument] plus a validation report.

Deliberately does not import from crawler/sama_finance_sector_crawler.py --
this re-implements the same parsing patterns in generalized, config-driven
form so dynamic_crawler/diff/compare.py is a real independent check against
the existing crawler, not the crawler being compared against itself.
"""

import logging
import re
from typing import List, Optional, Set

from models.models import RegulatoryDocument

from dynamic_crawler.discovery.sidebar_tree import Node, collect_top_categories, derive_nav_id, expand_node
from dynamic_crawler.extraction.field_extractor import extract_field, extract_page_title, extract_year
from dynamic_crawler.extraction.leaf_classifier import extract_body_links, is_structured_leaf
from dynamic_crawler.fetcher import Fetcher
from dynamic_crawler.urlnorm import absolutify_links, canonical
from dynamic_crawler.validation import validate_documents

logger = logging.getLogger(__name__)

_DEFAULT_GENERIC_LINK_TEXT = re.compile(
    r'^(click here|here|view|view here|download|read more|see here|see more|link|more)\.?$',
    re.IGNORECASE,
)


class GenericSidebarTreeEngine:
    def __init__(self, cfg: dict):
        if cfg["discovery"]["strategy"] != "sidebar_tree":
            raise ValueError("GenericSidebarTreeEngine only supports discovery.strategy=sidebar_tree")

        self.cfg = cfg
        self.base_url = cfg["base_url"]
        self.seed_url = cfg["seed_url"]
        self.category = cfg["tab_name"]

        self.fetcher = Fetcher(cfg["fetch"])
        self.nav_id = derive_nav_id(self.seed_url, cfg["discovery"]["sidebar"])
        self.folder_classes = cfg["discovery"]["sidebar"]["folder_li_classes"]
        self.top_level_cfg = cfg["discovery"]["top_level"]
        self.body_links_cfg = cfg["discovery"]["body_links"]

        generic_pattern = self.body_links_cfg.get("generic_link_text_pattern")
        self.generic_link_text_re = (
            re.compile(generic_pattern, re.IGNORECASE) if generic_pattern else _DEFAULT_GENERIC_LINK_TEXT
        )

        self.structured_indicator_selector = cfg["extraction"]["structured_indicator_selector"]
        self.fields_cfg = cfg["extraction"]["fields"]
        self.plain_content_selector = cfg["extraction"].get("plain_content_container_selector", "div.node__content")
        self.page_title_strip_suffix = cfg["extraction"].get("page_title", {}).get("strip_suffix_regex")

    # ---- leaf extraction ----

    def _build_structured_doc(self, soup, real_title: str, node_url: str, cur_path: List[str]) -> Optional[RegulatoryDocument]:
        if not is_structured_leaf(soup, self.structured_indicator_selector):
            return None

        extra_meta = {}
        kwargs = {}
        for key, field_cfg in self.fields_cfg.items():
            value = extract_field(soup, self.base_url, field_cfg)
            if value is None:
                continue
            target = field_cfg.get("target", key)
            if target.startswith("extra_meta."):
                extra_meta[target.split(".", 1)[1]] = value
            else:
                kwargs[target] = value

        published_date = kwargs.get("published_date")

        doc = RegulatoryDocument(
            regulator=self.cfg["regulator"],
            source_system=self.cfg["source_system"],
            category=self.category,
            title=real_title,
            document_url=node_url,
            published_date=published_date,
            reference_no=kwargs.get("reference_no"),
            year=extract_year(published_date),
            source_page_url=self.seed_url,
            file_type="PDF" if extra_meta.get("org_pdf_link") else None,
            extra_meta=extra_meta,
            document_html=kwargs.get("document_html"),
        )
        doc.doc_path = [self.cfg["regulator"], self.cfg["source_system"], self.category] + cur_path
        return doc

    def _build_content_doc(
        self,
        soup,
        real_title: str,
        node_url: str,
        cur_path: List[str],
        is_hub: bool = False,
    ) -> Optional[RegulatoryDocument]:
        """Capture a non-structured page as its own record.

        Covers three cases so the stored hierarchy mirrors the site tree exactly:
          - plain content pages (text, no info-table)               -> as before
          - hub/listing pages that carry their own intro text        -> is_hub=True
            (e.g. "Application Forms"; the links it lists, including
            downloadable-file links, are preserved inside document_html)
          - empty structural nodes (content area present but blank)  -> structural
            (e.g. "Microfinancing", a sidebar heading with no body)

        Returns None only when the page has no content area at all (a failed
        render / redirect), so we don't fabricate records for non-pages.
        """
        content_div = soup.select_one(self.plain_content_selector)
        if content_div is None:
            return None
        text_content = content_div.get_text(strip=True)

        extra_meta = {}
        if is_hub:
            extra_meta["hub_page"] = True
        if not text_content:
            extra_meta["structural_node"] = True

        doc = RegulatoryDocument(
            regulator=self.cfg["regulator"],
            source_system=self.cfg["source_system"],
            category=self.category,
            title=real_title,
            document_url=node_url,
            source_page_url=self.seed_url,
            file_type="HTML",
            extra_meta=extra_meta,
            document_html=absolutify_links(str(content_div), self.base_url),
        )
        doc.doc_path = [self.cfg["regulator"], self.cfg["source_system"], self.category] + cur_path
        return doc

    # ---- recursive walk ----

    def _process(
        self,
        node: Node,
        path: List[str],
        depth: int,
        visited: Set[str],
        results: List[RegulatoryDocument],
        from_listing: bool = False,
    ) -> None:
        key = canonical(node.url)

        if not from_listing:
            if key in visited:
                return
            visited.add(key)

        cur_path = path + [node.title]
        indent = "  " * depth
        logger.info(f"{indent}{'[xref] ' if from_listing else ''}Visiting: {node.title} ({node.url})")

        children = node.children
        page_soup = None

        if not from_listing and node.is_folder_hint and not children:
            children, page_soup = expand_node(self.fetcher, self.base_url, self.nav_id, node.url, self.folder_classes)

        if not from_listing and children:
            logger.info(f"{indent}-> folder with {len(children)} children")
            for child in children:
                self._process(child, cur_path, depth + 1, visited, results)
            return

        if page_soup is None:
            page_soup = self.fetcher.get(node.url)
        if not page_soup:
            return

        real_title = extract_page_title(page_soup, node.title, self.page_title_strip_suffix)
        if real_title != node.title:
            logger.info(f"{indent}  (retitled '{node.title}' -> '{real_title}')")
            cur_path = cur_path[:-1] + [real_title]

        doc = self._build_structured_doc(page_soup, real_title, node.url, cur_path)
        if doc:
            results.append(doc)
            logger.info(f"{indent}-> structured document captured")
            return

        body_links = extract_body_links(page_soup, self.base_url, self.body_links_cfg)

        # Capture the page itself as a record BEFORE following any body links, so
        # hub pages with their own intro text (e.g. "Application Forms") and empty
        # structural nodes (e.g. "Microfinancing") are recorded rather than skipped.
        doc = self._build_content_doc(page_soup, real_title, node.url, cur_path, is_hub=bool(body_links))
        if doc:
            results.append(doc)
            if body_links:
                kind = "hub/listing"
            elif "structural_node" in doc.extra_meta:
                kind = "structural (empty)"
            else:
                kind = "plain content"
            logger.info(f"{indent}-> {kind} page captured")

        # Still follow linked documents (preserves cross-referenced child pages).
        if body_links:
            logger.info(f"{indent}-> following {len(body_links)} linked documents")
            for link_title, link_url in body_links:
                child = Node(title=link_title, url=link_url)
                parent_path = path if self.generic_link_text_re.match(link_title.strip()) else cur_path
                self._process(child, parent_path, depth + 1, visited, results, from_listing=True)

    # ---- public API ----

    def fetch_documents(self, limit: Optional[int] = None) -> List[RegulatoryDocument]:
        results: List[RegulatoryDocument] = []
        visited: Set[str] = set()
        try:
            categories = collect_top_categories(self.fetcher, self.base_url, self.seed_url, self.top_level_cfg)
            if limit:
                categories = categories[:limit]
                logger.info(f"Limited to first {limit} categories")

            for i, cat in enumerate(categories, 1):
                logger.info(f"[{i}/{len(categories)}] === {cat.title} ===")
                self._process(cat, [], 0, visited, results)
        finally:
            self.fetcher.close()

        logger.info(f"Done: {len(results)} documents captured from {len(visited)} pages visited")
        return results

    def run(self, limit: Optional[int] = None):
        documents = self.fetch_documents(limit=limit)
        report = validate_documents(documents, self.cfg)
        return documents, report
