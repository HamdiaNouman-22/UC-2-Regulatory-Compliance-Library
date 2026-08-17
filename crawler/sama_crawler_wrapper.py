import logging
from typing import List
from crawler.sama_circulars_crawler import SAMARulebookCrawler
from crawler.sama_laws_and_regs_crawler import SAMALawsCrawler
from crawler.sama_rulebook_crawler import SAMAFullRulebookCrawler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SAMACombinedCrawler:
    """
    Combined SAMA crawler that runs:
    1. SAMA Rulebook Circulars Crawler
    2. SAMA Full Rulebook Crawler -- all sectors (Laws and Implementing
       Regulations, All Financial Institutions, Banking Sector, Finance
       Sector, Payment Systems, Money Exchange Sector, Credit Bureaus,
       Regulatory Sandbox), recursively, superseding the old SAMALawsCrawler's
       narrower "Laws and Implementing Regulations"-only coverage.
    3. SBP FE Manual Appendix III notifications -- unrelated to SAMA, but
       this crawl has always lived inside SAMALawsCrawler as a bundled side
       fetch; kept as-is (via SAMALawsCrawler.fetch_appendix3_documents) so
       this existing behavior isn't lost when its SAMA-laws half is replaced.
    """

    #: One label per sub-crawl, so the completeness gate sizes each against its
    #: own history. Without this, Circulars dying entirely hides inside the
    #: total's tolerance — the rulebook is large enough to absorb the loss.
    SOURCE_LABELS = ["SAMA Circulars", "SAMA Rulebook", "SBP Appendix III"]

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.circulars_crawler = SAMARulebookCrawler(headless=headless)
        self.full_rulebook_crawler = SAMAFullRulebookCrawler(use_selenium=False)
        self.sbp_appendix3_crawler = SAMALawsCrawler(headless=headless)
        self.last_result: dict = {}
        logger.info(f"Initialized SAMACombinedCrawler (headless={headless})")

    @property
    def source_names(self) -> List[str]:
        return list(self.SOURCE_LABELS)

    def fetch_documents(self, limit: dict = None) -> List:
        """
        Fetch documents from all SAMA/bundled sources sequentially.

        Args:
            limit: Optional dict with keys 'circulars', 'rulebook' and
                   'appendix3' to limit results.
                   Example: {'circulars': 5, 'rulebook': 2, 'appendix3': 3}

        Returns:
            Combined list of RegulatoryDocument objects from all sources
        """
        all_documents = []
        failures: List[str] = []          # which sub-crawls raised, and why
        counts: dict = {}                 # label -> documents, for the gate

        circulars_limit = None
        rulebook_limit = None
        appendix3_limit = None
        if limit and isinstance(limit, dict):
            circulars_limit = limit.get('circulars')
            rulebook_limit = limit.get('rulebook')
            appendix3_limit = limit.get('appendix3')

        logger.info("=" * 80)
        logger.info("STARTING SAMA COMBINED CRAWLER")
        logger.info("=" * 80)

        # 1. Fetch SAMA Circulars
        try:
            logger.info("\n[1/3] Fetching SAMA Rulebook Circulars...")
            circulars = self.circulars_crawler.fetch_documents(limit=circulars_limit)
            all_documents.extend(circulars)
            counts["SAMA Circulars"] = len(circulars)
            logger.info(f"Fetched {len(circulars)} circulars")
        except Exception as e:
            logger.error(f"Error fetching SAMA Circulars: {e}")
            import traceback
            logger.error(traceback.format_exc())
            failures.append(f"SAMA Circulars: {type(e).__name__}: {e}")
            counts["SAMA Circulars"] = 0

        # 2. Fetch full SAMA Rulebook (all sectors except Circulars)
        try:
            logger.info("\n[2/3] Fetching full SAMA Rulebook (all sectors)...")
            rulebook_docs = self.full_rulebook_crawler.fetch_documents(limit=rulebook_limit)
            all_documents.extend(rulebook_docs)
            counts["SAMA Rulebook"] = len(rulebook_docs)
            logger.info(f"Fetched {len(rulebook_docs)} rulebook documents")
        except Exception as e:
            logger.error(f"Error fetching SAMA Rulebook: {e}")
            import traceback
            logger.error(traceback.format_exc())
            failures.append(f"SAMA Rulebook: {type(e).__name__}: {e}")
            counts["SAMA Rulebook"] = 0

        # 3. Fetch SBP FE Manual Appendix III (unrelated to SAMA, but bundled
        #    here historically -- preserved as its own driver lifecycle so a
        #    failure here can't take down the rulebook fetch above or vice versa)
        try:
            logger.info("\n[3/3] Fetching SBP FE Manual Appendix III...")
            self.sbp_appendix3_crawler._init_driver()
            try:
                appendix3_docs = self.sbp_appendix3_crawler.fetch_appendix3_documents(limit=appendix3_limit)
            finally:
                self.sbp_appendix3_crawler._close_driver()
            all_documents.extend(appendix3_docs)
            counts["SBP Appendix III"] = len(appendix3_docs)
            logger.info(f"Fetched {len(appendix3_docs)} SBP Appendix III notifications")
        except Exception as e:
            logger.error(f"Error fetching SBP Appendix III: {e}")
            import traceback
            logger.error(traceback.format_exc())
            failures.append(f"SBP Appendix III: {type(e).__name__}: {e}")
            counts["SBP Appendix III"] = 0

        logger.info("\n" + "=" * 80)
        logger.info(f"SAMA COMBINED CRAWLING COMPLETE")
        logger.info(f"Total Documents: {len(all_documents)}")
        logger.info(f"  - Circulars: {len([d for d in all_documents if d.category == 'SAMA Circulars'])}")
        logger.info(f"  - Rulebook (non-Circulars): {len([d for d in all_documents if d.regulator == 'SAMA' and d.category != 'SAMA Circulars'])}")
        logger.info(f"  - SBP Appendix III: {len([d for d in all_documents if d.regulator == 'SBP'])}")
        logger.info("=" * 80)

        # The shape the completeness gate reads.
        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": failures},
            "by_source": counts,
            "failed_sources": [f.split(":")[0] for f in failures],
        }

        # AN EMPTY SAMA IS A BROKEN CRAWLER, NOT AN EMPTY REGULATOR.
        #
        # All three fetches above catch their own exceptions, log a traceback
        # and carry on — deliberately, so one failing cannot take the other two
        # down. The cost is that ALL THREE failing returns an empty list with no
        # exception, and from the caller's side that is indistinguishable from a
        # regulator that publishes nothing. The completeness gate then takes the
        # zero as a valid baseline and never reports the gap.
        #
        # This is not hypothetical. CMA reached exactly this state on
        # 2026-08-12T00:43 — nine tabs, no exception, 0 documents, verdict PASS,
        # caused by a transient network outage rather than anything on the site.
        #
        # SAMA holds 651 documents in the library and is the largest source by
        # an order of magnitude, so zero is never a true answer.
        if not all_documents:
            raise RuntimeError(
                "SAMA returned 0 documents. All three sub-crawls swallow their "
                "own errors, so this is a failed run rather than an empty "
                "regulator. Failures: "
                + ("; ".join(failures) if failures else
                   "none raised — every source returned an empty list, which "
                   "usually means the site did not load"))

        return all_documents

    def save_to_json(self, documents: List, filename: str = "sama_all_documents.json"):
        """Save all documents to JSON file"""
        import json
        from dataclasses import asdict

        data = [asdict(doc) for doc in documents]

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved {len(documents)} documents to {filename}")


# Example usage
if __name__ == "__main__":
    # Create combined crawler (set headless=False to see browser)
    crawler = SAMACombinedCrawler(headless=False)

    # Fetch documents with optional limits
    documents = crawler.fetch_documents(limit={'circulars': 3, 'rulebook': 2})

    # Print summary
    print("\n" + "=" * 80)
    print(f"Extracted {len(documents)} total documents")
    print("=" * 80)

    # Group by category
    circulars = [d for d in documents if d.category == 'SAMA Circulars']
    rulebook = [d for d in documents if d.regulator == 'SAMA' and d.category != 'SAMA Circulars']

    print(f"\nCirculars: {len(circulars)}")
    if circulars:
        print(f"  Example: {circulars[0].title}")

    print(f"\nRulebook (non-Circulars): {len(rulebook)}")
    if rulebook:
        print(f"  Example: {rulebook[0].title}")

    # Save to JSON
    crawler.save_to_json(documents)
