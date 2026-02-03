import logging
from typing import List
from crawler.sama_circulars_crawler import SAMARulebookCrawler
from crawler.sama_laws_and_regs_crawler import SAMALawsCrawler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SAMACombinedCrawler:
    """
    Combined SAMA crawler that runs both:
    1. SAMA Rulebook Circulars Crawler
    2. SAMA Laws and Implementing Regulations Crawler
    """

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.circulars_crawler = SAMARulebookCrawler(headless=headless)
        self.laws_crawler = SAMALawsCrawler(headless=headless)
        logger.info(f"Initialized SAMACombinedCrawler (headless={headless})")

    def fetch_documents(self, limit: dict = None) -> List:
        """
        Fetch documents from both SAMA sources sequentially

        Args:
            limit: Optional dict with keys 'circulars' and 'laws' to limit results
                   Example: {'circulars': 5, 'laws': 3}

        Returns:
            Combined list of RegulatoryDocument objects from both sources
        """
        all_documents = []

        # Set limits
        circulars_limit = None
        laws_limit = None
        if limit and isinstance(limit, dict):
            circulars_limit = limit.get('circulars')
            laws_limit = limit.get('laws')

        logger.info("=" * 80)
        logger.info("STARTING SAMA COMBINED CRAWLER")
        logger.info("=" * 80)

        # 1. Fetch SAMA Circulars
        try:
            logger.info("\n[1/2] Fetching SAMA Rulebook Circulars...")
            circulars = self.circulars_crawler.fetch_documents(limit=circulars_limit)
            all_documents.extend(circulars)
            logger.info(f"✓ Fetched {len(circulars)} circulars")
        except Exception as e:
            logger.error(f"✗ Error fetching SAMA Circulars: {e}")
            import traceback
            logger.error(traceback.format_exc())

        # 2. Fetch SAMA Laws
        try:
            logger.info("\n[2/2] Fetching SAMA Laws and Implementing Regulations...")
            laws = self.laws_crawler.fetch_documents(limit=laws_limit)
            all_documents.extend(laws)
            logger.info(f"✓ Fetched {len(laws)} laws")
        except Exception as e:
            logger.error(f"✗ Error fetching SAMA Laws: {e}")
            import traceback
            logger.error(traceback.format_exc())

        logger.info("\n" + "=" * 80)
        logger.info(f"SAMA COMBINED CRAWLING COMPLETE")
        logger.info(f"Total Documents: {len(all_documents)}")
        logger.info(f"  - Circulars: {len([d for d in all_documents if d.category == 'SAMA Circulars'])}")
        logger.info(f"  - Laws: {len([d for d in all_documents if d.category == 'Laws and Implementing Regulations'])}")
        logger.info("=" * 80)

        return all_documents

    def save_to_json(self, documents: List, filename: str = "sama_all_documents.json"):
        """Save all documents to JSON file"""
        import json
        from dataclasses import asdict

        data = [asdict(doc) for doc in documents]

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"✓ Saved {len(documents)} documents to {filename}")


# Example usage
if __name__ == "__main__":
    # Create combined crawler (set headless=False to see browser)
    crawler = SAMACombinedCrawler(headless=False)

    # Fetch documents with optional limits
    documents = crawler.fetch_documents(limit={'circulars': 3, 'laws': 2})

    # Print summary
    print("\n" + "=" * 80)
    print(f"Extracted {len(documents)} total documents")
    print("=" * 80)

    # Group by category
    circulars = [d for d in documents if d.category == 'SAMA Circulars']
    laws = [d for d in documents if d.category == 'Laws and Implementing Regulations']

    print(f"\nCirculars: {len(circulars)}")
    if circulars:
        print(f"  Example: {circulars[0].title}")

    print(f"\nLaws: {len(laws)}")
    if laws:
        print(f"  Example: {laws[0].title}")

    # Save to JSON
    crawler.save_to_json(documents)