# save_cache_from_crawl.py  — run once, then delete
import sys, pickle
from pathlib import Path

sys.path.insert(0, str(Path("tests")))
from cbb_test_crawlers.cbb_rulebook_crawler import crawl_rulebook_sidebar, SIDEBAR_SEED

docs = crawl_rulebook_sidebar(
    seed_url=SIDEBAR_SEED,
    request_delay=1.2,
    max_volumes=None,
    max_workers=8,
)
with open("docs_cache.pkl", "wb") as f:
    pickle.dump(docs, f)

print(f"Cached {len(docs)} docs -> docs_cache.pkl")