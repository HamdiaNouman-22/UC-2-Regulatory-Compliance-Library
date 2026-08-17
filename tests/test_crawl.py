import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
"""
Quick crawl tester — prints live as each doc is found.
Run: python test_crawl.py
"""
from cbb_test_crawlers.cbb_rulebook_crawler import _collect_volumes, _process, SIDEBAR_SEED

# ── Step 1: list all volumes ──────────────────────────────────────────────────
print("Fetching volume list...")
vols = _collect_volumes(SIDEBAR_SEED)
print(f"\nVolumes found: {len(vols)}")
for i, v in enumerate(vols, 1):
    print(f"  {i}. {v.text} | children visible in seed sidebar: {len(v.children)}")

# ── Step 2: crawl volume 0 live ───────────────────────────────────────────────
# Change vols[0] to vols[1], vols[2] etc. to test other volumes
TARGET_VOL = 0
vol = vols[TARGET_VOL]
print(f"\n{'='*60}")
print(f"Crawling: {vol.text}")
print(f"{'='*60}\n")

results  = []
visited  = set()
counters = {"folders": 0, "leaves": 0}

# Monkey-patch _process to print live
import cbb_test_crawlers.cbb_rulebook_crawler as mod
original_process = mod._process

def live_process(node, path, depth, visited, results, request_delay):
    before = len(results)
    original_process(node, path, depth, visited, results, request_delay)
    after = len(results)
    for doc in results[before:after]:
        tag = "[F]" if doc.is_folder else "[R]"
        print(f"  {tag} {'  ' * doc.depth}{' > '.join(doc.doc_path[-2:])}")

mod._process = live_process

original_process(vol, ["CBB Rulebook"], 0, visited, results, 1.2)

# ── Summary ───────────────────────────────────────────────────────────────────
folders = [d for d in results if d.is_folder]
leaves  = [d for d in results if not d.is_folder]
print(f"\n{'='*60}")
print(f"DONE — {vol.text}")
print(f"  Total : {len(results)}")
print(f"  Folders: {len(folders)}")
print(f"  Leaves : {len(leaves)}")