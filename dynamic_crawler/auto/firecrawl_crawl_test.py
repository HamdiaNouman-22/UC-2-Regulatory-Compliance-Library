"""Standalone Firecrawl CRAWL test — purely additive, writes only local files.

Plain-English purpose: point this at ONE regulator seed page. It asks Firecrawl
to start there, walk all the nested subpages, and hand back every document page
it can find. We save that link list locally so you can eyeball whether Firecrawl
finds the same documents your hand-built adapter finds — BEFORE we commit to it.

This touches nothing in the live pipeline and never writes the production DB.
It only calls the Firecrawl API and drops files under output/firecrawl_test/.

Talks to the Firecrawl v2 REST API directly with `requests` (no SDK needed),
the same raw-requests style used by dynamic_crawler/onboarding/llm_client.py.

Usage:
    python -m dynamic_crawler.auto.firecrawl_crawl_test
    python -m dynamic_crawler.auto.firecrawl_crawl_test --url <seed> --limit 50
"""

import argparse
import json
import logging
import os
import re
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Firecrawl's current API is v2. Override with FIRECRAWL_API_URL if your account differs.
API_BASE = os.getenv("FIRECRAWL_API_URL", "https://api.firecrawl.dev/v2").rstrip("/")

# Default test target: SAMA rulebook (a nested sidebar_tree site — crawl's sweet spot).
DEFAULT_SEED = "https://rulebook.sama.gov.sa/en/book-category/1365"

OUTPUT_DIR = Path("output") / "firecrawl_test"


class FirecrawlError(RuntimeError):
    pass


def _headers(api_key: str) -> dict:
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


def start_crawl(api_key: str, seed_url: str, limit: int, max_depth: int) -> str:
    """Kick off a crawl job. Returns the job id to poll."""
    body = {
        "url": seed_url,
        "limit": limit,                 # safety cap so the test stays cheap/fast
        "maxDiscoveryDepth": max_depth,  # how deep into nested subpages to walk
        # SAMA's document pages live on a different path than the seed, so by
        # default Firecrawl treats them as out-of-scope. This tells it to follow
        # links anywhere on the same domain.
        "crawlEntireDomain": True,
        "scrapeOptions": {"formats": ["markdown"]},
    }
    resp = requests.post(f"{API_BASE}/crawl", headers=_headers(api_key), json=body, timeout=60)
    if resp.status_code >= 400:
        raise FirecrawlError(f"crawl start failed {resp.status_code}: {resp.text[:500]}")
    data = resp.json()
    job_id = data.get("id")
    if not job_id:
        raise FirecrawlError(f"no job id in crawl response: {data}")
    logger.info("Crawl started. job_id=%s", job_id)
    return job_id


def poll_crawl(api_key: str, job_id: str, poll_seconds: int = 5, max_wait: int = 900) -> dict:
    """Poll the crawl job until it finishes; follow pagination to collect all pages."""
    status_url = f"{API_BASE}/crawl/{job_id}"
    collected = []
    waited = 0
    status = "scraping"
    payload = {}
    while waited <= max_wait:
        resp = requests.get(status_url, headers=_headers(api_key), timeout=60)
        if resp.status_code >= 400:
            raise FirecrawlError(f"crawl status failed {resp.status_code}: {resp.text[:500]}")
        payload = resp.json()
        status = payload.get("status", "unknown")
        done, total = payload.get("completed", 0), payload.get("total", 0)
        logger.info("status=%s  pages=%s/%s  collected=%s", status, done, total, len(collected))

        collected.extend(payload.get("data", []) or [])
        # Firecrawl paginates large result sets via a `next` cursor URL.
        next_url = payload.get("next")
        while next_url:
            r = requests.get(next_url, headers=_headers(api_key), timeout=60)
            r.raise_for_status()
            page = r.json()
            collected.extend(page.get("data", []) or [])
            next_url = page.get("next")

        if status in ("completed", "failed", "cancelled"):
            break
        time.sleep(poll_seconds)
        waited += poll_seconds

    payload["data"] = collected
    return payload


def _url_of(item: dict) -> str:
    md = item.get("metadata") or {}
    return md.get("sourceURL") or md.get("url") or item.get("url") or ""


def main() -> None:
    ap = argparse.ArgumentParser(description="Firecrawl crawl test (additive, local-only)")
    ap.add_argument("--url", default=DEFAULT_SEED, help="Seed URL to start the crawl from")
    ap.add_argument("--limit", type=int, default=30, help="Max pages to crawl (keep small for a test)")
    ap.add_argument("--max-depth", type=int, default=5, help="How deep to follow nested subpages")
    ap.add_argument("--label", default=None, help="Folder label for outputs (default: derived from host)")
    args = ap.parse_args()

    api_key = os.getenv("FIRECRAWL_KEY")
    if not api_key:
        raise FirecrawlError("Missing FIRECRAWL_KEY in environment / .env")

    label = args.label or re.sub(r"[^a-zA-Z0-9]+", "_", args.url.split("//")[-1])[:60]
    out_dir = OUTPUT_DIR / label
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Seed: %s", args.url)
    logger.info("Limit=%s  max_depth=%s  -> %s", args.limit, args.max_depth, out_dir)

    job_id = start_crawl(api_key, args.url, args.limit, args.max_depth)
    result = poll_crawl(api_key, job_id)

    pages = result.get("data", []) or []
    urls = sorted({u for u in (_url_of(p) for p in pages) if u})

    # Save raw result (everything Firecrawl returned) + a clean link list for eyeballing.
    (out_dir / "crawl_raw.json").write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    (out_dir / "links.txt").write_text("\n".join(urls), encoding="utf-8")

    print("\n" + "=" * 70)
    print(f"Firecrawl crawl finished: status={result.get('status')}")
    print(f"Pages returned: {len(pages)}   Unique URLs: {len(urls)}")
    print(f"Saved:\n  {out_dir / 'links.txt'}\n  {out_dir / 'crawl_raw.json'}")
    print("=" * 70)
    for u in urls[:40]:
        print("  ", u)
    if len(urls) > 40:
        print(f"   ... and {len(urls) - 40} more (see links.txt)")


if __name__ == "__main__":
    main()
