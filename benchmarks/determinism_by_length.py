"""
Does output LENGTH drive the run-to-run variance, or the provider?

Sends the same prompt N times at temperature 0 with a pinned provider, at two
scales: a short extraction (~700 chars out) and the real stage 1 prompt on a
full document (~14,000 chars out). Reports how many distinct outputs came back.

Expected result: short is stable, long is not. If so, no provider/temperature
setting will fix the variance -- only not re-running (the content-hash cache).

Usage:
    python benchmarks/determinism_by_length.py
    python benchmarks/determinism_by_length.py --runs 5 --provider Alibaba
    python benchmarks/determinism_by_length.py --short-only     # cheap, ~30s
"""
import argparse
import collections
import hashlib
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
load_dotenv(REPO / ".env", override=True)

KEY = os.getenv("OPENROUTER_API_KEY")
MODEL = "deepseek/deepseek-v3.2"
BASELINE_TEXT = REPO / "benchmarks" / "runs" / "baseline" / "input_clean_text.txt"

SHORT_PROMPT = """Extract every binding obligation from the text below as minified JSON:
{"obligations":[{"id":"OB-001","text":""}]}

Text:
A bank must obtain a licence from SAMA before commencing business. The bank shall
maintain a deposit with SAMA equal to 15% of its deposit liabilities. Banks must not
grant loans exceeding 25% of reserves to any single borrower. Every bank shall submit
audited annual accounts to SAMA within three months of year end. A bank must not engage
in wholesale trade. The board shall appoint two auditors approved by SAMA. Banks are
required to publish their balance sheet in two local newspapers annually."""


def long_prompt():
    """The real stage 1 prompt on the real document."""
    from processor.staged_LLM_Analyzer import StagedLLMAnalyzer
    text = BASELINE_TEXT.read_text(encoding="utf-8")
    return StagedLLMAnalyzer()._prompt_stage1(
        text, "Banking Control Law", "SAMA", "", "", "English")


def call(prompt, provider, max_tokens):
    payload = {
        "model": MODEL,
        "temperature": 0,
        "top_p": 1,
        "seed": 20250101,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_object"},
        "provider": {"order": [provider], "allow_fallbacks": False},
    }
    r = requests.post("https://openrouter.ai/api/v1/chat/completions",
                      headers={"Authorization": f"Bearer {KEY}"},
                      json=payload, timeout=300)
    if r.status_code != 200:
        return None, None, f"HTTP {r.status_code}: {r.text[:120]}"
    b = r.json()
    content = (b["choices"][0]["message"].get("content") or "")
    return content, b.get("provider"), None


def trial(name, prompt, provider, runs, max_tokens):
    print(f"\n{name}")
    print(f"  prompt {len(prompt):,} chars, {runs} calls, provider={provider}, temp=0, seed set")
    outs, served = [], []
    for i in range(runs):
        t0 = time.perf_counter()
        content, prov, err = call(prompt, provider, max_tokens)
        if err:
            print(f"    call {i+1}: FAILED {err}")
            return
        outs.append(content)
        served.append(prov)
        print(f"    call {i+1}: {len(content):>7,} chars  "
              f"{hashlib.sha256(content.encode()).hexdigest()[:8]}  "
              f"{time.perf_counter()-t0:>5.1f}s  via {prov}")
        time.sleep(0.4)

    counts = collections.Counter(hashlib.sha256(o.encode()).hexdigest()[:8] for o in outs)
    lens = sorted({len(o) for o in outs})
    print(f"  -> {len(counts)} distinct output(s) from {runs} calls   {dict(counts)}")
    print(f"  -> output lengths: {lens}")
    print(f"  -> served by: {sorted(set(str(s) for s in served))}")
    print(f"  -> {'STABLE' if len(counts) == 1 else 'VARIES'}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--provider", default=os.getenv("LLM_PROVIDER", "AtlasCloud"))
    p.add_argument("--short-only", action="store_true", help="skip the expensive long test")
    a = p.parse_args()

    if not KEY:
        raise SystemExit("Missing OPENROUTER_API_KEY")

    print("=" * 72)
    print("Is run-to-run variance driven by output LENGTH or by the provider?")
    print("=" * 72)

    trial("SHORT generation (~700 chars out)", SHORT_PROMPT, a.provider, a.runs, 900)

    if a.short_only:
        print("\n(skipped long test; drop --short-only to run it)")
        return
    if not BASELINE_TEXT.exists():
        print(f"\nSkipping long test: {BASELINE_TEXT} not found")
        return

    trial("LONG generation (real stage 1, ~14,000 chars out)",
          long_prompt(), a.provider, a.runs, 16000)

    print("\n" + "=" * 72)
    print("If SHORT is STABLE and LONG VARIES, the provider is not the problem and")
    print("no temperature/seed setting will fix it. The fix is the content-hash")
    print("cache -- analyse once, store, reuse. See docs/determinism.md.")
    print("=" * 72)


if __name__ == "__main__":
    main()
