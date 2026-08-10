"""
tests/test_smart_matching.py  —  READ-ONLY, no DB writes

Fetches TR revision feed May 25-June 30 2026 and tests two matching strategies
for identifying whether a changed page is a new regulation or an update of an
existing one.

Approach A  — section-code prefix match (fast, deterministic)
Approach B  — vector similarity (sentence-transformers or difflib fallback)
              + LLM (OpenRouter) with confidence score

Additions included:
  - Cross-folder section-code fallback when same-folder yields 0 candidates
  - Confidence field on LLM output; low-confidence flagged separately
  - Smart content snippet: amendment history box + keyword-triggered sentences
    + first paragraph (not just head-of-document)

Run:
    ./venv/Scripts/python.exe tests/test_smart_matching.py

Optional speedup (better similarity):
    pip install sentence-transformers
"""
import sys, os, re, json, time, logging, io
# Force UTF-8 output so Unicode characters don't crash on Windows cp1252 console
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from datetime import date

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import requests
import pyodbc

# ── Optional: sentence-transformers ──────────────────────────────────────────
try:
    from sentence_transformers import SentenceTransformer
    import numpy as np
    _model = SentenceTransformer("all-MiniLM-L6-v2")
    def _embed(texts: List[str]):
        return _model.encode(texts, normalize_embeddings=True)
    def _sim(a, b) -> float:          # a, b are embedding vectors
        return float(np.dot(a, b))
    USE_VECTORS = True
    print("[embed] sentence-transformers loaded  OK")
except ImportError:
    import difflib
    def _sim(a: str, b: str) -> float:  # a, b are strings in fallback
        return difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio()
    USE_VECTORS = False
    print("[embed] sentence-transformers not found — using difflib similarity")
    print("        install: pip install sentence-transformers\n")

# Import TR feed helpers from existing crawler
from crawler.cbb_monitoring_crawler import (
    _get_thomson_reuters_changes,
    _extract_doc_path_from_page,
    _extract_toc_links,
    _extract_content,
    _extract_section_code,
    _fetch,
    EMPTY_HASH,
)
import hashlib

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

# ── Config ────────────────────────────────────────────────────────────────────
FROM_DATE      = date(2026, 5, 25)
TO_DATE        = date(2026, 6, 30)
MAX_PAGES      = 20        # cap so test finishes in ~25 min
SIM_THRESH     = 0.50      # minimum similarity to treat as candidate
CONFIDENCE_MIN = 0.80      # below this → flag for review instead of auto-commit
SLEEP_S        = 1.5       # polite delay between TR fetches
OR_MODEL       = "deepseek/deepseek-v3.2"  # via OpenRouter (same as rest of project)
OR_URL         = "https://openrouter.ai/api/v1/chat/completions"
OUT_PATH       = ROOT / "tests" / "smart_match_results.json"

# Change-type keywords to look for in page content
_CHANGE_KWS = re.compile(
    r'\b(deleted|moved\s+to|transferred|repealed|amended|revised|replaced\s+by)\b',
    re.IGNORECASE,
)

# ── DB ────────────────────────────────────────────────────────────────────────
def _conn():
    cs = (
        f"DRIVER={os.getenv('MSSQL_DRIVER')};"
        f"SERVER={os.getenv('MSSQL_SERVER')};"
        f"DATABASE={os.getenv('MSSQL_DATABASE')};"
        f"UID={os.getenv('MSSQL_USERNAME')};"
        f"PWD={os.getenv('MSSQL_PASSWORD')};"
        f"TrustServerCertificate=yes;"
        f"ConnectRetryCount=3;ConnectRetryInterval=5;"
    )
    return pyodbc.connect(cs, timeout=30)

def load_db() -> Tuple[Dict, Dict, Dict]:
    conn = _conn()
    cur  = conn.cursor()

    cur.execute("""
        SELECT r.id, r.title, r.compliancecategory_id, r.source_page_url,
               rv.content_text
        FROM   regulations r
        LEFT JOIN (
            SELECT regulation_id, content_text
            FROM   regulation_versions
            WHERE  status = 'active'
        ) rv ON rv.regulation_id = r.id
        WHERE  r.regulator = 'Central Bank of Bahrain'
    """)
    regs: Dict[int, dict] = {}
    for row in cur.fetchall():
        rid = row[0]
        if rid not in regs:   # keep first active version per reg
            regs[rid] = {
                "id": rid, "title": row[1] or "",
                "cat_id": row[2], "url": row[3] or "",
                "content": (row[4] or "")[:600],
            }

    cur.execute("SELECT compliancecategory_id, title, parentid FROM compliancecategory")
    cats: Dict[int, dict] = {
        r[0]: {"id": r[0], "title": r[1] or "", "parent": r[2]}
        for r in cur.fetchall()
    }

    regs_by_cat: Dict[int, List[dict]] = {}
    for reg in regs.values():
        regs_by_cat.setdefault(reg["cat_id"], []).append(reg)

    # Also index all regs by section code for cross-folder fallback
    regs_by_sc: Dict[str, List[dict]] = {}
    for reg in regs.values():
        sc = _extract_section_code(reg["title"])
        if sc:
            regs_by_sc.setdefault(sc.upper(), []).append(reg)

    conn.close()
    return regs, cats, regs_by_cat, regs_by_sc


# ── Folder resolution ─────────────────────────────────────────────────────────
def resolve_folder(doc_path: List[str], cats: Dict) -> Optional[int]:
    parent, last = None, None
    for step in doc_path:
        match = next(
            (c["id"] for c in cats.values()
             if c["title"] == step and c["parent"] == parent), None
        )
        if match is None:
            sc = _extract_section_code(step)
            if sc:
                match = next(
                    (c["id"] for c in cats.values()
                     if c["title"].upper().startswith(sc.upper())
                     and c["parent"] == parent), None
                )
        if match is None:
            break
        last = parent = match
    return last

def ancestors(cat_id: int, cats: Dict) -> List[int]:
    chain, seen = [], set()
    cid = cat_id
    while cid and cid not in seen:
        seen.add(cid); chain.append(cid)
        cid = cats.get(cid, {}).get("parent")
    return chain


# ── Smart content snippet ─────────────────────────────────────────────────────
def smart_snippet(soup) -> str:
    """
    Three-part snippet (not just head-of-document):
      1. Amendment history box at the bottom of TR pages
      2. Sentences containing change keywords (deleted/moved/amended…)
      3. First paragraph as context
    """
    parts = []

    # 1. Amendment history box
    for box in soup.find_all(["div", "p", "table"],
                              class_=re.compile(r'amend|history|revision', re.I)):
        txt = box.get_text(" ", strip=True)
        if txt:
            parts.append(f"[History] {txt[:200]}")
            break

    # 2. Keyword sentences anywhere in the page
    body = soup.find("div", class_="field-items") or soup.find("article")
    if body:
        full_text = body.get_text(" ", strip=True)
        sentences = re.split(r'(?<=[.!?])\s+', full_text)
        kw_sents = [s for s in sentences if _CHANGE_KWS.search(s)]
        if kw_sents:
            parts.append("[Change] " + " | ".join(kw_sents[:3])[:300])

        # 3. First paragraph (fallback context)
        first_para = full_text[:250]
        parts.append(f"[Intro] {first_para}")

    return "\n".join(parts) if parts else ""


def _sig(text: str) -> str:
    """Normalized 500-char fingerprint for content-identity check."""
    return re.sub(r'\s+', ' ', (text or "").lower().strip())[:500]


# ── URL volume-suffix extractor ───────────────────────────────────────────────
def _url_vol_suffix(url: str) -> str:
    """Extract trailing '-N' suffix from URL slug.  '' for no suffix, '-0'/'-1' etc."""
    slug = url.rstrip("/").split("/")[-1]
    m = re.search(r'-(\d+)$', slug)
    return f"-{m.group(1)}" if m else ""


def _path_overlap(cat_id: int, doc_path: List[str], cats: Dict) -> int:
    """Count how many ancestor category titles appear in the incoming doc_path."""
    doc_set = set(doc_path)
    return sum(1 for cid in ancestors(cat_id, cats)
               if cid in cats and cats[cid]["title"] in doc_set)


# ── Approach A: section-code prefix match ────────────────────────────────────
def approach_a(title: str, folder_id: Optional[int],
               regs_by_cat: Dict, regs_by_sc: Dict, cats: Dict,
               new_url: str = "", doc_path: Optional[List[str]] = None) -> Optional[Dict]:
    sc = _extract_section_code(title)
    if not sc:
        return None

    # Stage 1: same folder subtree
    for cat_id in ancestors(folder_id, cats) if folder_id else []:
        for reg in regs_by_cat.get(cat_id, []):
            reg_sc = _extract_section_code(reg["title"])
            if reg_sc and reg_sc.upper() == sc.upper():
                return {"reg_id": reg["id"], "reg_title": reg["title"],
                        "method": "section_code_in_folder", "score": 1.0}

    # Stage 2: cross-folder fallback (handles module reorganisations)
    matches = regs_by_sc.get(sc.upper(), [])
    if len(matches) == 1:
        return {"reg_id": matches[0]["id"], "reg_title": matches[0]["title"],
                "method": "section_code_global_unique", "score": 0.9}
    if len(matches) > 1:
        # Primary: path overlap — pick candidate whose ancestor categories best match doc_path
        if doc_path:
            scored = [(m, _path_overlap(m["cat_id"], doc_path, cats)) for m in matches]
            scored.sort(key=lambda x: x[1], reverse=True)
            if scored[0][1] > scored[1][1]:   # clear winner
                best = scored[0][0]
                return {"reg_id": best["id"], "reg_title": best["title"],
                        "method": "section_code_path_match", "score": 0.95}

        # Fallback: URL volume suffix ('-0' = Islamic banks, '' = conventional)
        if new_url:
            incoming_sfx = _url_vol_suffix(new_url)
            vol_match = [m for m in matches
                         if _url_vol_suffix(m.get("url", "")) == incoming_sfx]
            if len(vol_match) == 1:
                return {"reg_id": vol_match[0]["id"], "reg_title": vol_match[0]["title"],
                        "method": "section_code_url_suffix", "score": 0.90}

        return {"reg_id": None, "reg_title": f"{len(matches)} candidates",
                "method": "section_code_global_ambiguous", "score": 0.5}

    return None


# ── Approach B: vector similarity + LLM ──────────────────────────────────────
def get_candidates(title: str, folder_id: Optional[int],
                   regs_by_cat: Dict, regs_by_sc: Dict,
                   cats: Dict) -> List[Dict]:
    pool = []

    # Stage 1: same folder subtree
    for cid in (ancestors(folder_id, cats) if folder_id else [])[:5]:
        pool.extend(regs_by_cat.get(cid, []))

    # Stage 2: cross-folder by section code (handles moves)
    sc = _extract_section_code(title)
    if sc:
        for reg in regs_by_sc.get(sc.upper(), []):
            if reg not in pool:
                pool.append(reg)

    if not pool:
        return []

    if USE_VECTORS:
        all_titles = [title] + [r["title"] for r in pool]
        embs = _embed(all_titles)
        q = embs[0]
        import numpy as np
        scored = [(float(np.dot(q, embs[i + 1])), r) for i, r in enumerate(pool)]
    else:
        scored = [(_sim(title, r["title"]), r) for r in pool]

    scored.sort(key=lambda x: x[0], reverse=True)
    return [{"score": round(s, 3), **r} for s, r in scored if s >= SIM_THRESH][:5]


def llm_decide(new_title: str, new_url: str, doc_path: List[str],
               snippet: str, candidates: List[Dict]) -> Dict:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        return {"is_update": False, "reg_id": None, "change_type": "ERROR",
                "confidence": 0.0, "reason": "OPENROUTER_API_KEY not set"}

    if not candidates:
        return {"is_update": False, "reg_id": None, "change_type": "NEW",
                "confidence": 0.95, "reason": "no similar candidates found"}

    cand_txt = "\n".join(
        f"  [{c['id']}] \"{c['title']}\" "
        f"(similarity {c['score']:.2f}, url: {c.get('url','')[:60]})"
        for c in candidates
    )
    prompt = (
        "You are a compliance analyst reviewing a CBB regulatory update.\n\n"
        f"NEW page from TR revision feed:\n"
        f"  Title  : {new_title}\n"
        f"  URL    : {new_url}\n"
        f"  Path   : {' >> '.join(doc_path) if doc_path else '(unknown)'}\n"
        f"  Content:\n{snippet}\n\n"
        f"EXISTING DB candidates (same folder/section-code, sorted by similarity):\n"
        f"{cand_txt}\n\n"
        "Decide: is this an UPDATE of a candidate, or a GENUINELY NEW section?\n\n"
        "Think like a human analyst:\n"
        "- Same section code (e.g. AU-5.5 in both) → almost certainly same regulation\n"
        "- Content says 'moved', 'deleted', 'amended' → update of the matching section\n"
        "- Old URL was node/revision, new is a slug → still check section codes, may be same\n"
        "- Completely different section code/topic → new\n\n"
        "Reply ONLY with valid JSON, nothing else:\n"
        '{"is_update": true|false, "reg_id": <number or null>, '
        '"change_type": "AMENDED"|"DELETED"|"MOVED"|"RENAMED"|"NEW", '
        '"confidence": <0.0-1.0>, '
        '"reason": "<one sentence explaining the decision>"}'
    )

    def _call_llm(messages):
        resp = requests.post(
            OR_URL,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": OR_MODEL, "max_tokens": 250, "messages": messages},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()

    def _parse_json(text: str):
        text = re.sub(r'^```(?:json)?\s*|\s*```$', '', text, flags=re.S).strip()
        # Extract first {...} block if there's surrounding prose
        m = re.search(r'\{.*\}', text, re.S)
        if m:
            text = m.group(0)
        return json.loads(text)

    try:
        messages = [{"role": "user", "content": prompt}]
        text = _call_llm(messages)
        try:
            result = _parse_json(text)
        except Exception:
            # Retry: ask model to return only the JSON object
            messages.append({"role": "assistant", "content": text})
            messages.append({"role": "user",
                              "content": "Your previous reply was not valid JSON. "
                                         "Reply ONLY with the JSON object, no extra text."})
            text2 = _call_llm(messages)
            result = _parse_json(text2)
        result.setdefault("confidence", 0.5)
        return result
    except Exception as e:
        return {"is_update": False, "reg_id": None, "change_type": "ERROR",
                "confidence": 0.0, "reason": str(e)[:120]}


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*68}")
    print(f"  Smart Matching Test  |  {FROM_DATE} to {TO_DATE}  |  max {MAX_PAGES} pages")
    print(f"  Similarity: {'sentence-transformers (vectors)' if USE_VECTORS else 'difflib (string ratio)'}")
    print(f"{'='*68}\n")

    print("Loading DB...")
    regs, cats, regs_by_cat, regs_by_sc = load_db()
    print(f"  {len(regs):,} regulations  |  {len(cats):,} categories  |  "
          f"{len(regs_by_sc):,} unique section codes\n")

    print("Fetching TR revision feed...")
    feed = _get_thomson_reuters_changes(FROM_DATE, TO_DATE)[:MAX_PAGES]
    print(f"  Processing {len(feed)} entries\n")

    results        = []
    agree_count    = 0
    disagree_count = 0
    low_conf_count = 0
    a_hits         = 0
    b_hits         = 0

    # Work queue: (url, feed_title, change_date, depth)
    # Folder pages get expanded; their children are appended to the queue
    visited = set()
    queue = [(e["url"], e["title"], e.get("change_date"), 0) for e in feed]

    i = 0
    while queue and len(results) < MAX_PAGES:
        url, feed_title, change_date, depth = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        i += 1
        indent = "  " * depth
        print(f"[{i:3}] {indent}{feed_title[:65]}")

        time.sleep(SLEEP_S)
        soup = _fetch(url)
        if not soup:
            print(f"       {indent}SKIP: fetch failed\n")
            continue

        # ── Detect folder page by empty content hash ──────────────────────────
        content_data = _extract_content(soup)
        content_hash = hashlib.md5(
            (content_data.get("content_text") or "").encode()
        ).hexdigest()

        if content_hash == EMPTY_HASH:
            children = _extract_toc_links(soup)
            if children:
                print(f"       {indent}FOLDER - expanding {len(children)} children")
                h1 = soup.find("h1") or soup.find("h2", class_="page-title")
                folder_title = h1.get_text(strip=True) if h1 else feed_title
                for child_url in children:
                    if child_url not in visited:
                        queue.insert(0, (child_url, child_url.split("/")[-1],
                                         change_date, depth + 1))
            else:
                print(f"       {indent}FOLDER - no children found, skipping")
            print()
            continue

        doc_path   = _extract_doc_path_from_page(soup)
        h1         = soup.find("h1") or soup.find("h2", class_="page-title")
        page_title = h1.get_text(strip=True) if h1 else feed_title
        snippet    = smart_snippet(soup)
        folder_id  = resolve_folder(doc_path, cats)

        # ── Content-identity check (skip TR false positives) ─────────────────
        new_content = (content_data.get("content_text") or "").strip()

        new_sig = _sig(new_content)

        # ── Approach A ───────────────────────────────────────────────────────
        a_res = approach_a(page_title, folder_id, regs_by_cat, regs_by_sc, cats,
                           new_url=url, doc_path=doc_path)

        # ── Approach B ───────────────────────────────────────────────────────
        candidates = get_candidates(page_title, folder_id, regs_by_cat, regs_by_sc, cats)

        # Compare against the A-matched candidate if resolved, else fallback to top-scored
        _decisive = {"section_code_in_folder", "section_code_volume_match",
                     "section_code_path_match", "section_code_global_unique"}
        a_decisive = a_res and a_res.get("method") in _decisive and a_res.get("reg_id")
        if a_decisive:
            top = next((c for c in candidates if c["id"] == a_res["reg_id"]),
                       candidates[0] if candidates else None)
        else:
            top = candidates[0] if candidates else None

        if top and new_sig and _sig(top.get("content", "")) == new_sig:
            print(f"         UNCHANGED (content identical to reg={top['id']}) - skipping\n")
            results.append({
                "i": i, "url": url, "date": change_date,
                "feed_title": feed_title, "page_title": page_title,
                "doc_path": doc_path, "folder_id": folder_id,
                "section_code": _extract_section_code(page_title),
                "snippet": snippet[:400], "candidates": candidates,
                "approach_a": a_res,
                "approach_b": {"is_update": False, "reg_id": top["id"],
                               "change_type": "UNCHANGED",
                               "confidence": 1.0,
                               "reason": "content identical to existing DB record — TR re-publish"},
                "agreed": True, "needs_review": False,
            })
            continue

        # If A already resolved identity with high confidence, narrow LLM to that candidate only
        # (LLM's job becomes: what is the change_type? — not re-selecting the reg)
        if a_decisive:
            a_matched = [c for c in candidates if c["id"] == a_res["reg_id"]]
            llm_candidates = a_matched if a_matched else candidates
        else:
            llm_candidates = candidates

        b_res = llm_decide(page_title, url, doc_path, snippet, llm_candidates)

        # ── Compare ──────────────────────────────────────────────────────────
        a_id = a_res["reg_id"] if a_res else None
        b_id = b_res.get("reg_id")
        conf = b_res.get("confidence", 0.0)

        agreed = (a_id == b_id) or (a_res is None and not b_res.get("is_update"))
        needs_review = conf < CONFIDENCE_MIN

        if a_res and a_res["reg_id"]:  a_hits += 1
        if b_res.get("is_update"):     b_hits += 1
        if agreed:                     agree_count    += 1
        else:                          disagree_count += 1
        if needs_review:               low_conf_count += 1

        a_str = (f"MATCH reg={a_id} via {a_res['method']}" if a_res and a_id
                 else ("AMBIGUOUS" if a_res else "NEW (no code)"))
        b_str = (f"{b_res.get('change_type')} reg={b_id} conf={conf:.2f} "
                 f"| {b_res.get('reason','')[:55]}")
        flag  = "  !! LOW CONFIDENCE -> review" if needs_review else ""
        agree_str = "AGREE" if agreed else "X DISAGREE"

        print(f"         A: {a_str}")
        print(f"         B: {b_str}{flag}")
        print(f"         {agree_str}\n")

        results.append({
            "i":          i,
            "url":        url,
            "date":       change_date,
            "feed_title": feed_title,
            "page_title": page_title,
            "doc_path":   doc_path,
            "folder_id":  folder_id,
            "section_code": _extract_section_code(page_title),
            "snippet":    snippet[:400],
            "candidates": candidates,
            "approach_a": a_res,
            "approach_b": b_res,
            "agreed":     agreed,
            "needs_review": needs_review,
        })

    # ── Save ──────────────────────────────────────────────────────────────────
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)

    # ── Summary ───────────────────────────────────────────────────────────────
    total = len(results)
    pct   = lambda n: f"{100*n//total if total else 0}%"

    print(f"\n{'='*68}")
    print(f"  RESULTS — {total} pages processed")
    print(f"  Approach A (section-code)     : {a_hits:3} matches     ({pct(a_hits)})")
    print(f"  Approach B (vector + LLM)     : {b_hits:3} updates     ({pct(b_hits)})")
    print(f"  Agreement                     : {agree_count}/{total}  ({pct(agree_count)})")
    print(f"  Disagreements                 : {disagree_count}  ({pct(disagree_count)})")
    print(f"  Low-confidence (need review)  : {low_conf_count}  ({pct(low_conf_count)})")
    print(f"  Full results -> {OUT_PATH}")
    print(f"{'='*68}\n")

    # Print disagreements for quick review
    diffs = [r for r in results if not r["agreed"]]
    if diffs:
        print(f"DISAGREEMENTS ({len(diffs)}) — cases where A and B don't agree:")
        for d in diffs:
            print(f"  [{d['i']}] {d['page_title'][:60]}")
            print(f"       A: {d['approach_a']}")
            print(f"       B: {d['approach_b']}")
            print()

    review_cases = [r for r in results if r["needs_review"]]
    if review_cases:
        print(f"LOW CONFIDENCE ({len(review_cases)}) — LLM uncertain, would route to review queue:")
        for r in review_cases:
            print(f"  [{r['i']}] conf={r['approach_b'].get('confidence'):.2f}  "
                  f"{r['page_title'][:55]}  | {r['approach_b'].get('reason','')[:55]}")


if __name__ == "__main__":
    main()
