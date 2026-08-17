"""
crawler/smart_matcher.py
Smart identity matching for CBB regulation pages.

Given a newly-scraped TR page (title, url, doc_path, content_text, soup),
determines whether it is an update of an existing regulation or genuinely new.

Steps
-----
A. Section-code + folder + path-overlap disambiguation  (deterministic, fast)
B. Vector/difflib similarity + LLM confirmation          (fallback when A ambiguous)
C. Content fingerprint check                             (skips TR false-positive re-publishes)

Usage
-----
    from crawler.smart_matcher import SmartMatcher
    sm = SmartMatcher(repo)          # loads DB data once
    result = sm.match(title, url, doc_path, content_text, soup)
    # result["existing_id"]       -> int or None
    # result["monitoring_status"] -> "new" | "modified" | "unchanged"
"""

import os
import re
import json
import logging
from typing import Optional, List, Dict

import requests

log = logging.getLogger(__name__)

_OR_MODEL      = "deepseek/deepseek-v3.2"
_OR_URL        = "https://openrouter.ai/api/v1/chat/completions"
_SIM_THRESH    = 0.50
_CONF_MIN      = 0.80

_DECISIVE_METHODS = frozenset({
    "section_code_in_folder",
    "section_code_volume_match",
    "section_code_path_match",
    "section_code_global_unique",
})

_CHANGE_KWS = re.compile(
    r'\b(deleted|moved\s+to|transferred|repealed|amended|revised|replaced\s+by)\b',
    re.IGNORECASE,
)

# ── Optional: sentence-transformers (better similarity) ──────────────────────
try:
    from sentence_transformers import SentenceTransformer
    import numpy as np
    _st_model = SentenceTransformer("all-MiniLM-L6-v2")

    def _embed(texts: List[str]):
        return _st_model.encode(texts, normalize_embeddings=True)

    def _cosine(a, b) -> float:
        return float(np.dot(a, b))

    _USE_VECTORS = True
    log.info("SmartMatcher: sentence-transformers loaded")
except ImportError:
    import difflib

    def _cosine(a: str, b: str) -> float:   # type: ignore[misc]
        return difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio()

    _USE_VECTORS = False
    log.info("SmartMatcher: sentence-transformers not found — using difflib similarity")


# ── Pure helpers (no DB dependency) ──────────────────────────────────────────

def _sig(text: str) -> str:
    """Normalised 500-char fingerprint for content-identity check."""
    return re.sub(r'\s+', ' ', (text or "").lower().strip())[:500]


def _url_vol_suffix(url: str) -> str:
    """Extract trailing '-N' from URL slug.  '' for no suffix."""
    slug = url.rstrip("/").split("/")[-1]
    m = re.search(r'-(\d+)$', slug)
    return f"-{m.group(1)}" if m else ""


def _ancestors(cat_id: int, cats: Dict) -> List[int]:
    chain, seen = [], set()
    cid = cat_id
    while cid and cid not in seen:
        seen.add(cid)
        chain.append(cid)
        cid = cats.get(cid, {}).get("parent")
    return chain


def _path_overlap(cat_id: int, doc_path: List[str], cats: Dict) -> int:
    doc_set = set(doc_path)
    return sum(1 for cid in _ancestors(cat_id, cats)
               if cid in cats and cats[cid]["title"] in doc_set)


def _resolve_folder(doc_path: List[str], cats: Dict) -> Optional[int]:
    from crawler.cbb_monitoring_crawler import _extract_section_code
    parent, last = None, None
    for step in doc_path:
        match = next(
            (c["id"] for c in cats.values()
             if c["title"] == step and c["parent"] == parent),
            None,
        )
        if match is None:
            sc = _extract_section_code(step)
            if sc:
                match = next(
                    (c["id"] for c in cats.values()
                     if c["title"].upper().startswith(sc.upper())
                     and c["parent"] == parent),
                    None,
                )
        if match is None:
            break
        last = parent = match
    return last


def _approach_a(
    title: str,
    folder_id: Optional[int],
    regs_by_cat: Dict,
    regs_by_sc: Dict,
    cats: Dict,
    new_url: str = "",
    doc_path: Optional[List[str]] = None,
) -> Optional[Dict]:
    from crawler.cbb_monitoring_crawler import _extract_section_code
    sc = _extract_section_code(title)
    if not sc:
        return None

    # Stage 1: same folder subtree
    for cat_id in (_ancestors(folder_id, cats) if folder_id else []):
        for reg in regs_by_cat.get(cat_id, []):
            reg_sc = _extract_section_code(reg["title"])
            if reg_sc and reg_sc.upper() == sc.upper():
                return {"reg_id": reg["id"], "reg_title": reg["title"],
                        "method": "section_code_in_folder", "score": 1.0}

    # Stage 2: cross-folder fallback
    matches = regs_by_sc.get(sc.upper(), [])
    if len(matches) == 1:
        return {"reg_id": matches[0]["id"], "reg_title": matches[0]["title"],
                "method": "section_code_global_unique", "score": 0.9}

    if len(matches) > 1:
        if doc_path:
            scored = [(m, _path_overlap(m["cat_id"], doc_path, cats)) for m in matches]
            scored.sort(key=lambda x: x[1], reverse=True)
            if scored[0][1] > scored[1][1]:
                best = scored[0][0]
                return {"reg_id": best["id"], "reg_title": best["title"],
                        "method": "section_code_path_match", "score": 0.95}

        if new_url:
            sfx = _url_vol_suffix(new_url)
            vol_match = [m for m in matches if _url_vol_suffix(m.get("url", "")) == sfx]
            if len(vol_match) == 1:
                return {"reg_id": vol_match[0]["id"], "reg_title": vol_match[0]["title"],
                        "method": "section_code_url_suffix", "score": 0.90}

        return {"reg_id": None, "reg_title": f"{len(matches)} candidates",
                "method": "section_code_global_ambiguous", "score": 0.5}

    return None


def _get_candidates(
    title: str,
    folder_id: Optional[int],
    regs_by_cat: Dict,
    regs_by_sc: Dict,
    cats: Dict,
) -> List[Dict]:
    from crawler.cbb_monitoring_crawler import _extract_section_code
    pool = []

    for cid in (_ancestors(folder_id, cats) if folder_id else [])[:5]:
        pool.extend(regs_by_cat.get(cid, []))

    sc = _extract_section_code(title)
    if sc:
        for reg in regs_by_sc.get(sc.upper(), []):
            if reg not in pool:
                pool.append(reg)

    if not pool:
        return []

    if _USE_VECTORS:
        all_titles = [title] + [r["title"] for r in pool]
        embs = _embed(all_titles)
        q = embs[0]
        import numpy as np   # noqa: F811
        scored = [(float(np.dot(q, embs[i + 1])), r) for i, r in enumerate(pool)]
    else:
        scored = [(_cosine(title, r["title"]), r) for r in pool]

    scored.sort(key=lambda x: x[0], reverse=True)
    return [{"score": round(s, 3), **r} for s, r in scored if s >= _SIM_THRESH][:5]


def _smart_snippet(soup) -> str:
    """Amendment history box + keyword sentences + first paragraph."""
    parts = []

    for box in soup.find_all(
        ["div", "p", "table"],
        class_=re.compile(r'amend|history|revision', re.I),
    ):
        txt = box.get_text(" ", strip=True)
        if txt:
            parts.append(f"[History] {txt[:200]}")
            break

    body = soup.find("div", class_="field-items") or soup.find("article")
    if body:
        full_text = body.get_text(" ", strip=True)
        sentences = re.split(r'(?<=[.!?])\s+', full_text)
        kw_sents = [s for s in sentences if _CHANGE_KWS.search(s)]
        if kw_sents:
            parts.append("[Change] " + " | ".join(kw_sents[:3])[:300])
        parts.append(f"[Intro] {full_text[:250]}")

    return "\n".join(parts) if parts else ""


def _llm_decide(
    new_title: str,
    new_url: str,
    doc_path: List[str],
    snippet: str,
    candidates: List[Dict],
) -> Dict:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        return {"is_update": False, "reg_id": None, "change_type": "ERROR",
                "confidence": 0.0, "reason": "OPENROUTER_API_KEY not set"}

    if not candidates:
        return {"is_update": False, "reg_id": None, "change_type": "NEW",
                "confidence": 0.95, "reason": "no similar candidates found"}

    cand_txt = "\n".join(
        f"  [{c['id']}] \"{c['title']}\" "
        f"(similarity {c.get('score', 0):.2f}, url: {c.get('url', '')[:60]})"
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
        "- Same section code (e.g. AU-5.5 in both) -> almost certainly same regulation\n"
        "- Content says 'moved', 'deleted', 'amended' -> update of the matching section\n"
        "- Completely different section code/topic -> new\n\n"
        'Reply ONLY with valid JSON: {"is_update": true|false, "reg_id": <number or null>, '
        '"change_type": "AMENDED"|"DELETED"|"MOVED"|"RENAMED"|"NEW", '
        '"confidence": <0.0-1.0>, "reason": "<one sentence>"}'
    )

    def _call(messages):
        resp = requests.post(
            _OR_URL,
            headers={"Authorization": f"Bearer {api_key}",
                     "Content-Type": "application/json"},
            json={"model": _OR_MODEL, "max_tokens": 250, "messages": messages},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()

    def _parse(text: str):
        text = re.sub(r'^```(?:json)?\s*|\s*```$', '', text, flags=re.S).strip()
        m = re.search(r'\{.*\}', text, re.S)
        if m:
            text = m.group(0)
        return json.loads(text)

    try:
        msgs = [{"role": "user", "content": prompt}]
        raw = _call(msgs)
        try:
            result = _parse(raw)
        except Exception:
            msgs.append({"role": "assistant", "content": raw})
            msgs.append({"role": "user",
                          "content": "Reply ONLY with the JSON object, no extra text."})
            result = _parse(_call(msgs))
        result.setdefault("confidence", 0.5)
        return result
    except Exception as e:
        return {"is_update": False, "reg_id": None, "change_type": "ERROR",
                "confidence": 0.0, "reason": str(e)[:120]}


# ── Main class ────────────────────────────────────────────────────────────────

class SmartMatcher:
    """
    Loads regulation/category data from DB once, then provides match() for each page.

    Parameters
    ----------
    repo : MSSQLRepository
        Repository instance — used only to borrow a connection for the initial load.
    """

    def __init__(self, repo):
        self.regs: Dict[int, dict] = {}
        self.cats: Dict[int, dict] = {}
        self.regs_by_cat: Dict[int, List[dict]] = {}
        self.regs_by_sc: Dict[str, List[dict]] = {}
        self._load(repo)

    def _load(self, repo) -> None:
        from crawler.cbb_monitoring_crawler import _extract_section_code
        conn = repo._get_conn()
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
        for row in cur.fetchall():
            rid = row[0]
            if rid not in self.regs:
                self.regs[rid] = {
                    "id": rid, "title": row[1] or "",
                    "cat_id": row[2], "url": row[3] or "",
                    "content": (row[4] or "")[:600],
                }

        cur.execute(
            "SELECT compliancecategory_id, title, parentid FROM compliancecategory"
        )
        self.cats = {
            r[0]: {"id": r[0], "title": r[1] or "", "parent": r[2]}
            for r in cur.fetchall()
        }

        for reg in self.regs.values():
            self.regs_by_cat.setdefault(reg["cat_id"], []).append(reg)
            sc = _extract_section_code(reg["title"])
            if sc:
                self.regs_by_sc.setdefault(sc.upper(), []).append(reg)

        conn.close()
        log.info(
            f"SmartMatcher loaded: {len(self.regs):,} regs, "
            f"{len(self.cats):,} cats, {len(self.regs_by_sc):,} section codes"
        )

    def match(
        self,
        title: str,
        url: str,
        doc_path: List[str],
        content_text: str,
        soup,
        url_existing_id: Optional[int] = None,
    ) -> Dict:
        """
        Returns a dict:
          existing_id        : int or None
          monitoring_status  : "new" | "modified" | "unchanged"
          method             : str  (for audit logging)
          confidence         : float
          change_type        : str  (AMENDED / DELETED / MOVED / NEW / UNCHANGED)
        """
        # ── 1. URL exact match (passed in from caller's URL lookup) ───────────
        if url_existing_id:
            stored = self.regs.get(url_existing_id, {}).get("content", "")
            new_s  = _sig(content_text)
            if new_s and stored and _sig(stored) == new_s:
                return self._result(url_existing_id, "unchanged", "url_exact", 1.0, "UNCHANGED")
            return self._result(url_existing_id, "modified", "url_exact", 1.0, "AMENDED")

        # ── 2. Approach A: section-code + path disambiguation ────────────────
        folder_id = _resolve_folder(doc_path, self.cats)
        a_res = _approach_a(title, folder_id, self.regs_by_cat, self.regs_by_sc,
                            self.cats, new_url=url, doc_path=doc_path)

        a_decisive = (
            a_res is not None
            and a_res.get("method") in _DECISIVE_METHODS
            and a_res.get("reg_id") is not None
        )

        # ── 3. Content fingerprint (UNCHANGED — TR re-publish detection) ──────
        candidates = _get_candidates(title, folder_id, self.regs_by_cat,
                                     self.regs_by_sc, self.cats)
        top = (
            next((c for c in candidates if c["id"] == a_res["reg_id"]), None)
            if a_decisive else None
        ) or (candidates[0] if candidates else None)

        new_sig = _sig(content_text)
        if top and new_sig and _sig(top.get("content", "")) == new_sig:
            log.info(f"  SmartMatch UNCHANGED (fingerprint) -> reg_id={top['id']} [{title[:55]}]")
            return self._result(top["id"], "unchanged", "content_fingerprint", 1.0, "UNCHANGED")

        # ── 4. A decisive — use directly, skip LLM ───────────────────────────
        if a_decisive:
            log.info(
                f"  SmartMatch A [{a_res['method']}] -> reg_id={a_res['reg_id']} [{title[:55]}]"
            )
            return self._result(a_res["reg_id"], "modified",
                                a_res["method"], a_res["score"], "AMENDED")

        # ── 5. Approach B: LLM confirmation ───────────────────────────────────
        llm_candidates = (
            [c for c in candidates if c["id"] == a_res["reg_id"]] or candidates
        ) if a_res else candidates

        snippet = _smart_snippet(soup) if soup is not None else ""
        b = _llm_decide(title, url, doc_path, snippet, llm_candidates)

        if b.get("is_update") and b.get("reg_id"):
            conf = b.get("confidence", 0.5)
            status = "modified" if conf >= _CONF_MIN else "new"
            log.info(
                f"  SmartMatch LLM {b.get('change_type')} conf={conf:.2f} "
                f"-> reg_id={b['reg_id']} [{title[:55]}]"
            )
            return {
                "existing_id":       b["reg_id"],
                "monitoring_status": status,
                "method":            f"llm_{b.get('change_type','AMENDED').lower()}",
                "confidence":        conf,
                "change_type":       b.get("change_type", "AMENDED"),
                "llm_reason":        b.get("reason", ""),
            }

        log.info(f"  SmartMatch NEW (LLM/no-match) [{title[:55]}]")
        return {
            "existing_id":       None,
            "monitoring_status": "new",
            "method":            b.get("change_type", "NEW").lower(),
            "confidence":        b.get("confidence", 0.5),
            "change_type":       b.get("change_type", "NEW"),
            "llm_reason":        b.get("reason", ""),
        }

    @staticmethod
    def _result(
        existing_id: Optional[int],
        status: str,
        method: str,
        confidence: float,
        change_type: str,
    ) -> Dict:
        return {
            "existing_id":       existing_id,
            "monitoring_status": status,
            "method":            method,
            "confidence":        confidence,
            "change_type":       change_type,
        }
