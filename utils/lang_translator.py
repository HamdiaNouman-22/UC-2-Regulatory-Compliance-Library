"""
translator.py — Arabic/English translation using Google Translate (free, no API key).

Uses the `deep-translator` library which wraps Google Translate for free.

Install:
    pip install deep-translator

Why NOT an LLM for translation:
    - LLMs are slow (2-10s per call vs ~200ms for Google Translate)
    - LLMs cost money per token
    - Google Translate is actually excellent for English→Arabic (Modern Standard Arabic)
    - deep-translator handles HTML natively — preserves all tags automatically
    - No API key, no rate limit concerns for normal usage volumes

Public API:
    translate_text(text, target_lang)               -> str
    translate_html(html, target_lang)               -> str
    translate_texts_batch(texts, target_lang)       -> List[str]
    translate_regulation(reg, target_lang)          -> dict
    translate_gap_result(gap_result, target_lang)   -> dict
    translate_compliance_requirement(req, lang)     -> dict
    translate_v2_gap_result(gap_result, lang)       -> dict
"""

import logging
import re
from typing import Dict, List, Optional

from deep_translator import GoogleTranslator

logger = logging.getLogger(__name__)

# Google Translate character limit per request.
_MAX_CHARS = 4500

# Fields that must NEVER be translated — IDs, codes, URLs, timestamps, enums
SKIP_FIELDS = {
    "id", "regulation_id", "session_id", "category_id", "parent_id",
    "document_url", "source_page_url", "doc_path",
    "created_at", "updated_at", "published_date",
    "reference_no", "year", "status", "type",
    "regulator", "source_system",
    "control_key", "kisetup_key", "formula",
    "match_status", "coverage_status",
    "compliancecategory_id", "matched_requirement_id",
    "control_id", "kisetup_id",
    "is_suggested", "has_more", "total", "limit", "offset",
    "current_page", "total_pages",
    # V2-specific non-translatable fields
    "obligation_id", "requirement_id",
    "criticality", "obligation_type", "execution_category",
}


def _get_translator(source: str = "en", target: str = "ar") -> GoogleTranslator:
    """Create a GoogleTranslator instance."""
    return GoogleTranslator(source=source, target=target)


# ------------------------------------------------------------------ #
#  Plain text translation                                              #
# ------------------------------------------------------------------ #

def translate_text(text: str, target_lang: str = "ar") -> str:
    """
    Translate a single plain-text string using Google Translate.
    Returns the original string silently on any failure.
    """
    if not text or not text.strip():
        return text
    if target_lang == "en":
        return text

    try:
        translator = _get_translator(target=target_lang)
        return translator.translate(text)
    except Exception as e:
        logger.error(f"translate_text failed: {e}")
        return text


# ------------------------------------------------------------------ #
#  HTML translation — regex-based, never re-serializes the HTML       #
# ------------------------------------------------------------------ #

_TEXT_NODE_RE = re.compile(r'(?<=>)([^<]+)(?=<)', re.DOTALL)


def translate_html(html: str, target_lang: str = "ar") -> str:
    """
    Translate HTML content while preserving ALL markup exactly as stored in DB.

    Uses regex on raw HTML string — never parses or re-serializes to avoid
    corrupting inline styles (rgb/hsl/% values etc.).
    """
    logger.info(f"translate_html called — using REGEX method, html length={len(html)}")
    if not html or not html.strip():
        return html
    if target_lang == "en":
        return html

    try:
        matches = [m for m in _TEXT_NODE_RE.finditer(html) if m.group(1).strip()]
        if not matches:
            return html

        translator = _get_translator(target=target_lang)
        result = html

        for m in reversed(matches):
            original_text = m.group(1)
            if not original_text.strip():
                continue
            try:
                translated = translator.translate(original_text[:_MAX_CHARS])
                if translated:
                    result = result[:m.start(1)] + translated + result[m.end(1):]
            except Exception as e:
                logger.error(f"Text node translation failed ('{original_text[:60]}'): {e}")

        return _inject_rtl(result)

    except Exception as e:
        logger.error(f"translate_html failed: {e}")
        return html


def _inject_rtl(html: str) -> str:
    """Add dir="rtl" to the first block-level opening tag via regex only."""
    def add_rtl(m):
        tag = m.group(0)
        if 'dir=' not in tag:
            tag = tag[:-1] + ' dir="rtl">'
        return tag
    return re.sub(r'<(div|section|article|main|body|p|table)(\s[^>]*)?>'
                  , add_rtl, html, count=1)


# ------------------------------------------------------------------ #
#  Batch text translation                                              #
# ------------------------------------------------------------------ #

def translate_texts_batch(texts: List[str], target_lang: str = "ar") -> List[str]:
    """
    Translate a list of strings efficiently — reuses one translator instance.
    Empty / whitespace-only strings are passed through unchanged.
    """
    if target_lang == "en" or not texts:
        return texts

    results = []
    translator = _get_translator(target=target_lang)

    for text in texts:
        if not text or not text.strip():
            results.append(text)
            continue
        try:
            results.append(translator.translate(text))
        except Exception as e:
            logger.error(f"translate_texts_batch item failed: {e}")
            results.append(text)

    return results


# ------------------------------------------------------------------ #
#  Regulation translator                                               #
# ------------------------------------------------------------------ #

def translate_regulation(reg: Dict, target_lang: str = "ar") -> Dict:
    """
    Translate only the human-readable fields of a regulation dict.

    Translated fields: title, category, department, category_info.title
    HTML field:        document_html
    Skipped:          All IDs, URLs, dates, reference codes, status enums
    """
    if target_lang == "en":
        return reg

    result = dict(reg)
    translator = _get_translator(target=target_lang)

    for field in ["title", "category", "department"]:
        val = result.get(field)
        if val and isinstance(val, str):
            try:
                result[field] = translator.translate(val)
            except Exception as e:
                logger.error(f"translate_regulation field '{field}' failed: {e}")

    if result.get("document_html") and isinstance(result["document_html"], str):
        result["document_html"] = translate_html(result["document_html"], target_lang)

    ci = result.get("category_info")
    if ci and isinstance(ci, dict) and ci.get("title"):
        ci = dict(ci)
        try:
            ci["title"] = translator.translate(ci["title"])
        except Exception as e:
            logger.error(f"translate_regulation category_info.title failed: {e}")
        result["category_info"] = ci

    return result


# ------------------------------------------------------------------ #
#  V1 gap result translator (kept for session retrieval compatibility) #
# ------------------------------------------------------------------ #

def translate_gap_result(gap_result: Dict, target_lang: str = "ar") -> Dict:
    """
    Translate the human-readable fields of a V1 gap analysis result.
    coverage_status is a controlled enum — not translated.
    """
    if target_lang == "en":
        return gap_result

    result = dict(gap_result)
    translator = _get_translator(target=target_lang)

    for field in ["requirement_text", "obligation_text", "evidence_text",
                  "gap_description", "controls", "kpis"]:
        val = result.get(field)
        if val and isinstance(val, str):
            try:
                result[field] = translator.translate(val)
            except Exception as e:
                logger.error(f"translate_gap_result field '{field}' failed: {e}")

    return result


# ------------------------------------------------------------------ #
#  V2 gap result translator                                            #
# ------------------------------------------------------------------ #

# Human-readable text fields in a V2 GapResult that should be translated.
# Intentionally excludes: obligation_id, requirement_id, criticality,
# obligation_type, execution_category, coverage_status (IDs/enums).
_V2_GAP_TRANSLATABLE_FIELDS = [
    "obligation_text",
    "requirement_text",   # alias — same value as obligation_text
    "evidence_text",
    "gap_description",
    "controls",
    "kpis",
    "requirement_title",
]


def translate_v2_gap_result(gap_result: Dict, target_lang: str = "ar") -> Dict:
    """
    Translate only the human-readable fields of a V2 gap result dict.

    Skips all ID, enum, and code fields:
        obligation_id, requirement_id, criticality, obligation_type,
        execution_category, coverage_status
    """
    if target_lang == "en":
        return gap_result

    result = dict(gap_result)
    translator = _get_translator(target=target_lang)

    for field in _V2_GAP_TRANSLATABLE_FIELDS:
        val = result.get(field)
        if val and isinstance(val, str):
            try:
                result[field] = translator.translate(val)
            except Exception as e:
                logger.error(f"translate_v2_gap_result field '{field}' failed: {e}")

    # Translate nested controls list (each control is a dict or string)
    ctrl_list = result.get("controls_detail")
    if ctrl_list and isinstance(ctrl_list, list):
        translated_ctrls = []
        for ctrl in ctrl_list:
            if isinstance(ctrl, dict):
                ctrl = dict(ctrl)
                for f in ["control_title", "control_description", "match_explanation"]:
                    if ctrl.get(f) and isinstance(ctrl[f], str):
                        try:
                            ctrl[f] = translator.translate(ctrl[f])
                        except Exception as e:
                            logger.error(f"translate_v2_gap_result controls_detail.{f} failed: {e}")
            translated_ctrls.append(ctrl)
        result["controls_detail"] = translated_ctrls

    return result


# ------------------------------------------------------------------ #
#  Compliance requirement translator                                   #
# ------------------------------------------------------------------ #

def translate_compliance_requirement(req: Dict, target_lang: str = "ar") -> Dict:
    """
    Translate all human-readable fields in a compliance requirement dict.

    Handles shapes returned by /compliance-analysis-full and /requirement-mapping:
        Strings:      requirement_text, department, risk_level, reference,
                      title, description, extracted_requirement_text,
                      matched_requirement_title, matched_requirement_description,
                      match_explanation, control_title, control_description,
                      kpi_title, kpi_description
        Lists[str]:   repercussions, controls, kpis
        Nested dict:  matching → its own string fields + controls/kpis sub-lists

    Skipped (IDs/codes/enums): match_status, coverage_status, control_key,
    kisetup_key, formula, obligation_id, requirement_id, criticality,
    obligation_type, execution_category, and all numeric IDs.
    """
    if target_lang == "en":
        return req

    result = dict(req)
    translator = _get_translator(target=target_lang)

    STRING_FIELDS = [
        "requirement_text",
        "obligation_text",
        "department",
        "risk_level",
        "reference",
        "title",
        "description",
        "extracted_requirement_text",
        "matched_requirement_title",
        "matched_requirement_description",
        "match_explanation",
        "control_title",
        "control_description",
        "kpi_title",
        "kpi_description",
        "requirement_title",
    ]
    for field in STRING_FIELDS:
        val = result.get(field)
        if val and isinstance(val, str):
            try:
                result[field] = translator.translate(val)
            except Exception as e:
                logger.error(f"translate_compliance_requirement field '{field}' failed: {e}")

    LIST_FIELDS = ["repercussions", "controls", "kpis"]
    for field in LIST_FIELDS:
        val = result.get(field)
        if val and isinstance(val, list):
            str_items = [item for item in val if item and isinstance(item, str)]
            if str_items:
                try:
                    translated_items = translate_texts_batch(str_items, target_lang)
                    translated_iter = iter(translated_items)
                    result[field] = [
                        next(translated_iter) if (item and isinstance(item, str)) else item
                        for item in val
                    ]
                except Exception as e:
                    logger.error(f"translate_compliance_requirement list field '{field}' failed: {e}")

    matching = result.get("matching")
    if matching and isinstance(matching, dict):
        matching = dict(matching)

        for field in ["matched_requirement_title", "matched_requirement_description", "match_explanation"]:
            val = matching.get(field)
            if val and isinstance(val, str):
                try:
                    matching[field] = translator.translate(val)
                except Exception as e:
                    logger.error(f"matching.{field} translation failed: {e}")

        controls = matching.get("controls", [])
        if controls:
            translated_controls = []
            for ctrl in controls:
                ctrl = dict(ctrl)
                for f in ["control_title", "control_description", "match_explanation"]:
                    if ctrl.get(f) and isinstance(ctrl.get(f), str):
                        try:
                            ctrl[f] = translator.translate(ctrl[f])
                        except Exception as e:
                            logger.error(f"matching.control.{f} translation failed: {e}")
                translated_controls.append(ctrl)
            matching["controls"] = translated_controls

        kpis = matching.get("kpis", [])
        if kpis:
            translated_kpis = []
            for kpi in kpis:
                kpi = dict(kpi)
                for f in ["kpi_title", "kpi_description", "match_explanation"]:
                    if kpi.get(f) and isinstance(kpi.get(f), str):
                        try:
                            kpi[f] = translator.translate(kpi[f])
                        except Exception as e:
                            logger.error(f"matching.kpi.{f} translation failed: {e}")
                translated_kpis.append(kpi)
            matching["kpis"] = translated_kpis

        result["matching"] = matching

    return result