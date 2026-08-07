"""OpenRouter LLM client, copied from the codebase's existing pattern
(processor/LlmAnalyzer.py::_call_llm) rather than introducing a second LLM
integration style. Every other LLM call site in this repo (LlmAnalyzer,
staged_LLM_Analyzer, smart_matcher, requirement_matcher, metadata_extractor,
gap_analyzer) uses raw requests.post to OpenRouter with OPENROUTER_API_KEY --
this does the same, generalized for config-proposal use instead of regulation
analysis.
"""

import json
import logging
import os
import re

import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_MODEL = "deepseek/deepseek-v3.2"


class LlmClientError(RuntimeError):
    pass


def call_llm(
    system_prompt: str,
    user_prompt: str,
    model: str = DEFAULT_MODEL,
    temperature: float = 0.1,
    max_tokens: int = 8000,
) -> str:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise LlmClientError("Missing OPENROUTER_API_KEY environment variable")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "http://localhost:3000",
        "X-Title": "Regulatory Crawler Config Onboarding",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    try:
        response = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=120)
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        return content.strip() if content else "{}"
    except requests.exceptions.RequestException as e:
        logger.error(f"OpenRouter API error: {e}")
        if getattr(e, "response", None) is not None:
            logger.error(f"API response: {e.response.text[:500]}")
        raise LlmClientError(str(e)) from e


def extract_json_from_llm_response(text: str) -> dict:
    """Robust JSON extraction from an LLM response. Ported from the general-purpose
    fallback strategies in processor/LlmAnalyzer.py::extract_json_from_llm_response
    (direct parse -> strip markdown fences -> code-block regex -> balanced-brace scan),
    with the task-specific "requirements" pattern dropped since it doesn't apply here.
    """
    original_text = text
    text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    cleaned_text = re.sub(r'^```(?:json)?\s*\n?', '', text, flags=re.IGNORECASE | re.MULTILINE)
    cleaned_text = re.sub(r'\n?```\s*$', '', cleaned_text, flags=re.MULTILINE)
    cleaned_text = cleaned_text.strip()
    if cleaned_text != text:
        try:
            return json.loads(cleaned_text)
        except json.JSONDecodeError:
            pass

    code_block_patterns = [
        r'```json\s*\n(.*?)\n?```',
        r'```\s*\n(\{.*?\})\s*\n?```',
        r'```json\s*(.*?)```',
        r'```\s*(\{.*?\})```',
    ]
    for pattern in code_block_patterns:
        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if match:
            candidate = match.group(1).strip()
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                continue

    brace_count = 0
    start_idx = text.find('{')
    if start_idx != -1:
        for i in range(start_idx, len(text)):
            if text[i] == '{':
                brace_count += 1
            elif text[i] == '}':
                brace_count -= 1
                if brace_count == 0:
                    candidate = text[start_idx:i + 1]
                    try:
                        return json.loads(candidate)
                    except json.JSONDecodeError:
                        break

    logger.error("JSON extraction completely failed.")
    logger.error(f"Response preview (first 500 chars):\n{original_text[:500]}")
    raise LlmClientError("Could not extract valid JSON from LLM response")
