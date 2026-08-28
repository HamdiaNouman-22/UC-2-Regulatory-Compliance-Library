import os
import json
import logging
import re
import requests
from typing import Dict, Any, List
from dotenv import load_dotenv
from bs4 import BeautifulSoup, NavigableString
from processor.Text_Extractor import OCRProcessor
import pytesseract
from utils.lang_detector import detect_language

load_dotenv()
logger = logging.getLogger(__name__)
class LLMAnalyzer:
    """
    Analyzes KSA banking regulations using LLM with OCR fallback for scanned documents.
    """

    def __init__(
            self,
            model: str = "deepseek/deepseek-v3.2",
            max_chunk_size: int = 12000,
            min_text_length: int = 300
    ):
        self.model = model
        self.max_chunk_size = max_chunk_size
        self.min_text_length = min_text_length
        self.openrouter_api_key = os.getenv("OPENROUTER_API_KEY")

        if not self.openrouter_api_key:
            raise ValueError("Missing OPENROUTER_API_KEY environment variable")

        # Verify OCR capability at startup
        if not OCRProcessor.is_ocr_available():
            logger.warning(
                "Arabic OCR not available. Scanned documents will fail.\n"
                "Install Tesseract with Arabic support:\n"
                "  Linux: sudo apt-get install tesseract-ocr tesseract-ocr-ara\n"
                "  Windows: Install from https://github.com/UB-Mannheim/tesseract/wiki\n"
                "           Then download ara.traineddata from https://github.com/tesseract-ocr/tessdata"
            )

    def split_text_into_chunks(self, text: str, max_chars: int = None) -> List[Dict]:
        """Split text into chunks of <= max_chars, keeping paragraph integrity."""
        max_chars = max_chars or self.max_chunk_size
        paragraphs = text.split('\n\n')
        chunks, current_chunk = [], []
        current_length = 0

        for para in paragraphs:
            if current_length + len(para) > max_chars and current_chunk:
                chunks.append({"text": "\n\n".join(current_chunk), "chunk_num": len(chunks) + 1, "total_chunks": 0})
                current_chunk = []
                current_length = 0
            current_chunk.append(para)
            current_length += len(para)

        if current_chunk:
            chunks.append({"text": "\n\n".join(current_chunk), "chunk_num": len(chunks) + 1, "total_chunks": 0})

        total = len(chunks)
        for c in chunks:
            c["total_chunks"] = total

        return chunks

    # ------------------------------------------------------------------ #
    #  HTML -> text                                                       #
    # ------------------------------------------------------------------ #
    # Tags that end a line. BeautifulSoup.get_text() has no concept of block
    # layout: it concatenates strings, so a heading runs straight into the
    # paragraph under it and a list becomes one sentence. Marking the block
    # boundaries before extracting is what keeps the document's shape.
    _BLOCK_TAGS = (
        "p", "div", "section", "article", "aside", "main", "br", "hr",
        "h1", "h2", "h3", "h4", "h5", "h6",
        "li", "ul", "ol", "dl", "dt", "dd",
        "table", "thead", "tbody", "tfoot", "tr",
        "blockquote", "pre", "figure", "figcaption", "form", "fieldset",
    )

    @staticmethod
    def _tables_to_text(soup) -> int:
        """Render every <table> as one line per row, cells joined by ' | '.

        WHY: get_text() puts NO boundary between cells, so CBE's licensed-bank
        table arrived as

            # Bank Code Bank Name 1 AAIB Arab African International Bank 2 BDC
            Banque Du Caire 3 BM Banque Misr ...

        which is indistinguishable from prose. Nothing downstream can recover
        which value sat in which column, and the model is left to guess whether
        "1 AAIB" is a row or a phrase.

        Innermost first (`reversed` puts a nested table before its container in
        find_all's document order), so an outer table reads the inner one's
        already-rendered text instead of re-flattening it.
        """
        n = 0
        for table in reversed(soup.find_all("table")):
            if table.parent is None:          # already consumed by an outer pass
                continue
            lines = []
            for row in table.find_all("tr"):
                cells = [" ".join(c.get_text(" ", strip=True).split())
                         for c in row.find_all(["th", "td"])]
                if any(cells):
                    lines.append(" | ".join(cells))
            if not lines:
                continue
            table.replace_with(NavigableString("\n" + "\n".join(lines) + "\n"))
            n += 1
        return n

    @staticmethod
    def _images_to_text(soup) -> int:
        """Keep an image's alt text; drop the tag either way.

        get_text() ignores attributes, so an illustrated obligation lost its
        caption entirely. Where alt is empty the image carries information no
        text pipeline can reach — CBE's POS statistics chart is one — and that
        needs OCR, not this.
        """
        n = 0
        for img in soup.find_all("img"):
            alt = " ".join((img.get("alt") or "").split())
            img.replace_with(NavigableString(f"\n[image: {alt}]\n" if alt else ""))
            if alt:
                n += 1
        return n

    def normalize_input_text(self, content: str, content_type: str = "html") -> str:
        """
        Normalize text for LLM consumption.

        Args:
            content: Either HTML string, pre-cleaned PDF text, or file path
            content_type: "html", "pdf_text", or "pdf_file"
        """

        # CASE 1: Already cleaned PDF text from smart extraction
        if content_type == "pdf_text" or ("PAGE" in content[:300] and "=" * 60 in content[:300]):
            logger.info("Detected pre-cleaned PDF text from smart extraction")
            text = re.sub(r'\s+', ' ', content)
            text = re.sub(r'\n{3,}', '\n\n', text)
            return text.strip()

        # CASE 2: HTML content (from document_html)
        if content_type == "html":
            logger.info("Processing HTML content")
            soup = BeautifulSoup(content, 'html.parser')

            for tag in soup(['script', 'style', 'noscript', 'header', 'footer', 'svg']):
                tag.decompose()

            n_tables = self._tables_to_text(soup)
            n_alts = self._images_to_text(soup)

            # Mark where the blocks end BEFORE extracting, so headings, list
            # items and paragraphs stay on their own lines.
            for tag in soup.find_all(self._BLOCK_TAGS):
                tag.insert_before(NavigableString("\n"))
                tag.insert_after(NavigableString("\n"))

            text = soup.get_text(separator=' ').strip()
            # Collapse RUNS OF SPACES, not all whitespace. The previous
            # `re.sub(r'\s+', ' ', text)` flattened the whole document to a
            # single line, which (a) destroyed every table and list boundary and
            # (b) left split_text_into_chunks — which splits on '\n\n' — with
            # exactly one paragraph to work with, so chunking never happened.
            text = re.sub(r'[^\S\n]+', ' ', text)
            text = re.sub(r' *\n *', '\n', text)
            text = re.sub(r'\n{3,}', '\n\n', text).strip()

            if n_tables or n_alts:
                logger.info("HTML normalize: %d table(s) rendered as rows, "
                            "%d image alt text(s) kept", n_tables, n_alts)

            if len(text) > self.min_text_length:
                logger.info(f"Extracted {len(text)} chars from HTML")
                return self._post_clean_text(text)

            # HTML extraction failed - return what we have
            logger.warning(
                f"HTML text too short ({len(text)} chars). "
                f"Cannot apply OCR to HTML content. Returning available text."
            )
            return self._post_clean_text(text) if text else ""

        # CASE 3: PDF file path (for OCR)
        if content_type == "pdf_file" and os.path.isfile(content):
            logger.info("Processing PDF file with OCR")
            text, metadata = OCRProcessor.extract_text_from_pdf_smart(content)

            if len(text) < self.min_text_length:
                raise ValueError(
                    f"Extracted text too short after OCR ({len(text)} chars)"
                )

            return self._post_clean_text(text)

        # Fallback: treat as raw text
        logger.warning("Unknown content type, treating as raw text")
        return self._post_clean_text(content)

    def _post_clean_text(self, text: str) -> str:
        """Final cleanup pass for LLM consumption"""
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]{2,}', ' ', text)
        return text.strip()

    def extract_json_from_llm_response(self, text: str) -> dict:
        """
        Extract JSON from LLM response - prioritizes finding "requirements": [ pattern
        """
        original_text = text
        text = text.strip()

        # Strategy 1: Direct parse
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # Strategy 2: Find "requirements": [ pattern (YOUR IDEA - PRIORITIZED)
        # This is the most reliable indicator of our expected JSON structure
        requirements_pattern = r'"requirements"\s*:\s*\['
        match = re.search(requirements_pattern, text)

        if match:
            logger.info("Found 'requirements': [ pattern in response")
            start_pos = match.start()

            # Search backwards for opening {
            json_start = -1
            for i in range(start_pos - 1, -1, -1):
                if text[i] == '{':
                    json_start = i
                    break
                # Stop if we hit something that's not whitespace or newline
                if text[i] not in [' ', '\n', '\r', '\t'] and text[i] != '{':
                    # Keep searching, might be part of markdown
                    continue

            if json_start != -1:
                # Find matching closing }
                brace_count = 0
                json_end = -1

                for i in range(json_start, len(text)):
                    if text[i] == '{':
                        brace_count += 1
                    elif text[i] == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            json_end = i + 1
                            break

                if json_end != -1:
                    json_candidate = text[json_start:json_end]
                    try:
                        parsed = json.loads(json_candidate)
                        logger.info("Successfully extracted JSON using 'requirements': [ pattern")
                        return parsed
                    except json.JSONDecodeError as e:
                        logger.warning(f"Found 'requirements' pattern but JSON invalid: {e}")
                        # Try cleaning it
                        json_candidate = json_candidate.strip()
                        # Remove any trailing commas before closing braces/brackets
                        json_candidate = re.sub(r',(\s*[}\]])', r'\1', json_candidate)
                        try:
                            parsed = json.loads(json_candidate)
                            logger.info("Successfully extracted JSON after cleaning trailing commas")
                            return parsed
                        except json.JSONDecodeError:
                            pass

        # Strategy 3: Strip markdown code blocks
        cleaned_text = re.sub(r'^```(?:json)?\s*\n?', '', text, flags=re.IGNORECASE | re.MULTILINE)
        cleaned_text = re.sub(r'\n?```\s*$', '', cleaned_text, flags=re.MULTILINE)
        cleaned_text = cleaned_text.strip()

        if cleaned_text != text:
            try:
                logger.info("Successfully parsed after removing markdown code blocks")
                return json.loads(cleaned_text)
            except json.JSONDecodeError:
                pass

        # Strategy 4: Extract from code block patterns
        code_block_patterns = [
            r'```json\s*\n(.*?)\n?```',
            r'```\s*\n(\{.*?\})\s*\n?```',
            r'```json\s*(.*?)```',
            r'```\s*(\{.*?\})```'
        ]

        for pattern in code_block_patterns:
            match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
            if match:
                json_candidate = match.group(1).strip()
                try:
                    logger.info(f"Extracted JSON using code block pattern")
                    return json.loads(json_candidate)
                except json.JSONDecodeError:
                    continue

        # Strategy 5: Balanced brace matching from first {
        brace_count = 0
        start_idx = text.find('{')
        if start_idx != -1:
            for i in range(start_idx, len(text)):
                if text[i] == '{':
                    brace_count += 1
                elif text[i] == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        try:
                            json_candidate = text[start_idx:i + 1]
                            logger.info("Extracted JSON using balanced brace matching")
                            return json.loads(json_candidate)
                        except json.JSONDecodeError:
                            break

        # All strategies failed
        logger.error("JSON extraction completely failed.")
        logger.error(f"Response preview (first 500 chars):\n{original_text[:500]}")

        # Check if "requirements" keyword exists at all
        if "requirements" in text.lower():
            logger.error("Found 'requirements' keyword but couldn't extract valid JSON")

        return {"requirements": []}
    # PROMPT ENGINEERING

    def _build_prompt(
            self,
            text: str,
            document_title: str,
            chunk_info: str = None
    ) -> str:
        chunk_context = f"\n{chunk_info}\n" if chunk_info else ""

        return f"""
You are the Compliance Head of a KSA Bank.

REGULATION DOCUMENT: {document_title}{chunk_context}

REGULATION TEXT:
{text}

================================================================================

Your task is to extract, analyze, and summarize requirements, internal actions, and compliance tasks arising from the regulation above.

IMPORTANT:
- If the regulation does NOT impose explicit legal obligations on banks, identify necessary INTERNAL compliance actions (e.g., awareness, documentation update, policy alignment, vendor communication).
- Do NOT invent penalties, timelines, or enforcement actions unless explicitly stated.
- All extracted items must be defensible during audit or regulatory review.
- Focus ONLY on actionable requirements - skip general background information.
- Each requirement must be distinct - do not repeat similar items.

Please follow the structure below:

1. List of Requirements / Actionable Tasks:
- requirement_text must closely mirror the wording and intent of the regulation.
- Describe WHAT is required, not HOW the bank must implement it internally.
- Do not assume systems, policies, committees, timelines, or documents unless explicitly stated in the regulation.
- Use precise verbs from the regulation (e.g., shall maintain, shall notify, shall provide, shall obtain, shall retain).
- Identify each regulatory requirement OR internal compliance task triggered by the regulation.
- Suggest the relevant department responsible for each item.

2. Risk Assessment:
- Assess the risk level (High / Medium / Low) based on impact and likelihood of misalignment or misinterpretation.

3. Repercussions:
- List potential repercussions such as regulatory observations, supervisory comments, audit findings, or operational confusion.
- Do NOT state monetary penalties unless explicitly mentioned.

4. Controls and KPIs:
- Identify controls required to ensure continued compliance or regulatory alignment.
- Suggest measurable KPIs ONLY where logically applicable.
- KPIs must be specific, calculable, and measurable (e.g., "Percentage of transactions screened within 24h" NOT "Improve screening")

5. Output Format:
- Present the output in a structured, Excel-ready format with the following columns:
  - requirement_text (the actual requirement/task)
  - department (responsible department)
  - risk_level (High/Medium/Low)
  - repercussions (potential consequences - array of strings)
  - controls (required controls - array of strings)
  - kpis (measurable indicators - array of strings)
  - reference (specific article/clause/paragraph from regulation)

6. Reference Field:
   - Must clearly point to the paragraph, clause, or section from which the task is derived
   - If no numbering exists, describe the paragraph location (e.g., "Final paragraph of Section 3.2")

Additional rules:
- Output in JSON format ONLY with structure: {{"requirements": [...]}}
-Return ONLY raw JSON - NO markdown formatting
- Do NOT wrap in ```json or ``` code blocks

- Each requirement object MUST have these exact fields:
  requirement_text, department, risk_level, repercussions, controls, kpis, reference

- repercussions, controls, and kpis MUST be arrays of strings (not single strings)

- Do NOT include general background information as a requirement.
- Do NOT repeat the same task.
- Keep language concise and professional.
- Output in English only.
"""

    def analyze_regulation(
            self,
            content: str,
            regulation_id: int,
            document_title: str = "Untitled Regulation",
            content_type: str = "html"
    ) -> Dict[str, Any]:

        try:
            # Extract meaningful text with type hint
            text = self.normalize_input_text(content, content_type=content_type)


            # LANGUAGE DETECTION
            lang = detect_language(text)
            logger.info(f"Detected language for regulation {regulation_id}: {lang}")

            if lang not in ("en", "ar"):
                logger.warning(f"Unsupported or unclear language ({lang}) for regulation {regulation_id}")

            if len(text) < self.min_text_length:
                raise ValueError(f"Insufficient text for analysis ({len(text)} chars).")

            # Decide if chunking is needed
            if len(text) > self.max_chunk_size:
                logger.info(f"Text exceeds max_chunk_size ({len(text)} chars). Using chunked analysis.")
                chunks = self.split_text_into_chunks(text)
                analysis_result = self.analyze_regulation_chunked(chunks, regulation_id, document_title)
            else:
                prompt = self._build_prompt(text, document_title)
                response_text = self._call_llm(prompt)
                analysis_result = self.extract_json_from_llm_response(response_text)

            # Deduplicate requirements
            unique_requirements = self._deduplicate_requirements(analysis_result.get("requirements", []))

            return {"requirements": unique_requirements}

        except Exception as e:
            logger.exception(f"Analysis failed for regulation {regulation_id}")
            raise

    def analyze_regulation_chunked(
            self,
            chunks: List[Dict],
            regulation_id: int,
            document_title: str = "Untitled Regulation"
    ) -> Dict[str, Any]:
        """
        Analyze regulation in chunks with OCR fallback per chunk
        """
        all_requirements = []

        for chunk in chunks:
            try:
                chunk_info = f"[Chunk {chunk['chunk_num']} of {chunk['total_chunks']}]"
                logger.info(f"Analyzing {chunk_info} for regulation {regulation_id}...")

                # Critical: Apply OCR fallback to each chunk
                clean_text = self.normalize_input_text(chunk["text"])

                prompt = self._build_prompt(clean_text, document_title, chunk_info)
                response = self._call_llm(prompt)

                # Log raw response for debugging
                logger.debug(f"Raw LLM response length: {len(response)} chars")
                logger.debug(f"Response starts with: {response[:100]}")

                parsed = self.extract_json_from_llm_response(response)

                # Check if parsing succeeded
                if not parsed.get("requirements"):
                    logger.warning(f"{chunk_info}: No requirements extracted. Raw response: {response[:300]}")

                # Annotate with source chunk
                for req in parsed.get("requirements", []):
                    req["source_chunk"] = chunk["chunk_num"]
                    all_requirements.append(req)

                logger.info(
                    f"{chunk_info}: Found {len(parsed.get('requirements', []))} requirements"
                )

            except Exception as e:
                logger.error(f"Chunk {chunk.get('chunk_num', '?')} failed: {e}")
                logger.exception("Full traceback:")
                continue

        unique_requirements = self._deduplicate_requirements(all_requirements)
        logger.info(
            f"Chunked analysis complete: {len(all_requirements)} raw → "
            f"{len(unique_requirements)} unique requirements"
        )
        return {"requirements": unique_requirements}


    def _deduplicate_requirements(self, requirements: List[Dict]) -> List[Dict]:
        seen = set()
        unique = []

        for req in requirements:
            text = self._normalize_text(req.get("requirement_text", ""))
            if text and text not in seen:
                seen.add(text)
                unique.append(req)

        return unique

    def _normalize_text(self, text: str) -> str:
        if not text:
            return ""
        text = text.lower()
        text = re.sub(r'\s+', ' ', text)
        return text.strip(' .,:;!?')


    def _call_llm(self, prompt: str) -> str:
        """
        Call OpenRouter API with corrected URL (no trailing spaces)
        """
        url = "https://openrouter.ai/api/v1/chat/completions"

        headers = {
            "Authorization": f"Bearer {self.openrouter_api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "http://localhost:3000",
            "X-Title": "Saudi Banking Compliance Copilot"
        }

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a senior banking compliance officer with deep expertise in "
                        "Saudi Arabian regulations (SAMA, CMA). You extract precise requirements "
                        "from regulatory texts without hallucination."
                    )
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.1,  # Lower for precision
            "max_tokens": 8000
        }

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=120)
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            return content.strip() if content else '{"requirements": []}'
        except requests.exceptions.RequestException as e:
            logger.error(f"OpenRouter API error: {e}")
            if hasattr(e.response, 'text'):
                logger.error(f"API response: {e.response.text[:500]}")
            raise

    # UTILITIES

    def get_text_stats(self, content: str) -> Dict:
        """
        Get statistics about extracted text (after OCR if needed)
        """
        try:
            text = self.normalize_input_text(content)
            return {
                "characters": len(text),
                "estimated_tokens": len(text) // 4,
                "needs_chunking": len(text) > self.max_chunk_size,
                "sufficient_for_analysis": len(text) >= self.min_text_length,
                "ocr_used": len(text) > 500 and "ara" in text.lower()[:200]  # Heuristic
            }
        except Exception as e:
            return {
                "error": str(e),
                "characters": 0,
                "sufficient_for_analysis": False
            }
