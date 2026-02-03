import os
import json
import logging
import re
import requests
import base64
import io
from typing import Dict, Any, List
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from PIL import Image
import pytesseract

#OCR dependency - will warn if missing
try:


    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    logging.warning(
        "pytesseract not installed. OCR functionality disabled.\n"
        "Install with: pip install pytesseract pillow\n"
        "Also install Tesseract OCR engine and Arabic language pack."
    )

load_dotenv()
logger = logging.getLogger(__name__)

class OCRProcessor:
    """
    Handles OCR extraction from base64 images in HTML documents.
    Critical for processing scanned KSA regulatory documents (bilingual Arabic/English).
    """

    pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

    @staticmethod
    def is_ocr_available() -> bool:
        """Check if Tesseract OCR is properly installed with Arabic support"""
        if not OCR_AVAILABLE:
            return False

        try:
            langs = pytesseract.get_languages()
            return 'ara' in langs
        except Exception:
            return False

    @staticmethod
    def extract_text_from_html(html: str, lang: str = 'ara+eng') -> str:
        soup = BeautifulSoup(html, 'html.parser')

        for tag in soup(['script', 'style', 'noscript', 'header', 'footer', 'svg']):
            tag.decompose()

        # Attempt 1: Native text extraction
        text = soup.get_text(separator='\n\n').strip()
        text = re.sub(r'\s+', ' ', text)

        if len(text) > 500:
            logger.info(f"Standard text extraction successful ({len(text)} chars)")
            return text.strip()

        logger.warning(
            f"Low native text ({len(text)} chars). "
            f"Falling back to OCR on embedded images."
        )

        if not OCR_AVAILABLE:
            raise ValueError("OCR required but pytesseract is not available")

        soup = BeautifulSoup(html, 'html.parser')
        extracted_texts = []
        images_processed = 0

        for idx, img in enumerate(soup.find_all('img'), start=1):
            src = img.get('src', '')
            if not src or 'base64' not in src.lower():
                continue

            try:
                encoded = src.split(',', 1)[1]
                image_data = base64.b64decode(encoded)
                image = Image.open(io.BytesIO(image_data))

                try:
                    import numpy as np
                    import cv2

                    img_np = np.array(image)
                    gray = cv2.cvtColor(img_np, cv2.COLOR_RGB2GRAY)
                    gray = cv2.threshold(
                        gray, 0, 255,
                        cv2.THRESH_BINARY + cv2.THRESH_OTSU
                    )[1]
                    image = Image.fromarray(gray)

                except ImportError:
                    pass

                # OCR
                ocr_text = pytesseract.image_to_string(
                    image,
                    lang=lang,
                    config='--psm 6 --oem 3'
                ).strip()

                if ocr_text and len(ocr_text) > 50:
                    extracted_texts.append(
                        f"\n\n--- PAGE {idx} ---\n\n{ocr_text}"
                    )
                    images_processed += 1

            except Exception as e:
                logger.warning(f"OCR failed on image {idx}: {str(e)[:120]}")
                continue

        if not extracted_texts:
            raise ValueError("OCR failed to extract text from any embedded images")

        combined_text = "\n\n".join(extracted_texts)
        logger.info(
            f"OCR complete: {images_processed} pages, "
            f"{len(combined_text)} chars"
        )

        return OCRProcessor._clean_ocr_text(combined_text)

    @staticmethod
    def _clean_ocr_text(text: str) -> str:
        """Post-process OCR output to remove common artifacts"""
        # Fix common OCR errors in Arabic/English mixed text
        text = re.sub(r'\n{3,}', '\n\n', text)  # Excessive newlines
        text = re.sub(r'[ \t]{2,}', ' ', text)  # Excessive spaces
        text = re.sub(r'(\w)-\n(\w)', r'\1\2', text)  # Fix hyphenated word breaks
        text = text.strip()

        # Remove footer/page number patterns common in SAMA documents
        lines = text.split('\n')
        cleaned_lines = []
        for line in lines:
            # Skip lines that look like page numbers or document footers
            if re.fullmatch(r'(page\s*\d+|\d+|sama\s*document|\d+\s*of\s*\d+)', line.strip().lower()):
                continue
            cleaned_lines.append(line)

        return '\n'.join(cleaned_lines).strip()