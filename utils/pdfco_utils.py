import os
import time
import json
import requests
from dotenv import load_dotenv

load_dotenv()

PDFCO_KEY = os.getenv("PDFCO_API_KEY")

PDFCO_UPLOAD_URL = "https://api.pdf.co/v1/file/upload"
PDFCO_CONVERT_URL = "https://api.pdf.co/v1/pdf/convert/to/html"
PDFCO_JOB_CHECK_URL = "https://api.pdf.co/v1/job/check"


def pdfco_pdf_to_html(pdf_path: str, lang: str = "eng") -> str:
    """
    Convert a PDF (digital or scanned) to HTML using PDF.co.
    Supports English, Arabic, or mixed languages.

    Args:
        pdf_path: Path to the PDF file
        lang: OCR language codes (default "eng+ara")

    Returns:
        HTML content as string
    """

    if not PDFCO_KEY:
        raise RuntimeError("PDFCO_API_KEY not found in environment variables")

    headers = {"x-api-key": PDFCO_KEY}

    # --------------------------------------------------
    # 1️⃣ Upload PDF
    # --------------------------------------------------
    with open(pdf_path, "rb") as f:
        upload_resp = requests.post(
            PDFCO_UPLOAD_URL,
            headers=headers,
            files={"file": f}
        )

    upload_json = upload_resp.json()

    if upload_json.get("error"):
        raise RuntimeError(f"PDF.co upload failed: {upload_json.get('message')}")

    file_url = upload_json.get("url")

    # --------------------------------------------------
    # 2️⃣ Start async PDF → HTML conversion (OCR enabled)
    # --------------------------------------------------
    params = {
        "url": file_url,
        "ocr": True,
        "lang": lang,
        "async": False
    }

    convert_resp = requests.post(
        PDFCO_CONVERT_URL,
        headers=headers,
        params=params
    )

    convert_json = convert_resp.json()

    print("\n===== PDF.co CONVERT RESPONSE =====")
    print(json.dumps(convert_json, indent=2, ensure_ascii=False))
    print("===== END RESPONSE =====\n")
# If there's a URL, conversion is already done
    if convert_json.get("url"):
        html_url = convert_json["url"]
        html_resp = requests.get(html_url)
        html_resp.raise_for_status()
        return html_resp.text


    if convert_json.get("error"):
        raise RuntimeError(f"PDF.co conversion start failed: {convert_json.get('message')}")

        # Otherwise, fallback to async job polling
    job_id = convert_json.get("jobId")
    if not job_id:
        raise RuntimeError("PDF.co did not return jobId")
    # --------------------------------------------------
    # 3️⃣ Poll job status
    # --------------------------------------------------
    while True:
        time.sleep(3)

        check_resp = requests.get(
            PDFCO_JOB_CHECK_URL,
            headers=headers,
            params={"jobid": job_id}
        )

        check_json = check_resp.json()

        print("Job status:", check_json.get("status"))

        if check_json.get("status") == "success":
            break

        if check_json.get("status") == "failed":
            raise RuntimeError(f"PDF.co job failed: {check_json}")

    # --------------------------------------------------
    # 4️⃣ Retrieve HTML output
    # --------------------------------------------------

    # Case 1: Inline HTML
    if "html" in check_json:
        return check_json["html"]

    if "body" in check_json:
        return check_json["body"]

    # Case 2: Single HTML URL
    if "url" in check_json:
        html_resp = requests.get(check_json["url"])
        html_resp.raise_for_status()
        return html_resp.text

    # Case 3: Multiple HTML pages
    if "urls" in check_json:
        html_pages = []
        for url in check_json["urls"]:
            r = requests.get(url)
            r.raise_for_status()
            html_pages.append(r.text)
        return "\n".join(html_pages)

    raise RuntimeError(f"No HTML output found: {check_json}")
