"""Download the actual PDF files referenced by a crawl's documents.

The generated adapter is sandboxed and CANNOT write files or open arbitrary
network connections beyond the rate-limited Fetcher — by design. Downloading the
real PDF documents is therefore done here, by TRUSTED post-processing code, after
the crawl produces its document list. Files land under <work_dir>/pdfs/ and each
document is annotated with its local path in extra_meta['local_pdf'] so the review
Excel/JSON can point at the saved file.

Nothing here touches the production DB; it only writes local files for review.
"""

import logging
import re
import time
from pathlib import Path
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


def _is_pdf_doc(doc) -> bool:
    ft = (getattr(doc, "file_type", None) or "").lower()
    url = (getattr(doc, "document_url", None) or "")
    return ft == "pdf" or url.lower().endswith(".pdf")


def _safe_name(doc, index: int) -> str:
    ref = (getattr(doc, "reference_no", None) or "").strip()
    title = (getattr(doc, "title", None) or "").strip()
    stem = ref or title or urlparse(getattr(doc, "document_url", "") or "").path.rsplit("/", 1)[-1]
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("_")[:80] or "document"
    return f"{index:04d}_{stem}.pdf"


def download_pdfs(documents, work_dir, delay: float = 0.2, timeout: int = 40,
                  max_files: int = None) -> dict:
    """Download every PDF document into <work_dir>/pdfs/. Annotates each doc's
    extra_meta with local_pdf (relative path) / pdf_download_error. Returns a summary."""
    out_dir = Path(work_dir) / "pdfs"
    out_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": _UA})

    pdf_docs = [d for d in documents if _is_pdf_doc(d)]
    if max_files:
        pdf_docs = pdf_docs[:max_files]

    ok, failed = 0, 0
    logger.info(f"Downloading {len(pdf_docs)} PDF file(s) -> {out_dir}")
    for i, doc in enumerate(pdf_docs, 1):
        url = doc.document_url
        if not isinstance(getattr(doc, "extra_meta", None), dict):
            doc.extra_meta = {}
        fname = _safe_name(doc, i)
        dest = out_dir / fname
        try:
            resp = session.get(url, timeout=timeout, stream=True, allow_redirects=True)
            ctype = resp.headers.get("Content-Type", "")
            if resp.status_code >= 400:
                raise requests.RequestException(f"HTTP {resp.status_code}")
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(65536):
                    if chunk:
                        f.write(chunk)
            # Guard against saving an HTML error page as a .pdf
            if dest.stat().st_size < 200 or ("pdf" not in ctype.lower()
                                             and not dest.read_bytes()[:5].startswith(b"%PDF")):
                raise requests.RequestException(f"not a PDF (content-type={ctype})")
            doc.extra_meta["local_pdf"] = str(Path("pdfs") / fname)
            ok += 1
        except Exception as e:
            doc.extra_meta["pdf_download_error"] = str(e)
            failed += 1
            logger.warning(f"PDF download failed [{i}/{len(pdf_docs)}] {url}: {e}")
        if delay:
            time.sleep(delay)

    session.close()
    logger.info(f"PDF download complete: {ok} saved, {failed} failed, in {out_dir}")
    return {"pdf_dir": str(out_dir), "downloaded": ok, "failed": failed, "total": len(pdf_docs)}
