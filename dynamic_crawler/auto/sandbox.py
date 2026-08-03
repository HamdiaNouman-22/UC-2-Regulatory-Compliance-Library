"""Runs a generated adapter in an isolated subprocess and returns its documents.

The parent never imports or exec()s the untrusted adapter — it only ever spawns
`python -m dynamic_crawler.auto.runner ...` with a wall-clock timeout. On expiry
the whole process tree is killed. Results come back as JSON on disk.
"""

import json
import logging
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from models.models import RegulatoryDocument

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class SandboxResult:
    ok: bool
    documents: List[RegulatoryDocument]
    fetch_count: int
    error: Optional[str] = None
    traceback: Optional[str] = None
    timed_out: bool = False


def _docs_from_dicts(raw: list) -> List[RegulatoryDocument]:
    field_names = set(RegulatoryDocument.__dataclass_fields__.keys())
    docs = []
    for d in raw:
        kwargs = {k: v for k, v in d.items() if k in field_names}
        docs.append(RegulatoryDocument(**kwargs))
    return docs


def run(
    adapter_path: str,
    limit: Optional[int] = None,
    backend: str = "requests",
    max_fetches: int = 1500,
    request_delay: float = 1.0,
    timeout_seconds: int = 1800,
) -> SandboxResult:
    """Execute the adapter at adapter_path in a sandboxed subprocess."""
    out_fd, out_path = tempfile.mkstemp(suffix=".json", prefix="adapter_out_")
    os.close(out_fd)

    cmd = [
        sys.executable, "-m", "dynamic_crawler.auto.runner", adapter_path,
        "--out", out_path,
        "--backend", backend,
        "--max-fetches", str(max_fetches),
        "--request-delay", str(request_delay),
    ]
    if limit is not None:
        cmd += ["--limit", str(limit)]

    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get("PYTHONPATH", "")

    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP  # allow killing the tree on Windows

    try:
        proc = subprocess.run(
            cmd, cwd=str(REPO_ROOT), env=env,
            capture_output=True, text=True,
            timeout=timeout_seconds, creationflags=creationflags,
        )
    except subprocess.TimeoutExpired:
        logger.warning(f"Adapter run timed out after {timeout_seconds}s")
        _safe_unlink(out_path)
        return SandboxResult(ok=False, documents=[], fetch_count=0,
                             error=f"Timed out after {timeout_seconds}s", timed_out=True)

    try:
        with open(out_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        # Runner died before writing output (e.g. import-guard rejection at load).
        return SandboxResult(
            ok=False, documents=[], fetch_count=0,
            error="Runner produced no output",
            traceback=(proc.stderr or proc.stdout or "")[-4000:],
        )
    finally:
        _safe_unlink(out_path)

    if "error" in payload:
        return SandboxResult(
            ok=False, documents=[], fetch_count=payload.get("fetch_count", 0),
            error=payload["error"], traceback=payload.get("traceback"),
        )

    docs = _docs_from_dicts(payload.get("documents", []))
    return SandboxResult(ok=True, documents=docs, fetch_count=payload.get("fetch_count", 0))


def _safe_unlink(path: str):
    try:
        os.unlink(path)
    except OSError:
        pass
