"""The change-detection fixes in NewOrchestrator, tested without a database.

WHY THIS FILE LOOKS LIKE THIS

Same reason as test_stage_a.py. `orch.py` imports the parent orchestrator, which
pulls in the OCR stack (fitz, pdf2image, paddle, torch). None of that is needed
to check classification logic, and installing it is gigabytes, so the heavy
modules are stubbed before the import.

What is verified here:

  archive id          analysis is archived against the version holding the OLD
                      content, not the one that replaced it
  row refresh         a modify rewrites the row's content, not only its hash
  disappeared         the completeness gate is fed from the repository, so it
                      works on MSSQL and not only on the Excel preview repo
  configurable id     a source config can change what "the same document" means
  early exit          an unchanged inventory actually stops

    venv/Scripts/python.exe -m pytest tests/test_stage_b.py -v
    venv/Scripts/python.exe tests/test_stage_b.py          # no pytest needed
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class _Any:
    """Absorbs whatever a stubbed module is asked to do at import time.
    Chainable, so `Builder.from_all().with_x().build()` survives.
    """

    def __getattr__(self, name):
        return _Any()

    def __call__(self, *a, **kw):
        return _Any()

    def __iter__(self):
        return iter(())

    def __bool__(self):
        return False


class _StubFinder:
    """Fabricates any module under PREFIXES that is not really installed.

    The pipeline's OCR dependencies are imported at module scope by the parent
    orchestrator but never called on these paths.
    """

    PREFIXES = ("fitz", "pdf2image", "pytesseract", "PIL", "cv2", "paddle",
                "paddleocr", "paddlex", "torch", "transformers", "easyocr",
                "docx", "pptx", "camelot", "pdfplumber", "layoutparser",
                "lingua", "langdetect", "openai", "tiktoken", "selenium",
                "bs4", "httpx", "aiohttp", "tenacity")

    def find_module(self, name, path=None):
        return self if name.split(".")[0] in self.PREFIXES else None

    def load_module(self, name):
        mod = types.ModuleType(name)
        mod.__getattr__ = lambda attr: _Any()
        mod.__path__ = []
        sys.modules[name] = mod
        return mod


for _name in _StubFinder.PREFIXES:
    if _name not in sys.modules:
        try:
            __import__(_name)
        except Exception:
            _m = types.ModuleType(_name)
            _m.__getattr__ = lambda attr: _Any()
            _m.__path__ = []
            sys.modules[_name] = _m

from dynamic_crawler.formfill.orch import NewOrchestrator          # noqa: E402


# --------------------------------------------------------------------------- #
#  doubles                                                                     #
# --------------------------------------------------------------------------- #

class Doc:
    def __init__(self, **kw):
        self.document_url = kw.pop("document_url", "")
        self.doc_path = kw.pop("doc_path", [])
        self.content_hash = kw.pop("content_hash", "")
        self.extra_meta = kw.pop("extra_meta", {})
        for k, v in kw.items():
            setattr(self, k, v)


class FakeRepo:
    """Records calls. Reads answer from `rows`; writes only get recorded."""

    def __init__(self, rows=None):
        self.rows = rows or []
        self.calls = []
        self._next_version = 100

    # ---- reads ---- #
    def find_by_identity(self, url, path):
        self.calls.append(("find_by_identity", url, path))
        return next((r for r in self.rows
                     if r.get("document_url") == url
                     and " > ".join(r.get("doc_path") or []) == path), None)

    def find_by_identity_fields(self, fields):
        self.calls.append(("find_by_identity_fields", dict(fields)))
        for r in self.rows:
            for k, v in fields.items():
                stored = r.get(k)
                if isinstance(stored, (list, tuple)):
                    stored = " > ".join(str(x) for x in stored)
                if (str(stored) if stored is not None else "") != v:
                    break
            else:
                return r
        return None

    def find_by_reference(self, ref):
        self.calls.append(("find_by_reference", ref))
        return None

    def find_regulations_by_source(self, source_system):
        self.calls.append(("find_regulations_by_source", source_system))
        return [r for r in self.rows if r.get("source_system") == source_system]

    def get_regulation_by_id(self, rid):
        return next((r for r in self.rows if r.get("id") == rid), {})

    def last_good_run(self, source):
        return getattr(self, "_last_run", None)

    def counts(self):
        return {}

    # ---- writes ---- #
    def __getattr__(self, name):
        def record(*a, **kw):
            self.calls.append((name, a, kw))
            if name == "insert_regulation_version":
                self._next_version += 1
                return self._next_version
            if name == "_insert_regulation":
                return 999
            return None
        return record

    def named(self, name):
        return [c for c in self.calls if c[0] == name]


class FakeCrawler:
    def __init__(self, docs=None, source_system="SRC"):
        self.docs = docs or []
        self.source_system = source_system

    def fetch_documents(self):
        return list(self.docs)


def build(repo, crawler=None, **kw):
    """A NewOrchestrator with the parent's __init__ bypassed."""
    o = NewOrchestrator.__new__(NewOrchestrator)
    o.repo = repo
    o.crawler = crawler or FakeCrawler()
    o.source_name = kw.pop("source_name", "src")
    o.identity = NewOrchestrator._clean_identity(kw.pop("identity", None))
    o.version_key = kw.pop("version_key", None)
    o.analyse = False
    o.limit = None
    o.report = {}
    return o


# --------------------------------------------------------------------------- #
#  archive against the OLD version id                                          #
# --------------------------------------------------------------------------- #

def test_archive_uses_the_old_version_not_the_new_one():
    repo = FakeRepo(rows=[{"id": 7, "content_hash": "old",
                           "document_html": "<p>old</p>"}])
    o = build(repo)
    doc = Doc(document_url="u", content_hash="new", title="T",
              extra_meta={"monitoring_status": "modified",
                          "existing_regulation_id": 7})
    o._timed = lambda *a, **k: _null_ctx()
    o.extract_text_content_unified = lambda *a, **k: ("", None)
    o._log_step = lambda *a, **k: None
    o._process_versioned_doc(doc)

    versions = repo.named("insert_regulation_version")
    assert len(versions) == 2, "old snapshot then new snapshot"
    old_id, new_id = 101, 102
    assert versions[0][2]["status"] == "inactive"
    assert versions[1][2]["status"] == "active"

    archived = repo.named("archive_current_analysis")
    assert archived, "analysis must be archived on a modify"
    assert archived[0][1] == (7, old_id), (
        f"archived against {archived[0][1][1]}, expected the OLD version {old_id}")
    assert archived[0][1][1] != new_id


class _null_ctx:
    def __enter__(self):
        return {"status": "", "message": ""}

    def __exit__(self, *a):
        return False


# --------------------------------------------------------------------------- #
#  a modify refreshes the row, not just the hash                               #
# --------------------------------------------------------------------------- #

def test_modify_rewrites_content_not_only_the_hash():
    doc = Doc(document_url="u", content_hash="new", title="New Title",
              document_html="<p>new</p>", published_date="2026-08-01",
              extra_meta={"a": 1})
    fields = NewOrchestrator._modified_row_fields(doc, "new")
    assert fields["content_hash"] == "new"
    assert fields["document_html"] == "<p>new</p>", (
        "the new hash must not sit next to the old html")
    assert fields["title"] == "New Title"
    assert fields["published_date"] == "2026-08-01"
    assert "extra_meta" in fields


def test_modify_never_blanks_a_field_the_crawl_did_not_return():
    doc = Doc(document_url="u", content_hash="new", title="", document_html=None)
    fields = NewOrchestrator._modified_row_fields(doc, "new")
    assert "title" not in fields, "an empty title must not erase the stored one"
    assert "document_html" not in fields
    assert fields == {"content_hash": "new"}


# --------------------------------------------------------------------------- #
#  the disappeared bucket                                                      #
# --------------------------------------------------------------------------- #

def test_disappeared_is_filled_from_the_repository():
    stored = [
        {"id": 1, "document_url": "a", "doc_path": ["X"], "content_hash": "h1",
         "source_system": "SRC"},
        {"id": 2, "document_url": "b", "doc_path": ["X"], "content_hash": "h2",
         "source_system": "SRC"},
    ]
    repo = FakeRepo(rows=stored)
    o = build(repo, FakeCrawler(source_system="SRC"))
    seen = Doc(document_url="a", doc_path=["X"], content_hash="h1")

    buckets = o.classify_documents([seen])
    assert [r["id"] for r in buckets["disappeared"]] == [2], (
        "the document the crawl did not return must be reported")
    assert repo.named("find_regulations_by_source"), (
        "must ask the repository, not an Excel-only attribute")


def test_disappeared_is_empty_when_the_run_saw_everything():
    stored = [{"id": 1, "document_url": "a", "doc_path": ["X"],
               "content_hash": "h1", "source_system": "SRC"}]
    o = build(FakeRepo(rows=stored), FakeCrawler(source_system="SRC"))
    buckets = o.classify_documents([Doc(document_url="a", doc_path=["X"],
                                        content_hash="h1")])
    assert buckets["disappeared"] == []
    assert len(buckets["unchanged"]) == 1


# --------------------------------------------------------------------------- #
#  configurable identity                                                       #
# --------------------------------------------------------------------------- #

def test_identity_defaults_to_url_and_path():
    o = build(FakeRepo())
    assert o.identity == ("document_url", "doc_path")
    o.classify_documents([Doc(document_url="u", doc_path=["A", "B"])])
    assert o.repo.named("find_by_identity")[0][1:] == ("u", "A > B")


def test_a_configured_identity_changes_the_lookup():
    stored = [{"id": 5, "reference_no": "R-1", "content_hash": "h",
               "source_system": "SRC"}]
    repo = FakeRepo(rows=stored)
    o = build(repo, FakeCrawler(source_system="SRC"),
              identity=["reference_no"])
    buckets = o.classify_documents([Doc(reference_no="R-1", content_hash="h")])

    assert repo.named("find_by_identity_fields"), "must use the generic lookup"
    assert repo.named("find_by_identity") == [], "must not use the url lookup"
    assert len(buckets["unchanged"]) == 1, "matched on reference_no alone"


def test_identity_accepts_a_bare_string():
    assert NewOrchestrator._clean_identity("page") == ("page",)
    assert NewOrchestrator._clean_identity([]) == ("document_url", "doc_path")
    assert NewOrchestrator._clean_identity(None) == ("document_url", "doc_path")


def test_a_repo_that_cannot_honour_the_config_says_so():
    class LegacyRepo:
        """Only the two-column lookup, like a repo written before this."""

        def find_by_identity(self, url, path):
            return None

    o = build(LegacyRepo(), identity=["reference_no"])
    try:
        o.classify_documents([Doc(reference_no="R-1")])
    except NotImplementedError as e:
        assert "reference_no" in str(e)
    else:
        raise AssertionError(
            "a repo without the generic lookup must raise, not classify "
            "every document as new")


# --------------------------------------------------------------------------- #
#  the early exit                                                              #
# --------------------------------------------------------------------------- #

def test_unchanged_inventory_stops_before_processing():
    stored = [{"id": 1, "document_url": "a", "doc_path": ["X"],
               "content_hash": "h1", "source_system": "SRC"}]
    repo = FakeRepo(rows=stored)
    docs = [Doc(document_url="a", doc_path=["X"], content_hash="h1")]
    o = build(repo, FakeCrawler(docs, source_system="SRC"))
    o.check_run_trustworthy = lambda d: (True, [])
    o._inventory_hash = lambda d: "INV"
    repo._last_run = {"inventory_hash": "INV"}
    processed = []
    o._process_docs = lambda todo, name: processed.append(todo)

    report = o.run_for_regulator("REG")
    assert processed == [], "an unchanged inventory must not process anything"
    assert report["processed"] == 0
    assert "skipped" in report
    assert repo.named("record_run") == [], "no run recorded for a no-op"


def test_a_changed_inventory_still_processes():
    repo = FakeRepo(rows=[])
    docs = [Doc(document_url="a", doc_path=["X"], content_hash="h1")]
    o = build(repo, FakeCrawler(docs, source_system="SRC"))
    o.check_run_trustworthy = lambda d: (True, [])
    o._inventory_hash = lambda d: "NEW-INV"
    repo._last_run = {"inventory_hash": "OLD-INV"}
    processed = []
    o._process_docs = lambda todo, name: processed.append(todo)

    report = o.run_for_regulator("REG")
    assert len(processed) == 1 and len(processed[0]) == 1
    assert report["processed"] == 1


def test_pending_work_overrides_an_unchanged_hash():
    """A previous run that died mid-way leaves work behind. The hash is the
    same, but the buckets are not empty, so it must not skip."""
    repo = FakeRepo(rows=[])
    docs = [Doc(document_url="a", doc_path=["X"], content_hash="h1")]
    o = build(repo, FakeCrawler(docs, source_system="SRC"))
    o.check_run_trustworthy = lambda d: (True, [])
    o._inventory_hash = lambda d: "INV"
    repo._last_run = {"inventory_hash": "INV"}
    processed = []
    o._process_docs = lambda todo, name: processed.append(todo)

    report = o.run_for_regulator("REG")
    assert processed, "one new document is pending; the run must not skip"
    assert report["processed"] == 1


# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"  ok    {name}")
            except Exception as e:
                failed += 1
                print(f"  FAIL  {name}: {e}")
    print(f"\n{failed} failed" if failed else "\nall passed")
    sys.exit(1 if failed else 0)
