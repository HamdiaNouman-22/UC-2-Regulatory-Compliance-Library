"""The sweep over the stored inventory: two bytes per document we already hold.

WHY THIS FILE LOOKS LIKE THIS

No network and no database. `fingerprint.tokens_for` is replaced per test with a
dict of the answers a regulator would have given, which is the same trick
tests/test_fingerprint.py uses on the requests session one level down.

What is verified here:

  identity        rows become observations keyed on EACH ROW's own declared
                  identity, not on the sweep's default
  regulator scope a source_system shared by two regulators cannot leak the other
                  one's documents into the sweep
  blocked hosts   a host we are banned from is never probed, and the count and
                  the host reach the report
  no validator    a row with no probeable url is reported as one, not folded into
                  `unchanged`
  detect only     the sweep can never report an absence, because it only reads
                  urls it already knew
  confirm tier    it runs for the shortlist only, hashes the document's bytes,
                  and refuses to hash a partial read
  configuration   the (regulator, source_system) pair resolves the confirm tier,
                  and the two pairs sharing one source_system resolve differently

    venv/Scripts/python.exe -m pytest tests/test_inventory_sweep.py -v
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class _Any:
    def __getattr__(self, name):
        return _Any()

    def __call__(self, *a, **kw):
        return _Any()

    def __iter__(self):
        return iter(())

    def __bool__(self):
        return False


_PREFIXES = ("fitz", "pdf2image", "pytesseract", "PIL", "cv2", "paddle",
             "paddleocr", "paddlex", "torch", "transformers", "easyocr",
             "docx", "pptx", "camelot", "pdfplumber", "layoutparser",
             "lingua", "langdetect", "openai", "tiktoken", "selenium",
             "bs4", "httpx", "aiohttp", "tenacity")

for _name in _PREFIXES:
    if _name not in sys.modules:
        try:
            __import__(_name)
        except Exception:
            _m = types.ModuleType(_name)
            _m.__getattr__ = lambda attr: _Any()
            _m.__path__ = []
            sys.modules[_name] = _m

from dynamic_crawler import changesignal as cs                        # noqa: E402
from dynamic_crawler import fingerprint                               # noqa: E402
from dynamic_crawler import inventory_sweep as iv                     # noqa: E402
from dynamic_crawler.change_state import ChangeStateStore             # noqa: E402


# --------------------------------------------------------------------------- #
#  doubles                                                                     #
# --------------------------------------------------------------------------- #

class FakeRepo:
    """Stored regulation rows, shaped as both repos now return them."""

    def __init__(self, rows):
        self.rows = rows
        self.lookups = []

    def find_regulations_by_source(self, source_system, regulator=None):
        self.lookups.append((source_system, regulator))
        return [dict(r) for r in self.rows
                if r.get("source_system") == source_system
                and (not regulator or r.get("regulator") == regulator)]


def row(id=1, url="https://sdaia.gov.sa/1.pdf", source="Laws and Regulations",
        regulator="SDAIA", **kw) -> dict:
    out = {"id": id, "title": f"document {id}", "document_url": url,
           "doc_path": ["Policies"], "content_hash": "H",
           "source_system": source, "regulator": regulator, "extra_meta": {}}
    out.update(kw)
    return out


def answers(tokens: dict):
    """Replace the probe with what a regulator would have answered."""
    return lambda urls, workers=None, timeout=20.0: {
        u: tokens.get(u, ("", fingerprint.BASIS_FAILED)) for u in urls}


@pytest.fixture
def probed(monkeypatch):
    def install(tokens):
        monkeypatch.setattr(iv.fingerprint, "tokens_for", answers(tokens))
    return install


def store_at(tmp_path) -> ChangeStateStore:
    return ChangeStateStore(tmp_path / "state.json", source="SDAIA/Laws")


def sweep(repo, **kw) -> iv.StoredInventorySweep:
    kw.setdefault("regulator", "SDAIA")
    kw.setdefault("source_system", "Laws and Regulations")
    return iv.StoredInventorySweep(repo, **kw)


# --------------------------------------------------------------------------- #
#  what the sweep reads                                                        #
# --------------------------------------------------------------------------- #

def test_rows_become_observations_keyed_on_their_own_declared_identity(probed):
    """The row carries what its source decided, so the sweep must not impose the
    default on a source that keys on something else."""
    probed({"https://x/1.pdf": ("{G},4", "etag")})
    repo = FakeRepo([row(url="https://x/1.pdf", reference_no="C-9",
                         extra_meta={"identity_fields": ["reference_no"]})])
    observations = sweep(repo).sweep()
    assert [o.key for o in observations] == ["reference_no=C-9"]
    assert observations[0].token == "{G},4"


def test_a_row_without_a_declared_identity_takes_the_configured_default(probed):
    probed({"https://x/1.pdf": ("{G},4", "etag")})
    repo = FakeRepo([row(url="https://x/1.pdf")])
    observations = sweep(repo, identity=("document_url",)).sweep()
    assert [o.key for o in observations] == ["document_url=https://x/1.pdf"]


def test_the_regulator_scopes_the_lookup(probed):
    """AML and SIMAH share "Rules and Regulations". A sweep that read both would
    probe simah.com, the one host we are banned from."""
    probed({})
    repo = FakeRepo([row(id=1, source="Rules and Regulations", regulator="AML",
                         url="https://aml.gov.sa/1.pdf"),
                     row(id=2, source="Rules and Regulations", regulator="SIMAH",
                         url="https://www.simah.com/x.pdf")])
    s = sweep(repo, regulator="AML", source_system="Rules and Regulations")
    assert len(s.sweep()) == 1
    assert repo.lookups == [("Rules and Regulations", "AML")]


def test_limit_slices_before_the_probe(probed):
    probed({f"https://x/{i}.pdf": ("t", "etag") for i in range(5)})
    repo = FakeRepo([row(id=i, url=f"https://x/{i}.pdf") for i in range(5)])
    assert len(sweep(repo, limit=2).sweep()) == 2


# --------------------------------------------------------------------------- #
#  hosts we may not touch                                                      #
# --------------------------------------------------------------------------- #

def test_a_blocked_host_is_never_probed_and_is_reported(monkeypatch):
    """Not "probed and discarded" — the request must never leave."""
    asked = []

    def spy(urls, workers=None, timeout=20.0):
        asked.extend(urls)
        return {u: ("t", "etag") for u in urls}

    monkeypatch.setattr(iv.fingerprint, "tokens_for", spy)
    repo = FakeRepo([row(id=1, url="https://sdaia.gov.sa/1.pdf"),
                     row(id=2, url="https://www.simah.com/rules.pdf")])
    s = sweep(repo, skip_hosts=["simah.com"])
    observations = s.sweep()

    assert asked == ["https://sdaia.gov.sa/1.pdf"]
    assert len(observations) == 1
    assert s.stats["skipped_hosts"] == {"simah.com": 1}


def test_a_subdomain_of_a_blocked_host_is_blocked_too(probed):
    probed({})
    repo = FakeRepo([row(url="https://cdn.simah.com/x.pdf")])
    s = sweep(repo, skip_hosts=["simah.com"])
    assert s.sweep() == []
    assert s.stats["skipped_hosts"] == {"simah.com": 1}


def test_the_block_does_not_lift_itself_on_a_date():
    """`until` is when the entry may be reviewed. A block that expired by itself
    would send the next run at a banned host with nobody having decided."""
    config = {"skip_hosts": [{"host": "simah.com", "until": "1999-01-01"}]}
    assert iv.skip_hosts(config) == ["simah.com"]


# --------------------------------------------------------------------------- #
#  honest counts                                                               #
# --------------------------------------------------------------------------- #

def test_a_row_with_no_probeable_url_is_reported_not_hidden(probed, tmp_path):
    probed({"https://x/1.pdf": ("{G},4", "etag")})
    repo = FakeRepo([row(id=1, url="https://x/1.pdf"), row(id=2, url="")])
    s = sweep(repo)
    report, _buckets = cs.run_sweep(s, store_at(tmp_path))
    assert s.stats["no_probeable_url"] == 1
    assert report["without_token"] == 1
    assert report["observed"] == 2


def test_a_failed_probe_reaches_the_report_as_unknown(probed, tmp_path):
    store = store_at(tmp_path)
    probed({"https://x/1.pdf": ("{G},4", "etag")})
    repo = FakeRepo([row(url="https://x/1.pdf")])
    cs.run_sweep(sweep(repo), store)                    # baseline

    probed({})                                          # every probe now fails
    report, buckets = cs.run_sweep(sweep(repo), store)
    assert report["counts"][cs.UNKNOWN] == 1
    assert report["counts"][cs.UNCHANGED] == 0
    assert report["by_basis"] == {fingerprint.BASIS_FAILED: 1}
    assert buckets[cs.UNKNOWN][0][1] == "the probe did not run"


def test_a_moved_version_counter_is_modified(probed, tmp_path):
    store = store_at(tmp_path)
    repo = FakeRepo([row(url="https://x/1.pdf")])
    probed({"https://x/1.pdf": ("{G},4", "etag")})
    cs.run_sweep(sweep(repo), store)

    probed({"https://x/1.pdf": ("{G},5", "etag")})
    _report, buckets = cs.run_sweep(sweep(repo), store)
    assert ([o.key for o, _w in buckets[cs.MODIFIED]]
            == ["document_url=https://x/1.pdf|doc_path=Policies"])


def test_a_stored_inventory_sweep_can_never_report_an_absence(probed, tmp_path):
    """It reads only urls we already store. Discovering a withdrawal needs the
    crawl, and claiming one from here would be a guess."""
    store = store_at(tmp_path)
    probed({"https://x/1.pdf": ("t", "etag"), "https://x/2.pdf": ("t", "etag")})
    two = FakeRepo([row(id=1, url="https://x/1.pdf"), row(id=2, url="https://x/2.pdf")])
    cs.run_sweep(sweep(two), store)

    one = FakeRepo([row(id=1, url="https://x/1.pdf")])
    report, buckets = cs.run_sweep(sweep(one), store)
    assert iv.StoredInventorySweep.covers_inventory is False
    assert buckets[cs.MISSING] == []
    assert "not measured" in report["missing"]


# --------------------------------------------------------------------------- #
#  the confirm tier                                                            #
# --------------------------------------------------------------------------- #

def test_confirm_runs_for_the_shortlist_only(probed, tmp_path, monkeypatch):
    store = store_at(tmp_path)
    repo = FakeRepo([row(id=1, url="https://x/1.pdf"), row(id=2, url="https://x/2.pdf")])
    probed({"https://x/1.pdf": ("4", "etag"), "https://x/2.pdf": ("4", "etag")})
    cs.run_sweep(sweep(repo, confirm_required=True), store)

    probed({"https://x/1.pdf": ("5", "etag"), "https://x/2.pdf": ("4", "etag")})
    s = sweep(repo, confirm_required=True)
    confirmed = []
    monkeypatch.setattr(s, "confirm", lambda obs: confirmed.append(obs.url) or "H")
    cs.run_sweep(s, store)
    assert confirmed == ["https://x/1.pdf"]


def test_a_bulk_republish_is_not_a_change(probed, tmp_path, monkeypatch):
    """One regulator re-uploaded its whole library in three seconds and every
    counter moved. The content hash is what says nothing was edited."""
    store = store_at(tmp_path)
    repo = FakeRepo([row(url="https://x/1.pdf")])
    probed({"https://x/1.pdf": ("4", "etag")})
    s = sweep(repo, confirm_required=True)
    monkeypatch.setattr(s, "confirm", lambda obs: "SAME")
    cs.run_sweep(s, store)
    store.records["document_url=https://x/1.pdf|doc_path=Policies"]["confirm_hash"] = "SAME"

    probed({"https://x/1.pdf": ("5", "etag")})
    s2 = sweep(repo, confirm_required=True)
    monkeypatch.setattr(s2, "confirm", lambda obs: "SAME")
    _report, buckets = cs.run_sweep(s2, store)
    assert buckets[cs.UNCHANGED] and not buckets[cs.MODIFIED]


def test_confirm_hashes_the_bytes_and_refuses_a_partial_read(monkeypatch):
    class Response:
        def __init__(self, chunks):
            self.chunks = chunks

        def raise_for_status(self):
            pass

        def iter_content(self, size):
            return iter(self.chunks)

    s = sweep(FakeRepo([]), confirm_required=True)
    obs = cs.Observation(key="k", url="https://x/1.pdf")

    monkeypatch.setattr(iv.requests, "get",
                        lambda *a, **kw: Response([b"hello ", b"world"]))
    import hashlib
    assert s.confirm(obs) == hashlib.sha256(b"hello world").hexdigest()

    monkeypatch.setattr(iv, "MAX_CONFIRM_BYTES", 4)
    with pytest.raises(ValueError) as e:
        s.confirm(obs)
    assert "partial read" in str(e.value)


def test_confirm_is_skipped_when_the_source_does_not_need_one():
    s = sweep(FakeRepo([]), confirm_required=False)
    assert s.confirm(cs.Observation(key="k", url="https://x/1.pdf")) is None


# --------------------------------------------------------------------------- #
#  configuration                                                               #
# --------------------------------------------------------------------------- #

CONFIG = {
    "defaults": {"identity": ["document_url", "doc_path"], "confirm": False,
                 "workers": 4, "timeout": 20},
    "sources": [
        {"regulator": "SDAIA", "source_system": "Laws and Regulations",
         "confirm": False},
        {"regulator": "MISA", "source_system": "Laws and Regulations",
         "confirm": True},
        {"regulator": "Anti-Money Laundering Permanent Committee",
         "source_system": "Rules and Regulations", "confirm": True},
    ],
}


def test_the_pair_resolves_the_confirm_tier_not_the_source_system_alone():
    """Two regulators publish under "Laws and Regulations" and they do not agree
    about the confirm tier. Matching on the source alone hands one of them the
    other's setting."""
    assert iv.settings_for(CONFIG, "SDAIA", "Laws and Regulations")["confirm"] is False
    assert iv.settings_for(CONFIG, "MISA", "Laws and Regulations")["confirm"] is True


def test_an_unlisted_source_takes_the_defaults():
    settings = iv.settings_for(CONFIG, "Tadawul", "Rules")
    assert settings["confirm"] is False and settings["workers"] == 4


def test_the_shipped_config_parses_and_says_what_was_measured():
    config = iv.load_config()
    aml = iv.settings_for(config, "Anti-Money Laundering Permanent Committee",
                          "Rules and Regulations")
    sdaia = iv.settings_for(config, "SDAIA", "Laws and Regulations")
    assert aml["confirm"] is True        # every document at version 5, 3s apart
    assert sdaia["confirm"] is False     # counters 1-4, 18 distinct timestamps
    assert "simah.com" in iv.skip_hosts(config)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
