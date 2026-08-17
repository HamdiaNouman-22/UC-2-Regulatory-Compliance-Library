"""Turning "absent again" into "propose a withdrawal", and refusing to.

This is the one decision in the change-signal tier that can empty a compliance
library, so most of what is verified here is the refusals.

  attribution     a signal may not judge an absence recorded by another signal,
                  because two signals share one state file per source
  the streak      two consecutive sweeps AND a span of real time — running the
                  CLI twice in one second is two sweeps and no evidence
  the gate        a sweep that saw nothing, collided, cannot observe an absence
                  at all, or lost documents proposes nothing
  seen is seen    a collided key is never offered as absent, because the sweep
                  did read a document there
  the write       exists on both repos, is called by nothing, and sets a status
                  rather than deleting a row

No network and no database.

    venv/Scripts/python.exe -m pytest tests/test_withdrawal.py -v
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

from dynamic_crawler import changesignal as cs                       # noqa: E402
from dynamic_crawler import withdrawal as wd                         # noqa: E402
from dynamic_crawler.change_state import ChangeStateStore            # noqa: E402
from dynamic_crawler.formfill.excel_repo import ExcelRepo            # noqa: E402


# --------------------------------------------------------------------------- #
#  doubles                                                                     #
# --------------------------------------------------------------------------- #

class FakeSignal(cs.ChangeSignal):
    def __init__(self, observations, *, name="fake", covers_inventory=True):
        self.observations = list(observations)
        self.name = name
        self.covers_inventory = covers_inventory

    def sweep(self):
        return list(self.observations)


def obs(url, token="T1", page="p"):
    fields = {"page": page, "document_url": url}
    return cs.Observation(key=cs.identity_key(fields), fields=fields,
                          identity_fields=("page", "document_url"),
                          token=token, basis="etag", url=url, title=url)


def store_at(tmp_path, name="state.json") -> ChangeStateStore:
    return ChangeStateStore(tmp_path / name, source="source:TEST")


def record(store, key, *, signal="fake", misses=0, first_missed="",
           last_missed="", url="https://x/1"):
    """A stored record in whatever state the case under test needs."""
    store.records[key] = {"fields": {}, "identity_fields": [], "signal": signal,
                          "title": "a document", "url": url,
                          "token": "T1", "basis": "etag", "confirm_hash": "",
                          "first_seen": "2026-08-01T00:00:00Z",
                          "last_seen": "2026-08-08T00:00:00Z",
                          "last_verdict": cs.UNCHANGED, "misses": misses}
    if first_missed:
        store.records[key]["first_missed"] = first_missed
    if last_missed:
        store.records[key]["last_missed"] = last_missed
    return key


def sweep_of(store, signal):
    """Run a sweep and return (report, proposals)."""
    report, buckets = cs.run_sweep(signal, store)
    return report, wd.proposals(signal, store, report, buckets)


# --------------------------------------------------------------------------- #
#  the streak: how many sweeps, and over how long                              #
# --------------------------------------------------------------------------- #

def test_one_absence_is_not_enough():
    verdict, why = wd.decide(
        {"signal": "fake", "misses": 1, "first_missed": "2026-08-01T00:00:00Z",
         "last_missed": "2026-08-01T00:00:00Z"}, "fake", True, [])
    assert verdict == wd.WATCHING
    assert "1 sweep(s), 2 required" in why


def test_two_absences_a_second_apart_are_two_sweeps_and_no_evidence():
    """The rule says two consecutive runs, and nothing stops two runs of the CLI
    in one second. A regulator mid-republish is not a withdrawal."""
    verdict, why = wd.decide(
        {"signal": "fake", "misses": 2, "first_missed": "2026-08-01T00:00:00Z",
         "last_missed": "2026-08-01T00:00:01Z"}, "fake", True, [])
    assert verdict == wd.WATCHING
    assert "only over 0.0h" in why


def test_two_absences_over_a_day_are_proposed():
    verdict, why = wd.decide(
        {"signal": "fake", "misses": 2, "first_missed": "2026-08-01T00:00:00Z",
         "last_missed": "2026-08-02T00:00:00Z"}, "fake", True, [])
    assert verdict == wd.PROPOSED
    assert "2 consecutive sweeps over 24.0h" in why


def test_a_streak_with_no_span_is_watched_not_proposed():
    """A record written before both ends were stamped. Silence is not consent."""
    verdict, why = wd.decide({"signal": "fake", "misses": 9}, "fake", True, [])
    assert verdict == wd.WATCHING
    assert "no measured span" in why


def test_the_span_is_measured_from_the_first_miss_not_the_first_sighting():
    assert wd.span_hours({"first_missed": "2026-08-01T00:00:00Z",
                          "last_missed": "2026-08-01T12:00:00Z"}) == 12.0
    assert wd.span_hours({"first_seen": "2020-01-01T00:00:00Z"}) is None


# --------------------------------------------------------------------------- #
#  attribution — two signals, one state file                                   #
# --------------------------------------------------------------------------- #

def test_a_signal_may_not_judge_another_signals_absence():
    """The store is per SOURCE, not per signal, so one source's state file can
    hold records from two sweeps whose identity keys are different shapes
    entirely. Without this the second sweep reports the first's documents as
    withdrawal candidates."""
    verdict, why = wd.decide(
        {"signal": "gosi-json:SocialInsurance", "misses": 5,
         "first_missed": "2026-08-01T00:00:00Z",
         "last_missed": "2026-08-05T00:00:00Z"}, "stored-inventory", True, [])
    assert verdict == wd.NOT_JUDGED
    assert "recorded by gosi-json:SocialInsurance" in why


def test_an_unattributed_record_is_not_judged_by_anyone():
    """A state file written before records carried a signal name. It self-heals
    on the next sweep that SEES the document; until then nobody may act."""
    verdict, why = wd.decide(
        {"misses": 4, "first_missed": "2026-08-01T00:00:00Z",
         "last_missed": "2026-08-05T00:00:00Z"}, "fake", True, [])
    assert verdict == wd.NOT_JUDGED
    assert "cannot be attributed" in why


def test_two_signals_in_one_store_do_not_propose_each_others_documents(tmp_path):
    """End to end, because this is the case that would empty a library."""
    store = store_at(tmp_path)
    first = FakeSignal([obs("https://x/1")], name="signal-a")
    cs.run_sweep(first, store)

    # A different signal sweeps the same source and sees its own document only.
    second = FakeSignal([obs("https://x/2")], name="signal-b")
    report, out = sweep_of(store, second)

    assert report["counts"][cs.MISSING] == 1
    assert out["counts"][wd.PROPOSED] == 0
    assert out["counts"][wd.NOT_JUDGED] == 1
    assert "signal-a" in out[wd.NOT_JUDGED][0]["why"]


def test_the_signal_name_is_stamped_on_every_record(tmp_path):
    store = store_at(tmp_path)
    cs.run_sweep(FakeSignal([obs("https://x/1")], name="signal-a"), store)
    assert store.get(obs("https://x/1").key)["signal"] == "signal-a"


# --------------------------------------------------------------------------- #
#  the sweep gate                                                              #
# --------------------------------------------------------------------------- #

def test_a_detect_only_signal_proposes_nothing(tmp_path):
    """It reads only urls we already store, so absence is not something it can
    observe — and `missing` is not even in its report."""
    store = store_at(tmp_path)
    signal = FakeSignal([obs("https://x/1")], covers_inventory=False)
    report, out = sweep_of(store, signal)
    assert out["may_propose"] is False
    assert "not something it can observe" in out["blocked_by"][0]
    assert out["counts"] == {wd.PROPOSED: 0, wd.WATCHING: 0, wd.NOT_JUDGED: 0}


def test_a_collision_puts_the_whole_sweep_out_of_bounds(tmp_path):
    """Which document is which is in doubt for the source, so no absence in it
    can be trusted."""
    store = store_at(tmp_path)
    record(store, "page=p|document_url=https://x/2", misses=2,
           first_missed="2026-08-01T00:00:00Z",
           last_missed="2026-08-03T00:00:00Z")
    store.records["page=p|document_url=https://x/2"]["fields"] = {
        "page": "p", "document_url": "https://x/2"}
    clash = obs("https://x/1")
    store.records[clash.key] = {"fields": {"page": "other", "document_url": "z"},
                                "misses": 0, "signal": "fake"}

    report, out = sweep_of(store, FakeSignal([clash]))
    assert len(report["collisions"]) == 1
    assert out["may_propose"] is False
    assert "in doubt" in out["blocked_by"][0]
    assert out["counts"][wd.PROPOSED] == 0
    assert out["counts"][wd.WATCHING] == 1


def test_a_collided_key_is_never_reported_absent(tmp_path):
    """The sweep DID read a document at that key. Dropping it from `seen` put it
    in `missing`, which is the bucket a withdrawal is proposed from — the same
    trap that makes an empty identity fatal in the orchestrator."""
    store = store_at(tmp_path)
    clash = obs("https://x/1")
    store.records[clash.key] = {"fields": {"page": "other", "document_url": "z"},
                                "misses": 0, "signal": "fake"}
    report, buckets = cs.run_sweep(FakeSignal([clash]), store)
    assert len(report["collisions"]) == 1
    assert [k for k, _ in buckets[cs.MISSING]] == []
    assert store.get(clash.key)["misses"] == 0


def test_a_sweep_that_observed_nothing_proposes_nothing(tmp_path):
    store = store_at(tmp_path)
    record(store, "page=p|document_url=https://x/1", misses=2,
           first_missed="2026-08-01T00:00:00Z",
           last_missed="2026-08-03T00:00:00Z")
    report, out = sweep_of(store, FakeSignal([]))
    assert out["may_propose"] is False
    assert "observed nothing" in "; ".join(out["blocked_by"])
    assert out["counts"][wd.PROPOSED] == 0


def test_a_sweep_whose_count_collapsed_proposes_nothing():
    """SDAIA swung by 70 documents between runs of identical code. A run that
    LOST documents is not a run in which documents were withdrawn."""
    assert "over the 5 a source this size allows" in wd.count_drop(94, 100)
    assert wd.count_drop(95, 100) is None          # exactly the allowance
    assert wd.count_drop(120, 100) is None         # a gain is not a loss
    assert wd.count_drop(5, None) is None          # no baseline yet
    assert wd.count_drop(363, 415) is not None     # the measured SDAIA swing


def test_the_allowance_is_one_document_or_the_percentage_whichever_is_larger():
    """Most sources here hold 12-17 documents, where ONE document is 6-8%. Under
    a flat 5% no small source could ever produce a proposal, and this whole layer
    would be inert on exactly the regulators it was built for."""
    assert wd.count_drop(16, 17) is None           # one of SIMAH's 17 articles
    assert wd.count_drop(15, 17) is not None       # two at once is a parse failure
    assert wd.count_drop(11, 12) is None           # one of GOSI's 12
    assert wd.count_drop(59, 62) is None           # three of MHRSD's 62 is 4.8%
    assert wd.count_drop(58, 62) is not None       # four is 6.5%


def test_the_count_baseline_is_the_previous_sweep_not_this_one(tmp_path):
    """Read before the run is noted, or every sweep compares against itself and
    the gate can never fire."""
    store = store_at(tmp_path)
    signal = FakeSignal([obs(f"https://x/{i}") for i in range(10)])
    first, _ = sweep_of(store, signal)
    assert first["observed_last"] is None
    assert store.last_observed("fake") == 10

    signal.observations = [obs("https://x/0")]
    second, out = sweep_of(store, signal)
    assert second["observed_last"] == 10
    assert out["may_propose"] is False
    assert "9 fewer" in "; ".join(out["blocked_by"])


# --------------------------------------------------------------------------- #
#  the streak's memory                                                        #
# --------------------------------------------------------------------------- #

def test_seeing_a_document_again_clears_both_ends_of_the_streak(tmp_path):
    store = store_at(tmp_path)
    signal = FakeSignal([obs("https://x/1"), obs("https://x/2")])
    cs.run_sweep(signal, store)

    signal.observations = [obs("https://x/1")]
    cs.run_sweep(signal, store)
    gone = obs("https://x/2").key
    assert store.get(gone)["misses"] == 1
    assert store.get(gone)["first_missed"]

    signal.observations = [obs("https://x/1"), obs("https://x/2")]
    cs.run_sweep(signal, store)
    assert store.get(gone)["misses"] == 0
    assert "first_missed" not in store.get(gone)


def test_the_first_miss_is_stamped_once_and_the_last_every_time(tmp_path):
    store = store_at(tmp_path)
    key = record(store, "k")
    store.missed(key)
    first = store.get(key)["first_missed"]
    store.missed(key)
    assert store.get(key)["first_missed"] == first
    assert store.get(key)["misses"] == 2


def test_the_run_log_round_trips_and_is_capped(tmp_path):
    store = store_at(tmp_path)
    for i in range(15):
        store.note_run("fake", i)
    store.save()
    reloaded = ChangeStateStore(store.path).load()
    assert len(reloaded.runs) == 10
    assert reloaded.last_observed("fake") == 14
    assert reloaded.last_observed("other") is None


# --------------------------------------------------------------------------- #
#  the proposal is a proposal                                                  #
# --------------------------------------------------------------------------- #

def test_a_watched_document_says_which_condition_stopped_it(tmp_path):
    """A second miss with no elapsed time between them: the streak is long
    enough and the span is not, and the entry says so rather than only counting."""
    store = store_at(tmp_path)
    key = record(store, "page=p|document_url=https://x/9", misses=1,
                 url="https://x/9")
    store.records[key]["fields"] = {"page": "p", "document_url": "https://x/9"}
    store.note_run("fake", 1)

    report, out = sweep_of(store, FakeSignal([obs("https://x/1")]))
    entry = out[wd.WATCHING][0]
    assert entry["key"] == key
    assert entry["url"] == "https://x/9"
    assert entry["misses"] == 2
    assert entry["first_seen"] == "2026-08-01T00:00:00Z"
    assert "only over 0.0h" in entry["why"]
    assert out[wd.PROPOSED] == []
    assert out["confirmed"] is False
    assert "404s is a library problem" in out["next_step"]
    assert "2 consecutive sweeps" in out["rule"]


def test_a_document_absent_twice_over_a_day_reaches_the_proposal(tmp_path):
    """The whole path: absent, absent again, and enough elapsed time."""
    store = store_at(tmp_path)
    key = record(store, "page=p|document_url=https://x/9", misses=2,
                 first_missed="2026-08-01T00:00:00Z",
                 last_missed="2026-08-02T00:00:00Z")
    store.records[key]["fields"] = {"page": "p", "document_url": "https://x/9"}
    store.note_run("fake", 2)

    signal = FakeSignal([obs("https://x/1"), obs("https://x/2")])
    report, out = sweep_of(store, signal)
    assert out["may_propose"] is True
    assert out["counts"][wd.PROPOSED] == 1
    assert out[wd.PROPOSED][0]["key"] == key
    assert out["confirmed"] is False


# --------------------------------------------------------------------------- #
#  the write — present on both repos, called by nothing                        #
# --------------------------------------------------------------------------- #

def test_withdrawing_sets_a_status_and_keeps_the_row(tmp_path):
    repo = ExcelRepo(tmp_path / "book.xlsx")
    rid = repo._insert_regulation({"title": "a rule", "regulator": "SDAIA",
                                   "source_system": "Laws and Regulations",
                                   "document_url": "https://x/1", "doc_path": [],
                                   "status": "active"})
    repo.insert_regulation_version(rid, content_text="text", status="active")

    repo.mark_regulation_withdrawn(rid, "absent from 2 consecutive sweeps")

    row = repo.get_regulation_by_id(rid)
    assert row["status"] == "withdrawn"
    assert row["title"] == "a rule"            # kept, not deleted
    versions = repo.get_regulation_versions(rid)
    assert [v["status"] for v in versions] == ["inactive", "withdrawn"]
    assert versions[-1]["change_summary"] == "absent from 2 consecutive sweeps"


def test_a_withdrawn_row_leaves_the_gate_and_every_sweep(tmp_path):
    """Which is the point of a status rather than a delete: the completeness gate
    stops offering it as disappeared and no sweep probes it again."""
    repo = ExcelRepo(tmp_path / "book.xlsx")
    rid = repo._insert_regulation({"title": "a rule", "regulator": "SDAIA",
                                   "source_system": "Laws and Regulations",
                                   "document_url": "https://x/1", "doc_path": [],
                                   "status": "active"})
    assert len(repo.find_regulations_by_source("Laws and Regulations",
                                               regulator="SDAIA")) == 1
    repo.mark_regulation_withdrawn(rid, "confirmed by a person")
    assert repo.find_regulations_by_source("Laws and Regulations",
                                           regulator="SDAIA") == []


def test_withdrawing_something_that_is_not_there_raises(tmp_path):
    """A half-applied withdrawal is worse than a failed one."""
    repo = ExcelRepo(tmp_path / "book.xlsx")
    with pytest.raises(ValueError):
        repo.mark_regulation_withdrawn(4242, "nothing to withdraw")


def test_the_report_renders_an_absent_document_without_raising(tmp_path):
    """The `missing` bucket holds bare identity keys, and `getattr(str, "title")`
    finds the string METHOD rather than the default — so building the shortlist
    raised on the first document any sweep reported absent. Four adapters shipped
    over this because no test had ever built a report with an absence in it.
    """
    from dynamic_crawler.cli import sweep as sweep_cli

    signal = FakeSignal([obs("https://x/1"), obs("https://x/2")])
    sweep_cli._run(signal, "TEST/source", state_root=tmp_path)

    signal.observations = [obs("https://x/1")]
    report = sweep_cli._run(signal, "TEST/source", state_root=tmp_path)

    absent = report["shortlist"][cs.MISSING]
    assert absent[0]["key"] == obs("https://x/2").key
    assert absent[0]["title"] == ""            # a key has no title, and says so
    assert "absent from 1 consecutive sweep(s)" in absent[0]["why"]
    assert report["withdrawals"]["counts"][wd.WATCHING] == 1


def test_nothing_in_the_sweep_path_calls_the_write():
    """D7 builds the decision, not the action. The write is on the wrong side of
    the read-only agreement until a senior developer signs it off."""
    import inspect

    from dynamic_crawler import inventory_sweep
    from dynamic_crawler.cli import sweep as sweep_cli
    for module in (wd, cs, inventory_sweep, sweep_cli):
        assert "mark_regulation_withdrawn" not in inspect.getsource(module)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
