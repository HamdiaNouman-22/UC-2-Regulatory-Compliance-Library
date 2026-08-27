"""The matching cache: decide once, keep the answer.

Requirement matching disagrees with itself on roughly 2-3 of every 39
obligations. Temperature 0, a fixed seed and a pinned provider measurably do NOT
help, because the disagreements are genuine ties between similar internal
requirements rather than sampling noise -- see docs/determinism.md. The only way
stored verdicts stay stable is not to re-ask.

These tests pin the two properties that make that safe:

  * the key must be STABLE against things that are not real changes -- the order
    a SELECT returned the register in, the order the analyzer emitted
    obligations in, an edited description
  * the key must MOVE on things that are real -- a changed obligation, and a
    register that gained a requirement, because a new requirement can
    legitimately turn a `new` verdict into `partially_matched`

Get the first wrong and nothing is ever cached. Get the second wrong and the
library keeps a verdict it should have revised, which is worse.
"""
from processor import analysis_cache as ac

REQS = [{"id": 1, "title": "Monthly reporting"}, {"id": 2, "title": "Board composition"}]
CTRL = [{"id": 10, "title": "Report submitted"}]
KPIS = [{"id": 20, "title": "Days to submit"}]
OBS = ["Every bank shall furnish SAMA by the end of the month.",
       "No person shall be a member of more than one board."]
MODEL = "deepseek/deepseek-v3.2"


def corpus(reqs=REQS, ctrl=CTRL, kpis=KPIS):
    return ac.corpus_fingerprint(reqs, ctrl, kpis)


def match_hash(obs=OBS, reqs=REQS):
    return ac.compute_match_hash(obs, corpus(reqs=reqs), MODEL)


# --------------------------------------------------------------------------- #
#  the fingerprint is stable against non-changes                              #
# --------------------------------------------------------------------------- #

def test_the_same_register_gives_the_same_fingerprint():
    assert corpus() == corpus()


def test_the_order_the_register_came_back_in_does_not_matter():
    """A SELECT with no ORDER BY may return rows in any order; that is not a
    change to the register and must not invalidate every verdict."""
    assert corpus(reqs=list(reversed(REQS))) == corpus()


def test_editing_only_a_description_does_not_invalidate_anything():
    """Descriptions get copy-edited. A typo fix in one of them must not force
    every regulation in the library to be re-matched."""
    a = ac.corpus_fingerprint([{"id": 1, "title": "Monthly reporting", "description": "x"}])
    b = ac.corpus_fingerprint([{"id": 1, "title": "Monthly reporting",
                                "description": "TOTALLY DIFFERENT TEXT"}])
    assert a == b


def test_the_order_obligations_were_extracted_in_does_not_matter():
    """Each obligation is matched independently, so the sequence the analyzer
    happened to emit them in is not part of the input."""
    assert match_hash(obs=list(reversed(OBS))) == match_hash()


# --------------------------------------------------------------------------- #
#  ...and moves on real ones                                                  #
# --------------------------------------------------------------------------- #

def test_a_changed_obligation_changes_the_key():
    assert match_hash(obs=OBS[:1] + ["a different obligation entirely"]) != match_hash()


def test_a_register_that_gained_a_requirement_changes_the_key():
    """The point of the register: a requirement added today can cover an
    obligation that was `new` yesterday, and that verdict should be revisited."""
    assert match_hash(reqs=REQS + [{"id": 3, "title": "New rule"}]) != match_hash()


# --------------------------------------------------------------------------- #
#  the decision                                                               #
# --------------------------------------------------------------------------- #

def test_nothing_stored_yet_means_run():
    should, _, _ = ac.decide_matching({}, OBS, corpus(), MODEL,
                                      has_existing_rows=False, force=False)
    assert should is True


def test_unchanged_input_with_stored_mappings_is_skipped():
    meta = {ac.MATCH_HASH_KEY: match_hash()}
    should, _, why = ac.decide_matching(meta, OBS, corpus(), MODEL,
                                       has_existing_rows=True, force=False)
    assert should is False
    assert "unchanged" in why


def test_a_changed_input_runs_again():
    meta = {ac.MATCH_HASH_KEY: "0" * 64}
    should, _, _ = ac.decide_matching(meta, OBS, corpus(), MODEL,
                                      has_existing_rows=True, force=False)
    assert should is True


def test_force_runs_even_when_the_input_is_unchanged():
    meta = {ac.MATCH_HASH_KEY: match_hash()}
    should, _, _ = ac.decide_matching(meta, OBS, corpus(), MODEL,
                                      has_existing_rows=True, force=True)
    assert should is True


def test_mappings_that_predate_the_cache_are_trusted_not_rerun():
    """Matching ran before this cache existed. Re-running everything once to
    populate a hash would change verdicts that are already stored and reviewed —
    so trust them and record the hash instead."""
    should, _, why = ac.decide_matching({}, OBS, corpus(), MODEL,
                                        has_existing_rows=True, force=False)
    assert should is False
    assert "predate" in why


def test_the_register_growing_reopens_the_verdicts():
    meta = {ac.MATCH_HASH_KEY: match_hash()}
    should, _, why = ac.decide_matching(
        meta, OBS, corpus(reqs=REQS + [{"id": 3, "title": "New"}]), MODEL,
        has_existing_rows=True, force=False)
    assert should is True
    assert "register changed" in why


# --------------------------------------------------------------------------- #
#  confidence                                                                 #
# --------------------------------------------------------------------------- #

def test_a_reply_without_a_confidence_field_counts_as_high():
    """An older model, or a cached reply from before the field existed, must
    behave exactly as it did rather than flooding the review queue."""
    from processor.requirement_matcher import RequirementMatcher
    m = RequirementMatcher.__new__(RequirementMatcher)
    out = m._parse_response('{"match_status":"new","matched_id":null,"explanation":"x"}')
    assert out["confidence"] == "high"


def test_a_low_confidence_verdict_is_carried_through():
    from processor.requirement_matcher import RequirementMatcher
    m = RequirementMatcher.__new__(RequirementMatcher)
    out = m._parse_response(
        '{"match_status":"partially_matched","matched_id":26,'
        '"confidence":"low","explanation":"two candidates"}')
    assert out["confidence"] == "low"
    assert out["matched_id"] == 26


def test_an_unparseable_reply_is_low_confidence_not_a_confident_new():
    """A verdict nobody could read must not be presented as settled."""
    from processor.requirement_matcher import RequirementMatcher
    m = RequirementMatcher.__new__(RequirementMatcher)
    out = m._parse_response("the model apologised instead of answering")
    assert out["match_status"] == "new"
    assert out["confidence"] == "low"
