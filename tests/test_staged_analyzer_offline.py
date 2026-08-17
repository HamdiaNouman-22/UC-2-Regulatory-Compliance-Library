"""Offline unit checks: exercise every non-LLM code path with fake model output."""
import os, sys, json
sys.path.insert(0, r"d:\UC-2-Regulatory-Compliance-Library\UC-2-Regulatory-Compliance-Library")
os.chdir(r"d:\UC-2-Regulatory-Compliance-Library\UC-2-Regulatory-Compliance-Library")
from dotenv import load_dotenv
load_dotenv(r"d:\UC-2-Regulatory-Compliance-Library\UC-2-Regulatory-Compliance-Library\.env", override=True)

from processor.staged_LLM_Analyzer import StagedLLMAnalyzer

a = StagedLLMAnalyzer()
fails = []


def check(name, cond, detail=""):
    print(("  PASS  " if cond else "  FAIL  ") + name + (f"   {detail}" if detail and not cond else ""))
    if not cond:
        fails.append(name)


# ---------------------------------------------------------------- stage 1
s1 = {
    "regulator": "SAMA", "reference": "X", "publication_date": "2020-01-01",
    "requirements": [
        {"requirement_id": "REQ-001", "requirement_title": "Licensing", "obligations": [
            {"obligation_id": "REQ-001-OB-001", "obligation_text": "The bank must obtain a licence.", "source_reference": "Art 1"},
            {"obligation_id": "REQ-001-OB-002", "obligation_text": "the bank   MUST obtain a licence.", "source_reference": "Art 1b"},
            {"obligation_id": "REQ-001-OB-003", "obligation_text": "The bank must notify SAMA and publish a notice.", "source_reference": "Art 2"},
        ]},
        {"requirement_id": "REQ-002", "requirement_title": "Capital", "obligations": [
            {"obligation_id": "REQ-002-OB-001", "obligation_text": "The bank shall maintain capital.", "source_reference": "Art 5"},
        ]},
    ],
}

print("\n[1] exact dedup")
removed = a._dedupe_exact(json.loads(json.dumps(s1)))
check("removes case/whitespace duplicate", removed == 1, f"removed={removed}")

work = json.loads(json.dumps(s1))
a._dedupe_exact(work)
check("keeps 3 obligations total", sum(len(r["obligations"]) for r in work["requirements"]) == 3)

# Regression: dedup must be per requirement group, not global. Two topic groups
# can legitimately contain the same sentence; a global `seen` deleted the second
# group entirely.
_cross = {"requirements": [
    {"requirement_id": "REQ-001", "requirement_title": "Reporting", "obligations": [
        {"obligation_id": "A", "obligation_text": "The bank must notify SAMA.", "source_reference": "x"}]},
    {"requirement_id": "REQ-002", "requirement_title": "Licensing", "obligations": [
        {"obligation_id": "B", "obligation_text": "The bank must notify SAMA.", "source_reference": "y"}]},
]}
_n = a._dedupe_exact(_cross)
check("same text in DIFFERENT groups is kept", _n == 0 and len(_cross["requirements"]) == 2,
      f"removed={_n} groups={[r['requirement_id'] for r in _cross['requirements']]}")

# ---------------------------------------------------------------- stage 2
print("\n[2] stage2 delta rehydration")
index = a._index_stage1(work)
deltas = [
    {"i": "REQ-001-OB-001", "t": "Preventive", "c": "High", "e": ["Policy"], "m": "Inspect licence.", "q": 5, "r": False, "x": "Ongoing_Control"},
    # split into two, with new text
    {"i": "REQ-001-OB-003-A", "t": "Reporting", "c": "Medium", "e": ["Report"], "m": "Check notice.", "q": 4, "r": False, "x": "One_Off_Reporting", "n": "The bank must notify SAMA."},
    {"i": "REQ-001-OB-003-B", "t": "Documentation", "c": "Low", "e": ["Record"], "m": "Check publication.", "q": 4, "r": False, "x": "One_Off_Reporting", "n": "The bank must publish a notice."},
    # out-of-taxonomy values -- must be coerced, not passed through
    {"i": "REQ-002-OB-001", "t": "Corrective", "c": "Critical", "e": ["License"], "m": "Recompute ratio.", "q": 9, "r": False, "x": "Ongoing_Control"},
    {"i": "GHOST-OB-999", "t": "Preventive", "c": "High", "e": [], "m": "x", "q": 3, "r": False, "x": "Ongoing_Control"},
]
s2 = a._rehydrate_stage2(work, index, deltas)
obs = [o for r in s2["requirements"] for o in r["normalized_obligations"]]
by = {o["obligation_id"]: o for o in obs}

check("shape has normalized_obligations", all("normalized_obligations" in r for r in s2["requirements"]))
check("unknown id dropped", "GHOST-OB-999" not in by)
check("4 obligations rehydrated", len(obs) == 4, f"got {len(obs)}")
check("text inherited when 'n' absent",
      by["REQ-001-OB-001"]["obligation_text"] == "The bank must obtain a licence.")
check("split child uses new text",
      by["REQ-001-OB-003-A"]["obligation_text"] == "The bank must notify SAMA.")
check("split child inherits source_reference from parent",
      by["REQ-001-OB-003-A"]["source_reference"] == "Art 2")
check("split children land in parent requirement",
      len(s2["requirements"][0]["normalized_obligations"]) == 3)
check("'Corrective' coerced to Preventive",
      by["REQ-002-OB-001"]["obligation_type"] == "Preventive",
      by["REQ-002-OB-001"]["obligation_type"])
check("'Critical' coerced to Medium", by["REQ-002-OB-001"]["criticality"] == "Medium")
check("'License' evidence coerced to Other", by["REQ-002-OB-001"]["evidence_expected"] == ["Other"])
check("clarity 9 clamped to 3", by["REQ-002-OB-001"]["clarity_score"] == 3)
check("coercion sets needs_manual_review", by["REQ-002-OB-001"]["needs_manual_review"] is True)
check("clean obligation not flagged", by["REQ-001-OB-001"]["needs_manual_review"] is False)

expected_fields = {"obligation_id", "obligation_text", "obligation_type", "criticality",
                   "evidence_expected", "test_method", "clarity_score",
                   "needs_manual_review", "source_reference", "execution_category"}
check("field set identical to old schema", set(by["REQ-001-OB-001"]) == expected_fields,
      str(set(by["REQ-001-OB-001"]) ^ expected_fields))

# ---------------------------------------------------------------- stage 3
print("\n[3] control expansion")
ctrl = a._expand_control({"title": "Licence Check", "obj": "Ensure licence", "desc": "d",
                          "owner": "Compliance", "type": "Corrective", "exec": "Automated Check",
                          "freq": "Daily", "level": "Process", "evidence": "Log",
                          "steps": ["a", "b", "c"], "risk": "Severe"})
check("expands to full field names", ctrl["control_title"] == "Licence Check")
check("bad control_type coerced", ctrl["control_type"] == "Preventive", ctrl["control_type"])
check("bad execution_type coerced", ctrl["execution_type"] == "Manual", ctrl["execution_type"])
check("bad residual risk coerced", ctrl["residual_risk_if_failed"] == "Medium")
check("key_steps preserved", ctrl["key_steps"] == ["a", "b", "c"])
check("null control stays null", a._expand_control(None) is None)

ongoing = a._ongoing_by_requirement(s2)
check("ongoing groups detected", len(ongoing) == 2, str([g["requirement_id"] for g in ongoing]))

# ---------------------------------------------------------------- stage 4
print("\n[4] stage 4 rendering -- fabrication must be impossible")
s3_empty = {"requirements": []}
rows = a._assemble_rows(work, s2, s3_empty, "", 999)
md = a._render_stage4(rows, {"executive_summary": "E", "requirement_overview": "R",
                             "implications": "I"}, "Test Doc", s2, s3_empty)
check("all six sections present", all(f"## {i}." in md for i in range(1, 7)))
check("no fabricated control rows when s3 empty",
      "| Control Title |" not in md)
check("empty s3 with ongoing obligations is flagged as failure",
      "Stage 3 failure" in md)
check("obligation inventory has one row per obligation",
      md.count("| REQ-") >= 4)

s3_full = {"requirements": [{"requirement_id": "REQ-002", "requirement_title": "Capital",
                             "obligations": [{"obligation_id": "REQ-002-OB-001",
                                              "obligation_text": "t",
                                              "execution_category": "Ongoing_Control",
                                              "control": ctrl}]}]}
rows2 = a._assemble_rows(work, s2, s3_full, "", 999)
md2 = a._render_stage4(rows2, {}, "Test Doc", s2, s3_full)
check("control table rendered from real data", "Licence Check" in md2)
check("missing prose degrades gracefully", "_Not generated._" in md2)

# ---------------------------------------------------------------- rows
print("\n[5] row/DB compatibility")
r0 = rows2[0]
expected_row = {"regulation_id", "requirement_id", "requirement_title", "execution_category",
                "criticality", "obligation_type", "analysis_json", "stage1_json",
                "stage2_json", "stage3_json", "stage4_md", "schema_version"}
check("row keys unchanged", set(r0) == expected_row, str(set(r0) ^ expected_row))
check("schema_version still v2", r0["schema_version"] == "v2")
aj = json.loads(rows2[1]["analysis_json"])
check("analysis_json carries controls", len(aj["controls"]) == 1)
check("stage3_json keeps obligations shape",
      "obligations" in json.loads(rows2[1]["stage3_json"]))
check("stage2_json keeps normalized_obligations shape",
      "normalized_obligations" in json.loads(rows2[0]["stage2_json"]))

# Regression: when stage 2 returns nothing (parse failure) the row must fall back
# to stage 1's obligations. `.get(key, default)` did not, because the key exists
# with an empty list -- so rows were written with zero obligations, silently.
_empty_s2 = a._rehydrate_stage2(work, a._index_stage1(work), [])
_fallback_rows = a._assemble_rows(work, _empty_s2, {"requirements": []}, "", 1)
_n_obs = len(json.loads(_fallback_rows[0]["analysis_json"])["obligations"])
check("stage2 parse failure falls back to stage 1 obligations", _n_obs > 0,
      f"got {_n_obs} obligations, expected stage 1's")

# markdown escaping
print("\n[6] misc")
check("pipes escaped in markdown", a._md_escape("a|b") == "a\\|b")
check("parse_json returns {} on garbage", a._parse_json("not json") == {})
check("parse_json strips fences", a._parse_json('```json\n{"a":1}\n```') == {"a": 1})

print("\n" + ("ALL PASSED" if not fails else f"{len(fails)} FAILURE(S): {fails}"))


def test_offline_checks_pass():
    """Exposes the checks above to pytest.

    They run at import, which is fine — they touch no network and no database.
    What is NOT fine is calling sys.exit() at import: pytest imports every test
    module during collection, so a bare sys.exit aborted the ENTIRE run with
    `INTERNALERROR ... SystemExit: 0` and silently took the other ~314 tests
    with it. Hence the __main__ guard below.
    """
    assert not fails, f"{len(fails)} offline check(s) failed: {fails}"


if __name__ == "__main__":
    sys.exit(1 if fails else 0)
