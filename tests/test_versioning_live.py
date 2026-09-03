"""Versioning, tested standalone against the real database.

WHY NOT WAIT FOR A SITE TO CHANGE
    Versioning is logic: "same hash -> nothing, different hash -> archive the old
    row and write a new one". Waiting for a regulator to publish an amendment to
    exercise that is slow and untargeted. This drives the same code path with a
    hash we control.

WHAT IT TOUCHES
    Nothing that already exists. Every row is created under the regulator
    `__VERSIONING_TEST__`, which no crawler produces, so cleanup is exact and
    cannot catch a real document. No pre-existing regulation is read, updated or
    deleted at any point.

    Cleanup runs in a `finally`, so it happens even when an assertion fails --
    the failure case is precisely when you least want test rows left behind.

FIVE PHASES
    1  first sight            -> classified new,       1 version,  it is active
    2  crawled again, same    -> classified unchanged, 0 new versions
    3  content changed        -> classified modified,  2 versions, old inactive
    4  changed again          -> classified modified,  3 versions, exactly 1 active
    5  back to phase-3 text   -> modified, and the version rows do not duplicate
"""
from __future__ import annotations

import os
import sys
import hashlib

ROOT = r"d:\UC-2-Regulatory-Compliance-Library\UC-2-Regulatory-Compliance-Library"
sys.path.insert(0, ROOT)
os.chdir(ROOT)

from dotenv import load_dotenv
load_dotenv(".env", override=True)

from models.models import RegulatoryDocument
from storage.mssql_repo import MSSQLRepository
from orchestrator.orchestrator import Orchestrator

TEST_REGULATOR = "__VERSIONING_TEST__"
SOURCE_SYSTEM = "__VERSIONING_TEST_SOURCE__"
TITLE = "Synthetic Instrument For Versioning Test"
URL = "https://example.invalid/__versioning_test__/instrument.html"

PASS, FAIL = [], []


def check(label, got, want):
    ok = got == want
    (PASS if ok else FAIL).append(label)
    print(f"   [{'PASS' if ok else 'FAIL'}] {label}: got {got!r}, want {want!r}")


def repo_of():
    return MSSQLRepository({
        "server": os.getenv("MSSQL_SERVER"), "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"), "password": os.getenv("MSSQL_PASSWORD"),
        "driver": os.getenv("MSSQL_DRIVER", "{ODBC Driver 17 for SQL Server}")})


class OneDoc:
    """A crawler that returns exactly what the phase wants it to return."""
    source_system = SOURCE_SYSTEM

    def __init__(self, body):
        self.body = body

    def fetch_documents(self):
        # Long enough that text extraction is satisfied by the html and nothing
        # tries to download the (deliberately unreachable) url.
        html = f"<html><body><p>{self.body}</p></body></html>"
        d = RegulatoryDocument(
            regulator=TEST_REGULATOR, source_system=SOURCE_SYSTEM,
            category=SOURCE_SYSTEM, title=TITLE, document_url=URL,
            source_page_url=URL, file_type="HTML", document_html=html,
            doc_path=[TEST_REGULATOR, SOURCE_SYSTEM, TITLE],
        )
        d.content_hash = hashlib.md5(self.body.encode()).hexdigest()[:16]
        return [d]


def counts(c):
    out = {}
    for label, q in [
        ("regulations", "SELECT COUNT(*) FROM regulations"),
        ("versions", "SELECT COUNT(*) FROM regulation_versions"),
        ("folders", "SELECT COUNT(*) FROM compliancecategory"),
        ("run_history", "SELECT COUNT(*) FROM run_history"),
    ]:
        c.execute(q)
        out[label] = c.fetchone()[0]
    return out


def test_rows(c):
    c.execute("SELECT id FROM regulations WHERE regulator = ?", TEST_REGULATOR)
    ids = [r[0] for r in c.fetchall()]
    vers = []
    if ids:
        q = ",".join("?" * len(ids))
        c.execute(f"SELECT version_id, regulation_id, content_hash, status "
                  f"FROM regulation_versions WHERE regulation_id IN ({q}) "
                  f"ORDER BY version_id", *ids)
        vers = c.fetchall()
    return ids, vers


def run_phase(n, body, expect):
    from processor.downloader import Downloader
    o = Orchestrator(crawler=OneDoc(body), repo=repo_of(),
                     downloader=Downloader(), source_name=TEST_REGULATOR,
                     analyse=False)
    rep = o.run_for_regulator(TEST_REGULATOR) or {}
    cls = rep.get("classified", {})
    got = next((k for k in ("new", "modified", "unchanged") if cls.get(k)), "none")
    print(f"\n-- phase {n}: {expect} --")
    check(f"phase {n} classified", got, expect)
    return rep


def main():
    repo = repo_of()
    conn = repo._get_conn()
    c = conn.cursor()

    stray_ids, _ = test_rows(c)
    if stray_ids:
        print(f"refusing to start: {len(stray_ids)} row(s) already exist under "
              f"{TEST_REGULATOR}. Clean them first.")
        return 2

    base = counts(c)
    print("baseline:", base)

    try:
        run_phase(1, "original text of the instrument, article one and two", "new")
        ids, vers = test_rows(c)
        check("phase 1 regulations created", len(ids), 1)
        check("phase 1 version rows", len(vers), 1)
        check("phase 1 active versions", sum(1 for v in vers if v[3] == "active"), 1)

        run_phase(2, "original text of the instrument, article one and two", "unchanged")
        ids, vers = test_rows(c)
        check("phase 2 version rows (unchanged must add none)", len(vers), 1)

        run_phase(3, "AMENDED text, article one, two and a new three", "modified")
        ids, vers = test_rows(c)
        check("phase 3 version rows (retire, do not copy)", len(vers), 2)
        check("phase 3 active versions", sum(1 for v in vers if v[3] == "active"), 1)
        check("phase 3 newest row is the active one",
              max(vers, key=lambda v: v[0])[3], "active")

        run_phase(4, "AMENDED AGAIN, articles one to four", "modified")
        ids, vers = test_rows(c)
        check("phase 4 version rows (retire, do not copy)", len(vers), 3)
        check("phase 4 active versions", sum(1 for v in vers if v[3] == "active"), 1)
        check("phase 4 distinct hashes", len({v[2] for v in vers}), 3)

        run_phase(5, "AMENDED text, article one, two and a new three", "modified")
        ids, vers = test_rows(c)
        check("phase 5 active versions", sum(1 for v in vers if v[3] == "active"), 1)
        check("phase 5 regulations still 1 (no duplicate row)", len(ids), 1)
        check("phase 5 version rows", len(vers), 4)
        # NOT "all hashes distinct". Phase 5 restores phase 3's text, and a
        # document reverting to earlier content is a real change worth its own
        # row -- the hash repeats legitimately. The invariant is that no two
        # CONSECUTIVE versions are identical, which is what a duplicate is.
        seq = [v[2] for v in sorted(vers, key=lambda v: v[0])]
        check("no two consecutive versions are identical",
              sum(1 for a, b in zip(seq, seq[1:]) if a == b), 0)

    finally:
        # ---- CLEANUP, whatever happened above ----
        print("\n-- cleanup --")
        ids, vers = test_rows(c)
        if ids:
            q = ",".join("?" * len(ids))
            c.execute(f"DELETE FROM regulation_versions WHERE regulation_id IN ({q})", *ids)
            print(f"   deleted {c.rowcount} version row(s)")
            c.execute(f"DELETE FROM compliance_analysis WHERE regulation_id IN ({q})", *ids)
            c.execute(f"DELETE FROM regulations WHERE id IN ({q})", *ids)
            print(f"   deleted {c.rowcount} regulation row(s)")
        # Folders. Collect the test root and everything beneath it, level by
        # level, then delete DEEPEST FIRST so no row is ever orphaned from its
        # parent mid-delete. Scoped to a root titled TEST_REGULATOR, which no
        # real crawler creates, so this cannot reach a real folder.
        c.execute("SELECT compliancecategory_id FROM compliancecategory WHERE title = ?", TEST_REGULATOR)
        levels = [[r[0] for r in c.fetchall()]]
        while levels[-1]:
            q = ",".join("?" * len(levels[-1]))
            c.execute(f"SELECT compliancecategory_id FROM compliancecategory WHERE parentid IN ({q})",
                      *levels[-1])
            levels.append([r[0] for r in c.fetchall()])
        removed = 0
        for level in reversed(levels):
            for i in level:
                c.execute("DELETE FROM compliancecategory WHERE compliancecategory_id = ?", i)
                removed += c.rowcount
        print(f"   deleted {removed} folder row(s)")
        c.execute("DELETE FROM run_history WHERE source LIKE ?", f"%{TEST_REGULATOR}%")
        print(f"   deleted {c.rowcount} run_history row(s)")
        conn.commit()

        after = counts(c)
        print("\nbaseline:", base)
        print("after   :", after)
        clean = after == base
        print(("\nCLEAN: every table is back to its baseline."
               if clean else
               "\nNOT CLEAN -- these differ: "
               + str({k: (base[k], after[k]) for k in base if base[k] != after[k]})))
        if not clean:
            FAIL.append("cleanup returned tables to baseline")
        else:
            PASS.append("cleanup returned tables to baseline")

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    for f in FAIL:
        print("   FAILED:", f)
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
