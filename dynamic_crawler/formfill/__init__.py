"""formfill — LLM fills a small form; our code does the crawling.

    schema.py    the form, and everything checkable without a network call
    inspect.py   render the page, produce a ~3 KB structural digest
    propose.py   the ONLY module that talks to an LLM
    runner.py    the crawl, driven by the form. Imports no LLM code, ever.
    verify.py    run it N times, judge the spread, gate approval

Read FORMFILL.md in the parent folder first.

Kept separate from `dynamic_crawler/onboarding/` and `dynamic_crawler/auto/`,
which are the older "LLM writes the crawler" attempt, so the two are never
confused for each other.
"""
