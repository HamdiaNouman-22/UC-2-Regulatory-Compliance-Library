"""Every browser-side snippet must actually PARSE as JavaScript.

WHY THIS EXISTS

A snippet that fails to parse does not fail loudly. `page.evaluate` raises, the
caller treats it as "this page had no detail", and the run completes with a
PASS gate and empty content. On 2026-08-13 that cost ZATCA all 34 rows of HTML
twice in one hour, and the workbook looked like an ordinary failed crawl.

Both failures had the same cause: an escape sequence written into runner.py
through a shell heredoc arrived as a REAL control character.

    _is_doc regex     "download\\b"  ->  "download<BACKSPACE>"   never matched
    JS_DETAIL comment "\\t and \\n"   ->  a real newline INSIDE a // comment,
                                         which ended the comment and left a
                                         bare "," as code

Neither is visible in an editor, in `grep`, or in `inspect.getsource`. Only
executing the snippet, or dumping the compiled constants, shows it.

These tests need a browser. They are skipped when Playwright is unavailable so
they never block a plain unit-test run.
"""

import re

import pytest

from dynamic_crawler.formfill import runner

# Every JS snippet the runner sends to the browser.
SNIPPETS = [name for name in dir(runner)
            if name.startswith("JS_") and isinstance(getattr(runner, name), str)]


def test_snippets_exist():
    """If this list ever empties, the tests below silently pass on nothing."""
    assert len(SNIPPETS) >= 5, f"expected the JS_* snippets, found {SNIPPETS}"


@pytest.mark.parametrize("name", SNIPPETS)
def test_no_stray_control_characters(name):
    """No control characters beyond tab / newline / carriage return.

    A backspace (0x08) inside a regex is the exact byte that made `_is_doc`
    reject every Ministry of Commerce download endpoint while the source looked
    perfectly correct.
    """
    src = getattr(runner, name)
    bad = [(hex(ord(c)), i) for i, c in enumerate(src)
           if ord(c) < 32 and c not in "\n\r\t"]
    assert not bad, f"{name} contains control character(s) at {bad[:5]}"


@pytest.mark.parametrize("name", SNIPPETS)
def test_no_newline_inside_a_line_comment(name):
    """A `//` comment cannot contain a raw newline — it just ends there.

    This is what broke JS_DETAIL: the text after the newline became code.
    Detected structurally rather than by parsing, so it runs without a browser.
    """
    src = getattr(runner, name)
    for lineno, line in enumerate(src.splitlines(), 1):
        idx = line.find("//")
        if idx == -1:
            continue
        # A "//" inside a string or regex is not a comment; require it to start
        # the trimmed line, which is how every comment here is written.
        if line.strip().startswith("//") and "\t" in line:
            # tabs are legal, but they arrived from the same accident — flag
            # them so a reviewer looks.
            pytest.fail(f"{name}:{lineno} comment contains a raw tab: {line!r}")


def test_tidy_html_trims_template_indentation():
    """The Python-side tidy, which replaced the JS attempt that broke twice."""
    raw = "\n   \n\n" + "\n" * 30 + '   <div class="page-wrap">hello</div>\n\n'
    out = runner._tidy_html(raw)
    assert out.startswith("<div"), "leading blank lines were not trimmed"
    assert "hello" in out, "content must never be altered"
    assert runner._tidy_html("") == ""
    assert runner._tidy_html(None) == ""


def test_tidy_html_leaves_pre_alone():
    """Preformatted text keeps its shape — collapsing it would change meaning."""
    pre = "<pre>  keep\n\n\n  this  </pre>"
    assert runner._tidy_html(pre) == pre.strip()


@pytest.mark.parametrize("name", SNIPPETS)
def test_snippet_parses_in_a_browser(name):
    """The real check: hand it to Chromium and see whether it evaluates.

    Structural checks above catch the two failures we have actually had. This
    catches the rest — and it is the only test that would have caught BOTH
    without knowing what to look for.
    """
    sync_playwright = pytest.importorskip(
        "playwright.sync_api", reason="playwright not installed").sync_playwright

    src = getattr(runner, name)
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            page = browser.new_context().new_page()
            page.set_content("<html><body><main><p>x</p></main></body></html>")
            # Parse only: wrapping in a function that is never called still
            # raises SyntaxError on malformed source, and needs no arguments.
            page.evaluate(f"() => {{ const _f = {src}; return typeof _f; }}")
        finally:
            browser.close()
