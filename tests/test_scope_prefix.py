"""`scope: prefix` must mean the seed's SECTION, not the seed's filename.

The bug these lock down produced 38 ZATCA rows and 47 Ministry of Commerce rows
that were entirely site chrome — Contact Us, Careers, News, Brand Identity — and
not one regulation. `seed_prefix` was `path.rstrip("/")`, so a seed pointing at a
page put that page's own filename in the prefix and no sibling could match it.

MISA, whose seed is a directory, was unaffected and did crawl correctly. That is
the tell: the failure is specific to file-leaf seeds, which is what every
SharePoint regulator gives us.
"""

from urllib.parse import urlparse

import pytest

from generic_crawler.crawler import scope_prefix


@pytest.mark.parametrize("seed_path,expected", [
    # SharePoint: the filename AND the Pages/ store come off.
    ("/en/RulesRegulations/Pages/rules.aspx", "/en/RulesRegulations"),   # ZATCA
    ("/en/Regulations/pages/default.aspx",   "/en/Regulations"),        # MC
    # A directory seed is already correct and must not be touched.
    ("/activities/laws",  "/activities/laws"),                          # MISA
    ("/activities/laws/", "/activities/laws"),
    # Case of the Pages/ segment varies between the two sites above.
    ("/en/Section/PAGES/x.aspx", "/en/Section"),
])
def test_prefix_is_the_section(seed_path, expected):
    assert scope_prefix(seed_path) == expected


def test_never_returns_empty_for_a_real_section():
    """An empty prefix matches every path, silently turning `prefix` into `host`
    — which is the unbounded crawl this whole fix exists to prevent. The last
    real segment is kept rather than stripping down to nothing."""
    assert scope_prefix("/pages/default.aspx") == "/pages"


@pytest.mark.parametrize("path", ["/", ""])
def test_root_seed_has_no_prefix(path):
    """A seed at the host root genuinely has no section to scope to."""
    assert scope_prefix(path) == ""


def test_zatca_chrome_is_out_and_regulations_are_in():
    """The exact URLs from the bad ZATCA run, with the real sections it missed."""
    prefix = scope_prefix("/en/RulesRegulations/Pages/rules.aspx")

    def in_scope(url):
        return urlparse(url).path.startswith(prefix)

    for chrome in [
        "https://zatca.gov.sa/en/ContactUs",
        "https://zatca.gov.sa/en/MediaCenter/News",
        "https://zatca.gov.sa/en/MediaCenter/Authorityidentity",
        "https://zatca.gov.sa/en/AboutUs/Pages/AboutZATCA.aspx",
    ]:
        assert not in_scope(chrome), f"chrome page still in scope: {chrome}"

    for real in [
        "https://zatca.gov.sa/en/RulesRegulations/Taxes/Pages/default.aspx",
        "https://zatca.gov.sa/en/RulesRegulations/Agreements",
        "https://zatca.gov.sa/en/RulesRegulations/InformationExchange/Pages/default.aspx",
    ]:
        assert in_scope(real), f"real section excluded: {real}"


def test_seed_itself_stays_in_scope():
    """Whatever else changes, the page we were pointed at must remain reachable."""
    seed = "/en/RulesRegulations/Pages/rules.aspx"
    assert seed.startswith(scope_prefix(seed))
