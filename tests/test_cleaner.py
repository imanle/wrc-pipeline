"""Tests for the HTML cleaner.

No I/O at all. The fixture below mirrors the structure confirmed on a live
decision page: chrome outside ``div.content``, and inside it a script, a
letterhead image, 1x1 spacer GIFs, non-breaking spaces in party names, empty
spacing paragraphs, and the parties table with a legitimately empty cell.
"""

from __future__ import annotations

import pytest

from wrc_pipeline.settings import load_settings
from wrc_pipeline.transform.cleaner import (
    CLEANER_VERSION,
    clean_html,
    is_html_extension,
    summarise,
)


@pytest.fixture(scope="module")
def cfg():
    return load_settings()


def _page(content: str = "", title: str = "ADJ-00047352") -> bytes:
    """A decision page with realistic chrome around the content."""
    return f"""<html><head><title>Decisions</title></head><body id="b">
      <header><nav>English | Gaeilge | Home | Search</nav></header>
      <div class="container searchbanner">This website contains decisions...</div>
      <div class="container mb-4"><div class="container"><div class="row">
        <div class="col-sm-3"><a href="/search">Return to Search</a></div>
        <div class="col-sm-9">
          <h1 class="page-title">{title}</h1>
          <div class="content">{content}</div>
        </div>
      </div></div></div>
      <footer>Workplace Relations Commission | Privacy Policy</footer>
    </body></html>""".encode()


DECISION = """
  <p>ADJUDICATION OFFICER Recommendation under Industrial Relations Act 1969</p>
  <p>Parties: Sarah\u00a0McGuirk   v   Rhenus  Logistics Ireland</p>
  <div class="table-responsive"><table>
    <tr><th></th><th>Worker</th><th>Employer</th></tr>
    <tr><td>Anonymised Parties</td><td>Employee</td><td></td></tr>
  </table></div>
  <p>I am recommending that the Respondent pay \u20ac1,500.00 within four weeks.</p>
"""


# --------------------------------------------------------------------------- #
# Selection
# --------------------------------------------------------------------------- #
def test_content_selector_is_reported(cfg):
    """Which selector matched is stored per record: if the primary one starts
    failing and a fallback takes over, that shows up as data."""
    assert clean_html(_page(DECISION), cfg).selector_used == "div.content"


def test_falls_back_to_the_next_selector(cfg):
    """div.content missing, div.col-sm-9 present."""
    page = _page(DECISION).replace(b'class="content"', b'class="somethingelse"')
    assert clean_html(page, cfg).selector_used == "div.col-sm-9"


def test_no_matching_selector_returns_nothing(cfg):
    """Deliberately NOT falling back to <body>: that would emit a document full
    of navigation that passes every other check."""
    result = clean_html(b"<html><body><p>bare page</p></body></html>", cfg)
    assert result.content == b""
    assert result.selector_used is None


def test_title_is_captured_from_outside_the_content_root(cfg):
    """h1.page-title is a sibling of div.content, so it must be read before the
    content is detached."""
    assert clean_html(_page(DECISION), cfg).title == "ADJ-00047352"


# --------------------------------------------------------------------------- #
# Chrome exclusion -- the whitelist's whole purpose
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "chrome",
    [
        "English | Gaeilge",
        "Return to Search",
        "This website contains decisions",
        "Privacy Policy",
        "Workplace Relations Commission |",
    ],
)
def test_chrome_is_excluded(cfg, chrome):
    output = clean_html(_page(DECISION), cfg).content.decode()
    assert chrome not in output


def test_unknown_chrome_is_excluded_by_construction(cfg):
    """The argument for a whitelist over a blacklist: content the site adds later
    is excluded without anyone updating a strip list."""
    page = _page(DECISION).replace(
        b"<footer>", b'<div class="survey-banner">Take our survey!</div><footer>'
    )
    assert "Take our survey" not in clean_html(page, cfg).content.decode()


def test_decision_text_survives(cfg):
    output = clean_html(_page(DECISION), cfg).content.decode()
    assert "ADJUDICATION OFFICER Recommendation" in output
    assert "\u20ac1,500.00" in output


# --------------------------------------------------------------------------- #
# Stripping
# --------------------------------------------------------------------------- #
def test_scripts_and_decorative_images_are_stripped(cfg):
    content = (
        '<script>var x=1;</script><img src="/i/signature_logo.png">'
        '<img src="/i/ecblank.gif" width="1" height="1">' + DECISION
    )
    result = clean_html(_page(content), cfg)
    output = result.content.decode()

    assert "var x=1" not in output
    assert "signature_logo" not in output
    assert "ecblank" not in output
    assert result.stripped["script"] == 1


def test_stripping_is_reported_per_selector(cfg):
    """So "which documents did the cleaner have to work on?" is answerable."""
    result = clean_html(_page("<script>a</script><script>b</script>" + DECISION), cfg)
    assert result.stripped["script"] == 2


# --------------------------------------------------------------------------- #
# Preservation -- why preserve_selectors exists
# --------------------------------------------------------------------------- #
def test_tables_are_kept(cfg):
    """The parties, division and signature tables are substantive content. A
    generic boilerplate remover would discard them as layout."""
    result = clean_html(_page(DECISION), cfg)
    assert result.tables_kept == 1
    assert "Anonymised Parties" in result.content.decode()


def test_empty_table_cells_survive_the_empty_element_pass(cfg):
    """An empty Employer cell is data, and dropping it shifts every column after
    it. This is the specific reason preserve_selectors is applied to the pruning
    pass rather than only to stripping."""
    output = clean_html(_page(DECISION), cfg).content.decode()
    assert output.count("<td>") == 3  # including the empty one
    assert "<th></th>" in output


def test_empty_spacing_paragraphs_are_pruned(cfg):
    """The source uses bare <p></p> and <p><span></span></p> for spacing."""
    content = DECISION + "<p></p><p><span></span></p><div><p>  </p></div>"
    result = clean_html(_page(content), cfg)

    assert result.pruned_empty >= 3
    assert "<p></p>" not in result.content.decode()


def test_a_paragraph_wrapping_only_an_image_is_kept(cfg):
    """Empty of text but not empty of meaning."""
    content = DECISION + '<p><img src="/i/chart.png"></p>'
    assert "chart.png" in clean_html(_page(content), cfg).content.decode()


# --------------------------------------------------------------------------- #
# Whitespace
# --------------------------------------------------------------------------- #
def test_non_breaking_spaces_are_collapsed(cfg):
    """Party names arrive as "Sarah\\xa0McGuirk", so a search for the plain name
    misses. Observed on live records."""
    output = clean_html(_page(DECISION), cfg).content.decode()
    assert "Sarah McGuirk" in output
    assert "\u00a0" not in output


def test_repeated_spaces_are_collapsed(cfg):
    assert "Rhenus Logistics Ireland" in clean_html(_page(DECISION), cfg).content.decode()


# --------------------------------------------------------------------------- #
# Output shape
# --------------------------------------------------------------------------- #
def test_output_is_a_standalone_document(cfg):
    """Not a fragment: without a charset declaration, party names and euro signs
    render as mojibake when the file is opened directly."""
    output = clean_html(_page(DECISION), cfg).content.decode()
    assert output.startswith("<!DOCTYPE html>")
    assert '<meta charset="utf-8">' in output
    assert output.rstrip().endswith("</html>")


def test_the_title_heading_is_reinstated(cfg):
    """It lives outside the content root on the source page but belongs with the
    decision -- the brief's screenshot shows it at the top of the boxed region."""
    assert "<h1>ADJ-00047352</h1>" in clean_html(_page(DECISION), cfg).content.decode()


def test_output_is_smaller_than_the_input(cfg):
    page = _page(DECISION)
    assert len(clean_html(page, cfg).content) < len(page)


def test_text_chars_counts_text_not_markup(cfg):
    result = clean_html(_page(DECISION), cfg)
    assert 0 < result.text_chars < len(result.content)


# --------------------------------------------------------------------------- #
# Thin content
# --------------------------------------------------------------------------- #
def test_thin_extraction_is_flagged(cfg):
    """Below the threshold the extraction probably grabbed the wrong node. The
    runner writes it anyway with a quality_flag -- losing a document because our
    selector underperformed would be worse."""
    assert clean_html(_page("<p>Short.</p>"), cfg).is_thin is True


def test_a_full_decision_is_not_thin(cfg):
    long_decision = DECISION + "<p>" + ("Findings and conclusions. " * 40) + "</p>"
    assert clean_html(_page(long_decision), cfg).is_thin is False


# --------------------------------------------------------------------------- #
# Extension routing (requirement 6)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("ext", [".html", ".htm", ".aspx"])
def test_html_extensions_are_cleaned(cfg, ext):
    assert is_html_extension(ext, cfg) is True


@pytest.mark.parametrize("ext", [".pdf", ".doc", ".docx", ".rtf"])
def test_binary_extensions_are_not_cleaned(cfg, ext):
    """Requirement 6a: stored as they are, no transformation."""
    assert is_html_extension(ext, cfg) is False


def test_missing_extension_is_not_treated_as_html(cfg):
    assert is_html_extension(None, cfg) is False


# --------------------------------------------------------------------------- #
# Encoding and robustness
# --------------------------------------------------------------------------- #
def test_malformed_html_does_not_raise(cfg):
    """The parser is lenient by design: a broken tag in one document must not
    abort a run of a thousand."""
    page = b'<html><body><div class="content"><p>Unclosed <b>bold</body>'
    assert clean_html(page, cfg).selector_used == "div.content"


def test_invalid_bytes_are_replaced_not_fatal(cfg):
    page = b'<html><body><div class="content"><p>caf\xff\xfe</p></div></body></html>'
    assert clean_html(page, cfg).content  # produced something rather than raising


def test_cleaner_version_is_set():
    """Bumping it forces a re-transform of unchanged sources, which is how
    improved cleaning logic takes effect."""
    assert CLEANER_VERSION


def test_summarise_reports_the_useful_numbers(cfg):
    stats = summarise(clean_html(_page(DECISION), cfg))
    assert set(stats) == {
        "selector_used",
        "text_chars",
        "tables_kept",
        "elements_stripped",
        "empty_pruned",
    }
