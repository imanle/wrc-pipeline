"""HTML cleaning for the transformation stage.

Pure functions: bytes in, bytes out, no MongoDB, no object storage, no network.
Everything hard about this stage lives here, which means it can be tested against
fixture HTML directly and the runner is left with nothing but plumbing.

Whitelist, not blacklist
------------------------
The brief asks for "only the relevant content of the document (excluding
navigation bars/buttons, headers, footers, etc.)". Two ways to get there:

* strip the chrome you know about -- ``header``, ``footer``, ``nav``;
* select the content you want and discard everything else.

The second is chosen. A blacklist only removes what it has been told about, so
the day the site adds a survey banner, a cookie notice or a "Add To My
Documents" widget, that text silently lands in the curated corpus. Selecting
``div.content`` positively means unknown chrome is excluded by construction --
the failure mode becomes "we extracted too little", which ``min_content_chars``
catches, rather than "we extracted junk", which nothing catches.

Three passes, in order
----------------------
1. **Select** the content root: the first of ``content_selectors`` that matches.
2. **Strip** known noise from inside it (``strip_selectors``): scripts, styles,
   the decorative letterhead, 1x1 spacer GIFs.
3. **Prune** elements left empty, EXCEPT inside ``preserve_selectors``.

Step 3's exception is the whole reason ``preserve_selectors`` exists. The parties
and signature tables carry substantive content -- who the parties were, which
division heard it, who signed it -- and they contain legitimately empty cells. A
naive empty-element pass would hollow them out, and a generic boilerplate remover
would discard them outright as layout.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from bs4 import BeautifulSoup, NavigableString, Tag

from ..settings import Settings, TransformSettings, get_settings

# Bumping this forces every document to be re-transformed on the next run, even
# when its source bytes are unchanged. That is the point: improved cleaning logic
# has to be able to take effect, and the alternative -- manually clearing the
# curated zone -- is a step someone will forget.
CLEANER_VERSION = "1"

# Elements that are meaningful even with no text: a table cell holding a blank in
# the parties grid is data ("Anonymised Parties" with an empty Employer column),
# and dropping it shifts every column after it.
NEVER_EMPTY_PRUNED = {"td", "th", "tr", "table", "thead", "tbody", "br", "hr", "img"}

# &nbsp; and friends. The source uses non-breaking spaces for layout, so party
# names arrive as "Sarah\xa0McGuirk" and a search for "Sarah McGuirk" misses.
_WHITESPACE_RUN = re.compile(r"[\s\u00a0\u2007\u202f]+")


@dataclass
class CleanedDocument:
    """Result of cleaning one HTML document."""

    content: bytes
    title: str | None
    text_chars: int
    selector_used: str | None
    stripped: dict[str, int] = field(default_factory=dict)
    pruned_empty: int = 0
    tables_kept: int = 0

    @property
    def is_thin(self) -> bool:
        """Whether the extraction looks too small to be a real decision.

        Not an error by itself -- see ``quality_flag`` handling in the runner.
        """
        return self.text_chars < 200


def _matches_any(element: Tag, selectors: list[str]) -> bool:
    """Whether *element* itself matches any of *selectors*.

    Uses the element's own soup to run the selector and checks membership, since
    BeautifulSoup has no "does this node match" primitive.
    """
    for selector in selectors:
        try:
            if element in element.parent.select(selector) if element.parent else False:
                return True
        except Exception:  # noqa: BLE001 -- a bad selector must not kill the run
            continue
    return False


def _is_inside(element: Tag, roots: list[Tag]) -> bool:
    """Whether *element* is one of *roots* or nested inside one."""
    for root in roots:
        if element is root:
            return True
        for parent in element.parents:
            if parent is root:
                return True
    return False


def _normalise_whitespace(root: Tag) -> None:
    """Collapse whitespace runs in text nodes, in place.

    Operates on text nodes rather than on the serialised HTML: rewriting the
    whole string would also touch attribute values and tag structure, and the
    goal is only that "Sarah\\xa0McGuirk" becomes searchable as "Sarah McGuirk".
    """
    for text in list(root.find_all(string=True)):
        if isinstance(text, NavigableString):
            collapsed = _WHITESPACE_RUN.sub(" ", str(text))
            if collapsed != str(text):
                text.replace_with(collapsed)


def _prune_empty(root: Tag, preserved: list[Tag]) -> int:
    """Remove elements with no text and no meaningful children.

    Iterates until nothing changes, because emptying a child can leave its parent
    empty -- the source is full of nested ``<p><span></span></p>`` spacing. Skips
    anything inside *preserved*, and anything whose tag is structural.
    """
    removed = 0
    changed = True

    while changed:
        changed = False
        for element in list(root.find_all()):
            if element.name in NEVER_EMPTY_PRUNED:
                continue
            if _is_inside(element, preserved):
                continue
            if element.get_text(strip=True):
                continue
            # Keeps an element that only wraps an image or a rule.
            if element.find(["img", "br", "hr", "table"]):
                continue
            element.decompose()
            removed += 1
            changed = True

    return removed


def clean_html(
    raw: bytes,
    settings: Settings | None = None,
    encoding: str = "utf-8",
) -> CleanedDocument:
    """Extract the decision from one stored HTML page.

    Returns a standalone document, not a fragment: a bare ``<div>`` with no
    charset declaration renders party names wrong when the file is opened
    directly, and the curated zone is meant to be usable on its own.
    """
    cfg: TransformSettings = (settings or get_settings()).transform
    soup = BeautifulSoup(raw.decode(encoding, errors="replace"), "html.parser")

    # --- 1. select ---------------------------------------------------------- #
    content: Tag | None = None
    selector_used: str | None = None
    for selector in cfg.content_selectors:
        found = soup.select_one(selector)
        if found is not None:
            content, selector_used = found, selector
            break

    if content is None:
        # No whitelist selector matched. Returning empty rather than falling back
        # to <body> is deliberate: the fallback would produce a plausible-looking
        # document full of navigation, and the thin-content check would pass it.
        return CleanedDocument(content=b"", title=None, text_chars=0, selector_used=None)

    # The identifier heading sits OUTSIDE div.content, so it has to be collected
    # before the content root is detached from the page.
    title_element = soup.select_one(cfg.title_selector) if cfg.title_selector else None
    title = title_element.get_text(strip=True) if title_element else None

    # --- 2. strip known noise from inside the content ----------------------- #
    stripped: dict[str, int] = {}
    for selector in cfg.strip_selectors:
        matches = content.select(selector)
        if matches:
            stripped[selector] = len(matches)
        for element in matches:
            element.decompose()

    # --- 3. prune what is left empty, except inside preserved subtrees ------ #
    preserved: list[Tag] = []
    for selector in cfg.preserve_selectors:
        preserved.extend(content.select(selector))

    if cfg.normalise_whitespace:
        _normalise_whitespace(content)

    pruned = _prune_empty(content, preserved) if cfg.drop_empty_elements else 0

    # --- assemble ----------------------------------------------------------- #
    text = content.get_text(" ", strip=True)
    document = _wrap(content, title, encoding)

    return CleanedDocument(
        content=document.encode("utf-8"),
        title=title,
        text_chars=len(text),
        selector_used=selector_used,
        stripped=stripped,
        pruned_empty=pruned,
        tables_kept=len(content.select("table")),
    )


def _wrap(content: Tag, title: str | None, encoding: str) -> str:
    """Wrap the extracted content in a minimal, valid HTML document.

    The charset declaration is not decoration: these decisions contain non-ASCII
    party names and euro signs, and a fragment without it renders as mojibake in
    a browser. The title heading is re-added because it lives outside the content
    root on the source page but belongs with the decision.
    """
    heading = f"<h1>{title}</h1>\n" if title else ""
    return (
        "<!DOCTYPE html>\n"
        '<html lang="en">\n<head>\n'
        f'<meta charset="{encoding}">\n'
        + (f"<title>{title}</title>\n" if title else "")
        + "</head>\n<body>\n"
        + heading
        + str(content)
        + "\n</body>\n</html>\n"
    )


def is_html_extension(ext: str | None, settings: Settings | None = None) -> bool:
    """Whether *ext* is one this stage should clean.

    Requirement 6 branches here: HTML pages get parsed and reduced to their
    content, while PDFs and DOCs are stored byte-for-byte with no transformation.
    """
    if not ext:
        return False
    return (settings or get_settings()).scraping.document_extensions.is_html(ext)


def summarise(document: CleanedDocument) -> dict[str, Any]:
    """Cleaning statistics for the structured log and the curated record.

    Stored per document rather than aggregated because the useful question later
    is "which documents did the cleaner struggle with?", and that needs
    per-record numbers.
    """
    return {
        "selector_used": document.selector_used,
        "text_chars": document.text_chars,
        "tables_kept": document.tables_kept,
        "elements_stripped": sum(document.stripped.values()),
        "empty_pruned": document.pruned_empty,
    }
