"""Which ARTICLE of a saved page moved, rather than whether the page moved.

A snapshot already carries a whole-page sha256, and for a page that is one
instrument in numbered articles that hash answers the wrong question: a rotating
banner flips it, and an amendment to article 9 says only "the page changed". One
hash per article says which.

It reads a file that is already on disk and makes no request of any kind, which
is what makes it the one signal usable against a host we are blocked from.
"""

from __future__ import annotations

import hashlib
import logging
import re
from html.parser import HTMLParser
from pathlib import Path
from typing import List, Optional

from dynamic_crawler.changesignal import ChangeSignal, Observation, identity_key

logger = logging.getLogger(__name__)

BASIS_ARTICLE = "article-text sha256"

#: The accordion SIMAH's law is rendered in. Kept as class-name fragments rather
#: than a CSS selector so this needs no parser beyond the standard library.
LABEL_CLASS = "accordion-button"
BODY_CLASS = "accordion-body"

_SKIP = ("script", "style", "noscript")
_WS = re.compile(r"\s+")


class _Articles(HTMLParser):
    """Label and body text per accordion item, in document order.

    Text and never markup: `collapse show`, `aria-expanded` and an empty `style`
    sit on whichever article happened to be open when the page was captured, so a
    hash of the HTML moves when nothing in the law has.
    """

    def __init__(self, label_class=LABEL_CLASS, body_class=BODY_CLASS):
        super().__init__(convert_charrefs=True)
        self.label_class = label_class
        self.body_class = body_class
        self.items: List[tuple] = []
        self._label, self._body = [], []
        self._in_label = self._in_body = 0
        self._skip = 0
        self._pending: Optional[str] = None

    def handle_starttag(self, tag, attrs):
        if tag in _SKIP:
            self._skip += 1
            return
        classes = dict(attrs).get("class") or ""
        if self._in_label:
            self._in_label += 1
        elif self.label_class in classes:
            self._in_label, self._label = 1, []
        if self._in_body:
            self._in_body += 1
        elif self.body_class in classes:
            self._in_body, self._body = 1, []

    def handle_endtag(self, tag):
        if tag in _SKIP:
            self._skip = max(0, self._skip - 1)
            return
        if self._in_label:
            self._in_label -= 1
            if not self._in_label:
                self._pending = _WS.sub(" ", "".join(self._label)).strip()
        if self._in_body:
            self._in_body -= 1
            if not self._in_body:
                # A body closing pairs with the label most recently closed; an
                # item with no label is still an item and keeps its position.
                self.items.append((self._pending or "",
                                   _WS.sub(" ", " ".join(self._body)).strip()))
                self._pending = None

    def handle_data(self, data):
        if self._skip:
            return
        if self._in_label:
            self._label.append(data)
        if self._in_body:
            self._body.append(data)


def articles(html: str, label_class=LABEL_CLASS, body_class=BODY_CLASS) -> List[tuple]:
    """`[(label, text)]`, one per accordion item."""
    parser = _Articles(label_class, body_class)
    parser.feed(html or "")
    parser.close()
    return parser.items


def article_hash(text: str) -> str:
    return hashlib.sha256(_WS.sub(" ", text or "").strip().encode("utf-8")).hexdigest()


class SnapshotArticleSweep(ChangeSignal):
    """One saved page, one observation per article. No requests, ever.

    `confirm` is not implemented because there is nothing stronger to ask: the
    token IS the article's text. Everywhere else the token is something a server
    maintains and the hash is the corroboration; here they are the same read.
    """

    # The snapshot is the whole page, so an article that is no longer in it is
    # genuinely gone rather than merely unobserved.
    covers_inventory = True
    confirm_required = False

    def __init__(self, name: str, *, html: Optional[str] = None, store=None,
                 page: Optional[str] = None, allow_stale: bool = False,
                 label_class=LABEL_CLASS, body_class=BODY_CLASS):
        self.source_name = str(name)
        self._html = html
        self.store = store
        self.page = page or self.source_name
        self.allow_stale = bool(allow_stale)
        self.label_class = label_class
        self.body_class = body_class
        self.stats: dict = {}

    @property
    def name(self) -> str:
        return f"snapshot-articles:{self.source_name}"

    def _read(self) -> str:
        if self._html is not None:
            return self._html
        if self.store is None:
            raise ValueError(f"{self.source_name}: give either html or a "
                             f"snapshot store to read it from")
        state = self.store.state()
        if state == "missing":
            raise ValueError(f"{self.source_name}: no snapshot to read")
        if state == "stale" and not self.allow_stale:
            # Replaying a snapshot past its grace period reports `unchanged`
            # forever while the law moves on — a false negative in the one system
            # whose job is noticing change.
            raise ValueError(f"{self.source_name}: the snapshot is stale "
                             f"({self.store.describe()}) — refresh it, or say "
                             f"allow_stale to sweep it knowingly")
        self.stats["snapshot"] = state
        self.stats["captured_at"] = self.store.manifest().get("captured_at", "")
        return self.store.html()

    def sweep(self) -> List[Observation]:
        found = articles(self._read(), self.label_class, self.body_class)
        if not found:
            # A capture that stored a block page, or markup that moved. Either
            # way this signal reports absences, so an empty parse must raise
            # rather than propose the whole instrument for withdrawal.
            raise ValueError(f"{self.source_name}: no articles matched "
                             f".{self.label_class}/.{self.body_class} — refusing "
                             f"to read an unparsed page as an empty one")

        observations, empty = [], 0
        for position, (label, text) in enumerate(found, start=1):
            # The label is the article's own number and is what a reader acts on;
            # its position is the fallback so an unlabelled item still keys.
            article = label or f"item-{position}"
            fields = {"page": self.page, "article": article}
            if not text:
                empty += 1
            observations.append(Observation(
                key=identity_key(fields), fields=fields,
                identity_fields=("page", "article"),
                token=article_hash(text) if text else "",
                basis=BASIS_ARTICLE if text else "article-empty",
                url=str(self.store.manifest().get("url", "")) if self.store else "",
                title=article))

        self.stats.update({
            "articles": len(observations),
            "empty_articles": empty,
            "characters": sum(len(t) for _l, t in found),
        })
        return observations


__all__ = ["SnapshotArticleSweep", "articles", "article_hash", "BASIS_ARTICLE",
           "LABEL_CLASS", "BODY_CLASS"]
