"""Fetch a plain web page's visible text, using conditional GETs to avoid re-downloading unchanged content."""

from __future__ import annotations

from html.parser import HTMLParser

from core.models import WebPageSource
from services.http_client import HttpClient

WATCHER_USER_AGENT = "daily-task-automation-watcher/1.0 (+personal automation; low-frequency polling)"
SKIPPED_TAGS = {"script", "style"}


class _VisibleTextExtractor(HTMLParser):
    """Collect non-empty visible text nodes, skipping script/style content."""

    def __init__(self) -> None:
        """Initialize parser state for tracking skipped tags and collected lines."""
        super().__init__()
        self._skip_depth = 0
        self.lines: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Increase skip depth when entering a script/style element."""
        if tag in SKIPPED_TAGS:
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        """Decrease skip depth when leaving a script/style element."""
        if tag in SKIPPED_TAGS and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        """Record non-empty visible text outside of skipped elements."""
        if self._skip_depth:
            return
        text = data.strip()
        if text:
            self.lines.append(text)


class WebPageWatcherService:
    """Detect and describe visible-text changes to a plain web page."""

    def __init__(self, http_client: HttpClient) -> None:
        """Store the shared HTTP client used for page requests."""
        self.http_client = http_client

    def fetch_page_text(
        self,
        source: WebPageSource,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> tuple[str | None, str | None, str | None]:
        """Fetch a page's visible text, skipping the download when unmodified.

        Sends If-None-Match/If-Modified-Since when prior values are known, so a
        server that supports conditional requests can respond 304 Not Modified
        instead of re-sending the full page.

        Args:
            source: Web page identity to fetch.
            etag: Previously observed ETag response header, if any.
            last_modified: Previously observed Last-Modified response header, if any.

        Returns:
            A (text, etag, last_modified) tuple. text is None when the server
            responded 304 Not Modified, meaning nothing changed.
        """
        headers = {"User-Agent": WATCHER_USER_AGENT}
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified

        response = self.http_client.request_with_backoff("GET", source.url, headers=headers)
        if response.status_code == 304:
            return None, etag, last_modified

        response.raise_for_status()
        return (
            self.extract_visible_text(response.text),
            response.headers.get("ETag"),
            response.headers.get("Last-Modified"),
        )

    def extract_visible_text(self, html_text: str) -> str:
        """Strip HTML markup down to newline-separated visible text."""
        extractor = _VisibleTextExtractor()
        extractor.feed(html_text)
        return "\n".join(extractor.lines)

    def parse_text_lines(self, text: str) -> list[list[str]]:
        """Represent each line of text as a single-column row for reuse with row diffing."""
        return [[line] for line in text.splitlines()]
