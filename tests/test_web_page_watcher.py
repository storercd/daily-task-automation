"""Unit tests for WebPageWatcherService: HTML text extraction and line parsing."""

from __future__ import annotations

from services.web_page_watcher import WebPageWatcherService


def build_service() -> WebPageWatcherService:
    return WebPageWatcherService(http_client=None)


def test_extract_visible_text_strips_tags_and_whitespace():
    service = build_service()
    html = """
    <html>
      <head><title>Schedule</title></head>
      <body>
        <h1>Competition Schedule</h1>
        <p>Jan 1 - Regionals</p>
      </body>
    </html>
    """

    text = service.extract_visible_text(html)

    assert text == "Schedule\nCompetition Schedule\nJan 1 - Regionals"


def test_extract_visible_text_skips_script_and_style_content():
    service = build_service()
    html = """
    <html>
      <head><style>body { color: red; }</style></head>
      <body>
        <script>console.log("hello");</script>
        <p>Jan 1 - Regionals</p>
      </body>
    </html>
    """

    text = service.extract_visible_text(html)

    assert text == "Jan 1 - Regionals"


def test_parse_text_lines_wraps_each_line_as_a_single_column_row():
    service = build_service()

    rows = service.parse_text_lines("Jan 1 - Regionals\nFeb 15 - State")

    assert rows == [["Jan 1 - Regionals"], ["Feb 15 - State"]]
