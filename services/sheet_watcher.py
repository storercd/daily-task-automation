"""Fetch a publicly readable Google Sheet exported as CSV, for change diffing."""

from __future__ import annotations

import csv
import io
from pathlib import Path

from core.models import RowChange, SheetSource
from services.content_diff import ContentDiffService
from services.http_client import HttpClient

SHEET_EXPORT_URL_TEMPLATE = "https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"


class SheetWatcherService:
    """Detect and describe changes to a publicly readable Google Sheet."""

    def __init__(self, http_client: HttpClient) -> None:
        """Store the shared HTTP client and generic content-diff collaborator."""
        self.http_client = http_client
        self.content_diff = ContentDiffService()

    def fetch_sheet_csv(self, source: SheetSource) -> str:
        """Download the current sheet tab contents as CSV text.

        Args:
            source: Sheet identity to fetch.

        Returns:
            Raw CSV text for the sheet tab.
        """
        url = SHEET_EXPORT_URL_TEMPLATE.format(spreadsheet_id=source.spreadsheet_id, gid=source.gid)
        response = self.http_client.request_with_backoff("GET", url)
        response.raise_for_status()
        return response.text

    def parse_csv_rows(self, csv_text: str) -> list[list[str]]:
        """Parse CSV text into a list of row value lists."""
        return list(csv.reader(io.StringIO(csv_text)))

    def hash_content(self, csv_text: str) -> str:
        """Compute a stable content hash used for alert deduplication."""
        return self.content_diff.hash_content(csv_text)

    def snapshot_path(self, base_dir: Path, source_name: str) -> Path:
        """Build the snapshot file path for one watched source."""
        return self.content_diff.snapshot_path(base_dir, source_name, extension="csv")

    def load_snapshot(self, snapshot_path: Path) -> str | None:
        """Load the previously stored snapshot's raw CSV text, if any."""
        return self.content_diff.load_snapshot(snapshot_path)

    def save_snapshot(self, snapshot_path: Path, csv_text: str) -> None:
        """Persist the current CSV text as the new snapshot."""
        self.content_diff.save_snapshot(snapshot_path, csv_text)

    def diff_rows(self, old_rows: list[list[str]], new_rows: list[list[str]]) -> list[RowChange]:
        """Compute row-level and cell-level changes between two CSV row sets."""
        return self.content_diff.diff_rows(old_rows, new_rows)

    def format_change_summary(self, source_name: str, changes: list[RowChange]) -> str:
        """Format a human-readable summary of row changes for an alert description."""
        return self.content_diff.format_change_summary(source_name, changes, unit="row")
