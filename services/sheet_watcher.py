"""Fetch, snapshot, and diff a publicly readable Google Sheet exported as CSV."""

from __future__ import annotations

import csv
import hashlib
import io
from difflib import SequenceMatcher
from pathlib import Path

from core.models import RowChange, SheetSource
from services.http_client import HttpClient

SHEET_EXPORT_URL_TEMPLATE = "https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"


class SheetWatcherService:
    """Detect and describe changes to a publicly readable Google Sheet."""

    def __init__(self, http_client: HttpClient) -> None:
        """Store the shared HTTP client used for sheet export requests."""
        self.http_client = http_client

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
        return hashlib.sha256(csv_text.encode("utf-8")).hexdigest()

    def snapshot_path(self, base_dir: Path, source_name: str) -> Path:
        """Build the snapshot file path for one watched source."""
        safe_name = source_name.strip().lower().replace(" ", "-")
        return base_dir / f"{safe_name}.csv"

    def load_snapshot(self, snapshot_path: Path) -> str | None:
        """Load the previously stored snapshot's raw CSV text, if any."""
        if not snapshot_path.exists():
            return None
        return snapshot_path.read_text(encoding="utf-8")

    def save_snapshot(self, snapshot_path: Path, csv_text: str) -> None:
        """Persist the current CSV text as the new snapshot."""
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_text(csv_text, encoding="utf-8")

    def diff_rows(self, old_rows: list[list[str]], new_rows: list[list[str]]) -> list[RowChange]:
        """Compute row-level and cell-level changes between two CSV row sets."""
        old_tuples = [tuple(row) for row in old_rows]
        new_tuples = [tuple(row) for row in new_rows]
        matcher = SequenceMatcher(a=old_tuples, b=new_tuples, autojunk=False)
        changes: list[RowChange] = []
        for op, old_start, old_end, new_start, new_end in matcher.get_opcodes():
            if op == "equal":
                continue
            changes.extend(
                self._build_changes_for_block(op, old_rows[old_start:old_end], new_rows[new_start:new_end], new_start)
            )
        return changes

    def _build_changes_for_block(
        self,
        op: str,
        old_block: list[list[str]],
        new_block: list[list[str]],
        new_start: int,
    ) -> list[RowChange]:
        """Build RowChange entries for one diff opcode block."""
        if op == "replace" and len(old_block) == len(new_block):
            return [
                self._build_modified_row(new_start + offset, old_row, new_row)
                for offset, (old_row, new_row) in enumerate(zip(old_block, new_block))
            ]

        changes: list[RowChange] = []
        for offset, old_row in enumerate(old_block):
            changes.append(
                RowChange(kind="removed", row_index=new_start + offset, old_row=old_row, new_row=None, cell_changes=[])
            )
        for offset, new_row in enumerate(new_block):
            changes.append(
                RowChange(kind="added", row_index=new_start + offset, old_row=None, new_row=new_row, cell_changes=[])
            )
        return changes

    def _build_modified_row(self, row_index: int, old_row: list[str], new_row: list[str]) -> RowChange:
        """Build a RowChange describing per-cell differences within one row."""
        column_count = max(len(old_row), len(new_row))
        cell_changes = [
            (column_index, self._cell_value(old_row, column_index), self._cell_value(new_row, column_index))
            for column_index in range(column_count)
            if self._cell_value(old_row, column_index) != self._cell_value(new_row, column_index)
        ]
        return RowChange(
            kind="modified", row_index=row_index, old_row=old_row, new_row=new_row, cell_changes=cell_changes
        )

    def _cell_value(self, row: list[str], column_index: int) -> str:
        """Return a row's value at a column index, or an empty string when absent."""
        return row[column_index] if column_index < len(row) else ""

    def format_change_summary(self, source_name: str, changes: list[RowChange]) -> str:
        """Format a human-readable summary of row changes for an alert description."""
        lines = [f"Detected {len(changes)} row change(s) in '{source_name}'.", ""]
        lines.extend(self._format_change_line(change) for change in changes)
        return "\n".join(lines)

    def _format_change_line(self, change: RowChange) -> str:
        """Format one row change as a single description line."""
        if change.kind == "added":
            return f"+ Row {change.row_index + 1} added: {change.new_row}"
        if change.kind == "removed":
            return f"- Row {change.row_index + 1} removed: {change.old_row}"
        cell_summary = ", ".join(
            f"col {column_index + 1}: '{old_value}' -> '{new_value}'"
            for column_index, old_value, new_value in change.cell_changes
        )
        return f"~ Row {change.row_index + 1} modified: {cell_summary}"
