"""Generic snapshot storage and row/line diffing shared by watch-source services."""

from __future__ import annotations

import hashlib
from difflib import SequenceMatcher
from pathlib import Path

from core.models import RowChange


class ContentDiffService:
    """Hash, snapshot, and diff row-shaped content regardless of its original format."""

    def hash_content(self, text: str) -> str:
        """Compute a stable content hash used for alert deduplication."""
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def snapshot_path(self, base_dir: Path, source_name: str, extension: str = "txt") -> Path:
        """Build the snapshot file path for one watched source."""
        safe_name = source_name.strip().lower().replace(" ", "-")
        return base_dir / f"{safe_name}.{extension}"

    def load_snapshot(self, snapshot_path: Path) -> str | None:
        """Load the previously stored snapshot's raw text, if any."""
        if not snapshot_path.exists():
            return None
        return snapshot_path.read_text(encoding="utf-8")

    def save_snapshot(self, snapshot_path: Path, text: str) -> None:
        """Persist the current text as the new snapshot."""
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_text(text, encoding="utf-8")

    def diff_rows(self, old_rows: list[list[str]], new_rows: list[list[str]]) -> list[RowChange]:
        """Compute row-level and cell-level changes between two row sets."""
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

    def format_change_summary(self, source_name: str, changes: list[RowChange], unit: str = "row") -> str:
        """Format a human-readable summary of row changes for an alert description."""
        lines = [f"Detected {len(changes)} {unit} change(s) in '{source_name}'.", ""]
        lines.extend(self._format_change_line(change, unit) for change in changes)
        return "\n".join(lines)

    def _format_change_line(self, change: RowChange, unit: str) -> str:
        """Format one row change as a single description line."""
        if change.kind == "added":
            return f"+ {unit.capitalize()} {change.row_index + 1} added: {self._display_row(change.new_row)}"
        if change.kind == "removed":
            return f"- {unit.capitalize()} {change.row_index + 1} removed: {self._display_row(change.old_row)}"
        cell_summary = ", ".join(self._format_cell_change(change, column_index, old_value, new_value)
            for column_index, old_value, new_value in change.cell_changes
        )
        return f"~ {unit.capitalize()} {change.row_index + 1} modified: {cell_summary}"

    def _format_cell_change(self, change: RowChange, column_index: int, old_value: str, new_value: str) -> str:
        """Format one changed cell, omitting the column label for single-column rows."""
        if change.old_row is not None and len(change.old_row) == 1:
            return f"'{old_value}' -> '{new_value}'"
        return f"col {column_index + 1}: '{old_value}' -> '{new_value}'"

    def _display_row(self, row: list[str] | None) -> str:
        """Render a row for display, unwrapping single-column rows to plain text."""
        if row is None:
            return ""
        return row[0] if len(row) == 1 else str(row)
