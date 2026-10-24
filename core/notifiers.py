"""Notifier abstractions so alert delivery can vary independently of change detection."""

from __future__ import annotations

from typing import Protocol

from core.models import RowChange


class ChangeNotifier(Protocol):
    """Deliver an alert describing changes detected in a watched source."""

    def notify_change(
        self,
        source_name: str,
        changes: list[RowChange],
        content_hash: str,
        unit: str = "row",
        source_url: str = "",
    ) -> None:
        """Send an alert for the given source's detected changes.

        Args:
            source_name: Human-readable name of the watched source.
            changes: Row-level changes detected since the previous snapshot.
            content_hash: Stable hash of the new content, used for alert deduplication.
            unit: Noun describing one change entry (e.g. "row" or "line").
            source_url: Link to the watched source, included in the alert when provided.
        """
        ...
