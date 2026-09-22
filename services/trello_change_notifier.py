"""Trello-backed implementation of the ChangeNotifier protocol."""

from __future__ import annotations

from core.models import RowChange, WatchConfig
from services.content_diff import ContentDiffService
from services.trello import TrelloService

WATCH_DIFF_MARKER_PREFIX = "WATCH-DIFF-KEY:"


class TrelloChangeNotifier:
    """Create a Trello card describing changes detected in a watched source."""

    def __init__(
        self,
        trello_service: TrelloService,
        content_diff_service: ContentDiffService,
        config: WatchConfig,
        list_id: str,
    ) -> None:
        """Store collaborators needed to build and create a Trello alert card."""
        self.trello_service = trello_service
        self.content_diff_service = content_diff_service
        self.config = config
        self.list_id = list_id

    def notify_change(self, source_name: str, changes: list[RowChange], content_hash: str, unit: str = "row") -> None:
        """Create a deduplicated Trello alert card for the detected changes.

        Args:
            source_name: Human-readable name of the watched source.
            changes: Row-level changes detected since the previous snapshot.
            content_hash: Stable hash of the new content, used to avoid duplicate alerts.
            unit: Noun describing one change entry (e.g. "row" or "line").
        """
        marker = f"{WATCH_DIFF_MARKER_PREFIX} {source_name}::{content_hash}"
        description = self.content_diff_service.format_change_summary(source_name, changes, unit=unit)
        self.trello_service.create_alert_card(
            self.config,
            self.list_id,
            f"[CHANGE] {source_name} updated",
            description,
            marker,
        )
