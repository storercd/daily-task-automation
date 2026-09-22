"""Trello-backed implementation of the ChangeNotifier protocol."""

from __future__ import annotations

from core.models import RowChange, WatchConfig
from services.sheet_watcher import SheetWatcherService
from services.trello import TrelloService

SHEET_DIFF_MARKER_PREFIX = "SHEET-DIFF-KEY:"


class TrelloChangeNotifier:
    """Create a Trello card describing changes detected in a watched source."""

    def __init__(
        self,
        trello_service: TrelloService,
        sheet_watcher_service: SheetWatcherService,
        config: WatchConfig,
        list_id: str,
    ) -> None:
        """Store collaborators needed to build and create a Trello alert card."""
        self.trello_service = trello_service
        self.sheet_watcher_service = sheet_watcher_service
        self.config = config
        self.list_id = list_id

    def notify_change(self, source_name: str, changes: list[RowChange], content_hash: str) -> None:
        """Create a deduplicated Trello alert card for the detected changes.

        Args:
            source_name: Human-readable name of the watched source.
            changes: Row-level changes detected since the previous snapshot.
            content_hash: Stable hash of the new content, used to avoid duplicate alerts.
        """
        marker = f"{SHEET_DIFF_MARKER_PREFIX} {source_name}::{content_hash}"
        description = self.sheet_watcher_service.format_change_summary(source_name, changes)
        self.trello_service.create_alert_card(
            self.config,
            self.list_id,
            f"[CHANGE] {source_name} updated",
            description,
            marker,
        )
