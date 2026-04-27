"""Trello API integration service used by automation routines."""

from __future__ import annotations

import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from core.errors import SyncError
from core.models import CalendarEvent, Config, TrelloCard
from services.http_client import HttpClient


class TrelloService:
    """Interact with Trello APIs used by automation routines."""

    def __init__(self, http_client: HttpClient, api_base_url: str, uid_marker_prefix: str) -> None:
        """Initialize Trello client settings and marker formatting behavior."""
        self.http_client = http_client
        self.api_base_url = api_base_url
        self.uid_marker_prefix = uid_marker_prefix

    def request(
        self,
        method: str,
        path: str,
        api_key: str,
        api_token: str,
        allow_retries: bool = True,
        **kwargs,
    ):
        """Call a Trello API endpoint and return decoded JSON."""
        params = kwargs.pop("params", {})
        params.update({"key": api_key, "token": api_token})
        response = self.http_client.request_with_backoff(
            method,
            f"{self.api_base_url}{path}",
            retry_enabled=allow_retries,
            params=params,
            **kwargs,
        )
        response.raise_for_status()
        return response.json()

    def parse_trello_datetime(self, value: str | None, timezone: ZoneInfo) -> datetime | None:
        """Parse Trello RFC3339 datetime and convert to local timezone."""
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone)

    def find_board_id(self, config: Config) -> str:
        """Resolve configured Trello board name to board ID."""
        boards = self.request(
            "GET",
            "/members/me/boards",
            config.trello_api_key,
            config.trello_api_token,
            params={"fields": "name"},
        )
        for board in boards:
            if board["name"] == config.trello_board_name:
                return board["id"]
        raise SyncError(f"Trello board not found: {config.trello_board_name}")

    def find_list_id(self, config: Config, board_id: str) -> str:
        """Resolve configured Trello list name to list ID on a board."""
        lists = self.request(
            "GET",
            f"/boards/{board_id}/lists",
            config.trello_api_key,
            config.trello_api_token,
            params={"fields": "name"},
        )
        for trello_list in lists:
            if trello_list["name"] == config.trello_list_name:
                return trello_list["id"]
        raise SyncError(f"Trello list not found on board {config.trello_board_name}: {config.trello_list_name}")

    def load_open_board_cards(self, config: Config, board_id: str, timezone: ZoneInfo) -> list[TrelloCard]:
        """Load open cards from a board with normalized due-date fields."""
        cards = self.request(
            "GET",
            f"/boards/{board_id}/cards",
            config.trello_api_key,
            config.trello_api_token,
            params={"fields": "name,due,dueComplete,idList,closed"},
        )

        open_cards: list[TrelloCard] = []
        for card in cards:
            if card.get("closed"):
                continue

            open_cards.append(
                TrelloCard(
                    card_id=card["id"],
                    name=card.get("name", ""),
                    due=self.parse_trello_datetime(card.get("due"), timezone),
                    due_complete=bool(card.get("dueComplete")),
                    list_id=card["idList"],
                )
            )

        return open_cards

    def move_card_to_list(self, config: Config, card_id: str, list_id: str) -> None:
        """Move a Trello card to the top of a list."""
        self.request(
            "PUT",
            f"/cards/{card_id}",
            config.trello_api_key,
            config.trello_api_token,
            params={"idList": list_id, "pos": "top"},
        )

    def extract_event_uid(self, card_description: str) -> str | None:
        """Extract the GCAL UID marker from card description text."""
        for line in card_description.splitlines():
            if line.startswith(self.uid_marker_prefix):
                return line.replace(self.uid_marker_prefix, "", 1).strip()
        return None

    def load_existing_event_markers(self, config: Config, list_id: str) -> tuple[set[str], dict[str, list[str]]]:
        """Collect existing event markers from cards already in a list."""
        cards = self.request(
            "GET",
            f"/lists/{list_id}/cards",
            config.trello_api_key,
            config.trello_api_token,
            params={"fields": "name,desc"},
        )

        existing_event_keys: set[str] = set()
        legacy_cards_by_uid: dict[str, list[str]] = {}
        for card in cards:
            marker = self.extract_event_uid(card.get("desc", ""))
            if not marker:
                continue

            if "::" in marker:
                existing_event_keys.add(marker)
                continue

            legacy_cards_by_uid.setdefault(marker, []).append(card["id"])

        return existing_event_keys, legacy_cards_by_uid

    def migrate_legacy_card_marker(
        self,
        config: Config,
        card_id: str,
        legacy_uid: str,
        event: CalendarEvent,
    ) -> None:
        """Upgrade a legacy card marker from UID-only to occurrence marker."""
        card = self.request(
            "GET",
            f"/cards/{card_id}",
            config.trello_api_key,
            config.trello_api_token,
            params={"fields": "desc"},
        )
        existing_description = card.get("desc", "")
        updated_description = existing_description.replace(
            f"{self.uid_marker_prefix} {legacy_uid}",
            f"{self.uid_marker_prefix} {event.event_key}",
            1,
        )
        if updated_description == existing_description:
            return

        self.request(
            "PUT",
            f"/cards/{card_id}",
            config.trello_api_key,
            config.trello_api_token,
            params={"desc": updated_description},
        )

    def build_card_description(self, event: CalendarEvent) -> str:
        """Build Trello card description text for an event."""
        description_parts = []
        if event.description:
            description_parts.append(event.description)
        description_parts.append(f"{self.uid_marker_prefix} {event.event_key}")
        return "\n\n".join(description_parts)

    def card_exists_for_event(self, config: Config, list_id: str, event_key: str) -> bool:
        """Check whether a list already contains an event occurrence marker."""
        existing_event_keys, _ = self.load_existing_event_markers(config, list_id)
        return event_key in existing_event_keys

    def create_card(self, config: Config, list_id: str, event: CalendarEvent) -> bool:
        """Create a Trello card for an event with transient-error recovery."""
        card_params = {
            "idList": list_id,
            "name": event.summary,
            "desc": self.build_card_description(event),
            "pos": "top",
        }

        for attempt_number in range(1, self.http_client.max_attempts + 1):
            try:
                self.request(
                    "POST",
                    "/cards",
                    config.trello_api_key,
                    config.trello_api_token,
                    allow_retries=False,
                    params=card_params,
                )
                return True
            except Exception as error:
                if self.card_exists_for_event(config, list_id, event.event_key):
                    print(
                        f"Recovered existing card after transient create failure: {event.summary}",
                        file=sys.stderr,
                    )
                    return False

                if (
                    not self.http_client.is_retryable_request_error(error)
                    or attempt_number == self.http_client.max_attempts
                ):
                    raise error

                self.http_client.log_retry_attempt(
                    f"Transient failure creating card for {event.summary}: {error}",
                    attempt_number,
                )

        raise SyncError(f"Card creation retries exhausted for event {event.summary}")
