"""Shared dataclasses for configuration and integration payloads."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class Config:
    """Store required runtime settings loaded from environment variables."""

    ical_url: str
    trello_api_key: str
    trello_api_token: str
    trello_board_name: str
    trello_list_name: str


@dataclass
class CalendarEvent:
    """Represent one calendar occurrence that may map to a Trello card."""

    uid: str
    event_key: str
    summary: str
    description: str
    is_all_day: bool


@dataclass
class TrelloCard:
    """Capture card fields needed for due-date triage decisions."""

    card_id: str
    name: str
    due: datetime | None
    due_complete: bool
    list_id: str
