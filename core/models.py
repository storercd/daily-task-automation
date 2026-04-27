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


@dataclass
class MonthlyConfig:
    """Store monthly low-tide task settings loaded from environment variables."""

    noaa_station_id: str
    target_calendar_id: str
    google_oauth_access_token: str
    google_oauth_client_id: str
    google_oauth_client_secret: str
    google_oauth_refresh_token: str
    google_oauth_token_url: str


@dataclass
class LowTidePrediction:
    """Represent one low-tide prediction used for calendar event creation."""

    timestamp: datetime
    height_feet: float
