"""Google Calendar iCal fetch and parsing services."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import recurring_ical_events
from icalendar import Calendar

from core.errors import SyncError
from core.models import CalendarEvent
from services.http_client import HttpClient


class GoogleCalendarService:
    """Fetch and parse Google Calendar iCal data."""

    def __init__(self, http_client: HttpClient) -> None:
        """Initialize the service with a shared HTTP client."""
        self.http_client = http_client

    def fetch_calendar(self, ical_url: str) -> Calendar:
        """Fetch and parse an iCal feed."""
        response = self.http_client.request_with_backoff("GET", ical_url)
        if response.status_code == 404 and "calendar.google.com" in ical_url:
            raise SyncError(
                "Google Calendar returned 404 for the iCal URL. "
                "Use the calendar's 'Secret address in iCal format' from "
                "Settings and sharing -> Integrate calendar."
            )
        response.raise_for_status()
        return Calendar.from_ical(response.text)

    def normalize_description(self, raw_description: str | None) -> str:
        """Normalize optional event description text."""
        if not raw_description:
            return ""
        return raw_description.strip()

    def format_occurrence_value(self, value: date | datetime, timezone: ZoneInfo) -> str:
        """Format an occurrence value for event-key generation."""
        if isinstance(value, datetime):
            return self.as_local_datetime(value, timezone).isoformat()
        return value.isoformat()

    def as_local_datetime(self, value: date | datetime, timezone: ZoneInfo) -> datetime:
        """Convert a date-like value into local timezone-aware datetime."""
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone)
            return value.astimezone(timezone)
        return datetime.combine(value, time.min, tzinfo=timezone)

    def build_event_key(self, uid: str, occurrence_value: date | datetime, timezone: ZoneInfo) -> str:
        """Build a unique deduplication key for an event occurrence."""
        return f"{uid}::{self.format_occurrence_value(occurrence_value, timezone)}"

    def parse_events_for_date(
        self,
        calendar: Calendar,
        target_date: date,
        timezone: ZoneInfo,
    ) -> tuple[list[CalendarEvent], list[str]]:
        """Extract occurrences that belong to the target local date."""
        matching_events: list[CalendarEvent] = []
        warnings: list[str] = []
        start_of_day = datetime.combine(target_date, time.min, tzinfo=timezone)
        end_of_day = start_of_day + timedelta(days=1)

        for component in recurring_ical_events.of(calendar).between(start_of_day, end_of_day):
            start_value = component.decoded("DTSTART")
            occurrence_day = (
                self.as_local_datetime(start_value, timezone).date()
                if isinstance(start_value, datetime)
                else start_value
            )
            if occurrence_day != target_date:
                continue

            uid = str(component.get("UID", "")).strip()
            summary = str(component.get("SUMMARY", "")).strip()
            description = self.normalize_description(component.get("DESCRIPTION"))
            is_all_day = isinstance(start_value, date) and not isinstance(start_value, datetime)

            if not uid:
                warnings.append(f"Skipped event with no UID: {summary or '<no summary>'}")
                continue

            if not summary:
                warnings.append(f"Skipped event with no summary for UID {uid}")
                continue

            if not is_all_day:
                warnings.append(f"Event is not all-day and should be corrected: {summary}")

            matching_events.append(
                CalendarEvent(
                    uid=uid,
                    event_key=self.build_event_key(uid, start_value, timezone),
                    summary=summary,
                    description=description,
                    is_all_day=is_all_day,
                )
            )

        return matching_events, warnings
