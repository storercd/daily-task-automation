"""Google Calendar event management for monthly automation tasks."""

from __future__ import annotations

from datetime import datetime

from core.models import MonthlyConfig
from services.http_client import HttpClient


class GoogleCalendarEventService:
    """Create and deduplicate Google Calendar events."""

    def __init__(self, http_client: HttpClient, marker_prefix: str) -> None:
        """Initialize calendar event service with shared HTTP client and marker prefix."""
        self.http_client = http_client
        self.marker_prefix = marker_prefix

    def get_access_token(self, config: MonthlyConfig) -> str:
        """Return an access token, refreshing it when needed."""
        if config.google_oauth_access_token:
            return config.google_oauth_access_token

        token_response = self.http_client.request_with_backoff(
            "POST",
            config.google_oauth_token_url,
            data={
                "client_id": config.google_oauth_client_id,
                "client_secret": config.google_oauth_client_secret,
                "refresh_token": config.google_oauth_refresh_token,
                "grant_type": "refresh_token",
            },
        )
        token_response.raise_for_status()
        token_payload = token_response.json()
        return str(token_payload.get("access_token", "")).strip()

    def load_existing_event_markers(
        self,
        config: MonthlyConfig,
        access_token: str,
        window_start: datetime,
        window_end: datetime,
    ) -> set[str]:
        """Load marker keys from existing events in a date window."""
        existing_markers: set[str] = set()
        page_token: str | None = None

        while True:
            params = {
                "timeMin": window_start.isoformat(),
                "timeMax": window_end.isoformat(),
                "singleEvents": "true",
                "orderBy": "startTime",
                "maxResults": 2500,
            }
            if page_token:
                params["pageToken"] = page_token

            response = self.http_client.request_with_backoff(
                "GET",
                f"https://www.googleapis.com/calendar/v3/calendars/{config.target_calendar_id}/events",
                headers={"Authorization": f"Bearer {access_token}"},
                params=params,
            )
            response.raise_for_status()
            payload = response.json()

            for event in payload.get("items", []):
                description = str(event.get("description", ""))
                for line in description.splitlines():
                    if line.startswith(self.marker_prefix):
                        marker = line.replace(self.marker_prefix, "", 1).strip()
                        if marker:
                            existing_markers.add(marker)

            page_token = payload.get("nextPageToken")
            if not page_token:
                break

        return existing_markers

    def create_event(
        self,
        config: MonthlyConfig,
        access_token: str,
        summary: str,
        start_at: datetime,
        end_at: datetime,
        marker: str,
        timezone_name: str,
    ) -> None:
        """Create a timed Google Calendar event with marker metadata."""
        payload = {
            "summary": summary,
            "description": f"{self.marker_prefix} {marker}",
            "start": {
                "dateTime": start_at.isoformat(),
                "timeZone": timezone_name,
            },
            "end": {
                "dateTime": end_at.isoformat(),
                "timeZone": timezone_name,
            },
        }

        response = self.http_client.request_with_backoff(
            "POST",
            f"https://www.googleapis.com/calendar/v3/calendars/{config.target_calendar_id}/events",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        response.raise_for_status()
