"""NOAA tide prediction service for low-tide event generation."""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

from core.errors import SyncError
from core.models import LowTidePrediction
from services.http_client import HttpClient


class NoaaTideService:
    """Fetch and filter NOAA tide predictions."""

    def __init__(self, http_client: HttpClient) -> None:
        """Initialize the service with a shared HTTP client."""
        self.http_client = http_client

    def fetch_negative_low_tides(
        self,
        station_id: str,
        month_start: date,
        month_end: date,
        timezone: ZoneInfo,
    ) -> list[LowTidePrediction]:
        """Return low-tide predictions below 0 feet for a month."""
        response = self.http_client.request_with_backoff(
            "GET",
            "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter",
            params={
                "product": "predictions",
                "application": "daily-task-automation",
                "begin_date": month_start.strftime("%Y%m%d"),
                "end_date": month_end.strftime("%Y%m%d"),
                "datum": "MLLW",
                "station": station_id,
                "time_zone": "lst_ldt",
                "units": "english",
                "interval": "hilo",
                "format": "json",
            },
        )
        response.raise_for_status()
        payload = response.json()

        if "error" in payload:
            error_message = payload["error"].get("message", "unknown NOAA API error")
            raise SyncError(f"NOAA tide API error for station {station_id}: {error_message}")

        predictions = payload.get("predictions")
        if not isinstance(predictions, list):
            raise SyncError("NOAA tide API response missing predictions list")

        negative_low_tides: list[LowTidePrediction] = []
        for prediction in predictions:
            tide_type = str(prediction.get("type", "")).strip().upper()
            if tide_type != "L":
                continue

            height_text = str(prediction.get("v", "")).strip()
            timestamp_text = str(prediction.get("t", "")).strip()
            if not height_text or not timestamp_text:
                continue

            height_feet = float(height_text)
            if height_feet >= 0:
                continue

            local_timestamp = datetime.strptime(timestamp_text, "%Y-%m-%d %H:%M").replace(tzinfo=timezone)
            negative_low_tides.append(
                LowTidePrediction(timestamp=local_timestamp, height_feet=height_feet)
            )

        return negative_low_tides
