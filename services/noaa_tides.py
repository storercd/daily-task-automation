"""NOAA tide prediction service for low-tide event generation."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from astral import Observer
from astral.sun import sun

from core.errors import SyncError
from core.models import LowTidePrediction, LowTideScan
from services.http_client import HttpClient

EVERETT_OBSERVER = Observer(latitude=47.97898, longitude=-122.20208)
DAYLIGHT_BUFFER = timedelta(minutes=30)


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
        """Return low-tide predictions below 0 feet within waking hours for a month."""
        return self.scan_low_tides(station_id, month_start, month_end, timezone).in_window

    def scan_low_tides(
        self,
        station_id: str,
        month_start: date,
        month_end: date,
        timezone: ZoneInfo,
    ) -> LowTideScan:
        """Examine all low tides for a month and report threshold and time-window filtering."""
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

        examined_count = 0
        below_threshold: list[LowTidePrediction] = []
        in_window: list[LowTidePrediction] = []
        for prediction in predictions:
            tide_type = str(prediction.get("type", "")).strip().upper()
            if tide_type != "L":
                continue

            height_text = str(prediction.get("v", "")).strip()
            timestamp_text = str(prediction.get("t", "")).strip()
            if not height_text or not timestamp_text:
                continue

            examined_count += 1
            height_feet = float(height_text)
            if height_feet >= 0:
                continue

            local_timestamp = datetime.strptime(timestamp_text, "%Y-%m-%d %H:%M").replace(tzinfo=timezone)
            tide = LowTidePrediction(timestamp=local_timestamp, height_feet=height_feet)
            below_threshold.append(tide)
            if is_within_waking_hours(local_timestamp, timezone):
                in_window.append(tide)

        return LowTideScan(examined_count, below_threshold, in_window)


def is_within_waking_hours(timestamp: datetime, timezone: ZoneInfo) -> bool:
    """Return whether a timestamp is within 30 minutes of daylight hours."""
    solar_times = sun(EVERETT_OBSERVER, date=timestamp.date(), tzinfo=timezone)
    earliest_time = solar_times["sunrise"] - DAYLIGHT_BUFFER
    latest_time = solar_times["sunset"] + DAYLIGHT_BUFFER
    return earliest_time <= timestamp <= latest_time
