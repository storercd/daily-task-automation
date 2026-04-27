"""Synchronize Google Calendar all-day tasks into a Trello triage list.

The script creates missing cards for today's all-day events, migrates legacy UID
markers, and moves due incomplete cards into the configured triage list.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from icalendar import Calendar
from tzlocal import get_localzone

from core.errors import SyncError
from core.models import CalendarEvent, Config, LowTidePrediction, MonthlyConfig, TrelloCard
from services.google_calendar import GoogleCalendarService
from services.google_calendar_events import GoogleCalendarEventService
from services.http_client import HttpClient
from services.noaa_tides import NoaaTideService
from services.trello import TrelloService

TRELLO_API_BASE_URL = "https://api.trello.com/1"
UID_MARKER_PREFIX = "GCAL-UID:"
LOW_TIDE_MARKER_PREFIX = "LOW-TIDE-KEY:"
RUN_CALENDAR_SYNC = True
RUN_DUE_CARD_TRIAGE = True
MAX_REQUEST_ATTEMPTS = 3
INITIAL_RETRY_DELAY_SECONDS = 2
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
DATE_STATUS_FILE_PATH = Path("logs") / "processed_dates.json"
DEFAULT_NOAA_STATION_ID = "9447659"
DEFAULT_GOOGLE_OAUTH_TOKEN_URL = "https://oauth2.googleapis.com/token"
MONTHLY_EVENT_DURATION = timedelta(hours=1)

HTTP_CLIENT = HttpClient(
    max_attempts=MAX_REQUEST_ATTEMPTS,
    initial_retry_delay_seconds=INITIAL_RETRY_DELAY_SECONDS,
    retryable_status_codes=RETRYABLE_STATUS_CODES,
)
GOOGLE_CALENDAR_SERVICE = GoogleCalendarService(HTTP_CLIENT)
GOOGLE_CALENDAR_EVENT_SERVICE = GoogleCalendarEventService(HTTP_CLIENT, LOW_TIDE_MARKER_PREFIX)
NOAA_TIDE_SERVICE = NoaaTideService(HTTP_CLIENT)
TRELLO_SERVICE = TrelloService(HTTP_CLIENT, TRELLO_API_BASE_URL, UID_MARKER_PREFIX)


def ensure_parent_directory(file_path: str) -> None:
    """Ensure the target file's parent directory exists.

    Args:
        file_path: Path to a file whose parent directory should be created.
    """
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)


def load_processed_date_statuses(file_path: str) -> dict[str, dict[str, str | int]]:
    """Load per-day processing statuses from disk.

    Invalid keys or values are ignored so minor manual edits do not break runs.

    Args:
        file_path: JSON file path that stores per-day status metadata.

    Returns:
        A mapping keyed by ISO date with status metadata dictionaries.

    Raises:
        SyncError: If the file contains invalid JSON or a non-object root.
    """
    path = Path(file_path)
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as file_handle:
        try:
            loaded_data = json.load(file_handle)
        except json.JSONDecodeError as error:
            raise SyncError(f"Invalid processed-date status file: {file_path}") from error

    if not isinstance(loaded_data, dict):
        raise SyncError(f"Processed-date status file must contain a JSON object: {file_path}")

    normalized_statuses: dict[str, dict[str, str | int]] = {}
    for key, value in loaded_data.items():
        if not isinstance(key, str) or not isinstance(value, dict):
            continue
        normalized_statuses[key] = value

    return normalized_statuses


def save_processed_date_statuses(file_path: str, statuses: dict[str, dict[str, str | int]]) -> None:
    """Persist per-day processing statuses as sorted JSON.

    Args:
        file_path: Destination JSON file path.
        statuses: Status data keyed by ISO date.
    """
    ensure_parent_directory(file_path)
    with Path(file_path).open("w", encoding="utf-8") as file_handle:
        json.dump(statuses, file_handle, indent=2, sort_keys=True)
        file_handle.write("\n")


def parse_iso_date(value: str) -> date | None:
    """Parse an ISO date string.

    Args:
        value: Date string expected in YYYY-MM-DD format.

    Returns:
        Parsed date when valid; otherwise None.
    """
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def build_dates_to_process(
    current_date: date,
    statuses: dict[str, dict[str, str | int]],
) -> list[date]:
    """Compute dates that should be processed in this run.

    Args:
        current_date: Local current date for the run.
        statuses: Existing status map keyed by ISO date.

    Returns:
        Sorted set of dates including failed days, missing backfill days, and
        current_date when it has not already succeeded.
    """
    valid_recorded_dates = sorted(
        parsed_date
        for date_key in statuses
        for parsed_date in [parse_iso_date(date_key)]
        if parsed_date is not None and parsed_date <= current_date
    )

    failed_dates = sorted(
        parsed_date
        for date_key, status in statuses.items()
        for parsed_date in [parse_iso_date(date_key)]
        if parsed_date is not None
        and parsed_date < current_date
        and status.get("status") != "success"
    )

    backfill_dates: list[date] = []
    if valid_recorded_dates:
        next_unrecorded_day = valid_recorded_dates[-1] + timedelta(days=1)
        current_day = next_unrecorded_day
        while current_day <= current_date:
            backfill_dates.append(current_day)
            current_day += timedelta(days=1)

    all_dates: set[date] = set()
    current_date_key = current_date.isoformat()
    current_date_status = statuses.get(current_date_key, {})
    if current_date_status.get("status") != "success":
        all_dates.add(current_date)

    all_dates.update(backfill_dates)
    all_dates.update(failed_dates)

    return sorted(all_dates)


def load_config() -> Config:
    """Load and validate required environment variables.

    Returns:
        Config populated from environment variables and .env values.

    Raises:
        SyncError: If one or more required environment variables are missing.
    """
    load_dotenv()

    config = Config(
        ical_url=os.getenv("ICAL_URL", "").strip(),
        trello_api_key=os.getenv("TRELLO_API_KEY", "").strip(),
        trello_api_token=os.getenv("TRELLO_API_TOKEN", "").strip(),
        trello_board_name=os.getenv("TRELLO_BOARD_NAME", "").strip(),
        trello_list_name=os.getenv("TRELLO_LIST_NAME", "").strip(),
    )

    missing_values = [
        name
        for name, value in (
            ("ICAL_URL", config.ical_url),
            ("TRELLO_API_KEY", config.trello_api_key),
            ("TRELLO_API_TOKEN", config.trello_api_token),
            ("TRELLO_BOARD_NAME", config.trello_board_name),
            ("TRELLO_LIST_NAME", config.trello_list_name),
        )
        if not value
    ]
    if missing_values:
        raise SyncError(f"Missing required environment variables: {', '.join(missing_values)}")

    return config


def load_monthly_config() -> MonthlyConfig:
    """Load and validate required environment variables for monthly tasks."""
    load_dotenv()

    config = MonthlyConfig(
        noaa_station_id=os.getenv("NOAA_STATION_ID", DEFAULT_NOAA_STATION_ID).strip() or DEFAULT_NOAA_STATION_ID,
        target_calendar_id=os.getenv("LOW_TIDE_CALENDAR_ID", "").strip(),
        google_oauth_access_token=os.getenv("GOOGLE_OAUTH_ACCESS_TOKEN", "").strip(),
        google_oauth_client_id=os.getenv("GOOGLE_OAUTH_CLIENT_ID", "").strip(),
        google_oauth_client_secret=os.getenv("GOOGLE_OAUTH_CLIENT_SECRET", "").strip(),
        google_oauth_refresh_token=os.getenv("GOOGLE_OAUTH_REFRESH_TOKEN", "").strip(),
        google_oauth_token_url=(
            os.getenv("GOOGLE_OAUTH_TOKEN_URL", DEFAULT_GOOGLE_OAUTH_TOKEN_URL).strip()
            or DEFAULT_GOOGLE_OAUTH_TOKEN_URL
        ),
    )

    missing_values: list[str] = []
    if not config.target_calendar_id:
        missing_values.append("LOW_TIDE_CALENDAR_ID")

    requires_oauth_refresh = not config.google_oauth_access_token
    if requires_oauth_refresh and not config.google_oauth_client_id:
        missing_values.append("GOOGLE_OAUTH_CLIENT_ID")
    if requires_oauth_refresh and not config.google_oauth_client_secret:
        missing_values.append("GOOGLE_OAUTH_CLIENT_SECRET")
    if requires_oauth_refresh and not config.google_oauth_refresh_token:
        missing_values.append("GOOGLE_OAUTH_REFRESH_TOKEN")

    if missing_values:
        raise SyncError(f"Missing required monthly environment variables: {', '.join(missing_values)}")

    return config


def build_month_bounds(target_date: date) -> tuple[date, date]:
    """Return first and last dates for target_date's month."""
    month_start = target_date.replace(day=1)
    if month_start.month == 12:
        next_month_start = date(month_start.year + 1, 1, 1)
    else:
        next_month_start = date(month_start.year, month_start.month + 1, 1)
    month_end = next_month_start - timedelta(days=1)
    return month_start, month_end


def build_low_tide_marker(station_id: str, prediction: LowTidePrediction) -> str:
    """Build deduplication marker for one low-tide prediction."""
    return f"{station_id}::{prediction.timestamp.isoformat()}"


def get_local_timezone() -> ZoneInfo:
    """Return the system local timezone as ZoneInfo.

    Returns:
        Local timezone normalized to ZoneInfo.
    """
    local_zone = get_localzone()
    if isinstance(local_zone, ZoneInfo):
        return local_zone
    return ZoneInfo(str(local_zone))


def get_retry_delay_seconds(attempt_number: int) -> int:
    """Compute exponential backoff delay for a retry attempt."""
    return HTTP_CLIENT.get_retry_delay_seconds(attempt_number)


def is_retryable_http_error(error: requests.HTTPError) -> bool:
    """Determine whether an HTTP error is retryable."""
    return HTTP_CLIENT.is_retryable_http_error(error)


def is_retryable_request_error(error: Exception) -> bool:
    """Determine whether a request exception should be retried."""
    return HTTP_CLIENT.is_retryable_request_error(error)


def log_retry_attempt(message: str, attempt_number: int) -> None:
    """Log retry details and sleep using exponential backoff."""
    HTTP_CLIENT.log_retry_attempt(message, attempt_number)


def request_with_backoff(
    method: str,
    url: str,
    retry_enabled: bool = True,
    **kwargs,
) -> requests.Response:
    """Issue an HTTP request with retry and backoff."""
    return HTTP_CLIENT.request_with_backoff(method, url, retry_enabled=retry_enabled, **kwargs)


def fetch_calendar(ical_url: str) -> Calendar:
    """Fetch and parse an iCal feed."""
    return GOOGLE_CALENDAR_SERVICE.fetch_calendar(ical_url)


def normalize_description(raw_description: str | None) -> str:
    """Normalize optional event description text."""
    return GOOGLE_CALENDAR_SERVICE.normalize_description(raw_description)


def format_occurrence_value(value: date | datetime, timezone: ZoneInfo) -> str:
    """Format an occurrence value for event-key generation."""
    return GOOGLE_CALENDAR_SERVICE.format_occurrence_value(value, timezone)


def as_local_datetime(value: date | datetime, timezone: ZoneInfo) -> datetime:
    """Convert a date-like value into local timezone-aware datetime."""
    return GOOGLE_CALENDAR_SERVICE.as_local_datetime(value, timezone)


def build_event_key(uid: str, occurrence_value: date | datetime, timezone: ZoneInfo) -> str:
    """Build a unique deduplication key for an event occurrence."""
    return GOOGLE_CALENDAR_SERVICE.build_event_key(uid, occurrence_value, timezone)


def parse_events_for_today(
    calendar: Calendar,
    target_date: date,
    timezone: ZoneInfo,
) -> tuple[list[CalendarEvent], list[str]]:
    """Extract occurrences that belong to the target local date."""
    return GOOGLE_CALENDAR_SERVICE.parse_events_for_date(calendar, target_date, timezone)


def trello_request(
    method: str,
    path: str,
    api_key: str,
    api_token: str,
    allow_retries: bool = True,
    **kwargs,
):
    """Call a Trello API endpoint and return the decoded JSON body."""
    return TRELLO_SERVICE.request(
        method,
        path,
        api_key,
        api_token,
        allow_retries=allow_retries,
        **kwargs,
    )


def parse_trello_datetime(value: str | None, timezone: ZoneInfo) -> datetime | None:
    """Parse Trello RFC3339 datetime and convert to local timezone."""
    return TRELLO_SERVICE.parse_trello_datetime(value, timezone)


def find_board_id(config: Config) -> str:
    """Resolve configured Trello board name to board ID."""
    return TRELLO_SERVICE.find_board_id(config)


def find_list_id(config: Config, board_id: str) -> str:
    """Resolve configured Trello list name to list ID on a board."""
    return TRELLO_SERVICE.find_list_id(config, board_id)


def load_open_board_cards(config: Config, board_id: str, timezone: ZoneInfo) -> list[TrelloCard]:
    """Load open cards from a board with normalized due-date fields."""
    return TRELLO_SERVICE.load_open_board_cards(config, board_id, timezone)


def move_card_to_list(config: Config, card_id: str, list_id: str) -> None:
    """Move a Trello card to the top of a list."""
    TRELLO_SERVICE.move_card_to_list(config, card_id, list_id)


def extract_event_uid(card_description: str) -> str | None:
    """Extract the GCAL UID marker from card description text."""
    return TRELLO_SERVICE.extract_event_uid(card_description)


def load_existing_event_markers(config: Config, list_id: str) -> tuple[set[str], dict[str, list[str]]]:
    """Collect existing event markers from cards already in a list."""
    return TRELLO_SERVICE.load_existing_event_markers(config, list_id)


def migrate_legacy_card_marker(config: Config, card_id: str, legacy_uid: str, event: CalendarEvent) -> None:
    """Upgrade a legacy card marker from UID-only to occurrence marker."""
    TRELLO_SERVICE.migrate_legacy_card_marker(config, card_id, legacy_uid, event)


def build_card_description(event: CalendarEvent) -> str:
    """Build Trello card description text for an event."""
    return TRELLO_SERVICE.build_card_description(event)


def card_exists_for_event(config: Config, list_id: str, event_key: str) -> bool:
    """Check whether a list already contains an event occurrence marker."""
    return TRELLO_SERVICE.card_exists_for_event(config, list_id, event_key)


def create_card(config: Config, list_id: str, event: CalendarEvent) -> bool:
    """Create a Trello card for an event with transient-error recovery."""
    return TRELLO_SERVICE.create_card(config, list_id, event)


def run_calendar_sync(
    config: Config,
    timezone: ZoneInfo,
    processing_day: date,
    triage_list_id: str,
) -> None:
    """Sync today's calendar events into the triage list.

    Args:
        config: Runtime configuration with calendar and Trello settings.
        timezone: Local timezone for date-sensitive event parsing.
        processing_day: Date whose events should be synchronized.
        triage_list_id: Destination Trello list ID.
    """
    calendar = fetch_calendar(config.ical_url)
    events, warnings = parse_events_for_today(calendar, processing_day, timezone)
    existing_event_keys, legacy_cards_by_uid = load_existing_event_markers(config, triage_list_id)

    created_count = 0
    skipped_count = 0
    migrated_count = 0
    for event in events:
        if event.event_key in existing_event_keys:
            skipped_count += 1
            print(f"Skipped existing card for event: {event.summary}")
            continue

        legacy_card_ids = legacy_cards_by_uid.get(event.uid, [])
        if legacy_card_ids:
            migrate_legacy_card_marker(config, legacy_card_ids[0], event.uid, event)
            existing_event_keys.add(event.event_key)
            migrated_count += 1
            skipped_count += 1
            print(f"Migrated existing legacy card for event: {event.summary}")
            continue

        created_new_card = create_card(config, triage_list_id, event)
        existing_event_keys.add(event.event_key)
        if created_new_card:
            created_count += 1
            print(f"Created card: {event.summary}")
            continue

        skipped_count += 1
        print(f"Skipped existing card after retry recovery: {event.summary}")

    print(
        f"Calendar sync processed {len(events)} event(s) for {processing_day.isoformat()}: "
        f"{created_count} created, {skipped_count} skipped, {migrated_count} migrated."
    )
    for warning in warnings:
        print(f"WARNING: {warning}")


def run_due_card_triage(
    config: Config,
    timezone: ZoneInfo,
    processing_day: date,
    board_id: str,
    triage_list_id: str,
) -> None:
    """Move due and incomplete cards into the triage list.

    Args:
        config: Runtime configuration with Trello credentials.
        timezone: Local timezone for due-date comparison.
        processing_day: Current local date cutoff for due triage.
        board_id: Board ID to scan for open cards.
        triage_list_id: Destination list for eligible cards.
    """
    cards = load_open_board_cards(config, board_id, timezone)

    moved_count = 0
    already_in_triage_count = 0
    eligible_count = 0
    for card in cards:
        if card.due is None or card.due_complete:
            continue

        if card.due.date() > processing_day:
            continue

        eligible_count += 1
        if card.list_id == triage_list_id:
            already_in_triage_count += 1
            print(f"Skipped due card already in triage: {card.name}")
            continue

        move_card_to_list(config, card.card_id, triage_list_id)
        moved_count += 1
        print(f"Moved due card to triage: {card.name}")

    print(
        f"Due-card triage processed {eligible_count} eligible card(s) for {processing_day.isoformat()}: "
        f"{moved_count} moved, {already_in_triage_count} already in triage."
    )


def run_daily() -> int:
    """Run daily automation and persist per-date outcomes.

    Returns:
        Zero when all required dates are processed successfully.

    Raises:
        SyncError: If one or more dates fail during processing.
    """
    config = load_config()
    timezone = get_local_timezone()
    current_date = datetime.now(timezone).date()
    board_id = find_board_id(config)
    triage_list_id = find_list_id(config, board_id)

    status_file_exists = Path(DATE_STATUS_FILE_PATH).exists()
    processed_date_statuses = load_processed_date_statuses(DATE_STATUS_FILE_PATH)

    if not status_file_exists:
        save_processed_date_statuses(DATE_STATUS_FILE_PATH, processed_date_statuses)

    dates_to_process = (
        [current_date]
        if not status_file_exists
        else build_dates_to_process(current_date, processed_date_statuses)
    )
    if len(dates_to_process) > 1:
        print(f"Backfill required for {len(dates_to_process) - 1} date(s) before today.")

    failed_dates: list[str] = []
    for processing_day in dates_to_process:
        processing_day_key = processing_day.isoformat()
        previous_status = processed_date_statuses.get(processing_day_key, {})
        previous_attempt_count = previous_status.get("attempt_count", 0)
        attempt_count = previous_attempt_count + 1 if isinstance(previous_attempt_count, int) else 1

        print(f"Processing date: {processing_day_key}")
        try:
            if RUN_CALENDAR_SYNC:
                run_calendar_sync(config, timezone, processing_day, triage_list_id)

            if RUN_DUE_CARD_TRIAGE:
                run_due_card_triage(config, timezone, processing_day, board_id, triage_list_id)

            processed_date_statuses[processing_day_key] = {
                "status": "success",
                "attempt_count": attempt_count,
                "last_run_at": datetime.now(timezone).isoformat(),
                "last_error": "",
            }
        except Exception as error:
            processed_date_statuses[processing_day_key] = {
                "status": "failed",
                "attempt_count": attempt_count,
                "last_run_at": datetime.now(timezone).isoformat(),
                "last_error": str(error),
            }
            failed_dates.append(processing_day_key)
            print(f"ERROR processing date {processing_day_key}: {error}", file=sys.stderr)
        finally:
            save_processed_date_statuses(DATE_STATUS_FILE_PATH, processed_date_statuses)

    if failed_dates:
        raise SyncError(f"Daily automation failed for date(s): {', '.join(failed_dates)}")

    return 0


def run_monthly(target_month_date: date | None = None) -> int:
    """Run monthly automation tasks.

    Args:
        target_month_date: Optional date specifying which month to process.
                          Defaults to the next month if None.

    Returns:
        Zero when all monthly tasks complete successfully.

    Raises:
        SyncError: If one or more monthly tasks fail.
    """
    config = load_monthly_config()
    timezone = get_local_timezone()
    if target_month_date is None:
        current_date = datetime.now(timezone).date()
        # Process the next month (e.g., process May during April)
        _, current_month_end = build_month_bounds(current_date)
        target_month_date = current_month_end + timedelta(days=1)
    month_start, month_end = build_month_bounds(target_month_date)

    predictions = NOAA_TIDE_SERVICE.fetch_negative_low_tides(
        config.noaa_station_id,
        month_start,
        month_end,
        timezone,
    )
    if not predictions:
        print(
            f"No negative low tides found for {month_start.strftime('%Y-%m')} "
            f"at station {config.noaa_station_id}."
        )
        return 0

    access_token = GOOGLE_CALENDAR_EVENT_SERVICE.get_access_token(config)
    if not access_token:
        raise SyncError("Could not obtain a Google OAuth access token for monthly tasks.")

    window_start = datetime.combine(month_start, time.min, tzinfo=timezone)
    window_end = datetime.combine(month_end + timedelta(days=1), time.min, tzinfo=timezone)
    existing_markers = GOOGLE_CALENDAR_EVENT_SERVICE.load_existing_event_markers(
        config,
        access_token,
        window_start,
        window_end,
    )

    created_count = 0
    skipped_count = 0
    for prediction in predictions:
        marker = build_low_tide_marker(config.noaa_station_id, prediction)
        if marker in existing_markers:
            skipped_count += 1
            print(f"Skipped existing low-tide event: {prediction.timestamp.isoformat()}")
            continue

        summary = f"Low Tide: {prediction.height_feet:.2f}"
        GOOGLE_CALENDAR_EVENT_SERVICE.create_event(
            config,
            access_token,
            summary,
            prediction.timestamp,
            prediction.timestamp + MONTHLY_EVENT_DURATION,
            marker,
            str(timezone),
        )
        existing_markers.add(marker)
        created_count += 1
        print(f"Created low-tide event: {summary} at {prediction.timestamp.isoformat()}")

    print(
        f"Monthly low-tide task processed {len(predictions)} prediction(s) for "
        f"{month_start.strftime('%Y-%m')}: {created_count} created, {skipped_count} skipped."
    )
    return 0


def main() -> int:
    """Parse CLI arguments and dispatch to the appropriate routine.

    Usage:
        python main.py [daily|monthly] [optional-date-for-monthly]

    For monthly, optional-date format: YYYY-MM or YYYY-MM-DD (defaults to current month).

    Returns:
        Exit code from the selected routine.
    """
    routine = "daily"
    target_date = None

    if len(sys.argv) > 1:
        routine = sys.argv[1].lower()

    if routine == "daily":
        return run_daily()
    elif routine == "monthly":
        if len(sys.argv) > 2:
            date_str = sys.argv[2]
            try:
                if len(date_str) == 7:  # YYYY-MM format
                    year, month = date_str.split("-")
                    target_date = date(int(year), int(month), 1)
                elif len(date_str) == 10:  # YYYY-MM-DD format
                    target_date = date.fromisoformat(date_str)
                else:
                    raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM or YYYY-MM-DD.")
            except (ValueError, AttributeError) as e:
                print(f"Error parsing date: {e}", file=sys.stderr)
                raise SystemExit(1)
        return run_monthly(target_date)
    else:
        print(f"Unknown routine: {routine}. Use 'daily' or 'monthly'.", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except requests.HTTPError as error:
        response = error.response
        status = response.status_code if response is not None else "unknown"
        body = response.text if response is not None else str(error)
        print(f"HTTP error ({status}): {body}", file=sys.stderr)
        raise SystemExit(1)
    except SyncError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
