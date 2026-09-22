"""Synchronize Google Calendar all-day tasks into a Trello triage list.

The script creates missing cards for today's all-day events, migrates legacy UID
markers, and moves due incomplete cards into the configured triage list.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from icalendar import Calendar
from tzlocal import get_localzone

from core.errors import SyncError
from core.models import (
    CalendarEvent,
    Config,
    LowTidePrediction,
    MonthlyConfig,
    RowChange,
    SheetSource,
    TrelloCard,
    WatchConfig,
    WebPageSource,
)
from core.notifiers import ChangeNotifier
from services.content_diff import ContentDiffService
from services.google_calendar import GoogleCalendarService
from services.google_calendar_events import GoogleCalendarEventService
from services.http_client import HttpClient
from services.noaa_tides import NoaaTideService
from services.sheet_watcher import SheetWatcherService
from services.trello import TrelloService
from services.trello_change_notifier import TrelloChangeNotifier
from services.web_page_watcher import WebPageWatcherService

TRELLO_API_BASE_URL = "https://api.trello.com/1"
UID_MARKER_PREFIX = "GCAL-UID:"
LOW_TIDE_MARKER_PREFIX = "LOW-TIDE-KEY:"
FAILURE_ALERT_MARKER_PREFIX = "FAILURE-ALERT-KEY:"
RUN_CALENDAR_SYNC = True
RUN_DUE_CARD_TRIAGE = True
MAX_REQUEST_ATTEMPTS = 3
INITIAL_RETRY_DELAY_SECONDS = 2
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
DATE_STATUS_FILE_PATH = Path("logs") / "processed_dates.json"
SHEET_WATCH_SNAPSHOT_DIR = Path("state") / "sheet_watchers"
WEB_PAGE_WATCH_SNAPSHOT_DIR = Path("state") / "web_page_watchers"
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
SHEET_WATCHER_SERVICE = SheetWatcherService(HTTP_CLIENT)
WEB_PAGE_WATCHER_SERVICE = WebPageWatcherService(HTTP_CLIENT)
CONTENT_DIFF_SERVICE = ContentDiffService()


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


def parse_sheet_sources(sources_json: str) -> list[SheetSource]:
    """Parse the SHEET_WATCHERS environment variable into SheetSource entries.

    Args:
        sources_json: JSON array string, e.g. '[{"name": "X", "spreadsheet_id": "abc", "gid": "0"}]'.

    Returns:
        Parsed list of SheetSource entries.

    Raises:
        SyncError: If the value is not valid JSON or not a JSON array of objects.
    """
    if not sources_json:
        return []

    try:
        raw_sources = json.loads(sources_json)
    except json.JSONDecodeError as error:
        raise SyncError("SHEET_WATCHERS must be valid JSON") from error

    if not isinstance(raw_sources, list):
        raise SyncError("SHEET_WATCHERS must be a JSON array")

    try:
        return [
            SheetSource(name=entry["name"], spreadsheet_id=entry["spreadsheet_id"], gid=str(entry.get("gid", "0")))
            for entry in raw_sources
        ]
    except (KeyError, TypeError) as error:
        raise SyncError("Each SHEET_WATCHERS entry requires 'name' and 'spreadsheet_id'") from error


def parse_web_page_sources(sources_json: str) -> list[WebPageSource]:
    """Parse the WEB_PAGE_WATCHERS environment variable into WebPageSource entries.

    Args:
        sources_json: JSON array string, e.g. '[{"name": "X", "url": "https://..."}]'.

    Returns:
        Parsed list of WebPageSource entries.

    Raises:
        SyncError: If the value is not valid JSON or not a JSON array of objects.
    """
    if not sources_json:
        return []

    try:
        raw_sources = json.loads(sources_json)
    except json.JSONDecodeError as error:
        raise SyncError("WEB_PAGE_WATCHERS must be valid JSON") from error

    if not isinstance(raw_sources, list):
        raise SyncError("WEB_PAGE_WATCHERS must be a JSON array")

    try:
        return [WebPageSource(name=entry["name"], url=entry["url"]) for entry in raw_sources]
    except (KeyError, TypeError) as error:
        raise SyncError("Each WEB_PAGE_WATCHERS entry requires 'name' and 'url'") from error


def load_watch_config() -> WatchConfig:
    """Load and validate required environment variables for the watch routines."""
    load_dotenv()

    trello_api_key = os.getenv("TRELLO_API_KEY", "").strip()
    trello_api_token = os.getenv("TRELLO_API_TOKEN", "").strip()
    trello_board_name = os.getenv("TRELLO_BOARD_NAME", "").strip()
    trello_list_name = os.getenv("TRELLO_LIST_NAME", "").strip()
    sheet_sources = parse_sheet_sources(os.getenv("SHEET_WATCHERS", "").strip())
    web_page_sources = parse_web_page_sources(os.getenv("WEB_PAGE_WATCHERS", "").strip())

    missing_values = [
        name
        for name, value in (
            ("TRELLO_API_KEY", trello_api_key),
            ("TRELLO_API_TOKEN", trello_api_token),
            ("TRELLO_BOARD_NAME", trello_board_name),
            ("TRELLO_LIST_NAME", trello_list_name),
        )
        if not value
    ]
    if missing_values:
        raise SyncError(f"Missing required watch environment variables: {', '.join(missing_values)}")
    if not sheet_sources and not web_page_sources:
        raise SyncError("At least one of SHEET_WATCHERS or WEB_PAGE_WATCHERS must be configured")

    return WatchConfig(
        trello_api_key=trello_api_key,
        trello_api_token=trello_api_token,
        trello_board_name=trello_board_name,
        trello_list_name=trello_list_name,
        sheet_sources=sheet_sources,
        web_page_sources=web_page_sources,
    )


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


def fetch_sheet_csv(source: SheetSource) -> str:
    """Download the current sheet tab contents as CSV text."""
    return SHEET_WATCHER_SERVICE.fetch_sheet_csv(source)


def parse_csv_rows(csv_text: str) -> list[list[str]]:
    """Parse CSV text into a list of row value lists."""
    return SHEET_WATCHER_SERVICE.parse_csv_rows(csv_text)


def hash_sheet_content(csv_text: str) -> str:
    """Compute a stable content hash used for alert deduplication."""
    return SHEET_WATCHER_SERVICE.hash_content(csv_text)


def sheet_snapshot_path(source_name: str) -> Path:
    """Build the snapshot file path for one watched sheet source."""
    return SHEET_WATCHER_SERVICE.snapshot_path(SHEET_WATCH_SNAPSHOT_DIR, source_name)


def load_sheet_snapshot(snapshot_path: Path) -> str | None:
    """Load the previously stored snapshot's raw CSV text, if any."""
    return SHEET_WATCHER_SERVICE.load_snapshot(snapshot_path)


def save_sheet_snapshot(snapshot_path: Path, csv_text: str) -> None:
    """Persist the current CSV text as the new snapshot."""
    SHEET_WATCHER_SERVICE.save_snapshot(snapshot_path, csv_text)


def diff_sheet_rows(old_rows: list[list[str]], new_rows: list[list[str]]) -> list[RowChange]:
    """Compute row-level and cell-level changes between two CSV row sets."""
    return SHEET_WATCHER_SERVICE.diff_rows(old_rows, new_rows)


def fetch_web_page_text(
    source: WebPageSource,
    etag: str | None,
    last_modified: str | None,
) -> tuple[str | None, str | None, str | None]:
    """Fetch a web page's visible text, or None when a conditional GET reports no change."""
    return WEB_PAGE_WATCHER_SERVICE.fetch_page_text(source, etag, last_modified)


def parse_web_page_lines(text: str) -> list[list[str]]:
    """Represent each line of page text as a single-column row for diffing."""
    return WEB_PAGE_WATCHER_SERVICE.parse_text_lines(text)


def hash_web_page_content(text: str) -> str:
    """Compute a stable content hash used for alert deduplication."""
    return CONTENT_DIFF_SERVICE.hash_content(text)


def web_page_snapshot_path(source_name: str) -> Path:
    """Build the snapshot file path for one watched web page source."""
    return CONTENT_DIFF_SERVICE.snapshot_path(WEB_PAGE_WATCH_SNAPSHOT_DIR, source_name, extension="txt")


def load_web_page_snapshot(snapshot_path: Path) -> str | None:
    """Load the previously stored snapshot's raw text, if any."""
    return CONTENT_DIFF_SERVICE.load_snapshot(snapshot_path)


def save_web_page_snapshot(snapshot_path: Path, text: str) -> None:
    """Persist the current page text as the new snapshot."""
    CONTENT_DIFF_SERVICE.save_snapshot(snapshot_path, text)


def diff_web_page_lines(old_rows: list[list[str]], new_rows: list[list[str]]) -> list[RowChange]:
    """Compute line-level changes between two page text snapshots."""
    return CONTENT_DIFF_SERVICE.diff_rows(old_rows, new_rows)


def web_page_metadata_path(source_name: str) -> Path:
    """Build the conditional-GET metadata sidecar path for one watched web page source."""
    return CONTENT_DIFF_SERVICE.snapshot_path(WEB_PAGE_WATCH_SNAPSHOT_DIR, source_name, extension="meta.json")


def load_web_page_metadata(metadata_path: Path) -> dict[str, str]:
    """Load previously observed ETag/Last-Modified headers for a web page source."""
    if not metadata_path.exists():
        return {}
    try:
        return json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_web_page_metadata(metadata_path: Path, etag: str | None, last_modified: str | None) -> None:
    """Persist ETag/Last-Modified headers so future checks can use conditional GETs."""
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata = {"etag": etag or "", "last_modified": last_modified or ""}
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")


def get_local_timezone() -> ZoneInfo:
    """Return the system local timezone as ZoneInfo.

    Returns:
        Local timezone normalized to ZoneInfo.
    """
    local_zone = get_localzone()
    if isinstance(local_zone, ZoneInfo):
        return local_zone
    return ZoneInfo(str(local_zone))


def get_current_date(timezone: ZoneInfo | None = None) -> date:
    """Return the current local calendar date in the active timezone.

    Args:
        timezone: Optional timezone override used when tests need a deterministic date.

    Returns:
        The current date in the supplied timezone or the system local timezone.
    """
    active_timezone = timezone or get_local_timezone()
    return datetime.now(active_timezone).date()


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
    current_date = get_current_date(timezone)
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


def check_sheet_source(source: SheetSource, notifier: ChangeNotifier) -> None:
    """Fetch, diff, and alert on changes for a single watched sheet source.

    Args:
        source: Sheet identity to check.
        notifier: Destination for a change alert when a prior snapshot differs.

    On first run for a source (no prior snapshot), only a baseline snapshot is
    recorded; no alert is sent since there is nothing to compare against.
    """
    snapshot_path = sheet_snapshot_path(source.name)
    current_csv = fetch_sheet_csv(source)
    previous_csv = load_sheet_snapshot(snapshot_path)

    if previous_csv is None:
        save_sheet_snapshot(snapshot_path, current_csv)
        print(f"Recorded initial snapshot for sheet source: {source.name}")
        return

    if current_csv == previous_csv:
        print(f"No changes detected for sheet source: {source.name}")
        return

    changes = diff_sheet_rows(parse_csv_rows(previous_csv), parse_csv_rows(current_csv))
    if not changes:
        # Raw export text can drift slightly (whitespace/quoting) with no real row changes.
        save_sheet_snapshot(snapshot_path, current_csv)
        print(f"No changes detected for sheet source: {source.name}")
        return

    notifier.notify_change(source.name, changes, hash_sheet_content(current_csv))
    save_sheet_snapshot(snapshot_path, current_csv)
    print(f"Detected {len(changes)} row change(s) for sheet source: {source.name}")


def check_web_page_source(source: WebPageSource, notifier: ChangeNotifier) -> None:
    """Fetch, diff, and alert on changes for a single watched web page source.

    Args:
        source: Web page identity to check.
        notifier: Destination for a change alert when a prior snapshot differs.

    Uses a conditional GET (ETag/Last-Modified) when prior values are known, so
    a page that has not changed can be confirmed with a cheap 304 response
    instead of re-downloading and re-diffing the full page.
    """
    snapshot_path = web_page_snapshot_path(source.name)
    metadata_path = web_page_metadata_path(source.name)
    previous_text = load_web_page_snapshot(snapshot_path)
    previous_metadata = load_web_page_metadata(metadata_path)

    current_text, etag, last_modified = fetch_web_page_text(
        source, previous_metadata.get("etag") or None, previous_metadata.get("last_modified") or None
    )
    save_web_page_metadata(metadata_path, etag, last_modified)

    if current_text is None:
        print(f"Not modified since last check for web page source: {source.name}")
        return

    if previous_text is None:
        save_web_page_snapshot(snapshot_path, current_text)
        print(f"Recorded initial snapshot for web page source: {source.name}")
        return

    if current_text == previous_text:
        print(f"No changes detected for web page source: {source.name}")
        return

    changes = diff_web_page_lines(parse_web_page_lines(previous_text), parse_web_page_lines(current_text))
    if not changes:
        save_web_page_snapshot(snapshot_path, current_text)
        print(f"No changes detected for web page source: {source.name}")
        return

    notifier.notify_change(source.name, changes, hash_web_page_content(current_text), unit="line")
    save_web_page_snapshot(snapshot_path, current_text)
    print(f"Detected {len(changes)} line change(s) for web page source: {source.name}")


def run_watch() -> int:
    """Check all configured sheet sources for changes and alert via Trello.

    Returns:
        Zero when all watched sources were checked without an unrecoverable error.

    Raises:
        SyncError: If one or more watched sources fail to fetch or diff.
    """
    config = load_watch_config()
    board_id = find_board_id(config)
    triage_list_id = find_list_id(config, board_id)
    notifier = TrelloChangeNotifier(TRELLO_SERVICE, CONTENT_DIFF_SERVICE, config, triage_list_id)

    failed_source_names: list[str] = []
    for source in config.sheet_sources:
        try:
            check_sheet_source(source, notifier)
        except Exception as error:
            failed_source_names.append(source.name)
            print(f"ERROR checking sheet source {source.name}: {error}", file=sys.stderr)

    if failed_source_names:
        raise SyncError(f"Sheet watch failed for source(s): {', '.join(failed_source_names)}")

    return 0


def run_watch_web_pages() -> int:
    """Check all configured web page sources for changes and alert via Trello.

    Returns:
        Zero when all watched sources were checked without an unrecoverable error.

    Raises:
        SyncError: If one or more watched sources fail to fetch or diff.
    """
    config = load_watch_config()
    board_id = find_board_id(config)
    triage_list_id = find_list_id(config, board_id)
    notifier = TrelloChangeNotifier(TRELLO_SERVICE, CONTENT_DIFF_SERVICE, config, triage_list_id)

    failed_source_names: list[str] = []
    for source in config.web_page_sources:
        try:
            check_web_page_source(source, notifier)
        except Exception as error:
            failed_source_names.append(source.name)
            print(f"ERROR checking web page source {source.name}: {error}", file=sys.stderr)

    if failed_source_names:
        raise SyncError(f"Web page watch failed for source(s): {', '.join(failed_source_names)}")

    return 0


def notify_job_failure(job_name: str, error: Exception) -> None:
    """Create a deduplicated Trello alert card identifying a failing automation job.

    Args:
        job_name: Routine name that failed (e.g. "daily", "monthly", "watch").
        error: The exception raised by the failing routine.

    Failures to send the alert itself are logged but not re-raised, so they
    never mask the original routine failure.
    """
    try:
        config = load_config()
        board_id = find_board_id(config)
        list_id = find_list_id(config, board_id)
        today_key = get_current_date().isoformat()
        marker = f"{FAILURE_ALERT_MARKER_PREFIX} {job_name}::{today_key}"
        TRELLO_SERVICE.create_alert_card(
            config,
            list_id,
            f"[ALERT] {job_name} automation failed",
            f"Job '{job_name}' failed with error:\n\n{error}",
            marker,
        )
    except Exception as notify_error:
        print(f"Failed to send failure alert for job '{job_name}': {notify_error}", file=sys.stderr)


def run_routine_with_failure_alert(job_name: str, routine: Callable[[], int]) -> int:
    """Run a routine and send a Trello failure alert if it raises.

    Args:
        job_name: Human-readable routine name used in the alert card.
        routine: Zero-argument callable that runs the routine and returns an exit code.

    Returns:
        The routine's exit code.

    Raises:
        Exception: Re-raises whatever the routine raised, after sending the alert.
    """
    try:
        return routine()
    except Exception as error:
        notify_job_failure(job_name, error)
        raise


def main() -> int:
    """Parse CLI arguments and dispatch to the appropriate routine.

    Usage:
        python main.py [daily|monthly|watch|watch-web] [optional-date-for-monthly]

    For monthly, optional-date format: YYYY-MM or YYYY-MM-DD (defaults to current month).

    Returns:
        Exit code from the selected routine.
    """
    routine = "daily"
    target_date = None

    if len(sys.argv) > 1:
        routine = sys.argv[1].lower()

    if routine == "daily":
        return run_routine_with_failure_alert("daily", run_daily)
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
        return run_routine_with_failure_alert("monthly", lambda: run_monthly(target_date))
    elif routine == "watch":
        return run_routine_with_failure_alert("watch", run_watch)
    elif routine == "watch-web":
        return run_routine_with_failure_alert("watch-web", run_watch_web_pages)
    else:
        print(f"Unknown routine: {routine}. Use 'daily', 'monthly', 'watch', or 'watch-web'.", file=sys.stderr)
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
