"""Synchronize Google Calendar all-day tasks into a Trello triage list.

The script creates missing cards for today's all-day events, migrates legacy UID
markers, and moves due incomplete cards into the configured triage list.
"""

from __future__ import annotations

import json
import os
import sys
import time as time_module
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import recurring_ical_events
import requests
from dotenv import load_dotenv
from icalendar import Calendar
from tzlocal import get_localzone

TRELLO_API_BASE_URL = "https://api.trello.com/1"
UID_MARKER_PREFIX = "GCAL-UID:"
RUN_CALENDAR_SYNC = True
RUN_DUE_CARD_TRIAGE = True
MAX_REQUEST_ATTEMPTS = 3
INITIAL_RETRY_DELAY_SECONDS = 2
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
DATE_STATUS_FILE_PATH = Path("logs") / "processed_dates.json"


@dataclass
class Config:
    """Store required runtime settings loaded from environment variables.

    Attributes:
        ical_url: iCal feed URL used to fetch calendar events.
        trello_api_key: Trello API key.
        trello_api_token: Trello API token.
        trello_board_name: Human-readable board name used for lookup.
        trello_list_name: Human-readable triage list name used for lookup.
    """

    ical_url: str
    trello_api_key: str
    trello_api_token: str
    trello_board_name: str
    trello_list_name: str


@dataclass
class CalendarEvent:
    """Represent one calendar occurrence that may map to a Trello card.

    Attributes:
        uid: Calendar UID from the source event.
        event_key: Deduplication marker formatted as uid::occurrence.
        summary: Event title used as Trello card name.
        description: Optional event description copied to card description.
        is_all_day: Whether DTSTART represents an all-day event.
    """

    uid: str
    event_key: str
    summary: str
    description: str
    is_all_day: bool


@dataclass
class TrelloCard:
    """Capture the card fields needed for due-date triage decisions.

    Attributes:
        card_id: Trello card identifier.
        name: Card title.
        due: Parsed due datetime in local timezone, if set.
        due_complete: Whether Trello marks the card due date as complete.
        list_id: Identifier of the current containing list.
    """

    card_id: str
    name: str
    due: datetime | None
    due_complete: bool
    list_id: str


class SyncError(Exception):
    """Raise when a domain-level synchronization step cannot be completed."""

    pass


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
    """Compute exponential backoff delay for a retry attempt.

    Args:
        attempt_number: 1-based retry attempt index.

    Returns:
        Delay in seconds before the next retry.
    """
    return INITIAL_RETRY_DELAY_SECONDS * (2 ** (attempt_number - 1))


def is_retryable_http_error(error: requests.HTTPError) -> bool:
    """Determine whether an HTTP error is retryable.

    Args:
        error: HTTPError that may include a response object.

    Returns:
        True when the response status code is in RETRYABLE_STATUS_CODES.
    """
    response = error.response
    return response is not None and response.status_code in RETRYABLE_STATUS_CODES


def is_retryable_request_error(error: Exception) -> bool:
    """Determine whether a request exception should be retried.

    Args:
        error: Exception raised during HTTP interaction.

    Returns:
        True for transient connection/timeout errors and retryable HTTP errors.
    """
    if isinstance(error, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(error, requests.HTTPError):
        return is_retryable_http_error(error)
    return False


def log_retry_attempt(message: str, attempt_number: int) -> None:
    """Log retry details and sleep using exponential backoff.

    Args:
        message: Error context to include in retry logging.
        attempt_number: 1-based attempt index used to compute delay.
    """
    delay_seconds = get_retry_delay_seconds(attempt_number)
    print(
        f"{message}. Retrying in {delay_seconds} second(s) "
        f"(attempt {attempt_number + 1}/{MAX_REQUEST_ATTEMPTS}).",
        file=sys.stderr,
    )
    time_module.sleep(delay_seconds)


def request_with_backoff(
    method: str,
    url: str,
    retry_enabled: bool = True,
    **kwargs,
) -> requests.Response:
    """Issue an HTTP request with retry and backoff.

    Args:
        method: HTTP method passed to requests.request.
        url: Absolute URL to request.
        retry_enabled: Whether transient failures should be retried.
        **kwargs: Additional keyword arguments forwarded to requests.request.

    Returns:
        Successful or final-response requests.Response object.

    Raises:
        requests.ConnectionError: If final attempt fails with connection error.
        requests.Timeout: If final attempt fails with timeout.
        SyncError: If retry loop exhausts without returning a response.
    """
    for attempt_number in range(1, MAX_REQUEST_ATTEMPTS + 1):
        try:
            response = requests.request(method, url, timeout=30, **kwargs)
            if (
                retry_enabled
                and response.status_code in RETRYABLE_STATUS_CODES
                and attempt_number < MAX_REQUEST_ATTEMPTS
            ):
                log_retry_attempt(
                    f"Received retryable HTTP status {response.status_code} for {method} {url}",
                    attempt_number,
                )
                continue
            return response
        except (requests.ConnectionError, requests.Timeout) as error:
            if not retry_enabled or attempt_number == MAX_REQUEST_ATTEMPTS:
                raise error
            log_retry_attempt(f"Transient request failure for {method} {url}: {error}", attempt_number)

    raise SyncError(f"Request retries exhausted for {method} {url}")


def fetch_calendar(ical_url: str) -> Calendar:
    """Fetch and parse an iCal feed.

    Args:
        ical_url: Public or secret iCal URL.

    Returns:
        Parsed Calendar object.

    Raises:
        SyncError: If Google Calendar returns 404 for a non-secret URL.
        requests.HTTPError: If the HTTP response status is not successful.
    """
    response = request_with_backoff("GET", ical_url)
    if response.status_code == 404 and "calendar.google.com" in ical_url:
        raise SyncError(
            "Google Calendar returned 404 for the iCal URL. "
            "Use the calendar's 'Secret address in iCal format' from "
            "Settings and sharing -> Integrate calendar."
        )
    response.raise_for_status()
    return Calendar.from_ical(response.text)


def normalize_description(raw_description: str | None) -> str:
    """Normalize optional event description text.

    Args:
        raw_description: Description value from an iCal component.

    Returns:
        Trimmed description or an empty string.
    """
    if not raw_description:
        return ""
    return raw_description.strip()


def format_occurrence_value(value: date | datetime, timezone: ZoneInfo) -> str:
    """Format an occurrence value for event-key generation.

    Args:
        value: Occurrence date or datetime from DTSTART.
        timezone: Local timezone for datetime normalization.

    Returns:
        ISO-8601 string representing the occurrence.
    """
    if isinstance(value, datetime):
        return as_local_datetime(value, timezone).isoformat()
    return value.isoformat()


def as_local_datetime(value: date | datetime, timezone: ZoneInfo) -> datetime:
    """Convert a date-like value into local timezone-aware datetime.

    Args:
        value: Date or datetime source value.
        timezone: Target local timezone.

    Returns:
        Timezone-aware datetime in local timezone.
    """
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone)
        return value.astimezone(timezone)
    return datetime.combine(value, time.min, tzinfo=timezone)


def build_event_key(uid: str, occurrence_value: date | datetime, timezone: ZoneInfo) -> str:
    """Build a unique deduplication key for an event occurrence.

    Args:
        uid: Calendar event UID.
        occurrence_value: Event DTSTART value for one occurrence.
        timezone: Local timezone used to normalize datetime values.

    Returns:
        Marker in the format "{uid}::{occurrence}".
    """
    return f"{uid}::{format_occurrence_value(occurrence_value, timezone)}"


def parse_events_for_today(
    calendar: Calendar,
    target_date: date,
    timezone: ZoneInfo,
) -> tuple[list[CalendarEvent], list[str]]:
    """Extract occurrences that belong to the target local date.

    Args:
        calendar: Parsed iCal calendar.
        target_date: Local date to extract.
        timezone: Local timezone used for datetime interpretation.

    Returns:
        Tuple of normalized CalendarEvent entries and warning strings for
        skipped or problematic events.
    """
    matching_events: list[CalendarEvent] = []
    warnings: list[str] = []
    start_of_day = datetime.combine(target_date, time.min, tzinfo=timezone)
    end_of_day = start_of_day + timedelta(days=1)

    for component in recurring_ical_events.of(calendar).between(start_of_day, end_of_day):
        start_value = component.decoded("DTSTART")
        occurrence_day = (
            as_local_datetime(start_value, timezone).date()
            if isinstance(start_value, datetime)
            else start_value
        )
        if occurrence_day != target_date:
            continue

        uid = str(component.get("UID", "")).strip()
        summary = str(component.get("SUMMARY", "")).strip()
        description = normalize_description(component.get("DESCRIPTION"))
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
                event_key=build_event_key(uid, start_value, timezone),
                summary=summary,
                description=description,
                is_all_day=is_all_day,
            )
        )

    return matching_events, warnings


def trello_request(
    method: str,
    path: str,
    api_key: str,
    api_token: str,
    allow_retries: bool = True,
    **kwargs,
):
    """Call a Trello API endpoint and return the decoded JSON body.

    Args:
        method: HTTP method.
        path: Trello API path beginning with '/'.
        api_key: Trello API key.
        api_token: Trello API token.
        allow_retries: Whether transient network/status errors are retried.
        **kwargs: Extra request arguments, including optional params mapping.

    Returns:
        Parsed JSON response payload.

    Raises:
        requests.HTTPError: If Trello returns an unsuccessful response.
    """
    params = kwargs.pop("params", {})
    params.update({"key": api_key, "token": api_token})
    response = request_with_backoff(
        method,
        f"{TRELLO_API_BASE_URL}{path}",
        retry_enabled=allow_retries,
        params=params,
        **kwargs,
    )
    response.raise_for_status()
    return response.json()


def parse_trello_datetime(value: str | None, timezone: ZoneInfo) -> datetime | None:
    """Parse Trello RFC3339 datetime and convert to local timezone.

    Args:
        value: Trello datetime string or None.
        timezone: Target local timezone.

    Returns:
        Localized datetime when value is present; otherwise None.
    """
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone)


def find_board_id(config: Config) -> str:
    """Resolve configured Trello board name to board ID.

    Args:
        config: Runtime configuration with target board name.

    Returns:
        Trello board ID for the configured board name.

    Raises:
        SyncError: If the board name is not found for the authenticated user.
    """
    boards = trello_request(
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


def find_list_id(config: Config, board_id: str) -> str:
    """Resolve configured Trello list name to list ID on a board.

    Args:
        config: Runtime configuration with target list name.
        board_id: Board ID that should contain the target list.

    Returns:
        Trello list ID for the configured list name.

    Raises:
        SyncError: If the list name is not found on the target board.
    """
    lists = trello_request(
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


def load_open_board_cards(config: Config, board_id: str, timezone: ZoneInfo) -> list[TrelloCard]:
    """Load open cards from a board with normalized due-date fields.

    Args:
        config: Runtime configuration with Trello credentials.
        board_id: Board ID to scan.
        timezone: Local timezone for due-date conversion.

    Returns:
        List of TrelloCard records for non-archived cards.
    """
    cards = trello_request(
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
                due=parse_trello_datetime(card.get("due"), timezone),
                due_complete=bool(card.get("dueComplete")),
                list_id=card["idList"],
            )
        )

    return open_cards


def move_card_to_list(config: Config, card_id: str, list_id: str) -> None:
    """Move a Trello card to the top of a list.

    Args:
        config: Runtime configuration with Trello credentials.
        card_id: Card ID to move.
        list_id: Destination list ID.
    """
    trello_request(
        "PUT",
        f"/cards/{card_id}",
        config.trello_api_key,
        config.trello_api_token,
        params={"idList": list_id, "pos": "top"},
    )


def extract_event_uid(card_description: str) -> str | None:
    """Extract the GCAL UID marker from card description text.

    Args:
        card_description: Trello card description field.

    Returns:
        Marker value when present; otherwise None.
    """
    for line in card_description.splitlines():
        if line.startswith(UID_MARKER_PREFIX):
            return line.replace(UID_MARKER_PREFIX, "", 1).strip()
    return None


def load_existing_event_markers(config: Config, list_id: str) -> tuple[set[str], dict[str, list[str]]]:
    """Collect existing event markers from cards already in a list.

    Args:
        config: Runtime configuration with Trello credentials.
        list_id: Trello list ID to inspect.

    Returns:
        Tuple containing current event keys and legacy UID mappings to card IDs.
    """
    cards = trello_request(
        "GET",
        f"/lists/{list_id}/cards",
        config.trello_api_key,
        config.trello_api_token,
        params={"fields": "name,desc"},
    )

    existing_event_keys: set[str] = set()
    legacy_cards_by_uid: dict[str, list[str]] = {}
    for card in cards:
        marker = extract_event_uid(card.get("desc", ""))
        if not marker:
            continue

        if "::" in marker:
            existing_event_keys.add(marker)
            continue

        legacy_cards_by_uid.setdefault(marker, []).append(card["id"])

    return existing_event_keys, legacy_cards_by_uid


def migrate_legacy_card_marker(config: Config, card_id: str, legacy_uid: str, event: CalendarEvent) -> None:
    """Upgrade a legacy card marker from UID-only to occurrence marker.

    Args:
        config: Runtime configuration with Trello credentials.
        card_id: Card ID whose description should be updated.
        legacy_uid: Legacy UID marker currently stored on the card.
        event: Event whose occurrence marker should replace the legacy marker.
    """
    card = trello_request(
        "GET",
        f"/cards/{card_id}",
        config.trello_api_key,
        config.trello_api_token,
        params={"fields": "desc"},
    )
    existing_description = card.get("desc", "")
    updated_description = existing_description.replace(
        f"{UID_MARKER_PREFIX} {legacy_uid}",
        f"{UID_MARKER_PREFIX} {event.event_key}",
        1,
    )
    if updated_description == existing_description:
        return

    trello_request(
        "PUT",
        f"/cards/{card_id}",
        config.trello_api_key,
        config.trello_api_token,
        params={"desc": updated_description},
    )


def build_card_description(event: CalendarEvent) -> str:
    """Build Trello card description text for an event.

    Args:
        event: Calendar event used to populate description and marker.

    Returns:
        Description content with optional body text and required UID marker.
    """
    description_parts = []
    if event.description:
        description_parts.append(event.description)
    description_parts.append(f"{UID_MARKER_PREFIX} {event.event_key}")
    return "\n\n".join(description_parts)


def card_exists_for_event(config: Config, list_id: str, event_key: str) -> bool:
    """Check whether a list already contains an event occurrence marker.

    Args:
        config: Runtime configuration with Trello credentials.
        list_id: Trello list ID to check.
        event_key: Event occurrence marker to search for.

    Returns:
        True when the marker already exists in the target list.
    """
    existing_event_keys, _ = load_existing_event_markers(config, list_id)
    return event_key in existing_event_keys


def create_card(config: Config, list_id: str, event: CalendarEvent) -> bool:
    """Create a Trello card for an event with transient-error recovery.

    Args:
        config: Runtime configuration with Trello credentials.
        list_id: Trello list ID where the card should be created.
        event: Event to convert into a Trello card.

    Returns:
        True when a card was newly created; False when recovery detected that
        the card already exists.

    Raises:
        Exception: Re-raises non-retryable or final-attempt create failures.
        SyncError: If retry loop exhausts without success.
    """
    card_params = {
        "idList": list_id,
        "name": event.summary,
        "desc": build_card_description(event),
        "pos": "top",
    }

    for attempt_number in range(1, MAX_REQUEST_ATTEMPTS + 1):
        try:
            trello_request(
                "POST",
                "/cards",
                config.trello_api_key,
                config.trello_api_token,
                allow_retries=False,
                params=card_params,
            )
            return True
        except Exception as error:
            if card_exists_for_event(config, list_id, event.event_key):
                print(
                    f"Recovered existing card after transient create failure: {event.summary}",
                    file=sys.stderr,
                )
                return False

            if not is_retryable_request_error(error) or attempt_number == MAX_REQUEST_ATTEMPTS:
                raise error

            log_retry_attempt(f"Transient failure creating card for {event.summary}: {error}", attempt_number)

    raise SyncError(f"Card creation retries exhausted for event {event.summary}")


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


def run_monthly() -> int:
    """Run monthly automation tasks.

    Returns:
        Zero when all monthly tasks complete successfully.

    Raises:
        SyncError: If one or more monthly tasks fail.
    """
    # Monthly tasks will be added here as needed.
    print("No monthly tasks configured yet.")
    return 0


def main() -> int:
    """Parse CLI arguments and dispatch to the appropriate routine.

    Returns:
        Exit code from the selected routine.
    """
    routine = "daily"
    if len(sys.argv) > 1:
        routine = sys.argv[1].lower()

    if routine == "daily":
        return run_daily()
    elif routine == "monthly":
        return run_monthly()
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
