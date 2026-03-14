from __future__ import annotations

import os
import sys
import time as time_module
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
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


@dataclass
class Config:
    ical_url: str
    trello_api_key: str
    trello_api_token: str
    trello_board_name: str
    trello_list_name: str


@dataclass
class CalendarEvent:
    uid: str
    event_key: str
    summary: str
    description: str
    is_all_day: bool


@dataclass
class TrelloCard:
    card_id: str
    name: str
    due: datetime | None
    due_complete: bool
    list_id: str


class SyncError(Exception):
    pass


def load_config() -> Config:
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
    local_zone = get_localzone()
    if isinstance(local_zone, ZoneInfo):
        return local_zone
    return ZoneInfo(str(local_zone))


def get_retry_delay_seconds(attempt_number: int) -> int:
    return INITIAL_RETRY_DELAY_SECONDS * (2 ** (attempt_number - 1))


def is_retryable_http_error(error: requests.HTTPError) -> bool:
    response = error.response
    return response is not None and response.status_code in RETRYABLE_STATUS_CODES


def is_retryable_request_error(error: Exception) -> bool:
    if isinstance(error, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(error, requests.HTTPError):
        return is_retryable_http_error(error)
    return False


def log_retry_attempt(message: str, attempt_number: int) -> None:
    delay_seconds = get_retry_delay_seconds(attempt_number)
    print(
        f"{message}. Retrying in {delay_seconds} second(s) "
        f"(attempt {attempt_number + 1}/{MAX_REQUEST_ATTEMPTS}).",
        file=sys.stderr,
    )
    time_module.sleep(delay_seconds)


def request_with_backoff(method: str, url: str, retry_enabled: bool = True, **kwargs) -> requests.Response:
    for attempt_number in range(1, MAX_REQUEST_ATTEMPTS + 1):
        try:
            response = requests.request(method, url, timeout=30, **kwargs)
            if retry_enabled and response.status_code in RETRYABLE_STATUS_CODES and attempt_number < MAX_REQUEST_ATTEMPTS:
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
    response = request_with_backoff("GET", ical_url)
    if response.status_code == 404 and "calendar.google.com" in ical_url:
        raise SyncError(
            "Google Calendar returned 404 for the iCal URL. Use the calendar's 'Secret address in iCal format' from Settings and sharing -> Integrate calendar."
        )
    response.raise_for_status()
    return Calendar.from_ical(response.text)


def normalize_description(raw_description: str | None) -> str:
    if not raw_description:
        return ""
    return raw_description.strip()


def format_occurrence_value(value: date | datetime, timezone: ZoneInfo) -> str:
    if isinstance(value, datetime):
        return as_local_datetime(value, timezone).isoformat()
    return value.isoformat()


def as_local_datetime(value: date | datetime, timezone: ZoneInfo) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone)
        return value.astimezone(timezone)
    return datetime.combine(value, time.min, tzinfo=timezone)


def build_event_key(uid: str, occurrence_value: date | datetime, timezone: ZoneInfo) -> str:
    return f"{uid}::{format_occurrence_value(occurrence_value, timezone)}"


def parse_events_for_today(calendar: Calendar, today: date, timezone: ZoneInfo) -> tuple[list[CalendarEvent], list[str]]:
    matching_events: list[CalendarEvent] = []
    warnings: list[str] = []
    start_of_day = datetime.combine(today, time.min, tzinfo=timezone)
    end_of_day = start_of_day + timedelta(days=1)

    for component in recurring_ical_events.of(calendar).between(start_of_day, end_of_day):
        start_value = component.decoded("DTSTART")
        occurrence_day = as_local_datetime(start_value, timezone).date() if isinstance(start_value, datetime) else start_value
        if occurrence_day != today:
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


def trello_request(method: str, path: str, api_key: str, api_token: str, allow_retries: bool = True, **kwargs):
    params = kwargs.pop("params", {})
    params.update({"key": api_key, "token": api_token})
    response = request_with_backoff(method, f"{TRELLO_API_BASE_URL}{path}", retry_enabled=allow_retries, params=params, **kwargs)
    response.raise_for_status()
    return response.json()


def parse_trello_datetime(value: str | None, timezone: ZoneInfo) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone)


def find_board_id(config: Config) -> str:
    boards = trello_request("GET", "/members/me/boards", config.trello_api_key, config.trello_api_token, params={"fields": "name"})
    for board in boards:
        if board["name"] == config.trello_board_name:
            return board["id"]
    raise SyncError(f"Trello board not found: {config.trello_board_name}")


def find_list_id(config: Config, board_id: str) -> str:
    lists = trello_request("GET", f"/boards/{board_id}/lists", config.trello_api_key, config.trello_api_token, params={"fields": "name"})
    for trello_list in lists:
        if trello_list["name"] == config.trello_list_name:
            return trello_list["id"]
    raise SyncError(f"Trello list not found on board {config.trello_board_name}: {config.trello_list_name}")


def load_open_board_cards(config: Config, board_id: str, timezone: ZoneInfo) -> list[TrelloCard]:
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
    trello_request(
        "PUT",
        f"/cards/{card_id}",
        config.trello_api_key,
        config.trello_api_token,
        params={"idList": list_id, "pos": "top"},
    )


def extract_event_uid(card_description: str) -> str | None:
    for line in card_description.splitlines():
        if line.startswith(UID_MARKER_PREFIX):
            return line.replace(UID_MARKER_PREFIX, "", 1).strip()
    return None


def load_existing_event_markers(config: Config, list_id: str) -> tuple[set[str], dict[str, list[str]]]:
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
    description_parts = []
    if event.description:
        description_parts.append(event.description)
    description_parts.append(f"{UID_MARKER_PREFIX} {event.event_key}")
    return "\n\n".join(description_parts)


def card_exists_for_event(config: Config, list_id: str, event_key: str) -> bool:
    existing_event_keys, _ = load_existing_event_markers(config, list_id)
    return event_key in existing_event_keys


def create_card(config: Config, list_id: str, event: CalendarEvent) -> bool:
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


def run_calendar_sync(config: Config, timezone: ZoneInfo, today: date, triage_list_id: str) -> None:
    calendar = fetch_calendar(config.ical_url)
    events, warnings = parse_events_for_today(calendar, today, timezone)
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
        f"Calendar sync processed {len(events)} event(s) for {today.isoformat()}: "
        f"{created_count} created, {skipped_count} skipped, {migrated_count} migrated."
    )
    for warning in warnings:
        print(f"WARNING: {warning}")


def run_due_card_triage(config: Config, timezone: ZoneInfo, today: date, board_id: str, triage_list_id: str) -> None:
    cards = load_open_board_cards(config, board_id, timezone)

    moved_count = 0
    already_in_triage_count = 0
    eligible_count = 0
    for card in cards:
        if card.due is None or card.due_complete:
            continue

        if card.due.date() > today:
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
        f"Due-card triage processed {eligible_count} eligible card(s) for {today.isoformat()}: "
        f"{moved_count} moved, {already_in_triage_count} already in triage."
    )


def run() -> int:
    config = load_config()
    timezone = get_local_timezone()
    today = datetime.now(timezone).date()
    board_id = find_board_id(config)
    triage_list_id = find_list_id(config, board_id)

    if RUN_CALENDAR_SYNC:
        run_calendar_sync(config, timezone, today, triage_list_id)

    if RUN_DUE_CARD_TRIAGE:
        run_due_card_triage(config, timezone, today, board_id, triage_list_id)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except requests.HTTPError as error:
        response = error.response
        status = response.status_code if response is not None else "unknown"
        body = response.text if response is not None else str(error)
        print(f"HTTP error ({status}): {body}", file=sys.stderr)
        raise SystemExit(1)
    except SyncError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
