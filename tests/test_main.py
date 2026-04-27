from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest
import requests

import main


class FakeComponent:
    def __init__(self, start_value, uid: str = "uid-1", summary: str = "Task", description: str | None = " Desc "):
        self._start_value = start_value
        self._values = {
            "UID": uid,
            "SUMMARY": summary,
            "DESCRIPTION": description,
        }

    def decoded(self, key: str):
        if key != "DTSTART":
            raise KeyError(key)
        return self._start_value

    def get(self, key: str, default=None):
        return self._values.get(key, default)


class FakeRecurringCalendar:
    def __init__(self, components):
        self._components = components

    def between(self, start_of_day, end_of_day):
        return self._components


class FakeResponse:
    def __init__(self, status_code: int = 200, text: str = "", json_data=None):
        self.status_code = status_code
        self.text = text
        self._json_data = json_data if json_data is not None else {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)

    def json(self):
        return self._json_data


@pytest.fixture
def timezone() -> ZoneInfo:
    return ZoneInfo("America/Los_Angeles")


@pytest.fixture
def config() -> main.Config:
    return main.Config(
        ical_url="https://calendar.example.com/private.ics",
        trello_api_key="key",
        trello_api_token="token",
        trello_board_name="To Do",
        trello_list_name="Triage",
    )


@pytest.fixture
def sample_event() -> main.CalendarEvent:
    return main.CalendarEvent(
        uid="uid-1",
        event_key="uid-1::2026-03-14",
        summary="Sample Task",
        description="Task description",
        is_all_day=True,
    )


def test_parse_events_for_today_returns_all_day_and_timed_events(monkeypatch, timezone):
    today = date(2026, 3, 14)
    components = [
        FakeComponent(today, uid="all-day", summary="All Day"),
        FakeComponent(datetime(2026, 3, 14, 10, 30, tzinfo=timezone), uid="timed", summary="Timed"),
        FakeComponent(date(2026, 3, 15), uid="tomorrow", summary="Tomorrow"),
        FakeComponent(today, uid="", summary="Missing UID"),
        FakeComponent(today, uid="missing-summary", summary=""),
    ]
    monkeypatch.setattr(main.recurring_ical_events, "of", lambda calendar: FakeRecurringCalendar(components))

    events, warnings = main.parse_events_for_today(object(), today, timezone)

    assert [event.summary for event in events] == ["All Day", "Timed"]
    assert events[0].is_all_day is True
    assert events[1].is_all_day is False
    assert events[0].description == "Desc"
    assert "Event is not all-day and should be corrected: Timed" in warnings
    assert "Skipped event with no UID: Missing UID" in warnings
    assert "Skipped event with no summary for UID missing-summary" in warnings


def test_fetch_calendar_raises_helpful_message_for_google_404(monkeypatch):
    monkeypatch.setattr(main, "request_with_backoff", lambda method, url, **kwargs: FakeResponse(status_code=404))

    with pytest.raises(main.SyncError, match="Secret address in iCal format"):
        main.fetch_calendar("https://calendar.google.com/calendar/ical/public/basic.ics")


def test_request_with_backoff_retries_connection_errors(monkeypatch):
    attempts = []
    sleeps = []

    def fake_request(method, url, timeout, **kwargs):
        attempts.append((method, url, timeout))
        if len(attempts) < 3:
            raise requests.ConnectionError("reset")
        return FakeResponse(status_code=200)

    monkeypatch.setattr(main.requests, "request", fake_request)
    monkeypatch.setattr(main.time_module, "sleep", sleeps.append)

    response = main.request_with_backoff("GET", "https://example.com")

    assert response.status_code == 200
    assert len(attempts) == 3
    assert sleeps == [2, 4]


def test_request_with_backoff_retries_retryable_status_codes(monkeypatch):
    responses = [FakeResponse(status_code=503), FakeResponse(status_code=200)]
    sleeps = []

    monkeypatch.setattr(main.requests, "request", lambda method, url, timeout, **kwargs: responses.pop(0))
    monkeypatch.setattr(main.time_module, "sleep", sleeps.append)

    response = main.request_with_backoff("GET", "https://example.com")

    assert response.status_code == 200
    assert sleeps == [2]


def test_create_card_retries_transient_failure_then_succeeds(monkeypatch, config, sample_event):
    call_count = {"count": 0}
    sleeps = []

    def fake_trello_request(method, path, api_key, api_token, allow_retries=True, **kwargs):
        call_count["count"] += 1
        if call_count["count"] == 1:
            raise requests.ConnectionError("reset")
        return {"id": "card-1"}

    monkeypatch.setattr(main, "trello_request", fake_trello_request)
    monkeypatch.setattr(main, "card_exists_for_event", lambda config, list_id, event_key: False)
    monkeypatch.setattr(main.time_module, "sleep", sleeps.append)

    created_new_card = main.create_card(config, "list-1", sample_event)

    assert created_new_card is True
    assert call_count["count"] == 2
    assert sleeps == [2]


def test_create_card_recovers_when_card_exists_after_failure(monkeypatch, config, sample_event, capsys):
    def fake_trello_request(method, path, api_key, api_token, allow_retries=True, **kwargs):
        raise requests.ConnectionError("reset")

    monkeypatch.setattr(main, "trello_request", fake_trello_request)
    monkeypatch.setattr(main, "card_exists_for_event", lambda config, list_id, event_key: True)

    created_new_card = main.create_card(config, "list-1", sample_event)

    captured = capsys.readouterr()
    assert created_new_card is False
    assert "Recovered existing card after transient create failure" in captured.err


def test_run_calendar_sync_tracks_created_skipped_and_migrated(monkeypatch, config, timezone, capsys):
    today = date(2026, 3, 14)
    created_event = main.CalendarEvent("new-uid", "new-uid::2026-03-14", "New Task", "", True)
    skipped_event = main.CalendarEvent("skip-uid", "skip-uid::2026-03-14", "Skipped Task", "", True)
    migrated_event = main.CalendarEvent("legacy-uid", "legacy-uid::2026-03-14", "Migrated Task", "", True)

    monkeypatch.setattr(main, "fetch_calendar", lambda ical_url: object())
    monkeypatch.setattr(
        main,
        "parse_events_for_today",
        lambda calendar, current_day, zone: (
            [created_event, skipped_event, migrated_event],
            ["warn-1"],
        ),
    )
    monkeypatch.setattr(
        main,
        "load_existing_event_markers",
        lambda config, list_id: (
            {skipped_event.event_key},
            {migrated_event.uid: ["card-123"]},
        ),
    )
    monkeypatch.setattr(main, "create_card", lambda config, list_id, event: True)
    migrated = []
    monkeypatch.setattr(
        main,
        "migrate_legacy_card_marker",
        lambda config, card_id, legacy_uid, event: migrated.append(
            (card_id, legacy_uid, event.summary)
        ),
    )

    main.run_calendar_sync(config, timezone, today, "triage-list")

    output = capsys.readouterr().out
    assert "Created card: New Task" in output
    assert "Skipped existing card for event: Skipped Task" in output
    assert "Migrated existing legacy card for event: Migrated Task" in output
    assert "1 created, 2 skipped, 1 migrated." in output
    assert "WARNING: warn-1" in output
    assert migrated == [("card-123", "legacy-uid", "Migrated Task")]


def test_run_due_card_triage_moves_only_due_incomplete_cards(monkeypatch, config, timezone, capsys):
    today = date(2026, 3, 14)
    cards = [
        main.TrelloCard("move-me", "Move Me", datetime(2026, 3, 13, 12, 0, tzinfo=timezone), False, "other-list"),
        main.TrelloCard(
            "already-there",
            "Already There",
            datetime(2026, 3, 14, 8, 0, tzinfo=timezone),
            False,
            "triage-list",
        ),
        main.TrelloCard("future", "Future", datetime(2026, 3, 15, 9, 0, tzinfo=timezone), False, "other-list"),
        main.TrelloCard("done", "Done", datetime(2026, 3, 14, 9, 0, tzinfo=timezone), True, "other-list"),
        main.TrelloCard("undated", "Undated", None, False, "other-list"),
    ]
    moved = []

    monkeypatch.setattr(main, "load_open_board_cards", lambda config, board_id, zone: cards)
    monkeypatch.setattr(main, "move_card_to_list", lambda config, card_id, list_id: moved.append((card_id, list_id)))

    main.run_due_card_triage(config, timezone, today, "board-1", "triage-list")

    output = capsys.readouterr().out
    assert "Moved due card to triage: Move Me" in output
    assert "Skipped due card already in triage: Already There" in output
    assert "1 moved, 1 already in triage." in output
    assert moved == [("move-me", "triage-list")]


def test_run_returns_zero_when_routines_enabled(monkeypatch, config, timezone):
    status_updates = []

    monkeypatch.setattr(main, "load_config", lambda: config)
    monkeypatch.setattr(main, "get_local_timezone", lambda: timezone)
    monkeypatch.setattr(main, "load_processed_date_statuses", lambda file_path: {})
    monkeypatch.setattr(
        main,
        "save_processed_date_statuses",
        lambda file_path, statuses: status_updates.append(dict(statuses)),
    )
    monkeypatch.setattr(main.Path, "exists", lambda _self: False)
    monkeypatch.setattr(main, "find_board_id", lambda config: "board-1")
    monkeypatch.setattr(main, "find_list_id", lambda config, board_id: "list-1")
    monkeypatch.setattr(main, "run_calendar_sync", lambda config, timezone, today, list_id: None)
    monkeypatch.setattr(main, "run_due_card_triage", lambda config, timezone, today, board_id, list_id: None)

    assert main.run() == 0
    assert status_updates


def test_build_dates_to_process_includes_backfill_and_failed_dates():
    today = date(2026, 4, 27)
    statuses = {
        "2026-04-23": {"status": "success"},
        "2026-04-24": {"status": "failed"},
        "2026-04-25": {"status": "success"},
    }

    dates_to_process = main.build_dates_to_process(today, statuses)

    assert dates_to_process == [
        date(2026, 4, 24),
        date(2026, 4, 26),
        date(2026, 4, 27),
    ]


def test_build_dates_to_process_skips_successful_today():
    today = date(2026, 4, 27)
    statuses = {
        "2026-04-26": {"status": "success"},
        "2026-04-27": {"status": "success"},
    }

    dates_to_process = main.build_dates_to_process(today, statuses)

    assert dates_to_process == []


def test_build_dates_to_process_retries_failed_today():
    today = date(2026, 4, 27)
    statuses = {
        "2026-04-27": {"status": "failed"},
    }

    dates_to_process = main.build_dates_to_process(today, statuses)

    assert dates_to_process == [today]


def test_run_creates_date_status_file_and_processes_today_only_when_missing(monkeypatch, config, timezone, tmp_path):
    status_file_path = tmp_path / "processed_dates.json"
    saved_snapshots = []
    processed_days = []

    monkeypatch.setattr(main, "DATE_STATUS_FILE_PATH", str(status_file_path))
    monkeypatch.setattr(main, "load_config", lambda: config)
    monkeypatch.setattr(main, "get_local_timezone", lambda: timezone)
    monkeypatch.setattr(main, "find_board_id", lambda config: "board-1")
    monkeypatch.setattr(main, "find_list_id", lambda config, board_id: "list-1")
    monkeypatch.setattr(
        main,
        "run_calendar_sync",
        lambda config, timezone, day, list_id: processed_days.append(day),
    )
    monkeypatch.setattr(main, "run_due_card_triage", lambda config, timezone, day, board_id, list_id: None)

    def fake_save(file_path, statuses):
        saved_snapshots.append(dict(statuses))

    monkeypatch.setattr(main, "save_processed_date_statuses", fake_save)
    monkeypatch.setattr(main.Path, "exists", lambda _self: False)

    assert main.run() == 0
    assert processed_days == [date(2026, 4, 27)]
    assert len(saved_snapshots) >= 2


def test_run_backfills_missing_dates_from_status_file(monkeypatch, config, timezone):
    stored_statuses = {"2026-04-25": {"status": "success", "attempt_count": 1}}
    status_saves = []
    processed_days = []

    monkeypatch.setattr(main, "load_config", lambda: config)
    monkeypatch.setattr(main, "get_local_timezone", lambda: timezone)
    monkeypatch.setattr(main, "find_board_id", lambda config: "board-1")
    monkeypatch.setattr(main, "find_list_id", lambda config, board_id: "list-1")
    monkeypatch.setattr(main, "load_processed_date_statuses", lambda file_path: dict(stored_statuses))
    monkeypatch.setattr(
        main,
        "save_processed_date_statuses",
        lambda file_path, statuses: status_saves.append(dict(statuses)),
    )
    monkeypatch.setattr(main.Path, "exists", lambda _self: True)
    monkeypatch.setattr(
        main,
        "run_calendar_sync",
        lambda config, timezone, day, list_id: processed_days.append(day),
    )
    monkeypatch.setattr(main, "run_due_card_triage", lambda config, timezone, day, board_id, list_id: None)

    assert main.run() == 0
    assert processed_days == [date(2026, 4, 26), date(2026, 4, 27)]
    assert status_saves


def test_run_marks_failure_and_raises_sync_error(monkeypatch, config, timezone):
    saved_statuses = []

    monkeypatch.setattr(main, "load_config", lambda: config)
    monkeypatch.setattr(main, "get_local_timezone", lambda: timezone)
    monkeypatch.setattr(main, "find_board_id", lambda config: "board-1")
    monkeypatch.setattr(main, "find_list_id", lambda config, board_id: "list-1")
    monkeypatch.setattr(main, "load_processed_date_statuses", lambda file_path: {})
    monkeypatch.setattr(
        main,
        "save_processed_date_statuses",
        lambda file_path, statuses: saved_statuses.append(dict(statuses)),
    )
    monkeypatch.setattr(main.Path, "exists", lambda _self: False)
    monkeypatch.setattr(
        main,
        "run_calendar_sync",
        lambda config, timezone, day, list_id: (_ for _ in ()).throw(
            main.SyncError("boom")
        ),
    )
    monkeypatch.setattr(main, "run_due_card_triage", lambda config, timezone, day, board_id, list_id: None)

    with pytest.raises(main.SyncError, match="Daily automation failed"):
        main.run()

    assert saved_statuses
    latest_statuses = saved_statuses[-1]
    assert latest_statuses["2026-04-27"]["status"] == "failed"
