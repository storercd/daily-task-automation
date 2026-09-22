from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest
import requests

import main
from services import google_calendar as google_calendar_module
from services import http_client as http_client_module


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
    monkeypatch.setattr(
        google_calendar_module.recurring_ical_events,
        "of",
        lambda calendar: FakeRecurringCalendar(components),
    )

    events, warnings = main.parse_events_for_today(object(), today, timezone)

    assert [event.summary for event in events] == ["All Day", "Timed"]
    assert events[0].is_all_day is True
    assert events[1].is_all_day is False
    assert events[0].description == "Desc"
    assert "Event is not all-day and should be corrected: Timed" in warnings
    assert "Skipped event with no UID: Missing UID" in warnings
    assert "Skipped event with no summary for UID missing-summary" in warnings


def test_fetch_calendar_raises_helpful_message_for_google_404(monkeypatch):
    monkeypatch.setattr(
        main.HTTP_CLIENT,
        "request_with_backoff",
        lambda method, url, **kwargs: FakeResponse(status_code=404),
    )

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

    monkeypatch.setattr(http_client_module.requests, "request", fake_request)
    monkeypatch.setattr(http_client_module.time_module, "sleep", sleeps.append)

    response = main.request_with_backoff("GET", "https://example.com")

    assert response.status_code == 200
    assert len(attempts) == 3
    assert sleeps == [2, 4]


def test_request_with_backoff_retries_retryable_status_codes(monkeypatch):
    responses = [FakeResponse(status_code=503), FakeResponse(status_code=200)]
    sleeps = []

    monkeypatch.setattr(
        http_client_module.requests,
        "request",
        lambda method, url, timeout, **kwargs: responses.pop(0),
    )
    monkeypatch.setattr(http_client_module.time_module, "sleep", sleeps.append)

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

    monkeypatch.setattr(main.TRELLO_SERVICE, "request", fake_trello_request)
    monkeypatch.setattr(main.TRELLO_SERVICE, "card_exists_for_event", lambda config, list_id, event_key: False)
    monkeypatch.setattr(http_client_module.time_module, "sleep", sleeps.append)

    created_new_card = main.create_card(config, "list-1", sample_event)

    assert created_new_card is True
    assert call_count["count"] == 2
    assert sleeps == [2]


def test_create_card_recovers_when_card_exists_after_failure(monkeypatch, config, sample_event, capsys):
    def fake_trello_request(method, path, api_key, api_token, allow_retries=True, **kwargs):
        raise requests.ConnectionError("reset")

    monkeypatch.setattr(main.TRELLO_SERVICE, "request", fake_trello_request)
    monkeypatch.setattr(main.TRELLO_SERVICE, "card_exists_for_event", lambda config, list_id, event_key: True)

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

    assert main.run_daily() == 0
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

    assert main.run_daily() == 0
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

    assert main.run_daily() == 0
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
        main.run_daily()

    assert saved_statuses
    latest_statuses = saved_statuses[-1]
    assert latest_statuses["2026-04-27"]["status"] == "failed"


def test_run_monthly_creates_and_skips_existing_events(monkeypatch, timezone, capsys):
    monthly_config = main.MonthlyConfig(
        noaa_station_id="9447659",
        target_calendar_id="calendar-id",
        google_oauth_access_token="access-token",
        google_oauth_client_id="",
        google_oauth_client_secret="",
        google_oauth_refresh_token="",
        google_oauth_token_url="https://oauth2.googleapis.com/token",
    )
    month_start = date(2026, 4, 1)
    month_end = date(2026, 4, 30)
    prediction_one = main.LowTidePrediction(
        timestamp=datetime(2026, 4, 7, 6, 5, tzinfo=timezone),
        height_feet=-1.53,
    )
    prediction_two = main.LowTidePrediction(
        timestamp=datetime(2026, 4, 9, 7, 15, tzinfo=timezone),
        height_feet=-0.12,
    )
    existing_marker = main.build_low_tide_marker(monthly_config.noaa_station_id, prediction_two)
    created_events = []

    monkeypatch.setattr(main, "load_monthly_config", lambda: monthly_config)
    monkeypatch.setattr(main, "get_local_timezone", lambda: timezone)
    monkeypatch.setattr(main, "build_month_bounds", lambda current_date: (month_start, month_end))
    monkeypatch.setattr(
        main.NOAA_TIDE_SERVICE,
        "fetch_negative_low_tides",
        lambda station_id, month_start, month_end, timezone: [prediction_one, prediction_two],
    )
    monkeypatch.setattr(main.GOOGLE_CALENDAR_EVENT_SERVICE, "get_access_token", lambda config: "token")
    monkeypatch.setattr(
        main.GOOGLE_CALENDAR_EVENT_SERVICE,
        "load_existing_event_markers",
        lambda config, access_token, window_start, window_end: {existing_marker},
    )
    monkeypatch.setattr(
        main.GOOGLE_CALENDAR_EVENT_SERVICE,
        "create_event",
        lambda config, access_token, summary, start_at, end_at, marker, timezone_name: created_events.append(
            (summary, start_at, end_at, marker, timezone_name)
        ),
    )

    assert main.run_monthly() == 0

    assert len(created_events) == 1
    assert created_events[0][0] == "Low Tide: -1.53"
    assert created_events[0][1] == prediction_one.timestamp
    assert created_events[0][2] == prediction_one.timestamp + main.MONTHLY_EVENT_DURATION

    output = capsys.readouterr().out
    assert "1 created, 1 skipped" in output


def test_run_monthly_returns_zero_when_no_negative_low_tides(monkeypatch, timezone, capsys):
    monthly_config = main.MonthlyConfig(
        noaa_station_id="9447659",
        target_calendar_id="calendar-id",
        google_oauth_access_token="access-token",
        google_oauth_client_id="",
        google_oauth_client_secret="",
        google_oauth_refresh_token="",
        google_oauth_token_url="https://oauth2.googleapis.com/token",
    )
    month_start = date(2026, 4, 1)
    month_end = date(2026, 4, 30)

    monkeypatch.setattr(main, "load_monthly_config", lambda: monthly_config)
    monkeypatch.setattr(main, "get_local_timezone", lambda: timezone)
    monkeypatch.setattr(main, "build_month_bounds", lambda current_date: (month_start, month_end))
    monkeypatch.setattr(
        main.NOAA_TIDE_SERVICE,
        "fetch_negative_low_tides",
        lambda station_id, month_start, month_end, timezone: [],
    )

    assert main.run_monthly() == 0
    output = capsys.readouterr().out
    assert "No negative low tides found" in output


@pytest.fixture
def watch_config() -> main.WatchConfig:
    return main.WatchConfig(
        trello_api_key="key",
        trello_api_token="token",
        trello_board_name="To Do",
        trello_list_name="Watch",
        sources=[main.SheetSource(name="Choir Schedule", spreadsheet_id="sheet-1", gid="0")],
    )


def test_parse_sheet_sources_parses_json():
    sources = main.parse_sheet_sources(
        '[{"name": "Choir Schedule", "spreadsheet_id": "abc123", "gid": "42"}]'
    )

    assert sources == [main.SheetSource(name="Choir Schedule", spreadsheet_id="abc123", gid="42")]


def test_parse_sheet_sources_defaults_gid_to_zero():
    sources = main.parse_sheet_sources('[{"name": "Choir Schedule", "spreadsheet_id": "abc123"}]')

    assert sources[0].gid == "0"


def test_parse_sheet_sources_raises_for_invalid_json():
    with pytest.raises(main.SyncError, match="valid JSON"):
        main.parse_sheet_sources("not-json")


def test_parse_sheet_sources_raises_for_missing_fields():
    with pytest.raises(main.SyncError, match="requires 'name'"):
        main.parse_sheet_sources('[{"name": "Choir Schedule"}]')


def test_load_watch_config_raises_for_missing_environment_variables(monkeypatch):
    monkeypatch.delenv("SHEET_WATCHERS", raising=False)
    monkeypatch.setenv("TRELLO_API_KEY", "key")
    monkeypatch.setenv("TRELLO_API_TOKEN", "token")
    monkeypatch.setenv("TRELLO_BOARD_NAME", "To Do")
    monkeypatch.setenv("TRELLO_LIST_NAME", "Watch")
    monkeypatch.setattr(main, "load_dotenv", lambda: None)

    with pytest.raises(main.SyncError, match="SHEET_WATCHERS"):
        main.load_watch_config()


def test_check_sheet_source_records_initial_snapshot_without_alert(monkeypatch, tmp_path, watch_config, capsys):
    source = watch_config.sources[0]
    notified = []

    monkeypatch.setattr(main, "SHEET_WATCH_SNAPSHOT_DIR", tmp_path)
    monkeypatch.setattr(main, "fetch_sheet_csv", lambda src: "name,date\nAlice,2026-01-01\n")

    class FakeNotifier:
        def notify_change(self, source_name, changes, content_hash):
            notified.append((source_name, changes, content_hash))

    main.check_sheet_source(source, FakeNotifier())

    assert notified == []
    assert "Recorded initial snapshot" in capsys.readouterr().out
    assert main.sheet_snapshot_path(source.name).read_text() == "name,date\nAlice,2026-01-01\n"


def test_check_sheet_source_notifies_on_change(monkeypatch, tmp_path, watch_config, capsys):
    source = watch_config.sources[0]
    snapshot_path = tmp_path / "choir-schedule.csv"
    snapshot_path.write_text("name,date\nAlice,2026-01-01\n")
    notified = []

    monkeypatch.setattr(main, "SHEET_WATCH_SNAPSHOT_DIR", tmp_path)
    monkeypatch.setattr(main, "fetch_sheet_csv", lambda src: "name,date\nAlice,2026-02-15\n")

    class FakeNotifier:
        def notify_change(self, source_name, changes, content_hash):
            notified.append((source_name, changes, content_hash))

    main.check_sheet_source(source, FakeNotifier())

    assert len(notified) == 1
    assert notified[0][0] == "Choir Schedule"
    assert notified[0][1][0].kind == "modified"
    assert snapshot_path.read_text() == "name,date\nAlice,2026-02-15\n"
    assert "Detected 1 row change(s)" in capsys.readouterr().out


def test_check_sheet_source_skips_alert_when_unchanged(monkeypatch, tmp_path, watch_config, capsys):
    source = watch_config.sources[0]
    (tmp_path / "choir-schedule.csv").write_text("name,date\nAlice,2026-01-01\n")

    monkeypatch.setattr(main, "SHEET_WATCH_SNAPSHOT_DIR", tmp_path)
    monkeypatch.setattr(main, "fetch_sheet_csv", lambda src: "name,date\nAlice,2026-01-01\n")

    class FakeNotifier:
        def notify_change(self, source_name, changes, content_hash):
            raise AssertionError("notify_change should not be called when content is unchanged")

    main.check_sheet_source(source, FakeNotifier())

    assert "No changes detected" in capsys.readouterr().out


def test_check_sheet_source_skips_alert_when_raw_text_drifts_but_rows_match(
    monkeypatch, tmp_path, watch_config, capsys
):
    source = watch_config.sources[0]
    snapshot_path = tmp_path / "choir-schedule.csv"
    snapshot_path.write_text("name,date\nAlice,2026-01-01\n")

    monkeypatch.setattr(main, "SHEET_WATCH_SNAPSHOT_DIR", tmp_path)
    monkeypatch.setattr(main, "fetch_sheet_csv", lambda src: 'name,date\n"Alice",2026-01-01\n')

    class FakeNotifier:
        def notify_change(self, source_name, changes, content_hash):
            raise AssertionError("notify_change should not be called when no rows actually changed")

    main.check_sheet_source(source, FakeNotifier())

    assert "No changes detected" in capsys.readouterr().out
    assert snapshot_path.read_text() == 'name,date\n"Alice",2026-01-01\n'


def test_run_watch_raises_and_continues_after_source_failure(monkeypatch, watch_config):
    working_source = main.SheetSource(name="Working Sheet", spreadsheet_id="sheet-2", gid="0")
    watch_config.sources.append(working_source)
    checked = []

    monkeypatch.setattr(main, "load_watch_config", lambda: watch_config)
    monkeypatch.setattr(main, "find_board_id", lambda config: "board-1")
    monkeypatch.setattr(main, "find_list_id", lambda config, board_id: "list-1")

    def fake_check(source, notifier):
        if source.name == "Choir Schedule":
            raise main.SyncError("fetch failed")
        checked.append(source.name)

    monkeypatch.setattr(main, "check_sheet_source", fake_check)

    with pytest.raises(main.SyncError, match="Choir Schedule"):
        main.run_watch()

    assert checked == ["Working Sheet"]


def test_notify_job_failure_creates_alert_card(monkeypatch, config, timezone):
    created_cards = []

    monkeypatch.setattr(main, "load_config", lambda: config)
    monkeypatch.setattr(main, "get_local_timezone", lambda: timezone)
    monkeypatch.setattr(main, "find_board_id", lambda config: "board-1")
    monkeypatch.setattr(main, "find_list_id", lambda config, board_id: "list-1")
    monkeypatch.setattr(
        main.TRELLO_SERVICE,
        "create_alert_card",
        lambda config, list_id, name, description, marker: created_cards.append((name, description, marker)),
    )

    main.notify_job_failure("watch", main.SyncError("boom"))

    assert len(created_cards) == 1
    assert "watch" in created_cards[0][0]
    assert "boom" in created_cards[0][1]


def test_run_routine_with_failure_alert_sends_alert_and_reraises(monkeypatch):
    alerts = []
    monkeypatch.setattr(main, "notify_job_failure", lambda job_name, error: alerts.append((job_name, error)))

    def failing_routine():
        raise main.SyncError("boom")

    with pytest.raises(main.SyncError, match="boom"):
        main.run_routine_with_failure_alert("watch", failing_routine)

    assert alerts[0][0] == "watch"


def test_run_routine_with_failure_alert_returns_result_on_success(monkeypatch):
    monkeypatch.setattr(main, "notify_job_failure", lambda job_name, error: pytest.fail("should not be called"))

    assert main.run_routine_with_failure_alert("watch", lambda: 0) == 0
