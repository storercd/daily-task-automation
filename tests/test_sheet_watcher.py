"""Unit tests for SheetWatcherService: CSV parsing, hashing, snapshots, and diffing."""

from __future__ import annotations

from services.sheet_watcher import SheetWatcherService


def build_service() -> SheetWatcherService:
    return SheetWatcherService(http_client=None)


def test_parse_csv_rows_parses_simple_csv():
    service = build_service()

    rows = service.parse_csv_rows("name,date\nAlice,2026-01-01\nBob,2026-01-02\n")

    assert rows == [["name", "date"], ["Alice", "2026-01-01"], ["Bob", "2026-01-02"]]


def test_hash_content_is_stable_and_sensitive_to_change():
    service = build_service()

    first_hash = service.hash_content("a,b\n1,2\n")
    same_hash = service.hash_content("a,b\n1,2\n")
    different_hash = service.hash_content("a,b\n1,3\n")

    assert first_hash == same_hash
    assert first_hash != different_hash


def test_snapshot_round_trip(tmp_path):
    service = build_service()
    snapshot_path = service.snapshot_path(tmp_path, "Choir Schedule")

    assert service.load_snapshot(snapshot_path) is None

    service.save_snapshot(snapshot_path, "name,date\nAlice,2026-01-01\n")

    assert snapshot_path.name == "choir-schedule.csv"
    assert service.load_snapshot(snapshot_path) == "name,date\nAlice,2026-01-01\n"


def test_diff_rows_detects_added_row():
    service = build_service()
    old_rows = [["name", "date"], ["Alice", "2026-01-01"]]
    new_rows = [["name", "date"], ["Alice", "2026-01-01"], ["Bob", "2026-01-02"]]

    changes = service.diff_rows(old_rows, new_rows)

    assert len(changes) == 1
    assert changes[0].kind == "added"
    assert changes[0].new_row == ["Bob", "2026-01-02"]


def test_diff_rows_detects_removed_row():
    service = build_service()
    old_rows = [["name", "date"], ["Alice", "2026-01-01"], ["Bob", "2026-01-02"]]
    new_rows = [["name", "date"], ["Alice", "2026-01-01"]]

    changes = service.diff_rows(old_rows, new_rows)

    assert len(changes) == 1
    assert changes[0].kind == "removed"
    assert changes[0].old_row == ["Bob", "2026-01-02"]


def test_diff_rows_detects_modified_cell():
    service = build_service()
    old_rows = [["name", "date"], ["Alice", "2026-01-01"]]
    new_rows = [["name", "date"], ["Alice", "2026-02-15"]]

    changes = service.diff_rows(old_rows, new_rows)

    assert len(changes) == 1
    assert changes[0].kind == "modified"
    assert changes[0].cell_changes == [(1, "2026-01-01", "2026-02-15")]


def test_format_change_summary_includes_row_details():
    service = build_service()
    old_rows = [["name", "date"], ["Alice", "2026-01-01"]]
    new_rows = [["name", "date"], ["Alice", "2026-02-15"]]
    changes = service.diff_rows(old_rows, new_rows)

    summary = service.format_change_summary("Choir Schedule", changes)

    assert "Detected 1 row change(s) in 'Choir Schedule'" in summary
    assert "Row 2 modified" in summary
    assert "2026-01-01" in summary and "2026-02-15" in summary
