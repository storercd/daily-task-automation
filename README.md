# Daily Task Automation

This script runs daily Trello routines, a monthly low-tide routine, and two watch routines:

- It reads all events for the current local day from a Google Calendar iCal feed and creates Trello cards in the `Triage` list.
- It moves Trello cards with a due date of today or earlier into the `Triage` list.
- It fetches NOAA monthly high/low tide predictions for Everett, WA (station `9447659`), finds low tides below `0.00` feet, and creates one-hour Google Calendar events for those times.
- It checks configured, publicly-readable Google Sheets for row/cell changes since the last check, and creates a Trello alert card describing what changed.
- It checks configured plain web pages for visible-text changes since the last check, and creates a Trello alert card describing what changed.

Behavior:
- All-day events are imported.
- Timed events are still imported, but a warning is printed so the event can be corrected.
- Duplicate calendar cards are skipped based on a per-occurrence event marker.
- Open Trello cards with incomplete due dates of today or earlier are moved to `Triage`.
- Each run records per-date status in `logs/processed_dates.json`.
- If previous dates were missed or failed, the next run backfills those dates automatically before completing today.
- On first run (when the status file does not exist), the status file is created and only today is processed.
- If any routine (`daily`, `monthly`, `watch`, or `watch-web`) fails, a Trello alert card identifying the failing job is created (deduplicated per job per day) so it can be investigated.

## Setup

1. Create a virtual environment.
2. Activate it.
3. Install dependencies from `requirements.txt`.
4. Copy `.env.example` to `.env`.
5. Fill in your Google Calendar secret iCal URL, Trello API key, and Trello token.

## Run

```bash
source .venv/bin/activate
python main.py
```

Run monthly routine explicitly:

```bash
source .venv/bin/activate
python main.py monthly
```

Monthly routine configuration (`.env`):

- `NOAA_STATION_ID` (default `9447659`)
- `LOW_TIDE_CALENDAR_ID` (Google Calendar ID where events are created)
- OAuth for event creation:
	- Option A: `GOOGLE_OAUTH_ACCESS_TOKEN`
	- Option B (recommended for automation): `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`, `GOOGLE_OAUTH_REFRESH_TOKEN`
	- Optional override: `GOOGLE_OAUTH_TOKEN_URL` (defaults to `https://oauth2.googleapis.com/token`)

Run sheet-watch routine explicitly:

```bash
source .venv/bin/activate
python main.py watch
```

Sheet-watch routine configuration (`.env`):

- Uses the same `TRELLO_API_KEY`, `TRELLO_API_TOKEN`, `TRELLO_BOARD_NAME`, and `TRELLO_LIST_NAME` as the daily routine.
- `SHEET_WATCHERS`: a JSON array of watched sheet tabs, e.g. `[{"name": "Choir Schedule", "spreadsheet_id": "<id>", "gid": "0"}]`.
  - Each sheet must be shared as "Anyone with the link" (view only) so it can be fetched via the public CSV export endpoint.
  - `spreadsheet_id` and `gid` come from the sheet's URL: `.../spreadsheets/d/<spreadsheet_id>/edit?gid=<gid>`.
- On the first run for a source, only a baseline snapshot is recorded to `state/sheet_watchers/<name>.csv`; no alert is sent.
- Subsequent runs diff the new CSV against the stored snapshot (row-level and cell-level) and create a Trello card in `TRELLO_LIST_NAME` describing the change, deduplicated by a content-hash marker.
- Alerting is decoupled from detection (see `core/notifiers.py`), so a non-Trello notifier could be added later without changing the fetch/diff logic.

Run web-page-watch routine explicitly:

```bash
source .venv/bin/activate
python main.py watch-web
```

Web-page-watch routine configuration (`.env`):

- Uses the same Trello settings as the sheet-watch routine.
- `WEB_PAGE_WATCHERS`: a JSON array of watched pages, e.g. `[{"name": "Competition Schedule", "url": "https://..."}]`.
- At least one of `SHEET_WATCHERS` or `WEB_PAGE_WATCHERS` must be configured.
- The page's HTML is reduced to visible text (script/style content stripped), then diffed line-by-line, reusing the same row-diff logic as the sheet watcher (each line is treated as a one-column row).
- Snapshots are stored in `state/web_page_watchers/<name>.txt`.
- To avoid needlessly re-fetching an unchanged page, requests include `If-None-Match`/`If-Modified-Since` based on the previously observed `ETag`/`Last-Modified` response headers (stored in a `.meta.json` sidecar file). A server that supports conditional requests responds `304 Not Modified` instead of resending the page, so this routine can run more frequently than the sheet-watch routine without generating extra load once nothing has changed. This is why `watch-web` has its own, more frequent LaunchAgent schedule (see below) separate from `watch`.

## Test

Install dev dependencies and run the unit test suite:

```bash
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest
```

## Schedule Daily With launchd

This repository includes a macOS LaunchAgent that runs the script every day at 5:00 AM local time.

Files:
- `scripts/run_daily_task_automation.sh`
- `launchd/com.storercd.daily-task-automation.plist`

Install and load it:

```bash
mkdir -p logs ~/Library/LaunchAgents
chmod +x scripts/run_daily_task_automation.sh
cp launchd/com.storercd.daily-task-automation.plist ~/Library/LaunchAgents/
launchctl bootout gui/"$(id -u)" ~/Library/LaunchAgents/com.storercd.daily-task-automation.plist 2>/dev/null || true
launchctl bootstrap gui/"$(id -u)" ~/Library/LaunchAgents/com.storercd.daily-task-automation.plist
launchctl enable gui/"$(id -u)"/com.storercd.daily-task-automation
```

Useful commands:

```bash
launchctl print gui/"$(id -u)"/com.storercd.daily-task-automation
launchctl kickstart -k gui/"$(id -u)"/com.storercd.daily-task-automation
tail -f logs/launchd.stdout.log logs/launchd.stderr.log
```

To change the schedule, edit `Hour` and `Minute` in the plist, copy it back into `~/Library/LaunchAgents/`, then run the `bootout` and `bootstrap` commands again.

The sheet-watch routine has its own LaunchAgent, kept separate from the daily job so its
frequency can be changed independently later. It currently runs once a day at 6:00 AM:

Files:
- `scripts/run_watch_task_automation.sh`
- `launchd/com.storercd.watch-task-automation.plist`

```bash
mkdir -p state ~/Library/LaunchAgents
chmod +x scripts/run_watch_task_automation.sh
cp launchd/com.storercd.watch-task-automation.plist ~/Library/LaunchAgents/
launchctl bootout gui/"$(id -u)" ~/Library/LaunchAgents/com.storercd.watch-task-automation.plist 2>/dev/null || true
launchctl bootstrap gui/"$(id -u)" ~/Library/LaunchAgents/com.storercd.watch-task-automation.plist
launchctl enable gui/"$(id -u)"/com.storercd.watch-task-automation
```

To run it more often later (e.g. hourly), switch its `StartCalendarInterval` for a
`StartInterval` (seconds) in the plist, then repeat the `bootout`/`bootstrap` commands above.

The web-page-watch routine also has its own LaunchAgent. It runs every 15 minutes
(`StartInterval` of 900 seconds) since conditional GETs make frequent, unchanged checks
cheap for the origin server:

Files:
- `scripts/run_watch_web_task_automation.sh`
- `launchd/com.storercd.watch-web-task-automation.plist`

```bash
mkdir -p state ~/Library/LaunchAgents
chmod +x scripts/run_watch_web_task_automation.sh
cp launchd/com.storercd.watch-web-task-automation.plist ~/Library/LaunchAgents/
launchctl bootout gui/"$(id -u)" ~/Library/LaunchAgents/com.storercd.watch-web-task-automation.plist 2>/dev/null || true
launchctl bootstrap gui/"$(id -u)" ~/Library/LaunchAgents/com.storercd.watch-web-task-automation.plist
launchctl enable gui/"$(id -u)"/com.storercd.watch-web-task-automation
```

## Notes

- The script uses your machine's local timezone to decide what counts as "today".
- Google Calendar access should use the calendar's `Secret address in iCal format`, not the public URL.
- Created cards include a metadata marker in the description so reruns can skip duplicates reliably.
- A project-local `.venv` is recommended so scheduled runs and manual runs use the same interpreter.
- The top of [main.py](main.py) contains `RUN_CALENDAR_SYNC` and `RUN_DUE_CARD_TRIAGE` switches so either routine can be disabled while testing the other.
