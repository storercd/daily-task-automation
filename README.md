# Daily Task Automation

This script runs two daily routines against your `To Do` Trello board:

- It reads all events for the current local day from a Google Calendar iCal feed and creates Trello cards in the `Triage` list.
- It moves Trello cards with a due date of today or earlier into the `Triage` list.

Behavior:
- All-day events are imported.
- Timed events are still imported, but a warning is printed so the event can be corrected.
- Duplicate calendar cards are skipped based on a per-occurrence event marker.
- Open Trello cards with incomplete due dates of today or earlier are moved to `Triage`.

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

## Notes

- The script uses your machine's local timezone to decide what counts as "today".
- Google Calendar access should use the calendar's `Secret address in iCal format`, not the public URL.
- Created cards include a metadata marker in the description so reruns can skip duplicates reliably.
- A project-local `.venv` is recommended so scheduled runs and manual runs use the same interpreter.
- The top of [main.py](main.py) contains `RUN_CALENDAR_SYNC` and `RUN_DUE_CARD_TRIAGE` switches so either routine can be disabled while testing the other.
