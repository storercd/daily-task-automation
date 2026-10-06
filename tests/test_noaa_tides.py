from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from services import noaa_tides

TIMEZONE = ZoneInfo("America/Los_Angeles")


def test_is_within_waking_hours_includes_thirty_minute_buffer(monkeypatch):
    sunrise = datetime(2026, 4, 15, 6, 30, tzinfo=TIMEZONE)
    sunset = datetime(2026, 4, 15, 19, 45, tzinfo=TIMEZONE)
    monkeypatch.setattr(
        noaa_tides,
        "sun",
        lambda observer, date, tzinfo: {"sunrise": sunrise, "sunset": sunset},
    )

    assert noaa_tides.is_within_waking_hours(sunrise - timedelta(minutes=30), TIMEZONE)
    assert noaa_tides.is_within_waking_hours(sunset + timedelta(minutes=30), TIMEZONE)
    assert not noaa_tides.is_within_waking_hours(sunrise - timedelta(minutes=31), TIMEZONE)
    assert not noaa_tides.is_within_waking_hours(sunset + timedelta(minutes=31), TIMEZONE)


def test_is_within_waking_hours_uses_timestamp_date(monkeypatch):
    requested_dates = []

    def fake_sun(observer, date, tzinfo):
        requested_dates.append(date)
        return {
            "sunrise": datetime(2026, 4, 15, 6, 30, tzinfo=TIMEZONE),
            "sunset": datetime(2026, 4, 15, 19, 45, tzinfo=TIMEZONE),
        }

    monkeypatch.setattr(noaa_tides, "sun", fake_sun)

    noaa_tides.is_within_waking_hours(datetime(2026, 4, 15, 12, tzinfo=TIMEZONE), TIMEZONE)

    assert requested_dates == [date(2026, 4, 15)]
