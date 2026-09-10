"""Tests for cinema-local time handling.

Showtimes are stored as naive wall-clock time in the cinemas' timezone, so
every comparison against "now" must use that timezone - regardless of how the
server is configured. Containers default to UTC, which is exactly the case
these tests pin down.
"""
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from timeutil import CINEMA_TZ, now_local

needs_tzset = pytest.mark.skipif(
    not hasattr(time, "tzset"), reason="TZ switching requires a Unix platform"
)


@pytest.fixture
def server_tz(monkeypatch):
    """Run the process in an arbitrary server timezone, restoring it afterwards."""
    def _set(name: str) -> None:
        monkeypatch.setenv("TZ", name)
        time.tzset()

    yield _set
    time.tzset()  # monkeypatch restored TZ; re-read it


def _berlin_wall_clock() -> datetime:
    return datetime.now(ZoneInfo("Europe/Berlin")).replace(tzinfo=None)


# ── now_local is independent of the server timezone ──────────────────────────

@needs_tzset
def test_now_local_is_berlin_wall_clock_on_utc_server(server_tz):
    server_tz("UTC")
    assert abs(now_local() - _berlin_wall_clock()) < timedelta(seconds=5)


@needs_tzset
def test_now_local_differs_from_naive_now_on_utc_server(server_tz):
    """The whole point: datetime.now() is NOT usable for showtime comparisons."""
    server_tz("UTC")
    offset = _berlin_wall_clock() - datetime.now()
    assert offset >= timedelta(minutes=59), "Berlin is ahead of UTC year-round"
    assert abs(now_local() - datetime.now()) >= timedelta(minutes=59)


@needs_tzset
def test_now_local_unaffected_by_a_far_away_server_timezone(server_tz):
    server_tz("America/New_York")
    assert abs(now_local() - _berlin_wall_clock()) < timedelta(seconds=5)


@needs_tzset
def test_now_local_is_naive(server_tz):
    """Naive on purpose - it is compared against naive showtime strings."""
    server_tz("UTC")
    assert now_local().tzinfo is None


# ── daylight saving time ─────────────────────────────────────────────────────

def test_cinema_tz_summer_offset_is_two_hours():
    assert datetime(2026, 7, 1, 12, tzinfo=CINEMA_TZ).utcoffset() == timedelta(hours=2)


def test_cinema_tz_winter_offset_is_one_hour():
    assert datetime(2026, 1, 15, 12, tzinfo=CINEMA_TZ).utcoffset() == timedelta(hours=1)
