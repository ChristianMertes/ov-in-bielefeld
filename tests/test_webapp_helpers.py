"""Tests for webapp helper/formatting functions."""
from datetime import datetime, timedelta

import pytest

from webapp import (
    _next_showtime_label,
    _format_date_de,
    _format_time,
    _format_votes,
)

# Fixed reference point: Tuesday 2026-03-10 14:00
NOW = datetime(2026, 3, 10, 14, 0, 0)


def _dt(days_offset, hour=20, minute=0):
    """Return an ISO datetime string offset from NOW."""
    dt = NOW + timedelta(days=days_offset)
    return dt.replace(hour=hour, minute=minute, second=0).isoformat()


# ── _next_showtime_label ──────────────────────────────────────────────────────

def test_label_heute():
    assert _next_showtime_label(_dt(0, 20, 0), NOW) == "heute 20:00 Uhr"


def test_label_morgen():
    assert _next_showtime_label(_dt(1, 19, 30), NOW) == "morgen 19:30 Uhr"


def test_label_uebermorgen():
    assert _next_showtime_label(_dt(2, 18, 0), NOW) == "übermorgen 18:00 Uhr"


def test_label_this_week_thursday():
    # NOW is Tuesday 2026-03-10; +3 days = Friday 2026-03-13 (same week)
    assert _next_showtime_label(_dt(3), NOW) == "Freitag 20:00 Uhr"


def test_label_next_week():
    # +7 days from Tuesday = next Tuesday (different calendar week)
    result = _next_showtime_label(_dt(7), NOW)
    assert result == "nächste Woche Dienstag"


def test_label_next_week_boundary_sunday_to_monday():
    """Calendar-week correctness: Sunday March 15 (week of Mon Mar 9), showtime
    Monday March 23 (week of Mon Mar 23) → week_diff=2 → 'übernächste Woche'.
    The old day-delta code wrongly returned 'nächste Woche' because delta=8 < 14."""
    sunday = datetime(2026, 3, 15, 14, 0, 0)    # Sunday, week of Mon Mar 9
    monday_8d = datetime(2026, 3, 23, 20, 0, 0)  # Monday 8 days later, week of Mon Mar 23
    assert _next_showtime_label(monday_8d.isoformat(), sunday) == "übernächste Woche Montag"



def test_label_week_after_next():
    result = _next_showtime_label(_dt(14), NOW)
    assert result == "übernächste Woche Dienstag"


def test_label_in_n_weeks():
    result = _next_showtime_label(_dt(21), NOW)
    assert "3 Wochen" in result


def test_label_far_future_shows_date():
    """More than 4 weeks out should return 'DD. Monatsname' format."""
    far = datetime(2026, 9, 10, 20, 0, 0)
    result = _next_showtime_label(far.isoformat(), NOW)
    assert result == "10. September"


def test_label_invalid_datetime():
    assert _next_showtime_label("not-a-date", NOW) == ""


# ── _format_date_de ───────────────────────────────────────────────────────────

def test_format_date_de_wednesday():
    assert _format_date_de("2026-03-11T19:30:00") == "Mi, 11.03."


def test_format_date_de_saturday():
    assert _format_date_de("2026-03-14T21:00:00") == "Sa, 14.03."


def test_format_date_de_invalid():
    assert _format_date_de("bad") == "bad"


# ── _format_time ─────────────────────────────────────────────────────────────

def test_format_time():
    assert _format_time("2026-03-12T19:30:00") == "19:30"


def test_format_time_midnight():
    assert _format_time("2026-03-12T00:00:00") == "00:00"


def test_format_time_invalid():
    assert _format_time("bad") == "bad"


# ── _format_votes ─────────────────────────────────────────────────────────────

def test_format_votes_millions():
    assert _format_votes(1_234_567) == "1.2M"


def test_format_votes_exact_million():
    assert _format_votes(1_000_000) == "1.0M"


def test_format_votes_thousands():
    assert _format_votes(45_678) == "46K"


def test_format_votes_small():
    assert _format_votes(999) == "999"


def test_format_votes_none():
    assert _format_votes(None) == ""


def test_format_votes_zero():
    assert _format_votes(0) == ""
