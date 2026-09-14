"""Tests for sneak preview detection and release-date mapping."""
from datetime import date

import pytest

from sneak import is_sneak_title, official_release_date, sneak_language

# ── is_sneak_title ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("title", [
    "SNEAK PREVIEW",
    "SNEAK PREVIEW - English Edition",
    "Sneak Preview",
    "Sneak OV",
    "sneak",
    "SNEAK PREVIEW OmU",
    "Sneak-Preview",
    "Kamera: Sneak Preview",
])
def test_sneak_titles_recognised(title):
    assert is_sneak_title(title)


@pytest.mark.parametrize("title", [
    "Sneakers",
    "The Sneaky Life of Maxwell Silverbear",
    "Inception",
    "Wallace & Gromit: Sneaking Around",
    "",
])
def test_regular_titles_not_mistaken_for_sneaks(title):
    assert not is_sneak_title(title)


# ── sneak_language ────────────────────────────────────────────────────────────

def test_language_from_english_edition():
    assert sneak_language("SNEAK PREVIEW - English Edition") == "en"


def test_language_from_german_word_for_english():
    assert sneak_language("Sneak Preview (Englische Fassung)") == "en"


def test_language_from_french_marker():
    assert sneak_language("Sneak Preview - Französische Fassung") == "fr"


def test_language_unknown_without_marker():
    assert sneak_language("Sneak OV") is None


# ── official_release_date ─────────────────────────────────────────────────────

def test_wednesday_sneak_points_to_next_day():
    # Wed 2026-09-16 → Thu 2026-09-17
    assert official_release_date(date(2026, 9, 16)) == date(2026, 9, 17)


def test_monday_sneak_points_to_same_week_thursday():
    assert official_release_date(date(2026, 9, 14)) == date(2026, 9, 17)


def test_thursday_sneak_looks_ahead_a_week():
    assert official_release_date(date(2026, 9, 17)) == date(2026, 9, 24)


def test_friday_sneak_points_to_following_thursday():
    assert official_release_date(date(2026, 9, 18)) == date(2026, 9, 24)


def test_release_date_is_always_a_thursday():
    for day in range(1, 29):
        assert official_release_date(date(2026, 9, day)).weekday() == 3
