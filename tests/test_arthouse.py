"""Tests for the Arthouse scraper using a real HTML fixture."""
import re
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from scrapers.arthouse import (
    _parse_german_date,
    _parse_film_block,
    scrape_arthouse,
)
from bs4 import BeautifulSoup


# ── _parse_german_date ────────────────────────────────────────────────────────

def test_parse_german_date_heute():
    result = _parse_german_date("Heute", "21:00")
    dt = datetime.fromisoformat(result)
    assert dt.date() == datetime.now().date()
    assert dt.hour == 21
    assert dt.minute == 0


def test_parse_german_date_morgen():
    result = _parse_german_date("Morgen", "19:30")
    dt = datetime.fromisoformat(result)
    assert dt.date() == (datetime.now() + timedelta(days=1)).date()
    assert dt.hour == 19
    assert dt.minute == 30


def test_parse_german_date_weekday_format():
    result = _parse_german_date("Do, 12.03", "20:00")
    dt = datetime.fromisoformat(result)
    assert dt.month == 3
    assert dt.day == 12
    assert dt.hour == 20
    assert dt.minute == 0


def test_parse_german_date_single_digit_day():
    result = _parse_german_date("Fr, 7.03", "18:00")
    dt = datetime.fromisoformat(result)
    assert dt.month == 3
    assert dt.day == 7


def test_parse_german_date_unknown_falls_back_to_today():
    result = _parse_german_date("Unbekannt", "15:00")
    dt = datetime.fromisoformat(result)
    assert dt.date() == datetime.now().date()
    assert dt.hour == 15


# ── Fixture-based scraper tests ───────────────────────────────────────────────

@pytest.fixture(scope="module")
def scraped_films(arthouse_html):
    """Run the scraper against the real HTML fixture (no network calls)."""
    mock_resp = MagicMock()
    mock_resp.text = arthouse_html
    mock_resp.raise_for_status = MagicMock()
    mock_resp.encoding = "utf-8"

    with patch("scrapers.arthouse.requests.get", return_value=mock_resp), \
         patch("scrapers.arthouse._fetch_film_detail", return_value={}):
        return scrape_arthouse()


def test_scrape_returns_films(scraped_films):
    assert len(scraped_films) > 0


def test_scrape_only_ov_omu_films(scraped_films):
    """Every returned film must have at least one OmU/OV showtime."""
    for film in scraped_films:
        assert film["showtimes"], f"Film '{film['title_display']}' has no showtimes"
        for st in film["showtimes"]:
            assert st["language_tag"] in ("OmU", "OV"), (
                f"Unexpected tag '{st['language_tag']}' in '{film['title_display']}'"
            )


def test_scrape_known_film_present(scraped_films):
    titles = {f["title_display"] for f in scraped_films}
    assert "Blood & Sinners" in titles


def test_scrape_prefix_stripped(scraped_films):
    """CINÉMA_FRANÇAIS: prefix must be removed from the display title."""
    titles = {f["title_display"] for f in scraped_films}
    for t in titles:
        assert not t.startswith("CINÉMA_FRANÇAIS"), f"Prefix not stripped: {t}"
    # At least one French film should be present with a clean title
    assert any("La brigade" in t or "La petite dernière" in t for t in titles)


def test_scrape_cinema_detection(scraped_films):
    """Showtimes must be attributed to lichtwerk or kamera."""
    all_cinemas = {st["cinema"] for f in scraped_films for st in f["showtimes"]}
    assert all_cinemas <= {"lichtwerk", "kamera"}
    # Fixture contains both cinemas
    assert "lichtwerk" in all_cinemas
    assert "kamera" in all_cinemas


def test_scrape_showtimes_are_iso_datetimes(scraped_films):
    for film in scraped_films:
        for st in film["showtimes"]:
            try:
                datetime.fromisoformat(st["showtime"])
            except ValueError:
                pytest.fail(
                    f"Invalid ISO datetime '{st['showtime']}' in '{film['title_display']}'"
                )


def test_scrape_booking_urls_present(scraped_films):
    """Every showtime should have a kinoheld booking URL."""
    for film in scraped_films:
        for st in film["showtimes"]:
            assert "kinoheld.de" in (st.get("booking_url") or ""), (
                f"Missing booking URL for '{film['title_display']}'"
            )
