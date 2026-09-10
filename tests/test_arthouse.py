"""Tests for the Arthouse scraper using a real HTML fixture."""
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from scrapers.arthouse import _fix_mojibake, _parse_german_date, scrape_arthouse

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


# ── _fix_mojibake ─────────────────────────────────────────────────────────────
# The programme page serves proper UTF-8 for accented letters but raw
# Windows-1252 bytes for punctuation, which decode to invisible C1 control
# characters. They break TMDb title lookups, so they must be mapped back.

def test_fix_mojibake_apostrophe():
    assert _fix_mojibake("L\x92étranger") == "L’étranger"


def test_fix_mojibake_en_dash():
    assert _fix_mojibake("La brigade \x96 Die Küchenbrigade") == (
        "La brigade – Die Küchenbrigade"
    )


def test_fix_mojibake_leaves_clean_text_untouched():
    clean = "Bir kar tanesinin ömrü – Die Lebensdauer einer Schneeflocke"
    assert _fix_mojibake(clean) == clean


def test_fix_mojibake_keeps_accented_letters():
    assert _fix_mojibake("CINÉMA_FRANÇAIS: Ma mère") == "CINÉMA_FRANÇAIS: Ma mère"


def test_fix_mojibake_handles_unassigned_c1_codepoints():
    """0x81/0x8d/0x8f/0x90/0x9d have no Windows-1252 meaning; leave them as-is."""
    assert _fix_mojibake("a\x81b") == "a\x81b"


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


def test_scrape_titles_have_no_control_characters(scraped_films):
    """No scraped title may contain C1 control characters (mojibake)."""
    for film in scraped_films:
        for key in ("title_display", "title_raw"):
            bad = [ch for ch in film[key] if 0x80 <= ord(ch) <= 0x9F]
            assert not bad, (
                f"Control chars {[hex(ord(c)) for c in bad]} in {key} {film[key]!r}"
            )


def test_scrape_repairs_double_title_separator(scraped_films):
    """A combined 'Original – Deutsch' title must use a real dash, not \\x96."""
    titles = {f["title_display"] for f in scraped_films}
    combined = [t for t in titles if "Die Küchenbrigade" in t]
    assert combined, f"Expected the French double title in {len(titles)} titles"
    assert "–" in combined[0]


def test_scrape_booking_urls_present(scraped_films):
    """Every showtime should have a kinoheld booking URL."""
    for film in scraped_films:
        for st in film["showtimes"]:
            assert "kinoheld.de" in (st.get("booking_url") or ""), (
                f"Missing booking URL for '{film['title_display']}'"
            )
