"""Tests for scrape_cinemaxx() and scrape_arthouse() with mocked HTTP responses.

These tests verify the full scraper flow (session setup, API calls, parsing)
without requiring network access, using realistic fixture data.
"""
from unittest.mock import MagicMock, patch

import pytest
import requests

from scrapers.arthouse import scrape_arthouse
from scrapers.cinemaxx import scrape_cinemaxx

# ═════════════════════════════════════════════════════════════════════════════
# CinemaxX fixtures
# ═════════════════════════════════════════════════════════════════════════════

CINEMAXX_FILMS_RESPONSE = {
    "result": [
        {
            "filmId": "F100",
            "filmTitle": "The Brutalist",
            "originalTitle": "The Brutalist",
            "sessionAttributes": [
                {"attributeType": "Language", "value": "english", "name": "Englisch", "shortName": "EN"},
            ],
            "filmAttributes": [],
            "runningTime": 215,
            "releaseDate": "2025-02-20",
            "posterImageSrc": "https://img.example.com/brutalist.jpg",
            "filmUrl": "/filme/the-brutalist",
            "genres": ["Drama"],
        },
        {
            "filmId": "F200",
            "filmTitle": "Normaler Film",
            "originalTitle": "Normaler Film",
            "sessionAttributes": [],
            "filmAttributes": [],
            "runningTime": 90,
            "releaseDate": "2025-03-01",
            "posterImageSrc": None,
            "filmUrl": "",
            "genres": [],
        },
        {
            "filmId": "F300",
            "filmTitle": "Le Dernier Souffle",
            "originalTitle": "Le Dernier Souffle",
            "sessionAttributes": [
                {"attributeType": "Language", "value": "french", "name": "Französisch", "shortName": "FR"},
            ],
            "filmAttributes": [
                {"attributeType": "ShowAttribute", "value": "om-u", "name": "OmU"},
            ],
            "runningTime": 105,
            "releaseDate": "2025-01-10",
            "posterImageSrc": None,
            "filmUrl": "",
            "genres": ["Thriller"],
        },
    ]
}

CINEMAXX_SHOWINGS_F100 = {
    "result": [
        {
            "sessions": [
                {
                    "startTime": "2025-03-20T20:00:00",
                    "endTime": "2025-03-20T23:35:00",
                    "bookingUrl": "/buchtickets/zusammenfassung/1336/F100/S1",
                    "attributes": [
                        {"attributeType": "Language", "value": "english", "name": "Englisch", "shortName": "EN"},
                    ],
                },
                {
                    "startTime": "2025-03-21T17:00:00",
                    "endTime": "2025-03-21T20:35:00",
                    "bookingUrl": "/buchtickets/zusammenfassung/1336/F100/S2",
                    "attributes": [
                        {"attributeType": "Format", "value": "2d", "name": "2D"},
                    ],
                },
            ]
        }
    ]
}

CINEMAXX_SHOWINGS_F300 = {
    "result": [
        {
            "sessions": [
                {
                    "startTime": "2025-03-22T19:30:00",
                    "bookingUrl": "/buchtickets/zusammenfassung/1336/F300/S1",
                    "attributes": [
                        {"attributeType": "Language", "value": "french", "name": "Französisch", "shortName": "FR"},
                        {"attributeType": "ShowAttribute", "value": "om-u", "name": "OmU"},
                    ],
                },
            ]
        }
    ]
}


def _cinemaxx_get_side_effect(url, **kwargs):
    """Route CinemaxX HTTP GETs to fixture data."""
    resp = MagicMock(spec=requests.Response)
    resp.ok = True
    resp.raise_for_status = MagicMock()

    if "jetzt-im-kino" in url:
        # Session setup page
        resp.text = "<html></html>"
        resp.cookies = MagicMock()
        return resp

    if url.endswith("/films"):
        resp.json.return_value = CINEMAXX_FILMS_RESPONSE
        return resp

    if "F100" in url and "showingGroups" in url:
        resp.json.return_value = CINEMAXX_SHOWINGS_F100
        return resp

    if "F300" in url and "showingGroups" in url:
        resp.json.return_value = CINEMAXX_SHOWINGS_F300
        return resp

    # Default: empty showings
    resp.json.return_value = {"result": []}
    return resp


class TestScrapeCinemaxx:
    def test_returns_only_ov_films(self):
        """Only English/French OV films are returned, normal German films are filtered."""
        with patch("scrapers.cinemaxx.requests.Session") as mock_session_cls:
            session = MagicMock()
            session.get.side_effect = _cinemaxx_get_side_effect
            session.headers = {}
            mock_session_cls.return_value = session

            films = scrape_cinemaxx()

        titles = [f["title_display"] for f in films]
        assert "The Brutalist" in titles
        assert "Le Dernier Souffle" in titles
        assert "Normaler Film" not in titles

    def test_english_film_has_ov_showtimes(self):
        with patch("scrapers.cinemaxx.requests.Session") as mock_session_cls:
            session = MagicMock()
            session.get.side_effect = _cinemaxx_get_side_effect
            session.headers = {}
            mock_session_cls.return_value = session

            films = scrape_cinemaxx()

        brutalist = next(f for f in films if "Brutalist" in f["title_display"])
        # Only the English-language session should remain (not the plain 2D one)
        assert len(brutalist["showtimes"]) == 1
        assert brutalist["showtimes"][0]["language_tag"] == "OV"
        assert "cinemaxx.de" in brutalist["showtimes"][0]["booking_url"]

    def test_french_film_tagged_omu(self):
        with patch("scrapers.cinemaxx.requests.Session") as mock_session_cls:
            session = MagicMock()
            session.get.side_effect = _cinemaxx_get_side_effect
            session.headers = {}
            mock_session_cls.return_value = session

            films = scrape_cinemaxx()

        french = next(f for f in films if "Souffle" in f["title_display"])
        assert len(french["showtimes"]) == 1
        assert french["showtimes"][0]["language_tag"] == "OmU"

    def test_metadata_extracted(self):
        with patch("scrapers.cinemaxx.requests.Session") as mock_session_cls:
            session = MagicMock()
            session.get.side_effect = _cinemaxx_get_side_effect
            session.headers = {}
            mock_session_cls.return_value = session

            films = scrape_cinemaxx()

        brutalist = next(f for f in films if "Brutalist" in f["title_display"])
        assert brutalist["duration_minutes"] == 215
        assert brutalist["release_year"] == 2025
        assert brutalist["_original_title"] == "The Brutalist"

    def test_api_failure_returns_empty(self):
        """If the film-list API fails, return empty list instead of crashing."""
        with patch("scrapers.cinemaxx.requests.Session") as mock_session_cls:
            session = MagicMock()
            session.headers = {}

            msg = "API down"

            def fail_get(url, **kwargs):
                if "jetzt-im-kino" in url:
                    return MagicMock()
                raise requests.RequestException(msg)

            session.get.side_effect = fail_get
            mock_session_cls.return_value = session

            films = scrape_cinemaxx()

        assert films == []

    def test_empty_api_result(self):
        with patch("scrapers.cinemaxx.requests.Session") as mock_session_cls:
            session = MagicMock()
            session.headers = {}

            def empty_get(url, **kwargs):
                resp = MagicMock()
                resp.ok = True
                resp.raise_for_status = MagicMock()
                if "jetzt-im-kino" in url:
                    return resp
                resp.json.return_value = {"result": []}
                return resp

            session.get.side_effect = empty_get
            mock_session_cls.return_value = session

            films = scrape_cinemaxx()

        assert films == []


# ═════════════════════════════════════════════════════════════════════════════
# Arthouse fixtures
# ═════════════════════════════════════════════════════════════════════════════

ARTHOUSE_HTML = """
<html><body>
<div class="row flex-row">
  <div class="col-xs-12"><h2>CINÉMA_FRANÇAIS: Anatomie d'une chute</h2></div>
  <div class="col-sm-3">
    <a href="/filme/anatomie-dune-chute/">Details anzeigen</a>
    Dauer: 151 Min
  </div>
  <div class="col-sm-9 programme-table-main-grid-movieitem-showtimes">
    <h3>Spielzeiten & Tickets</h3>
    <table>
      <thead><tr><th>Heute</th><th>Morgen</th></tr></thead>
      <tbody><tr>
        <td><a href="https://www.kinoheld.de/kino/bielefeld/lichtwerk?showId=1">20:30OmU</a></td>
        <td><a href="https://www.kinoheld.de/kino/bielefeld/kamera-filmkunsttheater?showId=2">18:00OV</a></td>
      </tr></tbody>
    </table>
  </div>
</div>

<div class="row flex-row">
  <div class="col-xs-12"><h2>Deutscher Film ohne OV</h2></div>
  <div class="col-sm-3">Dauer: 90 Min</div>
  <div class="col-sm-9 programme-table-main-grid-movieitem-showtimes">
    <h3>Spielzeiten & Tickets</h3>
    <table>
      <thead><tr><th>Heute</th></tr></thead>
      <tbody><tr>
        <td><a href="https://www.kinoheld.de/kino/bielefeld/lichtwerk?showId=3">19:00</a></td>
      </tr></tbody>
    </table>
  </div>
</div>

<div class="row flex-row">
  <div class="col-xs-12"><h2>Conclave (2024)</h2></div>
  <div class="col-sm-3">
    <a href="/filme/conclave/">Details anzeigen</a>
    Dauer: 120 Min
  </div>
  <div class="col-sm-9 programme-table-main-grid-movieitem-showtimes">
    <h3>Spielzeiten & Tickets</h3>
    <table>
      <thead><tr><th>Heute</th></tr></thead>
      <tbody><tr>
        <td><a href="https://www.kinoheld.de/kino/bielefeld/lichtwerk?showId=4">21:00OV</a></td>
      </tr></tbody>
    </table>
  </div>
</div>
</body></html>
"""


def _arthouse_get_side_effect(url, **kwargs):
    """Route arthouse HTTP GETs to fixture HTML."""
    resp = MagicMock(spec=requests.Response)
    resp.raise_for_status = MagicMock()
    resp.encoding = "utf-8"

    if "/programm/" in url:
        resp.text = ARTHOUSE_HTML
        return resp

    # Detail page stub
    resp.text = "<html><body>Produktion Frankreich 2023 R: Justine Triet.</body></html>"
    return resp


class TestScrapeArthouse:
    def test_returns_only_ov_omu_films(self):
        """Films without any OmU/OV showtimes are excluded."""
        with patch("scrapers.arthouse.requests.get", side_effect=_arthouse_get_side_effect):
            films = scrape_arthouse()

        titles = [f["title_display"] for f in films]
        assert "Anatomie d'une chute" in titles  # prefix stripped
        assert any("Conclave" in t for t in titles)
        assert "Deutscher Film ohne OV" not in titles

    def test_prefix_stripped_from_title(self):
        with patch("scrapers.arthouse.requests.get", side_effect=_arthouse_get_side_effect):
            films = scrape_arthouse()

        anatomie = next(f for f in films if "Anatomie" in f["title_display"])
        assert not anatomie["title_display"].startswith("CINÉMA")

    def test_cinema_detection(self):
        with patch("scrapers.arthouse.requests.get", side_effect=_arthouse_get_side_effect):
            films = scrape_arthouse()

        anatomie = next(f for f in films if "Anatomie" in f["title_display"])
        cinemas = {s["cinema"] for s in anatomie["showtimes"]}
        assert "lichtwerk" in cinemas
        assert "kamera" in cinemas

    def test_language_tags_extracted(self):
        with patch("scrapers.arthouse.requests.get", side_effect=_arthouse_get_side_effect):
            films = scrape_arthouse()

        anatomie = next(f for f in films if "Anatomie" in f["title_display"])
        tags = {s["language_tag"] for s in anatomie["showtimes"]}
        assert "OmU" in tags
        assert "OV" in tags

    def test_duration_extracted(self):
        with patch("scrapers.arthouse.requests.get", side_effect=_arthouse_get_side_effect):
            films = scrape_arthouse()

        anatomie = next(f for f in films if "Anatomie" in f["title_display"])
        assert anatomie["duration_minutes"] == 151

    def test_year_from_title(self):
        with patch("scrapers.arthouse.requests.get", side_effect=_arthouse_get_side_effect):
            films = scrape_arthouse()

        conclave = next(f for f in films if "Conclave" in f["title_display"])
        assert conclave["release_year"] == 2024

    def test_detail_page_enriches_year(self):
        """Films without a year in the title get _arthouse_year from the detail page."""
        with patch("scrapers.arthouse.requests.get", side_effect=_arthouse_get_side_effect):
            films = scrape_arthouse()

        anatomie = next(f for f in films if "Anatomie" in f["title_display"])
        assert anatomie.get("_arthouse_year") == 2023

    def test_booking_urls_present(self):
        with patch("scrapers.arthouse.requests.get", side_effect=_arthouse_get_side_effect):
            films = scrape_arthouse()

        anatomie = next(f for f in films if "Anatomie" in f["title_display"])
        for st in anatomie["showtimes"]:
            assert "kinoheld.de" in st["booking_url"]

    def test_showtimes_are_iso_datetimes(self):
        with patch("scrapers.arthouse.requests.get", side_effect=_arthouse_get_side_effect):
            films = scrape_arthouse()

        for film in films:
            for st in film["showtimes"]:
                # Should be parseable as ISO datetime
                from datetime import datetime
                datetime.fromisoformat(st["showtime"])

    def test_http_error_raises(self):
        """If the programme page fails, the exception propagates."""
        msg = "Connection refused"

        def fail(url, **kwargs):
            raise requests.RequestException(msg)

        with (
            patch("scrapers.arthouse.requests.get", side_effect=fail),
            pytest.raises(requests.RequestException),
        ):
            scrape_arthouse()
