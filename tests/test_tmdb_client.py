"""Tests for the TMDb client."""
from unittest.mock import MagicMock, patch

import settings
from database import get_db, set_tmdb_cache
from tmdb_client import (
    _clean_title_for_search,
    _extract_year,
    _get_movie_details,
    _tmdb_search_request,
    get_imdb_url,
    get_omdb_url,
    get_tmdb_url,
    is_relevant_language,
    lookup_film,
)


def _mock_resp(json_data, status=200):
    m = MagicMock()
    m.json.return_value = json_data
    m.status_code = status
    return m


_SEARCH_RESULT = {
    "results": [{
        "id": 27205,
        "title": "Inception",
        "original_title": "Inception",
        "original_language": "en",
        "release_date": "2010-07-16",
        "popularity": 85.4,
        "overview": "A thief...",
        "poster_path": "/poster.jpg",
    }]
}

_DETAIL_RESULT = {
    "title": "Inception",
    "imdb_id": "tt1375666",
    "runtime": 148,
}


# ── _clean_title_for_search ───────────────────────────────────────────────────

def test_clean_removes_year_in_parens():
    assert _clean_title_for_search("Dune (2021)") == "Dune"


def test_clean_removes_german_der():
    assert _clean_title_for_search("Der Pate") == "Pate"


def test_clean_removes_german_die():
    assert _clean_title_for_search("Die Welle") == "Welle"


def test_clean_removes_german_das():
    assert _clean_title_for_search("Das Boot") == "Boot"


def test_clean_removes_3d():
    assert _clean_title_for_search("Avatar 3D") == "Avatar"


def test_clean_removes_imax():
    assert _clean_title_for_search("Interstellar IMAX") == "Interstellar"


def test_clean_removes_4dx():
    assert _clean_title_for_search("Top Gun 4DX") == "Top Gun"


def test_clean_leaves_normal_title():
    assert _clean_title_for_search("Oppenheimer") == "Oppenheimer"


def test_clean_strips_whitespace():
    assert _clean_title_for_search("  Dune  ") == "Dune"


# ── _extract_year ─────────────────────────────────────────────────────────────

def test_extract_year_standard_date():
    assert _extract_year("2023-07-21") == 2023


def test_extract_year_none_input():
    assert _extract_year(None) is None


def test_extract_year_empty_string():
    assert _extract_year("") is None


def test_extract_year_no_year_in_string():
    assert _extract_year("unknown") is None


def test_extract_year_year_only():
    assert _extract_year("1994") == 1994


# ── URL helpers ───────────────────────────────────────────────────────────────

def test_get_imdb_url_with_id():
    assert get_imdb_url("tt0111161") == "https://www.imdb.com/title/tt0111161/"


def test_get_imdb_url_none():
    assert get_imdb_url(None) is None


def test_get_imdb_url_empty_string():
    assert get_imdb_url("") is None


def test_get_tmdb_url_with_id():
    assert get_tmdb_url(278) == "https://www.themoviedb.org/movie/278"


def test_get_tmdb_url_none():
    assert get_tmdb_url(None) is None


def test_get_tmdb_url_zero():
    assert get_tmdb_url(0) is None


def test_get_omdb_url_with_id():
    assert get_omdb_url("tt0111161") == "https://www.omdbapi.com/?i=tt0111161"


def test_get_omdb_url_none():
    assert get_omdb_url(None) is None


# ── is_relevant_language ──────────────────────────────────────────────────────

def test_is_relevant_en():
    assert is_relevant_language("en") is True


def test_is_relevant_fr():
    assert is_relevant_language("fr") is True


def test_is_not_relevant_de():
    assert is_relevant_language("de") is False


def test_is_not_relevant_unknown():
    assert is_relevant_language("zh") is False


# ── _get_movie_details ────────────────────────────────────────────────────────

def test_get_movie_details_success():
    with patch("tmdb_client.requests.get", return_value=_mock_resp(_DETAIL_RESULT)):
        result = _get_movie_details(27205, "fakekey")
    assert result is not None
    assert result["imdb_id"] == "tt1375666"
    assert result["runtime"] == 148


def test_get_movie_details_network_error():
    import requests as req
    with patch("tmdb_client.requests.get", side_effect=req.RequestException("timeout")):
        result = _get_movie_details(27205, "fakekey")
    assert result is None


# ── _tmdb_search_request ──────────────────────────────────────────────────────

def test_tmdb_search_request_returns_result():
    with (
        patch("tmdb_client.requests.get", return_value=_mock_resp(_SEARCH_RESULT)),
        patch("tmdb_client._get_movie_details", return_value=_DETAIL_RESULT),
    ):
        result = _tmdb_search_request("Inception", "fakekey", language="en-US")
    assert result is not None
    assert result["tmdb_id"] == 27205
    assert result["imdb_id"] == "tt1375666"
    assert result["original_language"] == "en"
    assert result["release_year"] == 2010
    assert result["runtime_minutes"] == 148
    assert result["poster_url"] == "https://image.tmdb.org/t/p/w500/poster.jpg"


def test_tmdb_search_request_empty_results():
    with patch("tmdb_client.requests.get", return_value=_mock_resp({"results": []})):
        result = _tmdb_search_request("NoSuchFilm", "fakekey")
    assert result is None


def test_tmdb_search_request_network_error():
    import requests as req
    with patch("tmdb_client.requests.get", side_effect=req.RequestException("timeout")):
        result = _tmdb_search_request("Inception", "fakekey")
    assert result is None


def test_tmdb_search_request_title_de_cleared_when_same_as_original():
    """title_de should be None when the German title equals the original title."""
    search = {"results": [{**_SEARCH_RESULT["results"][0], "title": "Inception"}]}
    detail = {**_DETAIL_RESULT, "title": "Inception"}  # same as original_title
    with (
        patch("tmdb_client.requests.get", return_value=_mock_resp(search)),
        patch("tmdb_client._get_movie_details", return_value=detail),
    ):
        result = _tmdb_search_request("Inception", "fakekey")
    assert result["title_de"] is None


def test_tmdb_search_request_title_de_set_when_different():
    """title_de is populated when the German title differs from the original."""
    search = {"results": [{**_SEARCH_RESULT["results"][0], "original_title": "Inception"}]}
    detail = {**_DETAIL_RESULT, "title": "Inception – Der Anfang"}
    with (
        patch("tmdb_client.requests.get", return_value=_mock_resp(search)),
        patch("tmdb_client._get_movie_details", return_value=detail),
    ):
        result = _tmdb_search_request("Inception", "fakekey")
    assert result["title_de"] == "Inception – Der Anfang"


def test_tmdb_search_request_no_poster():
    search = {"results": [{**_SEARCH_RESULT["results"][0], "poster_path": None}]}
    with (
        patch("tmdb_client.requests.get", return_value=_mock_resp(search)),
        patch("tmdb_client._get_movie_details", return_value=_DETAIL_RESULT),
    ):
        result = _tmdb_search_request("Inception", "fakekey")
    assert result["poster_url"] is None


# ── lookup_film ────────────────────────────────────────────────────────────────

def test_lookup_film_no_api_key(monkeypatch):
    monkeypatch.setattr(settings, "TMDB_API_KEY", "")
    assert lookup_film("Inception") is None


def test_lookup_film_negative_cache_hit(db, monkeypatch):
    """A cached negative result (tmdb_id=None) returns None without an HTTP call."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "fakekey")
    with get_db() as conn:
        set_tmdb_cache(conn, "Obscure|", tmdb_id=None)

    with patch("tmdb_client.requests.get") as mock_get:
        result = lookup_film("Obscure")
    assert result is None
    mock_get.assert_not_called()


def test_lookup_film_positive_cache_hit(db, monkeypatch):
    """A cached positive result is returned without an HTTP call."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "fakekey")
    with get_db() as conn:
        set_tmdb_cache(conn, "Inception|2010",
                       tmdb_id=27205, imdb_id="tt1375666",
                       original_language="en", release_year=2010)

    with patch("tmdb_client.requests.get") as mock_get:
        result = lookup_film("Inception", year=2010)
    assert result is not None
    assert result["tmdb_id"] == 27205
    mock_get.assert_not_called()


def test_lookup_film_cache_miss_calls_api_and_caches(db, monkeypatch):
    """On a cache miss, TMDb is called and the result is written to the cache."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "fakekey")

    with (
        patch("tmdb_client.requests.get", return_value=_mock_resp(_SEARCH_RESULT)),
        patch("tmdb_client._get_movie_details", return_value=_DETAIL_RESULT),
    ):
        result = lookup_film("Inception", year=2010)

    assert result is not None
    assert result["tmdb_id"] == 27205

    # Confirm it was cached
    with get_db() as conn:
        cached = conn.execute(
            "SELECT tmdb_id FROM tmdb_cache WHERE title_query = ?", ("Inception|2010",)
        ).fetchone()
    assert cached["tmdb_id"] == 27205


def test_lookup_film_api_miss_caches_negative(db, monkeypatch):
    """When TMDb returns nothing, a negative entry is cached to prevent repeated lookups."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "fakekey")

    with patch("tmdb_client._search_tmdb", return_value=None):
        result = lookup_film("Unbekannter Film")

    assert result is None
    with get_db() as conn:
        cached = conn.execute(
            "SELECT tmdb_id FROM tmdb_cache WHERE title_query LIKE 'Unbekannter%'"
        ).fetchone()
    assert cached is not None
    assert cached["tmdb_id"] is None
