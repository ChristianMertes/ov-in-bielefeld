"""Tests for the TMDb client."""
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
import requests

import settings
from database import get_db, set_tmdb_cache
from tmdb_client import (
    MAX_RELEASE_CANDIDATES,
    NEGATIVE_CACHE_TTL,
    TmdbUnavailableError,
    _clean_title_for_search,
    _extract_year,
    _get_movie_details,
    _tmdb_search_request,
    contradicts_production,
    discover_releases,
    find_by_credits,
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


def test_clean_preserves_die_hard():
    """German article stripping was removed – 'Die Hard' must survive."""
    assert _clean_title_for_search("Die Hard") == "Die Hard"


def test_clean_preserves_das_boot():
    """Das Boot is both the German and English title – must survive."""
    assert _clean_title_for_search("Das Boot") == "Das Boot"


def test_clean_preserves_der_pate():
    """TMDb handles German articles fine; no need to strip them."""
    assert _clean_title_for_search("Der Pate") == "Der Pate"


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


def test_tmdb_search_request_network_error_raises():
    """A network error must be distinguishable from 'TMDb knows no such film'."""
    import requests as req
    with patch("tmdb_client.requests.get", side_effect=req.RequestException("timeout")), \
         pytest.raises(TmdbUnavailableError):
        _tmdb_search_request("Inception", "fakekey")


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


def _age_cache_entry(key: str, age: timedelta) -> None:
    """Backdate a cache entry's timestamp (SQLite stores UTC via datetime('now'))."""
    stamp = (datetime.now(UTC).replace(tzinfo=None) - age).strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        conn.execute("UPDATE tmdb_cache SET cached_at = ? WHERE title_query = ?",
                     (stamp, key))


# ── transient failures must not poison the cache ──────────────────────────────
# A negative entry blocks the orchestrator's language filter (which only runs
# on a TMDb match), so a brief outage must not make a film permanently
# unfilterable.

def test_lookup_film_network_error_does_not_cache(db, monkeypatch):
    import requests as req
    monkeypatch.setattr(settings, "TMDB_API_KEY", "fakekey")

    with patch("tmdb_client.requests.get", side_effect=req.RequestException("boom")):
        result = lookup_film("Flaky Film")

    assert result is None
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM tmdb_cache WHERE title_query = ?", ("Flaky Film|",)
        ).fetchone()
    assert row is None, "a transient outage must not be cached as 'not found'"


def test_lookup_film_retries_after_negative_entry_expires(db, monkeypatch):
    """An expired negative entry is re-checked instead of trusted forever."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "fakekey")
    with get_db() as conn:
        set_tmdb_cache(conn, "Late Addition|", tmdb_id=None)
    _age_cache_entry("Late Addition|", NEGATIVE_CACHE_TTL + timedelta(days=1))

    with (
        patch("tmdb_client.requests.get", return_value=_mock_resp(_SEARCH_RESULT)),
        patch("tmdb_client._get_movie_details", return_value=_DETAIL_RESULT),
    ):
        result = lookup_film("Late Addition")

    assert result is not None
    assert result["tmdb_id"] == 27205


def test_lookup_film_fresh_negative_entry_still_trusted(db, monkeypatch):
    monkeypatch.setattr(settings, "TMDB_API_KEY", "fakekey")
    with get_db() as conn:
        set_tmdb_cache(conn, "Recent Miss|", tmdb_id=None)
    _age_cache_entry("Recent Miss|", NEGATIVE_CACHE_TTL - timedelta(days=1))

    with patch("tmdb_client.requests.get") as mock_get:
        result = lookup_film("Recent Miss")
    assert result is None
    mock_get.assert_not_called()


def test_lookup_film_positive_entry_not_expired_by_ttl(db, monkeypatch):
    """The TTL applies to negative entries only; positives stay valid."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "fakekey")
    with get_db() as conn:
        set_tmdb_cache(conn, "Old Hit|", tmdb_id=27205, original_language="en")
    _age_cache_entry("Old Hit|", NEGATIVE_CACHE_TTL + timedelta(days=365))

    with patch("tmdb_client.requests.get") as mock_get:
        result = lookup_film("Old Hit")
    assert result is not None
    assert result["tmdb_id"] == 27205
    mock_get.assert_not_called()


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


# ── discover_releases ─────────────────────────────────────────────────────────

_DISCOVER_RESULT = {
    "results": [{
        "id": 1204680,
        "title": "Coyote vs. ACME",
        "original_title": "Coyote vs. Acme",
        "original_language": "en",
        "release_date": "2026-09-17",
        "popularity": 593.0,
        "overview": "Wile E. Coyote verklagt ACME.",
        "poster_path": "/coyote.jpg",
    }]
}

_DISCOVER_DETAIL = {
    "title": "Coyote vs. ACME",
    "imdb_id": "tt1756855",
    "runtime": 101,
    "release_date": "2026-08-20",
    "overview": "Wile E. Coyote verklagt ACME.",
}


def test_discover_releases_maps_fields(monkeypatch):
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get") as mock_get:
        mock_get.side_effect = [_mock_resp(_DISCOVER_RESULT), _mock_resp(_DISCOVER_DETAIL)]
        candidates = discover_releases("2026-09-17")

    assert len(candidates) == 1
    c = candidates[0]
    assert c["tmdb_id"] == 1204680
    assert c["title_original"] == "Coyote vs. Acme"
    assert c["title_de"] == "Coyote vs. ACME"
    assert c["imdb_id"] == "tt1756855"
    assert c["runtime_minutes"] == 101
    assert c["poster_url"].endswith("/coyote.jpg")
    assert c["popularity"] == 593.0


def test_discover_releases_year_comes_from_detail(monkeypatch):
    """The discover date is the German start, so the film's own year needs the detail call."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get") as mock_get:
        mock_get.side_effect = [
            _mock_resp(_DISCOVER_RESULT),
            _mock_resp({"release_date": "2025-12-14", "runtime": 109}),
        ]
        candidates = discover_releases("2026-09-17")

    assert candidates[0]["release_year"] == 2025


def _de_releases(*dates):
    return {"results": [{"iso_3166_1": "DE",
                         "release_dates": [{"release_date": f"{d}T00:00:00.000Z", "type": t}
                                           for d, t in dates]}]}


def test_discover_releases_drops_film_returning_to_cinemas(monkeypatch):
    """An earlier German cinema date means a re-release, which no sneak shows."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get") as mock_get:
        mock_get.side_effect = [
            _mock_resp(_DISCOVER_RESULT),
            _mock_resp({
                "release_date": "2026-08-20",
                "release_dates": _de_releases(("2026-03-05", 3), ("2026-09-17", 3)),
            }),
        ]
        assert discover_releases("2026-09-17") == []


def test_discover_releases_drops_film_too_old_for_a_first_release(monkeypatch):
    """A 2001 film opening in 2026 is a catalogue title, even without German history."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get") as mock_get:
        mock_get.side_effect = [
            _mock_resp(_DISCOVER_RESULT),
            _mock_resp({"release_date": "2001-01-01", "runtime": 69,
                        "release_dates": _de_releases(("2026-09-17", 3))}),
        ]
        assert discover_releases("2026-09-17") == []


def test_discover_releases_keeps_film_delayed_by_a_festival_run(monkeypatch):
    """Premiering abroad months before the German start is normal, not a re-release."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get") as mock_get:
        mock_get.side_effect = [
            _mock_resp(_DISCOVER_RESULT),
            _mock_resp({"release_date": "2025-12-14",
                        "release_dates": _de_releases(("2026-09-17", 3))}),
        ]
        assert len(discover_releases("2026-09-17")) == 1


def test_discover_releases_keeps_film_without_release_data(monkeypatch):
    """No data is no reason to drop a candidate."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get") as mock_get:
        mock_get.side_effect = [_mock_resp(_DISCOVER_RESULT), _mock_resp({})]
        assert len(discover_releases("2026-09-17")) == 1


def test_discover_releases_asks_for_release_dates_in_one_call(monkeypatch):
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get") as mock_get:
        mock_get.side_effect = [_mock_resp(_DISCOVER_RESULT), _mock_resp(_DISCOVER_DETAIL)]
        discover_releases("2026-09-17")

    detail_params = mock_get.call_args_list[1].kwargs["params"]
    assert detail_params["append_to_response"] == "release_dates"


def test_discover_releases_queries_german_theatrical_starts(monkeypatch):
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get") as mock_get:
        mock_get.return_value = _mock_resp({"results": []})
        discover_releases("2026-09-17", original_language="fr")

    params = mock_get.call_args.kwargs["params"]
    assert params["region"] == "DE"
    assert params["with_original_language"] == "fr"
    assert params["release_date.gte"] == "2026-09-17"
    assert params["release_date.lte"] == "2026-09-17"


def test_discover_releases_empty_list_when_no_releases(monkeypatch):
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get") as mock_get:
        mock_get.return_value = _mock_resp({"results": []})
        assert discover_releases("2026-09-17") == []


def test_discover_releases_none_when_tmdb_unreachable(monkeypatch):
    """None must be distinguishable from 'no releases', so stale data can be kept."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get", side_effect=requests.ConnectionError("boom")):
        assert discover_releases("2026-09-17") is None


def test_discover_releases_none_without_api_key(monkeypatch):
    monkeypatch.setattr(settings, "TMDB_API_KEY", "")
    assert discover_releases("2026-09-17") is None


def test_discover_releases_caps_candidate_count(monkeypatch):
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    many = {"results": [
        {**_DISCOVER_RESULT["results"][0], "id": i} for i in range(MAX_RELEASE_CANDIDATES + 5)
    ]}
    with patch("tmdb_client.requests.get") as mock_get:
        mock_get.side_effect = [_mock_resp(many), *(
            _mock_resp(_DISCOVER_DETAIL) for _ in range(MAX_RELEASE_CANDIDATES)
        )]
        candidates = discover_releases("2026-09-17")

    assert len(candidates) == MAX_RELEASE_CANDIDATES


def test_discover_releases_survives_failing_detail_lookup(monkeypatch):
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get") as mock_get:
        mock_get.side_effect = [
            _mock_resp(_DISCOVER_RESULT),
            requests.ConnectionError("detail down"),
        ]
        candidates = discover_releases("2026-09-17")

    assert candidates[0]["title_original"] == "Coyote vs. Acme"
    assert candidates[0]["imdb_id"] is None
    assert candidates[0]["release_year"] == 2026


# ── find_by_credits ───────────────────────────────────────────────────────────

_PERSON_RESULT = {"results": [{"id": 3896, "name": "Luchino Visconti"}]}

_CREDIT_DISCOVER = {
    "results": [{
        "id": 11418,
        "title": "Gewalt und Leidenschaft",
        "original_title": "Gruppo di famiglia in un interno",
        "original_language": "it",
        "release_date": "1974-10-11",
        "popularity": 7.3,
        "overview": "Ein alternder Professor ...",
        "poster_path": "/gruppo.jpg",
    }]
}


def test_find_by_credits_identifies_a_film_behind_a_banner(monkeypatch):
    """The case the title search cannot reach: 'Cinema! Italia! Gewalt und Leidenschaft'."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get") as mock_get:
        mock_get.side_effect = [
            _mock_resp(_PERSON_RESULT),      # person search
            _mock_resp(_CREDIT_DISCOVER),    # discover by credits
            _mock_resp({"imdb_id": "tt0072420", "runtime": 121,
                        "title": "Gewalt und Leidenschaft"}),
        ]
        film = find_by_credits("Cinema! Italia! Gewalt und Leidenschaft", 1974,
                               director="Luchino Visconti")

    assert film["tmdb_id"] == 11418
    assert film["original_language"] == "it"
    assert film["imdb_id"] == "tt0072420"
    assert film["title_original"] == "Gruppo di famiglia in un interno"


def test_find_by_credits_accepts_an_original_title_match(monkeypatch):
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    discover = {"results": [{**_CREDIT_DISCOVER["results"][0], "title": "Ganz anders"}]}
    with patch("tmdb_client.requests.get") as mock_get:
        mock_get.side_effect = [_mock_resp(_PERSON_RESULT), _mock_resp(discover), _mock_resp({})]
        film = find_by_credits("Cinema! Italia! Gruppo di famiglia in un interno", 1974,
                               cast=["Burt Lancaster"])

    assert film["tmdb_id"] == 11418


def test_find_by_credits_rejects_a_candidate_the_title_does_not_confirm(monkeypatch):
    """Credits plus a year alone return any film by the same people."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    discover = {"results": [{**_CREDIT_DISCOVER["results"][0],
                             "title": "Suddenly Princess",
                             "original_title": "Suddenly Princess"}]}
    with patch("tmdb_client.requests.get") as mock_get:
        mock_get.side_effect = [_mock_resp(_PERSON_RESULT), _mock_resp(discover)]
        assert find_by_credits("NIPPON CINEMA: Perfect Blue", 1997, cast=["Kon Satoshi"]) is None


def test_find_by_credits_does_not_match_on_a_foreign_script_title(monkeypatch):
    """Normalising must not reduce a Cyrillic title to a letter that matches anything."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    discover = {"results": [{**_CREDIT_DISCOVER["results"][0],
                             "title": "S Миколай та загублений лист",
                             "original_title": "Миколай та загублений лист"}]}
    with patch("tmdb_client.requests.get") as mock_get:
        mock_get.side_effect = [_mock_resp(_PERSON_RESULT), _mock_resp(discover)]
        assert find_by_credits("St. Nicholas and the Lost Letter", 2026, cast=["Someone Else"]) is None


def test_find_by_credits_searches_a_year_window(monkeypatch):
    """Page year and TMDb release year differ by up to a year for festival films."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get") as mock_get:
        mock_get.side_effect = [_mock_resp(_PERSON_RESULT), _mock_resp({"results": []})]
        find_by_credits("L’attachement", 2024, cast=["Valeria Bruni Tedeschi"])

    params = mock_get.call_args_list[1].kwargs["params"]
    assert params["primary_release_date.gte"] == "2023-01-01"
    assert params["primary_release_date.lte"] == "2025-12-31"


def test_find_by_credits_combines_cast_members(monkeypatch):
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get") as mock_get:
        mock_get.side_effect = [
            _mock_resp({"results": [{"id": 1}]}),
            _mock_resp({"results": [{"id": 2}]}),
            _mock_resp({"results": []}),
        ]
        find_by_credits("Irgendein Film", 2025, cast=["Eine Person", "Noch Eine", "Dritte"])

    assert mock_get.call_args_list[2].kwargs["params"]["with_cast"] == "1,2"


def test_find_by_credits_without_people_asks_nothing(monkeypatch):
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get") as mock_get:
        assert find_by_credits("Irgendein Film", 2025, cast=[]) is None
    mock_get.assert_not_called()


def test_find_by_credits_without_year_asks_nothing(monkeypatch):
    """A year is what keeps the candidate field small enough to trust."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get") as mock_get:
        assert find_by_credits("Irgendein Film", None, cast=["Eine Person"]) is None
    mock_get.assert_not_called()


def test_find_by_credits_survives_an_unknown_person(monkeypatch):
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get") as mock_get:
        mock_get.side_effect = [_mock_resp({"results": []})]
        assert find_by_credits("Irgendein Film", 2025, cast=["Niemand Bekanntes"]) is None


# ── contradicts_production ────────────────────────────────────────────────────

def _detail(primary, countries, cinema_dates=()):
    return {
        "release_date": primary,
        "production_countries": [{"iso_3166_1": c} for c in countries],
        "release_dates": {"results": [
            {"iso_3166_1": iso,
             "release_dates": [{"release_date": f"{d}T00:00:00.000Z", "type": t}]}
            for iso, d, t in cinema_dates
        ]},
    }


def _contradicts(detail, year, country, monkeypatch):
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get", return_value=_mock_resp(detail)):
        return contradicts_production(1, year, country)


def test_matching_year_and_country_is_no_objection(monkeypatch):
    detail = _detail("2025-10-09", ["IT"])
    assert _contradicts(detail, 2025, "Italien", monkeypatch) is False


def test_wrong_film_contradicts_on_both_counts(monkeypatch):
    """'Eid': the page says Israel 2024, the match is an Icelandic film from 2016."""
    detail = _detail("2016-09-01", ["IS", "DE", "GB"])
    assert _contradicts(detail, 2024, "Israel", monkeypatch) is True


def test_festival_year_counts_as_the_production_year(monkeypatch):
    """The page states the production year, TMDb the German release a year later."""
    detail = _detail("2025-04-01", ["FR"], [("FR", "2024-09-05", 1)])
    assert _contradicts(detail, 2024, "Frankreich", monkeypatch) is False


def test_german_release_year_counts_too(monkeypatch):
    detail = _detail("2024-05-01", ["FR"], [("DE", "2026-01-15", 3)])
    assert _contradicts(detail, 2026, "Frankreich", monkeypatch) is False


def test_year_off_by_one_is_tolerated(monkeypatch):
    """'Krashanka 2.0': page 2025, TMDb 2026, and it is the right film."""
    detail = _detail("2026-03-01", ["UA"])
    assert _contradicts(detail, 2025, "Ukraine", monkeypatch) is False


def test_a_contradicting_country_is_enough_to_object(monkeypatch):
    """'Eid': the page says Israel, the year agrees, and the match is still wrong."""
    detail = _detail("2024-06-11", ["US"])
    assert _contradicts(detail, 2024, "Israel", monkeypatch) is True


def test_a_contradicting_year_is_enough_to_object(monkeypatch):
    detail = _detail("2019-01-01", ["IT"])
    assert _contradicts(detail, 2025, "Italien", monkeypatch) is True


def test_no_page_country_leaves_the_year_to_decide(monkeypatch):
    detail = _detail("2025-10-09", ["IT"])
    assert _contradicts(detail, 2025, None, monkeypatch) is False


def test_no_page_year_leaves_the_country_to_decide(monkeypatch):
    detail = _detail("2016-09-01", ["IS"])
    assert _contradicts(detail, None, "Israel", monkeypatch) is True


def test_an_unknown_page_country_raises_no_objection_of_its_own(monkeypatch):
    """A country name nobody mapped is not evidence; the year still counts."""
    detail = _detail("2024-09-01", ["IS"])
    assert _contradicts(detail, 2024, "Sansibar", monkeypatch) is False


def test_no_page_facts_at_all_means_no_objection(monkeypatch):
    detail = _detail("2016-09-01", ["IS"])
    assert _contradicts(detail, None, None, monkeypatch) is False


def test_film_without_countries_on_tmdb_raises_no_country_objection(monkeypatch):
    detail = _detail("2024-09-01", [])
    assert _contradicts(detail, 2024, "Israel", monkeypatch) is False


def test_origin_country_counts_as_a_production_country(monkeypatch):
    detail = {**_detail("2025-10-09", []), "origin_country": ["IT"]}
    assert _contradicts(detail, 2025, "Italien", monkeypatch) is False


def test_unreachable_tmdb_means_no_objection(monkeypatch):
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with patch("tmdb_client.requests.get", side_effect=requests.ConnectionError("boom")):
        assert contradicts_production(1, 2024, "Israel") is False


# ── Picking among search results ──────────────────────────────────────────────

_REMAKE_RESULTS = {"results": [
    {"id": 4584, "title": "Sinn und Sinnlichkeit", "original_title": "Sense and Sensibility",
     "original_language": "en", "release_date": "1995-12-13", "popularity": 37.0},
    {"id": 1503762, "title": "Sinn und Sinnlichkeit", "original_title": "Sense and Sensibility",
     "original_language": "en", "release_date": "2026-09-23", "popularity": 34.0},
]}


def test_search_prefers_the_year_the_programme_states(monkeypatch):
    """TMDb ranks by popularity, which hands a remake's search the classic."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with (
        patch("tmdb_client.requests.get", return_value=_mock_resp(_REMAKE_RESULTS)),
        patch("tmdb_client._get_movie_details", return_value={}),
    ):
        film = _tmdb_search_request("Sinn und Sinnlichkeit", "key", prefer_year=2026)

    assert film["tmdb_id"] == 1503762
    assert film["release_year"] == 2026


def test_search_without_a_preferred_year_keeps_tmdbs_order(monkeypatch):
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with (
        patch("tmdb_client.requests.get", return_value=_mock_resp(_REMAKE_RESULTS)),
        patch("tmdb_client._get_movie_details", return_value={}),
    ):
        film = _tmdb_search_request("Sinn und Sinnlichkeit", "key")

    assert film["tmdb_id"] == 4584


def test_search_prefers_the_classic_when_that_is_what_runs(monkeypatch):
    """A repertory screening states 1995, and then the classic is the right film."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with (
        patch("tmdb_client.requests.get", return_value=_mock_resp(_REMAKE_RESULTS)),
        patch("tmdb_client._get_movie_details", return_value={}),
    ):
        film = _tmdb_search_request("Sinn und Sinnlichkeit", "key", prefer_year=1995)

    assert film["tmdb_id"] == 4584


def test_search_tolerates_a_year_off_by_one(monkeypatch):
    """Closest wins, so a page a year out still lands on the right film."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with (
        patch("tmdb_client.requests.get", return_value=_mock_resp(_REMAKE_RESULTS)),
        patch("tmdb_client._get_movie_details", return_value={}),
    ):
        film = _tmdb_search_request("Sinn und Sinnlichkeit", "key", prefer_year=2025)

    assert film["tmdb_id"] == 1503762


def test_search_does_not_prefer_a_result_without_a_release_date(monkeypatch):
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    results = {"results": [
        {"id": 1, "title": "Egal", "original_language": "en", "popularity": 50.0},
        {"id": 2, "title": "Egal", "original_language": "en",
         "release_date": "2026-01-01", "popularity": 45.0},
    ]}
    with (
        patch("tmdb_client.requests.get", return_value=_mock_resp(results)),
        patch("tmdb_client._get_movie_details", return_value={}),
    ):
        film = _tmdb_search_request("Egal", "key", prefer_year=2026)

    assert film["tmdb_id"] == 2


def test_search_keeps_the_popular_result_when_no_title_fits(monkeypatch):
    """A marathon listed as a 2025 production must not pull in a 2025 short film."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    results = {"results": [
        {"id": 120, "title": "Der Herr der Ringe: Die Gefährten",
         "original_title": "The Lord of the Rings: The Fellowship of the Ring",
         "original_language": "en", "release_date": "2001-12-18", "popularity": 60.0},
        {"id": 1434460, "title": "Herr der Ringe", "original_title": "Herr der Ringe",
         "original_language": "en", "release_date": "2025-03-29", "popularity": 1.0},
    ]}
    with (
        patch("tmdb_client.requests.get", return_value=_mock_resp(results)),
        patch("tmdb_client._get_movie_details", return_value={}),
    ):
        film = _tmdb_search_request("Lord Of The Rings Triple (Extended Version - English)",
                                    "key", prefer_year=2025)

    assert film["tmdb_id"] == 120


def test_lookup_film_caches_preferences_separately(db, monkeypatch):
    """The same title with a different stated year can be a different film."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with (
        patch("tmdb_client.requests.get", return_value=_mock_resp(_REMAKE_RESULTS)),
        patch("tmdb_client._get_movie_details", return_value={}),
    ):
        remake = lookup_film("Sinn und Sinnlichkeit", prefer_year=2026)
        classic = lookup_film("Sinn und Sinnlichkeit", prefer_year=1995)

    assert remake["tmdb_id"] == 1503762
    assert classic["tmdb_id"] == 4584


def test_search_does_not_let_an_obscure_namesake_win_on_the_year(monkeypatch):
    """A 14-minute short named "Herr der Ringe" must not beat Peter Jackson."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    results = {"results": [
        {"id": 120, "title": "Herr der Ringe", "original_title": "Herr der Ringe",
         "original_language": "en", "release_date": "2001-12-18", "popularity": 60.0},
        {"id": 1434460, "title": "Herr der Ringe", "original_title": "Herr der Ringe",
         "original_language": "en", "release_date": "2025-03-29", "popularity": 1.0},
    ]}
    with (
        patch("tmdb_client.requests.get", return_value=_mock_resp(results)),
        patch("tmdb_client._get_movie_details", return_value={}),
    ):
        film = _tmdb_search_request("Herr der Ringe", "key", prefer_year=2025)

    assert film["tmdb_id"] == 120


def test_search_lets_a_comparably_popular_remake_win_on_the_year(monkeypatch):
    """The remake case this heuristic exists for: same league, newer year."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "key")
    with (
        patch("tmdb_client.requests.get", return_value=_mock_resp(_REMAKE_RESULTS)),
        patch("tmdb_client._get_movie_details", return_value={}),
    ):
        film = _tmdb_search_request("Sinn und Sinnlichkeit", "key", prefer_year=2026)

    assert film["tmdb_id"] == 1503762
