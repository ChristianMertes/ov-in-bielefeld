"""Tests for the orchestrator logic."""
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

import database
import settings
from database import get_film_by_id
from orchestrator import _enrich_with_tmdb, _refresh_sneak_candidates, _write_film, run_scrape


def _future(days=1):
    return (datetime.now() + timedelta(days=days)).replace(microsecond=0).isoformat()


def _film_data(title="Test Film", **kwargs):
    return {
        "title_display": title,
        "showtimes": [{"cinema": "lichtwerk", "showtime": _future(), "language_tag": "OV",
                        "booking_url": "https://example.com"}],
        **kwargs,
    }


# ── _enrich_with_tmdb ─────────────────────────────────────────────────────────

def test_enrich_no_tmdb_result_passes_through():
    """Film with no TMDb match is retained; _tmdb_data is set to None."""
    with patch("orchestrator.lookup_film", return_value=None):
        result = _enrich_with_tmdb(_film_data("Unknown Film"))
    assert result is not None
    assert result["_tmdb_data"] is None


def test_enrich_relevant_language_passes_through():
    tmdb = {"original_language": "en", "tmdb_id": 100, "release_year": 2020}
    with patch("orchestrator.lookup_film", return_value=tmdb):
        result = _enrich_with_tmdb(_film_data())
    assert result is not None
    assert result["_tmdb_data"] == tmdb


def test_enrich_french_language_passes_through():
    tmdb = {"original_language": "fr", "tmdb_id": 200}
    with patch("orchestrator.lookup_film", return_value=tmdb):
        result = _enrich_with_tmdb(_film_data())
    assert result is not None


def test_enrich_irrelevant_language_filtered_out():
    """A German-language film found via TMDb should be dropped."""
    tmdb = {"original_language": "de", "tmdb_id": 999}
    with patch("orchestrator.lookup_film", return_value=tmdb):
        result = _enrich_with_tmdb(_film_data())
    assert result is None


def test_enrich_uses_original_title_first():
    """If _original_title is present, it is tried before title_display."""
    film = _film_data(title="Der Pate", _original_title="The Godfather")
    calls = []

    def fake_lookup(title, year=None):
        calls.append(title)
        return {"original_language": "en", "tmdb_id": 238} if title == "The Godfather" else None

    with patch("orchestrator.lookup_film", side_effect=fake_lookup):
        result = _enrich_with_tmdb(film)
    assert calls[0] == "The Godfather"
    assert result is not None


def test_enrich_falls_back_to_display_title():
    """Falls back to title_display when _original_title lookup returns nothing."""
    film = _film_data(title="Oppenheimer", _original_title="Unknown Original")
    tmdb = {"original_language": "en", "tmdb_id": 872585}

    def fake_lookup(title, year=None):
        return tmdb if title == "Oppenheimer" else None

    with patch("orchestrator.lookup_film", side_effect=fake_lookup):
        result = _enrich_with_tmdb(film)
    assert result is not None
    assert result["_tmdb_data"] == tmdb


def test_enrich_subtitle_retry():
    """If the full title fails, retry with the part after ': '."""
    film = _film_data(title="Dune: Part Two")
    tmdb = {"original_language": "en", "tmdb_id": 693134}

    def fake_lookup(title, year=None):
        return tmdb if title == "Part Two" else None

    with patch("orchestrator.lookup_film", side_effect=fake_lookup):
        result = _enrich_with_tmdb(film)
    assert result is not None


# Arthouse joins the original and German title with a dash. TMDb knows each
# half but not the combination, so the halves must be tried as a fallback –
# without it, non-EN/FR films get no TMDb match and thus escape the language
# filter (which only applies when a match exists).

def test_enrich_splits_double_title_and_finds_original():
    film = _film_data(title="Bir kar tanesinin ömrü – Die Lebensdauer einer Schneeflocke")
    tmdb = {"original_language": "tr", "tmdb_id": 4242}

    def fake_lookup(title, year=None):
        return tmdb if title == "Bir kar tanesinin ömrü" else None

    with patch("orchestrator.lookup_film", side_effect=fake_lookup):
        result = _enrich_with_tmdb(film)
    assert result is None, "Turkish film should be filtered out once TMDb resolves it"


def test_enrich_splits_double_title_german_half():
    """The German half is tried too, since either half may come first."""
    film = _film_data(title="Der Astronaut - Project Hail Mary")
    tmdb = {"original_language": "en", "tmdb_id": 687163}

    def fake_lookup(title, year=None):
        return tmdb if title == "Project Hail Mary" else None

    with patch("orchestrator.lookup_film", side_effect=fake_lookup):
        result = _enrich_with_tmdb(film)
    assert result is not None
    assert result["_tmdb_data"] == tmdb


def test_enrich_splits_double_title_after_prefix_strip():
    """Prefix and dash split must combine: 'PREFIX: Original – Deutsch'."""
    film = _film_data(title="CINÉMA_FRANÇAIS: La brigade – Die Küchenbrigade")
    tmdb = {"original_language": "fr", "tmdb_id": 800}

    def fake_lookup(title, year=None):
        return tmdb if title == "La brigade" else None

    with patch("orchestrator.lookup_film", side_effect=fake_lookup):
        result = _enrich_with_tmdb(film)
    assert result is not None


def test_enrich_does_not_split_hyphenated_words():
    """A dash without surrounding spaces is part of the title, not a separator."""
    film = _film_data(title="Spider-Man")
    tried = []

    def fake_lookup(title, year=None):
        tried.append(title)

    with patch("orchestrator.lookup_film", side_effect=fake_lookup):
        _enrich_with_tmdb(film)
    assert tried == ["Spider-Man"]


def test_enrich_full_title_still_preferred():
    """The complete title is tried first; halves are only a fallback."""
    film = _film_data(title="Vera - Die Erbin")
    tried = []

    def fake_lookup(title, year=None):
        tried.append(title)
        return {"original_language": "en", "tmdb_id": 1} if title == "Vera - Die Erbin" else None

    with patch("orchestrator.lookup_film", side_effect=fake_lookup):
        result = _enrich_with_tmdb(film)
    assert tried == ["Vera - Die Erbin"]
    assert result is not None


def test_enrich_year_mismatch_triggers_retry():
    """If TMDb year differs from arthouse year by > 3, retry with the correct year."""
    film = _film_data(title="Nosferatu", _arthouse_year=2024)
    wrong_tmdb = {"original_language": "en", "tmdb_id": 1, "release_year": 1922}
    right_tmdb = {"original_language": "en", "tmdb_id": 2, "release_year": 2024}

    call_years = []

    def fake_lookup(title, year=None):
        call_years.append(year)
        if year == 2024:
            return right_tmdb
        return wrong_tmdb

    with patch("orchestrator.lookup_film", side_effect=fake_lookup):
        result = _enrich_with_tmdb(film)
    assert 2024 in call_years
    assert result["_tmdb_data"] == right_tmdb


def test_enrich_no_year_mismatch_keeps_original():
    """A small year difference (≤ 3) does not trigger a retry."""
    film = _film_data(title="Some Film", _arthouse_year=2023)
    tmdb = {"original_language": "en", "tmdb_id": 42, "release_year": 2022}

    with patch("orchestrator.lookup_film", return_value=tmdb) as mock_lookup:
        result = _enrich_with_tmdb(film)
    # lookup called once (no retry)
    assert mock_lookup.call_count == 1
    assert result["_tmdb_data"] == tmdb


# ── _write_film ───────────────────────────────────────────────────────────────

def test_write_film_with_tmdb_data(db):
    tmdb = {
        "tmdb_id": 27205, "imdb_id": "tt1375666", "title_original": "Inception",
        "original_language": "en", "release_year": 2010, "runtime_minutes": 148,
        "title_de": None, "poster_url": None, "overview": "A thief...", "tmdb_popularity": 85.0,
    }
    film = _film_data("Inception", _tmdb_data=tmdb)
    film_id, is_new = _write_film(db, film)
    assert is_new is True
    row = get_film_by_id(db, film_id)
    assert row["tmdb_id"] == 27205
    assert row["imdb_id"] == "tt1375666"
    assert row["release_year"] == 2010


def test_write_film_without_tmdb_data_uses_scraper_fallback(db):
    """When _tmdb_data is None, scraper metadata is used as fallback."""
    film = _film_data("Unknown Film", _tmdb_data=None, release_year=2023, duration_minutes=95)
    film_id, is_new = _write_film(db, film)
    assert is_new is True
    row = get_film_by_id(db, film_id)
    assert row["release_year"] == 2023
    assert row["runtime_minutes"] == 95


def test_write_film_inserts_showtimes(db):
    film = _film_data("Film With Shows", _tmdb_data=None)
    film["showtimes"] = [
        {"cinema": "lichtwerk", "showtime": _future(1), "language_tag": "OV",
         "booking_url": "https://a.com"},
        {"cinema": "kamera", "showtime": _future(2), "language_tag": "OmU",
         "booking_url": None},
    ]
    film_id, _ = _write_film(db, film)
    count = db.execute(
        "SELECT COUNT(*) FROM showtimes WHERE film_id = ?", (film_id,)
    ).fetchone()[0]
    assert count == 2


def test_write_film_skips_placeholder_showtimes(db):
    """Showtimes with _placeholder=True must not be inserted into the DB."""
    film = _film_data("Placeholder Film", _tmdb_data=None)
    film["showtimes"] = [
        {"cinema": "cinemaxx", "showtime": _future(1), "_placeholder": True},
        {"cinema": "lichtwerk", "showtime": _future(2), "language_tag": "OV"},
    ]
    film_id, _ = _write_film(db, film)
    count = db.execute(
        "SELECT COUNT(*) FROM showtimes WHERE film_id = ?", (film_id,)
    ).fetchone()[0]
    assert count == 1


def test_write_film_returns_false_for_existing(db):
    film = _film_data("Existing Film", _tmdb_data={"tmdb_id": 999, "original_language": "en",
                                                    "imdb_id": None, "title_original": None,
                                                    "title_de": None, "poster_url": None,
                                                    "overview": "", "release_year": 2020,
                                                    "runtime_minutes": None, "tmdb_popularity": None})
    _write_film(db, film)
    _, is_new = _write_film(db, film)
    assert is_new is False


# ── run_scrape ────────────────────────────────────────────────────────────────

def _mock_scraper_film(title="Scraped Film", language="en"):
    return {
        "title_display": title,
        "showtimes": [{"cinema": "lichtwerk", "showtime": _future(), "language_tag": "OV"}],
    }


@pytest.fixture
def patched_run(tmp_path, monkeypatch):
    """Patch DB_PATH so run_scrape uses an isolated temp database."""
    db_path = str(tmp_path / "run.db")
    monkeypatch.setattr(database, "DB_PATH", db_path)
    monkeypatch.setattr(settings, "DB_PATH", db_path)


def test_run_scrape_all_scrapers_fail(patched_run):
    """If all scrapers raise, run_scrape returns None (no films)."""
    with (
        patch("orchestrator.scrape_arthouse", side_effect=Exception("arthouse down")),
        patch("orchestrator.scrape_cinemaxx", side_effect=Exception("cinemaxx down")),
    ):
        result = run_scrape()
    assert result is None


def test_run_scrape_returns_stats(patched_run):
    film = _mock_scraper_film()
    tmdb = {"original_language": "en", "tmdb_id": 1, "imdb_id": None, "title_original": "Scraped Film",
            "title_de": None, "poster_url": None, "overview": "", "release_year": 2024,
            "runtime_minutes": 100, "tmdb_popularity": 10.0}
    with (
        patch("orchestrator.scrape_arthouse", return_value=[film]),
        patch("orchestrator.scrape_cinemaxx", return_value=[]),
        patch("orchestrator.lookup_film", return_value=tmdb),
        patch("orchestrator.fetch_imdb_ratings", return_value={}),
        patch("orchestrator.fetch_rt_scores", return_value={}),
    ):
        result = run_scrape()
    assert result is not None
    assert result["total_films"] == 1
    assert result["new_films"] == 1
    assert "Scraped Film" in result["new_film_titles"]


def test_run_scrape_calls_notify_for_new_film(patched_run):
    film = _mock_scraper_film()
    tmdb = {"original_language": "en", "tmdb_id": 2, "imdb_id": None, "title_original": None,
            "title_de": None, "poster_url": None, "overview": "", "release_year": 2024,
            "runtime_minutes": None, "tmdb_popularity": None}
    callback = MagicMock()
    with (
        patch("orchestrator.scrape_arthouse", return_value=[film]),
        patch("orchestrator.scrape_cinemaxx", return_value=[]),
        patch("orchestrator.lookup_film", return_value=tmdb),
        patch("orchestrator.fetch_imdb_ratings", return_value={}),
        patch("orchestrator.fetch_rt_scores", return_value={}),
    ):
        run_scrape(notify_callback=callback)
    assert callback.call_count == 1
    film_id_arg = callback.call_args[0][0]
    assert isinstance(film_id_arg, int)


def test_run_scrape_filters_non_en_fr_films(patched_run):
    """Films whose TMDb original_language is not en/fr are excluded from the DB."""
    film = _mock_scraper_film("Hindi Film")
    tmdb = {"original_language": "hi", "tmdb_id": 3}
    with (
        patch("orchestrator.scrape_arthouse", return_value=[film]),
        patch("orchestrator.scrape_cinemaxx", return_value=[]),
        patch("orchestrator.lookup_film", return_value=tmdb),
        patch("orchestrator.fetch_imdb_ratings", return_value={}),
        patch("orchestrator.fetch_rt_scores", return_value={}),
    ):
        result = run_scrape()
    assert result is not None
    assert result["total_films"] == 0


def test_run_scrape_invalidates_cache(patched_run):
    with (
        patch("orchestrator.scrape_arthouse", return_value=[_mock_scraper_film()]),
        patch("orchestrator.scrape_cinemaxx", return_value=[]),
        patch("orchestrator.lookup_film", return_value={"original_language": "en", "tmdb_id": 5,
                                                         "imdb_id": None, "title_original": None,
                                                         "title_de": None, "poster_url": None,
                                                         "overview": "", "release_year": None,
                                                         "runtime_minutes": None, "tmdb_popularity": None}),
        patch("orchestrator.fetch_imdb_ratings", return_value={}),
        patch("orchestrator.fetch_rt_scores", return_value={}),
        patch("orchestrator.cache.invalidate") as mock_invalidate,
    ):
        run_scrape()
    mock_invalidate.assert_called_once()


# ── Sneak previews ────────────────────────────────────────────────────────────

def test_sneak_title_never_reaches_tmdb():
    """The entry that showed a real film called 'All In: Sneak Preview'."""
    with patch("orchestrator.lookup_film") as mock_lookup:
        result = _enrich_with_tmdb(_film_data("SNEAK PREVIEW - English Edition"))
    mock_lookup.assert_not_called()
    assert result is not None
    assert result["_is_sneak"] is True
    assert result["_tmdb_data"] is None


def test_regular_title_still_reaches_tmdb():
    with patch("orchestrator.lookup_film", return_value=None) as mock_lookup:
        result = _enrich_with_tmdb(_film_data("Sneakers"))
    mock_lookup.assert_called()
    assert "_is_sneak" not in result


def test_write_sneak_film_sets_flag(db):
    _write_film(db, {**_film_data("SNEAK PREVIEW"), "_is_sneak": True})
    film = db.execute("SELECT * FROM films").fetchone()
    assert film["is_sneak"] == 1


def test_write_sneak_film_drops_scraper_metadata(db):
    """Year, runtime and poster belong to the placeholder, not to the film shown."""
    _write_film(db, {
        **_film_data("SNEAK PREVIEW"),
        "_is_sneak": True,
        "release_year": 2024,
        "duration_minutes": 120,
        "_poster_url": "https://example.com/placeholder.jpg",
        "_original_title": "Sneak Preview",
    })
    film = db.execute("SELECT * FROM films").fetchone()
    assert film["release_year"] is None
    assert film["runtime_minutes"] is None
    assert film["poster_url"] is None
    assert film["title_original"] is None


def test_write_sneak_film_language_from_title(db):
    _write_film(db, {**_film_data("SNEAK PREVIEW - English Edition"), "_is_sneak": True})
    assert db.execute("SELECT * FROM films").fetchone()["original_language"] == "en"


def test_write_sneak_film_language_from_scraper_attribute(db):
    _write_film(db, {
        **_film_data("Sneak Preview"), "_is_sneak": True, "_detected_language": "fr",
    })
    assert db.execute("SELECT * FROM films").fetchone()["original_language"] == "fr"


def test_write_sneak_film_language_defaults_to_english(db):
    _write_film(db, {**_film_data("Sneak OV"), "_is_sneak": True})
    assert db.execute("SELECT * FROM films").fetchone()["original_language"] == "en"


def test_write_sneak_film_reuses_row_across_scrapes(db):
    """The same weekly sneak must not accumulate one row per scrape."""
    _write_film(db, {**_film_data("SNEAK PREVIEW"), "_is_sneak": True})
    _write_film(db, {**_film_data("SNEAK PREVIEW"), "_is_sneak": True})
    assert db.execute("SELECT COUNT(*) FROM films").fetchone()[0] == 1


# ── _refresh_sneak_candidates ─────────────────────────────────────────────────

def _sneak_db(db):
    """Insert a sneak with a Wednesday showtime; returns its release Thursday."""
    from database import upsert_film, upsert_showtime
    wednesday = _next_weekday(2)
    film_id, _ = upsert_film(db, "SNEAK PREVIEW", is_sneak=1, original_language="en")
    upsert_showtime(db, film_id, "kamera", f"{wednesday.isoformat()}T20:00:00", "OmU", None)
    db.commit()
    return (wednesday + timedelta(days=1)).isoformat()


def _next_weekday(weekday):
    today = datetime.now().date()
    return today + timedelta(days=((weekday - today.weekday()) % 7) or 7)


def _candidate(tmdb_id=1):
    return {"tmdb_id": tmdb_id, "title_original": "Runner", "title_de": None,
            "original_language": "en", "poster_url": None, "overview": "",
            "release_year": 2026, "runtime_minutes": 100, "imdb_id": None,
            "popularity": 5.0}


def test_refresh_sneak_candidates_stores_release_lineup(db):
    release_date = _sneak_db(db)
    with patch("orchestrator.discover_releases", return_value=[_candidate()]) as mock_discover:
        _refresh_sneak_candidates()

    mock_discover.assert_called_once_with(release_date, "en")
    rows = db.execute("SELECT * FROM sneak_candidates").fetchall()
    assert [r["release_date"] for r in rows] == [release_date]


def test_refresh_sneak_candidates_keeps_stored_list_when_tmdb_down(db):
    release_date = _sneak_db(db)
    from database import replace_sneak_candidates
    replace_sneak_candidates(db, release_date, [_candidate(42)])
    db.commit()

    with patch("orchestrator.discover_releases", return_value=None):
        _refresh_sneak_candidates()

    rows = db.execute("SELECT tmdb_id FROM sneak_candidates").fetchall()
    assert [r["tmdb_id"] for r in rows] == [42]


def test_refresh_sneak_candidates_without_sneaks_queries_nothing(db):
    with patch("orchestrator.discover_releases") as mock_discover:
        _refresh_sneak_candidates()
    mock_discover.assert_not_called()
