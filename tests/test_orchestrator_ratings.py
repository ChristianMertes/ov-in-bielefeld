"""Tests for orchestrator Phase 4 (IMDb ratings + RT scores) and error paths."""
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

import database
import settings
from orchestrator import run_scrape


def _future(days=1):
    return (datetime.now() + timedelta(days=days)).replace(microsecond=0).isoformat()


def _mock_scraper_film(title="Rated Film"):
    return {
        "title_display": title,
        "showtimes": [{"cinema": "lichtwerk", "showtime": _future(), "language_tag": "OV"}],
    }


_TMDB = {
    "original_language": "en", "tmdb_id": 100, "imdb_id": "tt0111161",
    "title_original": "The Shawshank Redemption", "title_de": "Die Verurteilten",
    "poster_url": None, "overview": "...", "release_year": 1994,
    "runtime_minutes": 142, "tmdb_popularity": 80.0,
}


@pytest.fixture
def _isolated_db(tmp_path, monkeypatch):
    db_path = str(tmp_path / "ratings.db")
    monkeypatch.setattr(database, "DB_PATH", db_path)
    monkeypatch.setattr(settings, "DB_PATH", db_path)


# ── Phase 4: IMDb ratings applied to DB ──────────────────────────────────────

@pytest.mark.usefixtures("_isolated_db")
def test_run_scrape_applies_imdb_ratings():
    """When fetch_imdb_ratings returns data, films get their rating/votes updated."""
    film = _mock_scraper_film("Shawshank")
    ratings = {"tt0111161": {"rating": 9.3, "votes": 2800000}}
    with (
        patch("orchestrator.scrape_arthouse", return_value=[film]),
        patch("orchestrator.scrape_cinemaxx", return_value=[]),
        patch("orchestrator.lookup_film", return_value=_TMDB),
        patch("orchestrator.fetch_imdb_ratings", return_value=ratings),
        patch("orchestrator.fetch_rt_scores", return_value={}),
    ):
        result = run_scrape()

    assert result is not None
    # Verify the rating was written to the DB
    from database import get_db
    with get_db() as db:
        film_row = db.execute(
            "SELECT imdb_rating, imdb_votes FROM films WHERE imdb_id = 'tt0111161'"
        ).fetchone()
    assert film_row is not None
    assert film_row["imdb_rating"] == 9.3
    assert film_row["imdb_votes"] == 2800000


@pytest.mark.usefixtures("_isolated_db")
def test_run_scrape_applies_rt_scores():
    """When fetch_rt_scores returns data, films get their RT score updated."""
    film = _mock_scraper_film("Shawshank")
    rt_scores = {"tt0111161": 91}
    with (
        patch("orchestrator.scrape_arthouse", return_value=[film]),
        patch("orchestrator.scrape_cinemaxx", return_value=[]),
        patch("orchestrator.lookup_film", return_value=_TMDB),
        patch("orchestrator.fetch_imdb_ratings", return_value={}),
        patch("orchestrator.fetch_rt_scores", return_value=rt_scores),
    ):
        result = run_scrape()

    assert result is not None
    from database import get_db
    with get_db() as db:
        film_row = db.execute(
            "SELECT rt_score FROM films WHERE imdb_id = 'tt0111161'"
        ).fetchone()
    assert film_row is not None
    assert film_row["rt_score"] == 91


@pytest.mark.usefixtures("_isolated_db")
def test_run_scrape_applies_both_ratings():
    """Both IMDb rating and RT score applied in one scrape run."""
    film = _mock_scraper_film("Shawshank")
    ratings = {"tt0111161": {"rating": 9.3, "votes": 2800000}}
    rt_scores = {"tt0111161": 91}
    with (
        patch("orchestrator.scrape_arthouse", return_value=[film]),
        patch("orchestrator.scrape_cinemaxx", return_value=[]),
        patch("orchestrator.lookup_film", return_value=_TMDB),
        patch("orchestrator.fetch_imdb_ratings", return_value=ratings),
        patch("orchestrator.fetch_rt_scores", return_value=rt_scores),
    ):
        result = run_scrape()

    assert result is not None
    from database import get_db
    with get_db() as db:
        row = db.execute(
            "SELECT imdb_rating, rt_score FROM films WHERE imdb_id = 'tt0111161'"
        ).fetchone()
    assert row["imdb_rating"] == 9.3
    assert row["rt_score"] == 91


@pytest.mark.usefixtures("_isolated_db")
def test_run_scrape_ratings_skipped_when_no_imdb_ids():
    """Films without imdb_id skip the entire ratings phase."""
    tmdb_no_imdb = {**_TMDB, "imdb_id": None}
    film = _mock_scraper_film("No IMDB")
    with (
        patch("orchestrator.scrape_arthouse", return_value=[film]),
        patch("orchestrator.scrape_cinemaxx", return_value=[]),
        patch("orchestrator.lookup_film", return_value=tmdb_no_imdb),
        patch("orchestrator.fetch_imdb_ratings") as mock_imdb,
        patch("orchestrator.fetch_rt_scores") as mock_rt,
    ):
        run_scrape()
    # fetch functions are still called with empty list since get_films_with_imdb_id returns []
    # but the if films_to_rate: guard skips the whole block
    mock_imdb.assert_not_called()
    mock_rt.assert_not_called()


# ── Error paths ──────────────────────────────────────────────────────────────

@pytest.mark.usefixtures("_isolated_db")
def test_run_scrape_tmdb_enrichment_error_continues():
    """If _enrich_with_tmdb raises for one film, other films still proceed."""
    film_a = _mock_scraper_film("Film A")
    film_b = _mock_scraper_film("Film B")

    call_count = 0

    def flaky_lookup(title, year=None):
        nonlocal call_count
        call_count += 1
        if "Film A" in str(title):
            msg = "TMDb exploded"
            raise RuntimeError(msg)
        return {**_TMDB, "tmdb_id": 200 + call_count}

    with (
        patch("orchestrator.scrape_arthouse", return_value=[film_a, film_b]),
        patch("orchestrator.scrape_cinemaxx", return_value=[]),
        patch("orchestrator.lookup_film", side_effect=flaky_lookup),
        patch("orchestrator.fetch_imdb_ratings", return_value={}),
        patch("orchestrator.fetch_rt_scores", return_value={}),
    ):
        result = run_scrape()

    assert result is not None
    # Film B should have made it through
    assert result["total_films"] >= 1


@pytest.mark.usefixtures("_isolated_db")
def test_run_scrape_notify_callback_error_does_not_crash():
    """If the notify callback raises, scrape still completes."""
    film = _mock_scraper_film("Callback Fail")
    tmdb = {**_TMDB, "tmdb_id": 300}

    def exploding_callback(film_id, film_data):
        msg = "notification service down"
        raise RuntimeError(msg)

    with (
        patch("orchestrator.scrape_arthouse", return_value=[film]),
        patch("orchestrator.scrape_cinemaxx", return_value=[]),
        patch("orchestrator.lookup_film", return_value=tmdb),
        patch("orchestrator.fetch_imdb_ratings", return_value={}),
        patch("orchestrator.fetch_rt_scores", return_value={}),
    ):
        result = run_scrape(notify_callback=exploding_callback)

    assert result is not None
    assert result["total_films"] == 1
