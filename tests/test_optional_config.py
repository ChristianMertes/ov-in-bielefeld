"""Tests that the app works gracefully with optional config values missing."""
from unittest.mock import MagicMock, patch

import pytest

import database
import settings
import telegram_bot
from orchestrator import run_scrape


def _future(days=1):
    from datetime import datetime, timedelta
    return (datetime.now() + timedelta(days=days)).replace(microsecond=0).isoformat()


def _mock_scraper_film(title="Test Film"):
    return {
        "title_display": title,
        "showtimes": [{"cinema": "lichtwerk", "showtime": _future(), "language_tag": "OV"}],
    }


@pytest.fixture
def _isolated_db(tmp_path, monkeypatch):
    """Patch DB paths for an isolated temp database."""
    db_path = str(tmp_path / "opt.db")
    monkeypatch.setattr(database, "DB_PATH", db_path)
    monkeypatch.setattr(settings, "DB_PATH", db_path)


# ── Missing TMDB_API_KEY ─────────────────────────────────────────────────────

@pytest.mark.usefixtures("_isolated_db")
def test_run_scrape_without_tmdb_key(monkeypatch):
    """With empty TMDB_API_KEY, lookup_film returns None; films are still saved."""
    monkeypatch.setattr(settings, "TMDB_API_KEY", "")
    film = _mock_scraper_film("No TMDb Film")
    with (
        patch("orchestrator.scrape_arthouse", return_value=[film]),
        patch("orchestrator.scrape_cinemaxx", return_value=[]),
        patch("orchestrator.lookup_film", return_value=None),
        patch("orchestrator.fetch_imdb_ratings", return_value={}),
        patch("orchestrator.fetch_rt_scores", return_value={}),
    ):
        result = run_scrape()
    assert result is not None
    assert result["total_films"] == 1
    assert "No TMDb Film" in result["new_film_titles"]


# ── Missing OMDB_API_KEY ─────────────────────────────────────────────────────

@pytest.mark.usefixtures("_isolated_db")
def test_run_scrape_without_omdb_key(monkeypatch):
    """With OMDB_API_KEY=None, fetch_rt_scores returns {} immediately."""
    monkeypatch.setattr(settings, "OMDB_API_KEY", None)
    film = _mock_scraper_film("No OMDb Film")
    tmdb = {"original_language": "en", "tmdb_id": 10, "imdb_id": "tt0001",
            "title_original": "No OMDb Film", "title_de": None, "poster_url": None,
            "overview": "", "release_year": 2024, "runtime_minutes": 90, "tmdb_popularity": 5.0}
    with (
        patch("orchestrator.scrape_arthouse", return_value=[film]),
        patch("orchestrator.scrape_cinemaxx", return_value=[]),
        patch("orchestrator.lookup_film", return_value=tmdb),
        patch("orchestrator.fetch_imdb_ratings", return_value={}),
        patch("orchestrator.fetch_rt_scores", return_value={}) as mock_rt,
    ):
        result = run_scrape()
    assert result is not None
    # RT fetch was called but returned empty (OMDB_API_KEY guard is inside fetch_rt_scores)
    mock_rt.assert_called_once()


# ── Missing Telegram credentials ─────────────────────────────────────────────

def test_send_message_without_credentials(monkeypatch):
    """send_message must return False without making HTTP calls when unconfigured."""
    monkeypatch.setattr(telegram_bot, "BOT_TOKEN", None)
    monkeypatch.setattr(telegram_bot, "CHAT_ID", None)
    with patch("telegram_bot.requests.post") as mock_post:
        result = telegram_bot.send_message("test")
    assert result is False
    mock_post.assert_not_called()


@pytest.mark.usefixtures("_isolated_db")
def test_run_scrape_with_notify_callback_unconfigured_telegram(monkeypatch):
    """Notify callback (which calls send_message) gracefully returns False
    when Telegram is not configured, without crashing the scrape."""
    monkeypatch.setattr(telegram_bot, "BOT_TOKEN", None)
    monkeypatch.setattr(telegram_bot, "CHAT_ID", None)
    film = _mock_scraper_film("Notify Film")
    tmdb = {"original_language": "en", "tmdb_id": 20, "imdb_id": None,
            "title_original": None, "title_de": None, "poster_url": None,
            "overview": "", "release_year": 2024, "runtime_minutes": None, "tmdb_popularity": None}
    callback = MagicMock()
    with (
        patch("orchestrator.scrape_arthouse", return_value=[film]),
        patch("orchestrator.scrape_cinemaxx", return_value=[]),
        patch("orchestrator.lookup_film", return_value=tmdb),
        patch("orchestrator.fetch_imdb_ratings", return_value={}),
        patch("orchestrator.fetch_rt_scores", return_value={}),
    ):
        result = run_scrape(notify_callback=callback)
    assert result is not None
    # Callback was still called even though Telegram is not configured
    # (it's up to the callback to handle the missing config)
    callback.assert_called_once()


def test_handle_updates_without_token(monkeypatch):
    """handle_updates returns immediately without HTTP calls when no token."""
    monkeypatch.setattr(telegram_bot, "BOT_TOKEN", None)
    with patch("telegram_bot.requests.get") as mock_get:
        telegram_bot.handle_updates()
    mock_get.assert_not_called()
