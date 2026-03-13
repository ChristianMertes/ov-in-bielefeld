"""Tests for the ratings client (HTTP mocked)."""
from unittest.mock import MagicMock, patch

import requests as req

import settings
from ratings_client import fetch_imdb_ratings, fetch_rt_scores


def _mock_resp(json_data):
    m = MagicMock()
    m.json.return_value = json_data
    return m


# ── fetch_imdb_ratings ────────────────────────────────────────────────────────

def test_fetch_imdb_ratings_empty_input():
    assert fetch_imdb_ratings([]) == {}


def test_fetch_imdb_ratings_returns_ratings():
    data = [
        {"imdbId": "tt0111161", "rating": 9.3, "votes": 2_800_000},
        {"imdbId": "tt0068646", "rating": 9.2, "votes": 1_900_000},
    ]
    with patch("ratings_client.requests.get", return_value=_mock_resp(data)):
        result = fetch_imdb_ratings(["tt0111161", "tt0068646"])
    assert result["tt0111161"] == {"rating": 9.3, "votes": 2_800_000}
    assert result["tt0068646"] == {"rating": 9.2, "votes": 1_900_000}


def test_fetch_imdb_ratings_omits_item_with_none_rating():
    data = [{"imdbId": "tt0111161", "rating": None}]
    with patch("ratings_client.requests.get", return_value=_mock_resp(data)):
        result = fetch_imdb_ratings(["tt0111161"])
    assert result == {}


def test_fetch_imdb_ratings_omits_item_without_rating_key():
    data = [{"imdbId": "tt0111161"}]
    with patch("ratings_client.requests.get", return_value=_mock_resp(data)):
        result = fetch_imdb_ratings(["tt0111161"])
    assert result == {}


def test_fetch_imdb_ratings_omits_item_without_imdb_id():
    data = [{"rating": 8.5, "votes": 100}]
    with patch("ratings_client.requests.get", return_value=_mock_resp(data)):
        result = fetch_imdb_ratings(["tt0111161"])
    assert result == {}


def test_fetch_imdb_ratings_network_error():
    with patch("ratings_client.requests.get", side_effect=req.RequestException("timeout")):
        result = fetch_imdb_ratings(["tt0111161"])
    assert result == {}


# ── fetch_rt_scores ───────────────────────────────────────────────────────────

def test_fetch_rt_scores_no_api_key(monkeypatch):
    monkeypatch.setattr(settings, "OMDB_API_KEY", None)
    assert fetch_rt_scores(["tt0111161"]) == {}


def test_fetch_rt_scores_empty_input(monkeypatch):
    monkeypatch.setattr(settings, "OMDB_API_KEY", "fakekey")
    assert fetch_rt_scores([]) == {}


def test_fetch_rt_scores_returns_rt_score(monkeypatch):
    monkeypatch.setattr(settings, "OMDB_API_KEY", "fakekey")
    data = {
        "Response": "True",
        "Ratings": [
            {"Source": "Internet Movie Database", "Value": "9.3/10"},
            {"Source": "Rotten Tomatoes", "Value": "91%"},
        ],
    }
    with patch("ratings_client.requests.get", return_value=_mock_resp(data)):
        result = fetch_rt_scores(["tt0111161"])
    assert result["tt0111161"] == 91


def test_fetch_rt_scores_no_rt_source(monkeypatch):
    monkeypatch.setattr(settings, "OMDB_API_KEY", "fakekey")
    data = {
        "Response": "True",
        "Ratings": [{"Source": "Internet Movie Database", "Value": "9.3/10"}],
    }
    with patch("ratings_client.requests.get", return_value=_mock_resp(data)):
        result = fetch_rt_scores(["tt0111161"])
    assert result == {}


def test_fetch_rt_scores_omdb_response_false(monkeypatch):
    monkeypatch.setattr(settings, "OMDB_API_KEY", "fakekey")
    data = {"Response": "False", "Error": "Movie not found!"}
    with patch("ratings_client.requests.get", return_value=_mock_resp(data)):
        result = fetch_rt_scores(["tt9999999"])
    assert result == {}


def test_fetch_rt_scores_multiple_films_independent(monkeypatch):
    """Each film is a separate HTTP call; one missing doesn't affect others."""
    monkeypatch.setattr(settings, "OMDB_API_KEY", "fakekey")
    hits = [
        _mock_resp({"Response": "True", "Ratings": [{"Source": "Rotten Tomatoes", "Value": "85%"}]}),
        _mock_resp({"Response": "False"}),
        _mock_resp({"Response": "True", "Ratings": [{"Source": "Rotten Tomatoes", "Value": "72%"}]}),
    ]
    with patch("ratings_client.requests.get", side_effect=hits):
        result = fetch_rt_scores(["tt0000001", "tt0000002", "tt0000003"])
    assert result == {"tt0000001": 85, "tt0000003": 72}


def test_fetch_rt_scores_network_error_skips_film(monkeypatch):
    """A per-film network error is logged and skipped; other films still processed."""
    monkeypatch.setattr(settings, "OMDB_API_KEY", "fakekey")
    hits = [
        req.RequestException("timeout"),
        _mock_resp({"Response": "True", "Ratings": [{"Source": "Rotten Tomatoes", "Value": "80%"}]}),
    ]
    with patch("ratings_client.requests.get", side_effect=hits):
        result = fetch_rt_scores(["tt0000001", "tt0000002"])
    assert "tt0000001" not in result
    assert result["tt0000002"] == 80
