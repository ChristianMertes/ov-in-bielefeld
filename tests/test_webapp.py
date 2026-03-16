"""Tests for the webapp routes using FastAPI TestClient."""
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

import database
import settings
from database import upsert_film, upsert_showtime
from webapp import _is_future, app


def _future(days=1):
    return (datetime.now() + timedelta(days=days)).replace(microsecond=0).isoformat()


def _past(days=1):
    return (datetime.now() - timedelta(days=days)).replace(microsecond=0).isoformat()


@pytest.fixture
def client(tmp_path, monkeypatch):
    """TestClient backed by an isolated temp database."""
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(database, "DB_PATH", db_path)
    monkeypatch.setattr(settings, "DB_PATH", db_path)
    import cache as _cache
    with _cache._lock:
        _cache._store.clear()
        _cache._store_plain.clear()
        _cache._version = -1.0
    with TestClient(app) as c:
        yield c


@pytest.fixture
def client_with_film(tmp_path, monkeypatch):
    """TestClient plus a pre-inserted film with a future showtime."""
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(database, "DB_PATH", db_path)
    monkeypatch.setattr(settings, "DB_PATH", db_path)
    import cache as _cache
    with _cache._lock:
        _cache._store.clear()
        _cache._store_plain.clear()
        _cache._version = -1.0
    with TestClient(app) as c:
        # Use get_db() (auto-commits) so data is visible to subsequent route requests
        from database import get_db
        with get_db() as conn:
            film_id, _ = upsert_film(
                conn, "Inception",
                original_language="en", tmdb_id=27205, imdb_id="tt1375666",
                release_year=2010, runtime_minutes=148,
            )
            upsert_showtime(conn, film_id, "lichtwerk", _future(1), "OV", "https://example.com/book")
            upsert_showtime(conn, film_id, "kamera", _future(3), "OmU", None)
        yield c, film_id


# ── utility endpoints ─────────────────────────────────────────────────────────

def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_robots_txt_status(client):
    resp = client.get("/robots.txt")
    assert resp.status_code == 200


def test_robots_txt_disallows_api(client):
    resp = client.get("/robots.txt")
    assert "Disallow: /api/" in resp.text


def test_robots_txt_allows_root(client):
    resp = client.get("/robots.txt")
    assert "Allow: /" in resp.text


def test_sitemap_xml_status(client):
    resp = client.get("/sitemap.xml")
    assert resp.status_code == 200
    assert "application/xml" in resp.headers["content-type"]


def test_sitemap_xml_contains_root(client):
    resp = client.get("/sitemap.xml")
    assert "<urlset" in resp.text
    assert "<url>" in resp.text


def test_sitemap_xml_contains_film_url(client_with_film):
    client, film_id = client_with_film
    resp = client.get("/sitemap.xml")
    assert f"/film/{film_id}" in resp.text


# ── index route ───────────────────────────────────────────────────────────────

def test_index_empty_db(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]


def test_index_with_film(client_with_film):
    client, _ = client_with_film
    resp = client.get("/")
    assert resp.status_code == 200
    assert "Inception" in resp.text


def test_index_cinema_filter_matches(client_with_film):
    client, _ = client_with_film
    resp = client.get("/?cinema=lichtwerk")
    assert resp.status_code == 200
    assert "Inception" in resp.text


def test_index_cinema_filter_excludes(client_with_film):
    client, _ = client_with_film
    resp = client.get("/?cinema=cinemaxx")
    assert resp.status_code == 200
    assert "Inception" not in resp.text


def test_index_vary_header_always_present(client):
    resp = client.get("/")
    assert resp.headers.get("vary") == "Accept-Encoding"


def test_index_brotli_response(client):
    resp = client.get("/", headers={"Accept-Encoding": "br, gzip, deflate"})
    assert resp.status_code == 200
    assert resp.headers.get("content-encoding") == "br"
    assert resp.headers.get("vary") == "Accept-Encoding"


def test_index_plain_response_no_content_encoding(client):
    # httpx sends br by default; override to force plain response
    resp = client.get("/", headers={"Accept-Encoding": "gzip, deflate"})
    assert resp.headers.get("content-encoding") is None


def test_index_sort_by_title(client_with_film):
    client, _ = client_with_film
    resp = client.get("/?sort=title")
    assert resp.status_code == 200


def test_index_sort_by_rating(client_with_film):
    client, _ = client_with_film
    resp = client.get("/?sort=rating")
    assert resp.status_code == 200


# ── film detail route ─────────────────────────────────────────────────────────

def test_film_detail_not_found(client):
    resp = client.get("/film/99999")
    assert resp.status_code == 404


def test_film_detail_found(client_with_film):
    client, film_id = client_with_film
    resp = client.get(f"/film/{film_id}")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "Inception" in resp.text


def test_film_detail_vary_header(client_with_film):
    client, film_id = client_with_film
    resp = client.get(f"/film/{film_id}")
    assert resp.headers.get("vary") == "Accept-Encoding"


def test_film_detail_brotli(client_with_film):
    client, film_id = client_with_film
    resp = client.get(f"/film/{film_id}", headers={"Accept-Encoding": "br"})
    assert resp.status_code == 200
    assert resp.headers.get("content-encoding") == "br"


# ── api/films route ───────────────────────────────────────────────────────────

def test_api_films_empty_db(client):
    resp = client.get("/api/films")
    assert resp.status_code == 200
    assert resp.json() == []


def test_api_films_returns_film(client_with_film):
    client, film_id = client_with_film
    resp = client.get("/api/films")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["title_display"] == "Inception"
    assert len(data[0]["showtimes"]) == 2


def test_api_films_cinema_filter(client_with_film):
    client, _ = client_with_film
    resp = client.get("/api/films?cinema=lichtwerk")
    data = resp.json()
    assert len(data) == 1

    resp2 = client.get("/api/films?cinema=cinemaxx")
    assert resp2.json() == []


# ── past showtime filtering ──────────────────────────────────────────────────

# Unit tests for _is_future

def test_is_future_with_future_datetime():
    now = datetime(2026, 3, 16, 14, 0, 0)
    assert _is_future("2026-03-16T20:00:00", now) is True


def test_is_future_with_past_datetime():
    now = datetime(2026, 3, 16, 14, 0, 0)
    assert _is_future("2026-03-16T10:00:00", now) is False


def test_is_future_with_equal_datetime():
    now = datetime(2026, 3, 16, 14, 0, 0)
    assert _is_future("2026-03-16T14:00:00", now) is True


def test_is_future_with_invalid_string():
    """Invalid datetime strings are kept (treated as future) to avoid hiding data."""
    now = datetime(2026, 3, 16, 14, 0, 0)
    assert _is_future("not-a-date", now) is True


# Integration tests: insert two future showtimes, then advance webapp's "now"
# past the first one so only the view-layer filter (not the DB) drops it.

SOON = (datetime.now() + timedelta(days=2)).replace(hour=20, minute=0, second=0, microsecond=0)
LATER = (datetime.now() + timedelta(days=4)).replace(hour=20, minute=0, second=0, microsecond=0)
BETWEEN = SOON + timedelta(days=1)  # after SOON, before LATER


@pytest.fixture
def client_mixed_showtimes(tmp_path, monkeypatch):
    """Film with two future showtimes. Webapp's now is shifted to be between them."""
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(database, "DB_PATH", db_path)
    monkeypatch.setattr(settings, "DB_PATH", db_path)
    import cache as _cache
    with _cache._lock:
        _cache._store.clear()
        _cache._store_plain.clear()
        _cache._version = -1.0
    with TestClient(app) as c:
        from database import get_db
        with get_db() as conn:
            film_id, _ = upsert_film(
                conn, "Time Travel Film",
                original_language="en", tmdb_id=88888,
            )
            upsert_showtime(conn, film_id, "lichtwerk", SOON.isoformat(),
                            "OV", "https://example.com/soon")
            upsert_showtime(conn, film_id, "kamera", LATER.isoformat(),
                            "OmU", "https://example.com/later")
        yield c, film_id


def test_index_hides_past_showtimes(client_mixed_showtimes, monkeypatch):
    """Showtimes that were future at DB-query time but are past at render time
    must not appear on the index page."""
    client, _ = client_mixed_showtimes
    import webapp
    monkeypatch.setattr(webapp, "datetime", _FakeDatetime)
    resp = client.get("/", headers={"Accept-Encoding": "gzip"})
    assert resp.status_code == 200
    assert "example.com/later" in resp.text
    assert "example.com/soon" not in resp.text


def test_film_detail_hides_past_showtimes(client_mixed_showtimes, monkeypatch):
    """Same filtering on the film detail page."""
    client, film_id = client_mixed_showtimes
    import webapp
    monkeypatch.setattr(webapp, "datetime", _FakeDatetime)
    resp = client.get(f"/film/{film_id}", headers={"Accept-Encoding": "gzip"})
    assert resp.status_code == 200
    assert "example.com/later" in resp.text
    assert "example.com/soon" not in resp.text


def test_api_films_hides_past_showtimes(client_mixed_showtimes, monkeypatch):
    """Same filtering on the JSON API."""
    client, _ = client_mixed_showtimes
    import webapp
    monkeypatch.setattr(webapp, "datetime", _FakeDatetime)
    resp = client.get("/api/films")
    data = resp.json()
    assert len(data) == 1
    urls = [st["booking_url"] for st in data[0]["showtimes"]]
    assert "https://example.com/later" in urls
    assert "https://example.com/soon" not in urls


class _FakeDatetime(datetime):
    """datetime subclass whose now() returns BETWEEN, while fromisoformat still works."""

    @classmethod
    def now(cls, tz=None):  # noqa: ANN001
        return BETWEEN
