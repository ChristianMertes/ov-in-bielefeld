"""Tests for the database layer."""
from datetime import datetime, timedelta

import database
from database import (
    upsert_film,
    upsert_showtime,
    get_upcoming_films,
    get_film_by_id,
    get_film_showtimes,
    cleanup_old_showtimes,
)


def _future(days=1):
    return (datetime.now() + timedelta(days=days)).isoformat()


def _past(days=1):
    return (datetime.now() - timedelta(days=days)).isoformat()


# ── Schema ────────────────────────────────────────────────────────────────────

def test_tables_exist(db):
    tables = {r[0] for r in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert {"films", "showtimes", "tmdb_cache"} <= tables


# ── upsert_film ───────────────────────────────────────────────────────────────

def test_upsert_film_insert(db):
    film_id = upsert_film(db, "Test Film", original_language="en")
    assert isinstance(film_id, int)
    assert film_id > 0


def test_upsert_film_returns_same_id_on_duplicate(db):
    id1 = upsert_film(db, "Same Film")
    id2 = upsert_film(db, "Same Film")
    assert id1 == id2


def test_upsert_film_updates_metadata(db):
    film_id = upsert_film(db, "Updatable Film")
    upsert_film(db, "Updatable Film", imdb_id="tt1234567")
    row = get_film_by_id(db, film_id)
    assert row["imdb_id"] == "tt1234567"


def test_upsert_film_does_not_overwrite_with_none(db):
    film_id = upsert_film(db, "Film With Data", imdb_id="tt9999999")
    upsert_film(db, "Film With Data")  # no imdb_id → should not overwrite
    row = get_film_by_id(db, film_id)
    assert row["imdb_id"] == "tt9999999"


# ── upsert_showtime ───────────────────────────────────────────────────────────

def test_upsert_showtime_insert(db):
    film_id = upsert_film(db, "Showtime Film")
    upsert_showtime(db, film_id, "lichtwerk", _future(1), "OmU", "https://example.com")
    rows = db.execute("SELECT * FROM showtimes WHERE film_id = ?", (film_id,)).fetchall()
    assert len(rows) == 1
    assert rows[0]["language_tag"] == "OmU"


def test_upsert_showtime_ignores_duplicate(db):
    film_id = upsert_film(db, "Dupe Showtime Film")
    st = _future(2)
    upsert_showtime(db, film_id, "kamera", st, "OV")
    upsert_showtime(db, film_id, "kamera", st, "OV")
    count = db.execute(
        "SELECT COUNT(*) FROM showtimes WHERE film_id = ?", (film_id,)
    ).fetchone()[0]
    assert count == 1


# ── get_upcoming_films ────────────────────────────────────────────────────────

def test_get_upcoming_films_returns_future_only(db):
    film_id = upsert_film(db, "Future Film")
    upsert_showtime(db, film_id, "lichtwerk", _future(1), "OV")
    past_film_id = upsert_film(db, "Past Film")
    upsert_showtime(db, past_film_id, "lichtwerk", _past(1), "OV")
    db.commit()

    films = get_upcoming_films(db)
    titles = {f["title_display"] for f in films}
    assert "Future Film" in titles
    assert "Past Film" not in titles


def test_get_upcoming_films_cinema_filter(db):
    film_a = upsert_film(db, "Lichtwerk Film")
    upsert_showtime(db, film_a, "lichtwerk", _future(1), "OV")
    film_b = upsert_film(db, "Kamera Film")
    upsert_showtime(db, film_b, "kamera", _future(1), "OV")
    db.commit()

    results = get_upcoming_films(db, cinema="lichtwerk")
    titles = {f["title_display"] for f in results}
    assert "Lichtwerk Film" in titles
    assert "Kamera Film" not in titles


def test_get_upcoming_films_sorted_by_next_showtime(db):
    film_a = upsert_film(db, "Film A")
    upsert_showtime(db, film_a, "lichtwerk", _future(5), "OV")
    film_b = upsert_film(db, "Film B")
    upsert_showtime(db, film_b, "lichtwerk", _future(2), "OV")
    db.commit()

    films = get_upcoming_films(db)
    titles = [f["title_display"] for f in films]
    assert titles.index("Film B") < titles.index("Film A")


# ── cleanup_old_showtimes ─────────────────────────────────────────────────────

def test_cleanup_removes_old_showtimes(db):
    film_id = upsert_film(db, "Old Film")
    upsert_showtime(db, film_id, "lichtwerk", _past(10), "OV")
    db.commit()

    cleanup_old_showtimes(db, days_old=7)
    db.commit()

    remaining = db.execute(
        "SELECT COUNT(*) FROM showtimes WHERE film_id = ?", (film_id,)
    ).fetchone()[0]
    assert remaining == 0


def test_cleanup_removes_orphan_films(db):
    film_id = upsert_film(db, "Orphan Film")
    upsert_showtime(db, film_id, "lichtwerk", _past(10), "OV")
    db.commit()

    cleanup_old_showtimes(db, days_old=7)
    db.commit()

    assert get_film_by_id(db, film_id) is None


def test_cleanup_keeps_future_showtimes(db):
    film_id = upsert_film(db, "Surviving Film")
    upsert_showtime(db, film_id, "lichtwerk", _future(3), "OV")
    db.commit()

    cleanup_old_showtimes(db, days_old=7)
    db.commit()

    assert get_film_by_id(db, film_id) is not None
