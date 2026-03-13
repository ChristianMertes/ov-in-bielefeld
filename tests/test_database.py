"""Tests for the database layer."""
from datetime import datetime, timedelta

from database import cleanup_old_showtimes, get_film_by_id, get_upcoming_films, upsert_film, upsert_showtime


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


def test_no_unique_title_display_constraint(db):
    """The old UNIQUE(title_display) must not exist; identity is via partial indexes."""
    schema = db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='films'"
    ).fetchone()["sql"]
    assert "UNIQUE(title_display)" not in schema


# ── upsert_film identity ──────────────────────────────────────────────────────

def test_upsert_film_insert(db):
    film_id, is_new = upsert_film(db, "Test Film", original_language="en")
    assert isinstance(film_id, int)
    assert film_id > 0
    assert is_new is True


def test_upsert_film_is_new_false_on_update(db):
    _, is_new_first = upsert_film(db, "Same Film")
    _, is_new_second = upsert_film(db, "Same Film")
    assert is_new_first is True
    assert is_new_second is False


def test_upsert_film_returns_same_id_on_duplicate(db):
    id1, _ = upsert_film(db, "Same Film")
    id2, _ = upsert_film(db, "Same Film")
    assert id1 == id2


def test_upsert_film_identity_by_tmdb_id(db):
    """Two calls with the same tmdb_id map to one row even if titles differ slightly."""
    id1, _ = upsert_film(db, "Blood & Sinners", tmdb_id=12345, release_year=2024)
    id2, _ = upsert_film(db, "Blood and Sinners", tmdb_id=12345, release_year=2024)
    assert id1 == id2


def test_upsert_film_different_tmdb_id_same_title_creates_separate_rows(db):
    """Same display title but different tmdb_ids (e.g. remake vs original) → separate rows."""
    id1, _ = upsert_film(db, "The Fly", tmdb_id=11111, release_year=1958)
    id2, _ = upsert_film(db, "The Fly", tmdb_id=22222, release_year=1986)
    assert id1 != id2


def test_upsert_film_identity_by_title_year_without_tmdb(db):
    """Without tmdb_id, (title, release_year) is the key."""
    id1, _ = upsert_film(db, "Nosferatu", release_year=1922)
    id2, _ = upsert_film(db, "Nosferatu", release_year=1922)
    assert id1 == id2


def test_upsert_film_same_title_different_year_without_tmdb_creates_separate_rows(db):
    id1, _ = upsert_film(db, "Nosferatu", release_year=1922)
    id2, _ = upsert_film(db, "Nosferatu", release_year=2024)
    assert id1 != id2


def test_upsert_film_provisional_upgraded_by_tmdb(db):
    """A provisional row (no tmdb_id, no year) is upgraded in-place when enrichment runs."""
    # Simulate: scraper inserts without TMDb data
    id1, _ = upsert_film(db, "Mystery Film")
    # Simulate: later scrape enriches with TMDb result
    id2, _ = upsert_film(db, "Mystery Film", tmdb_id=99999, release_year=2023)
    assert id1 == id2
    row = get_film_by_id(db, id1)
    assert row["tmdb_id"] == 99999
    assert row["release_year"] == 2023


def test_upsert_film_updates_metadata(db):
    film_id, _ = upsert_film(db, "Updatable Film")
    upsert_film(db, "Updatable Film", imdb_id="tt1234567")
    row = get_film_by_id(db, film_id)
    assert row["imdb_id"] == "tt1234567"


def test_upsert_film_does_not_overwrite_with_none(db):
    film_id, _ = upsert_film(db, "Film With Data", imdb_id="tt9999999")
    upsert_film(db, "Film With Data")  # no imdb_id → should not overwrite
    row = get_film_by_id(db, film_id)
    assert row["imdb_id"] == "tt9999999"


# ── upsert_showtime ───────────────────────────────────────────────────────────

def test_upsert_showtime_insert(db):
    film_id, _ = upsert_film(db, "Showtime Film")
    upsert_showtime(db, film_id, "lichtwerk", _future(1), "OmU", "https://example.com")
    rows = db.execute("SELECT * FROM showtimes WHERE film_id = ?", (film_id,)).fetchall()
    assert len(rows) == 1
    assert rows[0]["language_tag"] == "OmU"


def test_upsert_showtime_ignores_duplicate(db):
    film_id, _ = upsert_film(db, "Dupe Showtime Film")
    st = _future(2)
    upsert_showtime(db, film_id, "kamera", st, "OV")
    upsert_showtime(db, film_id, "kamera", st, "OV")
    count = db.execute(
        "SELECT COUNT(*) FROM showtimes WHERE film_id = ?", (film_id,)
    ).fetchone()[0]
    assert count == 1


def test_upsert_showtime_enriches_null_language_tag(db):
    film_id, _ = upsert_film(db, "Enrich Lang Film")
    st = _future(2)
    upsert_showtime(db, film_id, "kamera", st, None, "https://example.com")
    upsert_showtime(db, film_id, "kamera", st, "OmU", None)
    row = db.execute("SELECT * FROM showtimes WHERE film_id = ?", (film_id,)).fetchone()
    assert row["language_tag"] == "OmU"
    assert row["booking_url"] == "https://example.com"  # original preserved


def test_upsert_showtime_enriches_null_booking_url(db):
    film_id, _ = upsert_film(db, "Enrich URL Film")
    st = _future(3)
    upsert_showtime(db, film_id, "lichtwerk", st, "OV", None)
    upsert_showtime(db, film_id, "lichtwerk", st, None, "https://kinoheld.de/ticket/123")
    row = db.execute("SELECT * FROM showtimes WHERE film_id = ?", (film_id,)).fetchone()
    assert row["booking_url"] == "https://kinoheld.de/ticket/123"
    assert row["language_tag"] == "OV"  # original preserved


def test_upsert_showtime_does_not_overwrite_existing_values(db):
    film_id, _ = upsert_film(db, "No Overwrite Film")
    st = _future(4)
    upsert_showtime(db, film_id, "cinemaxx", st, "OV", "https://original.url")
    upsert_showtime(db, film_id, "cinemaxx", st, "OmU", "https://different.url")
    row = db.execute("SELECT * FROM showtimes WHERE film_id = ?", (film_id,)).fetchone()
    assert row["language_tag"] == "OV"                   # first value wins
    assert row["booking_url"] == "https://original.url"  # first value wins


# ── get_upcoming_films ────────────────────────────────────────────────────────

def test_get_upcoming_films_returns_future_only(db):
    film_id, _ = upsert_film(db, "Future Film")
    upsert_showtime(db, film_id, "lichtwerk", _future(1), "OV")
    past_film_id, _ = upsert_film(db, "Past Film")
    upsert_showtime(db, past_film_id, "lichtwerk", _past(1), "OV")
    db.commit()

    films = get_upcoming_films(db)
    titles = {f["title_display"] for f in films}
    assert "Future Film" in titles
    assert "Past Film" not in titles


def test_get_upcoming_films_cinema_filter(db):
    film_a, _ = upsert_film(db, "Lichtwerk Film")
    upsert_showtime(db, film_a, "lichtwerk", _future(1), "OV")
    film_b, _ = upsert_film(db, "Kamera Film")
    upsert_showtime(db, film_b, "kamera", _future(1), "OV")
    db.commit()

    results = get_upcoming_films(db, cinema="lichtwerk")
    titles = {f["title_display"] for f in results}
    assert "Lichtwerk Film" in titles
    assert "Kamera Film" not in titles


def test_get_upcoming_films_sorted_by_next_showtime(db):
    film_a, _ = upsert_film(db, "Film A")
    upsert_showtime(db, film_a, "lichtwerk", _future(5), "OV")
    film_b, _ = upsert_film(db, "Film B")
    upsert_showtime(db, film_b, "lichtwerk", _future(2), "OV")
    db.commit()

    films = get_upcoming_films(db)
    titles = [f["title_display"] for f in films]
    assert titles.index("Film B") < titles.index("Film A")


# ── cleanup_old_showtimes ─────────────────────────────────────────────────────

def test_cleanup_removes_old_showtimes(db):
    film_id, _ = upsert_film(db, "Old Film")
    upsert_showtime(db, film_id, "lichtwerk", _past(10), "OV")
    db.commit()

    cleanup_old_showtimes(db, days_old=7)
    db.commit()

    remaining = db.execute(
        "SELECT COUNT(*) FROM showtimes WHERE film_id = ?", (film_id,)
    ).fetchone()[0]
    assert remaining == 0


def test_cleanup_removes_orphan_films(db):
    film_id, _ = upsert_film(db, "Orphan Film")
    upsert_showtime(db, film_id, "lichtwerk", _past(10), "OV")
    db.commit()

    cleanup_old_showtimes(db, days_old=7)
    db.commit()

    assert get_film_by_id(db, film_id) is None


def test_cleanup_keeps_future_showtimes(db):
    film_id, _ = upsert_film(db, "Surviving Film")
    upsert_showtime(db, film_id, "lichtwerk", _future(3), "OV")
    db.commit()

    cleanup_old_showtimes(db, days_old=7)
    db.commit()

    assert get_film_by_id(db, film_id) is not None
