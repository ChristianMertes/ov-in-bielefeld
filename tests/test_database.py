"""Tests for the database layer."""
import sqlite3
from datetime import datetime, timedelta

import database
from database import (
    cleanup_old_showtimes,
    get_film_by_id,
    get_film_showtimes,
    get_films_with_imdb_id,
    get_new_unnotified_films,
    get_showtimes_for_films,
    get_tmdb_cache,
    get_upcoming_films,
    init_db,
    mark_film_notified,
    set_tmdb_cache,
    update_film_ratings,
    update_film_rt_score,
    upsert_film,
    upsert_showtime,
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


# ── upsert_showtime: empty-string enrichment ──────────────────────────────────

def test_upsert_showtime_enriches_empty_string_language_tag(db):
    """Empty-string language_tag is treated the same as NULL (NULLIF logic)."""
    film_id, _ = upsert_film(db, "Empty String Film")
    st = _future(2)
    upsert_showtime(db, film_id, "lichtwerk", st, "", None)
    upsert_showtime(db, film_id, "lichtwerk", st, "OmU", None)
    row = db.execute("SELECT language_tag FROM showtimes WHERE film_id = ?", (film_id,)).fetchone()
    assert row["language_tag"] == "OmU"


def test_upsert_showtime_enriches_empty_string_booking_url(db):
    """Empty-string booking_url is treated the same as NULL (NULLIF logic)."""
    film_id, _ = upsert_film(db, "Empty URL Film")
    st = _future(3)
    upsert_showtime(db, film_id, "kamera", st, "OV", "")
    upsert_showtime(db, film_id, "kamera", st, None, "https://kinoheld.de/ticket/99")
    row = db.execute("SELECT booking_url FROM showtimes WHERE film_id = ?", (film_id,)).fetchone()
    assert row["booking_url"] == "https://kinoheld.de/ticket/99"


# ── get_upcoming_films: aggregated columns ────────────────────────────────────

def test_get_upcoming_films_cinemas_aggregated(db):
    """A film playing at multiple cinemas lists all of them in the cinemas column."""
    film_id, _ = upsert_film(db, "Multi-Cinema Film")
    upsert_showtime(db, film_id, "lichtwerk", _future(1), "OV")
    upsert_showtime(db, film_id, "kamera", _future(2), "OV")
    db.commit()

    films = get_upcoming_films(db)
    row = next(f for f in films if f["title_display"] == "Multi-Cinema Film")
    cinemas = set(row["cinemas"].split(","))
    assert cinemas == {"lichtwerk", "kamera"}


def test_get_upcoming_films_language_tags_aggregated(db):
    """language_tags column contains all distinct tags for the film's future showtimes."""
    film_id, _ = upsert_film(db, "Bilingual Film")
    upsert_showtime(db, film_id, "lichtwerk", _future(1), "OV")
    upsert_showtime(db, film_id, "lichtwerk", _future(2), "OmU")
    db.commit()

    films = get_upcoming_films(db)
    row = next(f for f in films if f["title_display"] == "Bilingual Film")
    tags = set(row["language_tags"].split(","))
    assert tags == {"OV", "OmU"}


def test_get_upcoming_films_next_showtime_is_earliest(db):
    """next_showtime is the MIN across all future showtimes for the film."""
    film_id, _ = upsert_film(db, "Multi-Show Film")
    early = _future(1)
    late = _future(5)
    upsert_showtime(db, film_id, "lichtwerk", late, "OV")
    upsert_showtime(db, film_id, "lichtwerk", early, "OV")
    db.commit()

    films = get_upcoming_films(db)
    row = next(f for f in films if f["title_display"] == "Multi-Show Film")
    assert row["next_showtime"] == early


def test_get_upcoming_films_excludes_past_showtimes_from_aggregation(db):
    """Past showtimes don't appear in cinemas/language_tags even if film has future ones."""
    film_id, _ = upsert_film(db, "Mixed Time Film")
    upsert_showtime(db, film_id, "cinemaxx", _past(1), "OV")   # past — should be ignored
    upsert_showtime(db, film_id, "lichtwerk", _future(1), "OmU")
    db.commit()

    films = get_upcoming_films(db)
    row = next(f for f in films if f["title_display"] == "Mixed Time Film")
    assert row["cinemas"] == "lichtwerk"
    assert row["language_tags"] == "OmU"


# ── get_film_by_id ────────────────────────────────────────────────────────────

def test_get_film_by_id_found(db):
    film_id, _ = upsert_film(db, "Findable Film", imdb_id="tt1111111", original_language="en")
    row = get_film_by_id(db, film_id)
    assert row is not None
    assert row["title_display"] == "Findable Film"
    assert row["imdb_id"] == "tt1111111"
    assert row["original_language"] == "en"


def test_get_film_by_id_not_found(db):
    assert get_film_by_id(db, 99999) is None


# ── get_film_showtimes ────────────────────────────────────────────────────────

def test_get_film_showtimes_returns_future_only(db):
    film_id, _ = upsert_film(db, "Showtime Query Film")
    upsert_showtime(db, film_id, "lichtwerk", _past(1), "OV")
    upsert_showtime(db, film_id, "lichtwerk", _future(1), "OV")
    upsert_showtime(db, film_id, "lichtwerk", _future(3), "OV")
    db.commit()

    rows = get_film_showtimes(db, film_id)
    assert len(rows) == 2
    for row in rows:
        assert row["showtime"] >= datetime.now().isoformat()


def test_get_film_showtimes_ordered_chronologically(db):
    film_id, _ = upsert_film(db, "Ordered Showtimes Film")
    upsert_showtime(db, film_id, "lichtwerk", _future(5), "OV")
    upsert_showtime(db, film_id, "lichtwerk", _future(1), "OV")
    upsert_showtime(db, film_id, "lichtwerk", _future(3), "OV")
    db.commit()

    rows = get_film_showtimes(db, film_id)
    times = [r["showtime"] for r in rows]
    assert times == sorted(times)


def test_get_film_showtimes_empty_when_all_past(db):
    film_id, _ = upsert_film(db, "All Past Film")
    upsert_showtime(db, film_id, "lichtwerk", _past(2), "OV")
    db.commit()

    assert get_film_showtimes(db, film_id) == []


# ── get_showtimes_for_films ───────────────────────────────────────────────────

def test_get_showtimes_for_films_empty_input(db):
    assert get_showtimes_for_films(db, []) == {}


def test_get_showtimes_for_films_single_film(db):
    film_id, _ = upsert_film(db, "Single Film")
    upsert_showtime(db, film_id, "lichtwerk", _future(1), "OV")
    upsert_showtime(db, film_id, "lichtwerk", _future(2), "OmU")
    db.commit()

    result = get_showtimes_for_films(db, [film_id])
    assert film_id in result
    assert len(result[film_id]) == 2


def test_get_showtimes_for_films_multiple_films_no_cross_contamination(db):
    """Each film only gets its own showtimes, not those of other films."""
    film_a, _ = upsert_film(db, "Film A")
    film_b, _ = upsert_film(db, "Film B")
    upsert_showtime(db, film_a, "lichtwerk", _future(1), "OV")
    upsert_showtime(db, film_a, "lichtwerk", _future(2), "OV")
    upsert_showtime(db, film_b, "kamera", _future(1), "OmU")
    db.commit()

    result = get_showtimes_for_films(db, [film_a, film_b])
    assert len(result[film_a]) == 2
    assert len(result[film_b]) == 1
    assert all(r["film_id"] == film_a for r in result[film_a])
    assert all(r["film_id"] == film_b for r in result[film_b])


def test_get_showtimes_for_films_excludes_past(db):
    film_id, _ = upsert_film(db, "Past Showtime Film")
    upsert_showtime(db, film_id, "lichtwerk", _past(1), "OV")
    upsert_showtime(db, film_id, "lichtwerk", _future(1), "OV")
    db.commit()

    result = get_showtimes_for_films(db, [film_id])
    assert len(result[film_id]) == 1


def test_get_showtimes_for_films_film_with_no_showtimes_gets_empty_list(db):
    film_id, _ = upsert_film(db, "No Shows Film")
    # No showtimes inserted
    result = get_showtimes_for_films(db, [film_id])
    assert result[film_id] == []


def test_get_showtimes_for_films_ordered_chronologically(db):
    film_id, _ = upsert_film(db, "Ordered Batch Film")
    upsert_showtime(db, film_id, "lichtwerk", _future(5), "OV")
    upsert_showtime(db, film_id, "lichtwerk", _future(1), "OV")
    upsert_showtime(db, film_id, "lichtwerk", _future(3), "OV")
    db.commit()

    result = get_showtimes_for_films(db, [film_id])
    times = [r["showtime"] for r in result[film_id]]
    assert times == sorted(times)


# ── get_tmdb_cache / set_tmdb_cache ──────────────────────────────────────────

def test_get_tmdb_cache_miss(db):
    assert get_tmdb_cache(db, "nonexistent|2024") is None


def test_set_and_get_tmdb_cache_full_hit(db):
    set_tmdb_cache(db, "Inception|2010",
                   tmdb_id=27205, imdb_id="tt1375666", title_original="Inception",
                   original_language="en", release_year=2010, runtime_minutes=148)
    db.commit()

    row = get_tmdb_cache(db, "Inception|2010")
    assert row is not None
    assert row["tmdb_id"] == 27205
    assert row["imdb_id"] == "tt1375666"
    assert row["original_language"] == "en"
    assert row["runtime_minutes"] == 148


def test_set_tmdb_cache_negative_result(db):
    """tmdb_id=None is used to cache a negative lookup (not found / not relevant)."""
    set_tmdb_cache(db, "Obscure German Film|", tmdb_id=None)
    db.commit()

    row = get_tmdb_cache(db, "Obscure German Film|")
    assert row is not None
    assert row["tmdb_id"] is None


def test_set_tmdb_cache_replaces_on_conflict(db):
    """INSERT OR REPLACE: a second set overwrites the first."""
    set_tmdb_cache(db, "Dupe|2020", tmdb_id=111, original_language="fr")
    set_tmdb_cache(db, "Dupe|2020", tmdb_id=222, original_language="en")
    db.commit()

    row = get_tmdb_cache(db, "Dupe|2020")
    assert row["tmdb_id"] == 222
    assert row["original_language"] == "en"


# ── notifications ─────────────────────────────────────────────────────────────

def test_new_film_appears_in_unnotified(db):
    film_id, _ = upsert_film(db, "Brand New Film")
    db.commit()

    rows = get_new_unnotified_films(db)
    ids = [r["id"] for r in rows]
    assert film_id in ids


def test_mark_film_notified_removes_from_unnotified(db):
    film_id, _ = upsert_film(db, "To Be Notified Film")
    db.commit()

    mark_film_notified(db, film_id)
    db.commit()

    rows = get_new_unnotified_films(db)
    ids = [r["id"] for r in rows]
    assert film_id not in ids


# ── ratings updates ───────────────────────────────────────────────────────────

def test_update_film_ratings(db):
    film_id, _ = upsert_film(db, "Rated Film", imdb_id="tt9999001")
    db.commit()

    update_film_ratings(db, film_id, imdb_rating=8.5, imdb_votes=120_000)
    db.commit()

    row = get_film_by_id(db, film_id)
    assert row["imdb_rating"] == 8.5
    assert row["imdb_votes"] == 120_000


def test_update_film_rt_score(db):
    film_id, _ = upsert_film(db, "RT Film", imdb_id="tt9999002")
    db.commit()

    update_film_rt_score(db, film_id, rt_score=92)
    db.commit()

    row = get_film_by_id(db, film_id)
    assert row["rt_score"] == 92


# ── get_films_with_imdb_id ────────────────────────────────────────────────────

def test_get_films_with_imdb_id_only_returns_films_with_id(db):
    with_id, _ = upsert_film(db, "Has IMDb", imdb_id="tt0000001")
    without_id, _ = upsert_film(db, "No IMDb")
    db.commit()

    rows = get_films_with_imdb_id(db)
    ids = [r["id"] for r in rows]
    assert with_id in ids
    assert without_id not in ids


def test_get_films_with_imdb_id_includes_imdb_id_value(db):
    film_id, _ = upsert_film(db, "IMDb Film", imdb_id="tt0076759")
    db.commit()

    rows = get_films_with_imdb_id(db)
    row = next(r for r in rows if r["id"] == film_id)
    assert row["imdb_id"] == "tt0076759"


# ── migration: UNIQUE(title_display) removal ─────────────────────────────────

OLD_SCHEMA = """\
CREATE TABLE films (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title_display TEXT NOT NULL,
    title_original TEXT,
    title_de TEXT,
    original_language TEXT,
    tmdb_id INTEGER,
    imdb_id TEXT,
    poster_url TEXT,
    overview TEXT,
    release_year INTEGER,
    runtime_minutes INTEGER,
    first_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    notified INTEGER NOT NULL DEFAULT 0,
    imdb_rating REAL,
    imdb_votes INTEGER,
    rt_score INTEGER,
    tmdb_popularity REAL,
    UNIQUE(title_display)
);
CREATE TABLE showtimes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    film_id INTEGER NOT NULL REFERENCES films(id) ON DELETE CASCADE,
    cinema TEXT NOT NULL,
    showtime TEXT NOT NULL,
    language_tag TEXT,
    booking_url TEXT,
    scraped_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(film_id, cinema, showtime)
);
CREATE TABLE tmdb_cache (
    title_query TEXT PRIMARY KEY,
    tmdb_id INTEGER,
    imdb_id TEXT,
    title_original TEXT,
    original_language TEXT,
    poster_url TEXT,
    overview TEXT,
    release_year INTEGER,
    runtime_minutes INTEGER,
    cached_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


def test_migration_from_old_schema_preserves_data(tmp_path, monkeypatch):
    """init_db migrates UNIQUE(title_display) away while keeping existing rows."""
    db_path = str(tmp_path / "migrate.db")
    monkeypatch.setattr(database, "DB_PATH", db_path)

    # Create old-schema DB with seed data
    conn = sqlite3.connect(db_path)
    conn.executescript(OLD_SCHEMA)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        "INSERT INTO films (title_display, original_language) VALUES (?, ?)",
        ("Inception", "en"),
    )
    film_id = conn.execute("SELECT id FROM films WHERE title_display='Inception'").fetchone()[0]
    conn.execute(
        "INSERT INTO showtimes (film_id, cinema, showtime) VALUES (?, ?, ?)",
        (film_id, "lichtwerk", _future(1)),
    )
    conn.commit()
    conn.close()

    # Run init_db which triggers the migration
    init_db()

    # Verify data survived
    conn = database.get_connection()
    row = conn.execute("SELECT * FROM films WHERE title_display='Inception'").fetchone()
    assert row is not None
    assert row["original_language"] == "en"

    st = conn.execute("SELECT * FROM showtimes WHERE film_id=?", (row["id"],)).fetchone()
    assert st is not None
    assert st["cinema"] == "lichtwerk"
    conn.close()


def test_migration_showtimes_fk_not_broken(tmp_path, monkeypatch):
    """After migration, showtimes FK must reference 'films', not '_films_old'.

    This is the bug: SQLite 3.26+ rewrites FK references when renaming a table,
    so RENAME films TO _films_old changes showtimes' FK to _films_old(id).
    After _films_old is dropped, any FK-checked operation on showtimes crashes
    with 'no such table: main._films_old'.
    """
    db_path = str(tmp_path / "fk.db")
    monkeypatch.setattr(database, "DB_PATH", db_path)

    conn = sqlite3.connect(db_path)
    conn.executescript(OLD_SCHEMA)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        "INSERT INTO films (title_display) VALUES (?)", ("Old Film",)
    )
    conn.execute(
        "INSERT INTO showtimes (film_id, cinema, showtime) VALUES (1, 'lichtwerk', ?)",
        (_past(10),),
    )
    conn.commit()
    conn.close()

    init_db()

    # This is the operation that crashes with the bug
    conn = database.get_connection()
    cleanup_old_showtimes(conn, days_old=7)
    conn.commit()
    conn.close()


def test_migration_no_leftover_films_old_table(tmp_path, monkeypatch):
    """The temporary _films_old table must not exist after migration."""
    db_path = str(tmp_path / "leftover.db")
    monkeypatch.setattr(database, "DB_PATH", db_path)

    conn = sqlite3.connect(db_path)
    conn.executescript(OLD_SCHEMA)
    conn.commit()
    conn.close()

    init_db()

    conn = database.get_connection()
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert "_films_old" not in tables
    conn.close()


def test_migration_showtimes_schema_references_films(tmp_path, monkeypatch):
    """After migration, the showtimes CREATE TABLE must reference 'films', not '_films_old'."""
    db_path = str(tmp_path / "schema.db")
    monkeypatch.setattr(database, "DB_PATH", db_path)

    conn = sqlite3.connect(db_path)
    conn.executescript(OLD_SCHEMA)
    conn.commit()
    conn.close()

    init_db()

    conn = database.get_connection()
    schema = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='showtimes'"
    ).fetchone()["sql"]
    assert "_films_old" not in schema
    assert "films(id)" in schema.lower() or "films (id)" in schema.lower()
    conn.close()


def test_repair_broken_showtimes_fk(tmp_path, monkeypatch):
    """init_db repairs a DB where a previous buggy migration left showtimes
    referencing _films_old instead of films."""
    db_path = str(tmp_path / "repair.db")
    monkeypatch.setattr(database, "DB_PATH", db_path)

    # Simulate the broken state: films table is correct (no UNIQUE(title_display)),
    # but showtimes references _films_old
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE films (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title_display TEXT NOT NULL,
            title_original TEXT,
            title_de TEXT,
            original_language TEXT,
            tmdb_id INTEGER,
            imdb_id TEXT,
            poster_url TEXT,
            overview TEXT,
            release_year INTEGER,
            runtime_minutes INTEGER,
            first_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
            notified INTEGER NOT NULL DEFAULT 0,
            imdb_rating REAL,
            imdb_votes INTEGER,
            rt_score INTEGER,
            tmdb_popularity REAL
        );
        CREATE TABLE showtimes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            film_id INTEGER NOT NULL REFERENCES "_films_old"(id) ON DELETE CASCADE,
            cinema TEXT NOT NULL,
            showtime TEXT NOT NULL,
            language_tag TEXT,
            booking_url TEXT,
            scraped_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(film_id, cinema, showtime)
        );
        CREATE TABLE tmdb_cache (
            title_query TEXT PRIMARY KEY,
            tmdb_id INTEGER,
            imdb_id TEXT,
            title_original TEXT,
            title_de TEXT,
            original_language TEXT,
            poster_url TEXT,
            overview TEXT,
            release_year INTEGER,
            runtime_minutes INTEGER,
            tmdb_popularity REAL,
            cached_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    conn.execute(
        "INSERT INTO films (title_display) VALUES (?)", ("Broken FK Film",)
    )
    conn.execute(
        "INSERT INTO showtimes (film_id, cinema, showtime) VALUES (1, 'lichtwerk', ?)",
        (_past(10),),
    )
    conn.commit()
    conn.close()

    # init_db should detect and repair the broken FK
    init_db()

    # Verify the FK now references 'films', and cleanup works
    conn = database.get_connection()
    schema = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='showtimes'"
    ).fetchone()["sql"]
    assert "_films_old" not in schema

    # This would crash before the repair
    cleanup_old_showtimes(conn, days_old=7)
    conn.commit()
    conn.close()
