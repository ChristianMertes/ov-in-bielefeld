"""Database layer using SQLite."""
import sqlite3
import os
from datetime import datetime, date
from contextlib import contextmanager
from typing import Optional

DB_PATH = os.environ.get("KINO_DB_PATH", "kino_ov.db")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_db():
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_db() as db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS films (
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

            CREATE TABLE IF NOT EXISTS showtimes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                film_id INTEGER NOT NULL REFERENCES films(id) ON DELETE CASCADE,
                cinema TEXT NOT NULL,  -- 'lichtwerk', 'kamera', 'cinemaxx'
                showtime TEXT NOT NULL,  -- ISO datetime
                language_tag TEXT,  -- 'OmU', 'OV', 'OmU-en', 'OmU-fr', etc.
                booking_url TEXT,
                scraped_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(film_id, cinema, showtime)
            );

            CREATE TABLE IF NOT EXISTS tmdb_cache (
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

            CREATE INDEX IF NOT EXISTS idx_showtimes_film ON showtimes(film_id);
            CREATE INDEX IF NOT EXISTS idx_showtimes_datetime ON showtimes(showtime);
            CREATE INDEX IF NOT EXISTS idx_films_notified ON films(notified);
        """)
        # Migrate existing DBs that predate title_de columns
        for stmt in [
            "ALTER TABLE films ADD COLUMN title_de TEXT",
            "ALTER TABLE tmdb_cache ADD COLUMN title_de TEXT",
            "ALTER TABLE films ADD COLUMN imdb_rating REAL",
            "ALTER TABLE films ADD COLUMN imdb_votes INTEGER",
            "ALTER TABLE films ADD COLUMN rt_score INTEGER",
            "ALTER TABLE films ADD COLUMN tmdb_popularity REAL",
            "ALTER TABLE tmdb_cache ADD COLUMN tmdb_popularity REAL",
        ]:
            try:
                db.execute(stmt)
            except Exception:
                pass  # Column already exists


def upsert_film(db: sqlite3.Connection, title_display: str, **kwargs) -> int:
    """Insert or update a film. Returns the film ID."""
    existing = db.execute(
        "SELECT id FROM films WHERE title_display = ?", (title_display,)
    ).fetchone()

    if existing:
        film_id = existing["id"]
        # Update metadata if we have new info
        updates = []
        values = []
        for key in ("title_original", "title_de", "original_language", "tmdb_id", "imdb_id",
                     "poster_url", "overview", "release_year", "runtime_minutes", "tmdb_popularity"):
            if key in kwargs and kwargs[key] is not None:
                updates.append(f"{key} = ?")
                values.append(kwargs[key])
        if updates:
            values.append(film_id)
            db.execute(
                f"UPDATE films SET {', '.join(updates)} WHERE id = ?",
                values
            )
        return film_id
    else:
        cols = ["title_display"]
        vals = [title_display]
        for key in ("title_original", "title_de", "original_language", "tmdb_id", "imdb_id",
                     "poster_url", "overview", "release_year", "runtime_minutes", "tmdb_popularity"):
            if key in kwargs and kwargs[key] is not None:
                cols.append(key)
                vals.append(kwargs[key])
        placeholders = ", ".join(["?"] * len(vals))
        col_str = ", ".join(cols)
        cursor = db.execute(
            f"INSERT INTO films ({col_str}) VALUES ({placeholders})",
            vals
        )
        return cursor.lastrowid


def upsert_showtime(db: sqlite3.Connection, film_id: int, cinema: str,
                    showtime: str, language_tag: str = None,
                    booking_url: str = None):
    """Insert a showtime, ignore if duplicate."""
    db.execute("""
        INSERT OR IGNORE INTO showtimes (film_id, cinema, showtime, language_tag, booking_url)
        VALUES (?, ?, ?, ?, ?)
    """, (film_id, cinema, showtime, language_tag, booking_url))


def get_upcoming_films(db: sqlite3.Connection, cinema: str = None) -> list:
    """Get films with future showtimes, optionally filtered by cinema."""
    now = datetime.now().isoformat()
    query = """
        SELECT DISTINCT f.*, 
            MIN(s.showtime) as next_showtime,
            GROUP_CONCAT(DISTINCT s.cinema) as cinemas,
            GROUP_CONCAT(DISTINCT s.language_tag) as language_tags
        FROM films f
        JOIN showtimes s ON f.id = s.film_id
        WHERE s.showtime >= ?
    """
    params = [now]
    if cinema:
        query += " AND s.cinema = ?"
        params.append(cinema)
    query += " GROUP BY f.id ORDER BY MIN(s.showtime)"
    return db.execute(query, params).fetchall()


def get_film_showtimes(db: sqlite3.Connection, film_id: int) -> list:
    """Get all future showtimes for a specific film."""
    now = datetime.now().isoformat()
    return db.execute("""
        SELECT * FROM showtimes
        WHERE film_id = ? AND showtime >= ?
        ORDER BY showtime
    """, (film_id, now)).fetchall()


def get_film_by_id(db: sqlite3.Connection, film_id: int) -> Optional[sqlite3.Row]:
    return db.execute("SELECT * FROM films WHERE id = ?", (film_id,)).fetchone()


def get_new_unnotified_films(db: sqlite3.Connection) -> list:
    """Get films that haven't been notified yet."""
    return db.execute(
        "SELECT * FROM films WHERE notified = 0"
    ).fetchall()


def mark_film_notified(db: sqlite3.Connection, film_id: int):
    db.execute("UPDATE films SET notified = 1 WHERE id = ?", (film_id,))


def get_tmdb_cache(db: sqlite3.Connection, title_query: str) -> Optional[sqlite3.Row]:
    return db.execute(
        "SELECT * FROM tmdb_cache WHERE title_query = ?", (title_query,)
    ).fetchone()


def set_tmdb_cache(db: sqlite3.Connection, title_query: str, **kwargs):
    cols = ["title_query"] + list(kwargs.keys())
    vals = [title_query] + list(kwargs.values())
    placeholders = ", ".join(["?"] * len(vals))
    col_str = ", ".join(cols)
    db.execute(
        f"INSERT OR REPLACE INTO tmdb_cache ({col_str}) VALUES ({placeholders})",
        vals
    )


def get_films_with_imdb_id(db: sqlite3.Connection) -> list:
    """Get all films that have an IMDb ID."""
    return db.execute(
        "SELECT id, imdb_id FROM films WHERE imdb_id IS NOT NULL"
    ).fetchall()


def update_film_ratings(db: sqlite3.Connection, film_id: int,
                        imdb_rating: float, imdb_votes: int):
    db.execute(
        "UPDATE films SET imdb_rating = ?, imdb_votes = ? WHERE id = ?",
        (imdb_rating, imdb_votes, film_id)
    )


def update_film_rt_score(db: sqlite3.Connection, film_id: int, rt_score: int):
    db.execute(
        "UPDATE films SET rt_score = ? WHERE id = ?",
        (rt_score, film_id)
    )


def cleanup_old_showtimes(db: sqlite3.Connection, days_old: int = 7):
    """Remove showtimes older than N days."""
    from datetime import timedelta
    cutoff = (datetime.now() - timedelta(days=days_old)).isoformat()
    db.execute("DELETE FROM showtimes WHERE showtime < ?", (cutoff,))
    # Also remove films with no remaining showtimes
    db.execute("""
        DELETE FROM films WHERE id NOT IN (
            SELECT DISTINCT film_id FROM showtimes
        )
    """)
