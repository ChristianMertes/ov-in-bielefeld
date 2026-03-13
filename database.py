"""Database layer using SQLite."""
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import datetime, timedelta

import settings

DB_PATH = settings.DB_PATH


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_db() -> Iterator[sqlite3.Connection]:
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with get_db() as db:
        # Fresh-install schema: no UNIQUE(title_display) — identity is managed
        # via partial indexes created below (idx_films_tmdb_id / idx_films_title_year).
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
                tmdb_popularity REAL
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

        # Migration: existing DBs may have the old UNIQUE(title_display) inline constraint.
        # Recreate the table without it so different films with the same title can coexist.
        schema_row = db.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='films'"
        ).fetchone()
        if schema_row and "UNIQUE(title_display)" in (schema_row["sql"] or ""):
            db.execute("PRAGMA foreign_keys=OFF")
            try:
                db.execute("ALTER TABLE films RENAME TO _films_old")
                db.execute("""
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
                    )
                """)
                db.execute("INSERT INTO films SELECT * FROM _films_old")
                db.execute("DROP TABLE _films_old")
            finally:
                db.execute("PRAGMA foreign_keys=ON")

        # Column and index migrations (idempotent — errors mean already applied)
        for stmt in [
            "ALTER TABLE films ADD COLUMN title_de TEXT",
            "ALTER TABLE tmdb_cache ADD COLUMN title_de TEXT",
            "ALTER TABLE films ADD COLUMN imdb_rating REAL",
            "ALTER TABLE films ADD COLUMN imdb_votes INTEGER",
            "ALTER TABLE films ADD COLUMN rt_score INTEGER",
            "ALTER TABLE films ADD COLUMN tmdb_popularity REAL",
            "ALTER TABLE tmdb_cache ADD COLUMN tmdb_popularity REAL",
            # Canonical identity: TMDb ID (most reliable)
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_films_tmdb_id"
            " ON films(tmdb_id) WHERE tmdb_id IS NOT NULL",
            # Natural-key fallback: title + year for films without a TMDb match
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_films_title_year"
            " ON films(title_display, release_year) WHERE tmdb_id IS NULL AND release_year IS NOT NULL",
        ]:
            with suppress(Exception):
                db.execute(stmt)


def upsert_film(db: sqlite3.Connection, title_display: str, **kwargs) -> tuple[int, bool]:  # noqa: ANN003
    """Insert or update a film. Returns (film_id, is_new).

    Identity is resolved in priority order:
    1. tmdb_id — canonical, most reliable
    2. (title_display, release_year) with tmdb_id IS NULL — natural key fallback
    3. (title_display, year=NULL, tmdb_id=NULL) — provisional row, upgradeable

    This prevents two remakes / re-releases with the same display title from
    collapsing into one row once TMDb IDs or release years are available.
    """
    tmdb_id = kwargs.get("tmdb_id")
    release_year = kwargs.get("release_year")

    existing = None

    # Priority 1: canonical identity — TMDb ID
    if tmdb_id is not None:
        existing = db.execute(
            "SELECT id FROM films WHERE tmdb_id = ?", (tmdb_id,)
        ).fetchone()

    # Priority 2: natural key for films without a TMDb match but with a known year
    if existing is None and release_year is not None:
        existing = db.execute(
            "SELECT id FROM films WHERE title_display = ? AND release_year = ? AND tmdb_id IS NULL",
            (title_display, release_year),
        ).fetchone()

    # Priority 3: provisional row — same title, no year, no tmdb_id yet
    # (will be upgraded to a real identity on the next scrape once enrichment succeeds)
    if existing is None:
        existing = db.execute(
            "SELECT id FROM films WHERE title_display = ? AND release_year IS NULL AND tmdb_id IS NULL",
            (title_display,),
        ).fetchone()

    if existing:
        film_id = existing["id"]
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
                f"UPDATE films SET {', '.join(updates)} WHERE id = ?",  # noqa: S608
                values,
            )
        return film_id, False
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
        f"INSERT INTO films ({col_str}) VALUES ({placeholders})",  # noqa: S608
        vals,
    )
    if cursor.lastrowid is None:
        msg = "INSERT returned no lastrowid"
        raise RuntimeError(msg)
    return cursor.lastrowid, True


def upsert_showtime(db: sqlite3.Connection, film_id: int, cinema: str,
                    showtime: str, language_tag: str | None = None,
                    booking_url: str | None = None) -> None:
    """Insert a showtime; on conflict enrich null/empty language_tag and booking_url."""
    db.execute("""
        INSERT INTO showtimes (film_id, cinema, showtime, language_tag, booking_url)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(film_id, cinema, showtime) DO UPDATE SET
            language_tag = COALESCE(NULLIF(showtimes.language_tag, ''), excluded.language_tag),
            booking_url  = COALESCE(NULLIF(showtimes.booking_url,  ''), excluded.booking_url)
    """, (film_id, cinema, showtime, language_tag, booking_url))


def get_upcoming_films(db: sqlite3.Connection, cinema: str | None = None) -> list:
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


def get_showtimes_for_films(db: sqlite3.Connection, film_ids: list[int]) -> dict[int, list]:
    """Fetch all future showtimes for a set of film IDs in one query.

    Returns a dict mapping film_id → list of showtime dicts, ordered by showtime.
    Callers use this to avoid N+1 queries when rendering a list of films.
    """
    if not film_ids:
        return {}
    now = datetime.now().isoformat()
    placeholders = ",".join("?" * len(film_ids))
    rows = db.execute(
        f"SELECT * FROM showtimes WHERE film_id IN ({placeholders}) AND showtime >= ?"  # noqa: S608
        " ORDER BY showtime",
        [*film_ids, now],
    ).fetchall()
    result: dict[int, list] = {fid: [] for fid in film_ids}
    for row in rows:
        result[row["film_id"]].append(dict(row))
    return result


def get_film_by_id(db: sqlite3.Connection, film_id: int) -> sqlite3.Row | None:
    return db.execute("SELECT * FROM films WHERE id = ?", (film_id,)).fetchone()


def get_new_unnotified_films(db: sqlite3.Connection) -> list:
    """Get films that haven't been notified yet."""
    return db.execute(
        "SELECT * FROM films WHERE notified = 0"
    ).fetchall()


def mark_film_notified(db: sqlite3.Connection, film_id: int) -> None:
    db.execute("UPDATE films SET notified = 1 WHERE id = ?", (film_id,))


def get_tmdb_cache(db: sqlite3.Connection, title_query: str) -> sqlite3.Row | None:
    return db.execute(
        "SELECT * FROM tmdb_cache WHERE title_query = ?", (title_query,)
    ).fetchone()


def set_tmdb_cache(db: sqlite3.Connection, title_query: str, **kwargs) -> None:  # noqa: ANN003
    cols = ["title_query", *list(kwargs.keys())]
    vals = [title_query, *list(kwargs.values())]
    placeholders = ", ".join(["?"] * len(vals))
    col_str = ", ".join(cols)
    db.execute(
        f"INSERT OR REPLACE INTO tmdb_cache ({col_str}) VALUES ({placeholders})",  # noqa: S608
        vals
    )


def get_films_with_imdb_id(db: sqlite3.Connection) -> list:
    """Get all films that have an IMDb ID."""
    return db.execute(
        "SELECT id, imdb_id FROM films WHERE imdb_id IS NOT NULL"
    ).fetchall()


def update_film_ratings(db: sqlite3.Connection, film_id: int,
                        imdb_rating: float, imdb_votes: int) -> None:
    db.execute(
        "UPDATE films SET imdb_rating = ?, imdb_votes = ? WHERE id = ?",
        (imdb_rating, imdb_votes, film_id)
    )


def update_film_rt_score(db: sqlite3.Connection, film_id: int, rt_score: int) -> None:
    db.execute(
        "UPDATE films SET rt_score = ? WHERE id = ?",
        (rt_score, film_id)
    )


def cleanup_old_showtimes(db: sqlite3.Connection, days_old: int = 7) -> None:
    """Remove showtimes older than N days."""
    cutoff = (datetime.now() - timedelta(days=days_old)).isoformat()
    db.execute("DELETE FROM showtimes WHERE showtime < ?", (cutoff,))
    # Also remove films with no remaining showtimes
    db.execute("""
        DELETE FROM films WHERE id NOT IN (
            SELECT DISTINCT film_id FROM showtimes
        )
    """)
