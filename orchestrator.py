"""Orchestrator: runs scrapers, enriches with TMDb, updates database.

Processing happens in three distinct phases to avoid nested SQLite connections:

  Phase 1 – Scraping: fetch film/showtime data from all sources (no DB writes)
  Phase 2 – TMDb enrichment: one lookup per film, each opens/closes its own DB
             connection to read/write the tmdb_cache table
  Phase 3 – DB writes: single transaction for all film and showtime upserts
"""
from dotenv import load_dotenv
load_dotenv()

import logging
from datetime import datetime

from database import (
    get_db, init_db, upsert_film, upsert_showtime,
    cleanup_old_showtimes, get_new_unnotified_films,
    get_films_with_imdb_id, update_film_ratings, update_film_rt_score,
)
from ratings_client import fetch_imdb_ratings, fetch_rt_scores
from scrapers.arthouse import scrape_arthouse
from scrapers.cinemaxx import scrape_cinemaxx
from tmdb_client import lookup_film, is_relevant_language
import cache

logger = logging.getLogger(__name__)


def run_scrape(notify_callback=None):
    """Run all scrapers, enrich data, update database, and optionally notify."""
    init_db()

    # ── Phase 1: Scraping ──────────────────────────────────────────────────
    all_films = []

    try:
        arthouse_films = scrape_arthouse()
        all_films.extend(arthouse_films)
        logger.info(f"Arthouse: {len(arthouse_films)} OV/OmU films")
    except Exception as e:
        logger.error(f"Arthouse scrape failed: {e}")

    try:
        cinemaxx_films = scrape_cinemaxx()
        all_films.extend(cinemaxx_films)
        logger.info(f"CinemaxX: {len(cinemaxx_films)} OV/OmU films")
    except Exception as e:
        logger.error(f"CinemaxX scrape failed: {e}")

    if not all_films:
        logger.warning("No films found from any source!")
        return

    # ── Phase 2: TMDb enrichment ───────────────────────────────────────────
    # All lookup_film() calls open/close their own short-lived DB connections
    # to access tmdb_cache. This phase must complete before Phase 3 opens the
    # main write transaction, to avoid nested write locks on the same DB.
    enriched_films = []
    for film_data in all_films:
        try:
            film_data = _enrich_with_tmdb(film_data)
            if film_data is not None:
                enriched_films.append(film_data)
        except Exception as e:
            title = film_data.get("title_display", "?")
            logger.error(f"TMDb enrichment failed for '{title}': {e}")

    logger.info(
        f"TMDb: {len(all_films)} films checked, "
        f"{len(enriched_films)} passed language filter"
    )

    # ── Phase 3: DB writes ─────────────────────────────────────────────────
    new_films = []
    with get_db() as db:
        for film_data in enriched_films:
            try:
                film_id, is_new = _write_film(db, film_data)
                if is_new:
                    new_films.append((film_id, film_data))
            except Exception as e:
                title = film_data.get("title_display", "?")
                logger.error(f"DB write failed for '{title}': {e}")

        cleanup_old_showtimes(db)

    logger.info(
        f"Scrape complete: {len(enriched_films)} films written, "
        f"{len(new_films)} new"
    )

    # ── Phase 4: IMDb ratings ──────────────────────────────────────────────
    with get_db() as db:
        films_to_rate = get_films_with_imdb_id(db)

    if films_to_rate:
        imdb_ids = [f["imdb_id"] for f in films_to_rate]

        ratings = fetch_imdb_ratings(imdb_ids)
        if ratings:
            with get_db() as db:
                for film in films_to_rate:
                    r = ratings.get(film["imdb_id"])
                    if r:
                        update_film_ratings(db, film["id"], r["rating"], r["votes"])
            logger.info(f"IMDb ratings: updated {len(ratings)} of {len(films_to_rate)} films")

        rt_scores = fetch_rt_scores(imdb_ids)
        if rt_scores:
            with get_db() as db:
                for film in films_to_rate:
                    score = rt_scores.get(film["imdb_id"])
                    if score is not None:
                        update_film_rt_score(db, film["id"], score)
            logger.info(f"RT scores: updated {len(rt_scores)} of {len(films_to_rate)} films")

    if notify_callback and new_films:
        for film_id, film_data in new_films:
            try:
                notify_callback(film_id, film_data)
            except Exception as e:
                logger.error(f"Notification failed for '{film_data['title_display']}': {e}")

    cache.invalidate()

    return {
        "total_films": len(enriched_films),
        "new_films": len(new_films),
        "new_film_titles": [f["title_display"] for _, f in new_films],
    }


def _enrich_with_tmdb(film_data: dict) -> dict | None:
    """Look up film on TMDb and filter by original language.

    Returns film_data with _tmdb_data added, or None if the film should be
    skipped (wrong original language).

    Each call opens/closes its own DB connection (for tmdb_cache only).
    This must NOT be called while a write transaction is open.
    """
    title = film_data["title_display"]
    year = film_data.get("release_year")
    original_title = film_data.get("_original_title", "")
    arthouse_year = film_data.get("_arthouse_year")  # From detail page scrape

    def _try_lookup(t, y):
        """Lookup, then retry with stripped prefix if needed."""
        result = lookup_film(t, y)
        if not result and ": " in t:
            result = lookup_film(t.split(": ", 1)[1], y)
        return result

    tmdb_data = None
    if original_title and original_title.strip():
        tmdb_data = _try_lookup(original_title.strip(), year)
    if not tmdb_data:
        tmdb_data = _try_lookup(title, year)

    # If arthouse detail page gave us a year, check for year mismatch.
    # A large discrepancy means TMDb found the wrong film — retry with the correct year.
    if tmdb_data and arthouse_year:
        tmdb_year = tmdb_data.get("release_year")
        if tmdb_year and abs(tmdb_year - arthouse_year) > 3:
            logger.debug(
                f"Year mismatch for '{title}': TMDb={tmdb_year}, arthouse={arthouse_year} — retrying"
            )
            retry = _try_lookup(title, arthouse_year)
            tmdb_data = retry  # May be None if correct film not on TMDb

    if tmdb_data:
        orig_lang = tmdb_data.get("original_language", "")
        if orig_lang and not is_relevant_language(orig_lang):
            logger.debug(
                f"Skipping '{title}': original language '{orig_lang}' not EN/FR"
            )
            return None

    film_data["_tmdb_data"] = tmdb_data
    return film_data


def _write_film(db, film_data: dict) -> tuple[int, bool]:
    """Write a single film and its showtimes to the DB.

    Must be called within an open get_db() context.
    Returns (film_id, is_new).
    """
    title = film_data["title_display"]
    tmdb_data = film_data.get("_tmdb_data")

    kwargs = {}
    if tmdb_data:
        kwargs.update({
            "title_original": tmdb_data.get("title_original"),
            "title_de": tmdb_data.get("title_de"),
            "original_language": tmdb_data.get("original_language"),
            "tmdb_id": tmdb_data.get("tmdb_id"),
            "imdb_id": tmdb_data.get("imdb_id"),
            "poster_url": tmdb_data.get("poster_url"),
            "overview": tmdb_data.get("overview"),
            "release_year": tmdb_data.get("release_year"),
            "runtime_minutes": tmdb_data.get("runtime_minutes"),
            "tmdb_popularity": tmdb_data.get("tmdb_popularity"),
        })
    else:
        # No TMDb match — use scraper metadata as fallback
        year = film_data.get("release_year")
        if year:
            kwargs["release_year"] = year
        if film_data.get("duration_minutes"):
            kwargs["runtime_minutes"] = film_data["duration_minutes"]
        if film_data.get("_original_title"):
            kwargs["title_original"] = film_data["_original_title"]
        if film_data.get("_poster_url"):
            kwargs["poster_url"] = film_data["_poster_url"]

    film_id, is_new = upsert_film(db, title, **kwargs)

    for st in film_data.get("showtimes", []):
        if st.get("_placeholder"):
            continue
        upsert_showtime(
            db, film_id,
            cinema=st["cinema"],
            showtime=st["showtime"],
            language_tag=st.get("language_tag"),
            booking_url=st.get("booking_url"),
        )

    return film_id, is_new


if __name__ == "__main__":
    from log_setup import setup_logging
    setup_logging()
    result = run_scrape()
    if result:
        print(f"\nResults: {result['total_films']} films, {result['new_films']} new")
        if result["new_film_titles"]:
            print("New films:")
            for t in result["new_film_titles"]:
                print(f"  - {t}")
