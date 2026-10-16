"""Orchestrator: runs scrapers, enriches with TMDb, updates database.

Processing happens in three distinct phases to avoid nested SQLite connections:

  Phase 1 – Scraping: fetch film/showtime data from all sources (no DB writes)
  Phase 2 – TMDb enrichment: one lookup per film, each opens/closes its own DB
             connection to read/write the tmdb_cache table
  Phase 3 – DB writes: single transaction for all film and showtime upserts
"""
import logging
import re
import sqlite3
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

import cache
from database import (
    cleanup_old_showtimes,
    cleanup_old_sneak_candidates,
    delete_unmatched_films,
    get_db,
    get_films_with_imdb_id,
    get_sneak_showtimes,
    init_db,
    replace_sneak_candidates,
    update_film_ratings,
    update_film_rt_score,
    upsert_film,
    upsert_showtime,
)
from production_country import maybe_en_or_fr
from ratings_client import fetch_imdb_ratings, fetch_rt_scores
from scrapers.arthouse import scrape_arthouse
from scrapers.cinemaxx import scrape_cinemaxx
from sneak import DEFAULT_SNEAK_LANGUAGE, is_sneak_title, official_release_date, sneak_language
from timeutil import now_local
from tmdb_client import (
    contradicts_production,
    discover_releases,
    find_by_credits,
    is_relevant_language,
    lookup_film,
)

logger = logging.getLogger(__name__)

# Arthouse joins the original and German title with a dash, e.g.
# "Bir kar tanesinin ömrü – Die Lebensdauer einer Schneeflocke". Only a dash
# surrounded by whitespace separates two titles; "Spider-Man" must stay intact.
_TITLE_SEPARATOR = re.compile(r"\s+[-–—]\s+")

# Shorter fragments ("Teil 2", "OV") are too ambiguous to search TMDb with.
_MIN_TITLE_PART_LEN = 3

# A series banner such as "Cinema! Italia!" is printed in front of the title
# with no separator at all, so the colon split below cannot see it. The
# scrapers strip the banners they know; this catches the next one, because an
# unsearchable title means no match, and no match means no language filter.
# Two or more exclamation-terminated words are a banner, never a title, which
# is what keeps "Mamma Mia! Here We Go Again" whole.
_SERIES_BANNER = re.compile(r"^(?:\S+!\s+){2,}(?=\S)")

# Marks a film dropped on nothing but its production country, which is the one
# drop the site's visitors cannot see for themselves.
_DROPPED_BY_COUNTRY = "production_country"

# How many release Thursdays to fetch a candidate line-up for in advance, on
# top of the ones upcoming sneaks already point at. Two covers the week a
# cinema's programme reaches into, so a sneak's line-up is stored before the
# sneak is announced.
PREFETCH_RELEASE_DATES = 2


def _title_candidates(title: str) -> list[str]:
    """Return the TMDb search variants for a scraped title, most specific first.

    Scraped titles carry section prefixes ("CINÉMA_FRANÇAIS: ...") and often
    join two titles with a dash. TMDb knows the individual titles but not those
    combinations, so the prefix-stripped and split forms are tried as fallbacks.
    """
    candidates = [title]
    if ": " in title:
        candidates.append(title.split(": ", 1)[1])

    for base in list(candidates):
        without_banner = _SERIES_BANNER.sub("", base, count=1)
        if without_banner != base:
            candidates.append(without_banner)

    for base in list(candidates):
        parts = [p.strip() for p in _TITLE_SEPARATOR.split(base, maxsplit=1)]
        if len(parts) == 2:
            candidates.extend(p for p in parts if len(p) >= _MIN_TITLE_PART_LEN)

    unique = []
    for candidate in candidates:
        if candidate and candidate not in unique:
            unique.append(candidate)
    return unique


def run_scrape(notify_callback: Callable[[int, dict], None] | None = None) -> dict[str, Any] | None:
    """Run all scrapers, enrich data, update database, and optionally notify."""
    init_db()

    # ── Phase 1: Scraping ──────────────────────────────────────────────────
    all_films = []

    try:
        arthouse_films = scrape_arthouse()
        all_films.extend(arthouse_films)
        logger.info("Arthouse: %d OV/OmU films", len(arthouse_films))
    except Exception as e:  # noqa: BLE001
        logger.error("Arthouse scrape failed: %s", e)

    try:
        cinemaxx_films = scrape_cinemaxx()
        all_films.extend(cinemaxx_films)
        logger.info("CinemaxX: %d OV/OmU films", len(cinemaxx_films))
    except Exception as e:  # noqa: BLE001
        logger.error("CinemaxX scrape failed: %s", e)

    if not all_films:
        logger.warning("No films found from any source!")
        return None

    # ── Phase 2: TMDb enrichment ───────────────────────────────────────────
    # All lookup_film() calls open/close their own short-lived DB connections
    # to access tmdb_cache. This phase must complete before Phase 3 opens the
    # main write transaction, to avoid nested write locks on the same DB.
    enriched_films = []
    rejected_titles: list[str] = []
    dropped_unidentified: list[dict] = []
    for film_data in all_films:
        try:
            enriched = _enrich_with_tmdb(film_data)
            if enriched is not None:
                enriched_films.append(enriched)
            else:
                rejected_titles.extend(_stored_under(film_data))
                if film_data.get("_drop_reason") == _DROPPED_BY_COUNTRY:
                    dropped_unidentified.append({
                        "title": film_data["title_display"],
                        "country": film_data.get("_production_country"),
                    })
        except Exception as e:  # noqa: BLE001
            title = film_data.get("title_display", "?")
            logger.error("TMDb enrichment failed for '%s': %s", title, e)

    logger.info("TMDb: %d films checked, %d passed language filter", len(all_films), len(enriched_films))

    # ── Phase 3: DB writes ─────────────────────────────────────────────────
    new_films = []
    with get_db() as db:
        # Before the writes, so a film another source delivers in this same run
        # is not removed again right after being stored.
        removed = delete_unmatched_films(db, rejected_titles)
        if removed:
            logger.info("Removed %d film(s) now known not to be EN/FR", removed)

        for film_data in enriched_films:
            try:
                film_id, is_new = _write_film(db, film_data)
                if is_new:
                    new_films.append((film_id, film_data))
            except Exception as e:  # noqa: BLE001
                title = film_data.get("title_display", "?")
                logger.error("DB write failed for '%s': %s", title, e)

        cleanup_old_showtimes(db)

    logger.info("Scrape complete: %d films written, %d new", len(enriched_films), len(new_films))

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
            logger.info("IMDb ratings: updated %d of %d films", len(ratings), len(films_to_rate))

        rt_scores = fetch_rt_scores(imdb_ids)
        if rt_scores:
            with get_db() as db:
                for film in films_to_rate:
                    score = rt_scores.get(film["imdb_id"])
                    if score is not None:
                        update_film_rt_score(db, film["id"], score)
            logger.info("RT scores: updated %d of %d films", len(rt_scores), len(films_to_rate))

    # ── Phase 5: Sneak preview candidates ──────────────────────────────────
    try:
        _refresh_sneak_candidates()
    except Exception as e:  # noqa: BLE001
        logger.error("Sneak candidate refresh failed: %s", e)

    if notify_callback and new_films:
        for film_id, film_data in new_films:
            try:
                notify_callback(film_id, film_data)
            except Exception as e:  # noqa: BLE001
                logger.error("Notification failed for '%s': %s", film_data["title_display"], e)

    cache.invalidate()

    return {
        "total_films": len(enriched_films),
        "new_films": len(new_films),
        "new_film_titles": [f["title_display"] for _, f in new_films],
        # Dropping a film leaves no trace on the site, so the caller gets told
        # and can pass it on to whoever runs this.
        "dropped_unidentified": dropped_unidentified,
    }


def _stored_under(film_data: dict) -> list[str]:
    """Return the titles an earlier scrape could have stored this film under.

    Scrapers clean the title they report, so a film that was stored before a
    banner was known sits under the raw one.
    """
    titles = [film_data.get("title_display"), film_data.get("title_raw")]
    return list(dict.fromkeys(t for t in titles if t))


def _sneak_release_dates(showtimes: list) -> dict[str, str]:
    """Return the release dates to fetch a line-up for, each with its language.

    Every upcoming sneak contributes the Thursday it points at. On top of that
    the next few Thursdays are fetched ahead of time: a cinema announces its
    programme about a week out, so without the head start the Thursday a newly
    announced sneak points at would have no line-up stored until the scrape
    after the one that discovered the sneak.
    """
    languages_by_date: dict[str, str] = {}
    for st in showtimes:
        try:
            screening_date = datetime.fromisoformat(st["showtime"]).date()
        except (ValueError, TypeError):
            continue
        release_date = official_release_date(screening_date).isoformat()
        languages_by_date.setdefault(
            release_date, st["original_language"] or DEFAULT_SNEAK_LANGUAGE
        )

    if not languages_by_date:
        return {}  # No sneaks in the programme – nothing to look ahead for.

    language = next(iter(languages_by_date.values()))
    upcoming = official_release_date(now_local().date())
    for _ in range(PREFETCH_RELEASE_DATES):
        languages_by_date.setdefault(upcoming.isoformat(), language)
        upcoming += timedelta(days=7)

    return languages_by_date


def _refresh_sneak_candidates() -> None:
    """Fetch the release line-up every upcoming sneak preview could draw from.

    A sneak shows a film that opens the next release Thursday, so one TMDb
    discover query per distinct Thursday gives the shortlist. TMDb being down
    leaves the stored line-up in place rather than emptying the page.
    """
    with get_db() as db:
        showtimes = get_sneak_showtimes(db)

    for release_date, language in _sneak_release_dates(showtimes).items():
        candidates = discover_releases(release_date, language)
        if candidates is None:
            logger.warning("Sneak candidates for %s unavailable – keeping stored list", release_date)
            continue
        with get_db() as db:
            replace_sneak_candidates(db, release_date, candidates)
        logger.info("Sneak candidates for %s (%s): %d", release_date, language, len(candidates))

    with get_db() as db:
        cleanup_old_sneak_candidates(db)


def _enrich_with_tmdb(film_data: dict) -> dict | None:
    """Look up film on TMDb and filter by original language.

    Returns film_data with _tmdb_data added, or None if the film should be
    skipped (wrong original language).

    Each call opens/closes its own DB connection (for tmdb_cache only).
    This must NOT be called while a write transaction is open.
    """
    title = film_data["title_display"]

    if is_sneak_title(title):
        # A sneak's title is a placeholder, not a film title – and TMDb knows a
        # real film called "Sneak Preview", which a lookup happily returns.
        film_data["_is_sneak"] = True
        film_data["_tmdb_data"] = None
        return film_data

    year = film_data.get("release_year")
    original_title = film_data.get("_original_title", "")
    arthouse_year = film_data.get("_arthouse_year")  # From detail page scrape

    def _try_lookup(t: str, y: int | None) -> dict | None:
        """Try the title, then its prefix-stripped and dash-split variants."""
        for candidate in _title_candidates(t):
            # The page's production year does not narrow the search, it picks
            # between what comes back – otherwise a remake loses to the
            # better-known film it is remaking.
            result = lookup_film(candidate, y, prefer_year=arthouse_year or y)
            if result:
                return result
        return None

    tmdb_data = None
    if original_title and original_title.strip():
        tmdb_data = _try_lookup(original_title.strip(), year)
    if not tmdb_data:
        tmdb_data = _try_lookup(title, year)

    # If the arthouse detail page gave us a year, a large discrepancy means
    # TMDb found the wrong film – retry with the correct year.
    if tmdb_data and arthouse_year:
        tmdb_year = tmdb_data.get("release_year")
        if tmdb_year and abs(tmdb_year - arthouse_year) > 3:
            logger.debug(
                "Year mismatch for '%s': TMDb=%s, arthouse=%s – retrying", title, tmdb_year, arthouse_year
            )
            retry = _try_lookup(title, arthouse_year)
            if retry:  # May be None if correct film not on TMDb – keep original match
                tmdb_data = retry

    # A title search answers with its best guess, never with a certainty. Where
    # the page states a year or a production country, hold the guess against
    # them: a match that disagrees with the cinema is a different film, and its
    # language would be the wrong one to filter on.
    page_country = film_data.get("_production_country")
    page_year = arthouse_year or year

    def _accept(candidate: dict | None) -> dict | None:
        """Return the match unless the programme page contradicts it."""
        if candidate and contradicts_production(candidate["tmdb_id"], page_year, page_country):
            logger.warning(
                "Dropping the TMDb match for '%s': it contradicts the programme page", title
            )
            return None
        return candidate

    tmdb_data = _accept(tmdb_data)

    # Search again, now constrained to the year the page states. TMDb's own
    # year filter returns a different set of films, not just a different order,
    # so this surfaces candidates the popularity-ranked search never offered.
    if not tmdb_data and page_year:
        tmdb_data = _accept(_try_lookup(title, page_year))

    # Still nothing: ask who made the film. A banner glued to the front of the
    # title, or a German title TMDb files differently, both end up here, and
    # the credits identify the film where the title cannot.
    if not tmdb_data:
        tmdb_data = find_by_credits(
            film_data.get("title_raw") or title,
            page_year,
            cast=film_data.get("_cast"),
            director=film_data.get("_director"),
        )

    if tmdb_data:
        orig_lang = tmdb_data.get("original_language", "")
        if orig_lang and not is_relevant_language(orig_lang):
            logger.debug("Skipping '%s': original language '%s' not EN/FR", title, orig_lang)
            film_data["_drop_reason"] = "language"
            return None
    else:
        country = film_data.get("_production_country")
        if maybe_en_or_fr(country) is False:
            # Unidentified and from a country that makes neither English nor
            # French originals. That is thin evidence, so it is logged and
            # reported rather than applied quietly.
            logger.warning(
                "Dropping '%s': nothing identified it and it is a %s production", title, country
            )
            film_data["_drop_reason"] = _DROPPED_BY_COUNTRY
            return None
        film_data["_unidentified"] = True

    film_data["_tmdb_data"] = tmdb_data
    return film_data


def _write_film(db: sqlite3.Connection, film_data: dict) -> tuple[int, bool]:
    """Write a single film and its showtimes to the DB.

    Must be called within an open get_db() context.
    Returns (film_id, is_new).
    """
    title = film_data["title_display"]
    tmdb_data = film_data.get("_tmdb_data")

    kwargs: dict[str, Any] = {}
    if film_data.get("_is_sneak"):
        # Deliberately no year, runtime, poster or original title: what the
        # scrapers report for a sneak describes the slot in the programme, not
        # the film that will run in it. The language does hold – cinemas
        # announce whether a sneak is an OV one.
        kwargs["is_sneak"] = 1
        kwargs["original_language"] = (
            sneak_language(title)
            or film_data.get("_detected_language")
            or DEFAULT_SNEAK_LANGUAGE
        )
    elif tmdb_data:
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
        # No TMDb match – use scraper metadata as fallback
        year = film_data.get("release_year")
        if year:
            kwargs["release_year"] = year
        if film_data.get("duration_minutes"):
            kwargs["runtime_minutes"] = film_data["duration_minutes"]
        if film_data.get("_original_title"):
            kwargs["title_original"] = film_data["_original_title"]
        if film_data.get("_poster_url"):
            kwargs["poster_url"] = film_data["_poster_url"]
        # Scrapers that matched an explicit EN/FR language attribute expose it;
        # persist it so the film stays visible under ?lang= filters even without
        # a TMDb match. Arthouse films without a match leave this None by design.
        if film_data.get("_detected_language"):
            kwargs["original_language"] = film_data["_detected_language"]

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
