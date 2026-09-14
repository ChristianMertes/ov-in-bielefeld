"""TMDb API integration for resolving German titles to original titles and metadata.

Requires a TMDb API key (free for non-commercial use):
  https://www.themoviedb.org/settings/api

Set the environment variable TMDB_API_KEY.
"""
import logging
import re
from datetime import UTC, datetime, timedelta

import requests

import settings
from database import get_db, get_tmdb_cache, set_tmdb_cache

logger = logging.getLogger(__name__)

TMDB_BASE_URL = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500"

# Languages we're interested in for OV screenings
RELEVANT_LANGUAGES = {"en", "fr"}

# A negative entry suppresses the orchestrator's language filter, which only
# runs on a TMDb match – so never trust one forever. Films also get added to
# TMDb after their cinema release, which this re-check picks up.
NEGATIVE_CACHE_TTL = timedelta(days=7)


class TmdbUnavailableError(RuntimeError):
    """TMDb could not be reached – distinct from 'TMDb knows no such film'."""


def _redact(e: Exception) -> str:
    """Stringify an exception, redacting any API key in URL query params.

    Handles the empty-key case naturally: a missing key simply won't match.
    """
    return re.sub(r"(api_key|apikey)=[^&\s]+", r"\1=REDACTED", str(e), flags=re.IGNORECASE)


def lookup_film(title: str, year: int | None = None) -> dict | None:
    """Look up a film by its (German) display title on TMDb.

    Returns dict with metadata or None if not found/not relevant.
    """
    api_key = settings.TMDB_API_KEY
    if not api_key:
        logger.warning("TMDB_API_KEY not set. Skipping metadata lookup.")
        return None

    # Check cache first
    cache_key = f"{title}|{year or ''}"
    with get_db() as db:
        cached = get_tmdb_cache(db, cache_key)
    if cached:
        if cached["tmdb_id"] is not None:
            return dict(cached)
        if not _negative_entry_expired(cached["cached_at"]):
            return None  # Previously looked up and not found/not relevant

    # Clean up title for search
    search_title = _clean_title_for_search(title)

    try:
        result = _search_tmdb(search_title, year, api_key)
    except TmdbUnavailableError as e:
        # Caching this would turn a blip into a permanent "not found"
        logger.warning("TMDb unavailable for '%s', not caching: %s", title, _redact(e))
        return None

    # Cache the result (even if None, to avoid repeated lookups)
    with get_db() as db:
        if result:
            set_tmdb_cache(db, cache_key, **result)
        else:
            set_tmdb_cache(db, cache_key, tmdb_id=None)

    return result


def _negative_entry_expired(cached_at: str | None) -> bool:
    """Return True if a negative cache entry is old enough to re-check."""
    if not cached_at:
        return True
    try:
        stamp = datetime.fromisoformat(cached_at)
    except (ValueError, TypeError):
        return True
    # SQLite's datetime('now') default writes naive UTC
    return datetime.now(UTC).replace(tzinfo=None) - stamp > NEGATIVE_CACHE_TTL


def _clean_title_for_search(title: str) -> str:
    """Remove noise from title for better TMDb search results."""
    # Remove year in parentheses (TMDb has its own year filter)
    title = re.sub(r"\s*\(\d{4}\)\s*", " ", title)
    # Remove "3D", "IMAX" etc.
    title = re.sub(r"\b(3D|IMAX|4DX|Dolby)\b", "", title, flags=re.IGNORECASE)
    return title.strip()


def _search_tmdb(title: str, year: int | None, api_key: str) -> dict | None:
    """Search TMDb for a film by title. Tries German title first, then original."""
    # Strategy 1: Search in German language context
    result = _tmdb_search_request(title, api_key, language="de-DE", year=year)
    if result:
        return result

    # Strategy 2: Search without language restriction (might catch original titles)
    result = _tmdb_search_request(title, api_key, language="en-US", year=year)
    if result:
        return result

    # Strategy 3: If title looks like it might be the original title
    # (no German articles, contains English/French words), try direct search
    if re.search(r"[A-Za-z]{3,}", title) and not re.search(r"[äöüÄÖÜß]", title):
        result = _tmdb_search_request(title, api_key, language=None, year=year)
        if result:
            return result

    logger.debug("No TMDb match for '%s' (year=%s)", title, year)
    return None


def _tmdb_search_request(title: str, api_key: str, language: str | None = None,
                         year: int | None = None) -> dict | None:
    """Execute a TMDb search API request."""
    params: dict[str, str | int] = {
        "api_key": api_key,
        "query": title,
    }
    if language:
        params["language"] = language
    if year:
        params["year"] = year

    try:
        resp = requests.get(
            f"{TMDB_BASE_URL}/search/movie",
            params=params,
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.error("TMDb API error: %s", _redact(e))
        msg = f"TMDb search failed: {_redact(e)}"
        raise TmdbUnavailableError(msg) from e

    results = data.get("results", [])
    if not results:
        return None

    # Take the first result (highest relevance)
    movie = results[0]

    original_language = movie.get("original_language", "")

    # Get more details (including IMDb ID, runtime, and German title)
    details = _get_movie_details(movie["id"], api_key, language="de-DE")

    tmdb_id = movie["id"]
    poster_path = movie.get("poster_path")

    # German title: prefer details response (searched with de-DE), fall back to search result title
    title_de = (details.get("title") if details else None) or movie.get("title")
    # Don't store German title if it's the same as the original title
    if title_de == movie.get("original_title"):
        title_de = None

    return {
        "tmdb_id": tmdb_id,
        "imdb_id": details.get("imdb_id") if details else None,
        "title_original": movie.get("original_title"),
        "title_de": title_de,
        "original_language": original_language,
        "poster_url": f"{TMDB_IMAGE_BASE}{poster_path}" if poster_path else None,
        "overview": movie.get("overview", ""),
        "release_year": _extract_year(movie.get("release_date")),
        "runtime_minutes": details.get("runtime") if details else None,
        "tmdb_popularity": movie.get("popularity"),
    }


def _get_movie_details(tmdb_id: int, api_key: str, language: str = "de-DE") -> dict | None:
    """Fetch detailed movie info including IMDb ID."""
    try:
        resp = requests.get(
            f"{TMDB_BASE_URL}/movie/{tmdb_id}",
            params={"api_key": api_key, "language": language},
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.error("TMDb detail fetch error for ID %s: %s", tmdb_id, _redact(e))
        return None


# Theatrical release types in TMDb's release-date data: 2 = limited, 3 = wide.
# Both count as a cinema start, and distributors tag German starts either way.
_THEATRICAL_RELEASE_TYPES = "2|3"

# Bounds the per-candidate detail lookups below. A German release Thursday
# carries a handful of films per language; anything past that is noise.
MAX_RELEASE_CANDIDATES = 12


def discover_releases(release_date: str, original_language: str = "en") -> list[dict] | None:
    """Return films opening in German cinemas on a given date, most popular first.

    Used to work out which films an upcoming sneak preview could be showing.
    Returns None if TMDb could not be reached – distinct from an empty list,
    which means TMDb knows of no release on that date.
    """
    api_key = settings.TMDB_API_KEY
    if not api_key:
        logger.warning("TMDB_API_KEY not set. Skipping release discovery.")
        return None

    params = {
        "api_key": api_key,
        "region": "DE",
        "language": "de-DE",
        "sort_by": "popularity.desc",
        "with_release_type": _THEATRICAL_RELEASE_TYPES,
        "with_original_language": original_language,
        "release_date.gte": release_date,
        "release_date.lte": release_date,
    }

    try:
        resp = requests.get(f"{TMDB_BASE_URL}/discover/movie", params=params, timeout=10)
        resp.raise_for_status()
        results = resp.json().get("results", [])
    except requests.RequestException as e:
        logger.error("TMDb discover failed for %s: %s", release_date, _redact(e))
        return None

    return [
        _release_candidate(movie, api_key)
        for movie in results[:MAX_RELEASE_CANDIDATES]
    ]


def _release_candidate(movie: dict, api_key: str) -> dict:
    """Build a candidate record from a discover result, enriched with details.

    The discover response dates a film by the release we queried for, so the
    film's own release year – which is what tells a new film apart from a
    re-release sharing that date – has to come from the detail endpoint.
    """
    details = _get_movie_details(movie["id"], api_key, language="de-DE") or {}

    title_de = details.get("title") or movie.get("title")
    if title_de == movie.get("original_title"):
        title_de = None

    poster_path = movie.get("poster_path")

    return {
        "tmdb_id": movie["id"],
        "imdb_id": details.get("imdb_id"),
        "title_original": movie.get("original_title"),
        "title_de": title_de,
        "original_language": movie.get("original_language", ""),
        "poster_url": f"{TMDB_IMAGE_BASE}{poster_path}" if poster_path else None,
        "overview": details.get("overview") or movie.get("overview", ""),
        "release_year": _extract_year(details.get("release_date") or movie.get("release_date")),
        "runtime_minutes": details.get("runtime"),
        "popularity": movie.get("popularity"),
    }


def is_relevant_language(language_code: str) -> bool:
    """Check if a language code is one we care about (en, fr)."""
    return language_code in RELEVANT_LANGUAGES


def _extract_year(date_str: str | None) -> int | None:
    if not date_str:
        return None
    match = re.search(r"(\d{4})", date_str)
    return int(match.group(1)) if match else None


def get_imdb_url(imdb_id: str | None) -> str | None:
    """Construct an IMDb URL from an IMDb ID."""
    if not imdb_id:
        return None
    return f"https://www.imdb.com/title/{imdb_id}/"


def get_tmdb_url(tmdb_id: int | None) -> str | None:
    """Construct a TMDb URL."""
    if not tmdb_id:
        return None
    return f"https://www.themoviedb.org/movie/{tmdb_id}"


def get_omdb_url(imdb_id: str | None) -> str | None:
    """Construct an OMDb URL from an IMDb ID."""
    if not imdb_id:
        return None
    return f"https://www.omdbapi.com/?i={imdb_id}"
