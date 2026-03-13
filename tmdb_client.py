"""TMDb API integration for resolving German titles to original titles and metadata.

Requires a TMDb API key (free for non-commercial use):
  https://www.themoviedb.org/settings/api

Set the environment variable TMDB_API_KEY.
"""
import os
import re
import logging
from typing import Optional
import requests

from database import get_db, get_tmdb_cache, set_tmdb_cache

logger = logging.getLogger(__name__)

TMDB_BASE_URL = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500"

# Languages we're interested in for OV screenings
RELEVANT_LANGUAGES = {"en", "fr"}

# Languages to exclude (German films shown "OmU" are just German with German subs)
EXCLUDE_LANGUAGES = {"de"}


def lookup_film(title: str, year: Optional[int] = None) -> Optional[dict]:
    """Look up a film by its (German) display title on TMDb.

    Returns dict with metadata or None if not found/not relevant.
    """
    api_key = os.environ.get("TMDB_API_KEY")
    if not api_key:
        logger.warning("TMDB_API_KEY not set. Skipping metadata lookup.")
        return None

    # Check cache first
    cache_key = f"{title}|{year or ''}"
    with get_db() as db:
        cached = get_tmdb_cache(db, cache_key)
        if cached:
            if cached["tmdb_id"] is None:
                return None  # Previously looked up and not found/not relevant
            return dict(cached)

    # Clean up title for search
    search_title = _clean_title_for_search(title)

    result = _search_tmdb(search_title, year, api_key)

    # Cache the result (even if None, to avoid repeated lookups)
    with get_db() as db:
        if result:
            set_tmdb_cache(db, cache_key, **result)
        else:
            set_tmdb_cache(db, cache_key, tmdb_id=None)

    return result


def _clean_title_for_search(title: str) -> str:
    """Remove noise from title for better TMDb search results."""
    # Remove year in parentheses (TMDb has its own year filter)
    title = re.sub(r"\s*\(\d{4}\)\s*", " ", title)
    # Remove common German prefixes/suffixes
    title = re.sub(r"^(Der|Die|Das)\s+", "", title, flags=re.IGNORECASE)
    # Remove "3D", "IMAX" etc.
    title = re.sub(r"\b(3D|IMAX|4DX|Dolby)\b", "", title, flags=re.IGNORECASE)
    return title.strip()


def _search_tmdb(title: str, year: Optional[int], api_key: str) -> Optional[dict]:
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

    logger.debug(f"No TMDb match for '{title}' (year={year})")
    return None


def _tmdb_search_request(title: str, api_key: str, language: Optional[str] = None,
                         year: Optional[int] = None) -> Optional[dict]:
    """Execute a TMDb search API request."""
    params = {
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
        logger.error(f"TMDb API error: {e}")
        return None

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


def _get_movie_details(tmdb_id: int, api_key: str, language: str = "de-DE") -> Optional[dict]:
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
        logger.error(f"TMDb detail fetch error for ID {tmdb_id}: {e}")
        return None


def is_relevant_language(language_code: str) -> bool:
    """Check if a language code is one we care about (en, fr)."""
    return language_code in RELEVANT_LANGUAGES


def _extract_year(date_str: Optional[str]) -> Optional[int]:
    if not date_str:
        return None
    match = re.search(r"(\d{4})", date_str)
    return int(match.group(1)) if match else None


def get_imdb_url(imdb_id: Optional[str]) -> Optional[str]:
    """Construct an IMDb URL from an IMDb ID."""
    if not imdb_id:
        return None
    return f"https://www.imdb.com/title/{imdb_id}/"


def get_tmdb_url(tmdb_id: Optional[int]) -> Optional[str]:
    """Construct a TMDb URL."""
    if not tmdb_id:
        return None
    return f"https://www.themoviedb.org/movie/{tmdb_id}"


def get_omdb_url(imdb_id: Optional[str]) -> Optional[str]:
    """Construct an OMDb URL from an IMDb ID."""
    if not imdb_id:
        return None
    return f"https://www.omdbapi.com/?i={imdb_id}"
