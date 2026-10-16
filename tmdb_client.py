"""TMDb API integration for resolving German titles to original titles and metadata.

Requires a TMDb API key (free for non-commercial use):
  https://www.themoviedb.org/settings/api

Set the environment variable TMDB_API_KEY.
"""
import logging
import re
from datetime import UTC, date, datetime, timedelta

import requests

import settings
from database import get_db, get_tmdb_cache, set_tmdb_cache
from production_country import iso_codes

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


def lookup_film(title: str, year: int | None = None,
                prefer_year: int | None = None) -> dict | None:
    """Look up a film by its (German) display title on TMDb.

    `prefer_year` is the year the cinema states. It does not narrow the search,
    it decides between the films it returns – see _best_match.

    Returns dict with metadata or None if not found/not relevant.
    """
    api_key = settings.TMDB_API_KEY
    if not api_key:
        logger.warning("TMDB_API_KEY not set. Skipping metadata lookup.")
        return None

    # Check cache first. The preference belongs in the key, since the same
    # title can resolve to a different film under a different one; it is only
    # appended when it says something new, so earlier entries stay valid.
    cache_key = f"{title}|{year or ''}"
    if prefer_year and prefer_year != year:
        cache_key += f"|~{prefer_year}"
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
        result = _search_tmdb(search_title, year, api_key, prefer_year)
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


def _search_tmdb(title: str, year: int | None, api_key: str,
                 prefer_year: int | None = None) -> dict | None:
    """Search TMDb for a film by title. Tries German title first, then original."""
    # Strategy 1: Search in German language context
    result = _tmdb_search_request(title, api_key, language="de-DE", year=year,
                                  prefer_year=prefer_year)
    if result:
        return result

    # Strategy 2: Search without language restriction (might catch original titles)
    result = _tmdb_search_request(title, api_key, language="en-US", year=year,
                                  prefer_year=prefer_year)
    if result:
        return result

    # Strategy 3: If title looks like it might be the original title
    # (no German articles, contains English/French words), try direct search
    if re.search(r"[A-Za-z]{3,}", title) and not re.search(r"[äöüÄÖÜß]", title):
        result = _tmdb_search_request(title, api_key, language=None, year=year,
                                      prefer_year=prefer_year)
        if result:
            return result

    logger.debug("No TMDb match for '%s' (year=%s)", title, year)
    return None


def _tmdb_search_request(title: str, api_key: str, language: str | None = None,
                         year: int | None = None,
                         prefer_year: int | None = None) -> dict | None:
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

    movie = _best_match(results, prefer_year, title)

    # Get more details (including IMDb ID, runtime, and German title)
    details = _get_movie_details(movie["id"], api_key, language="de-DE")

    return _film_record(movie, details)


def _film_record(movie: dict, details: dict | None) -> dict:
    """Map a TMDb movie plus its detail response into the shape we store."""
    poster_path = movie.get("poster_path")

    # German title: prefer details response (searched with de-DE), fall back to search result title
    title_de = (details.get("title") if details else None) or movie.get("title")
    # Don't store German title if it's the same as the original title
    if title_de == movie.get("original_title"):
        title_de = None

    return {
        "tmdb_id": movie["id"],
        "imdb_id": details.get("imdb_id") if details else None,
        "title_original": movie.get("original_title"),
        "title_de": title_de,
        "original_language": movie.get("original_language", ""),
        "poster_url": f"{TMDB_IMAGE_BASE}{poster_path}" if poster_path else None,
        "overview": movie.get("overview", ""),
        "release_year": _extract_year(movie.get("release_date")),
        "runtime_minutes": details.get("runtime") if details else None,
        "tmdb_popularity": movie.get("popularity"),
    }


# Ranks a result TMDb gives no release date for behind every dated one,
# without dropping it: it is still better than nothing.
_UNDATED_DISTANCE = 10_000

# How popular a result has to be, relative to the search's top hit, before the
# stated year may promote it over that hit.
_MIN_POPULARITY_SHARE = 0.1


def _best_match(results: list[dict], prefer_year: int | None, title: str) -> dict:
    """Pick the search result closest to the year the programme states.

    TMDb sorts by popularity, which hands a remake's search the classic:
    "Sinn und Sinnlichkeit" returns Ang Lee's 1995 film ahead of the 2026
    adaptation the cinema is actually showing, and the API's own `year` filter
    does not help, because it matches any release date a film carries and the
    classic was re-released alongside the remake.

    The year only ever decides between films that go by this title, because a
    stated year is not always the film's. A marathon of the Lord of the Rings
    trilogy is listed as a 2025 production, and going by the year alone that
    pulls a 14-minute short called "Herr der Ringe" ahead of Peter Jackson.
    Where no result carries the title, popularity stays the better guess.
    """
    if prefer_year is None:
        return results[0]

    # An obscure namesake does not overtake a famous film on the year alone. A
    # CinemaxX marathon of the trilogy arrives as "Herr der Ringe" dated to the
    # event year, 2025, and a 14-minute short of that name really was released
    # in 2025 – Peter Jackson's entry is sixty times as popular, and the right
    # answer. A remake, which this heuristic exists for, is in the same league
    # as the film it remakes.
    popularity_floor = results[0].get("popularity", 0) * _MIN_POPULARITY_SHARE
    plausible = [
        movie for movie in results
        if _title_confirms(movie, title) and movie.get("popularity", 0) >= popularity_floor
    ]
    if not plausible:
        return results[0]

    def distance(indexed: tuple[int, dict]) -> tuple[int, int]:
        position, movie = indexed
        release_year = _extract_year(movie.get("release_date"))
        gap = abs(release_year - prefer_year) if release_year else _UNDATED_DISTANCE
        return gap, position  # position keeps TMDb's ranking as the tie-break

    return min(enumerate(plausible), key=distance)[1]


def _get_movie_details(tmdb_id: int, api_key: str, language: str = "de-DE",
                      append: str | None = None) -> dict | None:
    """Fetch detailed movie info including IMDb ID.

    `append` pulls sub-resources into the same request (TMDb's
    append_to_response), so asking for release dates costs no extra call.
    """
    params = {"api_key": api_key, "language": language}
    if append:
        params["append_to_response"] = append
    try:
        resp = requests.get(
            f"{TMDB_BASE_URL}/movie/{tmdb_id}",
            params=params,
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

# Numeric counterparts of _THEATRICAL_RELEASE_TYPES, for reading release dates
# back out of a detail response.
_THEATRICAL_TYPE_IDS = {2, 3}

# A German start this long after the film's own release is a re-release or a
# catalogue title. Festival runs and staggered international releases put a
# year or so between the two, so the cut-off sits well clear of those.
_MAX_RELEASE_DELAY = timedelta(days=2 * 365)


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

    candidates = []
    for movie in results[:MAX_RELEASE_CANDIDATES]:
        details = _get_movie_details(movie["id"], api_key, language="de-DE",
                                     append="release_dates") or {}
        if _is_rerelease(details, release_date):
            logger.debug("Skipping re-release '%s' for %s", movie.get("title"), release_date)
            continue
        candidates.append(_release_candidate(movie, details))
    return candidates


def _parse_date(value: str | None) -> date | None:
    """Parse a TMDb date, which may carry a time part ('2019-04-24T00:00:00Z')."""
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _german_theatrical_dates(details: dict) -> list[date]:
    """Return the German cinema release dates TMDb knows for a film."""
    for country in details.get("release_dates", {}).get("results", []):
        if country.get("iso_3166_1") != "DE":
            continue
        return [
            parsed
            for entry in country.get("release_dates", [])
            if entry.get("type") in _THEATRICAL_TYPE_IDS
            and (parsed := _parse_date(entry.get("release_date")))
        ]
    return []


def _is_rerelease(details: dict, release_date: str) -> bool:
    """True if this German start is not the film's first run in cinemas.

    A sneak preview shows a film about to open, so a film returning to cinemas
    is not a candidate. Two signs, because TMDb has either for most films and
    rarely both: an earlier German cinema date, or a film that is simply too
    old for this to be its first release anywhere. Without data on a film,
    neither triggers and the film stays a candidate.
    """
    start = _parse_date(release_date)
    if start is None:
        return False

    own_release = _parse_date(details.get("release_date"))
    if own_release and start - own_release > _MAX_RELEASE_DELAY:
        return True

    return any(earlier < start for earlier in _german_theatrical_dates(details))


def _release_candidate(movie: dict, details: dict) -> dict:
    """Build a candidate record from a discover result and its detail response.

    The discover response dates a film by the release we queried for, so the
    film's own release year – which is what tells a new film apart from an
    older one sharing that date – has to come from the detail endpoint.
    """
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


# People to search a film by. The director identifies a film on its own where
# the page names one; two cast members are enough everywhere else, and asking
# for more only costs lookups.
_MAX_CAST_LOOKUPS = 2

# Shorter than this, a confirmed title is too generic to trust as a match.
_MIN_TITLE_MATCH = 3

# Programme pages state the production year, TMDb the release year, and for a
# film premiering at a festival the two differ by up to a year either way.
_CREDIT_YEAR_SLACK = 1


def find_by_credits(title: str, year: int | None, cast: list[str] | None = None,
                    director: str | None = None) -> dict | None:
    """Identify a film from the people in it and the year it was produced.

    For when the title itself cannot be searched – a programme banner glued to
    the front of it, a German title TMDb files differently. The credits narrow
    the field to a handful of films; `title` then has to confirm one of them,
    by containing its German or original title. Without that confirmation this
    returns nothing, because credits and a year alone would just as happily
    return the wrong film from the same director.
    """
    api_key = settings.TMDB_API_KEY
    if not api_key or not year:
        return None

    crew_ids = _person_ids([director] if director else [], api_key)
    cast_ids = _person_ids((cast or [])[:_MAX_CAST_LOOKUPS], api_key)
    if not crew_ids and not cast_ids:
        return None

    for movie in _discover_by_people(cast_ids, crew_ids, year, api_key):
        if not _title_confirms(movie, title):
            continue
        details = _get_movie_details(movie["id"], api_key, language="de-DE")
        if details and details.get("title"):
            movie = {**movie, "title": details["title"]}
        logger.info("Identified '%s' by credits as TMDb %s", title, movie["id"])
        return _film_record(movie, details)

    return None


def _person_ids(names: list[str], api_key: str) -> list[int]:
    """Resolve person names to TMDb ids, skipping the ones it does not know."""
    ids = []
    for name in names:
        try:
            resp = requests.get(
                f"{TMDB_BASE_URL}/search/person",
                params={"api_key": api_key, "query": name},
                timeout=10,
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
        except requests.RequestException as e:
            logger.warning("TMDb person search failed for '%s': %s", name, _redact(e))
            continue
        if results:
            ids.append(results[0]["id"])
    return ids


def _discover_by_people(cast_ids: list[int], crew_ids: list[int], year: int,
                        api_key: str) -> list[dict]:
    """Return the films these people made around that year, most popular first."""
    params: dict[str, str | int] = {
        "api_key": api_key,
        "language": "de-DE",
        "sort_by": "popularity.desc",
        "primary_release_date.gte": f"{year - _CREDIT_YEAR_SLACK}-01-01",
        "primary_release_date.lte": f"{year + _CREDIT_YEAR_SLACK}-12-31",
    }
    if cast_ids:
        params["with_cast"] = ",".join(str(i) for i in cast_ids)
    if crew_ids:
        params["with_crew"] = ",".join(str(i) for i in crew_ids)

    try:
        resp = requests.get(f"{TMDB_BASE_URL}/discover/movie", params=params, timeout=10)
        resp.raise_for_status()
        return resp.json().get("results", [])
    except requests.RequestException as e:
        logger.warning("TMDb credit discovery failed: %s", _redact(e))
        return []


def _normalise_title(title: str | None) -> str:
    """Reduce a title to its words, keeping letters of any script."""
    return " ".join(re.sub(r"\W+", " ", title or "", flags=re.UNICODE).casefold().split())


def _title_confirms(movie: dict, scraped_title: str) -> bool:
    """True if a candidate's German or original title sits in the scraped one."""
    haystack = f" {_normalise_title(scraped_title)} "
    for key in ("title", "original_title"):
        needle = _normalise_title(movie.get(key))
        if len(needle) >= _MIN_TITLE_MATCH and f" {needle} " in haystack:
            return True
    return False


# Release-date types TMDb uses for a cinema run: 1 premiere (festivals),
# 2 limited, 3 theatrical. A programme page's production year can legitimately
# be any of them, from the festival premiere to the German start.
_CINEMA_RELEASE_TYPES = {1, 2, 3}

# How far the stated production year may sit from every date TMDb knows before
# it counts as contradicting the match. Pages are off by a year either way.
_PRODUCTION_YEAR_SLACK = 1


def contradicts_production(tmdb_id: int, page_year: int | None,
                           page_country: str | None) -> bool:
    """True if TMDb's record for this film disagrees with what the page states.

    A title search returns its best guess, not a certainty, and a wrong guess
    carries a wrong original language into the filter – an Israeli film matched
    to an Icelandic one was listed here as English. The page states a
    production year and a production country, and either disagreeing is enough
    to stop trusting the match; what the film actually is gets decided from its
    credits afterwards.

    Each side is generous before it objects, because each is wrong on its own
    for about one film in thirty. A year counts as agreeing if it comes within
    a year of any date TMDb has the film reaching a cinema, festival premieres
    included, which covers a page stating the production year where TMDb
    carries the German release. A country counts as agreeing unless the two
    sets of countries are entirely disjoint. Where a side has no data at all,
    it raises no objection.
    """
    api_key = settings.TMDB_API_KEY
    if not api_key:
        return False

    expected_countries = iso_codes(page_country)
    if not page_year and not expected_countries:
        return False  # Nothing to hold the match against

    details = _get_movie_details(tmdb_id, api_key, language="de-DE",
                                append="release_dates")
    if not details:
        return False

    tmdb_countries = {
        country["iso_3166_1"]
        for country in details.get("production_countries", [])
        if country.get("iso_3166_1")
    } | set(details.get("origin_country") or [])
    cinema_years = _cinema_release_years(details)

    country_objects = bool(expected_countries and tmdb_countries
                           and not expected_countries & tmdb_countries)
    year_objects = bool(page_year and cinema_years and not any(
        abs(page_year - year) <= _PRODUCTION_YEAR_SLACK for year in cinema_years
    ))
    if not (country_objects or year_objects):
        return False

    logger.warning(
        "TMDb %s contradicts the page (%s): page %s %s vs TMDb %s %s",
        tmdb_id,
        " and ".join(
            part for part, objects in (("country", country_objects), ("year", year_objects))
            if objects
        ),
        sorted(expected_countries), page_year,
        sorted(tmdb_countries), sorted(cinema_years),
    )
    return True


def _cinema_release_years(details: dict) -> set[int]:
    """Every year TMDb has this film reaching a cinema, premieres included."""
    years = set()
    primary = _extract_year(details.get("release_date"))
    if primary:
        years.add(primary)
    for country in details.get("release_dates", {}).get("results", []):
        for entry in country.get("release_dates", []):
            if entry.get("type") in _CINEMA_RELEASE_TYPES:
                year = _extract_year(entry.get("release_date"))
                if year:
                    years.add(year)
    return years


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
