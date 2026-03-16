"""Scraper for CinemaxX Bielefeld.

Two-step API approach:
  Step 1 — Film list with OV/language attributes:
    GET /api/microservice/showings/films
        ?cinemaId=1336&minEmbargoLevel=2&includeSessionAttributes=true
    Returns all films; `sessionAttributes` and `filmAttributes` tell us
    which films have OV/English/French showings.

  Step 2 — Session times for OV films:
    GET /api/microservice/showings/cinemas/1336/films/{filmId}/showingGroups
    Returns concrete showtimes grouped by date.
    Each session has its own `attributes` array — we keep only sessions
    that are English/French OV/OmU.

OV/OmU detection:
  - Language attribute with value "english" or "french"
  - Film/session attribute with value "ov" or "om-u"

Booking URLs: relative (/buchtickets/...) → prefixed with https://www.cinemaxx.de

Important: A session cookie is needed. Loading the cinema page once
establishes the session (no login required).

Cinema ID for Bielefeld: 1336
"""
import logging
import re

import requests

logger = logging.getLogger(__name__)

API_BASE = "https://www.cinemaxx.de/api/microservice/showings"
CINEMA_PAGE = "https://www.cinemaxx.de/kinoprogramm/bielefeld/jetzt-im-kino"
CINEMAXX_BASE = "https://www.cinemaxx.de"
BIELEFELD_CINEMA_ID = "1336"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": CINEMA_PAGE,
    "Origin": "https://www.cinemaxx.de",
}

# Language attribute values we care about
RELEVANT_LANGUAGES = {"english", "englisch", "french", "französisch", "francais"}

# Attribute values indicating OV/OmU
OV_VALUES = {"ov", "om-u", "omu", "omeu", "original-version"}


def _get_session() -> requests.Session:
    """Create a requests session with cookies from the main page.

    The API returns 401 without a valid session cookie.
    Loading the cinema page once establishes the session.
    """
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get(CINEMA_PAGE, timeout=15)
        logger.debug("Session established, cookies: %s", list(session.cookies.keys()))
    except requests.RequestException as e:
        logger.warning("Could not establish session: %s", e)
    return session


def scrape_cinemaxx() -> list[dict]:
    """Scrape CinemaxX Bielefeld for OV/OmU showings in English/French."""
    logger.info("Scraping CinemaxX Bielefeld...")

    http = _get_session()

    try:
        resp = http.get(f"{API_BASE}/films", params={
            "cinemaId": BIELEFELD_CINEMA_ID,
            "minEmbargoLevel": "2",
            "includesSession": "false",
            "includeSessionAttributes": "true",
        }, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.error("CinemaxX API failed: %s", e)
        return []

    all_films = data.get("result", [])
    if not all_films:
        logger.warning("No films returned from CinemaxX API")
        return []

    logger.info("CinemaxX API returned %d films for Bielefeld", len(all_films))

    result = []
    for film in all_films:
        parsed = _parse_film(film)
        if parsed:
            film_id = film.get("filmId", "")
            parsed["showtimes"] = _fetch_ov_showtimes(http, film_id)
            if not parsed["showtimes"]:
                logger.debug("Skipping '%s': no OV sessions found", parsed["title_display"])
                continue
            result.append(parsed)

    logger.info("CinemaxX: %d English/French OV/OmU films", len(result))
    return result


def _fetch_ov_showtimes(http: requests.Session, film_id: str) -> list[dict]:
    """Fetch OV/English/French sessions for a film from the showingGroups endpoint."""
    if not film_id:
        return []

    url = f"{API_BASE}/cinemas/{BIELEFELD_CINEMA_ID}/films/{film_id}/showingGroups"
    try:
        resp = http.get(url, timeout=15)
        resp.raise_for_status()
        groups = resp.json().get("result", [])
    except requests.RequestException as e:
        logger.warning("Could not fetch showtimes for %s: %s", film_id, e)
        return []

    showtimes = []
    for group in groups:
        for sess in group.get("sessions", []):
            attrs = sess.get("attributes", [])
            # Keep only OV/English/French sessions
            if not (_has_language(attrs, RELEVANT_LANGUAGES) or _has_ov_marker(attrs)):
                continue

            lang_tag = "OmU" if any(_is_omu_attr(a) for a in attrs) else "OV"
            booking_path = sess.get("bookingUrl", "")
            booking_url = (
                CINEMAXX_BASE + booking_path
                if booking_path.startswith("/")
                else booking_path
            )

            showtimes.append({
                "cinema": "cinemaxx",
                "showtime": sess.get("startTime", ""),
                "language_tag": lang_tag,
                "booking_url": booking_url,
            })

    return showtimes


def _parse_film(film: dict) -> dict | None:
    """Parse a CinemaxX film if it has English/French OV/OmU showings."""
    title_de = (film.get("filmTitle") or "").strip()
    title_orig = (film.get("originalTitle") or "").strip()
    if not title_de:
        return None

    session_attrs = film.get("sessionAttributes", [])
    film_attrs = film.get("filmAttributes", [])
    all_attrs = session_attrs + film_attrs

    has_relevant_language = _has_language(all_attrs, RELEVANT_LANGUAGES)
    has_heuristic = _title_heuristic_ov(title_de)

    if not (has_relevant_language or has_heuristic):
        return None

    lang_tag = "OV"
    if any(_attr_value_matches(a, OV_VALUES) for a in all_attrs if _is_omu_attr(a)):
        lang_tag = "OmU"

    return {
        "title_display": _clean_title(title_de),
        "title_raw": title_de,
        "detail_url": film.get("filmUrl", ""),
        "duration_minutes": film.get("runningTime") or None,
        "release_year": _extract_year(film.get("releaseDate")),
        "showtimes": [],  # filled in by scrape_cinemaxx after this call
        "_cinemaxx_film_id": film.get("filmId", ""),
        "_original_title": title_orig,
        "_poster_url": film.get("posterImageSrc"),
        "_genres": film.get("genres", []),
        "_lang_tag": lang_tag,
    }


def _has_language(attrs: list, target_langs: set) -> bool:
    """Check if any Language attribute matches our target languages.

    Only matches on the `value` field (e.g., "english", "french"),
    NOT on the name (which could be "Englische Untertitel" or
    "Japanese Audio with English Subtitles" — false positives).
    """
    for attr in attrs:
        if not isinstance(attr, dict):
            continue
        if attr.get("attributeType") != "Language":
            continue
        value = (attr.get("value") or "").lower().strip()
        short = (attr.get("shortName") or "").lower().strip()
        if value in target_langs or short in target_langs:
            return True
    return False


def _has_ov_marker(attrs: list) -> bool:
    """Check if any attribute indicates OV or OmU."""
    for attr in attrs:
        if not isinstance(attr, dict):
            continue
        value = (attr.get("value") or "").lower()
        if value in OV_VALUES:
            return True
    return False


def _is_omu_attr(attr: dict) -> bool:
    """Check if this specific attribute is an OmU marker."""
    value = (attr.get("value") or "").lower()
    return value in {"om-u", "omu", "omeu"}


def _attr_value_matches(attr: dict, values: set) -> bool:
    """Check if attribute value is in the given set."""
    return (attr.get("value") or "").lower() in values


def _title_heuristic_ov(title: str) -> bool:
    """Heuristic: detect OV from title for films without attributes."""
    t = title.lower().strip()
    if t == "sneak ov" or t.endswith(" ov"):
        return True
    return bool("(english)" in t or "- english" in t)


def _clean_title(title: str) -> str:
    title = title.strip()
    return re.sub(r'^["\s]+|["\s]+$', "", title)


def _extract_year(date_str: str | None) -> int | None:
    if not date_str:
        return None
    match = re.search(r"(\d{4})", date_str)
    return int(match.group(1)) if match else None


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    films = scrape_cinemaxx()
    if not films:
        print("No OV/OmU films found.")
    for f in films:
        print(f"\n{'='*60}")
        print(f"  {f['title_display']}")
        if f.get("_original_title") and f["_original_title"] != f["title_display"]:
            print(f"  Original: {f['_original_title']}")
        print(f"  [{f.get('_lang_tag', '?')}] Year: {f.get('release_year')} "
              f"Runtime: {f.get('duration_minutes')} min")
        for st in f["showtimes"]:
            print(f"    {st['showtime']} [{st['language_tag']}] {st['booking_url']}")
