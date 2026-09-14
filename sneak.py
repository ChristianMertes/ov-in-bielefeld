"""Sneak previews: recognising them and mapping them to the films they may show.

A sneak preview screens a film the cinema keeps secret until the lights go down,
so the programme entry carries a placeholder title ("SNEAK PREVIEW", "Sneak OV")
instead of a film title. Two consequences drive this module:

1. Placeholder titles must never reach TMDb. A search for "SNEAK PREVIEW" finds
   a real film of that name and the entry then shows that film's poster, plot
   and ratings – all of it wrong.

2. Bielefeld sneaks run on Wednesday evening, and the film shown is almost
   always one that opens officially the next day: German cinema releases happen
   on Thursdays. So while the film is secret, the shortlist is not – every
   English-language film opening on that Thursday is a candidate.

Kept free of database and network imports so both layers can use it.
"""
import re
from datetime import date, timedelta

# Shown in place of a real poster on sneak entries.
SNEAK_POSTER_URL = "/static/sneak-preview-poster.webp"

# German cinema releases happen on Thursdays.
_RELEASE_WEEKDAY = 3

# Placeholder titles lead with "Sneak" ("Sneak OV", "SNEAK PREVIEW - English
# Edition"); the phrase form also catches it behind a section prefix. Neither
# matches "Sneakers" or "The Sneaky Life", which are film titles, not sneaks.
_SNEAK_LEADING = re.compile(r"^\W*sneak\b", re.IGNORECASE)
_SNEAK_PHRASE = re.compile(r"\bsneak[\s\-_]*preview\b", re.IGNORECASE)

_LANGUAGE_MARKERS = [
    ("en", re.compile(r"\benglisc?h\w*\b", re.IGNORECASE)),
    ("fr", re.compile(r"\b(french|franz(ö|oe)sisc?h\w*|fran(ç|c)ais)\b", re.IGNORECASE)),
]

# A sneak that made it past the scrapers' OV filter without naming its language
# is an English one: CinemaxX labels those "Sneak OV", and Bielefeld has no
# French sneak. Guessing here is what keeps sneaks visible under ?lang=en.
DEFAULT_SNEAK_LANGUAGE = "en"


def is_sneak_title(title: str) -> bool:
    """Return True if this programme title is a sneak preview placeholder."""
    if not title:
        return False
    return bool(_SNEAK_LEADING.match(title) or _SNEAK_PHRASE.search(title))


def sneak_language(title: str) -> str | None:
    """Return the language code a sneak title names ("en"/"fr"), else None."""
    for code, pattern in _LANGUAGE_MARKERS:
        if pattern.search(title or ""):
            return code
    return None


def official_release_date(screening_date: date) -> date:
    """Return the release Thursday whose line-up a sneak on this date draws from.

    The Wednesday case (+1 day) is the one that occurs in practice. A sneak on
    the release Thursday itself already shares the day with its own openings,
    so it looks ahead to the following week.
    """
    offset = (_RELEASE_WEEKDAY - screening_date.weekday()) % 7
    return screening_date + timedelta(days=offset or 7)
