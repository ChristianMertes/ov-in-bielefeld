"""Wall-clock time in the cinemas' timezone.

Both sources deliver naive local times – CinemaxX returns "2026-09-10T16:15:00"
with no offset, and the arthouse page prints plain German dates and times – so
showtimes are stored as naive Berlin wall-clock strings.

Comparing those against `datetime.now()` is only correct when the process
happens to run in that same timezone, which a container does not: it defaults
to UTC. A screening would then stay listed for the length of the UTC offset
(two hours in summer) after it started, and relative labels like "heute" would
be a day off between midnight and 02:00. Every comparison against a showtime
therefore goes through `now_local()`.
"""
from datetime import datetime
from zoneinfo import ZoneInfo

CINEMA_TZ = ZoneInfo("Europe/Berlin")


def now_local() -> datetime:
    """Current wall-clock time in the cinemas' timezone, as a naive datetime.

    Naive on purpose: it is compared against the naive showtime strings in the
    database, and mixing aware and naive datetimes raises TypeError.
    """
    return datetime.now(CINEMA_TZ).replace(tzinfo=None)
