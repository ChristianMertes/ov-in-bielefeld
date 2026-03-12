"""Web application for browsing OV/OmU cinema listings in Bielefeld."""
from dotenv import load_dotenv
load_dotenv()

import os
from datetime import datetime, date, timedelta
from collections import defaultdict
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from database import init_db, get_db, get_upcoming_films, get_film_showtimes, get_film_by_id
from tmdb_client import get_imdb_url, get_tmdb_url, get_omdb_url


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Kino OV Bielefeld", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/images", StaticFiles(directory="images"), name="images")
templates = Jinja2Templates(directory="templates")

# Add custom template filters
templates.env.filters["imdb_url"] = get_imdb_url
templates.env.filters["tmdb_url"] = get_tmdb_url
templates.env.filters["omdb_url"] = get_omdb_url


def _format_date_de(dt_str: str) -> str:
    """Format ISO datetime to German date string."""
    try:
        dt = datetime.fromisoformat(dt_str)
        weekdays = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]
        return f"{weekdays[dt.weekday()]}, {dt.day:02d}.{dt.month:02d}."
    except (ValueError, TypeError):
        return dt_str


def _format_time(dt_str: str) -> str:
    """Format ISO datetime to HH:MM."""
    try:
        dt = datetime.fromisoformat(dt_str)
        return f"{dt.hour:02d}:{dt.minute:02d}"
    except (ValueError, TypeError):
        return dt_str


def _format_votes(votes) -> str:
    """Format vote count as compact string: 1234567 → '1.2M'."""
    if not votes:
        return ""
    if votes >= 1_000_000:
        return f"{votes / 1_000_000:.1f}M"
    if votes >= 1_000:
        return f"{votes / 1_000:.0f}K"
    return str(votes)


_WEEKDAYS_DE = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
_MONTHS_DE = ["Januar", "Februar", "März", "April", "Mai", "Juni",
              "Juli", "August", "September", "Oktober", "November", "Dezember"]


def _next_showtime_label(dt_str: str, now: datetime) -> str:
    """Return a friendly German label for the next showtime relative to now."""
    try:
        dt = datetime.fromisoformat(dt_str)
    except (ValueError, TypeError):
        return ""
    today = now.date()
    show_date = dt.date()
    delta = (show_date - today).days
    time_str = f"{dt.hour:02d}:{dt.minute:02d} Uhr"
    weekday = _WEEKDAYS_DE[dt.weekday()]
    # Calendar-week difference: compare Mondays of each week
    monday_now = today - timedelta(days=today.weekday())
    monday_show = show_date - timedelta(days=show_date.weekday())
    week_diff = (monday_show - monday_now).days // 7
    if delta == 0:
        return f"heute {time_str}"
    elif delta == 1:
        return f"morgen {time_str}"
    elif delta == 2:
        return f"übermorgen {time_str}"
    elif week_diff == 0:
        return f"{weekday} {time_str}"
    elif week_diff == 1:
        return f"nächste Woche {weekday}"
    elif week_diff == 2:
        return f"übernächste Woche {weekday}"
    elif week_diff <= 4:
        return f"{weekday} in {week_diff} Wochen"
    else:
        return f"{show_date.day}. {_MONTHS_DE[show_date.month - 1]}"


templates.env.filters["date_de"] = _format_date_de
templates.env.filters["time_hm"] = _format_time
templates.env.filters["votes_fmt"] = _format_votes


CINEMA_DISPLAY_NAMES = {
    "lichtwerk": "Lichtwerk",
    "kamera": "Kamera",
    "cinemaxx": "CinemaxX",
}

LANGUAGE_DISPLAY_NAMES = {
    "en": "Englisch",
    "fr": "Französisch",
}


@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    cinema: str = Query(None, description="Filter by cinema"),
    lang: str = Query(None, description="Filter by language"),
    sort: str = Query("date", description="Sort by: date or title"),
):
    """Main page showing all upcoming OV/OmU films."""
    now = datetime.now()
    with get_db() as db:
        films_raw = get_upcoming_films(db, cinema=cinema)

        films = []
        for f in films_raw:
            film_dict = dict(f)
            # Get all showtimes for this film
            showtimes = get_film_showtimes(db, f["id"])

            # Group showtimes by date and cinema
            by_date = defaultdict(list)
            for st in showtimes:
                try:
                    dt = datetime.fromisoformat(st["showtime"])
                    date_key = dt.date().isoformat()
                except (ValueError, TypeError):
                    date_key = "unknown"
                by_date[date_key].append(dict(st))

            film_dict["showtimes_by_date"] = dict(sorted(by_date.items()))
            film_dict["showtimes_list"] = [dict(st) for st in showtimes]
            film_dict["cinema_list"] = (
                f["cinemas"].split(",") if f["cinemas"] else []
            )
            film_dict["language_tag_list"] = (
                f["language_tags"].split(",") if f["language_tags"] else []
            )
            film_dict["next_showtime_label"] = _next_showtime_label(
                f["next_showtime"], now
            )
            films.append(film_dict)

    if lang:
        films = [f for f in films if f.get("original_language") == lang]

    if sort == "title":
        films.sort(key=lambda f: f["title_display"].lower())
    elif sort == "rating":
        films.sort(key=lambda f: f.get("imdb_rating") or 0, reverse=True)
    elif sort == "rt":
        films.sort(key=lambda f: f.get("rt_score") or 0, reverse=True)
    elif sort == "popularity":
        films.sort(key=lambda f: f.get("tmdb_popularity") or 0, reverse=True)

    return templates.TemplateResponse("index.html", {
        "request": request,
        "films": films,
        "cinema_filter": cinema,
        "lang_filter": lang,
        "sort": sort,
        "cinema_names": CINEMA_DISPLAY_NAMES,
        "language_names": LANGUAGE_DISPLAY_NAMES,
        "now": now,
    })


@app.get("/film/{film_id}", response_class=HTMLResponse)
async def film_detail(request: Request, film_id: int):
    """Detail page for a single film."""
    with get_db() as db:
        film = get_film_by_id(db, film_id)
        if not film:
            return HTMLResponse("Film not found", status_code=404)

        showtimes = get_film_showtimes(db, film_id)

        # Group by date
        by_date = defaultdict(list)
        for st in showtimes:
            try:
                dt = datetime.fromisoformat(st["showtime"])
                date_key = dt.date().isoformat()
            except (ValueError, TypeError):
                date_key = "unknown"
            by_date[date_key].append(dict(st))

    return templates.TemplateResponse("film_detail.html", {
        "request": request,
        "film": dict(film),
        "showtimes_by_date": dict(sorted(by_date.items())),
        "cinema_names": CINEMA_DISPLAY_NAMES,
        "imdb_url": get_imdb_url(film["imdb_id"]),
        "tmdb_url": get_tmdb_url(film["tmdb_id"]),
    })


@app.get("/api/films")
async def api_films(cinema: str = None):
    """JSON API endpoint for external consumption."""
    with get_db() as db:
        films = get_upcoming_films(db, cinema=cinema)
        result = []
        for f in films:
            film_dict = dict(f)
            showtimes = get_film_showtimes(db, f["id"])
            film_dict["showtimes"] = [dict(st) for st in showtimes]
            result.append(film_dict)
    return result


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
