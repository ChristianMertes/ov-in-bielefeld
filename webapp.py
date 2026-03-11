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
from tmdb_client import get_imdb_url, get_tmdb_url


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Kino OV Bielefeld", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Add custom template filters
templates.env.filters["imdb_url"] = get_imdb_url
templates.env.filters["tmdb_url"] = get_tmdb_url


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


templates.env.filters["date_de"] = _format_date_de
templates.env.filters["time_hm"] = _format_time


CINEMA_DISPLAY_NAMES = {
    "lichtwerk": "Lichtwerk",
    "kamera": "Kamera",
    "cinemaxx": "CinemaxX",
}


@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    cinema: str = Query(None, description="Filter by cinema"),
    sort: str = Query("date", description="Sort by: date or title"),
):
    """Main page showing all upcoming OV/OmU films."""
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
            films.append(film_dict)

    if sort == "title":
        films.sort(key=lambda f: f["title_display"].lower())

    return templates.TemplateResponse("index.html", {
        "request": request,
        "films": films,
        "cinema_filter": cinema,
        "sort": sort,
        "cinema_names": CINEMA_DISPLAY_NAMES,
        "now": datetime.now(),
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
