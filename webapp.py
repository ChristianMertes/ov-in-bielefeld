"""Web application for browsing OV/OmU cinema listings in Bielefeld."""
import logging
import time
from collections import defaultdict
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Annotated

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import cache
from database import get_db, get_film_by_id, get_film_showtimes, get_showtimes_for_films, get_upcoming_films, init_db
from log_setup import setup_logging
from tmdb_client import get_imdb_url, get_omdb_url, get_tmdb_url

_access_log = logging.getLogger("access")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    setup_logging()
    # Redirect uvicorn's loggers through our root logger (timestamps, log file).
    # Suppress uvicorn.access specifically — our middleware produces richer logs.
    for _name in ("uvicorn", "uvicorn.error"):
        _uv = logging.getLogger(_name)
        _uv.handlers.clear()
        _uv.propagate = True
    _uv_access = logging.getLogger("uvicorn.access")
    _uv_access.handlers.clear()
    _uv_access.propagate = False  # silenced; middleware takes over
    init_db()
    yield


app = FastAPI(title="Kino OV Bielefeld", lifespan=lifespan)


@app.middleware("http")
async def access_log_middleware(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
    start = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000

    # Real client IP: X-Forwarded-For is set by Caddy (and most reverse proxies)
    forwarded_for = request.headers.get("X-Forwarded-For")
    client_ip = (
        forwarded_for.split(",")[0].strip()
        if forwarded_for
        else (request.client.host if request.client else "-")
    )

    path = request.url.path
    if request.url.query:
        path += f"?{request.url.query}"

    _access_log.info(
        '%s "%s %s" %d %.0fms ref="%s" ua="%s"',
        client_ip,
        request.method,
        path,
        response.status_code,
        duration_ms,
        request.headers.get("Referer", "-"),
        request.headers.get("User-Agent", "-"),
    )
    return response


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


def _format_votes(votes: int | None) -> str:
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
    if delta == 1:
        return f"morgen {time_str}"
    if delta == 2:
        return f"übermorgen {time_str}"
    if week_diff == 0:
        return f"{weekday} {time_str}"
    if week_diff == 1:
        return f"nächste Woche {weekday}"
    if week_diff == 2:
        return f"übernächste Woche {weekday}"
    if week_diff <= 4:
        return f"{weekday} in {week_diff} Wochen"
    return f"{show_date.day}. {_MONTHS_DE[show_date.month - 1]}"


def _is_future(dt_str: str, now: datetime) -> bool:
    """Return True if the ISO datetime string is in the future."""
    try:
        return datetime.fromisoformat(dt_str) >= now
    except (ValueError, TypeError):
        return True


templates.env.filters["date_de"] = _format_date_de
templates.env.filters["time_hm"] = _format_time
templates.env.filters["votes_fmt"] = _format_votes


def _supports_brotli(request: Request) -> bool:
    return "br" in request.headers.get("Accept-Encoding", "")


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
    cinema: Annotated[str | None, Query(description="Filter by cinema")] = None,
    lang: Annotated[str | None, Query(description="Filter by language")] = None,
    sort: Annotated[str, Query(description="Sort by: date or title")] = "date",
) -> Response:
    """Main page showing all upcoming OV/OmU films."""
    cache_key = f"index:{cinema or ''}:{lang or ''}:{sort}"
    brotli_ok = _supports_brotli(request)
    cached = cache.get(cache_key) if brotli_ok else cache.get_plain(cache_key)
    if cached:
        headers = {"Vary": "Accept-Encoding"}
        if brotli_ok:
            headers["Content-Encoding"] = "br"
        return Response(content=cached, media_type="text/html; charset=utf-8", headers=headers)

    now = datetime.now()
    with get_db() as db:
        films_raw = get_upcoming_films(db, cinema=cinema)
        showtimes_by_film = get_showtimes_for_films(db, [f["id"] for f in films_raw])

        films = []
        for f in films_raw:
            film_dict = dict(f)
            showtimes = showtimes_by_film.get(f["id"], [])

            # Group showtimes by date, dropping any that are in the past
            by_date = defaultdict(list)
            for st in showtimes:
                try:
                    dt = datetime.fromisoformat(st["showtime"])
                    if dt < now:
                        continue
                    date_key = dt.date().isoformat()
                except (ValueError, TypeError):
                    date_key = "unknown"
                by_date[date_key].append(st)

            film_dict["showtimes_by_date"] = dict(sorted(by_date.items()))
            film_dict["showtimes_list"] = [
                st for st in showtimes if _is_future(st.get("showtime", ""), now)
            ]
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

    html = templates.get_template("index.html").render({
        "request": request,
        "films": films,
        "cinema_filter": cinema,
        "lang_filter": lang,
        "sort": sort,
        "cinema_names": CINEMA_DISPLAY_NAMES,
        "language_names": LANGUAGE_DISPLAY_NAMES,
        "now": now,
    })
    if brotli_ok:
        content = cache.put(cache_key, html)
        headers = {"Content-Encoding": "br", "Vary": "Accept-Encoding"}
    else:
        content = cache.put_plain(cache_key, html)
        headers = {"Vary": "Accept-Encoding"}
    return Response(content=content, media_type="text/html; charset=utf-8", headers=headers)


@app.get("/film/{film_id}", response_class=HTMLResponse)
async def film_detail(request: Request, film_id: int) -> Response:
    """Detail page for a single film."""
    cache_key = f"film:{film_id}"
    brotli_ok = _supports_brotli(request)
    cached = cache.get(cache_key) if brotli_ok else cache.get_plain(cache_key)
    if cached:
        headers = {"Vary": "Accept-Encoding"}
        if brotli_ok:
            headers["Content-Encoding"] = "br"
        return Response(content=cached, media_type="text/html; charset=utf-8", headers=headers)

    with get_db() as db:
        film = get_film_by_id(db, film_id)
        if not film:
            html = templates.get_template("404.html").render({"request": request})
            return HTMLResponse(html, status_code=404)

        showtimes = get_film_showtimes(db, film_id)
        now = datetime.now()

        # Group by date, dropping any that are in the past
        by_date = defaultdict(list)
        for st in showtimes:
            try:
                dt = datetime.fromisoformat(st["showtime"])
                if dt < now:
                    continue
                date_key = dt.date().isoformat()
            except (ValueError, TypeError):
                date_key = "unknown"
            by_date[date_key].append(dict(st))

    html = templates.get_template("film_detail.html").render({
        "request": request,
        "film": dict(film),
        "showtimes_by_date": dict(sorted(by_date.items())),
        "cinema_names": CINEMA_DISPLAY_NAMES,
        "imdb_url": get_imdb_url(film["imdb_id"]),
        "tmdb_url": get_tmdb_url(film["tmdb_id"]),
    })
    if brotli_ok:
        content = cache.put(cache_key, html)
        headers = {"Content-Encoding": "br", "Vary": "Accept-Encoding"}
    else:
        content = cache.put_plain(cache_key, html)
        headers = {"Vary": "Accept-Encoding"}
    return Response(content=content, media_type="text/html; charset=utf-8", headers=headers)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/robots.txt", response_class=PlainTextResponse)
async def robots_txt() -> str:
    return (
        "User-agent: *\n"
        "Allow: /\n"
        "Disallow: /api/\n"
        "Disallow: /health\n"
        "\n"
        "Sitemap: https://ov-in-bielefeld.de/sitemap.xml\n"
    )


@app.get("/sitemap.xml")
async def sitemap_xml(request: Request) -> Response:
    base = str(request.base_url).rstrip("/")
    urls = [base + "/"]
    with get_db() as db:
        films = get_upcoming_films(db)
        urls.extend(f"{base}/film/{f['id']}" for f in films)
    today = datetime.now().date().isoformat()
    url_entries = "\n".join(
        f"  <url><loc>{u}</loc><lastmod>{today}</lastmod></url>" for u in urls
    )
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f"{url_entries}\n"
        "</urlset>"
    )
    return Response(content=xml, media_type="application/xml")


@app.get("/api/films")
async def api_films(cinema: str | None = None) -> list[dict[str, object]]:
    """JSON API endpoint for external consumption."""
    now = datetime.now()
    with get_db() as db:
        films = get_upcoming_films(db, cinema=cinema)
        showtimes_by_film = get_showtimes_for_films(db, [f["id"] for f in films])
        result = []
        for f in films:
            film_dict = dict(f)
            film_dict["showtimes"] = [
                st for st in showtimes_by_film.get(f["id"], [])
                if _is_future(st.get("showtime", ""), now)
            ]
            result.append(film_dict)
    return result


if __name__ == "__main__":
    import uvicorn

    import settings
    uvicorn.run(app, host="0.0.0.0", port=settings.PORT)  # noqa: S104
