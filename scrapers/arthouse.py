"""Scraper for Lichtwerk & Kamera (arthousekinos-bielefeld.de).

The programme page lists all films with their showtimes in a table.
OmU/OV tags appear as text within showtime links.
Cinema is identified from the booking URL (lichtwerk vs kamera-filmkunsttheater).
"""
import re
import logging
from datetime import datetime, timedelta
from typing import Optional
from bs4 import BeautifulSoup
import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://www.arthousekinos-bielefeld.de"
PROGRAMME_URL = f"{BASE_URL}/programm/"

# We fetch the full programme page. OmU/OV filtering is client-side JS,
# but the tags are present in the HTML for all showtimes.
HEADERS = {
    "User-Agent": "ov-in-bielefeld.de/1.0 (personal cinema aggregator; christian.leichsenring+ov-in-bielefeld.de@gmail.com)"
}


def scrape_arthouse() -> list[dict]:
    """Scrape Lichtwerk & Kamera programme. Returns list of film dicts."""
    logger.info("Scraping Arthouse Kinos Bielefeld...")
    resp = requests.get(PROGRAMME_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    resp.encoding = "utf-8"

    soup = BeautifulSoup(resp.text, "html.parser")
    films = []

    # Each film is in a programme-entry block.
    # Looking at the HTML structure: films are in divs/sections with
    # a title, poster, details, and a showtime table.
    # We'll look for film blocks by finding headings or known CSS patterns.

    # The markdown output showed ## headings for each film, which correspond
    # to h2 or similar elements in the programme listing.
    # Let's find all film containers.

    # Film entries are structured as:
    #   <div class="row flex-row">
    #     <div class="col-xs-12"><h2>TITLE</h2></div>       ← real title here
    #     <div class="col-sm-3 ...">duration, detail link</div>
    #     <div class="col-sm-9 programme-table-main-grid-movieitem-showtimes">
    #       <h3>Spielzeiten & Tickets</h3>                  ← NOT the title
    #       <table>...showtimes...</table>
    #     </div>
    #   </div>
    # We identify film entries by finding the showtime column, then walk up to the row.
    seen = set()
    film_blocks = []
    for st_div in soup.find_all("div", class_="programme-table-main-grid-movieitem-showtimes"):
        row = st_div.find_parent("div", class_="flex-row")
        if row is not None:
            row_id = id(row)
            if row_id not in seen:
                seen.add(row_id)
                film_blocks.append(row)

    if not film_blocks:
        film_blocks = _find_film_blocks_fallback(soup)

    for block in film_blocks:
        try:
            film = _parse_film_block(block)
            if film and film["showtimes"]:
                films.append(film)
        except Exception as e:
            logger.warning(f"Error parsing film block: {e}")
            continue

    logger.info(f"Found {len(films)} films with OV/OmU showtimes")

    # Fetch detail pages for films missing a release year (improves TMDb disambiguation)
    for film in films:
        if film.get("detail_url") and not film.get("release_year"):
            try:
                detail = _fetch_film_detail(film["detail_url"])
                # Store separately — don't overwrite release_year; used as a retry hint
                if detail.get("year"):
                    film["_arthouse_year"] = detail["year"]
                if detail.get("director"):
                    film["_director"] = detail["director"]
                if detail.get("country"):
                    film["_production_country"] = detail["country"]
            except Exception as e:
                logger.debug(f"Detail fetch failed for '{film['title_display']}': {e}")

    return films


def _fetch_film_detail(url: str) -> dict:
    """Fetch production year, director, and country from an arthouse film detail page."""
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    text = BeautifulSoup(resp.text, "html.parser").get_text(" ", strip=True)

    result = {}

    # "Produktion Indonesien 2024" or "Produktion: Frankreich, 2025"
    prod_match = re.search(r"Produktion\s*:?\s*([^\n|·•]{3,60})", text)
    if prod_match:
        prod_text = prod_match.group(1)
        year_m = re.search(r"\b(20\d{2}|19\d{2})\b", prod_text)
        if year_m:
            result["year"] = int(year_m.group(1))
        country_m = re.match(r"([A-Za-zÄÖÜäöüß/ -]+?)[\s,]+(?:20|19)\d{2}", prod_text)
        if country_m:
            result["country"] = country_m.group(1).strip()

    # "R: Tumpal Tampubolon." or "Regie: Name"
    dir_match = re.search(r"\bR(?:egie)?\s*:\s*([^\n|·•.]{3,60})", text)
    if dir_match:
        result["director"] = dir_match.group(1).strip()

    return result


def _find_film_blocks_fallback(soup: BeautifulSoup) -> list:
    """Find film blocks by looking for detail links to /filme/."""
    blocks = []
    # Find all links to film detail pages
    detail_links = soup.find_all("a", href=re.compile(r"/filme/[^/]+/$"))

    # Group by parent containers
    seen_parents = set()
    for link in detail_links:
        # Walk up to find a meaningful container
        parent = link.parent
        for _ in range(10):
            if parent is None or parent.name in ("body", "html"):
                break
            # Check if this looks like a film container
            # (has both a detail link and booking links)
            booking_links = parent.find_all("a", href=re.compile(r"kinoheld\.de"))
            if booking_links:
                parent_id = id(parent)
                if parent_id not in seen_parents:
                    seen_parents.add(parent_id)
                    blocks.append(parent)
                break
            parent = parent.parent

    return blocks


def _parse_film_block(block) -> Optional[dict]:
    """Parse a single film block into a structured dict."""
    # Extract title
    title = None

    # Try heading elements first; skip "Spielzeiten & Tickets" (showtime section header)
    for tag in ("h2", "h3", "h4"):
        heading = block.find(tag)
        if heading:
            text = heading.get_text(strip=True)
            if text.lower() not in ("spielzeiten & tickets", "spielzeiten und tickets"):
                title = text
                break

    # Try detail link text
    if not title:
        detail_link = block.find("a", href=re.compile(r"/filme/[^/]+/$"))
        if detail_link:
            # Title might be in the link text or a nearby element
            title_el = detail_link.find(["h2", "h3", "h4", "strong", "span"])
            if title_el:
                title = title_el.get_text(strip=True)
            elif detail_link.get_text(strip=True) not in ("Details anzeigen", "Trailer ansehen"):
                title = detail_link.get_text(strip=True)

    if not title:
        return None

    # Clean up title: remove prefixes like "CINÉMA_FRANÇAIS:", "best_of_cinema:", etc.
    title_clean = title
    prefix_match = re.match(
        r"^(?:CINÉMA_FRANÇAIS|best_of_cinema|filmhaus_präsentiert|Familiensache|"
        r"GWÖ|GRÜNES KINO|Retrospektive Spielberg|Exhibition on Screen)\s*:\s*",
        title, re.IGNORECASE
    )
    if prefix_match:
        title_clean = title[prefix_match.end():].strip()

    # Extract detail URL
    detail_url = None
    detail_link = block.find("a", href=re.compile(r"/filme/[^/]+/$"))
    if detail_link:
        href = detail_link["href"]
        detail_url = href if href.startswith("http") else BASE_URL + href

    # Extract duration
    duration = None
    text = block.get_text()
    dur_match = re.search(r"(?:Dauer|Laufzeit):\s*(\d+)\s*Min", text)
    if dur_match:
        duration = int(dur_match.group(1))

    # Extract year from title (e.g., "Jurassic Park (1993)")
    year = None
    year_match = re.search(r"\((\d{4})\)", title)
    if year_match:
        year = int(year_match.group(1))

    # Extract showtimes with OmU/OV tags
    showtimes = _extract_showtimes(block)

    # Only keep showtimes tagged as OmU or OV
    ov_showtimes = [s for s in showtimes if s.get("language_tag")]

    return {
        "title_display": title_clean,
        "title_raw": title,
        "detail_url": detail_url,
        "duration_minutes": duration,
        "release_year": year,
        "showtimes": ov_showtimes,
        "all_showtimes": showtimes,
    }


def _extract_showtimes(block) -> list[dict]:
    """Extract all showtimes from a film block."""
    showtimes = []

    # Find all booking links (kinoheld.de)
    booking_links = block.find_all("a", href=re.compile(r"kinoheld\.de"))

    for link in booking_links:
        href = link.get("href", "")
        link_text = link.get_text(strip=True)

        if not link_text or link_text in ("Details anzeigen", "Trailer ansehen"):
            continue

        # Parse time from link text (e.g., "21:00OmU", "19:30", "18:00OV")
        time_match = re.match(r"(\d{1,2}:\d{2})\s*(OmU|OV)?", link_text)
        if not time_match:
            continue

        time_str = time_match.group(1)
        lang_tag = time_match.group(2)  # None, "OmU", or "OV"

        # Determine cinema from URL
        cinema = "lichtwerk"  # default
        if "kamera-filmkunsttheater" in href:
            cinema = "kamera"

        # Determine date from table header context
        # The showtime table has date headers; we need to find which column this link is in
        showtime_dt = _resolve_showtime_date(link, time_str)

        # Check for Salon indicator
        is_salon = bool(link.get("title", "").lower().count("salon"))

        showtimes.append({
            "cinema": cinema,
            "showtime": showtime_dt,
            "language_tag": lang_tag,
            "booking_url": href,
            "is_salon": is_salon,
        })

    return showtimes


def _resolve_showtime_date(link_element, time_str: str) -> str:
    """Try to determine the date of a showtime from its table context."""
    # Walk up to find the table
    table = link_element.find_parent("table")
    if not table:
        # Fallback: try to find date from nearby text
        return _guess_date_from_context(link_element, time_str)

    # Find which column (td) this link is in
    td = link_element.find_parent("td")
    if not td:
        return _guess_date_from_context(link_element, time_str)

    tr = td.find_parent("tr")
    if not tr:
        return _guess_date_from_context(link_element, time_str)

    # Get column index
    tds = tr.find_all("td")
    col_idx = None
    for i, cell in enumerate(tds):
        if cell == td:
            col_idx = i
            break

    if col_idx is None:
        return _guess_date_from_context(link_element, time_str)

    # Find header row for this column
    thead = table.find("thead")
    if thead:
        header_cells = thead.find_all(["th", "td"])
    else:
        # First row might be the header
        first_row = table.find("tr")
        header_cells = first_row.find_all(["th", "td"]) if first_row else []

    if col_idx < len(header_cells):
        header_text = header_cells[col_idx].get_text(strip=True)
        return _parse_german_date(header_text, time_str)

    return _guess_date_from_context(link_element, time_str)


def _parse_german_date(header_text: str, time_str: str) -> str:
    """Parse German date header like 'Do, 12.03' or 'Heute' into ISO datetime."""
    now = datetime.now()
    current_year = now.year

    if header_text.lower() in ("heute",):
        date_obj = now.date()
    elif header_text.lower() in ("morgen",):
        date_obj = (now + timedelta(days=1)).date()
    else:
        # Try patterns like "Do, 12.03", "Fr, 13.03", "Sa, 14.03"
        date_match = re.search(r"(\d{1,2})\.(\d{1,2})", header_text)
        if date_match:
            day = int(date_match.group(1))
            month = int(date_match.group(2))
            # Determine year: if month is far in the past, it's probably next year
            try:
                date_obj = datetime(current_year, month, day).date()
                if date_obj < now.date() - timedelta(days=30):
                    date_obj = datetime(current_year + 1, month, day).date()
            except ValueError:
                date_obj = now.date()
        else:
            date_obj = now.date()

    hour, minute = map(int, time_str.split(":"))
    dt = datetime.combine(date_obj, datetime.min.time().replace(hour=hour, minute=minute))
    return dt.isoformat()


def _guess_date_from_context(element, time_str: str) -> str:
    """Fallback: try to extract date from surrounding text."""
    # Look for a date pattern in nearby text
    parent = element.parent
    for _ in range(5):
        if parent is None:
            break
        text = parent.get_text()
        date_match = re.search(r"(\d{1,2})\.(\d{1,2})\.?(\d{2,4})?", text)
        if date_match:
            day = int(date_match.group(1))
            month = int(date_match.group(2))
            year = int(date_match.group(3)) if date_match.group(3) else datetime.now().year
            if year < 100:
                year += 2000
            try:
                hour, minute = map(int, time_str.split(":"))
                dt = datetime(year, month, day, hour, minute)
                return dt.isoformat()
            except ValueError:
                pass
        parent = parent.parent

    # Last resort: use today
    hour, minute = map(int, time_str.split(":"))
    dt = datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)
    return dt.isoformat()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    films = scrape_arthouse()
    for f in films:
        print(f"\n{'='*60}")
        print(f"Title: {f['title_display']}")
        print(f"Year: {f.get('release_year')}")
        print(f"Duration: {f.get('duration_minutes')} min")
        for s in f["showtimes"]:
            print(f"  {s['cinema']:10} {s['showtime']} [{s['language_tag']}] {s.get('booking_url', '')[:60]}")
