"""Telegram bot that notifies about new OV/OmU films.

Set environment variables:
  TELEGRAM_BOT_TOKEN  — from @BotFather
  TELEGRAM_CHAT_ID    — your personal chat ID (use @userinfobot to find it)

The bot can be run in two modes:
1. As a standalone long-polling bot (for receiving commands)
2. As a notification sender (called from the scraper orchestrator)
"""
import logging
from datetime import datetime

import requests

import settings
from database import get_db, get_film_by_id, get_film_showtimes, get_new_unnotified_films, mark_film_notified
from tmdb_client import get_imdb_url

logger = logging.getLogger(__name__)

BOT_TOKEN = settings.TELEGRAM_BOT_TOKEN
CHAT_ID = settings.TELEGRAM_CHAT_ID
WEBAPP_URL = settings.WEBAPP_URL


def send_message(text: str, parse_mode: str = "HTML",
                 disable_web_preview: bool = True) -> bool:
    """Send a message to the configured Telegram chat."""
    if not BOT_TOKEN or not CHAT_ID:
        logger.warning("Telegram not configured (missing BOT_TOKEN or CHAT_ID)")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_preview,
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except requests.RequestException as e:
        logger.error(f"Telegram send failed: {e}")
        return False


def notify_new_film(film_id: int, film_data: dict | None = None) -> None:
    """Send a notification about a new film appearing in the programme."""
    with get_db() as db:
        film = get_film_by_id(db, film_id)
        if not film:
            return

        showtimes = get_film_showtimes(db, film_id)

        # Build message
        title_original = film["title_original"] or film["title_display"]
        title_de = film["title_de"]
        lang = (film["original_language"] or "").upper()
        year = film["release_year"] or ""
        runtime = film["runtime_minutes"]
        imdb_id = film["imdb_id"]

        lines = ["🎬 <b>Neuer Film im OV-Programm</b>"]
        lines.append("")
        lines.append(f"<b>{_escape_html(title_original)}</b>")

        if title_de and title_de != title_original:
            lines.append(f"<i>{_escape_html(title_de)}</i>")

        meta_parts = []
        if year:
            meta_parts.append(str(year))
        if runtime:
            meta_parts.append(f"{runtime} min")
        if lang:
            meta_parts.append(lang)
        if meta_parts:
            lines.append(" · ".join(meta_parts))

        if showtimes:
            lines.append("")
            lines.append("📅 <b>Vorstellungen:</b>")
            for st in showtimes[:8]:  # Limit to 8 showtimes
                try:
                    dt = datetime.fromisoformat(st["showtime"])
                    weekdays = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]
                    date_str = f"{weekdays[dt.weekday()]} {dt.day:02d}.{dt.month:02d}."
                    time_str = f"{dt.hour:02d}:{dt.minute:02d}"
                except (ValueError, TypeError):
                    date_str = "?"
                    time_str = "?"

                cinema = st["cinema"].capitalize()
                tag = f" [{st['language_tag']}]" if st["language_tag"] else ""
                lines.append(f"  {date_str} {time_str} — {cinema}{tag}")

            if len(showtimes) > 8:
                lines.append(f"  ... und {len(showtimes) - 8} weitere")

        lines.append("")

        # Links
        link_parts = []
        if imdb_id:
            link_parts.append(f'<a href="{get_imdb_url(imdb_id)}">IMDb</a>')
        link_parts.append(f'<a href="{WEBAPP_URL}/film/{film_id}">Details</a>')
        lines.append(" · ".join(link_parts))

        message = "\n".join(lines)

        if send_message(message):
            mark_film_notified(db, film_id)
            logger.info(f"Notified about: {title_original}")
        else:
            logger.error(f"Failed to notify about: {title_original}")


def notify_all_pending() -> None:
    """Send notifications for all films not yet notified."""
    with get_db() as db:
        pending = get_new_unnotified_films(db)

    if not pending:
        logger.info("No pending notifications")
        return

    logger.info(f"Sending {len(pending)} pending notifications")
    for film in pending:
        notify_new_film(film["id"])


def _escape_html(text: str) -> str:
    """Escape HTML special characters for Telegram."""
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))


# =========================================================================
# Bot command handling (for interactive use)
# =========================================================================
def handle_updates() -> None:
    """Long-polling loop to handle bot commands."""
    if not BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set")
        return

    logger.info("Starting Telegram bot polling...")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"

    # Drain any updates that arrived while the bot was offline so we don't
    # replay old commands on every restart.
    offset = 0
    try:
        resp = requests.get(url, params={"timeout": 0}, timeout=5)
        if resp.ok:
            pending = resp.json().get("result", [])
            if pending:
                offset = pending[-1]["update_id"] + 1
                logger.info(f"Skipped {len(pending)} stale updates on startup")
    except requests.RequestException as e:
        logger.warning(f"Could not drain pending updates on startup: {e}")

    while True:
        try:
            resp = requests.get(url, params={
                "offset": offset,
                "timeout": 30,
            }, timeout=35)
            resp.raise_for_status()
            data = resp.json()

            for update in data.get("result", []):
                offset = update["update_id"] + 1
                _process_update(update)

        except requests.RequestException as e:
            logger.error(f"Polling error: {e}")
            import time
            time.sleep(5)


def _process_update(update: dict) -> None:
    """Process a single Telegram update."""
    message = update.get("message", {})
    text = message.get("text", "")
    chat_id = message.get("chat", {}).get("id")

    if not text or not chat_id:
        return

    if text.startswith("/start"):
        send_message(
            "👋 Hallo! Ich bin der Kino-OV-Bot für Bielefeld.\n\n"
            "Ich benachrichtige dich, wenn neue Filme im Original "
            "(Englisch/Französisch) ins Programm kommen.\n\n"
            f"Deine Chat-ID: <code>{chat_id}</code>\n\n"
            "Befehle:\n"
            "/programm — Aktuelle OV/OmU-Filme\n"
            "/info — Bot-Info",
        )

    elif text.startswith("/programm"):
        with get_db() as db:
            from database import get_upcoming_films
            films = get_upcoming_films(db)

        if not films:
            send_message("Keine OV/OmU-Filme aktuell im Programm.")
            return

        lines = ["🎬 <b>Aktuelle OV/OmU-Filme:</b>\n"]
        for f in films[:15]:
            title = _escape_html(f["title_display"])
            lang = (f["original_language"] or "").upper()
            cinemas = f["cinemas"] or ""
            lines.append(f"• <b>{title}</b> [{lang}] — {cinemas}")

        if len(films) > 15:
            lines.append(f"\n... und {len(films) - 15} weitere.")

        lines.append(f"\n🔗 <a href=\"{WEBAPP_URL}\">Alle Filme ansehen</a>")
        send_message("\n".join(lines))

    elif text.startswith("/info"):
        send_message(
            "ℹ️ <b>Kino OV Bielefeld Bot</b>\n\n"
            "Quellen: Lichtwerk, Kamera, CinemaxX\n"
            "Sprachen: Englisch, Französisch\n"
            f"Web: {WEBAPP_URL}"
        )


if __name__ == "__main__":
    from log_setup import setup_logging
    setup_logging()
    handle_updates()
