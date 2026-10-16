"""Central configuration: loads .env and exposes all env-derived settings.

Import from here instead of reading os.environ directly or calling load_dotenv()
in individual modules. Python's module cache guarantees load_dotenv() runs once.
"""
import os

from dotenv import load_dotenv

load_dotenv()

# ── Database ──────────────────────────────────────────────────────────────────
DB_PATH: str = os.environ.get("KINO_DB_PATH", "kino_ov.db")

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR: str = os.environ.get("KINO_LOG_DIR", "logs")

# ── Web server ────────────────────────────────────────────────────────────────
PORT: int = int(os.environ.get("PORT", "8000"))
WEBAPP_URL: str = os.environ.get("WEBAPP_URL", "http://localhost:8000").rstrip("/")

# ── TMDb ──────────────────────────────────────────────────────────────────────
TMDB_API_KEY: str = os.environ.get("TMDB_API_KEY", "")

# ── OMDb (Rotten Tomatoes scores) ─────────────────────────────────────────────
OMDB_API_KEY: str | None = os.environ.get("OMDB_API_KEY")

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN: str | None = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID: str | None = os.environ.get("TELEGRAM_CHAT_ID")

# Alerts that need a person rather than a log file – a film dropped where
# nobody can see it is missing. A second bot keeps those out of the public
# group; pointing the existing bot at a private chat works just as well, which
# is why only the chat id has to be set.
TELEGRAM_ADMIN_BOT_TOKEN: str | None = (
    os.environ.get("TELEGRAM_ADMIN_BOT_TOKEN") or TELEGRAM_BOT_TOKEN
)
TELEGRAM_ADMIN_CHAT_ID: str | None = os.environ.get("TELEGRAM_ADMIN_CHAT_ID")

# ── Display names ───────────────────────────────────────────────────────────────
# Brand-correct display names for cinema keys (shared by webapp and telegram bot).
CINEMA_DISPLAY_NAMES: dict[str, str] = {
    "lichtwerk": "Lichtwerk",
    "kamera": "Kamera",
    "cinemaxx": "CinemaxX",
}
