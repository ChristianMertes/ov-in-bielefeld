"""Central configuration: loads .env and exposes all env-derived settings.

Import from here instead of reading os.environ directly or calling load_dotenv()
in individual modules. Python's module cache guarantees load_dotenv() runs once.
"""
from dotenv import load_dotenv
import os

load_dotenv()

# ── Database ──────────────────────────────────────────────────────────────────
DB_PATH: str = os.environ.get("KINO_DB_PATH", "kino_ov.db")

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR: str = os.environ.get("KINO_LOG_DIR", "logs")

# ── Web server ────────────────────────────────────────────────────────────────
PORT: int = int(os.environ.get("PORT", "8000"))
WEBAPP_URL: str = os.environ.get("WEBAPP_URL", "http://localhost:8000")

# ── TMDb ──────────────────────────────────────────────────────────────────────
TMDB_API_KEY: str = os.environ.get("TMDB_API_KEY", "")

# ── OMDb (Rotten Tomatoes scores) ─────────────────────────────────────────────
OMDB_API_KEY: str | None = os.environ.get("OMDB_API_KEY")

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN: str | None = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID: str | None = os.environ.get("TELEGRAM_CHAT_ID")
