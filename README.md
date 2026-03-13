# Kino OV Bielefeld

Aggregates original-version (OV/OmU) film screenings in English and French from cinemas in Bielefeld, Germany, and displays them in a compact web overview. Sends Telegram notifications when new films appear in the programme.

## Sources

| Cinema | Method | Status |
|--------|--------|--------|
| **Lichtwerk** | HTML scraping (arthousekinos-bielefeld.de) | ✅ |
| **Kamera** | HTML scraping (same site) | ✅ |
| **CinemaxX** | REST API (/api/microservice/showings/) | ✅ |

## Features

- Scrapes multiple times daily, more frequently on Wednesdays (when the new Kinowoche programme starts)
- TMDb integration: original title, poster, IMDb/RT ratings, runtime
- Language filtering via TMDb: only English and French films, no German, Japanese, etc.
- Arthouse detail pages fetched for better year disambiguation on TMDb
- Telegram bot: notification for new films + `/programm` command
- Web UI with cinema filter and sorting by date, title, rating, or popularity
- In-process Brotli page cache, invalidated after each scrape and at midnight (date-relative labels)
- Daily-rotating log files, old logs compressed with xz

## Quick start (local)

### Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/) (Python package manager)
- [TMDb API key](https://www.themoviedb.org/settings/api) (free)
- [Telegram bot token](https://t.me/BotFather) (optional, for notifications)

### Setup

```bash
git clone <repo-url> && cd ov-in-bielefeld
cp .env.example .env
# Edit .env: set TMDB_API_KEY (and optionally TELEGRAM_* keys)

uv sync
```

### Run

```bash
# Initial scrape (populates the database):
uv run python orchestrator.py

# Web app (with auto-reload):
uv run python main.py
# → http://localhost:8000

# Scheduler (background scraping + Telegram notifications):
uv run python scheduler.py
```

### Tests

```bash
uv run pytest
```

79 tests covering the scraper parsing logic (including a real HTML fixture from the arthouse site), OV/language detection, date-label formatting, and the database layer. Runs in under a second.

## Deployment (Hetzner VPS)

Recommended setup: **Hetzner Cloud CAX11** (ARM, ~€3.29/month) + Docker Compose.

### Server setup

```bash
# Install Docker
curl -fsSL https://get.docker.com | sh

# Create a dedicated user (recommended)
adduser kino
usermod -aG docker kino
# Add SSH key for the kino user, then log in as kino

# Clone repo and configure
git clone <repo-url> && cd ov-in-bielefeld
cp .env.example .env
nano .env  # Set TMDB_API_KEY, TELEGRAM_*, WEBAPP_URL

# Start
docker compose up -d

# Live logs (stdout from both containers)
docker compose logs -f

# Persistent log file (rotates daily, old logs xz-compressed)
docker compose exec web tail -f /app/logs/kino.log
```

### Reverse proxy (Caddy)

```
kino.your-domain.de {
    reverse_proxy localhost:8000
}
```

Caddy automatically obtains a Let's Encrypt certificate.

### Updates

```bash
git pull
docker compose up -d --build
```

## Architecture

```
ov-in-bielefeld/
├── webapp.py           # FastAPI + Jinja2 web app, Brotli cache, access log middleware
├── orchestrator.py     # Scraper runner + TMDb enrichment + DB writes
├── scheduler.py        # APScheduler (midnight cache flush, 06:00 daily, Wednesdays more often)
├── telegram_bot.py     # Telegram notifications + /programm command
├── database.py         # SQLite layer (films, showtimes, tmdb_cache)
├── tmdb_client.py      # TMDb API client + IMDb/OMDb URL helpers
├── ratings_client.py   # IMDb rating + Rotten Tomatoes score fetching
├── cache.py            # In-process Brotli page cache with sentinel-file invalidation
├── log_setup.py        # Centralised logging: stderr + daily-rotating xz log file
├── main.py             # Dev launcher (uvicorn with reload)
├── scrapers/
│   ├── arthouse.py     # Lichtwerk & Kamera (HTML scraping)
│   └── cinemaxx.py     # CinemaxX (two-step REST API)
├── templates/          # Jinja2 HTML templates
├── static/             # CSS, assets
├── tests/              # pytest suite (79 tests)
│   ├── fixtures/       # Real HTML fixture from arthouse site (xz-compressed)
│   ├── test_arthouse.py
│   ├── test_cinemaxx.py
│   ├── test_webapp_helpers.py
│   └── test_database.py
├── pyproject.toml      # Dependencies (uv)
├── docker-compose.yml
└── Dockerfile
```

## Scraping schedule

| When | Frequency | Why |
|------|-----------|-----|
| Daily 00:00 | once | Flush page cache (date labels go stale at midnight) |
| Daily 06:00 | once | Refresh base data |
| Wednesdays 08:00–20:00 | every 2h | New Kinowoche programme starts Wednesday |
| Other days | every 6h | Catch late additions and changes |

## Environment variables (.env)

| Variable | Required | Description |
|----------|----------|-------------|
| `TMDB_API_KEY` | ✅ | TMDb API key (free) |
| `OMDB_API_KEY` | optional | OMDb API key for Rotten Tomatoes scores (free, 1000 req/day) |
| `TELEGRAM_BOT_TOKEN` | optional | Bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | optional | Your chat ID (use @userinfobot to find it) |
| `WEBAPP_URL` | optional | Public URL for Telegram links (default: http://localhost:8000) |
| `KINO_DB_PATH` | optional | Path to SQLite database (default: kino_ov.db) |
| `KINO_LOG_DIR` | optional | Log directory (default: logs/, in Docker: /app/logs) |
| `PORT` | optional | Web server port (default: 8000) |
