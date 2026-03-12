# Kino OV Bielefeld

Aggregiert OV/OmU-Vorstellungen (Englisch & Französisch) aus den Bielefelder Kinos und zeigt sie in einer kompakten Web-Übersicht. Benachrichtigt per Telegram über neue Filme.

## Quellen

| Kino | Methode | Status |
|------|---------|--------|
| **Lichtwerk** | HTML-Scraping (arthousekinos-bielefeld.de) | ✅ |
| **Kamera** | HTML-Scraping (gleiche Seite) | ✅ |
| **CinemaxX** | REST API (/api/microservice/showings/) | ✅ |

## Features

- Scraping mehrmals täglich, häufiger mittwochs (Kinowoche-Start)
- TMDb-Integration: Originaltitel, Poster, IMDb-Links, Laufzeit, deutscher Titel
- Sprachfilterung via TMDb: nur Englisch/Französisch, keine deutschen/japanischen/etc. Filme
- Arthouse-Detailseiten für bessere Jahres-Disambiguierung bei TMDb
- Telegram-Bot: Benachrichtigung bei neuen Filmen + `/programm`-Befehl
- Web-Übersicht mit Filter nach Kino und Sortierung nach Datum/Titel

## Schnellstart (lokal)

### Voraussetzungen

- [uv](https://docs.astral.sh/uv/getting-started/installation/) (Python-Packagemanager)
- [TMDb API Key](https://www.themoviedb.org/settings/api) (kostenlos)
- [Telegram Bot Token](https://t.me/BotFather) (optional, für Benachrichtigungen)

### Setup

```bash
git clone <repo-url> && cd ov-in-bielefeld
cp .env.example .env
# .env editieren: TMDB_API_KEY (und optional TELEGRAM_* Keys) setzen

uv sync
```

### Starten

```bash
# Erster Scrape (befüllt die Datenbank):
uv run python orchestrator.py

# Web-App (mit Auto-Reload):
uv run python main.py
# → http://localhost:8000

# Scheduler (Hintergrund-Scraping + Telegram-Notifications):
uv run python scheduler.py
```

## Deployment (Hetzner VPS)

Empfohlene Variante: **Hetzner Cloud CAX11** (ARM, ~€3.29/Monat) + Docker Compose.

### Setup auf dem Server

```bash
# Docker installieren (falls noch nicht vorhanden)
curl -fsSL https://get.docker.com | sh

# Neuen User anlegen (empfohlen)
adduser kino
usermod -aG docker kino
# SSH-Key für kino-User hinterlegen, dann als kino einloggen

# Repo klonen und .env befüllen
git clone <repo-url> && cd ov-in-bielefeld
cp .env.example .env
nano .env  # TMDB_API_KEY, TELEGRAM_*, WEBAPP_URL setzen

# Starten
docker compose up -d

# Logs prüfen
docker compose logs -f
```

### Reverse Proxy (Caddy)

```
kino.deine-domain.de {
    reverse_proxy localhost:8000
}
```

Caddy holt automatisch ein Let's Encrypt Zertifikat.

### Updates einspielen

```bash
git pull
docker compose up -d --build
```

## Architektur

```
ov-in-bielefeld/
├── webapp.py           # FastAPI + Jinja2 Web-App
├── orchestrator.py     # Scraper + TMDb-Enrichment + DB-Update
├── scheduler.py        # APScheduler (täglich 06:00, Mi häufiger)
├── telegram_bot.py     # Telegram-Notifications + /programm-Befehl
├── database.py         # SQLite-Layer
├── tmdb_client.py      # TMDb API Integration
├── main.py             # Dev-Launcher (uvicorn mit reload)
├── scrapers/
│   ├── arthouse.py     # Lichtwerk & Kamera (HTML-Scraping)
│   └── cinemaxx.py     # CinemaxX (REST API, zwei Schritte)
├── templates/          # Jinja2 HTML-Templates
├── static/             # CSS, Assets
├── pyproject.toml      # Abhängigkeiten (uv)
├── docker-compose.yml
└── Dockerfile
```

## Scraping-Zeitplan

| Wann | Frequenz | Warum |
|------|----------|-------|
| Täglich 06:00 | 1x | Basisdaten aktuell halten |
| Mittwochs 08–20 Uhr | alle 2h | Neue Kinowoche fängt mittwochs an |
| Andere Tage | alle 6h | Nachzügler und Änderungen erfassen |

## Umgebungsvariablen (.env)

| Variable | Pflicht | Beschreibung |
|----------|---------|--------------|
| `TMDB_API_KEY` | ✅ | TMDb API Key (kostenlos) |
| `OMDB_API_KEY` | optional | OMDb API Key für Rotten-Tomatoes-Scores (kostenlos, 1000 req/Tag) |
| `TELEGRAM_BOT_TOKEN` | optional | Bot-Token von @BotFather |
| `TELEGRAM_CHAT_ID` | optional | Eigene Chat-ID (z.B. via @userinfobot) |
| `WEBAPP_URL` | optional | Öffentliche URL für Telegram-Links (default: http://localhost:8000) |
| `KINO_DB_PATH` | optional | Pfad zur SQLite-DB (default: kino_ov.db) |
| `PORT` | optional | Web-Port (default: 8000) |
