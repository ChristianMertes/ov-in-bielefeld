FROM python:3.13-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install dependencies first (cached layer unless pyproject.toml/uv.lock changes)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy application code
COPY . .

# SQLite data directory, log directory + unprivileged user
RUN mkdir -p /data /app/logs && \
    useradd -u 1000 -m app && \
    chown -R app /app /data

ENV KINO_DB_PATH=/data/kino_ov.db
ENV KINO_LOG_DIR=/app/logs

USER app

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

CMD ["uv", "run", "uvicorn", "webapp:app", "--host", "0.0.0.0", "--port", "8000"]
