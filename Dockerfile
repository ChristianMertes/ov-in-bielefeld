FROM python:3.13-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install dependencies first (cached layer unless pyproject.toml/uv.lock changes)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy application code
COPY . .

# SQLite data directory
RUN mkdir -p /data
ENV KINO_DB_PATH=/data/kino_ov.db

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "webapp:app", "--host", "0.0.0.0", "--port", "8000"]
