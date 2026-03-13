"""Dev launcher: starts the web app with auto-reload.

Usage:
    uv run python main.py
"""
import uvicorn

import settings
from log_setup import setup_logging

if __name__ == "__main__":
    setup_logging()
    uvicorn.run("webapp:app", host="127.0.0.1", port=settings.PORT, reload=True)
