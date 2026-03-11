"""Dev launcher: loads .env and starts the web app with auto-reload.

Usage:
    uv run python main.py
"""
from dotenv import load_dotenv
load_dotenv()

import os
import uvicorn

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("webapp:app", host="127.0.0.1", port=port, reload=True)
