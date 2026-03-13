"""Centralised logging configuration with daily rotation and xz compression.

Sets up the root logger with two handlers:
  - StreamHandler  → stderr (always; Docker captures this)
  - TimedRotatingFileHandler → logs/kino.log, rotates at midnight,
    compresses rotated files with xz, keeps 30 days of history

Log directory: settings.LOG_DIR ($KINO_LOG_DIR, default: logs/)
"""
import logging
import lzma
import os
import shutil
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import settings


def _xz_namer(name: str) -> str:
    return name + ".xz"


def _xz_rotator(source: str, dest: str) -> None:
    with open(source, "rb") as f_in, lzma.open(dest, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(source)


def setup_logging(level: int = logging.INFO) -> None:
    """Configure root logger. Safe to call multiple times; subsequent calls are no-ops."""
    root = logging.getLogger()

    # Already has a file handler → already configured
    if any(isinstance(h, TimedRotatingFileHandler) for h in root.handlers):
        return

    root.setLevel(level)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    # stderr — Docker / systemd capture this
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)
    root.addHandler(stream_handler)

    # Daily-rotating file, old logs compressed with xz
    log_dir = Path(settings.LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)

    file_handler = TimedRotatingFileHandler(
        log_dir / "kino.log",
        when="midnight",
        backupCount=30,
        encoding="utf-8",
        utc=False,
    )
    file_handler.namer = _xz_namer
    file_handler.rotator = _xz_rotator
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)
