"""In-process page cache storing pre-compressed (gzip) HTML bytes.

Cache entries are keyed by route + query parameters. Invalidation is
cross-process: the orchestrator touches a sentinel file on the shared
/data volume after each scrape; the webapp detects the mtime change and
clears the store on the next request.
"""
import gzip
import os
import threading
from pathlib import Path

_lock = threading.Lock()
_store: dict[str, bytes] = {}
_version: float = -1.0


def _sentinel() -> Path:
    return Path(os.environ.get("KINO_DB_PATH", "kino_ov.db")).parent / ".last_scrape"


def _mtime() -> float:
    try:
        return _sentinel().stat().st_mtime
    except FileNotFoundError:
        return 0.0


def get(key: str) -> bytes | None:
    """Return cached gzip bytes for key, or None if stale/absent."""
    global _store, _version
    v = _mtime()
    with _lock:
        if v != _version:
            _store.clear()
            _version = v
        return _store.get(key)


def put(key: str, html: str) -> bytes:
    """Compress html, store under key, and return the compressed bytes."""
    compressed = gzip.compress(html.encode(), compresslevel=6)
    with _lock:
        _store[key] = compressed
    return compressed


def invalidate() -> None:
    """Touch the sentinel file to invalidate the cache in all processes."""
    path = _sentinel()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
