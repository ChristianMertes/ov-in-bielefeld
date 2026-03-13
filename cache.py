"""In-process page cache storing pre-compressed (Brotli) and plain HTML bytes.

Cache entries are keyed by route + query parameters. Two parallel stores exist:
- _store: Brotli-compressed bytes, served when client sends Accept-Encoding: br
- _store_plain: raw UTF-8 bytes, served when client doesn't support Brotli

Invalidation is cross-process: the orchestrator touches a sentinel file on the
shared /data volume after each scrape; the webapp detects the mtime change and
clears both stores on the next request.
"""
import threading
from pathlib import Path

import brotli  # type: ignore[import-untyped]

import settings

_lock = threading.Lock()
_store: dict[str, bytes] = {}
_store_plain: dict[str, bytes] = {}
_version: float = -1.0


def _sentinel() -> Path:
    return Path(settings.DB_PATH).parent / ".last_scrape"


def _mtime() -> float:
    try:
        return _sentinel().stat().st_mtime
    except FileNotFoundError:
        return 0.0


def _check_version(v: float) -> None:
    """Clear both stores if the sentinel has changed. Must be called under _lock."""
    global _version
    if v != _version:
        _store.clear()
        _store_plain.clear()
        _version = v


def get(key: str) -> bytes | None:
    """Return cached Brotli bytes for key, or None if stale/absent."""
    v = _mtime()
    with _lock:
        _check_version(v)
        return _store.get(key)


def put(key: str, html: str) -> bytes:
    """Compress html, store under key, and return the compressed bytes."""
    compressed = brotli.compress(html.encode(), quality=6)
    with _lock:
        _store[key] = compressed
    return compressed


def get_plain(key: str) -> bytes | None:
    """Return cached plain (uncompressed) bytes for key, or None if stale/absent."""
    v = _mtime()
    with _lock:
        _check_version(v)
        return _store_plain.get(key)


def put_plain(key: str, html: str) -> bytes:
    """Encode html as UTF-8, store under key, and return the raw bytes."""
    raw = html.encode()
    with _lock:
        _store_plain[key] = raw
    return raw


def invalidate() -> None:
    """Touch the sentinel file to invalidate the cache in all processes."""
    path = _sentinel()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
