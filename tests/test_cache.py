"""Tests for the in-process page cache."""
import cache
import settings


def _reset():
    """Clear both stores and sync version to current mtime so get() won't immediately re-clear."""
    with cache._lock:
        cache._store.clear()
        cache._store_plain.clear()
        cache._version = cache._mtime()


# ── put / get (Brotli) ────────────────────────────────────────────────────────

def test_put_returns_bytes(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", str(tmp_path / "kino.db"))
    _reset()
    result = cache.put("k", "<html>hello</html>")
    assert isinstance(result, bytes)
    assert len(result) > 0


def test_get_returns_stored_bytes(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", str(tmp_path / "kino.db"))
    _reset()
    stored = cache.put("page1", "<html>hi</html>")
    assert cache.get("page1") == stored


def test_get_miss_returns_none(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", str(tmp_path / "kino.db"))
    _reset()
    assert cache.get("nonexistent") is None


def test_brotli_bytes_decompress_correctly(tmp_path, monkeypatch):
    import brotli as _brotli  # type: ignore[import-untyped]
    monkeypatch.setattr(settings, "DB_PATH", str(tmp_path / "kino.db"))
    _reset()
    html = "<html><body>Test content</body></html>"
    cache.put("page", html)
    decompressed = _brotli.decompress(cache.get("page")).decode()
    assert decompressed == html


# ── put_plain / get_plain ─────────────────────────────────────────────────────

def test_put_plain_returns_bytes(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", str(tmp_path / "kino.db"))
    _reset()
    result = cache.put_plain("k", "<html>hello</html>")
    assert isinstance(result, bytes)


def test_get_plain_returns_stored_bytes(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", str(tmp_path / "kino.db"))
    _reset()
    stored = cache.put_plain("page1", "<html>hi</html>")
    assert cache.get_plain("page1") == stored


def test_get_plain_content_is_utf8(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", str(tmp_path / "kino.db"))
    _reset()
    html = "<html>Übermorgen</html>"
    cache.put_plain("page", html)
    assert cache.get_plain("page").decode() == html


def test_get_plain_miss_returns_none(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", str(tmp_path / "kino.db"))
    _reset()
    assert cache.get_plain("nonexistent") is None


# ── stores are independent ────────────────────────────────────────────────────

def test_brotli_and_plain_stores_are_independent(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", str(tmp_path / "kino.db"))
    _reset()
    cache.put("page", "<html>compressed</html>")
    assert cache.get_plain("page") is None  # plain store untouched

    cache.put_plain("page2", "<html>plain</html>")
    assert cache.get("page2") is None  # brotli store untouched


# ── expiry ───────────────────────────────────────────────────────────────────
# Without a TTL, a page rendered right after the 6am scrape is served all day,
# so showtimes that have since passed stay on screen no matter how often the
# visitor reloads.

def test_entry_expires_after_ttl(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", str(tmp_path / "kino.db"))
    _reset()
    clock = [1000.0]
    monkeypatch.setattr(cache.time, "monotonic", lambda: clock[0])

    cache.put("page", "<html>hi</html>")
    assert cache.get("page") is not None

    clock[0] += cache.TTL_SECONDS + 1
    assert cache.get("page") is None


def test_entry_survives_until_ttl(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", str(tmp_path / "kino.db"))
    _reset()
    clock = [1000.0]
    monkeypatch.setattr(cache.time, "monotonic", lambda: clock[0])

    cache.put("page", "<html>hi</html>")
    clock[0] += cache.TTL_SECONDS - 1
    assert cache.get("page") is not None


def test_plain_entry_expires_after_ttl(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", str(tmp_path / "kino.db"))
    _reset()
    clock = [1000.0]
    monkeypatch.setattr(cache.time, "monotonic", lambda: clock[0])

    cache.put_plain("page", "<html>hi</html>")
    assert cache.get_plain("page") is not None

    clock[0] += cache.TTL_SECONDS + 1
    assert cache.get_plain("page") is None


def test_expired_entries_are_dropped_not_just_hidden(tmp_path, monkeypatch):
    """An expired entry must not linger in the store and leak memory."""
    monkeypatch.setattr(settings, "DB_PATH", str(tmp_path / "kino.db"))
    _reset()
    clock = [1000.0]
    monkeypatch.setattr(cache.time, "monotonic", lambda: clock[0])

    cache.put("page", "<html>hi</html>")
    cache.put_plain("page", "<html>hi</html>")
    clock[0] += cache.TTL_SECONDS + 1
    cache.get("page")
    cache.get_plain("page")

    with cache._lock:
        assert "page" not in cache._store
        assert "page" not in cache._store_plain


# ── invalidation ─────────────────────────────────────────────────────────────

def test_invalidate_clears_both_stores(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", str(tmp_path / "kino.db"))
    _reset()
    cache.put("page", "<html>hi</html>")
    cache.put_plain("page", "<html>hi</html>")

    cache.invalidate()

    assert cache.get("page") is None
    assert cache.get_plain("page") is None


def test_sentinel_mtime_change_clears_both_stores(tmp_path, monkeypatch):
    """Simulates what happens when the orchestrator touches the sentinel file."""
    monkeypatch.setattr(settings, "DB_PATH", str(tmp_path / "kino.db"))
    _reset()

    # Prime both caches
    cache.put("p", "<html>a</html>")
    cache.put_plain("p", "<html>a</html>")
    assert cache.get("p") is not None
    assert cache.get_plain("p") is not None

    # Touch sentinel (same as orchestrator does after scrape)
    cache.invalidate()

    assert cache.get("p") is None
    assert cache.get_plain("p") is None
