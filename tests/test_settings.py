"""Tests for settings module."""
import settings


def test_webapp_url_has_no_trailing_slash(monkeypatch):
    """WEBAPP_URL must never end with a slash, regardless of env var value."""
    monkeypatch.setenv("WEBAPP_URL", "https://example.com/")
    # Re-read the env var the same way settings.py does
    import importlib
    importlib.reload(settings)
    assert not settings.WEBAPP_URL.endswith("/")


def test_webapp_url_without_trailing_slash_unchanged(monkeypatch):
    monkeypatch.setenv("WEBAPP_URL", "https://example.com")
    import importlib
    importlib.reload(settings)
    assert settings.WEBAPP_URL == "https://example.com"
