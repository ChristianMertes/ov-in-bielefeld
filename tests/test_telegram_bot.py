"""Tests for the Telegram bot module."""
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

import database
import telegram_bot
from database import get_film_by_id, init_db, upsert_film, upsert_showtime
from telegram_bot import (
    _escape_html,
    _process_update,
    handle_updates,
    notify_all_pending,
    notify_new_film,
    send_message,
)


def _future(days: int = 1) -> str:
    return (datetime.now() + timedelta(days=days)).replace(microsecond=0).isoformat()


@pytest.fixture
def tg_db(tmp_path, monkeypatch):
    """Isolated DB for telegram bot tests, with DB_PATH patched everywhere."""
    db_path = str(tmp_path / "tg_test.db")
    monkeypatch.setattr(database, "DB_PATH", db_path)
    init_db()
    conn = database.get_connection()
    yield conn
    conn.close()


@pytest.fixture
def _configured(monkeypatch):
    """Patch BOT_TOKEN and CHAT_ID so send_message doesn't bail out."""
    monkeypatch.setattr(telegram_bot, "BOT_TOKEN", "fake-token")
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "123456")


def _insert_film(db, title="Test Film", imdb_id="tt1234567", notified=0, **kwargs):
    """Insert a film and a future showtime, return the film_id."""
    film_id, _ = upsert_film(db, title, imdb_id=imdb_id, **kwargs)
    upsert_showtime(db, film_id, cinema="lichtwerk", showtime=_future(),
                    language_tag="OV", booking_url="https://example.com/book")
    if notified:
        db.execute("UPDATE films SET notified = 1 WHERE id = ?", (film_id,))
    db.commit()
    return film_id


# ── _escape_html ─────────────────────────────────────────────────────────────

def test_escape_html_ampersand():
    assert _escape_html("A & B") == "A &amp; B"


def test_escape_html_angle_brackets():
    assert _escape_html("<script>") == "&lt;script&gt;"


def test_escape_html_plain_text_unchanged():
    assert _escape_html("Hello World") == "Hello World"


def test_escape_html_all_entities():
    assert _escape_html("a<b&c>d") == "a&lt;b&amp;c&gt;d"


# ── send_message ─────────────────────────────────────────────────────────────

def test_send_message_no_token(monkeypatch):
    """Returns False without making any HTTP call when token is missing."""
    monkeypatch.setattr(telegram_bot, "BOT_TOKEN", None)
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "123")
    assert send_message("hello") is False


def test_send_message_no_chat_id(monkeypatch):
    monkeypatch.setattr(telegram_bot, "BOT_TOKEN", "tok")
    monkeypatch.setattr(telegram_bot, "CHAT_ID", None)
    assert send_message("hello") is False


def test_send_message_no_token_and_no_chat_id(monkeypatch):
    monkeypatch.setattr(telegram_bot, "BOT_TOKEN", None)
    monkeypatch.setattr(telegram_bot, "CHAT_ID", None)
    assert send_message("hello") is False


@pytest.mark.usefixtures("_configured")
def test_send_message_success(monkeypatch):
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    with patch("telegram_bot.requests.post", return_value=mock_resp) as mock_post:
        result = send_message("hello")
    assert result is True
    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args
    assert call_kwargs[1]["json"]["text"] == "hello"
    assert call_kwargs[1]["json"]["chat_id"] == "123456"


@pytest.mark.usefixtures("_configured")
def test_send_message_request_error(monkeypatch):
    import requests
    msg = "timeout"
    with patch("telegram_bot.requests.post", side_effect=requests.RequestException(msg)):
        result = send_message("hello")
    assert result is False


# ── notify_new_film ──────────────────────────────────────────────────────────

def test_notify_new_film_marks_notified(tg_db, monkeypatch):
    monkeypatch.setattr(telegram_bot, "BOT_TOKEN", "fake")
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "123")
    film_id = _insert_film(tg_db, title="Inception", imdb_id="tt1375666",
                           release_year=2010, runtime_minutes=148,
                           original_language="en", title_original="Inception")
    with patch("telegram_bot.send_message", return_value=True) as mock_send:
        notify_new_film(film_id)
    mock_send.assert_called_once()
    msg = mock_send.call_args[0][0]
    assert "Inception" in msg
    assert "2010" in msg
    # Film should now be marked as notified
    row = get_film_by_id(tg_db, film_id)
    assert row["notified"] == 1


def test_notify_new_film_includes_showtimes(tg_db, monkeypatch):
    monkeypatch.setattr(telegram_bot, "BOT_TOKEN", "fake")
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "123")
    film_id = _insert_film(tg_db, title="Dune", imdb_id="tt0001")
    with patch("telegram_bot.send_message", return_value=True) as mock_send:
        notify_new_film(film_id)
    msg = mock_send.call_args[0][0]
    assert "Vorstellungen" in msg
    assert "Lichtwerk" in msg  # cinema capitalized


def test_notify_new_film_send_failure_does_not_mark(tg_db, monkeypatch):
    monkeypatch.setattr(telegram_bot, "BOT_TOKEN", "fake")
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "123")
    film_id = _insert_film(tg_db, title="Failed Film", imdb_id="tt9999")
    with patch("telegram_bot.send_message", return_value=False):
        notify_new_film(film_id)
    row = get_film_by_id(tg_db, film_id)
    assert row["notified"] == 0


def test_notify_new_film_nonexistent_id(tg_db, monkeypatch):
    monkeypatch.setattr(telegram_bot, "BOT_TOKEN", "fake")
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "123")
    with patch("telegram_bot.send_message") as mock_send:
        notify_new_film(99999)
    mock_send.assert_not_called()


def test_notify_new_film_showtime_overflow(tg_db, monkeypatch):
    """When more than 8 showtimes exist, the message should say '... und N weitere'."""
    monkeypatch.setattr(telegram_bot, "BOT_TOKEN", "fake")
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "123")
    film_id, _ = upsert_film(tg_db, "Popular Film", imdb_id="tt0002")
    for i in range(10):
        upsert_showtime(tg_db, film_id, cinema="cinemaxx",
                        showtime=_future(i + 1), language_tag="OV")
    tg_db.commit()
    with patch("telegram_bot.send_message", return_value=True) as mock_send:
        notify_new_film(film_id)
    msg = mock_send.call_args[0][0]
    assert "2 weitere" in msg


def test_notify_new_film_includes_imdb_link(tg_db, monkeypatch):
    monkeypatch.setattr(telegram_bot, "BOT_TOKEN", "fake")
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "123")
    film_id = _insert_film(tg_db, title="IMDb Film", imdb_id="tt1234567")
    with patch("telegram_bot.send_message", return_value=True) as mock_send:
        notify_new_film(film_id)
    msg = mock_send.call_args[0][0]
    assert "IMDb" in msg
    assert "imdb.com" in msg


def test_notify_new_film_includes_webapp_link(tg_db, monkeypatch):
    monkeypatch.setattr(telegram_bot, "BOT_TOKEN", "fake")
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "123")
    monkeypatch.setattr(telegram_bot, "WEBAPP_URL", "https://kino.example.com")
    film_id = _insert_film(tg_db, title="Link Film", imdb_id="tt0003")
    with patch("telegram_bot.send_message", return_value=True) as mock_send:
        notify_new_film(film_id)
    msg = mock_send.call_args[0][0]
    assert "https://kino.example.com/film/" in msg


# ── notify_all_pending ───────────────────────────────────────────────────────

def test_notify_all_pending_sends_for_unnotified(tg_db, monkeypatch):
    monkeypatch.setattr(telegram_bot, "BOT_TOKEN", "fake")
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "123")
    _insert_film(tg_db, title="Film A", imdb_id="tt0010", notified=0)
    _insert_film(tg_db, title="Film B", imdb_id="tt0011", notified=0)
    _insert_film(tg_db, title="Film C", imdb_id="tt0012", notified=1)
    with patch("telegram_bot.notify_new_film") as mock_notify:
        notify_all_pending()
    assert mock_notify.call_count == 2


def test_notify_all_pending_nothing_pending(tg_db, monkeypatch):
    monkeypatch.setattr(telegram_bot, "BOT_TOKEN", "fake")
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "123")
    _insert_film(tg_db, title="Already Notified", imdb_id="tt0020", notified=1)
    with patch("telegram_bot.notify_new_film") as mock_notify:
        notify_all_pending()
    mock_notify.assert_not_called()


# ── handle_updates ───────────────────────────────────────────────────────────

def test_handle_updates_no_token(monkeypatch):
    """Should return immediately without making any HTTP call."""
    monkeypatch.setattr(telegram_bot, "BOT_TOKEN", None)
    with patch("telegram_bot.requests.get") as mock_get:
        handle_updates()
    mock_get.assert_not_called()


def test_handle_updates_drains_stale_and_polls(monkeypatch):
    """Should drain stale updates, then poll. We break the loop via exception."""
    monkeypatch.setattr(telegram_bot, "BOT_TOKEN", "fake-token")
    import requests as req

    drain_resp = MagicMock()
    drain_resp.ok = True
    drain_resp.json.return_value = {"result": [{"update_id": 100}]}

    call_count = 0

    def fake_get(url, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return drain_resp
        msg = "stop loop"
        raise req.RequestException(msg)

    with (
        patch("telegram_bot.requests.get", side_effect=fake_get),
        patch("telegram_bot.time.sleep", side_effect=StopIteration),
        pytest.raises(StopIteration),
    ):
        handle_updates()
    assert call_count == 2


# ── _process_update ──────────────────────────────────────────────────────────

def test_process_update_start_command(monkeypatch):
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "42")
    update = {"message": {"text": "/start", "chat": {"id": 42}}}
    with patch("telegram_bot.send_message") as mock_send:
        _process_update(update)
    mock_send.assert_called_once()
    msg = mock_send.call_args[0][0]
    assert "Hallo" in msg
    assert "42" in msg


def test_process_update_info_command(monkeypatch):
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "42")
    update = {"message": {"text": "/info", "chat": {"id": 42}}}
    with patch("telegram_bot.send_message") as mock_send:
        _process_update(update)
    mock_send.assert_called_once()
    msg = mock_send.call_args[0][0]
    assert "Lichtwerk" in msg


def test_process_update_programm_with_films(tg_db, monkeypatch):
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "42")
    monkeypatch.setattr(telegram_bot, "WEBAPP_URL", "https://kino.test")
    _insert_film(tg_db, title="OV Film", imdb_id="tt0030")
    update = {"message": {"text": "/programm", "chat": {"id": 42}}}
    with patch("telegram_bot.send_message") as mock_send:
        _process_update(update)
    mock_send.assert_called_once()
    msg = mock_send.call_args[0][0]
    assert "OV Film" in msg
    assert "https://kino.test" in msg


def test_process_update_programm_empty(tg_db, monkeypatch):
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "42")
    update = {"message": {"text": "/programm", "chat": {"id": 42}}}
    with patch("telegram_bot.send_message") as mock_send:
        _process_update(update)
    mock_send.assert_called_once()
    assert "Keine" in mock_send.call_args[0][0]


def test_process_update_unknown_command(monkeypatch):
    """Unknown commands should not trigger any message."""
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "42")
    update = {"message": {"text": "/unknown", "chat": {"id": 42}}}
    with patch("telegram_bot.send_message") as mock_send:
        _process_update(update)
    mock_send.assert_not_called()


def test_process_update_no_text(monkeypatch):
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "42")
    update = {"message": {"chat": {"id": 42}}}
    with patch("telegram_bot.send_message") as mock_send:
        _process_update(update)
    mock_send.assert_not_called()


def test_process_update_no_chat_id():
    update = {"message": {"text": "/start"}}
    with patch("telegram_bot.send_message") as mock_send:
        _process_update(update)
    mock_send.assert_not_called()


def test_process_update_wrong_chat_id_ignored(monkeypatch):
    """Commands from a chat that isn't CHAT_ID must be silently ignored."""
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "123456")
    update = {"message": {"text": "/start", "chat": {"id": 999999}}}
    with patch("telegram_bot.send_message") as mock_send:
        _process_update(update)
    mock_send.assert_not_called()


def test_process_update_correct_chat_id_handled(monkeypatch):
    """Commands from the configured CHAT_ID must be processed."""
    monkeypatch.setattr(telegram_bot, "CHAT_ID", "42")
    update = {"message": {"text": "/start", "chat": {"id": 42}}}
    with patch("telegram_bot.send_message") as mock_send:
        _process_update(update)
    mock_send.assert_called_once()
