"""Shared test fixtures."""
import lzma
from pathlib import Path

import pytest

import database

FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def arthouse_html() -> str:
    with lzma.open(FIXTURE_DIR / "Programm_Lichtwerk_und_Kamera.html.xz", "rt", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def db(tmp_path, monkeypatch):
    """Initialised SQLite database in a temp file. Patches database.DB_PATH."""
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(database, "DB_PATH", db_path)
    database.init_db()
    conn = database.get_connection()
    yield conn
    conn.close()
