import os
import sqlite3
import tempfile

import bot


def test_init_db_creates_indexes(tmp_path, monkeypatch):
    db_file = tmp_path / "test.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    bot.DB_PATH = str(db_file)
    # Ensure DB is initialized
    bot.init_db()
    conn = sqlite3.connect(str(db_file))
    try:
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = {row[0] for row in c.fetchall()}
    finally:
        conn.close()
    # Check that our indexes exist
    assert "idx_users_state" in indexes
    assert "idx_event_snapshots_start" in indexes
    assert "idx_meeting_drafts_user" in indexes
