"""SQLite database for MBB Social Branding Tracker."""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "tracker.db"


def get_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS reels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            title TEXT DEFAULT '',
            posted_date TEXT,
            platform TEXT DEFAULT 'instagram',
            account TEXT DEFAULT '',
            custom_fields TEXT DEFAULT '{}',
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reel_id INTEGER NOT NULL REFERENCES reels(id) ON DELETE CASCADE,
            views INTEGER,
            likes INTEGER,
            comments INTEGER,
            fetched_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS custom_columns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS monthly_views (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reel_id INTEGER NOT NULL REFERENCES reels(id) ON DELETE CASCADE,
            month TEXT NOT NULL,
            cumulative_views INTEGER,
            is_manual INTEGER DEFAULT 0,
            updated_at TEXT DEFAULT (datetime('now')),
            UNIQUE(reel_id, month)
        );
    """)
    # Migration: add platform column if DB existed before
    cols = [r[1] for r in conn.execute("PRAGMA table_info(reels)").fetchall()]
    if "platform" not in cols:
        conn.execute("ALTER TABLE reels ADD COLUMN platform TEXT DEFAULT 'instagram'")
    if "account" not in cols:
        conn.execute("ALTER TABLE reels ADD COLUMN account TEXT DEFAULT ''")
    conn.commit()
    conn.close()
