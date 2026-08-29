import os

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

load_dotenv()


class Database:
    def __init__(self, connection): self.connection = connection
    def __enter__(self): return self
    def __exit__(self, exc_type, *_):
        if exc_type: self.connection.rollback()
        else: self.connection.commit()
        self.connection.close()
    def execute(self, sql, params=None): return self.connection.execute(sql.replace("?", "%s"), params)
    def executemany(self, sql, params_seq):
        """Run a bounded batch with the same parameterized SQL conversion."""
        with self.connection.cursor() as cursor:
            return cursor.executemany(sql.replace("?", "%s"), params_seq)
    def commit(self): self.connection.commit()


def connect():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    return Database(psycopg.connect(url, row_factory=dict_row, prepare_threshold=None))


def init_db() -> None:
    with connect() as db:
        db.execute("""
        CREATE TABLE IF NOT EXISTS oauth_pending (state TEXT PRIMARY KEY, verifier TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP);
        CREATE TABLE IF NOT EXISTS oauth_token (id INTEGER PRIMARY KEY CHECK (id=1), access_token TEXT NOT NULL, refresh_token TEXT, expires_at BIGINT NOT NULL, spotify_user_id TEXT);
        CREATE TABLE IF NOT EXISTS sync_run (id TEXT PRIMARY KEY, status TEXT NOT NULL, stage TEXT NOT NULL, message TEXT, listed_playlists INTEGER DEFAULT 0, readable_playlists INTEGER DEFAULT 0, imported_items INTEGER DEFAULT 0, skipped_playlists INTEGER DEFAULT 0, started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, last_progress_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, completed_at TIMESTAMPTZ);
        CREATE TABLE IF NOT EXISTS playlist (id BIGSERIAL PRIMARY KEY, spotify_id TEXT UNIQUE NOT NULL, name TEXT NOT NULL, owner_id TEXT, snapshot_id TEXT, track_total INTEGER DEFAULT 0, readable INTEGER DEFAULT 1, error_reason TEXT, spotify_url TEXT, updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP);
        CREATE TABLE IF NOT EXISTS track (id BIGSERIAL PRIMARY KEY, spotify_id TEXT UNIQUE, isrc TEXT, name TEXT NOT NULL, normalized_name TEXT NOT NULL, duration_ms INTEGER, album_name TEXT, release_date TEXT, release_year INTEGER, linked_from_id TEXT, is_local INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS artist (id BIGSERIAL PRIMARY KEY, spotify_id TEXT UNIQUE, name TEXT NOT NULL, normalized_name TEXT NOT NULL, genres_checked_at TIMESTAMPTZ);
        CREATE TABLE IF NOT EXISTS artist_genre (artist_id BIGINT NOT NULL REFERENCES artist(id) ON DELETE CASCADE, genre TEXT NOT NULL, PRIMARY KEY(artist_id,genre));
        CREATE TABLE IF NOT EXISTS track_artist (track_id BIGINT NOT NULL REFERENCES track(id) ON DELETE CASCADE, artist_id BIGINT NOT NULL REFERENCES artist(id) ON DELETE CASCADE, ordinal INTEGER NOT NULL, PRIMARY KEY(track_id,artist_id));
        CREATE TABLE IF NOT EXISTS playlist_item (playlist_id BIGINT NOT NULL REFERENCES playlist(id) ON DELETE CASCADE, position INTEGER NOT NULL, track_id BIGINT REFERENCES track(id) ON DELETE SET NULL, item_type TEXT NOT NULL, added_at TEXT, PRIMARY KEY(playlist_id,position));
        CREATE TABLE IF NOT EXISTS assessment (playlist_id BIGINT PRIMARY KEY REFERENCES playlist(id) ON DELETE CASCADE, score REAL, confidence REAL NOT NULL, summary TEXT NOT NULL, factors_json TEXT NOT NULL, genre_json TEXT NOT NULL DEFAULT '[]', created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP);
        CREATE TABLE IF NOT EXISTS playlist_pair (playlist_a_id BIGINT NOT NULL REFERENCES playlist(id) ON DELETE CASCADE, playlist_b_id BIGINT NOT NULL REFERENCES playlist(id) ON DELETE CASCADE, shared_tracks INTEGER NOT NULL, jaccard REAL NOT NULL, containment REAL NOT NULL, artist_similarity REAL, year_similarity REAL, classification TEXT NOT NULL, PRIMARY KEY(playlist_a_id,playlist_b_id));
        ALTER TABLE sync_run ADD COLUMN IF NOT EXISTS last_progress_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP;
        """)


def row_dict(row):
    return dict(row) if row else None
