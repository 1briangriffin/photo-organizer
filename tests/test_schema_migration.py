"""
Tests for the v1 → v2 schema migration.

Properties locked in:
- An existing v1 catalog (schema_version row = 1, no command_runs table) is
  brought forward by init_schema(): the table is created AND the version row
  is bumped to CURRENT_SCHEMA_VERSION.
- init_schema is idempotent on an already-v2 DB.
"""
import sqlite3
from pathlib import Path

from photo_organizer.database.schema import (
    CURRENT_SCHEMA_VERSION,
    _create_core_schema,
    init_schema,
)


def _build_v1_db(db_path: Path) -> None:
    """Create a minimal v1-shaped catalog: schema_version row = 1, no command_runs."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY);")
        conn.execute("INSERT INTO schema_version (version) VALUES (1);")
        conn.commit()
    finally:
        conn.close()


def test_v1_db_migrates_to_current_version(tmp_path):
    db_path = tmp_path / "v1.db"
    _build_v1_db(db_path)

    conn = sqlite3.connect(str(db_path))
    try:
        init_schema(conn)
    finally:
        conn.close()

    # Verify: schema_version row bumped and v3 tables exist.
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        assert version == CURRENT_SCHEMA_VERSION, (
            f"Expected schema_version to be bumped to {CURRENT_SCHEMA_VERSION}, got {version}"
        )

        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='command_runs'"
        )
        assert cur.fetchone() is not None, "command_runs table must be created on migration"
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='file_observations'"
        )
        assert cur.fetchone() is not None, "file_observations table must be created"
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='run_actions'"
        )
        assert cur.fetchone() is not None, "run_actions table must be created"
    finally:
        conn.close()


def test_init_schema_is_idempotent_on_current_db(tmp_path):
    """Running init_schema twice on the same DB must not error or regress state."""
    db_path = tmp_path / "fresh.db"
    conn = sqlite3.connect(str(db_path))
    try:
        init_schema(conn)
        init_schema(conn)
        cur = conn.execute("SELECT version FROM schema_version")
        rows = cur.fetchall()
    finally:
        conn.close()

    assert len(rows) == 1, "schema_version must contain exactly one row"
    assert rows[0][0] == CURRENT_SCHEMA_VERSION


def test_v2_backfills_file_location_state_with_path_keys(tmp_path):
    db_path = tmp_path / "v2.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY);")
        conn.execute("INSERT INTO schema_version (version) VALUES (2);")
        _create_core_schema(conn)
        conn.execute(
            """
            INSERT INTO files (
                id, hash, sparse_hash, type, ext, orig_name, orig_path, dest_path,
                size_bytes, first_seen_at, last_seen_at
            )
            VALUES (1, 'abc', NULL, 'jpeg', '.jpg', 'IMG.JPG',
                    'C:/Photos/IMG.JPG', 'C:/Photos/Out/IMG.JPG',
                    123, '2026-01-01T00:00:00+00:00', '2026-01-02T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO file_occurrences
            (path, file_id, seen_at, mtime, size_bytes, hash, hash_is_sparse)
            VALUES ('C:/Photos/Out/IMG.JPG', 1, 1767225600, 10.0, 123, 'abc', 0)
            """
        )
        conn.commit()
        init_schema(conn)

        cur = conn.execute("SELECT version FROM schema_version")
        assert cur.fetchone()[0] == CURRENT_SCHEMA_VERSION

        cur = conn.execute(
            """
            SELECT path, path_key, root_kind, status
            FROM file_location_state
            WHERE file_id = 1
            """
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    assert rows
    assert ("C:/Photos/Out/IMG.JPG", r"c:\photos\out\img.jpg", "dest", "present") in rows


def test_v3_schema_adds_relationship_provenance_columns(tmp_path):
    db_path = tmp_path / "fresh.db"
    conn = sqlite3.connect(str(db_path))
    try:
        init_schema(conn)
        for table in ("raw_outputs", "raw_sidecars", "psd_source_links"):
            cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
            assert "created_by_run_id" in cols
    finally:
        conn.close()
