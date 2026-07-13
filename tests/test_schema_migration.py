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

import pytest

from photo_organizer.database import schema as schema_module
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
            SELECT path, path_key, root_kind, root_path_key, status
            FROM file_location_state
            WHERE file_id = 1
            """
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    assert rows
    assert ("C:/Photos/Out/IMG.JPG", r"c:\photos\out\img.jpg", "dest", None, "present") in rows


def test_failed_migration_does_not_advance_schema_version(tmp_path, monkeypatch):
    db_path = tmp_path / "v4.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY);")
        conn.execute("INSERT INTO schema_version (version) VALUES (4);")
        conn.commit()

        def fail_migration(_conn):
            raise RuntimeError("simulated migration failure")

        monkeypatch.setitem(schema_module.MIGRATIONS, 5, fail_migration)

        with pytest.raises(RuntimeError, match="simulated migration failure"):
            init_schema(conn)

        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
    finally:
        conn.close()

    assert version == 4


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


def test_v4_schema_adds_face_tables(tmp_path):
    db_path = tmp_path / "fresh.db"
    conn = sqlite3.connect(str(db_path))
    try:
        init_schema(conn)
        cur = conn.execute("SELECT version FROM schema_version")
        assert cur.fetchone()[0] == CURRENT_SCHEMA_VERSION

        expected = {
            "face_persons",
            "face_detections",
            "face_embeddings",
            "face_clusters",
            "face_cluster_members",
            "face_person_links",
        }
        cur = conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name LIKE 'face_%'
            """
        )
        actual = {row[0] for row in cur.fetchall()}
    finally:
        conn.close()

    assert expected <= actual


def test_v3_db_migrates_to_v4_face_tables(tmp_path):
    db_path = tmp_path / "v3.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY);")
        conn.execute("INSERT INTO schema_version (version) VALUES (3);")
        _create_core_schema(conn)
        schema_module._migration_3_catalog_state(conn)
        conn.commit()
        init_schema(conn)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='face_detections'"
        ).fetchone()
    finally:
        conn.close()

    assert version == CURRENT_SCHEMA_VERSION
    assert table is not None


def test_v4_db_migrates_to_per_run_action_idempotency(tmp_path):
    db_path = tmp_path / "v4.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY);")
        conn.execute("INSERT INTO schema_version (version) VALUES (4);")
        _create_core_schema(conn)
        schema_module._migration_3_catalog_state(conn)
        schema_module._migration_4_face_tables(conn)
        conn.commit()

        init_schema(conn)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        indexes = {
            row[1]: bool(row[2])
            for row in conn.execute("PRAGMA index_list(run_actions)")
        }
    finally:
        conn.close()

    assert version == CURRENT_SCHEMA_VERSION
    assert indexes["idx_run_actions_run_idempotency_key"] is True
    assert indexes["idx_run_actions_idempotency_key"] is False


def test_v6_db_migrates_to_proposal_lifecycle_columns(tmp_path):
    db_path = tmp_path / "v6.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY);")
        conn.execute("INSERT INTO schema_version (version) VALUES (6);")
        _create_core_schema(conn)
        schema_module._migration_3_catalog_state(conn)
        schema_module._migration_4_face_tables(conn)
        schema_module._migration_5_run_action_attempt_history(conn)
        schema_module._migration_6_camera_identity_metadata(conn)
        # Recreate run_actions in its pre-v7 shape (no resolved_* columns).
        conn.execute("DROP TABLE run_actions")
        conn.execute("""
            CREATE TABLE run_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                proposed_by_run_id INTEGER NOT NULL REFERENCES command_runs(id),
                applied_by_run_id INTEGER REFERENCES command_runs(id),
                action_type TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id INTEGER,
                source_path TEXT,
                source_path_key TEXT,
                target_path TEXT,
                target_path_key TEXT,
                status TEXT NOT NULL,
                confidence INTEGER,
                method TEXT,
                idempotency_key TEXT NOT NULL,
                phase INTEGER NOT NULL,
                sequence INTEGER NOT NULL DEFAULT 0,
                depends_on_action_id INTEGER REFERENCES run_actions(id),
                payload_json TEXT,
                created_at TEXT NOT NULL,
                applied_at TEXT,
                error_message TEXT
            );
        """)
        conn.execute("""
            INSERT INTO run_actions (
                proposed_by_run_id, action_type, entity_type, status,
                idempotency_key, phase, created_at
            )
            VALUES (1, 'move_file', 'file', 'proposed', 'k1', 90,
                    '2026-01-01T00:00:00+00:00')
        """)
        conn.commit()

        init_schema(conn)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(run_actions)")
        }
        row = conn.execute(
            "SELECT status, resolved_by_run_id, resolved_at, resolution_note "
            "FROM run_actions WHERE idempotency_key = 'k1'"
        ).fetchone()
    finally:
        conn.close()

    assert version == CURRENT_SCHEMA_VERSION
    assert {"resolved_by_run_id", "resolved_at", "resolution_note"} <= columns
    assert row == ("proposed", None, None, None)


def test_v5_db_migrates_to_camera_identity_metadata(tmp_path):
    db_path = tmp_path / "v5.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY);")
        conn.execute("INSERT INTO schema_version (version) VALUES (5);")
        _create_core_schema(conn)
        conn.execute("DROP TABLE media_metadata")
        conn.execute("""
            CREATE TABLE media_metadata (
                file_id INTEGER PRIMARY KEY,
                capture_datetime TEXT,
                camera_model TEXT,
                lens_model TEXT,
                width INTEGER,
                height INTEGER,
                duration_sec REAL,
                aspect_ratio REAL,
                phash TEXT,
                FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
            );
        """)
        schema_module._migration_3_catalog_state(conn)
        schema_module._migration_4_face_tables(conn)
        schema_module._migration_5_run_action_attempt_history(conn)
        conn.commit()

        init_schema(conn)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(media_metadata)")
        }
    finally:
        conn.close()

    assert version == CURRENT_SCHEMA_VERSION
    assert "camera_serial_number" in columns
    assert "camera_file_number" in columns
