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

from photo_organizer.database.schema import CURRENT_SCHEMA_VERSION, init_schema


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

    # Verify: schema_version row bumped and command_runs exists.
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
