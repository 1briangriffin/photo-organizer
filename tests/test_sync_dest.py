"""
Tests for PhotoOrganizerApp.sync_dest() (--sync-dest).

Verifies end-to-end behaviour: rename detection, optional new-file import,
dry-run preservation, and that no source scan is required.
"""
import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from photo_organizer.core import PhotoOrganizerApp
from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.models import FileRecord


def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    conn.close()
    return db_path


def _seed_file(db_path: Path, content: bytes, dest_path: str) -> int:
    """Insert a file record with a known dest_path."""
    conn = sqlite3.connect(str(db_path))
    ops = DBOperations(conn)
    h = hashlib.sha256(content).hexdigest()
    rec = FileRecord(
        hash=h, sparse_hash=None, hash_is_sparse=False,
        type="jpeg", ext=".jpg",
        orig_name=Path(dest_path).name,
        orig_path=Path(dest_path),
        size_bytes=len(content), mtime=0.0,
        is_seed=False, name_score=1,
        capture_datetime=datetime(2023, 6, 1),
        camera_model=None, lens_model=None, duration_sec=None,
    )
    fid = ops.upsert_file_record(rec)
    ops.update_dest_path(fid, dest_path)
    conn.commit()
    conn.close()
    return fid


def _start_run(conn: sqlite3.Connection, *, dry_run: bool) -> int:
    cur = conn.execute(
        """
        INSERT INTO command_runs (
            tool, command, started_at, exit_status, dry_run, argv_json
        )
        VALUES ('photo-organizer', 'sync-dest', '2026-01-01T00:00:00Z',
                'running', ?, '[]')
        """,
        (1 if dry_run else 0,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def test_sync_dest_detects_rename(tmp_path):
    """A file renamed in dest has its dest_path updated in the catalog."""
    dest_dir = tmp_path / "dest" / "2023" / "2023-06"
    dest_dir.mkdir(parents=True)

    content = b"photo data" * 100
    old_path = str(dest_dir / "IMG_0001.jpg")
    new_path = dest_dir / "Vacation_001.jpg"
    new_path.write_bytes(content)

    db_path = _make_db(tmp_path)
    _seed_file(db_path, content, old_path)

    app = PhotoOrganizerApp(db_path)
    app.sync_dest(dest_root=tmp_path / "dest", dry_run=False)

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT dest_path FROM files")
    result = cur.fetchone()[0]
    conn.close()

    assert result == str(new_path)


def test_sync_dest_dry_run_does_not_update_db(tmp_path):
    """dry_run=True: rename is detected but catalog is unchanged."""
    dest_dir = tmp_path / "dest" / "2023" / "2023-06"
    dest_dir.mkdir(parents=True)

    content = b"photo data" * 100
    old_path = str(dest_dir / "IMG_0001.jpg")
    new_path = dest_dir / "Vacation_001.jpg"
    new_path.write_bytes(content)

    db_path = _make_db(tmp_path)
    _seed_file(db_path, content, old_path)

    app = PhotoOrganizerApp(db_path)
    app.sync_dest(dest_root=tmp_path / "dest", dry_run=True)

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT dest_path FROM files")
    result = cur.fetchone()[0]
    conn.close()

    assert result == old_path, "dry_run must not update dest_path"


def test_sync_dest_dry_run_records_canonical_path_proposal(tmp_path):
    dest_dir = tmp_path / "dest" / "2023" / "2023-06"
    dest_dir.mkdir(parents=True)

    content = b"photo data" * 100
    old_path = str(dest_dir / "IMG_0001.jpg")
    new_path = dest_dir / "Vacation_001.jpg"
    new_path.write_bytes(content)

    db_path = _make_db(tmp_path)
    file_id = _seed_file(db_path, content, old_path)

    conn = sqlite3.connect(str(db_path))
    try:
        run_id = _start_run(conn, dry_run=True)
        conn.commit()
    finally:
        conn.close()

    PhotoOrganizerApp(db_path).sync_dest(
        dest_root=tmp_path / "dest",
        dry_run=True,
        run_id=run_id,
    )

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(
            """
            SELECT action_type, status, source_path, target_path
            FROM run_actions
            WHERE proposed_by_run_id = ?
            """,
            (run_id,),
        )
        actions = cur.fetchall()
        cur = conn.execute("SELECT dest_path FROM files WHERE id = ?", (file_id,))
        stored_path = cur.fetchone()[0]
    finally:
        conn.close()

    assert ("update_canonical_dest_path", "proposed", old_path, str(new_path)) in actions
    assert stored_path == old_path


def test_sync_dest_real_run_records_applied_canonical_path_action(tmp_path):
    dest_dir = tmp_path / "dest" / "2023" / "2023-06"
    dest_dir.mkdir(parents=True)

    content = b"photo data" * 100
    old_path = str(dest_dir / "IMG_0001.jpg")
    new_path = dest_dir / "Vacation_001.jpg"
    new_path.write_bytes(content)

    db_path = _make_db(tmp_path)
    file_id = _seed_file(db_path, content, old_path)

    conn = sqlite3.connect(str(db_path))
    try:
        run_id = _start_run(conn, dry_run=False)
        conn.commit()
    finally:
        conn.close()

    PhotoOrganizerApp(db_path).sync_dest(
        dest_root=tmp_path / "dest",
        dry_run=False,
        run_id=run_id,
    )

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(
            """
            SELECT action_type, status, applied_by_run_id
            FROM run_actions
            WHERE proposed_by_run_id = ?
            """,
            (run_id,),
        )
        actions = cur.fetchall()
        cur = conn.execute("SELECT dest_path FROM files WHERE id = ?", (file_id,))
        stored_path = cur.fetchone()[0]
    finally:
        conn.close()

    assert ("update_canonical_dest_path", "applied", run_id) in actions
    assert stored_path == str(new_path)


def test_sync_dest_import_new_sets_dest_path(tmp_path):
    """--import-new: a new file in dest is imported with dest_path set to its location."""
    dest_dir = tmp_path / "dest" / "2023" / "2023-06"
    dest_dir.mkdir(parents=True)

    new_file = dest_dir / "new_photo.jpg"
    new_file.write_bytes(b"brand new photo" * 200)

    db_path = _make_db(tmp_path)

    app = PhotoOrganizerApp(db_path)
    app.sync_dest(dest_root=tmp_path / "dest", dry_run=False, import_new=True)

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT dest_path FROM files")
    row = cur.fetchone()
    conn.close()

    assert row is not None
    assert row[0] == str(new_file)


def test_sync_dest_without_import_new_does_not_import(tmp_path):
    """Without --import-new, new files in dest are reported but not imported."""
    dest_dir = tmp_path / "dest" / "2023" / "2023-06"
    dest_dir.mkdir(parents=True)
    (dest_dir / "new_photo.jpg").write_bytes(b"new" * 200)

    db_path = _make_db(tmp_path)

    app = PhotoOrganizerApp(db_path)
    app.sync_dest(dest_root=tmp_path / "dest", dry_run=False, import_new=False)

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM files")
    count = cur.fetchone()[0]
    conn.close()

    assert count == 0, "files must not be imported when import_new=False"


def test_sync_dest_does_not_require_src(tmp_path):
    """sync_dest runs successfully with no source directory involved."""
    dest_dir = tmp_path / "dest"
    dest_dir.mkdir()
    db_path = _make_db(tmp_path)

    app = PhotoOrganizerApp(db_path)
    # Should not raise — no src argument
    app.sync_dest(dest_root=dest_dir, dry_run=True)
