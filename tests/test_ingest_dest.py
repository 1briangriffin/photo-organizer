"""
Tests for PhotoOrganizerApp.ingest_dest() (--ingest-dest).

Covers:
- XMP sidecar co-located with organized RAW is linked and stays in place
- JPEG exported next to organized RAW is re-routed to dest/output
- Dry-run: dest_path not persisted; import/link data is kept
- No new files: returns early without error
- Savepoint rollback on planning failure
"""
import hashlib
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from photo_organizer.core import PhotoOrganizerApp
from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.models import FileRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    conn.close()
    return db_path


def _seed_organized_raw(db_path: Path, raw_file: Path) -> int:
    """Insert an already-organized RAW with dest_path set to its current location."""
    conn = sqlite3.connect(str(db_path))
    ops = DBOperations(conn)
    content = raw_file.read_bytes()
    h = hashlib.sha256(content).hexdigest()
    rec = FileRecord(
        hash=h, sparse_hash=None, hash_is_sparse=False,
        type="raw", ext=raw_file.suffix.lower(),
        orig_name=raw_file.name,
        orig_path=raw_file,
        size_bytes=len(content), mtime=raw_file.stat().st_mtime,
        is_seed=False, name_score=1,
        capture_datetime=datetime(2023, 6, 15, 10, 0, 0),
        camera_model="Canon EOS R5", lens_model=None, duration_sec=None,
    )
    fid = ops.upsert_file_record(rec)
    ops.upsert_media_metadata(fid, rec)
    ops.update_dest_path(fid, str(raw_file))
    conn.commit()
    conn.close()
    return fid


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_no_new_files_returns_cleanly(tmp_path):
    """ingest_dest with no new files in dest exits without error."""
    dest = tmp_path / "dest"
    dest.mkdir()
    db_path = _make_db(tmp_path)

    app = PhotoOrganizerApp(db_path)
    app.ingest_dest(dest_root=dest, dry_run=False)  # should not raise


def test_xmp_sidecar_linked_to_organized_raw(tmp_path):
    """XMP written next to an organized RAW is linked in raw_sidecars."""
    raw_dir = tmp_path / "dest" / "raw" / "2023" / "2023-06"
    raw_dir.mkdir(parents=True)

    raw_file = raw_dir / "IMG_0042.CR2"
    raw_file.write_bytes(b"RAW" * 2000)

    xmp_file = raw_dir / "IMG_0042.xmp"
    xmp_file.write_bytes(b"<xmp>edit data</xmp>")

    db_path = _make_db(tmp_path)
    _seed_organized_raw(db_path, raw_file)

    app = PhotoOrganizerApp(db_path)
    app.ingest_dest(dest_root=tmp_path / "dest", dry_run=False)

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM raw_sidecars rs
        JOIN files r ON rs.raw_file_id = r.id
        JOIN files s ON rs.sidecar_file_id = s.id
        WHERE r.orig_name = 'IMG_0042.CR2' AND s.orig_name = 'IMG_0042.xmp'
    """)
    count = cur.fetchone()[0]
    conn.close()

    assert count == 1, "XMP must be linked to its RAW via raw_sidecars"


def test_jpeg_exported_to_raw_folder_rerouted_to_output(tmp_path):
    """JPEG exported next to organized RAW is moved to dest/output, not left in dest/raw."""
    raw_dir = tmp_path / "dest" / "raw" / "2023" / "2023-06"
    raw_dir.mkdir(parents=True)

    raw_file = raw_dir / "IMG_0042.CR2"
    raw_file.write_bytes(b"RAW" * 2000)

    # DPP exported JPEG landed in dest/raw — wrong place
    jpeg_file = raw_dir / "IMG_0042.jpg"
    jpeg_file.write_bytes(b"JPEG" * 500)

    db_path = _make_db(tmp_path)
    _seed_organized_raw(db_path, raw_file)

    app = PhotoOrganizerApp(db_path)
    app.ingest_dest(dest_root=tmp_path / "dest", move=True, dry_run=False)

    # JPEG must have moved out of dest/raw
    assert not jpeg_file.exists(), "JPEG must be moved out of dest/raw"

    # JPEG must now be somewhere under dest/output
    output_jpegs = list((tmp_path / "dest" / "output").rglob("*.jpg"))
    assert len(output_jpegs) == 1, "JPEG must land under dest/output"


def test_dry_run_does_not_persist_dest_paths(tmp_path):
    """dry_run=True: planning savepoint is rolled back, no dest_path values persist."""
    raw_dir = tmp_path / "dest" / "raw" / "2023" / "2023-06"
    raw_dir.mkdir(parents=True)

    raw_file = raw_dir / "IMG_0042.CR2"
    raw_file.write_bytes(b"RAW" * 2000)

    jpeg_file = raw_dir / "IMG_0042.jpg"
    jpeg_file.write_bytes(b"JPEG" * 500)

    db_path = _make_db(tmp_path)
    _seed_organized_raw(db_path, raw_file)

    app = PhotoOrganizerApp(db_path)
    app.ingest_dest(dest_root=tmp_path / "dest", dry_run=True)

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM files WHERE dest_path IS NOT NULL AND type != 'raw'")
    count = cur.fetchone()[0]
    conn.close()

    assert count == 0, "dry_run must not persist dest_path for newly ingested files"
    assert jpeg_file.exists(), "dry_run must not move files"


def test_dry_run_preserves_import_and_link_data(tmp_path):
    """dry_run=True: imported file records and sidecar links survive the planning rollback."""
    raw_dir = tmp_path / "dest" / "raw" / "2023" / "2023-06"
    raw_dir.mkdir(parents=True)

    raw_file = raw_dir / "IMG_0042.CR2"
    raw_file.write_bytes(b"RAW" * 2000)

    xmp_file = raw_dir / "IMG_0042.xmp"
    xmp_file.write_bytes(b"<xmp/>")

    db_path = _make_db(tmp_path)
    _seed_organized_raw(db_path, raw_file)

    app = PhotoOrganizerApp(db_path)
    app.ingest_dest(dest_root=tmp_path / "dest", dry_run=True)

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM files WHERE type = 'sidecar'")
    assert cur.fetchone()[0] == 1, "imported sidecar record must survive dry-run rollback"

    cur.execute("SELECT COUNT(*) FROM raw_sidecars")
    assert cur.fetchone()[0] == 1, "sidecar link must survive dry-run rollback"
    conn.close()


def test_planning_failure_rolls_back_savepoint(tmp_path):
    """If planning raises, the savepoint is rolled back and no dest_paths persist."""
    raw_dir = tmp_path / "dest" / "raw" / "2023" / "2023-06"
    raw_dir.mkdir(parents=True)

    raw_file = raw_dir / "IMG_0042.CR2"
    raw_file.write_bytes(b"RAW" * 2000)

    jpeg_file = raw_dir / "IMG_0042.jpg"
    jpeg_file.write_bytes(b"JPEG" * 500)

    db_path = _make_db(tmp_path)
    _seed_organized_raw(db_path, raw_file)

    app = PhotoOrganizerApp(db_path)
    with patch(
        "photo_organizer.organization.rules.DestinationPlanner.plan_all",
        side_effect=RuntimeError("disk full"),
    ):
        with pytest.raises(RuntimeError, match="disk full"):
            app.ingest_dest(dest_root=tmp_path / "dest", dry_run=False)

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM files WHERE dest_path IS NOT NULL AND type != 'raw'")
    count = cur.fetchone()[0]
    conn.close()

    assert count == 0, "savepoint must roll back on planning failure"
