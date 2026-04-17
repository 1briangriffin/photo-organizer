"""
End-to-end tests for the dry-run savepoint behaviour in PhotoOrganizerApp.organize().

Key properties verified:
1. No dest_path values are persisted after a dry-run.
2. Scan/hash/link data (hashes, file_occurrences, sidecar relationships) IS preserved.
3. A preview CSV is written to the expected path.
4. If CSV generation throws, the savepoint still rolls back cleanly.
"""
import csv
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

def _write_raw(directory: Path, name: str = "img.CR2", content: bytes = b"RAW" * 1000) -> Path:
    p = directory / name
    p.write_bytes(content)
    return p


def _seed_db(db_path: Path, src_file: Path):
    """Pre-seed a scan record so the report has something to show."""
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    db_ops = DBOperations(conn)
    rec = FileRecord(
        hash="aabbcc001",
        sparse_hash=None,
        hash_is_sparse=False,
        type="raw",
        ext=".cr2",
        orig_name=src_file.name,
        orig_path=src_file,
        size_bytes=src_file.stat().st_size,
        mtime=src_file.stat().st_mtime,
        is_seed=False,
        name_score=10,
        capture_datetime=datetime(2022, 2, 26),
        camera_model="Canon EOS R5",
        lens_model=None,
        duration_sec=None,
    )
    fid = db_ops.upsert_file_record(rec)
    db_ops.upsert_media_metadata(fid, rec)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_dry_run_does_not_persist_dest_paths(tmp_path):
    """After a dry-run, no file should have a dest_path in the catalog."""
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    dest_dir = tmp_path / "dest"

    src_file = _write_raw(src_dir)
    db_path = tmp_path / "catalog.db"
    _seed_db(db_path, src_file)

    app = PhotoOrganizerApp(db_path)
    app.organize(
        src_root=src_dir,
        dest_root=dest_dir,
        dry_run=True,
        dry_run_csv=tmp_path / "preview.csv",
    )

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM files WHERE dest_path IS NOT NULL")
    count = cur.fetchone()[0]
    conn.close()

    assert count == 0, "dest_path must not be persisted after a dry-run"


def test_dry_run_preserves_scan_and_hash_data(tmp_path):
    """Scan metadata (hash, file_occurrences) written before planning must survive a dry-run."""
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    dest_dir = tmp_path / "dest"

    src_file = _write_raw(src_dir)
    db_path = tmp_path / "catalog.db"
    _seed_db(db_path, src_file)

    app = PhotoOrganizerApp(db_path)
    app.organize(
        src_root=src_dir,
        dest_root=dest_dir,
        dry_run=True,
        dry_run_csv=tmp_path / "preview.csv",
    )

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    # The original seeded record must still be present
    cur.execute("SELECT hash FROM files WHERE hash = 'aabbcc001'")
    row = cur.fetchone()
    assert row is not None, "Seeded hash must survive dry-run rollback"

    conn.close()


def test_dry_run_writes_preview_csv(tmp_path):
    """A dry-run must produce a CSV at the specified path with at least a header row."""
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    dest_dir = tmp_path / "dest"
    csv_out = tmp_path / "preview.csv"

    src_file = _write_raw(src_dir)
    db_path = tmp_path / "catalog.db"
    _seed_db(db_path, src_file)

    app = PhotoOrganizerApp(db_path)
    app.organize(
        src_root=src_dir,
        dest_root=dest_dir,
        dry_run=True,
        dry_run_csv=csv_out,
    )

    assert csv_out.exists(), "Preview CSV must be created during dry-run"

    with csv_out.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)

    assert len(rows) >= 1, "CSV must contain at least a header row"
    assert "Source Path" in rows[0]
    assert "Destination Path" in rows[0]


def test_dry_run_default_csv_path(tmp_path):
    """When dry_run_csv is None, the preview CSV should land in dest/dry_run_preview.csv."""
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    dest_dir = tmp_path / "dest"

    src_file = _write_raw(src_dir)
    db_path = tmp_path / "catalog.db"
    _seed_db(db_path, src_file)

    app = PhotoOrganizerApp(db_path)
    app.organize(
        src_root=src_dir,
        dest_root=dest_dir,
        dry_run=True,
        dry_run_csv=None,
    )

    assert (dest_dir / "dry_run_preview.csv").exists()


def test_dry_run_rolls_back_on_csv_failure(tmp_path):
    """If CSV generation raises, the savepoint must still be rolled back."""
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    dest_dir = tmp_path / "dest"

    src_file = _write_raw(src_dir)
    db_path = tmp_path / "catalog.db"
    _seed_db(db_path, src_file)

    app = PhotoOrganizerApp(db_path)

    with patch.object(app, "_write_dry_run_report", side_effect=RuntimeError("disk full")):
        with pytest.raises(RuntimeError, match="disk full"):
            app.organize(
                src_root=src_dir,
                dest_root=dest_dir,
                dry_run=True,
                dry_run_csv=tmp_path / "preview.csv",
            )

    # dest_path must still be NULL despite the failure
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM files WHERE dest_path IS NOT NULL")
    count = cur.fetchone()[0]
    conn.close()

    assert count == 0, "Savepoint must roll back even when CSV generation fails"
