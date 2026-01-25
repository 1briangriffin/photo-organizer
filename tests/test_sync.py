"""Tests for path synchronization functionality."""

import pytest
from pathlib import Path
from datetime import datetime

from photo_organizer.database.ops import DBOperations
from photo_organizer.sync.path_sync import DestinationSyncer, SyncStatus
from photo_organizer.models import FileRecord


@pytest.fixture
def db_ops(tmp_path):
    """Create an in-memory database with schema for testing."""
    import sqlite3
    from photo_organizer.database.schema import init_schema

    conn = sqlite3.connect(":memory:")
    init_schema(conn)
    yield DBOperations(conn)
    conn.close()


def _create_test_file(tmp_path: Path, filename: str, content: bytes = b"test content") -> Path:
    """Create a test file with given content."""
    file_path = tmp_path / filename
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(content)
    return file_path


def test_detect_unchanged_file(db_ops, tmp_path):
    """Verify that files at expected locations are detected as unchanged."""
    # Create a file on disk
    dest_dir = tmp_path / "output" / "2024" / "2024-01"
    dest_dir.mkdir(parents=True)
    file_path = dest_dir / "IMG_1234.jpg"
    file_path.write_bytes(b"jpeg content here")

    # Add file to database with dest_path matching actual location
    jpeg = FileRecord(
        hash="abc123hash",
        type="jpeg",
        ext=".jpg",
        orig_name="IMG_1234.jpg",
        orig_path=Path("/src/IMG_1234.jpg"),
        size_bytes=17,
        is_seed=True,
        name_score=50,
        capture_datetime=datetime(2024, 1, 15, 10, 30, 0),
    )
    file_id = db_ops.upsert_file_record(jpeg)
    db_ops.update_dest_path(file_id, str(file_path))
    db_ops.conn.commit()

    # Run sync
    syncer = DestinationSyncer(db_ops)
    report = syncer.sync_destinations([tmp_path / "output"], dry_run=True)

    # File should be detected as unchanged
    assert report.unchanged_count == 1
    assert report.renamed_count == 0
    assert report.missing_count == 0


def test_detect_renamed_file_same_directory(db_ops, tmp_path):
    """Verify that renamed files in the same directory are detected and updated."""
    # Create a file on disk with NEW name
    dest_dir = tmp_path / "output" / "2024" / "2024-01"
    dest_dir.mkdir(parents=True)
    new_file_path = dest_dir / "Vacation_Beach.jpg"
    file_content = b"jpeg content for rename test"
    new_file_path.write_bytes(file_content)

    # Compute hash of the content (same as what syncer will compute)
    import hashlib
    content_hash = hashlib.sha256(file_content).hexdigest()

    # Add file to database with OLD name (simulating rename)
    jpeg = FileRecord(
        hash=content_hash,
        type="jpeg",
        ext=".jpg",
        orig_name="IMG_1234.jpg",
        orig_path=Path("/src/IMG_1234.jpg"),
        size_bytes=len(file_content),
        is_seed=True,
        name_score=50,
        capture_datetime=datetime(2024, 1, 15, 10, 30, 0),
    )
    file_id = db_ops.upsert_file_record(jpeg)
    old_path = str(dest_dir / "IMG_1234.jpg")
    db_ops.update_dest_path(file_id, old_path)
    db_ops.conn.commit()

    # Run sync (not dry run to test actual update)
    syncer = DestinationSyncer(db_ops)
    report = syncer.sync_destinations([tmp_path / "output"], dry_run=False)

    # File should be detected as renamed
    assert report.renamed_count == 1
    assert len(report.renamed_files) == 1
    assert report.renamed_files[0][0] == old_path
    assert report.renamed_files[0][1] == str(new_file_path)

    # Verify database was updated
    db_ops.conn.commit()
    cur = db_ops.conn.cursor()
    cur.execute("SELECT dest_path FROM files WHERE id = ?", (file_id,))
    updated_path = cur.fetchone()[0]
    assert updated_path == str(new_file_path)


def test_detect_missing_file(db_ops, tmp_path):
    """Verify that files in DB but not on disk are detected as missing."""
    # Create destination directory (but no file)
    dest_dir = tmp_path / "output" / "2024" / "2024-01"
    dest_dir.mkdir(parents=True)

    # Add file to database that doesn't exist on disk
    jpeg = FileRecord(
        hash="missinghash123",
        type="jpeg",
        ext=".jpg",
        orig_name="deleted_photo.jpg",
        orig_path=Path("/src/deleted_photo.jpg"),
        size_bytes=1000,
        is_seed=True,
        name_score=50,
        capture_datetime=datetime(2024, 1, 15, 10, 30, 0),
    )
    file_id = db_ops.upsert_file_record(jpeg)
    missing_path = str(dest_dir / "deleted_photo.jpg")
    db_ops.update_dest_path(file_id, missing_path)
    db_ops.conn.commit()

    # Run sync
    syncer = DestinationSyncer(db_ops)
    report = syncer.sync_destinations([tmp_path / "output"], dry_run=True)

    # File should be detected as missing
    assert report.missing_count == 1
    assert missing_path in report.missing_files


def test_detect_new_file_not_in_database(db_ops, tmp_path):
    """Verify that files on disk but not in DB are detected as new."""
    # Create a file on disk
    dest_dir = tmp_path / "output" / "2024" / "2024-01"
    dest_dir.mkdir(parents=True)
    file_path = dest_dir / "new_export.jpg"
    file_path.write_bytes(b"new jpeg content")

    # Don't add anything to database

    # Run sync
    syncer = DestinationSyncer(db_ops)
    report = syncer.sync_destinations([tmp_path / "output"], dry_run=True)

    # File should be detected as new
    assert report.new_count == 1
    assert str(file_path) in report.new_files


def test_dry_run_does_not_modify_database(db_ops, tmp_path):
    """Verify that dry run mode doesn't modify the database."""
    # Create a file on disk with NEW name
    dest_dir = tmp_path / "output" / "2024" / "2024-01"
    dest_dir.mkdir(parents=True)
    new_file_path = dest_dir / "Renamed_Photo.jpg"
    file_content = b"content for dry run test"
    new_file_path.write_bytes(file_content)

    # Compute hash
    import hashlib
    content_hash = hashlib.sha256(file_content).hexdigest()

    # Add file to database with OLD name
    jpeg = FileRecord(
        hash=content_hash,
        type="jpeg",
        ext=".jpg",
        orig_name="Original_Photo.jpg",
        orig_path=Path("/src/Original_Photo.jpg"),
        size_bytes=len(file_content),
        is_seed=True,
        name_score=50,
        capture_datetime=datetime(2024, 1, 15, 10, 30, 0),
    )
    file_id = db_ops.upsert_file_record(jpeg)
    old_path = str(dest_dir / "Original_Photo.jpg")
    db_ops.update_dest_path(file_id, old_path)
    db_ops.conn.commit()

    # Run sync with dry_run=True
    syncer = DestinationSyncer(db_ops)
    report = syncer.sync_destinations([tmp_path / "output"], dry_run=True)

    # Should detect rename
    assert report.renamed_count == 1

    # But database should NOT be updated
    cur = db_ops.conn.cursor()
    cur.execute("SELECT dest_path FROM files WHERE id = ?", (file_id,))
    current_path = cur.fetchone()[0]
    assert current_path == old_path  # Still the old path


def test_csv_output(db_ops, tmp_path):
    """Verify that CSV report is written correctly."""
    # Create files on disk
    dest_dir = tmp_path / "output" / "2024" / "2024-01"
    dest_dir.mkdir(parents=True)

    # Create a renamed file
    renamed_path = dest_dir / "Renamed.jpg"
    file_content = b"renamed file content"
    renamed_path.write_bytes(file_content)

    import hashlib
    content_hash = hashlib.sha256(file_content).hexdigest()

    jpeg = FileRecord(
        hash=content_hash,
        type="jpeg",
        ext=".jpg",
        orig_name="Original.jpg",
        orig_path=Path("/src/Original.jpg"),
        size_bytes=len(file_content),
        is_seed=True,
        name_score=50,
        capture_datetime=datetime(2024, 1, 15, 10, 30, 0),
    )
    file_id = db_ops.upsert_file_record(jpeg)
    db_ops.update_dest_path(file_id, str(dest_dir / "Original.jpg"))
    db_ops.conn.commit()

    # Run sync with CSV output
    csv_path = tmp_path / "sync_report.csv"
    syncer = DestinationSyncer(db_ops)
    report = syncer.sync_destinations([tmp_path / "output"], dry_run=True, csv_output=str(csv_path))

    # Verify CSV was created
    assert csv_path.exists()

    # Read and verify CSV content
    content = csv_path.read_text()
    assert "SYNC SUMMARY" in content
    assert "RENAMED FILES" in content
    assert "Original.jpg" in content
    assert "Renamed.jpg" in content


def test_moved_file_not_auto_updated(db_ops, tmp_path):
    """Verify that files moved to different directories are detected but not updated."""
    # Create file in a DIFFERENT directory than expected
    old_dir = tmp_path / "output" / "2024" / "2024-01"
    new_dir = tmp_path / "output" / "2024" / "2024-02"  # Different month folder
    new_dir.mkdir(parents=True)
    old_dir.mkdir(parents=True)

    moved_path = new_dir / "IMG_1234.jpg"
    file_content = b"moved file content"
    moved_path.write_bytes(file_content)

    import hashlib
    content_hash = hashlib.sha256(file_content).hexdigest()

    # Add file to database with path in OLD directory
    jpeg = FileRecord(
        hash=content_hash,
        type="jpeg",
        ext=".jpg",
        orig_name="IMG_1234.jpg",
        orig_path=Path("/src/IMG_1234.jpg"),
        size_bytes=len(file_content),
        is_seed=True,
        name_score=50,
        capture_datetime=datetime(2024, 1, 15, 10, 30, 0),
    )
    file_id = db_ops.upsert_file_record(jpeg)
    old_path = str(old_dir / "IMG_1234.jpg")
    db_ops.update_dest_path(file_id, old_path)
    db_ops.conn.commit()

    # Run sync (not dry run)
    syncer = DestinationSyncer(db_ops)
    report = syncer.sync_destinations([tmp_path / "output"], dry_run=False)

    # Should detect as MOVED, not RENAMED
    assert report.moved_count == 1
    assert report.renamed_count == 0
    assert len(report.moved_files) == 1

    # Database should NOT be updated (moves require manual intervention)
    cur = db_ops.conn.cursor()
    cur.execute("SELECT dest_path FROM files WHERE id = ?", (file_id,))
    current_path = cur.fetchone()[0]
    assert current_path == old_path  # Still the old path


def test_import_new_files(db_ops, tmp_path):
    """Verify that new files can be imported into the database."""
    # Create a new JPEG file on disk (not in database)
    dest_dir = tmp_path / "output" / "2024" / "2024-01"
    dest_dir.mkdir(parents=True)
    new_file = dest_dir / "new_export.jpg"
    # Create minimal valid JPEG content (JFIF header)
    jpeg_content = bytes([
        0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00,
        0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0xFF, 0xD9
    ])
    new_file.write_bytes(jpeg_content)

    # Run sync to detect new file
    syncer = DestinationSyncer(db_ops)
    report = syncer.sync_destinations([tmp_path / "output"], dry_run=True)

    # Should detect as new
    assert report.new_count == 1
    assert str(new_file) in report.new_files

    # Now import the new file
    syncer.import_new_files(report.new_files, report, dry_run=False)
    db_ops.conn.commit()

    # Verify file was imported
    assert report.imported_count == 1
    assert str(new_file) in report.imported_files

    # Verify database has the file
    cur = db_ops.conn.cursor()
    cur.execute("SELECT id, type, dest_path FROM files WHERE dest_path = ?", (str(new_file),))
    row = cur.fetchone()
    assert row is not None
    assert row[1] == "jpeg"
    assert row[2] == str(new_file)


def test_import_new_files_dry_run(db_ops, tmp_path):
    """Verify that dry run mode doesn't import files."""
    # Create a new file on disk
    dest_dir = tmp_path / "output" / "2024" / "2024-01"
    dest_dir.mkdir(parents=True)
    new_file = dest_dir / "test_photo.jpg"
    new_file.write_bytes(b"test jpeg content for dry run")

    # Run sync to detect new file
    syncer = DestinationSyncer(db_ops)
    report = syncer.sync_destinations([tmp_path / "output"], dry_run=True)
    assert report.new_count == 1

    # Import with dry_run=True
    syncer.import_new_files(report.new_files, report, dry_run=True)

    # Should track as imported in report
    assert report.imported_count == 1

    # But database should NOT have the file
    cur = db_ops.conn.cursor()
    cur.execute("SELECT COUNT(*) FROM files WHERE dest_path = ?", (str(new_file),))
    count = cur.fetchone()[0]
    assert count == 0


def test_import_sets_dest_path(db_ops, tmp_path):
    """Verify that imported files have dest_path set to current location."""
    # Create a new file
    dest_dir = tmp_path / "output" / "2024" / "2024-01"
    dest_dir.mkdir(parents=True)
    new_file = dest_dir / "already_organized.jpg"
    new_file.write_bytes(b"already in destination location")

    # Detect and import
    syncer = DestinationSyncer(db_ops)
    report = syncer.sync_destinations([tmp_path / "output"], dry_run=True)
    syncer.import_new_files(report.new_files, report, dry_run=False)
    db_ops.conn.commit()

    # Verify dest_path is set
    cur = db_ops.conn.cursor()
    cur.execute("SELECT dest_path FROM files WHERE dest_path = ?", (str(new_file),))
    row = cur.fetchone()
    assert row is not None
    assert row[0] == str(new_file)


def test_import_multiple_file_types(db_ops, tmp_path):
    """Verify that different file types are imported correctly."""
    dest_dir = tmp_path / "output" / "2024" / "2024-01"
    dest_dir.mkdir(parents=True)

    # Create files of different types
    jpeg_file = dest_dir / "photo.jpg"
    jpeg_file.write_bytes(b"jpeg content")

    # Create a CR2 file (RAW)
    raw_file = dest_dir / "photo.cr2"
    raw_file.write_bytes(b"raw content here")

    # Run sync and import
    syncer = DestinationSyncer(db_ops)
    report = syncer.sync_destinations([tmp_path / "output"], dry_run=True)
    assert report.new_count == 2

    syncer.import_new_files(report.new_files, report, dry_run=False)
    db_ops.conn.commit()

    assert report.imported_count == 2

    # Verify types
    cur = db_ops.conn.cursor()
    cur.execute("SELECT type FROM files WHERE dest_path = ?", (str(jpeg_file),))
    assert cur.fetchone()[0] == "jpeg"

    cur.execute("SELECT type FROM files WHERE dest_path = ?", (str(raw_file),))
    assert cur.fetchone()[0] == "raw"


def test_import_csv_includes_imported_files(db_ops, tmp_path):
    """Verify that CSV report includes imported files section."""
    dest_dir = tmp_path / "output" / "2024" / "2024-01"
    dest_dir.mkdir(parents=True)
    new_file = dest_dir / "imported_photo.jpg"
    new_file.write_bytes(b"content for csv test")

    # Detect, import, and write CSV
    syncer = DestinationSyncer(db_ops)
    report = syncer.sync_destinations([tmp_path / "output"], dry_run=True)
    syncer.import_new_files(report.new_files, report, dry_run=False)

    csv_path = tmp_path / "import_report.csv"
    syncer._write_csv_report(str(csv_path), report)

    # Verify CSV content
    content = csv_path.read_text()
    assert "IMPORTED FILES" in content
    assert "imported_photo.jpg" in content
