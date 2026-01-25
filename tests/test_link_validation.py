"""Tests for link validation and consistency checking."""

import pytest
from pathlib import Path
from datetime import datetime

from photo_organizer.database.ops import DBOperations
from photo_organizer.sync.link_validation import LinkConsistencyChecker
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


def _create_raw_and_jpeg(db_ops, raw_name: str, jpeg_name: str, raw_dest: str = None, jpeg_dest: str = None):
    """Helper to create a linked RAW and JPEG pair."""
    dt = datetime(2024, 1, 15, 10, 30, 0)

    # Create RAW
    raw = FileRecord(
        hash=f"raw_hash_{raw_name}",
        type="raw",
        ext=".cr2",
        orig_name=raw_name,
        orig_path=Path(f"/src/{raw_name}"),
        size_bytes=10000,
        is_seed=True,
        name_score=50,
        capture_datetime=dt,
        camera_model="Canon EOS 5D",
    )
    raw_id = db_ops.upsert_file_record(raw)
    db_ops.upsert_media_metadata(raw_id, raw)
    if raw_dest:
        db_ops.update_dest_path(raw_id, raw_dest)

    # Create JPEG
    jpeg = FileRecord(
        hash=f"jpeg_hash_{jpeg_name}",
        type="jpeg",
        ext=".jpg",
        orig_name=jpeg_name,
        orig_path=Path(f"/src/{jpeg_name}"),
        size_bytes=500,
        is_seed=False,
        name_score=50,
        capture_datetime=dt,
        camera_model="Canon EOS 5D",
    )
    jpeg_id = db_ops.upsert_file_record(jpeg)
    db_ops.upsert_media_metadata(jpeg_id, jpeg)
    if jpeg_dest:
        db_ops.update_dest_path(jpeg_id, jpeg_dest)

    # Create link
    db_ops.conn.execute(
        "INSERT INTO raw_outputs (raw_file_id, output_file_id, link_method, confidence) VALUES (?, ?, ?, ?)",
        (raw_id, jpeg_id, "exact_stem_datetime", 95)
    )
    db_ops.conn.commit()

    return raw_id, jpeg_id


def test_consistent_link_exact_match(db_ops):
    """Verify that files with matching stems are detected as consistent."""
    _create_raw_and_jpeg(
        db_ops,
        raw_name="IMG_1234.CR2",
        jpeg_name="IMG_1234.jpg",
        raw_dest="/dest/raw/IMG_1234.CR2",
        jpeg_dest="/dest/output/IMG_1234.jpg"
    )

    checker = LinkConsistencyChecker(db_ops)
    report = checker.check_all_links()

    assert report.total_links == 1
    assert report.consistent_count == 1
    assert report.mismatch_count == 0


def test_consistent_link_with_edit_suffix(db_ops):
    """Verify that JPEG with _edit suffix is still considered consistent."""
    _create_raw_and_jpeg(
        db_ops,
        raw_name="IMG_1234.CR2",
        jpeg_name="IMG_1234_edit.jpg",
        raw_dest="/dest/raw/IMG_1234.CR2",
        jpeg_dest="/dest/output/IMG_1234_edit.jpg"
    )

    checker = LinkConsistencyChecker(db_ops)
    report = checker.check_all_links()

    assert report.total_links == 1
    assert report.consistent_count == 1
    assert report.mismatch_count == 0


def test_consistent_link_with_timestamp_suffix(db_ops):
    """Verify that organizer timestamp suffixes are ignored in matching."""
    _create_raw_and_jpeg(
        db_ops,
        raw_name="IMG_1234.CR2",
        jpeg_name="IMG_1234.jpg",
        raw_dest="/dest/raw/IMG_1234_2024-01-15_10-30-00.CR2",
        jpeg_dest="/dest/output/IMG_1234_2024-01-15_10-30-00.jpg"
    )

    checker = LinkConsistencyChecker(db_ops)
    report = checker.check_all_links()

    assert report.total_links == 1
    assert report.consistent_count == 1
    assert report.mismatch_count == 0


def test_mismatch_different_stems(db_ops):
    """Verify that files with different stems are detected as mismatched."""
    _create_raw_and_jpeg(
        db_ops,
        raw_name="Vacation_Beach.CR2",
        jpeg_name="IMG_1234.jpg",
        raw_dest="/dest/raw/Vacation_Beach.CR2",
        jpeg_dest="/dest/output/IMG_1234.jpg"
    )

    checker = LinkConsistencyChecker(db_ops)
    report = checker.check_all_links()

    assert report.total_links == 1
    assert report.consistent_count == 0
    assert report.mismatch_count == 1
    assert len(report.mismatches) == 1

    mismatch = report.mismatches[0]
    assert "Vacation_Beach" in mismatch.raw_stem
    assert "IMG_1234" in mismatch.output_stem


def test_mismatch_suggests_rename_jpeg(db_ops):
    """Verify suggestion to rename JPEG when RAW has descriptive name."""
    _create_raw_and_jpeg(
        db_ops,
        raw_name="Wedding_Portrait.CR2",
        jpeg_name="IMG_5678.jpg",
        raw_dest="/dest/raw/Wedding_Portrait.CR2",
        jpeg_dest="/dest/output/IMG_5678.jpg"
    )

    checker = LinkConsistencyChecker(db_ops)
    report = checker.check_all_links()

    assert report.mismatch_count == 1
    mismatch = report.mismatches[0]

    # Should suggest renaming JPEG to match RAW
    assert "Rename JPEG" in mismatch.suggestion
    assert "Wedding_Portrait" in mismatch.suggestion


def test_mismatch_suggests_rename_raw(db_ops):
    """Verify suggestion to rename RAW when JPEG has descriptive name."""
    _create_raw_and_jpeg(
        db_ops,
        raw_name="IMG_1234.CR2",
        jpeg_name="Birthday_Party.jpg",
        raw_dest="/dest/raw/IMG_1234.CR2",
        jpeg_dest="/dest/output/Birthday_Party.jpg"
    )

    checker = LinkConsistencyChecker(db_ops)
    report = checker.check_all_links()

    assert report.mismatch_count == 1
    mismatch = report.mismatches[0]

    # Should suggest renaming RAW to match JPEG
    assert "Rename RAW" in mismatch.suggestion
    assert "Birthday_Party" in mismatch.suggestion


def test_multiple_links_mixed_consistency(db_ops):
    """Verify handling of multiple links with some consistent and some not."""
    # Consistent pair
    _create_raw_and_jpeg(
        db_ops,
        raw_name="IMG_1111.CR2",
        jpeg_name="IMG_1111.jpg",
        raw_dest="/dest/raw/IMG_1111.CR2",
        jpeg_dest="/dest/output/IMG_1111.jpg"
    )

    # Mismatched pair
    _create_raw_and_jpeg(
        db_ops,
        raw_name="Sunset_Photo.CR2",
        jpeg_name="IMG_2222.jpg",
        raw_dest="/dest/raw/Sunset_Photo.CR2",
        jpeg_dest="/dest/output/IMG_2222.jpg"
    )

    checker = LinkConsistencyChecker(db_ops)
    report = checker.check_all_links()

    assert report.total_links == 2
    assert report.consistent_count == 1
    assert report.mismatch_count == 1


def test_csv_report_generation(db_ops, tmp_path):
    """Verify CSV report is written correctly."""
    _create_raw_and_jpeg(
        db_ops,
        raw_name="Landscape.CR2",
        jpeg_name="IMG_9999.jpg",
        raw_dest="/dest/raw/Landscape.CR2",
        jpeg_dest="/dest/output/IMG_9999.jpg"
    )

    checker = LinkConsistencyChecker(db_ops)
    report = checker.check_all_links()

    csv_path = tmp_path / "link_report.csv"
    checker.write_csv_report(str(csv_path), report)

    assert csv_path.exists()

    content = csv_path.read_text()
    assert "LINK VALIDATION SUMMARY" in content
    assert "NAMING MISMATCHES" in content
    assert "Landscape" in content
    assert "IMG_9999" in content


def test_get_linked_files_for_raw(db_ops):
    """Verify querying linked files for a specific RAW."""
    raw_id, jpeg_id = _create_raw_and_jpeg(
        db_ops,
        raw_name="Test.CR2",
        jpeg_name="Test.jpg",
        raw_dest="/dest/raw/Test.CR2",
        jpeg_dest="/dest/output/Test.jpg"
    )

    checker = LinkConsistencyChecker(db_ops)
    linked = checker.get_linked_files_for_raw(raw_id)

    assert len(linked) == 1
    assert linked[0][0] == jpeg_id


def test_get_linked_raw_for_output(db_ops):
    """Verify querying linked RAW for a specific output."""
    raw_id, jpeg_id = _create_raw_and_jpeg(
        db_ops,
        raw_name="Test.CR2",
        jpeg_name="Test.jpg",
        raw_dest="/dest/raw/Test.CR2",
        jpeg_dest="/dest/output/Test.jpg"
    )

    checker = LinkConsistencyChecker(db_ops)
    linked = checker.get_linked_raw_for_output(jpeg_id)

    assert linked is not None
    assert linked[0] == raw_id


def test_is_camera_name_detection(db_ops):
    """Verify camera name pattern detection."""
    checker = LinkConsistencyChecker(db_ops)

    # Should be detected as camera names
    assert checker._is_camera_name("IMG_1234") is True
    assert checker._is_camera_name("DSC_5678") is True
    assert checker._is_camera_name("DSCF1234") is True
    assert checker._is_camera_name("PXL_20240115") is True

    # Should NOT be detected as camera names
    assert checker._is_camera_name("Vacation_Photo") is False
    assert checker._is_camera_name("Wedding_Portrait") is False
    assert checker._is_camera_name("My_Cat") is False
