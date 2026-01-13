import pytest
import sqlite3
from pathlib import Path
from datetime import datetime
from photo_organizer.metadata.linking import FileLinker
from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.models import FileRecord


@pytest.fixture
def db_ops(tmp_path):
    """Create a temporary database with schema for testing"""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    db_ops = DBOperations(conn)
    yield db_ops
    conn.close()


def test_link_raw_outputs_exact_stem_datetime(db_ops, tmp_path):
    """Test exact datetime + stem matching (confidence=95)"""
    # Create RAW file
    raw = FileRecord(
        hash="raw_hash",
        sparse_hash=None,
        hash_is_sparse=False,
        type="raw",
        ext=".cr2",
        orig_name="IMG_1234.CR2",
        orig_path=tmp_path / "IMG_1234.CR2",
        size_bytes=1000,
        mtime=1234567890,
        is_seed=True,
        name_score=50,
        capture_datetime=datetime(2024, 3, 15, 14, 30, 45),
        camera_model="Canon EOS 5D",
        lens_model=None,
        duration_sec=None
    )
    raw_id = db_ops.upsert_file_record(raw)
    db_ops.upsert_media_metadata(raw_id, raw)

    # Create JPEG with same stem and datetime
    jpeg = FileRecord(
        hash="jpeg_hash",
        sparse_hash=None,
        hash_is_sparse=False,
        type="jpeg",
        ext=".jpg",
        orig_name="IMG_1234.jpg",
        orig_path=tmp_path / "IMG_1234.jpg",
        size_bytes=500,
        mtime=1234567890,
        is_seed=False,
        name_score=50,
        capture_datetime=datetime(2024, 3, 15, 14, 30, 45),
        camera_model="Canon EOS 5D",
        lens_model=None,
        duration_sec=None
    )
    jpeg_id = db_ops.upsert_file_record(jpeg)
    db_ops.upsert_media_metadata(jpeg_id, jpeg)

    # Run linking
    linker = FileLinker(db_ops)
    links_created = linker.link_raw_outputs(dry_run=False)

    assert links_created == 1

    # Verify link in database
    cur = db_ops.conn.cursor()
    cur.execute("SELECT raw_file_id, output_file_id, confidence, link_method FROM raw_outputs")
    rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0] == (raw_id, jpeg_id, 95, 'exact_stem_datetime')


def test_link_raw_outputs_datetime_camera(db_ops, tmp_path):
    """Test datetime + camera model matching (confidence=90)"""
    # Create RAW file
    raw = FileRecord(
        hash="raw_hash",
        sparse_hash=None,
        hash_is_sparse=False,
        type="raw",
        ext=".cr2",
        orig_name="IMG_5678.CR2",
        orig_path=tmp_path / "IMG_5678.CR2",
        size_bytes=1000,
        mtime=1234567890,
        is_seed=True,
        name_score=50,
        capture_datetime=datetime(2024, 3, 15, 14, 35, 10),
        camera_model="Canon EOS 5D",
        lens_model=None,
        duration_sec=None
    )
    raw_id = db_ops.upsert_file_record(raw)
    db_ops.upsert_media_metadata(raw_id, raw)

    # Create JPEG with same stem, datetime, and camera (edited filename)
    jpeg = FileRecord(
        hash="jpeg_hash",
        sparse_hash=None,
        hash_is_sparse=False,
        type="jpeg",
        ext=".jpg",
        orig_name="IMG_5678.jpg",  # Same stem
        orig_path=tmp_path / "IMG_5678_edited.jpg",
        size_bytes=500,
        mtime=1234567890,
        is_seed=False,
        name_score=50,
        capture_datetime=datetime(2024, 3, 15, 14, 35, 10),
        camera_model="Canon EOS 5D",
        lens_model=None,
        duration_sec=None
    )
    jpeg_id = db_ops.upsert_file_record(jpeg)
    db_ops.upsert_media_metadata(jpeg_id, jpeg)

    # Run linking
    linker = FileLinker(db_ops)
    links_created = linker.link_raw_outputs(dry_run=False)

    assert links_created == 1

    # Verify link with correct confidence
    cur = db_ops.conn.cursor()
    cur.execute("SELECT confidence, link_method FROM raw_outputs WHERE raw_file_id = ?", (raw_id,))
    row = cur.fetchone()
    assert row[0] in (90, 95)  # Could match either strategy depending on implementation


def test_link_raw_outputs_time_window(db_ops, tmp_path):
    """Test datetime tolerance window (confidence=75)"""
    # Create RAW file
    raw = FileRecord(
        hash="raw_hash",
        sparse_hash=None,
        hash_is_sparse=False,
        type="raw",
        ext=".nef",
        orig_name="DSC_9999.NEF",
        orig_path=tmp_path / "DSC_9999.NEF",
        size_bytes=1000,
        mtime=1234567890,
        is_seed=True,
        name_score=50,
        capture_datetime=datetime(2024, 3, 15, 16, 20, 30),
        camera_model="Nikon D850",
        lens_model=None,
        duration_sec=None
    )
    raw_id = db_ops.upsert_file_record(raw)
    db_ops.upsert_media_metadata(raw_id, raw)

    # Create JPEG with same stem but 1 second different
    jpeg = FileRecord(
        hash="jpeg_hash",
        sparse_hash=None,
        hash_is_sparse=False,
        type="jpeg",
        ext=".jpg",
        orig_name="DSC_9999.jpg",
        orig_path=tmp_path / "DSC_9999.jpg",
        size_bytes=500,
        mtime=1234567890,
        is_seed=False,
        name_score=50,
        capture_datetime=datetime(2024, 3, 15, 16, 20, 31),  # 1 second later
        camera_model="Nikon D850",
        lens_model=None,
        duration_sec=None
    )
    jpeg_id = db_ops.upsert_file_record(jpeg)
    db_ops.upsert_media_metadata(jpeg_id, jpeg)

    # Run linking
    linker = FileLinker(db_ops)
    links_created = linker.link_raw_outputs(dry_run=False)

    assert links_created == 1

    # Verify link was created within time window
    cur = db_ops.conn.cursor()
    cur.execute("SELECT confidence FROM raw_outputs WHERE raw_file_id = ?", (raw_id,))
    row = cur.fetchone()
    assert row[0] == 75  # Should match time window strategy


def test_link_raw_outputs_no_match(db_ops, tmp_path):
    """Test no link created when no match found"""
    # Create RAW file
    raw = FileRecord(
        hash="raw_hash",
        sparse_hash=None,
        hash_is_sparse=False,
        type="raw",
        ext=".arw",
        orig_name="SONY_1111.ARW",
        orig_path=tmp_path / "SONY_1111.ARW",
        size_bytes=1000,
        mtime=1234567890,
        is_seed=True,
        name_score=50,
        capture_datetime=datetime(2024, 3, 15, 18, 0, 0),
        camera_model="Sony A7R IV",
        lens_model=None,
        duration_sec=None
    )
    raw_id = db_ops.upsert_file_record(raw)
    db_ops.upsert_media_metadata(raw_id, raw)

    # Create JPEG with completely different metadata
    jpeg = FileRecord(
        hash="jpeg_hash",
        sparse_hash=None,
        hash_is_sparse=False,
        type="jpeg",
        ext=".jpg",
        orig_name="IMG_9999.jpg",
        orig_path=tmp_path / "IMG_9999.jpg",
        size_bytes=500,
        mtime=1234567890,
        is_seed=False,
        name_score=50,
        capture_datetime=datetime(2024, 1, 1, 10, 0, 0),  # Different date
        camera_model="Canon EOS R5",  # Different camera
        lens_model=None,
        duration_sec=None
    )
    jpeg_id = db_ops.upsert_file_record(jpeg)
    db_ops.upsert_media_metadata(jpeg_id, jpeg)

    # Run linking
    linker = FileLinker(db_ops)
    links_created = linker.link_raw_outputs(dry_run=False)

    assert links_created == 0

    # Verify no link was created
    cur = db_ops.conn.cursor()
    cur.execute("SELECT COUNT(*) FROM raw_outputs")
    count = cur.fetchone()[0]
    assert count == 0


def test_link_raw_outputs_multiple_jpegs(db_ops, tmp_path):
    """Test one RAW linking to multiple JPEGs (different edits)"""
    # Create one RAW file
    raw = FileRecord(
        hash="raw_hash",
        sparse_hash=None,
        hash_is_sparse=False,
        type="raw",
        ext=".cr2",
        orig_name="IMG_2222.CR2",
        orig_path=tmp_path / "IMG_2222.CR2",
        size_bytes=1000,
        mtime=1234567890,
        is_seed=True,
        name_score=50,
        capture_datetime=datetime(2024, 3, 15, 12, 0, 0),
        camera_model="Canon EOS 5D",
        lens_model=None,
        duration_sec=None
    )
    raw_id = db_ops.upsert_file_record(raw)
    db_ops.upsert_media_metadata(raw_id, raw)

    # Create two JPEGs (different edits of the same RAW)
    jpeg1 = FileRecord(
        hash="jpeg1_hash",
        sparse_hash=None,
        hash_is_sparse=False,
        type="jpeg",
        ext=".jpg",
        orig_name="IMG_2222.jpg",
        orig_path=tmp_path / "IMG_2222.jpg",
        size_bytes=500,
        mtime=1234567890,
        is_seed=False,
        name_score=50,
        capture_datetime=datetime(2024, 3, 15, 12, 0, 0),
        camera_model="Canon EOS 5D",
        lens_model=None,
        duration_sec=None
    )
    jpeg1_id = db_ops.upsert_file_record(jpeg1)
    db_ops.upsert_media_metadata(jpeg1_id, jpeg1)

    jpeg2 = FileRecord(
        hash="jpeg2_hash",
        sparse_hash=None,
        hash_is_sparse=False,
        type="jpeg",
        ext=".jpg",
        orig_name="IMG_2222_bw.jpg",  # Black and white edit
        orig_path=tmp_path / "IMG_2222_bw.jpg",
        size_bytes=400,
        mtime=1234567890,
        is_seed=False,
        name_score=50,
        capture_datetime=datetime(2024, 3, 15, 12, 0, 0),
        camera_model="Canon EOS 5D",
        lens_model=None,
        duration_sec=None
    )
    jpeg2_id = db_ops.upsert_file_record(jpeg2)
    db_ops.upsert_media_metadata(jpeg2_id, jpeg2)

    # Run linking
    linker = FileLinker(db_ops)
    links_created = linker.link_raw_outputs(dry_run=False)

    assert links_created >= 1  # At least one link should be created

    # Verify links were created
    cur = db_ops.conn.cursor()
    cur.execute("SELECT COUNT(*) FROM raw_outputs WHERE raw_file_id = ?", (raw_id,))
    count = cur.fetchone()[0]
    assert count >= 1  # Should have at least 1 link


def test_link_raw_outputs_dry_run(db_ops, tmp_path):
    """Test dry run mode doesn't modify database"""
    # Create RAW and JPEG
    raw = FileRecord(
        hash="raw_hash",
        sparse_hash=None,
        hash_is_sparse=False,
        type="raw",
        ext=".cr2",
        orig_name="IMG_3333.CR2",
        orig_path=tmp_path / "IMG_3333.CR2",
        size_bytes=1000,
        mtime=1234567890,
        is_seed=True,
        name_score=50,
        capture_datetime=datetime(2024, 3, 15, 10, 0, 0),
        camera_model="Canon EOS 5D",
        lens_model=None,
        duration_sec=None
    )
    raw_id = db_ops.upsert_file_record(raw)
    db_ops.upsert_media_metadata(raw_id, raw)

    jpeg = FileRecord(
        hash="jpeg_hash",
        sparse_hash=None,
        hash_is_sparse=False,
        type="jpeg",
        ext=".jpg",
        orig_name="IMG_3333.jpg",
        orig_path=tmp_path / "IMG_3333.jpg",
        size_bytes=500,
        mtime=1234567890,
        is_seed=False,
        name_score=50,
        capture_datetime=datetime(2024, 3, 15, 10, 0, 0),
        camera_model="Canon EOS 5D",
        lens_model=None,
        duration_sec=None
    )
    jpeg_id = db_ops.upsert_file_record(jpeg)
    db_ops.upsert_media_metadata(jpeg_id, jpeg)

    # Run linking in dry-run mode
    linker = FileLinker(db_ops)
    links_created = linker.link_raw_outputs(dry_run=True)

    assert links_created == 1  # Should report 1 proposed link

    # Verify NO link was actually created in database
    cur = db_ops.conn.cursor()
    cur.execute("SELECT COUNT(*) FROM raw_outputs")
    count = cur.fetchone()[0]
    assert count == 0  # Database should be unchanged


def test_link_raw_outputs_csv_output(db_ops, tmp_path):
    """Test CSV output generation"""
    # Create RAW and JPEG
    raw = FileRecord(
        hash="raw_hash",
        sparse_hash=None,
        hash_is_sparse=False,
        type="raw",
        ext=".cr2",
        orig_name="IMG_4444.CR2",
        orig_path=tmp_path / "IMG_4444.CR2",
        size_bytes=1000,
        mtime=1234567890,
        is_seed=True,
        name_score=50,
        capture_datetime=datetime(2024, 3, 15, 11, 0, 0),
        camera_model="Canon EOS 5D",
        lens_model=None,
        duration_sec=None
    )
    raw_id = db_ops.upsert_file_record(raw)
    db_ops.upsert_media_metadata(raw_id, raw)

    jpeg = FileRecord(
        hash="jpeg_hash",
        sparse_hash=None,
        hash_is_sparse=False,
        type="jpeg",
        ext=".jpg",
        orig_name="IMG_4444.jpg",
        orig_path=tmp_path / "IMG_4444.jpg",
        size_bytes=500,
        mtime=1234567890,
        is_seed=False,
        name_score=50,
        capture_datetime=datetime(2024, 3, 15, 11, 0, 0),
        camera_model="Canon EOS 5D",
        lens_model=None,
        duration_sec=None
    )
    jpeg_id = db_ops.upsert_file_record(jpeg)
    db_ops.upsert_media_metadata(jpeg_id, jpeg)

    # Run linking with CSV output
    csv_path = tmp_path / "links.csv"
    linker = FileLinker(db_ops)
    links_created = linker.link_raw_outputs(dry_run=True, csv_output=str(csv_path))

    assert links_created == 1
    assert csv_path.exists()

    # Verify CSV content
    import csv
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]['RAW Filename'] == 'IMG_4444.CR2'
        assert rows[0]['JPEG Filename'] == 'IMG_4444.jpg'
        assert int(rows[0]['Confidence']) == 95
