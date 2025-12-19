import pytest
from pathlib import Path
from datetime import datetime
from photo_organizer.models import FileRecord

def test_deduplication_logic(db_ops):
    """Verify that same hash yields same ID, and priority rules work."""
    dt = datetime(2022, 1, 1)
    
    rec1 = FileRecord(
        hash="hash123",
        type="raw",
        ext=".dng",
        orig_name="photo1.dng",
        orig_path=Path("/src1/photo1.dng"),
        size_bytes=1000,
        is_seed=False,
        name_score=5,
        capture_datetime=dt,
    )
    
    # Same hash, better name
    rec2 = FileRecord(
        hash="hash123",
        type="raw",
        ext=".dng",
        orig_name="photo2.dng",
        orig_path=Path("/src2/photo2.dng"),
        size_bytes=1000,
        is_seed=False,
        name_score=10,
        capture_datetime=dt,
    )

    id1 = db_ops.upsert_file_record(rec1)
    id2 = db_ops.upsert_file_record(rec2)

    assert id1 == id2, "Should return same ID for identical hash"

    # Verify rec2 updated the canonical info (higher score)
    cur = db_ops.conn.cursor()
    cur.execute("SELECT orig_name FROM files WHERE id = ?", (id1,))
    assert cur.fetchone()[0] == "photo2.dng"