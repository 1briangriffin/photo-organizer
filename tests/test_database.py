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

def test_sparse_hash_upgraded_to_full(db_ops, tmp_path):
    """Sparse-only insert should upgrade to full hash when available."""
    rec_sparse = FileRecord(
        hash=None,
        sparse_hash="s-sparse-1",
        hash_is_sparse=True,
        type="raw",
        ext=".dng",
        orig_name="photo_sparse.dng",
        orig_path=Path("/src/photo_sparse.dng"),
        size_bytes=1000,
        is_seed=False,
        name_score=1,
    )
    sparse_id = db_ops.upsert_file_record(rec_sparse)

    rec_full = FileRecord(
        hash="fullhash-1",
        sparse_hash="s-sparse-1",
        hash_is_sparse=False,
        type="raw",
        ext=".dng",
        orig_name="photo_full.dng",
        orig_path=Path("/src/photo_full.dng"),
        size_bytes=1000,
        is_seed=False,
        name_score=2,
    )
    full_id = db_ops.upsert_file_record(rec_full)

    assert sparse_id == full_id

    cur = db_ops.conn.cursor()
    cur.execute("SELECT hash, sparse_hash, orig_name FROM files WHERE id = ?", (sparse_id,))
    stored_hash, stored_sparse, stored_name = cur.fetchone()
    assert stored_hash == "fullhash-1"
    assert stored_sparse == "s-sparse-1"
    assert stored_name == "photo_full.dng"
