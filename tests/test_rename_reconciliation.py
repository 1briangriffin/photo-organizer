"""
Tests for the rename-detection half of DestinationSyncer.

Covers:
- Change 4: dual-key (full-hash + sparse-hash) lookup with full-preferred.
- Change 5: on-rename full-hash upgrade path, including UNIQUE-conflict
  handling when another row already owns the full hash.
- RenameRecord plumbing: detect-only populates the structured list without
  writing to the catalog; apply_renames persists them later.
"""
import hashlib
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.models import FileRecord
from photo_organizer.sync.path_sync import DestinationSyncer


@pytest.fixture
def db_ops():
    conn = sqlite3.connect(":memory:")
    init_schema(conn)
    yield DBOperations(conn)
    conn.close()


def _write_catalog_raw(db_ops: DBOperations, dest_path: Path, *,
                       full_hash: str | None, sparse_hash: str | None,
                       is_sparse: bool, content_len: int) -> int:
    """Insert a RAW row directly with the exact hash fields we want — bypasses
    the scanner so we can construct sparse-only and full+sparse scenarios."""
    rec = FileRecord(
        hash=full_hash,
        sparse_hash=sparse_hash,
        hash_is_sparse=is_sparse,
        type="raw", ext=dest_path.suffix.lower(),
        orig_name=dest_path.name, orig_path=dest_path,
        size_bytes=content_len, mtime=0.0,
        is_seed=False, name_score=10,
        capture_datetime=datetime(2024, 5, 1, 12, 0, 0),
        camera_model=None, lens_model=None, duration_sec=None,
    )
    fid = db_ops.upsert_file_record(rec)
    db_ops.upsert_media_metadata(fid, rec)
    db_ops.update_dest_path(fid, str(dest_path))
    db_ops.conn.commit()
    return fid


# ---------------------------------------------------------------------------
# Change 1: detect/apply split
# ---------------------------------------------------------------------------

def test_apply_renames_false_records_rename_without_writing(db_ops, tmp_path):
    """apply_renames=False populates report.renames but does not mutate dest_path."""
    raw_dir = tmp_path / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)
    content = b"RAW_BYTES" * 500
    new_path = raw_dir / "IMG_new.CR2"
    new_path.write_bytes(content)
    old_path = raw_dir / "IMG_old.CR2"
    full_hash = hashlib.sha256(content).hexdigest()

    fid = _write_catalog_raw(
        db_ops, old_path,
        full_hash=full_hash, sparse_hash=None, is_sparse=False,
        content_len=len(content),
    )

    report = DestinationSyncer(db_ops).sync_destinations(
        [tmp_path / "raw"], apply_renames=False,
    )

    assert report.renamed_count == 1
    assert len(report.renames) == 1
    rec = report.renames[0]
    assert rec.file_id == fid
    assert rec.old_path == str(old_path)
    assert rec.new_path == str(new_path)

    cur = db_ops.conn.cursor()
    cur.execute("SELECT dest_path FROM files WHERE id = ?", (fid,))
    assert cur.fetchone()[0] == str(old_path), \
        "apply_renames=False must NOT mutate dest_path"


def test_apply_renames_true_persists_rename(db_ops, tmp_path):
    """apply_renames=True (default) persists the rename in the same call."""
    raw_dir = tmp_path / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)
    content = b"RAW_BYTES" * 500
    new_path = raw_dir / "IMG_new.CR2"
    new_path.write_bytes(content)
    old_path = raw_dir / "IMG_old.CR2"
    full_hash = hashlib.sha256(content).hexdigest()

    fid = _write_catalog_raw(
        db_ops, old_path,
        full_hash=full_hash, sparse_hash=None, is_sparse=False,
        content_len=len(content),
    )

    DestinationSyncer(db_ops).sync_destinations(
        [tmp_path / "raw"], apply_renames=True,
    )

    cur = db_ops.conn.cursor()
    cur.execute("SELECT dest_path FROM files WHERE id = ?", (fid,))
    assert cur.fetchone()[0] == str(new_path)


# ---------------------------------------------------------------------------
# Change 4: dual-key lookup
# ---------------------------------------------------------------------------

def test_dual_key_lookup_matches_sparse_only_catalog_row(db_ops, tmp_path):
    """
    A catalog row stored with sparse_hash only (no full hash — the usual
    state for large files whose sparse key has never collided) must still be
    matched when the on-disk file is re-hashed. The sparse half of the
    dual-key lookup handles this.
    """
    raw_dir = tmp_path / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)
    # Use LARGE_FILE_THRESHOLD + slack so the hasher picks sparse.
    content = b"BIG_RAW" * 2_000_000  # ~14 MB
    new_path = raw_dir / "IMG_new.CR2"
    new_path.write_bytes(content)
    old_path = raw_dir / "IMG_old.CR2"

    # Seed a sparse-only catalog row. Compute the sparse hash the same way
    # the scanner would by going through FileHasher directly.
    from photo_organizer.scanning.hasher import FileHasher
    tmp_file = tmp_path / "seed_scratch.CR2"
    tmp_file.write_bytes(content)
    hash_result = FileHasher().compute_hash(tmp_file, known_sparse_hashes=set())
    tmp_file.unlink()
    assert hash_result.is_sparse, "fixture expects sparse-path; bump content size if not"

    fid = _write_catalog_raw(
        db_ops, old_path,
        full_hash=None, sparse_hash=hash_result.sparse_hash, is_sparse=True,
        content_len=len(content),
    )

    report = DestinationSyncer(db_ops).sync_destinations(
        [tmp_path / "raw"], apply_renames=False,
    )

    assert report.renamed_count == 1
    rec = report.renames[0]
    assert rec.file_id == fid
    assert rec.matched_hash_is_sparse is True, \
        "sparse-only catalog row must be matched via the sparse key"


def test_dual_key_lookup_prefers_full_hash_over_sparse(db_ops, tmp_path):
    """
    When a catalog row has both a full hash AND a sparse hash (the post-
    collision-escalation case), the dual-key lookup must prefer the full-hash
    match. The RenameRecord's matched_hash_is_sparse reflects that.
    """
    raw_dir = tmp_path / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)
    # Small-enough content so the hasher writes a plain full hash.
    content = b"small_content" * 100
    new_path = raw_dir / "IMG_new.CR2"
    new_path.write_bytes(content)
    old_path = raw_dir / "IMG_old.CR2"
    full_hash = hashlib.sha256(content).hexdigest()
    # Synthesize a fake sparse hash for this row so both keys are indexed.
    sparse_hash = "s-fake_sparse_for_collision_scenario"

    fid = _write_catalog_raw(
        db_ops, old_path,
        full_hash=full_hash, sparse_hash=sparse_hash, is_sparse=False,
        content_len=len(content),
    )

    report = DestinationSyncer(db_ops).sync_destinations(
        [tmp_path / "raw"], apply_renames=False,
    )

    assert report.renamed_count == 1
    rec = report.renames[0]
    assert rec.matched_hash_is_sparse is False, \
        "full-hash match must win when both keys index the same row"
    assert rec.matched_hash == full_hash


# ---------------------------------------------------------------------------
# Change 5: full-hash upgrade on rename apply
# ---------------------------------------------------------------------------

def test_rename_apply_upgrades_sparse_only_row_to_full_hash(db_ops, tmp_path):
    """
    When the hasher escalated to full-hash (collision with a known sparse
    key) and recorded observed_full_hash on the RenameRecord, apply_renames
    must upgrade files.hash from NULL to the full value.
    """
    raw_dir = tmp_path / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)
    content = b"BIG_RAW" * 2_000_000
    new_path = raw_dir / "IMG_new.CR2"
    new_path.write_bytes(content)
    old_path = raw_dir / "IMG_old.CR2"
    full_hash = hashlib.sha256(content).hexdigest()

    from photo_organizer.scanning.hasher import FileHasher
    tmp_file = tmp_path / "seed_scratch.CR2"
    tmp_file.write_bytes(content)
    sparse_key = FileHasher().compute_hash(tmp_file, known_sparse_hashes=set()).sparse_hash
    tmp_file.unlink()

    fid = _write_catalog_raw(
        db_ops, old_path,
        full_hash=None, sparse_hash=sparse_key, is_sparse=True,
        content_len=len(content),
    )

    # Force collision so hasher computes the full hash too.
    syncer = DestinationSyncer(db_ops)
    # Seed another sparse-only row with the same sparse key so hashing the
    # target file triggers the collision-escalation path.
    dup = FileRecord(
        hash=None, sparse_hash=sparse_key, hash_is_sparse=True,
        type="raw", ext=".cr2",
        orig_name="dup.CR2", orig_path=Path("/no/such/dup.CR2"),
        size_bytes=len(content), mtime=0.0,
        is_seed=False, name_score=1,
        capture_datetime=None,
        camera_model=None, lens_model=None, duration_sec=None,
    )
    db_ops.upsert_file_record(dup)
    db_ops.conn.commit()

    report = syncer.sync_destinations([tmp_path / "raw"], apply_renames=True)
    assert report.renamed_count == 1

    cur = db_ops.conn.cursor()
    cur.execute("SELECT hash FROM files WHERE id = ?", (fid,))
    stored = cur.fetchone()[0]
    assert stored == full_hash, \
        f"full-hash upgrade must persist on apply_renames; got {stored}"


def test_rename_apply_full_hash_upgrade_tolerates_unique_conflict(
        db_ops, tmp_path, caplog):
    """
    files.hash is UNIQUE. If another catalog row already owns the full hash
    we just observed (duplicate content catalogued twice), the upgrade
    UPDATE hits IntegrityError. apply_renames must log-and-continue — the
    rename itself still succeeds, the row just remains sparse-only.
    """
    raw_dir = tmp_path / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)
    content = b"BIG_RAW_DUP" * 2_000_000
    new_path = raw_dir / "IMG_new.CR2"
    new_path.write_bytes(content)
    old_path = raw_dir / "IMG_old.CR2"
    full_hash = hashlib.sha256(content).hexdigest()

    from photo_organizer.scanning.hasher import FileHasher
    tmp_file = tmp_path / "seed_scratch.CR2"
    tmp_file.write_bytes(content)
    sparse_key = FileHasher().compute_hash(tmp_file, known_sparse_hashes=set()).sparse_hash
    tmp_file.unlink()

    # Row A: sparse-only, the one being renamed. Its sparse_hash in
    # known_sparse_hashes is enough to force the scanner to escalate to full
    # when it encounters the new file (no separate "Row C" needed).
    fid = _write_catalog_raw(
        db_ops, old_path,
        full_hash=None, sparse_hash=sparse_key, is_sparse=True,
        content_len=len(content),
    )
    # Row B: already owns the full hash we're about to try to upgrade to.
    # dest_path lives outside the sync root so Row B is excluded from
    # _load_expected_files — it stays in the `files` table, just not loaded
    # into the in-memory lookup, so the match lands on Row A by sparse and
    # only the UPDATE then hits the UNIQUE conflict.
    _write_catalog_raw(
        db_ops, Path("/elsewhere/other.CR2"),
        full_hash=full_hash, sparse_hash=None, is_sparse=False,
        content_len=len(content),
    )

    with caplog.at_level(logging.WARNING):
        report = DestinationSyncer(db_ops).sync_destinations(
            [tmp_path / "raw"], apply_renames=True,
        )

    assert report.renamed_count == 1
    # The rename itself persisted.
    cur = db_ops.conn.cursor()
    cur.execute("SELECT dest_path, hash FROM files WHERE id = ?", (fid,))
    dest_path_after, hash_after = cur.fetchone()
    assert dest_path_after == str(new_path), \
        "rename must still apply even when full-hash upgrade conflicts"
    assert hash_after is None, \
        "row stays sparse-only when full-hash upgrade hits UNIQUE conflict"
    # A warning was emitted so the user can investigate the duplicate.
    assert any("UNIQUE" in rec.message or "Full-hash upgrade blocked" in rec.message
               for rec in caplog.records), \
        "UNIQUE conflict must produce a warning log for operator visibility"
