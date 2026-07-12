"""
RAW metadata backfill tests: candidate selection, NULL-only identity fill,
the --backfill-raw-metadata command, and the apply-time fill during RAW
edit-aware rename reconciliation.
"""
import hashlib
import os
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from photo_organizer.core import PhotoOrganizerApp
from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.metadata.extract import ImageMetadata
from photo_organizer.models import FileRecord
from photo_organizer.sync.path_sync import DestinationSyncer, RenameRecord, SyncReport

CAPTURE_DT = datetime(2024, 5, 1, 12, 0, 0)


@pytest.fixture
def conn(tmp_path):
    db_path = tmp_path / "catalog.db"
    c = sqlite3.connect(str(db_path))
    init_schema(c)
    try:
        yield c
    finally:
        c.close()


@pytest.fixture
def db_ops(conn):
    return DBOperations(conn)


def _seed_file(db_ops, *, path: Path, file_type="raw", content=b"raw bytes",
               serial=None, file_number=None, with_metadata=True,
               dest_path=None) -> int:
    rec = FileRecord(
        hash=hashlib.sha256(content + path.name.encode()).hexdigest(),
        sparse_hash=None,
        hash_is_sparse=False,
        type=file_type,
        ext=path.suffix.lower(),
        orig_name=path.name,
        orig_path=path,
        size_bytes=len(content),
        mtime=CAPTURE_DT.timestamp(),
        is_seed=False,
        name_score=1,
        capture_datetime=CAPTURE_DT,
        camera_model="Canon Test",
        camera_serial_number=serial,
        camera_file_number=file_number,
    )
    file_id = db_ops.upsert_file_record(rec)
    if with_metadata:
        db_ops.upsert_media_metadata(file_id, rec)
    db_ops.update_dest_path(file_id, str(dest_path or path))
    return file_id


def _identity_row(conn, file_id):
    return conn.execute(
        "SELECT camera_serial_number, camera_file_number "
        "FROM media_metadata WHERE file_id = ?",
        (file_id,),
    ).fetchone()


# ---------------------------------------------------------------------------
# DBOperations primitives
# ---------------------------------------------------------------------------

def test_backfill_candidates_select_incomplete_raw_rows(conn, db_ops, tmp_path):
    dest = tmp_path / "dest"
    missing_both = _seed_file(db_ops, path=dest / "a.CR2")
    missing_serial = _seed_file(db_ops, path=dest / "b.CR2", file_number="1000042")
    no_metadata_row = _seed_file(db_ops, path=dest / "c.CR2", with_metadata=False)
    complete = _seed_file(db_ops, path=dest / "d.CR2",
                          serial="SN123", file_number="1000043")
    jpeg = _seed_file(db_ops, path=dest / "e.JPG", file_type="jpeg")
    other_root = _seed_file(db_ops, path=tmp_path / "elsewhere" / "f.CR2")
    conn.commit()

    ids = {row[0] for row in db_ops.get_raw_metadata_backfill_candidates([dest])}
    assert ids == {missing_both, missing_serial, no_metadata_row}
    assert complete not in ids
    assert jpeg not in ids
    assert other_root not in ids

    unscoped = {row[0] for row in db_ops.get_raw_metadata_backfill_candidates()}
    assert other_root in unscoped

    limited = db_ops.get_raw_metadata_backfill_candidates([dest], limit=1)
    assert len(limited) == 1


def test_fill_camera_identity_only_fills_nulls(conn, db_ops, tmp_path):
    dest = tmp_path / "dest"
    partial = _seed_file(db_ops, path=dest / "a.CR2", serial="KEEP-ME")
    absent = _seed_file(db_ops, path=dest / "b.CR2", with_metadata=False)
    conn.commit()

    assert db_ops.fill_camera_identity_if_null(partial, "NEW-SERIAL", "1000042")
    assert _identity_row(conn, partial) == ("KEEP-ME", "1000042")

    assert db_ops.fill_camera_identity_if_null(absent, "SN999", None)
    assert _identity_row(conn, absent) == ("SN999", None)

    # Nothing to contribute → no-op, reported as such.
    assert not db_ops.fill_camera_identity_if_null(partial, None, None)


# ---------------------------------------------------------------------------
# backfill_raw_metadata command
# ---------------------------------------------------------------------------

def _build_backfill_fixture(tmp_path):
    dest = tmp_path / "dest"
    raw_dir = dest / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)

    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    db_ops = DBOperations(conn)

    on_disk = raw_dir / "IMG_0042.CR2"
    on_disk.write_bytes(b"raw bytes" * 100)
    os.utime(on_disk, (CAPTURE_DT.timestamp(), CAPTURE_DT.timestamp()))
    no_tags = raw_dir / "IMG_0043.CR2"
    no_tags.write_bytes(b"other raw bytes" * 100)

    ids = {
        "on_disk": _seed_file(db_ops, path=on_disk),
        "no_tags": _seed_file(db_ops, path=no_tags),
        "vanished": _seed_file(db_ops, path=raw_dir / "IMG_0044.CR2"),
        "complete": _seed_file(db_ops, path=raw_dir / "IMG_0045.CR2",
                               serial="SN1", file_number="1000045"),
    }
    conn.commit()
    conn.close()
    return db_path, dest, ids


def test_backfill_fills_identity_from_disk(tmp_path, monkeypatch):
    db_path, dest, ids = _build_backfill_fixture(tmp_path)

    def fake_metadata(self, path, *, include_camera_identity=False):
        assert include_camera_identity
        if path.name == "IMG_0042.CR2":
            return ImageMetadata(
                capture_datetime=CAPTURE_DT,
                camera_model="Canon Test",
                lens_model=None,
                camera_serial_number="SN-BACKFILL",
                camera_file_number="1000042",
            )
        return ImageMetadata(None, None, None)

    monkeypatch.setattr(
        "photo_organizer.metadata.extract.MetadataExtractor.get_image_metadata_details",
        fake_metadata,
    )

    stats = PhotoOrganizerApp(db_path).backfill_raw_metadata(dest_root=dest)

    assert stats == {
        "candidates": 3,
        "missing_on_disk": 1,
        "updated": 1,
        "no_identity_found": 1,
    }
    conn = sqlite3.connect(str(db_path))
    try:
        assert _identity_row(conn, ids["on_disk"]) == ("SN-BACKFILL", "1000042")
        assert _identity_row(conn, ids["no_tags"]) == (None, None)
        assert _identity_row(conn, ids["complete"]) == ("SN1", "1000045")
    finally:
        conn.close()

    # Second run: the filled row is no longer a candidate.
    stats_again = PhotoOrganizerApp(db_path).backfill_raw_metadata(dest_root=dest)
    assert stats_again["candidates"] == 2
    assert stats_again["updated"] == 0


def test_backfill_dry_run_reports_scope_without_extraction(tmp_path, monkeypatch):
    db_path, dest, _ids = _build_backfill_fixture(tmp_path)

    def explode(self, path, **kwargs):
        raise AssertionError("dry-run must not invoke metadata extraction")

    monkeypatch.setattr(
        "photo_organizer.metadata.extract.MetadataExtractor.get_image_metadata_details",
        explode,
    )

    stats = PhotoOrganizerApp(db_path).backfill_raw_metadata(
        dest_root=dest, dry_run=True,
    )
    assert stats == {
        "candidates": 3,
        "missing_on_disk": 1,
        "updated": 0,
        "no_identity_found": 0,
    }


def test_backfill_respects_limit(tmp_path, monkeypatch):
    db_path, dest, _ids = _build_backfill_fixture(tmp_path)
    monkeypatch.setattr(
        "photo_organizer.metadata.extract.MetadataExtractor.get_image_metadata_details",
        lambda self, path, **kwargs: ImageMetadata(None, None, None),
    )

    stats = PhotoOrganizerApp(db_path).backfill_raw_metadata(
        dest_root=dest, limit=1,
    )
    assert stats["candidates"] == 1


# ---------------------------------------------------------------------------
# Apply-time fill during rename reconciliation
# ---------------------------------------------------------------------------

def test_apply_renames_fills_identity_from_rename_record(conn, db_ops, tmp_path):
    dest = tmp_path / "dest"
    old_path = dest / "IMG_0042.CR2"
    file_id = _seed_file(db_ops, path=old_path)
    db_ops.record_occurrence(
        file_id=file_id,
        path=old_path,
        is_seed=False,
        mtime=CAPTURE_DT.timestamp(),
        size_bytes=100,
        hash_value="h-old",
        is_sparse=False,
    )
    conn.commit()

    report = SyncReport()
    report.renames.append(RenameRecord(
        file_id=file_id,
        old_path=str(old_path),
        new_path=str(dest / "Edited_0042.CR2"),
        mtime=CAPTURE_DT.timestamp(),
        size_bytes=100,
        matched_hash="h-new",
        matched_hash_is_sparse=False,
        observed_full_hash=None,
        match_method="raw_edit_metadata_same_directory",
        confidence=95,
        observed_camera_serial="SN-EDIT",
        observed_camera_file_number="1000042",
    ))

    applied = DestinationSyncer(db_ops).apply_renames(report)
    conn.commit()

    assert applied == {file_id}
    assert _identity_row(conn, file_id) == ("SN-EDIT", "1000042")


def test_ingest_dest_raw_edit_rename_fills_identity_end_to_end(tmp_path, monkeypatch):
    dest = tmp_path / "dest"
    raw_dir = dest / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)
    ts = CAPTURE_DT.timestamp()

    old_raw = raw_dir / "IMG_0042_2024-05-01_12-00-00.CR2"
    edited_raw = raw_dir / "Soccer_0042_2024-05-01_12-00-01.CR2"
    edited_raw.write_bytes(b"modified raw bytes" * 500)
    os.utime(edited_raw, (ts, ts))

    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    db_ops = DBOperations(conn)
    raw_id = _seed_file(db_ops, path=old_raw, content=b"original raw bytes" * 500)
    db_ops.record_occurrence(
        file_id=raw_id,
        path=old_raw,
        is_seed=False,
        mtime=ts,
        size_bytes=len(b"original raw bytes" * 500),
        hash_value=hashlib.sha256(b"original raw bytes" * 500 + old_raw.name.encode()).hexdigest(),
        is_sparse=False,
    )
    run_id = conn.execute(
        """
        INSERT INTO command_runs (
            tool, command, started_at, exit_status, dry_run, argv_json
        )
        VALUES ('photo-organizer', 'ingest-dest', '2026-01-01T00:00:00Z',
                'running', 0, '[]')
        """
    ).lastrowid
    conn.commit()
    conn.close()

    def fake_metadata(self, path, *, include_camera_identity=False):
        return ImageMetadata(
            capture_datetime=CAPTURE_DT,
            camera_model="Canon Test",
            lens_model=None,
            camera_serial_number="SN-DPP" if include_camera_identity else None,
            camera_file_number="1000042" if include_camera_identity else None,
        )

    monkeypatch.setattr(
        "photo_organizer.metadata.extract.MetadataExtractor.get_image_metadata_details",
        fake_metadata,
    )

    stats = PhotoOrganizerApp(db_path).ingest_dest(
        dest_root=dest, dry_run=False, run_id=run_id,
    )

    conn = sqlite3.connect(str(db_path))
    try:
        dest_path = conn.execute(
            "SELECT dest_path FROM files WHERE id = ?", (raw_id,)
        ).fetchone()[0]
        identity = _identity_row(conn, raw_id)
    finally:
        conn.close()

    assert stats["renamed"] == 1
    assert dest_path == str(edited_raw)
    assert identity == ("SN-DPP", "1000042")
