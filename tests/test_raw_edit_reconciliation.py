import hashlib
import csv
import os
import sqlite3
from datetime import datetime
from pathlib import Path

from photo_organizer.core import PhotoOrganizerApp
from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.metadata.extract import ImageMetadata
from photo_organizer.models import FileRecord


def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    conn.close()
    return db_path


def _start_run(conn: sqlite3.Connection, *, dry_run: bool) -> int:
    cur = conn.execute(
        """
        INSERT INTO command_runs (
            tool, command, started_at, exit_status, dry_run, argv_json
        )
        VALUES ('photo-organizer', 'ingest-dest', '2026-01-01T00:00:00Z',
                'running', ?, '[]')
        """,
        (1 if dry_run else 0,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _seed_raw(
    db_path: Path,
    *,
    old_raw_path: Path,
    original_content: bytes,
    capture_dt: datetime,
) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        init_schema(conn)
        ops = DBOperations(conn)
        rec = FileRecord(
            hash=hashlib.sha256(original_content).hexdigest(),
            sparse_hash=None,
            hash_is_sparse=False,
            type="raw",
            ext=old_raw_path.suffix.lower(),
            orig_name=old_raw_path.name,
            orig_path=old_raw_path,
            size_bytes=len(original_content),
            mtime=capture_dt.timestamp(),
            is_seed=False,
            name_score=1,
            capture_datetime=capture_dt,
            camera_model=None,
        )
        file_id = ops.upsert_file_record(rec)
        ops.upsert_media_metadata(file_id, rec)
        ops.update_dest_path(file_id, str(old_raw_path))
        ops.record_occurrence(
            file_id=file_id,
            path=old_raw_path,
            is_seed=False,
            mtime=capture_dt.timestamp(),
            size_bytes=len(original_content),
            hash_value=rec.hash,
            is_sparse=False,
        )
        conn.commit()
        return file_id
    finally:
        conn.close()


def _dest_path(db_path: Path, file_id: int) -> str | None:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT dest_path FROM files WHERE id = ?",
            (file_id,),
        ).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def test_ingest_dest_dry_run_proposes_modified_raw_rename_without_importing(tmp_path):
    dest = tmp_path / "dest"
    raw_dir = dest / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)
    capture_dt = datetime(2024, 5, 1, 12, 0, 0)
    ts = capture_dt.timestamp()

    old_raw = raw_dir / "IMG_0042_2024-05-01_12-00-00.CR2"
    edited_raw = raw_dir / "Soccer_0042_2024-05-01_12-00-01.CR2"
    original_content = b"original raw bytes" * 500
    edited_raw.write_bytes(b"modified raw bytes" * 500)
    os.utime(edited_raw, (ts, ts))

    db_path = _make_db(tmp_path)
    raw_id = _seed_raw(
        db_path,
        old_raw_path=old_raw,
        original_content=original_content,
        capture_dt=capture_dt,
    )
    conn = sqlite3.connect(str(db_path))
    try:
        run_id = _start_run(conn, dry_run=True)
        conn.commit()
    finally:
        conn.close()

    PhotoOrganizerApp(db_path).ingest_dest(dest_root=dest, dry_run=True, run_id=run_id)

    conn = sqlite3.connect(str(db_path))
    try:
        actions = conn.execute(
            """
            SELECT action_type, status, source_path, target_path, method, confidence
            FROM run_actions
            WHERE proposed_by_run_id = ?
            """,
            (run_id,),
        ).fetchall()
        observations = conn.execute(
            """
            SELECT observation_type, path, match_method
            FROM file_observations
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchall()
        raw_count = conn.execute("SELECT COUNT(*) FROM files WHERE type = 'raw'").fetchone()[0]
    finally:
        conn.close()

    assert _dest_path(db_path, raw_id) == str(old_raw)
    assert raw_count == 1
    assert (
        "update_canonical_dest_path",
        "proposed",
        str(old_raw),
        str(edited_raw),
        "raw_edit_metadata_same_directory",
        95,
    ) in actions
    assert not any(row[0] in {"move_file", "copy_file"} for row in actions)
    assert (
        "modified_rename_candidate",
        str(edited_raw),
        "raw_edit_metadata_same_directory",
    ) in observations

    preview = dest / "ingest_dry_run_preview.csv"
    with preview.open(newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    assert [
        str(edited_raw),
        "raw",
        str(edited_raw),
        "Update catalog path",
        "",
    ] in rows


def test_ingest_dest_real_run_applies_modified_raw_rename(tmp_path):
    dest = tmp_path / "dest"
    raw_dir = dest / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)
    capture_dt = datetime(2024, 5, 1, 12, 0, 0)
    ts = capture_dt.timestamp()

    old_raw = raw_dir / "IMG_0042_2024-05-01_12-00-00.CR2"
    edited_raw = raw_dir / "Soccer_0042_2024-05-01_12-00-01.CR2"
    edited_raw.write_bytes(b"modified raw bytes" * 500)
    os.utime(edited_raw, (ts, ts))

    db_path = _make_db(tmp_path)
    raw_id = _seed_raw(
        db_path,
        old_raw_path=old_raw,
        original_content=b"original raw bytes" * 500,
        capture_dt=capture_dt,
    )
    conn = sqlite3.connect(str(db_path))
    try:
        run_id = _start_run(conn, dry_run=False)
        conn.commit()
    finally:
        conn.close()

    PhotoOrganizerApp(db_path).ingest_dest(dest_root=dest, dry_run=False, run_id=run_id)

    conn = sqlite3.connect(str(db_path))
    try:
        action = conn.execute(
            """
            SELECT status, applied_by_run_id, method
            FROM run_actions
            WHERE action_type = 'update_canonical_dest_path'
            """,
        ).fetchone()
        raw_count = conn.execute("SELECT COUNT(*) FROM files WHERE type = 'raw'").fetchone()[0]
        occurrence = conn.execute(
            "SELECT file_id FROM file_occurrences WHERE path = ?",
            (str(edited_raw),),
        ).fetchone()
    finally:
        conn.close()

    assert _dest_path(db_path, raw_id) == str(edited_raw)
    assert action == ("applied", run_id, "raw_edit_metadata_same_directory")
    assert raw_count == 1
    assert occurrence == (raw_id,)


def test_ambiguous_modified_raw_candidates_are_review_only(tmp_path):
    dest = tmp_path / "dest"
    raw_dir = dest / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)
    capture_dt = datetime(2024, 5, 1, 12, 0, 0)
    ts = capture_dt.timestamp()

    edited_raw = raw_dir / "Edited_0042.CR2"
    edited_raw.write_bytes(b"edited raw bytes" * 500)
    os.utime(edited_raw, (ts, ts))

    db_path = _make_db(tmp_path)
    raw_a = _seed_raw(
        db_path,
        old_raw_path=raw_dir / "IMG_0042_a.CR2",
        original_content=b"original raw a" * 500,
        capture_dt=capture_dt,
    )
    raw_b = _seed_raw(
        db_path,
        old_raw_path=raw_dir / "COPY_0042_b.CR2",
        original_content=b"original raw b" * 500,
        capture_dt=capture_dt,
    )
    conn = sqlite3.connect(str(db_path))
    try:
        run_id = _start_run(conn, dry_run=True)
        conn.commit()
    finally:
        conn.close()

    stats = PhotoOrganizerApp(db_path).ingest_dest(
        dest_root=dest,
        dry_run=True,
        run_id=run_id,
    )

    conn = sqlite3.connect(str(db_path))
    try:
        action_count = conn.execute("SELECT COUNT(*) FROM run_actions").fetchone()[0]
        raw_count = conn.execute("SELECT COUNT(*) FROM files WHERE type = 'raw'").fetchone()[0]
        observations = conn.execute(
            """
            SELECT file_id, observation_type, match_method
            FROM file_observations
            WHERE run_id = ? AND path = ?
            ORDER BY file_id
            """,
            (run_id, str(edited_raw)),
        ).fetchall()
    finally:
        conn.close()

    assert stats["renamed"] == 0
    assert stats["imported"] == 0
    assert stats["review_required"] == 1
    assert _dest_path(db_path, raw_a) != str(edited_raw)
    assert _dest_path(db_path, raw_b) != str(edited_raw)
    assert raw_count == 2
    assert action_count == 0
    assert observations == [
        (raw_a, "modified_rename_candidate", "raw_edit_metadata_ambiguous"),
        (raw_b, "modified_rename_candidate", "raw_edit_metadata_ambiguous"),
    ]


def test_camera_file_number_disambiguates_modified_raw_candidate(tmp_path, monkeypatch):
    dest = tmp_path / "dest"
    raw_dir = dest / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)
    capture_dt = datetime(2024, 5, 1, 12, 0, 0)
    ts = capture_dt.timestamp()

    edited_raw = raw_dir / "Edited_event_0001_2024-05-01-12-00-00.CR2"
    edited_raw.write_bytes(b"edited raw bytes" * 500)
    os.utime(edited_raw, (ts, ts))

    db_path = _make_db(tmp_path)
    raw_0042 = _seed_raw(
        db_path,
        old_raw_path=raw_dir / "EOS_R5_2024_05_01_0042_2024-05-01_12-00-00.CR2",
        original_content=b"original raw 0042" * 500,
        capture_dt=capture_dt,
    )
    raw_0043 = _seed_raw(
        db_path,
        old_raw_path=raw_dir / "EOS_R5_2024_05_01_0043_2024-05-01_12-00-00.CR2",
        original_content=b"original raw 0043" * 500,
        capture_dt=capture_dt,
    )

    def fake_metadata(self, path, *, include_camera_identity=False):
        return ImageMetadata(
            capture_datetime=capture_dt,
            camera_model=None,
            lens_model=None,
            camera_file_number="1000042" if include_camera_identity else None,
        )

    monkeypatch.setattr(
        "photo_organizer.metadata.extract.MetadataExtractor.get_image_metadata_details",
        fake_metadata,
    )

    conn = sqlite3.connect(str(db_path))
    try:
        run_id = _start_run(conn, dry_run=True)
        conn.commit()
    finally:
        conn.close()

    stats = PhotoOrganizerApp(db_path).ingest_dest(
        dest_root=dest,
        dry_run=True,
        run_id=run_id,
    )

    conn = sqlite3.connect(str(db_path))
    try:
        action = conn.execute(
            """
            SELECT entity_id, source_path, target_path, status, method
            FROM run_actions
            WHERE action_type = 'update_canonical_dest_path'
            """,
        ).fetchone()
        review_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM file_observations
            WHERE run_id = ?
              AND observation_type = 'modified_rename_candidate'
              AND match_method = 'raw_edit_metadata_ambiguous'
            """,
            (run_id,),
        ).fetchone()[0]
    finally:
        conn.close()

    assert stats["renamed"] == 1
    assert stats["review_required"] == 0
    assert action == (
        raw_0042,
        str(raw_dir / "EOS_R5_2024_05_01_0042_2024-05-01_12-00-00.CR2"),
        str(edited_raw),
        "proposed",
        "raw_edit_metadata_same_directory",
    )
    assert _dest_path(db_path, raw_0042) != str(edited_raw)
    assert _dest_path(db_path, raw_0043) != str(edited_raw)
    assert review_count == 0
