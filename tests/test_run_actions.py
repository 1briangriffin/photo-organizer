import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path

from photo_organizer.core import PhotoOrganizerApp
from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
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


def _seed_missing_raw(
    db_path: Path,
    *,
    old_raw_path: Path,
    matching_content: bytes,
    capture_dt: datetime,
) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        ops = DBOperations(conn)
        rec = FileRecord(
            hash=hashlib.sha256(matching_content).hexdigest(),
            sparse_hash=None,
            hash_is_sparse=False,
            type="raw",
            ext=old_raw_path.suffix.lower(),
            orig_name=old_raw_path.name,
            orig_path=old_raw_path,
            size_bytes=len(matching_content),
            mtime=capture_dt.timestamp(),
            is_seed=False,
            name_score=1,
            capture_datetime=capture_dt,
            camera_model="Canon Test",
        )
        file_id = ops.upsert_file_record(rec)
        ops.upsert_media_metadata(file_id, rec)
        ops.update_dest_path(file_id, str(old_raw_path))
        conn.commit()
        return file_id
    finally:
        conn.close()


def _build_dpp_ingest_fixture(tmp_path: Path):
    dest = tmp_path / "dest"
    raw_dir = dest / "raw" / "2023" / "2023-06"
    raw_dir.mkdir(parents=True)

    capture_dt = datetime(2023, 6, 15, 10, 0, 0)
    ts = capture_dt.timestamp()

    old_raw = raw_dir / "IMG_0042_2023-06-15_10-00-00.CR2"
    new_raw = raw_dir / "Beach_0042_2023-06-15-10-00-00.CR2"
    jpeg = raw_dir / "Beach_0042_2023-06-15-10-00-00.JPG"

    raw_content = b"raw bytes" * 500
    new_raw.write_bytes(raw_content)
    jpeg.write_bytes(b"jpeg bytes" * 500)
    for path in (new_raw, jpeg):
        import os
        os.utime(path, (ts, ts))

    db_path = _make_db(tmp_path)
    raw_id = _seed_missing_raw(
        db_path,
        old_raw_path=old_raw,
        matching_content=raw_content,
        capture_dt=capture_dt,
    )
    return db_path, dest, old_raw, new_raw, jpeg, raw_id


def test_ingest_dest_dry_run_persists_proposed_run_actions(tmp_path):
    db_path, dest, old_raw, new_raw, jpeg, raw_id = _build_dpp_ingest_fixture(tmp_path)

    conn = sqlite3.connect(str(db_path))
    try:
        run_id = _start_run(conn, dry_run=True)
        conn.commit()
    finally:
        conn.close()

    PhotoOrganizerApp(db_path).ingest_dest(
        dest_root=dest,
        move=True,
        dry_run=True,
        run_id=run_id,
    )

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(
            """
            SELECT action_type, status, source_path, target_path
            FROM run_actions
            WHERE proposed_by_run_id = ?
            ORDER BY phase, sequence, id
            """,
            (run_id,),
        )
        actions = cur.fetchall()

        cur = conn.execute("SELECT dest_path FROM files WHERE id = ?", (raw_id,))
        raw_dest = cur.fetchone()[0]
        cur = conn.execute("SELECT COUNT(*) FROM raw_outputs")
        raw_output_count = cur.fetchone()[0]
    finally:
        conn.close()

    assert ("update_canonical_dest_path", "proposed", str(old_raw), str(new_raw)) in actions
    assert any(row[0] == "link_raw_output" and row[1] == "proposed" for row in actions)
    assert any(row[0] == "move_file" and row[1] == "proposed" and row[2] == str(jpeg) for row in actions)
    assert raw_dest == str(old_raw)
    assert raw_output_count == 0


def test_ingest_dest_real_run_marks_actions_applied_and_sets_link_provenance(tmp_path):
    db_path, dest, old_raw, new_raw, jpeg, raw_id = _build_dpp_ingest_fixture(tmp_path)

    conn = sqlite3.connect(str(db_path))
    try:
        run_id = _start_run(conn, dry_run=False)
        conn.commit()
    finally:
        conn.close()

    PhotoOrganizerApp(db_path).ingest_dest(
        dest_root=dest,
        move=True,
        dry_run=False,
        run_id=run_id,
    )

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(
            """
            SELECT action_type, status
            FROM run_actions
            WHERE proposed_by_run_id = ? OR applied_by_run_id = ?
            """,
            (run_id, run_id),
        )
        actions = cur.fetchall()

        cur = conn.execute("SELECT dest_path FROM files WHERE id = ?", (raw_id,))
        raw_dest = cur.fetchone()[0]
        cur = conn.execute(
            """
            SELECT ro.created_by_run_id, f.orig_path, f.dest_path
            FROM raw_outputs ro
            JOIN files f ON f.id = ro.output_file_id
            """
        )
        link_row = cur.fetchone()
    finally:
        conn.close()

    assert ("update_canonical_dest_path", "applied") in actions
    assert ("link_raw_output", "applied") in actions
    assert ("move_file", "applied") in actions
    assert raw_dest == str(new_raw)
    assert link_row is not None
    assert link_row[0] == run_id
    assert link_row[1] == str(jpeg)
    assert link_row[2] is not None
    assert not jpeg.exists()


def test_organize_dry_run_records_proposed_actions_without_persisting_links(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    dest = tmp_path / "dest"
    raw = src / "IMG_1000.CR2"
    sidecar = src / "IMG_1000.xmp"
    raw.write_bytes(b"raw bytes" * 500)
    sidecar.write_bytes(b"<xmp/>")

    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    try:
        run_id = _start_run(conn, dry_run=True)
        conn.commit()
    finally:
        conn.close()

    PhotoOrganizerApp(db_path).organize(
        src_root=src,
        dest_root=dest,
        dry_run=True,
        dry_run_csv=tmp_path / "preview.csv",
        run_id=run_id,
    )

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(
            """
            SELECT action_type, status
            FROM run_actions
            WHERE proposed_by_run_id = ?
            """,
            (run_id,),
        )
        actions = cur.fetchall()
        cur = conn.execute("SELECT COUNT(*) FROM raw_sidecars")
        sidecar_links = cur.fetchone()[0]
        cur = conn.execute("SELECT COUNT(*) FROM files WHERE dest_path IS NOT NULL")
        planned_dest_paths = cur.fetchone()[0]
    finally:
        conn.close()

    assert ("link_raw_sidecar", "proposed") in actions
    assert any(action_type == "copy_file" and status == "proposed"
               for action_type, status in actions)
    assert sidecar_links == 0
    assert planned_dest_paths == 0
