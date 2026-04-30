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


def _start_run(conn: sqlite3.Connection, *, dry_run: bool = True) -> int:
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


def _seed_catalog_file(db_path: Path, file_path: Path, *, file_type: str = "jpeg") -> int:
    content = file_path.read_bytes()
    conn = sqlite3.connect(str(db_path))
    try:
        ops = DBOperations(conn)
        rec = FileRecord(
            hash=hashlib.sha256(content).hexdigest(),
            sparse_hash=None,
            hash_is_sparse=False,
            type=file_type,
            ext=file_path.suffix.lower(),
            orig_name=file_path.name,
            orig_path=file_path,
            size_bytes=len(content),
            mtime=file_path.stat().st_mtime,
            is_seed=False,
            name_score=1,
            capture_datetime=datetime(2023, 6, 15, 10, 0, 0),
        )
        file_id = ops.upsert_file_record(rec)
        ops.update_dest_path(file_id, str(file_path))
        conn.commit()
        return file_id
    finally:
        conn.close()


def test_ingest_dest_dry_run_records_rename_and_new_observations(tmp_path):
    raw_dir = tmp_path / "dest" / "raw" / "2023" / "2023-06"
    raw_dir.mkdir(parents=True)

    old_file = raw_dir / "IMG_OLD.jpg"
    new_file = raw_dir / "IMG_NEW.jpg"
    content = b"same image bytes" * 100
    new_file.write_bytes(content)

    db_path = _make_db(tmp_path)
    renamed_file_id = _seed_catalog_file(db_path, new_file)

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("UPDATE files SET orig_name = ?, orig_path = ?, dest_path = ? WHERE id = ?",
                     (old_file.name, str(old_file), str(old_file), renamed_file_id))
        run_id = _start_run(conn)
        conn.commit()
    finally:
        conn.close()

    brand_new = raw_dir / "brand_new.jpg"
    brand_new.write_bytes(b"brand new jpeg" * 100)

    app = PhotoOrganizerApp(db_path)
    app.ingest_dest(dest_root=tmp_path / "dest", dry_run=True, run_id=run_id)

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(
            """
            SELECT observation_type, path
            FROM file_observations
            WHERE run_id = ?
            ORDER BY id
            """,
            (run_id,),
        )
        observations = cur.fetchall()
        cur = conn.execute(
            "SELECT dest_path FROM files WHERE id = ?",
            (renamed_file_id,),
        )
        stored_dest = cur.fetchone()[0]
    finally:
        conn.close()

    assert ("renamed_candidate", str(new_file)) in observations
    assert ("missing_expected", str(old_file)) in observations
    assert any(row == ("new_candidate", str(brand_new)) for row in observations)
    assert stored_dest == str(old_file)


def test_validate_dest_records_missing_expected_observation(tmp_path):
    dest = tmp_path / "dest"
    dest.mkdir()
    missing = dest / "missing.jpg"

    db_path = _make_db(tmp_path)
    missing.write_bytes(b"photo" * 100)
    file_id = _seed_catalog_file(db_path, missing)
    missing.unlink()

    conn = sqlite3.connect(str(db_path))
    try:
        run_id = _start_run(conn)
        conn.commit()
    finally:
        conn.close()

    PhotoOrganizerApp(db_path).validate_dest(
        dest_root=dest,
        report_csv=tmp_path / "validation.csv",
        run_id=run_id,
    )

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(
            """
            SELECT observation_type, path
            FROM file_observations
            WHERE run_id = ? AND file_id = ?
            """,
            (run_id, file_id),
        )
        observations = cur.fetchall()
        cur = conn.execute(
            "SELECT status FROM file_location_state WHERE file_id = ? AND path = ?",
            (file_id, str(missing)),
        )
        state = cur.fetchone()[0]
    finally:
        conn.close()

    assert ("missing_expected", str(missing)) in observations
    assert state == "missing"
