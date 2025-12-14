import sqlite3
import pytest
from pathlib import Path
from datetime import datetime

# Import the query tool being tested
import photo_catalog_query as pcq

# Import the NEW structure components
from photo_organizer.models import FileRecord
from photo_organizer.database.schema import init_schema
from photo_organizer.database.ops import DBOperations


@pytest.fixture
def conn(tmp_path):
    """Fixture to create a fresh DB connection for each test."""
    db_path = tmp_path / "db.sqlite"
    c = sqlite3.connect(db_path)
    init_schema(c)  # Use new init_schema
    try:
        yield c
    finally:
        c.close()

@pytest.fixture
def db_ops(conn):
    """Fixture to provide the database operations wrapper."""
    return DBOperations(conn)


def test_connect_db_missing(tmp_path):
    missing = tmp_path / "none.db"
    with pytest.raises(SystemExit):
        pcq.connect_db(missing)


def test_resolve_raw_id_and_listing(conn, db_ops, capsys):
    dt_obj = datetime.fromisoformat("2021-01-01T12:00:00")

    # Use FileRecord directly from models
    raw = FileRecord(
        hash="h1",
        type="raw",
        ext=".dng",
        orig_name="raw1.dng",
        orig_path=Path("/src/raw1.dng"),
        size_bytes=1,
        is_seed=False,
        name_score=1,
        capture_datetime=dt_obj,
        camera_model="cam",
    )
    out = FileRecord(
        hash="h2",
        type="jpeg",
        ext=".jpg",
        orig_name="raw1.jpg",
        orig_path=Path("/src/raw1.jpg"),
        size_bytes=1,
        is_seed=False,
        name_score=1,
        capture_datetime=dt_obj,
        camera_model="cam",
    )

    # Use db_ops methods instead of global po.* functions
    raw_id = db_ops.upsert_file_record(raw)
    db_ops.upsert_media_metadata(raw_id, raw)
    
    out_id = db_ops.upsert_file_record(out)
    db_ops.upsert_media_metadata(out_id, out)
    
    conn.execute(
        "INSERT INTO raw_outputs (raw_file_id, output_file_id, link_method, confidence) VALUES (?, ?, ?, ?)",
        (raw_id, out_id, "test", 100),
    )
    conn.commit()

    # _resolve_raw_id_from_path should find by orig_path and dest_path
    assert pcq._resolve_raw_id_from_path(conn, Path(raw.orig_path)) == raw_id
    conn.execute("UPDATE files SET dest_path = ? WHERE id = ?", ("/dest/raw1.dng", raw_id))
    conn.commit()
    assert pcq._resolve_raw_id_from_path(conn, Path("/dest/raw1.dng")) == raw_id

    pcq.list_unprocessed_raws(conn)
    out_lines = capsys.readouterr().out
    # Should not list the processed raw (linked)
    assert str(raw_id) not in out_lines


def test_show_raw_details_and_unknown_files(conn, db_ops, capsys):
    dt_obj = datetime.fromisoformat("2022-02-02T10:00:00")

    raw = FileRecord(
        hash="h1",
        type="raw",
        ext=".dng",
        orig_name="raw2.dng",
        orig_path=Path("/src/raw2.dng"),
        size_bytes=1,
        is_seed=False,
        name_score=1,
        capture_datetime=dt_obj,
        camera_model="cam2",
        lens_model="lens",
    )
    
    raw_id = db_ops.upsert_file_record(raw)
    db_ops.upsert_media_metadata(raw_id, raw)

    other = FileRecord(
        hash="h3",
        type="other",
        ext=".bin",
        orig_name="file.bin",
        orig_path=Path("/src/file.bin"),
        size_bytes=5,
        is_seed=False,
        name_score=0,
    )
    db_ops.upsert_file_record(other)

    pcq.show_raw_details(conn, raw_id)
    out = capsys.readouterr().out
    assert "RAW file:" in out
    assert "cam2" in out
    assert "lens" in out

    pcq.list_unknown_files(conn)
    out_unknown = capsys.readouterr().out
    assert "file.bin" in out_unknown
