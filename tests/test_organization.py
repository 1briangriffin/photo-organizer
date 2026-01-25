import pytest
from pathlib import Path
from datetime import datetime
from photo_organizer.models import FileRecord
from photo_organizer.core import PhotoOrganizerApp
from photo_organizer.organization.rules import DestinationPlanner
from photo_organizer.organization.mover import FileMover
from photo_organizer.metadata.linking import FileLinker

def test_decide_dest_defers_creation(db_ops, tmp_path):
    dest_root = tmp_path / "dest"
    
    rec = FileRecord(
        hash="h1", type="raw", ext=".dng",
        orig_name="img.dng", orig_path=Path("/src/img.dng"),
        size_bytes=10, is_seed=False, name_score=1,
        capture_datetime=datetime(2021, 1, 1)
    )
    db_ops.upsert_file_record(rec)
    db_ops.upsert_media_metadata(1, rec)

    planner = DestinationPlanner(db_ops)
    planner.plan_all(dest_root)

    # Check DB
    cur = db_ops.conn.cursor()
    cur.execute("SELECT dest_path FROM files WHERE id=1")
    dest = cur.fetchone()[0]
    
    assert dest is not None
    assert "2021" in dest
    # Verify folders were NOT created yet
    assert not Path(dest).parent.exists()

def test_jpeg_grouping(db_ops, tmp_path):
    dest_root = tmp_path / "dest"
    dt = datetime(2021, 1, 1)

    # Main JPEG
    rec1 = FileRecord(
        hash="h_main", type="jpeg", ext=".jpg",
        orig_name="img.jpg", orig_path=Path("/src/img.jpg"),
        size_bytes=10, is_seed=False, name_score=1,
        capture_datetime=dt, width=2000, height=2000
    )
    # Small Resize
    rec2 = FileRecord(
        hash="h_small", type="jpeg", ext=".jpg",
        orig_name="img (1).jpg", orig_path=Path("/src/img (1).jpg"),
        size_bytes=5, is_seed=False, name_score=0,
        capture_datetime=dt, width=100, height=100
    )

    for r in [rec1, rec2]:
        fid = db_ops.upsert_file_record(r)
        db_ops.upsert_media_metadata(fid, r)

    planner = DestinationPlanner(db_ops)
    planner.plan_all(dest_root)

    cur = db_ops.conn.cursor()
    # Check paths
    cur.execute("SELECT dest_path FROM files ORDER BY id")
    rows = cur.fetchall()
    
    main_dest = rows[0][0]
    small_dest = rows[1][0]

    assert "_resized_" not in main_dest
    assert "_resized_" in small_dest

def test_sidecar_linking(db_ops, tmp_path):
    # Setup RAW and Sidecar
    raw = FileRecord(
        hash="raw_h", type="raw", ext=".dng",
        orig_name="img.dng", orig_path=Path("/src/img.dng"),
        size_bytes=10, is_seed=False, name_score=1,
        capture_datetime=datetime(2021, 1, 1)
    )
    sidecar = FileRecord(
        hash="xmp_h", type="sidecar", ext=".xmp",
        orig_name="img.xmp", orig_path=Path("/src/img.xmp"),
        size_bytes=1, is_seed=False, name_score=0
    )

    db_ops.upsert_file_record(raw)
    db_ops.upsert_file_record(sidecar)

    linker = FileLinker(db_ops)
    linker.link_raw_sidecars()

    cur = db_ops.conn.cursor()
    cur.execute("SELECT * FROM raw_sidecars")
    assert cur.fetchone() is not None

def test_mover_execute(db_ops, tmp_path):
    src = tmp_path / "test.file"
    src.write_text("content")
    dest = tmp_path / "dest" / "test.file"

    # Seed DB with a pending move
    rec = FileRecord(
        hash="h1", type="other", ext=".file",
        orig_name="test.file", orig_path=src,
        size_bytes=7, is_seed=False, name_score=1,
        capture_datetime=datetime.now()
    )
    fid = db_ops.upsert_file_record(rec)
    db_ops.update_dest_path(fid, str(dest))

    mover = FileMover(db_ops)
    mover.execute(move_mode=False, dry_run=False)

    assert dest.exists()
    assert dest.read_text() == "content"

    cur = db_ops.conn.cursor()
    cur.execute("SELECT path, hash_is_sparse FROM file_occurrences WHERE path = ?", (str(dest),))
    row = cur.fetchone()
    assert row is not None
    assert row[0] == str(dest)
    assert row[1] == 0


def test_linked_psd_prefers_output_folder(db_ops, tmp_path):
    dest_root = tmp_path / "dest"
    dt = datetime(2021, 1, 1)

    raw = FileRecord(
        hash="raw_h", type="raw", ext=".dng",
        orig_name="img.dng", orig_path=Path("/src/img.dng"),
        size_bytes=10, is_seed=False, name_score=1,
        capture_datetime=dt
    )
    psd = FileRecord(
        hash="psd_h", type="psd", ext=".psd",
        orig_name="img.psd", orig_path=Path("/src/img.psd"),
        size_bytes=15, is_seed=False, name_score=1,
        capture_datetime=dt
    )

    raw_id = db_ops.upsert_file_record(raw)
    db_ops.upsert_media_metadata(raw_id, raw)

    psd_id = db_ops.upsert_file_record(psd)
    db_ops.upsert_media_metadata(psd_id, psd)

    linker = FileLinker(db_ops)
    linker.link_psds()

    planner = DestinationPlanner(db_ops)
    planner.plan_all(dest_root)

    PhotoOrganizerApp(Path("dummy.db"))._assign_linked_destinations(db_ops)

    cur = db_ops.conn.cursor()
    cur.execute("SELECT dest_path FROM files WHERE id = ?", (psd_id,))
    psd_dest = cur.fetchone()[0]

    assert psd_dest is not None
    dest_parts = [part.lower() for part in Path(psd_dest).parts]
    assert "output" in dest_parts
    assert "raw" not in dest_parts


def test_auto_sync_detects_renamed_files(db_ops, tmp_path):
    """Verify that auto_sync detects and updates renamed files after organization."""
    import hashlib

    dest_root = tmp_path / "dest"
    dest_dir = dest_root / "output" / "2024" / "2024-01"
    dest_dir.mkdir(parents=True)

    # Create a file already in the destination with a renamed name
    file_content = b"test jpeg content for auto sync"
    content_hash = hashlib.sha256(file_content).hexdigest()
    renamed_file = dest_dir / "Vacation_Photo.jpg"
    renamed_file.write_bytes(file_content)

    # Add the file to database with OLD name (simulating it was renamed on disk)
    rec = FileRecord(
        hash=content_hash,
        type="jpeg",
        ext=".jpg",
        orig_name="IMG_1234.jpg",
        orig_path=Path("/src/IMG_1234.jpg"),
        size_bytes=len(file_content),
        is_seed=False,
        name_score=50,
        capture_datetime=datetime(2024, 1, 15, 10, 30, 0),
    )
    file_id = db_ops.upsert_file_record(rec)
    db_ops.upsert_media_metadata(file_id, rec)
    old_path = str(dest_dir / "IMG_1234.jpg")
    db_ops.update_dest_path(file_id, old_path)
    db_ops.conn.commit()

    # Import and run _run_auto_sync directly (to avoid full organize pipeline)
    app = PhotoOrganizerApp(Path("dummy.db"))
    app._run_auto_sync(db_ops, dest_root, db_ops.conn)
    db_ops.conn.commit()

    # Verify the path was updated
    cur = db_ops.conn.cursor()
    cur.execute("SELECT dest_path FROM files WHERE id = ?", (file_id,))
    updated_path = cur.fetchone()[0]

    assert updated_path == str(renamed_file)
    assert "Vacation_Photo.jpg" in updated_path
