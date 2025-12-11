import os
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest
from PIL import Image

import photo_organizer as po


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    po.init_db(c)
    try:
        yield c
    finally:
        c.close()


def test_compute_file_hash_and_handle(tmp_path):
    p = tmp_path / "sample.bin"
    data = b"hello world" * 10
    p.write_bytes(data)

    h1 = po.compute_file_hash(p)
    with open(p, "rb") as f:
        h2 = po.compute_file_hash_from_handle(f)
    assert h1 == h2


def test_iter_files_scandir_orders_and_skips(tmp_path):
    root = tmp_path
    skip_dir = root / "skip"
    skip_dir.mkdir()
    (skip_dir / "skip.txt").write_text("skip")

    sub = root / "a"
    sub.mkdir()
    (sub / "b.txt").write_text("b")
    (root / "c.txt").write_text("c")

    files = list(po.iter_files_scandir(root, skip_dest=skip_dir))
    assert (skip_dir / "skip.txt") not in files
    assert files == [root / "c.txt", sub / "b.txt"]


@pytest.mark.parametrize(
    "name,expected",
    [
        ("file.cr2", "raw"),
        ("photo.JPG", "jpeg"),
        ("clip.MP4", "video"),
        ("layout.psd", "psd"),
        ("scan.tiff", "tiff"),
        ("meta.xmp", "sidecar"),
        ("._hidden", "other"),
        ("unknown.xyz", "other"),
    ],
)
def test_classify_extension(name, expected, tmp_path):
    p = tmp_path / name
    p.write_text("data")
    assert po.classify_extension(p) == expected


def test_normalize_stem_and_descriptiveness():
    assert po.normalize_stem_for_grouping("IMG_0001 (1)") == "img_0001"
    assert po.descriptiveness_score("IMG_0001") < po.descriptiveness_score("family-vacation-2023")


def test_gather_file_record_single_open(monkeypatch, tmp_path):
    img_path = tmp_path / "test.dng"
    img_path.write_bytes(b"rawdata")
    dt = datetime(2020, 1, 2, 3, 4, 5)

    monkeypatch.setattr(po, "get_image_metadata_exif", lambda path, fileobj=None: (dt, "cam", "lens"))
    calls = []

    def fake_hash(fh, chunk_size=po.DEFAULT_HASH_CHUNK_SIZE):
        calls.append(fh.tell())
        fh.seek(0)
        return "fakehash"

    monkeypatch.setattr(po, "compute_file_hash_from_handle", fake_hash)

    rec = po.gather_file_record(img_path, "raw", is_seed=False, use_phash=False)

    assert rec.hash == "fakehash"
    assert rec.capture_datetime == dt
    assert calls, "hash function should be invoked"


def test_scan_tree_populates_db_and_sidecar_index(monkeypatch, tmp_path, conn):
    root = tmp_path / "src"
    root.mkdir()
    raw = root / "shot.dng"
    raw.write_bytes(b"raw")
    sidecar = root / "shot.xmp"
    sidecar.write_text("meta")
    video = root / "clip.mp4"
    video.write_bytes(b"video")

    jpeg = root / "photo.jpg"
    with Image.new("RGB", (10, 10), color="red") as im:
        im.save(jpeg)

    dt = datetime(2020, 5, 6, 7, 8, 9)
    monkeypatch.setattr(po, "get_image_metadata_exif", lambda path, fileobj=None: (dt, "cam", "lens"))
    monkeypatch.setattr(po, "get_video_metadata", lambda path: (dt, 1.23))

    index = po.scan_tree(conn, root, is_seed=False, use_phash=False)

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM files")
    assert cur.fetchone()[0] == 4

    key = (raw.parent, raw.stem.lower())
    assert key in index
    assert index[key]["raw"]
    assert index[key]["sidecar"]


def test_decide_dest_for_file_defers_dirs(conn, tmp_path):
    dest_root = tmp_path / "dest"
    dest_root.mkdir()
    dt = datetime(2021, 1, 1, 12, 0, 0)

    rec1 = po.FileRecord(
        hash="h1",
        type="raw",
        ext=".dng",
        orig_name="img.dng",
        orig_path=Path("/src/img.dng"),
        size_bytes=1,
        is_seed=False,
        name_score=1,
        capture_datetime=dt,
    )
    rec2 = po.FileRecord(
        hash="h2",
        type="raw",
        ext=".dng",
        orig_name="img.dng",
        orig_path=Path("/src/img_copy.dng"),
        size_bytes=1,
        is_seed=False,
        name_score=1,
        capture_datetime=dt,
    )

    id1 = po.upsert_file_record(conn, rec1)
    po.upsert_media_metadata(conn, id1, rec1)
    id2 = po.upsert_file_record(conn, rec2)
    po.upsert_media_metadata(conn, id2, rec2)

    po.decide_dest_for_file(conn, dest_root)

    cur = conn.cursor()
    cur.execute("SELECT dest_path FROM files WHERE id IN (?, ?)", (id1, id2))
    dests = [Path(row[0]) for row in cur.fetchall()]
    assert len(set(dests)) == 2

    dest_dirs = {p.parent for p in dests}
    for d in dest_dirs:
        assert not d.exists(), "dest directories should not be created during planning"


def test_copy_or_move_files_dry_run_and_real(conn, tmp_path):
    src = tmp_path / "src.bin"
    src.write_bytes(b"data")
    dest = tmp_path / "out" / "dest.bin"

    rec = po.FileRecord(
        hash="hcopy",
        type="other",
        ext=".bin",
        orig_name=src.name,
        orig_path=src,
        size_bytes=src.stat().st_size,
        is_seed=False,
        name_score=0,
    )
    fid = po.upsert_file_record(conn, rec)
    conn.execute("UPDATE files SET dest_path = ? WHERE id = ?", (str(dest), fid))
    conn.commit()

    po.copy_or_move_files(conn, move=False, dry_run=True)
    assert not dest.exists()

    po.copy_or_move_files(conn, move=False, dry_run=False)
    assert dest.exists()
    assert dest.read_bytes() == b"data"


def test_build_raw_output_links(conn):
    dt = datetime(2022, 2, 2, 12, 0, 0)
    raw = po.FileRecord(
        hash="hraw",
        type="raw",
        ext=".dng",
        orig_name="IMG_0001.dng",
        orig_path=Path("/src/IMG_0001.dng"),
        size_bytes=1,
        is_seed=False,
        name_score=1,
        capture_datetime=dt,
        camera_model="cam",
    )
    out = po.FileRecord(
        hash="hout",
        type="jpeg",
        ext=".jpg",
        orig_name="IMG_0001.JPG",
        orig_path=Path("/src/IMG_0001.JPG"),
        size_bytes=1,
        is_seed=False,
        name_score=1,
        capture_datetime=dt,
        camera_model="cam",
    )

    raw_id = po.upsert_file_record(conn, raw)
    po.upsert_media_metadata(conn, raw_id, raw)
    out_id = po.upsert_file_record(conn, out)
    po.upsert_media_metadata(conn, out_id, out)

    po.build_raw_output_links(conn, use_phash=False)
    cur = conn.cursor()
    cur.execute("SELECT raw_file_id, output_file_id, link_method FROM raw_outputs")
    rows = cur.fetchall()
    assert rows == [(raw_id, out_id, "filename_time")]


# ==================== DEDUPLICATION & EXACTLY-ONCE GUARANTEES ====================

def test_deduplication_same_hash_skipped(conn):
    """Verify that when scanning the same file twice (same hash), only one record exists."""
    dt = datetime(2022, 1, 1, 12, 0, 0)
    
    # Two records with same hash but different paths
    rec1 = po.FileRecord(
        hash="identical",
        type="raw",
        ext=".dng",
        orig_name="photo1.dng",
        orig_path=Path("/src1/photo1.dng"),
        size_bytes=1000,
        is_seed=False,
        name_score=5,
        capture_datetime=dt,
    )
    rec2 = po.FileRecord(
        hash="identical",  # same hash!
        type="raw",
        ext=".dng",
        orig_name="photo2.dng",
        orig_path=Path("/src2/photo2.dng"),
        size_bytes=1000,
        is_seed=False,
        name_score=10,  # higher name score should become canonical
        capture_datetime=dt,
    )
    
    id1 = po.upsert_file_record(conn, rec1)
    id2 = po.upsert_file_record(conn, rec2)
    
    # Should reuse the same ID
    assert id1 == id2, "Same hash should produce same file_id"
    
    # Verify canonical record has higher name_score
    cur = conn.cursor()
    cur.execute("SELECT orig_name, name_score FROM files WHERE id = ?", (id1,))
    row = cur.fetchone()
    assert row[1] == 10, "Higher name_score should be canonical"
    assert row[0] == "photo2.dng", "Canonical name should be the one with higher score"


def test_orphan_jpeg_grouping_no_duplication(conn, tmp_path):
    """Verify JPEGs with no RAW are grouped correctly and each ends up exactly once."""
    dest_root = tmp_path / "dest"
    dest_root.mkdir()
    
    dt = datetime(2023, 6, 15, 14, 30, 45)
    
    # Three JPEGs: same capture time, same normalized stem, different resolutions
    jpegs = [
        po.FileRecord(
            hash="jpeg_high_res",
            type="jpeg",
            ext=".jpg",
            orig_name="vacation_001.jpg",
            orig_path=Path("/src/vacation_001.jpg"),
            size_bytes=2000000,
            is_seed=False,
            name_score=8,
            capture_datetime=dt,
            width=4000,
            height=3000,
        ),
            po.FileRecord(
                hash="jpeg_low_res",
                type="jpeg",
                ext=".jpg",
                orig_name="vacation_001 (2).jpg",
                orig_path=Path("/src/vacation_001 (2).jpg"),
            size_bytes=500000,
            is_seed=False,
            name_score=5,
            capture_datetime=dt,
            width=1000,
            height=750,
        ),
        po.FileRecord(
            hash="jpeg_medium_res",
            type="jpeg",
            ext=".jpg",
            orig_name="vacation_001 (1).jpg",
            orig_path=Path("/src/vacation_001 (1).jpg"),
            size_bytes=1500000,
            is_seed=False,
            name_score=7,
            capture_datetime=dt,
            width=3000,
            height=2250,
        ),
    ]
    
    jpeg_ids = []
    for rec in jpegs:
        fid = po.upsert_file_record(conn, rec)
        po.upsert_media_metadata(conn, fid, rec)
        jpeg_ids.append(fid)
    
    po.decide_dest_for_file(conn, dest_root)
    
    # Verify all three JPEGs got different dest_paths
    cur = conn.cursor()
    cur.execute(
        "SELECT id, dest_path FROM files WHERE id IN (?, ?, ?) ORDER BY id",
        tuple(jpeg_ids),
    )
    rows = cur.fetchall()
    
    assert len(rows) == 3, "All three JPEGs should have dest_paths"
    dest_paths = [Path(row[1]) for row in rows]
    assert len(set(dest_paths)) == 3, "Each JPEG should have a unique dest_path"
    
    # Highest resolution should be main (no '_resized_' marker) and others should include '_resized_' in filename
    for row in rows:
        fid, dest_path_str = row
        dest_path = Path(dest_path_str)
        name = dest_path.name
        if fid == jpeg_ids[0]:  # highest res (4000x3000)
            assert "_resized_" not in name, "Highest res JPEG should be main version"
        else:
            assert "_resized_" in name, "Lower res JPEGs should have '_resized_' in filename"


def test_sidecar_not_copied_orphan(conn, tmp_path):
    """Verify sidecars are copied to same folder as their linked RAW files."""
    dest_root = tmp_path / "dest"
    dest_root.mkdir()
    
    raw = po.FileRecord(
        hash="raw_hash",
        type="raw",
        ext=".dng",
        orig_name="photo.dng",
        orig_path=Path("/src/photo.dng"),
        size_bytes=1000,
        is_seed=False,
        name_score=1,
        capture_datetime=datetime(2023, 1, 1),
    )
    sidecar = po.FileRecord(
        hash="sidecar_hash",
        type="sidecar",
        ext=".xmp",
        orig_name="photo.xmp",
        orig_path=Path("/src/photo.xmp"),
        size_bytes=100,
        is_seed=False,
        name_score=0,
    )
    
    raw_id = po.upsert_file_record(conn, raw)
    po.upsert_media_metadata(conn, raw_id, raw)
    sidecar_id = po.upsert_file_record(conn, sidecar)
    
    # Link sidecar to RAW
    conn.execute(
        "INSERT INTO raw_sidecars (raw_file_id, sidecar_file_id) VALUES (?, ?)",
        (raw_id, sidecar_id),
    )
    conn.commit()

    po.decide_dest_for_file(conn, dest_root)
    po.assign_sidecar_destinations(conn)
    po.decide_dest_for_file(conn, dest_root)
    
    # RAW should have dest_path
    cur = conn.cursor()
    cur.execute("SELECT dest_path FROM files WHERE id = ?", (raw_id,))
    assert cur.fetchone()[0] is not None, "RAW should have dest_path"

    # Sidecar should now have dest_path in same folder as RAW
    cur.execute("SELECT dest_path FROM files WHERE id = ?", (sidecar_id,))
    sidecar_dest = cur.fetchone()[0]
    assert sidecar_dest is not None, "Linked sidecar should have dest_path"
    
    # Verify sidecar is in same folder as RAW
    cur.execute("SELECT dest_path FROM files WHERE id = ?", (raw_id,))
    raw_dest = cur.fetchone()[0]
    assert Path(sidecar_dest).parent == Path(raw_dest).parent, "Sidecar should be in same folder as RAW"
    assert Path(sidecar_dest).name == "photo.xmp", "Sidecar should preserve its original name"


def test_unknown_files_not_copied(conn, tmp_path):
    """Verify 'other' type files are cataloged but never copied."""
    dest_root = tmp_path / "dest"
    dest_root.mkdir()
    
    unknown = po.FileRecord(
        hash="unknown_hash",
        type="other",
        ext=".xyz",
        orig_name="mystery.xyz",
        orig_path=Path("/src/mystery.xyz"),
        size_bytes=1000,
        is_seed=False,
        name_score=0,
    )
    
    unk_id = po.upsert_file_record(conn, unknown)
    po.decide_dest_for_file(conn, dest_root)
    
    # Unknown file should not have dest_path
    cur = conn.cursor()
    cur.execute("SELECT dest_path FROM files WHERE id = ?", (unk_id,))
    assert cur.fetchone()[0] is None, "Unknown (type='other') should not have dest_path"


def test_end_to_end_deduplication_single_copy(tmp_path, monkeypatch, conn):
    """
    Comprehensive integration test: scan mixed files, verify each ends up in dest exactly once.
    This is the critical test for the exactly-once guarantee.
    """
    src_root = tmp_path / "src"
    src_root.mkdir()
    dest_root = tmp_path / "dest"
    dest_root.mkdir()
    
    # Create test files
    raw_file = src_root / "photo.dng"
    raw_file.write_bytes(b"rawdata" * 100)
    
    jpeg_file = src_root / "photo.jpg"
    with Image.new("RGB", (800, 600), color="blue") as im:
        im.save(jpeg_file)
    
    video_file = src_root / "clip.mp4"
    video_file.write_bytes(b"videodata" * 100)
    
    sidecar_file = src_root / "photo.xmp"
    sidecar_file.write_text("<xmp>metadata</xmp>")
    
    unknown_file = src_root / "readme.txt"
    unknown_file.write_text("unknown")
    
    # Mock metadata extraction
    dt = datetime(2024, 3, 15, 10, 30, 0)
    monkeypatch.setattr(po, "get_image_metadata_exif", lambda path, fileobj=None: (dt, "Canon", "50mm"))
    monkeypatch.setattr(po, "get_video_metadata", lambda path: (dt, 5.0))
    
    # Scan
    index = po.scan_tree(conn, src_root, is_seed=False, use_phash=False)
    
    # Verify 5 files in catalog
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM files")
    assert cur.fetchone()[0] == 5, "Should have 5 files cataloged"
    
    # Plan destinations
    
    # Link sidecars to RAWs using the index
    po.link_raw_sidecars_from_index(conn, index)

    # Decide destinations for files, then assign sidecar destinations based on RAW dests
    po.decide_dest_for_file(conn, dest_root)
    po.assign_sidecar_destinations(conn)
    
    # Verify only copyable types have dest_path
    cur.execute("SELECT type, COUNT(*) FROM files WHERE dest_path IS NOT NULL GROUP BY type")
    type_counts = dict(cur.fetchall())
    assert type_counts.get("raw") == 1, "1 RAW should have dest_path"
    assert type_counts.get("jpeg") == 1, "1 JPEG should have dest_path"
    assert type_counts.get("video") == 1, "1 VIDEO should have dest_path"
    assert type_counts.get("sidecar") == 1, "1 linked sidecar should have dest_path"
    assert "other" not in type_counts, "0 unknown files should have dest_path"
    
    # Copy files (not dry-run)
    po.copy_or_move_files(conn, move=False, dry_run=False)
    
    # Count files in dest tree
    copied_files = list(dest_root.rglob("*"))
    copied_files = [f for f in copied_files if f.is_file()]  # exclude dirs
    assert len(copied_files) == 4, f"Should have 4 copied files (RAW + sidecar + JPEG + video), got {len(copied_files)}: {copied_files}"
    
    # Verify directory structure
    raw_dir = dest_root / "raw"
    output_dir = dest_root / "output"
    
    assert raw_dir.exists(), "raw/ dir should exist"
    assert output_dir.exists(), "output/ dir should exist"
    
    raw_files = list(raw_dir.rglob("*"))
    raw_files = [f for f in raw_files if f.is_file()]
    assert len(raw_files) == 2, f"Should have 2 files in raw/ (RAW + sidecar), got {len(raw_files)}"
    
    out_files = list(output_dir.rglob("*"))
    out_files = [f for f in out_files if f.is_file()]
    assert len(out_files) == 2, f"Should have 2 files in output/ (JPEG + video), got {len(out_files)}"

def test_sidecar_orphan_not_copied(conn, tmp_path):
    """Verify orphan sidecars (not linked to any RAW) are cataloged but not copied."""
    dest_root = tmp_path / "dest"
    dest_root.mkdir()
    
    sidecar = po.FileRecord(
        hash="orphan_sidecar_hash",
        type="sidecar",
        ext=".xmp",
        orig_name="orphan.xmp",
        orig_path=Path("/src/orphan.xmp"),
        size_bytes=100,
        is_seed=False,
        name_score=0,
    )
    
    sidecar_id = po.upsert_file_record(conn, sidecar)
    
    po.decide_dest_for_file(conn, dest_root)
    po.assign_sidecar_destinations(conn)
    
    # Orphan sidecar should not have dest_path
    cur = conn.cursor()
    cur.execute("SELECT dest_path FROM files WHERE id = ?", (sidecar_id,))
    assert cur.fetchone()[0] is None, "Orphan sidecar (no RAW link) should not have dest_path"

