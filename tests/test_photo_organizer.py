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
