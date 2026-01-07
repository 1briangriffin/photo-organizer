import pytest
from pathlib import Path
from datetime import datetime
from photo_organizer.scanning.filesystem import DiskScanner
from photo_organizer.scanning.hasher import FileHasher
from photo_organizer.models import FileRecord
from photo_organizer import config

def test_compute_file_hash(tmp_path):
    p = tmp_path / "sample.bin"
    data = b"hello world" * 10
    p.write_bytes(data)

    hasher = FileHasher()
    # Test full hash (small file)
    res = hasher.compute_hash(p, set())
    assert res.full_hash is not None
    assert not res.is_sparse

def test_scanner_iterates_and_skips(tmp_path):
    root = tmp_path
    skip_dir = root / "skip"
    skip_dir.mkdir()
    (skip_dir / "skip.txt").write_text("skip")

    sub = root / "a"
    sub.mkdir()
    (sub / "b.txt").write_text("b")
    (root / "c.txt").write_text("c")

    scanner = DiskScanner()
    # We access the internal _iter_files to verify traversal logic directly
    files = list(scanner._iter_files(root, skip_dirs={skip_dir}))
    
    assert (skip_dir / "skip.txt") not in files
    assert (root / "c.txt") in files
    assert (sub / "b.txt") in files

def test_classify_extension():
    # We can check the config map directly as the logic is now a simple lookup
    assert config.EXT_TO_TYPE.get('.cr2') == 'raw'
    assert config.EXT_TO_TYPE.get('.jpg') == 'jpeg'
    assert config.EXT_TO_TYPE.get('.mp4') == 'video'
    assert config.EXT_TO_TYPE.get('.xyz', 'other') == 'other'

def test_scanner_produces_records(monkeypatch, tmp_path):
    # Setup files
    img_path = tmp_path / "test.dng"
    img_path.write_bytes(b"rawdata")

    # Mock metadata extraction to avoid external dependencies in unit test
    dt = datetime(2020, 1, 2, 3, 4, 5)

    # Mock MetadataExtractor
    from photo_organizer.metadata.extract import MetadataExtractor
    monkeypatch.setattr(MetadataExtractor, "get_image_metadata", lambda self, p: (dt, "cam", "lens"))

    scanner = DiskScanner()
    results = list(scanner.scan(tmp_path, is_seed=False, known_sparse_hashes=set()))

    assert len(results) == 1
    rec = results[0]
    assert isinstance(rec, FileRecord)
    assert rec.type == 'raw'
    assert rec.capture_datetime == dt

def test_other_files_not_hashed(tmp_path):
    """Verify that 'other' type files (including Mac metadata files) are not hashed."""
    # Create a Mac metadata file
    mac_file = tmp_path / "._test.jpg"
    mac_file.write_bytes(b"mac metadata")

    # Create an unsupported extension
    other_file = tmp_path / "test.xyz"
    other_file.write_bytes(b"unsupported format")

    scanner = DiskScanner()
    results = list(scanner.scan(tmp_path, is_seed=False, known_sparse_hashes=set()))

    assert len(results) == 2

    for rec in results:
        assert rec.type == 'other'
        # Verify no hash was computed
        assert rec.hash is None
        assert rec.sparse_hash is None
        assert rec.hash_is_sparse is False
