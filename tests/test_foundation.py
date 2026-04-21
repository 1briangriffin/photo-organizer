"""
Tests for PR1 Foundation changes:
- SyncReport.confirmed_files populated on path match
- get_all_dest_files() scoped to dest_root
- import_new_files() ingest_mode skips dest_path assignment
- link_raw_outputs() called in organize pipeline
- CLI mode matrix: invalid combos rejected, src optional for dest-only modes
"""
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.models import FileRecord
from photo_organizer.sync.path_sync import DestinationSyncer, SyncReport


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_schema(c)
    yield c
    c.close()

@pytest.fixture
def db_ops(conn):
    return DBOperations(conn)


def _insert_file(db_ops, hash_val, orig_path, dest_path=None, file_type="raw"):
    rec = FileRecord(
        hash=hash_val, sparse_hash=None, hash_is_sparse=False,
        type=file_type, ext=".cr2",
        orig_name=Path(orig_path).name, orig_path=Path(orig_path),
        size_bytes=100, mtime=0.0, is_seed=False, name_score=1,
        capture_datetime=datetime(2022, 1, 1),
        camera_model=None, lens_model=None, duration_sec=None,
    )
    fid = db_ops.upsert_file_record(rec)
    if dest_path:
        db_ops.update_dest_path(fid, dest_path)
    return fid


# ---------------------------------------------------------------------------
# SyncReport.confirmed_files
# ---------------------------------------------------------------------------

def test_confirmed_files_populated_on_match(tmp_path):
    """Files found at their expected dest_path appear in confirmed_files."""
    dest = tmp_path / "dest"
    dest.mkdir()
    img = dest / "img.cr2"
    img.write_bytes(b"x" * 100)

    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    ops = DBOperations(conn)
    _insert_file(ops, "aabb01", "/src/img.cr2", str(img))
    conn.commit()

    syncer = DestinationSyncer(ops)
    report = syncer.sync_destinations([dest], apply_renames=False)

    assert str(img) in report.confirmed_files
    assert report.unchanged_count == 1


def test_confirmed_files_empty_when_missing(tmp_path):
    """A file with dest_path not on disk does not appear in confirmed_files."""
    dest = tmp_path / "dest"
    dest.mkdir()

    conn = sqlite3.connect(":memory:")
    init_schema(conn)
    ops = DBOperations(conn)
    _insert_file(ops, "aabb02", "/src/img.cr2", str(dest / "missing.cr2"))
    conn.commit()

    syncer = DestinationSyncer(ops)
    report = syncer.sync_destinations([dest], apply_renames=False)

    assert report.confirmed_files == []
    assert report.missing_count == 1


# ---------------------------------------------------------------------------
# get_all_dest_files() root scoping
# ---------------------------------------------------------------------------

def test_dest_files_scoped_to_root(tmp_path, db_ops):
    """Only files whose dest_path falls under dest_root are returned."""
    root_a = tmp_path / "root_a"
    root_b = tmp_path / "root_b"
    dest_a = str(root_a / "img_a.cr2")
    dest_b = str(root_b / "img_b.cr2")

    _insert_file(db_ops, "hash_a", "/src/a.cr2", dest_a)
    _insert_file(db_ops, "hash_b", "/src/b.cr2", dest_b)

    results = db_ops.get_all_dest_files([root_a])
    paths = [r[1] for r in results]

    assert dest_a in paths
    assert dest_b not in paths


def test_dest_files_scoped_does_not_match_prefix_sibling(tmp_path, db_ops):
    """Regression: scoping to ``root_a`` must not match ``root_ab`` siblings.

    Previously the LIKE pattern was ``<root>%`` with no separator boundary,
    so ``root_a%`` matched ``root_ab/img.jpg``. The fix uses ``<root><sep>%``
    with ESCAPE '\\'.
    """
    root_a = tmp_path / "root_a"
    root_ab = tmp_path / "root_ab"
    dest_a = str(root_a / "img_a.cr2")
    dest_ab = str(root_ab / "img_ab.cr2")

    _insert_file(db_ops, "hash_pa", "/src/a.cr2", dest_a)
    _insert_file(db_ops, "hash_pab", "/src/ab.cr2", dest_ab)

    results = db_ops.get_all_dest_files([root_a])
    paths = [r[1] for r in results]

    assert dest_a in paths
    assert dest_ab not in paths, "prefix-sharing sibling must not leak into root_a scope"


def test_dest_files_scoped_escapes_like_metacharacters(tmp_path, db_ops):
    """A directory literally named with an underscore must not act as a wildcard.

    LIKE treats ``_`` as a single-character wildcard. Without ESCAPE, a root
    like ``foo_bar`` would also match ``fooXbar``. The fix escapes ``_``, ``%``,
    and ``\\`` in the root before the LIKE comparison.
    """
    root_literal = tmp_path / "foo_bar"
    root_wildcard_match = tmp_path / "fooXbar"
    dest_literal = str(root_literal / "img1.cr2")
    dest_decoy = str(root_wildcard_match / "img2.cr2")

    _insert_file(db_ops, "hash_lit", "/src/lit.cr2", dest_literal)
    _insert_file(db_ops, "hash_dec", "/src/dec.cr2", dest_decoy)

    results = db_ops.get_all_dest_files([root_literal])
    paths = [r[1] for r in results]

    assert dest_literal in paths
    assert dest_decoy not in paths, "underscore in root name must be escaped, not treated as LIKE wildcard"


def test_dest_files_no_filter_returns_all(tmp_path, db_ops):
    """Passing no dest_roots returns all files with a dest_path."""
    _insert_file(db_ops, "hash_c", "/src/c.cr2", str(tmp_path / "root_a" / "img_c.cr2"))
    _insert_file(db_ops, "hash_d", "/src/d.cr2", str(tmp_path / "root_b" / "img_d.cr2"))

    results = db_ops.get_all_dest_files(None)
    assert len(results) == 2


def test_scoped_sync_does_not_flag_other_roots_as_missing(tmp_path):
    """Validating root_a must not report root_b files as missing."""
    root_a = tmp_path / "root_a"
    root_a.mkdir()
    img_a = root_a / "img_a.cr2"
    img_a.write_bytes(b"x" * 100)

    conn = sqlite3.connect(":memory:")
    init_schema(conn)
    ops = DBOperations(conn)
    _insert_file(ops, "hash_e", "/src/a.cr2", str(img_a))
    # root_b file — NOT on disk, should not appear in root_a scan
    _insert_file(ops, "hash_f", "/src/b.cr2", str(tmp_path / "root_b" / "img_b.cr2"))
    conn.commit()

    syncer = DestinationSyncer(ops)
    report = syncer.sync_destinations([root_a], apply_renames=False)

    assert report.missing_count == 0, "root_b file must not be reported as missing in root_a scan"
    assert report.unchanged_count == 1


# ---------------------------------------------------------------------------
# import_new_files() ingest_mode
# ---------------------------------------------------------------------------

def test_ingest_mode_skips_dest_path(tmp_path):
    """ingest_mode=True: imported file has dest_path IS NULL after import."""
    src = tmp_path / "img.cr2"
    src.write_bytes(b"RAW" * 500)

    conn = sqlite3.connect(":memory:")
    init_schema(conn)
    ops = DBOperations(conn)
    syncer = DestinationSyncer(ops)
    report = SyncReport()

    syncer.import_new_files([str(src)], report, dry_run=False, ingest_mode=True)

    cur = conn.cursor()
    cur.execute("SELECT dest_path FROM files")
    row = cur.fetchone()
    assert row is not None
    assert row[0] is None, "ingest_mode must leave dest_path NULL"


def test_normal_import_sets_dest_path(tmp_path):
    """ingest_mode=False (default): imported file gets dest_path set to its current location."""
    src = tmp_path / "img.cr2"
    src.write_bytes(b"RAW" * 500)

    conn = sqlite3.connect(":memory:")
    init_schema(conn)
    ops = DBOperations(conn)
    syncer = DestinationSyncer(ops)
    report = SyncReport()

    syncer.import_new_files([str(src)], report, dry_run=False, ingest_mode=False)

    cur = conn.cursor()
    cur.execute("SELECT dest_path FROM files")
    row = cur.fetchone()
    assert row is not None
    assert row[0] == str(src), "normal import must set dest_path to current file location"


# ---------------------------------------------------------------------------
# CLI mode matrix
# ---------------------------------------------------------------------------

def _parse(args):
    """Run parse_args with patched sys.argv; returns parsed namespace or raises SystemExit."""
    import sys
    from photo_organizer.main import parse_args
    old = sys.argv
    sys.argv = ["photo-organizer"] + args
    try:
        return parse_args()
    finally:
        sys.argv = old


def test_src_required_for_organize(tmp_path):
    with pytest.raises(SystemExit):
        _parse([str(tmp_path)])  # only dest, no src


def test_src_optional_for_sync_dest(tmp_path):
    args = _parse([str(tmp_path), "--sync-dest"])
    assert args.sync_dest is True
    assert args.src is None


def test_src_optional_for_validate_dest(tmp_path):
    args = _parse([str(tmp_path), "--validate-dest"])
    assert args.validate_dest is True


def test_src_optional_for_ingest_dest(tmp_path):
    args = _parse([str(tmp_path), "--ingest-dest"])
    assert args.ingest_dest is True


def test_modes_mutually_exclusive(tmp_path):
    with pytest.raises(SystemExit):
        _parse([str(tmp_path), "--report", "--sync-dest"])
    with pytest.raises(SystemExit):
        _parse([str(tmp_path), "--validate-dest", "--ingest-dest"])


def test_move_rejected_with_sync_dest(tmp_path):
    with pytest.raises(SystemExit):
        _parse([str(tmp_path), "--sync-dest", "--move"])


def test_move_rejected_with_validate_dest(tmp_path):
    with pytest.raises(SystemExit):
        _parse([str(tmp_path), "--validate-dest", "--move"])


def test_import_new_requires_sync_dest(tmp_path):
    with pytest.raises(SystemExit):
        _parse([str(tmp_path / "src"), str(tmp_path), "--import-new"])


def test_auto_sync_rejected_with_dest_only_modes(tmp_path):
    with pytest.raises(SystemExit):
        _parse([str(tmp_path), "--sync-dest", "--auto-sync"])
