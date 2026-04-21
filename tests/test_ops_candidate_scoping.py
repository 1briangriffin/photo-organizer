"""
Tests for Change 2's candidate-scoping on DB ops helpers:
fetch_primary_files, fetch_jpeg_groups, get_pending_moves_for_ids.

These are the "input filter" side of Phase B — stray rows outside the
pipeline's working set must stay invisible.
"""
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.models import FileRecord


@pytest.fixture
def db_ops():
    conn = sqlite3.connect(":memory:")
    init_schema(conn)
    yield DBOperations(conn)
    conn.close()


def _seed(db_ops, *, type_: str, ext: str, orig_name: str, dest_path: str | None,
          hash_value: str) -> int:
    """Seed a catalogued row. If dest_path is None, row is left unassigned
    (planner-visible)."""
    rec = FileRecord(
        hash=hash_value, sparse_hash=None, hash_is_sparse=False,
        type=type_, ext=ext,
        orig_name=orig_name, orig_path=Path(f"/src/{orig_name}"),
        size_bytes=100, mtime=0.0, is_seed=False, name_score=10,
        capture_datetime=datetime(2024, 5, 1, 12, 0, 0),
        camera_model=None, lens_model=None, duration_sec=None,
    )
    fid = db_ops.upsert_file_record(rec)
    db_ops.upsert_media_metadata(fid, rec)
    if dest_path is not None:
        db_ops.update_dest_path(fid, dest_path)
    db_ops.conn.commit()
    return fid


# ---------------------------------------------------------------------------
# fetch_primary_files
# ---------------------------------------------------------------------------

def test_fetch_primary_files_no_filter_returns_all_unassigned(db_ops):
    a = _seed(db_ops, type_="raw", ext=".cr2", orig_name="a.cr2",
              dest_path=None, hash_value="ha")
    b = _seed(db_ops, type_="raw", ext=".cr2", orig_name="b.cr2",
              dest_path=None, hash_value="hb")
    ids = {row[0] for row in db_ops.fetch_primary_files(candidate_ids=None)}
    assert ids == {a, b}


def test_fetch_primary_files_scoped_hides_rows_outside_candidates(db_ops):
    a = _seed(db_ops, type_="raw", ext=".cr2", orig_name="a.cr2",
              dest_path=None, hash_value="ha")
    b = _seed(db_ops, type_="raw", ext=".cr2", orig_name="b.cr2",
              dest_path=None, hash_value="hb")
    # Only `a` is in this run's working set. `b` is a leftover from some
    # prior interrupted run.
    ids = {row[0] for row in db_ops.fetch_primary_files(candidate_ids={a})}
    assert ids == {a}


def test_fetch_primary_files_empty_candidate_set_returns_nothing(db_ops):
    _seed(db_ops, type_="raw", ext=".cr2", orig_name="a.cr2",
          dest_path=None, hash_value="ha")
    assert db_ops.fetch_primary_files(candidate_ids=set()) == []


def test_fetch_primary_files_skips_already_assigned(db_ops):
    """dest_path IS NULL filter still applies alongside candidate scoping."""
    a = _seed(db_ops, type_="raw", ext=".cr2", orig_name="a.cr2",
              dest_path="/dest/a.cr2", hash_value="ha")
    # Even if caller marks it a candidate, already-assigned rows are excluded.
    assert db_ops.fetch_primary_files(candidate_ids={a}) == []


# ---------------------------------------------------------------------------
# fetch_jpeg_groups
# ---------------------------------------------------------------------------

def test_fetch_jpeg_groups_scoped_hides_rows_outside_candidates(db_ops):
    a = _seed(db_ops, type_="jpeg", ext=".jpg", orig_name="a.jpg",
              dest_path=None, hash_value="ja")
    b = _seed(db_ops, type_="jpeg", ext=".jpg", orig_name="b.jpg",
              dest_path=None, hash_value="jb")
    rows = db_ops.fetch_jpeg_groups(candidate_ids={a})
    assert {r["id"] for r in rows} == {a}


def test_fetch_jpeg_groups_unfiltered_returns_all(db_ops):
    a = _seed(db_ops, type_="jpeg", ext=".jpg", orig_name="a.jpg",
              dest_path=None, hash_value="ja")
    b = _seed(db_ops, type_="jpeg", ext=".jpg", orig_name="b.jpg",
              dest_path=None, hash_value="jb")
    rows = db_ops.fetch_jpeg_groups(candidate_ids=None)
    assert {r["id"] for r in rows} == {a, b}


# ---------------------------------------------------------------------------
# get_pending_moves_for_ids
# ---------------------------------------------------------------------------

def test_get_pending_moves_for_ids_only_returns_candidates(db_ops):
    a = _seed(db_ops, type_="raw", ext=".cr2", orig_name="a.cr2",
              dest_path="/dest/a.cr2", hash_value="ha")
    b = _seed(db_ops, type_="raw", ext=".cr2", orig_name="b.cr2",
              dest_path="/dest/b.cr2", hash_value="hb")
    rows = db_ops.get_pending_moves_for_ids({a})
    assert [r[0] for r in rows] == [a]


def test_get_pending_moves_for_ids_empty_returns_empty(db_ops):
    _seed(db_ops, type_="raw", ext=".cr2", orig_name="a.cr2",
          dest_path="/dest/a.cr2", hash_value="ha")
    assert db_ops.get_pending_moves_for_ids(set()) == []


def test_get_pending_moves_for_ids_skips_unassigned(db_ops):
    """dest_path IS NOT NULL filter still applies — a candidate id whose row
    hasn't been planned yet should not be returned."""
    a = _seed(db_ops, type_="raw", ext=".cr2", orig_name="a.cr2",
              dest_path=None, hash_value="ha")
    assert db_ops.get_pending_moves_for_ids({a}) == []
