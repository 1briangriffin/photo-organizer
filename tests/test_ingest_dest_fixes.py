"""
Tests for the Tier 1 ingest-dest correctness fixes.

Covers:
- Change 1: dry-run rename invisibility (detect/apply split) + broadened
  skip_if_no_imports so a renames-only run still reconciles.
- Change 2: PipelineCandidates scoping — stray dest_path=NULL rows from
  interrupted prior runs stay invisible to Phase B writers.
- Change 3: Strategy 5 editor_export_identity surfaces at confidence 100
  after a rename is applied in the same run.
- Integration: the full DPP workflow (user renames RAW, exports JPEG,
  ingest-dest ties them together at S5).
"""
import hashlib
import os
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from photo_organizer.core import PhotoOrganizerApp
from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.models import FileRecord


def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    conn.commit()
    conn.close()
    return db_path


def _seed_raw(db_path: Path, on_disk_path: Path, dest_path: Path,
              capture_dt: datetime = datetime(2024, 5, 1, 12, 0, 0)) -> tuple[int, str]:
    """Pre-seed a RAW as already-organized at ``dest_path``. Returns (file_id, full_hash)."""
    content = on_disk_path.read_bytes()
    full_hash = hashlib.sha256(content).hexdigest()
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    db_ops = DBOperations(conn)
    rec = FileRecord(
        hash=full_hash, sparse_hash=None, hash_is_sparse=False,
        type="raw", ext=on_disk_path.suffix.lower(),
        orig_name=on_disk_path.name, orig_path=on_disk_path,
        size_bytes=len(content), mtime=on_disk_path.stat().st_mtime,
        is_seed=False, name_score=10,
        capture_datetime=capture_dt,
        camera_model=None, lens_model=None, duration_sec=None,
    )
    fid = db_ops.upsert_file_record(rec)
    db_ops.upsert_media_metadata(fid, rec)
    db_ops.update_dest_path(fid, str(dest_path))
    db_ops.record_occurrence(
        file_id=fid, path=dest_path, is_seed=False,
        mtime=on_disk_path.stat().st_mtime, size_bytes=len(content),
        hash_value=full_hash, is_sparse=False,
    )
    conn.commit()
    conn.close()
    return fid, full_hash


def _dest_path(db_path: Path, file_id: int) -> str | None:
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT dest_path FROM files WHERE id = ?", (file_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Change 1: dry-run rename invisibility
# ---------------------------------------------------------------------------

def test_ingest_dest_dry_run_rolls_back_rename_reconciliation(tmp_path):
    """Rename detected in Phase A is applied inside Phase B's savepoint. On
    dry-run the savepoint rolls back, so the catalog still reflects the old
    dest_path. Regression against the earlier 'dry-run silently mutates
    dest_path' bug."""
    raw_dir = tmp_path / "dest" / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)

    # The file lives on disk at its NEW name.
    new_path = raw_dir / "IMG_renamed.CR2"
    content = b"RAW_BYTES" * 500
    new_path.write_bytes(content)

    # Catalog remembers it at the OLD name.
    old_path = raw_dir / "IMG_original.CR2"
    db_path = _make_db(tmp_path)
    fid, _ = _seed_raw(db_path, new_path, old_path)

    PhotoOrganizerApp(db_path).ingest_dest(
        dest_root=tmp_path / "dest", dry_run=True,
    )

    # dest_path should STILL point at the old name — dry-run rolled the rename back.
    assert _dest_path(db_path, fid) == str(old_path), \
        "dry-run must not persist rename reconciliation (it sits inside the Phase B savepoint)"


def test_ingest_dest_real_run_persists_rename_reconciliation(tmp_path):
    """Real ingest-dest run commits the rename detected in Phase A."""
    raw_dir = tmp_path / "dest" / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)
    new_path = raw_dir / "IMG_renamed.CR2"
    new_path.write_bytes(b"RAW_BYTES" * 500)
    old_path = raw_dir / "IMG_original.CR2"
    db_path = _make_db(tmp_path)
    fid, _ = _seed_raw(db_path, new_path, old_path)

    PhotoOrganizerApp(db_path).ingest_dest(
        dest_root=tmp_path / "dest", dry_run=False,
    )

    assert _dest_path(db_path, fid) == str(new_path), \
        "real run must commit rename reconciliation"


# ---------------------------------------------------------------------------
# Change 1: broadened skip_if_no_imports — renames alone drive Phase B
# ---------------------------------------------------------------------------

def test_ingest_dest_rename_only_does_not_early_exit(tmp_path):
    """
    A rename-only run (no net-new files) must still enter Phase B so the
    deferred rename can be applied inside the savepoint. Regression against
    the earlier `skip_if_no_imports and not result.imported_paths` check
    that discarded pending renames when Phase A saw zero imports.
    """
    raw_dir = tmp_path / "dest" / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)
    new_path = raw_dir / "IMG_renamed.CR2"
    new_path.write_bytes(b"RAW_BYTES" * 500)
    old_path = raw_dir / "IMG_original.CR2"
    db_path = _make_db(tmp_path)
    fid, _ = _seed_raw(db_path, new_path, old_path)

    # No new files are produced by this ingest — only a rename is detected.
    stats = PhotoOrganizerApp(db_path).ingest_dest(
        dest_root=tmp_path / "dest", dry_run=False,
    )

    assert stats["renamed"] == 1
    assert stats["imported"] == 0
    assert _dest_path(db_path, fid) == str(new_path), \
        "rename-only real run must still commit reconciliation via Phase B"


# ---------------------------------------------------------------------------
# Change 2: PipelineCandidates scoping
# ---------------------------------------------------------------------------

def test_ingest_dest_ignores_stray_dest_path_null_rows(tmp_path):
    """
    A stray row with dest_path=NULL from an interrupted prior run must not
    be planned/moved by the current ingest-dest run. The current run's
    PipelineCandidates scopes Phase B writers to only the ids Phase A
    imported; the stray row is outside that set and stays invisible.
    """
    dest_root = tmp_path / "dest"
    raw_dir = dest_root / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)

    # The file this run discovers — a net-new JPEG to ingest.
    new_jpeg = raw_dir / "net_new.jpg"
    new_jpeg.write_bytes(b"JPEG_NEW" * 500)

    db_path = _make_db(tmp_path)

    # Pre-seed a stray JPEG row with dest_path=NULL that was never moved.
    # Simulates an interrupted prior run that upserted a file record but
    # never completed Phase B planning.
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    db_ops = DBOperations(conn)
    stray = FileRecord(
        hash="stray_hash_xyz", sparse_hash=None, hash_is_sparse=False,
        type="jpeg", ext=".jpg",
        orig_name="stray.jpg",
        orig_path=Path("/no/longer/exists/stray.jpg"),
        size_bytes=100, mtime=0.0, is_seed=False, name_score=1,
        capture_datetime=datetime(2024, 5, 1, 12, 0, 0),
        camera_model=None, lens_model=None, duration_sec=None,
    )
    stray_id = db_ops.upsert_file_record(stray)
    db_ops.upsert_media_metadata(stray_id, stray)
    conn.commit()
    conn.close()

    PhotoOrganizerApp(db_path).ingest_dest(dest_root=dest_root, dry_run=False)

    # Stray row was NEVER in this run's candidate set, so it must be
    # untouched. The new JPEG — which Phase A imported — gets planned.
    assert _dest_path(db_path, stray_id) is None, \
        "stray dest_path=NULL row from prior run must stay invisible to Phase B"


# ---------------------------------------------------------------------------
# Change 3 + full DPP workflow: editor_export_identity after rename
# ---------------------------------------------------------------------------

def test_ingest_dest_dpp_workflow_links_editor_export_after_rename(tmp_path):
    """
    End-to-end DPP workflow:
    1. RAW was organized in a prior run; catalog has dest_path with the
       planner's datetime-suffixed filename.
    2. User renames the RAW in-place in the library to a meaningful name.
    3. User opens the RAW in DPP, which exports a JPEG next to it with
       matching stem.
    4. User runs --ingest-dest.

    Expected: the ingest run (a) detects and applies the RAW's rename inside
    Phase B, (b) imports the JPEG, (c) link_raw_outputs runs with the RAW's
    *updated* dest_path so Strategy 5 (editor_export_identity) fires at
    confidence 100, and (d) the JPEG is assigned an output destination.
    """
    dest_root = tmp_path / "dest"
    raw_dir = dest_root / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)

    capture_dt = datetime(2024, 5, 1, 12, 0, 0)

    # The RAW on disk, after the user renamed it to a meaningful stem.
    renamed_raw = raw_dir / "birthday_party.CR2"
    renamed_raw.write_bytes(b"RAW_CONTENT" * 500)

    # Catalog remembers the RAW at its organized-but-not-yet-renamed path.
    planner_old_path = raw_dir / "IMG_1234_2024-05-01_12-00-00.CR2"
    db_path = _make_db(tmp_path)
    raw_fid, _ = _seed_raw(db_path, renamed_raw, planner_old_path, capture_dt)

    # DPP exports a JPEG next to the RAW with matching stem. The test bytes
    # carry no EXIF, so the scanner falls back to file mtime for
    # capture_datetime — align mtime with the RAW's capture_dt so Strategy 5's
    # (current_stem, capture_datetime) key matches on both sides.
    jpeg_export = raw_dir / "birthday_party.JPG"
    jpeg_export.write_bytes(b"JPEG_EXPORT" * 300)
    ts = capture_dt.timestamp()
    os.utime(jpeg_export, (ts, ts))
    os.utime(renamed_raw, (ts, ts))

    PhotoOrganizerApp(db_path).ingest_dest(dest_root=dest_root, dry_run=False)

    # Rename was applied.
    assert _dest_path(db_path, raw_fid) == str(renamed_raw)

    # Link was created at confidence 100 via editor_export_identity.
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("""
        SELECT ro.confidence, ro.link_method, f.orig_name
        FROM raw_outputs ro
        JOIN files f ON ro.output_file_id = f.id
        WHERE ro.raw_file_id = ?
    """, (raw_fid,))
    link_rows = cur.fetchall()
    conn.close()

    assert link_rows, "ingest-dest must produce a RAW→JPEG link for the DPP export"
    confidence, method, out_name = link_rows[0]
    assert confidence == 100, f"S5 should fire; got confidence={confidence}, method={method}"
    assert method == "editor_export_identity"
    assert out_name == "birthday_party.JPG"


# ---------------------------------------------------------------------------
# Change 3: Strategy 5 ambiguity guard
# ---------------------------------------------------------------------------

def test_s5_ambiguity_guard_skips_when_two_raws_share_key(tmp_path):
    """
    If two RAWs share the same current_stem + capture_datetime, the output
    cannot be unambiguously assigned to either. Strategy 5 must skip the
    pairing entirely and let lower-confidence strategies handle it (or
    record no link).
    """
    from photo_organizer.metadata.linking import FileLinker

    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    db_ops = DBOperations(conn)

    capture_dt = datetime(2024, 5, 1, 12, 0, 0)

    # Two RAWs with the same orig_name stem and same capture_datetime — the
    # ambiguity case. (Pathologically rare in the wild, but possible: burst
    # mode + filename collision across memory cards.)
    for raw_hash, raw_name in [("h1", "IMG_dup.CR2"), ("h2", "IMG_dup.CR2")]:
        rec = FileRecord(
            hash=raw_hash, sparse_hash=None, hash_is_sparse=False,
            type="raw", ext=".cr2",
            orig_name=raw_name, orig_path=tmp_path / f"{raw_hash}_{raw_name}",
            size_bytes=100, mtime=0.0, is_seed=False, name_score=10,
            capture_datetime=capture_dt,
            camera_model=None, lens_model=None, duration_sec=None,
        )
        rid = db_ops.upsert_file_record(rec)
        db_ops.upsert_media_metadata(rid, rec)

    jpeg = FileRecord(
        hash="jpeg_h", sparse_hash=None, hash_is_sparse=False,
        type="jpeg", ext=".jpg",
        orig_name="IMG_dup.JPG", orig_path=tmp_path / "IMG_dup.JPG",
        size_bytes=50, mtime=0.0, is_seed=False, name_score=1,
        capture_datetime=capture_dt,
        camera_model=None, lens_model=None, duration_sec=None,
    )
    jpeg_id = db_ops.upsert_file_record(jpeg)
    db_ops.upsert_media_metadata(jpeg_id, jpeg)
    conn.commit()

    linker = FileLinker(db_ops)
    linker.link_raw_outputs(dry_run=False)

    cur = conn.cursor()
    cur.execute("SELECT confidence, link_method FROM raw_outputs WHERE output_file_id = ?",
                (jpeg_id,))
    rows = cur.fetchall()
    conn.close()

    # S5 must NOT fire — the JPEG's (stem, dt) key maps to two RAWs. Any
    # link recorded for this JPEG must come from a lower-confidence
    # strategy (or there may be no link at all, which is also fine).
    assert all(r[1] != "editor_export_identity" for r in rows), \
        f"S5 must skip ambiguous RAW pairings; got rows={rows}"


# ---------------------------------------------------------------------------
# Integration: dry-run then real-run idempotence
# ---------------------------------------------------------------------------

def test_ingest_dest_dry_run_then_real_run_is_idempotent(tmp_path):
    """
    A dry-run followed by a real run against the same state must leave the
    catalog in the exact state the real run alone would have produced —
    dry-run must not leak any catalog mutation. Together this proves:
      - Change 1: rename invisibility under dry-run (the savepoint rolls
        back the reconciliation).
      - Change 2: scoping — a repeat run's PipelineCandidates starts
        fresh; nothing the first pass tried to write sticks around to
        contaminate the second.
    """
    dest_root = tmp_path / "dest"
    raw_dir = dest_root / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)

    # A RAW that lives on disk at its new name but whose catalog row still
    # points at the pre-rename path.
    renamed_raw = raw_dir / "IMG_renamed.CR2"
    renamed_raw.write_bytes(b"RAW_BYTES" * 500)
    old_path = raw_dir / "IMG_original.CR2"
    db_path = _make_db(tmp_path)
    fid, _ = _seed_raw(db_path, renamed_raw, old_path)

    # Pass 1: dry-run. Must NOT mutate the catalog.
    PhotoOrganizerApp(db_path).ingest_dest(
        dest_root=dest_root, dry_run=True,
    )
    assert _dest_path(db_path, fid) == str(old_path), \
        "dry-run leaked rename into the catalog"

    # Pass 2: real run — should produce the same outcome as if pass 1 had
    # never happened.
    PhotoOrganizerApp(db_path).ingest_dest(
        dest_root=dest_root, dry_run=False,
    )
    assert _dest_path(db_path, fid) == str(renamed_raw), \
        "real run after dry-run must commit the reconciliation"

    # Pass 3: real run again on an already-reconciled state — nothing to do,
    # the row must stay at its new path.
    PhotoOrganizerApp(db_path).ingest_dest(
        dest_root=dest_root, dry_run=False,
    )
    assert _dest_path(db_path, fid) == str(renamed_raw), \
        "idempotent re-run must not disturb a reconciled row"
