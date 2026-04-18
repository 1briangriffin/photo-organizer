"""
Regression tests for the unified pipeline's savepoint contract.

Contract:
- Observed reality (files rows, media_metadata, source/destination file_occurrences)
  is committed outside the savepoint — it survives a dry-run and persists across
  connections after a real run.
- Inferred relationships (raw_sidecars, raw_outputs, psd_source_links) and planned
  dest_path writes sit inside the savepoint — they roll back on dry-run and only
  persist after a real-run commit.

Covers both organize() and ingest_dest(), plus the incidental fix that commits
destination file_occurrences after FileMover.execute() on real runs.
"""
import sqlite3
from datetime import datetime
from pathlib import Path

from photo_organizer.core import PhotoOrganizerApp
from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.models import FileRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "catalog.db"
    c = sqlite3.connect(str(db_path))
    init_schema(c)
    c.commit()
    c.close()
    return db_path


def _seed_raw_and_sidecar(db_path: Path, raw_path: Path, sidecar_path: Path) -> None:
    """Pre-seed a RAW + matching .xmp sidecar so link_raw_sidecars will match them."""
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    db_ops = DBOperations(conn)
    raw = FileRecord(
        hash="raw_h", sparse_hash=None, hash_is_sparse=False,
        type="raw", ext=".cr2",
        orig_name=raw_path.name, orig_path=raw_path,
        size_bytes=100, mtime=0.0, is_seed=False, name_score=10,
        capture_datetime=datetime(2024, 5, 1),
        camera_model=None, lens_model=None, duration_sec=None,
    )
    sidecar = FileRecord(
        hash="xmp_h", sparse_hash=None, hash_is_sparse=False,
        type="sidecar", ext=".xmp",
        orig_name=sidecar_path.name, orig_path=sidecar_path,
        size_bytes=10, mtime=0.0, is_seed=False, name_score=1,
        capture_datetime=None,
        camera_model=None, lens_model=None, duration_sec=None,
    )
    raw_id = db_ops.upsert_file_record(raw)
    db_ops.upsert_media_metadata(raw_id, raw)
    db_ops.upsert_file_record(sidecar)
    conn.commit()
    conn.close()


def _count(db_path: Path, table: str, where: str = "") -> int:
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    sql = f"SELECT COUNT(*) FROM {table}"
    if where:
        sql += f" WHERE {where}"
    cur.execute(sql)
    n = cur.fetchone()[0]
    conn.close()
    return n


# ---------------------------------------------------------------------------
# organize() — dry-run rolls back link inference
# ---------------------------------------------------------------------------

def test_organize_dry_run_rolls_back_raw_sidecars(tmp_path):
    """organize --dry-run must leave raw_sidecars empty even when matching pairs exist."""
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    dest_dir = tmp_path / "dest"

    # The DB has the pair pre-seeded so linking finds a match. Scanner is pointed at
    # an empty src_dir, so Phase A is a no-op and we isolate the savepoint behaviour.
    raw_path = tmp_path / "img.CR2"
    sidecar_path = tmp_path / "img.xmp"
    db_path = _make_db(tmp_path)
    _seed_raw_and_sidecar(db_path, raw_path, sidecar_path)

    PhotoOrganizerApp(db_path).organize(
        src_root=src_dir, dest_root=dest_dir, dry_run=True,
        dry_run_csv=tmp_path / "preview.csv",
    )

    assert _count(db_path, "raw_sidecars") == 0, \
        "sidecar links are inferred and must roll back on dry-run"
    assert _count(db_path, "files", "dest_path IS NOT NULL") == 0, \
        "dest_path writes must roll back on dry-run"
    # Observed reality survives.
    assert _count(db_path, "files") >= 2, \
        "pre-seeded file rows (observed) must survive"


# ---------------------------------------------------------------------------
# organize() — real run persists links AND destination file_occurrences
# ---------------------------------------------------------------------------

def test_organize_real_run_persists_links_via_fresh_connection(tmp_path):
    """Real organize run commits link inference so it is visible to a fresh connection."""
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    dest_dir = tmp_path / "dest"

    raw_path = tmp_path / "img.CR2"
    sidecar_path = tmp_path / "img.xmp"
    db_path = _make_db(tmp_path)
    _seed_raw_and_sidecar(db_path, raw_path, sidecar_path)

    PhotoOrganizerApp(db_path).organize(
        src_root=src_dir, dest_root=dest_dir, dry_run=False,
    )

    # Fresh connection proves the writes reached disk, not just the in-process transaction.
    assert _count(db_path, "raw_sidecars") == 1, \
        "real run must commit sidecar links"


def test_organize_real_run_persists_destination_file_occurrence(tmp_path):
    """
    After a real organize run, FileMover writes a destination occurrence via
    record_occurrence(). That write lives in the pipeline's final commit, so a
    fresh connection must see it. Guards against the prior latent bug where the
    destination occurrence was never committed.
    """
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    dest_dir = tmp_path / "dest"

    # A real file on disk the scanner will pick up, hash, and the mover will copy.
    src_file = src_dir / "photo.jpg"
    src_file.write_bytes(b"FAKE_JPEG_BYTES" * 100)

    db_path = _make_db(tmp_path)

    PhotoOrganizerApp(db_path).organize(
        src_root=src_dir, dest_root=dest_dir, dry_run=False,
    )

    # Fresh connection — proves commit, not just transaction visibility.
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT dest_path FROM files WHERE orig_name = 'photo.jpg'")
    row = cur.fetchone()
    assert row is not None and row[0], "file must have a dest_path after real run"
    dest_path = row[0]

    cur.execute(
        "SELECT COUNT(*) FROM file_occurrences WHERE path = ?",
        (dest_path,),
    )
    n = cur.fetchone()[0]
    conn.close()

    assert n >= 1, \
        "destination file_occurrence must be committed and visible to a fresh connection"


# ---------------------------------------------------------------------------
# ingest_dest() — dry-run rolls back link inference
# ---------------------------------------------------------------------------

def test_ingest_dest_dry_run_rolls_back_link_tables(tmp_path):
    """ingest-dest --dry-run must leave raw_sidecars empty even when new sidecars co-locate with tracked RAWs."""
    raw_dir = tmp_path / "dest" / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)
    raw_file = raw_dir / "IMG_9999.CR2"
    raw_file.write_bytes(b"RAW" * 2000)
    xmp_file = raw_dir / "IMG_9999.xmp"
    xmp_file.write_bytes(b"<xmp/>")

    db_path = _make_db(tmp_path)
    # Pre-seed the RAW as already-organized (has dest_path) so the sidecar linker matches by dest.
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    db_ops = DBOperations(conn)
    raw = FileRecord(
        hash="raw_existing", sparse_hash=None, hash_is_sparse=False,
        type="raw", ext=".cr2",
        orig_name=raw_file.name, orig_path=raw_file,
        size_bytes=raw_file.stat().st_size, mtime=raw_file.stat().st_mtime,
        is_seed=False, name_score=10,
        capture_datetime=datetime(2024, 5, 1),
        camera_model=None, lens_model=None, duration_sec=None,
    )
    rid = db_ops.upsert_file_record(raw)
    db_ops.upsert_media_metadata(rid, raw)
    db_ops.update_dest_path(rid, str(raw_file))
    conn.commit()
    conn.close()

    PhotoOrganizerApp(db_path).ingest_dest(
        dest_root=tmp_path / "dest", dry_run=True,
    )

    # Sidecar record (observed) persists; link (inferred) does not.
    assert _count(db_path, "files", "type = 'sidecar'") == 1, \
        "imported sidecar row is observed reality and must survive dry-run"
    assert _count(db_path, "raw_sidecars") == 0, \
        "sidecar link is inferred and must roll back on dry-run"


# ---------------------------------------------------------------------------
# Sequential: dry-run then real run — links are created exactly once
# ---------------------------------------------------------------------------

def test_organize_real_run_assigns_sidecar_dest_path(tmp_path):
    """
    After a real organize run with a RAW + matching sidecar, both files must have
    a dest_path. The sidecar's dest_path is derived from the RAW's dest_path via
    _assign_linked_destinations, which requires the RAW to have been planned first.

    Regression: an earlier refactor ordered _assign_linked_destinations before
    plan_all(), leaving sidecars unassigned because their RAW parent had no dest_path
    at the time of assignment.
    """
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    dest_dir = tmp_path / "dest"

    raw_path = tmp_path / "IMG_0001.CR2"
    sidecar_path = tmp_path / "IMG_0001.xmp"
    db_path = _make_db(tmp_path)
    _seed_raw_and_sidecar(db_path, raw_path, sidecar_path)

    PhotoOrganizerApp(db_path).organize(
        src_root=src_dir, dest_root=dest_dir, dry_run=False,
    )

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT orig_name, dest_path FROM files ORDER BY orig_name")
    rows = cur.fetchall()
    conn.close()

    by_name = {name: dest for name, dest in rows}
    assert by_name.get("IMG_0001.CR2"), "RAW must have dest_path after real run"
    assert by_name.get("IMG_0001.xmp"), \
        "Sidecar must inherit dest_path from its RAW after real organize run"


def test_ingest_dest_rename_only_persists_across_fresh_connection(tmp_path):
    """
    A rename-only ingest-dest real run (no net-new files) must still commit the
    rename reconciliation. Regression: an earlier discoverer returned early when
    sync_destinations found no new files, discarding pending rename writes when
    DBManager closed without committing.
    """
    import hashlib

    dest_root = tmp_path / "dest"
    raw_dir = dest_root / "raw" / "2024" / "2024-05"
    raw_dir.mkdir(parents=True)

    content = b"RAW_BYTES" * 500
    # File lives on disk at its NEW name.
    new_path = raw_dir / "IMG_newname.CR2"
    new_path.write_bytes(content)
    full_hash = hashlib.sha256(content).hexdigest()

    # Catalog remembers it at the OLD name. sync_destinations must detect the rename
    # (same parent dir, same hash) and update dest_path to new_path.
    old_path = raw_dir / "IMG_oldname.CR2"

    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    db_ops = DBOperations(conn)
    rec = FileRecord(
        hash=full_hash, sparse_hash=None, hash_is_sparse=False,
        type="raw", ext=".cr2",
        orig_name="IMG_oldname.CR2", orig_path=old_path,
        size_bytes=len(content), mtime=new_path.stat().st_mtime,
        is_seed=False, name_score=10,
        capture_datetime=datetime(2024, 5, 1),
        camera_model=None, lens_model=None, duration_sec=None,
    )
    fid = db_ops.upsert_file_record(rec)
    db_ops.upsert_media_metadata(fid, rec)
    db_ops.update_dest_path(fid, str(old_path))
    db_ops.record_occurrence(
        file_id=fid, path=old_path, is_seed=False,
        mtime=new_path.stat().st_mtime, size_bytes=len(content),
        hash_value=full_hash, is_sparse=False,
    )
    conn.commit()
    conn.close()

    PhotoOrganizerApp(db_path).ingest_dest(dest_root=dest_root, dry_run=False)

    # Fresh connection confirms the rename was committed.
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT dest_path FROM files WHERE id = ?", (fid,))
    dest = cur.fetchone()[0]
    conn.close()

    assert dest == str(new_path), \
        f"rename reconciliation must persist; got dest_path={dest}"


def test_organize_dry_run_preview_respects_skip_dirs(tmp_path):
    """
    Dry-run preview CSV must honour skip_dirs so the preview matches what the real
    organize run would actually process. Files under skipped directories must not
    appear in the preview.
    """
    import csv as _csv

    src_dir = tmp_path / "src"
    keep_dir = src_dir / "keep"
    skip_dir = src_dir / "skip"
    keep_dir.mkdir(parents=True)
    skip_dir.mkdir(parents=True)

    (keep_dir / "ok.jpg").write_bytes(b"KEEP" * 100)
    (skip_dir / "bad.jpg").write_bytes(b"SKIP" * 100)

    dest_dir = tmp_path / "dest"
    db_path = _make_db(tmp_path)
    preview_csv = tmp_path / "preview.csv"

    PhotoOrganizerApp(db_path).organize(
        src_root=src_dir, dest_root=dest_dir, dry_run=True,
        dry_run_csv=preview_csv,
        skip_dirs={skip_dir},
    )

    with preview_csv.open(newline="", encoding="utf-8") as f:
        rows = list(_csv.reader(f))

    all_paths = [cell for row in rows for cell in row]
    joined = "\n".join(all_paths)
    assert "ok.jpg" in joined, "kept file must appear in preview"
    assert "bad.jpg" not in joined, "skipped file must not appear in preview CSV"


def test_organize_dry_run_then_real_run_creates_links_once(tmp_path):
    """A dry-run followed by a real run must end with exactly one link row.

    Regression against a savepoint leak that would either double-insert (if the
    rollback didn't fire) or fail to persist (if the savepoint release left state
    in limbo).
    """
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    dest_dir = tmp_path / "dest"

    raw_path = tmp_path / "img.CR2"
    sidecar_path = tmp_path / "img.xmp"
    db_path = _make_db(tmp_path)
    _seed_raw_and_sidecar(db_path, raw_path, sidecar_path)

    app = PhotoOrganizerApp(db_path)

    app.organize(src_root=src_dir, dest_root=dest_dir, dry_run=True,
                 dry_run_csv=tmp_path / "preview.csv")
    assert _count(db_path, "raw_sidecars") == 0

    app.organize(src_root=src_dir, dest_root=dest_dir, dry_run=False)
    assert _count(db_path, "raw_sidecars") == 1, \
        "real run after dry-run must commit exactly one link"
