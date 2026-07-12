"""
Intentional-deletion (file retirement) tests: schema v8 status columns,
retire/restore operations with run-action audit, exclusion from
expected-on-disk state (validate-dest / sync-dest / reconciliation /
backfill), and the photo-catalog-query surface.
"""
import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

import photo_catalog_query as pcq
from photo_organizer.core import PhotoOrganizerApp
from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.models import FileRecord
from photo_organizer.pipeline.lifecycle import restore_files, retire_files

CAPTURE_DT = datetime(2024, 5, 1, 12, 0, 0)


@pytest.fixture
def conn(tmp_path):
    db_path = tmp_path / "catalog.db"
    c = sqlite3.connect(str(db_path))
    init_schema(c)
    try:
        yield c
    finally:
        c.close()


@pytest.fixture
def db_ops(conn):
    return DBOperations(conn)


def _seed_file(db_ops, *, path: Path, file_type="raw", content=b"raw bytes",
               write_to_disk=False) -> int:
    rec = FileRecord(
        hash=hashlib.sha256(content + path.name.encode()).hexdigest(),
        sparse_hash=None,
        hash_is_sparse=False,
        type=file_type,
        ext=path.suffix.lower(),
        orig_name=path.name,
        orig_path=path,
        size_bytes=len(content),
        mtime=CAPTURE_DT.timestamp(),
        is_seed=False,
        name_score=1,
        capture_datetime=CAPTURE_DT,
        camera_model="Canon Test",
    )
    file_id = db_ops.upsert_file_record(rec)
    db_ops.upsert_media_metadata(file_id, rec)
    db_ops.update_dest_path(file_id, str(path))
    if write_to_disk:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return file_id


def _start_run(conn, *, command="retire-file", tool="photo-catalog-query"):
    return conn.execute(
        """
        INSERT INTO command_runs (
            tool, command, started_at, exit_status, dry_run, argv_json
        )
        VALUES (?, ?, '2026-01-01T00:00:00Z', 'running', 0, '[]')
        """,
        (tool, command),
    ).lastrowid


def _status_row(conn, file_id):
    return conn.execute(
        "SELECT status, status_changed_by_run_id, status_note "
        "FROM files WHERE id = ?",
        (file_id,),
    ).fetchone()


# ---------------------------------------------------------------------------
# Schema migration
# ---------------------------------------------------------------------------

def test_v7_db_migrates_to_file_retirement_columns(tmp_path):
    from photo_organizer.database import schema as schema_module
    from photo_organizer.database.schema import CURRENT_SCHEMA_VERSION, _create_core_schema

    db_path = tmp_path / "v7.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY);")
        conn.execute("INSERT INTO schema_version (version) VALUES (7);")
        _create_core_schema(conn)
        # Recreate files in its pre-v8 shape (no status columns).
        conn.execute("DROP TABLE files")
        conn.execute("""
            CREATE TABLE files (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                hash            TEXT UNIQUE,
                sparse_hash     TEXT,
                type            TEXT NOT NULL,
                ext             TEXT NOT NULL,
                orig_name       TEXT NOT NULL,
                orig_path       TEXT NOT NULL,
                dest_path       TEXT,
                size_bytes      INTEGER,
                is_seed         INTEGER NOT NULL DEFAULT 0,
                name_score      INTEGER NOT NULL DEFAULT 0,
                first_seen_at   TEXT NOT NULL,
                last_seen_at    TEXT NOT NULL
            );
        """)
        conn.execute("""
            INSERT INTO files (hash, type, ext, orig_name, orig_path,
                               first_seen_at, last_seen_at)
            VALUES ('h1', 'raw', '.cr2', 'a.CR2', 'C:/src/a.CR2',
                    '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
        """)
        for version in range(3, 8):
            schema_module.MIGRATIONS[version](conn)
        conn.commit()

        init_schema(conn)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        columns = {row[1] for row in conn.execute("PRAGMA table_info(files)")}
        status = conn.execute("SELECT status FROM files WHERE hash = 'h1'").fetchone()[0]
    finally:
        conn.close()

    assert version == CURRENT_SCHEMA_VERSION
    assert {"status", "status_changed_at", "status_changed_by_run_id",
            "status_note"} <= columns
    assert status == "active"


# ---------------------------------------------------------------------------
# retire / restore operations
# ---------------------------------------------------------------------------

def test_retire_and_restore_roundtrip_with_audit(conn, db_ops, tmp_path):
    dest = tmp_path / "dest"
    file_id = _seed_file(db_ops, path=dest / "a.CR2")
    run_id = _start_run(conn)
    conn.commit()

    retired, skipped = retire_files(
        db_ops, [file_id, 9999], run_id=run_id, note="blurry duplicate",
    )
    conn.commit()
    assert retired == [file_id]
    assert skipped == [9999]
    assert _status_row(conn, file_id) == ("retired", run_id, "blurry duplicate")

    action = conn.execute(
        """
        SELECT action_type, entity_id, status, source_path, method
        FROM run_actions WHERE proposed_by_run_id = ?
        """,
        (run_id,),
    ).fetchone()
    assert action == ("retire_file", file_id, "applied",
                      str(dest / "a.CR2"), "user_decision")

    # Retiring again is a no-op skip.
    retired_again, skipped_again = retire_files(db_ops, [file_id], run_id=run_id)
    assert retired_again == []
    assert skipped_again == [file_id]

    restore_run = _start_run(conn, command="restore-file")
    restored, _ = restore_files(db_ops, [file_id], run_id=restore_run)
    conn.commit()
    assert restored == [file_id]
    assert _status_row(conn, file_id)[0] == "active"
    restore_action = conn.execute(
        "SELECT action_type FROM run_actions WHERE proposed_by_run_id = ?",
        (restore_run,),
    ).fetchone()
    assert restore_action == ("restore_file",)


def test_set_file_status_rejects_unknown_status(db_ops):
    with pytest.raises(ValueError):
        db_ops.set_file_status([1], status="deleted", run_id=None)


# ---------------------------------------------------------------------------
# Exclusion from expected-on-disk state
# ---------------------------------------------------------------------------

def test_retired_files_excluded_from_expected_and_candidate_queries(conn, db_ops, tmp_path):
    dest = tmp_path / "dest"
    active_id = _seed_file(db_ops, path=dest / "a.CR2")
    retired_id = _seed_file(db_ops, path=dest / "b.CR2")
    retire_files(db_ops, [retired_id], run_id=None)
    conn.commit()

    dest_ids = {row[0] for row in db_ops.get_all_dest_files([dest])}
    assert dest_ids == {active_id}

    recon_ids = {row[0] for row in db_ops.get_raw_edit_reconciliation_candidates([dest])}
    assert recon_ids == {active_id}

    backfill_ids = {row[0] for row in db_ops.get_raw_metadata_backfill_candidates([dest])}
    assert backfill_ids == {active_id}

    retired_rows = db_ops.get_retired_files([dest])
    assert [row[0] for row in retired_rows] == [retired_id]


def test_validate_dest_reports_retired_separately_from_missing(tmp_path):
    dest = tmp_path / "dest"
    dest.mkdir()
    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    db_ops = DBOperations(conn)

    on_disk = _seed_file(db_ops, path=dest / "present.CR2", write_to_disk=True)
    missing_id = _seed_file(db_ops, path=dest / "gone.CR2")
    retired_id = _seed_file(db_ops, path=dest / "deleted_on_purpose.CR2")
    retire_files(db_ops, [retired_id], run_id=None, note="rejected shot")
    conn.commit()
    conn.close()

    csv_path = tmp_path / "validation.csv"
    stats = PhotoOrganizerApp(db_path).validate_dest(dest_root=dest, report_csv=csv_path)

    assert stats["confirmed"] == 1
    assert stats["missing"] == 1
    assert stats["retired"] == 1

    content = csv_path.read_text(encoding="utf-8")
    missing_section = content.split("=== MISSING ===")[1].split("===")[0]
    assert "gone.CR2" in missing_section
    assert "deleted_on_purpose.CR2" not in missing_section
    retired_section = content.split("=== RETIRED (intentionally deleted) ===")[1].split("===")[0]
    assert "deleted_on_purpose.CR2" in retired_section
    assert "rejected shot" in retired_section


def test_sync_dest_does_not_count_retired_as_missing(tmp_path):
    dest = tmp_path / "dest"
    dest.mkdir()
    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    db_ops = DBOperations(conn)
    retired_id = _seed_file(db_ops, path=dest / "gone.CR2")
    retire_files(db_ops, [retired_id], run_id=None)
    conn.commit()
    conn.close()

    stats = PhotoOrganizerApp(db_path).sync_dest(dest_root=dest, dry_run=True)
    assert stats["missing"] == 0


# ---------------------------------------------------------------------------
# photo-catalog-query surface
# ---------------------------------------------------------------------------

def test_retire_cli_accepts_ids_and_paths_and_records_run(tmp_path, capsys):
    dest = tmp_path / "dest"
    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    db_ops = DBOperations(conn)
    by_id = _seed_file(db_ops, path=dest / "a.CR2")
    by_path = _seed_file(db_ops, path=dest / "b.CR2")
    conn.commit()

    pcq.set_file_status_cmd(
        conn, db_path,
        [str(by_id), str(dest / "b.CR2"), str(dest / "nope.CR2")],
        retire=True, note="culled",
    )
    out = capsys.readouterr().out
    assert "No catalog file with dest_path" in out
    assert f"Retired 2 file(s): {by_id}, {by_path}" in out

    reviewer = conn.execute(
        "SELECT id, command, exit_status, db_mutates FROM command_runs "
        "WHERE command = 'retire-file'"
    ).fetchone()
    assert reviewer is not None
    assert reviewer[2] == "success"
    assert reviewer[3] == 1
    assert _status_row(conn, by_id) == ("retired", reviewer[0], "culled")

    pcq.list_retired(conn)
    out = capsys.readouterr().out
    assert "2 retired file(s):" in out
    assert "culled" in out

    pcq.set_file_status_cmd(conn, db_path, [str(by_id)], retire=False)
    assert _status_row(conn, by_id)[0] == "active"
    conn.close()


def test_retire_cli_with_nothing_to_do_records_no_run(tmp_path, capsys):
    dest = tmp_path / "dest"
    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    conn.commit()

    pcq.set_file_status_cmd(conn, db_path, [str(dest / "nope.CR2")], retire=True)
    out = capsys.readouterr().out
    assert "Nothing to retire." in out
    runs = conn.execute("SELECT COUNT(*) FROM command_runs").fetchone()[0]
    conn.close()
    assert runs == 0
