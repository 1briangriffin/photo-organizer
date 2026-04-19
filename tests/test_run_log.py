"""
Tests for RunRecorder and the command_runs table.

Covers:
- success path writes finished_at + exit_status=success + stats/argv/params
- dry-run flag round-trips
- interrupted path (KeyboardInterrupt) leaves exit_status=interrupted with finished_at
- error path sets exit_status=error plus error_type/error_message
- argv_json round-trips as a list
- nullable src_root/dest_root (tools like photo-faces may not have both)
- a start that crashes (e.g. schema missing) does NOT raise out of start()
- row_id remains None after a failed start → finalize is a no-op
"""
import json
import sqlite3
from pathlib import Path

import pytest

from photo_organizer.database.schema import init_schema
from photo_organizer.run_log import RunRecorder, iso_now


def _open_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    conn.close()
    return db_path


def _fetch_row(db_path: Path, rid: int) -> dict:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute("SELECT * FROM command_runs WHERE id = ?", (rid,))
        row = cur.fetchone()
        assert row is not None, f"No command_runs row with id={rid}"
        return dict(row)
    finally:
        conn.close()


def test_iso_now_emits_z_suffix():
    """Contract: ISO-8601 UTC with a literal 'Z', not '+00:00'."""
    ts = iso_now()
    assert "T" in ts
    assert ts.endswith("Z")
    assert "+00:00" not in ts


def test_start_leaves_row_in_running_state(tmp_path):
    """After start() and before any finish_*, exit_status must be 'running'
    with finished_at NULL — so a crash leaves an honest trail."""
    db_path = _open_db(tmp_path)
    recorder = RunRecorder(db_path, tool="photo-organizer", command="organize",
                           argv=["photo-organizer"], db_path_for_row=db_path)
    recorder.start()

    row = _fetch_row(db_path, recorder._row_id)
    assert row["exit_status"] == "running"
    assert row["finished_at"] is None


def test_success_path_records_finish_and_stats(tmp_path):
    db_path = _open_db(tmp_path)
    recorder = RunRecorder(
        db_path, tool="photo-organizer", command="organize",
        argv=["photo-organizer", "src", "dest"],
        params={"dry_run": False, "move": True},
        dry_run=False,
        db_path_for_row=db_path,
        src_root=tmp_path / "src",
        dest_root=tmp_path / "dest",
        app_version="photo-organizer 0.1.0",
    )
    recorder.start()
    assert recorder._row_id is not None
    recorder.finish_success(stats={"scanned": 42, "imported": 10},
                            db_mutates=True, files_mutate=True)

    row = _fetch_row(db_path, recorder._row_id)
    assert row["tool"] == "photo-organizer"
    assert row["command"] == "organize"
    assert row["exit_status"] == "success"
    assert row["dry_run"] == 0
    assert row["db_mutates"] == 1
    assert row["files_mutate"] == 1
    assert row["started_at"] is not None
    assert row["finished_at"] is not None
    assert row["app_version"] == "photo-organizer 0.1.0"
    assert row["error_type"] is None
    assert row["error_message"] is None
    stats = json.loads(row["stats_json"])
    assert stats == {"scanned": 42, "imported": 10}


def test_dry_run_flag_roundtrips(tmp_path):
    db_path = _open_db(tmp_path)
    recorder = RunRecorder(
        db_path, tool="photo-organizer", command="organize",
        argv=["photo-organizer", "--dry-run", "src", "dest"],
        dry_run=True, db_path_for_row=db_path,
    )
    recorder.start()
    recorder.finish_success(db_mutates=True, files_mutate=False)

    row = _fetch_row(db_path, recorder._row_id)
    assert row["dry_run"] == 1
    assert row["db_mutates"] == 1
    assert row["files_mutate"] == 0


def test_interrupted_sets_status_and_preserves_note(tmp_path):
    db_path = _open_db(tmp_path)
    recorder = RunRecorder(db_path, tool="photo-organizer", command="organize",
                           argv=["photo-organizer"], db_path_for_row=db_path)
    recorder.start()
    recorder.finish_interrupted(note="cancelled by user")

    row = _fetch_row(db_path, recorder._row_id)
    assert row["exit_status"] == "interrupted"
    assert row["error_type"] == "KeyboardInterrupt"
    assert row["error_message"] == "cancelled by user"
    assert row["finished_at"] is not None


def test_error_path_captures_type_and_message(tmp_path):
    db_path = _open_db(tmp_path)
    recorder = RunRecorder(db_path, tool="photo-organizer", command="organize",
                           argv=["photo-organizer"], db_path_for_row=db_path)
    recorder.start()

    try:
        raise ValueError("boom")
    except ValueError as e:
        recorder.finish_error(e)

    row = _fetch_row(db_path, recorder._row_id)
    assert row["exit_status"] == "error"
    assert row["error_type"] == "ValueError"
    assert row["error_message"] == "boom"


def test_system_exit_is_recordable(tmp_path):
    """SystemExit from _dispatch (e.g., missing DB) should be recordable as an error."""
    db_path = _open_db(tmp_path)
    recorder = RunRecorder(db_path, tool="photo-organizer", command="sync-dest",
                           argv=["photo-organizer", "--sync-dest", "dest"],
                           db_path_for_row=db_path)
    recorder.start()
    try:
        raise SystemExit("Database not found")
    except SystemExit as e:
        recorder.finish_error(e)

    row = _fetch_row(db_path, recorder._row_id)
    assert row["exit_status"] == "error"
    assert row["error_type"] == "SystemExit"
    assert "Database not found" in row["error_message"]


def test_argv_json_roundtrips_as_list(tmp_path):
    db_path = _open_db(tmp_path)
    argv = ["photo-organizer", "--dry-run", "D:\\Pending", "D:\\Organized"]
    recorder = RunRecorder(db_path, tool="photo-organizer", command="organize",
                           argv=argv, db_path_for_row=db_path)
    recorder.start()
    recorder.finish_success()

    row = _fetch_row(db_path, recorder._row_id)
    assert json.loads(row["argv_json"]) == argv


def test_nullable_src_and_dest_for_faces_tool(tmp_path):
    """photo-faces operates on the catalog alone — src_root/dest_root should be
    accepted as NULL without raising."""
    db_path = _open_db(tmp_path)
    recorder = RunRecorder(
        db_path, tool="photo-faces", command="train",
        argv=["photo-faces", "train"],
        db_path_for_row=db_path,
        src_root=None, dest_root=None,
    )
    recorder.start()
    recorder.finish_success(stats={"faces_embedded": 1234}, db_mutates=True)

    row = _fetch_row(db_path, recorder._row_id)
    assert row["tool"] == "photo-faces"
    assert row["src_root"] is None
    assert row["dest_root"] is None
    assert row["exit_status"] == "success"


def test_start_failure_swallowed_and_finalize_is_noop(tmp_path):
    """If the DB has no schema, start() should log and swallow rather than raise.
    A subsequent finalize should also be a no-op (row_id is None)."""
    db_path = tmp_path / "unsinit.db"
    # Create an empty file with no schema — command_runs will not exist.
    sqlite3.connect(str(db_path)).close()

    recorder = RunRecorder(db_path, tool="photo-organizer", command="organize",
                           argv=["photo-organizer"], db_path_for_row=db_path)
    recorder.start()  # must not raise
    assert recorder._row_id is None

    # Should silently do nothing; no table, no row.
    recorder.finish_success(stats={"scanned": 0})


def test_params_none_stored_as_null(tmp_path):
    db_path = _open_db(tmp_path)
    recorder = RunRecorder(db_path, tool="photo-organizer", command="organize",
                           argv=["photo-organizer"], params=None,
                           db_path_for_row=db_path)
    recorder.start()
    recorder.finish_success()

    row = _fetch_row(db_path, recorder._row_id)
    assert row["params_json"] is None
