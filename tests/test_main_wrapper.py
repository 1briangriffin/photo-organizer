"""
Integration tests for the main() command wrapper.

Properties verified:
- Commands that require an existing DB (report/sync-dest/validate-dest/
  ingest-dest) exit with status 1 and do NOT create the catalog file.
- organize is allowed to create the catalog and is recorded in command_runs.
- _compute_mutation_flags reports actual counts, not intent:
    * organize real-run with no on-disk source files records files_mutate=0
      (nothing was moved or copied, even though move_mode=True).
    * organize dry-run records db_mutates=1 when scanned>0 (scan data persists
      via Phase A), files_mutate=0.
"""
import sqlite3
import sys
from pathlib import Path

import pytest

from photo_organizer import main as main_mod
from photo_organizer.main import _compute_mutation_flags


# ---------------------------------------------------------------------------
# _compute_mutation_flags — actual semantics
# ---------------------------------------------------------------------------

class _Args:
    """Tiny shim so we don't need argparse.Namespace boilerplate."""
    def __init__(self, **kw):
        self.__dict__.update(kw)


def test_flags_organize_real_run_no_files_mutated():
    stats = {"scanned": 0, "moved": 0, "copied": 0}
    db, files = _compute_mutation_flags("organize", _Args(dry_run=False), stats)
    assert db is False and files is False


def test_flags_organize_dry_run_scanned_only():
    stats = {"scanned": 10, "moved": 0, "copied": 0}
    db, files = _compute_mutation_flags("organize", _Args(dry_run=True), stats)
    assert db is True, "scan data is committed in Phase A even on dry-run"
    assert files is False


def test_flags_organize_real_run_with_copies():
    stats = {"scanned": 10, "moved": 0, "copied": 7}
    db, files = _compute_mutation_flags("organize", _Args(dry_run=False), stats)
    assert db is True and files is True


def test_flags_ingest_dest_dry_run_no_imports():
    stats = {"renamed": 3, "imported": 0, "moved": 0, "copied": 0}
    db, files = _compute_mutation_flags("ingest-dest", _Args(dry_run=True), stats)
    # Dry-run renames are not persisted; no imports ⇒ no catalog mutation.
    assert db is False and files is False


def test_flags_ingest_dest_dry_run_with_imports():
    stats = {"renamed": 0, "imported": 5, "moved": 0, "copied": 0}
    db, files = _compute_mutation_flags("ingest-dest", _Args(dry_run=True), stats)
    assert db is True, "ingest-mode imports are always real writes"
    assert files is False


def test_flags_sync_dest_dry_run_always_false():
    stats = {"renamed": 3, "imported": 0}
    db, files = _compute_mutation_flags("sync-dest", _Args(dry_run=True), stats)
    assert db is False and files is False


def test_flags_sync_dest_real_run_no_changes():
    stats = {"renamed": 0, "imported": 0}
    db, files = _compute_mutation_flags("sync-dest", _Args(dry_run=False), stats)
    assert db is False and files is False


def test_flags_sync_dest_real_run_with_renames():
    stats = {"renamed": 4, "imported": 0}
    db, files = _compute_mutation_flags("sync-dest", _Args(dry_run=False), stats)
    assert db is True and files is False


def test_flags_validate_and_report_readonly():
    stats = {}
    for cmd in ("validate-dest", "report"):
        db, files = _compute_mutation_flags(cmd, _Args(dry_run=False), stats)
        assert db is False and files is False, f"{cmd} must be read-only"


# ---------------------------------------------------------------------------
# main() wrapper — missing-DB precondition
# ---------------------------------------------------------------------------

def _invoke_main(monkeypatch, argv):
    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(SystemExit) as excinfo:
        main_mod.main()
    return excinfo.value.code


@pytest.mark.parametrize("mode_flag,extra", [
    ("--sync-dest", []),
    ("--validate-dest", []),
    ("--ingest-dest", []),
])
def test_missing_db_rejects_dest_only_modes(monkeypatch, tmp_path, mode_flag, extra):
    """Running a dest-only mode against a non-existent DB must exit 1 and
    NOT create the catalog (that bypass was the reported regression)."""
    dest = tmp_path / "dest"
    # dest is permitted to be auto-created by setup_logging — the catalog is not.
    argv = ["photo-organizer", mode_flag, str(dest), "--db", str(tmp_path / "absent.db"), *extra]

    code = _invoke_main(monkeypatch, argv)
    assert code == 1
    assert not (tmp_path / "absent.db").exists(), \
        "main() must not create the DB when a dest-only mode is run without one"


def test_missing_db_rejects_report(monkeypatch, tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    dest = tmp_path / "dest"
    argv = ["photo-organizer", str(src), str(dest), "--report",
            "--db", str(tmp_path / "absent.db")]
    code = _invoke_main(monkeypatch, argv)
    assert code == 1
    assert not (tmp_path / "absent.db").exists()


def test_organize_creates_db_and_records_running_row(monkeypatch, tmp_path):
    """organize is the only mode allowed to create the catalog. After it runs,
    the catalog must exist and hold a command_runs row for this invocation."""
    src = tmp_path / "src"
    src.mkdir()
    dest = tmp_path / "dest"
    db_path = tmp_path / "catalog.db"

    argv = ["photo-organizer", str(src), str(dest), "--db", str(db_path), "--dry-run"]
    code = _invoke_main(monkeypatch, argv)
    assert code == 0
    assert db_path.exists()

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(
            "SELECT command, exit_status, dry_run FROM command_runs ORDER BY id DESC LIMIT 1"
        )
        row = cur.fetchone()
    finally:
        conn.close()

    assert row is not None, "organize should leave a command_runs row"
    cmd, status, dry = row
    assert cmd == "organize"
    assert status == "success"
    assert dry == 1
