"""
Proposal lifecycle tests: key-level auto-supersede in RunActionRecorder,
scope-level supersede of stale proposals, user rejection, and the
photo-catalog-query review surface.
"""
import sqlite3
from pathlib import Path

import pytest

import photo_catalog_query as pcq
from photo_organizer.core import PhotoOrganizerApp
from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.pipeline.actions import RunActionRecorder, canonical_path_action
from photo_organizer.pipeline.lifecycle import (
    list_proposals,
    reject_proposals,
    supersede_stale_proposals,
)


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


def _start_run(conn, *, dry_run, command="ingest-dest", tool="photo-organizer",
               src_root=None, dest_root="C:/Photos/dest"):
    cur = conn.execute(
        """
        INSERT INTO command_runs (
            tool, command, started_at, exit_status, dry_run,
            src_root, dest_root, argv_json
        )
        VALUES (?, ?, '2026-01-01T00:00:00Z', 'running', ?, ?, ?, '[]')
        """,
        (tool, command, 1 if dry_run else 0, src_root, dest_root),
    )
    return int(cur.lastrowid)


def _action(file_id=1, new_path="C:/Photos/new.jpg", status="proposed"):
    return canonical_path_action(
        file_id=file_id,
        old_path="C:/Photos/old.jpg",
        new_path=new_path,
        status=status,
        sequence=1,
    )


def _statuses_by_run(conn, idempotency_key):
    return dict(conn.execute(
        "SELECT proposed_by_run_id, status FROM run_actions WHERE idempotency_key = ?",
        (idempotency_key,),
    ).fetchall())


# ---------------------------------------------------------------------------
# Key-level auto-supersede in RunActionRecorder.record()
# ---------------------------------------------------------------------------

def test_reproposal_supersedes_prior_pending_proposal(conn, db_ops):
    run_1 = _start_run(conn, dry_run=True)
    run_2 = _start_run(conn, dry_run=True)
    action = _action()

    RunActionRecorder(db_ops, run_1).record(action)
    RunActionRecorder(db_ops, run_2).record(action)
    conn.commit()

    assert _statuses_by_run(conn, action.idempotency_key) == {
        run_1: "superseded",
        run_2: "proposed",
    }
    resolved_by, resolved_at = conn.execute(
        "SELECT resolved_by_run_id, resolved_at FROM run_actions "
        "WHERE proposed_by_run_id = ?",
        (run_1,),
    ).fetchone()
    assert resolved_by == run_2
    assert resolved_at is not None


def test_apply_supersedes_prior_pending_proposal(conn, db_ops):
    run_1 = _start_run(conn, dry_run=True)
    run_2 = _start_run(conn, dry_run=False)

    RunActionRecorder(db_ops, run_1).record(_action(status="proposed"))
    RunActionRecorder(db_ops, run_2).record(_action(status="applied"))
    conn.commit()

    key = _action().idempotency_key
    assert _statuses_by_run(conn, key) == {
        run_1: "superseded",
        run_2: "applied",
    }


def test_skipped_supersedes_but_failed_does_not(conn, db_ops):
    run_1 = _start_run(conn, dry_run=True)
    run_2 = _start_run(conn, dry_run=False)
    run_3 = _start_run(conn, dry_run=True)
    run_4 = _start_run(conn, dry_run=False)

    RunActionRecorder(db_ops, run_1).record(_action(status="proposed"))
    RunActionRecorder(db_ops, run_2).record(_action(status="failed"))
    conn.commit()
    key = _action().idempotency_key
    assert _statuses_by_run(conn, key)[run_1] == "proposed", (
        "a failed attempt must leave the prior proposal pending"
    )

    RunActionRecorder(db_ops, run_3).record(
        _action(file_id=2, status="proposed"))
    RunActionRecorder(db_ops, run_4).record(
        _action(file_id=2, status="skipped"))
    conn.commit()
    key_2 = _action(file_id=2).idempotency_key
    assert _statuses_by_run(conn, key_2)[run_3] == "superseded"


def test_rerecording_same_run_does_not_supersede_own_proposal(conn, db_ops):
    run_1 = _start_run(conn, dry_run=True)
    action = _action()

    recorder = RunActionRecorder(db_ops, run_1)
    recorder.record(action)
    recorder.record(action)
    conn.commit()

    assert _statuses_by_run(conn, action.idempotency_key) == {run_1: "proposed"}


def test_rejected_proposal_is_not_superseded_by_later_runs(conn, db_ops):
    run_1 = _start_run(conn, dry_run=True)
    RunActionRecorder(db_ops, run_1).record(_action())
    action_id = conn.execute(
        "SELECT id FROM run_actions WHERE proposed_by_run_id = ?", (run_1,)
    ).fetchone()[0]
    reject_proposals(db_ops, [action_id], run_id=None, note="not wanted")

    run_2 = _start_run(conn, dry_run=True)
    RunActionRecorder(db_ops, run_2).record(_action())
    conn.commit()

    assert _statuses_by_run(conn, _action().idempotency_key) == {
        run_1: "rejected",
        run_2: "proposed",
    }


# ---------------------------------------------------------------------------
# Scope-level supersede of stale proposals
# ---------------------------------------------------------------------------

def test_scope_supersede_resolves_unregenerated_proposals(conn, db_ops):
    run_1 = _start_run(conn, dry_run=True)
    run_2 = _start_run(conn, dry_run=False)

    # run_1 proposed something run_2 never regenerated (file vanished).
    RunActionRecorder(db_ops, run_1).record(_action())
    count = supersede_stale_proposals(db_ops, run_2)
    conn.commit()

    assert count == 1
    assert _statuses_by_run(conn, _action().idempotency_key) == {run_1: "superseded"}


def test_scope_supersede_ignores_other_scopes_and_own_run(conn, db_ops):
    same_scope_old = _start_run(conn, dry_run=True)
    other_command = _start_run(conn, dry_run=True, command="organize")
    other_dest = _start_run(conn, dry_run=True, dest_root="D:/Other")
    current = _start_run(conn, dry_run=True)

    RunActionRecorder(db_ops, same_scope_old).record(_action(file_id=1))
    RunActionRecorder(db_ops, other_command).record(_action(file_id=2))
    RunActionRecorder(db_ops, other_dest).record(_action(file_id=3))
    RunActionRecorder(db_ops, current).record(_action(file_id=4))

    count = supersede_stale_proposals(db_ops, current)
    conn.commit()

    assert count == 1
    statuses = dict(conn.execute(
        "SELECT proposed_by_run_id, status FROM run_actions"
    ).fetchall())
    assert statuses == {
        same_scope_old: "superseded",
        other_command: "proposed",
        other_dest: "proposed",
        current: "proposed",
    }


def test_scope_supersede_only_considers_earlier_runs(conn, db_ops):
    run_1 = _start_run(conn, dry_run=True)
    run_2 = _start_run(conn, dry_run=True)

    RunActionRecorder(db_ops, run_2).record(_action())
    count = supersede_stale_proposals(db_ops, run_1)
    conn.commit()

    assert count == 0
    assert _statuses_by_run(conn, _action().idempotency_key) == {run_2: "proposed"}


def test_scope_supersede_unknown_run_is_noop(conn, db_ops):
    assert supersede_stale_proposals(db_ops, 9999) == 0


# ---------------------------------------------------------------------------
# Rejection
# ---------------------------------------------------------------------------

def test_reject_proposals_sets_resolution_and_skips_non_pending(conn, db_ops):
    run_1 = _start_run(conn, dry_run=True)
    reviewer_run = _start_run(conn, dry_run=False, tool="photo-catalog-query",
                              command="reject-proposal", dest_root=None)
    RunActionRecorder(db_ops, run_1).record(_action(file_id=1))
    RunActionRecorder(db_ops, run_1).record(_action(file_id=2, status="applied"))
    conn.commit()

    pending_id, applied_id = [
        row[0] for row in conn.execute(
            "SELECT id FROM run_actions ORDER BY id").fetchall()
    ]

    rejected, skipped = reject_proposals(
        db_ops, [pending_id, applied_id, 12345],
        run_id=reviewer_run, note="wrong destination",
    )
    conn.commit()

    assert rejected == [pending_id]
    assert skipped == [applied_id, 12345]
    status, resolved_by, note = conn.execute(
        "SELECT status, resolved_by_run_id, resolution_note FROM run_actions WHERE id = ?",
        (pending_id,),
    ).fetchone()
    assert (status, resolved_by, note) == ("rejected", reviewer_run, "wrong destination")


# ---------------------------------------------------------------------------
# list_proposals
# ---------------------------------------------------------------------------

def test_list_proposals_filters_and_joins_run_context(conn, db_ops):
    run_1 = _start_run(conn, dry_run=True)
    RunActionRecorder(db_ops, run_1).record(_action(file_id=1))
    RunActionRecorder(db_ops, run_1).record(_action(file_id=2, status="applied"))
    conn.commit()

    pending = list_proposals(db_ops)
    assert len(pending) == 1
    row = pending[0]
    assert row["status"] == "proposed"
    assert row["action_type"] == "update_canonical_dest_path"
    assert row["proposed_by_run_id"] == run_1
    assert row["command"] == "ingest-dest"
    assert row["run_dry_run"] == 1

    assert list_proposals(db_ops, status="applied")[0]["entity_id"] == 2
    assert list_proposals(db_ops, action_type="no_such_type") == []
    assert list_proposals(db_ops, proposed_by_run_id=run_1 + 100) == []


# ---------------------------------------------------------------------------
# End-to-end: dry-run proposals are resolved by the real run
# ---------------------------------------------------------------------------

def test_real_organize_run_leaves_no_pending_proposals_for_scope(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    dest = tmp_path / "dest"
    (src / "IMG_1000.CR2").write_bytes(b"raw bytes" * 500)
    (src / "IMG_1000.xmp").write_bytes(b"<xmp/>")

    db_path = tmp_path / "catalog.db"
    c = sqlite3.connect(str(db_path))
    init_schema(c)

    def start_pipeline_run(dry_run):
        run_id = _start_run(c, dry_run=dry_run, command="organize",
                            src_root=str(src), dest_root=str(dest))
        c.commit()
        return run_id

    app = PhotoOrganizerApp(db_path)
    dry_run_id = start_pipeline_run(dry_run=True)
    app.organize(src_root=src, dest_root=dest, dry_run=True,
                 dry_run_csv=tmp_path / "preview.csv", run_id=dry_run_id)

    pending_before = c.execute(
        "SELECT COUNT(*) FROM run_actions WHERE status = 'proposed'"
    ).fetchone()[0]
    assert pending_before > 0

    real_run_id = start_pipeline_run(dry_run=False)
    app.organize(src_root=src, dest_root=dest, dry_run=False,
                 run_id=real_run_id)

    rows = c.execute(
        "SELECT status, resolved_by_run_id FROM run_actions "
        "WHERE proposed_by_run_id = ?",
        (dry_run_id,),
    ).fetchall()
    c.close()

    assert rows, "dry-run proposals must survive as history"
    assert all(status == "superseded" for status, _ in rows)
    assert all(resolved_by == real_run_id for _, resolved_by in rows)


# ---------------------------------------------------------------------------
# photo-catalog-query surface
# ---------------------------------------------------------------------------

def test_show_proposals_prints_pending_rows(conn, db_ops, capsys):
    run_1 = _start_run(conn, dry_run=True)
    RunActionRecorder(db_ops, run_1).record(_action())
    conn.commit()

    pcq.show_proposals(conn)
    out = capsys.readouterr().out
    assert "update_canonical_dest_path" in out
    assert f"proposed by run #{run_1}" in out
    assert "C:/Photos/new.jpg" in out


def test_show_proposals_empty_message(conn, capsys):
    pcq.show_proposals(conn)
    assert "No run_actions rows" in capsys.readouterr().out


def test_reject_proposal_cmd_records_command_run(tmp_path, capsys):
    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    db_ops = DBOperations(conn)
    run_1 = _start_run(conn, dry_run=True)
    RunActionRecorder(db_ops, run_1).record(_action())
    conn.commit()
    action_id = conn.execute(
        "SELECT id FROM run_actions WHERE proposed_by_run_id = ?", (run_1,)
    ).fetchone()[0]

    pcq.reject_proposals_cmd(conn, db_path, [action_id, 999], note="duplicate")
    out = capsys.readouterr().out
    assert f"Rejected 1 proposal(s): {action_id}" in out
    assert "Skipped 1 id(s)" in out

    status, resolved_by, note = conn.execute(
        "SELECT status, resolved_by_run_id, resolution_note "
        "FROM run_actions WHERE id = ?",
        (action_id,),
    ).fetchone()
    assert status == "rejected"
    assert note == "duplicate"

    reviewer = conn.execute(
        "SELECT id, tool, command, exit_status, db_mutates FROM command_runs "
        "WHERE command = 'reject-proposal'"
    ).fetchone()
    conn.close()
    assert reviewer is not None
    assert reviewer[0] == resolved_by
    assert reviewer[1] == "photo-catalog-query"
    assert reviewer[3] == "success"
    assert reviewer[4] == 1
