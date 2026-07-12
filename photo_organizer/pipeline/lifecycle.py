"""
Proposal lifecycle management for run_actions.

A run_action row with status 'proposed' is a pending proposal (typically from
a dry-run). Pending proposals resolve one of three ways:

- superseded: a newer run re-proposed, applied, or obsoleted the same action.
  Key-level supersede happens automatically in RunActionRecorder.record();
  scope-level supersede (proposals the newer run did not regenerate at all)
  happens via supersede_stale_proposals() at the end of a successful run of
  the same command scope.
- rejected: a user explicitly declined the proposal (reject_proposals()).
- The proposal simply describes work a later run performed: the applying run
  records its own 'applied'/'skipped'/'failed' row (attempt history is
  per-run), and the pending row is marked superseded as above.

Real runs never re-apply a stored proposal by run_id; they always recompute
against current disk state. Proposals are review artifacts, not work queues.
"""
import logging
from datetime import datetime, UTC
from typing import Any, Optional, Sequence

from ..database.ops import DBOperations
from .actions import ActionSpec, PHASE_FILE_STATUS_APPLY, RunActionRecorder

PENDING_STATUS = "proposed"


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()


def supersede_stale_proposals(db_ops: DBOperations, run_id: int) -> int:
    """
    Mark still-pending proposals from earlier runs of this run's command scope
    as superseded.

    Scope = same (tool, command, src_root, dest_root) as the given run. This
    catches proposals the current run did not regenerate at all (e.g. the file
    was deleted or renamed since the proposing dry-run), which key-level
    supersede in RunActionRecorder can never touch.

    Returns the number of proposals superseded.
    """
    row = db_ops.conn.execute(
        "SELECT tool, command, src_root, dest_root FROM command_runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    if row is None:
        return 0
    tool, command, src_root, dest_root = row

    cur = db_ops.conn.execute(
        """
        UPDATE run_actions
           SET status = 'superseded',
               resolved_by_run_id = ?,
               resolved_at = ?
         WHERE status = 'proposed'
           AND proposed_by_run_id != ?
           AND proposed_by_run_id IN (
               SELECT id FROM command_runs
                WHERE tool = ?
                  AND command = ?
                  AND src_root IS ?
                  AND dest_root IS ?
                  AND id < ?
           )
        """,
        (run_id, _iso_now(), run_id, tool, command, src_root, dest_root, run_id),
    )
    count = cur.rowcount if cur.rowcount is not None and cur.rowcount > 0 else 0
    if count:
        logging.info(
            f"Superseded {count} stale proposal(s) from earlier "
            f"{tool} {command} runs of the same scope."
        )
    return count


def reject_proposals(
    db_ops: DBOperations,
    action_ids: Sequence[int],
    *,
    run_id: Optional[int],
    note: Optional[str] = None,
) -> tuple[list[int], list[int]]:
    """
    Mark pending proposals as rejected by user decision.

    Only rows currently in 'proposed' status are touched; ids that are missing
    or already resolved are reported back rather than modified.

    Returns (rejected_ids, skipped_ids).
    """
    now = _iso_now()
    rejected: list[int] = []
    skipped: list[int] = []
    for action_id in action_ids:
        cur = db_ops.conn.execute(
            """
            UPDATE run_actions
               SET status = 'rejected',
                   resolved_by_run_id = ?,
                   resolved_at = ?,
                   resolution_note = ?
             WHERE id = ?
               AND status = 'proposed'
            """,
            (run_id, now, note, int(action_id)),
        )
        if cur.rowcount:
            rejected.append(int(action_id))
        else:
            skipped.append(int(action_id))
    return rejected, skipped


def _set_file_status_with_audit(
    db_ops: DBOperations,
    file_ids: Sequence[int],
    *,
    status: str,
    run_id: Optional[int],
    note: Optional[str],
) -> tuple[list[int], list[int]]:
    action_type = "retire_file" if status == "retired" else "restore_file"
    changed, skipped = db_ops.set_file_status(
        file_ids, status=status, run_id=run_id, note=note,
    )
    recorder = RunActionRecorder(db_ops, run_id)
    for sequence, file_id in enumerate(changed, start=1):
        row = db_ops.conn.execute(
            "SELECT dest_path FROM files WHERE id = ?", (file_id,)
        ).fetchone()
        recorder.record(ActionSpec(
            action_type=action_type,
            entity_type="file",
            entity_id=file_id,
            source_path=row[0] if row else None,
            target_path=None,
            status="applied",
            phase=PHASE_FILE_STATUS_APPLY,
            sequence=sequence,
            idempotency_key=f"{action_type}:{file_id}",
            method="user_decision",
            payload={"note": note} if note else None,
        ))
    return changed, skipped


def retire_files(
    db_ops: DBOperations,
    file_ids: Sequence[int],
    *,
    run_id: Optional[int],
    note: Optional[str] = None,
) -> tuple[list[int], list[int]]:
    """
    Mark files as intentionally deleted/retired.

    Retired files keep their catalog history (hashes, links, occurrences) but
    are excluded from expected-on-disk state: validate-dest stops reporting
    them as MISSING and sync/reconciliation no longer match against them.
    A re-encountered copy of a retired file's content still deduplicates
    against the retired row — restore_files() is the explicit way back.

    Returns (retired_ids, skipped_ids) — skipped ids were missing or already
    retired.
    """
    return _set_file_status_with_audit(
        db_ops, file_ids, status="retired", run_id=run_id, note=note,
    )


def restore_files(
    db_ops: DBOperations,
    file_ids: Sequence[int],
    *,
    run_id: Optional[int],
    note: Optional[str] = None,
) -> tuple[list[int], list[int]]:
    """Reactivate previously retired files. Returns (restored_ids, skipped_ids)."""
    return _set_file_status_with_audit(
        db_ops, file_ids, status="active", run_id=run_id, note=note,
    )


def list_proposals(
    db_ops: DBOperations,
    *,
    status: str = PENDING_STATUS,
    action_type: Optional[str] = None,
    proposed_by_run_id: Optional[int] = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    Return run_action rows of the given status, newest first, joined with
    proposing-run context so a reviewer can judge staleness.
    """
    filters = ["ra.status = ?"]
    params: list[Any] = [status]
    if action_type:
        filters.append("ra.action_type = ?")
        params.append(action_type)
    if proposed_by_run_id is not None:
        filters.append("ra.proposed_by_run_id = ?")
        params.append(proposed_by_run_id)
    params.append(int(limit))

    cur = db_ops.conn.execute(
        f"""
        SELECT ra.id, ra.action_type, ra.entity_type, ra.entity_id,
               ra.source_path, ra.target_path, ra.status, ra.confidence,
               ra.method, ra.created_at, ra.proposed_by_run_id,
               ra.resolved_by_run_id, ra.resolved_at, ra.resolution_note,
               cr.tool, cr.command, cr.started_at AS run_started_at,
               cr.dry_run AS run_dry_run
          FROM run_actions ra
          LEFT JOIN command_runs cr ON cr.id = ra.proposed_by_run_id
         WHERE {" AND ".join(filters)}
         ORDER BY ra.id DESC
         LIMIT ?
        """,
        params,
    )
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]
