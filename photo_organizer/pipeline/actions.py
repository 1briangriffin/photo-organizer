"""
Durable run action recording.
"""
import json
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from ..database.ops import DBOperations
from ..path_identity import normalize_path_key


PHASE_CANONICAL_PATH_APPLY = 40
PHASE_FILE_STATUS_APPLY = 50
PHASE_RELATIONSHIP_APPLY = 60
PHASE_FILESYSTEM_APPLY = 90
PHASE_FACE_DETECT_OBSERVE = 210
PHASE_FACE_EMBED_OBSERVE = 220
PHASE_FACE_CLUSTER_APPLY = 240
PHASE_FACE_PERSON_LINK_APPLY = 260


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _json_payload(payload: Optional[Mapping[str, Any]]) -> Optional[str]:
    if payload is None:
        return None
    return json.dumps(payload, default=str, ensure_ascii=False, sort_keys=True)


@dataclass(frozen=True)
class ActionSpec:
    action_type: str
    entity_type: str
    entity_id: Optional[int]
    source_path: Optional[str]
    target_path: Optional[str]
    status: str
    phase: int
    sequence: int
    idempotency_key: str
    confidence: Optional[int] = None
    method: Optional[str] = None
    payload: Optional[Mapping[str, Any]] = None
    error_message: Optional[str] = None


# Recording an action with one of these statuses resolves any still-pending
# proposal of the same idempotency_key from an earlier run: the newer run
# either re-proposed it, applied it, or found it already satisfied. A 'failed'
# attempt does NOT supersede — the prior proposal may still be worth applying.
_SUPERSEDING_STATUSES = frozenset({"proposed", "applied", "skipped"})


class RunActionRecorder:
    def __init__(self, db_ops: DBOperations, run_id: Optional[int]):
        self.db = db_ops
        self.run_id = run_id

    @property
    def enabled(self) -> bool:
        return self.run_id is not None

    def record_many(self, actions: Iterable[ActionSpec]) -> None:
        if not self.enabled:
            return
        for action in actions:
            self.record(action)

    def _supersede_prior_proposals(self, idempotency_key: str, now: str) -> None:
        self.db.conn.execute(
            """
            UPDATE run_actions
               SET status = 'superseded',
                   resolved_by_run_id = ?,
                   resolved_at = ?
             WHERE idempotency_key = ?
               AND status = 'proposed'
               AND proposed_by_run_id != ?
            """,
            (self.run_id, now, idempotency_key, self.run_id),
        )

    def record(self, action: ActionSpec) -> None:
        if not self.enabled:
            return

        now = _iso_now()
        if action.status in _SUPERSEDING_STATUSES:
            self._supersede_prior_proposals(action.idempotency_key, now)
        applied_by_run_id = self.run_id if action.status != "proposed" else None
        applied_at = now if action.status != "proposed" else None
        source_path_key = normalize_path_key(action.source_path)
        target_path_key = normalize_path_key(action.target_path)

        if action.status == "proposed":
            self.db.conn.execute(
                """
                INSERT OR IGNORE INTO run_actions (
                    proposed_by_run_id, applied_by_run_id, action_type, entity_type,
                    entity_id, source_path, source_path_key, target_path, target_path_key,
                    status, confidence, method, idempotency_key, phase, sequence,
                    payload_json, created_at, applied_at, error_message
                )
                VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
                """,
                (
                    self.run_id,
                    action.action_type,
                    action.entity_type,
                    action.entity_id,
                    action.source_path,
                    source_path_key,
                    action.target_path,
                    target_path_key,
                    action.status,
                    action.confidence,
                    action.method,
                    action.idempotency_key,
                    action.phase,
                    action.sequence,
                    _json_payload(action.payload),
                    now,
                    action.error_message,
                ),
            )
            return

        self.db.conn.execute(
            """
            INSERT INTO run_actions (
                proposed_by_run_id, applied_by_run_id, action_type, entity_type,
                entity_id, source_path, source_path_key, target_path, target_path_key,
                status, confidence, method, idempotency_key, phase, sequence,
                payload_json, created_at, applied_at, error_message
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(proposed_by_run_id, idempotency_key) DO UPDATE SET
                applied_by_run_id = excluded.applied_by_run_id,
                entity_id = excluded.entity_id,
                source_path = excluded.source_path,
                source_path_key = excluded.source_path_key,
                target_path = excluded.target_path,
                target_path_key = excluded.target_path_key,
                status = excluded.status,
                confidence = excluded.confidence,
                method = excluded.method,
                phase = excluded.phase,
                sequence = excluded.sequence,
                payload_json = excluded.payload_json,
                applied_at = excluded.applied_at,
                error_message = excluded.error_message
            """,
            (
                self.run_id,
                applied_by_run_id,
                action.action_type,
                action.entity_type,
                action.entity_id,
                action.source_path,
                source_path_key,
                action.target_path,
                target_path_key,
                action.status,
                action.confidence,
                action.method,
                action.idempotency_key,
                action.phase,
                action.sequence,
                _json_payload(action.payload),
                now,
                applied_at,
                action.error_message,
            ),
        )


def canonical_path_action(
    *,
    file_id: int,
    old_path: str,
    new_path: str,
    status: str,
    sequence: int,
    confidence: int = 100,
    method: str = "hash_same_directory",
) -> ActionSpec:
    target_key = normalize_path_key(new_path) or str(new_path)
    return ActionSpec(
        action_type="update_canonical_dest_path",
        entity_type="file",
        entity_id=file_id,
        source_path=old_path,
        target_path=new_path,
        status=status,
        confidence=confidence,
        method=method,
        phase=PHASE_CANONICAL_PATH_APPLY,
        sequence=sequence,
        idempotency_key=f"update_canonical_dest_path:{file_id}:{target_key}",
    )


def link_action(
    *,
    action_type: str,
    entity_id: int,
    left_id: int,
    right_id: int,
    status: str,
    sequence: int,
    confidence: Optional[int] = None,
    method: Optional[str] = None,
) -> ActionSpec:
    return ActionSpec(
        action_type=action_type,
        entity_type="relationship",
        entity_id=entity_id,
        source_path=None,
        target_path=None,
        status=status,
        confidence=confidence,
        method=method,
        phase=PHASE_RELATIONSHIP_APPLY,
        sequence=sequence,
        idempotency_key=f"{action_type}:{left_id}:{right_id}",
        payload={"left_id": left_id, "right_id": right_id},
    )


def move_action(
    *,
    file_id: int,
    source_path: str,
    target_path: str,
    move_mode: bool,
    status: str,
    sequence: int,
) -> ActionSpec:
    action_type = "move_file" if move_mode else "copy_file"
    source_key = normalize_path_key(source_path) or source_path
    target_key = normalize_path_key(target_path) or target_path
    return ActionSpec(
        action_type=action_type,
        entity_type="file",
        entity_id=file_id,
        source_path=source_path,
        target_path=target_path,
        status=status,
        phase=PHASE_FILESYSTEM_APPLY,
        sequence=sequence,
        idempotency_key=f"{action_type}:{file_id}:{source_key}:{target_key}",
    )


def action_status_from_target(target_path: str) -> str:
    return "applied" if Path(target_path).exists() else "failed"
