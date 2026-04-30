"""
Durable observation recording for command runs.
"""
import json
from datetime import datetime, UTC
from pathlib import Path
from typing import Any, Mapping, Optional

from ..database.ops import DBOperations
from ..path_identity import normalize_path_key


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _json_payload(payload: Optional[Mapping[str, Any]]) -> Optional[str]:
    if payload is None:
        return None
    return json.dumps(payload, default=str, ensure_ascii=False, sort_keys=True)


class ObservationRecorder:
    """Append observations and update the latest-location cache."""

    def __init__(self, db_ops: DBOperations, run_id: Optional[int]):
        self.db = db_ops
        self.run_id = run_id

    @property
    def enabled(self) -> bool:
        return self.run_id is not None

    def record_file_present(
        self,
        *,
        file_id: int,
        path: Path,
        root_kind: str,
        hash_value: Optional[str] = None,
        sparse_hash: Optional[str] = None,
        hash_is_sparse: bool = False,
        size_bytes: Optional[int] = None,
        mtime: Optional[float] = None,
        match_method: Optional[str] = None,
        confidence: Optional[int] = None,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.record_file_observation(
            observation_type="present",
            file_id=file_id,
            path=path,
            root_kind=root_kind,
            hash_value=hash_value,
            sparse_hash=sparse_hash,
            hash_is_sparse=hash_is_sparse,
            size_bytes=size_bytes,
            mtime=mtime,
            match_method=match_method,
            confidence=confidence,
            payload=payload,
        )

    def record_missing_expected(
        self,
        *,
        file_id: int,
        path: Path,
        root_kind: str = "dest",
        payload: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.record_file_observation(
            observation_type="missing_expected",
            file_id=file_id,
            path=path,
            root_kind=root_kind,
            payload=payload,
        )

    def record_rename_candidate(
        self,
        *,
        file_id: int,
        old_path: Path,
        new_path: Path,
        root_kind: str = "dest",
        hash_value: Optional[str] = None,
        sparse_hash: Optional[str] = None,
        hash_is_sparse: bool = False,
        size_bytes: Optional[int] = None,
        mtime: Optional[float] = None,
        match_method: Optional[str] = None,
        confidence: Optional[int] = None,
    ) -> None:
        self.record_file_observation(
            observation_type="renamed_candidate",
            file_id=file_id,
            path=new_path,
            root_kind=root_kind,
            hash_value=hash_value,
            sparse_hash=sparse_hash,
            hash_is_sparse=hash_is_sparse,
            size_bytes=size_bytes,
            mtime=mtime,
            match_method=match_method,
            confidence=confidence,
            payload={"old_path": str(old_path)},
        )

    def record_move_candidate(
        self,
        *,
        file_id: int,
        old_path: Path,
        new_path: Path,
        root_kind: str = "dest",
        hash_value: Optional[str] = None,
        sparse_hash: Optional[str] = None,
        hash_is_sparse: bool = False,
        size_bytes: Optional[int] = None,
        mtime: Optional[float] = None,
        match_method: Optional[str] = None,
        confidence: Optional[int] = None,
    ) -> None:
        self.record_file_observation(
            observation_type="moved_candidate",
            file_id=file_id,
            path=new_path,
            root_kind=root_kind,
            hash_value=hash_value,
            sparse_hash=sparse_hash,
            hash_is_sparse=hash_is_sparse,
            size_bytes=size_bytes,
            mtime=mtime,
            match_method=match_method,
            confidence=confidence,
            payload={"old_path": str(old_path)},
        )

    def record_new_candidate(
        self,
        *,
        path: Path,
        root_kind: str = "dest",
        hash_value: Optional[str] = None,
        sparse_hash: Optional[str] = None,
        hash_is_sparse: bool = False,
        size_bytes: Optional[int] = None,
        mtime: Optional[float] = None,
        file_id: Optional[int] = None,
    ) -> None:
        self.record_file_observation(
            observation_type="new_candidate",
            file_id=file_id,
            path=path,
            root_kind=root_kind,
            hash_value=hash_value,
            sparse_hash=sparse_hash,
            hash_is_sparse=hash_is_sparse,
            size_bytes=size_bytes,
            mtime=mtime,
        )

    def record_file_observation(
        self,
        *,
        observation_type: str,
        file_id: Optional[int],
        path: Path,
        root_kind: str,
        hash_value: Optional[str] = None,
        sparse_hash: Optional[str] = None,
        hash_is_sparse: bool = False,
        size_bytes: Optional[int] = None,
        mtime: Optional[float] = None,
        match_method: Optional[str] = None,
        confidence: Optional[int] = None,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> None:
        if not self.enabled:
            return

        observed_at = _iso_now()
        path_key = normalize_path_key(path)
        if path_key is None:
            return

        self.db.conn.execute(
            """
            INSERT INTO file_observations (
                run_id, file_id, observed_at, observation_type, path, path_key,
                root_kind, root_path_key, hash, sparse_hash, hash_is_sparse,
                size_bytes, mtime, match_method, confidence, payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.run_id,
                file_id,
                observed_at,
                observation_type,
                str(path),
                path_key,
                root_kind,
                None,
                hash_value if not hash_is_sparse else None,
                sparse_hash or (hash_value if hash_is_sparse else None),
                int(hash_is_sparse),
                size_bytes,
                mtime,
                match_method,
                confidence,
                _json_payload(payload),
            ),
        )

        if file_id is not None:
            status = "missing" if observation_type == "missing_expected" else "present"
            self.materialize_latest_state(
                file_id=file_id,
                path=path,
                path_key=path_key,
                root_kind=root_kind,
                status=status,
                observed_at=observed_at,
                hash_value=hash_value,
                sparse_hash=sparse_hash,
                hash_is_sparse=hash_is_sparse,
                size_bytes=size_bytes,
                mtime=mtime,
            )

    def materialize_latest_state(
        self,
        *,
        file_id: int,
        path: Path,
        path_key: str,
        root_kind: str,
        status: str,
        observed_at: str,
        hash_value: Optional[str],
        sparse_hash: Optional[str],
        hash_is_sparse: bool,
        size_bytes: Optional[int],
        mtime: Optional[float],
    ) -> None:
        self.db.conn.execute(
            """
            INSERT INTO file_location_state (
                file_id, path, path_key, root_kind, root_path_key, status,
                first_observed_run_id, last_observed_run_id, first_seen_at, last_seen_at,
                hash, sparse_hash, hash_is_sparse, size_bytes, mtime
            )
            VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path_key) DO UPDATE SET
                file_id = excluded.file_id,
                path = excluded.path,
                root_kind = excluded.root_kind,
                status = excluded.status,
                last_observed_run_id = excluded.last_observed_run_id,
                last_seen_at = excluded.last_seen_at,
                hash = excluded.hash,
                sparse_hash = excluded.sparse_hash,
                hash_is_sparse = excluded.hash_is_sparse,
                size_bytes = excluded.size_bytes,
                mtime = excluded.mtime
            """,
            (
                file_id,
                str(path),
                path_key,
                root_kind,
                status,
                self.run_id,
                self.run_id,
                observed_at,
                observed_at,
                hash_value if not hash_is_sparse else None,
                sparse_hash or (hash_value if hash_is_sparse else None),
                int(hash_is_sparse),
                size_bytes,
                mtime,
            ),
        )
