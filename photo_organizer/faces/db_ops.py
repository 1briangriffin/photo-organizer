"""
Database operations for face observations and accepted face state.
"""
import json
import struct
from datetime import datetime, UTC
from typing import Mapping, Optional, Sequence

from ..database.ops import DBOperations
from ..pipeline.actions import (
    ActionSpec,
    PHASE_FACE_CLUSTER_APPLY,
    PHASE_FACE_DETECT_OBSERVE,
    PHASE_FACE_EMBED_OBSERVE,
    PHASE_FACE_PERSON_LINK_APPLY,
    RunActionRecorder,
)


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _json_payload(payload: Optional[Mapping]) -> Optional[str]:
    if payload is None:
        return None
    return json.dumps(payload, default=str, ensure_ascii=False, sort_keys=True)


def _pack_embedding(values: Sequence[float]) -> bytes:
    return struct.pack(f"{len(values)}f", *[float(v) for v in values])


class FaceDBOperations:
    def __init__(self, db_ops: DBOperations):
        self.db = db_ops

    def record_detection(
        self,
        *,
        run_id: int,
        file_id: int,
        detection_index: int,
        bbox: tuple[float, float, float, float],
        confidence: Optional[float],
        model_name: str,
        model_version: str,
        image_hash: Optional[str],
        payload: Optional[Mapping] = None,
    ) -> int:
        """Record a model-produced face detection observation."""
        now = _iso_now()
        self.db.conn.execute(
            """
            INSERT OR IGNORE INTO face_detections (
                file_id, detection_index, bbox_x, bbox_y, bbox_w, bbox_h,
                confidence, model_name, model_version, image_hash, status,
                observed_by_run_id, created_at, payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'observed', ?, ?, ?)
            """,
            (
                file_id,
                detection_index,
                bbox[0],
                bbox[1],
                bbox[2],
                bbox[3],
                confidence,
                model_name,
                model_version,
                image_hash,
                run_id,
                now,
                _json_payload(payload),
            ),
        )
        row = self.db.conn.execute(
            """
            SELECT id FROM face_detections
            WHERE file_id = ?
              AND model_name = ?
              AND model_version = ?
              AND COALESCE(image_hash, '') = COALESCE(?, '')
              AND detection_index = ?
            """,
            (file_id, model_name, model_version, image_hash, detection_index),
        ).fetchone()
        if row is None:
            raise RuntimeError("face_detections insert failed to return an identity")
        detection_id = int(row[0])

        RunActionRecorder(self.db, run_id).record(ActionSpec(
            action_type="face_detect",
            entity_type="face_detection",
            entity_id=detection_id,
            source_path=None,
            target_path=None,
            status="applied",
            phase=PHASE_FACE_DETECT_OBSERVE,
            sequence=detection_index,
            idempotency_key=(
                f"face_detect:{model_name}:{model_version}:{file_id}:"
                f"{image_hash or ''}:{detection_index}"
            ),
            confidence=int(confidence * 100) if confidence is not None else None,
            method=model_name,
            payload={"file_id": file_id, "detection_index": detection_index},
        ))
        return detection_id

    def record_embedding(
        self,
        *,
        run_id: int,
        detection_id: int,
        embedding: Sequence[float],
        model_name: str,
        model_version: str,
    ) -> int:
        """Record an embedding vector for an observed detection."""
        now = _iso_now()
        blob = _pack_embedding(embedding)
        self.db.conn.execute(
            """
            INSERT OR IGNORE INTO face_embeddings (
                detection_id, model_name, model_version, vector_dim, embedding,
                observed_by_run_id, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (detection_id, model_name, model_version, len(embedding), blob, run_id, now),
        )
        row = self.db.conn.execute(
            """
            SELECT id FROM face_embeddings
            WHERE detection_id = ? AND model_name = ? AND model_version = ?
            """,
            (detection_id, model_name, model_version),
        ).fetchone()
        if row is None:
            raise RuntimeError("face_embeddings insert failed to return an identity")
        embedding_id = int(row[0])

        RunActionRecorder(self.db, run_id).record(ActionSpec(
            action_type="face_embed",
            entity_type="face_embedding",
            entity_id=embedding_id,
            source_path=None,
            target_path=None,
            status="applied",
            phase=PHASE_FACE_EMBED_OBSERVE,
            sequence=0,
            idempotency_key=f"face_embed:{model_name}:{model_version}:{detection_id}",
            method=model_name,
            payload={"detection_id": detection_id, "vector_dim": len(embedding)},
        ))
        return embedding_id

    def propose_cluster_assignment(
        self,
        *,
        run_id: int,
        detection_id: int,
        cluster_key: str,
        model_name: str,
        model_version: str,
        confidence: Optional[float] = None,
    ) -> int:
        """Create a durable proposed cluster assignment action."""
        now = _iso_now()
        cur = self.db.conn.execute(
            """
            INSERT INTO face_clusters (
                cluster_key, model_name, model_version, status,
                created_by_run_id, updated_by_run_id, created_at, updated_at
            )
            VALUES (?, ?, ?, 'proposed', ?, ?, ?, ?)
            ON CONFLICT(cluster_key, model_name, model_version) DO UPDATE SET
                updated_by_run_id = excluded.updated_by_run_id,
                updated_at = excluded.updated_at
            RETURNING id
            """,
            (cluster_key, model_name, model_version, run_id, run_id, now, now),
        )
        cluster_id = int(cur.fetchone()[0])

        RunActionRecorder(self.db, run_id).record(ActionSpec(
            action_type="face_cluster_assign",
            entity_type="face_detection",
            entity_id=detection_id,
            source_path=None,
            target_path=None,
            status="proposed",
            phase=PHASE_FACE_CLUSTER_APPLY,
            sequence=0,
            idempotency_key=f"face_cluster_assign:{cluster_id}:{detection_id}",
            confidence=int(confidence * 100) if confidence is not None else None,
            method=model_name,
            payload={"cluster_id": cluster_id, "detection_id": detection_id},
        ))
        return cluster_id

    def create_person(
        self,
        *,
        run_id: int,
        display_name: Optional[str],
        status: str = "active",
        payload: Optional[Mapping] = None,
    ) -> int:
        now = _iso_now()
        cur = self.db.conn.execute(
            """
            INSERT INTO face_persons (
                display_name, status, created_by_run_id, updated_by_run_id,
                created_at, updated_at, payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (display_name, status, run_id, run_id, now, now, _json_payload(payload)),
        )
        if cur.lastrowid is None:
            raise RuntimeError("face_persons insert failed to return an identity")
        return int(cur.lastrowid)

    def link_detection_to_person(
        self,
        *,
        run_id: int,
        detection_id: int,
        person_id: int,
        confidence: Optional[float],
        link_method: str,
        status: str = "accepted",
    ) -> int:
        now = _iso_now()
        self.db.conn.execute(
            """
            INSERT OR IGNORE INTO face_person_links (
                person_id, detection_id, cluster_id, confidence, link_method, status,
                created_by_run_id, updated_by_run_id, created_at, updated_at
            )
            VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                person_id,
                detection_id,
                confidence,
                link_method,
                status,
                run_id,
                run_id,
                now,
                now,
            ),
        )
        row = self.db.conn.execute(
            """
            SELECT id FROM face_person_links
            WHERE person_id = ? AND detection_id = ?
            """,
            (person_id, detection_id),
        ).fetchone()
        if row is None:
            raise RuntimeError("face_person_links insert failed to return an identity")
        link_id = int(row[0])

        RunActionRecorder(self.db, run_id).record(ActionSpec(
            action_type="face_person_link",
            entity_type="face_person_link",
            entity_id=link_id,
            source_path=None,
            target_path=None,
            status="applied" if status == "accepted" else status,
            phase=PHASE_FACE_PERSON_LINK_APPLY,
            sequence=0,
            idempotency_key=f"face_person_link:{person_id}:detection:{detection_id}",
            confidence=int(confidence * 100) if confidence is not None else None,
            method=link_method,
            payload={"person_id": person_id, "detection_id": detection_id},
        ))
        return link_id
