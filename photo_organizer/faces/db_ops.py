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


def _unpack_embedding(blob: bytes, vector_dim: int) -> tuple[float, ...]:
    return struct.unpack(f"{vector_dim}f", blob)


# Sentinel detection_index marking "this file was scanned and no faces were
# found" so the unscanned-files query stops returning it. Sentinel rows carry
# status='no_faces' and zeroed bboxes; consumers of real detections must
# filter on status='observed' (or detection_index >= 0).
NO_FACES_DETECTION_INDEX = -1


class FaceDBOperations:
    def __init__(self, db_ops: DBOperations):
        self.db = db_ops

    def get_unscanned_files(
        self,
        *,
        model_name: str,
        model_version: str,
        eligible_types: Optional[set[str]] = None,
        limit: Optional[int] = None,
    ) -> list[tuple[int, str, Optional[str], str, Optional[str]]]:
        """
        Return active catalog files that have no face_detections row (real or
        no-faces sentinel) for the given model.

        RAW files whose capture already has a linked JPEG/TIFF output are
        always excluded — the demosaiced output is scanned instead, avoiding
        duplicate detections of the same capture.

        Shape: (file_id, orig_path, dest_path, type, hash).
        """
        from . import config

        types = eligible_types if eligible_types is not None else config.FACE_ELIGIBLE_TYPES
        placeholders = ",".join("?" for _ in types)
        params: list = [*types, model_name, model_version]
        limit_clause = ""
        if limit is not None:
            limit_clause = " LIMIT ?"
            params.append(int(limit))
        cur = self.db.conn.execute(
            f"""
            SELECT f.id, f.orig_path, f.dest_path, f.type, f.hash
            FROM files f
            WHERE f.type IN ({placeholders})
              AND f.status = 'active'
              AND f.id NOT IN (
                  SELECT file_id FROM face_detections
                  WHERE model_name = ? AND model_version = ?
              )
              AND f.id NOT IN (
                  SELECT ro.raw_file_id FROM raw_outputs ro
                  JOIN files outf ON ro.output_file_id = outf.id
                  WHERE outf.type IN ('jpeg', 'tiff')
              )
            ORDER BY f.id{limit_clause}
            """,
            params,
        )
        return cur.fetchall()

    def record_no_faces_scan(
        self,
        *,
        run_id: int,
        file_id: int,
        model_name: str,
        model_version: str,
        image_hash: Optional[str],
    ) -> None:
        """Record the no-faces sentinel so the file is not re-scanned."""
        self.db.conn.execute(
            """
            INSERT OR IGNORE INTO face_detections (
                file_id, detection_index, bbox_x, bbox_y, bbox_w, bbox_h,
                confidence, model_name, model_version, image_hash, status,
                observed_by_run_id, created_at, payload_json
            )
            VALUES (?, ?, 0, 0, 0, 0, NULL, ?, ?, ?, 'no_faces', ?, ?, NULL)
            """,
            (
                file_id,
                NO_FACES_DETECTION_INDEX,
                model_name,
                model_version,
                image_hash,
                run_id,
                _iso_now(),
            ),
        )

    def set_detection_thumbnail(self, detection_id: int, thumbnail_path: str) -> None:
        """Merge the thumbnail path into the detection's payload_json."""
        row = self.db.conn.execute(
            "SELECT payload_json FROM face_detections WHERE id = ?",
            (detection_id,),
        ).fetchone()
        if row is None:
            return
        payload = json.loads(row[0]) if row[0] else {}
        payload["thumbnail_path"] = thumbnail_path
        self.db.conn.execute(
            "UPDATE face_detections SET payload_json = ? WHERE id = ?",
            (_json_payload(payload), detection_id),
        )

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

    def upsert_cluster(
        self,
        *,
        run_id: int,
        cluster_key: str,
        model_name: str,
        model_version: str,
        era_start: Optional[str] = None,
        era_end: Optional[str] = None,
        representative_embedding: Optional[Sequence[float]] = None,
        status: str = "proposed",
        payload: Optional[Mapping] = None,
    ) -> int:
        """Create or refresh a cluster row with era range and representative
        (L2-normalized mean) embedding for cross-age linking."""
        now = _iso_now()
        rep_blob = (
            _pack_embedding(representative_embedding)
            if representative_embedding is not None else None
        )
        rep_dim = len(representative_embedding) if representative_embedding is not None else None
        cur = self.db.conn.execute(
            """
            INSERT INTO face_clusters (
                cluster_key, model_name, model_version, status,
                era_start, era_end, representative_embedding, representative_dim,
                created_by_run_id, updated_by_run_id, created_at, updated_at,
                payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cluster_key, model_name, model_version) DO UPDATE SET
                status = excluded.status,
                era_start = excluded.era_start,
                era_end = excluded.era_end,
                representative_embedding = excluded.representative_embedding,
                representative_dim = excluded.representative_dim,
                updated_by_run_id = excluded.updated_by_run_id,
                updated_at = excluded.updated_at,
                payload_json = excluded.payload_json
            RETURNING id
            """,
            (
                cluster_key, model_name, model_version, status,
                era_start, era_end, rep_blob, rep_dim,
                run_id, run_id, now, now,
                _json_payload(payload),
            ),
        )
        return int(cur.fetchone()[0])

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
        """Create a durable proposed cluster assignment: the cluster row (if
        absent), the face_cluster_members row, and the reviewable run action."""
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

        self.db.conn.execute(
            """
            INSERT INTO face_cluster_members (
                cluster_id, detection_id, status, confidence,
                created_by_run_id, updated_by_run_id, created_at, updated_at
            )
            VALUES (?, ?, 'proposed', ?, ?, ?, ?, ?)
            ON CONFLICT(cluster_id, detection_id) DO UPDATE SET
                status = excluded.status,
                confidence = excluded.confidence,
                updated_by_run_id = excluded.updated_by_run_id,
                updated_at = excluded.updated_at
            WHERE face_cluster_members.status != 'accepted'
            """,
            (cluster_id, detection_id, confidence, run_id, run_id, now, now),
        )

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

    def supersede_proposed_clusters(
        self,
        *,
        run_id: int,
        model_name: str,
        model_version: str,
    ) -> int:
        """
        Mark still-proposed clusters (and their proposed memberships) for a
        model as superseded ahead of a re-clustering run. Accepted clusters
        and accepted memberships are never touched.

        Returns the number of clusters superseded.
        """
        now = _iso_now()
        self.db.conn.execute(
            """
            UPDATE face_cluster_members
               SET status = 'superseded', updated_by_run_id = ?, updated_at = ?
             WHERE status = 'proposed'
               AND cluster_id IN (
                   SELECT id FROM face_clusters
                   WHERE model_name = ? AND model_version = ? AND status = 'proposed'
               )
            """,
            (run_id, now, model_name, model_version),
        )
        cur = self.db.conn.execute(
            """
            UPDATE face_clusters
               SET status = 'superseded', updated_by_run_id = ?, updated_at = ?
             WHERE model_name = ? AND model_version = ? AND status = 'proposed'
            """,
            (run_id, now, model_name, model_version),
        )
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    def get_embeddings_with_capture_dates(
        self,
        *,
        model_name: str,
        model_version: str,
    ) -> list[tuple[int, tuple[float, ...], Optional[str]]]:
        """
        Return (detection_id, embedding, capture_datetime) for every observed
        detection of the given model. No-faces sentinels are excluded via
        status='observed'. capture_datetime comes from the scanned file's
        media_metadata and may be None.
        """
        cur = self.db.conn.execute(
            """
            SELECT d.id, e.embedding, e.vector_dim, m.capture_datetime
            FROM face_detections d
            JOIN face_embeddings e
              ON e.detection_id = d.id
             AND e.model_name = d.model_name
             AND e.model_version = d.model_version
            LEFT JOIN media_metadata m ON m.file_id = d.file_id
            WHERE d.status = 'observed'
              AND d.model_name = ?
              AND d.model_version = ?
            ORDER BY d.id
            """,
            (model_name, model_version),
        )
        return [
            (int(det_id), _unpack_embedding(blob, int(dim)), capture_str)
            for det_id, blob, dim, capture_str in cur.fetchall()
        ]

    def get_persons_with_birth_dates(self) -> list[tuple[int, Optional[str], str]]:
        """Return active persons with a birth date: (id, display_name, birth_date)."""
        cur = self.db.conn.execute(
            """
            SELECT id, display_name, birth_date
            FROM face_persons
            WHERE status = 'active' AND birth_date IS NOT NULL
            ORDER BY id
            """
        )
        return cur.fetchall()

    def create_person(
        self,
        *,
        run_id: int,
        display_name: Optional[str],
        birth_date: Optional[str] = None,
        status: str = "active",
        payload: Optional[Mapping] = None,
    ) -> int:
        now = _iso_now()
        cur = self.db.conn.execute(
            """
            INSERT INTO face_persons (
                display_name, birth_date, status, created_by_run_id,
                updated_by_run_id, created_at, updated_at, payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (display_name, birth_date, status, run_id, run_id, now, now,
             _json_payload(payload)),
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
