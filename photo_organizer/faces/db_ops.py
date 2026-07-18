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

    def mark_detection_not_a_face(self, *, run_id: int, detection_id: int) -> None:
        """User verdict that a single detection is not a face."""
        self.db.conn.execute(
            """
            UPDATE face_detections SET status = 'not_a_face'
             WHERE id = ? AND status = 'observed'
            """,
            (detection_id,),
        )
        RunActionRecorder(self.db, run_id).record(ActionSpec(
            action_type="face_detection_reject",
            entity_type="face_detection",
            entity_id=detection_id,
            source_path=None,
            target_path=None,
            status="applied",
            phase=PHASE_FACE_CLUSTER_APPLY,
            sequence=0,
            idempotency_key=f"face_detection_reject:{detection_id}",
            method="user_not_a_face",
            payload={"detection_id": detection_id},
        ))

    def mark_detection_depiction(self, *, run_id: int, detection_id: int) -> None:
        """User verdict that a detection is a real face but not a live one:
        a framed photo, a screen, a poster, a mirror. Unlike not_a_face the
        embedding is genuine — but the photo's capture date says nothing
        about when the face looked like this, so depictions are excluded
        from era clustering, tracklets, co-occurrence, and anchors (all of
        which assume faces are live at capture time)."""
        self.db.conn.execute(
            """
            UPDATE face_detections SET status = 'depiction'
             WHERE id = ? AND status = 'observed'
            """,
            (detection_id,),
        )
        RunActionRecorder(self.db, run_id).record(ActionSpec(
            action_type="face_detection_depiction",
            entity_type="face_detection",
            entity_id=detection_id,
            source_path=None,
            target_path=None,
            status="applied",
            phase=PHASE_FACE_CLUSTER_APPLY,
            sequence=0,
            idempotency_key=f"face_detection_depiction:{detection_id}",
            method="user_depiction",
            payload={"detection_id": detection_id},
        ))

    def mark_detection_not_a_person(self, *, run_id: int,
                                    detection_id: int) -> None:
        """User verdict that a detection is a face-like object but not a
        person: a doll, a statue, a mannequin. Distinct from not_a_face
        (detector pareidolia — bricks, pipes) and from depiction (a real
        person's face at the wrong date): the embedding is face-shaped but
        carries no person identity, so like the other verdicts it leaves
        every working set (queries filter status='observed')."""
        self.db.conn.execute(
            """
            UPDATE face_detections SET status = 'not_a_person'
             WHERE id = ? AND status = 'observed'
            """,
            (detection_id,),
        )
        RunActionRecorder(self.db, run_id).record(ActionSpec(
            action_type="face_detection_not_a_person",
            entity_type="face_detection",
            entity_id=detection_id,
            source_path=None,
            target_path=None,
            status="applied",
            phase=PHASE_FACE_CLUSTER_APPLY,
            sequence=0,
            idempotency_key=f"face_detection_not_a_person:{detection_id}",
            method="user_not_a_person",
            payload={"detection_id": detection_id},
        ))

    def get_photo_info(self, file_id: int) -> Optional[dict]:
        """Current path, type, and capture date for one photo."""
        row = self.db.conn.execute(
            """
            SELECT COALESCE(f.dest_path, f.orig_path), f.type,
                   mm.capture_datetime
            FROM files f
            LEFT JOIN media_metadata mm ON mm.file_id = f.id
            WHERE f.id = ?
            """,
            (file_id,),
        ).fetchone()
        if row is None:
            return None
        return {"path": row[0], "file_type": row[1],
                "capture_datetime": row[2]}

    def _live_member_ids(self, cluster_id: int) -> set[int]:
        """Detection ids currently (proposed or accepted) in a cluster."""
        rows = self.db.conn.execute(
            """
            SELECT detection_id FROM face_cluster_members
            WHERE cluster_id = ? AND status IN ('proposed', 'accepted')
            """,
            (cluster_id,),
        ).fetchall()
        return {int(row[0]) for row in rows}

    def reject_face_proposals(self, action_ids: Sequence[int], *,
                              run_id: int,
                              note: Optional[str] = None) -> dict:
        """
        Reject pending face proposals AND make the decision durable.

        Cluster ids and keys are regenerated by every re-clustering run, so
        a bare run_actions rejection cannot stop the same pair from being
        re-proposed under fresh ids. This wrapper snapshots the detection
        ids on each side at rejection time into face_cannot_links —
        detections are the stable anchor — and the linker, refinement, and
        accept phases consume those rows as cannot-link constraints.

        Merge rejections record (detections_a, detections_b); assignment
        rejections record (detections_a, person_id). Detections sitting in
        BOTH clusters of a rejected merge (overlapping-window duplicates)
        can't take a side and are dropped from the snapshot.

        Returns {"rejected": [...], "skipped": [...],
                 "cannot_links_created": N}.
        """
        from ..pipeline.lifecycle import reject_proposals

        ids = [int(a) for a in action_ids]
        payloads: dict[int, tuple[str, dict]] = {}
        if ids:
            placeholders = ",".join("?" for _ in ids)
            rows = self.db.conn.execute(
                f"""
                SELECT id, action_type, payload_json FROM run_actions
                WHERE id IN ({placeholders}) AND status = 'proposed'
                  AND action_type IN ('face_cluster_merge',
                                      'face_person_assign')
                  AND payload_json IS NOT NULL
                """,
                ids,
            ).fetchall()
            payloads = {
                int(row[0]): (row[1], json.loads(row[2])) for row in rows
            }

        rejected, skipped = reject_proposals(self.db, ids, run_id=run_id,
                                             note=note)

        now = _iso_now()
        created = 0
        for action_id in rejected:
            entry = payloads.get(action_id)
            if entry is None:
                continue  # not a face proposal; nothing to snapshot
            action_type, payload = entry
            if action_type == "face_cluster_merge":
                side_a = self._live_member_ids(int(payload["cluster_a_id"]))
                side_b = self._live_member_ids(int(payload["cluster_b_id"]))
                shared = side_a & side_b
                side_a -= shared
                side_b -= shared
                if not side_a or not side_b:
                    continue
                columns = ("detections_a, detections_b", "?, ?")
                values = (json.dumps(sorted(side_a)),
                          json.dumps(sorted(side_b)))
            else:  # face_person_assign
                side_a = self._live_member_ids(int(payload["cluster_id"]))
                if not side_a:
                    continue
                columns = ("detections_a, person_id", "?, ?")
                values = (json.dumps(sorted(side_a)),
                          int(payload["person_id"]))
            self.db.conn.execute(
                f"""
                INSERT INTO face_cannot_links (
                    {columns[0]}, source_action_id, status,
                    created_by_run_id, created_at, note
                )
                VALUES ({columns[1]}, ?, 'active', ?, ?, ?)
                """,
                (*values, action_id, run_id, now, note),
            )
            created += 1

        return {"rejected": rejected, "skipped": skipped,
                "cannot_links_created": created}

    def get_active_cannot_links(self) -> list[dict]:
        """Active cannot-link constraints, detection sets parsed.

        detections_b is None for person-assignment constraints (person_id
        set instead)."""
        rows = self.db.conn.execute(
            """
            SELECT id, detections_a, detections_b, person_id
            FROM face_cannot_links WHERE status = 'active'
            ORDER BY id
            """
        ).fetchall()
        return [
            {
                "id": int(row[0]),
                "detections_a": {int(d) for d in json.loads(row[1])},
                "detections_b": ({int(d) for d in json.loads(row[2])}
                                 if row[2] else None),
                "person_id": int(row[3]) if row[3] is not None else None,
            }
            for row in rows
        ]

    def evict_cluster_members(self, *, run_id: int, cluster_id: int,
                              detection_ids: Sequence[int],
                              note: Optional[str] = None) -> dict:
        """
        Batch verdict that specific member detections do NOT belong in this
        cluster ("right face, wrong group"). In one save:

        - the memberships flip to 'rejected'
        - ONE cannot-link constraint is recorded: {evicted} vs {remaining
          live members}. Batching matters: faces evicted together (e.g. two
          person-B faces in a person-A cluster) must never be constrained
          against EACH OTHER, only against the group they left.
        - the evicted ids are pruned from any existing constraint side that
          overlaps this cluster's remaining members — earlier snapshots
          that mis-included them as part of this group get corrected
          (explicit human verdicts always win over derived snapshots).

        The clustering phase enforces these constraints on future runs, so
        an evicted face cannot silently rejoin the same group.

        Returns {"evicted": N, "constraints_created": 0|1,
                 "constraint_sides_pruned": N}.
        """
        now = _iso_now()
        evicted = sorted({int(d) for d in detection_ids})
        result = {"evicted": 0, "constraints_created": 0,
                  "constraint_sides_pruned": 0}
        if not evicted:
            return result

        placeholders = ",".join("?" for _ in evicted)
        cur = self.db.conn.execute(
            f"""
            UPDATE face_cluster_members
               SET status = 'rejected', updated_by_run_id = ?, updated_at = ?
             WHERE cluster_id = ? AND detection_id IN ({placeholders})
               AND status IN ('proposed', 'accepted')
            """,
            (run_id, now, cluster_id, *evicted),
        )
        result["evicted"] = cur.rowcount or 0
        if not result["evicted"]:
            return result

        remaining = self._live_member_ids(cluster_id)
        evicted_set = set(evicted)

        # Correct earlier snapshots: any active constraint side that
        # overlaps this group's remaining members and still contains a
        # now-evicted detection mis-attributed it.
        for constraint in self.get_active_cannot_links():
            for column in ("detections_a", "detections_b"):
                side = constraint[column]
                if side is None:
                    continue
                if (side & remaining) and (side & evicted_set):
                    pruned = sorted(side - evicted_set)
                    if pruned:
                        self.db.conn.execute(
                            f"UPDATE face_cannot_links SET {column} = ? "
                            f"WHERE id = ?",
                            (json.dumps(pruned), constraint["id"]),
                        )
                    else:
                        self.db.conn.execute(
                            "UPDATE face_cannot_links SET status = 'retired' "
                            "WHERE id = ?",
                            (constraint["id"],),
                        )
                    result["constraint_sides_pruned"] += 1

        if remaining:
            self.db.conn.execute(
                """
                INSERT INTO face_cannot_links (
                    detections_a, detections_b, status,
                    created_by_run_id, created_at, note
                )
                VALUES (?, ?, 'active', ?, ?, ?)
                """,
                (json.dumps(evicted), json.dumps(sorted(remaining)),
                 run_id, now, note),
            )
            result["constraints_created"] = 1

        RunActionRecorder(self.db, run_id).record(ActionSpec(
            action_type="face_member_evict",
            entity_type="face_cluster",
            entity_id=cluster_id,
            source_path=None,
            target_path=None,
            status="applied",
            phase=PHASE_FACE_CLUSTER_APPLY,
            sequence=0,
            idempotency_key=(
                f"face_member_evict:{cluster_id}:"
                f"{','.join(str(d) for d in evicted)}"
            ),
            method="user_evict",
            payload={"cluster_id": cluster_id, "detection_ids": evicted},
        ))
        return result

    def get_cluster_members_detail(self, cluster_id: int) -> list[dict]:
        """
        Every live member of a cluster with its centroid similarity,
        MOST SUSPECT FIRST (ascending similarity) — the review order for
        thorough cleanup: intruders and depictions sit at the top pages.
        """
        import numpy as np

        cur = self.db.conn.execute(
            """
            SELECT d.id, e.embedding, e.vector_dim,
                   json_extract(d.payload_json, '$.thumbnail_path'),
                   mm.capture_datetime
            FROM face_cluster_members m
            JOIN face_detections d ON d.id = m.detection_id
            JOIN face_embeddings e
              ON e.detection_id = d.id
             AND e.model_name = d.model_name
             AND e.model_version = d.model_version
            LEFT JOIN media_metadata mm ON mm.file_id = d.file_id
            WHERE m.cluster_id = ?
              AND m.status IN ('proposed', 'accepted')
              AND d.status = 'observed'
            """,
            (cluster_id,),
        )
        members = []
        for det_id, blob, dim, thumb, capture in cur.fetchall():
            members.append((int(det_id),
                            np.frombuffer(blob, dtype=np.float32,
                                          count=int(dim)),
                            thumb, capture))
        if not members:
            return []

        matrix = np.stack([m[1] for m in members])
        rep_row = self.db.conn.execute(
            "SELECT representative_embedding, representative_dim "
            "FROM face_clusters WHERE id = ?",
            (cluster_id,),
        ).fetchone()
        if rep_row and rep_row[0] is not None:
            centroid = np.frombuffer(rep_row[0], dtype=np.float32,
                                     count=int(rep_row[1]))
        else:
            centroid = matrix.mean(axis=0)
            norm = np.linalg.norm(centroid)
            if norm > 0:
                centroid = centroid / norm
        similarities = matrix @ centroid

        detail = [
            {"detection_id": det_id, "thumbnail_path": thumb,
             "capture_datetime": capture,
             "similarity": float(similarities[i])}
            for i, (det_id, _emb, thumb, capture) in enumerate(members)
        ]
        detail.sort(key=lambda m: m["similarity"])
        return detail

    def get_pending_same_photo_flags(self) -> list[dict]:
        """Pending same-photo review flags (possible depictions or twins),
        with each detection's thumbnail for side-by-side review."""
        rows = self.db.conn.execute(
            """
            SELECT a.id, a.confidence, a.payload_json,
                   json_extract(da.payload_json, '$.thumbnail_path'),
                   json_extract(db.payload_json, '$.thumbnail_path')
            FROM run_actions a
            JOIN face_detections da
              ON da.id = json_extract(a.payload_json, '$.detection_a')
            JOIN face_detections db
              ON db.id = json_extract(a.payload_json, '$.detection_b')
            WHERE a.action_type = 'face_same_photo_review'
              AND a.status = 'proposed'
            ORDER BY a.confidence DESC, a.id
            """
        ).fetchall()
        flags = []
        for action_id, confidence, payload_json, thumb_a, thumb_b in rows:
            payload = json.loads(payload_json or "{}")
            flags.append({
                "action_id": int(action_id),
                "confidence": confidence,
                "file_id": payload.get("file_id"),
                "detection_a": payload.get("detection_a"),
                "detection_b": payload.get("detection_b"),
                "similarity": payload.get("similarity"),
                "thumbnail_a": thumb_a,
                "thumbnail_b": thumb_b,
            })
        return flags

    def get_resolved_same_photo_flag_keys(self) -> set:
        """Idempotency keys of same-photo flags a human already resolved
        (dismissed as twins, or handled via depiction). Idempotency is
        per-run, so without this skip-set every link run would resurrect
        dismissed flags."""
        rows = self.db.conn.execute(
            """
            SELECT DISTINCT idempotency_key FROM run_actions
            WHERE action_type = 'face_same_photo_review'
              AND status IN ('rejected', 'applied')
            """
        ).fetchall()
        return {row[0] for row in rows}

    def resolve_same_photo_flag(self, *, run_id: int, action_id: int,
                                note: str) -> None:
        """Mark a same-photo flag as handled (depiction marked, or twins)."""
        self.db.conn.execute(
            """
            UPDATE run_actions
               SET status = 'rejected', resolved_by_run_id = ?,
                   resolved_at = ?, resolution_note = ?
             WHERE id = ? AND status = 'proposed'
            """,
            (run_id, _iso_now(), note, action_id),
        )

    # A merge proposal is "mechanical" when it needs no human judgment:
    # window duplicates are the same detections clustered twice by
    # overlapping era windows (true by construction), and tracklet merges
    # are physical sequence evidence — unless they carry a same-photo
    # contradiction (possible depiction/twins), which stays with a human.
    _MECHANICAL_MERGE_WHERE = """
        action_type = 'face_cluster_merge'
        AND (method = 'window_duplicate'
             OR (method = 'same_event_tracklet'
                 AND COALESCE(json_extract(payload_json,
                     '$.signals.same_photo_overlap'), 0) = 0))
    """

    def get_pending_mechanical_merge_ids(self) -> list[int]:
        """Ids of pending construction-true merge proposals (bulk accept)."""
        rows = self.db.conn.execute(
            f"""
            SELECT id FROM run_actions
            WHERE status = 'proposed' AND {self._MECHANICAL_MERGE_WHERE}
            ORDER BY id
            """
        ).fetchall()
        return [int(row[0]) for row in rows]

    def get_pending_judgment_proposals(self, limit: int = 40) -> list[dict]:
        """Pending face suggestions that need a human decision — merges and
        assignments minus the mechanical tier — highest confidence first.
        This is the review queue; without the mechanical exclusion tens of
        thousands of confidence-100 window duplicates bury every real
        decision."""
        rows = self.db.conn.execute(
            f"""
            SELECT id, action_type, confidence, payload_json
            FROM run_actions
            WHERE status = 'proposed'
              AND action_type IN ('face_cluster_merge', 'face_person_assign')
              AND NOT ({self._MECHANICAL_MERGE_WHERE})
            ORDER BY confidence DESC, id
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        return [
            {"id": int(row[0]), "action_type": row[1],
             "confidence": row[2], "payload_json": row[3]}
            for row in rows
        ]

    def get_live_members_for_clusters(
        self, cluster_ids: Sequence[int],
    ) -> dict[int, set[int]]:
        """{cluster_id: live member detection ids} for the given clusters."""
        members: dict[int, set[int]] = {}
        ids = [int(c) for c in cluster_ids]
        chunk_size = 500  # keep well under SQLite's bound-parameter limit
        for start in range(0, len(ids), chunk_size):
            chunk = ids[start:start + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.db.conn.execute(
                f"""
                SELECT cluster_id, detection_id FROM face_cluster_members
                WHERE cluster_id IN ({placeholders})
                  AND status IN ('proposed', 'accepted')
                """,
                chunk,
            ).fetchall()
            for cluster_id, detection_id in rows:
                members.setdefault(int(cluster_id), set()).add(int(detection_id))
        return members

    def mark_cluster_not_faces(self, *, run_id: int, cluster_id: int) -> int:
        """
        User verdict that a cluster's contents are not faces (detector
        pareidolia). The cluster is rejected and every member detection is
        marked not_a_face — which removes those detections from ALL clusters
        and future clustering (a non-face is a non-face everywhere), while
        keeping the file itself marked as scanned.

        Returns the number of detections marked.
        """
        now = _iso_now()
        cur = self.db.conn.execute(
            """
            UPDATE face_detections
               SET status = 'not_a_face'
             WHERE status = 'observed'
               AND id IN (
                   SELECT detection_id FROM face_cluster_members
                   WHERE cluster_id = ?
               )
            """,
            (cluster_id,),
        )
        marked = cur.rowcount or 0
        self.db.conn.execute(
            """
            UPDATE face_clusters
               SET status = 'rejected', updated_by_run_id = ?, updated_at = ?
             WHERE id = ?
            """,
            (run_id, now, cluster_id),
        )
        RunActionRecorder(self.db, run_id).record(ActionSpec(
            action_type="face_cluster_reject",
            entity_type="face_cluster",
            entity_id=cluster_id,
            source_path=None,
            target_path=None,
            status="applied",
            phase=PHASE_FACE_CLUSTER_APPLY,
            sequence=0,
            idempotency_key=f"face_cluster_reject:{cluster_id}",
            method="user_not_a_face",
            payload={"cluster_id": cluster_id, "detections_marked": marked},
        ))
        return marked

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
        min_det_score: Optional[float] = None,
    ):
        """
        Return (detection_id, embedding, capture_datetime) for every observed
        detection of the given model. No-faces sentinels are excluded via
        status='observed'; min_det_score additionally excludes low-confidence
        detections (pareidolia junk) without touching the stored rows.
        capture_datetime comes from the scanned file's media_metadata and may
        be None.

        Embeddings are float32 numpy views over the stored blobs (no Python
        float boxing — this path loads the whole library at once).
        """
        import numpy as np

        score_clause = ""
        params: list = [model_name, model_version]
        if min_det_score is not None:
            score_clause = " AND d.confidence >= ?"
            params.append(min_det_score)
        cur = self.db.conn.execute(
            f"""
            SELECT d.id, e.embedding, e.vector_dim, m.capture_datetime
            FROM face_detections d
            JOIN face_embeddings e
              ON e.detection_id = d.id
             AND e.model_name = d.model_name
             AND e.model_version = d.model_version
            LEFT JOIN media_metadata m ON m.file_id = d.file_id
            WHERE d.status = 'observed'
              AND d.model_name = ?
              AND d.model_version = ?{score_clause}
            ORDER BY d.id
            """,
            params,
        )
        return [
            (int(det_id),
             np.frombuffer(blob, dtype=np.float32, count=int(dim)),
             capture_str)
            for det_id, blob, dim, capture_str in cur.fetchall()
        ]

    def get_detections_for_tracklets(
        self,
        *,
        model_name: str,
        model_version: str,
        min_det_score: Optional[float] = None,
    ):
        """
        Return (detection_id, file_id, capture_datetime, embedding) for
        observed detections with a capture date — the input to same-event
        tracklet building. Depictions and not-a-face verdicts are excluded
        via status='observed'; undated files are excluded because sequence
        evidence needs timestamps.
        """
        import numpy as np

        score_clause = ""
        params: list = [model_name, model_version]
        if min_det_score is not None:
            score_clause = " AND d.confidence >= ?"
            params.append(min_det_score)
        cur = self.db.conn.execute(
            f"""
            SELECT d.id, d.file_id, m.capture_datetime, e.embedding, e.vector_dim
            FROM face_detections d
            JOIN face_embeddings e
              ON e.detection_id = d.id
             AND e.model_name = d.model_name
             AND e.model_version = d.model_version
            JOIN media_metadata m
              ON m.file_id = d.file_id AND m.capture_datetime IS NOT NULL
            WHERE d.status = 'observed'
              AND d.model_name = ?
              AND d.model_version = ?{score_clause}
            ORDER BY d.id
            """,
            params,
        )
        return [
            (int(det_id), int(file_id), capture_str,
             np.frombuffer(blob, dtype=np.float32, count=int(dim)))
            for det_id, file_id, capture_str, blob, dim in cur.fetchall()
        ]

    def get_clusters_for_linking(
        self,
        *,
        model_name: str,
        model_version: str,
    ) -> list[dict]:
        """
        Return live (proposed or accepted) clusters with era metadata and a
        representative embedding, for cross-age link scoring.
        """
        import numpy as np

        cur = self.db.conn.execute(
            """
            SELECT id, cluster_key, status, era_start, era_end,
                   representative_embedding, representative_dim
            FROM face_clusters
            WHERE model_name = ? AND model_version = ?
              AND status IN ('proposed', 'accepted')
              AND representative_embedding IS NOT NULL
            ORDER BY id
            """,
            (model_name, model_version),
        )
        return [
            {
                "id": int(row[0]),
                "cluster_key": row[1],
                "status": row[2],
                "era_start": row[3],
                "era_end": row[4],
                # float32 view, ready for dot products — pair scoring must
                # not pay a conversion per comparison.
                "representative": np.frombuffer(row[5], dtype=np.float32,
                                                count=int(row[6])),
            }
            for row in cur.fetchall()
        ]

    def get_cluster_link_context(
        self,
        *,
        model_name: str,
        model_version: str,
    ) -> dict:
        """
        Bulk context for cross-age linking: one query over live memberships,
        reduced in Python to a dict with

          co_occurrence:  {cluster_id: {other_cluster_id: shared_photo_count}}
          median_ages:    {cluster_id: median estimated_age or None}
          member_counts:  {cluster_id: live member count}
          shared_members: {(lo_id, hi_id): count of shared detections}
          det_clusters:   {detection_id: [cluster_ids]} (tracklet->cluster map)

        Detections carrying a user verdict (not_a_face, depiction) are
        excluded — neither is identity evidence at the photo's capture time.

        shared_members is the window-duplicate signal: clusters sharing the
        same detection rows are the same identity by construction (a
        detection cannot be two people), independent of any model score.

        Equivalent to per-cluster readers but O(1) queries instead of
        O(clusters) — at tens of thousands of clusters the per-query overhead
        dominates the whole link run.
        """
        from itertools import combinations

        cur = self.db.conn.execute(
            """
            SELECT m.cluster_id, m.detection_id, d.file_id,
                   json_extract(d.payload_json, '$.estimated_age')
            FROM face_cluster_members m
            JOIN face_clusters c ON c.id = m.cluster_id
            JOIN face_detections d ON d.id = m.detection_id
            WHERE m.status IN ('proposed', 'accepted')
              AND c.status IN ('proposed', 'accepted')
              AND d.status = 'observed'
              AND c.model_name = ? AND c.model_version = ?
            """,
            (model_name, model_version),
        )
        file_clusters: dict[int, set[int]] = {}
        det_clusters: dict[int, list[int]] = {}
        member_counts: dict[int, int] = {}
        ages: dict[int, list[float]] = {}
        for cluster_id, detection_id, file_id, age in cur.fetchall():
            cluster_id = int(cluster_id)
            file_clusters.setdefault(int(file_id), set()).add(cluster_id)
            det_clusters.setdefault(int(detection_id), []).append(cluster_id)
            member_counts[cluster_id] = member_counts.get(cluster_id, 0) + 1
            if age is not None:
                ages.setdefault(cluster_id, []).append(float(age))

        co_files: dict[tuple[int, int], int] = {}
        for clusters_in_file in file_clusters.values():
            for a, b in combinations(sorted(clusters_in_file), 2):
                co_files[(a, b)] = co_files.get((a, b), 0) + 1

        co_occurrence: dict[int, dict[int, int]] = {}
        for (a, b), count in co_files.items():
            co_occurrence.setdefault(a, {})[b] = count
            co_occurrence.setdefault(b, {})[a] = count

        shared_members: dict[tuple[int, int], int] = {}
        for clusters_of_det in det_clusters.values():
            if len(clusters_of_det) < 2:
                continue
            for a, b in combinations(sorted(set(clusters_of_det)), 2):
                shared_members[(a, b)] = shared_members.get((a, b), 0) + 1

        median_ages: dict[int, Optional[float]] = {}
        for cluster_id, cluster_ages in ages.items():
            cluster_ages.sort()
            mid = len(cluster_ages) // 2
            if len(cluster_ages) % 2:
                median_ages[cluster_id] = cluster_ages[mid]
            else:
                median_ages[cluster_id] = (cluster_ages[mid - 1] + cluster_ages[mid]) / 2

        return {
            "co_occurrence": co_occurrence,
            "median_ages": median_ages,
            "member_counts": member_counts,
            "shared_members": shared_members,
            "det_clusters": det_clusters,
        }

    def get_co_occurring_clusters(
        self,
        cluster_id: int,
    ) -> dict[int, int]:
        """
        Return {other_cluster_id: shared_photo_count} for clusters whose
        members appear in the same files as this cluster's members.
        Superseded memberships/clusters are ignored.
        """
        cur = self.db.conn.execute(
            """
            SELECT other_m.cluster_id, COUNT(DISTINCT d.file_id)
            FROM face_cluster_members m
            JOIN face_detections d ON d.id = m.detection_id
            JOIN face_detections other_d ON other_d.file_id = d.file_id
            JOIN face_cluster_members other_m ON other_m.detection_id = other_d.id
            JOIN face_clusters other_c ON other_c.id = other_m.cluster_id
            WHERE m.cluster_id = ?
              AND other_m.cluster_id != ?
              AND m.status IN ('proposed', 'accepted')
              AND other_m.status IN ('proposed', 'accepted')
              AND other_c.status IN ('proposed', 'accepted')
            GROUP BY other_m.cluster_id
            """,
            (cluster_id, cluster_id),
        )
        return {int(row[0]): int(row[1]) for row in cur.fetchall()}

    def get_cluster_median_age(self, cluster_id: int) -> Optional[float]:
        """Median estimated age of the cluster's member detections (from the
        detection payload written by the scan pipeline)."""
        cur = self.db.conn.execute(
            """
            SELECT json_extract(d.payload_json, '$.estimated_age')
            FROM face_cluster_members m
            JOIN face_detections d ON d.id = m.detection_id
            WHERE m.cluster_id = ?
              AND m.status IN ('proposed', 'accepted')
              AND json_extract(d.payload_json, '$.estimated_age') IS NOT NULL
            ORDER BY 1
            """,
            (cluster_id,),
        )
        ages = [float(row[0]) for row in cur.fetchall()]
        if not ages:
            return None
        mid = len(ages) // 2
        if len(ages) % 2:
            return ages[mid]
        return (ages[mid - 1] + ages[mid]) / 2

    def get_labeled_person_embeddings(self) -> dict[int, list[tuple[float, ...]]]:
        """
        Return {person_id: [embedding, ...]} for a person's accepted
        detections — the supervised anchors for cross-age linking and
        refinement. Covers both link shapes: direct detection-level links and
        cluster-level links through accepted memberships. Empty until
        identities have been seeded/reviewed. Detections later marked with a
        verdict (not_a_face, depiction) are excluded: a depiction's embedding
        is real but its capture date is not the person's, so it must not
        shape anchors.
        """
        cur = self.db.conn.execute(
            """
            SELECT l.person_id, e.embedding, e.vector_dim
            FROM face_person_links l
            JOIN face_embeddings e ON e.detection_id = l.detection_id
            JOIN face_detections d ON d.id = l.detection_id
            WHERE l.status = 'accepted'
              AND l.detection_id IS NOT NULL
              AND d.status = 'observed'
            UNION
            SELECT l.person_id, e.embedding, e.vector_dim
            FROM face_person_links l
            JOIN face_cluster_members m
              ON m.cluster_id = l.cluster_id AND m.status = 'accepted'
            JOIN face_embeddings e ON e.detection_id = m.detection_id
            JOIN face_detections d ON d.id = m.detection_id
            WHERE l.status = 'accepted'
              AND l.cluster_id IS NOT NULL
              AND d.status = 'observed'
            """
        )
        anchors: dict[int, list[tuple[float, ...]]] = {}
        for person_id, blob, dim in cur.fetchall():
            anchors.setdefault(int(person_id), []).append(
                _unpack_embedding(blob, int(dim))
            )
        return anchors

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

    def find_person_by_name(self, name: str) -> Optional[tuple[int, Optional[str], Optional[str]]]:
        """Case-insensitive lookup of an active person by display name.
        Returns (id, display_name, birth_date) or None."""
        row = self.db.conn.execute(
            """
            SELECT id, display_name, birth_date FROM face_persons
            WHERE LOWER(display_name) = LOWER(?) AND status = 'active'
            """,
            (name,),
        ).fetchone()
        return (int(row[0]), row[1], row[2]) if row else None

    def update_person(
        self,
        *,
        run_id: int,
        person_id: int,
        display_name: Optional[str] = None,
        birth_date: Optional[str] = None,
        payload: Optional[Mapping] = None,
    ) -> bool:
        """Update person fields (None leaves a field unchanged). Returns True
        when the row exists and was updated."""
        cur = self.db.conn.execute(
            """
            UPDATE face_persons
               SET display_name = COALESCE(?, display_name),
                   birth_date = COALESCE(?, birth_date),
                   payload_json = COALESCE(?, payload_json),
                   updated_by_run_id = ?,
                   updated_at = ?
             WHERE id = ?
            """,
            (display_name, birth_date, _json_payload(payload),
             run_id, _iso_now(), person_id),
        )
        return bool(cur.rowcount)

    def accept_cluster(self, *, run_id: int, cluster_id: int) -> None:
        """Mark a cluster and its proposed memberships accepted."""
        now = _iso_now()
        self.db.conn.execute(
            """
            UPDATE face_clusters
               SET status = 'accepted', updated_by_run_id = ?, updated_at = ?
             WHERE id = ? AND status != 'accepted'
            """,
            (run_id, now, cluster_id),
        )
        self.db.conn.execute(
            """
            UPDATE face_cluster_members
               SET status = 'accepted', updated_by_run_id = ?, updated_at = ?
             WHERE cluster_id = ? AND status = 'proposed'
            """,
            (run_id, now, cluster_id),
        )

    def link_cluster_to_person(
        self,
        *,
        run_id: int,
        cluster_id: int,
        person_id: int,
        link_method: str,
        confidence: Optional[float] = None,
    ) -> int:
        """Create an accepted person link for a whole cluster."""
        now = _iso_now()
        self.db.conn.execute(
            """
            INSERT OR IGNORE INTO face_person_links (
                person_id, detection_id, cluster_id, confidence, link_method,
                status, created_by_run_id, updated_by_run_id, created_at, updated_at
            )
            VALUES (?, NULL, ?, ?, ?, 'accepted', ?, ?, ?, ?)
            """,
            (person_id, cluster_id, confidence, link_method,
             run_id, run_id, now, now),
        )
        row = self.db.conn.execute(
            """
            SELECT id FROM face_person_links
            WHERE person_id = ? AND cluster_id = ?
            """,
            (person_id, cluster_id),
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
            status="applied",
            phase=PHASE_FACE_PERSON_LINK_APPLY,
            sequence=0,
            idempotency_key=f"face_person_link:{person_id}:cluster:{cluster_id}",
            confidence=int(confidence * 100) if confidence is not None else None,
            method=link_method,
            payload={"person_id": person_id, "cluster_id": cluster_id},
        ))
        return link_id

    def absorb_person(self, *, run_id: int, absorbed_id: int,
                      winner_id: int) -> dict:
        """
        Fold one person into another: every accepted link of the absorbed
        person is retracted and re-created against the winner (skipping ones
        the winner already has), and the absorbed person is marked 'merged'
        with a pointer to the winner. Used when accepting merges/assignments
        that connect anonymous person groups, and by the Name People page.
        """
        now = _iso_now()
        stats = {"cluster_links_moved": 0, "detection_links_moved": 0}

        cluster_links = self.db.conn.execute(
            """
            SELECT cluster_id FROM face_person_links
            WHERE person_id = ? AND status = 'accepted' AND cluster_id IS NOT NULL
            """,
            (absorbed_id,),
        ).fetchall()
        detection_links = self.db.conn.execute(
            """
            SELECT detection_id, confidence, link_method FROM face_person_links
            WHERE person_id = ? AND status = 'accepted' AND detection_id IS NOT NULL
            """,
            (absorbed_id,),
        ).fetchall()

        self.db.conn.execute(
            """
            UPDATE face_person_links
               SET status = 'retracted', updated_by_run_id = ?, updated_at = ?
             WHERE person_id = ? AND status = 'accepted'
            """,
            (run_id, now, absorbed_id),
        )
        for (cluster_id,) in cluster_links:
            self.link_cluster_to_person(
                run_id=run_id, cluster_id=int(cluster_id), person_id=winner_id,
                link_method="merge_absorb",
            )
            stats["cluster_links_moved"] += 1
        for detection_id, confidence, _method in detection_links:
            self.link_detection_to_person(
                run_id=run_id, detection_id=int(detection_id),
                person_id=winner_id, confidence=confidence,
                link_method="merge_absorb",
            )
            stats["detection_links_moved"] += 1

        self.db.conn.execute(
            """
            UPDATE face_persons
               SET status = 'merged', updated_by_run_id = ?, updated_at = ?,
                   payload_json = json_set(COALESCE(payload_json, '{}'),
                                           '$.merged_into', ?)
             WHERE id = ?
            """,
            (run_id, now, winner_id, absorbed_id),
        )
        return stats

    def get_accepted_cluster_person_links(self) -> list[tuple[int, int]]:
        """Return (cluster_id, person_id) for accepted cluster-level links."""
        cur = self.db.conn.execute(
            """
            SELECT cluster_id, person_id FROM face_person_links
            WHERE status = 'accepted' AND cluster_id IS NOT NULL
            """
        )
        return [(int(row[0]), int(row[1])) for row in cur.fetchall()]

    def get_face_proposals(
        self,
        action_ids: Sequence[int],
        *,
        action_types: tuple[str, ...] = ("face_cluster_merge", "face_person_assign"),
    ) -> tuple[list[dict], list[int]]:
        """
        Load pending face proposals by run_actions id.

        Returns (proposals, skipped_ids) where each proposal dict carries
        action_id, action_type, confidence, and the parsed payload. Ids that
        are missing, of another type, or already resolved are skipped.
        """
        proposals: list[dict] = []
        skipped: list[int] = []
        for action_id in action_ids:
            row = self.db.conn.execute(
                """
                SELECT id, action_type, status, confidence, payload_json
                FROM run_actions WHERE id = ?
                """,
                (int(action_id),),
            ).fetchone()
            if (row is None or row[1] not in action_types
                    or row[2] != "proposed" or not row[4]):
                skipped.append(int(action_id))
                continue
            proposals.append({
                "action_id": int(row[0]),
                "action_type": row[1],
                "confidence": row[3],
                "payload": json.loads(row[4]),
            })
        return proposals, skipped

    def get_unassigned_cluster_representatives(
        self,
        *,
        model_name: str,
        model_version: str,
    ) -> list[dict]:
        """Live clusters with a representative embedding that are not yet
        linked to any person — refinement's auto-assign candidates."""
        cur = self.db.conn.execute(
            """
            SELECT c.id, c.cluster_key, c.representative_embedding,
                   c.representative_dim
            FROM face_clusters c
            WHERE c.model_name = ? AND c.model_version = ?
              AND c.status IN ('proposed', 'accepted')
              AND c.representative_embedding IS NOT NULL
              AND c.id NOT IN (
                  SELECT cluster_id FROM face_person_links
                  WHERE cluster_id IS NOT NULL AND status = 'accepted'
              )
            ORDER BY c.id
            """,
            (model_name, model_version),
        )
        return [
            {
                "id": int(row[0]),
                "cluster_key": row[1],
                "representative": _unpack_embedding(row[2], int(row[3])),
            }
            for row in cur.fetchall()
        ]

    def get_stats(self) -> dict:
        """Aggregate counts for the review dashboard."""
        conn = self.db.conn
        one = lambda sql, *p: conn.execute(sql, p).fetchone()[0]  # noqa: E731
        total = one(
            "SELECT COUNT(*) FROM face_detections WHERE status = 'observed'"
        )
        # Both link shapes count: direct detection-level labels (photo
        # labeling) and cluster-level links through accepted memberships.
        assigned = one(
            """
            SELECT COUNT(*) FROM (
                SELECT detection_id FROM face_person_links
                WHERE detection_id IS NOT NULL AND status = 'accepted'
                UNION
                SELECT m.detection_id
                FROM face_cluster_members m
                JOIN face_person_links l
                  ON l.cluster_id = m.cluster_id AND l.status = 'accepted'
                WHERE m.status = 'accepted'
            )
            """
        )
        named = one(
            """
            SELECT COUNT(*) FROM (
                SELECT l.detection_id FROM face_person_links l
                JOIN face_persons p
                  ON p.id = l.person_id AND p.status = 'active'
                 AND p.display_name IS NOT NULL
                WHERE l.detection_id IS NOT NULL AND l.status = 'accepted'
                UNION
                SELECT m.detection_id
                FROM face_cluster_members m
                JOIN face_person_links l
                  ON l.cluster_id = m.cluster_id AND l.status = 'accepted'
                JOIN face_persons p
                  ON p.id = l.person_id AND p.status = 'active'
                 AND p.display_name IS NOT NULL
                WHERE m.status = 'accepted'
            )
            """
        )
        return {
            "detections_named": named,
            "total_detections": total,
            "photos_with_faces": one(
                "SELECT COUNT(DISTINCT file_id) FROM face_detections "
                "WHERE status = 'observed'"
            ),
            "clusters_live": one(
                "SELECT COUNT(*) FROM face_clusters "
                "WHERE status IN ('proposed', 'accepted')"
            ),
            "persons_named": one(
                "SELECT COUNT(*) FROM face_persons "
                "WHERE status = 'active' AND display_name IS NOT NULL"
            ),
            "persons_unnamed": one(
                "SELECT COUNT(*) FROM face_persons "
                "WHERE status = 'active' AND display_name IS NULL"
            ),
            "detections_assigned": assigned,
            "pending_merges": one(
                "SELECT COUNT(*) FROM run_actions "
                "WHERE action_type = 'face_cluster_merge' AND status = 'proposed'"
            ),
            "pending_assignments": one(
                "SELECT COUNT(*) FROM run_actions "
                "WHERE action_type = 'face_person_assign' AND status = 'proposed'"
            ),
        }

    def get_clusters_for_review(self, limit: int = 50) -> list[dict]:
        """Live clusters with no accepted person link, largest first."""
        cur = self.db.conn.execute(
            """
            SELECT c.id, c.cluster_key, c.status, c.era_start, c.era_end,
                   COUNT(m.detection_id) AS members
            FROM face_clusters c
            LEFT JOIN face_cluster_members m
              ON m.cluster_id = c.id AND m.status IN ('proposed', 'accepted')
            WHERE c.status IN ('proposed', 'accepted')
              AND c.id NOT IN (
                  SELECT cluster_id FROM face_person_links
                  WHERE cluster_id IS NOT NULL AND status = 'accepted'
              )
            GROUP BY c.id
            ORDER BY members DESC, c.id
            LIMIT ?
            """,
            (limit,),
        )
        return [
            {
                "id": int(row[0]),
                "cluster_key": row[1],
                "status": row[2],
                "era_start": row[3],
                "era_end": row[4],
                "members": int(row[5]),
            }
            for row in cur.fetchall()
        ]

    def get_cluster_overview(self, cluster_id: int) -> Optional[dict]:
        """One cluster's review header — same shape as
        get_clusters_for_review rows plus the linked person's name (jump
        targets may already be linked). None when the id doesn't exist or
        the cluster is no longer live."""
        row = self.db.conn.execute(
            """
            SELECT c.id, c.cluster_key, c.status, c.era_start, c.era_end,
                   (
                       SELECT COUNT(*) FROM face_cluster_members m
                       WHERE m.cluster_id = c.id
                         AND m.status IN ('proposed', 'accepted')
                   ) AS members,
                   (
                       SELECT p.display_name
                       FROM face_person_links l
                       JOIN face_persons p ON p.id = l.person_id
                       WHERE l.cluster_id = c.id AND l.status = 'accepted'
                       LIMIT 1
                   ) AS person_name
            FROM face_clusters c
            WHERE c.id = ? AND c.status IN ('proposed', 'accepted')
            """,
            (cluster_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "id": int(row[0]), "cluster_key": row[1], "status": row[2],
            "era_start": row[3], "era_end": row[4],
            "members": int(row[5]), "person_name": row[6],
        }

    def get_cluster_review_sample(self, cluster_id: int,
                                  limit: int = 5) -> list[dict]:
        """
        Sample a cluster for trustworthiness review: the medoid (member
        closest to the cluster centroid) first, then the boundary — members
        picked by farthest-point sampling, so they are maximally dissimilar
        both from the centroid and from each other. A coherent cluster shows
        the same person even at its edges; a contaminated one reveals the
        intruders here rather than in a flattering random sample.

        Each entry carries `similarity` (cosine to the centroid) and `role`
        ('core' or 'edge').
        """
        import numpy as np

        cur = self.db.conn.execute(
            """
            SELECT d.id, e.embedding, e.vector_dim,
                   json_extract(d.payload_json, '$.thumbnail_path'),
                   mm.capture_datetime
            FROM face_cluster_members m
            JOIN face_detections d ON d.id = m.detection_id
            JOIN face_embeddings e
              ON e.detection_id = d.id
             AND e.model_name = d.model_name
             AND e.model_version = d.model_version
            LEFT JOIN media_metadata mm ON mm.file_id = d.file_id
            WHERE m.cluster_id = ?
              AND m.status IN ('proposed', 'accepted')
              AND d.status = 'observed'
            """,
            (cluster_id,),
        )
        members = []
        for det_id, blob, dim, thumb, capture in cur.fetchall():
            members.append((int(det_id), np.frombuffer(blob, dtype=np.float32,
                                                       count=int(dim)),
                            thumb, capture))
        if not members:
            return []

        matrix = np.stack([m[1] for m in members])
        rep_row = self.db.conn.execute(
            "SELECT representative_embedding, representative_dim "
            "FROM face_clusters WHERE id = ?",
            (cluster_id,),
        ).fetchone()
        if rep_row and rep_row[0] is not None:
            centroid = np.frombuffer(rep_row[0], dtype=np.float32,
                                     count=int(rep_row[1]))
        else:
            centroid = matrix.mean(axis=0)
            norm = np.linalg.norm(centroid)
            if norm > 0:
                centroid = centroid / norm

        sims = matrix @ centroid
        order: list[int] = [int(np.argmax(sims))]  # medoid first

        if len(members) > 1:
            # Farthest-point sampling: each pick is the member LEAST similar
            # to everything already picked (medoid included), yielding a
            # boundary sample that is dissimilar to the centroid and
            # mutually dissimilar. closeness[i] = max similarity of member i
            # to any picked member; picking can only raise it.
            closeness = matrix @ matrix[order[0]]
            closeness[order[0]] = np.inf
            while len(order) < min(limit, len(members)):
                next_i = int(np.argmin(closeness))
                order.append(next_i)
                closeness = np.maximum(closeness, matrix @ matrix[next_i])
                closeness[next_i] = np.inf

        return [
            {
                "detection_id": members[i][0],
                "thumbnail_path": members[i][2],
                "capture_datetime": members[i][3],
                "similarity": float(sims[i]),
                "role": "core" if pos == 0 else "edge",
            }
            for pos, i in enumerate(order)
        ]

    def get_cluster_thumbnails(self, cluster_id: int, limit: int = 10) -> list[dict]:
        """Sample member detections with thumbnail paths and capture dates,
        best faces first. Non-face verdicts are excluded."""
        cur = self.db.conn.execute(
            """
            SELECT d.id, d.confidence,
                   json_extract(d.payload_json, '$.thumbnail_path'),
                   mm.capture_datetime
            FROM face_cluster_members m
            JOIN face_detections d ON d.id = m.detection_id
            LEFT JOIN media_metadata mm ON mm.file_id = d.file_id
            WHERE m.cluster_id = ?
              AND m.status IN ('proposed', 'accepted')
              AND d.status = 'observed'
            ORDER BY d.confidence DESC
            LIMIT ?
            """,
            (cluster_id, limit),
        )
        return [
            {
                "detection_id": int(row[0]),
                "confidence": row[1],
                "thumbnail_path": row[2],
                "capture_datetime": row[3],
            }
            for row in cur.fetchall()
        ]

    def get_person_detection_timeline(self, person_id: int) -> list[dict]:
        """Accepted detections of a person ordered by capture date, with
        thumbnails and age estimates for the timeline view."""
        cur = self.db.conn.execute(
            """
            SELECT DISTINCT d.id,
                   json_extract(d.payload_json, '$.thumbnail_path'),
                   json_extract(d.payload_json, '$.estimated_age'),
                   mm.capture_datetime
            FROM face_detections d
            LEFT JOIN media_metadata mm ON mm.file_id = d.file_id
            WHERE d.status = 'observed'
              AND (
                  d.id IN (
                      SELECT m.detection_id
                      FROM face_cluster_members m
                      JOIN face_person_links l
                        ON l.cluster_id = m.cluster_id AND l.status = 'accepted'
                      WHERE m.status = 'accepted' AND l.person_id = ?
                  )
                  OR d.id IN (
                      SELECT detection_id FROM face_person_links
                      WHERE detection_id IS NOT NULL
                        AND status = 'accepted' AND person_id = ?
                  )
              )
            ORDER BY mm.capture_datetime, d.id
            """,
            (person_id, person_id),
        )
        return [
            {
                "detection_id": int(row[0]),
                "thumbnail_path": row[1],
                "estimated_age": row[2],
                "capture_datetime": row[3],
            }
            for row in cur.fetchall()
        ]

    def get_photos_for_labeling(
        self,
        *,
        limit: int = 20,
        min_det_score: Optional[float] = None,
    ) -> list[dict]:
        """
        Sample the photos most worth labeling: each photo is scored by the
        summed size of the live, not-yet-person-linked clusters its faces
        belong to — so one label session resolves the maximum face mass.
        Photos whose faces are all linked (or junk) score zero and drop out.

        The result is spread across capture years (best photo per year first,
        then by score) so early labels cover the whole timeline rather than
        one photo-dense stretch.
        """
        score_clause = ""
        faces_score_clause = ""
        params: list = []
        if min_det_score is not None:
            faces_score_clause = " AND d2.confidence >= ?"
            params.append(min_det_score)
            score_clause = " AND d.confidence >= ?"
        cur = self.db.conn.execute(
            f"""
            SELECT d.file_id,
                   COALESCE(f.dest_path, f.orig_path) AS path,
                   f.type,
                   mm.capture_datetime,
                   -- Count what the Label Photos page will actually render:
                   -- every observed detection above the floor, not just the
                   -- ones contributing to the labeling-value score.
                   (
                       SELECT COUNT(*) FROM face_detections d2
                       WHERE d2.file_id = d.file_id
                         AND d2.status = 'observed'{faces_score_clause}
                   ) AS faces,
                   SUM(cs.size) AS score
            FROM face_detections d
            JOIN files f ON f.id = d.file_id AND f.status = 'active'
            LEFT JOIN media_metadata mm ON mm.file_id = d.file_id
            JOIN face_cluster_members m
              ON m.detection_id = d.id AND m.status IN ('proposed', 'accepted')
            JOIN face_clusters c
              ON c.id = m.cluster_id AND c.status IN ('proposed', 'accepted')
             AND c.id NOT IN (
                 SELECT cluster_id FROM face_person_links
                 WHERE cluster_id IS NOT NULL AND status = 'accepted'
             )
            JOIN (
                SELECT cluster_id, COUNT(*) AS size
                FROM face_cluster_members
                WHERE status IN ('proposed', 'accepted')
                GROUP BY cluster_id
            ) cs ON cs.cluster_id = m.cluster_id
            WHERE d.status = 'observed'{score_clause}
              AND d.id NOT IN (
                  SELECT detection_id FROM face_person_links
                  WHERE detection_id IS NOT NULL AND status = 'accepted'
              )
            GROUP BY d.file_id
            ORDER BY score DESC
            """,
            params + ([min_det_score] if min_det_score is not None else []),
        )
        rows = [
            {
                "file_id": int(r[0]),
                "path": r[1],
                "file_type": r[2],
                "capture_datetime": r[3],
                "faces": int(r[4]),
                "score": int(r[5]),
            }
            for r in cur.fetchall()
        ]

        # Year-spread pass: the best photo of each year is picked first so
        # early labels cover the whole timeline, but the picks themselves are
        # ordered by labeling value — the highest-payoff photo leads.
        seen_years: set = set()
        spread: list[dict] = []
        rest: list[dict] = []
        for row in rows:
            year = (row["capture_datetime"] or "")[:4]
            if year and year not in seen_years:
                seen_years.add(year)
                spread.append(row)
            else:
                rest.append(row)
        spread.sort(key=lambda r: -r["score"])
        return (spread + rest)[:limit]

    def get_photo_detections(
        self,
        file_id: int,
        *,
        min_det_score: Optional[float] = None,
    ) -> list[dict]:
        """
        The faces in one photo, with bbox, thumbnail, the person they resolve
        to (via accepted links, directly or through their cluster), and the
        largest live cluster they belong to (for label-whole-cluster).
        """
        score_clause = ""
        params: list = [file_id]
        if min_det_score is not None:
            score_clause = " AND d.confidence >= ?"
            params.append(min_det_score)
        cur = self.db.conn.execute(
            f"""
            SELECT d.id, d.bbox_x, d.bbox_y, d.bbox_w, d.bbox_h, d.confidence,
                   json_extract(d.payload_json, '$.thumbnail_path'),
                   (
                       SELECT l.person_id FROM face_person_links l
                       WHERE l.detection_id = d.id AND l.status = 'accepted'
                       LIMIT 1
                   ) AS direct_person,
                   (
                       SELECT l.person_id
                       FROM face_cluster_members m
                       JOIN face_person_links l
                         ON l.cluster_id = m.cluster_id AND l.status = 'accepted'
                       WHERE m.detection_id = d.id AND m.status = 'accepted'
                       LIMIT 1
                   ) AS cluster_person,
                   (
                       SELECT m.cluster_id
                       FROM face_cluster_members m
                       JOIN face_clusters c ON c.id = m.cluster_id
                       WHERE m.detection_id = d.id
                         AND m.status IN ('proposed', 'accepted')
                         AND c.status IN ('proposed', 'accepted')
                       ORDER BY (
                           SELECT COUNT(*) FROM face_cluster_members m2
                           WHERE m2.cluster_id = m.cluster_id
                             AND m2.status IN ('proposed', 'accepted')
                       ) DESC
                       LIMIT 1
                   ) AS largest_cluster
            FROM face_detections d
            WHERE d.file_id = ? AND d.status = 'observed'{score_clause}
            ORDER BY d.detection_index
            """,
            params,
        )
        rows = [
            {
                "detection_id": int(r[0]),
                "bbox": (r[1], r[2], r[3], r[4]),
                "confidence": r[5],
                "thumbnail_path": r[6],
                "person_id": r[7] if r[7] is not None else r[8],
                "largest_cluster_id": r[9],
            }
            for r in cur.fetchall()
        ]
        cluster_ids = {r["largest_cluster_id"] for r in rows
                       if r["largest_cluster_id"] is not None}
        sizes = {}
        if cluster_ids:
            placeholders = ",".join("?" for _ in cluster_ids)
            sizes = dict(self.db.conn.execute(
                f"""
                SELECT cluster_id, COUNT(*) FROM face_cluster_members
                WHERE cluster_id IN ({placeholders})
                  AND status IN ('proposed', 'accepted')
                GROUP BY cluster_id
                """,
                list(cluster_ids),
            ).fetchall())
        for row in rows:
            row["cluster_size"] = sizes.get(row["largest_cluster_id"], 0)
        return rows

    def get_persons_summary(self) -> list[dict]:
        """Active persons with accepted cluster and detection counts."""
        cur = self.db.conn.execute(
            """
            SELECT p.id, p.display_name, p.birth_date,
                   COUNT(DISTINCT l.cluster_id) AS clusters,
                   COUNT(DISTINCT m.detection_id) AS detections
            FROM face_persons p
            LEFT JOIN face_person_links l
              ON l.person_id = p.id AND l.status = 'accepted'
             AND l.cluster_id IS NOT NULL
            LEFT JOIN face_cluster_members m
              ON m.cluster_id = l.cluster_id AND m.status = 'accepted'
            WHERE p.status = 'active'
            GROUP BY p.id
            ORDER BY p.id
            """
        )
        return [
            {
                "id": int(row[0]),
                "display_name": row[1],
                "birth_date": row[2],
                "clusters": int(row[3]),
                "detections": int(row[4]),
            }
            for row in cur.fetchall()
        ]

    def get_photos_for_person(
        self,
        person_id: int,
        *,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> list[dict]:
        """
        Photos containing an accepted appearance of the person, through
        accepted cluster memberships and direct detection-level links.

        Each row resolves the RAW source at query time: when the matched
        file is an output linked in raw_outputs, raw_path carries the source
        RAW's canonical path.
        """
        filters = ""
        params: list = [person_id, person_id]
        if date_from:
            filters += " AND mm.capture_datetime >= ?"
            params.append(date_from)
        if date_to:
            filters += " AND mm.capture_datetime <= ?"
            params.append(date_to)
        cur = self.db.conn.execute(
            f"""
            SELECT DISTINCT f.id, COALESCE(f.dest_path, f.orig_path),
                   mm.capture_datetime, raw_f.dest_path
            FROM face_detections d
            JOIN files f ON f.id = d.file_id
            LEFT JOIN media_metadata mm ON mm.file_id = f.id
            LEFT JOIN raw_outputs ro ON ro.output_file_id = f.id
            LEFT JOIN files raw_f ON raw_f.id = ro.raw_file_id
            WHERE d.status = 'observed'
              AND (
                  d.id IN (
                      SELECT m.detection_id
                      FROM face_cluster_members m
                      JOIN face_person_links l
                        ON l.cluster_id = m.cluster_id AND l.status = 'accepted'
                      WHERE m.status = 'accepted' AND l.person_id = ?
                  )
                  OR d.id IN (
                      SELECT detection_id FROM face_person_links
                      WHERE detection_id IS NOT NULL
                        AND status = 'accepted' AND person_id = ?
                  )
              ){filters}
            ORDER BY mm.capture_datetime, f.id
            """,
            params,
        )
        return [
            {
                "file_id": int(row[0]),
                "path": row[1],
                "capture_datetime": row[2],
                "raw_path": row[3],
            }
            for row in cur.fetchall()
        ]

    def mark_merge_proposal_applied(self, *, action_id: int, run_id: int) -> None:
        """Resolve an accepted merge proposal as applied by this run."""
        self.db.conn.execute(
            """
            UPDATE run_actions
               SET status = 'applied', applied_by_run_id = ?, applied_at = ?
             WHERE id = ? AND status = 'proposed'
            """,
            (run_id, _iso_now(), action_id),
        )

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
