"""
Semi-supervised refinement.

After identities have been seeded/labeled, each named person's accepted
detections define an anchor embedding (L2-normalized mean). Refinement scans
clusters not yet linked to a person and, where a cluster's representative is
very close to exactly one anchor — with a clear margin over the second-best
match — proposes assigning the cluster to that person.

Assignments are proposals (`face_person_assign` run actions), not accepted
state: review them alongside merge suggestions and apply with
`photo-faces accept`. Re-running refine supersedes suggestions it no longer
generates (labels changed, thresholds changed).
"""
import logging
from pathlib import Path
from typing import Any, Dict

import numpy as np

from ..database.db import DBManager
from ..database.ops import DBOperations
from ..pipeline.actions import (
    ActionSpec,
    PHASE_FACE_MERGE_PROPOSE,
    RunActionRecorder,
)
from ..pipeline.lifecycle import supersede_stale_proposals
from . import config
from .db_ops import FaceDBOperations


class RefinementEngine:
    """Proposes high-confidence cluster→person assignments from anchors."""

    def __init__(self, db_path: Path,
                 threshold: float = config.AUTO_ASSIGN_THRESHOLD,
                 margin: float = config.AUTO_ASSIGN_MARGIN):
        self.db_manager = DBManager(db_path)
        self.threshold = threshold
        self.margin = margin

    def run(self, *, run_id: int) -> Dict[str, Any]:
        stats = {
            "anchors": 0,
            "clusters_evaluated": 0,
            "assignments_proposed": 0,
            "assignments_superseded": 0,
        }

        with self.db_manager as conn:
            db_ops = DBOperations(conn)
            face_ops = FaceDBOperations(db_ops)
            recorder = RunActionRecorder(db_ops, run_id)

            anchors = self._compute_anchors(face_ops)
            stats["anchors"] = len(anchors)
            candidates = face_ops.get_unassigned_cluster_representatives(
                model_name=config.MODEL_NAME,
                model_version=config.MODEL_VERSION_TAG,
            )
            stats["clusters_evaluated"] = len(candidates)

            if anchors:
                try:
                    from tqdm import tqdm
                except ImportError:  # pragma: no cover - tqdm is a core dependency
                    tqdm = lambda x, **kw: x  # noqa: E731

                person_ids = list(anchors)
                anchor_matrix = np.stack([anchors[pid] for pid in person_ids])

                for cluster in tqdm(candidates, desc="Matching clusters to anchors",
                                    unit="cluster"):
                    rep = np.asarray(cluster["representative"], dtype=np.float32)
                    similarities = anchor_matrix @ rep
                    order = np.argsort(similarities)[::-1]
                    best = float(similarities[order[0]])
                    second = float(similarities[order[1]]) if len(order) > 1 else -1.0

                    if best < self.threshold or best - second < self.margin:
                        continue

                    person_id = person_ids[int(order[0])]
                    recorder.record(ActionSpec(
                        action_type="face_person_assign",
                        entity_type="face_cluster",
                        entity_id=cluster["id"],
                        source_path=None,
                        target_path=None,
                        status="proposed",
                        phase=PHASE_FACE_MERGE_PROPOSE,
                        sequence=stats["assignments_proposed"] + 1,
                        idempotency_key=(
                            f"face_person_assign:{config.MODEL_NAME}:"
                            f"{config.MODEL_VERSION_TAG}:{cluster['id']}:{person_id}"
                        ),
                        confidence=int(round(best * 100)),
                        method="anchor_auto_assign",
                        payload={
                            "cluster_id": cluster["id"],
                            "cluster_key": cluster["cluster_key"],
                            "person_id": person_id,
                            "similarity": round(best, 3),
                            "margin": round(best - second, 3),
                        },
                    ))
                    stats["assignments_proposed"] += 1
            elif candidates:
                logging.info(
                    "No labeled persons with accepted detections yet — "
                    "seed and accept identities first."
                )

            stats["assignments_superseded"] = supersede_stale_proposals(
                db_ops, run_id,
            )
            conn.commit()

            logging.info(
                f"Refinement: {stats['assignments_proposed']} assignment(s) "
                f"proposed from {stats['anchors']} anchor(s) over "
                f"{stats['clusters_evaluated']} unassigned cluster(s); "
                f"{stats['assignments_superseded']} stale suggestion(s) superseded."
            )
        return stats

    @staticmethod
    def _compute_anchors(face_ops: FaceDBOperations) -> dict[int, np.ndarray]:
        anchors: dict[int, np.ndarray] = {}
        for person_id, embeddings in face_ops.get_labeled_person_embeddings().items():
            mean_emb = np.mean(np.asarray(embeddings, dtype=np.float32), axis=0)
            norm = np.linalg.norm(mean_emb)
            if norm > 0:
                mean_emb = mean_emb / norm
            anchors[person_id] = mean_emb
        return anchors
